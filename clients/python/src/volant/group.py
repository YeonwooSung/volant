"""High-level group-coordinated consumer (v0.31 + v0.37 heartbeat).

Mirrors ``crates/volant-client/src/group.rs`` on the existing sync
``Client`` RPCs: JoinGroup, Heartbeat, LeaveGroup, OffsetFetch,
OffsetCommit, Fetch. The broker still assigns partitions.

v0.37 starts a background heartbeat thread after a successful join
(interval ``session_timeout_ms / 3``, clamped to 100–3000 ms). Pass
``heartbeat=False`` to keep the v0.31 poll-only loop.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from .assignor import range_assign_multi
from .client import Client, FetchResult, JoinGroupResult
from .codec import BrokerError, FetchRecord, OffsetCommitEntry

# Wire sentinel: unknown / not-committed offset (docs/PHASE3_SPEC.md).
OFFSET_UNKNOWN = (1 << 64) - 1

# Heartbeat / membership codes that mean "re-JoinGroup".
# 9 RebalanceInProgress, 10 UnknownMemberId, 11 IllegalGeneration.
_REJOIN_CODES = frozenset({9, 10, 11})

_DEFAULT_SESSION_TIMEOUT_MS = 10_000
_DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000
_POLL_MAX_MESSAGES = 100
_POLL_MAX_BYTES = 4 * 1024 * 1024
_HB_INTERVAL_MIN_MS = 100
_HB_INTERVAL_MAX_MS = 3000
_ASSIGNOR_BROKER = "broker"
_ASSIGNOR_RANGE = "range"
_RESET_EARLIEST = "earliest"
_RESET_LATEST = "latest"
_RESET_NONE = "none"


def heartbeat_interval_ms(session_timeout_ms: int) -> int:
    """Background heartbeat period: ``session_timeout_ms / 3``, clamped."""
    interval = session_timeout_ms // 3
    if interval < _HB_INTERVAL_MIN_MS:
        return _HB_INTERVAL_MIN_MS
    if interval > _HB_INTERVAL_MAX_MS:
        return _HB_INTERVAL_MAX_MS
    return interval


def _normalize_assignor(name: Optional[str]) -> str:
    if name is None or name == "" or name == _ASSIGNOR_BROKER:
        return _ASSIGNOR_BROKER
    if name == _ASSIGNOR_RANGE:
        return _ASSIGNOR_RANGE
    raise ValueError(f"unknown assignor: {name!r}")


def _normalize_auto_offset_reset(name: Optional[str]) -> str:
    if name is None or name == "" or name == _RESET_EARLIEST:
        return _RESET_EARLIEST
    if name == _RESET_LATEST:
        return _RESET_LATEST
    if name == _RESET_NONE:
        return _RESET_NONE
    raise ValueError(f"unknown auto_offset_reset: {name!r}")


def _clamp_fetch_max_messages(n: int) -> int:
    if n is None or n <= 0:
        return _POLL_MAX_MESSAGES
    return int(n)


def _clamp_fetch_max_bytes(n: int) -> int:
    if n is None or n <= 0:
        return _POLL_MAX_BYTES
    return int(n)


@dataclass
class FetchedRecord:
    """A record returned by :meth:`GroupConsumer.poll` with topic context."""

    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes
    timestamp_ms: int = -1
    headers: list[tuple[str, bytes]] = field(default_factory=list)

    @classmethod
    def from_fetch(cls, topic: str, partition: int, rec: FetchRecord) -> FetchedRecord:
        return cls(
            topic=topic,
            partition=partition,
            offset=rec.offset,
            key=rec.key,
            value=rec.value,
            timestamp_ms=rec.timestamp_ms,
            headers=list(rec.headers),
        )


def _is_rejoin(exc: BaseException) -> bool:
    return isinstance(exc, BrokerError) and exc.code in _REJOIN_CODES


class GroupConsumer:
    """Join a group, poll assigned partitions, commit positions, leave.

    After a successful join a background thread heartbeats at
    :func:`heartbeat_interval_ms` so a silent consumer does not expire.
    ``poll`` / ``commit`` share an internal lock with that thread (join
    state + GroupConsumer RPCs) but are **not** a fully concurrent API:
    do not call them from multiple threads, and do not use the same
    ``Client`` for other RPCs while the consumer is open.

    Example::

        from volant import Client, GroupConsumer
        c = Client("127.0.0.1:9092")
        g = GroupConsumer.join(c, group="g", topics=["t"], session_timeout_ms=10_000)
        recs = g.poll(max_wait_ms=500)
        g.commit()
        g.close()
        # Opt-in auto-commit (v0.48). Default off. interval 0 = after every poll.
        g = GroupConsumer.join(
            c, group="g", topics=["t"], auto_commit=True, auto_commit_interval_ms=5000
        )
        # Opt-in auto_offset_reset (v0.62/v0.70). Default earliest (ListOffsets earliest).
        g = GroupConsumer.join(c, group="g", topics=["t"], auto_offset_reset="latest")
        # Poll fetch size (v0.75). Default 100 / 4MiB; not Kafka max.poll.records.
        g = GroupConsumer.join(c, group="g", topics=["t"], fetch_max_messages=10)
    """

    def __init__(
        self,
        client: Client,
        group_id: str,
        topics: list[str],
        session_timeout_ms: int,
        group_instance_id: str = "",
        heartbeat: bool = True,
        assignor: str = _ASSIGNOR_BROKER,
        auto_commit: bool = False,
        auto_commit_interval_ms: int = _DEFAULT_AUTO_COMMIT_INTERVAL_MS,
        auto_offset_reset: str = _RESET_EARLIEST,
        fetch_max_messages: int = _POLL_MAX_MESSAGES,
        fetch_max_bytes: int = _POLL_MAX_BYTES,
    ) -> None:
        self._client = client
        self._group_id = group_id
        self._topics = list(topics)
        self._session_timeout_ms = session_timeout_ms
        self._group_instance_id = group_instance_id
        self._heartbeat_enabled = heartbeat
        self._assignor = _normalize_assignor(assignor)
        self._auto_offset_reset = _normalize_auto_offset_reset(auto_offset_reset)
        self._fetch_max_messages = _clamp_fetch_max_messages(fetch_max_messages)
        self._fetch_max_bytes = _clamp_fetch_max_bytes(fetch_max_bytes)
        self._auto_commit = auto_commit
        self._auto_commit_interval_ms = max(0, auto_commit_interval_ms)
        self._last_auto_commit: Optional[float] = None
        self._dirty = False
        self._member_id = ""
        self._generation = 0
        self._assignment: list[tuple[str, int]] = []
        self._last_revoked: list[tuple[str, int]] = []
        self._positions: dict[tuple[str, int], int] = {}
        self._closed = False
        self._heartbeat_count = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None

    @classmethod
    def join(
        cls,
        client: Client,
        group: str,
        topics: Optional[list[str]] = None,
        session_timeout_ms: int = _DEFAULT_SESSION_TIMEOUT_MS,
        *,
        group_instance_id: str = "",
        heartbeat: bool = True,
        assignor: str = _ASSIGNOR_BROKER,
        auto_commit: bool = False,
        auto_commit_interval_ms: int = _DEFAULT_AUTO_COMMIT_INTERVAL_MS,
        auto_offset_reset: str = _RESET_EARLIEST,
        fetch_max_messages: int = _POLL_MAX_MESSAGES,
        fetch_max_bytes: int = _POLL_MAX_BYTES,
    ) -> GroupConsumer:
        """Join ``group`` on ``topics``. Empty ``member_id`` on first join.

        ``group_instance_id`` is Phase 12 static membership (empty = dynamic).
        Re-join after error 9/10/11 resends the same instance id.
        ``heartbeat=True`` (default) starts the v0.37 background loop.
        ``heartbeat=False`` keeps v0.31 poll-only heartbeats.
        ``assignor`` is ``"broker"`` (default: honor JoinGroup assignment)
        or ``"range"`` (replace the fetch set with a local range over
        JoinGroup members, else DescribeGroup; still no SyncGroup).
        Unknown values raise
        ``ValueError``. Empty is ``"broker"``.
        ``auto_commit=False`` (default) keeps explicit ``commit()``. When
        on, a successful ``poll`` that returned records commits assigned
        positions (interval 0 = every such poll; else first successful
        poll, then every ``auto_commit_interval_ms``). Not Kafka
        ``enable.auto.commit`` (no background commit thread).
        ``auto_offset_reset`` is ``"earliest"`` (default: native ListOffsets
        earliest), ``"latest"`` (ListOffsets latest / LEO), or ``"none"``
        (raise if OffsetFetch is missing / ``OFFSET_UNKNOWN``). Invalid
        strings raise ``ValueError`` before JoinGroup. Not Kafka
        ``auto.offset.reset`` (no timestamp).
        ``fetch_max_messages`` / ``fetch_max_bytes`` bound each assigned
        ``fetch`` inside ``poll`` (default 100 / 4MiB). Values ``<= 0``
        clamp to those defaults. Not Kafka ``max.poll.records``.
        ``poll`` still takes only ``max_wait_ms``.
        """
        timeout = (
            _DEFAULT_SESSION_TIMEOUT_MS
            if session_timeout_ms == 0
            else session_timeout_ms
        )
        this = cls(
            client,
            group,
            list(topics) if topics else [],
            timeout,
            group_instance_id=group_instance_id,
            heartbeat=heartbeat,
            assignor=assignor,
            auto_commit=auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            auto_offset_reset=auto_offset_reset,
            fetch_max_messages=fetch_max_messages,
            fetch_max_bytes=fetch_max_bytes,
        )
        this._do_join()
        this._start_heartbeat()
        return this

    def _range_members_from_join(
        self, result: JoinGroupResult
    ) -> tuple[list[str], list[list[str]]]:
        """JoinGroup trailer members for local range, or empty lists to fall back."""
        members = list(getattr(result, "members", None) or [])
        if not members:
            return [], []
        topics = [list(self._topics) for _ in members]
        return members, topics

    def _range_members_from_describe(self) -> tuple[list[str], list[list[str]]]:
        """DescribeGroup members for local range, or empty lists to solo-fallback."""
        try:
            desc = self._client.describe_group(self._group_id)
        except Exception:
            return [], []
        members = getattr(desc, "members", None) or []
        ids: list[str] = []
        topics: list[list[str]] = []
        seen = False
        for member in members:
            mid = getattr(member, "member_id", "")
            subscribed = list(getattr(member, "topics", None) or [])
            ids.append(mid)
            topics.append(subscribed)
            if mid == self._member_id:
                seen = True
        if not seen:
            ids.append(self._member_id)
            topics.append(list(self._topics))
        if not ids or self._member_id not in ids:
            return [], []
        return ids, topics

    def _local_range_assignment(
        self, result: JoinGroupResult | None = None
    ) -> list[tuple[str, int]]:
        meta = self._client.metadata()
        counts: dict[str, int] = {}
        for topic in meta.topics:
            counts[topic.name] = len(topic.partitions)
        member_ids: list[str] = []
        member_topics: list[list[str]] = []
        if result is not None:
            member_ids, member_topics = self._range_members_from_join(result)
        if not member_ids:
            member_ids, member_topics = self._range_members_from_describe()
        if not member_ids:
            member_ids = [self._member_id]
            member_topics = [list(self._topics)]
        assigned = range_assign_multi(member_ids, member_topics, counts)
        if not assigned:
            return []
        try:
            idx = member_ids.index(self._member_id)
        except ValueError:
            assigned = range_assign_multi(
                [self._member_id],
                [list(self._topics)],
                counts,
            )
            return list(assigned[0]) if assigned else []
        return list(assigned[idx])

    def _do_join(self) -> None:
        previous = list(self._assignment)
        # v0.220: retry Join on generation-fence 9 only. Default
        # max_retries=0 (first 9 surfaces). 10/11 stay on the existing
        # heartbeat/poll rejoin path. Do not bump heartbeat_count.
        max_retries = max(0, int(getattr(self._client, "max_retries", 0) or 0))
        retry_attempt = 0
        while True:
            try:
                result: JoinGroupResult = self._client.join_group(
                    self._group_id,
                    topics=self._topics,
                    session_timeout_ms=self._session_timeout_ms,
                    member_id=self._member_id,
                    group_instance_id=self._group_instance_id,
                )
                break
            except BrokerError as exc:
                if exc.code == 9 and retry_attempt < max_retries:
                    retry_attempt += 1
                    ms = max(
                        0, int(getattr(self._client, "retry_backoff_ms", 0) or 0)
                    )
                    if ms:
                        time.sleep(ms / 1000.0)
                    continue
                raise
        self._member_id = result.member_id
        self._generation = result.generation
        new_assignment = [(a.topic, int(a.partition)) for a in result.assignment]
        # SyncGroup peek/confirm (v0.207). Best-effort: empty or any
        # error (including 9/10/11) keeps the JoinGroup assignment.
        try:
            synced = self._client.sync_group(
                self._group_id, self._member_id, self._generation
            )
            if synced:
                new_assignment = [(a.topic, int(a.partition)) for a in synced]
        except Exception:
            pass
        if self._assignor == _ASSIGNOR_RANGE:
            new_assignment = self._local_range_assignment(result)

        old_set = set(previous)
        new_set = set(new_assignment)

        revoked = sorted(old_set - new_set)
        if self._assignor != _ASSIGNOR_RANGE:
            for a in result.revoked:
                tp = (a.topic, int(a.partition))
                if tp not in revoked:
                    revoked.append(tp)
            revoked.sort()

        added = sorted(new_set - old_set)

        for tp in revoked:
            self._positions.pop(tp, None)

        self._assignment = new_assignment
        self._last_revoked = revoked

        if added or (not self._positions and self._assignment):
            # First join: positions empty and assignment full → fetch all.
            # Rebalance: only OffsetFetch newly assigned partitions.
            to_fetch = new_assignment if not previous else added
            self._fetch_positions_for(to_fetch)

        missing = [tp for tp in self._assignment if tp not in self._positions]
        if missing:
            self._apply_reset(missing)

    def _fetch_positions_for(self, partitions: list[tuple[str, int]]) -> None:
        if not partitions:
            return
        by_topic: dict[str, list[int]] = {}
        for topic, partition in partitions:
            by_topic.setdefault(topic, []).append(partition)
        unknown: list[tuple[str, int]] = []
        for topic, wanted in by_topic.items():
            fetched = self._client.offset_fetch(self._group_id, topic)
            found = {int(p): int(off) for p, off in fetched}
            for partition in wanted:
                off = found.get(partition)
                if off is None or off == OFFSET_UNKNOWN:
                    unknown.append((topic, partition))
                    continue
                self._positions[(topic, partition)] = off
        if unknown:
            self._apply_reset(unknown)

    def _apply_reset(self, partitions: list[tuple[str, int]]) -> None:
        if not partitions:
            return
        if self._auto_offset_reset == _RESET_NONE:
            topic, partition = partitions[0]
            raise ValueError(
                f"no committed offset for {topic}-{partition} "
                f"and auto_offset_reset={self._auto_offset_reset!r}"
            )
        use_earliest = self._auto_offset_reset == _RESET_EARLIEST
        by_topic: dict[str, list[int]] = {}
        for topic, partition in partitions:
            by_topic.setdefault(topic, []).append(partition)
        for topic, wanted in by_topic.items():
            listings = self._client.list_offsets(topic, wanted)
            found = {
                int(e.partition): int(e.earliest if use_earliest else e.latest)
                for e in listings
            }
            for partition in wanted:
                if partition not in found:
                    raise ValueError(
                        f"list_offsets missing partition {topic}-{partition}"
                    )
                self._positions[(topic, partition)] = found[partition]

    def _heartbeat(self) -> None:
        self._heartbeat_count += 1
        self._client.heartbeat(self._group_id, self._member_id, self._generation)

    def _fetch_assigned(self, max_wait_ms: int) -> list[FetchedRecord]:
        out: list[FetchedRecord] = []
        revoked = set(self._last_revoked)
        for topic, partition in list(self._assignment):
            if (topic, partition) in revoked:
                continue
            from_off = self._positions.get((topic, partition), 0)
            batch: FetchResult = self._client.fetch(
                topic,
                partition,
                from_off,
                max_messages=_clamp_fetch_max_messages(self._fetch_max_messages),
                max_bytes=_clamp_fetch_max_bytes(self._fetch_max_bytes),
                max_wait_ms=max_wait_ms,
            )
            for rec in batch.records:
                nxt = rec.offset + 1 if rec.offset < OFFSET_UNKNOWN else rec.offset
                self._positions[(topic, partition)] = nxt
                out.append(FetchedRecord.from_fetch(topic, partition, rec))
        return out

    def poll(self, max_wait_ms: int = 500) -> list[FetchedRecord]:
        """Heartbeat, fetch each assigned partition, advance positions.

        On broker error 9/10/11 (rebalance / unknown member / illegal
        generation) re-joins and retries the fetch once.
        """
        with self._lock:
            self._ensure_open()
            try:
                self._heartbeat()
            except BrokerError as exc:
                if not _is_rejoin(exc):
                    raise
                self._do_join()
            try:
                recs = self._fetch_assigned(max_wait_ms)
            except BrokerError as exc:
                if not _is_rejoin(exc):
                    raise
                self._do_join()
                recs = self._fetch_assigned(max_wait_ms)
            if recs:
                self._dirty = True
                self._maybe_auto_commit()
            return recs

    def commit(self) -> None:
        """Commit assigned positions in one OffsetCommit (member_id + generation)."""
        with self._lock:
            self._ensure_open()
            self._commit_unlocked()

    def _commit_unlocked(self) -> None:
        if self._positions:
            assigned = set(self._assignment)
            entries = [
                OffsetCommitEntry(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    metadata="",
                )
                for (topic, partition), offset in self._positions.items()
                if not assigned or (topic, partition) in assigned
            ]
            if entries:
                self._client.commit_offsets(
                    self._group_id,
                    entries,
                    member_id=self._member_id,
                    generation=self._generation,
                )
        self._last_auto_commit = time.monotonic()
        self._dirty = False

    def _maybe_auto_commit(self) -> None:
        if not self._auto_commit:
            return
        now = time.monotonic()
        if self._auto_commit_interval_ms > 0 and self._last_auto_commit is not None:
            elapsed_ms = (now - self._last_auto_commit) * 1000.0
            if elapsed_ms < self._auto_commit_interval_ms:
                return
        self._commit_unlocked()

    def close(self) -> None:
        """Stop the heartbeat thread (if any), then LeaveGroup.

        Does not close the underlying :class:`Client`. Idempotent.
        Auto-commit on + uncommitted positions: best-effort commit once
        (errors swallowed), then leave.
        """
        self._stop.set()
        t = self._hb_thread
        if t is not None and t is not threading.current_thread():
            t.join(timeout=5.0)
        with self._lock:
            if self._closed:
                return
            if self._auto_commit and self._dirty:
                try:
                    self._commit_unlocked()
                except Exception:
                    pass
            self._closed = True
            if self._member_id:
                self._client.leave_group(self._group_id, self._member_id)

    def leave(self) -> None:
        """Alias for :meth:`close` (Rust ``GroupConsumer::leave``)."""
        self.close()

    def _start_heartbeat(self) -> None:
        if not self._heartbeat_enabled:
            return
        self._stop.clear()
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="volant-group-heartbeat",
            daemon=True,
        )
        self._hb_thread.start()

    def _heartbeat_loop(self) -> None:
        interval = heartbeat_interval_ms(self._session_timeout_ms) / 1000.0
        while not self._stop.wait(interval):
            try:
                self._heartbeat_once()
            except Exception:
                continue

    def _heartbeat_once(self) -> None:
        with self._lock:
            if self._closed or self._stop.is_set():
                return
            try:
                self._heartbeat()
            except BrokerError as exc:
                if not _is_rejoin(exc):
                    return
                self._do_join()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("GroupConsumer is closed")

    def __enter__(self) -> GroupConsumer:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def member_id(self) -> str:
        with self._lock:
            return self._member_id

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    @property
    def assignment(self) -> list[tuple[str, int]]:
        with self._lock:
            return list(self._assignment)

    @property
    def last_revoked(self) -> list[tuple[str, int]]:
        with self._lock:
            return list(self._last_revoked)

    @property
    def positions(self) -> dict[tuple[str, int], int]:
        with self._lock:
            return dict(self._positions)

    @property
    def session_timeout_ms(self) -> int:
        return self._session_timeout_ms

    @property
    def group_instance_id(self) -> str:
        return self._group_instance_id

    @property
    def assignor(self) -> str:
        return self._assignor

    @property
    def heartbeat_count(self) -> int:
        """Heartbeat RPCs issued by poll + background (not JoinGroup / SyncGroup)."""
        return self._heartbeat_count

    @property
    def auto_offset_reset(self) -> str:
        return self._auto_offset_reset

    @property
    def fetch_max_messages(self) -> int:
        return self._fetch_max_messages

    @fetch_max_messages.setter
    def fetch_max_messages(self, value: int) -> None:
        self._fetch_max_messages = _clamp_fetch_max_messages(value)

    @property
    def fetch_max_bytes(self) -> int:
        return self._fetch_max_bytes

    @fetch_max_bytes.setter
    def fetch_max_bytes(self, value: int) -> None:
        self._fetch_max_bytes = _clamp_fetch_max_bytes(value)
