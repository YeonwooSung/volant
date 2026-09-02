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
from .codec import BrokerError, FetchRecord

# Wire sentinel: unknown / not-committed offset (docs/PHASE3_SPEC.md).
OFFSET_UNKNOWN = (1 << 64) - 1

# Heartbeat / membership codes that mean "re-JoinGroup".
# 9 RebalanceInProgress, 10 UnknownMemberId, 11 IllegalGeneration.
_REJOIN_CODES = frozenset({9, 10, 11})

_DEFAULT_SESSION_TIMEOUT_MS = 10_000
_DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000
_POLL_MAX_MESSAGES = 100
_HB_INTERVAL_MIN_MS = 100
_HB_INTERVAL_MAX_MS = 3000
_ASSIGNOR_BROKER = "broker"
_ASSIGNOR_RANGE = "range"


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
    ) -> None:
        self._client = client
        self._group_id = group_id
        self._topics = list(topics)
        self._session_timeout_ms = session_timeout_ms
        self._group_instance_id = group_instance_id
        self._heartbeat_enabled = heartbeat
        self._assignor = _normalize_assignor(assignor)
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
    ) -> GroupConsumer:
        """Join ``group`` on ``topics``. Empty ``member_id`` on first join.

        ``group_instance_id`` is Phase 12 static membership (empty = dynamic).
        Re-join after error 9/10/11 resends the same instance id.
        ``heartbeat=True`` (default) starts the v0.37 background loop.
        ``heartbeat=False`` keeps v0.31 poll-only heartbeats.
        ``assignor`` is ``"broker"`` (default: honor JoinGroup assignment)
        or ``"range"`` (replace the fetch set with a solo local range after
        metadata). Unknown values raise ``ValueError``. Empty is ``"broker"``.
        ``auto_commit=False`` (default) keeps explicit ``commit()``. When
        on, a successful ``poll`` that returned records commits assigned
        positions (interval 0 = every such poll; else first successful
        poll, then every ``auto_commit_interval_ms``). Not Kafka
        ``enable.auto.commit`` (no background commit thread).
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
        )
        this._do_join()
        this._start_heartbeat()
        return this

    def _local_range_assignment(self) -> list[tuple[str, int]]:
        meta = self._client.metadata()
        counts: dict[str, int] = {}
        for topic in meta.topics:
            counts[topic.name] = len(topic.partitions)
        assigned = range_assign_multi(
            [self._member_id],
            [list(self._topics)],
            counts,
        )
        return list(assigned[0]) if assigned else []

    def _do_join(self) -> None:
        previous = list(self._assignment)
        result: JoinGroupResult = self._client.join_group(
            self._group_id,
            topics=self._topics,
            session_timeout_ms=self._session_timeout_ms,
            member_id=self._member_id,
            group_instance_id=self._group_instance_id,
        )
        self._member_id = result.member_id
        self._generation = result.generation
        new_assignment = [(a.topic, int(a.partition)) for a in result.assignment]
        if self._assignor == _ASSIGNOR_RANGE:
            new_assignment = self._local_range_assignment()

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

        for tp in self._assignment:
            self._positions.setdefault(tp, 0)

    def _fetch_positions_for(self, partitions: list[tuple[str, int]]) -> None:
        if not partitions:
            return
        by_topic: dict[str, list[int]] = {}
        for topic, partition in partitions:
            by_topic.setdefault(topic, []).append(partition)
        for topic, wanted in by_topic.items():
            fetched = self._client.offset_fetch(self._group_id, topic)
            found = {int(p): int(off) for p, off in fetched}
            for partition in wanted:
                if partition not in found:
                    continue
                off = found[partition]
                self._positions[(topic, partition)] = (
                    0 if off == OFFSET_UNKNOWN else off
                )

    def _heartbeat(self) -> None:
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
                max_messages=_POLL_MAX_MESSAGES,
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
        """Commit current positions with the joined member_id + generation."""
        with self._lock:
            self._ensure_open()
            self._commit_unlocked()

    def _commit_unlocked(self) -> None:
        if self._positions:
            assigned = set(self._assignment)
            for (topic, partition), offset in self._positions.items():
                if assigned and (topic, partition) not in assigned:
                    continue
                self._client.offset_commit(
                    self._group_id,
                    topic,
                    partition,
                    offset,
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
