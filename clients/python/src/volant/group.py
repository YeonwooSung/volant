"""High-level group-coordinated consumer (v0.31).

Mirrors ``crates/volant-client/src/group.rs`` on the existing sync
``Client`` RPCs: JoinGroup, Heartbeat, LeaveGroup, OffsetFetch,
OffsetCommit, Fetch. The broker still assigns partitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .client import Client, FetchResult, JoinGroupResult
from .codec import BrokerError, FetchRecord

# Wire sentinel: unknown / not-committed offset (docs/PHASE3_SPEC.md).
OFFSET_UNKNOWN = (1 << 64) - 1

# Heartbeat / membership codes that mean "re-JoinGroup".
# 9 RebalanceInProgress, 10 UnknownMemberId, 11 IllegalGeneration.
_REJOIN_CODES = frozenset({9, 10, 11})

_DEFAULT_SESSION_TIMEOUT_MS = 10_000
_POLL_MAX_MESSAGES = 100


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

    Example::

        from volant import Client, GroupConsumer
        c = Client("127.0.0.1:9092")
        g = GroupConsumer.join(c, group="g", topics=["t"], session_timeout_ms=10_000)
        recs = g.poll(max_wait_ms=500)
        g.commit()
        g.close()
    """

    def __init__(
        self,
        client: Client,
        group_id: str,
        topics: list[str],
        session_timeout_ms: int,
        group_instance_id: str = "",
    ) -> None:
        self._client = client
        self._group_id = group_id
        self._topics = list(topics)
        self._session_timeout_ms = session_timeout_ms
        self._group_instance_id = group_instance_id
        self._member_id = ""
        self._generation = 0
        self._assignment: list[tuple[str, int]] = []
        self._last_revoked: list[tuple[str, int]] = []
        self._positions: dict[tuple[str, int], int] = {}
        self._closed = False

    @classmethod
    def join(
        cls,
        client: Client,
        group: str,
        topics: Optional[list[str]] = None,
        session_timeout_ms: int = _DEFAULT_SESSION_TIMEOUT_MS,
        *,
        group_instance_id: str = "",
    ) -> GroupConsumer:
        """Join ``group`` on ``topics``. Empty ``member_id`` on first join."""
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
        )
        this._do_join()
        return this

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

        old_set = set(previous)
        new_set = set(new_assignment)

        revoked = sorted(old_set - new_set)
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
        self._ensure_open()
        try:
            self._heartbeat()
        except BrokerError as exc:
            if not _is_rejoin(exc):
                raise
            self._do_join()
        try:
            return self._fetch_assigned(max_wait_ms)
        except BrokerError as exc:
            if not _is_rejoin(exc):
                raise
            self._do_join()
            return self._fetch_assigned(max_wait_ms)

    def commit(self) -> None:
        """Commit current positions with the joined member_id + generation."""
        self._ensure_open()
        if not self._positions:
            return
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

    def close(self) -> None:
        """Leave the group. Does not close the underlying :class:`Client`."""
        if self._closed:
            return
        self._closed = True
        if self._member_id:
            self._client.leave_group(self._group_id, self._member_id)

    def leave(self) -> None:
        """Alias for :meth:`close` (Rust ``GroupConsumer::leave``)."""
        self.close()

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
        return self._member_id

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def assignment(self) -> list[tuple[str, int]]:
        return list(self._assignment)

    @property
    def last_revoked(self) -> list[tuple[str, int]]:
        return list(self._last_revoked)

    @property
    def positions(self) -> dict[tuple[str, int], int]:
        return dict(self._positions)

    @property
    def session_timeout_ms(self) -> int:
        return self._session_timeout_ms
