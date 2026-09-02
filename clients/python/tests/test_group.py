"""Unit tests for GroupConsumer against a fake Client."""

from __future__ import annotations

import unittest

from volant import FetchedRecord, GroupConsumer
from volant.client import FetchResult, JoinGroupResult
from volant.codec import Assignment, BrokerError, FetchRecord
from volant.group import OFFSET_UNKNOWN


class FakeClient:
    """Duck-typed Client: records calls, serves scripted replies."""

    def __init__(self) -> None:
        self.joins: list[dict] = []
        self.heartbeats: list[tuple[str, str, int]] = []
        self.leaves: list[tuple[str, str]] = []
        self.fetches: list[tuple[str, int, int, int]] = []
        self.commits: list[dict] = []
        self.offset_fetches: list[tuple[str, str]] = []

        self.join_queue: list[JoinGroupResult] = []
        self.heartbeat_codes: list[int] = []
        self.fetch_error: BrokerError | None = None
        self.fetch_error_once: BrokerError | None = None
        self.log: dict[tuple[str, int], list[FetchRecord]] = {}
        self.committed: dict[tuple[str, int], int] = {}

    def join_group(
        self,
        group: str,
        topics=None,
        session_timeout_ms: int = 10_000,
        *,
        member_id: str = "",
        group_instance_id: str = "",
    ) -> JoinGroupResult:
        self.joins.append(
            {
                "group": group,
                "topics": list(topics) if topics else [],
                "session_timeout_ms": session_timeout_ms,
                "member_id": member_id,
                "group_instance_id": group_instance_id,
            }
        )
        if self.join_queue:
            return self.join_queue.pop(0)
        return JoinGroupResult(
            member_id="m1",
            generation=1,
            assignment=[Assignment(topic="t", partition=0)],
        )

    def heartbeat(self, group: str, member_id: str, generation: int) -> int:
        self.heartbeats.append((group, member_id, generation))
        code = self.heartbeat_codes.pop(0) if self.heartbeat_codes else 0
        if code != 0:
            raise BrokerError(code, op="heartbeat")
        return 0

    def leave_group(self, group: str, member_id: str) -> None:
        self.leaves.append((group, member_id))

    def fetch(
        self,
        topic: str,
        partition: int,
        offset: int = 0,
        *,
        max_messages: int = 128,
        max_bytes: int = 4 * 1024 * 1024,
        max_wait_ms: int = 0,
    ) -> FetchResult:
        del max_bytes
        self.fetches.append((topic, partition, offset, max_wait_ms))
        if self.fetch_error is not None:
            raise self.fetch_error
        if self.fetch_error_once is not None:
            err = self.fetch_error_once
            self.fetch_error_once = None
            raise err
        recs = [
            r
            for r in self.log.get((topic, partition), [])
            if r.offset >= offset
        ][:max_messages]
        hwm = (recs[-1].offset + 1) if recs else offset
        return FetchResult(
            topic=topic, partition=partition, high_watermark=hwm, records=recs
        )

    def offset_commit(
        self,
        group: str,
        topic: str,
        partition: int,
        offset: int,
        *,
        member_id: str = "",
        generation: int = 0,
        metadata: str = "",
    ) -> None:
        self.commits.append(
            {
                "group": group,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "member_id": member_id,
                "generation": generation,
                "metadata": metadata,
            }
        )
        self.committed[(topic, partition)] = offset

    def offset_fetch(self, group: str, topic: str) -> list[tuple[int, int]]:
        self.offset_fetches.append((group, topic))
        return [
            (p, off) for (t, p), off in self.committed.items() if t == topic
        ]


def _rec(offset: int, value: bytes) -> FetchRecord:
    return FetchRecord(offset=offset, timestamp_ms=0, key=None, value=value)


def _join(
    assignment: list[tuple[str, int]],
    *,
    member_id: str = "m1",
    generation: int = 1,
    revoked: list[tuple[str, int]] | None = None,
) -> JoinGroupResult:
    return JoinGroupResult(
        member_id=member_id,
        generation=generation,
        assignment=[Assignment(topic=t, partition=p) for t, p in assignment],
        revoked=[Assignment(topic=t, partition=p) for t, p in (revoked or [])],
    )


class TestGroupConsumer(unittest.TestCase):
    def test_join_fetches_committed_positions(self) -> None:
        c = FakeClient()
        c.committed[("t", 0)] = 5
        c.join_queue.append(_join([("t", 0)]))
        g = GroupConsumer.join(c, group="g", topics=["t"], session_timeout_ms=10_000)
        self.assertEqual(g.member_id, "m1")
        self.assertEqual(g.generation, 1)
        self.assertEqual(g.assignment, [("t", 0)])
        self.assertEqual(g.positions, {("t", 0): 5})
        self.assertEqual(c.joins[0]["member_id"], "")
        self.assertEqual(c.offset_fetches, [("g", "t")])

    def test_join_unknown_offset_starts_at_zero(self) -> None:
        c = FakeClient()
        c.committed[("t", 0)] = OFFSET_UNKNOWN
        c.join_queue.append(_join([("t", 0)]))
        g = GroupConsumer.join(c, "g", ["t"])
        self.assertEqual(g.positions, {("t", 0): 0})

    def test_join_missing_offset_starts_at_zero(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0), ("t", 1)]))
        g = GroupConsumer.join(c, "g", ["t"])
        self.assertEqual(g.positions, {("t", 0): 0, ("t", 1): 0})

    def test_session_timeout_zero_defaults(self) -> None:
        c = FakeClient()
        g = GroupConsumer.join(c, "g", ["t"], session_timeout_ms=0)
        self.assertEqual(g.session_timeout_ms, 10_000)
        self.assertEqual(c.joins[0]["session_timeout_ms"], 10_000)

    def test_join_sends_group_instance_id(self) -> None:
        c = FakeClient()
        g = GroupConsumer.join(c, "g", ["t"], group_instance_id="inst-1")
        self.assertEqual(c.joins[0]["group_instance_id"], "inst-1")
        self.assertEqual(c.joins[0]["member_id"], "")
        self.assertEqual(g.group_instance_id, "inst-1")

    def test_join_default_is_dynamic(self) -> None:
        c = FakeClient()
        g = GroupConsumer.join(c, "g", ["t"])
        self.assertEqual(c.joins[0]["group_instance_id"], "")
        self.assertEqual(g.group_instance_id, "")

    def test_rejoin_keeps_group_instance_id(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)], generation=1))
        c.join_queue.append(_join([("t", 0)], member_id="m1", generation=2))
        c.heartbeat_codes.append(9)
        g = GroupConsumer.join(c, "g", ["t"], group_instance_id="inst-1")
        g.poll()
        self.assertEqual(len(c.joins), 2)
        self.assertEqual(c.joins[0]["group_instance_id"], "inst-1")
        self.assertEqual(c.joins[1]["group_instance_id"], "inst-1")
        self.assertEqual(c.joins[1]["member_id"], "m1")
        self.assertEqual(g.group_instance_id, "inst-1")

    def test_poll_fetches_and_advances(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)]))
        c.log[("t", 0)] = [_rec(0, b"a"), _rec(1, b"b")]
        g = GroupConsumer.join(c, "g", ["t"])
        recs = g.poll(max_wait_ms=500)
        self.assertEqual(len(recs), 2)
        self.assertIsInstance(recs[0], FetchedRecord)
        self.assertEqual(recs[0].topic, "t")
        self.assertEqual(recs[0].partition, 0)
        self.assertEqual(recs[0].value, b"a")
        self.assertEqual(recs[1].value, b"b")
        self.assertEqual(g.positions, {("t", 0): 2})
        self.assertEqual(c.heartbeats, [("g", "m1", 1)])
        self.assertEqual(c.fetches, [("t", 0, 0, 500)])

    def test_commit_uses_joined_member_and_generation(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)]))
        c.log[("t", 0)] = [_rec(0, b"a")]
        g = GroupConsumer.join(c, "g", ["t"])
        g.poll()
        g.commit()
        self.assertEqual(len(c.commits), 1)
        commit = c.commits[0]
        self.assertEqual(commit["group"], "g")
        self.assertEqual(commit["topic"], "t")
        self.assertEqual(commit["partition"], 0)
        self.assertEqual(commit["offset"], 1)
        self.assertEqual(commit["member_id"], "m1")
        self.assertEqual(commit["generation"], 1)
        self.assertNotEqual(commit["member_id"], "")

    def test_close_leaves_group(self) -> None:
        c = FakeClient()
        g = GroupConsumer.join(c, "g", ["t"])
        g.close()
        self.assertEqual(c.leaves, [("g", "m1")])
        g.close()
        self.assertEqual(c.leaves, [("g", "m1")])
        with self.assertRaises(RuntimeError):
            g.poll()

    def test_context_manager_leaves(self) -> None:
        c = FakeClient()
        with GroupConsumer.join(c, "g", ["t"]) as g:
            self.assertEqual(g.member_id, "m1")
        self.assertEqual(c.leaves, [("g", "m1")])

    def test_poll_rejoins_on_heartbeat_error_9(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0), ("t", 1)], generation=1))
        c.join_queue.append(
            _join([("t", 0)], member_id="m1", generation=2, revoked=[("t", 1)])
        )
        c.heartbeat_codes.append(9)
        c.log[("t", 0)] = [_rec(0, b"keep")]
        c.log[("t", 1)] = [_rec(0, b"revoked")]
        g = GroupConsumer.join(c, "g", ["t"])
        self.assertEqual(set(g.assignment), {("t", 0), ("t", 1)})
        recs = g.poll()
        self.assertEqual(g.generation, 2)
        self.assertEqual(g.assignment, [("t", 0)])
        self.assertEqual(g.last_revoked, [("t", 1)])
        self.assertEqual([r.value for r in recs], [b"keep"])
        self.assertNotIn(("t", 1), g.positions)
        fetched_tps = {(t, p) for t, p, _off, _w in c.fetches}
        self.assertEqual(fetched_tps, {("t", 0)})
        self.assertEqual(len(c.joins), 2)

    def test_poll_rejoins_on_unknown_member_10(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)], generation=1))
        c.join_queue.append(_join([("t", 0)], member_id="m2", generation=2))
        c.heartbeat_codes.append(10)
        g = GroupConsumer.join(c, "g", ["t"])
        g.poll()
        self.assertEqual(g.member_id, "m2")
        self.assertEqual(g.generation, 2)

    def test_poll_fetch_error_9_retries_once(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)], generation=1))
        c.join_queue.append(_join([("t", 0)], generation=2))
        c.fetch_error_once = BrokerError(9, op="fetch")
        c.log[("t", 0)] = [_rec(0, b"x")]
        g = GroupConsumer.join(c, "g", ["t"])
        recs = g.poll()
        self.assertEqual([r.value for r in recs], [b"x"])
        self.assertEqual(g.generation, 2)
        self.assertEqual(len(c.joins), 2)

    def test_poll_fetch_error_9_retry_once_then_raises(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)], generation=1))
        c.join_queue.append(_join([("t", 0)], generation=2))
        c.fetch_error = BrokerError(9, op="fetch")
        g = GroupConsumer.join(c, "g", ["t"])
        with self.assertRaises(BrokerError) as ctx:
            g.poll()
        self.assertEqual(ctx.exception.code, 9)
        self.assertEqual(len(c.joins), 2)

    def test_poll_other_broker_error_does_not_rejoin(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)]))
        c.heartbeat_codes.append(15)
        g = GroupConsumer.join(c, "g", ["t"])
        with self.assertRaises(BrokerError) as ctx:
            g.poll()
        self.assertEqual(ctx.exception.code, 15)
        self.assertEqual(len(c.joins), 1)

    def test_cooperative_retains_positions_drops_revoked(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0), ("t", 1)], generation=1))
        c.log[("t", 0)] = [_rec(0, b"p0")]
        c.log[("t", 1)] = [_rec(0, b"p1")]
        g = GroupConsumer.join(c, "g", ["t"])
        recs = g.poll()
        self.assertEqual(len(recs), 2)
        self.assertEqual(g.positions, {("t", 0): 1, ("t", 1): 1})
        snapshot = g.positions

        c.heartbeat_codes.append(9)
        c.join_queue.append(
            _join([("t", 0)], generation=2, revoked=[("t", 1)])
        )
        c.log[("t", 0)] = [_rec(0, b"p0")]  # already consumed; from=1 → empty
        g.poll()
        self.assertEqual(g.assignment, [("t", 0)])
        self.assertEqual(g.last_revoked, [("t", 1)])
        self.assertEqual(g.positions[("t", 0)], snapshot[("t", 0)])
        self.assertNotIn(("t", 1), g.positions)
        # Rejoin OffsetFetch only the newly added set (none here).
        self.assertEqual(c.offset_fetches, [("g", "t")])

    def test_cooperative_offset_fetch_only_added(self) -> None:
        c = FakeClient()
        c.join_queue.append(_join([("t", 0)], generation=1))
        g = GroupConsumer.join(c, "g", ["t"])
        self.assertEqual(c.offset_fetches, [("g", "t")])
        c.committed[("t", 1)] = 7
        c.heartbeat_codes.append(9)
        c.join_queue.append(_join([("t", 0), ("t", 1)], generation=2))
        g.poll()
        self.assertEqual(g.positions[("t", 0)], 0)
        self.assertEqual(g.positions[("t", 1)], 7)
        # First join fetched t; rejoin fetches t again for the added partition.
        self.assertEqual(c.offset_fetches, [("g", "t"), ("g", "t")])


if __name__ == "__main__":
    unittest.main()
