"""Unit tests for range_assign / optional GroupConsumer assignor="range"."""

from __future__ import annotations

import unittest

from volant import GroupConsumer, OffsetListing, range_assign, range_assign_multi
from volant.client import DescribeGroupResult, FetchResult, JoinGroupResult
from volant.codec import (
    Assignment,
    BrokerError,
    FetchRecord,
    GroupMemberInfo,
    MetadataResponse,
    PartitionInfo,
    TopicInfo,
)


class TestRangeAssign(unittest.TestCase):
    def test_uneven_partitions(self) -> None:
        parts = range_assign(5, ["a", "b"])
        self.assertEqual(len(parts[0]) + len(parts[1]), 5)
        self.assertEqual(parts[0], [0, 1, 2])
        self.assertEqual(parts[1], [3, 4])

    def test_even_split(self) -> None:
        self.assertEqual(range_assign(4, ["m0", "m1"]), [[0, 1], [2, 3]])

    def test_single_member_gets_all(self) -> None:
        self.assertEqual(range_assign(3, ["solo"]), [[0, 1, 2]])

    def test_three_members_seven_partitions(self) -> None:
        parts = range_assign(7, ["c", "a", "b"])
        self.assertEqual(parts[1], [0, 1, 2])
        self.assertEqual(parts[2], [3, 4])
        self.assertEqual(parts[0], [5, 6])

    def test_empty_members_or_zero_partitions(self) -> None:
        self.assertEqual(range_assign(5, []), [])
        self.assertEqual(range_assign(0, ["a", "b"]), [[], []])

    def test_multi_topic_disjoint_cover(self) -> None:
        assigns = range_assign_multi(
            ["m1", "m2"],
            [["t"], ["t"]],
            {"t": 4},
        )
        self.assertEqual(assigns[0], [("t", 0), ("t", 1)])
        self.assertEqual(assigns[1], [("t", 2), ("t", 3)])
        all_parts = {p for a in assigns for _, p in a}
        self.assertEqual(all_parts, {0, 1, 2, 3})

    def test_multi_skips_missing_topic(self) -> None:
        assigns = range_assign_multi(
            ["solo"],
            [["missing", "t"]],
            {"t": 2},
        )
        self.assertEqual(assigns[0], [("t", 0), ("t", 1)])

    def test_multi_empty_members(self) -> None:
        self.assertEqual(range_assign_multi([], [], {}), [])

    def test_multi_length_mismatch(self) -> None:
        with self.assertRaises(ValueError):
            range_assign_multi(["a"], [], {})


def _topic(name: str, n: int) -> TopicInfo:
    parts = [
        PartitionInfo(
            partition_id=i, leader=0, hwm=0, replicas=[], isr=[], leader_epoch=0
        )
        for i in range(n)
    ]
    return TopicInfo(name=name, topic_id=1, error_code=0, partitions=parts)


def _rec(offset: int, value: bytes) -> FetchRecord:
    return FetchRecord(offset=offset, timestamp_ms=0, key=None, value=value)


class FakeClient:
    def __init__(self) -> None:
        self.joins: list[dict] = []
        self.heartbeats: list[tuple[str, str, int]] = []
        self.leaves: list[tuple[str, str]] = []
        self.fetches: list[tuple[str, int, int, int]] = []
        self.commits: list[dict] = []
        self.offset_fetches: list[tuple[str, str]] = []
        self.metadatas: int = 0
        self.describes: list[str] = []
        self.describe_result: DescribeGroupResult | None = None
        self.describe_error: BaseException | None = None
        self.join_queue: list[JoinGroupResult] = []
        self.meta = MetadataResponse(brokers=[], topics=[])
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
                "member_id": member_id,
                "session_timeout_ms": session_timeout_ms,
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

    def sync_group(self, group: str, member_id: str, generation: int):
        return []

    def heartbeat(self, group: str, member_id: str, generation: int) -> int:
        self.heartbeats.append((group, member_id, generation))
        return 0

    def leave_group(self, group: str, member_id: str) -> None:
        self.leaves.append((group, member_id))

    def metadata(self, topics=None) -> MetadataResponse:
        del topics
        self.metadatas += 1
        return self.meta

    def describe_group(self, group: str) -> DescribeGroupResult:
        self.describes.append(group)
        if self.describe_error is not None:
            raise self.describe_error
        if self.describe_result is not None:
            return self.describe_result
        return DescribeGroupResult(group_id=group, generation=1, members=[])

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
        recs = [
            r for r in self.log.get((topic, partition), []) if r.offset >= offset
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

    def list_offsets(self, topic: str, partitions=None) -> list[OffsetListing]:
        parts = [int(p) for p in partitions] if partitions else []
        return [OffsetListing(partition=p, earliest=0, latest=0) for p in parts]


class TestGroupConsumerRangeAssignor(unittest.TestCase):
    def test_range_fetches_every_partition_from_metadata(self) -> None:
        c = FakeClient()
        c.join_queue.append(
            JoinGroupResult(
                member_id="m1",
                generation=1,
                assignment=[Assignment(topic="t", partition=0)],
            )
        )
        c.meta = MetadataResponse(brokers=[], topics=[_topic("t", 3)])
        for p in range(3):
            c.log[("t", p)] = [_rec(0, bytes([p]))]
        g = GroupConsumer.join(c, "g", ["t"], assignor="range")
        self.assertEqual(g.assignor, "range")
        self.assertEqual(g.assignment, [("t", 0), ("t", 1), ("t", 2)])
        self.assertEqual(c.metadatas, 1)
        self.assertEqual(c.describes, ["g"])
        recs = g.poll(max_wait_ms=0)
        self.assertEqual([(r.partition, r.value) for r in recs], [
            (0, b"\x00"),
            (1, b"\x01"),
            (2, b"\x02"),
        ])
        fetched = {(t, p) for t, p, _off, _w in c.fetches}
        self.assertEqual(fetched, {("t", 0), ("t", 1), ("t", 2)})
        g.close()
        self.assertEqual(c.leaves, [("g", "m1")])

    def test_broker_default_does_not_call_metadata(self) -> None:
        c = FakeClient()
        c.meta = MetadataResponse(brokers=[], topics=[_topic("t", 3)])
        g = GroupConsumer.join(c, "g", ["t"])
        self.assertEqual(g.assignor, "broker")
        self.assertEqual(g.assignment, [("t", 0)])
        self.assertEqual(c.metadatas, 0)
        self.assertEqual(c.describes, [])
        g.poll(max_wait_ms=0)
        fetched = {(t, p) for t, p, _off, _w in c.fetches}
        self.assertEqual(fetched, {("t", 0)})

    def test_empty_assignor_is_broker(self) -> None:
        c = FakeClient()
        g = GroupConsumer.join(c, "g", ["t"], assignor="")
        self.assertEqual(g.assignor, "broker")
        self.assertEqual(g.assignment, [("t", 0)])
        self.assertEqual(c.metadatas, 0)
        self.assertEqual(c.describes, [])

    def test_range_describe_two_members_splits_half(self) -> None:
        for member, want in (
            ("m-a", [("t", 0), ("t", 1)]),
            ("m-b", [("t", 2), ("t", 3)]),
        ):
            c = FakeClient()
            c.join_queue.append(
                JoinGroupResult(
                    member_id=member,
                    generation=1,
                    assignment=[Assignment(topic="t", partition=0)],
                )
            )
            c.meta = MetadataResponse(brokers=[], topics=[_topic("t", 4)])
            c.describe_result = DescribeGroupResult(
                group_id="g",
                generation=1,
                members=[
                    GroupMemberInfo(member_id="m-a", topics=["t"]),
                    GroupMemberInfo(member_id="m-b", topics=["t"]),
                ],
            )
            g = GroupConsumer.join(c, "g", ["t"], assignor="range")
            self.assertEqual(g.assignment, want)
            self.assertEqual(c.describes, ["g"])
            g.close()

    def test_range_describe_error_falls_back_to_solo(self) -> None:
        c = FakeClient()
        c.join_queue.append(
            JoinGroupResult(
                member_id="m-a",
                generation=1,
                assignment=[Assignment(topic="t", partition=0)],
            )
        )
        c.meta = MetadataResponse(brokers=[], topics=[_topic("t", 4)])
        c.describe_error = BrokerError(2, op="describe_group")
        g = GroupConsumer.join(c, "g", ["t"], assignor="range")
        self.assertEqual(g.assignment, [("t", 0), ("t", 1), ("t", 2), ("t", 3)])
        self.assertEqual(c.describes, ["g"])
        g.close()

    def test_range_describe_omits_self_still_includes(self) -> None:
        c = FakeClient()
        c.join_queue.append(
            JoinGroupResult(
                member_id="m-b",
                generation=1,
                assignment=[Assignment(topic="t", partition=0)],
            )
        )
        c.meta = MetadataResponse(brokers=[], topics=[_topic("t", 4)])
        c.describe_result = DescribeGroupResult(
            group_id="g",
            generation=1,
            members=[GroupMemberInfo(member_id="m-a", topics=["t"])],
        )
        g = GroupConsumer.join(c, "g", ["t"], assignor="range")
        self.assertEqual(g.assignment, [("t", 2), ("t", 3)])
        self.assertEqual(c.describes, ["g"])
        g.close()

    def test_range_join_members_skips_describe(self) -> None:
        for member, want in (
            ("m-a", [("t", 0), ("t", 1)]),
            ("m-b", [("t", 2), ("t", 3)]),
        ):
            c = FakeClient()
            c.join_queue.append(
                JoinGroupResult(
                    member_id=member,
                    generation=1,
                    assignment=[Assignment(topic="t", partition=0)],
                    members=["m-a", "m-b"],
                )
            )
            c.meta = MetadataResponse(brokers=[], topics=[_topic("t", 4)])
            c.describe_error = BrokerError(2, op="describe_group")
            g = GroupConsumer.join(c, "g", ["t"], assignor="range")
            self.assertEqual(g.assignment, want)
            self.assertEqual(c.describes, [])
            g.close()

    def test_unknown_assignor_raises(self) -> None:
        c = FakeClient()
        with self.assertRaises(ValueError) as ctx:
            GroupConsumer.join(c, "g", ["t"], assignor="sticky")
        self.assertIn("unknown assignor", str(ctx.exception))
        self.assertEqual(c.joins, [])


if __name__ == "__main__":
    unittest.main()
