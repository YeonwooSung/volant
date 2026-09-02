"""Payload encode/decode fixtures matching crates/volant-protocol/src/payload.rs."""

from __future__ import annotations

import unittest

from volant.codec import (
    Assignment,
    AuthRequest,
    AuthResponse,
    BrokerInfo,
    CreateTopicRequest,
    CreateTopicResponse,
    DeleteTopicRequest,
    DeleteTopicResponse,
    FetchRecord,
    FetchRequest,
    FetchResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    JoinGroupRequest,
    JoinGroupResponse,
    LeaveGroupRequest,
    LeaveGroupResponse,
    MetadataRequest,
    MetadataResponse,
    OffsetCommitEntry,
    OffsetCommitRequest,
    OffsetCommitResponse,
    OffsetEntry,
    OffsetFetchEntry,
    OffsetFetchRequest,
    OffsetFetchResponse,
    PartitionInfo,
    ProduceMessage,
    ProduceRequest,
    ProduceResponse,
    ScramFinalRequest,
    ScramFinalResponse,
    ScramFirstRequest,
    ScramFirstResponse,
    TopicInfo,
    decode_auth_request,
    decode_auth_response,
    decode_scram_final_request,
    decode_scram_final_response,
    decode_scram_first_request,
    decode_scram_first_response,
    decode_create_topic_request,
    decode_create_topic_response,
    decode_delete_topic_request,
    decode_delete_topic_response,
    decode_fetch_request,
    decode_fetch_response,
    decode_heartbeat_request,
    decode_heartbeat_response,
    decode_join_group_request,
    decode_join_group_response,
    decode_leave_group_request,
    decode_leave_group_response,
    decode_metadata_request,
    decode_metadata_response,
    decode_offset_commit_request,
    decode_offset_commit_response,
    decode_offset_fetch_request,
    decode_offset_fetch_response,
    decode_produce_request,
    decode_produce_response,
    decode_response,
    encode_auth_request,
    encode_auth_response,
    encode_scram_final_request,
    encode_scram_final_response,
    encode_scram_first_request,
    encode_scram_first_response,
    encode_create_topic_request,
    encode_create_topic_response,
    encode_delete_topic_request,
    encode_delete_topic_response,
    encode_fetch_request,
    encode_fetch_response,
    encode_heartbeat_request,
    encode_heartbeat_response,
    encode_join_group_request,
    encode_join_group_response,
    encode_leave_group_request,
    encode_leave_group_response,
    encode_metadata_request,
    encode_metadata_response,
    encode_offset_commit_request,
    encode_offset_commit_response,
    encode_offset_fetch_request,
    encode_offset_fetch_response,
    encode_produce_request,
    encode_produce_response,
    OP_AUTH_RESPONSE,
    OP_SCRAM_FINAL_RESPONSE,
    OP_SCRAM_FIRST_RESPONSE,
    OP_HEARTBEAT,
    OP_JOIN_GROUP,
    OP_LEAVE_GROUP,
    OP_OFFSET_COMMIT,
    OP_OFFSET_FETCH,
)


def _hx(data: bytes) -> str:
    return data.hex()


class TestProduceCodec(unittest.TestCase):
    def test_value_only_exact_bytes(self) -> None:
        # topic "t", partition 0, acks 1, one message: null key, value b"v",
        # timestamp -1, no headers, Phase 10 trailer (0, 0, -1).
        req = ProduceRequest(
            topic="t",
            partition=0,
            acks=1,
            messages=[ProduceMessage(key=None, value=b"v", timestamp_ms=-1)],
            producer_id=0,
            producer_epoch=0,
            base_sequence=-1,
        )
        raw = encode_produce_request(req)
        expected = bytes.fromhex(
            "0100"  # string len 1
            "74"  # 't'
            "00000000"  # partition 0 i32
            "01"  # acks
            "01000000"  # 1 message
            "ffffffff"  # null key
            "01000000"  # value len 1
            "76"  # 'v'
            "ffffffffffffffff"  # timestamp -1
            "00000000"  # 0 headers
            "0000000000000000"  # producer_id
            "0000"  # epoch
            "ffffffff"  # base_sequence -1
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_produce_request(raw), req)

    def test_keyed_with_headers_matches_rust_roundtrip_shape(self) -> None:
        # Same fields as payload.rs produce_roundtrip().
        req = ProduceRequest(
            topic="events",
            partition=-1,
            acks=1,
            messages=[
                ProduceMessage(
                    key=b"k",
                    value=b"v",
                    timestamp_ms=-1,
                    headers=[("h", b"hv")],
                )
            ],
            producer_id=0,
            producer_epoch=0,
            base_sequence=-1,
        )
        raw = encode_produce_request(req)
        expected = bytes.fromhex(
            "0600"  # "events" len
            "6576656e7473"
            "ffffffff"  # partition -1
            "01"
            "01000000"
            "01000000"  # key Some(b"k")
            "6b"
            "01000000"
            "76"
            "ffffffffffffffff"
            "01000000"  # 1 header
            "0100"  # "h"
            "68"
            "02000000"  # b"hv"
            "6876"
            "0000000000000000"
            "0000"
            "ffffffff"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_produce_request(raw), req)

    def test_legacy_produce_without_trailer(self) -> None:
        # payload.rs produce_legacy_without_trailer_decodes
        raw = bytes.fromhex(
            "010074"  # "t"
            "00000000"  # partition 0
            "01"
            "01000000"
            "ffffffff"
            "0100000076"
            "ffffffffffffffff"
            "00000000"
        )
        decoded = decode_produce_request(raw)
        self.assertEqual(decoded.producer_id, 0)
        self.assertEqual(decoded.producer_epoch, 0)
        self.assertEqual(decoded.base_sequence, -1)
        self.assertEqual(decoded.messages[0].value, b"v")
        self.assertIsNone(decoded.messages[0].key)

    def test_produce_response_roundtrip(self) -> None:
        resp = ProduceResponse(
            topic="t", partition=0, base_offset=0, count=1, error_code=0
        )
        raw = encode_produce_response(resp)
        expected = bytes.fromhex("010074" "00000000" "0000000000000000" "01000000" "0000")
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_produce_response(raw), resp)


class TestFetchCodec(unittest.TestCase):
    def test_fetch_request_exact_bytes(self) -> None:
        req = FetchRequest(
            topic="t",
            partition=0,
            from_offset=0,
            max_messages=10,
            max_bytes=4096,
            max_wait_ms=0,
        )
        raw = encode_fetch_request(req)
        expected = bytes.fromhex(
            "010074"
            "00000000"
            "0000000000000000"
            "0a000000"
            "00100000"
            "00000000"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_fetch_request(raw), req)

    def test_fetch_response_null_key(self) -> None:
        resp = FetchResponse(
            topic="t",
            partition=0,
            high_watermark=1,
            error_code=0,
            records=[
                FetchRecord(
                    offset=0,
                    timestamp_ms=-1,
                    key=None,
                    value=b"hello",
                    headers=[],
                )
            ],
        )
        raw = encode_fetch_response(resp)
        expected = bytes.fromhex(
            "010074"
            "00000000"
            "0100000000000000"
            "0000"
            "01000000"
            "0000000000000000"
            "ffffffffffffffff"
            "ffffffff"
            "05000000"
            "68656c6c6f"
            "00000000"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_fetch_response(raw), resp)


class TestCreateMetadataCodec(unittest.TestCase):
    def test_create_topic_request(self) -> None:
        req = CreateTopicRequest(name="t", partitions=1, configs=[])
        raw = encode_create_topic_request(req)
        expected = bytes.fromhex("010074" "01000000" "00000000")
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_create_topic_request(raw), req)

    def test_create_topic_legacy_without_configs(self) -> None:
        raw = bytes.fromhex("010074" "02000000")
        decoded = decode_create_topic_request(raw)
        self.assertEqual(decoded.name, "t")
        self.assertEqual(decoded.partitions, 2)
        self.assertEqual(decoded.configs, [])

    def test_create_topic_response(self) -> None:
        resp = CreateTopicResponse(topic_id=1, name="t", partitions=1, error_code=0)
        raw = encode_create_topic_response(resp)
        expected = bytes.fromhex("01000000" "010074" "01000000" "0000")
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_create_topic_response(raw), resp)

    def test_delete_topic_roundtrip(self) -> None:
        req = DeleteTopicRequest(name="t")
        raw = encode_delete_topic_request(req)
        self.assertEqual(raw, bytes.fromhex("010074"))
        self.assertEqual(decode_delete_topic_request(raw), req)
        resp = DeleteTopicResponse(name="t", error_code=0)
        rraw = encode_delete_topic_response(resp)
        self.assertEqual(rraw, bytes.fromhex("0100740000"))
        self.assertEqual(decode_delete_topic_response(rraw), resp)

    def test_metadata_request_all_topics(self) -> None:
        req = MetadataRequest(topics=[])
        raw = encode_metadata_request(req)
        self.assertEqual(raw, bytes.fromhex("00000000"))
        self.assertEqual(decode_metadata_request(raw), req)

    def test_metadata_response_one_broker_one_partition(self) -> None:
        resp = MetadataResponse(
            brokers=[BrokerInfo(node_id=1, host="127.0.0.1", port=9092)],
            topics=[
                TopicInfo(
                    name="t",
                    topic_id=1,
                    error_code=0,
                    partitions=[
                        PartitionInfo(
                            partition_id=0,
                            leader=1,
                            hwm=0,
                            replicas=[1],
                            isr=[1],
                            leader_epoch=0,
                        )
                    ],
                )
            ],
        )
        raw = encode_metadata_response(resp)
        expected = bytes.fromhex(
            "01000000"  # 1 broker
            "01000000"  # node 1
            "0900"  # host len 9
            "3132372e302e302e31"  # 127.0.0.1
            "8423"  # port 9092 le
            "01000000"  # 1 topic
            "010074"
            "01000000"  # topic_id
            "0000"  # error
            "01000000"  # 1 partition
            "00000000"  # id 0
            "01000000"  # leader 1
            "0000000000000000"  # hwm
            "01000000"  # 1 replica
            "01000000"
            "01000000"  # 1 isr
            "01000000"
            "00000000"  # leader_epoch
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_metadata_response(raw), resp)


class TestOffsetCodec(unittest.TestCase):
    def test_offset_commit_request_payload_rs_fixture(self) -> None:
        # crates/volant-protocol/src/payload.rs group_request_roundtrips
        req = OffsetCommitRequest(
            group_id="g1",
            member_id="m1",
            generation=2,
            entries=[
                OffsetCommitEntry(
                    topic="events", partition=1, offset=42, metadata="cli"
                )
            ],
        )
        raw = encode_offset_commit_request(req)
        expected = bytes.fromhex(
            "0200"
            "6731"  # "g1"
            "0200"
            "6d31"  # "m1"
            "02000000"  # generation 2
            "01000000"  # 1 entry
            "0600"
            "6576656e7473"  # "events"
            "01000000"  # partition 1
            "2a00000000000000"  # offset 42
            "0300"
            "636c69"  # "cli"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_offset_commit_request(raw), req)

    def test_offset_commit_request_admin_shape(self) -> None:
        req = OffsetCommitRequest(
            group_id="g",
            member_id="",
            generation=0,
            entries=[OffsetCommitEntry(topic="t", partition=0, offset=5, metadata="")],
        )
        raw = encode_offset_commit_request(req)
        expected = bytes.fromhex(
            "010067"
            "0000"
            "00000000"
            "01000000"
            "010074"
            "00000000"
            "0500000000000000"
            "0000"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_offset_commit_request(raw), req)

    def test_offset_commit_response(self) -> None:
        resp = OffsetCommitResponse(error_code=0)
        raw = encode_offset_commit_response(resp)
        self.assertEqual(raw, bytes.fromhex("0000"))
        self.assertEqual(decode_offset_commit_response(raw), resp)
        self.assertEqual(decode_response(OP_OFFSET_COMMIT, raw), resp)

    def test_offset_fetch_request_payload_rs_fixture(self) -> None:
        req = OffsetFetchRequest(
            group_id="g1",
            entries=[OffsetEntry(topic="events", partition=1)],
        )
        raw = encode_offset_fetch_request(req)
        expected = bytes.fromhex(
            "02006731"
            "01000000"
            "06006576656e7473"
            "01000000"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_offset_fetch_request(raw), req)

    def test_offset_fetch_request_empty_entries(self) -> None:
        req = OffsetFetchRequest(group_id="g1", entries=[])
        raw = encode_offset_fetch_request(req)
        expected = bytes.fromhex("02006731" "00000000")
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_offset_fetch_request(raw), req)

    def test_offset_fetch_response_unknown_offset(self) -> None:
        # payload.rs group_response_roundtrips: offset = u64::MAX
        resp = OffsetFetchResponse(
            error_code=0,
            entries=[
                OffsetFetchEntry(
                    topic="events",
                    partition=0,
                    offset=(1 << 64) - 1,
                    metadata="",
                )
            ],
        )
        raw = encode_offset_fetch_response(resp)
        expected = bytes.fromhex(
            "0000"
            "01000000"
            "06006576656e7473"
            "00000000"
            "ffffffffffffffff"
            "0000"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_offset_fetch_response(raw), resp)
        self.assertEqual(decode_response(OP_OFFSET_FETCH, raw), resp)

    def test_offset_fetch_response_committed(self) -> None:
        resp = OffsetFetchResponse(
            error_code=0,
            entries=[
                OffsetFetchEntry(topic="t", partition=0, offset=5, metadata="")
            ],
        )
        raw = encode_offset_fetch_response(resp)
        expected = bytes.fromhex(
            "0000"
            "01000000"
            "010074"
            "00000000"
            "0500000000000000"
            "0000"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_offset_fetch_response(raw), resp)


class TestGroupCodec(unittest.TestCase):
    def test_join_group_request_payload_rs_fixture(self) -> None:
        # crates/volant-protocol/src/payload.rs group_request_roundtrips
        req = JoinGroupRequest(
            group_id="g1",
            member_id="",
            session_timeout_ms=10_000,
            topics=["events", "logs"],
            group_instance_id="",
        )
        raw = encode_join_group_request(req)
        expected = bytes.fromhex(
            "0200"
            "6731"  # "g1"
            "0000"  # empty member_id
            "10270000"  # session_timeout_ms 10000
            "02000000"  # 2 topics
            "0600"
            "6576656e7473"  # "events"
            "0400"
            "6c6f6773"  # "logs"
            "0000"  # empty group_instance_id
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_join_group_request(raw), req)

    def test_join_group_request_with_instance(self) -> None:
        req = JoinGroupRequest(
            group_id="g1",
            member_id="",
            session_timeout_ms=10_000,
            topics=["events"],
            group_instance_id="pod-1",
        )
        raw = encode_join_group_request(req)
        expected = bytes.fromhex(
            "02006731"
            "0000"
            "10270000"
            "01000000"
            "06006576656e7473"
            "0500706f642d31"  # "pod-1"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_join_group_request(raw), req)

    def test_join_group_request_legacy_without_instance(self) -> None:
        # payload.rs: legacy JoinGroup without instance trailer still decodes
        raw = bytes.fromhex(
            "02006731"  # "g1"
            "02006d31"  # "m1"
            "88130000"  # 5000
            "01000000"
            "010074"  # "t"
        )
        decoded = decode_join_group_request(raw)
        self.assertEqual(decoded.group_id, "g1")
        self.assertEqual(decoded.member_id, "m1")
        self.assertEqual(decoded.session_timeout_ms, 5000)
        self.assertEqual(decoded.topics, ["t"])
        self.assertEqual(decoded.group_instance_id, "")

    def test_join_group_response_payload_rs_fixture(self) -> None:
        # payload.rs group_response_roundtrips
        resp = JoinGroupResponse(
            error_code=0,
            generation=1,
            member_id="uuid-1",
            assignment=[
                Assignment(topic="events", partition=0),
                Assignment(topic="events", partition=1),
            ],
            revoked=[Assignment(topic="events", partition=2)],
        )
        raw = encode_join_group_response(resp)
        expected = bytes.fromhex(
            "0000"  # error_code
            "01000000"  # generation 1
            "0600"
            "757569642d31"  # "uuid-1"
            "02000000"  # 2 assignments
            "06006576656e7473"
            "00000000"
            "06006576656e7473"
            "01000000"
            "01000000"  # 1 revoked
            "06006576656e7473"
            "02000000"
        )
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_join_group_response(raw), resp)
        self.assertEqual(decode_response(OP_JOIN_GROUP, raw), resp)

    def test_join_group_response_legacy_without_revoked(self) -> None:
        raw = bytes.fromhex(
            "0000"
            "01000000"
            "0600757569642d31"
            "01000000"
            "06006576656e7473"
            "00000000"
        )
        decoded = decode_join_group_response(raw)
        self.assertEqual(decoded.error_code, 0)
        self.assertEqual(decoded.generation, 1)
        self.assertEqual(decoded.member_id, "uuid-1")
        self.assertEqual(decoded.assignment, [Assignment(topic="events", partition=0)])
        self.assertEqual(decoded.revoked, [])

    def test_heartbeat_request_payload_rs_fixture(self) -> None:
        req = HeartbeatRequest(group_id="g1", member_id="m1", generation=3)
        raw = encode_heartbeat_request(req)
        expected = bytes.fromhex("02006731" "02006d31" "03000000")
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_heartbeat_request(raw), req)

    def test_heartbeat_response_rebalance(self) -> None:
        resp = HeartbeatResponse(error_code=9)
        raw = encode_heartbeat_response(resp)
        self.assertEqual(raw, bytes.fromhex("0900"))
        self.assertEqual(decode_heartbeat_response(raw), resp)
        self.assertEqual(decode_response(OP_HEARTBEAT, raw), resp)

    def test_leave_group_request_payload_rs_fixture(self) -> None:
        req = LeaveGroupRequest(group_id="g1", member_id="m1")
        raw = encode_leave_group_request(req)
        expected = bytes.fromhex("02006731" "02006d31")
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_leave_group_request(raw), req)

    def test_leave_group_response(self) -> None:
        resp = LeaveGroupResponse(error_code=0)
        raw = encode_leave_group_response(resp)
        self.assertEqual(raw, bytes.fromhex("0000"))
        self.assertEqual(decode_leave_group_response(raw), resp)
        self.assertEqual(decode_response(OP_LEAVE_GROUP, raw), resp)


class TestAuthCodec(unittest.TestCase):
    def test_auth_request_s3cret(self) -> None:
        req = AuthRequest(token="s3cret")
        raw = encode_auth_request(req)
        expected = bytes.fromhex("0600733363726574")
        self.assertEqual(_hx(raw), _hx(expected))
        self.assertEqual(decode_auth_request(raw), req)

    def test_auth_response_ok_and_failed(self) -> None:
        ok = AuthResponse(error_code=0)
        raw = encode_auth_response(ok)
        self.assertEqual(raw, bytes.fromhex("0000"))
        self.assertEqual(decode_auth_response(raw), ok)
        self.assertEqual(decode_response(OP_AUTH_RESPONSE, raw), ok)

        fail = AuthResponse(error_code=17)
        raw = encode_auth_response(fail)
        self.assertEqual(raw, bytes.fromhex("1100"))
        self.assertEqual(decode_auth_response(raw), fail)
        self.assertEqual(decode_response(OP_AUTH_RESPONSE, raw), fail)


class TestScramCodec(unittest.TestCase):
    def test_scram_first_request(self) -> None:
        req = ScramFirstRequest(username="alice", client_nonce="n1")
        raw = encode_scram_first_request(req)
        self.assertEqual(_hx(raw), "0500616c69636502006e31")
        self.assertEqual(decode_scram_first_request(raw), req)

    def test_scram_first_response(self) -> None:
        resp = ScramFirstResponse(
            error_code=0, combined_nonce="n1s1", salt=bytes([1, 2, 3]), iterations=4096
        )
        raw = encode_scram_first_response(resp)
        self.assertEqual(_hx(raw), "000004006e3173310300000001020300100000")
        self.assertEqual(decode_scram_first_response(raw), resp)
        self.assertEqual(decode_response(OP_SCRAM_FIRST_RESPONSE, raw), resp)

    def test_scram_final_request(self) -> None:
        req = ScramFinalRequest(
            username="alice", combined_nonce="n1s1", client_proof=bytes(32)
        )
        raw = encode_scram_final_request(req)
        expected = "0500616c69636504006e31733120000000" + ("00" * 32)
        self.assertEqual(_hx(raw), expected)
        self.assertEqual(decode_scram_final_request(raw), req)

    def test_scram_final_response(self) -> None:
        resp = ScramFinalResponse(error_code=0, server_signature=bytes([9] * 32))
        raw = encode_scram_final_response(resp)
        expected = "000020000000" + ("09" * 32)
        self.assertEqual(_hx(raw), expected)
        self.assertEqual(decode_scram_final_response(raw), resp)
        self.assertEqual(decode_response(OP_SCRAM_FINAL_RESPONSE, raw), resp)


if __name__ == "__main__":
    unittest.main()
