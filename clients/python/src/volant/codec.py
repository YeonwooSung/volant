"""Little-endian native payload encode/decode.

Matches `crates/volant-protocol/src/payload.rs` for the MVP opcodes:
Produce, Fetch, CreateTopic, Metadata, DeleteTopic, OffsetCommit,
OffsetFetch, JoinGroup, Heartbeat, LeaveGroup, Auth, ScramFirst,
ScramFinal.

Header fields are big-endian (see :mod:`volant.frame`); **payload** integers
and length prefixes are little-endian.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Optional

from .frame import ProtocolError

OP_PRODUCE = 1
OP_FETCH = 2
OP_CREATE_TOPIC = 3
OP_METADATA = 4
OP_DELETE_TOPIC = 5
OP_OFFSET_COMMIT = 6
OP_OFFSET_FETCH = 7
OP_JOIN_GROUP = 8
OP_HEARTBEAT = 9
OP_LEAVE_GROUP = 10
OP_AUTH = 30
OP_AUTH_RESPONSE = 31
OP_SCRAM_FIRST = 60
OP_SCRAM_FIRST_RESPONSE = 61
OP_SCRAM_FINAL = 62
OP_SCRAM_FINAL_RESPONSE = 63
OP_ERROR = 0xFFFF

_NULL_LEN = 0xFFFFFFFF


class BrokerError(Exception):
    """Non-zero broker `error_code` or Error opcode."""

    def __init__(self, code: int, message: str = "", op: str = ""):
        self.code = code
        self.message = message
        self.op = op
        prefix = f"{op}: " if op else ""
        detail = message or f"error_code={code}"
        super().__init__(f"{prefix}{detail} (code={code})")


# --- wire helpers ----------------------------------------------------------


class _Writer:
    def __init__(self) -> None:
        self.buf = bytearray()

    def u8(self, v: int) -> None:
        self.buf.append(v & 0xFF)

    def u16_le(self, v: int) -> None:
        self.buf.extend(struct.pack("<H", v & 0xFFFF))

    def u32_le(self, v: int) -> None:
        self.buf.extend(struct.pack("<I", v & 0xFFFFFFFF))

    def i32_le(self, v: int) -> None:
        self.buf.extend(struct.pack("<i", v))

    def u64_le(self, v: int) -> None:
        self.buf.extend(struct.pack("<Q", v & 0xFFFFFFFFFFFFFFFF))

    def i64_le(self, v: int) -> None:
        self.buf.extend(struct.pack("<q", v))

    def raw(self, b: bytes) -> None:
        self.buf.extend(b)

    def finish(self) -> bytes:
        return bytes(self.buf)


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.i = 0

    def remaining(self) -> int:
        return len(self.data) - self.i

    def _need(self, n: int, msg: str) -> None:
        if self.remaining() < n:
            raise ProtocolError(msg)

    def u8(self) -> int:
        self._need(1, "truncated u8")
        v = self.data[self.i]
        self.i += 1
        return v

    def u16_le(self) -> int:
        self._need(2, "truncated u16")
        (v,) = struct.unpack_from("<H", self.data, self.i)
        self.i += 2
        return v

    def u32_le(self) -> int:
        self._need(4, "truncated u32")
        (v,) = struct.unpack_from("<I", self.data, self.i)
        self.i += 4
        return v

    def i32_le(self) -> int:
        self._need(4, "truncated i32")
        (v,) = struct.unpack_from("<i", self.data, self.i)
        self.i += 4
        return v

    def u64_le(self) -> int:
        self._need(8, "truncated u64")
        (v,) = struct.unpack_from("<Q", self.data, self.i)
        self.i += 8
        return v

    def i64_le(self) -> int:
        self._need(8, "truncated i64")
        (v,) = struct.unpack_from("<q", self.data, self.i)
        self.i += 8
        return v

    def take(self, n: int, msg: str = "truncated bytes") -> bytes:
        self._need(n, msg)
        out = self.data[self.i : self.i + n]
        self.i += n
        return out


def _put_string(w: _Writer, s: str) -> None:
    raw = s.encode("utf-8")
    if len(raw) > 0xFFFF:
        raise ProtocolError(f"string too long: {len(raw)} bytes")
    w.u16_le(len(raw))
    w.raw(raw)


def _get_string(r: _Reader) -> str:
    n = r.u16_le()
    raw = r.take(n, "truncated string body")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError(f"invalid utf-8: {e}") from e


def _put_bytes(w: _Writer, b: bytes) -> None:
    w.u32_le(len(b))
    w.raw(b)


def _get_bytes(r: _Reader) -> bytes:
    n = r.u32_le()
    if n == _NULL_LEN:
        raise ProtocolError("unexpected optional null in required bytes")
    return r.take(n, "truncated bytes body")


def _put_optional_bytes(w: _Writer, b: Optional[bytes]) -> None:
    if b is None:
        w.u32_le(_NULL_LEN)
    else:
        _put_bytes(w, b)


def _get_optional_bytes(r: _Reader) -> Optional[bytes]:
    n = r.u32_le()
    if n == _NULL_LEN:
        return None
    return r.take(n, "truncated optional bytes body")


def _put_headers(w: _Writer, headers: list[tuple[str, bytes]]) -> None:
    w.u32_le(len(headers))
    for name, value in headers:
        _put_string(w, name)
        _put_bytes(w, value)


def _get_headers(r: _Reader) -> list[tuple[str, bytes]]:
    count = r.u32_le()
    out: list[tuple[str, bytes]] = []
    for _ in range(count):
        name = _get_string(r)
        value = _get_bytes(r)
        out.append((name, value))
    return out


# --- request / response types ----------------------------------------------


@dataclass
class ProduceMessage:
    key: Optional[bytes]
    value: bytes
    timestamp_ms: int = -1
    headers: list[tuple[str, bytes]] = field(default_factory=list)


@dataclass
class ProduceRequest:
    topic: str
    partition: int
    acks: int
    messages: list[ProduceMessage]
    producer_id: int = 0
    producer_epoch: int = 0
    base_sequence: int = -1


@dataclass
class ProduceResponse:
    topic: str
    partition: int
    base_offset: int
    count: int
    error_code: int


@dataclass
class FetchRequest:
    topic: str
    partition: int
    from_offset: int
    max_messages: int
    max_bytes: int
    max_wait_ms: int


@dataclass
class FetchRecord:
    offset: int
    timestamp_ms: int
    key: Optional[bytes]
    value: bytes
    headers: list[tuple[str, bytes]] = field(default_factory=list)


@dataclass
class FetchResponse:
    topic: str
    partition: int
    high_watermark: int
    error_code: int
    records: list[FetchRecord]


@dataclass
class CreateTopicRequest:
    name: str
    partitions: int
    configs: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class CreateTopicResponse:
    topic_id: int
    name: str
    partitions: int
    error_code: int


@dataclass
class DeleteTopicRequest:
    name: str


@dataclass
class DeleteTopicResponse:
    name: str
    error_code: int


@dataclass
class MetadataRequest:
    topics: list[str] = field(default_factory=list)


@dataclass
class BrokerInfo:
    node_id: int
    host: str
    port: int


@dataclass
class PartitionInfo:
    partition_id: int
    leader: int
    hwm: int
    replicas: list[int]
    isr: list[int]
    leader_epoch: int


@dataclass
class TopicInfo:
    name: str
    topic_id: int
    error_code: int
    partitions: list[PartitionInfo]


@dataclass
class MetadataResponse:
    brokers: list[BrokerInfo]
    topics: list[TopicInfo]


@dataclass
class ErrorResponse:
    code: int
    message: str


@dataclass
class OffsetCommitEntry:
    topic: str
    partition: int
    offset: int
    metadata: str = ""


@dataclass
class OffsetCommitRequest:
    group_id: str
    member_id: str
    generation: int
    entries: list[OffsetCommitEntry]


@dataclass
class OffsetCommitResponse:
    error_code: int


@dataclass
class OffsetEntry:
    topic: str
    partition: int


@dataclass
class OffsetFetchRequest:
    group_id: str
    entries: list[OffsetEntry] = field(default_factory=list)


@dataclass
class OffsetFetchEntry:
    topic: str
    partition: int
    offset: int
    metadata: str = ""


@dataclass
class OffsetFetchResponse:
    error_code: int
    entries: list[OffsetFetchEntry]


@dataclass
class Assignment:
    topic: str
    partition: int


@dataclass
class JoinGroupRequest:
    group_id: str
    member_id: str
    session_timeout_ms: int
    topics: list[str] = field(default_factory=list)
    group_instance_id: str = ""


@dataclass
class JoinGroupResponse:
    error_code: int
    generation: int
    member_id: str
    assignment: list[Assignment] = field(default_factory=list)
    revoked: list[Assignment] = field(default_factory=list)


@dataclass
class HeartbeatRequest:
    group_id: str
    member_id: str
    generation: int


@dataclass
class HeartbeatResponse:
    error_code: int


@dataclass
class LeaveGroupRequest:
    group_id: str
    member_id: str


@dataclass
class LeaveGroupResponse:
    error_code: int


@dataclass
class AuthRequest:
    token: str


@dataclass
class AuthResponse:
    error_code: int


@dataclass
class ScramFirstRequest:
    username: str
    client_nonce: str


@dataclass
class ScramFirstResponse:
    error_code: int
    combined_nonce: str
    salt: bytes
    iterations: int


@dataclass
class ScramFinalRequest:
    username: str
    combined_nonce: str
    client_proof: bytes


@dataclass
class ScramFinalResponse:
    error_code: int
    server_signature: bytes


# --- produce ---------------------------------------------------------------


def encode_produce_request(req: ProduceRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.topic)
    w.i32_le(req.partition)
    w.u8(req.acks)
    w.u32_le(len(req.messages))
    for m in req.messages:
        _put_optional_bytes(w, m.key)
        _put_bytes(w, m.value)
        w.i64_le(m.timestamp_ms)
        _put_headers(w, m.headers)
    # Phase 10 idempotent trailer (always written by current encoders).
    w.u64_le(req.producer_id)
    w.u16_le(req.producer_epoch)
    w.i32_le(req.base_sequence)
    return w.finish()


def decode_produce_request(payload: bytes) -> ProduceRequest:
    r = _Reader(payload)
    topic = _get_string(r)
    partition = r.i32_le()
    acks = r.u8()
    n = r.u32_le()
    messages: list[ProduceMessage] = []
    for _ in range(n):
        key = _get_optional_bytes(r)
        value = _get_bytes(r)
        ts = r.i64_le()
        headers = _get_headers(r)
        messages.append(
            ProduceMessage(key=key, value=value, timestamp_ms=ts, headers=headers)
        )
    if r.remaining() >= 8 + 2 + 4:
        producer_id = r.u64_le()
        producer_epoch = r.u16_le()
        base_sequence = r.i32_le()
    else:
        producer_id, producer_epoch, base_sequence = 0, 0, -1
    return ProduceRequest(
        topic=topic,
        partition=partition,
        acks=acks,
        messages=messages,
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        base_sequence=base_sequence,
    )


def encode_produce_response(resp: ProduceResponse) -> bytes:
    w = _Writer()
    _put_string(w, resp.topic)
    w.u32_le(resp.partition)
    w.u64_le(resp.base_offset)
    w.u32_le(resp.count)
    w.u16_le(resp.error_code)
    return w.finish()


def decode_produce_response(payload: bytes) -> ProduceResponse:
    r = _Reader(payload)
    topic = _get_string(r)
    return ProduceResponse(
        topic=topic,
        partition=r.u32_le(),
        base_offset=r.u64_le(),
        count=r.u32_le(),
        error_code=r.u16_le(),
    )


# --- fetch -----------------------------------------------------------------


def encode_fetch_request(req: FetchRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.topic)
    w.u32_le(req.partition)
    w.u64_le(req.from_offset)
    w.u32_le(req.max_messages)
    w.u32_le(req.max_bytes)
    w.u32_le(req.max_wait_ms)
    return w.finish()


def decode_fetch_request(payload: bytes) -> FetchRequest:
    r = _Reader(payload)
    return FetchRequest(
        topic=_get_string(r),
        partition=r.u32_le(),
        from_offset=r.u64_le(),
        max_messages=r.u32_le(),
        max_bytes=r.u32_le(),
        max_wait_ms=r.u32_le(),
    )


def encode_fetch_response(resp: FetchResponse) -> bytes:
    w = _Writer()
    _put_string(w, resp.topic)
    w.u32_le(resp.partition)
    w.u64_le(resp.high_watermark)
    w.u16_le(resp.error_code)
    w.u32_le(len(resp.records))
    for rec in resp.records:
        w.u64_le(rec.offset)
        w.i64_le(rec.timestamp_ms)
        _put_optional_bytes(w, rec.key)
        _put_bytes(w, rec.value)
        _put_headers(w, rec.headers)
    return w.finish()


def decode_fetch_response(payload: bytes) -> FetchResponse:
    r = _Reader(payload)
    topic = _get_string(r)
    partition = r.u32_le()
    hwm = r.u64_le()
    error_code = r.u16_le()
    n = r.u32_le()
    records: list[FetchRecord] = []
    for _ in range(n):
        records.append(
            FetchRecord(
                offset=r.u64_le(),
                timestamp_ms=r.i64_le(),
                key=_get_optional_bytes(r),
                value=_get_bytes(r),
                headers=_get_headers(r),
            )
        )
    return FetchResponse(
        topic=topic,
        partition=partition,
        high_watermark=hwm,
        error_code=error_code,
        records=records,
    )


# --- create / delete topic -------------------------------------------------


def encode_create_topic_request(req: CreateTopicRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.name)
    w.u32_le(req.partitions)
    # Phase 13 config trailer (always written by current encoders).
    w.u32_le(len(req.configs))
    for k, v in req.configs:
        _put_string(w, k)
        _put_string(w, v)
    return w.finish()


def decode_create_topic_request(payload: bytes) -> CreateTopicRequest:
    r = _Reader(payload)
    name = _get_string(r)
    partitions = r.u32_le()
    configs: list[tuple[str, str]] = []
    if r.remaining() >= 4:
        n = r.u32_le()
        for _ in range(n):
            configs.append((_get_string(r), _get_string(r)))
    return CreateTopicRequest(name=name, partitions=partitions, configs=configs)


def encode_create_topic_response(resp: CreateTopicResponse) -> bytes:
    w = _Writer()
    w.u32_le(resp.topic_id)
    _put_string(w, resp.name)
    w.u32_le(resp.partitions)
    w.u16_le(resp.error_code)
    return w.finish()


def decode_create_topic_response(payload: bytes) -> CreateTopicResponse:
    r = _Reader(payload)
    topic_id = r.u32_le()
    name = _get_string(r)
    return CreateTopicResponse(
        topic_id=topic_id,
        name=name,
        partitions=r.u32_le(),
        error_code=r.u16_le(),
    )


def encode_delete_topic_request(req: DeleteTopicRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.name)
    return w.finish()


def decode_delete_topic_request(payload: bytes) -> DeleteTopicRequest:
    return DeleteTopicRequest(name=_get_string(_Reader(payload)))


def encode_delete_topic_response(resp: DeleteTopicResponse) -> bytes:
    w = _Writer()
    _put_string(w, resp.name)
    w.u16_le(resp.error_code)
    return w.finish()


def decode_delete_topic_response(payload: bytes) -> DeleteTopicResponse:
    r = _Reader(payload)
    return DeleteTopicResponse(name=_get_string(r), error_code=r.u16_le())


# --- metadata --------------------------------------------------------------


def encode_metadata_request(req: MetadataRequest) -> bytes:
    w = _Writer()
    w.u32_le(len(req.topics))
    for t in req.topics:
        _put_string(w, t)
    return w.finish()


def decode_metadata_request(payload: bytes) -> MetadataRequest:
    r = _Reader(payload)
    n = r.u32_le()
    return MetadataRequest(topics=[_get_string(r) for _ in range(n)])


def encode_metadata_response(resp: MetadataResponse) -> bytes:
    w = _Writer()
    w.u32_le(len(resp.brokers))
    for b in resp.brokers:
        w.u32_le(b.node_id)
        _put_string(w, b.host)
        w.u16_le(b.port)
    w.u32_le(len(resp.topics))
    for t in resp.topics:
        _put_string(w, t.name)
        w.u32_le(t.topic_id)
        w.u16_le(t.error_code)
        w.u32_le(len(t.partitions))
        for p in t.partitions:
            w.u32_le(p.partition_id)
            w.u32_le(p.leader)
            w.u64_le(p.hwm)
            w.u32_le(len(p.replicas))
            for replica in p.replicas:
                w.u32_le(replica)
            w.u32_le(len(p.isr))
            for replica in p.isr:
                w.u32_le(replica)
            w.u32_le(p.leader_epoch)
    return w.finish()


def decode_metadata_response(payload: bytes) -> MetadataResponse:
    r = _Reader(payload)
    n_brokers = r.u32_le()
    brokers: list[BrokerInfo] = []
    for _ in range(n_brokers):
        node_id = r.u32_le()
        host = _get_string(r)
        port = r.u16_le()
        brokers.append(BrokerInfo(node_id=node_id, host=host, port=port))
    n_topics = r.u32_le()
    topics: list[TopicInfo] = []
    for _ in range(n_topics):
        name = _get_string(r)
        topic_id = r.u32_le()
        error_code = r.u16_le()
        n_parts = r.u32_le()
        parts: list[PartitionInfo] = []
        for _ in range(n_parts):
            partition_id = r.u32_le()
            leader = r.u32_le()
            hwm = r.u64_le()
            n_rep = r.u32_le()
            replicas = [r.u32_le() for _ in range(n_rep)]
            n_isr = r.u32_le()
            isr = [r.u32_le() for _ in range(n_isr)]
            leader_epoch = r.u32_le()
            parts.append(
                PartitionInfo(
                    partition_id=partition_id,
                    leader=leader,
                    hwm=hwm,
                    replicas=replicas,
                    isr=isr,
                    leader_epoch=leader_epoch,
                )
            )
        topics.append(
            TopicInfo(
                name=name,
                topic_id=topic_id,
                error_code=error_code,
                partitions=parts,
            )
        )
    return MetadataResponse(brokers=brokers, topics=topics)


# --- offset commit / fetch -------------------------------------------------


def encode_offset_commit_request(req: OffsetCommitRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.group_id)
    _put_string(w, req.member_id)
    w.u32_le(req.generation)
    w.u32_le(len(req.entries))
    for e in req.entries:
        _put_string(w, e.topic)
        w.u32_le(e.partition)
        w.u64_le(e.offset)
        _put_string(w, e.metadata)
    return w.finish()


def decode_offset_commit_request(payload: bytes) -> OffsetCommitRequest:
    r = _Reader(payload)
    group_id = _get_string(r)
    member_id = _get_string(r)
    generation = r.u32_le()
    n = r.u32_le()
    entries: list[OffsetCommitEntry] = []
    for _ in range(n):
        topic = _get_string(r)
        partition = r.u32_le()
        offset = r.u64_le()
        metadata = _get_string(r)
        entries.append(
            OffsetCommitEntry(
                topic=topic, partition=partition, offset=offset, metadata=metadata
            )
        )
    return OffsetCommitRequest(
        group_id=group_id,
        member_id=member_id,
        generation=generation,
        entries=entries,
    )


def encode_offset_commit_response(resp: OffsetCommitResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    return w.finish()


def decode_offset_commit_response(payload: bytes) -> OffsetCommitResponse:
    r = _Reader(payload)
    return OffsetCommitResponse(error_code=r.u16_le())


def encode_offset_fetch_request(req: OffsetFetchRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.group_id)
    w.u32_le(len(req.entries))
    for e in req.entries:
        _put_string(w, e.topic)
        w.u32_le(e.partition)
    return w.finish()


def decode_offset_fetch_request(payload: bytes) -> OffsetFetchRequest:
    r = _Reader(payload)
    group_id = _get_string(r)
    n = r.u32_le()
    entries: list[OffsetEntry] = []
    for _ in range(n):
        topic = _get_string(r)
        partition = r.u32_le()
        entries.append(OffsetEntry(topic=topic, partition=partition))
    return OffsetFetchRequest(group_id=group_id, entries=entries)


def encode_offset_fetch_response(resp: OffsetFetchResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    w.u32_le(len(resp.entries))
    for e in resp.entries:
        _put_string(w, e.topic)
        w.u32_le(e.partition)
        w.u64_le(e.offset)
        _put_string(w, e.metadata)
    return w.finish()


def decode_offset_fetch_response(payload: bytes) -> OffsetFetchResponse:
    r = _Reader(payload)
    error_code = r.u16_le()
    n = r.u32_le()
    entries: list[OffsetFetchEntry] = []
    for _ in range(n):
        topic = _get_string(r)
        partition = r.u32_le()
        offset = r.u64_le()
        metadata = _get_string(r)
        entries.append(
            OffsetFetchEntry(
                topic=topic, partition=partition, offset=offset, metadata=metadata
            )
        )
    return OffsetFetchResponse(error_code=error_code, entries=entries)


# --- join / heartbeat / leave ----------------------------------------------


def _put_assignments(w: _Writer, items: list[Assignment]) -> None:
    w.u32_le(len(items))
    for a in items:
        _put_string(w, a.topic)
        w.u32_le(a.partition)


def _get_assignments(r: _Reader) -> list[Assignment]:
    n = r.u32_le()
    out: list[Assignment] = []
    for _ in range(n):
        topic = _get_string(r)
        partition = r.u32_le()
        out.append(Assignment(topic=topic, partition=partition))
    return out


def encode_join_group_request(req: JoinGroupRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.group_id)
    _put_string(w, req.member_id)
    w.u32_le(req.session_timeout_ms)
    w.u32_le(len(req.topics))
    for t in req.topics:
        _put_string(w, t)
    # Phase 12 trailing field (always written by current encoders).
    _put_string(w, req.group_instance_id)
    return w.finish()


def decode_join_group_request(payload: bytes) -> JoinGroupRequest:
    r = _Reader(payload)
    group_id = _get_string(r)
    member_id = _get_string(r)
    session_timeout_ms = r.u32_le()
    n = r.u32_le()
    topics = [_get_string(r) for _ in range(n)]
    # Phase 12 trailing field; legacy payloads omit it.
    group_instance_id = _get_string(r) if r.remaining() > 0 else ""
    return JoinGroupRequest(
        group_id=group_id,
        member_id=member_id,
        session_timeout_ms=session_timeout_ms,
        topics=topics,
        group_instance_id=group_instance_id,
    )


def encode_join_group_response(resp: JoinGroupResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    w.u32_le(resp.generation)
    _put_string(w, resp.member_id)
    _put_assignments(w, resp.assignment)
    # Phase 17 trailing revoked list (always written by current encoders).
    _put_assignments(w, resp.revoked)
    return w.finish()


def decode_join_group_response(payload: bytes) -> JoinGroupResponse:
    r = _Reader(payload)
    error_code = r.u16_le()
    generation = r.u32_le()
    member_id = _get_string(r)
    assignment = _get_assignments(r)
    # Phase 17 trailing revoked list; legacy payloads omit it.
    revoked = _get_assignments(r) if r.remaining() >= 4 else []
    return JoinGroupResponse(
        error_code=error_code,
        generation=generation,
        member_id=member_id,
        assignment=assignment,
        revoked=revoked,
    )


def encode_heartbeat_request(req: HeartbeatRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.group_id)
    _put_string(w, req.member_id)
    w.u32_le(req.generation)
    return w.finish()


def decode_heartbeat_request(payload: bytes) -> HeartbeatRequest:
    r = _Reader(payload)
    return HeartbeatRequest(
        group_id=_get_string(r),
        member_id=_get_string(r),
        generation=r.u32_le(),
    )


def encode_heartbeat_response(resp: HeartbeatResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    return w.finish()


def decode_heartbeat_response(payload: bytes) -> HeartbeatResponse:
    r = _Reader(payload)
    return HeartbeatResponse(error_code=r.u16_le())


def encode_leave_group_request(req: LeaveGroupRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.group_id)
    _put_string(w, req.member_id)
    return w.finish()


def decode_leave_group_request(payload: bytes) -> LeaveGroupRequest:
    r = _Reader(payload)
    return LeaveGroupRequest(group_id=_get_string(r), member_id=_get_string(r))


def encode_leave_group_response(resp: LeaveGroupResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    return w.finish()


def decode_leave_group_response(payload: bytes) -> LeaveGroupResponse:
    r = _Reader(payload)
    return LeaveGroupResponse(error_code=r.u16_le())


# --- auth ------------------------------------------------------------------


def encode_auth_request(req: AuthRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.token)
    return w.finish()


def decode_auth_request(payload: bytes) -> AuthRequest:
    return AuthRequest(token=_get_string(_Reader(payload)))


def encode_auth_response(resp: AuthResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    return w.finish()


def decode_auth_response(payload: bytes) -> AuthResponse:
    r = _Reader(payload)
    return AuthResponse(error_code=r.u16_le())


# --- scram -----------------------------------------------------------------


def encode_scram_first_request(req: ScramFirstRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.username)
    _put_string(w, req.client_nonce)
    return w.finish()


def decode_scram_first_request(payload: bytes) -> ScramFirstRequest:
    r = _Reader(payload)
    return ScramFirstRequest(username=_get_string(r), client_nonce=_get_string(r))


def encode_scram_first_response(resp: ScramFirstResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    _put_string(w, resp.combined_nonce)
    _put_bytes(w, resp.salt)
    w.u32_le(resp.iterations)
    return w.finish()


def decode_scram_first_response(payload: bytes) -> ScramFirstResponse:
    r = _Reader(payload)
    return ScramFirstResponse(
        error_code=r.u16_le(),
        combined_nonce=_get_string(r),
        salt=_get_bytes(r),
        iterations=r.u32_le(),
    )


def encode_scram_final_request(req: ScramFinalRequest) -> bytes:
    w = _Writer()
    _put_string(w, req.username)
    _put_string(w, req.combined_nonce)
    _put_bytes(w, req.client_proof)
    return w.finish()


def decode_scram_final_request(payload: bytes) -> ScramFinalRequest:
    r = _Reader(payload)
    return ScramFinalRequest(
        username=_get_string(r),
        combined_nonce=_get_string(r),
        client_proof=_get_bytes(r),
    )


def encode_scram_final_response(resp: ScramFinalResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.error_code)
    _put_bytes(w, resp.server_signature)
    return w.finish()


def decode_scram_final_response(payload: bytes) -> ScramFinalResponse:
    r = _Reader(payload)
    return ScramFinalResponse(error_code=r.u16_le(), server_signature=_get_bytes(r))


# --- error opcode ----------------------------------------------------------


def encode_error_response(resp: ErrorResponse) -> bytes:
    w = _Writer()
    w.u16_le(resp.code)
    _put_string(w, resp.message)
    return w.finish()


def decode_error_response(payload: bytes) -> ErrorResponse:
    r = _Reader(payload)
    return ErrorResponse(code=r.u16_le(), message=_get_string(r))


def decode_response(opcode: int, payload: bytes):
    """Dispatch a response payload by opcode."""
    if opcode == OP_PRODUCE:
        return decode_produce_response(payload)
    if opcode == OP_FETCH:
        return decode_fetch_response(payload)
    if opcode == OP_CREATE_TOPIC:
        return decode_create_topic_response(payload)
    if opcode == OP_METADATA:
        return decode_metadata_response(payload)
    if opcode == OP_DELETE_TOPIC:
        return decode_delete_topic_response(payload)
    if opcode == OP_OFFSET_COMMIT:
        return decode_offset_commit_response(payload)
    if opcode == OP_OFFSET_FETCH:
        return decode_offset_fetch_response(payload)
    if opcode == OP_JOIN_GROUP:
        return decode_join_group_response(payload)
    if opcode == OP_HEARTBEAT:
        return decode_heartbeat_response(payload)
    if opcode == OP_LEAVE_GROUP:
        return decode_leave_group_response(payload)
    if opcode == OP_AUTH_RESPONSE:
        return decode_auth_response(payload)
    if opcode == OP_SCRAM_FIRST_RESPONSE:
        return decode_scram_first_response(payload)
    if opcode == OP_SCRAM_FINAL_RESPONSE:
        return decode_scram_final_response(payload)
    if opcode == OP_ERROR:
        return decode_error_response(payload)
    raise ProtocolError(f"unknown response opcode {opcode}")
