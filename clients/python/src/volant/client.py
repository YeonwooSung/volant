"""Synchronous native-protocol TCP client."""

from __future__ import annotations

import socket
import ssl
from dataclasses import dataclass, field
from typing import Iterable, Optional, Union

from . import codec
from .codec import (
    Assignment,
    BrokerError,
    BrokerInfo,
    CreatePartitionsRequest,
    CreateTopicRequest,
    DeleteOffsetsRequest,
    DeleteOffsetsResponse,
    DescribeConfigsRequest,
    DescribeConfigsResponse,
    AlterConfigsRequest,
    AlterConfigsResponse,
    DeleteRecordsRequest,
    DeleteRecordsResponse,
    DeleteTopicRequest,
    DescribeGroupRequest,
    DescribeGroupResponse,
    FetchRecord,
    FetchRequest,
    FetchResponse,
    GroupListing,
    GroupMemberInfo,
    GroupState,
    HeartbeatRequest,
    HeartbeatResponse,
    JoinGroupRequest,
    JoinGroupResponse,
    LeaveGroupRequest,
    LeaveGroupResponse,
    ListGroupsResponse,
    ListMembersResponse,
    ListOffsetsRequest,
    ListOffsetsResponse,
    MembershipBroker,
    MembershipList,
    MetadataRequest,
    MetadataResponse,
    OffsetCommitEntry,
    OffsetListing,
    OffsetCommitRequest,
    OffsetCommitResponse,
    OffsetFetchRequest,
    OffsetFetchResponse,
    ProduceMessage,
    ProduceRequest,
    ProduceResponse,
    TopicInfo,
)
from .frame import (
    HEADER_LEN,
    MAX_PAYLOAD,
    PROTOCOL_VERSION,
    ProtocolError,
    encode_frame,
    try_decode_frame,
)


# Native ErrorCode::NotLeaderForPartition (crates/volant-protocol).
_NOT_LEADER = 13
# Native ErrorCode::UnknownProducerId — pid not allocated via InitProducerId.
_UNKNOWN_PRODUCER = 21


def _parse_addr(addr: str) -> tuple[str, int]:
    if addr.startswith("["):
        # [ipv6]:port
        host, _, port_s = addr[1:].partition("]:")
        if not port_s:
            raise ValueError(f"invalid address: {addr!r}")
        return host, int(port_s)
    host, sep, port_s = addr.rpartition(":")
    if not sep or not host:
        raise ValueError(f"invalid address: {addr!r} (expected host:port)")
    return host, int(port_s)


def _format_addr(host: str, port: int) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]:{port}"
    return f"{host}:{port}"


@dataclass
class ProduceResult:
    topic: str
    partition: int
    base_offset: int
    count: int


@dataclass
class DescribeConfigsResult:
    """Result of a successful DescribeConfigs (Phase 13 / v0.53)."""

    topic: str
    topic_id: int
    partition_count: int
    configs: list[tuple[str, str]]


@dataclass
class DeleteRecordsResult:
    """Result of DeleteRecords (Phase 14 / v0.52)."""

    topic: str
    partition: int
    low_watermark: int


@dataclass
class JoinGroupResult:
    """Result of a successful JoinGroup.

    Unpacks as ``(member_id, generation, assignment)`` to match the advertised
    client API. ``revoked`` is the Phase 17 cooperative trailer (may be empty).
    """

    member_id: str
    generation: int
    assignment: list[Assignment]
    revoked: list[Assignment] = field(default_factory=list)

    def __iter__(self):
        yield self.member_id
        yield self.generation
        yield self.assignment


@dataclass
class DescribeGroupResult:
    """Result of a successful DescribeGroup (Phase 11 / v0.49)."""

    group_id: str
    generation: int
    members: list[GroupMemberInfo]


@dataclass
class FetchResult:
    """Fetched batch. Iterate for :class:`FetchRecord`s."""

    topic: str
    partition: int
    high_watermark: int
    records: list[FetchRecord]

    def __iter__(self):
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)

    def tuples(self) -> list[tuple[int, Optional[bytes], bytes]]:
        """List of ``(offset, key, value)``."""
        return [(r.offset, r.key, r.value) for r in self.records]


def wrap_tls(
    sock: socket.socket,
    host: str,
    *,
    tls_insecure: bool = False,
    tls_ca: Optional[str] = None,
    tls_cert: Optional[str] = None,
    tls_key: Optional[str] = None,
) -> ssl.SSLSocket:
    """Wrap an already-connected TCP socket with TLS.

    ``tls_ca`` is a PEM CA file added to the default trust store (system
    roots, same idea as Rust ``webpki-roots`` + optional ``tls_ca``).
    ``tls_insecure`` skips verification (tests / lab only). ``tls_cert``
    and ``tls_key`` are optional client PEMs for mTLS and must be paired.
    """
    if (tls_cert is None) != (tls_key is None):
        raise ValueError("tls_cert and tls_key must both be set or both unset")
    if tls_insecure:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        if tls_ca:
            ctx.load_verify_locations(cafile=tls_ca)
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.check_hostname = True
    if tls_cert and tls_key:
        ctx.load_cert_chain(certfile=tls_cert, keyfile=tls_key)
    # SNI uses the dial host even when verification is off.
    return ctx.wrap_socket(sock, server_hostname=host)


class Client:
    """Sync TCP client for the native Volant protocol (MVP).

    Example::

        from volant import Client
        c = Client("127.0.0.1:9092")
        c.create_topic("t", partitions=1)
        c.create_partitions("t", 2)
        c.produce("t", 0, value=b"hello")
        batch = c.fetch("t", 0, offset=0)
        c.offset_commit(group="g", topic="t", partition=0, offset=5)
        offs = c.offset_fetch(group="g", topic="t")
        bounds = c.list_offsets("t")  # all partitions; or list_offsets("t", [0])
        member_id, generation, assignment = c.join_group(
            "g", topics=["t"], session_timeout_ms=10000
        )
        c.heartbeat("g", member_id, generation)
        c.leave_group("g", member_id)
        meta = c.metadata()
        c.close()

    Optional TLS (v0.27) wraps the socket after TCP connect::

        c = Client("127.0.0.1:9092", tls=True, tls_ca="ca.pem")

    Optional shared-token Auth (v0.42) is sent once after TCP (and TLS,
    if any) when ``auth_token`` is a non-empty string::

        c = Client("127.0.0.1:9092", auth_token="s3cret")
        c = Client("127.0.0.1:9092", tls=True, tls_ca="ca.pem", auth_token="s3cret")

    Optional idempotent produce (v0.47) sends InitProducerId (opcode 32)
    with an empty transactional_id on the first produce and attaches a
    per-partition sequence trailer. Default is off (trailer ``(0, 0, -1)``)::

        c = Client("127.0.0.1:9092", enable_idempotence=True)

    Optional SCRAM-SHA-256 (v0.46) runs after connect when both
    ``scram_username`` and ``scram_password`` are set and ``auth_token``
    is unset. Token wins if both are provided. Username without
    password (or vice versa) is a constructor error::

        c = Client("127.0.0.1:9092", scram_username="alice", scram_password="s3cret")

    Create/Delete/ListScramUsers (v0.55) are admin RPCs (opcodes 64–69),
    not the handshake. Password is sent in the clear on create (use TLS)::

        c.create_scram_user("alice", "s3cret")
        names = c.list_scram_users()
        c.delete_scram_user("alice")

    Membership overlay admin (v0.58) is native opcodes 102–107. Overlay
    is still SoT; follower forward is broker-side (v0.38)::

        gen = c.add_broker(2, "10.0.0.2", 9092, rack="r1")
        members = c.list_members()
        gen = c.remove_broker(2)
    """

    def __init__(
        self,
        addr: str,
        *,
        timeout: float = 10.0,
        tls: bool = False,
        tls_insecure: bool = False,
        tls_ca: Optional[str] = None,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
        auth_token: Optional[str] = None,
        max_redirects: int = 1,
        enable_idempotence: bool = False,
        scram_username: Optional[str] = None,
        scram_password: Optional[str] = None,
    ) -> None:
        if tls and (tls_cert is None) != (tls_key is None):
            raise ValueError("tls_cert and tls_key must both be set or both unset")
        user = scram_username or None
        password = scram_password or None
        if (user is None) != (password is None):
            raise ValueError("scram_username and scram_password must both be set")
        self.tls = bool(tls)
        self._timeout = timeout
        self._tls_insecure = tls_insecure
        self._tls_ca = tls_ca
        self._tls_cert = tls_cert
        self._tls_key = tls_key
        self.auth_token = auth_token or None
        self.scram_username = user
        self.scram_password = password
        # 0 = never redirect (raise on the first NotLeaderForPartition).
        self.max_redirects = max(0, int(max_redirects))
        self.enable_idempotence = bool(enable_idempotence)
        self._producer_id = 0
        self._producer_epoch = 0
        self._producer_ready = False
        self._next_seq: dict[tuple[str, int], int] = {}
        self._next_corr = 1
        self._buf = bytearray()
        self._sock = None  # type: ignore[assignment]
        self.addr = ""
        self._open(addr)
        try:
            self._maybe_authenticate()
        except Exception:
            self.close()
            raise

    def _open(self, addr: str) -> None:
        host, port = _parse_addr(addr)
        self.addr = _format_addr(host, port)
        raw = socket.create_connection((host, port), timeout=self._timeout)
        raw.settimeout(self._timeout)
        if self.tls:
            try:
                self._sock = wrap_tls(
                    raw,
                    host,
                    tls_insecure=self._tls_insecure,
                    tls_ca=self._tls_ca,
                    tls_cert=self._tls_cert,
                    tls_key=self._tls_key,
                )
            except Exception:
                try:
                    raw.close()
                except OSError:
                    pass
                raise
        else:
            self._sock = raw
        self._sock.settimeout(self._timeout)

    def _reconnect(self, addr: str) -> None:
        old = getattr(self, "_sock", None)
        self._sock = None  # type: ignore[assignment]
        self._buf = bytearray()
        if old is not None:
            try:
                old.close()
            except OSError:
                pass
        self._open(addr)
        self._maybe_authenticate()

    def _redirect_to_leader(self, topic: str, partition: int) -> bool:
        """Metadata → reconnect to the partition leader.

        Returns True when the caller should retry (redirected or already
        on that host). Returns False when Metadata has no leader / unknown
        broker / empty host — caller must raise the original error 13.
        """
        meta = self.metadata()
        leader_id = None
        for tinfo in meta.topics:
            if tinfo.name != topic:
                continue
            for part in tinfo.partitions:
                if part.partition_id == partition:
                    leader_id = part.leader
                    break
            if leader_id is not None:
                break
        if leader_id is None:
            return False
        broker = None
        for b in meta.brokers:
            if b.node_id == leader_id:
                broker = b
                break
        if broker is None or not broker.host:
            return False
        addr = _format_addr(broker.host, broker.port)
        if addr == self.addr:
            return True
        self._reconnect(addr)
        return True

    def close(self) -> None:
        sock = getattr(self, "_sock", None)
        if sock is not None:
            try:
                sock.close()
            finally:
                self._sock = None  # type: ignore[assignment]

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _next_correlation(self) -> int:
        corr = self._next_corr
        self._next_corr = (self._next_corr + 1) & 0xFFFFFFFF
        if self._next_corr == 0:
            self._next_corr = 1
        return corr

    def _send(self, opcode: int, payload: bytes) -> int:
        corr = self._next_correlation()
        frame = encode_frame(opcode, corr, payload)
        self._sock.sendall(frame)
        return corr

    def _recv_frame(self):
        while True:
            frame, rest = try_decode_frame(bytes(self._buf))
            if frame is not None:
                self._buf = bytearray(rest)
                return frame
            # Need more bytes. If we already have a header, read the rest.
            if len(self._buf) >= HEADER_LEN:
                payload_len = int.from_bytes(self._buf[8:12], "big")
                if payload_len > MAX_PAYLOAD:
                    raise ProtocolError(
                        f"payload too large: {payload_len} > {MAX_PAYLOAD}"
                    )
                need = HEADER_LEN + payload_len - len(self._buf)
            else:
                need = HEADER_LEN - len(self._buf)
            chunk = self._sock.recv(max(need, 4096))
            if not chunk:
                raise ProtocolError("connection closed while reading frame")
            self._buf.extend(chunk)

    def _round_trip(self, opcode: int, payload: bytes):
        corr = self._send(opcode, payload)
        frame = self._recv_frame()
        if frame.correlation_id != corr:
            raise ProtocolError(
                f"correlation mismatch: sent {corr}, got {frame.correlation_id}"
            )
        if frame.version != PROTOCOL_VERSION:
            raise ProtocolError(f"unsupported protocol version: {frame.version}")
        decoded = codec.decode_response(frame.opcode, frame.payload)
        if isinstance(decoded, codec.ErrorResponse):
            raise BrokerError(decoded.code, decoded.message)
        return decoded

    def _check(self, error_code: int, op: str) -> None:
        if error_code != 0:
            raise BrokerError(error_code, op=op)

    def _maybe_authenticate(self) -> None:
        if self.auth_token:
            self._authenticate(self.auth_token)
            return
        if self.scram_username and self.scram_password:
            self._authenticate_scram(self.scram_username, self.scram_password)

    def _authenticate(self, token: str) -> None:
        payload = codec.encode_auth_request(codec.AuthRequest(token=token))
        resp = self._round_trip(codec.OP_AUTH, payload)
        if not isinstance(resp, codec.AuthResponse):
            raise ProtocolError(f"unexpected response for auth: {type(resp)}")
        self._check(resp.error_code, "auth")

    def _reset_producer_id(self) -> None:
        self._producer_ready = False
        self._producer_id = 0
        self._producer_epoch = 0
        self._next_seq.clear()

    def _ensure_producer_id(self) -> None:
        if self._producer_ready:
            return
        payload = codec.encode_init_producer_id_request(
            codec.InitProducerIdRequest(transactional_id="")
        )
        resp = self._round_trip(codec.OP_INIT_PRODUCER_ID, payload)
        if not isinstance(resp, codec.InitProducerIdResponse):
            raise ProtocolError(
                f"unexpected response for init_producer_id: {type(resp)}"
            )
        self._check(resp.error_code, "init_producer_id")
        self._producer_id = resp.producer_id
        self._producer_epoch = resp.epoch
        self._producer_ready = True

    def _produce_trailer(self, topic: str, partition: int) -> tuple[int, int, int]:
        if not self.enable_idempotence:
            return 0, 0, -1
        self._ensure_producer_id()
        seq = self._next_seq.get((topic, partition), 0)
        return self._producer_id, self._producer_epoch, seq

    def _note_produce_success(self, topic: str, partition: int, count: int) -> None:
        if not self.enable_idempotence:
            return
        key = (topic, partition)
        cur = self._next_seq.get(key, 0)
        self._next_seq[key] = cur + max(0, int(count))

    def _authenticate_scram(self, username: str, password: str) -> None:
        from .scram import client_proof_and_server_sig, generate_client_nonce

        client_nonce = generate_client_nonce()
        payload = codec.encode_scram_first_request(
            codec.ScramFirstRequest(username=username, client_nonce=client_nonce)
        )
        first = self._round_trip(codec.OP_SCRAM_FIRST, payload)
        if not isinstance(first, codec.ScramFirstResponse):
            raise ProtocolError(f"unexpected response for scram first: {type(first)}")
        self._check(first.error_code, "scram first")
        proof, expected_sig = client_proof_and_server_sig(
            username,
            password,
            client_nonce,
            first.combined_nonce,
            first.salt,
            first.iterations,
        )
        payload = codec.encode_scram_final_request(
            codec.ScramFinalRequest(
                username=username,
                combined_nonce=first.combined_nonce,
                client_proof=proof,
            )
        )
        final = self._round_trip(codec.OP_SCRAM_FINAL, payload)
        if not isinstance(final, codec.ScramFinalResponse):
            raise ProtocolError(f"unexpected response for scram final: {type(final)}")
        self._check(final.error_code, "scram final")
        if final.server_signature != expected_sig:
            raise ProtocolError("scram server signature mismatch")

    def create_topic(
        self,
        name: str,
        partitions: int = 1,
        configs: Optional[list[tuple[str, str]]] = None,
    ) -> int:
        """Create a topic. Returns the broker-assigned topic id."""
        payload = codec.encode_create_topic_request(
            CreateTopicRequest(name=name, partitions=partitions, configs=configs or [])
        )
        resp = self._round_trip(codec.OP_CREATE_TOPIC, payload)
        if not isinstance(resp, codec.CreateTopicResponse):
            raise ProtocolError(f"unexpected response for create_topic: {type(resp)}")
        self._check(resp.error_code, "create_topic")
        return resp.topic_id

    def delete_topic(self, name: str) -> None:
        payload = codec.encode_delete_topic_request(DeleteTopicRequest(name=name))
        resp = self._round_trip(codec.OP_DELETE_TOPIC, payload)
        if not isinstance(resp, codec.DeleteTopicResponse):
            raise ProtocolError(f"unexpected response for delete_topic: {type(resp)}")
        self._check(resp.error_code, "delete_topic")

    def create_partitions(self, topic: str, total_count: int) -> int:
        """Grow ``topic`` to ``total_count`` partitions (native opcode 46).

        ``total_count`` must exceed the current count. Returns the new total.
        Non-zero ``error_code`` raises :class:`BrokerError`. This is not Kafka
        CreatePartitions (API key 37).
        """
        payload = codec.encode_create_partitions_request(
            CreatePartitionsRequest(topic=topic, total_count=total_count)
        )
        resp = self._round_trip(codec.OP_CREATE_PARTITIONS, payload)
        if not isinstance(resp, codec.CreatePartitionsResponse):
            raise ProtocolError(
                f"unexpected response for create_partitions: {type(resp)}"
            )
        self._check(resp.error_code, "create_partitions")
        return resp.partitions

    def produce(
        self,
        topic: str,
        partition: int,
        value: Optional[bytes] = None,
        *,
        key: Optional[bytes] = None,
        messages: Optional[Iterable[Union[bytes, ProduceMessage]]] = None,
        acks: int = 1,
        timestamp_ms: int = -1,
        headers: Optional[list[tuple[str, bytes]]] = None,
    ) -> ProduceResult:
        """Produce one value-only (or keyed) message, or an explicit batch.

        ``partition`` is sent as ``i32`` (use ``-1`` to let the broker assign).
        With ``enable_idempotence=False`` (default) the trailer is
        ``(0, 0, -1)``. When on, the first produce sends InitProducerId
        (empty transactional_id) and later produces attach pid/epoch/seq.
        """
        batch: list[ProduceMessage]
        if messages is not None:
            batch = []
            for m in messages:
                if isinstance(m, ProduceMessage):
                    batch.append(m)
                elif isinstance(m, (bytes, bytearray)):
                    batch.append(ProduceMessage(key=None, value=bytes(m)))
                else:
                    raise TypeError(f"unsupported produce message: {type(m)}")
        elif value is not None:
            batch = [
                ProduceMessage(
                    key=key,
                    value=value,
                    timestamp_ms=timestamp_ms,
                    headers=headers or [],
                )
            ]
        else:
            raise ValueError("produce() requires value= or messages=")

        reinit_budget = 1 if self.enable_idempotence else 0
        while True:
            producer_id, producer_epoch, base_sequence = self._produce_trailer(
                topic, partition
            )
            payload = codec.encode_produce_request(
                ProduceRequest(
                    topic=topic,
                    partition=partition,
                    acks=acks,
                    messages=batch,
                    producer_id=producer_id,
                    producer_epoch=producer_epoch,
                    base_sequence=base_sequence,
                )
            )
            max_attempts = 1 + self.max_redirects
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = self._round_trip(codec.OP_PRODUCE, payload)
                except BrokerError as e:
                    if (
                        e.code == _UNKNOWN_PRODUCER
                        and reinit_budget > 0
                    ):
                        reinit_budget -= 1
                        self._reset_producer_id()
                        break
                    if (
                        e.code == _NOT_LEADER
                        and attempt < max_attempts
                        and partition >= 0
                        and self._redirect_to_leader(topic, partition)
                    ):
                        continue
                    raise
                if not isinstance(resp, ProduceResponse):
                    raise ProtocolError(
                        f"unexpected response for produce: {type(resp)}"
                    )
                if (
                    resp.error_code == _UNKNOWN_PRODUCER
                    and reinit_budget > 0
                ):
                    reinit_budget -= 1
                    self._reset_producer_id()
                    break
                if (
                    resp.error_code == _NOT_LEADER
                    and attempt < max_attempts
                    and self._redirect_to_leader(resp.topic or topic, resp.partition)
                ):
                    continue
                self._check(resp.error_code, "produce")
                seq_part = resp.partition if partition < 0 else partition
                self._note_produce_success(topic, seq_part, len(batch))
                return ProduceResult(
                    topic=resp.topic,
                    partition=resp.partition,
                    base_offset=resp.base_offset,
                    count=resp.count,
                )

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
        payload = codec.encode_fetch_request(
            FetchRequest(
                topic=topic,
                partition=partition,
                from_offset=offset,
                max_messages=max_messages,
                max_bytes=max_bytes,
                max_wait_ms=max_wait_ms,
            )
        )
        max_attempts = 1 + self.max_redirects
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self._round_trip(codec.OP_FETCH, payload)
            except BrokerError as e:
                if (
                    e.code == _NOT_LEADER
                    and attempt < max_attempts
                    and self._redirect_to_leader(topic, partition)
                ):
                    continue
                raise
            if not isinstance(resp, FetchResponse):
                raise ProtocolError(f"unexpected response for fetch: {type(resp)}")
            if (
                resp.error_code == _NOT_LEADER
                and attempt < max_attempts
                and self._redirect_to_leader(resp.topic or topic, resp.partition)
            ):
                continue
            self._check(resp.error_code, "fetch")
            return FetchResult(
                topic=resp.topic,
                partition=resp.partition,
                high_watermark=resp.high_watermark,
                records=resp.records,
            )

    def metadata(self, topics: Optional[list[str]] = None) -> MetadataResponse:
        payload = codec.encode_metadata_request(
            MetadataRequest(topics=list(topics) if topics else [])
        )
        resp = self._round_trip(codec.OP_METADATA, payload)
        if not isinstance(resp, MetadataResponse):
            raise ProtocolError(f"unexpected response for metadata: {type(resp)}")
        return resp

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
        """Commit one group offset (admin path: empty member, generation 0)."""
        payload = codec.encode_offset_commit_request(
            OffsetCommitRequest(
                group_id=group,
                member_id=member_id,
                generation=generation,
                entries=[
                    OffsetCommitEntry(
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        metadata=metadata,
                    )
                ],
            )
        )
        resp = self._round_trip(codec.OP_OFFSET_COMMIT, payload)
        if not isinstance(resp, OffsetCommitResponse):
            raise ProtocolError(f"unexpected response for offset_commit: {type(resp)}")
        self._check(resp.error_code, "offset_commit")

    def list_offsets(
        self, topic: str, partitions: Optional[Iterable[int]] = None
    ) -> list[OffsetListing]:
        """List earliest/latest offsets for ``topic`` (native opcode 48).

        ``None`` or ``[]`` means all partitions (wire count 0). Returns
        :class:`OffsetListing` rows. Non-zero ``error_code`` raises
        :class:`BrokerError`. This is not Kafka ListOffsets (no timestamp
        or isolation); both ends of each log are returned.
        """
        payload = codec.encode_list_offsets_request(
            ListOffsetsRequest(
                topic=topic, partitions=list(partitions) if partitions else []
            )
        )
        resp = self._round_trip(codec.OP_LIST_OFFSETS, payload)
        if not isinstance(resp, ListOffsetsResponse):
            raise ProtocolError(f"unexpected response for list_offsets: {type(resp)}")
        self._check(resp.error_code, "list_offsets")
        return list(resp.entries)



    def delete_offsets(
        self,
        group: str,
        entries: Optional[list[tuple[str, int]]] = None,
    ) -> int:
        """Delete committed offsets for ``group`` (native opcode 38).

        ``None`` or ``[]`` deletes all offsets for the group (wire count 0).
        Returns the number of offset files removed. Non-zero ``error_code``
        raises :class:`BrokerError`. This is not Kafka OffsetDelete.
        """
        wire = (
            [codec.OffsetEntry(topic=t, partition=int(p)) for t, p in entries]
            if entries
            else []
        )
        payload = codec.encode_delete_offsets_request(
            DeleteOffsetsRequest(group_id=group, entries=wire)
        )
        resp = self._round_trip(codec.OP_DELETE_OFFSETS, payload)
        if not isinstance(resp, DeleteOffsetsResponse):
            raise ProtocolError(f"unexpected response for delete_offsets: {type(resp)}")
        self._check(resp.error_code, "delete_offsets")
        return resp.deleted_count


    def describe_configs(self, topic: str) -> DescribeConfigsResult:
        """Describe topic configuration (native opcode 40/41).

        Topic configs only (not Kafka DescribeConfigs / BROKER). Empty
        values mean the key is unset. Non-zero ``error_code`` raises
        :class:`BrokerError` with ``op="describe_configs"``.
        """
        payload = codec.encode_describe_configs_request(
            DescribeConfigsRequest(topic=topic)
        )
        resp = self._round_trip(codec.OP_DESCRIBE_CONFIGS, payload)
        if not isinstance(resp, DescribeConfigsResponse):
            raise ProtocolError(
                f"unexpected response for describe_configs: {type(resp)}"
            )
        self._check(resp.error_code, "describe_configs")
        return DescribeConfigsResult(
            topic=resp.topic,
            topic_id=resp.topic_id,
            partition_count=resp.partition_count,
            configs=list(resp.configs),
        )



    def alter_configs(self, topic: str, configs: list[tuple[str, str]]) -> None:
        """Alter topic configuration (native opcode 42/43).

        Empty value clears that key (same as Rust). Topic configs only.
        Non-zero ``error_code`` raises :class:`BrokerError` with
        ``op="alter_configs"``.
        """
        payload = codec.encode_alter_configs_request(
            AlterConfigsRequest(topic=topic, configs=list(configs or []))
        )
        resp = self._round_trip(codec.OP_ALTER_CONFIGS, payload)
        if not isinstance(resp, AlterConfigsResponse):
            raise ProtocolError(f"unexpected response for alter_configs: {type(resp)}")
        self._check(resp.error_code, "alter_configs")


    def delete_records(
        self,
        topic: str,
        partition: int,
        before_offset: int,
        wait_majority: int = 0,
    ) -> DeleteRecordsResult:
        """Delete records before ``before_offset`` (native opcode 44).

        Returns :class:`DeleteRecordsResult` with the new log start
        (``low_watermark``). ``wait_majority`` is the Phase 137 trailer:
        0 = broker default, 1 = force wait, 2 = force no-wait. Always
        written on the wire. Non-zero ``error_code`` raises
        :class:`BrokerError`. Error 13 is **not** redirected (Produce/Fetch
        only). This is not Kafka DeleteRecords (API key 21).
        """
        payload = codec.encode_delete_records_request(
            DeleteRecordsRequest(
                topic=topic,
                partition=partition,
                before_offset=before_offset,
                wait_majority=wait_majority,
            )
        )
        resp = self._round_trip(codec.OP_DELETE_RECORDS, payload)
        if not isinstance(resp, DeleteRecordsResponse):
            raise ProtocolError(
                f"unexpected response for delete_records: {type(resp)}"
            )
        self._check(resp.error_code, "delete_records")
        return DeleteRecordsResult(
            topic=resp.topic,
            partition=resp.partition,
            low_watermark=resp.low_watermark,
        )

    def offset_fetch(self, group: str, topic: str) -> list[tuple[int, int]]:
        """Fetch committed offsets for ``topic``.

        Returns ``[(partition, offset), ...]``. Empty wire entries mean all
        offsets for the group; this method filters to ``topic`` client-side
        (same as the CLI).
        """
        payload = codec.encode_offset_fetch_request(
            OffsetFetchRequest(group_id=group, entries=[])
        )
        resp = self._round_trip(codec.OP_OFFSET_FETCH, payload)
        if not isinstance(resp, OffsetFetchResponse):
            raise ProtocolError(f"unexpected response for offset_fetch: {type(resp)}")
        self._check(resp.error_code, "offset_fetch")
        return [(e.partition, e.offset) for e in resp.entries if e.topic == topic]

    def join_group(
        self,
        group: str,
        topics: Optional[list[str]] = None,
        session_timeout_ms: int = 10_000,
        *,
        member_id: str = "",
        group_instance_id: str = "",
    ) -> JoinGroupResult:
        """Join a consumer group.

        First join sends empty ``member_id`` (broker assigns one). Returns a
        result that unpacks as ``(member_id, generation, assignment)``.
        """
        timeout = 10_000 if session_timeout_ms == 0 else session_timeout_ms
        payload = codec.encode_join_group_request(
            JoinGroupRequest(
                group_id=group,
                member_id=member_id,
                session_timeout_ms=timeout,
                topics=list(topics) if topics else [],
                group_instance_id=group_instance_id,
            )
        )
        resp = self._round_trip(codec.OP_JOIN_GROUP, payload)
        if not isinstance(resp, JoinGroupResponse):
            raise ProtocolError(f"unexpected response for join_group: {type(resp)}")
        self._check(resp.error_code, "join_group")
        return JoinGroupResult(
            member_id=resp.member_id,
            generation=resp.generation,
            assignment=list(resp.assignment),
            revoked=list(resp.revoked),
        )

    def heartbeat(self, group: str, member_id: str, generation: int) -> int:
        """Heartbeat for group membership. Returns the broker error_code."""
        payload = codec.encode_heartbeat_request(
            HeartbeatRequest(
                group_id=group, member_id=member_id, generation=generation
            )
        )
        resp = self._round_trip(codec.OP_HEARTBEAT, payload)
        if not isinstance(resp, HeartbeatResponse):
            raise ProtocolError(f"unexpected response for heartbeat: {type(resp)}")
        self._check(resp.error_code, "heartbeat")
        return resp.error_code

    def leave_group(self, group: str, member_id: str) -> None:
        """Leave a consumer group."""
        payload = codec.encode_leave_group_request(
            LeaveGroupRequest(group_id=group, member_id=member_id)
        )
        resp = self._round_trip(codec.OP_LEAVE_GROUP, payload)
        if not isinstance(resp, LeaveGroupResponse):
            raise ProtocolError(f"unexpected response for leave_group: {type(resp)}")
        self._check(resp.error_code, "leave_group")

    def describe_group(self, group: str) -> DescribeGroupResult:
        """Describe a live consumer group (native opcode 34/35).

        Error 2 (NotFound, no live members) raises :class:`BrokerError`.
        """
        payload = codec.encode_describe_group_request(
            DescribeGroupRequest(group_id=group)
        )
        resp = self._round_trip(codec.OP_DESCRIBE_GROUP, payload)
        if not isinstance(resp, DescribeGroupResponse):
            raise ProtocolError(f"unexpected response for describe_group: {type(resp)}")
        self._check(resp.error_code, "describe_group")
        return DescribeGroupResult(
            group_id=resp.group_id,
            generation=resp.generation,
            members=list(resp.members),
        )

    def list_groups(self) -> list[GroupListing]:
        """List known consumer groups (native opcode 36/37)."""
        resp = self._round_trip(codec.OP_LIST_GROUPS, codec.encode_list_groups_request())
        if not isinstance(resp, ListGroupsResponse):
            raise ProtocolError(f"unexpected response for list_groups: {type(resp)}")
        self._check(resp.error_code, "list_groups")
        return list(resp.groups)

    def create_scram_user(
        self, username: str, password: str, iterations: int = 0
    ) -> None:
        """Create or replace a SCRAM user (native opcode 64/65).

        ``iterations=0`` means the broker default (4096). Password is sent
        in the clear (use TLS). This is not the v0.46 handshake (60–63).
        """
        payload = codec.encode_create_scram_user_request(
            codec.CreateScramUserRequest(
                username=username, password=password, iterations=iterations
            )
        )
        resp = self._round_trip(codec.OP_CREATE_SCRAM_USER, payload)
        if not isinstance(resp, codec.CreateScramUserResponse):
            raise ProtocolError(
                f"unexpected response for create_scram_user: {type(resp)}"
            )
        self._check(resp.error_code, "create_scram_user")

    def delete_scram_user(self, username: str) -> None:
        """Delete a SCRAM user (native opcode 66/67)."""
        payload = codec.encode_delete_scram_user_request(
            codec.DeleteScramUserRequest(username=username)
        )
        resp = self._round_trip(codec.OP_DELETE_SCRAM_USER, payload)
        if not isinstance(resp, codec.DeleteScramUserResponse):
            raise ProtocolError(
                f"unexpected response for delete_scram_user: {type(resp)}"
            )
        self._check(resp.error_code, "delete_scram_user")

    def list_scram_users(self) -> list[str]:
        """List SCRAM usernames (native opcode 68/69)."""
        resp = self._round_trip(
            codec.OP_LIST_SCRAM_USERS, codec.encode_list_scram_users_request()
        )
        if not isinstance(resp, codec.ListScramUsersResponse):
            raise ProtocolError(
                f"unexpected response for list_scram_users: {type(resp)}"
            )
        self._check(resp.error_code, "list_scram_users")
        return list(resp.usernames)

    def add_broker(
        self, id: int, host: str, port: int, rack: Optional[str] = None
    ) -> int:
        """Add a broker endpoint to the membership overlay (native 102/103).

        ``rack=None`` is absent on the wire (flag 0). Returns the overlay
        generation. Non-zero ``error_code`` raises :class:`BrokerError`
        with ``op="add_broker"``. Overlay is still SoT; this is not Kafka
        broker catalog / AlterPartitionReassignments.
        """
        payload = codec.encode_add_broker_request(
            codec.AddBrokerRequest(id=id, host=host, port=port, rack=rack)
        )
        resp = self._round_trip(codec.OP_ADD_BROKER, payload)
        if not isinstance(resp, codec.AddBrokerResponse):
            raise ProtocolError(f"unexpected response for add_broker: {type(resp)}")
        self._check(resp.error_code, "add_broker")
        return resp.generation

    def remove_broker(self, id: int) -> int:
        """Remove a broker from the membership overlay (native 104/105).

        Returns the overlay generation. Non-zero ``error_code`` raises
        :class:`BrokerError` with ``op="remove_broker"``.
        """
        payload = codec.encode_remove_broker_request(
            codec.RemoveBrokerRequest(id=id)
        )
        resp = self._round_trip(codec.OP_REMOVE_BROKER, payload)
        if not isinstance(resp, codec.RemoveBrokerResponse):
            raise ProtocolError(
                f"unexpected response for remove_broker: {type(resp)}"
            )
        self._check(resp.error_code, "remove_broker")
        return resp.generation

    def list_members(self) -> MembershipList:
        """List configured + live membership (native opcode 106/107).

        Returns :class:`MembershipList` (generation, brokers, live).
        Non-zero ``error_code`` raises :class:`BrokerError` with
        ``op="list_members"``. Overlay is still SoT.
        """
        resp = self._round_trip(
            codec.OP_LIST_MEMBERS, codec.encode_list_members_request()
        )
        if not isinstance(resp, ListMembersResponse):
            raise ProtocolError(
                f"unexpected response for list_members: {type(resp)}"
            )
        self._check(resp.error_code, "list_members")
        return MembershipList(
            generation=resp.generation,
            brokers=list(resp.brokers),
            live=list(resp.live),
        )


# Re-export result types used by callers.
__all__ = [
    "Assignment",
    "BrokerError",
    "BrokerInfo",
    "Client",
    "DescribeConfigsResult",
    "DeleteRecordsResult",
    "DescribeGroupResult",
    "FetchRecord",
    "FetchResult",
    "GroupListing",
    "GroupMemberInfo",
    "GroupState",
    "JoinGroupResult",
    "MembershipBroker",
    "MembershipList",
    "MetadataResponse",
    "OffsetListing",
    "ProduceMessage",
    "ProduceResult",
    "ProtocolError",
    "TopicInfo",
    "wrap_tls",
]
