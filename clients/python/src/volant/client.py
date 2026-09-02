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
    CreateTopicRequest,
    DeleteTopicRequest,
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


@dataclass
class ProduceResult:
    topic: str
    partition: int
    base_offset: int
    count: int


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
        c.produce("t", 0, value=b"hello")
        batch = c.fetch("t", 0, offset=0)
        c.offset_commit(group="g", topic="t", partition=0, offset=5)
        offs = c.offset_fetch(group="g", topic="t")
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
    ) -> None:
        host, port = _parse_addr(addr)
        self.addr = f"{host}:{port}"
        self.tls = bool(tls)
        if tls and (tls_cert is None) != (tls_key is None):
            raise ValueError("tls_cert and tls_key must both be set or both unset")
        raw = socket.create_connection((host, port), timeout=timeout)
        raw.settimeout(timeout)
        if tls:
            try:
                self._sock = wrap_tls(
                    raw,
                    host,
                    tls_insecure=tls_insecure,
                    tls_ca=tls_ca,
                    tls_cert=tls_cert,
                    tls_key=tls_key,
                )
            except Exception:
                try:
                    raw.close()
                except OSError:
                    pass
                raise
        else:
            self._sock = raw
        self._sock.settimeout(timeout)
        self._next_corr = 1
        self._buf = bytearray()
        self.auth_token = auth_token or None
        if self.auth_token:
            try:
                self._authenticate(self.auth_token)
            except Exception:
                self.close()
                raise

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

    def _authenticate(self, token: str) -> None:
        payload = codec.encode_auth_request(codec.AuthRequest(token=token))
        resp = self._round_trip(codec.OP_AUTH, payload)
        if not isinstance(resp, codec.AuthResponse):
            raise ProtocolError(f"unexpected response for auth: {type(resp)}")
        self._check(resp.error_code, "auth")

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
        Idempotent produce is **not** implemented; trailer is ``(0, 0, -1)``.
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
        payload = codec.encode_produce_request(
            ProduceRequest(
                topic=topic,
                partition=partition,
                acks=acks,
                messages=batch,
                producer_id=0,
                producer_epoch=0,
                base_sequence=-1,
            )
        )
        resp = self._round_trip(codec.OP_PRODUCE, payload)
        if not isinstance(resp, ProduceResponse):
            raise ProtocolError(f"unexpected response for produce: {type(resp)}")
        self._check(resp.error_code, "produce")
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
        resp = self._round_trip(codec.OP_FETCH, payload)
        if not isinstance(resp, FetchResponse):
            raise ProtocolError(f"unexpected response for fetch: {type(resp)}")
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


# Re-export result types used by callers.
__all__ = [
    "Assignment",
    "BrokerError",
    "BrokerInfo",
    "Client",
    "FetchRecord",
    "FetchResult",
    "JoinGroupResult",
    "MetadataResponse",
    "ProduceMessage",
    "ProduceResult",
    "ProtocolError",
    "TopicInfo",
    "wrap_tls",
]
