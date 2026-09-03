"""Synchronous native-protocol TCP client."""

from __future__ import annotations

import re
import socket
import ssl
import time
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
    ReassignPartitionsRequest,
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
# Native ErrorCode::NotController — controller-gated admin RPCs.
_NOT_CONTROLLER = 14
_CONTROLLER_ID_RE = re.compile(r"controller_id=(\d+)")
# Native ErrorCode::UnknownProducerId — pid not allocated via InitProducerId.
_UNKNOWN_PRODUCER = 21
# Native ErrorCode::InvalidTxnState — e.g. BeginTxn after the broker already began.
_INVALID_TXN_STATE = 22
# Transient produce codes (Rust is_transient_error_code). Not 13 / 21.
_IO = 6
_TIMEOUT = 7
_NOT_ENOUGH_REPLICAS = 15
_BROKER_NOT_AVAILABLE = 16


def _is_transient_broker(code: int) -> bool:
    return code in (_IO, _TIMEOUT, _NOT_ENOUGH_REPLICAS, _BROKER_NOT_AVAILABLE)


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


def _controller_id_hint(message: str) -> Optional[int]:
    """Parse ``controller_id=N`` from a NotController Error message."""
    if not message:
        return None
    m = _CONTROLLER_ID_RE.search(message)
    if m is None:
        return None
    return int(m.group(1))


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
        c.reassign_partitions("t", [1, 2])  # all partitions; or partition=0
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

    Optional produce/fetch/heartbeat/BeginTxn/EndTxn/admin/Auth/SCRAM
    retry (v0.61 / v0.66 / v0.74 / v0.99 / v0.103 / v0.106 / v0.108)
    retries transient broker codes 6, 7, 15, 16 and TCP I/O errors.
    Default ``max_retries=0`` (no extra attempts). ``retry_backoff_ms``
    defaults to 50; tests may set 0. Error 13 stays on the redirect
    budget; error 21 stays on the one re-Init. Controller-gated admin
    shares this budget; error 14 stays on ``max_redirects``. Heartbeat
    rebalance codes 9 / 10 / 11 are not retried. InvalidTxnState (22)
    is not retried. Auth 17 / 18 is not retried. SCRAM 17 / 18 and
    server-signature mismatch are not retried; a transient first or
    final restarts the handshake with a new client nonce::

        c = Client("127.0.0.1:9092", max_retries=3, retry_backoff_ms=50)
        c.max_retries = 3

    Optional native transactions (v0.57) send InitProducerId with a
    non-empty ``transactional_id``, then BeginTxn / EndTxn (opcodes
    50–53). Produce during an open txn reuses the v0.47 idempotent
    trailer. This is not Kafka transactions::

        c = Client("127.0.0.1:9092", transactional_id="txn-1")
        c.begin_transaction()
        c.produce("t", 0, value=b"hello")
        c.commit_transaction()

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

    Create/Delete/ListAcls (v0.56) are admin RPCs (opcodes 54–59).
    Delete is exact-match only. Empty principal/resource and
    ``resource_type=255`` list any. This is not Kafka CreateAcls::

        from volant.codec import AclBinding
        e = AclBinding("User:alice", 0, "events", 3, 1)
        c.create_acls([e])
        listed = c.list_acls()
        n = c.delete_acls([e])
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
        max_retries: int = 0,
        retry_backoff_ms: int = 50,
        transactional_id: Optional[str] = None,
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
        # Extra produce/fetch attempts on transient broker/transport errors.
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_ms = max(0, int(retry_backoff_ms))
        self.transactional_id = transactional_id or None
        self._producer_id = 0
        self._producer_epoch = 0
        self._producer_ready = False
        self._in_transaction = False
        self._next_seq: dict[tuple[str, int], int] = {}
        self._seq_at_begin: dict[tuple[str, int], int] = {}
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

    def reconnect(self, addr: str) -> None:
        """Reconnect to ``addr``, re-authenticating when a token or SCRAM is configured."""
        self._reconnect(addr)

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

    def _redirect_to_controller(self, controller_id: Optional[int] = None) -> bool:
        """Metadata → reconnect to the controller.

        If ``controller_id`` is known (parsed from ``controller_id=N`` in
        a 14 Error message, or Metadata's v0.77 trailer when non-zero),
        look that node up in Metadata brokers, then ``list_members()``
        if Metadata has no matching id. Otherwise pick the first
        advertised broker whose host:port is not this connection.

        Returns True when the caller should retry. Returns False on no
        other broker / lookup miss / empty host / reconnect fail — caller
        must raise the original error 14.
        """
        meta = self.metadata()
        if controller_id is None and meta.controller_id != 0:
            controller_id = meta.controller_id
        host: Optional[str] = None
        port = 0
        if controller_id is not None:
            for b in meta.brokers:
                if b.node_id == controller_id:
                    host, port = b.host, b.port
                    break
            if host is None:
                try:
                    members = self.list_members()
                except Exception:
                    return False
                for b in members.brokers:
                    if b.id == controller_id:
                        host, port = b.host, b.port
                        break
            if host is None or not host:
                return False
        else:
            for b in meta.brokers:
                if not b.host:
                    continue
                addr = _format_addr(b.host, b.port)
                if addr != self.addr:
                    host, port = b.host, b.port
                    break
            if host is None:
                return False
        addr = _format_addr(host, port)
        if addr == self.addr:
            return True
        try:
            self._reconnect(addr)
        except Exception:
            return False
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

    def _admin_round_trip(self, opcode: int, payload: bytes, expect_type, op: str):
        """Round-trip a controller-gated admin RPC.

        Error 14 follows ``max_redirects`` (not counted as a transient
        retry). Transient 6 / 7 / 15 / 16 and TCP/IO retry up to
        ``max_retries`` extra times (default 0).
        """
        max_attempts = 1 + self.max_redirects
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self._round_trip(opcode, payload)
            except BrokerError as e:
                if (
                    e.code == _NOT_CONTROLLER
                    and attempt < max_attempts
                    and self._redirect_to_controller(_controller_id_hint(e.message))
                ):
                    continue
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    attempt -= 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    attempt -= 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, expect_type):
                raise ProtocolError(f"unexpected response for {op}: {type(resp)}")
            if (
                resp.error_code == _NOT_CONTROLLER
                and attempt < max_attempts
                and self._redirect_to_controller(None)
            ):
                continue
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                attempt -= 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, op)
            return resp

    def _maybe_authenticate(self) -> None:
        if self.auth_token:
            self._authenticate(self.auth_token)
            return
        if self.scram_username and self.scram_password:
            self._authenticate_scram(self.scram_username, self.scram_password)

    def _authenticate(self, token: str) -> None:
        payload = codec.encode_auth_request(codec.AuthRequest(token=token))
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_AUTH, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, codec.AuthResponse):
                raise ProtocolError(f"unexpected response for auth: {type(resp)}")
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "auth")
            return

    def _uses_pid(self) -> bool:
        return bool(self.enable_idempotence or self.transactional_id)

    def _reset_producer_id(self) -> None:
        self._producer_ready = False
        self._producer_id = 0
        self._producer_epoch = 0
        self._in_transaction = False
        self._next_seq.clear()
        self._seq_at_begin.clear()

    def _ensure_producer_id(self) -> None:
        if self._producer_ready:
            return
        txn = self.transactional_id or ""
        payload = codec.encode_init_producer_id_request(
            codec.InitProducerIdRequest(transactional_id=txn)
        )
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_INIT_PRODUCER_ID, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, codec.InitProducerIdResponse):
                raise ProtocolError(
                    f"unexpected response for init_producer_id: {type(resp)}"
                )
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "init_producer_id")
            self._producer_id = resp.producer_id
            self._producer_epoch = resp.epoch
            self._producer_ready = True
            self._in_transaction = False
            self._next_seq.clear()
            return

    def _produce_trailer(self, topic: str, partition: int) -> tuple[int, int, int]:
        if not self._uses_pid():
            return 0, 0, -1
        self._ensure_producer_id()
        seq = self._next_seq.get((topic, partition), 0)
        return self._producer_id, self._producer_epoch, seq

    def _note_produce_success(self, topic: str, partition: int, count: int) -> None:
        if not self._uses_pid():
            return
        key = (topic, partition)
        cur = self._next_seq.get(key, 0)
        self._next_seq[key] = cur + max(0, int(count))

    def _sleep_produce_retry(self) -> None:
        ms = max(0, int(self.retry_backoff_ms))
        if ms:
            time.sleep(ms / 1000.0)

    def _authenticate_scram(self, username: str, password: str) -> None:
        """SCRAM-SHA-256 first+final as one unit (v0.108).

        Transient 6 / 7 / 15 / 16 and TCP/IO restart from first with a
        new client nonce (``max_retries`` extra times, default 0). 17 /
        18, other broker codes, and protocol errors (including server
        signature mismatch) are not retried. Token Auth is not wrapped.
        """
        from .scram import client_proof_and_server_sig, generate_client_nonce

        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                client_nonce = generate_client_nonce()
                payload = codec.encode_scram_first_request(
                    codec.ScramFirstRequest(username=username, client_nonce=client_nonce)
                )
                first = self._round_trip(codec.OP_SCRAM_FIRST, payload)
                if not isinstance(first, codec.ScramFirstResponse):
                    raise ProtocolError(
                        f"unexpected response for scram first: {type(first)}"
                    )
                if (
                    _is_transient_broker(first.error_code)
                    and retry_attempt < max_retries
                ):
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
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
                    raise ProtocolError(
                        f"unexpected response for scram final: {type(final)}"
                    )
                if (
                    _is_transient_broker(final.error_code)
                    and retry_attempt < max_retries
                ):
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                self._check(final.error_code, "scram final")
                if final.server_signature != expected_sig:
                    raise ProtocolError("scram server signature mismatch")
                return
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise

    def create_topic(
        self,
        name: str,
        partitions: int = 1,
        configs: Optional[list[tuple[str, str]]] = None,
    ) -> int:
        """Create a topic. Returns the broker-assigned topic id.

        Error 14 (NotController) follows ``max_redirects`` (same budget as
        Produce/Fetch error 13). Transient 6 / 7 / 15 / 16 and TCP/IO
        follow ``max_retries`` (default 0); 14 is not a retry.
        """
        payload = codec.encode_create_topic_request(
            CreateTopicRequest(name=name, partitions=partitions, configs=configs or [])
        )
        resp = self._admin_round_trip(
            codec.OP_CREATE_TOPIC, payload, codec.CreateTopicResponse, "create_topic"
        )
        return resp.topic_id

    def delete_topic(self, name: str) -> None:
        payload = codec.encode_delete_topic_request(DeleteTopicRequest(name=name))
        self._admin_round_trip(
            codec.OP_DELETE_TOPIC, payload, codec.DeleteTopicResponse, "delete_topic"
        )

    def create_partitions(self, topic: str, total_count: int) -> int:
        """Grow ``topic`` to ``total_count`` partitions (native opcode 46).

        ``total_count`` must exceed the current count. Returns the new total.
        Non-zero ``error_code`` raises :class:`BrokerError`. Error 14 follows
        ``max_redirects``. This is not Kafka CreatePartitions (API key 37).
        """
        payload = codec.encode_create_partitions_request(
            CreatePartitionsRequest(topic=topic, total_count=total_count)
        )
        resp = self._admin_round_trip(
            codec.OP_CREATE_PARTITIONS,
            payload,
            codec.CreatePartitionsResponse,
            "create_partitions",
        )
        return resp.partitions


    def add_broker(
        self, id: int, host: str, port: int, rack: Optional[str] = None
    ) -> int:
        """Add a broker endpoint to the membership overlay (native 102/103).

        ``rack=None`` is absent on the wire (flag 0). Returns the overlay
        generation. Non-zero ``error_code`` raises :class:`BrokerError`
        with ``op="add_broker"``. Overlay is still SoT; this is not Kafka
        broker catalog / AlterPartitionReassignments. Error 14 follows
        ``max_redirects`` when the broker cannot forward.
        """
        payload = codec.encode_add_broker_request(
            codec.AddBrokerRequest(id=id, host=host, port=port, rack=rack)
        )
        resp = self._admin_round_trip(
            codec.OP_ADD_BROKER, payload, codec.AddBrokerResponse, "add_broker"
        )
        return resp.generation



    def remove_broker(self, id: int) -> int:
        """Remove a broker from the membership overlay (native 104/105).

        Returns the overlay generation. Non-zero ``error_code`` raises
        :class:`BrokerError` with ``op="remove_broker"``. Error 14 follows
        ``max_redirects`` when the broker cannot forward.
        """
        payload = codec.encode_remove_broker_request(
            codec.RemoveBrokerRequest(id=id)
        )
        resp = self._admin_round_trip(
            codec.OP_REMOVE_BROKER,
            payload,
            codec.RemoveBrokerResponse,
            "remove_broker",
        )
        return resp.generation



    def list_members(self) -> MembershipList:
        """List configured + live membership (native opcode 106/107).

        Returns :class:`MembershipList` (generation, brokers, live).
        Non-zero ``error_code`` raises :class:`BrokerError` with
        ``op="list_members"``. Overlay is still SoT. Transient
        broker/transport errors retry up to ``max_retries`` extra
        times (default 0). Error 2 / 9 / 10 / 11 / 13 / 14 are not
        retried.
        """
        payload = codec.encode_list_members_request()
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_LIST_MEMBERS, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, ListMembersResponse):
                raise ProtocolError(
                    f"unexpected response for list_members: {type(resp)}"
                )
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "list_members")
            return MembershipList(
                generation=resp.generation,
                brokers=list(resp.brokers),
                live=list(resp.live),
            )

    def reassign_partitions(
        self,
        topic: str,
        replicas: list[int],
        partition: Optional[int] = None,
    ) -> int:
        """Reassign replicas for ``topic`` (native opcode 114).

        ``partition=None`` updates every partition (wire ``u32::MAX``).
        Empty ``replicas`` asks the controller to auto-place with the current
        membership (same as CreateTopic). Returns the assignment generation.
        Non-zero ``error_code`` raises :class:`BrokerError`. Error 14 follows
        ``max_redirects``. This is not Kafka AlterPartitionReassignments
        (API key 45).
        """
        wire_partition = (
            codec.REASSIGN_ALL_PARTITIONS if partition is None else int(partition)
        )
        payload = codec.encode_reassign_partitions_request(
            ReassignPartitionsRequest(
                topic=topic,
                partition=wire_partition,
                replicas=list(replicas) if replicas else [],
            )
        )
        resp = self._admin_round_trip(
            codec.OP_REASSIGN_PARTITIONS,
            payload,
            codec.ReassignPartitionsResponse,
            "reassign_partitions",
        )
        return resp.generation

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
        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Error 13 uses ``max_redirects`` only;
        error 21 uses the one re-Init only. Failed produce does not
        increment the idempotent sequence.
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

        reinit_budget = 1 if self._uses_pid() else 0
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
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
            max_redirect_attempts = 1 + self.max_redirects
            redirect_attempt = 0
            while True:
                redirect_attempt += 1
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
                        and redirect_attempt < max_redirect_attempts
                        and partition >= 0
                        and self._redirect_to_leader(topic, partition)
                    ):
                        continue
                    if (
                        _is_transient_broker(e.code)
                        and retry_attempt < max_retries
                    ):
                        retry_attempt += 1
                        redirect_attempt -= 1
                        self._sleep_produce_retry()
                        continue
                    raise
                except OSError:
                    if retry_attempt < max_retries:
                        retry_attempt += 1
                        redirect_attempt -= 1
                        self._sleep_produce_retry()
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
                    and redirect_attempt < max_redirect_attempts
                    and self._redirect_to_leader(resp.topic or topic, resp.partition)
                ):
                    continue
                if (
                    _is_transient_broker(resp.error_code)
                    and retry_attempt < max_retries
                ):
                    retry_attempt += 1
                    redirect_attempt -= 1
                    self._sleep_produce_retry()
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

    def begin_transaction(self) -> None:
        """Open a native transaction (opcode 50). Requires ``transactional_id``.

        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). InvalidTxnState (22) and txn fence /
        epoch errors are not retried.
        """
        if not self.transactional_id:
            raise ValueError("transactional_id not configured")
        self._ensure_producer_id()
        payload = codec.encode_begin_txn_request(
            codec.BeginTxnRequest(
                producer_id=self._producer_id, producer_epoch=self._producer_epoch
            )
        )
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_BEGIN_TXN, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, codec.BeginTxnResponse):
                raise ProtocolError(
                    f"unexpected response for begin_txn: {type(resp)}"
                )
            if resp.error_code == _INVALID_TXN_STATE:
                self._check(resp.error_code, "begin_txn")
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "begin_txn")
            self._seq_at_begin = dict(self._next_seq)
            self._in_transaction = True
            return

    def commit_transaction(
        self, offsets: Optional[Iterable[codec.TxnOffsetCommit]] = None
    ) -> list[codec.TxnProduceResult]:
        """Commit the open transaction (opcode 52, committed=1)."""
        return self._end_transaction(True, list(offsets) if offsets else [])

    def abort_transaction(self) -> None:
        """Abort the open transaction (opcode 52, committed=0) and rewind sequences."""
        self._end_transaction(False, [])

    def _end_transaction(
        self, committed: bool, offsets: list[codec.TxnOffsetCommit]
    ) -> list[codec.TxnProduceResult]:
        if not self._producer_ready:
            raise ValueError("producer id not initialized")
        payload = codec.encode_end_txn_request(
            codec.EndTxnRequest(
                producer_id=self._producer_id,
                producer_epoch=self._producer_epoch,
                committed=committed,
                offsets=offsets,
            )
        )
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_END_TXN, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, codec.EndTxnResponse):
                raise ProtocolError(
                    f"unexpected response for end_txn: {type(resp)}"
                )
            if resp.error_code == _INVALID_TXN_STATE:
                self._check(resp.error_code, "end_txn")
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "end_txn")
            self._in_transaction = False
            if not committed:
                self._next_seq = dict(self._seq_at_begin)
            self._seq_at_begin.clear()
            return list(resp.results)

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
        """Fetch records from topic/partition starting at ``offset``.

        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Error 13 uses ``max_redirects`` only.
        """
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
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            max_redirect_attempts = 1 + self.max_redirects
            redirect_attempt = 0
            while True:
                redirect_attempt += 1
                try:
                    resp = self._round_trip(codec.OP_FETCH, payload)
                except BrokerError as e:
                    if (
                        e.code == _NOT_LEADER
                        and redirect_attempt < max_redirect_attempts
                        and self._redirect_to_leader(topic, partition)
                    ):
                        continue
                    if (
                        _is_transient_broker(e.code)
                        and retry_attempt < max_retries
                    ):
                        retry_attempt += 1
                        self._sleep_produce_retry()
                        break
                    raise
                except OSError:
                    if retry_attempt < max_retries:
                        retry_attempt += 1
                        self._sleep_produce_retry()
                        break
                    raise
                if not isinstance(resp, FetchResponse):
                    raise ProtocolError(
                        f"unexpected response for fetch: {type(resp)}"
                    )
                if (
                    resp.error_code == _NOT_LEADER
                    and redirect_attempt < max_redirect_attempts
                    and self._redirect_to_leader(resp.topic or topic, resp.partition)
                ):
                    continue
                if (
                    _is_transient_broker(resp.error_code)
                    and retry_attempt < max_retries
                ):
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    break
                self._check(resp.error_code, "fetch")
                return FetchResult(
                    topic=resp.topic,
                    partition=resp.partition,
                    high_watermark=resp.high_watermark,
                    records=resp.records,
                )

    def metadata(self, topics: Optional[list[str]] = None) -> MetadataResponse:
        """Cluster brokers and topics (all topics when ``topics`` is empty).

        Native Metadata has no top-level ``error_code``; failures arrive
        as Error opcode / transport. Transient broker/transport errors
        retry up to ``max_retries`` extra times (default 0). Error 2 /
        9 / 10 / 11 / 13 / 14 are not retried.
        """
        payload = codec.encode_metadata_request(
            MetadataRequest(topics=list(topics) if topics else [])
        )
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_METADATA, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, MetadataResponse):
                raise ProtocolError(
                    f"unexpected response for metadata: {type(resp)}"
                )
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
        """Commit one group offset (admin path: empty member, generation 0).

        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Error 14 follows ``max_redirects``.
        Error 2 / 9 / 10 / 11 / 13 are not retried.
        """
        self.commit_offsets(
            group,
            [
                OffsetCommitEntry(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    metadata=metadata,
                )
            ],
            member_id=member_id,
            generation=generation,
        )

    def commit_offsets(
        self,
        group: str,
        entries: Iterable[Union[OffsetCommitEntry, tuple]],
        *,
        member_id: str = "",
        generation: int = 0,
    ) -> None:
        """Commit N group offsets in one OffsetCommit RPC (native opcode 6).

        ``entries`` are :class:`OffsetCommitEntry` or
        ``(topic, partition, offset)`` /
        ``(topic, partition, offset, metadata)`` tuples.
        ``generation = 0`` skips the broker generation check.
        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Error 14 follows ``max_redirects``.
        Error 2 / 9 / 10 / 11 / 13 are not retried.
        """
        parsed: list[OffsetCommitEntry] = []
        for e in entries or ():
            if isinstance(e, OffsetCommitEntry):
                parsed.append(e)
            elif isinstance(e, tuple) and len(e) == 3:
                topic, partition, offset = e
                parsed.append(
                    OffsetCommitEntry(
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        metadata="",
                    )
                )
            elif isinstance(e, tuple) and len(e) == 4:
                topic, partition, offset, metadata = e
                parsed.append(
                    OffsetCommitEntry(
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        metadata=metadata,
                    )
                )
            else:
                raise TypeError(f"unsupported offset commit entry: {type(e)}")
        payload = codec.encode_offset_commit_request(
            OffsetCommitRequest(
                group_id=group,
                member_id=member_id,
                generation=generation,
                entries=parsed,
            )
        )
        max_retries = max(0, int(self.max_retries))
        max_attempts = 1 + self.max_redirects
        retry_attempt = 0
        redirect_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_OFFSET_COMMIT, payload)
            except BrokerError as e:
                if (
                    e.code == _NOT_CONTROLLER
                    and redirect_attempt + 1 < max_attempts
                    and self._redirect_to_controller(_controller_id_hint(e.message))
                ):
                    redirect_attempt += 1
                    continue
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, OffsetCommitResponse):
                raise ProtocolError(
                    f"unexpected response for offset_commit: {type(resp)}"
                )
            if (
                resp.error_code == _NOT_CONTROLLER
                and redirect_attempt + 1 < max_attempts
                and self._redirect_to_controller(None)
            ):
                redirect_attempt += 1
                continue
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "offset_commit")
            return

    def list_offsets(
        self, topic: str, partitions: Optional[Iterable[int]] = None
    ) -> list[OffsetListing]:
        """List earliest/latest offsets for ``topic`` (native opcode 48).

        ``None`` or ``[]`` means all partitions (wire count 0). Returns
        :class:`OffsetListing` rows. Non-zero ``error_code`` raises
        :class:`BrokerError`. Transient broker/transport errors retry
        up to ``max_retries`` extra times (default 0). Error 13 follows
        Produce/Fetch redirect (``max_redirects``); 13 is not a
        transient retry. Error 2 / 9 / 10 / 11 / 14 are not retried.
        This is not Kafka ListOffsets (no timestamp or isolation);
        both ends of each log are returned.
        """
        parts = list(partitions) if partitions else []
        payload = codec.encode_list_offsets_request(
            ListOffsetsRequest(topic=topic, partitions=parts)
        )
        redirect_partition = int(parts[0]) if parts else 0
        max_retries = max(0, int(self.max_retries))
        max_attempts = 1 + self.max_redirects
        retry_attempt = 0
        redirect_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_LIST_OFFSETS, payload)
            except BrokerError as e:
                if (
                    e.code == _NOT_LEADER
                    and redirect_attempt + 1 < max_attempts
                    and self._redirect_to_leader(topic, redirect_partition)
                ):
                    redirect_attempt += 1
                    continue
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, ListOffsetsResponse):
                raise ProtocolError(
                    f"unexpected response for list_offsets: {type(resp)}"
                )
            if (
                resp.error_code == _NOT_LEADER
                and redirect_attempt + 1 < max_attempts
                and self._redirect_to_leader(
                    resp.topic or topic, redirect_partition
                )
            ):
                redirect_attempt += 1
                continue
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
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
        raises :class:`BrokerError`. Error 14 follows ``max_redirects``.
        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). This is not Kafka OffsetDelete.
        """
        wire = (
            [codec.OffsetEntry(topic=t, partition=int(p)) for t, p in entries]
            if entries
            else []
        )
        payload = codec.encode_delete_offsets_request(
            DeleteOffsetsRequest(group_id=group, entries=wire)
        )
        max_retries = max(0, int(self.max_retries))
        max_attempts = 1 + self.max_redirects
        retry_attempt = 0
        redirect_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_DELETE_OFFSETS, payload)
            except BrokerError as e:
                if (
                    e.code == _NOT_CONTROLLER
                    and redirect_attempt + 1 < max_attempts
                    and self._redirect_to_controller(_controller_id_hint(e.message))
                ):
                    redirect_attempt += 1
                    continue
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, DeleteOffsetsResponse):
                raise ProtocolError(
                    f"unexpected response for delete_offsets: {type(resp)}"
                )
            if (
                resp.error_code == _NOT_CONTROLLER
                and redirect_attempt + 1 < max_attempts
                and self._redirect_to_controller(None)
            ):
                redirect_attempt += 1
                continue
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "delete_offsets")
            return resp.deleted_count


    def describe_configs(self, topic: str) -> DescribeConfigsResult:
        """Describe topic configuration (native opcode 40/41).

        Topic configs only (not Kafka DescribeConfigs / BROKER). Empty
        values mean the key is unset. Non-zero ``error_code`` raises
        :class:`BrokerError` with ``op="describe_configs"``. Error 14
        follows ``max_redirects``.
        """
        payload = codec.encode_describe_configs_request(
            DescribeConfigsRequest(topic=topic)
        )
        resp = self._admin_round_trip(
            codec.OP_DESCRIBE_CONFIGS,
            payload,
            DescribeConfigsResponse,
            "describe_configs",
        )
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
        ``op="alter_configs"``. Error 14 follows ``max_redirects``.
        """
        payload = codec.encode_alter_configs_request(
            AlterConfigsRequest(topic=topic, configs=list(configs or []))
        )
        self._admin_round_trip(
            codec.OP_ALTER_CONFIGS,
            payload,
            AlterConfigsResponse,
            "alter_configs",
        )


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
        :class:`BrokerError`. Error 13 follows Produce/Fetch redirect
        (``max_redirects``); 13 is not a transient retry. Transient
        6 / 7 / 15 / 16 and TCP/IO follow ``max_retries`` (default 0).
        This is not Kafka DeleteRecords (API key 21).
        """
        payload = codec.encode_delete_records_request(
            DeleteRecordsRequest(
                topic=topic,
                partition=partition,
                before_offset=before_offset,
                wait_majority=wait_majority,
            )
        )
        max_retries = max(0, int(self.max_retries))
        max_attempts = 1 + self.max_redirects
        retry_attempt = 0
        redirect_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_DELETE_RECORDS, payload)
            except BrokerError as e:
                if (
                    e.code == _NOT_LEADER
                    and redirect_attempt + 1 < max_attempts
                    and self._redirect_to_leader(topic, partition)
                ):
                    redirect_attempt += 1
                    continue
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, DeleteRecordsResponse):
                raise ProtocolError(
                    f"unexpected response for delete_records: {type(resp)}"
                )
            if (
                resp.error_code == _NOT_LEADER
                and redirect_attempt + 1 < max_attempts
                and self._redirect_to_leader(resp.topic or topic, resp.partition)
            ):
                redirect_attempt += 1
                continue
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
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
        (same as the CLI). Transient broker/transport errors retry up to
        ``max_retries`` extra times (default 0). Error 14 follows
        ``max_redirects``.
        """
        payload = codec.encode_offset_fetch_request(
            OffsetFetchRequest(group_id=group, entries=[])
        )
        max_retries = max(0, int(self.max_retries))
        max_attempts = 1 + self.max_redirects
        retry_attempt = 0
        redirect_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_OFFSET_FETCH, payload)
            except BrokerError as e:
                if (
                    e.code == _NOT_CONTROLLER
                    and redirect_attempt + 1 < max_attempts
                    and self._redirect_to_controller(_controller_id_hint(e.message))
                ):
                    redirect_attempt += 1
                    continue
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, OffsetFetchResponse):
                raise ProtocolError(
                    f"unexpected response for offset_fetch: {type(resp)}"
                )
            if (
                resp.error_code == _NOT_CONTROLLER
                and redirect_attempt + 1 < max_attempts
                and self._redirect_to_controller(None)
            ):
                redirect_attempt += 1
                continue
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
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
        """Heartbeat for group membership. Returns the broker error_code.

        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Rebalance codes 9 / 10 / 11 are not
        retried.
        """
        payload = codec.encode_heartbeat_request(
            HeartbeatRequest(
                group_id=group, member_id=member_id, generation=generation
            )
        )
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_HEARTBEAT, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, HeartbeatResponse):
                raise ProtocolError(
                    f"unexpected response for heartbeat: {type(resp)}"
                )
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "heartbeat")
            return resp.error_code

    def leave_group(self, group: str, member_id: str) -> None:
        """Leave a consumer group.

        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Error 10 (UnknownMemberId) is success
        (already left). Rebalance 9 / IllegalGeneration 11 / 13 / 14 /
        not-found 2 are not retried.
        """
        payload = codec.encode_leave_group_request(
            LeaveGroupRequest(group_id=group, member_id=member_id)
        )
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_LEAVE_GROUP, payload)
            except BrokerError as e:
                if e.code == 10:
                    return
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, LeaveGroupResponse):
                raise ProtocolError(
                    f"unexpected response for leave_group: {type(resp)}"
                )
            if resp.error_code == 10:
                return
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "leave_group")
            return

    def describe_group(self, group: str) -> DescribeGroupResult:
        """Describe a live consumer group (native opcode 34/35).

        Error 2 (NotFound, no live members) raises :class:`BrokerError`.
        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Error 2 / 9 / 10 / 11 / 13 / 14 are
        not retried.
        """
        payload = codec.encode_describe_group_request(
            DescribeGroupRequest(group_id=group)
        )
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_DESCRIBE_GROUP, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, DescribeGroupResponse):
                raise ProtocolError(
                    f"unexpected response for describe_group: {type(resp)}"
                )
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "describe_group")
            return DescribeGroupResult(
                group_id=resp.group_id,
                generation=resp.generation,
                members=list(resp.members),
            )

    def list_groups(self) -> list[GroupListing]:
        """List known consumer groups (native opcode 36/37).

        Transient broker/transport errors retry up to ``max_retries``
        extra times (default 0). Error 2 / 9 / 10 / 11 / 13 / 14 are
        not retried.
        """
        payload = codec.encode_list_groups_request()
        max_retries = max(0, int(self.max_retries))
        retry_attempt = 0
        while True:
            try:
                resp = self._round_trip(codec.OP_LIST_GROUPS, payload)
            except BrokerError as e:
                if _is_transient_broker(e.code) and retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            except OSError:
                if retry_attempt < max_retries:
                    retry_attempt += 1
                    self._sleep_produce_retry()
                    continue
                raise
            if not isinstance(resp, ListGroupsResponse):
                raise ProtocolError(
                    f"unexpected response for list_groups: {type(resp)}"
                )
            if (
                _is_transient_broker(resp.error_code)
                and retry_attempt < max_retries
            ):
                retry_attempt += 1
                self._sleep_produce_retry()
                continue
            self._check(resp.error_code, "list_groups")
            return list(resp.groups)

    def create_scram_user(
        self, username: str, password: str, iterations: int = 0
    ) -> None:
        """Create or replace a SCRAM user (native opcode 64/65).

        ``iterations=0`` means the broker default (4096). Password is sent
        in the clear (use TLS). This is not the v0.46 handshake (60–63).
        Error 14 follows ``max_redirects``.
        """
        payload = codec.encode_create_scram_user_request(
            codec.CreateScramUserRequest(
                username=username, password=password, iterations=iterations
            )
        )
        self._admin_round_trip(
            codec.OP_CREATE_SCRAM_USER,
            payload,
            codec.CreateScramUserResponse,
            "create_scram_user",
        )

    def delete_scram_user(self, username: str) -> None:
        """Delete a SCRAM user (native opcode 66/67).

        Error 14 follows ``max_redirects``.
        """
        payload = codec.encode_delete_scram_user_request(
            codec.DeleteScramUserRequest(username=username)
        )
        self._admin_round_trip(
            codec.OP_DELETE_SCRAM_USER,
            payload,
            codec.DeleteScramUserResponse,
            "delete_scram_user",
        )

    def list_scram_users(self) -> list[str]:
        """List SCRAM usernames (native opcode 68/69).

        Error 14 follows ``max_redirects``.
        """
        resp = self._admin_round_trip(
            codec.OP_LIST_SCRAM_USERS,
            codec.encode_list_scram_users_request(),
            codec.ListScramUsersResponse,
            "list_scram_users",
        )
        return list(resp.usernames)

    def create_acls(self, entries: list[codec.AclBinding]) -> None:
        """Create ACL bindings (native opcode 54/55).

        Enables enforcement on the broker. This is not Kafka CreateAcls
        (API key 30). Non-zero ``error_code`` raises :class:`BrokerError`
        with ``op="create_acls"``. Error 14 follows ``max_redirects``.
        """
        payload = codec.encode_create_acls_request(
            codec.CreateAclsRequest(entries=list(entries or []))
        )
        self._admin_round_trip(
            codec.OP_CREATE_ACLS, payload, codec.CreateAclsResponse, "create_acls"
        )

    def delete_acls(self, entries: list[codec.AclBinding]) -> int:
        """Delete exact-matching ACL bindings (native opcode 56/57).

        Returns the number of entries removed. No filter-delete. This is
        not Kafka DeleteAcls (API key 31). Non-zero ``error_code`` raises
        :class:`BrokerError` with ``op="delete_acls"``. Error 14 follows
        ``max_redirects``.
        """
        payload = codec.encode_delete_acls_request(
            codec.DeleteAclsRequest(entries=list(entries or []))
        )
        resp = self._admin_round_trip(
            codec.OP_DELETE_ACLS, payload, codec.DeleteAclsResponse, "delete_acls"
        )
        return resp.removed

    def list_acls(
        self,
        principal: str = "",
        resource_type: int = 255,
        resource: str = "",
    ) -> list[codec.AclBinding]:
        """List ACL bindings with optional filters (native opcode 58/59).

        Empty ``principal`` / ``resource`` = any. ``resource_type=255`` =
        any type. This is not Kafka DescribeAcls (API key 29). Non-zero
        ``error_code`` raises :class:`BrokerError` with ``op="list_acls"``.
        Error 14 follows ``max_redirects``.
        """
        payload = codec.encode_list_acls_request(
            codec.ListAclsRequest(
                principal=principal, resource_type=resource_type, resource=resource
            )
        )
        resp = self._admin_round_trip(
            codec.OP_LIST_ACLS, payload, codec.ListAclsResponse, "list_acls"
        )
        return list(resp.entries)


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
    "MetadataResponse",
    "OffsetListing",
    "ProduceMessage",
    "ProduceResult",
    "ProtocolError",
    "TopicInfo",
    "wrap_tls",
]
