"""Transactional producer helper (v0.63).

Thin wrapper around the v0.57 Client BeginTxn / EndTxn RPCs. Matches
``crates/volant-client/src/txn.rs``. Native opcodes 50–53 only; not
Kafka transactions.
"""

from __future__ import annotations

from typing import Iterable, Optional, Union

from .client import Client, ProduceResult
from .codec import ProduceMessage, TxnOffsetCommit, TxnProduceResult


class TransactionalProducer:
    """begin → produce* / add_offsets* → commit or abort.

    ``add_offsets`` queues locally; nothing is sent until ``commit``.
    Produce is write-through (same as the Rust helper).
    """

    def __init__(self, client: Client) -> None:
        if client is None or not client.transactional_id:
            raise ValueError("transactional_id not configured")
        self._client = client
        self._pending: list[TxnOffsetCommit] = []
        self._open = False

    @property
    def client(self) -> Client:
        return self._client

    def begin(self) -> None:
        if self._open:
            raise ValueError("transaction already open")
        self._client.begin_transaction()
        self._pending.clear()
        self._open = True

    def produce(
        self,
        topic: str,
        partition: int,
        value: Optional[bytes] = None,
        *,
        key: Optional[bytes] = None,
        messages: Optional[Iterable[Union[bytes, ProduceMessage]]] = None,
        acks: Optional[int] = None,
        timestamp_ms: int = -1,
        headers: Optional[list[tuple[str, bytes]]] = None,
    ) -> ProduceResult:
        return self._client.produce(
            topic,
            partition,
            value=value,
            key=key,
            messages=messages,
            acks=acks,
            timestamp_ms=timestamp_ms,
            headers=headers,
        )

    def add_offsets(
        self,
        group_id: str,
        entries: Iterable[tuple[str, int, int]],
    ) -> None:
        """Queue group offsets to commit atomically with EndTxn.

        ``entries`` are ``(topic, partition, offset)`` triples. Nothing is
        sent until :meth:`commit`.
        """
        for topic, partition, offset in entries:
            self._pending.append(
                TxnOffsetCommit(
                    group_id=group_id,
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    metadata="",
                )
            )

    def commit(self) -> list[TxnProduceResult]:
        if not self._open:
            raise ValueError("transaction is not open")
        offsets = self._pending
        self._pending = []
        results = self._client.commit_transaction(offsets=offsets)
        self._open = False
        return results

    def abort(self) -> None:
        if not self._open:
            raise ValueError("transaction is not open")
        self._pending.clear()
        self._client.abort_transaction()
        self._open = False

    def is_open(self) -> bool:
        return self._open
