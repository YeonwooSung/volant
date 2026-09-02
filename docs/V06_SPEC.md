# v0.6 — Kafka DeleteRecords per-request wait tag (flex v2)

**Status:** Shipped (residual slice; not Phase 155)  
**Theme:** Close the Kafka-side gap left by Phase 137: native DeleteRecords
already has a per-request `wait_majority` trailer; the Kafka path was
env/broker only.

## Goals

1. **Flexible DeleteRecords v2 request-level tag 0:** parse the final
   TAG_BUFFER after `timeout_ms` instead of blindly skipping it.
   - Tag **0** body = single `u8` `wait_majority`
     - `0` → broker knob (`VOLANT_DELETE_RECORDS_WAIT_MAJORITY` /
       `Broker::delete_records_wait_majority()`)
     - `1` → force wait on
     - `2` → force wait off
   - Same merge as native: `Broker::effective_delete_records_wait_majority`.
2. **Absent / empty / unknown tags → 0** (broker knob). Unknown tag ids are
   skipped (body consumed, ignored).
3. **v0–1 unchanged:** no invented classic field; still env/broker only.
4. **Wait-on honesty (Phase 148):** majority **before** local truncate;
   miss → Kafka **19** (`NOT_ENOUGH_REPLICAS`), log start unchanged.
5. Tests + this spec. Crate version stays **0.2.0**.

## Non-goals

| Deferred | Why |
|----------|-----|
| Classic v0–1 wait field | Not in Kafka wire; do not invent one |
| New Kafka API key / max version | `SUPPORTED_APIS` frozen (38 keys; DeleteRecords 0–2) |
| Native trailer change | Phase 137 already shipped |
| librdkafka / Java client sending the tag | Volant extension; those clients will not emit tag 0 |
| RequestVote / InstallSnapshot / metadata Raft | Do not open Phase 155 |
| Wait-off local-first rollback | Segment delete is irreversible (Phase 148 residual) |

## Wire

Kafka DeleteRecords has **no** standard wait field. This is a **Volant tagged
field extension on flexible v2 only**.

```text
DeleteRecords Request v2 (flexible)
  topics compact[{ name, partitions compact[{ partition, offset, TAG_BUFFER }], TAG_BUFFER }]
  timeout_ms
  TAG_BUFFER                    ← request-level (after timeout_ms)
      tag 0: wait_majority u8   ← Volant; not a Kafka standard field
      other tags: skipped
```

- Empty TAG_BUFFER (`uvarint(0)`) = flag `0`.
- Tag 0 with empty body = flag `0`.
- Header TAG_BUFFER (RequestHeader v2) is still skipped; the wait tag lives on
  the **request body**, not the header.
- librdkafka, kafka-python, kcat, and the Java admin client will not send
  tag 0. Only Volant tests / custom clients do.

```text
  Kafka DeleteRecords
       v0–1  → flag 0 → broker AtomicBool
       v2    → request-level tag 0 (absent → 0)
                    │
                    0 → broker knob
                    1 → force true
                    2 → force false
                    ▼
              effective_wait?
                 │
     ┌───────────┴───────────┐
     no                      yes
     local-first             majority first (Phase 148)
     client err = local      miss → Kafka 19, no truncate
```

## Exit criteria

- [x] v2 no tag uses the broker knob (default wait-off succeeds without majority).
- [x] v2 tag 0 = 1 on N=2 with one dead peer → Kafka **19**, log start unchanged.
- [x] v2 tag 0 = 2 overrides a wait-on knob → local truncate (no majority required).
- [x] v0/v1 still uses the broker knob only.
- [x] Unknown tag id is ignored; request still decodes.
- [x] Native `phase137_delete_records_request_wait_flag` still passes.

## Honesty

- **Not a Kafka standard field.** Documented here and on the DeleteRecords row
  of [KAFKA_COMPAT.md](./KAFKA_COMPAT.md). Standard clients will keep the
  broker-knob behavior (flag 0).
- Wait-off (default, tag 2, or v0–1 with knob off) is still **local-first**;
  truncate is irreversible if majority later misses.
- Native opcode-44 trailer is unchanged.

## Related

- [PHASE137_SPEC.md](./PHASE137_SPEC.md) — native trailer
- [PHASE148_SPEC.md](./PHASE148_SPEC.md) — majority-first truncate
- [PHASE135_SPEC.md](./PHASE135_SPEC.md) — broker wait knob
