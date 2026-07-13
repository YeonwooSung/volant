# Phase 10 implementation review

## Scope delivered

| Item | Status |
|------|--------|
| InitProducerId 32/33 | Done |
| Produce PID/epoch/seq trailer | Done (legacy decode OK) |
| Broker de-dupe | Done (in-memory) |
| Error codes 19–21 | Done |
| Client idempotence + retries | Done |
| Lag metrics + CLI | Done |
| Tests | `phase10_idempotent_lag` + protocol unit |

## Verify

```bash
cargo test --workspace
cargo test -p volant-broker --test phase10_idempotent_lag
```

## Honest limitations

- Producer PID map is not durable across broker restart
- No transactional / multi-partition EOS
- Idempotent client pins null-key messages to partition 0 (no shared RR counter)
