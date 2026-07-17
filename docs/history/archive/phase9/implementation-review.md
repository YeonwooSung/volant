# Phase 9 implementation review

## Scope delivered

| Item | Status |
|------|--------|
| Client webpki-roots + `tls_ca` | Done (`volant-client` feature `tls`) |
| Inter-broker TLS | Done (`volant-broker` feature `tls`, server flags) |
| Multi-node Helm | Done (`cluster.enabled` StatefulSet path) |
| Fuzz scaffold | Done (`fuzz/` + expanded chaos unit tests) |
| Docs | ROADMAP, ops.md, deploy README, PHASE9_SPEC |

## Verify

```bash
cargo test --workspace
cargo check -p volant-server --features tls
cargo check -p volant-client --features tls
cargo check -p volant-broker --features tls
# optional:
# cargo +nightly fuzz run decode_frame
```

## Flags (server, feature `tls`)

| Flag | Default | Notes |
|------|---------|-------|
| `--tls-cert` / `--tls-key` | unset | Enable TLS listen |
| `--tls-peer-insecure` | `true` | Lab self-signed inter-broker |
| `--tls-ca` | unset | CA for peer verify when not insecure |
| `--no-tls-inter-broker` | off | Force plaintext peers |

## Honest limitations

- No mTLS client identity mapping
- Metrics endpoint still unauthenticated
- cargo-fuzz not in CI (nightly optional only)
- Helm multi-node assumes image has `/bin/sh` (debian slim does)
