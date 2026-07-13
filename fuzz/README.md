# Protocol fuzz targets (Phase 9)

Optional [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) harness for
`volant-protocol` decode paths. **Not** a workspace member — requires nightly.

## Prerequisites

```bash
rustup install nightly
cargo install cargo-fuzz
```

## Run

```bash
# From repository root
cargo +nightly fuzz run decode_frame
cargo +nightly fuzz run decode_request
```

## CI note

Workspace CI always runs deterministic chaos unit tests in
`volant-protocol` (`chaos_decode_does_not_panic`, `chaos_frame_decode_extended`)
without nightly. Full corpus CI is optional/deferred.
