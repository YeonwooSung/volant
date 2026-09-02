#!/usr/bin/env bash
# Phase 112 / v0.15 — local fuzz corpus smoke helpers.
#
# Default path (CI / stable): deterministic unit tests only (no nightly).
# Optional path: short cargo-fuzz runs when nightly + cargo-fuzz are installed.
# `long` is a capped local campaign — CI does **not** run it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUNS="${FUZZ_SMOKE_RUNS:-200}"
LONG_SECS="${FUZZ_LONG_SECS:-30}"
MODE="${1:-test}"
TARGETS=(decode_frame decode_request decode_extended)

usage() {
  cat <<USAGE
Usage: $0 [test|fuzz|long|all]

  test  Run deterministic corpus smoke via cargo test (default; CI path)
  fuzz  Run cargo +nightly fuzz with -runs=\$FUZZ_SMOKE_RUNS (default 200)
  long  Capped cargo +nightly fuzz: -max_total_time=\$FUZZ_LONG_SECS per target
        (default 30s). Not run on CI.
  all   test then fuzz

Env:
  FUZZ_SMOKE_RUNS  Iteration cap for cargo-fuzz smoke (default 200)
  FUZZ_LONG_SECS   Wall-clock cap per target for \`long\` (default 30)
USAGE
}

run_test() {
  echo "==> deterministic corpus smoke (volant-protocol)"
  cargo test -p volant-protocol corpus_smoke -- --nocapture
}

need_fuzz() {
  if ! command -v cargo-fuzz >/dev/null 2>&1 && ! cargo fuzz --help >/dev/null 2>&1; then
    echo "cargo-fuzz not installed; skip fuzz path (install: cargo install cargo-fuzz)" >&2
    return 1
  fi
  if ! rustup run nightly rustc --version >/dev/null 2>&1; then
    echo "nightly toolchain missing; install: rustup install nightly" >&2
    return 1
  fi
}

run_fuzz() {
  need_fuzz || return 1
  for t in "${TARGETS[@]}"; do
    echo "==> cargo-fuzz ${t} (-runs=${RUNS})"
    cargo +nightly fuzz run "${t}" -- -runs="${RUNS}"
  done
}

run_long() {
  need_fuzz || return 1
  echo "==> long fuzz campaign (${LONG_SECS}s per target; not CI)"
  for t in "${TARGETS[@]}"; do
    echo "==> cargo-fuzz ${t} (-max_total_time=${LONG_SECS})"
    cargo +nightly fuzz run "${t}" -- -max_total_time="${LONG_SECS}"
  done
}

case "$MODE" in
  test) run_test ;;
  fuzz) run_fuzz ;;
  long) run_long ;;
  all)  run_test; run_fuzz ;;
  -h|--help) usage ;;
  *) usage; exit 2 ;;
esac
