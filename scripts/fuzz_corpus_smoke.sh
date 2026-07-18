#!/usr/bin/env bash
# Phase 112 — local fuzz corpus smoke helpers.
#
# Default path (CI / stable): deterministic unit tests only (no nightly).
# Optional path: short cargo-fuzz runs when nightly + cargo-fuzz are installed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUNS="${FUZZ_SMOKE_RUNS:-200}"
MODE="${1:-test}"

usage() {
  cat <<USAGE
Usage: $0 [test|fuzz|all]

  test  Run deterministic corpus smoke via cargo test (default; CI path)
  fuzz  Run cargo +nightly fuzz with -runs=\$FUZZ_SMOKE_RUNS (default 200)
  all   test then fuzz

Env:
  FUZZ_SMOKE_RUNS  Iteration cap for cargo-fuzz smoke (default 200)
USAGE
}

run_test() {
  echo "==> deterministic corpus smoke (volant-protocol)"
  cargo test -p volant-protocol corpus_smoke -- --nocapture
}

run_fuzz() {
  if ! command -v cargo-fuzz >/dev/null 2>&1 && ! cargo fuzz --help >/dev/null 2>&1; then
    echo "cargo-fuzz not installed; skip fuzz path (install: cargo install cargo-fuzz)" >&2
    return 1
  fi
  if ! rustup run nightly rustc --version >/dev/null 2>&1; then
    echo "nightly toolchain missing; install: rustup install nightly" >&2
    return 1
  fi
  echo "==> cargo-fuzz decode_frame (-runs=${RUNS})"
  cargo +nightly fuzz run decode_frame -- -runs="${RUNS}"
  echo "==> cargo-fuzz decode_request (-runs=${RUNS})"
  cargo +nightly fuzz run decode_request -- -runs="${RUNS}"
}

case "$MODE" in
  test) run_test ;;
  fuzz) run_fuzz ;;
  all)  run_test; run_fuzz ;;
  -h|--help) usage ;;
  *) usage; exit 2 ;;
esac
