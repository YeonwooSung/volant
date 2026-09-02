#!/usr/bin/env bash
# v0.19 — run Go native-client unit tests when go is present.
#
# Does not require a broker. E2E stays gated on VOLANT_E2E=1 (not set here).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/clients/go"

if ! command -v go >/dev/null 2>&1; then
  echo "go not found; skip Go client smoke" >&2
  exit 0
fi

echo "==> go test ./... (clients/go)"
go test ./...
