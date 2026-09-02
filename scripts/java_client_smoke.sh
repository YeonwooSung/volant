#!/usr/bin/env bash
# v0.23 — run Java native-client unit tests when mvn is present.
#
# Does not require a broker. E2E stays gated on VOLANT_E2E=1 (not set here).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/clients/java"

if ! command -v mvn >/dev/null 2>&1; then
  echo "mvn not found; skip Java client smoke" >&2
  exit 0
fi

echo "==> mvn -q test (clients/java)"
mvn -q test
