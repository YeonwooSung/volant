#!/usr/bin/env bash
# v0.14 — run Python native-client unit tests when python3 is present.
#
# Does not require a broker. E2E stays gated on VOLANT_E2E=1 (not set here).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/clients/python"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found; skip Python client smoke" >&2
  exit 0
fi

if python3 -c "import pytest" >/dev/null 2>&1; then
  echo "==> python3 -m pytest -q (clients/python)"
  PYTHONPATH=src python3 -m pytest -q
else
  echo "==> python3 -m unittest (pytest not installed)"
  PYTHONPATH=src python3 -m unittest discover -s tests -q
fi
