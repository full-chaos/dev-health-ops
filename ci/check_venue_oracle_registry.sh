#!/usr/bin/env bash
# Static venue-oracles registry check (CHAOS-6584, Trap #392): every venue test
# is in ci/venue_oracle_registry.tsv and every registry row names a real test.
# No Go toolchain, containers or Python: runs on every PR through
# tests/tooling/test_venue_oracle_registry.py. The hosted venue-oracles job
# (main + workflow_dispatch) runs the same check first, via ci/check_go.sh.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
# shellcheck source=ci/lib/venue_oracle_registry.sh
. "${ROOT}/ci/lib/venue_oracle_registry.sh"
[ "$#" -eq 0 ] || { printf 'check_venue_oracle_registry.sh accepts no arguments\n' >&2; exit 2; }
check_venue_oracle_registry
