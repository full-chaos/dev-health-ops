#!/usr/bin/env bash
# Compare sanitized normalized provider models across Python and Go.
set -euo pipefail

umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd -P)"
FIXTURE="${REPO_ROOT}/internal/providerfoundation/testdata/normalized_envelope_parity.json"
PYTHON_BIN="${PROVIDER_COMPAT_PYTHON:-${REPO_ROOT}/.venv/bin/python}"
TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/provider-compat.XXXXXX")"

cleanup() {
  rm -rf -- "${TEMP_DIR}"
}
trap cleanup EXIT HUP INT TERM

[ -x "${PYTHON_BIN}" ] || {
  printf 'provider compatibility harness: Python environment unavailable\n' >&2
  exit 1
}

cd "${REPO_ROOT}"
go run ./cmd/dev-health-provider-normalized-fixture -fixture "${FIXTURE}" >"${TEMP_DIR}/go.json"
"${PYTHON_BIN}" "${SCRIPT_DIR}/python_normalized_parity.py" "${FIXTURE}" >"${TEMP_DIR}/python.json"

if ! cmp -s "${TEMP_DIR}/go.json" "${TEMP_DIR}/python.json"; then
  printf 'provider compatibility harness: Go and Python normalized envelopes differ\n' >&2
  exit 1
fi

printf '{"schema_version":"v1","status":"ok","cases":7}\n'
