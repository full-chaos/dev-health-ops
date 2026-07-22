#!/usr/bin/env bash
# Atomically refresh the committed sanitized evidence with the runner's stdout.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd -P)"
OUTPUT="${REPO_ROOT}/docs/architecture/evidence/go-worker-migration/v1-river-spike/local-harness-results.json"
TEMP_RESULT=""

cleanup() {
  local status=$?

  trap - EXIT HUP INT TERM
  if [ -n "${TEMP_RESULT}" ] && [ -f "${TEMP_RESULT}" ]; then
    rm -f -- "${TEMP_RESULT}"
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

TEMP_RESULT="$(mktemp "${OUTPUT}.tmp.XXXXXX")"
GOTOOLCHAIN=go1.25.9 "${SCRIPT_DIR}/run.sh" >"${TEMP_RESULT}"
jq -e '
  .schema_version == 1
  and .evidence_scope == "local_ephemeral_compatibility_harness"
  and .redaction.contains_raw_logs == false
  and .redaction.contains_credentials_or_dsns == false
  and .redaction.contains_job_payloads == false
  and .redaction.contains_dynamic_ports == false
  and .redaction.contains_container_or_project_ids == false
' "${TEMP_RESULT}" >/dev/null
mv -- "${TEMP_RESULT}" "${OUTPUT}"
TEMP_RESULT=""

printf 'Recorded sanitized River compatibility evidence at %s\n' "${OUTPUT}"
