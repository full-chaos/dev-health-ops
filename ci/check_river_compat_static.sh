#!/usr/bin/env bash
# Validate the River compatibility harness without starting its services.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
HARNESS="${ROOT}/tests/compatibility/river/run.sh"
RECORDER="${ROOT}/tests/compatibility/river/record.sh"
COMPOSE_FILE="${ROOT}/tests/compatibility/river/compose.compatibility.yml"
RESULTS="${ROOT}/docs/architecture/evidence/go-worker-migration/v1-river-spike/local-harness-results.json"

for command_name in bash docker jq shellcheck; do
  command -v "${command_name}" >/dev/null 2>&1 || {
    printf 'ERROR: %s is required\n' "${command_name}" >&2
    exit 2
  }
done
docker compose version >/dev/null 2>&1 || {
  printf 'ERROR: Docker Compose v2 is required\n' >&2
  exit 2
}

bash -n "${HARNESS}"
bash -n "${RECORDER}"
shellcheck "${HARNESS}" "${RECORDER}" "${ROOT}/ci/check_go.sh" "${BASH_SOURCE[0]}"
docker compose \
  --project-name rivercompat-static-check \
  --file "${COMPOSE_FILE}" \
  config --quiet
jq empty \
  "${ROOT}/docs/architecture/evidence/go-worker-migration/v0-celery-baseline/capture.json" \
  "${ROOT}/docs/architecture/evidence/go-worker-migration/v0-celery-baseline/local-resource-snapshot.json" \
  "${ROOT}/docs/architecture/evidence/go-worker-migration/v1-river-spike/compatibility-matrix.json" \
  "${RESULTS}"
jq -e '
  .schema_version == 1
  and .status == "complete_with_architecture_blocker"
  and .architecture_blocker == "poll_only_running_cancel_not_propagated"
  and (.profiles | type) == "array"
  and (.profiles | length) == 2
  and .nested_n_minus_1.status == "pass"
  and (.nested_n_minus_1.phases | length) == 2
  and .versions.go == "go1.25.9"
  and .versions.river == "v0.40.0"
  and .versions.river_driver == "v0.40.0"
  and .versions.pgx == "v5.10.0"
  and .versions.river_n_minus_1 == "v0.39.0"
  and .versions.river_driver_n_minus_1 == "v0.39.0"
  and .versions.pgx_n_minus_1 == "v5.9.2"
  and .versions.python == "3.13.14"
  and .versions.riverqueue_python == "0.7.0"
  and .versions.sqlalchemy == "2.0.49"
  and .versions.asyncpg == "0.31.0"
  and all(.profiles[]; .python_transactions.scheduled_commit.job_contract.state == "scheduled")
  and .nested_n_minus_1.phases[1].current_insert.outcome == "inserted"
  and .nested_n_minus_1.phases[1].n_minus_one_consume.outcome == "completed"
  and .redaction.contains_credentials_or_dsns == false
' "${RESULTS}" >/dev/null

printf 'River compatibility static checks: clean\n'
