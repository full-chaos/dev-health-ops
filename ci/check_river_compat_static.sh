#!/usr/bin/env bash
# Validate the River compatibility harness without starting its services.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
HARNESS="${ROOT}/tests/compatibility/river/run.sh"
COMPOSE_FILE="${ROOT}/tests/compatibility/river/compose.compatibility.yml"

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
shellcheck "${HARNESS}" "${ROOT}/ci/check_go.sh" "${BASH_SOURCE[0]}"
docker compose \
  --project-name rivercompat-static-check \
  --file "${COMPOSE_FILE}" \
  config --quiet
jq empty \
  "${ROOT}/docs/architecture/evidence/go-worker-migration/v0-celery-baseline/capture.json" \
  "${ROOT}/docs/architecture/evidence/go-worker-migration/v0-celery-baseline/local-resource-snapshot.json" \
  "${ROOT}/docs/architecture/evidence/go-worker-migration/v1-river-spike/compatibility-matrix.json" \
  "${ROOT}/docs/architecture/evidence/go-worker-migration/v1-river-spike/local-harness-results.json"

printf 'River compatibility static checks: clean\n'
