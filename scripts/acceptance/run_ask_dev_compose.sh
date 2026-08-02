#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --web-root /absolute/path/to/dev-health-web" >&2
}

if [[ "${1:-}" != "--web-root" || -z "${2:-}" || "$#" -ne 2 ]]; then
  usage
  exit 64
fi

web_root="$(cd -- "$2" && pwd)"
if [[ ! -f "${web_root}/Dockerfile" || ! -f "${web_root}/package.json" ]]; then
  echo "--web-root must identify a dev-health-web checkout" >&2
  exit 64
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ops_root="$(cd -- "${script_dir}/../.." && pwd)"
compose_file="${ops_root}/compose.yml"
acceptance_compose_file="${ops_root}/tests/acceptance/compose.ask-dev.yml"
project_name="dev-health-ask-dev-acceptance"
fixture_org_id="0a155cab-8833-42ac-a4ef-0d121725a7b0"
oracle_file="${ops_root}/tests/acceptance/ask-dev-oracle.v1.json"
read_oracle_field() {
  "${ops_root}/.venv/bin/python" -c \
    'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))[sys.argv[2]])' \
    "${oracle_file}" "$1"
}
acceptance_question="$(read_oracle_field question)"
expected_metric_id="$(read_oracle_field expected_metric_id)"
expected_evidence_entity_fragment="$(read_oracle_field expected_evidence_entity_fragment)"
expected_claim_kind="$(read_oracle_field expected_claim_kind)"

export ASK_DEV_WEB_CONTEXT="${web_root}"
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-ask-dev-acceptance-unused}"

compose=(
  docker compose
  --project-name "${project_name}"
  --project-directory "${ops_root}"
  -f "${compose_file}"
  -f "${acceptance_compose_file}"
  --profile ask-dev-acceptance
)

report_failure() {
  status=$?
  if [[ "${status}" -ne 0 ]]; then
    echo "Ask Dev Compose acceptance failed; retained containers and recent logs follow." >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=200 api ask-dev-scripted-openai web >&2 || true
  fi
  exit "${status}"
}
trap report_failure EXIT

"${compose[@]}" config --quiet

# This project name and its container names are acceptance-only. Resetting its
# volumes makes the canonical fixture and known test credential deterministic
# without touching a normal dev-health-ops Compose project.
"${compose[@]}" down --volumes --remove-orphans
"${compose[@]}" up -d --build --wait \
  postgres pgbouncer clickhouse valkey migrate ask-dev-scripted-openai api

"${compose[@]}" exec -T api dev-hops fixtures generate \
  --sink clickhouse://ch:ch@clickhouse:8123/default \
  --org "${fixture_org_id}" \
  --repo-name meridian/web-app \
  --repo-count 1 \
  --days 28 \
  --commits-per-day 2 \
  --pr-count 10 \
  --team-count 3 \
  --seed 3200 \
  --with-metrics \
  --with-work-graph

ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL=http://127.0.0.1:18080 \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/prepare_ask_dev_acceptance.py"

# CHAOS-3300: the "Ask Dev" not-found original defect reproduction, proven
# through the real HTTP/SSE API surface (no Playwright/web needed for this
# one -- it never reaches the web UI at all). PYTHONPATH must include
# ops_root itself (not just src/) so `scripts.acceptance.*` imports resolve
# the same way run_ask_dev_provider_profile.sh's smoke invocation does.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL=http://127.0.0.1:18080 \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_not_found.py"

# CHAOS-3300: the "Ask Dev" exact-commit original defect reproduction --
# the positive control the not-found negative control needs to hold against.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL=http://127.0.0.1:18080 \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_exact_commit.py"

# CHAOS-3300: organization-wide DATA_TRUST and REMAINING_WORK questions,
# proven the same way -- real HTTP/SSE API, no web/Playwright.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL=http://127.0.0.1:18080 \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_core_intents.py"

"${compose[@]}" up -d --build --wait web

ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_COMPOSE_WEB_READY=1 \
ASK_DEV_ACCEPTANCE_WEB_URL=http://127.0.0.1:3002 \
ASK_DEV_ACCEPTANCE_QUESTION="${acceptance_question}" \
ASK_DEV_ACCEPTANCE_EXPECTED_METRIC_ID="${expected_metric_id}" \
ASK_DEV_ACCEPTANCE_EXPECTED_EVIDENCE_FRAGMENT="${expected_evidence_entity_fragment}" \
ASK_DEV_ACCEPTANCE_EXPECTED_CLAIM_KIND="${expected_claim_kind}" \
PLAYWRIGHT_LIVE_BACKEND_URL=http://127.0.0.1:18080 \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${web_root}/node_modules/.bin/playwright" test \
  -c "${web_root}/playwright.ask-dev-acceptance.config.ts"

"${compose[@]}" down --volumes --remove-orphans
trap - EXIT
echo "Ask Dev Compose acceptance completed successfully."
