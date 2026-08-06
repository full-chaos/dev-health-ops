#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --web-root /absolute/path/to/dev-health-web" >&2
}

if [[ "${1:-}" != "--web-root" || -z "${2:-}" || "$#" -ne 2 ]]; then
  usage
  exit 64
fi

# CHAOS-3219 Phase 1 env-hardening (Phase 0 finding): `docker compose`
# interpolates every `${VAR}` in compose.yml / the acceptance overlay(s)
# against THIS PROCESS's environment, not just an explicit --env-file. A
# direnv-loaded ops/.env commonly exports POSTGRES_DB=devhealth (plus a real
# STRIPE_SECRET_KEY, SETTINGS_ENCRYPTION_KEY, SENTRY_DSN, ...) for local dev
# -- reproduced live: `docker compose ... config` for the `migrate` service
# resolves `POSTGRES_URI: .../${POSTGRES_DB:-postgres}` to
# `.../devhealth` whenever a direnv-active shell invokes this launcher,
# even though this stack's own postgres hardcodes POSTGRES_DB=postgres
# (compose.yml) and never creates a "devhealth" database -- `dev-hops
# migrate postgres` then fails outright against a database that was never
# provisioned. Unsetting only the specific known offenders (e.g. just
# POSTGRES_DB) would leave every other `${VAR}` in compose.yml / the
# acceptance overlays exposed to the same class of bug the next time
# someone's local .env grows a new name that happens to collide. Instead,
# unset every variable interpolated anywhere in compose.yml or the
# acceptance overlays that this launcher does not itself deliberately set
# below -- so the stack only ever sees compose-file defaults or this
# launcher's own explicit exports, never whatever happens to be exported in
# the invoking shell. test_launcher_hardens_compose_interpolation_env (see
# tests/acceptance/test_ask_dev_compose.py) asserts this list is a superset
# of every `${VAR}` reference in both compose files, minus this launcher's
# own deliberate exports -- so a future `${NEW_VAR}` added to either compose
# file fails that test until it is triaged into this list or into an
# explicit launcher export.
unset \
  POSTGRES_DB CLICKHOUSE_DB LOG_LEVEL \
  MIGRATION_DATABASE_URI MIGRATION_DATABASE_URI_FILE \
  RIVER_DATABASE_SCHEMA \
  RIVER_DOMAIN_DATABASE_ROLE RIVER_DOMAIN_DATABASE_PASSWORD \
  RIVER_QUEUE_DATABASE_ROLE RIVER_QUEUE_DATABASE_PASSWORD \
  RIVER_COORDINATOR_DATABASE_ROLE RIVER_COORDINATOR_DATABASE_PASSWORD \
  SETTINGS_ENCRYPTION_KEY \
  STRIPE_SECRET_KEY STRIPE_WEBHOOK_SECRET STRIPE_PRICE_ID_TEAM STRIPE_PRICE_ID_ENTERPRISE \
  LICENSE_PRIVATE_KEY \
  EMAIL_PROVIDER EMAIL_API_KEY EMAIL_FROM_ADDRESS \
  OTEL_ENABLED OTEL_EXPORTER_OTLP_ENDPOINT OTEL_SERVICE_NAME OTEL_METRIC_EXPORT_INTERVAL \
  SENTRY_DSN SENTRY_ENVIRONMENT SENTRY_TRACES_RATE SENTRY_SEND_PII \
  WORKER_CONCURRENCY WORKER_HEAVY_CONCURRENCY \
  WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED WORKER_GITHUB_REPO_METADATA_ENABLED \
  DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER \
  BUGSINK_BASE_URL BUGSINK_CREATE_SUPERUSER

web_root="$(cd -- "$2" && pwd)"
if [[ ! -f "${web_root}/Dockerfile" || ! -f "${web_root}/package.json" ]]; then
  echo "--web-root must identify a dev-health-web checkout" >&2
  exit 64
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ops_root="$(cd -- "${script_dir}/../.." && pwd)"
compose_file="${ops_root}/compose.yml"
acceptance_compose_file="${ops_root}/tests/acceptance/compose.ask-dev.yml"
acceptance_acr_compose_file="${ops_root}/tests/acceptance/compose.ask-dev-acr.yml"
project_name="dev-health-ask-dev-acceptance"
fixture_org_id="0a155cab-8833-42ac-a4ef-0d121725a7b0"
oracle_file="${ops_root}/tests/acceptance/ask-dev-oracle.v1.json"
# CHAOS-3219 D3: ACR (Agent Context Runtime) evidence-adapter services are
# OFF by default -- arm with ASK_DEV_ACCEPTANCE_ACR=1. See
# tests/acceptance/compose.ask-dev-acr.yml for the wiring rationale and the
# sibling dev-health checkout it requires (ASK_DEV_ACCEPTANCE_PARENT_COMPOSE).
# Normalized and exported (not just a local var) -- ScenarioRecorder.write
# (acceptance_artifact.py) reads this in every smoke script's own process to
# record acr_armed in its execution artifact, so ACR-backed case evidence is
# provably from an ACR-armed run rather than one that merely happened to
# pass while ACR was off (Codex finding, MEDIUM, 2026-08-05).
acr_armed="${ASK_DEV_ACCEPTANCE_ACR:-0}"
export ASK_DEV_ACCEPTANCE_ACR="${acr_armed}"
# CHAOS-3219 Phase 1: where downstream corpus lanes read the second-org /
# disabled-entitlement-org ids this run provisioned (see
# provision_multi_org() in prepare_ask_dev_acceptance.py). Not committed
# repo state -- a fresh runtime artifact per acceptance run, same lifecycle
# as the containers themselves.
org_ids_output="${ASK_DEV_ACCEPTANCE_ORG_IDS_OUTPUT:-/tmp/ask-dev-acceptance-org-ids.json}"
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
# 18080 collides with the normal dev-health stack's ACR API, which means a
# developer with that stack up cannot run acceptance at all. Overridable so
# both can coexist; the default preserves existing behaviour exactly.
export ASK_DEV_ACCEPTANCE_API_PORT="${ASK_DEV_ACCEPTANCE_API_PORT:-18080}"
acceptance_api_url="http://127.0.0.1:${ASK_DEV_ACCEPTANCE_API_PORT}"
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-ask-dev-acceptance-unused}"

compose=(
  docker compose
  --project-name "${project_name}"
  --project-directory "${ops_root}"
  -f "${compose_file}"
  -f "${acceptance_compose_file}"
  --profile ask-dev-acceptance
)
# CHAOS-3463: split in two. `world-restore` checks that every table it is
# about to write is empty and then writes it; a running `worker`/`beat` fleet
# is a concurrent writer racing that check (beat dispatches monitor-queue-depths
# every 60s from the moment it starts). The jobs fleet is therefore brought up
# AFTER the restore -- it is still a required part of the stack, just not while
# a state-based precondition is being evaluated. Codex adversarial review
# (HIGH): the original single list started them before the restore.
boot_services=(postgres pgbouncer clickhouse valkey migrate ask-dev-scripted-openai api)
jobs_services=(worker beat)
log_services=(api ask-dev-scripted-openai worker beat web)

if [[ "${acr_armed}" == "1" ]]; then
  # See resolve_acr_parent_compose.sh for the worktree-layout bug this
  # closes and why it is a separate, independently-tested script.
  # shellcheck source=resolve_acr_parent_compose.sh
  source "${script_dir}/resolve_acr_parent_compose.sh"
  resolve_acr_parent_compose "${ops_root}"

  compose+=(-f "${acceptance_acr_compose_file}" --profile ask-dev-acceptance-acr)
  boot_services+=(acr-db-init acr-migrate acr-api)
  log_services+=(acr-api)
  # tests/acceptance/compose.ask-dev-acr.yml: the extended acr-db-init /
  # acr-migrate services interpolate ${POSTGRES_USER:-devhealth} /
  # ${POSTGRES_PASSWORD:-devhealth} straight from the parent repo's
  # compose.yml (acr-db-init's entrypoint embeds them in its `sh -c` string,
  # which Compose interpolates at config/up time same as any other ${VAR}).
  # THIS project's postgres hardcodes user/db "postgres" (ops/compose.yml),
  # not "devhealth" -- export the matching credentials here, scoped to only
  # the ACR-armed path so the non-ACR path stays untouched by yet another
  # interpolated var.
  export POSTGRES_USER=postgres
  export POSTGRES_PASSWORD=postgres
fi

report_failure() {
  status=$?
  if [[ "${status}" -ne 0 ]]; then
    echo "Ask Dev Compose acceptance failed; retained containers and recent logs follow." >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=200 "${log_services[@]}" >&2 || true
  fi
  exit "${status}"
}
trap report_failure EXIT

"${compose[@]}" config --quiet

# This project name and its container names are acceptance-only. Resetting its
# volumes makes the canonical fixture and known test credential deterministic
# without touching a normal dev-health-ops Compose project.
"${compose[@]}" down --volumes --remove-orphans
"${compose[@]}" up -d --build --wait "${boot_services[@]}"

# CHAOS-3463 (Phase 2 exit blockers B2 + B3): seed the pinned
# ask-dev-world.v1 into the databases this stack's API actually serves.
#
# It is a RESTORE of a snapshot, not a generation, and that is not an
# implementation detail -- it is the only thing that can work. `fixtures
# world` refuses `default`/`postgres` by name (`_require_scratch_database`,
# unchanged and not weakened here), and even if it did not, per-boot
# regeneration could never match the pinned WORLD_DIGEST: cross-generation
# digest reproducibility is declared-blocked (world.json's
# `cross_generation_digest_status`, CHAOS-3432). So the world is generated
# exactly ONCE by scripts/acceptance/mint_ask_dev_world_snapshot.sh and
# every boot restores those same bytes, which is the single-generation
# regime the digest pin is actually proven for.
#
# Ordering is load-bearing: this runs immediately after `migrate` and BEFORE
# `fixtures generate` / prepare_ask_dev_acceptance.py, because the restore
# refuses to write unless every table it carries is still empty -- the
# precondition that makes it impossible to run against a real database.
#
# It exits non-zero (and, under `set -e`, aborts the whole acceptance run)
# if the restored world's digest does not match the pin. A stack that cannot
# serve the pinned world must fail to come up, not quietly serve a different
# one and let the corpus mint receipts against it (ruling D2).
"${compose[@]}" exec -T api dev-hops fixtures world-restore \
  --manifest /app/tests/acceptance/world/ask-dev-world.v1/world.json \
  --sink clickhouse://ch:ch@clickhouse:8123/default \
  --postgres-uri postgresql+asyncpg://postgres:postgres@postgres:5432/postgres \
  --snapshot /app/tests/acceptance/world/ask-dev-world.v1/snapshot

# CHAOS-3463 / Codex adversarial review round 3 (HIGH, confirmed): prove on
# EVERY boot -- not only in the one-off mint -- that the world's principals can
# actually authenticate.
#
# The digest guard above is not a substitute, and the difference is the whole
# point of this step. WORLD_DIGEST covers `password_hash`, so a verified digest
# proves the credential BYTES restored identically to the ones the mint proved
# a login against. It cannot prove the API still ACCEPTS them: a login-path
# regression, a bcrypt cost/policy change, or an auth-service refactor would
# leave every restored byte identical and the digest green while no world
# principal can log in. The corpus then runs as the superuser, and its
# cross-tenant and entitlement cases quietly stop testing what they claim to.
#
# Runs against the stack's public API from the host, with a wrong-password
# negative control inside the script -- an API that accepted anything would
# otherwise satisfy every positive check here. Non-zero aborts the run under
# `set -e`, exactly like the digest guard: a stack that cannot authenticate the
# world it just restored must fail to come up.
"${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/assert_world_principals_can_log_in.py" \
  --api-url "${acceptance_api_url}" \
  --manifest "${ops_root}/tests/acceptance/world/ask-dev-world.v1/world.json"

# --overwrite-real-users (CHAOS-3463): the world restore above legitimately
# populates orgs/users first, which trips `_seed_auth_data`'s CHAOS-2458 guard
# ("refuse to seed fixture auth data into a non-empty auth database"). Without
# this opt-in the known-credential superuser admin@devhealth.example is never
# created and prepare_ask_dev_acceptance.py fails its login with HTTP 401
# (observed live). The two seedings do not overlap -- the world's users are
# @ask-dev-world-*.example in the world's own orgs, this one seeds
# admin@/onboarding@devhealth.example in ${fixture_org_id} -- so the restored
# world's rows, and therefore WORLD_DIGEST, are untouched.
# The jobs fleet starts only now: see boot_services/jobs_services above.
"${compose[@]}" up -d --build --wait "${jobs_services[@]}"

"${compose[@]}" exec -T api dev-hops fixtures generate \
  --sink clickhouse://ch:ch@clickhouse:8123/default \
  --overwrite-real-users \
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

# prepare_ask_dev_acceptance.py also provisions + verifies the second org
# (ask_dev entitlement enabled) and the disabled-entitlement org
# (provision_multi_org(); CHAOS-3219 Wave 4) and writes their ids to
# org_ids_output for downstream corpus lanes.
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
ASK_DEV_ACCEPTANCE_ORG_IDS_OUTPUT="${org_ids_output}" \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/prepare_ask_dev_acceptance.py"

# CHAOS-3300: the "Ask Dev" not-found original defect reproduction, proven
# through the real HTTP/SSE API surface (no Playwright/web needed for this
# one -- it never reaches the web UI at all). PYTHONPATH must include
# ops_root itself (not just src/) so `scripts.acceptance.*` imports resolve
# the same way run_ask_dev_provider_profile.sh's smoke invocation does.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_not_found.py"

# CHAOS-3300: the "Ask Dev" exact-commit original defect reproduction --
# the positive control the not-found negative control needs to hold against.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_exact_commit.py"

# CHAOS-3300: organization-wide DATA_TRUST and REMAINING_WORK questions,
# proven the same way -- real HTTP/SSE API, no web/Playwright.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_core_intents.py"

# CHAOS-3300: re-verify the inherited positive-control oracle over the same
# real HTTP/SSE surface (ops-side substance; the Playwright leg below still
# proves web/window equivalence separately).
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_inherited_oracle.py"

# CHAOS-3300: organization-wide multi-metric comparison (metric.comparison.v1).
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_metric_comparison.py"

# CHAOS-3300/CHAOS-3332: the team-attribution attack, re-verified on the
# fixed code -- a named TEAM subject now completes as a real (degraded but
# honest) answer instead of crashing to internal_error.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_team_attribution.py"

# CHAOS-3300/CHAOS-3297 stack-3: the four newly-wired health/workload/
# deficiency intents, plus the deliberately-unwired PORTFOLIO_STATUS
# fallback proof.
PYTHONPATH="${ops_root}/src:${ops_root}" \
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/smoke_ask_dev_stack3_intents.py"
# CHAOS-3337 shipped (ops #1402): this no longer needs `|| true` -- all four
# scenarios are expected to pass, and a hard failure here should now abort
# the launcher like every other scenario, not be silently swallowed.

"${compose[@]}" up -d --build --wait web

ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_COMPOSE_WEB_READY=1 \
ASK_DEV_ACCEPTANCE_WEB_URL=http://127.0.0.1:3002 \
ASK_DEV_ACCEPTANCE_QUESTION="${acceptance_question}" \
ASK_DEV_ACCEPTANCE_EXPECTED_METRIC_ID="${expected_metric_id}" \
ASK_DEV_ACCEPTANCE_EXPECTED_EVIDENCE_FRAGMENT="${expected_evidence_entity_fragment}" \
ASK_DEV_ACCEPTANCE_EXPECTED_CLAIM_KIND="${expected_claim_kind}" \
PLAYWRIGHT_LIVE_BACKEND_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${web_root}/node_modules/.bin/playwright" test \
  -c "${web_root}/playwright.ask-dev-acceptance.config.ts"

"${compose[@]}" down --volumes --remove-orphans
trap - EXIT
echo "Ask Dev Compose acceptance completed successfully."
