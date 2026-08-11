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
  BUGSINK_BASE_URL BUGSINK_CREATE_SUPERUSER \
  ASK_DEV_QUA_SHADOW_ENABLED ASK_DEV_QUA_COMMIT_ENABLED

web_root="$(cd -- "$2" && pwd)"
if [[ ! -f "${web_root}/Dockerfile" || ! -f "${web_root}/package.json" ]]; then
  echo "--web-root must identify a dev-health-web checkout" >&2
  exit 64
fi

# CHAOS-3532: arming the QUA ladder is a deliberate act HERE, and nowhere
# else.
#
# These two are the only ASK_DEV_-namespaced names this launcher clears
# rather than letting through. That prefix normally means "this launcher's
# own knob, cannot collide with an ambient dev .env by construction" -- the
# third bucket of test_launcher_hardens_compose_interpolation_env_for_every
# _var_it_boots. For these two the assumption behind that bucket is simply
# false: ops/.env sets ASK_DEV_QUA_SHADOW_ENABLED=1 and
# ASK_DEV_QUA_COMMIT_ENABLED=1 for the DEV stack, direnv exports that file
# into every shell under the ops tree, and passing them through would boot
# every acceptance stack from a developer shell silently ARMED.
#
# That is not a hypothetical leftover export. It was the live state of every
# ops shell on this machine, verified directly, hours after the passthrough
# version of this wiring was written -- which is why this reverses it.
#
# An armed stack is not merely "more feature on": the QUA shadow changes
# what every baseline corpus case measures, and armed runs are graded
# against predictions pre-registered on an UNARMED system. Silent arming
# invalidates the comparison rather than extending it.
#
# So: cleared unconditionally above, then translated from this launcher's
# own one-shot knob AFTER the clear. Setting the two flags directly cannot
# arm anything; only ASK_DEV_ACCEPTANCE_QUA=1 can.
if [[ "${ASK_DEV_ACCEPTANCE_QUA:-0}" == "1" ]]; then
  export ASK_DEV_QUA_SHADOW_ENABLED=1
  export ASK_DEV_QUA_COMMIT_ENABLED=1
else
  export ASK_DEV_QUA_SHADOW_ENABLED=0
  export ASK_DEV_QUA_COMMIT_ENABLED=0
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ops_root="$(cd -- "${script_dir}/../.." && pwd)"
compose_file="${ops_root}/compose.yml"
acceptance_compose_file="${ops_root}/tests/acceptance/compose.ask-dev.yml"
acceptance_acr_compose_file="${ops_root}/tests/acceptance/compose.ask-dev-acr.yml"
# A fresh local acceptance stack gets its own Compose namespace. CI and
# coordinated callers can still provide ASK_DEV_ACCEPTANCE_PROJECT_NAME so
# later teardown/verification processes address the same stack.
if [[ -z "${ASK_DEV_ACCEPTANCE_PROJECT_NAME:-}" ]]; then
  project_name="dev-health-ask-dev-acceptance-${RANDOM}${RANDOM}"
else
  project_name="${ASK_DEV_ACCEPTANCE_PROJECT_NAME}"
fi
export ASK_DEV_ACCEPTANCE_PROJECT_NAME="${project_name}"
fixture_org_id="0a155cab-8833-42ac-a4ef-0d121725a7b0"
oracle_file="${ops_root}/tests/acceptance/ask-dev-oracle.v1.json"
graph_oracle_file="${ops_root}/tests/acceptance/ask-dev-graph-oracle.v1.json"
world_manifest="${ops_root}/tests/acceptance/world/ask-dev-world.v1/world.json"
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
# A retained stack is consumed by a second process (for example the Wave 4
# corpus), so the dynamically assigned API port must survive this shell. Keep
# the path overrideable for CI, and namespace the default by Compose project so
# two local acceptance projects do not overwrite each other's URL artifact.
api_url_output="${ASK_DEV_ACCEPTANCE_API_URL_OUTPUT:-/tmp/ask-dev-acceptance-api-url-${project_name}.env}"
read_oracle_field() {
  "${ops_root}/.venv/bin/python" -c \
    'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))[sys.argv[2]])' \
    "${oracle_file}" "$1"
}
read_graph_oracle_field() {
  "${ops_root}/.venv/bin/python" -c \
    'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))[sys.argv[2]])' \
    "${graph_oracle_file}" "$1"
}
read_world_user_field() {
  PYTHONPATH="${ops_root}/src:${ops_root}" \
    "${ops_root}/.venv/bin/python" -c \
    'import json, sys; user = next(item for item in json.load(open(sys.argv[1], encoding="utf-8"))["users"] if item["alias"] == sys.argv[2]); print(user[sys.argv[3]])' \
    "${world_manifest}" "$1" "$2"
}
export DEV_HEALTH_BUILD_SHA="$(git -C "${ops_root}" rev-parse HEAD)"
acceptance_question="$(read_oracle_field question)"
expected_metric_id="$(read_oracle_field expected_metric_id)"
expected_evidence_entity_fragment="$(read_oracle_field expected_evidence_entity_fragment)"
expected_claim_kind="$(read_oracle_field expected_claim_kind)"

# The graph seed writes the primary partition for this exact world principal.
# Derive the browser credentials and expected org from the committed manifest,
# rather than reusing the generated fixture admin (which belongs to a
# different org and can make the graph gate prove an empty partition).
graph_primary_user_alias="primary.platform-admin"
graph_primary_user_email="$(read_world_user_field "${graph_primary_user_alias}" email)"
graph_primary_org_alias="$(read_world_user_field "${graph_primary_user_alias}" org_alias)"
if [[ "${graph_primary_org_alias}" != "primary" ]]; then
  echo "${graph_primary_user_alias} must belong to world org primary, got ${graph_primary_org_alias}" >&2
  exit 70
fi
graph_primary_user_password="$({
  OTEL_ENABLED=false \
  PYTHONPATH="${ops_root}/src:${ops_root}" \
    "${ops_root}/.venv/bin/python" -c \
    'from dev_health_ops.fixtures.world import password_for_alias; import sys; print(password_for_alias(sys.argv[1]))' \
    "${graph_primary_user_alias}"
})"
graph_primary_org_id="$({
  OTEL_ENABLED=false \
  PYTHONPATH="${ops_root}/src:${ops_root}" \
    "${ops_root}/.venv/bin/python" -c \
    'from dev_health_ops.fixtures.world import load_world_manifest; import sys; manifest = load_world_manifest(sys.argv[1]); print(manifest.org_id("primary"))' \
    "${world_manifest}"
})"

export ASK_DEV_WEB_CONTEXT="${web_root}"
# Let Docker allocate a free loopback port by default. Local development runs
# multiple Compose projects concurrently, so no fixed host port is safe.
# Callers may still pin a port for reproducible CI or manual debugging.
export ASK_DEV_ACCEPTANCE_API_PORT="${ASK_DEV_ACCEPTANCE_API_PORT:-0}"
export ASK_DEV_ACCEPTANCE_WEB_PORT="${ASK_DEV_ACCEPTANCE_WEB_PORT:-0}"
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-ask-dev-acceptance-unused}"

compose=(
  docker compose
  --project-name "${project_name}"
  --project-directory "${ops_root}"
  -f "${compose_file}"
  -f "${acceptance_compose_file}"
  --profile ask-dev-acceptance
  --profile graph-trial
)
# CHAOS-3463: split in two. `world-restore` checks that every table it is
# about to write is empty and then writes it; a running `worker`/`beat` fleet
# is a concurrent writer racing that check (beat dispatches monitor-queue-depths
# every 60s from the moment it starts). The jobs fleet is therefore brought up
# AFTER the restore -- it is still a required part of the stack, just not while
# a state-based precondition is being evaluated. Codex adversarial review
# (HIGH): the original single list started them before the restore.
boot_services=(postgres pgbouncer clickhouse valkey graph-trial-store migrate ask-dev-scripted-openai api)
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
acceptance_api_port="$("${compose[@]}" port api 8000 --index 1 | awk -F: '{print $NF}')"
if [[ ! "${acceptance_api_port}" =~ ^[0-9]+$ ]] || [[ "${acceptance_api_port}" == "0" ]]; then
  echo "Could not discover the acceptance API host port" >&2
  exit 70
fi
acceptance_api_url="http://127.0.0.1:${acceptance_api_port}"
export ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}"

# urllib honors both spellings when resolving ambient HTTP(S) proxies. Local
# acceptance API calls must bypass them: a proxy on this host can route
# 127.0.0.1 back into itself and consume the whole 20-second request timeout.
if [[ -n "${NO_PROXY:-}" ]]; then
  export NO_PROXY="${NO_PROXY},127.0.0.1,localhost"
else
  export NO_PROXY="127.0.0.1,localhost"
fi
if [[ -n "${no_proxy:-}" ]]; then
  export no_proxy="${no_proxy},127.0.0.1,localhost"
else
  export no_proxy="127.0.0.1,localhost"
fi

# CHAOS-3572: refuse to proceed unless the api container we just booted is
# serving THIS checkout. Runs immediately after boot and before anything --
# world-restore, fixtures, any smoke/corpus script -- reads or writes through
# it, so a wrong-worktree stack is refused here rather than discovered mid-
# measurement (or not discovered at all: docker ps, the API, and every test
# all look healthy regardless of which worktree booted the container). Same
# mechanism and exit code (70) as the mint guard #1582 added for the one-off
# mint flow; see container_source_guard.sh for why it is a shared function
# rather than a copy in every entrypoint.
# shellcheck source=container_source_guard.sh
source "${script_dir}/container_source_guard.sh"
container_source_guard_check "${ops_root}" "${compose[@]}"

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

# W3 positive graph path. This is a real projection from the restored
# world's canonical ClickHouse team metrics into the isolated trial store;
# it does not inject an answer or an expected graph state. The seed fails
# unless the source data itself contains a corroborated two-signal pressure
# candidate, and the browser later proves that production routing consumes it.
"${compose[@]}" exec -T api python \
  /app/scripts/acceptance/seed_ask_dev_graph_acceptance.py \
  --manifest /app/tests/acceptance/world/ask-dev-world.v1/world.json

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
  --manifest "${ops_root}/tests/acceptance/world/ask-dev-world.v1/world.json" \
  --boot-subset

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

# The graph seed targets the canonical primary world org, while the ordinary
# acceptance preparation above intentionally provisions a separate generated
# fixture org and two Wave 4 tenants. Prepare readiness for the same canonical
# principal before the graph browser gate, and fail if the login resolves to a
# different org than the seed's partition.
graph_prepared_org_id="$({
  PYTHONPATH="${ops_root}/src:${ops_root}" \
  ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
  TEST_SUPERUSER_EMAIL="${graph_primary_user_email}" \
  TEST_SUPERUSER_PASSWORD="${graph_primary_user_password}" \
    "${ops_root}/.venv/bin/python" -c '
import os
from scripts.acceptance.prepare_ask_dev_acceptance import AcceptanceApi, prepare

api = AcceptanceApi(os.environ["ASK_DEV_ACCEPTANCE_API_URL"])
print(prepare(api, email=os.environ["TEST_SUPERUSER_EMAIL"], password=os.environ["TEST_SUPERUSER_PASSWORD"]))
'
})"
if [[ "${graph_prepared_org_id}" != "${graph_primary_org_id}" ]]; then
  echo "graph readiness prepared org ${graph_prepared_org_id}, expected seeded org ${graph_primary_org_id}" >&2
  exit 70
fi
echo "ASK_DEV_GRAPH_PRIMARY_READINESS=PASSED org_id=${graph_prepared_org_id}"

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

acceptance_web_port="$("${compose[@]}" port web 3000 --index 1 | awk -F: '{print $NF}')"
if [[ ! "${acceptance_web_port}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Could not discover the acceptance Web host port" >&2
  exit 70
fi
acceptance_web_url="http://127.0.0.1:${acceptance_web_port}"
export ASK_DEV_ACCEPTANCE_WEB_URL="${acceptance_web_url}"

ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_COMPOSE_WEB_READY=1 \
ASK_DEV_ACCEPTANCE_WEB_URL="${acceptance_web_url}" \
ASK_DEV_ACCEPTANCE_QUESTION="${acceptance_question}" \
ASK_DEV_ACCEPTANCE_EXPECTED_METRIC_ID="${expected_metric_id}" \
ASK_DEV_ACCEPTANCE_EXPECTED_EVIDENCE_FRAGMENT="${expected_evidence_entity_fragment}" \
ASK_DEV_ACCEPTANCE_EXPECTED_CLAIM_KIND="${expected_claim_kind}" \
PLAYWRIGHT_LIVE_BACKEND_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${web_root}/node_modules/.bin/playwright" test \
  -c "${web_root}/playwright.ask-dev-acceptance.config.ts"

# CHAOS-3586 (unblocks CHAOS-3510 / Phase 4 Lane 4d): the Wave 4 access
# matrix. A SECOND playwright invocation rather than more env on the one
# above, because the two configs arm on different contracts and must be able
# to fail independently -- folding them together would let a Phase 1 oracle
# change take the access matrix down with it, and vice versa.
#
# ASK_DEV_WAVE4_ACCESS_MATRIX is deliberately its own knob, not implied by
# ASK_DEV_LIVE_ACCEPTANCE. A launcher predating this lane sets the latter but
# not the former, so it cannot appear to have run a matrix that did not exist
# yet -- the web config throws instead of silently proving nothing.
#
# ASK_DEV_ACCEPTANCE_ACR is forwarded rather than defaulted: the entitlement
# non-coupling rows assert against the DECLARED arming state, and the web
# config rejects anything that is not exactly "0" or "1". Forwarding
# ${acr_armed} (already normalized above) keeps that contract honest whether
# or not this run armed ACR.
# PRESENCE GUARD (CHAOS-3586 follow-up). The config below lives in
# dev-health-web and landed there AFTER this leg did -- an ordering mistake in
# the original change: the ops half had a hard dependency on the web half and
# merged first. Until the web change is on main, `playwright test -c <missing>`
# exits 1 and `set -e` kills the whole run AFTER a full boot, seed and Phase 1
# pass. That broke every launcher invocation whose --web-root is any checkout
# without the config, including the nightly workflow.
#
# A source-reading contract test cannot catch this: it proves the invocation
# exists in THIS file and can say nothing about whether the path resolves in
# another repository. The reference is cross-repo and was dangling.
#
# So: skip when absent -- but a skip must never be readable as a pass. The
# marker below is a single greppable line stating explicitly that the matrix
# did NOT run and why, so no log reader and no artifact scraper can mistake
# this run for one that exercised the matrix. When the web config IS present
# the leg is mandatory and any failure still aborts the run.
# MANDATORY (CHAOS-3510 landed the config on web main). The previous
# revision SKIPPED this leg with a loud marker when the config was absent.
# That skip was scaffolding for exactly one condition -- the ops half of this
# feature merged before the web half -- and that condition no longer exists:
# playwright.ask-dev-wave4.config.ts is on dev-health-web main.
#
# The scaffolding is REMOVED rather than left dormant. A skip path that is
# correct today and unreachable tomorrow is how a gate quietly stops gating:
# it survives because nothing fails when it fires, and the next person to
# delete or rename the config gets a green run and a marker nobody greps for.
#
# Unconditional on purpose. A developer pointing --web-root at a checkout
# predating CHAOS-3510 is TOLD, loudly, what to update -- "tolerant for local
# convenience" is precisely how the dormant path would survive.
wave4_config="${web_root}/playwright.ask-dev-wave4.config.ts"
if [[ ! -f "${wave4_config}" ]]; then
  echo "WAVE4_ACCESS_MATRIX=FAILED reason=config-absent web_root=${web_root}" >&2
  echo "The Wave 4 access matrix config is REQUIRED and was not found:" >&2
  echo "  ${wave4_config}" >&2
  echo "It has been on dev-health-web main since CHAOS-3510. A --web-root" >&2
  echo "without it is a checkout predating that change; update or rebase it." >&2
  echo "This leg is mandatory: refusing to report a run that never exercised" >&2
  echo "the Context Fabric Validation access matrix." >&2
  exit 1
fi
if [[ ! -s "${org_ids_output}" ]]; then
  echo "Wave 4 access matrix cannot run: ${org_ids_output} is missing or empty." >&2
  echo "prepare_ask_dev_acceptance.py must have written the org-ids artifact" >&2
  echo "(schema ask_dev_acceptance_org_ids.v1) before this point. Refusing to" >&2
  echo "run the matrix against unknown tenants rather than skipping it." >&2
  exit 1
fi
echo "WAVE4_ACCESS_MATRIX=RUNNING web_root=${web_root}"
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_COMPOSE_WEB_READY=1 \
ASK_DEV_WAVE4_ACCESS_MATRIX=1 \
ASK_DEV_ACCEPTANCE_WEB_URL="${acceptance_web_url}" \
ASK_DEV_ACCEPTANCE_ORG_IDS="${org_ids_output}" \
ASK_DEV_ACCEPTANCE_ACR="${acr_armed}" \
PLAYWRIGHT_LIVE_BACKEND_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
  "${web_root}/node_modules/.bin/playwright" test \
  -c "${wave4_config}"

if [[ ! -f "${graph_oracle_file}" ]]; then
  echo "W3_GRAPH_ACCEPTANCE=FAILED reason=oracle-absent path=${graph_oracle_file}" >&2
  exit 1
fi
graph_question="$(read_graph_oracle_field graph_question)"
graph_fallback_question="$(read_graph_oracle_field fallback_question)"
graph_ambiguous_question="$(read_graph_oracle_field ambiguous_question)"
graph_expected_state="$(read_graph_oracle_field expected_graph_state)"
graph_expected_fallback_state="$(read_graph_oracle_field expected_fallback_state)"
echo "W3_GRAPH_ACCEPTANCE=RUNNING backend_sha=${DEV_HEALTH_BUILD_SHA}"
ASK_DEV_GRAPH_LIVE_ACCEPTANCE=1 \
ASK_DEV_COMPOSE_WEB_READY=1 \
ASK_DEV_GRAPH_ACCEPTANCE_FALLBACK_ARM=1 \
ASK_DEV_ACCEPTANCE_WEB_URL="${acceptance_web_url}" \
ASK_DEV_GRAPH_ACCEPTANCE_QUESTION="${graph_question}" \
ASK_DEV_GRAPH_ACCEPTANCE_FALLBACK_QUESTION="${graph_fallback_question}" \
ASK_DEV_GRAPH_ACCEPTANCE_AMBIGUOUS_QUESTION="${graph_ambiguous_question}" \
ASK_DEV_GRAPH_ACCEPTANCE_EXPECTED_GRAPH_STATE="${graph_expected_state}" \
ASK_DEV_GRAPH_ACCEPTANCE_EXPECTED_FALLBACK_STATE="${graph_expected_fallback_state}" \
ASK_DEV_GRAPH_ACCEPTANCE_BACKEND_SHA="${DEV_HEALTH_BUILD_SHA}" \
PLAYWRIGHT_LIVE_BACKEND_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL="${graph_primary_user_email}" \
TEST_SUPERUSER_PASSWORD="${graph_primary_user_password}" \
  "${web_root}/node_modules/.bin/playwright" test \
  -c "${web_root}/playwright.ask-dev-graph-acceptance.config.ts"

# CHAOS-3219 Phase 5 (CI lane): keep the stack up for a caller that has more
# to run against it -- specifically scripts/acceptance/run_wave4_corpus.sh,
# whose own header states the stack must already be up and that it will never
# boot or tear one down. Without this the only way to run the launcher AND the
# armed corpus in one CI job is a hand-rolled copy of the launcher's boot
# sequence with the teardown deleted, which is what the local Phase 2 exit runs
# had to do -- a duplicate that drifts from the canonical launcher silently and
# is covered by none of its tests.
#
# Opt-IN, and deliberately not a "clean up on failure only" heuristic: the
# default path is unchanged (always tear down, success or failure), so a
# developer running this by hand can never leak a stack by forgetting a flag.
# The caller that sets it owns the teardown -- .github/workflows/
# ask-dev-acceptance.yml does it in an `if: always()` step, so a cancelled or
# failed job still releases the runner's disk.
if [[ "${ASK_DEV_ACCEPTANCE_KEEP_STACK:-0}" == "1" ]]; then
  mkdir -p "$(dirname -- "${api_url_output}")"
  printf 'export ASK_DEV_ACCEPTANCE_PROJECT_NAME=%q\n' "${project_name}" > "${api_url_output}"
  printf 'export ASK_DEV_ACCEPTANCE_API_URL=%q\n' "${acceptance_api_url}" >> "${api_url_output}"
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    printf 'ASK_DEV_ACCEPTANCE_PROJECT_NAME=%s\n' "${project_name}" >> "${GITHUB_ENV}"
    printf 'ASK_DEV_ACCEPTANCE_API_URL=%s\n' "${acceptance_api_url}" >> "${GITHUB_ENV}"
  fi
  trap - EXIT
  echo "Ask Dev Compose acceptance completed successfully; stack retained (ASK_DEV_ACCEPTANCE_KEEP_STACK=1)."
  echo "ASK_DEV_ACCEPTANCE_API_URL=${acceptance_api_url}"
  echo "ASK_DEV_ACCEPTANCE_API_URL_FILE=${api_url_output}"
  echo "Source it before an external corpus invocation: source ${api_url_output}"
  # Deliberately NOT spelled with the same literal as the real teardown command
  # below. Adversarial review 2026-08-06 (HIGH): the first version of this hint
  # printed `down --volumes --remove-orphans` verbatim, which put a SECOND
  # occurrence of that string into the file -- silently satisfying the
  # pre-existing `assert "down --volumes --remove-orphans" in launcher`
  # (test_launcher_owns_seed_readiness_web_and_fixed_browser_oracle) and this
  # commit's own ordering assertion with a print statement. Deleting the real
  # teardown then changed no test. A help message must not be able to stand in
  # for the command it describes.
  echo "Tear it down with: docker compose -p ${project_name} down -v --remove-orphans"
  exit 0
fi

"${compose[@]}" down --volumes --remove-orphans
trap - EXIT
echo "Ask Dev Compose acceptance completed successfully."
