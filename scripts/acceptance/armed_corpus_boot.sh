#!/usr/bin/env bash
# CHAOS-3575: boot the acceptance stack for an ARMED Wave 4 corpus run, with
# every precondition a measured run depends on checked LOUDLY and IN THE
# CONTAINER rather than assumed from compose.
#
# WHY THIS IS IN-TREE. This recipe previously existed only as a scratch copy
# under /tmp. A machine reboot on 2026-08-07 wiped it mid-run, and the archive
# copy it was rebuilt from contained NEITHER the QUA check NOR the engine
# identity check -- so "restore from the archive" would have silently booted a
# stack with two of its four preconditions missing. Anything a measured run
# depends on belongs in version control.
#
# It mirrors run_ask_dev_compose.sh's boot + seed portion, deliberately stops
# before the smoke/web/playwright leg, and never tears the stack down, because
# the armed corpus run needs the stack UP.
#
# Preconditions, each a HARD abort (exit 75) rather than an advisory line:
#   A  QUA shadow/commit flags are not armed, read from the api container's own
#      environment
#   B  the scripted engine serves the same role+script identity the host loaded
#   C  the platform cost allowance cannot gate the measurement
#   D  the allowance counter starts unspent
#
# macOS bash 3.2 compatible.
set -euo pipefail

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
  BUGSINK_BASE_URL BUGSINK_CREATE_SUPERUSER || true

# DERIVED, never hardcoded: the scratch copy of this script hardcoded one
# worktree's absolute path, so running it from anywhere else silently measured
# a DIFFERENT checkout than the one under test -- the compose
# --project-directory decides the /app bind mount.
ops_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
# Locating the web checkout has TWO layouts to satisfy, and the obvious one is
# wrong for the case this script is actually used in. `${ops_root}/../web` holds
# for the main checkout (ops/../web), but a git WORKTREE lives at
# ops-worktrees/<name>/, so the same expression resolves to the non-existent
# ops-worktrees/web -- and the armed run is normally driven FROM a worktree.
# Found by running it: the first boot on 2026-08-07 died exit 64 here.
#
# So fall back to the repo's COMMON dir, which points at the main checkout's
# .git regardless of which worktree is running: <common>/../../web.
web_root="${ASK_DEV_WEB_CONTEXT:-}"
if [[ -z "${web_root}" ]]; then
  web_root="$(cd -- "${ops_root}/../web" 2>/dev/null && pwd || true)"
fi
if [[ -z "${web_root}" || ! -d "${web_root}" ]]; then
  _common="$(git -C "${ops_root}" rev-parse --git-common-dir 2>/dev/null || true)"
  if [[ -n "${_common}" ]]; then
    web_root="$(cd -- "${_common}/../../web" 2>/dev/null && pwd || true)"
  fi
fi
if [[ -z "${web_root}" || ! -d "${web_root}" ]]; then
  echo "cannot locate the web checkout (tried ${ops_root}/../web and the" >&2
  echo "git common-dir sibling); set ASK_DEV_WEB_CONTEXT to its path." >&2
  exit 64
fi
project_name="${ASK_DEV_ACCEPTANCE_PROJECT_NAME:-dev-health-ask-dev-acceptance}"
fixture_org_id="0a155cab-8833-42ac-a4ef-0d121725a7b0"
world_dir="${ops_root}/tests/acceptance/world/ask-dev-world.v1"
log_dir="${ASK_DEV_ACCEPTANCE_LOG_DIR:-${ops_root}/tests/acceptance/artifacts/wave4}"
mkdir -p "${log_dir}"

venv_python="${ops_root}/.venv/bin/python"
if [[ ! -x "${venv_python}" ]]; then
  echo "expected the worktree venv at ${venv_python} -- run 'uv sync --all-extras --dev' first" >&2
  exit 64
fi

# chris's ops/.env + direnv export both QUA flags as =1 in an interactive
# shell. Force them off here so any compose passthrough resolves to off, then
# VERIFY in the container below: reasoning about compose semantics is not
# evidence, and precondition A exists because that reasoning was once wrong.
export ASK_DEV_QUA_SHADOW_ENABLED=0
export ASK_DEV_QUA_COMMIT_ENABLED=0

export ASK_DEV_WEB_CONTEXT="${web_root}"
export ASK_DEV_ACCEPTANCE_API_PORT="${ASK_DEV_ACCEPTANCE_API_PORT:-18099}"
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-ask-dev-acceptance-unused}"
export ASK_DEV_ACCEPTANCE_ACR="${ASK_DEV_ACCEPTANCE_ACR:-0}"
acceptance_api_url="http://127.0.0.1:${ASK_DEV_ACCEPTANCE_API_PORT}"

compose=(
  docker compose
  --project-name "${project_name}"
  --project-directory "${ops_root}"
  -f "${ops_root}/compose.yml"
  -f "${ops_root}/tests/acceptance/compose.ask-dev.yml"
  -f "${ops_root}/scripts/acceptance/acceptance_allowance_override.yml"
  --profile ask-dev-acceptance
)
# CHAOS-3463: worker/beat come up AFTER the restore -- a running jobs fleet is
# a concurrent writer racing world-restore's "every table I am about to write
# is empty" precondition.
boot_services=(postgres pgbouncer clickhouse valkey migrate ask-dev-scripted-openai api)
jobs_services=(worker beat)

echo "=== compose config --quiet ==="
"${compose[@]}" config --quiet

echo "=== down --volumes --remove-orphans (THIS project only) ==="
"${compose[@]}" down --volumes --remove-orphans

echo "=== up -d --build --wait (boot services, no jobs fleet) ==="
"${compose[@]}" up -d --build --wait "${boot_services[@]}"

echo "=== PRECONDITION A: QUA flags UNSET/OFF inside the api container's own env ==="
# `printenv NAME` exits 1 when unset, so the ${VAR:-<unset>} form is expanded
# INSIDE the container by sh -c -- not interpolated by this shell, which would
# report the launcher's view instead of the container's.
qua_shadow="$("${compose[@]}" exec -T api sh -c 'printf %s "${ASK_DEV_QUA_SHADOW_ENABLED:-<unset>}"')"
qua_commit="$("${compose[@]}" exec -T api sh -c 'printf %s "${ASK_DEV_QUA_COMMIT_ENABLED:-<unset>}"')"
echo "QUA_SHADOW_IN_CONTAINER=${qua_shadow}"
echo "QUA_COMMIT_IN_CONTAINER=${qua_commit}"
if [[ "${qua_shadow}" == "1" || "${qua_commit}" == "1" ]]; then
  echo "QUA_VERIFIED=ARMED (shadow=${qua_shadow} commit=${qua_commit}) -- HARD ABORT" >&2
  echo "A measured armed run must not begin against a QUA-armed stack." >&2
  exit 75
fi
echo "QUA_VERIFIED=not-armed (shadow=${qua_shadow} commit=${qua_commit})"

echo "=== PRECONDITION B: scripted engine identity (host vs container) ==="
# The same differential the armed run's own _scripted_engine_precondition
# fixture performs (role + role_script_identity_digest over by_fingerprint),
# run HERE so a stale mount costs a second rather than a full corpus run.
PYTHONPATH="${ops_root}/src:${ops_root}" "${venv_python}" - <<'PYEOF' || exit 75
import sys

from dev_health_ops.llm.agent.provider_scripts import (
    current_role,
    load_role_script,
    role_script_identity_digest,
)
from scripts.acceptance.corpus.compose_context import ComposeContext
from scripts.acceptance.corpus.scripted_engine import (
    ScriptedEngineUnavailableError,
    require_scripted_engine_loaded,
)

role = current_role()
script = load_role_script(role)
expected = role_script_identity_digest(script)
try:
    status = require_scripted_engine_loaded(
        ComposeContext.from_env(), expected_role=role, expected_digest=expected
    )
except ScriptedEngineUnavailableError as exc:
    print(f"ENGINE IDENTITY MISMATCH: {exc}", file=sys.stderr)
    raise SystemExit(1)
print(f"IDENTITY MATCH {status.script_digest} role={status.role} cases={status.cases}")
PYEOF

echo "=== PROOF 1: fixtures world-restore (digest-pinned) ==="
"${compose[@]}" exec -T api dev-hops fixtures world-restore \
  --manifest /app/tests/acceptance/world/ask-dev-world.v1/world.json \
  --sink clickhouse://ch:ch@clickhouse:8123/default \
  --postgres-uri postgresql+asyncpg://postgres:postgres@postgres:5432/postgres \
  --snapshot /app/tests/acceptance/world/ask-dev-world.v1/snapshot

echo "=== PROOF 2: assert_world_principals_can_log_in.py ==="
"${venv_python}" \
  "${ops_root}/scripts/acceptance/assert_world_principals_can_log_in.py" \
  --api-url "${acceptance_api_url}" \
  --manifest "${world_dir}/world.json" \
  --boot-subset

echo "=== up -d --build --wait (jobs fleet) ==="
"${compose[@]}" up -d --build --wait "${jobs_services[@]}"

echo "=== fixtures generate (--overwrite-real-users) ==="
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

echo "=== prepare_ask_dev_acceptance.py ==="
ASK_DEV_LIVE_ACCEPTANCE=1 \
ASK_DEV_ACCEPTANCE_API_URL="${acceptance_api_url}" \
TEST_SUPERUSER_EMAIL=admin@devhealth.example \
TEST_SUPERUSER_PASSWORD=devhealth123 \
ASK_DEV_ACCEPTANCE_ORG_IDS_OUTPUT="${log_dir}/acceptance-org-ids.json" \
  "${venv_python}" \
  "${ops_root}/scripts/acceptance/prepare_ask_dev_acceptance.py"

echo "=== PRECONDITION C: platform cost allowance cannot gate the measurement ==="
# The 2026-08-07 10:03 run was degraded to UNMEASURED because 59 of 90 cases
# 429'd on cost_limit_reached: the effective per-org limit resolves to the
# $100 DEFAULT while the runner budgeted against the $200 operator MAX.
# ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD is the ceiling an org may be
# CONFIGURED UP TO, not the effective limit, so both the operator max (the
# compose override beside this script) and the stored per-org row (below) have
# to move. Harness interference only -- no active corpus case asserts platform
# allowance behaviour.
"${compose[@]}" exec -T api python \
  /app/scripts/acceptance/set_platform_allowance.py

echo "=== PRECONDITION D: allowance counter starts unspent ==="
# `down --volumes` is EXPECTED to have wiped the Valkey counter. The lesson of
# the degraded run is that an assumed precondition is not a verified one: a
# counter carried over from an earlier run re-exhausts the budget mid-corpus
# and produces exactly the same unmeasurable result.
counter_dump="$(
  "${compose[@]}" exec -T valkey sh -c \
    'for db in 0 1 2 3; do valkey-cli -n "$db" --scan --pattern "askdev:allowance:*"; done' \
  | tr -d "\r"
)"
if [[ -n "${counter_dump}" ]]; then
  echo "STALE ALLOWANCE COUNTER(S) PRESENT AFTER down --volumes:" >&2
  echo "${counter_dump}" >&2
  echo "A measured run cannot begin against a partially-spent allowance." >&2
  exit 75
fi
echo "ALLOWANCE_COUNTER_VERIFIED=absent (fresh window, nothing spent)"

echo "=== STACK READY ==="
"${compose[@]}" ps
