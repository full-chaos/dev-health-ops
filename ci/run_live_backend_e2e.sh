#!/usr/bin/env bash
set -euo pipefail

EXIT_MISSING_DEP=3
EXIT_FAILURE=10

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}" || exit "${EXIT_FAILURE}"

# CHAOS-5362: the bare-binary River/worker/reconciler mechanism (build,
# provisioning, daemon start + /readyz, sync seed/finalize, native-family
# telemetry wait, teardown) is shared with ci/run_metrics_executed_proof.sh.
# shellcheck source=ci/lib/go_worker_fixture.sh
source "${ROOT_DIR}/ci/lib/go_worker_fixture.sh"
# go-api-prove end to end on the tools-pod bearer path (both bearers minted
# in-process by the tools-image helpers); see the file's own header.
# shellcheck source=ci/lib/go_api_prove_e2e.sh
source "${ROOT_DIR}/ci/lib/go_api_prove_e2e.sh"
# shellcheck source=ci/lib/e2e_fixture.sh
source "${ROOT_DIR}/ci/lib/e2e_fixture.sh"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: Required command '${cmd}' is not available."
    exit "${EXIT_MISSING_DEP}"
  fi
}

require_cmd curl
require_cmd psql

CLICKHOUSE_URI_DEFAULT="clickhouse://ch:ch@127.0.0.1:8123/default"
POSTGRES_URI_DEFAULT="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/test_db"
# CHAOS-7039: database 1, matching compose.yml's REAL deployed convention
# for BOTH planes (line ~595's Python `api` service REDIS_URL and line
# ~1268's `go-api` service VALKEY_URI both point at
# `redis://valkey:6379/1`) -- not this harness's pre-existing, long-lived
# db-0 default, which happened to never matter while only the Python
# process touched Valkey here. `dho api`'s Valkey client
# (internal/storage/valkey/factory.go's Config.Validate()) hard-requires
# database 1 for every one of its callers, including the externalingest
# stream producer the customer-push test now exercises against the real
# Go process -- measured: on db 0, `POST /batches` answered 202 correctly
# but the real XADD landed in a database this harness's own REDIS_URL-based
# test assertions and consumer never looked at, so `stream length +1` and
# `consumer processed > 0` both read as a silent no-op. Moving the default
# to db 1 makes the harness match prod's own topology instead of adding a
# THIRD divergent index.
REDIS_URL_DEFAULT="redis://127.0.0.1:6379/1"

CLICKHOUSE_URI="${CLICKHOUSE_URI:-${CLICKHOUSE_URI_DEFAULT}}"
POSTGRES_URI="${POSTGRES_URI:-${POSTGRES_URI_DEFAULT}}"
DATABASE_URI="${POSTGRES_URI}"
# CHAOS-2702: the customer-push external-ingest live e2e module needs a real
# Valkey/Redis instance (durable stream + bounded-recompute debounce), in
# addition to the Postgres/ClickHouse this harness already provisions.
REDIS_URL="${REDIS_URL:-${REDIS_URL_DEFAULT}}"
export REDIS_URL CLICKHOUSE_URI POSTGRES_URI DATABASE_URI
export DISABLE_DOTENV=1

API_HOST="${LIVE_E2E_API_HOST:-127.0.0.1}"
API_PORT="${LIVE_E2E_API_PORT:-18080}"
BASE_URL="http://${API_HOST}:${API_PORT}"

# BASE_URL is `dho api` (the Go binary) -- health/ready and the
# customer-push live e2e test below run against it. No Python api process
# boots in this harness: the GraphQL edge the prover leg measures is
# query-api's own /graphql (ci/lib/go_api_prove_e2e.sh), which stamps the
# plane and checks the principal and token_version itself.

# CHAOS-7039: `dho api`'s OWN operator listener (/healthz, /readyz,
# /metrics -- internal/platform/shell, distinct from the api-addr business
# routes above) defaults to :8080 (internal/platform/config/config.go's
# defaultHTTPAddress), the SAME default `dho query-api` uses for its own
# operator listener (query_api_e2e_start, ci/lib/go_api_prove_e2e.sh, run
# later in this script). Both processes are now `dho` binaries in the same
# harness for the first time -- give dho api an explicit, distinct
# operator port so it never collides (measured: unpatched, query-api's
# later start failed with "listen tcp :8080: bind: address already in
# use" and every check after it silently hit a stale process instead).
API_OPERATOR_PORT="${LIVE_E2E_API_OPERATOR_PORT:-18081}"

FIXTURE_SEED="${LIVE_E2E_FIXTURE_SEED:-20260219}"
FIXTURE_DAYS="${LIVE_E2E_FIXTURE_DAYS:-14}"
FIXTURE_REPO_NAME="${LIVE_E2E_FIXTURE_REPO_NAME:-acme/live-e2e}"
FIXTURE_PROVIDER="${LIVE_E2E_FIXTURE_PROVIDER:-github}"
FIXTURE_COMMITS_PER_DAY="${LIVE_E2E_COMMITS_PER_DAY:-6}"
FIXTURE_PR_COUNT="${LIVE_E2E_PR_COUNT:-24}"
export FIXTURE_PROVIDER


# Must match the org_id used in generate_auth_token() below.
E2E_ORG_ID="11111111-2222-4333-8444-555555555555"
READINESS_ATTEMPTS="${LIVE_E2E_READINESS_ATTEMPTS:-90}"
READINESS_SLEEP_SECS="${LIVE_E2E_READINESS_SLEEP_SECS:-2}"

# CHAOS-5362: bare-binary Go worker/reconciler config (ci/lib/go_worker_
# fixture.sh). Same defaults as ci/run_metrics_executed_proof.sh's own
# Postgres/River settings -- both jobs' service containers use the same
# postgres/postgres/test_db credentials (see .github/workflows/live-e2e.yml).
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_SUPERUSER="${POSTGRES_SUPERUSER:-postgres}"
POSTGRES_SUPERUSER_PASSWORD="${POSTGRES_SUPERUSER_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-test_db}"
POSTGRES_SUPERUSER_URI="${POSTGRES_URI}"

CLICKHOUSE_URI_HTTP="${CLICKHOUSE_URI}"
CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT:-9000}"
# Go's clickhouse-go speaks the native wire protocol, not Python's
# clickhouse-connect HTTP port -- see start_worker_stack's own comment.
# Derived from the HTTP DSN (clickhouse_native_uri): one address, so an override of any part of
# CLICKHOUSE_URI reaches the migrate, the worker and the fixtures alike, or fails loudly.
CLICKHOUSE_URI_NATIVE="$(clickhouse_native_uri "${CLICKHOUSE_URI_HTTP}" "${CLICKHOUSE_NATIVE_PORT}" CLICKHOUSE_URI)"

VALKEY_HOST="${LIVE_E2E_VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${LIVE_E2E_VALKEY_PORT:-6379}"

RIVER_DOMAIN_ROLE="devhealth_domain"
RIVER_QUEUE_ROLE="devhealth_queue"
RIVER_COORDINATOR_ROLE="devhealth_coordinator"
RIVER_DOMAIN_PASSWORD="devhealth_domain"
RIVER_QUEUE_PASSWORD="devhealth_queue"
RIVER_COORDINATOR_PASSWORD="devhealth_coordinator"

# CHAOS-5362: previously only ever read inline as
# "${SETTINGS_ENCRYPTION_KEY:-dev-key-not-for-prod}" (never actually
# assigned as a shell variable) -- start_worker_stack's plain `export
# SETTINGS_ENCRYPTION_KEY` needs it to actually be set under `set -u`.
SETTINGS_ENCRYPTION_KEY="${SETTINGS_ENCRYPTION_KEY:-dev-key-not-for-prod}"
WORKER_HTTP_PORT="${LIVE_E2E_WORKER_PORT:-18085}"
RECONCILER_HTTP_PORT="${LIVE_E2E_RECONCILER_PORT:-18086}"
QUERY_API_PORT="${LIVE_E2E_QUERY_API_PORT:-18090}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/live-backend-e2e.XXXXXX")"
# CHAOS-5362: BIN_DIR for the Go binaries build_go_binaries builds.
BIN_DIR="${TMP_DIR}/bin"
mkdir -p "${BIN_DIR}"
API_LOG_FILE="${LIVE_E2E_API_LOG_FILE:-${TMP_DIR}/api.log}"
API_PID=""
WORKER_PID=""
RECONCILER_PID=""
QUERY_API_PID=""

# CHAOS-5025 (shared teardown bound): see validate_teardown_wait_secs /
# stop_worker_stack in ci/lib/go_worker_fixture.sh.
TEARDOWN_WAIT_SECS="${LIVE_E2E_TEARDOWN_WAIT_SECS:-60}"
validate_teardown_wait_secs

# CHAOS-5362: this cleanup() now tears down 3 background daemons (api,
# worker, reconciler) via stop_worker_stack + a direct stop_service for the
# api, not just the original single API_PID -- so it adopts the same
# re-entrancy-safe trap discipline ci/run_metrics_executed_proof.sh already
# needed for the identical shape (codex r2/r3 findings there): a second
# signal arriving mid-teardown can re-enter cleanup() on bash's
# function-return path (`pop_var_context: head of shell_variables not a
# function context`), and a trapped INT/TERM RESUMES the script rather than
# exiting it, so a cancelled run would otherwise tear down services, delete
# TMP_DIR, and then keep executing the harness against nothing.
CLEANUP_DONE=""
cleanup() {
  local rc=$?
  if [ -n "${CLEANUP_DONE}" ]; then
    return "${rc}"
  fi
  CLEANUP_DONE=1
  trap '' INT TERM
  # Kill ORDER (CHAOS-5025): stop_worker_stack drains worker then
  # reconciler; the api stops here, directly, afterward. CHAOS-6279: no
  # longer load-bearing (see ci/lib/go_worker_fixture.sh's header comment)
  # -- kept as a conservative convention.
  stop_worker_stack
  stop_service "query-api" "${QUERY_API_PID}"
  stop_service "dho api" "${API_PID}"
  rm -rf "${TMP_DIR}" >/dev/null 2>&1 || true
  return "${rc}"
}
on_signal() {
  local sig="$1"
  cleanup
  trap - EXIT
  trap - "${sig}"
  kill -"${sig}" "$$"
}
trap cleanup EXIT
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

wait_for_redis() {
  # CHAOS-2702: fail fast (before burning time on fixture generation / API
  # boot) if the Valkey service container isn't reachable yet. Uses the
  # `cigate live-e2e valkey-ping` (a raw PING over TCP) instead of requiring a
  # `valkey-cli`/`redis-cli` binary on the runner.
  local i
  for ((i = 1; i <= READINESS_ATTEMPTS; i++)); do
    if REDIS_URL="${REDIS_URL}" "${BIN_DIR}/cigate" live-e2e valkey-ping; then
      echo "Valkey ready after ${i} attempt(s)."
      return 0
    fi
    sleep "${READINESS_SLEEP_SECS}"
  done
  echo "ERROR: Timed out waiting for Valkey readiness at ${REDIS_URL}."
  return 1
}


fetch_json() {
  local path="$1"
  local out_file="$2"
  local expected_status="$3"
  # base_url/bearer default to the Python api / AUTH_TOKEN (every existing
  # caller) -- CHAOS-6241's query-api REST checks pass both explicitly, to
  # hit the Go binary with its own envelope bearer instead.
  local base_url="${4:-${BASE_URL}}"
  local bearer="${5-${AUTH_TOKEN:-}}"
  local extra_headers=()
  if [ -n "${bearer}" ]; then
    extra_headers+=(-H "Authorization: Bearer ${bearer}")
  fi
  local status
  status="$(
    curl -sS -o "${out_file}" -w "%{http_code}" \
      -H "Accept: application/json" \
      "${extra_headers[@]}" \
      "${base_url}${path}"
  )"
  if [ "${status}" != "${expected_status}" ]; then
    echo "ERROR: ${path} returned HTTP ${status}, expected ${expected_status}."
    cat "${out_file}" || true
    return 1
  fi
}

wait_for_ready() {
  local readiness_file="${TMP_DIR}/health_ready.json"
  local i
  for ((i = 1; i <= READINESS_ATTEMPTS; i++)); do
    local status
    status="$(
      curl -sS -o "${readiness_file}" -w "%{http_code}" \
        -H "Accept: application/json" \
        "${BASE_URL}/ready" || true
    )"
    if [ "${status}" = "200" ]; then
      echo "API process ready after ${i} attempt(s), checking dependencies..."
      # Now verify critical dependencies via /health
      local health_status
      health_status="$(
        curl -sS -o "${readiness_file}" -w "%{http_code}" \
          -H "Accept: application/json" \
          "${BASE_URL}/health" || true
      )"
      if [ "${health_status}" = "200" ] && "${BIN_DIR}/cigate" live-e2e assert-health "${readiness_file}"
      then
        echo "All dependencies healthy."
        return 0
      fi
    fi

    if [ -n "${API_PID}" ] && ! kill -0 "${API_PID}" >/dev/null 2>&1; then
      echo "ERROR: API process exited before becoming ready."
      tail -n 200 "${API_LOG_FILE}" || true
      return 1
    fi
    sleep "${READINESS_SLEEP_SECS}"
  done

  echo "ERROR: Timed out waiting for API readiness."
  tail -n 200 "${API_LOG_FILE}" || true
  return 1
}

echo "==> building cigate (the e2e checks and fixture steps)"
go build -trimpath -o "${BIN_DIR}/cigate" ./cmd/cigate

echo "==> waiting for Valkey readiness"
wait_for_redis

# CHAOS-5362 (team-lead ruling): Python `--with-metrics`/`--with-work-graph`
# compute seeding is REMOVED from this gate entirely. It used to write
# derived metric rows (repo_metrics_daily, team_metrics_daily, ...) directly
# into ClickHouse via the fixture sink, bypassing sync ->
# metrics.daily_dispatch -> the Go/Python bridge -> job_daily.py entirely --
# fixture-backed readback, never pipeline proof, and increasingly a SILENT
# NO-OP as families' Python compute is deleted one by one (CHAOS-4263/4264
# ran undetected on this exact gap for a week: every existing check asserted
# a job RAN rather than that it produced real rows via the real path;
# #2335's repos_covered_pct=0.0 failure is the same class of bug surfacing
# again). Kept below: the base `fixtures generate` call, for its raw
# git_commits/PR/team data only -- team_wellbeing/cicd/deploy/incident and
# every other native-only family now get their rows from the real Go worker
# pipeline further down (seed_and_finalize_sync_targets /
# wait_for_native_family_telemetry, ci/lib/go_worker_fixture.sh), not from
# Python compute of any kind. For real executed-proof of the metrics
# pipeline generally, see the `metrics-executed-proof` job in
# .github/workflows/live-e2e.yml and `cigate metrics-proof`.
# dho loads a frozen world (CHAOS-7301) into a migrated ClickHouse: migrate first,
# to the head (ordering contract 2, which the worker started later must also use).
export OPERATIONAL_ORDERING_CONTRACT=2
build_go_binaries
CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}" "${BIN_DIR}/dho" migrate clickhouse upgrade
FIXTURES_SINK="$(clickhouse_http_sink "${CLICKHOUSE_URI}" CLICKHOUSE_URI)"
echo "==> generating deterministic ClickHouse fixtures (raw git/PR/team data only)"
(
  export ORG_ID="${E2E_ORG_ID}"
  unset POSTGRES_URI
  unset DATABASE_URI
  unset DATABASE_URL
  # http:// spelled explicitly: dho reads a clickhouse:// DSN as native except on port 8123 and
  # CLICKHOUSE_URI is overrideable.
  # The DSN reaches dho by environment, not argv (it can hold a password).
  CLICKHOUSE_URI="${FIXTURES_SINK}" "${BIN_DIR}/dho" fixtures generate \
    --db-type clickhouse \
    --repo-name "${FIXTURE_REPO_NAME}" \
    --provider "${FIXTURE_PROVIDER}" \
    --days "${FIXTURE_DAYS}" \
    --commits-per-day "${FIXTURE_COMMITS_PER_DAY}" \
    --pr-count "${FIXTURE_PR_COUNT}" \
    --seed "${FIXTURE_SEED}"
)

echo "==> migrating PostgreSQL"
build_go_binaries
MIGRATION_DATABASE_URI="${POSTGRES_URI}" DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 RIVER_DATABASE_SCHEMA=river \
  "${BIN_DIR}/dho" migrate postgres upgrade

# JWT_SECRET_KEY is now required (no SHA256 derivation fallback) — derive the same
# value generate_auth_token() uses so tokens match between API and e2e client.
if [ -z "${JWT_SECRET_KEY:-}" ]; then
  JWT_SECRET_KEY="$(
    ENC_KEY_INPUT="${SETTINGS_ENCRYPTION_KEY:-dev-key-not-for-prod}" "${BIN_DIR}/cigate" live-e2e jwt-secret
  )"
  export JWT_SECRET_KEY
fi

# CHAOS-7039: `dho api` (the Go binary) at BASE_URL. Needs its own dedicated
# Postgres/ClickHouse/Valkey env shape (internal/apiservice/service.go,
# internal/platform/config/config.go) rather than dev-hops's `--db`/
# `--analytics-db` flags: API_DATABASE_URI (accepts the same
# postgresql+asyncpg DSN, normalized at the Go boundary -- see
# internal/storage/postgres/factory.go's normalizeURI), CLICKHOUSE_URI
# (already the same variable name), and --api-addr (not --host/--port).
# VALKEY_URI is expressed separately from REDIS_URL, not because they
# differ (both are database 1 on the same instance now, see
# REDIS_URL_DEFAULT's own comment above) but because
# internal/storage/valkey/factory.go's Config.Validate() hard-requires
# SelectDB == 1 ("Valkey database 1") -- spelling it out here via
# VALKEY_HOST/VALKEY_PORT (the same pair start_worker_stack already uses,
# ci/lib/go_worker_fixture.sh) keeps that requirement visible at the call
# site rather than relying on REDIS_URL happening to agree.
echo "==> [CHAOS-7039] building dho for the api process"
build_go_binaries

echo "==> starting dho api at ${BASE_URL}"
(
  export API_DATABASE_URI="${POSTGRES_URI}"
  export CLICKHOUSE_URI="${CLICKHOUSE_URI}"
  # internal/apiservice/deps.go's health check (internal/api/health) reads
  # deps.ClickHouse, which buildDeps populates from API_CLICKHOUSE_URI (the
  # api's OWN dedicated ClickHouse login, CHAOS-6310) -- NOT from the
  # generic CLICKHOUSE_URI above, which this Service ignores for that
  # purpose. Without it deps.ClickHouse stays nil and /health always
  # answers "down" for clickhouse (measured: compose.yml's go-api service
  # itself does not set this either, a pre-existing gap out of this PR's
  # scope). CLICKHOUSE_URI_NATIVE, not CLICKHOUSE_URI: clickhouse-go speaks
  # the native wire protocol, not Python's HTTP port (measured: the HTTP
  # port answers with "unexpected packet [72]" -- 'H' -- to a native
  # handshake). Same throwaway instance; this harness has no per-service
  # ClickHouse login separation.
  export API_CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}"
  export VALKEY_URI="redis://${VALKEY_HOST}:${VALKEY_PORT}/1"
  export JWT_SECRET_KEY="${JWT_SECRET_KEY}"
  exec "${BIN_DIR}/dho" api --api-addr="${API_HOST}:${API_PORT}" --http-addr="127.0.0.1:${API_OPERATOR_PORT}"
) >"${API_LOG_FILE}" 2>&1 &
API_PID="$!"

echo "==> waiting for readiness"
wait_for_ready

# CHAOS-5362: drive the real Go dispatch/compute pipeline for every family
# whose Python compute is deleted, now that the --with-metrics/
# --with-work-graph fixture-direct write is removed entirely (see the
# comment above the fixtures-generate call).
#
# Reuses ci/run_metrics_executed_proof.sh's own mechanism (ci/lib/go_worker_
# fixture.sh): build the Go binaries, provision River against this job's
# throwaway Postgres, start dho worker + dho reconciler,
# seed real source rows through the real sync path for cicd/deployments/
# incidents/tests, finalize those sync_runs (triggering the real post-sync
# fanout -> metrics.daily_dispatch/daily_partition River jobs), and confirm
# via the worker's own /metrics endpoint that the rows came from the native
# executor, not a fail-open fallback (there is none left for these families).
#
# RISK-NOTE (CHAOS-5362): WORKER_REMAINING_COMPLEXITY_CONFIG_PATH does not
# exist on main today -- it is introduced by unmerged PR #2334
# (chaos-4291-complexity-native-rebuilt). This gate does not select the
# "remaining metrics" queue family that flag gates, so its absence here does
# not affect this job. Whichever of #2334 or this PR lands second must add
# the line exporting WORKER_REMAINING_COMPLEXITY_CONFIG_PATH (pointed at
# src/dev_health_ops/config/complexity.yaml) to start_worker_stack's exported
# env in ci/lib/go_worker_fixture.sh, once both are on main and this job
# picks up a queue that needs it.
echo "==> [CHAOS-5362] building the Go worker/reconciler binaries and provisioning River"
build_go_binaries
migrate_and_assert_river
provision_river

WORKER_LOG_FILE="${LIVE_E2E_WORKER_LOG_FILE:-${TMP_DIR}/worker.log}"
RECONCILER_LOG_FILE="${LIVE_E2E_RECONCILER_LOG_FILE:-${TMP_DIR}/reconciler.log}"
start_worker_stack "${WORKER_LOG_FILE}" "${RECONCILER_LOG_FILE}"

RUN_START="$(run_start_stamp)"
echo "==> [CHAOS-5362] seeding real source rows through the real sync path, run_start=${RUN_START}"
ORG_ID="${E2E_ORG_ID}" REPO_NAME="${FIXTURE_REPO_NAME}" BACKFILL_DAYS="${FIXTURE_DAYS}" \
  seed_and_finalize_sync_targets cicd deployments incidents tests

echo "==> [CHAOS-5362] confirming the native Go executors (not a Python fail-open) computed these rows"
# Reachable subset only (same causal-satisfaction rule as
# ci/run_metrics_executed_proof.sh's own NATIVE_TELEMETRY_FAMILIES): the four
# sync targets seeded above causally satisfy cicd/deploy/incident.
# team_wellbeing/repo_user_commit are causally satisfied by the git_commits
# the base fixtures-generate call above already wrote (that call does not
# pass --team-count, so commits may resolve to the "unassigned" bucket
# rather than a named team -- fine for this check, which only proves the
# native executor ran, not which team it attributed to). benchmarking is NOT
# included -- it needs a complexity-scan seed step this job does not
# perform, same exclusion reason as executed-proof's team_complexity.
# work_item_issue_type and work_item_investment (CHAOS-8810): the base
# fixtures-generate call writes work_items rows of this repository, so both
# families have source rows. A worker that starts without them (their refusal
# is scoped to the two families) fails here.
wait_for_native_family_telemetry team_wellbeing repo_user_commit cicd deploy incident work_item_issue_type work_item_investment

echo "==> generating auth token for authenticated endpoints"
AUTH_TOKEN="$(generate_auth_token)"

echo "==> validating /health"
HEALTH_FILE="${TMP_DIR}/health.json"
fetch_json "/health" "${HEALTH_FILE}" "200"
"${BIN_DIR}/cigate" live-e2e assert-health "${HEALTH_FILE}"

# CHAOS-6241: /api/v1/meta and /api/v1/home are Go-served in prod --
# main.py's bodies for both now unconditionally raise
# GoServedRouteUnavailableError, so validating them against the Python api
# process (BASE_URL) would only prove that sentinel fires, not that the
# pipeline actually works. query_api_e2e_start boots the real query-api
# binary (GO_API_HOME_ENABLED/GO_API_META_ENABLED on) that
# run_go_api_prove_e2e's GraphQL proof reuses below -- one process, one
# port, for both checks.
echo "==> starting query-api for the CHAOS-6241 REST checks (and the go-api-prove run below)"
query_api_e2e_start
QUERY_API_BASE_URL="http://127.0.0.1:${QUERY_API_PORT}"

echo "==> validating /api/v1/meta (query-api)"
META_FILE="${TMP_DIR}/meta.json"
# Public route (meta_route.go: "no envelope verifier dependency") -- no bearer.
fetch_json "/api/v1/meta" "${META_FILE}" "200" "${QUERY_API_BASE_URL}" ""
"${BIN_DIR}/cigate" live-e2e assert-meta "${META_FILE}"

# CHAOS-5362: repos_covered_pct (backed by repo_metrics_daily, written by
# the native repo_user_commit executor) is now REAL pipeline proof, not
# fixture-backed -- it reads back rows the seed_and_finalize_sync_targets /
# start_worker_stack pipeline above actually produced via the real Go
# worker. prs_linked_to_issues_pct/issues_with_cycle_states_pct (backed by
# work_item_cycle_times) are NOT covered by that pipeline and previously
# came from the now-removed --with-metrics call's Python compute; their
# current provenance after that removal was not re-verified here (RISK:
# assert_home.py may need updating if those two fields regress in CI).
echo "==> validating /api/v1/home (query-api)"
HOME_FILE="${TMP_DIR}/home.json"
QUERY_API_ENVELOPE_TOKEN="$(query_api_e2e_mint_envelope_token "${E2E_ORG_ID}")"
fetch_json "/api/v1/home" "${HOME_FILE}" "200" "${QUERY_API_BASE_URL}" "${QUERY_API_ENVELOPE_TOKEN}"
"${BIN_DIR}/cigate" live-e2e assert-home "${HOME_FILE}"

echo "==> running go-api-prove end to end with the envelope and the edge access token minted in-process"
run_go_api_prove_e2e

echo "Live backend e2e checks passed."
