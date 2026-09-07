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

PYTHONPATH="${ROOT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"
export PYTHONPATH

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: Required command '${cmd}' is not available."
    exit "${EXIT_MISSING_DEP}"
  fi
}

run_dev_hops() {
  if command -v dev-hops >/dev/null 2>&1; then
    dev-hops "$@"
    return
  fi
  # CHAOS-4411/4181/4407: `uv run dev-hops` here would trigger uv's own
  # implicit sync of the local editable project -- reintroducing both the
  # shared-cache lock (no UV_CACHE_DIR carries into a fresh `uv run`) and the
  # setuptools_scm worktree hang the AGENTS.md `--no-install-project` recipe
  # exists to avoid. The pure-module invocation needs neither.
  python3 -m dev_health_ops.cli "$@"
}

exec_dev_hops() {
  if command -v dev-hops >/dev/null 2>&1; then
    exec dev-hops "$@"
  fi
  # See run_dev_hops() above (CHAOS-4411/4181/4407) for why `uv run dev-hops`
  # is skipped here too.
  exec python3 -m dev_health_ops.cli "$@"
}

run_python() {
  if command -v python3 >/dev/null 2>&1; then
    python3 "$@"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    python "$@"
    return
  fi
  echo "ERROR: Python runtime not found."
  exit "${EXIT_MISSING_DEP}"
}

# ---------------------------------------------------------------------------
# The Python programs this harness runs, held as shell strings rather than
# here-documents.
#
# WHY NOT HERE-DOCUMENTS (CHAOS-3489, same class as CHAOS-3362 — do not
# "simplify" them back). Bash delivers a here-document by writing it into a
# pipe and only THEN forking the command that reads it, so the writing shell
# briefly holds both ends of that pipe itself. If the document does not fit in
# the pipe buffer, the write can never complete: nothing is reading, and
# because the writer owns the read end it cannot even get EPIPE. It blocks
# forever, before the interpreter is ever exec'd.
#
# Measured on this host with `cat >/dev/null <<EOF`, timeout 5:
#
#     400 bytes -> ok, 0.30s
#     512 bytes -> WEDGED
#    1024 bytes -> WEDGED
#    4000 bytes -> WEDGED
#
# while `lsof` reports those same pipes with the nominal 16384-byte capacity.
# Nominal is not actual: macOS hands out a small pipe buffer and defers
# expansion under kernel pipe-memory pressure, which a host running many
# concurrent agent sessions sits in persistently. Two of these programs were
# permanently over the line (2240 and 1201 bytes) and every one of them runs on
# the main path, so a wedge here stalls the whole harness with no output.
#
# Hosted CI runners have normal pipe buffers, so this never wedged in CI; it
# wedges on a developer machine, which is exactly the condition CHAOS-3362
# spent three days being misattributed to ClickHouse.
#
# The fix is to remove the pipe, not to shrink the programs: each is written to
# a private temp file with the `printf` BUILTIN (in-process, no pipe, no fork)
# and Python is handed that path. `cat >file <<EOF` would NOT help — the pipe
# IS the here-document delivery mechanism, not something the reader introduces,
# which is why the repro above uses `cat`. All seven are converted, not only
# the two over budget: 400 bytes is the largest size measured to COMPLETE, the
# 382-byte one below sits one edit away from it, and a mixed file invites the
# next stage to be added in the wedging shape.
#
# CONSTRAINT: these are single-quoted shell strings, so no program may contain
# a single-quote character. tests/tooling/test_local_validate_heredocs.py
# enforces that, checks each one compiles as Python, and budgets every
# here-document under ci/ at the measured 400 bytes.
# ---------------------------------------------------------------------------

PY_PROG_REDIS_PING='import os
import sys

import valkey

client = valkey.from_url(os.environ["REDIS_URL"])
try:
    client.ping()
except Exception:
    sys.exit(1)
'

# NOTE: the users INSERT binds auth_provider as a parameter rather than
# inlining the SQL literal it used to carry. Same value reaching the same
# column via the same SQLAlchemy text() call — the literal simply cannot be
# spelled inside a single-quoted shell string.
PY_PROG_AUTH_TOKEN='import hashlib, jwt, uuid, os
from datetime import datetime, timedelta, timezone

user_id = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
org_id = uuid.UUID(os.getenv("E2E_ORG_ID", "11111111-2222-4333-8444-555555555555"))

pg_uri = os.getenv("POSTGRES_URI", os.getenv("DATABASE_URI", ""))
if pg_uri:
    sync_uri = pg_uri.replace("+asyncpg", "", 1)
    from sqlalchemy import create_engine, text
    engine = create_engine(sync_uri)
    from dev_health_ops.models.git import Base
    import dev_health_ops.models.users  # register models
    Base.metadata.create_all(engine, checkfirst=True)
    now = datetime.now(timezone.utc).isoformat()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)"
            " VALUES (:id, :slug, :name, :settings, :tier, true, :now, :now)"
            " ON CONFLICT (id) DO NOTHING"
        ), {"id": str(org_id), "slug": "e2e-org", "name": "E2E Org",
            "settings": "{}", "tier": "enterprise", "now": now})
        conn.execute(text(
            "INSERT INTO users (id, email, is_active, is_verified, is_superuser, auth_provider, created_at, updated_at)"
            " VALUES (:id, :email, true, false, false, :auth_provider, :now, :now)"
            " ON CONFLICT (id) DO NOTHING"
        ), {"id": str(user_id), "email": "e2e@test.local", "auth_provider": "local", "now": now})
        conn.execute(text(
            "INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at)"
            " VALUES (:mid, :uid, :oid, :role, :now, :now)"
            " ON CONFLICT DO NOTHING"
        ), {"mid": str(uuid.uuid4()), "uid": str(user_id), "oid": str(org_id),
            "role": "admin", "now": now})
    engine.dispose()

enc_key = os.getenv("SETTINGS_ENCRYPTION_KEY", "dev-key-not-for-prod")
secret = hashlib.sha256(enc_key.encode()).hexdigest()
payload = {
    "sub": str(user_id),
    "email": "e2e@test.local",
    "org_id": str(org_id),
    "role": "admin",
    "is_superuser": False,
    "type": "access",
    "exp": datetime.now(timezone.utc) + timedelta(hours=1),
    "iat": datetime.now(timezone.utc),
    "jti": str(uuid.uuid4()),
}
print(jwt.encode(payload, secret, algorithm="HS256"))
'

PY_PROG_READY_HEALTH='import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text())
assert payload.get("status") == "ok", payload
services = payload.get("services", {})
assert services.get("clickhouse") == "ok", services
assert services.get("postgres") in ("ok", "not_configured"), services
'

PY_PROG_JWT_SECRET='import hashlib, os
enc_key = os.environ["ENC_KEY_INPUT"]
print(hashlib.sha256(enc_key.encode()).hexdigest())
'

PY_PROG_ASSERT_HEALTH='import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload["status"] == "ok", payload
services = payload.get("services", {})
assert services.get("clickhouse") == "ok", services
assert services.get("postgres") in ("ok", "not_configured"), services
'

PY_PROG_ASSERT_META='import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload.get("backend") == "clickhouse", payload
assert payload.get("limits", {}).get("max_days") == 365, payload
assert payload.get("limits", {}).get("max_repos") == 1000, payload
supported = payload.get("supported_endpoints", [])
assert "/api/v1/home" in supported, supported
'

PY_PROG_ASSERT_HOME='import json
import os
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
freshness = payload.get("freshness", {})
assert "last_ingested_at" in freshness, freshness
sources = freshness.get("sources", {})
allowed_states = {"ok", "degraded", "down", "stale", "unknown", "not_configured", "error"}
fixture_provider = os.environ.get("FIXTURE_PROVIDER", "github")
expected_sources = {"ci"}
if fixture_provider != "synthetic":
    expected_sources.add(fixture_provider)
assert set(sources) == expected_sources, sources
for key in expected_sources:
    assert key in sources, sources
    assert str(sources.get(key, "")).lower() in allowed_states, sources
coverage = freshness.get("coverage", {})
assert float(coverage.get("repos_covered_pct", 0.0)) > 0.0, coverage
deltas = payload.get("deltas", [])
assert len(deltas) >= 1, len(deltas)
for row in deltas:
    assert "metric" in row, row
    assert "value" in row, row
tiles = payload.get("tiles", {})
for key in ("understand", "measure", "align", "execute"):
    assert key in tiles, tiles
constraint = payload.get("constraint", {})
assert constraint.get("title"), constraint
assert constraint.get("evidence"), constraint
'

require_cmd curl

CLICKHOUSE_URI_DEFAULT="clickhouse://ch:ch@127.0.0.1:8123/default"
POSTGRES_URI_DEFAULT="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/test_db"
REDIS_URL_DEFAULT="redis://127.0.0.1:6379/0"

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
CLICKHOUSE_URI_NATIVE="clickhouse://ch:ch@127.0.0.1:${CLICKHOUSE_NATIVE_PORT}/default"

VALKEY_HOST="${LIVE_E2E_VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${LIVE_E2E_VALKEY_PORT:-6379}"

RIVER_DOMAIN_ROLE="devhealth_domain"
RIVER_QUEUE_ROLE="devhealth_queue"
RIVER_COORDINATOR_ROLE="devhealth_coordinator"
RIVER_DOMAIN_PASSWORD="devhealth_domain"
RIVER_QUEUE_PASSWORD="devhealth_queue"
RIVER_COORDINATOR_PASSWORD="devhealth_coordinator"

WORKER_OPERATIONAL_BRIDGE_TOKEN="${WORKER_OPERATIONAL_BRIDGE_TOKEN:-ci-live-e2e-bridge-token}"
# CHAOS-5362: previously only ever read inline as
# "${SETTINGS_ENCRYPTION_KEY:-dev-key-not-for-prod}" (never actually
# assigned as a shell variable) -- start_worker_stack's plain `export
# SETTINGS_ENCRYPTION_KEY` needs it to actually be set under `set -u`.
SETTINGS_ENCRYPTION_KEY="${SETTINGS_ENCRYPTION_KEY:-dev-key-not-for-prod}"
WORKER_HTTP_PORT="${LIVE_E2E_WORKER_PORT:-18085}"
RECONCILER_HTTP_PORT="${LIVE_E2E_RECONCILER_PORT:-18086}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/live-backend-e2e.XXXXXX")"
# CHAOS-5362: BIN_DIR for the Go binaries build_go_binaries builds.
BIN_DIR="${TMP_DIR}/bin"
mkdir -p "${BIN_DIR}"
API_LOG_FILE="${LIVE_E2E_API_LOG_FILE:-${TMP_DIR}/api.log}"
API_PID=""
WORKER_PID=""
RECONCILER_PID=""

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
  # Kill ORDER is load-bearing (CHAOS-5025): dev-health-worker is the API's
  # only client (--operational-bridge-url), so the API must be signalled
  # LAST -- stop_worker_stack drains worker then reconciler; the api stops
  # here, directly, afterward.
  stop_worker_stack
  stop_service "dev-hops api" "${API_PID}"
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

# A private SUBDIRECTORY of our own temp dir, not a shared /tmp and not TMP_DIR
# itself: `python <script>` puts the script directory on sys.path[0] (where
# `python -` put the CWD), so a stray .py sitting next to the script could
# shadow a real module. Keeping the programs away from the JSON payloads this
# harness also writes into TMP_DIR keeps that directory empty of anything but
# our own programs. Removed by cleanup() along with TMP_DIR.
PY_PROGRAM_DIR="${TMP_DIR}/py"

write_py_program() {
  local name="$1"
  local text="$2"
  # `printf` is a BUILTIN writing straight to the redirected file: no pipe, no
  # fork, nothing that can block. Using `cat >file <<EOF` here would
  # reintroduce exactly the here-document pipe this change exists to remove.
  if ! printf '%s' "${text}" >"${PY_PROGRAM_DIR}/${name}"; then
    echo "ERROR: could not write ${PY_PROGRAM_DIR}/${name}."
    return 1
  fi
}

write_py_programs() {
  if ! mkdir -p "${PY_PROGRAM_DIR}"; then
    echo "ERROR: could not create ${PY_PROGRAM_DIR}."
    return 1
  fi
  write_py_program redis_ping.py "${PY_PROG_REDIS_PING}"
  write_py_program auth_token.py "${PY_PROG_AUTH_TOKEN}"
  write_py_program ready_health.py "${PY_PROG_READY_HEALTH}"
  write_py_program jwt_secret.py "${PY_PROG_JWT_SECRET}"
  write_py_program assert_health.py "${PY_PROG_ASSERT_HEALTH}"
  write_py_program assert_meta.py "${PY_PROG_ASSERT_META}"
  write_py_program assert_home.py "${PY_PROG_ASSERT_HOME}"
}

# Materialize before any stage runs: every stage below hands Python a path from
# this directory.
write_py_programs

wait_for_redis() {
  # CHAOS-2702: fail fast (before burning time on fixture generation / API
  # boot) if the Valkey service container isn't reachable yet. Uses the
  # `valkey` python client (already a project dependency, see
  # tests/test_external_ingest_customer_push_live.py) instead of requiring a
  # `valkey-cli`/`redis-cli` binary on the runner.
  local i
  for ((i = 1; i <= READINESS_ATTEMPTS; i++)); do
    if REDIS_URL="${REDIS_URL}" run_python "${PY_PROGRAM_DIR}/redis_ping.py"; then
      echo "Valkey ready after ${i} attempt(s)."
      return 0
    fi
    sleep "${READINESS_SLEEP_SECS}"
  done
  echo "ERROR: Timed out waiting for Valkey readiness at ${REDIS_URL}."
  return 1
}

generate_auth_token() {
  run_python "${PY_PROGRAM_DIR}/auth_token.py"
}

fetch_json() {
  local path="$1"
  local out_file="$2"
  local expected_status="$3"
  local extra_headers=()
  if [ -n "${AUTH_TOKEN:-}" ]; then
    extra_headers+=(-H "Authorization: Bearer ${AUTH_TOKEN}")
  fi
  local status
  status="$(
    curl -sS -o "${out_file}" -w "%{http_code}" \
      -H "Accept: application/json" \
      "${extra_headers[@]}" \
      "${BASE_URL}${path}"
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
      if [ "${health_status}" = "200" ] && run_python "${PY_PROGRAM_DIR}/ready_health.py" "${readiness_file}"
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
# .github/workflows/live-e2e.yml and ci/assert_metrics_executed_proof.py.
echo "==> generating deterministic ClickHouse fixtures (raw git/PR/team data only)"
(
  export ORG_ID="${E2E_ORG_ID}"
  unset POSTGRES_URI
  unset DATABASE_URI
  unset DATABASE_URL
  run_dev_hops fixtures generate \
    --sink "${CLICKHOUSE_URI}" \
    --db-type clickhouse \
    --repo-name "${FIXTURE_REPO_NAME}" \
    --provider "${FIXTURE_PROVIDER}" \
    --days "${FIXTURE_DAYS}" \
    --commits-per-day "${FIXTURE_COMMITS_PER_DAY}" \
    --pr-count "${FIXTURE_PR_COUNT}" \
    --seed "${FIXTURE_SEED}"
)

echo "==> migrating PostgreSQL for internal credential lifecycle coverage"
run_dev_hops --db "${POSTGRES_URI}" migrate postgres upgrade

echo "==> running service credential subprocess lifecycle against live PostgreSQL"
DEV_HEALTH_POSTGRES_TEST_URI="${POSTGRES_URI}" \
  run_python -m pytest \
  tests/test_service_credentials_cli.py::test_service_credential_create_emits_only_token_and_db_flag_is_honored \
  -q

# JWT_SECRET_KEY is now required (no SHA256 derivation fallback) — derive the same
# value generate_auth_token() uses so tokens match between API and e2e client.
if [ -z "${JWT_SECRET_KEY:-}" ]; then
  JWT_SECRET_KEY="$(
    ENC_KEY_INPUT="${SETTINGS_ENCRYPTION_KEY:-dev-key-not-for-prod}" run_python "${PY_PROGRAM_DIR}/jwt_secret.py"
  )"
  export JWT_SECRET_KEY
fi

echo "==> starting API at ${BASE_URL}"
(
  export DATABASE_URI="${DATABASE_URI}"
  export CLICKHOUSE_URI="${CLICKHOUSE_URI}"
  export POSTGRES_URI="${POSTGRES_URI}"
  export JWT_SECRET_KEY="${JWT_SECRET_KEY}"
  # CHAOS-5362: the Go worker started below authenticates its operational-
  # bridge calls back into this API with this shared token (--operational-
  # bridge-allow-insecure=true, start_worker_stack) -- the API must know the
  # same value to accept them, same as ci/run_metrics_executed_proof.sh's
  # own API-start block.
  export WORKER_OPERATIONAL_BRIDGE_TOKEN="${WORKER_OPERATIONAL_BRIDGE_TOKEN}"
  exec_dev_hops \
    --db "${POSTGRES_URI}" \
    --analytics-db "${CLICKHOUSE_URI}" \
    api --host "${API_HOST}" --port "${API_PORT}"
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
# throwaway Postgres, start dev-health-worker + dev-health-reconciler
# pointed at the API already running above (its --operational-bridge-url),
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

RUN_START="$(run_python -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat())')"
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
wait_for_native_family_telemetry team_wellbeing repo_user_commit cicd deploy incident

echo "==> generating auth token for authenticated endpoints"
AUTH_TOKEN="$(generate_auth_token)"

echo "==> validating /health"
HEALTH_FILE="${TMP_DIR}/health.json"
fetch_json "/health" "${HEALTH_FILE}" "200"
run_python "${PY_PROGRAM_DIR}/assert_health.py" "${HEALTH_FILE}"

echo "==> validating /api/v1/meta"
META_FILE="${TMP_DIR}/meta.json"
fetch_json "/api/v1/meta" "${META_FILE}" "200"
run_python "${PY_PROGRAM_DIR}/assert_meta.py" "${META_FILE}"

# CHAOS-5362: repos_covered_pct (backed by repo_metrics_daily, written by
# the native repo_user_commit executor) is now REAL pipeline proof, not
# fixture-backed -- it reads back rows the seed_and_finalize_sync_targets /
# start_worker_stack pipeline above actually produced via the real Go
# worker. prs_linked_to_issues_pct/issues_with_cycle_states_pct (backed by
# work_item_cycle_times) are NOT covered by that pipeline and previously
# came from the now-removed --with-metrics call's Python compute; their
# current provenance after that removal was not re-verified here (RISK:
# assert_home.py may need updating if those two fields regress in CI).
echo "==> validating /api/v1/home"
HOME_FILE="${TMP_DIR}/home.json"
fetch_json "/api/v1/home" "${HOME_FILE}" "200"
run_python "${PY_PROGRAM_DIR}/assert_home.py" "${HOME_FILE}"

echo "==> running customer-push external-ingest live e2e test (CHAOS-2702)"
(
  export DISABLE_DOTENV=1
  export CLICKHOUSE_URI="${CLICKHOUSE_URI}"
  export POSTGRES_URI="${POSTGRES_URI}"
  export DATABASE_URI="${DATABASE_URI}"
  export REDIS_URL="${REDIS_URL}"
  export JWT_SECRET_KEY="${JWT_SECRET_KEY}"
  # Point the test's black-box `client` fixture at the real, already-booted
  # `dev-hops api` server process (BASE_URL, computed above) instead of an
  # in-process ASGITransport -- proves the real route-mounting/startup/
  # uvicorn-config path, not just the FastAPI app object.
  export LIVE_E2E_BASE_URL="${BASE_URL}"
  run_python -m pytest tests/test_external_ingest_customer_push_live.py -m clickhouse -q
)

echo "Live backend e2e checks passed."
