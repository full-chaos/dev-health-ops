#!/usr/bin/env bash
set -euo pipefail

# CHAOS-4266: executed-proof gate for the metrics family. Unlike
# run_live_backend_e2e.sh's `--with-metrics` fixture path (which writes
# derived metric rows directly into ClickHouse, bypassing sync entirely --
# see the FIXTURE-BACKED comments in that script), this job drives the REAL
# pipeline: seed real source rows through the real sync path (cicd/
# deployments/incidents/tests via `dev-hops sync <target> --provider
# synthetic`, CHAOS-4266's sync.py extension), let the REAL Go worker +
# reconciler process the resulting sync_dispatch_outbox(kind=post_sync) row
# exactly as a real provider sync would, then assert rows landed in
# ClickHouse (ci/assert_metrics_executed_proof.py).
#
# This is deliberately RED on current main: CHAOS-4263 (daily_metrics_
# partitions built from the wrong id space) means the partition this run
# produces carries repo_ids=[] even though the seeded org has a real repo in
# ClickHouse, so no family computes any rows. It goes green once CHAOS-4263
# merges -- do not mark it a required check before that (RISK-NOTES).
#
# ORDERING CONTRACT (CHAOS-4266): all four synthetic targets are seeded with
# --defer-finalize BEFORE any of them is finalized (see the seeding section
# below). A sync_run's finalize is what triggers the real post_sync fanout,
# and the fanout dispatches a remaining-metric family (dora needs
# cicd+deployments+incidents) off the FIRST qualifying sync_run to finalize,
# with no retry once the rest of the data later lands -- finalizing targets
# one at a time, immediately after each seed, races that dispatch against
# whichever target happens to be seeded first. Do not collapse the two loops
# below back into one without re-verifying dora stays non-zero across
# multiple runs.
#
# RISK-NOTES: this is a LIGHTER topology than prod/local-dev's
# deploy/docker-compose/compose.go-workers.yml -- no PgBouncer, no
# transaction-mode pooling, direct role DSNs, only the "metrics" and "sync"
# queues (not the full investment/reports/workgraph "heavy" set, which this
# gate does not need and was not verified here). CHAOS-4261 owns the
# production grant-provisioning gate; this script provisions the same three
# roles for CI's own throwaway database via the same checked-in
# scripts/worker/provision_river_roles.sql, not a hand-rolled substitute.

EXIT_MISSING_DEP=3
EXIT_FAILURE=10

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}" || exit "${EXIT_FAILURE}"

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
  if command -v uv >/dev/null 2>&1; then
    uv run dev-hops "$@"
    return
  fi
  python3 -m dev_health_ops.cli "$@"
}

# exec-flavored counterpart for a long-running process (the api server): `exec
# run_dev_hops ...` does NOT work -- `exec` replaces the shell with an
# EXTERNAL executable named "run_dev_hops", which does not exist, and fails
# with "exec: run_dev_hops: not found" before ever reaching dev-hops. Confirmed
# the hard way: this was the actual cause of this job's first CI run failing,
# not CHAOS-4263.
exec_dev_hops() {
  if command -v dev-hops >/dev/null 2>&1; then
    exec dev-hops "$@"
  fi
  if command -v uv >/dev/null 2>&1; then
    exec uv run dev-hops "$@"
  fi
  exec python3 -m dev_health_ops.cli "$@"
}

require_cmd go
require_cmd psql
require_cmd curl

# ---------------------------------------------------------------------------
# Connection settings. Defaults match this job's own services: block in
# .github/workflows/live-e2e.yml; every value is overridable for local runs.
# ---------------------------------------------------------------------------
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_SUPERUSER="${POSTGRES_SUPERUSER:-postgres}"
POSTGRES_SUPERUSER_PASSWORD="${POSTGRES_SUPERUSER_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-test_db}"

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-ch}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-ch}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"

VALKEY_HOST="${VALKEY_HOST:-localhost}"
VALKEY_PORT="${VALKEY_PORT:-6379}"

# Analytics DSNs: Python's clickhouse-connect speaks HTTP (8123); Go's
# clickhouse-go speaks the native wire protocol (9000). Pointing the Go
# binaries at 8123 fails immediately with "ClickHouse readiness check failed"
# (confirmed by hand; see deploy/go-workers/README.md).
CLICKHOUSE_URI_HTTP="clickhouse://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/${CLICKHOUSE_DB}"
CLICKHOUSE_URI_NATIVE="clickhouse://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@${CLICKHOUSE_HOST}:${CLICKHOUSE_NATIVE_PORT}/${CLICKHOUSE_DB}"
POSTGRES_SUPERUSER_URI="postgresql+asyncpg://${POSTGRES_SUPERUSER}:${POSTGRES_SUPERUSER_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

RIVER_DOMAIN_ROLE="devhealth_domain"
RIVER_QUEUE_ROLE="devhealth_queue"
RIVER_COORDINATOR_ROLE="devhealth_coordinator"
RIVER_DOMAIN_PASSWORD="devhealth_domain"
RIVER_QUEUE_PASSWORD="devhealth_queue"
RIVER_COORDINATOR_PASSWORD="devhealth_coordinator"

SETTINGS_ENCRYPTION_KEY="${SETTINGS_ENCRYPTION_KEY:-ci-metrics-executed-proof-key}"
WORKER_OPERATIONAL_BRIDGE_TOKEN="${WORKER_OPERATIONAL_BRIDGE_TOKEN:-ci-metrics-executed-proof-bridge-token}"
ORG_ID="${METRICS_PROOF_ORG_ID:-c0ffee00-dead-4bee-8bad-f00dfeedface}"
REPO_NAME="${METRICS_PROOF_REPO_NAME:-ci-metrics-executed-proof/repo}"
BACKFILL_DAYS="${METRICS_PROOF_BACKFILL_DAYS:-7}"

API_PORT="${METRICS_PROOF_API_PORT:-18081}"
WORKER_HTTP_PORT="${METRICS_PROOF_WORKER_PORT:-18085}"
RECONCILER_HTTP_PORT="${METRICS_PROOF_RECONCILER_PORT:-18086}"

READINESS_ATTEMPTS="${METRICS_PROOF_READINESS_ATTEMPTS:-60}"
READINESS_SLEEP_SECS="${METRICS_PROOF_READINESS_SLEEP_SECS:-2}"
COMPUTE_WAIT_ATTEMPTS="${METRICS_PROOF_COMPUTE_WAIT_ATTEMPTS:-40}"
COMPUTE_WAIT_SLEEP_SECS="${METRICS_PROOF_COMPUTE_WAIT_SLEEP_SECS:-3}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/metrics-executed-proof.XXXXXX")"
BIN_DIR="${TMP_DIR}/bin"
mkdir -p "${BIN_DIR}"

API_PID=""
WORKER_PID=""
RECONCILER_PID=""

cleanup() {
  local rc=$?
  for pid in "${API_PID}" "${WORKER_PID}" "${RECONCILER_PID}"; do
    if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" >/dev/null 2>&1 || true
    fi
  done
  rm -rf "${TMP_DIR}" >/dev/null 2>&1 || true
  return "${rc}"
}
trap cleanup EXIT INT TERM

wait_for_http_ready() {
  local name="$1" url="$2" log_file="$3" pid_var_name="$4"
  local i
  for ((i = 1; i <= READINESS_ATTEMPTS; i++)); do
    if curl -sS -o /dev/null -w '%{http_code}' "${url}" 2>/dev/null | grep -q '^200$'; then
      echo "${name} ready after ${i} attempt(s)."
      return 0
    fi
    local pid="${!pid_var_name}"
    if [ -n "${pid}" ] && ! kill -0 "${pid}" >/dev/null 2>&1; then
      echo "ERROR: ${name} process exited before becoming ready."
      tail -n 200 "${log_file}" || true
      return 1
    fi
    sleep "${READINESS_SLEEP_SECS}"
  done
  echo "ERROR: Timed out waiting for ${name} readiness at ${url}."
  tail -n 200 "${log_file}" || true
  return 1
}

echo "==> building Go binaries (dev-health-worker-migrate, dev-health-worker, dev-health-reconciler)"
go build -o "${BIN_DIR}/dev-health-worker-migrate" ./cmd/dev-health-worker-migrate
go build -o "${BIN_DIR}/dev-health-worker" ./cmd/dev-health-worker
go build -o "${BIN_DIR}/dev-health-reconciler" ./cmd/dev-health-reconciler

echo "==> applying Postgres (Alembic) migrations"
# DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 is required here, not optional: a
# plain `migrate postgres upgrade` targets ONLY the application_schema
# alembic branch (src/dev_health_ops/migrate.py::_effective_postgres_upgrade_
# revision) and deliberately skips 0066_activate_river_worker_job_routes,
# which leaves worker_job_routes exactly as 0064 seeded it -- transport=
# 'celery', generation=1 -- for metrics.daily_dispatch and every other
# non-canary kind. That table is a SEPARATE gate from
# sync_dispatch_transport_routes below: the reconciler's generic outbox relay
# (internal/joboutbox/relay.go -> internal/jobroute.Controller.DeferredKinds/
# Resolve) reads worker_job_routes live on every step and excludes any kind
# whose transport is 'celery' from claiming, regardless of what
# contracts/jobs/v1/registry.json says the checked-in route is and regardless
# of which queues dev-health-worker selects. Without this opt-in,
# metrics.daily_dispatch's outbox row is inserted by the post-sync fanout and
# then sits pending forever -- no error, no log line, just zero "queue":
# "metrics" activity, which is exactly the CHAOS-4266 root cause found while
# chasing the proof job staying red after CHAOS-4263 merged. Safe here only
# because this job's Postgres is its own throwaway CI database, never a
# shared or production-adjacent one (same justification as
# DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN below).
DATABASE_URI="${POSTGRES_SUPERUSER_URI}" OTEL_ENABLED=false \
  DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 \
  run_dev_hops --db "${POSTGRES_SUPERUSER_URI}" migrate postgres upgrade

# Fail closed, immediately, if the cutover above did not actually take: this
# gate exists because a silently-wrong dispatch topology reads as a clean
# green/red on the assertion below for an unrelated reason (or, before this
# assert existed, as a red that looked identical to the CHAOS-4263 shape).
# Never let that happen again undetected -- confirm every metrics.* kind that
# must be executable is actually on 'river' before doing anything else.
echo "==> asserting the River cutover actually took for the metrics kinds this gate depends on"
NON_RIVER_METRICS_ROUTES="$(PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
  --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
  --tuples-only --no-align --field-separator=' -> ' \
  -c "SELECT job_kind, transport FROM worker_job_routes
      WHERE job_kind IN (
        'metrics.daily_dispatch', 'metrics.daily_partition', 'metrics.daily_finalize',
        'metrics.remaining.capacity', 'metrics.remaining.complexity', 'metrics.remaining.dora',
        'metrics.remaining.membership_backfill', 'metrics.remaining.recommendations',
        'metrics.remaining.release_impact'
      )
      AND transport <> 'river';")"
if [ -n "${NON_RIVER_METRICS_ROUTES}" ]; then
  echo "ERROR: the following metrics.* kinds are not routed to 'river' after the migration step above:"
  echo "${NON_RIVER_METRICS_ROUTES}"
  echo "ERROR: this gate cannot produce a meaningful result against the wrong dispatch topology (CHAOS-4266)."
  exit "${EXIT_FAILURE}"
fi
echo "   confirmed: all metrics.* kinds this gate depends on are routed to river"

echo "==> applying ClickHouse migrations"
CLICKHOUSE_URI="${CLICKHOUSE_URI_HTTP}" OTEL_ENABLED=false \
  run_dev_hops migrate clickhouse upgrade

echo "==> provisioning the three River runtime roles (scripts/worker/provision_river_roles.sql)"
PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
  --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
  --set=ON_ERROR_STOP=1 \
  --set=domain_role="${RIVER_DOMAIN_ROLE}" \
  --set=queue_role="${RIVER_QUEUE_ROLE}" \
  --set=coordinator_role="${RIVER_COORDINATOR_ROLE}" \
  --set=domain_password="${RIVER_DOMAIN_PASSWORD}" \
  --set=queue_password="${RIVER_QUEUE_PASSWORD}" \
  --set=coordinator_password="${RIVER_COORDINATOR_PASSWORD}" \
  --file="${ROOT_DIR}/scripts/worker/provision_river_roles.sql"

echo "==> applying the pinned River schema + per-table domain/queue/coordinator grants"
MIGRATION_DATABASE_URI="postgresql://${POSTGRES_SUPERUSER}:${POSTGRES_SUPERUSER_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}" \
  RIVER_DOMAIN_DATABASE_ROLE="${RIVER_DOMAIN_ROLE}" \
  RIVER_QUEUE_DATABASE_ROLE="${RIVER_QUEUE_ROLE}" \
  RIVER_COORDINATOR_DATABASE_ROLE="${RIVER_COORDINATOR_ROLE}" \
  "${BIN_DIR}/dev-health-worker-migrate"

# A fresh Alembic install creates sync_dispatch_transport_routes rows with
# transport='celery' for every sync-orchestration kind (post_sync,
# dispatch_sync_run, finalize_sync_run, reference_discovery) -- prod flips
# these to 'river' as an explicit, authorized cutover action (CHAOS-4026),
# which a fresh CI database has no history to inherit. Without this flip the
# reconciler's outbox relay (internal/joboutbox) finds these kinds routed to
# celery and never enqueues the River job that would let dev-health-worker
# process the post_sync fanout at all -- confirmed by hand: the relay is
# silent (no log line, no state change) against the un-flipped default, and
# reproducibly enqueues within c1s of being flipped. This is CI-only test-data
# setup on a throwaway database, not a product code change.
echo "==> routing sync-orchestration kinds to river for this CI run (fresh installs default to celery)"
PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
  --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
  --set=ON_ERROR_STOP=1 \
  -c "UPDATE sync_dispatch_transport_routes SET transport='river', rollback_transport='celery', generation=generation+1, updated_at=now() WHERE kind IN ('post_sync','dispatch_sync_run','finalize_sync_run','reference_discovery') AND transport <> 'river';"

echo "==> starting dev-hops api (the Go worker's operational bridge)"
JWT_SECRET_KEY="$(SETTINGS_ENCRYPTION_KEY="${SETTINGS_ENCRYPTION_KEY}" python3 -c "import hashlib, os; print(hashlib.sha256(os.environ['SETTINGS_ENCRYPTION_KEY'].encode()).hexdigest())")"
# Overridable to a path OUTSIDE TMP_DIR (matching LIVE_E2E_API_LOG_FILE in
# ci/run_live_backend_e2e.sh): cleanup() below unconditionally rm -rf's
# TMP_DIR on exit, which runs before the workflow's own "Upload logs" step,
# so logs left only in TMP_DIR are already gone by upload time (codex
# review, CHAOS-4266 -- confirmed on the first live CI run: all 4 artifacts
# reported "No files were found").
API_LOG_FILE="${METRICS_PROOF_API_LOG_FILE:-${TMP_DIR}/api.log}"
(
  export DATABASE_URI="${POSTGRES_SUPERUSER_URI}"
  export CLICKHOUSE_URI="${CLICKHOUSE_URI_HTTP}"
  export SETTINGS_ENCRYPTION_KEY WORKER_OPERATIONAL_BRIDGE_TOKEN JWT_SECRET_KEY
  export REDIS_URL="redis://${VALKEY_HOST}:${VALKEY_PORT}/0"
  export ENVIRONMENT=test
  export OTEL_ENABLED=false
  exec_dev_hops --db "${DATABASE_URI}" --analytics-db "${CLICKHOUSE_URI}" api --host 127.0.0.1 --port "${API_PORT}"
) >"${API_LOG_FILE}" 2>&1 &
API_PID="$!"
wait_for_http_ready "dev-hops api" "http://127.0.0.1:${API_PORT}/health" "${API_LOG_FILE}" API_PID

echo "==> starting dev-health-worker (queues: metrics, sync)"
WORKER_LOG_FILE="${METRICS_PROOF_WORKER_LOG_FILE:-${TMP_DIR}/worker.log}"
(
  export POSTGRES_URI="postgresql://${RIVER_DOMAIN_ROLE}:${RIVER_DOMAIN_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  export WORKER_DATABASE_URI="postgresql://${RIVER_QUEUE_ROLE}:${RIVER_QUEUE_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  export CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}"
  export VALKEY_URI="redis://${VALKEY_HOST}:${VALKEY_PORT}/1"
  export SETTINGS_ENCRYPTION_KEY WORKER_OPERATIONAL_BRIDGE_TOKEN
  exec "${BIN_DIR}/dev-health-worker" \
    --queues=metrics,sync \
    --queue-concurrency=metrics=2,sync=1 \
    --worker-group=heavy \
    --shutdown-timeout=7260s \
    --http-addr=":${WORKER_HTTP_PORT}" \
    --river-schema=river \
    --domain-database-role="${RIVER_DOMAIN_ROLE}" \
    --queue-database-role="${RIVER_QUEUE_ROLE}" \
    --queue-database-mode=session \
    --domain-transaction-pooler=false \
    --operational-bridge-url="http://127.0.0.1:${API_PORT}" \
    --operational-bridge-allow-insecure=true \
    --pagerduty-webhook-transport=celery \
    --log-level=info
) >"${WORKER_LOG_FILE}" 2>&1 &
WORKER_PID="$!"
wait_for_http_ready "dev-health-worker" "http://127.0.0.1:${WORKER_HTTP_PORT}/readyz" "${WORKER_LOG_FILE}" WORKER_PID

echo "==> starting dev-health-reconciler"
RECONCILER_LOG_FILE="${METRICS_PROOF_RECONCILER_LOG_FILE:-${TMP_DIR}/reconciler.log}"
(
  export POSTGRES_URI="postgresql://${RIVER_DOMAIN_ROLE}:${RIVER_DOMAIN_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  export WORKER_DATABASE_URI="postgresql://${RIVER_QUEUE_ROLE}:${RIVER_QUEUE_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  export COORDINATOR_DATABASE_URI="postgresql://${RIVER_COORDINATOR_ROLE}:${RIVER_COORDINATOR_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  exec "${BIN_DIR}/dev-health-reconciler" \
    --http-addr=":${RECONCILER_HTTP_PORT}" \
    --river-schema=river \
    --domain-database-role="${RIVER_DOMAIN_ROLE}" \
    --queue-database-role="${RIVER_QUEUE_ROLE}" \
    --coordinator-database-role="${RIVER_COORDINATOR_ROLE}" \
    --queue-database-mode=session \
    --coordinator-database-mode=session \
    --domain-transaction-pooler=false \
    --log-level=info
) >"${RECONCILER_LOG_FILE}" 2>&1 &
RECONCILER_PID="$!"
wait_for_http_ready "dev-health-reconciler" "http://127.0.0.1:${RECONCILER_HTTP_PORT}/readyz" "${RECONCILER_LOG_FILE}" RECONCILER_PID

RUN_START="$(python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat())')"
# Ordering contract (CHAOS-4266): seed ALL FOUR targets' ClickHouse rows
# BEFORE finalizing ANY of their sync_runs. NativePostSyncService.Fanout
# dispatches a remaining-metric family (dora needs cicd+deployments+
# incidents) off the FIRST qualifying sync_run to finalize -- it does not
# wait for a caller's other targets, and it never gets a second chance (no
# retry once the rest of the data later lands). Finalizing each target
# immediately after seeding it -- the original shape of this loop --
# therefore raced dora against whichever target the loop happened to seed
# first, and finalizing cicd/deployments/incidents in ANY order fails the
# same way if `tests` (not dora-relevant) is finalized in between.
# `--defer-finalize` (sync.py) writes rows without completing the sync_run;
# `finalize-synthetic-sync` (sync.py) completes it afterward, standalone.
# This mirrors how the real pipeline never fans out before a provider's own
# sync actually completes -- it is the CI seeding step that was out of
# order, not the dispatch logic.
echo "==> seeding real source rows through the real sync path (dev-hops sync <target> --provider synthetic --defer-finalize), run_start=${RUN_START}"
for target in cicd deployments incidents tests; do
  echo "   -- ${target}"
  # DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1: required by sync_synthetic_target
  # (codex review, CHAOS-4266 round 3) because this path writes to the
  # GLOBAL, org-unscoped CHAOS-4114 executed-proof ledger under the real
  # "gitlab" provider identity -- safe ONLY because this job's Postgres is
  # its own throwaway database, never a shared or production-adjacent one.
  ORG_ID="${ORG_ID}" CLICKHOUSE_URI="${CLICKHOUSE_URI_HTTP}" DATABASE_URI="${POSTGRES_SUPERUSER_URI}" OTEL_ENABLED=false \
    DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1 \
    run_dev_hops sync "${target}" \
    --provider synthetic \
    --repo-name "${REPO_NAME}" \
    --backfill "${BACKFILL_DAYS}" \
    --defer-finalize
done

echo "==> finalizing the 4 deferred sync_runs (dev-hops sync finalize-synthetic-sync), now that every target's rows exist"
for target in cicd deployments incidents tests; do
  echo "   -- ${target}"
  ORG_ID="${ORG_ID}" DATABASE_URI="${POSTGRES_SUPERUSER_URI}" OTEL_ENABLED=false \
    DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1 \
    run_dev_hops sync finalize-synthetic-sync \
    --target "${target}" \
    --repo-name "${REPO_NAME}" \
    --backfill "${BACKFILL_DAYS}"
done

echo "==> waiting up to $((COMPUTE_WAIT_ATTEMPTS * COMPUTE_WAIT_SLEEP_SECS))s for the post-sync fanout to reach ClickHouse"
ASSERT_SUMMARY_JSON="${METRICS_PROOF_SUMMARY_JSON_FILE:-${TMP_DIR}/family-summary.json}"
# Scoped to families this job's own seeding can causally satisfy (codex
# review): cicd/deploy/testops_pipeline/testops_test come directly from the
# cicd/deployments/tests targets seeded above; dora is computed from
# deployments+cicd+incidents (all seeded) per _DORA_TARGETS in
# post_sync_dispatch.py. repo_user_commit/complexity need git commit/PR
# history, which this job never seeds (only cicd/deployments/incidents/tests,
# matching the ticket's stated source families) -- asserting them here would
# fail forever, for a reason unrelated to CHAOS-4263, even after it merges.
assert_readback() {
  PYTHONPATH="${PYTHONPATH}" python3 "${ROOT_DIR}/ci/assert_metrics_executed_proof.py" \
    --clickhouse-uri "${CLICKHOUSE_URI_HTTP}" \
    --org-id "${ORG_ID}" \
    --run-start "${RUN_START}" \
    --families cicd deploy testops_pipeline testops_test dora \
    --summary-json "${ASSERT_SUMMARY_JSON}"
}

i=0
until [ "${i}" -ge "${COMPUTE_WAIT_ATTEMPTS}" ]; do
  if assert_readback >"${TMP_DIR}/assert.log" 2>&1; then
    echo "   readback succeeded after $((i * COMPUTE_WAIT_SLEEP_SECS))s"
    break
  fi
  i=$((i + 1))
  sleep "${COMPUTE_WAIT_SLEEP_SECS}"
done

echo "==> final readback assertion (this is the pass/fail signal for the job)"
cat "${TMP_DIR}/assert.log" 2>/dev/null || true
FINAL_RC=0
assert_readback || FINAL_RC=$?

if [ -f "${ASSERT_SUMMARY_JSON}" ]; then
  echo "==> per-family rows-written summary"
  cat "${ASSERT_SUMMARY_JSON}"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    # One line per family (rows_written_<family>=<n>) rather than the whole
    # JSON blob behind a `name<<DELIMITER` block: that delimiter syntax reads
    # as an unterminated heredoc to ci/*.sh's heredoc-pipe-wedge scanner
    # (tests/tooling/test_local_validate_heredocs.py), which does not parse
    # bash well enough to see it is inside a quoted `echo` string, not an
    # actual heredoc.
    python3 -c "
import json
summary = json.load(open('${ASSERT_SUMMARY_JSON}'))
for family, info in summary.items():
    print(f'rows_written_{family}={info[\"rows_written\"]}')
" >>"${GITHUB_OUTPUT}"
  fi
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "### metrics-executed-proof: per-family rows written"
      echo '```json'
      cat "${ASSERT_SUMMARY_JSON}"
      echo '```'
    } >>"${GITHUB_STEP_SUMMARY}"
  fi
fi

if [ "${FINAL_RC}" -ne 0 ]; then
  # Dispatch-path diagnostic dump (CHAOS-4266): the readback assertion only
  # ever sees ClickHouse, so a zero-rows failure is silent on WHICH step of
  # seed -> outbox -> river -> compute never happened. Dump the three tables
  # that answer that, in order: is the kind even routed to river
  # (worker_job_routes), did the sync-orchestration outbox actually relay
  # (sync_dispatch_outbox), and did a River job for the metrics/sync queues
  # ever get inserted or run (river.river_job). `|| true` throughout: this is
  # best-effort diagnostics on an already-failing run, never a reason to mask
  # the real FINAL_RC below.
  echo "==> diagnostic dump (readback failed; dumping dispatch-path state)"
  echo "--- worker_job_routes (metrics.* kinds; gates the reconciler's generic outbox relay) ---"
  PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
    --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
    -c "SELECT job_kind, transport, paused, generation, updated_at FROM worker_job_routes WHERE job_kind LIKE 'metrics.%' ORDER BY job_kind;" \
    || true
  echo "--- sync_dispatch_outbox (post_sync/dispatch_sync_run/finalize_sync_run/reference_discovery for this run's org) ---"
  PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
    --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
    -c "SELECT kind, status, attempts, last_error, created_at, updated_at FROM sync_dispatch_outbox WHERE org_id = '${ORG_ID}' ORDER BY created_at;" \
    || true
  echo "--- worker_job_outbox (metrics.* kinds; the generic outbox the relay above drains) ---"
  PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
    --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
    -c "SELECT job_kind, status, attempt_count, scheduled_at, created_at FROM worker_job_outbox WHERE job_kind LIKE 'metrics.%' ORDER BY created_at;" \
    || true
  echo "--- river.river_job (metrics/sync queues; did the relay ever insert one) ---"
  PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
    --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
    -c "SELECT kind, queue, state, errors, attempted_at, finalized_at FROM river.river_job WHERE queue IN ('metrics', 'sync') ORDER BY id;" \
    || true
fi

echo "==> worker / reconciler logs (tail)"
echo "--- worker ---"
tail -n 100 "${WORKER_LOG_FILE}" || true
echo "--- reconciler ---"
tail -n 100 "${RECONCILER_LOG_FILE}" || true

exit "${FINAL_RC}"
