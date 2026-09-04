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
  # CHAOS-4411/4181/4407: `uv run dev-hops` here would trigger uv's own
  # implicit sync of the local editable project -- reintroducing both the
  # shared-cache lock (no UV_CACHE_DIR carries into a fresh `uv run`) and the
  # setuptools_scm worktree hang the AGENTS.md `--no-install-project` recipe
  # exists to avoid. The pure-module invocation needs neither.
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
  # See run_dev_hops() above (CHAOS-4411/4181/4407) for why `uv run dev-hops`
  # is skipped here too.
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

# CHAOS-5025: per-process teardown bound. The old cleanup() below did a bare
# `wait "${pid}"` with no bound at all, so ONE service that ignored SIGTERM
# pinned the whole job until GitHub's 6h cancel (run 33822295135: the proof
# itself PASSED at 00:41:40Z, then teardown hung for 5h55m).
TEARDOWN_WAIT_SECS="${METRICS_PROOF_TEARDOWN_WAIT_SECS:-60}"
# Validate it (codex r1 P2). An unvalidated value is not merely untidy: `[ x -lt
# abc ]` FAILS inside the watchdog's `while` and the `elapsed` `if`, and `set -e`
# does NOT exit on a failing condition in either context -- so a typo'd override
# silently turned graceful teardown into an IMMEDIATE SIGKILL with the escalation
# warning suppressed too (executed: `TEARDOWN_WAIT_SECS=abc` -> child killed, no
# WARNING line). Fail closed to the default, loudly, exactly as the API-side
# parser does for its own env var.
if ! printf '%s' "${TEARDOWN_WAIT_SECS}" | grep -Eq '^[0-9]+$' \
   || [ "${TEARDOWN_WAIT_SECS}" -lt 1 ] || [ "${TEARDOWN_WAIT_SECS}" -gt 3600 ]; then
  echo "WARNING: ignoring METRICS_PROOF_TEARDOWN_WAIT_SECS='${TEARDOWN_WAIT_SECS}' (want an integer 1..3600); using 60s." >&2
  TEARDOWN_WAIT_SECS=60
fi

# Resolve a service's process-GROUP id, but ONLY when it leads its own group.
#
# Why the check is load-bearing and not defensive noise: WITHOUT `set -m` a bash
# background job stays in the SCRIPT's own process group (measured: job pid
# 84573, pgid 84350 == the script's), so a bare `kill -- -"$pid"` would signal
# THIS SCRIPT. The services below are launched under `set -m` precisely so each
# leads its own group -- but nothing here may DEPEND on that having worked, so
# an unverified pgid is simply not used.
# It ALWAYS exits 0. The previous version ended on an `&&` chain, so the
# not-a-group-leader case returned 1 -- and under `set -e` the CALLER's
# `pgid="$(service_pgid ...)"` assignment then aborted stop_service() outright,
# before TERM, before the watchdog, before the fallback kill. The service was
# never signalled at all (codex r2 P2, executed: `worker=alive`). That made the
# fallback path strictly WORSE than no fix: a `set -m` that failed to take would
# have silently disabled teardown completely. A helper whose failure mode is
# "the caller silently stops" must not be able to fail.
service_pgid() {
  local pid="$1" pgid=""
  case "${pid}" in ''|*[!0-9]*) return 0 ;; esac
  [ "${pid}" -gt 1 ] || return 0
  pgid="$(ps -o pgid= -p "${pid}" 2>/dev/null | tr -d ' ')" || pgid=""
  if [ -n "${pgid}" ] && [ "${pgid}" = "${pid}" ]; then
    printf '%s' "${pgid}"
  fi
  return 0
}

stop_service() {
  local name="$1" pid="$2"
  [ -n "${pid}" ] || return 0
  # Refuse anything that is not a plausible child pid BEFORE any signal is sent
  # (codex r3 P3). service_pgid() already declines to resolve a group for pid<=1,
  # but that only suppressed the GROUP form -- the direct `kill -TERM "${pid}"`
  # below would still have fired, so running as root in a container this would
  # have signalled PID 1. A teardown helper must never be able to signal init.
  case "${pid}" in ''|*[!0-9]*) return 0 ;; esac
  [ "${pid}" -gt 1 ] || return 0
  kill -0 "${pid}" >/dev/null 2>&1 || return 0

  # Capture the group id NOW, while the leader is still alive. `ps` cannot report
  # it once the leader has been reaped, and the surviving descendants are exactly
  # the case this exists for -- the first version of this fix looked the pgid up
  # AFTER `wait` returned, got nothing, and left the child running (the very
  # defect it was written to close).
  local pgid=""
  # `|| pgid=""` even though service_pgid() cannot fail: this assignment is the
  # exact line that aborted teardown under errexit, so it does not rely on a
  # helper's exit status staying 0 forever.
  pgid="$(service_pgid "${pid}")" || pgid=""

  if [ -n "${pgid}" ]; then kill -TERM -- -"${pgid}" >/dev/null 2>&1 || true; fi
  kill -TERM "${pid}" >/dev/null 2>&1 || true

  # bash has no timed `wait`, and polling `kill -0` cannot tell a live process
  # from a not-yet-reaped zombie, so it would burn the full bound either way.
  # Arm a watchdog subshell instead: it SIGKILLs the service once the bound
  # expires, and THAT is what makes the `wait` below guaranteed to return.
  # It counts in 1s steps rather than one long `sleep`: cancelling the watchdog
  # SIGTERMs the subshell, which stops the SIGKILL from running (verified on
  # bigboy) but does NOT signal its `sleep` child, which survives reparented to
  # init for the rest of the interval. With one `sleep 60` that is a 60s orphan
  # per service in GitHub's "Terminate orphan process" list -- noise in exactly
  # the teardown log this change exists to make readable. 1s steps bound it to 1s
  # with no new dependency.
  local started="${SECONDS}"
  (
    waited=0
    while [ "${waited}" -lt "${TEARDOWN_WAIT_SECS}" ]; do
      sleep 1
      waited=$((waited + 1))
    done
    if [ -n "${pgid}" ]; then kill -KILL -- -"${pgid}" >/dev/null 2>&1 || true; fi
    kill -KILL "${pid}" >/dev/null 2>&1 || true
  ) &
  local watchdog="$!"

  wait "${pid}" >/dev/null 2>&1 || true
  local elapsed=$((SECONDS - started))

  # The leader is reaped, but descendants in its group can outlive it (codex r1
  # P2, executed: a wrapper that exits on TERM leaving a TERM-ignoring child made
  # the old cleanup() return with that child still running). Sweep the group.
  # NOT covered, deliberately: children the API starts with start_new_session=True
  # (worker_metrics.py's metric-compat subprocesses) are in their OWN session by
  # design, and no group signal from here can reach them -- that is the API's own
  # shutdown to perform, and it is precisely why the graceful-shutdown bound in
  # api/runner.py matters. See RISK-NOTES.
  if [ -n "${pgid}" ]; then kill -KILL -- -"${pgid}" >/dev/null 2>&1 || true; fi

  kill -TERM "${watchdog}" >/dev/null 2>&1 || true
  wait "${watchdog}" >/dev/null 2>&1 || true

  if [ "${elapsed}" -ge "${TEARDOWN_WAIT_SECS}" ]; then
    echo "WARNING: ${name} (pid ${pid}) ignored SIGTERM for ${TEARDOWN_WAIT_SECS}s; escalated to SIGKILL." >&2
  fi
}

CLEANUP_DONE=""
cleanup() {
  local rc=$?
  # Re-entrancy guard (codex r2 P3). The trap is installed for EXIT, INT and
  # TERM, so a SIGINT arriving while cleanup is already running re-enters it on
  # bash's function-return path -- observed: `pop_var_context: head of
  # shell_variables not a function context`, `local: can only be used in a
  # function`, and cleanup exiting 1 instead of completing. Run once, and drop
  # the traps on entry so the second signal cannot re-enter at all.
  if [ -n "${CLEANUP_DONE}" ]; then
    return "${rc}"
  fi
  CLEANUP_DONE=1
  # Block re-entry from a second signal while tearing down, but do NOT clear the
  # EXIT trap here -- on_signal() below manages that, because clearing EXIT from
  # inside cleanup is what let a cancelled run fall through and resume.
  trap '' INT TERM
  # Kill ORDER is load-bearing (CHAOS-5025), not cosmetic. dev-health-worker is
  # the API's only client (--operational-bridge-url below), so the API must be
  # signalled LAST. The old loop signalled it FIRST and then blocked on an
  # unbounded wait, which left the worker alive and still issuing bridge calls
  # into a shutting-down uvicorn -- 100 further job attempts over 16 minutes in
  # run 33822295135 -- so uvicorn's "waiting for connections to close" loop
  # never converged. Drain the clients first, then the server.
  stop_service "dev-health-worker" "${WORKER_PID}"
  stop_service "dev-health-reconciler" "${RECONCILER_PID}"
  stop_service "dev-hops api" "${API_PID}"
  rm -rf "${TMP_DIR}" >/dev/null 2>&1 || true
  return "${rc}"
}
# A trapped INT/TERM runs the handler and then RESUMES the script -- it does not
# exit (codex r3 P1). With `cleanup` bound directly to INT/TERM, a cancelled run
# tore down its services, deleted TMP_DIR, and then carried on executing the
# proof against nothing; the previous re-entrancy fix made it worse by also
# ignoring every later signal, so the run could no longer be stopped at all.
# Handle signals explicitly: tear down once, drop the EXIT trap so cleanup does
# not run twice, restore the signal's default disposition, and re-raise it so
# the script dies of the signal and reports 128+signum rather than continuing.
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

# Job control ON for the three service launches (codex r1 P2). Without `set -m`
# a background job stays in THIS script's process group, which makes a group
# signal in signal_service() both useless (it would not isolate the service) and
# dangerous (it would target the script). With it, each service's PGID == its
# PID, so the group form reaches the service's descendants and nothing else.
# Turned back off straight after the launches so the rest of the script keeps
# its normal non-job-control behaviour.
set -m

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
  # --shutdown-timeout is 7260s BY CONTRACT -- do not "optimise" it down.
  # CHAOS-5025 tried 120s and then 30s on the theory that 2h1m was an
  # unbounded-teardown time bomb. It is not, and the worker REFUSES to start
  # below the contract (CHAOS-3873, cmd/dev-health-worker/dependencies.go:1247+):
  #
  #     workerDrainBudget = shutdownTimeout - workerFinalizationBuffer(60s)
  #     require workerDrainBudget >= longestTimeout of the selected queues
  #     => shutdownTimeout >= longestTimeout + 60s
  #
  # For these queues longestTimeout is 7200s, so 7260s IS that minimum, not a
  # round number someone picked. At 30s the budget is -30s and startup fails
  # with `shutdown_timeout_below_drain_budget` -- which the caller then reports
  # as the far less helpful `queue_coverage_validation_failed`.
  #
  # It was never a teardown risk anyway: stop_service() SIGKILLs at
  # TEARDOWN_WAIT_SECS (60s) regardless of what the worker asks for, so the
  # harness bound dominates and the worker's own grace never decides how long
  # teardown takes. The H3 premise was simply wrong.
  #
  # NOTE: this comment lives ABOVE the command on purpose -- a `#` between
  # backslash-continued arguments would comment out every argument after it,
  # silently.
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

set +m

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

echo "   -- fixtures generate (CHAOS-4276: git_commits + a repo-pattern team for team_wellbeing/repo_user_commit)"
# chris's order (2026-08-26): use the EXISTING fixture system
# (src/dev_health_ops/fixtures/, dev-hops fixtures generate) rather than a
# new hand-rolled seeder. This writes git_commits/git_commit_stats via the
# SAME SyntheticDataGenerator the 4 targets above already use to insert_repo
# (repo_id derives from uuid5(namespace, repo_name), so --repo-name
# "${REPO_NAME}" lands on the identical repo row), plus `teams` rows whose
# repo_patterns are now populated from the real repo<->team ownership
# assignment (fixtures/runner.py run_fixtures_generation, extended in this
# PR -- see .remember/lanes/lane-fixtures-audit/fixtures-audit-2026-08-26.md
# section 3/5.2, which found repo_patterns was previously always [] for
# every fixture team). No sync_run, no DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN
# gate -- `fixtures generate` writes directly, independent of the
# sync_run/outbox/post_sync chain the loop above drives, so ordering
# relative to it is irrelevant.
#
# --team-count 1 (CHAOS-4276 codex round-1 finding 3): with repo-count 1 and
# team-count >= 2, _build_repo_team_assignments's own "every team must own at
# least one repo" fallback forces every team onto the SAME single repo, i.e.
# a multi-owner repo. RepoPatternTeamResolver's exact-match map (both the Go
# and Python implementations) holds exactly one team per repo pattern
# string, so seeding the same pattern for two co-owning teams makes
# whichever team was written last win and silently strands the other team's
# commits with no repo-pattern match -- the fixtures fix above deliberately
# does not emit a pattern for a multi-owner repo (see its own comment), so a
# 2-team/1-repo seed here would have exercised membership fallback instead
# of the repo-pattern-first path this job's comment above says it proves.
# One team keeps the repo genuinely single-owner.
ORG_ID="${ORG_ID}" CLICKHOUSE_URI="${CLICKHOUSE_URI_HTTP}" DATABASE_URI="${POSTGRES_SUPERUSER_URI}" OTEL_ENABLED=false \
  run_dev_hops fixtures generate \
  --sink "${CLICKHOUSE_URI_HTTP}" \
  --org "${ORG_ID}" \
  --repo-name "${REPO_NAME}" \
  --repo-count 1 \
  --days "${BACKFILL_DAYS}" \
  --team-count 1 \
  --seed 4276

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
# post_sync_dispatch.py. repo_user_commit/team_wellbeing (CHAOS-4276) come
# from the `dev-hops fixtures generate` call above (git_commits); team_
# wellbeing also needs a repo-pattern team, now populated by that same call
# (fixtures/runner.py run_fixtures_generation sets each team's repo_patterns
# from its real repo<->team ownership assignment -- CHAOS-4276), or every
# commit resolves to the less-meaningful "unassigned" bucket instead of
# actually exercising repo-pattern resolution. team_cognitive_load
# (CHAOS-4365 item 2) is derived entirely from this same run's
# user_metrics_daily + team_wellbeing rows plus team_repo_ownership (also
# seeded by the same fixtures call), so it is causally satisfied by the
# exact same seeding as team_wellbeing -- no separate fixture path needed.
# complexity is the one remaining exclusion: it needs persisted file
# CONTENTS (git_files.contents), which fixtures generate does not write by
# default -- asserting it here would fail forever, for a reason unrelated to
# CHAOS-4263, even after everything else merges.
assert_readback() {
  PYTHONPATH="${PYTHONPATH}" python3 "${ROOT_DIR}/ci/assert_metrics_executed_proof.py" \
    --clickhouse-uri "${CLICKHOUSE_URI_HTTP}" \
    --org-id "${ORG_ID}" \
    --run-start "${RUN_START}" \
    --families cicd deploy testops_pipeline testops_test dora repo_user_commit team_wellbeing team_cognitive_load \
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

echo "==> native-family telemetry proof (CHAOS-4276): confirms rows came from the Go executor, not the Python fail-open fallback"
# The readback JSON above proves rows LANDED; it cannot distinguish which
# path wrote them (TeamWellbeingExecutor vs. job_daily.py's Python compute
# after the native call fell open). worker_daily_metrics_native_family_
# outcome_total{family="team_wellbeing",outcome="computed"} is the ONLY
# signal that names which path actually ran (team-lead's ruling: without
# it, a green readback is not proof the native executor is the one being
# tested). Read from the worker's own /metrics endpoint while it is still
# running -- this must happen before the cleanup trap tears it down at
# script exit.
curl -sS "http://127.0.0.1:${WORKER_HTTP_PORT}/metrics" 2>/dev/null \
  | grep -E '^worker_daily_metrics_native_family_(outcome_total|rows_written_total)\{family="team_wellbeing"' \
  || echo "WARNING: no worker_daily_metrics_native_family_* series found for team_wellbeing"

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
