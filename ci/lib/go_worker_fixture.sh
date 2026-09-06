# shellcheck shell=bash
# CHAOS-5362: shared bare-binary Go worker/reconciler fixture mechanism.
#
# Extracted from ci/run_metrics_executed_proof.sh (CHAOS-4266) so
# ci/run_live_backend_e2e.sh can also drive the real Go dispatch/compute
# pipeline instead of writing derived metric rows directly into ClickHouse
# (which bypasses sync -> metrics.daily_dispatch -> the Go native executors
# entirely -- see the FIXTURE-BACKED comments in that script, and
# CHAOS-4263/CHAOS-4264, which ran undetected for a week because every check
# that existed asserted a job RAN rather than that it produced real rows via
# the real path).
#
# This file is SOURCED, never executed. It defines functions only; it does
# not set -e/-u/-o pipefail itself (the caller already has), and it does not
# read command-line arguments.
#
# CONTRACT: each function below documents the caller-set global variables it
# reads. This mirrors run_metrics_executed_proof.sh's own pre-extraction
# style (a single flat script relying on globals it set earlier), which is
# deliberate here: that script's own refactor to source this file must stay
# byte-for-byte identical in behavior, which is far easier to prove when the
# extracted functions are literally the same code operating on the same
# variable names, not a new parameter-passing convention. A caller that wants
# different values sets the same-named globals before calling -- it does not
# pass them as arguments.
#
# PID variables the daemon-start functions set (caller must declare them,
# typically as empty strings, before calling): API_PID, WORKER_PID,
# RECONCILER_PID. start_worker_stack/stop_worker_stack manage WORKER_PID and
# RECONCILER_PID; API_PID stays the caller's own responsibility (each script
# starts its API differently) -- stop it directly with stop_service, AFTER
# stop_worker_stack, to preserve the CHAOS-5025 kill order (worker ->
# reconciler -> api: the api is the worker's only client, so it must be
# signalled last).

# ---------------------------------------------------------------------------
# build_go_binaries -- builds the three Go binaries this fixture needs into
# BIN_DIR (caller-set global; created by the caller beforehand).
# ---------------------------------------------------------------------------
build_go_binaries() {
  echo "==> building Go binaries (dev-health-worker-migrate, dev-health-worker, dev-health-reconciler)"
  go build -o "${BIN_DIR}/dev-health-worker-migrate" ./cmd/dev-health-worker-migrate
  go build -o "${BIN_DIR}/dev-health-worker" ./cmd/dev-health-worker
  go build -o "${BIN_DIR}/dev-health-reconciler" ./cmd/dev-health-reconciler
}

# ---------------------------------------------------------------------------
# migrate_and_assert_river -- applies the Postgres (Alembic) migration with
# the River cutover opt-in, then fails closed immediately if the cutover did
# not actually take. Reads: POSTGRES_SUPERUSER_URI, POSTGRES_HOST/PORT/
# SUPERUSER/SUPERUSER_PASSWORD/DB, EXIT_FAILURE. Requires run_dev_hops() to
# already be defined by the caller.
# ---------------------------------------------------------------------------
migrate_and_assert_river() {
  echo "==> applying Postgres (Alembic) migrations"
  # DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 is required here, not optional: a
  # plain `migrate postgres upgrade` targets ONLY the application_schema
  # alembic branch and deliberately skips
  # 0066_activate_river_worker_job_routes, which leaves worker_job_routes
  # exactly as 0064 seeded it -- transport='celery', generation=1 -- for
  # metrics.daily_dispatch and every other non-canary kind. Without this
  # opt-in, the reconciler's generic outbox relay excludes any kind whose
  # transport is 'celery' from claiming regardless of what
  # contracts/jobs/v1/registry.json says the checked-in route is, and the
  # post-sync fanout's outbox row sits pending forever with no error (the
  # CHAOS-4266 root cause). Safe here only because this job's Postgres is its
  # own throwaway CI database, never a shared or production-adjacent one.
  DATABASE_URI="${POSTGRES_SUPERUSER_URI}" OTEL_ENABLED=false \
    DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 \
    run_dev_hops --db "${POSTGRES_SUPERUSER_URI}" migrate postgres upgrade

  # Fail closed, immediately, if the cutover above did not actually take:
  # this gate exists because a silently-wrong dispatch topology reads as a
  # clean green/red for an unrelated reason. Never let that happen
  # undetected -- confirm every metrics.* kind that must be executable is
  # actually on 'river' before doing anything else.
  echo "==> asserting the River cutover actually took for the metrics kinds this gate depends on"
  local non_river_metrics_routes
  non_river_metrics_routes="$(PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
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
  if [ -n "${non_river_metrics_routes}" ]; then
    echo "ERROR: the following metrics.* kinds are not routed to 'river' after the migration step above:"
    echo "${non_river_metrics_routes}"
    echo "ERROR: this gate cannot produce a meaningful result against the wrong dispatch topology (CHAOS-4266)."
    exit "${EXIT_FAILURE}"
  fi
  echo "   confirmed: all metrics.* kinds this gate depends on are routed to river"
}

# ---------------------------------------------------------------------------
# provision_river -- applies the ClickHouse migration, the three River
# runtime roles, the pinned River schema, and the sync-orchestration
# transport flip a fresh CI database needs before the Go worker can process
# anything. Call AFTER migrate_and_assert_river. Reads: CLICKHOUSE_URI_HTTP,
# POSTGRES_HOST/PORT/SUPERUSER/SUPERUSER_PASSWORD/DB, RIVER_DOMAIN_ROLE/
# QUEUE_ROLE/COORDINATOR_ROLE (+ their _PASSWORD counterparts), BIN_DIR,
# ROOT_DIR. Requires run_dev_hops() to already be defined by the caller.
# ---------------------------------------------------------------------------
provision_river() {
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
  # these to 'river' as an explicit, authorized cutover action, which a
  # fresh CI database has no history to inherit. Without this flip the
  # reconciler's outbox relay finds these kinds routed to celery and never
  # enqueues the River job that would let dev-health-worker process the
  # post_sync fanout at all. This is CI-only test-data setup on a throwaway
  # database, not a product code change.
  echo "==> routing sync-orchestration kinds to river for this CI run (fresh installs default to celery)"
  PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
    --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
    --set=ON_ERROR_STOP=1 \
    -c "UPDATE sync_dispatch_transport_routes SET transport='river', rollback_transport='celery', generation=generation+1, updated_at=now() WHERE kind IN ('post_sync','dispatch_sync_run','finalize_sync_run','reference_discovery') AND transport <> 'river';"
}

# ---------------------------------------------------------------------------
# wait_for_http_ready name url log_file pid_var_name -- polls url until it
# returns HTTP 200, or fails once the named pid dies or READINESS_ATTEMPTS is
# exhausted. Reads: READINESS_ATTEMPTS, READINESS_SLEEP_SECS.
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# start_worker_stack worker_log_file reconciler_log_file -- starts
# dev-health-worker (queues: metrics, sync) and dev-health-reconciler in the
# background, sets WORKER_PID/RECONCILER_PID, and waits for both /readyz
# endpoints. Brackets both launches with `set -m`/`set +m` itself (job
# control ON for the launches: without it a background job stays in THIS
# shell's process group, which makes stop_service()'s group-signal both
# useless and dangerous) -- callers do not need their own set -m/set +m.
#
# Reads: BIN_DIR, POSTGRES_HOST/PORT/DB, RIVER_DOMAIN_ROLE/QUEUE_ROLE/
# COORDINATOR_ROLE (+ passwords), CLICKHOUSE_URI_NATIVE, VALKEY_HOST/PORT,
# SETTINGS_ENCRYPTION_KEY, WORKER_OPERATIONAL_BRIDGE_TOKEN, API_PORT,
# WORKER_HTTP_PORT, RECONCILER_HTTP_PORT.
# ---------------------------------------------------------------------------
start_worker_stack() {
  local worker_log_file="$1" reconciler_log_file="$2"

  set -m

  echo "==> starting dev-health-worker (queues: metrics, sync)"
  (
    export POSTGRES_URI="postgresql://${RIVER_DOMAIN_ROLE}:${RIVER_DOMAIN_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
    export WORKER_DATABASE_URI="postgresql://${RIVER_QUEUE_ROLE}:${RIVER_QUEUE_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
    export CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}"
    export VALKEY_URI="redis://${VALKEY_HOST}:${VALKEY_PORT}/1"
    export SETTINGS_ENCRYPTION_KEY WORKER_OPERATIONAL_BRIDGE_TOKEN
    # --shutdown-timeout is 7260s BY CONTRACT -- do not "optimise" it down.
    # CHAOS-5025 tried 120s and then 30s on the theory that 2h1m was an
    # unbounded-teardown time bomb. It is not, and the worker REFUSES to
    # start below the contract (CHAOS-3873,
    # cmd/dev-health-worker/dependencies.go:1247+):
    #
    #     workerDrainBudget = shutdownTimeout - workerFinalizationBuffer(60s)
    #     require workerDrainBudget >= longestTimeout of the selected queues
    #     => shutdownTimeout >= longestTimeout + 60s
    #
    # For these queues longestTimeout is 7200s, so 7260s IS that minimum, not
    # a round number someone picked. At 30s the budget is -30s and startup
    # fails with `shutdown_timeout_below_drain_budget` -- which the caller
    # then reports as the far less helpful `queue_coverage_validation_failed`.
    #
    # It was never a teardown risk anyway: stop_service() SIGKILLs at
    # TEARDOWN_WAIT_SECS (60s) regardless of what the worker asks for, so the
    # harness bound dominates and the worker's own grace never decides how
    # long teardown takes.
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
  ) >"${worker_log_file}" 2>&1 &
  WORKER_PID="$!"

  echo "==> starting dev-health-reconciler"
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
  ) >"${reconciler_log_file}" 2>&1 &
  RECONCILER_PID="$!"

  set +m

  wait_for_http_ready "dev-health-worker" "http://127.0.0.1:${WORKER_HTTP_PORT}/readyz" "${worker_log_file}" WORKER_PID
  wait_for_http_ready "dev-health-reconciler" "http://127.0.0.1:${RECONCILER_HTTP_PORT}/readyz" "${reconciler_log_file}" RECONCILER_PID
}

# ---------------------------------------------------------------------------
# Teardown primitives. service_pgid/stop_service resolve and signal a
# service's own process group (only when it genuinely leads one -- an
# unverified pgid is never used), with a 1s-step SIGKILL watchdog bounding
# the whole thing at TEARDOWN_WAIT_SECS.
# ---------------------------------------------------------------------------

# validate_teardown_wait_secs -- fails closed to 60s (loudly) on a
# non-integer or out-of-range TEARDOWN_WAIT_SECS, exactly as the API-side
# parser does for its own env var. Call once, before installing traps.
validate_teardown_wait_secs() {
  if ! printf '%s' "${TEARDOWN_WAIT_SECS}" | grep -Eq '^[0-9]+$' \
     || [ "${TEARDOWN_WAIT_SECS}" -lt 1 ] || [ "${TEARDOWN_WAIT_SECS}" -gt 3600 ]; then
    echo "WARNING: ignoring an invalid TEARDOWN_WAIT_SECS='${TEARDOWN_WAIT_SECS}' (want an integer 1..3600); using 60s." >&2
    TEARDOWN_WAIT_SECS=60
  fi
}

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
  case "${pid}" in ''|*[!0-9]*) return 0 ;; esac
  [ "${pid}" -gt 1 ] || return 0
  kill -0 "${pid}" >/dev/null 2>&1 || return 0

  local pgid=""
  pgid="$(service_pgid "${pid}")" || pgid=""

  if [ -n "${pgid}" ]; then kill -TERM -- -"${pgid}" >/dev/null 2>&1 || true; fi
  kill -TERM "${pid}" >/dev/null 2>&1 || true

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

  if [ -n "${pgid}" ]; then kill -KILL -- -"${pgid}" >/dev/null 2>&1 || true; fi

  kill -KILL "${watchdog}" >/dev/null 2>&1 || true
  wait "${watchdog}" >/dev/null 2>&1 || true

  if [ "${elapsed}" -ge "${TEARDOWN_WAIT_SECS}" ]; then
    echo "WARNING: ${name} (pid ${pid}) ignored SIGTERM for ${TEARDOWN_WAIT_SECS}s; escalated to SIGKILL." >&2
  fi
}

# stop_worker_stack -- stops worker, then reconciler, in that order
# (CHAOS-5025 partial order; the caller stops its own API_PID via
# stop_service directly AFTER calling this, to complete the full worker ->
# reconciler -> api kill order: dev-health-worker is the API's only client
# via --operational-bridge-url, so the API must be signalled last or its
# shutdown never converges while the worker keeps issuing bridge calls into
# it). Reads: WORKER_PID, RECONCILER_PID.
stop_worker_stack() {
  stop_service "dev-health-worker" "${WORKER_PID}"
  stop_service "dev-health-reconciler" "${RECONCILER_PID}"
}

# ---------------------------------------------------------------------------
# seed_and_finalize_sync_targets target... -- seeds real source rows for
# every target through the real sync path (dev-hops sync <target> --provider
# synthetic --defer-finalize), THEN finalizes every target's deferred
# sync_run (dev-hops sync finalize-synthetic-sync), triggering
# NativePostSyncService.Fanout. Reads: ORG_ID, CLICKHOUSE_URI_HTTP,
# POSTGRES_SUPERUSER_URI, REPO_NAME, BACKFILL_DAYS.
#
# ORDERING CONTRACT (CHAOS-4266), enforced INSIDE this function rather than
# left to the caller: every target must be seeded before any of them is
# finalized. A sync_run's finalize is what triggers the post_sync fanout, and
# the fanout dispatches a remaining-metric family (dora needs
# cicd+deployments+incidents) off the FIRST qualifying sync_run to finalize,
# with no retry once the rest of the data later lands -- finalizing a target
# immediately after seeding it races that dispatch against whichever target
# happens to be seeded first.
# ---------------------------------------------------------------------------
seed_and_finalize_sync_targets() {
  echo "==> seeding real source rows through the real sync path (dev-hops sync <target> --provider synthetic --defer-finalize)"
  local target
  for target in "$@"; do
    echo "   -- ${target}"
    # DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1: this path writes to the global,
    # org-unscoped CHAOS-4114 executed-proof ledger under the real "gitlab"
    # provider identity -- safe only because this job's Postgres is its own
    # throwaway database, never a shared or production-adjacent one.
    ORG_ID="${ORG_ID}" CLICKHOUSE_URI="${CLICKHOUSE_URI_HTTP}" DATABASE_URI="${POSTGRES_SUPERUSER_URI}" OTEL_ENABLED=false \
      DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1 \
      run_dev_hops sync "${target}" \
      --provider synthetic \
      --repo-name "${REPO_NAME}" \
      --backfill "${BACKFILL_DAYS}" \
      --defer-finalize
  done

  echo "==> finalizing the deferred sync_runs (dev-hops sync finalize-synthetic-sync), now that every target's rows exist"
  for target in "$@"; do
    echo "   -- ${target}"
    ORG_ID="${ORG_ID}" DATABASE_URI="${POSTGRES_SUPERUSER_URI}" OTEL_ENABLED=false \
      DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1 \
      run_dev_hops sync finalize-synthetic-sync \
      --target "${target}" \
      --repo-name "${REPO_NAME}" \
      --backfill "${BACKFILL_DAYS}"
  done
}

# ---------------------------------------------------------------------------
# wait_for_native_family_telemetry family... -- confirms rows for each named
# family came from the Go native executor, not a Python fail-open fallback.
# A green ClickHouse readback alone cannot distinguish the two;
# worker_daily_metrics_native_family_outcome_total{family,outcome="computed"}
# is the only signal that names which path ran. Reads the worker's own
# /metrics endpoint (WORKER_HTTP_PORT) while it is still running -- callers
# must invoke this before tearing the worker down.
#
# Reads the snapshot TWICE (team-lead ruling): once immediately, once after a
# bounded wait (NATIVE_TELEMETRY_WAIT_SECS/NATIVE_TELEMETRY_POLL_SECS,
# default 60/5). A family whose rows Python wrote never produces a computed
# sample no matter how long this waits -- the Go executor never observed
# anything -- so waiting can only convert "not yet" into "computed"; it
# cannot manufacture a false pass. Exits 1 (not just returns) on failure,
# after dumping every checked family's raw series, because the worker is
# torn down by the caller's own cleanup trap moments after this returns and
# the evidence would otherwise be lost with it.
# ---------------------------------------------------------------------------
wait_for_native_family_telemetry() {
  local families=("$@")
  local wait_secs="${NATIVE_TELEMETRY_WAIT_SECS:-60}"
  local poll_secs="${NATIVE_TELEMETRY_POLL_SECS:-5}"

  # bash has no local-scoped functions -- these three are defined at global
  # scope on every call (harmless: always redefined identically, and this
  # function itself is only ever called once per process).
  read_snapshot() {
    curl -sS "http://127.0.0.1:${WORKER_HTTP_PORT}/metrics" 2>/dev/null || true
  }
  # `grep -c`, never `grep -q`: -q exits on the first match and closes the
  # pipe under printf, which emits "printf: write error: Broken pipe" on
  # every successful family -- an error string on the success path.
  family_computed() {
    local snapshot="$1" family="$2" hits
    hits="$(
      printf '%s\n' "${snapshot}" \
        | grep -cE "^worker_daily_metrics_native_family_outcome_total\{[^}]*family=\"${family}\"[^}]*outcome=\"computed\"[^}]*\} [1-9]" || true
    )"
    [ "${hits}" -gt 0 ]
  }
  missing_families() {
    local snapshot="$1" missing="" family
    for family in "${families[@]}"; do
      family_computed "${snapshot}" "${family}" || missing="${missing} ${family}"
    done
    printf '%s' "${missing}"
  }

  local snapshot_t0
  snapshot_t0="$(read_snapshot)"
  if [ -z "${snapshot_t0}" ]; then
    echo "FAIL: worker /metrics returned nothing on port ${WORKER_HTTP_PORT} -- cannot prove"
    echo "      which path computed these rows. A green readback alone does not"
    echo "      distinguish the native executor from the Python fail-open fallback."
    exit 1
  fi

  local missing_t0
  missing_t0="$(missing_families "${snapshot_t0}")"
  echo "  read 1 (T0):"
  local family
  for family in "${families[@]}"; do
    if family_computed "${snapshot_t0}" "${family}"; then
      echo "    ok   ${family}: outcome=computed"
    else
      echo "    --   ${family}: no outcome=computed sample yet"
    fi
  done

  local snapshot="${snapshot_t0}" waited=0
  if [ -n "${missing_t0}" ]; then
    echo "  waiting up to ${wait_secs}s for:${missing_t0}"
    while [ "${waited}" -lt "${wait_secs}" ]; do
      sleep "${poll_secs}"
      waited=$((waited + poll_secs))
      local next_snapshot
      next_snapshot="$(read_snapshot)"
      [ -n "${next_snapshot}" ] && snapshot="${next_snapshot}"
      [ -z "$(missing_families "${snapshot}")" ] && break
    done
  fi

  local still_missing
  still_missing="$(missing_families "${snapshot}")"
  echo "  read 2 (T0+${waited}s) -- THIS read decides pass/fail:"
  for family in "${families[@]}"; do
    if family_computed "${snapshot}" "${family}"; then
      case " ${missing_t0} " in
        *" ${family} "*)
          echo "    RACE ${family}: absent at T0, computed after ${waited}s."
          echo "         The gate read /metrics before this family finished. Not a"
          echo "         fail-open -- but the earlier read was reporting a false failure."
          ;;
        *) echo "    ok   ${family}: outcome=computed" ;;
      esac
    else
      echo "    FAIL ${family}: no outcome=computed sample after ${waited}s"
    fi
  done

  echo "--- native-family telemetry dump (all outcomes, every checked family)"
  for family in "${families[@]}"; do
    local family_series
    family_series="$(
      printf '%s\n' "${snapshot}" \
        | grep -E "^worker_daily_metrics_native_family_(outcome_total|rows_written_total)\{[^}]*family=\"${family}\"" || true
    )"
    if [ -z "${family_series}" ]; then
      echo "    ${family}: NO SERIES AT ALL -- not registered in"
      echo "               dailyMetricsNativeFamilies, so every observation for it was"
      echo "               refused by the collector and its absence is invisible."
    else
      printf '%s\n' "${family_series}" | sed 's/^/    /'
    fi
  done
  echo "--- end dump"

  if [ -n "${still_missing}" ]; then
    echo "FAIL: native-family telemetry missing for:${still_missing}"
    echo "      A green readback proves rows LANDED; this proves the Go executor is"
    echo "      what wrote them. Without it, a green gate is satisfied by Python's"
    echo "      fail-open fallback, which is precisely the regression this exists to catch."
    exit 1
  fi
}
