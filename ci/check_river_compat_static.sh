#!/usr/bin/env bash
# Validate the River compatibility harness without starting its services.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
HARNESS="${ROOT}/tests/compatibility/river/run.sh"
RECORDER="${ROOT}/tests/compatibility/river/record.sh"
COMPOSE_FILE="${ROOT}/tests/compatibility/river/compose.compatibility.yml"
RESULTS="${ROOT}/ci/evidence/go-worker-migration/v1-river-spike/local-harness-results.json"
GO_WORKFLOW="${ROOT}/.github/workflows/go.yml"

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
bash -n "${RECORDER}"
shellcheck "${HARNESS}" "${RECORDER}" "${ROOT}/ci/check_go.sh" "${BASH_SOURCE[0]}"
# shellcheck disable=SC2016 # This is a literal source-contract assertion.
grep -F 'if $mode == "direct" or $mode == "session" then' "${HARNESS}" >/dev/null || {
  printf 'ERROR: direct and session cancellation contracts must remain identical\n' >&2
  exit 1
}
grep -F 'pgbouncer-session-helm-smoke' "${HARNESS}" >/dev/null || {
  printf 'ERROR: the Helm PgBouncer startup smoke must run in the isolated harness\n' >&2
  exit 1
}
grep -F 'GREENLET_VERSION="3.5.0"' "${HARNESS}" >/dev/null || {
  printf 'ERROR: the Python async SQLAlchemy greenlet pin must remain preflight-validated\n' >&2
  exit 1
}
grep -F 'greenlet==3.5.0' "${GO_WORKFLOW}" >/dev/null || {
  printf 'ERROR: the hosted River compatibility job must install the greenlet pin explicitly\n' >&2
  exit 1
}
docker compose \
  --project-name rivercompat-static-check \
  --file "${COMPOSE_FILE}" \
  config --quiet

# CHAOS-4011: pgbouncer-session's backend budget (DEFAULT_POOL_SIZE +
# RESERVE_POOL_SIZE) must cover the SUM of every pgxpool the Go probe opens
# against it (probe.go's WorkerPoolMaxConns + InserterPoolMaxConns — two
# separate pools since the pool-budget structural fix, one per co-resident
# river.Client so neither's standing demand starves the other's). Session-mode
# PgBouncer pins one backend per client connection for that connection's
# whole life and never multiplexes between statements, so a probe that can
# reach that many concurrently-held connections needs at least that many
# backends provisioned up front; falling short queues the overflow
# connection until the harness's bounded waits (20s crash-candidate start,
# 90s matrix timeout) expire. This is the second time a PgBouncer
# pool-budget mismatch has shipped a defect (prod river pool 37 was the
# first) — enforce the invariant instead of only fixing the instance.
# CHAOS-4011 (codex review): extract each constant into its own variable and
# require both explicitly, rather than concatenating both greps' output into
# one stream and summing whatever came out — if a rename ever silently broke
# one grep, that would leave the other's single value looking like a valid
# (but wrong, too-low) sum, and this check would keep passing.
worker_pool_max_conns="$(
  grep -oE 'WorkerPoolMaxConns = [0-9]+' "${ROOT}/tests/compatibility/river/go/probe.go" \
    | grep -oE '[0-9]+$'
)"
inserter_pool_max_conns="$(
  grep -oE 'InserterPoolMaxConns = [0-9]+' "${ROOT}/tests/compatibility/river/go/probe.go" \
    | grep -oE '[0-9]+$'
)"
if [ -z "${worker_pool_max_conns}" ]; then
  printf 'ERROR: cannot find WorkerPoolMaxConns in tests/compatibility/river/go/probe.go\n' >&2
  exit 1
fi
if [ -z "${inserter_pool_max_conns}" ]; then
  printf 'ERROR: cannot find InserterPoolMaxConns in tests/compatibility/river/go/probe.go\n' >&2
  exit 1
fi
probe_max_conns_sum=$((worker_pool_max_conns + inserter_pool_max_conns))
session_pool_budget="$(
  docker compose \
    --project-name rivercompat-static-check \
    --file "${COMPOSE_FILE}" \
    config --format json \
    | jq '
        .services["pgbouncer-session"].environment as $env
        | (($env.DEFAULT_POOL_SIZE // "0") | tonumber)
          + (($env.RESERVE_POOL_SIZE // "0") | tonumber)
      '
)"
if [ "${session_pool_budget}" -lt "${probe_max_conns_sum}" ]; then
  printf 'ERROR: pgbouncer-session DEFAULT_POOL_SIZE+RESERVE_POOL_SIZE (%s) in %s is below probe.go WorkerPoolMaxConns+InserterPoolMaxConns (%s) in %s; session-mode PgBouncer cannot service that many concurrently-held connections (CHAOS-4011)\n' \
    "${session_pool_budget}" \
    "tests/compatibility/river/compose.compatibility.yml" \
    "${probe_max_conns_sum}" \
    "tests/compatibility/river/go/probe.go" \
    >&2
  exit 1
fi

jq empty \
  "${ROOT}/ci/evidence/go-worker-migration/v0-celery-baseline/capture.json" \
  "${ROOT}/ci/evidence/go-worker-migration/v0-celery-baseline/local-resource-snapshot.json" \
  "${ROOT}/ci/evidence/go-worker-migration/v1-river-spike/compatibility-matrix.json" \
  "${RESULTS}"
# This block pins the FROZEN CHAOS-3034 spike snapshot in ${RESULTS}, not the
# repo's live toolchain/dependency pins -- that is why .versions.river below
# is "v0.40.0" while go.mod currently requires v0.44.0, and why .versions.go
# is "go1.25.9" even after ci/check_go.sh's GO_TOOLCHAIN and go.mod moved to
# go1.27.0 (CHAOS-4606). record.sh's OUTPUT writes this exact file, and its
# own GOTOOLCHAIN now tracks go1.27.0 too -- so the NEXT time someone runs
# record.sh to refresh this evidence for an unrelated reason, every
# .versions.* value below (go included) must be re-pinned here to match the
# newly recorded snapshot, the same way the providersync test-count and
# alembic-head literals get re-pinned when their upstream source changes.
jq -e '
  .schema_version == 1
  and .status == "complete_with_architecture_blocker"
  and .architecture_blocker == "poll_only_running_cancel_not_propagated"
  and (.profiles | type) == "array"
  and [.profiles[].mode] == ["direct", "poll-only", "session"]
  and (.gate_truth_table | keys) == ["direct", "poll_only", "session"]
  and .nested_n_minus_1.status == "pass"
  and (.nested_n_minus_1.phases | length) == 2
  and .versions.go == "go1.25.9"
  and .versions.river == "v0.40.0"
  and .versions.river_driver == "v0.40.0"
  and .versions.pgx == "v5.10.0"
  and .versions.river_n_minus_1 == "v0.39.0"
  and .versions.river_driver_n_minus_1 == "v0.39.0"
  and .versions.pgx_n_minus_1 == "v5.9.2"
  and .versions.python == "3.13.14"
  and .versions.riverqueue_python == "0.7.0"
  and .versions.sqlalchemy == "2.0.49"
  and .versions.asyncpg == "0.31.0"
  and .versions.greenlet == "3.5.0"
  and .gate_truth_table.direct.backend_connection_delta_at_most_six == true
  and .gate_truth_table.direct.canceled_acquires_zero == true
  and .gate_truth_table.direct.enqueue_p95_within_limit == true
  and .gate_truth_table.direct.new_connections_at_most_six == true
  and .gate_truth_table.direct.cross_client_running_cancel == true
  and .gate_truth_table.direct.same_client_running_cancel == null
  and .gate_truth_table.session.backend_connection_delta_at_most_six == true
  and .gate_truth_table.session.canceled_acquires_zero == true
  and .gate_truth_table.session.enqueue_p95_within_limit == true
  and .gate_truth_table.session.new_connections_at_most_six == true
  and .gate_truth_table.session.cross_client_running_cancel == true
  and .gate_truth_table.session.same_client_running_cancel == null
  and .gate_truth_table.poll_only.cross_client_running_cancel == false
  and .gate_truth_table.poll_only.same_client_running_cancel == false
  and .helm_pgbouncer_startup == {
    status: "pass",
    mode: "session",
    container_identity: "postgres_uid_gid_70",
    root_filesystem: "read_only",
    writable_config_path: "/etc/pgbouncer"
  }
  and all(.profiles[]; .python_transactions.scheduled_commit.job_contract.state == "scheduled")
  and .nested_n_minus_1.phases[1].current_insert.outcome == "inserted"
  and .nested_n_minus_1.phases[1].n_minus_one_consume.outcome == "completed"
  and .redaction.contains_credentials_or_dsns == false
' "${RESULTS}" >/dev/null

printf 'River compatibility static checks: clean\n'
