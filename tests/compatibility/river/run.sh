#!/usr/bin/env bash
# Run the isolated River v0.40 compatibility matrix. All command output is
# captured under a private temporary directory; stdout is reserved for the one
# sanitized JSON result emitted after the containers have been removed.
set -euo pipefail

umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd -P)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.compatibility.yml"
PYTHON_CLI="${SCRIPT_DIR}/python_enqueue.py"
PYTHON_BIN="${RIVER_COMPAT_PYTHON:-${REPO_ROOT}/.venv/bin/python}"

SAMPLES=20
GO_TOOLCHAIN="go1.25.9"
PYTHON_VERSION="3.13.14"
RIVERQUEUE_PYTHON_VERSION="0.7.0"
SQLALCHEMY_VERSION="2.0.49"
ASYNCPG_VERSION="0.31.0"
FETCH_POLL_INTERVAL="250ms"
CRASH_CANDIDATE_JOB_TIMEOUT="30s"
CRASH_CANDIDATE_RESCUE_AFTER="31s"
RESCUE_WORKER_JOB_TIMEOUT="250ms"
RESCUE_WORKER_STUCK_AFTER="750ms"
RUN_TIMEOUT="90s"

TEMP_DIR=""
GO_BINARY=""
N_MINUS_ONE_BINARY=""
COMPOSE_ATTEMPTED=0
CRASH_PID=""
CURRENT_PHASE="bootstrap"

usage() {
  cat <<'EOF'
Usage: tests/compatibility/river/run.sh

Runs the isolated River v0.40 compatibility harness and writes one sanitized
JSON document to stdout. Progress and bounded failure messages use stderr.

Environment:
  RIVER_COMPAT_PYTHON  Python executable with the locked dev dependencies.
                       Defaults to .venv/bin/python in the repository root.
EOF
}

die() {
  printf 'river compatibility harness: %s\n' "$1" >&2
  exit 1
}

progress() {
  printf 'river compatibility harness: %s\n' "$1" >&2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

if [ "$#" -gt 0 ]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
fi

require_command docker
require_command go
require_command jq
require_command mktemp

[ -f "${COMPOSE_FILE}" ] || die "the pinned Compose file is missing"
[ -f "${PYTHON_CLI}" ] || die "the Python compatibility CLI is missing"
[ -x "${PYTHON_BIN}" ] || die "the compatibility Python executable is unavailable"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required"

temp_root="${TMPDIR:-/tmp}"
TEMP_DIR="$(mktemp -d "${temp_root%/}/river-compat.XXXXXX")" || die "cannot create a private temporary directory"
GO_BINARY="${TEMP_DIR}/river-compat"
compose_project="rivercompat-${UID:-0}-$$-${RANDOM}"
compose=(
  docker compose
  --project-name "${compose_project}"
  --file "${COMPOSE_FILE}"
)

cleanup() {
  local status=$?

  trap - EXIT HUP INT TERM
  if [ "${status}" -ne 0 ]; then
    progress "exited during ${CURRENT_PHASE} with status ${status}"
  fi
  if [ -n "${CRASH_PID}" ] && kill -0 "${CRASH_PID}" >/dev/null 2>&1; then
    kill -KILL "${CRASH_PID}" >/dev/null 2>&1 || true
    wait "${CRASH_PID}" >/dev/null 2>&1 || true
  fi
  if [ "${COMPOSE_ATTEMPTED}" -eq 1 ]; then
    "${compose[@]}" down -v --remove-orphans \
      >"${TEMP_DIR}/compose-down.stdout" \
      2>"${TEMP_DIR}/compose-down.stderr" || true
  fi
  if [ -n "${TEMP_DIR}" ] && [ -d "${TEMP_DIR}" ]; then
    rm -rf -- "${TEMP_DIR}"
  fi
  exit "${status}"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

compose_down() {
  [ "${COMPOSE_ATTEMPTED}" -eq 1 ] || return 0
  if ! "${compose[@]}" down -v --remove-orphans \
    >"${TEMP_DIR}/compose-down.stdout" \
    2>"${TEMP_DIR}/compose-down.stderr"; then
    return 1
  fi
  COMPOSE_ATTEMPTED=0
}

resolve_local_port() {
  local service="$1"
  local container_port="$2"
  local mapping
  local port

  if ! mapping="$("${compose[@]}" port "${service}" "${container_port}" 2>"${TEMP_DIR}/${service}-port.stderr")"; then
    die "cannot resolve the dynamic ${service} port"
  fi
  case "${mapping}" in
    127.0.0.1:*) port="${mapping#127.0.0.1:}" ;;
    *) die "${service} is not bound to an isolated localhost port" ;;
  esac
  case "${port}" in
    ''|*[!0-9]*) die "${service} returned an invalid dynamic port" ;;
  esac
  printf '%s' "${port}"
}

assert_single_json_object() {
  local output_file="$1"
  jq -s -e 'length == 1 and (.[0] | type == "object")' "${output_file}" >/dev/null 2>&1
}

assert_all_emitted_gates() {
  local output_file="$1"
  jq -e '
    (.gates | to_entries) as $gates
    | ([$gates[] | select(.key | test("(?i)(cross.?client.*cancel|same.?client.*cancel|cancel.*client)") )]) as $cancel_gates
    | .status == "ok"
    and ($gates | length) > 0
    and all($gates[]; (.value | type) == "boolean")
    and (
      if ($cancel_gates | length) == 0 then
        all($gates[]; .value == true)
      elif .mode == "direct" then
        all($gates[]; .value == true)
      elif .mode == "poll-only" then
        all(
          $gates[];
          if (.key | test("(?i)(cross.?client.*cancel|same.?client.*cancel|cancel.*client)"))
          then .value == false
          else .value == true
          end
        )
        and any(
          $cancel_gates[];
          .key | test("(?i)(cross.?client.*cancel|cancel.*cross.?client)")
        )
        and any(
          $cancel_gates[];
          .key | test("(?i)(same.?client.*cancel|cancel.*same.?client)")
        )
      else
        false
      end
    )
  ' "${output_file}" >/dev/null 2>&1
}

report_gate_failure() {
  local output_file="$1"

  # This bounded diagnostic contains only aggregate booleans and counters. It
  # deliberately omits job rows, identifiers, payloads, ports, and connection
  # material so a failed gate is actionable without weakening redaction.
  jq -c '{
    mode,
    gates,
    execute_latency_ms: .workload.execute_latency_ms,
    postgres_delta: .postgres.delta,
    pool_delta: .pool.delta
  }' "${output_file}" >&2 || true
}

run_go_checked() {
  local label="$1"
  local database_url="$2"
  local output_file="$3"
  shift 3

  if ! RIVER_COMPAT_DATABASE_URL="${database_url}" "${GO_BINARY}" "$@" \
    >"${output_file}" 2>"${TEMP_DIR}/${label}.stderr"; then
    die "${label} failed; captured details were discarded"
  fi
  assert_single_json_object "${output_file}" || die "${label} emitted invalid JSON"
  if ! assert_all_emitted_gates "${output_file}"; then
    report_gate_failure "${output_file}"
    die "${label} emitted an unexpected gate truth table"
  fi
}

run_n_minus_one_checked() {
  local label="$1"
  local database_url="$2"
  local output_file="$3"
  shift 3

  if ! RIVER_COMPAT_DATABASE_URL="${database_url}" "${N_MINUS_ONE_BINARY}" "$@" \
    >"${output_file}" 2>"${TEMP_DIR}/${label}.stderr"; then
    die "${label} failed; captured details were discarded"
  fi
  assert_single_json_object "${output_file}" || die "${label} emitted invalid JSON"
}

assert_matrix() {
  local output_file="$1"
  local mode="$2"
  local poll_only="$3"
  local queue="$4"

  jq -e \
    --arg mode "${mode}" \
    --arg queue "${queue}" \
    --argjson poll_only "${poll_only}" \
    --argjson samples "${SAMPLES}" '
      .status == "ok"
      and .mode == $mode
      and .go_version == "go1.25.9"
      and .pgx_version == "v5.10.0"
      and .river_driver_version == "v0.40.0"
      and .river_version == "v0.40.0"
      and .poll_only == $poll_only
      and .migration.latest_version > 0
      and .migration.version_count > 0
      and .workload.execute.queue == $queue
      and .workload.execute.state == "completed"
      and .workload.execute.outcome == "completed"
      and .workload.execute_latency_ms.count == $samples
      and .workload.execute_latency_ms.within_limit == true
      and .workload.cancel.state == "cancelled"
      and (
        if $mode == "direct" then
          .workload.cancel.outcome == "running_context_cancelled_cross_client"
          and .workload.running_cancellation.cross_client_context_cancelled == true
          and .workload.running_cancellation.same_client_attempted == false
          and .workload.running_cancellation.same_client_context_cancelled == false
          and .workload.running_cancellation.probe_release_used == false
        else
          .workload.cancel.outcome == "running_cancel_not_propagated_probe_released"
          and .workload.running_cancellation.cross_client_context_cancelled == false
          and .workload.running_cancellation.same_client_attempted == true
          and .workload.running_cancellation.same_client_context_cancelled == false
          and .workload.running_cancellation.probe_release_used == true
        end
      )
      and .workload.recovery.state == "completed"
      and .workload.recovery.outcome == "completed_after_retry"
      and .workload.recovery.attempt == 2
      and .workload.recovery.error_count == 1
      and .workload.scheduled.state == "cancelled"
      and .workload.scheduled.scheduled == true
      and .workload.scheduled.outcome == "scheduled_state_observed"
    ' "${output_file}" >/dev/null 2>&1 || die "${mode} matrix contract assertion failed"
}

run_python_case() {
  local label="$1"
  local database_url="$2"
  local mode="$3"
  local marker="$4"
  local queue="$5"
  local pgbouncer="$6"
  local output_file="$7"
  local scheduled_delay_ms="${8:-0}"
  local args

  args=(
    --database-url "${database_url}"
    --mode "${mode}"
    --marker "${marker}"
    --queue "${queue}"
    --priority 2
    --max-attempts 7
  )
  if [ "${pgbouncer}" = true ]; then
    args+=(--pgbouncer)
  fi
  if [ "${scheduled_delay_ms}" -gt 0 ]; then
    args+=(--scheduled-delay-ms "${scheduled_delay_ms}")
  fi

  if ! "${PYTHON_BIN}" "${PYTHON_CLI}" "${args[@]}" \
    >"${output_file}" 2>"${TEMP_DIR}/${label}.stderr"; then
    die "${label} failed; captured details were discarded"
  fi
  assert_single_json_object "${output_file}" || die "${label} emitted invalid JSON"
}

assert_python_scheduled_commit() {
  local output_file="$1"
  local marker="$2"
  local queue="$3"

  jq -e --arg marker "${marker}" --arg queue "${queue}" '
    .mode == "commit"
    and .marker == $marker
    and .domain_count == 1
    and .job_count == 1
    and (.job_id | type == "number")
    and .job_contract == {
      contract_version: 1,
      max_attempts: 7,
      priority: 2,
      queue: $queue,
      scheduled_after_create: true,
      source: "python",
      state: "scheduled",
      tags: ["phase0", "python"]
    }
  ' "${output_file}" >/dev/null 2>&1 || die "Python scheduled-commit contract assertion failed"
}

assert_python_commit() {
  local output_file="$1"
  local marker="$2"
  local queue="$3"

  jq -e --arg marker "${marker}" --arg queue "${queue}" '
    .mode == "commit"
    and .marker == $marker
    and .domain_count == 1
    and .job_count == 1
    and (.job_id | type == "number")
    and .job_contract == {
      contract_version: 1,
      max_attempts: 7,
      priority: 2,
      queue: $queue,
      scheduled_after_create: false,
      source: "python",
      state: "available",
      tags: ["phase0", "python"]
    }
  ' "${output_file}" >/dev/null 2>&1 || die "Python commit contract assertion failed"
}

assert_python_rollback() {
  local output_file="$1"
  local marker="$2"

  jq -e --arg marker "${marker}" '
    .mode == "rollback"
    and .marker == $marker
    and .domain_count == 0
    and .job_count == 0
    and .job_contract == null
  ' "${output_file}" >/dev/null 2>&1 || die "Python rollback contract assertion failed"
}

assert_python_unique() {
  local output_file="$1"
  local marker="$2"

  jq -e --arg marker "${marker}" '
    .mode == "unique"
    and .marker == $marker
    and .status == "unsupported"
    and .reason_code == "river_0_40_unique_index_contract_missing"
    and .sqlstate == "42P10"
  ' "${output_file}" >/dev/null 2>&1 || die "Python unique-insert incompatibility assertion failed"
}

assert_external_consume() {
  local output_file="$1"
  local mode="$2"
  local source="$3"
  local queue="$4"
  local expected_attempt="$5"
  local expected_errors="$6"
  local expected_max_attempts="$7"

  jq -e \
    --arg mode "${mode}" \
    --arg source "${source}" \
    --arg queue "${queue}" \
    --argjson expected_attempt "${expected_attempt}" \
    --argjson expected_errors "${expected_errors}" \
    --argjson expected_max_attempts "${expected_max_attempts}" '
      .status == "ok"
      and .mode == $mode
      and .go_version == "go1.25.9"
      and .pgx_version == "v5.10.0"
      and .river_driver_version == "v0.40.0"
      and .river_version == "v0.40.0"
      and .workload.external.source == $source
      and .workload.external.queue == $queue
      and .workload.external.priority == 2
      and .workload.external.max_attempts == $expected_max_attempts
      and .workload.external.state == "completed"
      and .workload.external.outcome == "external_completed"
      and .workload.external.attempt == $expected_attempt
      and .workload.external.error_count == $expected_errors
    ' "${output_file}" >/dev/null 2>&1 || die "${mode} external-consume assertion failed"
}

run_crash_recovery() {
  local mode="$1"
  local database_url="$2"
  local queue="$3"
  local output_file="$4"
  local marker="crash-${mode}-${compose_project}-${RANDOM}"
  local candidate_stdout="${TEMP_DIR}/${mode}-crash-candidate.stdout"
  local candidate_stderr="${TEMP_DIR}/${mode}-crash-candidate.stderr"
  local rescue_stdout="${TEMP_DIR}/${mode}-crash-rescue.stdout"
  local deadline
  local crash_status

  CURRENT_PHASE="${mode}_crash_candidate_start"
  (
    export RIVER_COMPAT_DATABASE_URL="${database_url}"
    exec "${GO_BINARY}" \
      --operation crash-candidate \
      --mode "${mode}" \
      --marker "${marker}" \
      --queue "${queue}" \
      --max-attempts 3 \
      --fetch-poll-interval "${FETCH_POLL_INTERVAL}" \
      --job-timeout "${CRASH_CANDIDATE_JOB_TIMEOUT}" \
      --rescue-stuck-after "${CRASH_CANDIDATE_RESCUE_AFTER}" \
      --timeout 45s
  ) >"${candidate_stdout}" 2>"${candidate_stderr}" &
  CRASH_PID=$!

  CURRENT_PHASE="${mode}_crash_candidate_wait_for_start"
  deadline=$((SECONDS + 20))
  until jq -s -e \
    --arg mode "${mode}" \
    --arg marker "${marker}" '
      length >= 1
      and .[0].event == "started"
      and .[0].mode == $mode
      and .[0].marker == $marker
      and .[0].attempt == 1
      and (.[0].job_id | type == "number")
    ' "${candidate_stdout}" >/dev/null 2>&1; do
    if ! kill -0 "${CRASH_PID}" >/dev/null 2>&1; then
      wait "${CRASH_PID}" >/dev/null 2>&1 || true
      CRASH_PID=""
      die "${mode} crash candidate exited before its first attempt started"
    fi
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      kill -KILL "${CRASH_PID}" >/dev/null 2>&1 || true
      wait "${CRASH_PID}" >/dev/null 2>&1 || true
      CRASH_PID=""
      die "${mode} crash candidate did not start within the bounded wait"
    fi
    sleep 0.05
  done

  progress "${mode} crash candidate emitted the first-attempt signal"
  CURRENT_PHASE="${mode}_crash_candidate_sigkill"
  kill -KILL "${CRASH_PID}" >/dev/null 2>&1 || die "cannot SIGKILL the ${mode} crash candidate"
  CURRENT_PHASE="${mode}_crash_candidate_wait_for_sigkill"
  if wait "${CRASH_PID}" >/dev/null 2>&1; then
    crash_status=0
  else
    crash_status=$?
  fi
  CRASH_PID=""
  progress "${mode} crash candidate exited with the expected SIGKILL status"
  CURRENT_PHASE="${mode}_crash_candidate_validate_signal"
  [ "${crash_status}" -eq 137 ] || die "${mode} crash candidate was not terminated by SIGKILL"
  assert_single_json_object "${candidate_stdout}" || die "${mode} crash candidate emitted unexpected output"

  # The replacement worker's maintenance loop ticks immediately. Wait until
  # the row is older than the configured stuck threshold so that initial tick
  # performs a real River rescue, then executes attempt 2.
  CURRENT_PHASE="${mode}_crash_rescue_age_wait"
  sleep 1
  CURRENT_PHASE="${mode}_crash_rescue_worker"
  run_go_checked \
    "${mode}-crash-rescue" \
    "${database_url}" \
    "${rescue_stdout}" \
    --operation consume \
    --mode "${mode}" \
    --marker "${marker}" \
    --queue "${queue}" \
    --expected-attempt 2 \
    --fetch-poll-interval "${FETCH_POLL_INTERVAL}" \
    --job-timeout "${RESCUE_WORKER_JOB_TIMEOUT}" \
    --rescue-stuck-after "${RESCUE_WORKER_STUCK_AFTER}" \
    --timeout 45s
  CURRENT_PHASE="${mode}_crash_rescue_validate"
  assert_external_consume "${rescue_stdout}" "${mode}" "go" "${queue}" 2 1 3

  CURRENT_PHASE="${mode}_crash_result_assemble"
  jq -n \
    --slurpfile started "${candidate_stdout}" \
    --slurpfile rescue "${rescue_stdout}" \
    --argjson exit_code "${crash_status}" '{
      first_attempt: {
        event: $started[0].event,
        attempt: $started[0].attempt
      },
      termination: {
        signal: "SIGKILL",
        exit_code: $exit_code
      },
      rescue: $rescue[0]
    }' >"${output_file}"
  progress "${mode} crash rescue completed attempt 2"
}

run_mode_matrix() {
  local mode="$1"
  local database_url="$2"
  local poll_only="$3"
  local queue="$4"
  local output_file="$5"

  progress "running the ${mode} 20-sample Go matrix"
  run_go_checked \
    "${mode}-matrix" \
    "${database_url}" \
    "${output_file}" \
    --operation matrix \
    --mode "${mode}" \
    --queue "${queue}" \
    --priority 2 \
    --max-attempts 3 \
    --samples "${SAMPLES}" \
    --fetch-poll-interval "${FETCH_POLL_INTERVAL}" \
    --timeout "${RUN_TIMEOUT}"
  assert_matrix "${output_file}" "${mode}" "${poll_only}" "${queue}"
}

run_profile() {
  local mode="$1"
  local database_url="$2"
  local poll_only="$3"
  local transport="$4"
  local matrix_stdout="$5"
  local output_file="$6"
  local queue="chaos3034-${mode}"
  local commit_stdout="${TEMP_DIR}/${mode}-python-commit.stdout"
  local scheduled_stdout="${TEMP_DIR}/${mode}-python-scheduled.stdout"
  local rollback_stdout="${TEMP_DIR}/${mode}-python-rollback.stdout"
  local unique_stdout="${TEMP_DIR}/${mode}-python-unique.stdout"
  local consume_stdout="${TEMP_DIR}/${mode}-python-consume.stdout"
  local crash_stdout="${TEMP_DIR}/${mode}-crash-result.json"
  local commit_marker="python-commit-${mode}-${compose_project}-${RANDOM}"
  local scheduled_marker="python-scheduled-${mode}-${compose_project}-${RANDOM}"
  local rollback_marker="python-rollback-${mode}-${compose_project}-${RANDOM}"
  local unique_marker="python-unique-${mode}-${compose_project}-${RANDOM}"

  progress "running the ${mode} Python transaction and cross-language matrix"
  run_python_case \
    "${mode}-python-commit" \
    "${database_url}" \
    commit \
    "${commit_marker}" \
    "${queue}" \
    "${poll_only}" \
    "${commit_stdout}"
  assert_python_commit "${commit_stdout}" "${commit_marker}" "${queue}"

  run_go_checked \
    "${mode}-python-consume" \
    "${database_url}" \
    "${consume_stdout}" \
    --operation consume \
    --mode "${mode}" \
    --marker "${commit_marker}" \
    --queue "${queue}" \
    --expected-attempt 1 \
    --fetch-poll-interval "${FETCH_POLL_INTERVAL}" \
    --timeout 60s
  assert_external_consume "${consume_stdout}" "${mode}" "python" "${queue}" 1 0 7

  run_python_case \
    "${mode}-python-scheduled" \
    "${database_url}" \
    commit \
    "${scheduled_marker}" \
    "${queue}" \
    "${poll_only}" \
    "${scheduled_stdout}" \
    300000
  assert_python_scheduled_commit "${scheduled_stdout}" "${scheduled_marker}" "${queue}"

  run_python_case \
    "${mode}-python-rollback" \
    "${database_url}" \
    rollback \
    "${rollback_marker}" \
    "${queue}" \
    "${poll_only}" \
    "${rollback_stdout}"
  assert_python_rollback "${rollback_stdout}" "${rollback_marker}"

  run_python_case \
    "${mode}-python-unique" \
    "${database_url}" \
    unique \
    "${unique_marker}" \
    "${queue}" \
    "${poll_only}" \
    "${unique_stdout}"
  assert_python_unique "${unique_stdout}" "${unique_marker}"

  progress "running the ${mode} SIGKILL and attempt-2 rescue probe"
  run_crash_recovery "${mode}" "${database_url}" "${queue}" "${crash_stdout}"

  jq -n \
    --arg mode "${mode}" \
    --arg transport "${transport}" \
    --slurpfile matrix "${matrix_stdout}" \
    --slurpfile commit "${commit_stdout}" \
    --slurpfile scheduled "${scheduled_stdout}" \
    --slurpfile rollback "${rollback_stdout}" \
    --slurpfile unique "${unique_stdout}" \
    --slurpfile consume "${consume_stdout}" \
    --slurpfile crash "${crash_stdout}" '{
      mode: $mode,
      transport: $transport,
      matrix: $matrix[0],
      python_transactions: {
        commit: {
          mode: $commit[0].mode,
          domain_count: $commit[0].domain_count,
          job_count: $commit[0].job_count,
          job_contract: $commit[0].job_contract
        },
        scheduled_commit: {
          mode: $scheduled[0].mode,
          domain_count: $scheduled[0].domain_count,
          job_count: $scheduled[0].job_count,
          job_contract: $scheduled[0].job_contract
        },
        rollback: {
          mode: $rollback[0].mode,
          domain_count: $rollback[0].domain_count,
          job_count: $rollback[0].job_count,
          job_contract: $rollback[0].job_contract
        },
        unique: {
          mode: $unique[0].mode,
          status: $unique[0].status,
          reason_code: $unique[0].reason_code,
          sqlstate: $unique[0].sqlstate
        }
      },
      cross_language_consume: $consume[0],
      crash_recovery: $crash[0]
    }' >"${output_file}"
}

run_nested_n_minus_one() {
  local phase="$1"
  local database_url="$2"
  local output_file="$3"
  local queue="chaos3034-n-minus-1"
  local marker="n-minus-1-${phase}-${compose_project}-${RANDOM}"
  local migrate_stdout="${TEMP_DIR}/n-minus-one-migrate.stdout"
  local work_stdout="${TEMP_DIR}/n-minus-one-work.stdout"
  local insert_stdout="${TEMP_DIR}/n-minus-one-insert.stdout"
  local consume_stdout="${TEMP_DIR}/n-minus-one-v0.40-consume.stdout"
  local current_insert_stdout="${TEMP_DIR}/n-minus-one-v0.40-insert.stdout"
  local n_minus_one_consume_stdout="${TEMP_DIR}/n-minus-one-v0.39-consume.stdout"

  case "${phase}" in
    before-v0.40-upgrade)
      progress "running River v0.39 migration-prefix and old-worker probes"
      run_n_minus_one_checked \
        n-minus-one-migrate \
        "${database_url}" \
        "${migrate_stdout}" \
        --operation migrate \
        --timeout 45s
      jq -e '
        .schema_version == 1
        and .status == "ok"
        and .operation == "migrate"
        and .go_version == "go1.25.9"
        and .pgx_version == "v5.9.2"
        and .river_driver_version == "v0.39.0"
        and .river_version == "v0.39.0"
        and .poll_only == false
        and .latest_migration == 6
        and .outcome == "migrated"
        and .applied_versions == [1, 2, 3, 4, 5, 6]
      ' "${migrate_stdout}" >/dev/null 2>&1 || die "River v0.39 migration-prefix assertion failed"

      run_n_minus_one_checked \
        n-minus-one-work \
        "${database_url}" \
        "${work_stdout}" \
        --operation work \
        --marker "${marker}" \
        --queue "${queue}" \
        --timeout 45s
      jq -e --arg marker "${marker}" --arg queue "${queue}" '
        .schema_version == 1
        and .status == "ok"
        and .operation == "work"
        and .go_version == "go1.25.9"
        and .pgx_version == "v5.9.2"
        and .river_driver_version == "v0.39.0"
        and .river_version == "v0.39.0"
        and .poll_only == false
        and .marker == $marker
        and .queue == $queue
        and (.job_id | type == "number")
        and .contract_version == 1
        and .source == "go"
        and .outcome == "completed"
        and .inserted_by_worker == true
      ' "${work_stdout}" >/dev/null 2>&1 || die "River v0.39 old-worker assertion failed"

      jq -n \
        --arg phase "${phase}" \
        --slurpfile migrate "${migrate_stdout}" \
        --slurpfile work "${work_stdout}" '{
          phase: $phase,
          status: "pass",
          migration: {
            go_version: $migrate[0].go_version,
            pgx_version: $migrate[0].pgx_version,
            river_driver_version: $migrate[0].river_driver_version,
            river_version: $migrate[0].river_version,
            latest_migration: $migrate[0].latest_migration,
            applied_versions: $migrate[0].applied_versions,
            outcome: $migrate[0].outcome
          },
          old_worker: {
            go_version: $work[0].go_version,
            pgx_version: $work[0].pgx_version,
            river_driver_version: $work[0].river_driver_version,
            river_version: $work[0].river_version,
            contract_version: $work[0].contract_version,
            source: $work[0].source,
            outcome: $work[0].outcome,
            inserted_by_worker: $work[0].inserted_by_worker
          }
        }' >"${output_file}"
      ;;
    after-v0.40-upgrade)
      local current_marker="n-minus-1-current-to-old-${compose_project}-${RANDOM}"
      progress "running both River v0.39/v0.40 orientations on schema 7"
      run_n_minus_one_checked \
        n-minus-one-insert \
        "${database_url}" \
        "${insert_stdout}" \
        --operation insert \
        --marker "${marker}" \
        --queue "${queue}" \
        --timeout 45s
      jq -e --arg marker "${marker}" --arg queue "${queue}" '
        .schema_version == 1
        and .status == "ok"
        and .operation == "insert"
        and .go_version == "go1.25.9"
        and .pgx_version == "v5.9.2"
        and .river_driver_version == "v0.39.0"
        and .river_version == "v0.39.0"
        and .poll_only == false
        and .marker == $marker
        and .queue == $queue
        and (.job_id | type == "number")
        and .contract_version == 1
        and .source == "go"
        and .outcome == "inserted"
      ' "${insert_stdout}" >/dev/null 2>&1 || die "River v0.39 schema-7 insert assertion failed"

      run_go_checked \
        n-minus-one-v0.40-consume \
        "${database_url}" \
        "${consume_stdout}" \
        --operation consume \
        --mode direct \
        --marker "${marker}" \
        --queue "${queue}" \
        --expected-attempt 1 \
        --fetch-poll-interval "${FETCH_POLL_INTERVAL}" \
        --timeout 60s
      jq -e --arg queue "${queue}" '
        .status == "ok"
        and .mode == "direct"
        and .go_version == "go1.25.9"
        and .pgx_version == "v5.10.0"
        and .river_driver_version == "v0.40.0"
        and .river_version == "v0.40.0"
        and .workload.external.source == "go"
        and .workload.external.queue == $queue
        and .workload.external.state == "completed"
        and .workload.external.outcome == "external_completed"
        and .workload.external.attempt == 1
      ' "${consume_stdout}" >/dev/null 2>&1 || die "River v0.40 N-1 consume assertion failed"

      run_go_checked \
        n-minus-one-v0.40-insert \
        "${database_url}" \
        "${current_insert_stdout}" \
        --operation insert \
        --mode direct \
        --marker "${current_marker}" \
        --queue "${queue}" \
        --priority 2 \
        --max-attempts 3 \
        --timeout 45s
      jq -e --arg queue "${queue}" '
        .status == "ok"
        and .mode == "direct"
        and .go_version == "go1.25.9"
        and .pgx_version == "v5.10.0"
        and .river_driver_version == "v0.40.0"
        and .river_version == "v0.40.0"
        and .migration.latest_version == 7
        and .workload.external.source == "go"
        and .workload.external.queue == $queue
        and .workload.external.priority == 2
        and .workload.external.max_attempts == 3
        and .workload.external.state == "available"
        and .workload.external.outcome == "inserted"
      ' "${current_insert_stdout}" >/dev/null 2>&1 || die "River v0.40 schema-7 insert assertion failed"

      run_n_minus_one_checked \
        n-minus-one-v0.39-consume \
        "${database_url}" \
        "${n_minus_one_consume_stdout}" \
        --operation work \
        --consume-existing \
        --marker "${current_marker}" \
        --queue "${queue}" \
        --timeout 45s
      jq -e --arg queue "${queue}" '
        .schema_version == 1
        and .status == "ok"
        and .operation == "work"
        and .go_version == "go1.25.9"
        and .pgx_version == "v5.9.2"
        and .river_driver_version == "v0.39.0"
        and .river_version == "v0.39.0"
        and .poll_only == false
        and .queue == $queue
        and (.job_id | type == "number")
        and .contract_version == 1
        and .source == "go"
        and .outcome == "completed"
        and ((.inserted_by_worker // false) == false)
      ' "${n_minus_one_consume_stdout}" >/dev/null 2>&1 || die "River v0.39 schema-7 consume assertion failed"

      jq -n \
        --arg phase "${phase}" \
        --slurpfile insert "${insert_stdout}" \
        --slurpfile consume "${consume_stdout}" \
        --slurpfile current_insert "${current_insert_stdout}" \
        --slurpfile n_minus_one_consume "${n_minus_one_consume_stdout}" '{
          phase: $phase,
          status: "pass",
          n_minus_one_insert: {
            go_version: $insert[0].go_version,
            pgx_version: $insert[0].pgx_version,
            river_driver_version: $insert[0].river_driver_version,
            river_version: $insert[0].river_version,
            contract_version: $insert[0].contract_version,
            source: $insert[0].source,
            outcome: $insert[0].outcome
          },
          current_consume: $consume[0],
          current_insert: {
            go_version: $current_insert[0].go_version,
            pgx_version: $current_insert[0].pgx_version,
            river_driver_version: $current_insert[0].river_driver_version,
            river_version: $current_insert[0].river_version,
            source: $current_insert[0].workload.external.source,
            outcome: $current_insert[0].workload.external.outcome
          },
          n_minus_one_consume: {
            go_version: $n_minus_one_consume[0].go_version,
            pgx_version: $n_minus_one_consume[0].pgx_version,
            river_driver_version: $n_minus_one_consume[0].river_driver_version,
            river_version: $n_minus_one_consume[0].river_version,
            contract_version: $n_minus_one_consume[0].contract_version,
            source: $n_minus_one_consume[0].source,
            outcome: $n_minus_one_consume[0].outcome,
            inserted_by_worker: ($n_minus_one_consume[0].inserted_by_worker // false)
          }
        }' >"${output_file}"
      ;;
    *) die "unknown N-1 compatibility phase" ;;
  esac
}

combine_nested_n_minus_one() {
  local before_upgrade="$1"
  local after_upgrade="$2"
  local output_file="$3"

  jq -n \
    --slurpfile before "${before_upgrade}" \
    --slurpfile after "${after_upgrade}" '{
      status: (
        if $before[0].status == "pass" and $after[0].status == "pass"
        then "pass"
        else "not_run"
        end
      ),
      phases: [$before[0], $after[0]]
    }' >"${output_file}"
}

assert_final_redaction() {
  local output_file="$1"

  jq -e '
    [
      paths(scalars) as $path
      | select($path[0] != "redaction")
      | ($path | map(tostring) | join("."))
      | select(test("(?i)(database.?url|dsn|password|credential|marker|job.?id|payload)"))
    ]
    | length == 0
  ' "${output_file}" >/dev/null 2>&1 || die "the combined result contains a forbidden field"

  if jq -e '
    .. | strings
    | select(test("postgresql(?:\\+asyncpg)?://|river_compat:river_compat"; "i"))
  ' "${output_file}" >/dev/null 2>&1; then
    die "the combined result contains connection material"
  fi
}

progress "validating the pinned Compose model"
if ! "${compose[@]}" config --quiet \
  >"${TEMP_DIR}/compose-config.stdout" \
  2>"${TEMP_DIR}/compose-config.stderr"; then
  die "the pinned Compose model is invalid"
fi

progress "checking the locked Python compatibility dependencies"
python_versions="${TEMP_DIR}/python-versions.json"
if ! "${PYTHON_BIN}" -c '
import json
import platform
from importlib.metadata import version

print(json.dumps({
    "asyncpg": version("asyncpg"),
    "python": platform.python_version(),
    "riverqueue": version("riverqueue"),
    "sqlalchemy": version("SQLAlchemy"),
}, separators=(",", ":"), sort_keys=True))
' \
  >"${python_versions}" \
  2>"${TEMP_DIR}/python-import.stderr"; then
  die "the locked Python compatibility dependencies are unavailable"
fi
if ! jq -e \
  --arg python "${PYTHON_VERSION}" \
  --arg riverqueue "${RIVERQUEUE_PYTHON_VERSION}" \
  --arg sqlalchemy "${SQLALCHEMY_VERSION}" \
  --arg asyncpg "${ASYNCPG_VERSION}" '
    .python == $python
    and .riverqueue == $riverqueue
    and .sqlalchemy == $sqlalchemy
    and .asyncpg == $asyncpg
  ' "${python_versions}" \
  >"${TEMP_DIR}/python-import.stdout" \
  2>"${TEMP_DIR}/python-version-check.stderr"; then
  die "the Python compatibility runtime does not match the recorded spike pins"
fi

progress "building the River v0.40 CLI once"
if ! (
  cd "${REPO_ROOT}"
  GOTOOLCHAIN="${GO_TOOLCHAIN}" GOCACHE="${TEMP_DIR}/go-cache" GOWORK=off \
    go build -mod=readonly -o "${GO_BINARY}" ./tests/compatibility/river/cmd/river-compat
) >"${TEMP_DIR}/go-build.stdout" 2>"${TEMP_DIR}/go-build.stderr"; then
  die "the River v0.40 CLI build failed; captured details were discarded"
fi

N_MINUS_ONE_BINARY="${TEMP_DIR}/river-nminus1"
progress "building the nested River v0.39 CLI once"
if ! (
  cd "${SCRIPT_DIR}/nminus1"
  GOTOOLCHAIN="${GO_TOOLCHAIN}" GOCACHE="${TEMP_DIR}/go-cache" GOWORK=off \
    go build -mod=readonly -o "${N_MINUS_ONE_BINARY}" .
) >"${TEMP_DIR}/n-minus-one-build.stdout" 2>"${TEMP_DIR}/n-minus-one-build.stderr"; then
  die "the nested River v0.39 CLI build failed; captured details were discarded"
fi

progress "starting the isolated pinned PostgreSQL and PgBouncer services"
COMPOSE_ATTEMPTED=1
if ! "${compose[@]}" up -d --wait postgres pgbouncer \
  >"${TEMP_DIR}/compose-up.stdout" \
  2>"${TEMP_DIR}/compose-up.stderr"; then
  die "the isolated compatibility services did not become healthy"
fi

postgres_port="$(resolve_local_port postgres 5432)"
pgbouncer_port="$(resolve_local_port pgbouncer 6432)"
direct_database_url="postgresql://river_compat:river_compat@127.0.0.1:${postgres_port}/river_compat"
poll_database_url="postgresql://river_compat:river_compat@127.0.0.1:${pgbouncer_port}/river_compat"

direct_profile="${TEMP_DIR}/direct-profile.json"
poll_profile="${TEMP_DIR}/poll-only-profile.json"
direct_matrix="${TEMP_DIR}/direct-matrix.stdout"
poll_matrix="${TEMP_DIR}/poll-only-matrix.stdout"
n_minus_one_before="${TEMP_DIR}/n-minus-one-before-upgrade.json"
n_minus_one_after="${TEMP_DIR}/n-minus-one-after-upgrade.json"
n_minus_one="${TEMP_DIR}/n-minus-one.json"
combined_result="${TEMP_DIR}/combined-result.json"

# The N-1 proof owns the migration-prefix order: v0.39 first creates its schema
# and work on a fresh database, v0.40 then migrates through schema 7, and both
# versions insert work that the other version consumes on the upgraded schema.
run_nested_n_minus_one before-v0.40-upgrade "${direct_database_url}" "${n_minus_one_before}"
run_mode_matrix direct "${direct_database_url}" false chaos3034-direct "${direct_matrix}"
run_nested_n_minus_one after-v0.40-upgrade "${direct_database_url}" "${n_minus_one_after}"

# Keep the second measured matrix as close to service startup as the required
# N/N-1 migration-prefix order permits, minimizing pg_isready contamination.
run_mode_matrix poll-only "${poll_database_url}" true chaos3034-poll-only "${poll_matrix}"

run_profile direct "${direct_database_url}" false direct_postgresql "${direct_matrix}" "${direct_profile}"
run_profile poll-only "${poll_database_url}" true pgbouncer_transaction_poll_only "${poll_matrix}" "${poll_profile}"
combine_nested_n_minus_one "${n_minus_one_before}" "${n_minus_one_after}" "${n_minus_one}"

jq -n \
  --argjson samples "${SAMPLES}" \
  --slurpfile direct "${direct_profile}" \
  --slurpfile poll "${poll_profile}" \
  --slurpfile n_minus_one "${n_minus_one}" \
  --slurpfile python_versions "${python_versions}" '{
    schema_version: 1,
    status: "complete_with_architecture_blocker",
    architecture_blocker: "poll_only_running_cancel_not_propagated",
    evidence_scope: "local_ephemeral_compatibility_harness",
    go_version: $direct[0].matrix.go_version,
    versions: {
      go: $direct[0].matrix.go_version,
      river: $direct[0].matrix.river_version,
      river_driver: $direct[0].matrix.river_driver_version,
      pgx: $direct[0].matrix.pgx_version,
      river_n_minus_1: $n_minus_one[0].phases[0].migration.river_version,
      river_driver_n_minus_1: $n_minus_one[0].phases[0].migration.river_driver_version,
      pgx_n_minus_1: $n_minus_one[0].phases[0].migration.pgx_version,
      python: $python_versions[0].python,
      riverqueue_python: $python_versions[0].riverqueue,
      sqlalchemy: $python_versions[0].sqlalchemy,
      asyncpg: $python_versions[0].asyncpg
    },
    samples_per_mode: $samples,
    gate_truth_table: {
      direct: {
        backend_connection_delta_at_most_six: $direct[0].matrix.gates.backend_connection_delta_at_most_six,
        canceled_acquires_zero: $direct[0].matrix.gates.canceled_acquires_zero,
        enqueue_p95_within_limit: $direct[0].matrix.gates.enqueue_p95_within_limit,
        new_connections_at_most_six: $direct[0].matrix.gates.new_connections_at_most_six,
        cross_client_running_cancel: $direct[0].matrix.gates.cross_client_running_cancel,
        same_client_running_cancel: $direct[0].matrix.gates.same_client_running_cancel
      },
      poll_only: {
        backend_connection_delta_at_most_six: $poll[0].matrix.gates.backend_connection_delta_at_most_six,
        canceled_acquires_zero: $poll[0].matrix.gates.canceled_acquires_zero,
        enqueue_p95_within_limit: $poll[0].matrix.gates.enqueue_p95_within_limit,
        new_connections_at_most_six: $poll[0].matrix.gates.new_connections_at_most_six,
        cross_client_running_cancel: $poll[0].matrix.gates.cross_client_running_cancel,
        same_client_running_cancel: $poll[0].matrix.gates.same_client_running_cancel
      }
    },
    profiles: [$direct[0], $poll[0]],
    nested_n_minus_1: $n_minus_one[0],
    redaction: {
      contains_raw_logs: false,
      contains_credentials_or_dsns: false,
      contains_job_payloads: false,
      contains_dynamic_ports: false,
      contains_container_or_project_ids: false
    }
  }' >"${combined_result}"

assert_final_redaction "${combined_result}"
progress "removing the isolated services and volumes"
compose_down || die "isolated Compose cleanup failed"

progress "compatibility harness completed"
jq . "${combined_result}"
