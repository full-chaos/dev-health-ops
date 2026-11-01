#!/usr/bin/env bash
# Run the repository's Go quality gates across the root module and every
# checked-in nested module (including the River N-1 compatibility module).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
GO_TOOLCHAIN="go1.27.3"
export GOTOOLCHAIN="${GO_TOOLCHAIN}"

# CHAOS-11794: the job deadline. A go test -timeout that is only a constant can sit past the end of the job: a call that starts
# late in a 25 min job with a 22 min -timeout is cancelled by the job cap before its own timeout prints the goroutines. Every
# integration go test call takes `job_go_test_timeout N` instead: min(N min, CHECK_GO_JOB_CAP_MIN - elapsed - 3 min) in seconds.
# CHECK_GO_JOB_CAP_MIN is the job's own timeout-minutes (a job-level env in the workflow; a test pins the two equal). The job
# start is taken at the first check_go.sh call of the job (CHECK_GO_JOB_START, appended to GITHUB_ENV for the later steps); it
# misses the checkout and setup steps, which the 3 min margin absorbs. No budget left: a named line and rc 2, never an unbounded
# call. A hosted run without the cap refuses too; outside CI without the cap the constants apply. CHECK_GO_NOW_EPOCH(_FILE) is a
# test clock, honoured only when GITHUB_ACTIONS is not true.
job_budget_init() {
  [ -n "${CHECK_GO_JOB_CAP_MIN:-}" ] || return 0
  if [ -z "${CHECK_GO_JOB_START:-}" ]; then
    CHECK_GO_JOB_START="$(job_budget_now)"
    export CHECK_GO_JOB_START
    [ -z "${GITHUB_ENV:-}" ] || printf 'CHECK_GO_JOB_START=%s\n' "${CHECK_GO_JOB_START}" >>"${GITHUB_ENV}"
  fi
}

job_budget_now() {
  if [ "${GITHUB_ACTIONS:-}" != true ]; then
    if [ -n "${CHECK_GO_NOW_EPOCH_FILE:-}" ]; then cat "${CHECK_GO_NOW_EPOCH_FILE}"; return 0; fi
    if [ -n "${CHECK_GO_NOW_EPOCH:-}" ]; then printf '%s\n' "${CHECK_GO_NOW_EPOCH}"; return 0; fi
  fi
  date +%s
}

# job_budget_remaining: the seconds of the job budget left after the 3 min margin, or "none" when no cap is set (outside CI).
job_budget_remaining() {
  local cap="${CHECK_GO_JOB_CAP_MIN:-}" now start
  if [ -z "${cap}" ]; then
    printf 'none\n'
    return 0
  fi
  case "${cap}" in *[!0-9]*) echo "check_go.sh: CHECK_GO_JOB_CAP_MIN must be a whole number of minutes, got '${cap}'" >&2; return 2 ;; esac
  job_budget_init
  start="${CHECK_GO_JOB_START:-}"
  case "${start}" in ''|*[!0-9]*) echo "check_go.sh: CHECK_GO_JOB_START is not a whole number of seconds ('${start}'); the go test is not started" >&2; return 2 ;; esac
  now="$(job_budget_now)"
  printf '%s\n' "$((cap * 60 - (now - start) - 180))"
}

job_go_test_timeout() {
  local const_min="${1:-}" cap="${CHECK_GO_JOB_CAP_MIN:-}" remaining secs
  case "${const_min}" in ''|*[!0-9]*) echo "check_go.sh: job_go_test_timeout needs a whole number of minutes" >&2; return 2 ;; esac
  if [ -z "${cap}" ]; then
    if [ "${GITHUB_ACTIONS:-}" = true ]; then
      echo "check_go.sh: CHECK_GO_JOB_CAP_MIN is not set for a guarded step; the job must carry its own timeout-minutes as that env value, so the go test is not started" >&2
      return 2
    fi
    printf '%ss\n' "$((const_min * 60))"
    return 0
  fi
  remaining="$(job_budget_remaining)" || return 2
  if [ "${remaining}" -le 0 ]; then
    echo "check_go.sh: job time budget exhausted before this go test call (its limit is ${const_min}m): cap ${cap}m, elapsed $((cap * 60 - 180 - remaining))s, 3 min kept back; the call is not started" >&2
    return 2
  fi
  secs=$((const_min * 60))
  [ "${remaining}" -ge "${secs}" ] || secs="${remaining}"
  printf '%ss\n' "${secs}"
}

job_budget_init
if [ "${1:-}" = "job-start" ]; then
  printf '%s\n' "${CHECK_GO_JOB_START:-}"
  exit 0
fi
if [ "${1:-}" = "job-timeout" ]; then
  [ "$#" -eq 2 ] || { echo "usage: check_go.sh job-timeout MINUTES" >&2; exit 2; }
  if job_go_test_timeout "$2"; then exit 0; fi
  exit 2
fi
# CHAOS-5224 / CHAOS-5268: precedence is an explicit DEV_HEALTH_GO_CACHE first,
# then an already-inherited GOCACHE, then a GOCACHE CONFIGURED on the machine
# (`go env -changed GOCACHE`: the Go env file written by `go env -w`, which is
# not in the environment of a non-login agent shell), and only when all three
# give nothing the tmp fallback. PR #2281 fixed the inherited GOCACHE case only;
# the build host's `go env -w` setting never reached this script, so every run
# wrote a cache under /tmp/dev-health-go-build-cache on the root disk (106 GB in
# three days, root disk full on 2026-10-04). Go's built-in DEFAULT dir is
# deliberately NOT used: on a hosted runner it would put the build cache where
# setup-go saves it, a CI behaviour change. `-changed` prints GOCACHE='<path>'
# (single-quoted) only when the value differs from the default; it is parsed,
# never eval'd. GOTOOLCHAIN=local: asking must never download a toolchain.
DEV_HEALTH_GO_CACHE="${DEV_HEALTH_GO_CACHE:-${GOCACHE:-}}"
if [ -z "${DEV_HEALTH_GO_CACHE}" ]; then
  go_changed_line="$(GOTOOLCHAIN=local go env -changed GOCACHE 2>/dev/null || true)"
  case "${go_changed_line}" in
    "GOCACHE='"*"'")
      go_changed_value="${go_changed_line#GOCACHE=\'}"
      go_changed_value="${go_changed_value%\'}"
      # A value holding a quote needs shell-unquoting we do not attempt: ignore it.
      case "${go_changed_value}" in
        *"'"*) ;;
        *) DEV_HEALTH_GO_CACHE="${go_changed_value}" ;;
      esac
      ;;
    GOCACHE=?*)
      DEV_HEALTH_GO_CACHE="${go_changed_line#GOCACHE=}"
      ;;
  esac
fi
if [ -z "${DEV_HEALTH_GO_CACHE}" ]; then
  DEV_HEALTH_GO_CACHE="${TMPDIR:-/tmp}/dev-health-go-build-cache"
  echo "check_go.sh: no Go build cache dir is configured (DEV_HEALTH_GO_CACHE, GOCACHE, go env -changed GOCACHE); using ${DEV_HEALTH_GO_CACHE}" >&2
fi
mkdir -p "${DEV_HEALTH_GO_CACHE}"
export GOCACHE="${DEV_HEALTH_GO_CACHE}"
DEV_HEALTH_GO_BUILD_OUTPUT=""
DEV_HEALTH_GO_BUILD_TEMP_ROOT=""
DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="${DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST:-${ROOT}/ci/go_integration_shards.d}"
DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST="${DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST:-${ROOT}/ci/go_providersync_test_shards.tsv}"
DEV_HEALTH_GO_PROVIDER_TEST_WEIGHTS="${DEV_HEALTH_GO_PROVIDER_TEST_WEIGHTS:-${ROOT}/ci/go_providersync_test_weights.tsv}"
# 75% of the 22 min go test -timeout of a providersync shard, in 10 ms units (1320 s * 0.75 = 990 s).
DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET="${DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET:-99000}"
DEV_HEALTH_GO_DAILY_TEST_SHARD_MANIFEST="${DEV_HEALTH_GO_DAILY_TEST_SHARD_MANIFEST:-${ROOT}/ci/go_daily_test_shards.tsv}"
DEV_HEALTH_GO_WORKERSERVICE_TEST_SHARD_MANIFEST="${DEV_HEALTH_GO_WORKERSERVICE_TEST_SHARD_MANIFEST:-${ROOT}/ci/go_workerservice_test_shards.tsv}"
DEV_HEALTH_GO_QAPISERVER_TEST_SHARD_MANIFEST="${DEV_HEALTH_GO_QAPISERVER_TEST_SHARD_MANIFEST:-${ROOT}/ci/go_queryapi_server_test_shards.tsv}"
DEV_HEALTH_GO_REMAINING_TEST_SHARD_MANIFEST="${DEV_HEALTH_GO_REMAINING_TEST_SHARD_MANIFEST:-${ROOT}/ci/go_remaining_test_shards.tsv}"
INTEGRATION_CONTAINER_HARNESS="${ROOT}/internal/testsupport/containers/harness.go"

# --- Ambient-env scrub (CHAOS-3988). ------------------------------------------------
# This script previously had NO scrub at all -- it inherited the caller's shell
# environment wholesale into every `go` invocation below. ops/.env's direnv setup
# exports GO_PROVIDER_ROUTES=all and DEV_HEALTH_ENV=local for local `dev-hops` CLI
# convenience:
#   - internal/platform/config (config.go:58-59) reads both directly via
#     os.LookupEnv to enable the "local all-routes" preset for the Go worker's own
#     typed config, exercised anywhere `go test ./...` builds that package.
#   - a live-Python oracle (since deleted) used to shell out to `python3`, whose
#     side treated the SAME pair as its own "local all-routes" preset, so the two
#     planners disagreed and the oracle went false-red -- an ambient-env
#     artifact (CHAOS-3988). See
#     ci/local_validate.sh's PROXY_OFF for the matching pytest-side scrub and the
#     fuller incident history (CHAOS-3986, CHAOS-3987, two lanes in one morning on
#     2026-08-21).
# Mirrors PROXY_OFF's mechanism exactly (an `env -u` array prefixed onto every
# invocation) rather than inventing a second one. Applied uniformly to every `go`
# call in this script, not only the one confirmed offender: none of these gate
# stages need either var, and CI never sets them, so scrubbing everywhere keeps
# this script's signal CI-equivalent by construction instead of by memory.
#
# GOFLAGS and GOEXPERIMENT are scrubbed for a sharper reason (CHAOS-4778): GOFLAGS
# is inherited by every `go` invocation, so an ambient `GOFLAGS=-tags=integration`
# turned `check_go.sh test` -- a verb CI declares container-free -- into a full
# integration run that starts Testcontainers. A caller's environment must not be
# able to change which suite a verb runs.
# CHAOS-11745: GO_TEST_LEG_JSON_DIR and DEV_HEALTH_GO_FRESH_SECURITY belong to the leg: a test that starts check_go.sh as a child
# must not empty the leg's -json file (its receipt reads it) or run in the leg's merge_group mode.
GO_ENV_OFF=(env -u GO_PROVIDER_ROUTES -u DEV_HEALTH_ENV -u GOFLAGS -u GOEXPERIMENT -u CHECK_GO_FRESH -u GO_TEST_LEG_JSON_DIR -u DEV_HEALTH_GO_FRESH_SECURITY)

# INTEGRATION_TEST_ENV (CHAOS-6634): environment for the MULTI-PACKAGE
# `go test -tags=integration pkgA pkgB ...` invocations below (integration and
# the integration-shard packages target). testcontainers-go derives its session
# id from the PARENT pid, so every test binary of one such invocation shares ONE
# session and ONE Ryuk reaper. When one package's process exits, Ryuk reaps the
# session's containers after its 10 s reconnection timeout -- including a
# SIBLING package's live container. Seen on shard 3: internal/admincli
# TestSeedVenueOracleMatchesThePythonProducer lost its Postgres ~10 s after internal/api/externalingest
# exited (`unexpected postmaster exit`, `No such container`, then the Python
# leg's `Connect call failed`). Reproduced with two throwaway packages in one
# `go test -p 2` run: the long-lived one is refused ~10 s after the short one
# exits, and survives with TESTCONTAINERS_RYUK_DISABLED=true.
# In CI (hosted runners are discarded, and every harness container has its own
# t.Cleanup Terminate) the reaper is off for those invocations. A value the
# caller already set wins. Outside CI the reaper stays on (orphan safety on a
# workstation), so a local multi-package run keeps the cross-package hazard.
INTEGRATION_TEST_ENV=()
if [ "${CI:-}" = "true" ] && [ -z "${TESTCONTAINERS_RYUK_DISABLED+x}" ]; then
  INTEGRATION_TEST_ENV=(TESTCONTAINERS_RYUK_DISABLED=true)
fi

# run_integration_go_test (CHAOS-10036): one testcontainers session id per
# `go test -tags=integration` call, so ONE label names every container the call
# starts, with the reaper on or off. After the call (pass, fail, INT, TERM) the
# containers of that session are removed by id (ci/reap-testcontainers-session.sh:
# label filter only, never a prune). A caller-set TESTCONTAINERS_SESSION_ID wins.
# The go test runs in its own session (setsid), so its pid is its process-group id and the trap
# stops the whole group (TERM, then KILL after 10 s) before the reap, but only when the pgid is that pid and not
# this shell's own group; otherwise it stops the recorded pid only. A SIGKILL of this script skips the trap; the
# reaper (when on) or a host sweep by session id covers that case.
TC_TRAP_PID="" TC_TRAP_SID="" TC_TRAP_PGID=""
tc_trap() {
  local i
  # A TERM before the setup loop recorded the group: resolve it now (the same private-group guard applies below).
  if [ -z "${TC_TRAP_PGID}" ]; then
    TC_TRAP_PGID="$(ps -o pgid= -p "${TC_TRAP_PID}" 2>/dev/null | tr -d ' ' || true)"
  fi
  # Group kill only for a private group: pgid == the recorded go test pid and != this shell's own group.
  if [ -n "${TC_TRAP_PGID}" ] && [ "${TC_TRAP_PGID}" = "${TC_TRAP_PID}" ] \
    && [ "${TC_TRAP_PGID}" != "$(ps -o pgid= -p "$$" | tr -d ' ')" ]; then
    kill -TERM -- "-${TC_TRAP_PGID}" 2>/dev/null || true
    for i in $(seq 1 100); do
      pgrep -g "${TC_TRAP_PGID}" >/dev/null 2>&1 || break
      sleep 0.1
    done
    if pgrep -g "${TC_TRAP_PGID}" >/dev/null 2>&1; then
      kill -KILL -- "-${TC_TRAP_PGID}" 2>/dev/null || true
    fi
  else
    echo "check_go.sh: no private process group for the go test; stopping pid ${TC_TRAP_PID} only" >&2
    kill -TERM "${TC_TRAP_PID}" 2>/dev/null || true
  fi
  wait "${TC_TRAP_PID}" 2>/dev/null || true
  bash "${SCRIPT_DIR}/reap-testcontainers-session.sh" "${TC_TRAP_SID}" || true
  exit 143
}

run_integration_go_test() {
  local module_dir="$1" sid pid rc=0 i own_pgid tmo
  local -a private_group=()
  shift
  tmo="$(job_go_test_timeout 32)" || return 2
  sid="${TESTCONTAINERS_SESSION_ID:-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')}"
  [[ "${sid}" =~ ^[A-Za-z0-9._-]{8,64}$ ]] \
    || die "TESTCONTAINERS_SESSION_ID must match ^[A-Za-z0-9._-]{8,64}\$"
  if command -v setsid >/dev/null 2>&1; then
    private_group=(setsid)
  fi
  (
    cd "${ROOT}/${module_dir}"
    exec ${private_group[@]+"${private_group[@]}"} "${GO_ENV_OFF[@]}" GOWORK=off TESTCONTAINERS_SESSION_ID="${sid}" ${INTEGRATION_TEST_ENV[@]+"${INTEGRATION_TEST_ENV[@]}"} \
      go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" "$@"
  ) &
  pid=$!
  TC_TRAP_PID="${pid}"
  TC_TRAP_SID="${sid}"
  TC_TRAP_PGID=""
  trap tc_trap INT TERM
  if [ "${#private_group[@]}" -gt 0 ]; then
    own_pgid="$(ps -o pgid= -p "$$" | tr -d ' ')"
    for i in $(seq 1 50); do
      kill -0 "${pid}" 2>/dev/null || { TC_TRAP_PGID=""; break; }
      TC_TRAP_PGID="$(ps -o pgid= -p "${pid}" 2>/dev/null | tr -d ' ' || true)"
      [ "${TC_TRAP_PGID}" = "${pid}" ] && [ "${TC_TRAP_PGID}" != "${own_pgid}" ] && break
      TC_TRAP_PGID=""
      sleep 0.1
    done
  fi
  wait "${pid}" || rc=$?
  trap - INT TERM
  bash "${SCRIPT_DIR}/reap-testcontainers-session.sh" "${sid}" || rc=$?
  return "${rc}"
}

# CHAOS-11794: the providersync shard pipeline (`go test -json | tee | jq`) runs in its own process group (setsid, only
# inside the backgrounded subshell: this script is never exec-replaced) under a hard step bound. A child that holds
# stdout open would keep `tee` alive after go test is gone and stall the step until the job cap with no log. The bound
# sits above the 22 min go test -timeout (its dump wins when it can) and below the 25 min job cap. At the bound the
# group's processes except tee/jq/bash get SIGQUIT (Go prints every goroutine; tee and jq stay up to carry the dump),
# then KILL after 10 s. Leftover group members are reaped after go test exits. ps prints pid, pgid, ppid, etimes and
# args only, never env. DEV_HEALTH_GO_STEP_BOUND_SECS overrides the bound for tests.
PROVIDERSYNC_STEP_BOUND_SECS=1380

group_ps_table() {
  ps -eo pid,pgid,ppid,etimes,args 2>/dev/null | awk -v g="$1" 'NR == 1 || $2 == g' | cut -c1-300
}

providersync_bound_watchdog() {
  local pid="$1" pgid="$2" bound="$3" marker="$4" s p
  for ((s = 0; s < bound; s++)); do
    sleep 1
    kill -0 "${pid}" 2>/dev/null || return 0
  done
  echo hit >"${marker}"
  printf 'check_go.sh: providersync step bound (%s s) hit; stopping the go test process group %s\n' "${bound}" "${pgid}" >&2
  group_ps_table "${pgid}" >&2
  for p in $(ps -eo pid=,pgid=,comm= | awk -v g="${pgid}" '$2 == g && $3 != "tee" && $3 != "jq" && $3 != "bash" { print $1 }'); do
    kill -QUIT "${p}" 2>/dev/null || true
  done
  for ((s = 0; s < 100; s++)); do
    kill -0 "${pid}" 2>/dev/null || return 0
    sleep 0.1
  done
  group_ps_table "${pgid}" >&2
  kill -KILL -- "-${pgid}" 2>/dev/null || true
}

run_providersync_bounded() {
  local test_regex="$1" json_file="$2"
  local bound="${DEV_HEALTH_GO_STEP_BOUND_SECS:-${PROVIDERSYNC_STEP_BOUND_SECS}}"
  local pid pgid own_pgid i rc=0 wd marker pidfile tmo remaining
  case "${bound}" in ''|*[!0-9]*) die "DEV_HEALTH_GO_STEP_BOUND_SECS must be a whole number of seconds" ;; esac
  if ! command -v setsid >/dev/null 2>&1; then
    echo "check_go.sh: the bounded providersync step needs setsid (util-linux) on PATH; it is missing, so the step does not start. Install a setsid on PATH (Linux: util-linux) or run this shard on Linux." >&2
    return 2
  fi
  tmo="$(job_go_test_timeout 22)" || return 2
  remaining="$(job_budget_remaining)" || return 2
  if [ "${remaining}" != none ] && [ "${remaining}" -lt "${bound}" ]; then bound="${remaining}"; fi
  marker="$(mktemp)"
  pidfile="$(mktemp)"
  (
    cd "${ROOT}"
    # shellcheck disable=SC2016
    exec setsid bash -c 'echo $$ >"$3"; set -o pipefail; out=$1; filter=$2; shift 3; "$@" | tee "${out}" | jq --unbuffered -rj "${filter}"' _ \
      "${json_file}" 'select(.Action == "output") | .Output' "${pidfile}" \
      "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" -json -run "${test_regex}" ./internal/providersync
  ) &
  pid=$!
  own_pgid="$(ps -o pgid= -p "$$" | tr -d ' ')"
  for i in $(seq 1 600); do
    [ -s "${pidfile}" ] && break
    kill -0 "${pid}" 2>/dev/null || break
    sleep 0.1
  done
  pgid="$(tr -d ' \n' <"${pidfile}")"
  case "${pgid}" in
    ''|*[!0-9]*|"${own_pgid}")
      echo "check_go.sh: the providersync step has no process group of its own; stopping it" >&2
      kill -KILL "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
      rm -f "${marker}" "${pidfile}"
      return 2
      ;;
  esac
  providersync_bound_watchdog "${pid}" "${pgid}" "${bound}" "${marker}" &
  wd=$!
  wait "${pid}" || rc=$?
  wait "${wd}" 2>/dev/null || true
  if pgrep -g "${pgid}" >/dev/null 2>&1; then
    echo "check_go.sh: processes left in the go test process group ${pgid}; stopping them" >&2
    group_ps_table "${pgid}" >&2
    kill -TERM -- "-${pgid}" 2>/dev/null || true
    for i in $(seq 1 50); do
      pgrep -g "${pgid}" >/dev/null 2>&1 || break
      sleep 0.1
    done
    kill -KILL -- "-${pgid}" 2>/dev/null || true
  fi
  [ ! -s "${marker}" ] || rc=124
  rm -f "${marker}" "${pidfile}"
  return "${rc}"
}

usage() {
  # Backticks in the literal help text document commands; they are not substitutions.
  # shellcheck disable=SC2016
  printf '%s\n' 'Usage: ci/check_go.sh [auth [STAGE]|fmt|vet|test|race|venue-oracles [SHARD COUNT]|venue-oracle-plan COUNT|ci-leg LEG [SHARD COUNT]|ci-leg-warm test|race SHARD COUNT|test-shard-plan SHARD COUNT|race-shard-plan SHARD COUNT|race-excluded-shard-plan SHARD COUNT|race-excluded-receipts JSON...|test-shard-wall-guard SHARD COUNT WALL LOGFILE|test-shard-unweighted-guard SHARD COUNT LOGFILE|security-fresh-receipt RAN JSON...|security-fresh-split PKG...|build|contract|ratchets|race-weights-rows|multi-replica-workers|job-timeout MINUTES|job-start|integration-vet|integration-coverage|race-excluded-ran|integration-shard-plan|integration-prepull|integration-shard|integration|fast|ci|all]

  auth   (CHAOS-4887) The Auth Control Plane gates over the auth packages ALONE: gofmt, vet, test,
         -race, the production-middleware stack test (G-69, must log its marker), one fuzz run per
         registered parser (each its own proof marker), govulncheck pinned and strict. STAGE is
         fmt|vet|test|race|race-integration|fuzz [TARGET]|fuzz-registry|vuln|middleware|closure|relevant BASE HEAD|paths-selftest|scope-selftest|all.
  fmt    Check gofmt without modifying files.
  vet    Run go vet ./... in every Go module.
  test   Run go test ./... in every Go module.
  race   Run go test -race ./... in every Go module.
  venue-oracles
         Run every venue oracle test (internal/testsupport/venueoracle):
         real Postgres/Valkey containers and the Go api, replayed against
         the recorded Python answers (the frozen goldens). No Python runs.
         Discovery is
         every test reaching a venueoracle harness entry point (a
         test marked //venueoracle:local-only is named and left out),
         never a hardcoded list. Fails if
         any discovered test has no proof file afterward (a skip, never a
         silent pass) or if discovery finds nothing. The verb sets
         DEV_HEALTH_VENUE_ORACLES=1 for each go test it runs (a skipped
         test fails the verb). NOT in `fast`/`ci`/`all`: it runs only
         from the dedicated venue-oracles CI job.
         WHAT RUNS is ci/venue_oracle_registry.d/ (CHAOS-6584, one file per
         package since CHAOS-6724): the verb
         validates it against discovery and against every Test*VenueOracle*
         by name before starting anything.
         The static half of that validation is ci/check_venue_oracle_registry.sh
         (no Go toolchain, containers or Python), run on every PR by
         the Go tooling checks.
         `venue-oracles SHARD COUNT` (CHAOS-6574) runs the SHARD-th of COUNT
         slices of the registry rows marked run, cost-balanced by the measured
         seconds in ci/venue_oracle_weights.d/ (ci/venue_oracle_shard.awk, the
         longest-processing-time assignment; CHAOS-6891), so the tests of one
         package spread over all slices; the hosted job runs COUNT matrix legs.
         A slice that selects zero rows fails; no arguments runs every row.
         Each leg logs its predicted and actual test seconds against
         VENUE_LEG_BUDGET_SECONDS (default 1800) and warns when over.
  venue-oracles --changed FILE [--list] [SHARD COUNT]
         (CHAOS-7653) Run only the registry rows of the packages that the paths in
         FILE (NUL- or newline-separated, repo-relative) lie in: the form the
         pull_request job runs, with a cost bounded by the packages a PR changes.
         A path that ci/venue_oracle_triggers.d/ declares for a test (code outside
         its package whose behaviour the test compares; CHAOS-9172) selects that
         test too, and so does a file directly in a package of the dependency
         closure (ci/venue_oracle_closure.d/, CHAOS-9935). No row selected is a loud no-op. With --list print the selected rows
         (SELECT, package, test, tab-separated) and run nothing.
         With SHARD COUNT (D6039a, CHAOS-9935) only the SHARD-th of COUNT
         cost-balanced slices of the SELECTED rows (ci/venue_oracle_shard.awk,
         weights ci/venue_oracle_weights.d/): the hosted job runs COUNT matrix
         legs, each within its time limit however many rows a change selects. A
         leg whose slice is empty is a plain no-op.
  venue-oracle-plan COUNT
         Print the cost-balanced plan of COUNT legs: every run row with its leg
         and weight, then one "#leg" line per leg (rows, predicted seconds).
         Needs no Go, containers or Python.
  ci-leg LEG [SHARD COUNT]
         One parallel slice of `ci` for the go-quality workflow (CHAOS-6690).
         GO_QUALITY_SELECTION_FILE (the go-quality-plan verdict) narrows the test
         and race legs to an AFFECTED package subset:
         static (format, vet, build, contract, integration-vet, shard plans),
         test SHARD COUNT (go test, the SHARD-th of COUNT weight-balanced
         package slices, ci/go_test_weights.tsv; shard 1 also runs the
         multi-replica worker gate), race SHARD
         COUNT (go test -race, the SHARD-th of COUNT weight-balanced package
         slices, ci/go_race_weights.d). The
         legs run exactly the steps of `ci` (pinned by a tooling test); each
         stage prints UTC start/done stamps.
  ci-leg-warm test|race SHARD COUNT
         Compile the packages of that leg slice with the leg own flags and
         run no test (go test -run ^$), to fill the leg Go build cache on a
         main push whose test and race legs the merge queue already ran.
  test-shard-plan SHARD COUNT
         Print the packages (module, import path, weights key) of the SHARD-th of
         COUNT weight-balanced slices of the unit tests (ci/go_test_weights.tsv,
         a package with no row weighs 1). Fails when the slice is empty, unless
         GO_QUALITY_SELECTION_FILE selects a package subset.
  race-shard-plan SHARD COUNT
         Print the packages (module, import path, weights key) the SHARD-th of
         COUNT race legs runs: its weight-balanced slice, its solo packages and
         internal/providersync (a test-name slice in every race leg).
  race-excluded-shard-plan SHARD COUNT
         Print the rows of ci/race_excluded_tests.tsv that the SHARD-th of COUNT
         test legs checks without -race: the rows whose package is in the plan of that
         shard (every row is checked in exactly one leg).
  race-excluded-receipts JSON...
         Read the receipt of every row of ci/race_excluded_tests.tsv from go test
         -json files: exactly one pass event per row, and a missing, skipped,
         failed, repeated or cached (package) result fails.
  test-shard-wall-guard SHARD COUNT WALL LOGFILE
         Log the wall seconds of a test shard and fail above 85% of the 30 min
         job cap (GO_TEST_SHARD_CAP_SECONDS), naming the shard and its three
         heaviest packages (measured in LOGFILE, else by weight).
  security-fresh-receipt RAN JSON...
         The cache receipt of a test or race leg (CHAOS-11448), read from the go test -json events of its stages: how many
         packages read "ok (cached)" (cached = NOT executed), and, with DEV_HEALTH_GO_FRESH_SECURITY=1 (merge_group), a
         failure when any package read "(cached)" or when a leg that ran RAN > 0 packages left no package summary
         (CHAOS-11745). security-fresh-split PKG... prints sec|plain per package.
  test-shard-unweighted-guard SHARD COUNT LOGFILE
         Fail when a package of the shard ran 45 s or more in LOGFILE (its go
         test log) and has no row in ci/go_test_weights.tsv; prints the row.
         From 30 s up to 45 s it only warns (::warning:: and a step-summary row).
         With DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY=1 (merge_group) it warns and exits 0; an unusable log still fails.
  build  Run go build ./... in every Go module.
  contract
         Validate the job contract tree and, when DEV_HEALTH_CONTRACT_BASE is
         set, reject breaking in-place changes against that directory.
  job-start
         Print CHECK_GO_JOB_START, the job start of the deadline (taken here when
         CHECK_GO_JOB_CAP_MIN is set and no earlier call took it), or an empty
         line without a cap. ci/check_golden_drift.sh takes it once for its rows.
  job-timeout MINUTES
         Print the go test -timeout, in seconds, of one integration call whose
         own limit is MINUTES: min(MINUTES, CHECK_GO_JOB_CAP_MIN - elapsed - 3 min).
         Rc 2 and a named line when no budget is left, or when CI has no cap.
         Does not need Go. ci/check_golden_drift.sh calls it for each row.
  multi-replica-workers
         Run the production ops-profile multi-replica claim, drain, and restart
         gate against real PostgreSQL with `-count=1`. Requires a non-zero
         measured-job proof artifact. Included in `fast`, `ci`, and `all`.
  integration-vet
         Compile-check every package under the integration build tag, across
         the WHOLE tree. No Docker required. This is what would have caught a
         compile break in an integration-tagged file the day it happened,
         instead of it sitting broken and silently unrun until someone passed
         -tags=integration by hand.
  integration-coverage
         Discover every integration-tagged package and print the run/skip
         plan (see INTEGRATION_DENYLIST in this script). No Docker required.
         Fails if the denylist names a package discovery does not find, or if
         discovery finds nothing at all.
  integration-shard-plan
         Validate ci/go_integration_shards.d/ against live package discovery,
         derive deterministic longest-processing-time-first shard assignments,
         print the complete assignment, and write a GitHub Actions `matrix`
         output (and `legs`, the leg count) when GITHUB_OUTPUT is set. No Docker
         required. With INTEGRATION_AFFECTED_FILE set to the output of
         cmd/cigate plan-verdict (CHAOS-9947), the matrix holds only the legs
         that cover an affected package; NONE plans no leg; unset, or a FULL
         first line, keeps every leg.
  ratchets
         Hold every closed list named in ci/ratchets.tsv to its size on the merge base: a list may
         shrink and never grow, and no pull request edits a shared number to say so (CHAOS-7904).
         The base is RATCHET_BASE_SHA when set (CI sets it to the base commit of the pull request), else the merge
         base with origin/main (the parent when HEAD is origin/main). A base that cannot be read is a
         failure, never a pass.
  race-weights-rows
         Hold ci/go_race_weights.d (one file per package) to the package list without running a test: every package has a
         file, no file names a package that is gone, each file holds one unique row (CHAOS-4233, CHAOS-10300). `fast`, `ci`
         and the static leg run it, so the stale row is named before push.
  integration-images
         Print "<key>\t<image>" for every image declared by the Go test
         container harness. The single source of truth for the dependency set:
         the pre-pull and the ghcr.io mirror workflow both read it, so neither
         re-parses harness.go on its own. No Docker required.
  integration-prepull
         Pre-pull every image declared by the Go test container harness --
         postgres, clickhouse, valkey, and the testcontainers-go reaper,
         which starts before the first container of any test binary whether or
         not a test asks for it. Retries registry failures at most three
         times per image before failing loudly. Takes no arguments on purpose:
         a per-job subset is a list nothing can check against what the job
         actually starts. CI runs this before every job that starts a container.
  integration-shard TARGET SHARD [--dry-run]
         Run exactly one validated integration shard. TARGET is `packages` for
         a package-level shard or `providersync` for a top-level test shard of
         the dominant internal/providersync package, or `daily` for a top-level
         test shard of internal/jobs/metrics/daily, or `workerservice` for a top-level
         test shard of internal/workerservice, or `queryapiserver` for a top-level
         test shard of internal/queryapi/server, or `remaining` for a top-level
         test shard of internal/jobs/metrics/remaining. `--dry-run` prints the
         exact selection without starting Docker-backed tests; CI never passes
         that option.
         Every target runs only the integration-tagged top-level tests (the
         untagged ones run in the go-quality test and race legs); a package
         whose tag-only test file declares func init or func TestMain runs whole.
  integration
         Discover and run EVERY integration-tagged package'\''s suite against
         real containers, except the (small, justified) INTEGRATION_DENYLIST.
         Inclusion is the default; exclusion is the explicit, loud exception.
         Same test selection as integration-shard.
  fast   Run fmt, vet, test, build, contract,
         integration-vet, and the integration shard-plan checks (PLAN only --
         does not execute the integration suite; see `ci`, `all`, and
         `integration`). No race detector -- the quick local-iteration mode.
  ci     Exactly `fast` plus the race detector (CHAOS-3948): fmt, vet, test,
         race, build, contract, integration-vet, and the
         integration shard-plan checks (PLAN only, same as `fast`), plus
         multi-replica-workers. This is byte-for-byte what `all` ran before
         CHAOS-3948 -- go.yml'\''s go-quality step uses this so its coverage
         stays unchanged. CI gets its real (sharded, parallel) integration
         signal from the separate go-storage-integration-plan/-shard jobs,
         not from this step.
  CHECK_GO_FRESH=1 (environment, plain local runs only)
         Every go call gets GOFLAGS=-count=1, so no test passes from the test
         cache. Refused (exit 2) under CI=true or GITHUB_ACTIONS=true.
         Hosted-like lane shard runs (CI=true) keep the go env GOCACHE and
         run go clean -testcache right before instead.
  all    Run fmt, vet, test, race, build, contract,
         integration-vet, the FULL integration suite (every non-denylisted
         package, unsharded -- Docker required), and multi-replica-workers
         (default). This is slower than `ci`: expect several more minutes on
         top of the unit/race suites (measured ~24m for the integration
         suite alone), and Docker running locally. The honest local pre-push
         signal `ci` cannot be, since `ci` stays PLAN-only by design.'
}

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 2
}

# CHAOS-11725: CHECK_GO_FRESH=1 is for plain local runs: GO_ENV_OFF sets GOFLAGS=-count=1 again after its scrub, so every go
# call runs its tests and none passes from the test cache (a test that reads a file through a child process, such as helm or
# bash, otherwise reads "ok (cached)" after an edit of only that file). Refused under CI=true or GITHUB_ACTIONS=true: hosted legs
# keep the test cache and its receipt, and an ignored knob would be a silent false green.
case "${CHECK_GO_FRESH:-}" in
  ""|0) ;;
  1)
    if [ "${CI:-}" = "true" ] || [ "${GITHUB_ACTIONS:-}" = "true" ]; then
      die "CHECK_GO_FRESH=1 is for local runs only; refused because CI=true or GITHUB_ACTIONS=true (hosted legs keep the test cache and its receipt; locally keep the go env GOCACHE and run go clean -testcache right before the run)"
    fi
    GO_ENV_OFF+=(GOFLAGS=-count=1)
    printf 'check_go.sh: CHECK_GO_FRESH=1: GOFLAGS=-count=1 on every go call (test cache off, local only)\n' >&2
    ;;
  *) die "CHECK_GO_FRESH must be 1 or unset, got '${CHECK_GO_FRESH}'" ;;
esac

cleanup_go_build_output() {
  if [ -z "${DEV_HEALTH_GO_BUILD_OUTPUT}" ]; then
    return 0
  fi
  case "${DEV_HEALTH_GO_BUILD_OUTPUT}" in
    "${DEV_HEALTH_GO_BUILD_TEMP_ROOT}"/dev-health-go-build.*)
      rm -rf -- "${DEV_HEALTH_GO_BUILD_OUTPUT}"
      ;;
    *)
      printf 'ERROR: refusing to remove unexpected build output %s\n' \
        "${DEV_HEALTH_GO_BUILD_OUTPUT}" >&2
      return 1
      ;;
  esac
  DEV_HEALTH_GO_BUILD_OUTPUT=""
}

command -v git >/dev/null 2>&1 || die "git is required"
command -v go >/dev/null 2>&1 || die "go is required"

case "$(go version)" in
  *" ${GO_TOOLCHAIN} "*) ;;
  *) die "Go ${GO_TOOLCHAIN#go} is required" ;;
esac
GOFMT="$(go env GOROOT)/bin/gofmt"
[ -x "${GOFMT}" ] || die "gofmt from ${GO_TOOLCHAIN} is required"

declare -a MODULE_DIRS=()

discover_modules() {
  local mod_file module_dir

  # Keep the production/root module first. Nested modules are deliberately run
  # separately because `go test ./...` stops at a nested go.mod boundary.
  if [ -f "${ROOT}/go.mod" ]; then
    MODULE_DIRS+=(".")
  fi

  while IFS= read -r -d '' mod_file; do
    [ "${mod_file}" != "go.mod" ] || continue
    [ -f "${ROOT}/${mod_file}" ] || continue
    case "${mod_file}" in
      vendor/*|*/vendor/*) continue ;;
    esac
    module_dir="${mod_file%/go.mod}"
    MODULE_DIRS+=("${module_dir}")
  done < <(
    git -C "${ROOT}" ls-files --cached --others --exclude-standard -z -- \
      ':(glob)**/go.mod'
  )

  [ "${#MODULE_DIRS[@]}" -gt 0 ] || die "no Go modules found under ${ROOT}"
}

print_modules() {
  local module_dir
  printf 'Go modules:\n'
  for module_dir in "${MODULE_DIRS[@]}"; do
    printf '  - %s\n' "${module_dir}"
  done
}

check_format() {
  local go_file output
  local -a go_files=()

  while IFS= read -r -d '' go_file; do
    [ -f "${ROOT}/${go_file}" ] || continue
    case "${go_file}" in
      vendor/*|*/vendor/*) continue ;;
    esac
    go_files+=("${go_file}")
  done < <(
    git -C "${ROOT}" ls-files --cached --others --exclude-standard -z -- '*.go'
  )

  if [ "${#go_files[@]}" -eq 0 ]; then
    printf 'gofmt: no Go files found\n'
    return 0
  fi

  output="$(cd "${ROOT}" && "${GOFMT}" -l "${go_files[@]}")"
  if [ -n "${output}" ]; then
    printf 'gofmt: these files need formatting:\n%s\n' "${output}" >&2
    return 1
  fi
  printf 'gofmt: clean\n'
}

run_in_modules() {
  local label="$1"
  shift
  local module_dir

  for module_dir in "${MODULE_DIRS[@]}"; do
    printf '%s: %s\n' "${label}" "${module_dir}"
    (
      cd "${ROOT}/${module_dir}"
      # Keep a nested N-1 compatibility module pinned to its own go.mod even if
      # a go.work file is introduced later at the repository root.
      "${GO_ENV_OFF[@]}" GOWORK=off "$@"
    )
  done
}

check_vet() {
  run_in_modules "go vet" go vet -mod=readonly -trimpath ./...
}

# Race-excluded proof (CHAOS-8135, CHAOS-8118, CHAOS-11406): a test compiled out of the -race leg by a `!race` build tag must
# still run in the non-race leg, or its measurement is lost silently. ci/race_excluded_tests.tsv is the closed list. The leg runs
# the packages that hold rows ONCE, fresh and without -race (`go test -json -count=1`), and the receipts are read from that one
# run: each row needs exactly one `pass` event of its own (Package + Test), and a missing event, a `skip`, a `fail`, a second
# terminal event, or a `(cached)` summary of its package all fail (a measurement that did not happen is a failure). An empty list
# or a missing file fails too. check_test and check_test_shard call it, so every leg that runs the unit tests runs it.
# With SHARD COUNT (CHAOS-10355, CHAOS-11406) race_excluded_rows lists only the rows whose package is in the SHARD-th of COUNT test
# shard plans; the plans partition `go list ./...`, so every row is checked in exactly one shard (`race-excluded-shard-plan SHARD
# COUNT` lists a shard's rows).
# race_excluded_rows [SHARD COUNT]: the rows of ci/race_excluded_tests.tsv (the shard's rows with SHARD COUNT), one per line.
# A line must be exactly <package><TAB><test> (CHAOS-11436): an empty line and a # comment are skipped; a whitespace-only line, one
# column, more than two columns or an empty column dies naming the line; a CRLF ending is trimmed.
race_excluded_rows() {
  local list="${GO_RACE_EXCLUDED_TESTS:-${ROOT}/ci/race_excluded_tests.tsv}" row shard="${1:-}" shard_count="${2:-}" plan="" plan_keys="" normalised
  local -a rows=()
  [ -f "${list}" ] || die "ci/race_excluded_tests.tsv is missing"
  normalised="$(awk -F'\t' '
    function trim(s) { gsub(/^[ \t\r]+|[ \t\r]+$/, "", s); return s }
    /^#/ || /^$/ { next }
    NF != 2 || trim($1) == "" || trim($2) == "" { printf "ERROR: census line %d of %s is not <package><TAB><test> (whitespace-only, 1 column, more than 2 columns, or an empty column): %s\n", NR, FILENAME, $0 > "/dev/stderr"; exit 1 }
    { key = trim($1) "\t" trim($2)
      if (key in seen) { printf "ERROR: duplicate census row (package<TAB>test) %s: lines %d and %d of %s\n", trim($1) " " trim($2), seen[key], NR, FILENAME > "/dev/stderr"; exit 1 }
      seen[key] = NR; print key }' "${list}")" || die "the race-excluded census holds a malformed or duplicated row"
  while IFS= read -r row; do [ -z "${row}" ] || rows+=("${row}"); done <<<"${normalised}"
  [ "${#rows[@]}" -gt 0 ] || die "ci/race_excluded_tests.tsv lists no test: nothing would be measured"
  if [ -n "${shard}" ]; then
    check_test_shard_args "${shard}" "${shard_count}"
    census_assert_listed "$(printf '%s\n' "${rows[@]}")"
    plan="$(check_test_shard_plan "${shard}" "${shard_count}")" || exit 2
    plan_keys="$(printf '%s\n' "${plan}" | awk -F'\t' '$1 == "." { print $3 }')"
  fi
  for row in "${rows[@]}"; do
    if [ -z "${shard}" ] || grep -qxF -- "${row%%$'\t'*}" <<<"${plan_keys}"; then printf '%s\n' "${row}"; fi
  done
}

# census_assert_listed ROWS: every row of ROWS names a package that `go list ./...` lists, whether or not this leg selects it. A
# stale row, or a directory `go list` omits (testdata, no buildable file), is in no shard plan, so no shard would ever check it.
census_assert_listed() {
  local census_rows="$1" modpath all row package
  modpath="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -m)" || die "go list -m failed"
  all="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -f '{{.ImportPath}}' ./...)" || die "go list failed"
  while IFS= read -r row; do
    [ -n "${row}" ] || continue
    package="${row%%$'\t'*}"
    grep -qxF -- "${modpath}/${package}" <<<"${all}" \
      || die "race-excluded package ${package} is not listed by go list ./...: no test shard would check its census row"
  done <<<"${census_rows}"
}

# census_package_partition KIND MODPATH KEYS: the import paths on stdin that are (KIND=rows) or are not (KIND=plain) packages
# of the census; KEYS is the newline list of census packages relative to the root module.
census_package_partition() {
  awk -v kind="$1" -v mod="$2" -v keys="$3" '
    BEGIN { n = split(keys, k, "\n"); for (i = 1; i <= n; i++) if (k[i] != "") isrow[k[i]] = 1 }
    NF { key = ($0 == mod) ? "." : substr($0, length(mod) + 2); if ((key in isrow) == (kind == "rows")) print }'
}

# leg_json_file KIND SHARD COUNT: the file a test or race leg keeps its go test -json in, emptied first:
# ${GO_TEST_LEG_JSON_DIR}/<KIND>-<SHARD>-of-<COUNT>.json, or /dev/null when the variable is unset (a local run, a stand-in test).
# It is a different directory from go-test-json/ (the uploaded providersync slice): the raw events carry test output and never leave
# the runner; the workflow reduces them to package/test/action/elapsed first.
leg_json_file() {
  local dir="${GO_TEST_LEG_JSON_DIR:-}"
  if [ -z "${dir}" ]; then
    printf '/dev/null\n'
    return 0
  fi
  mkdir -p -- "${dir}" && : >"${dir}/$1-$2-of-$3.json" || return 1
  printf '%s\n' "${dir}/$1-$2-of-$3.json"
}

# go_test_leg LOG LEGJSON ARG...: `go test -json ARG...` of one stage of a test or race leg (run it from the module directory).
# The plain log, on stdout and in LOG, is every Output field of the event stream verbatim, whole lines per package (GO_TEST_LEG_JQ; the -v form: more
# lines than plain go test, never fewer). A line that is not a JSON event passes through raw. The raw stream is kept in a stage
# temp file and appended to LEGJSON (/dev/null when the leg keeps no timings) when the stage ends. Every element of the pipe is
# read right after it: go test's own status is the stage status; a failed converter, a failed copy of the raw stream, a failed
# write of the plain log or of LEGJSON is named on stderr and makes the stage fail (rc 70) when go test passed. A failed converter
# drains its input (go test is never killed by a broken pipe) and the raw stream is printed into the plain log, so the log is
# never empty.
# CHAOS-12386: go test -json cuts a test line longer than 1024 bytes into several output events, and packages run in parallel, so an
# event of another package (its "ok" summary line) can land between two chunks of one line. The filter keeps the unfinished tail of
# each package apart (key: Package) and prints only whole lines: a line of one package is never mixed with another package's text.
# The tail of a package is printed when its package-level pass/fail/skip event arrives, and every tail at the end of the stream, so no
# byte is lost (a hung or killed package keeps its last words). A line that is not a JSON event is whole and is printed at once.
# shellcheck disable=SC2016
GO_TEST_LEG_JQ='
foreach ((inputs | ((fromjson? | select(type == "object" and ((.Action | type) == "string"))) // {Action: "raw", Output: (. + "\n")})), null) as $e (
  {tails: {}, out: ""};
    if $e == null then
      {tails: {}, out: ([.tails[]] | join(""))}
    elif $e.Action == "raw" then
      .out = $e.Output
    elif $e.Action == "output" then
      (($e.Package // "") | tostring) as $p
      | ((.tails[$p] // "") + (($e.Output // "") | tostring) | split("\n")) as $parts
      | .out = ($parts[:-1] | map(. + "\n") | join(""))
      | .tails[$p] = ($parts[-1] // "")
    elif ($e.Action | IN("pass", "fail", "skip")) and (($e.Test // "") == "") then
      (($e.Package // "") | tostring) as $p
      | .out = (.tails[$p] // "")
      | .tails[$p] = ""
    else
      .out = ""
    end;
    .out)'
go_test_leg() {
  local log="$1" legjson="$2" rc raw problem=0
  local -a ps fb
  shift 2
  command -v jq >/dev/null 2>&1 || die "the test and race legs need jq to read go test -json"
  raw="$(mktemp "${TMPDIR:-/tmp}/dev-health-leg-raw.XXXXXX")" || die "go_test_leg: no temp file for the raw go test -json stream"
  set +e
  "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath ${MERGE_GROUP_FRESH:+-count=1} -json "$@" 2>&1 \
    | tee "${raw}" \
    | { jq -nR --unbuffered -rj "${GO_TEST_LEG_JQ}"; jqrc=$?; [ "${jqrc}" -eq 0 ] || cat >/dev/null; exit "${jqrc}"; } \
    | tee -a "${log}"
  ps=("${PIPESTATUS[@]}")
  set -e
  rc="${ps[0]}"
  if [ "${ps[2]}" -ne 0 ]; then
    printf 'go_test_leg: JSON converter failed (jq rc %s): the raw go test -json stream follows in the plain log\n' "${ps[2]}" >&2
    set +e
    cat "${raw}" | tee -a "${log}"
    fb=("${PIPESTATUS[@]}")
    set -e
    if [ "${fb[1]}" -ne 0 ]; then
      printf 'go_test_leg: plain log write failed (tee rc %s)\n' "${fb[1]}" >&2
    fi
    problem=1
  fi
  if [ "${ps[1]}" -ne 0 ]; then
    printf 'go_test_leg: the copy of the raw stream failed (tee rc %s)\n' "${ps[1]}" >&2
    problem=1
  fi
  if [ "${ps[3]}" -ne 0 ]; then
    printf 'go_test_leg: the plain log write failed (tee rc %s)\n' "${ps[3]}" >&2
    problem=1
  fi
  if [ "${legjson}" != /dev/null ] && ! cat "${raw}" >>"${legjson}"; then
    printf 'go_test_leg: the raw file write failed: %s\n' "${legjson}" >&2
    problem=1
  fi
  rm -f -- "${raw}"
  if [ "${problem}" -ne 0 ] && [ "${rc}" -eq 0 ]; then rc=70; fi
  return "${rc}"
}

# cigates_go_test_timeout PKG...: the go test -timeout of one call (CHAOS-11787; interim, removed by CHAOS-11768 (widened)). The internal/cigates
# packages run 485-757 s in a hosted test leg against the 10m default, so a call whose package list holds one gets 20m (a hang ceiling, no
# time assert). COST: every other package of that call gets 20m too. The match is the exact import-path prefix with the slash: the bare
# internal/cigates, internal/cigatesX and other modules give 10m. The timeout is an argument of the call, never read from the environment.
cigates_go_test_timeout() {
  local p
  for p in "$@"; do
    case "${p}" in
      github.com/full-chaos/dev-health-ops/internal/cigates/?*) printf '20m\n'; return 0 ;;
    esac
  done
  printf '10m\n'
}

# go_test_json_run JSON_FILE LOG_FILE PKG...: go_test_json_run_t with the 10m go test -timeout.
go_test_json_run() {
  local json="$1" log="$2"
  shift 2
  go_test_json_run_t 10m "${json}" "${log}" "$@"
}

# go_test_json_run_t TIMEOUT JSON_FILE LOG_FILE PKG...: ONE fresh `go test -json` run of the packages (run it from the module directory).
# The events are appended to JSON_FILE; jq (GO_TEST_LEG_JQ, whole lines per package) turns them back into the plain go test log on stdout and in LOG_FILE (the ok lines
# the wall and unweighted guards read). pipefail keeps go test's exit code. TIMEOUT is an argument, never read from the environment.
go_test_json_run_t() {
  local timeout="$1" json="$2" log="$3"
  shift 3
  command -v jq >/dev/null 2>&1 || die "the race-excluded receipts need jq to read go test -json events"
  "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -json -count=1 -timeout "${timeout}" "$@" \
    | tee -a "${json}" \
    | jq -nR --unbuffered -rj "${GO_TEST_LEG_JQ}" \
    | tee -a "${log}"
}

# check_race_excluded_receipts ROWS JSON_FILE...: the receipt for every row of ROWS (package<TAB>test lines) from the go test -json
# events of the fresh run. One `pass` event of the test itself (not a subtest, not another package) per row; see above.
check_race_excluded_receipts() {
  local listing="$1" row package name count=0 modpath actions
  shift
  [ "$#" -gt 0 ] || die "race-excluded receipts need at least one go test -json file"
  command -v jq >/dev/null 2>&1 || die "the race-excluded receipts need jq to read go test -json events"
  load_quality_selection
  modpath="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -m)" || die "go list -m failed"
  while IFS= read -r row; do
    [ -n "${row}" ] || continue
    name="${row#*$'\t'}"
    package="${row%%$'\t'*}"
    [ -n "${name}" ] && [ -n "${package}" ] && [ "${name}" != "${package}" ] || die "race_excluded_tests.tsv row is not <package><TAB><test>"
    quality_selected . "${package}" || continue
    [ -d "${ROOT}/${package}" ] || die "race-excluded package ${package} does not exist"
    if jq -e --arg p "${modpath}/${package}" 'select(.Action == "output" and .Package == $p and ((.Output // "") | test("\\(cached\\)")))' "$@" >/dev/null 2>&1; then
      die "race-excluded package ${package} was served from the test cache: its tests did not run in the non-race leg (cached)"
    fi
    actions="$(jq -r --arg p "${modpath}/${package}" --arg n "${name}" \
      'select(.Package == $p and .Test == $n and (.Action == "pass" or .Action == "fail" or .Action == "skip")) | .Action' "$@")" \
      || die "cannot read the go test -json events for race-excluded test ${package} ${name}"
    case "${actions}" in
      pass) ;;
      "") die "race-excluded test ${package} ${name} did not run in the non-race leg (no terminal event)" ;;
      *skip*) die "race-excluded test ${package} ${name} was skipped in the non-race leg" ;;
      *fail*) die "race-excluded test ${package} ${name} failed in the non-race leg" ;;
      *) die "race-excluded test ${package} ${name} has more than one terminal event in the non-race leg" ;;
    esac
    printf 'race-excluded-ran: %s %s PASS\n' "${package}" "${name}"
    count=$((count + 1))
  done <<<"${listing}"
  printf 'race-excluded-ran: %d test(s) ran and passed without -race\n' "${count}"
}

# check_race_excluded_ran (verb race-excluded-ran, no SHARD): every census package once (one process per package, its rows in
# one -run), then the receipts.
check_race_excluded_ran() {
  local listing json package names
  load_quality_selection
  listing="$(race_excluded_rows)" || exit 2
  census_assert_listed "${listing}"
  json="$(mktemp "${TMPDIR:-/tmp}/dev-health-race-excluded.XXXXXX")"
  while IFS= read -r package; do
    [ -n "${package}" ] || continue
    quality_selected . "${package}" || continue
    [ -d "${ROOT}/${package}" ] || { rm -f -- "${json}"; die "race-excluded package ${package} does not exist"; }
    names="$(printf '%s\n' "${listing}" | awk -F'\t' -v p="${package}" '$1 == p { printf "%s%s", (n++ ? "|" : ""), $2 }')"
    (cd "${ROOT}" && go_test_json_run "${json}" /dev/null -run "^(${names})\$" "./${package}") \
      || { rm -f -- "${json}"; die "race-excluded tests of ${package} failed in the non-race leg"; }
  done < <(printf '%s\n' "${listing}" | cut -f1 | LC_ALL=C sort -u)
  check_race_excluded_receipts "${listing}" "${json}" || { rm -f -- "${json}"; return 1; }
  rm -f -- "${json}"
}

# SERIAL_HEAVY_TEST_PACKAGE (CHAOS-9867): internal/gqlgenguard runs ~8 whole-repository gqlgen generations one after
# another (no t.Parallel: it moves the working directory), each a `go run` that type-checks every package. Beside the
# other test binaries of one `go test ./...` it is starved of CPU: 545 s in the hosted test leg against 393 s in a race
# leg and ~213 s alone, with the default 600 s go test timeout as the only margin, so one slow runner timed it out
# (run 38786639144). It now runs alone, after the rest, so its time no longer depends on what else is scheduled.
SERIAL_HEAVY_TEST_PACKAGE="github.com/full-chaos/dev-health-ops/internal/gqlgenguard"

# GO_QUALITY_SELECTION_FILE: the verdict of `cmd/cigate plan-verdict --go-files-full` (the go-quality-plan job). Unset,
# empty or a FULL first line runs every package. `AFFECTED <n>` and n root-module keys run only those packages: the test and
# race slices are cut from them, a slice may then be empty, and the other modules run none. NONE never reaches a leg (the
# workflow skips the test and race legs); here it is a failure, as any other first line is.
QUALITY_FILTER_ACTIVE=0
declare -A QUALITY_KEYS=()
load_quality_selection() {
  QUALITY_FILTER_ACTIVE=0
  QUALITY_KEYS=()
  local file="${GO_QUALITY_SELECTION_FILE:-}" first line declared count=0
  [ -n "${file}" ] || return 0
  [ -f "${file}" ] || die "GO_QUALITY_SELECTION_FILE not found: ${file}"
  first="$(head -n 1 "${file}")"
  case "${first}" in
    "FULL "*) return 0 ;;
    "AFFECTED "*) ;;
    *) die "GO_QUALITY_SELECTION_FILE ${file} has an unknown first line '${first}'" ;;
  esac
  declared="${first#AFFECTED }"
  case "${declared}" in ""|*[!0-9]*) die "GO_QUALITY_SELECTION_FILE count is not numeric: '${declared}'" ;; esac
  while IFS= read -r line; do
    [ -n "${line}" ] || continue
    [ -d "${ROOT}/${line}" ] || die "go-quality selection names '${line}', which is not a directory of the root module"
    [ -z "${QUALITY_KEYS[${line}]+set}" ] || die "go-quality selection lists '${line}' more than once"
    QUALITY_KEYS["${line}"]=1
    count=$((count + 1))
  done < <(tail -n +2 "${file}")
  [ "${count}" -eq "${declared}" ] || die "go-quality selection declares ${declared} package(s) but lists ${count}"
  QUALITY_FILTER_ACTIVE=1
}

# quality_selected MODULE_DIR KEY: true when every package runs or KEY (relative to the root module) is selected.
quality_selected() {
  [ "${QUALITY_FILTER_ACTIVE}" -eq 0 ] || { [ "$1" = "." ] && [ -n "${QUALITY_KEYS[$2]+set}" ]; }
}

# quality_filter MODULE_DIR MODPATH: the import paths on stdin that the selection runs.
quality_filter() {
  local module_dir="$1" modpath="$2" pkg key
  while IFS= read -r pkg; do
    [ -n "${pkg}" ] || continue
    key="${pkg#"${modpath}/"}"
    [ "${pkg}" != "${modpath}" ] || key="."
    if quality_selected "${module_dir}" "${key}"; then printf '%s\n' "${pkg}"; fi
  done
}

check_test() {
  local module_dir all modpath rest rest_rows census_keys ran_heavy=0 json heavy_key
  census_keys="$(race_excluded_rows | cut -f1 | LC_ALL=C sort -u)" || exit 2
  census_assert_listed "$(race_excluded_rows)"
  json="$(mktemp "${TMPDIR:-/tmp}/dev-health-test-json.XXXXXX")"
  for module_dir in "${MODULE_DIRS[@]}"; do
    printf 'go test: %s\n' "${module_dir}"
    all="$(cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -f '{{.ImportPath}}' ./...)" \
      || die "go list failed in ${module_dir}"
    modpath="$(cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -m)" || die "go list -m failed in ${module_dir}"
    heavy_key="${SERIAL_HEAVY_TEST_PACKAGE#"${modpath}/"}"
    rest="$(printf '%s\n' "${all}" | grep -vx -- "${SERIAL_HEAVY_TEST_PACKAGE}" || true)"
    rest_rows=""
    if [ "${module_dir}" = "." ]; then
      rest_rows="$(printf '%s\n' "${rest}" | census_package_partition rows "${modpath}" "${census_keys}")"
      rest="$(printf '%s\n' "${rest}" | census_package_partition plain "${modpath}" "${census_keys}")"
    fi
    (
      cd "${ROOT}/${module_dir}"
      if [ -n "${rest}" ]; then
        # shellcheck disable=SC2086
        "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath ${rest}
      fi
      if [ -n "${rest_rows}" ]; then
        # shellcheck disable=SC2086
        go_test_json_run "${json}" /dev/null ${rest_rows}
      fi
    ) || { rm -f -- "${json}"; return 1; }
    if printf '%s\n' "${all}" | grep -qx -- "${SERIAL_HEAVY_TEST_PACKAGE}"; then
      ran_heavy=1
      printf 'go test (alone): %s\n' "${SERIAL_HEAVY_TEST_PACKAGE}"
      (
        cd "${ROOT}/${module_dir}"
        if [ "${module_dir}" = "." ] && grep -qxF -- "${heavy_key}" <<<"${census_keys}"; then
          go_test_json_run "${json}" /dev/null "${SERIAL_HEAVY_TEST_PACKAGE}"
        else
          "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath "${SERIAL_HEAVY_TEST_PACKAGE}"
        fi
      ) || { rm -f -- "${json}"; return 1; }
    fi
  done
  [ "${ran_heavy}" -eq 1 ] || { rm -f -- "${json}"; die "check_test never ran ${SERIAL_HEAVY_TEST_PACKAGE}: it was split out of ./... and must run alone"; }
  check_race_excluded_receipts "$(race_excluded_rows)" "${json}" || { rm -f -- "${json}"; return 1; }
  rm -f -- "${json}"
}

# CHAOS-10355: the test leg runs as COUNT weight-balanced shards (`ci-leg test SHARD COUNT`). The single leg held ~3,030 s of
# package time (19.6-28.5 min of the 30 min hosted cap); ci/go_test_weights.tsv holds the heavy packages (>= 20 s), a package
# with no row weighs 1, and ci/go_race_shard.awk (longest-processing-time) places them: a wrong weight costs balance, never
# coverage. internal/gqlgenguard stays alone (see SERIAL_HEAVY_TEST_PACKAGE), in whichever shard the split puts it.
GO_TEST_WEIGHTS="${GO_TEST_WEIGHTS:-${ROOT}/ci/go_test_weights.tsv}"
GO_TEST_SHARD_CAP_SECONDS="${GO_TEST_SHARD_CAP_SECONDS:-1800}"
GO_TEST_SHARD_WALL_PERCENT=85

check_test_shard_args() {
  local shard="${1:-}" count="${2:-}"
  case "${shard}" in ""|*[!0-9]*) die "test SHARD must be a positive integer, got '${shard}'" ;; esac
  case "${count}" in ""|*[!0-9]*) die "test COUNT must be a positive integer, got '${count}'" ;; esac
  { [ "${shard}" -ge 1 ] && [ "${count}" -ge 1 ] && [ "${shard}" -le "${count}" ]; } \
    || die "test shard ${shard} is outside 1..${count}"
  [ -f "${GO_TEST_WEIGHTS}" ] || die "test shards need ${GO_TEST_WEIGHTS}"
}

# check_test_shard_plan SHARD COUNT: one "module<TAB>package<TAB>key" line per package of the shard, key = import path relative
# to the module root (the weights key). A shard that selects no package fails: it would read green while running nothing.
check_test_shard_plan() {
  local shard="${1:-}" count="${2:-}" module_dir modpath pkg key selected=0
  check_test_shard_args "${shard}" "${count}"
  load_quality_selection
  for module_dir in "${MODULE_DIRS[@]}"; do
    modpath="$(cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -m)" || die "go list -m failed in ${module_dir}"
    while IFS= read -r pkg; do
      [ -n "${pkg}" ] || continue
      key="${pkg#"${modpath}/"}"
      [ "${pkg}" != "${modpath}" ] || key="."
      printf '%s\t%s\t%s\n' "${module_dir}" "${pkg}" "${key}"
      selected=$((selected + 1))
    done < <(cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -f '{{.ImportPath}}' ./... \
      | quality_filter "${module_dir}" "${modpath}" \
      | awk -v mod="${modpath}" -v shard="${shard}" -v count="${count}" -v weights="${GO_TEST_WEIGHTS}" -v defw=1 -f "${ROOT}/ci/go_race_shard.awk")
  done
  [ "${selected}" -gt 0 ] || [ "${QUALITY_FILTER_ACTIVE}" -eq 1 ] \
    || die "test shard ${shard}/${count} selected zero packages -- the matrix is wider than the package list, so this leg would read green while running nothing"
}

# check_test_shard_wall_guard SHARD COUNT WALL_SECONDS LOGFILE: logs the shard's wall time and FAILS above 85% of the job cap
# (GO_TEST_SHARD_CAP_SECONDS, 1800), naming the shard and its three heaviest packages: by measured time (the `ok` lines of the
# go test log), else by weight when the log holds none.
check_test_shard_wall_guard() {
  local shard="${1:-}" count="${2:-}" wall="${3:-}" log="${4:-}" limit top="" basis="measured time"
  check_test_shard_args "${shard}" "${count}"
  case "${wall}" in ""|*[!0-9]*) die "test shard wall seconds must be a non-negative integer, got '${wall}'" ;; esac
  limit=$((GO_TEST_SHARD_CAP_SECONDS * GO_TEST_SHARD_WALL_PERCENT / 100))
  printf 'test shard %s/%s: wall %ss (limit %ss = %s%% of the %ss cap)\n' "${shard}" "${count}" "${wall}" "${limit}" "${GO_TEST_SHARD_WALL_PERCENT}" "${GO_TEST_SHARD_CAP_SECONDS}"
  [ "${wall}" -gt "${limit}" ] || return 0
  if [ -f "${log}" ]; then
    top="$(awk -F'\t' '$1 ~ /^ok/ && $3 ~ /^[0-9.]+s$/ { t = $3; sub(/s$/, "", t); print t "\t" $2 }' "${log}" \
      | LC_ALL=C sort -t$'\t' -k1,1nr -k2,2 \
      | awk -F'\t' 'NR <= 3 { printf "%s%s (%ss)", (NR > 1 ? ", " : ""), $2, $1 }')"
  fi
  if [ -z "${top}" ]; then
    basis="weight (no package time in the log)"
    top="$(check_test_shard_plan "${shard}" "${count}" \
      | awk -F'\t' -v weights="${GO_TEST_WEIGHTS}" '
          BEGIN { while ((getline line < weights) > 0) { if (line ~ /^[[:space:]]*(#|$)/) continue; split(line, f, "\t"); w[f[1]] = f[2] + 0 } }
          { printf "%d\t%s\n", ($3 in w) ? w[$3] : 1, $2 }' \
      | LC_ALL=C sort -t$'\t' -k1,1nr -k2,2 \
      | awk -F'\t' 'NR <= 3 { printf "%s%s (weight %s)", (NR > 1 ? ", " : ""), $2, $1 }')"
  fi
  die "test shard ${shard}/${count} took ${wall}s, over ${GO_TEST_SHARD_WALL_PERCENT}% (${limit}s) of the ${GO_TEST_SHARD_CAP_SECONDS}s job cap; heaviest packages by ${basis}: ${top} -- rebalance ci/go_test_weights.tsv or add a shard"
}

# check_test_shard_unweighted_guard SHARD COUNT LOGFILE [partial] (CHAOS-10498): fails when a package of the shard ran 45 s or more
# (DEV_HEALTH_GO_TEST_UNWEIGHTED_FAIL_SECONDS) in LOGFILE (the leg's go test log) and has no row in ci/go_test_weights.tsv, and prints
# the row to add; from 30 s (DEV_HEALTH_GO_TEST_UNWEIGHTED_SLOW_SECONDS) up to the fail threshold it only warns (::warning:: plus a
# step-summary row, exit 0). A fail threshold below the warn threshold or not a whole number exits 2. A package with no row weighs
# 1, so an unweighted heavy package unbalances the shards without anyone seeing it; this makes it red on the leg that ran it.
# DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY=1 (go-quality.yml sets it on merge_group only, CHAOS-11682) turns a named package (awk exit 1) into a
# ::warning:: plus a step-summary line and exit 0: a package the entry did not touch must not eject it. An unusable log (awk exit 2) fails always.
check_test_shard_unweighted_guard() {
  local shard="${1:-}" count="${2:-}" log="${3:-}" partial="${4:-}" plan
  check_test_shard_args "${shard}" "${count}"
  { [ -f "${log}" ] && [ -r "${log}" ]; } || die "test-shard-unweighted-guard needs the shard's go test log as a readable file, got '${log}'"
  plan="$(check_test_shard_plan "${shard}" "${count}")"
  local out rc=0
  out="$(awk -v weights="${GO_TEST_WEIGHTS}" -v thresh="${DEV_HEALTH_GO_TEST_UNWEIGHTED_SLOW_SECONDS:-30}" -v failthresh="${DEV_HEALTH_GO_TEST_UNWEIGHTED_FAIL_SECONDS:-45}" -v label="${GO_TEST_WEIGHTS#"${ROOT}/"}" -v partial="${partial:+1}" \
    -f "${GO_TEST_UNWEIGHTED_SLOW_AWK:-${ROOT}/ci/go_test_unweighted_slow.awk}" <(printf '%s\n' "${plan}") "${log}" 2>&1)" || rc=$?
  local wline
  while IFS= read -r wline; do
    case "${wline}" in
      unweighted-slow-warning:*|nested-module-slow-warning:*)
        printf '::warning::%s\n' "${wline}" >&2
        if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
          printf 'unweighted-slow warning (add the row in a pull request): %s\n' "${wline}" >> "${GITHUB_STEP_SUMMARY}" || true
        fi ;;
    esac
  done <<< "${out}"
  if [ "${rc}" -ne 1 ] || [ "${DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY:-}" != "1" ]; then
    [ -z "${out}" ] || printf '%s\n' "${out}" >&2
    return "${rc}"
  fi
  printf '::warning::report only (merge_group): %s\n' "${out}" >&2
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    printf 'unweighted-slow guard (report only, add the row in a pull request):\n%s\n' "${out}" >> "${GITHUB_STEP_SUMMARY}" || true
  fi
}

# CHAOS-11448: merge_group runs the security packages of ci/security_fresh_packages.txt with -count=1 in their own call (their tests
# always execute). DEV_HEALTH_GO_FRESH_SECURITY=1 turns it on (go-quality.yml sets it on merge_group only).
SECURITY_FRESH_FILE="${DEV_HEALTH_GO_SECURITY_FRESH_LIST:-${ROOT}/ci/security_fresh_packages.txt}"
SECURITY_FRESH_SET=""

security_fresh_active() { [ "${DEV_HEALTH_GO_FRESH_SECURITY:-}" = "1" ]; }

# CHAOS-11745: on merge_group EVERY go test of the test and race legs carries -count=1 (${MERGE_GROUP_FRESH:+-count=1}): the
# restored build cache holds test results, and a test that reads a file through a child process (helm, bash, go, git) reads
# "ok (cached)" after that file changed. The build cache stays; only the test results are not reused. A pull request keeps them.
MERGE_GROUP_FRESH=""
if security_fresh_active; then MERGE_GROUP_FRESH=1; fi

# security_fresh_load: the import paths of the list (root module) in SECURITY_FRESH_SET; a path that does not exist is fatal.
security_fresh_load() {
  local line dir
  local -a pats=()
  [ -r "${SECURITY_FRESH_FILE}" ] || die "the security -count=1 list ${SECURITY_FRESH_FILE} is not a readable file"
  while IFS= read -r line || [ -n "${line}" ]; do
    line="${line%%#*}"
    line="${line//[[:space:]]/}"
    [ -n "${line}" ] || continue
    case "${line}" in ./*/...) ;; *) die "${SECURITY_FRESH_FILE}: '${line}' is not a ./dir/... pattern" ;; esac
    dir="${line%/...}"
    [ -d "${ROOT}/${dir}" ] || die "${SECURITY_FRESH_FILE}: ${line} names a directory that does not exist"
    pats+=("${line}")
  done < "${SECURITY_FRESH_FILE}"
  [ "${#pats[@]}" -gt 0 ] || die "${SECURITY_FRESH_FILE} lists no package"
  SECURITY_FRESH_SET="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly "${pats[@]}")" \
    || die "go list failed for ${SECURITY_FRESH_FILE}"
}

# security_split in|out: the import paths on stdin that are (in) or are not (out) in the security set. Needs security_fresh_load.
security_split() {
  local set_file rc=0
  set_file="$(mktemp "${TMPDIR:-/tmp}/dev-health-security-set.XXXXXX")"
  printf '%s\n' "${SECURITY_FRESH_SET}" > "${set_file}"
  if [ "$1" = in ]; then grep -Fx -f "${set_file}" || rc=$?; else grep -Fvx -f "${set_file}" || rc=$?; fi
  rm -f -- "${set_file}"
  [ "${rc}" -le 1 ] || return "${rc}"
}

# check_security_fresh_receipt JSON...: the cache receipt of a test or race leg, read from the go test -json events of its stages.
# A package's own summary event (an output event with no Test) that reads "ok ... (cached)" is cached = NOT executed; a line a
# test prints carries its Test and is never read as a summary. On merge_group any cached package fails the leg (CHAOS-11745); a
# "[no tests to run]" package has nothing to execute and is not counted. RAN is the number of packages the leg ran: on merge_group
# a leg that ran packages and left no package summary (an empty or unreadable events file) fails, it never reads as 0 cached.
check_security_fresh_receipt() {
  local ran="${1:-}" f events bad cached summaries set_file fresh sec_cached summary
  case "${ran}" in ""|*[!0-9]*) die "security-fresh-receipt needs RAN, the number of packages the leg ran, got '${ran}'" ;; esac
  shift
  [ "$#" -gt 0 ] || die "security-fresh-receipt needs the -json event file(s) of the leg"
  for f in "$@"; do
    { [ -f "${f}" ] && [ -r "${f}" ]; } || die "security-fresh-receipt needs the -json events of the leg as readable files, got '${f}'"
  done
  command -v jq >/dev/null 2>&1 || die "security-fresh-receipt needs jq to read the -json events"
  events="$(cat -- "$@" | jq -nR -r '
    inputs
    | fromjson?
    | select(type == "object" and .Action == "output" and ((.Test // "") == "") and ((.Package // "") != ""))
    | ((.Output // "") | tostring) as $o
    | select($o | test("^(ok|FAIL|\\?)\\s"))
    | ("S " + .Package),
      (select(($o | test("^ok\\s")) and ($o | test("\\[no tests to run\\]") | not))
       | (if ($o | test("\\(cached\\)")) then "C " else "R " end) + .Package)' | LC_ALL=C sort -u)" \
    || die "security-fresh-receipt could not read the -json events"
  bad="$(printf '%s\n' "${events}" | sed -n 's/^C //p')"
  cached="$(printf '%s\n' "${bad}" | grep -c . || true)"
  summaries="$(printf '%s\n' "${events}" | grep -c '^S ' || true)"
  summary="$(printf 'cache-receipt: %s package(s) reported "ok (cached)": cached = NOT executed by this leg\n' "${cached}")"
  if ! security_fresh_active; then
    summary="${summary}"$'\n'"security-fresh: off (event is not merge_group)"
    printf '%s\n' "${summary}"
    [ -z "${GITHUB_STEP_SUMMARY:-}" ] || printf '%s\n' "${summary}" >> "${GITHUB_STEP_SUMMARY}"
    return 0
  fi
  security_fresh_load
  set_file="$(mktemp "${TMPDIR:-/tmp}/dev-health-security-set.XXXXXX")"
  printf '%s\n' "${SECURITY_FRESH_SET}" > "${set_file}"
  fresh="$(printf '%s\n' "${events}" | sed -n 's/^R //p' | grep -Fxc -f "${set_file}" || true)"
  sec_cached="$(printf '%s\n' "${bad}" | grep -Fxc -f "${set_file}" || true)"
  rm -f -- "${set_file}"
  summary="${summary}"$'\n'"$(printf 'security-fresh: %s package(s) of ci/security_fresh_packages.txt executed with -count=1 on merge_group, %s cached' "${fresh}" "${sec_cached}")"
  summary="${summary}"$'\n'"$(printf 'merge-group-fresh: every go test of this leg ran with -count=1, %s package(s) cached' "${cached}")"
  printf '%s\n' "${summary}"
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || printf '%s\n' "${summary}" >> "${GITHUB_STEP_SUMMARY}"
  [ "${ran}" -eq 0 ] || [ "${summaries}" -gt 0 ] \
    || die "the -json events of this leg hold no package summary although it ran ${ran} package(s): the receipt cannot tell what ran"
  [ "${cached}" -eq 0 ] || die "package(s) read ok (cached) on merge_group: their tests were NOT executed: $(printf '%s\n' "${bad}" | tr '\n' ' ')"
}

# LEG_EVENTS_TMP: the -json events of a test or race leg that keeps no timings (GO_TEST_LEG_JSON_DIR unset), for its cache
# receipt; an EXIT trap removes it on every exit path.
LEG_EVENTS_TMP=""
remove_leg_events_tmp() { [ -z "${LEG_EVENTS_TMP}" ] || rm -f -- "${LEG_EVENTS_TMP}"; }
leg_events_file() {
  LEG_EVENTS_TMP="$(mktemp "${TMPDIR:-/tmp}/dev-health-$1-shard-events.XXXXXX")" || die "no temp file for the -json events of the $1 leg"
  trap remove_leg_events_tmp EXIT
}

# check_test_shard SHARD COUNT: the SHARD-th of COUNT slices of the unit tests (what check_test runs as one). The slices
# partition `go list ./...`; internal/gqlgenguard runs alone after the rest of its shard. The packages that hold race-excluded rows run once,
# fresh, with go test -json, and the shard reads the receipts of its rows from that run (check_race_excluded_receipts): no second execution.
check_test_shard() {
  local shard="${1:-}" count="${2:-}" plan log json started wall module_dir rest rest_rows rest_sec rows_timeout census_keys heavy_is_row listing legjson
  check_test_shard_args "${shard}" "${count}"
  plan="$(check_test_shard_plan "${shard}" "${count}")"
  listing="$(race_excluded_rows "${shard}" "${count}")" || exit 2
  census_keys="$(race_excluded_rows | cut -f1 | LC_ALL=C sort -u)" || exit 2
  log="$(mktemp "${TMPDIR:-/tmp}/dev-health-test-shard.XXXXXX")"
  json="$(mktemp "${TMPDIR:-/tmp}/dev-health-test-shard-json.XXXXXX")"
  legjson="$(leg_json_file test "${shard}" "${count}")" || exit 2
  # The cache receipt reads the stages' -json events: a leg that keeps no timings keeps them in a temp file.
  if [ "${legjson}" = /dev/null ]; then
    leg_events_file test
    legjson="${LEG_EVENTS_TMP}"
  fi
  started="$(date +%s)"
  if security_fresh_active; then security_fresh_load; fi
  for module_dir in "${MODULE_DIRS[@]}"; do
    rest="$(printf '%s\n' "${plan}" | awk -F'\t' -v m="${module_dir}" -v heavy="${SERIAL_HEAVY_TEST_PACKAGE}" -v keys="${census_keys}" '
      BEGIN { n = split(keys, k, "\n"); for (i = 1; i <= n; i++) if (k[i] != "") isrow[k[i]] = 1 }
      $1 == m && $2 != heavy && !(m == "." && ($3 in isrow)) { print $2 }')"
    rest_rows="$(printf '%s\n' "${plan}" | awk -F'\t' -v m="${module_dir}" -v heavy="${SERIAL_HEAVY_TEST_PACKAGE}" -v keys="${census_keys}" '
      BEGIN { n = split(keys, k, "\n"); for (i = 1; i <= n; i++) if (k[i] != "") isrow[k[i]] = 1 }
      $1 == m && $2 != heavy && m == "." && ($3 in isrow) { print $2 }')"
    # shellcheck disable=SC2086
    rows_timeout="$(cigates_go_test_timeout ${rest_rows})"
    rest_sec=""
    if security_fresh_active && [ "${module_dir}" = "." ]; then
      rest_sec="$(printf '%s\n' "${rest}" | security_split in)"
      rest="$(printf '%s\n' "${rest}" | security_split out)"
    fi
    printf 'go test%s (shard %s/%s): %s: %d package(s), %d with race-excluded rows\n' "${MERGE_GROUP_FRESH:+ -count=1}" "${shard}" "${count}" "${module_dir}" \
      "$(printf '%s\n%s\n' "${rest}" "${rest_rows}" | grep -c . || true)" "$(printf '%s\n' "${rest_rows}" | grep -c . || true)"
    if [ -n "${rest}" ]; then
      (
        cd "${ROOT}/${module_dir}"
        # shellcheck disable=SC2086
        go_test_leg "${log}" "${legjson}" -timeout "$(cigates_go_test_timeout ${rest})" ${rest}
      ) || { (check_test_shard_unweighted_guard "${shard}" "${count}" "${log}" partial) || true; rm -f -- "${log}" "${json}"; return 1; }
    fi
    if [ -n "${rest_sec}" ]; then
      printf 'go test (shard %s/%s, security -count=1): %s: %d package(s)\n' "${shard}" "${count}" "${module_dir}" "$(printf '%s\n' "${rest_sec}" | grep -c .)"
      (
        cd "${ROOT}/${module_dir}"
        # shellcheck disable=SC2086
        go_test_leg "${log}" "${legjson}" -timeout "$(cigates_go_test_timeout ${rest_sec})" ${rest_sec}
      ) || { (check_test_shard_unweighted_guard "${shard}" "${count}" "${log}" partial) || true; rm -f -- "${log}" "${json}"; return 1; }
    fi
    if [ -n "${rest_rows}" ]; then
      (
        cd "${ROOT}/${module_dir}"
        # shellcheck disable=SC2086
        go_test_json_run_t "${rows_timeout}" "${json}" "${log}" ${rest_rows}
      ) || { (check_test_shard_unweighted_guard "${shard}" "${count}" "${log}" partial) || true; rm -f -- "${log}" "${json}"; return 1; }
    fi
    if printf '%s\n' "${plan}" | awk -F'\t' -v m="${module_dir}" -v heavy="${SERIAL_HEAVY_TEST_PACKAGE}" '$1 == m && $2 == heavy { found = 1 } END { exit !found }'; then
      printf 'go test%s (shard %s/%s, alone): %s\n' "${MERGE_GROUP_FRESH:+ -count=1}" "${shard}" "${count}" "${SERIAL_HEAVY_TEST_PACKAGE}"
      heavy_is_row=0
      if [ "${module_dir}" = "." ] && printf '%s\n' "${plan}" | awk -F'\t' -v heavy="${SERIAL_HEAVY_TEST_PACKAGE}" -v keys="${census_keys}" '
          BEGIN { n = split(keys, k, "\n"); for (i = 1; i <= n; i++) isrow[k[i]] = 1 }
          $1 == "." && $2 == heavy && ($3 in isrow) { found = 1 } END { exit !found }'; then heavy_is_row=1; fi
      if [ "${heavy_is_row}" -eq 1 ]; then
        (
          cd "${ROOT}/${module_dir}"
          go_test_json_run "${json}" "${log}" "${SERIAL_HEAVY_TEST_PACKAGE}"
        ) || { (check_test_shard_unweighted_guard "${shard}" "${count}" "${log}" partial) || true; rm -f -- "${log}" "${json}"; return 1; }
      else
        (
          cd "${ROOT}/${module_dir}"
          go_test_leg "${log}" "${legjson}" -timeout 10m "${SERIAL_HEAVY_TEST_PACKAGE}"
        ) || { (check_test_shard_unweighted_guard "${shard}" "${count}" "${log}" partial) || true; rm -f -- "${log}" "${json}"; return 1; }
      fi
    fi
  done
  if [ "${legjson}" != /dev/null ]; then cat "${json}" >>"${legjson}"; fi
  check_race_excluded_receipts "${listing}" "${json}" || { rm -f -- "${log}" "${json}"; return 1; }
  (check_security_fresh_receipt "$(printf '%s\n' "${plan}" | grep -c . || true)" "${legjson}" "${json}") || { rm -f -- "${log}" "${json}"; return 1; }
  wall=$(($(date +%s) - started))
  (check_test_shard_unweighted_guard "${shard}" "${count}" "${log}") || { rm -f -- "${log}" "${json}"; return 1; }
  (check_test_shard_wall_guard "${shard}" "${count}" "${wall}" "${log}") || { rm -f -- "${log}" "${json}"; return 1; }
  rm -f -- "${log}" "${json}"
}

check_race() {
  run_in_modules "go test -race" go test -mod=readonly -trimpath -race ./...
}

# check_race_shard SHARD COUNT (CHAOS-6690): the SHARD-th of COUNT weight-balanced
# slices of every module's packages under `go test -race`. The race stage was 17
# of the 34 minutes of the single go-quality step, dominated by a few packages
# (goapiproof, providersync, gqlgenguard, workgraph/units), so the slices are
# balanced by measured package time (ci/go_race_weights.d, ci/go_race_shard.awk)
# and NOT by count. The slices always partition `go list ./...`: a wrong weight
# costs balance, never coverage. A slice that selects no package at all fails.
# CHAOS-10300: the weights are ONE FILE PER PACKAGE (ci/go_race_weights.d/<race_weights_file_name of the package>.tsv, one `package<TAB>seconds` row),
# so a pull request that adds a package adds a file and conflicts with no other pull request. The awks read one weights stream:
# race_weights_rows prints every row of the directory, in any order (the awks look rows up by key).
GO_RACE_WEIGHTS_DIR="${GO_RACE_WEIGHTS_DIR:-${ROOT}/ci/go_race_weights.d}"
# race_weights_file_name PACKAGE: the one file name of a package. `_` becomes `_u` and `/` becomes `__`, so two package paths
# never share a name (zzreview__collision is zzreview_u_ucollision.tsv, zzreview/collision is zzreview__collision.tsv); the
# root package `.` is `_root.tsv`, a name the `*.tsv` glob sees (a leading dot would be skipped).
race_weights_file_name() {
  if [ "$1" = "." ]; then
    printf '_root.tsv'
  else
    printf '%s.tsv' "$(printf '%s' "$1" | sed 's#_#_u#g; s#/#__#g')"
  fi
}
race_weights_rows() {
  local f
  local -a files=()
  shopt -s dotglob
  [ -d "${GO_RACE_WEIGHTS_DIR}" ] || die "race weights directory missing: ${GO_RACE_WEIGHTS_DIR}"
  for f in "${GO_RACE_WEIGHTS_DIR}"/*.tsv; do
    [ -f "${f}" ] || continue
    files+=("${f}")
  done
  [ "${#files[@]}" -gt 0 ] || die "race weights directory holds no .tsv file: ${GO_RACE_WEIGHTS_DIR}"
  # One awk over every file (a grep per file launched one process per weights row file, ~870 per call).
  # shellcheck disable=SC2016
  printf '%s\0' "${files[@]}" | xargs -0 awk '!/^(#|[[:space:]]*$)/' || true
}
# CHAOS-10289: packages that run alone (ci/go_race_solo.tsv): left out of the weight-balanced split, run in their own
# `go test -race` invocation after the shard's other packages, so no heavy co-tenant can starve them.
GO_RACE_SOLO="${GO_RACE_SOLO:-${ROOT}/ci/go_race_solo.tsv}"
# check_providersync_race_shard SHARD COUNT (CHAOS-8166): internal/providersync is one test binary of ~1,460 sequential tests that took
# 509 s under -race on an idle host (565 s in CI, 94% of the 600 s go test timeout), so its race run is split BY TEST NAME across the
# race legs: every leg runs the SHARD-th of COUNT weight-balanced test slices, weights ci/go_providersync_race_weights.tsv, ci/go_providersync_race_shard.awk.
# The test set comes from `go test -list` (never hand-listed). The slices must partition it exactly (each test in exactly one shard,
# no shard empty); anything else fails before a test runs. The race signal stays: nothing here is `!race`.
check_providersync_race_shard() {
  local shard="$1" count="$2" listing other all names selected_names s total=0 regex
  local weights="${GO_PROVIDERSYNC_RACE_WEIGHTS:-${ROOT}/ci/go_providersync_race_weights.tsv}"
  local shard_awk="${GO_PROVIDERSYNC_RACE_SHARD_AWK:-${ROOT}/ci/go_providersync_race_shard.awk}"
  [ -f "${weights}" ] || die "providersync race shards need ${weights}"
  # CHAOS-8275: list EVERY name of the RACE test binary (so a //go:build race file is seen too), not just ^Test of the plain build. A Fuzz*,
  # Example* or Benchmark* name would be selected by no `-run ^(Test...)$` shard and run in no race leg, so it fails here instead.
  listing="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -race ${MERGE_GROUP_FRESH:+-count=1} -list '.*' ./internal/providersync)" \
    || die "go test -race -list failed for internal/providersync"
  listing="$(printf '%s\n' "${listing}" | grep -Ev '^(ok|FAIL|\?|---)[[:space:]]' | grep -v '^$' || true)"
  other="$(printf '%s\n' "${listing}" | grep -v '^Test' || true)"
  [ -z "${other}" ] \
    || die "providersync lists a name that is not Test* and that no race shard would run: $(printf '%s' "${other}" | tr '\n' ' ')"
  all="$(printf '%s\n' "${listing}" | grep '^Test' || true)"
  [ -n "${all}" ] || die "go test -list reported no providersync test"
  [ "${count}" -le "$(printf '%s\n' "${all}" | wc -l)" ] \
    || die "providersync race shards: ${count} legs for fewer listed tests: a shard would select zero tests"
  names=""
  for ((s = 1; s <= count; s++)); do
    selected_names="$(printf '%s\n' "${all}" | awk -v shard="${s}" -v count="${count}" -v weights="${weights}" -f "${shard_awk}")" \
      || die "ci/go_providersync_race_shard.awk failed"
    [ -n "${selected_names}" ] || die "providersync race shard ${s}/${count} selected zero tests"
    total=$((total + $(printf '%s\n' "${selected_names}" | wc -l)))
    names+="${selected_names}"$'\n'
    if [ "${s}" -eq "${shard}" ]; then regex="^($(printf '%s\n' "${selected_names}" | paste -sd'|' -))\$"; fi
  done
  [ "${total}" -eq "$(printf '%s\n' "${all}" | wc -l)" ] \
    && [ "$(printf '%s' "${names}" | LC_ALL=C sort)" = "$(printf '%s\n' "${all}" | LC_ALL=C sort)" ] \
    || die "providersync race shards are not an exact partition of the ${total} listed tests"
  printf 'go test -race%s providersync shard %s/%s: %s of %s tests\n' "${MERGE_GROUP_FRESH:+ -count=1}" "${shard}" "${count}" "$(printf '%s\n' "${regex}" | tr -cd '|' | wc -c | awk '{print $1 + 1}')" "${total}"
  # CHAOS-10481: -json goes to a per-leg file (the leg number is in the name); jq turns it back into the plain go test log, and
  # pipefail keeps go test's exit code. The guard runs after a failed run too, so a timeout names the test that never ended.
  command -v jq >/dev/null 2>&1 || die "providersync race shards need jq to print the go test -json log"
  local json_dir="${GO_TEST_JSON_DIR:-${ROOT}/go-test-json}" json run_rc=0 guard_rc=0
  mkdir -p "${json_dir}"
  json="${json_dir}/providersync-race-${shard}.json"
  (
    set -o pipefail
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -race ${MERGE_GROUP_FRESH:+-count=1} -json -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" -run "${regex}" ./internal/providersync \
      | tee "${json}" \
      | jq -rj 'select(.Action == "output") | .Output'
  ) || run_rc=$?
  awk -v weights="${weights}" -v thresh="${DEV_HEALTH_GO_RACE_UNWEIGHTED_SLOW_SECONDS:-30}" -v label="${weights#"${ROOT}/"}" \
    -f "${GO_RACE_UNWEIGHTED_SLOW_AWK:-${ROOT}/ci/go_race_unweighted_slow.awk}" "${json}" || guard_rc=$?
  [ "${run_rc}" -ge "${guard_rc}" ] || run_rc="${guard_rc}"
  return "${run_rc}"
}

check_race_shard_args() {
  local shard="${1:-}" count="${2:-}"
  case "${shard}" in ""|*[!0-9]*) die "race SHARD must be a positive integer, got '${shard}'" ;; esac
  case "${count}" in ""|*[!0-9]*) die "race COUNT must be a positive integer, got '${count}'" ;; esac
  { [ "${shard}" -ge 1 ] && [ "${count}" -ge 1 ] && [ "${shard}" -le "${count}" ]; } \
    || die "race shard ${shard} is outside 1..${count}"
  [ -d "${GO_RACE_WEIGHTS_DIR}" ] || die "race shards need ${GO_RACE_WEIGHTS_DIR}"
  [ -f "${GO_RACE_EXCLUDED_PACKAGES}" ] || die "race shards need ${GO_RACE_EXCLUDED_PACKAGES}"
}

# CHAOS-11413: packages the race legs do not run (ci/race_excluded_packages.tsv, proven by internal/cigates/racescope). The test legs
# still run them once. race_shard_packages drops them before the weight split, so no race leg plans them, solo or not.
GO_RACE_EXCLUDED_PACKAGES="${GO_RACE_EXCLUDED_PACKAGES:-${ROOT}/ci/race_excluded_packages.tsv}"
race_excluded_filter() {
  local module_dir="$1" modpath="$2"
  if [ "${module_dir}" != "." ]; then
    cat
    return
  fi
  awk -F'\t' -v mod="${modpath}" -v list="${GO_RACE_EXCLUDED_PACKAGES}" '
    BEGIN { while ((getline line < list) > 0) { if (line ~ /^[[:space:]]*(#|$)/) continue; split(line, f, "\t"); skip[mod "/" f[1]] = 1 } close(list) }
    !($0 in skip)'
}

# race_shard_packages SHARD COUNT MODULE_DIR MODPATH [solo]: the import paths of the race slice (or, with solo, of its solo
# packages) of one module, after the go-quality selection. internal/providersync is never listed: it runs as test-name slices.
race_shard_packages() {
  local shard="$1" count="$2" module_dir="$3" modpath="$4" part="${5:-}"
  (cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly ./...) \
    | { if [ "${module_dir}" = "." ]; then grep -vx "${modpath}/internal/providersync"; else cat; fi; } \
    | quality_filter "${module_dir}" "${modpath}" \
    | race_excluded_filter "${module_dir}" "${modpath}" \
    | awk -v mod="${modpath}" -v shard="${shard}" -v count="${count}" -v weights=<(race_weights_rows) -v solo="${GO_RACE_SOLO}" -v part="${part}" -f "${ROOT}/ci/go_race_shard.awk"
}

# check_race_shard_plan SHARD COUNT: one "module<TAB>package<TAB>key" line per package the SHARD-th race leg runs.
check_race_shard_plan() {
  local shard="${1:-}" count="${2:-}" module_dir modpath pkg key
  check_race_shard_args "${shard}" "${count}"
  load_quality_selection
  for module_dir in "${MODULE_DIRS[@]}"; do
    modpath="$(cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -m)" || die "go list -m failed in ${module_dir}"
    {
      if [ "${module_dir}" = "." ] && quality_selected . internal/providersync; then printf '%s\n' "${modpath}/internal/providersync"; fi
      race_shard_packages "${shard}" "${count}" "${module_dir}" "${modpath}"
      race_shard_packages "${shard}" "${count}" "${module_dir}" "${modpath}" solo
    } | while IFS= read -r pkg; do
      [ -n "${pkg}" ] || continue
      key="${pkg#"${modpath}/"}"
      [ "${pkg}" != "${modpath}" ] || key="."
      printf '%s\t%s\t%s\n' "${module_dir}" "${pkg}" "${key}"
    done
  done
}

check_race_shard() {
  local shard="${1:-}" count="${2:-}" module_dir modpath pkg selected=0 missing racelog legjson ps_json=""
  check_race_shard_args "${shard}" "${count}"
  load_quality_selection
  local -a pkgs solo_pkgs pkgs_sec pkgs_all
  racelog="$(mktemp "${TMPDIR:-/tmp}/dev-health-race-shard.XXXXXX")"
  legjson="$(leg_json_file race "${shard}" "${count}")" || exit 2
  # The cache receipt reads the stages' -json events: a leg that keeps no timings keeps them in a temp file.
  if [ "${legjson}" = /dev/null ]; then
    leg_events_file race
    legjson="${LEG_EVENTS_TMP}"
  fi
  if security_fresh_active; then security_fresh_load; fi
  for module_dir in "${MODULE_DIRS[@]}"; do
    modpath="$(cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -m)"
    if [ "${module_dir}" = "." ]; then
      # CHAOS-8135: static and deterministic, before any test runs: every package of the root module has a weights row.
      # A package with no row weighs the shard awk's default and hides its cost from the shard plan.
      missing="$(cd "${ROOT}/${module_dir}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly ./... \
        | awk -v mod="${modpath}" -v weights=<(race_weights_rows) -f "${ROOT}/ci/go_race_missing_rows.awk")" \
        || die "ci/go_race_missing_rows.awk failed"
      [ -z "${missing}" ] \
        || die "race package(s) with NO row in ci/go_race_weights.d (add each one: its measured go test -race seconds, 1 if it has no test): $(printf '%s' "${missing}" | tr '\n' ' ')"
    fi
    pkgs=()
    while IFS= read -r pkg; do
      [ -n "${pkg}" ] && pkgs+=("${pkg}")
    done < <(race_shard_packages "${shard}" "${count}" "${module_dir}" "${modpath}")
    solo_pkgs=()
    while IFS= read -r pkg; do
      [ -n "${pkg}" ] && solo_pkgs+=("${pkg}")
    done < <(race_shard_packages "${shard}" "${count}" "${module_dir}" "${modpath}" solo)
    pkgs_sec=()
    if security_fresh_active && [ "${module_dir}" = "." ] && [ "${#pkgs[@]}" -gt 0 ]; then
      pkgs_all=("${pkgs[@]}")
      pkgs=()
      while IFS= read -r pkg; do
        [ -n "${pkg}" ] && pkgs_sec+=("${pkg}")
      done < <(printf '%s\n' "${pkgs_all[@]}" | security_split in)
      while IFS= read -r pkg; do
        [ -n "${pkg}" ] && pkgs+=("${pkg}")
      done < <(printf '%s\n' "${pkgs_all[@]}" | security_split out)
    fi
    printf 'go test -race%s (shard %s/%s): %s: %d package(s), %d solo\n' "${MERGE_GROUP_FRESH:+ -count=1}" "${shard}" "${count}" "${module_dir}" "${#pkgs[@]}" "${#solo_pkgs[@]}"
    if [ "${module_dir}" = "." ] && quality_selected . internal/providersync; then
      # CHAOS-8166: internal/providersync runs in EVERY race leg, one test-name shard each (it is not in pkgs above).
      selected=$((selected + 1))
      check_providersync_race_shard "${shard}" "${count}"
      ps_json="${GO_TEST_JSON_DIR:-${ROOT}/go-test-json}/providersync-race-${shard}.json"
    fi
    if [ "${#pkgs[@]}" -gt 0 ]; then
      selected=$((selected + ${#pkgs[@]}))
      (
        cd "${ROOT}/${module_dir}"
        go_test_leg "${racelog}" "${legjson}" -race -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${pkgs[@]}"
      ) || { rm -f -- "${racelog}"; return 1; }
    fi
    if [ "${#pkgs_sec[@]}" -gt 0 ]; then
      selected=$((selected + ${#pkgs_sec[@]}))
      printf 'go test -race (shard %s/%s): %s: security -count=1: %d package(s)\n' "${shard}" "${count}" "${module_dir}" "${#pkgs_sec[@]}"
      (
        cd "${ROOT}/${module_dir}"
        go_test_leg "${racelog}" "${legjson}" -race -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${pkgs_sec[@]}"
      ) || { rm -f -- "${racelog}"; return 1; }
    fi
    # CHAOS-10289: the solo packages run last and alone: the shard's packages and its providersync slice have finished, so
    # nothing else of this leg runs beside them.
    if [ "${#solo_pkgs[@]}" -gt 0 ]; then
      selected=$((selected + ${#solo_pkgs[@]}))
      printf 'go test -race%s (shard %s/%s): %s: solo, alone: %s\n' "${MERGE_GROUP_FRESH:+ -count=1}" "${shard}" "${count}" "${module_dir}" "${solo_pkgs[*]}"
      (
        cd "${ROOT}/${module_dir}"
        go_test_leg "${racelog}" "${legjson}" -race -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${solo_pkgs[@]}"
      ) || { rm -f -- "${racelog}"; return 1; }
    fi
  done
  (check_security_fresh_receipt "${selected}" "${legjson}" ${ps_json:+"${ps_json}"}) || { rm -f -- "${racelog}"; return 1; }
  rm -f -- "${racelog}"
  [ "${selected}" -gt 0 ] || [ "${QUALITY_FILTER_ACTIVE}" -eq 1 ] \
    || die "race shard ${shard}/${count} selected zero packages -- the matrix is wider than the package list, so this leg would read green while running nothing"
}

# ci_leg_stage NAME CMD... prints UTC start/done stamps around one stage so a
# leg's log says where its minutes went (CHAOS-6690). No `if`/`||` around the
# command on purpose: errexit is suppressed inside a function called in a
# condition, which would let a failing command in the middle of a stage pass.
# check_ratchets (CHAOS-7904): the closed lists of ci/ratchets.tsv against the merge base. Reads the base from git
# (RATCHET_BASE_SHA, else origin/main), so two pull requests that each shorten a list edit different lines and do not conflict.
check_ratchets() {
  printf 'ratchets: closed lists against the merge base\n'
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go run -mod=readonly -trimpath ./internal/testsupport/ratchet/cmd/ratchets
  )
}

# check_race_weights_text FILE...: the files hold only ASCII bytes and no NUL (bytes counted with LC_ALL=C).
check_race_weights_text() {
  [ "$(( $(cat -- "$@" | LC_ALL=C tr -dc '\000\200-\377' | wc -c) ))" -eq 0 ]
}

# check_race_weights_rows (CHAOS-4233, CHAOS-10300): the census of ci/go_race_weights.d, run by `fast`, `ci` and the static leg so a
# package added, removed or renamed names the missing or stale file on the author's machine. One file per package, one row each:
#   1. every file holds exactly one `package<TAB>seconds` row (comments and blank lines are ignored);
#   2. no package has two rows (two pull requests that weigh the same package in two files): both weights are printed;
#   3. each file is named after its package (/ as __), so a package has one possible path and a duplicate cannot hide;
#   4. every package of the root module has a file, and no file names a package that is gone.
# Order is not checked: the directory has none, and the shard awks look rows up by key.
check_race_weights_rows() {
  local modpath listed missing stale dup key rows files f
  shopt -s dotglob
  [ -d "${GO_RACE_WEIGHTS_DIR}" ] || die "race weights directory missing: ${GO_RACE_WEIGHTS_DIR}"
  # Rule 0: every file is ASCII without a NUL byte, so grep and every awk, in every locale, read the same rows (white space, digits
  # and ranges beyond ASCII differ between awks and locales). One pass over all files; a file is named only when that pass fails.
  command -v tr >/dev/null 2>&1 || die "race-weights-rows needs tr on PATH (the ASCII check of ci/go_race_weights.d)"
  if ! check_race_weights_text "${GO_RACE_WEIGHTS_DIR}"/*.tsv; then
    for f in "${GO_RACE_WEIGHTS_DIR}"/*.tsv; do
      check_race_weights_text "${f}" \
        || die "${f##*/} is not ASCII text: it holds a NUL byte or a byte above 0x7f (race weight files hold one package<TAB>seconds row, in ASCII)"
    done
  fi
  rows="$(race_weights_rows)" || die "race weights unreadable: ${GO_RACE_WEIGHTS_DIR}"
  # One awk pass over every file (a few forks, not several per file): the first file that breaks rule 1, then the first that
  # breaks rule 3, each as the message the per-file check prints. Rule 2 (the rows above) is checked between the two, as before.
  files="$(awk -F'\t' '
    BEGIN { for (i = 1; i < ARGC; i++) path[i] = ARGV[i]; n = ARGC - 1 }
    /^#/ || /^[[:space:]]*$/ { next }
    { if (++count[FILENAME] == 1) { body[FILENAME] = $0; nf[FILENAME] = NF; key[FILENAME] = $1; weight[FILENAME] = $2 } }
    END {
      for (i = 1; i <= n; i++) {
        f = path[i]; base = f; sub(/.*\//, "", base)
        if (count[f] != 1) { print "1" base " must hold exactly one row: package<TAB>seconds (race weight files, one per package)"; exit }
        if (nf[f] != 2) { print "1" base " must hold exactly two columns: package<TAB>seconds, a whole-number weight after the package (got: " body[f] ")"; exit }
        if (weight[f] == "" || weight[f] ~ /[^0-9]/) { print "1" base ": the weight \047" weight[f] "\047 is not a whole number of seconds (row: package<TAB>seconds)"; exit }
      }
      for (i = 1; i <= n; i++) {
        f = path[i]; base = f; sub(/.*\//, "", base)
        want = key[f]
        if (want == ".") want = "_root"
        else { gsub(/_/, "_u", want); gsub(/\//, "__", want) }
        want = want ".tsv"
        if (base != want) { print "3" base " holds package " key[f] ": name the file " want; exit }
      }
    }' "${GO_RACE_WEIGHTS_DIR}"/*.tsv)" || die "race weights unreadable: ${GO_RACE_WEIGHTS_DIR}"
  case "${files}" in 1*) die "${files#1}" ;; esac
  dup="$(printf '%s\n' "${rows}" | cut -f1 | LC_ALL=C sort | uniq -d)"
  if [ -n "${dup}" ]; then
    for key in ${dup}; do
      printf 'race weights: %s weighs twice: %s\n' "${key}" "$(printf '%s\n' "${rows}" | awk -F'\t' -v k="${key}" '$1 == k { printf "%s ", $2 }')" >&2
    done
    die "ci/go_race_weights.d weighs a package twice (rows above): delete the file that is not the newest measurement"
  fi
  case "${files}" in 3*) die "${files#3}" ;; esac
  modpath="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -m)" || die "go list -m failed"
  listed="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly ./...)" || die "go list ./... failed"
  missing="$(printf '%s\n' "${listed}" | awk -v mod="${modpath}" -v weights=<(race_weights_rows) -f "${ROOT}/ci/go_race_missing_rows.awk")" \
    || die "ci/go_race_missing_rows.awk failed"
  [ -z "${missing}" ] \
    || die "race package(s) with NO row in ci/go_race_weights.d (add ci/go_race_weights.d/<race_weights_file_name of the package>.tsv for each: package<TAB>its measured go test -race seconds, 1 if it has no test): $(printf '%s' "${missing}" | tr '\n' ' ')"
  stale="$(printf '%s\n' "${rows}" | cut -f1 | LC_ALL=C sort -u \
    | LC_ALL=C comm -23 - <(printf '%s\n' "${listed}" | awk -v mod="${modpath}" '{ if ($0 == mod) print "."; else if (index($0, mod "/") == 1) print substr($0, length(mod) + 2); else print $0 }' | LC_ALL=C sort))"
  [ -z "${stale}" ] \
    || die "ci/go_race_weights.d names package(s) that no longer exist (delete each file): $(printf '%s' "${stale}" | tr '\n' ' ')"
  # CHAOS-10289: the solo rows. A missing file would silently put every solo package back into the shared split.
  [ -f "${GO_RACE_SOLO}" ] || die "race solo file missing: ${GO_RACE_SOLO}"
  local solo_rows solo_bad
  solo_rows="$({ grep -v '^\(#\|[[:space:]]*$\)' "${GO_RACE_SOLO}" || [ "$?" -eq 1 ]; } | cut -f1)"
  solo_bad="$(printf '%s\n' "${solo_rows}" | sed '/^$/d' | LC_ALL=C sort -u | LC_ALL=C comm -23 - <(printf "%s\n" "${rows}" | cut -f1 | LC_ALL=C sort) | tr '\n' ' ')"
  [ -z "${solo_bad}" ] \
    || die "ci/go_race_solo.tsv names package(s) with no file in ci/go_race_weights.d (a solo row needs its measured weight): ${solo_bad}"
  [ -z "$(printf '%s\n' "${solo_rows}" | sed '/^$/d' | LC_ALL=C sort | uniq -d)" ] \
    || die "ci/go_race_solo.tsv lists a package twice"
  printf '%s\n' "${solo_rows}" | sed '/^$/d' | LC_ALL=C sort -c 2>/dev/null \
    || die "ci/go_race_solo.tsv is not sorted (LC_ALL=C)"
  ! printf '%s\n' "${solo_rows}" | grep -qx 'internal/providersync' \
    || die "ci/go_race_solo.tsv lists internal/providersync, which runs as test-name slices in every race leg and cannot be solo"
  shopt -u dotglob
  printf 'race-weights-rows: %d row(s) match the package list, %d solo\n' "$(printf '%s\n' "${rows}" | wc -l)" "$(printf '%s\n' "${solo_rows}" | sed '/^$/d' | wc -l)"
}

# check_plan_reads: the declared-read guard of the plan verdict (`cmd/cigate plan-reads`) over the whole tree: every
# package that names a path of a declared class in a string literal has a reads.tsv row, and --reach recomputes
# reach.tsv (the programs that call each code-scope unbounded site: go/packages, SSA and RTA over the module; about
# 3 min and a 6.5 GiB live heap on 4 cores under the tool's own 8 GiB soft limit; a peak RSS over the 10 GiB budget, reachBudgetMiB, fails: >= 30% under a 16 GiB scope, e.g. capped.sh's GOMEMLIMIT 85% of MemoryMax) and fails on any difference. It runs in the static leg, which runs on every change. With PLAN_READS_REACH_CACHE set (the
# GitHub-hosted legs only, go-quality.yml) a pull request reuses a store of the same input hash; PLAN_READS_REACH_FRESH (every
# other event) computes fresh and only stores (CHAOS-11421).
check_plan_reads() {
  (cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off GOMEMLIMIT="${GOMEMLIMIT:-10GiB}" go run -mod=readonly -trimpath ./cmd/cigate plan-reads --root "${ROOT}" --reach ${PLAN_READS_REACH_CACHE:+--reach-cache "${PLAN_READS_REACH_CACHE}"} ${PLAN_READS_REACH_FRESH:+--reach-cache-fresh})
}

ci_leg_stage() {
  local name="$1" started
  shift
  started="$(date +%s)"
  printf '==> stage %s start %s\n' "${name}" "$(date -u +%H:%M:%SZ)"
  "$@"
  printf '<== stage %s done in %ss at %s\n' "${name}" "$(($(date +%s) - started))" "$(date -u +%H:%M:%SZ)"
}

# check_ci_leg LEG [SHARD COUNT]: one parallel slice of `ci` for the go-quality
# workflow (CHAOS-6690). The legs together run exactly the steps of `ci`
# (internal/cigates/workflows/workflows_quality_legs_test.go pins the union against the `ci` case):
#   static   format, vet, build, contract, ratchets, integration-vet, the shard plans, the plan-reads guard
#   test     go test, slice SHARD of COUNT (weight-balanced, ci/go_test_weights.tsv); shard 1 also the multi-replica worker gate
#   race     go test -race, slice SHARD of COUNT (weight-balanced)
check_ci_leg() {
  local leg="${1:-}"
  case "${leg}" in
    static)
      ci_leg_stage format check_format
      ci_leg_stage vet check_vet
      ci_leg_stage build check_build
      ci_leg_stage contract check_contract
      ci_leg_stage ratchets check_ratchets
      ci_leg_stage race-weights-rows check_race_weights_rows
      ci_leg_stage integration-vet check_integration_vet
      ci_leg_stage plan-integration-shards plan_integration_shards
      ci_leg_stage plan-providersync-shards plan_providersync_test_shards
      ci_leg_stage plan-reads check_plan_reads
      ;;
    test)
      ci_leg_stage "test ${2:-}/${3:-}" check_test_shard "${2:-}" "${3:-}"
      load_quality_selection
      if [ "${2:-}" = "1" ] && quality_selected . internal/workerservice; then
        ci_leg_stage multi-replica-workers check_multi_replica_workers
      fi
      ;;
    race)
      ci_leg_stage "race ${2:-}/${3:-}" check_race_shard "${2:-}" "${3:-}"
      ;;
    *)
      die "ci-leg LEG must be one of static, test SHARD COUNT, race SHARD COUNT (got '${leg}')"
      ;;
  esac
  printf 'ci-leg %s: OK\n' "${leg}"
}

# check_ci_leg_warm test|race SHARD COUNT (CHAOS-11408): compile, run nothing. A main push that the merge queue already
# tested skips its test and race legs, and this fills the leg's Go build cache on a key miss so the pull requests that restore
# it still hit. The flags are the ones of the real leg (-mod=readonly -trimpath, -race for a race leg) plus -run '^$', so the
# compiled objects are the ones the real leg would cache.
check_ci_leg_warm() {
  local leg="${1:-}" shard="${2:-}" count="${3:-}" module_dir plan pkg
  local -a pkgs
  case "${leg}" in
    test) plan="$(check_test_shard_plan "${shard}" "${count}")" || die "test shard plan failed" ;;
    race) plan="$(check_race_shard_plan "${shard}" "${count}")" || die "race shard plan failed" ;;
    *) die "ci-leg-warm LEG must be test or race (got '${leg}')" ;;
  esac
  for module_dir in "${MODULE_DIRS[@]}"; do
    pkgs=()
    while IFS= read -r pkg; do
      [ -n "${pkg}" ] && pkgs+=("${pkg}")
    done < <(printf '%s\n' "${plan}" | awk -F'\t' -v m="${module_dir}" '$1 == m { print $2 }')
    [ "${#pkgs[@]}" -gt 0 ] || continue
    printf 'go test -run ^$ (%s %s/%s): %s: %d package(s)\n' "${leg}" "${shard}" "${count}" "${module_dir}" "${#pkgs[@]}"
    (
      cd "${ROOT}/${module_dir}"
      if [ "${leg}" = race ]; then
        "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -race -run '^$' "${pkgs[@]}"
      else
        "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -run '^$' "${pkgs[@]}"
      fi
    )
  done
  printf 'ci-leg-warm %s: OK\n' "${leg}"
}

# check_venue_oracles (CHAOS-6314) runs every venue oracle test in the repo,
# from the shared internal/testsupport/venueoracle harness: real
# Postgres/Valkey containers and the Go api, replayed against the recorded
# Python answers (frozen goldens). The live Python api is not started: the
# live oracles were deleted (CHAOS-7308) and no Python is installed for this
# verb. It is invoked ONLY from the dedicated venue-oracles CI jobs, never
# from `ci`/`fast`/`all`.
#
# WHAT RUNS is ci/venue_oracle_registry.d/ (CHAOS-6584, Trap #392; one file per
# package, ci/venue_oracle_registry.d/<package with / as __>.tsv, CHAOS-6724), one row
# per venue test. Discovery (below) is the CROSS-CHECK that keeps the registry
# honest: the verb fails before running anything when a test found by
# structure or by a Test*VenueOracle* name is not registered, or a registered
# test does not exist (ci/lib/venue_oracle_registry.sh).
#
# Discovery is by the harness, never by a test's name or a hardcoded list:
# in every package that has a *_test.go importing
# internal/testsupport/venueoracle, every top-level Test function (TestMain
# aside) whose code reaches a harness entry point -- a call of
# venueoracle.Start, Diff, WriteProof or WriteGoOnlyProof in its own body,
# or a name (function, var, const or type) of the same package's test files
# that reaches one, followed to a fixed point -- is discovered (a unit test
# that only reaches a pure helper such as venueoracle.RedactJSON is not),
# whatever its parameter is called, however its signature is wrapped, and
# in whichever file of the package its helpers live (a name-based grep once
# ran 2 of the 13 admin oracles; a one-line `(t *testing.T)` pattern missed
# wrapped signatures and cross-file helpers). The match is by name within
# the package, so it can only err towards discovering too much, and a
# wrongly discovered test fails the verb loudly (no proof), never silently.
# The one opt-out is VENUE_ORACLE_LOCAL_ONLY_MARKER directly above a test,
# logged by name and counted in the summary; a harness package that runs
# no test at all fails the verb. Tests run one `go test` invocation per
# package.
#
# Each discovered test's own venueoracle.Diff (or WriteProof) writes a proof
# file (its own t.Name()) into DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR once it
# has actually sent a request to both planes and compared them -- NOT Start,
# which only proves the venue was built, not that anything was compared. A
# test that compares no Python response (Go against a recorded Python truth,
# or a property of the venue) writes a Go-only proof instead
# (venueoracle.WriteGoOnlyProof, "go-only: <reason>"): accepted, but counted
# apart and named with its reason in the summary, so it never reads as a
# parity comparison. This verb fails loudly, rule-4-style, if a proof is
# missing for ANY discovered name after every invocation completes, or if
# discovery itself found zero tests (the discovery mechanism is broken, not
# a genuinely oracle-free tree).
#
# Two same-named VenueOracle tests in one directory (an internal test
# package and an external `_test` package can both declare the identical
# function name) are rejected at discovery: go test's own text and JSON
# event streams cannot tell the two apart by name, so no per-test signal
# here could distinguish "the real one ran" from "a same-named impostor
# ran while the real one silently didn't" -- the only honest response is to
# refuse rather than trust an ambiguous signal.
#
# Status is read from `go test -json` (via jq), not a text-line grep: a
# grep anchored on `--- SKIP: <name>` cannot see a skip inside a SUBTEST
# (`<name>/Sub`, no top-level line at all) and can be fooled by ordinary
# captured output that happens to look like a status line. The JSON event
# stream's Action/Test fields distinguish a real skip (at any subtest
# depth) from captured output, structurally. DEV_HEALTH_VENUE_ORACLES=1
# is always set by this verb, so no discovered test or subtest
# has a legitimate reason to skip under it. Each package also gets its own
# proof subdirectory, not one shared namespace, so two DIFFERENT packages
# with a same-named test can't let one satisfy the other's check.
# VENUE_ORACLE_LOCAL_ONLY_MARKER, on the line directly above a test
# function, keeps that one test out of this verb: it is for a test that
# needs something hosted CI does not hold (a real Stripe test-mode key) and
# skips without it. The marker is named in the log, never silent.
# The registry functions (VENUE_ORACLE_LOCAL_ONLY_MARKER, discovery,
# check_venue_oracle_registry) live in ci/lib/venue_oracle_registry.sh so the
# static check runs on a host with no Go toolchain (ci/check_venue_oracle_
# registry.sh, called by the Go tooling checks).
# shellcheck source=ci/lib/venue_oracle_registry.sh
. "${ROOT}/ci/lib/venue_oracle_registry.sh"

# The venue-oracles shard plan (CHAOS-6891). A leg used to be every COUNT-th row of
# the sorted registry, blind to what a row costs: the legs' test time ranged 1677s
# to 2207s in the last green run against a 2400s job cap with ~200s of setup, the
# worst leg finished at 39:59, and three new rows pushed two other legs over. The
# legs are now cost-balanced (ci/venue_oracle_shard.awk, the longest-processing-time
# assignment ci/go_race_shard.awk uses) by the measured seconds in
# ci/venue_oracle_weights.d/ (edit a row by hand from a run log). A wrong or
# missing weight costs balance, never coverage: the legs always partition the run
# rows. the Go tooling checks fail a PR whose registry rows
# lack a weight or whose heaviest leg would exceed the leg's test-time budget.
# The weights are a DIRECTORY of per-package row files named like the registry's
# (CHAOS-6926), so two PRs that add venue rows in different packages do not conflict.
VENUE_ORACLE_WEIGHTS="${ROOT}/ci/venue_oracle_weights.d"
VENUE_ORACLE_DEFAULT_WEIGHT=60
# venue_oracle_run_rows prints every registry `run` row as "<package dir>\t<test>",
# sorted by (package, test).
venue_oracle_run_rows() {
  venue_oracle_registry_rows | LC_ALL=C sort -k1,1 -k2,2 | awk '$3 == "run" { printf "%s\t%s\n", $1, $2 }'
}
# venue_oracle_assign SHARD COUNT [plan]: the shard's rows ("<package dir>\t<test>"),
# or with `plan` every row with its shard and weight plus one "#leg" line per shard.
venue_oracle_assign() {
  venue_oracle_run_rows | venue_oracle_assign_stdin "$@"
}
# venue_oracle_assign_stdin SHARD COUNT [plan]: the same assignment over the "<package dir>\t<test>" rows on stdin
# (the rows a change set selected, for `--changed FILE SHARD COUNT`).
venue_oracle_assign_stdin() {
  local shard="$1" count="$2" plan="${3:-}"
  awk -v shard="${shard}" -v count="${count}" -v plan="${plan:+1}" \
    -v weights=<(venue_oracle_weight_rows) -v defw="${VENUE_ORACLE_DEFAULT_WEIGHT}" -f "${ROOT}/ci/venue_oracle_shard.awk"
}
# venue_oracle_weight_rows prints every weight row (the concatenation of the weights
# directory's *.tsv files in C-locale order; a single file is accepted too). A missing
# weights path is an error, not "no weights": silently planning every row at the
# default would unbalance the legs without a word.
venue_oracle_weight_rows() {
  if [ -d "${VENUE_ORACLE_WEIGHTS}" ]; then
    (
      LC_ALL=C
      local file found=0
      for file in "${VENUE_ORACLE_WEIGHTS}"/*.tsv; do
        [ -e "${file}" ] || continue
        found=1
        cat "${file}"
      done
      [ "${found}" -eq 1 ] || { echo "venue-oracles: no *.tsv weight files in ${VENUE_ORACLE_WEIGHTS}" >&2; exit 1; }
    )
  elif [ -f "${VENUE_ORACLE_WEIGHTS}" ]; then
    cat "${VENUE_ORACLE_WEIGHTS}"
  else
    echo "venue-oracles: weights not found: ${VENUE_ORACLE_WEIGHTS}" >&2
    return 1
  fi
}
# venue-oracle-plan COUNT: print the cost-balanced plan of COUNT legs (rows, predicted
# seconds). Needs neither Go, containers nor Python.
check_venue_oracle_plan() {
  local count="${1:-}"
  case "${count}" in ""|*[!0-9]*|0) die "venue-oracle-plan COUNT must be a positive integer, got '${count}'" ;; esac
  venue_oracle_assign 0 "${count}" plan || die "venue-oracle-plan: the registry could not be read (see above)"
}

# check_venue_oracles [SHARD COUNT] (CHAOS-6574). With no arguments it runs every
# registry `run` row (the local full run). With SHARD COUNT it runs the rows
# whose 0-based position in the registry's sorted `run` sequence, modulo COUNT,
# is SHARD-1: sorted by (package, test), so every package's tests spread over
# all shards (one package -- apiservice/admin -- alone took 23 min of a 67 min
# sequential job, so a package-level split could not get under ~23 min). The
# hosted job runs COUNT matrix legs; a leg whose slice is empty FAILS (a
# matrix wider than the registry is a config error, never a green no-op).
# venue_oracle_changed_selects DIR: succeeds when a path in VENUE_CHANGED_FILE (NUL- or
# newline-separated, repo-relative) belongs to the package in DIR: a file directly in DIR,
# or under DIR/testdata (the goldens the package replays). A file in a SUB-package does
# not select its parent: internal/apiservice/billingvenue/x.go selects billingvenue, not
# internal/apiservice, and internal/apiservice/admin is not selected by a change in
# internal/apiservice/adminllmvenue.
#
# With NAME it also succeeds when a changed path is equal to, or under, a path that
# ci/venue_oracle_triggers.d/ declares for the test NAME of DIR (CHAOS-9172): the code
# outside its package whose behaviour that venue test compares.
# vo_grep / vo_awk: a match test whose own failure (rc above 1: unreadable file, bad program) is an error, never "no match".
vo_grep() {
  local rc=0
  grep "$@" || rc=$?
  [ "${rc}" -le 1 ] || die "venue-oracles --changed: grep failed (rc ${rc}) reading the change set or a selection file"
  return "${rc}"
}
vo_awk() {
  local rc=0
  awk "$@" || rc=$?
  [ "${rc}" -le 1 ] || die "venue-oracles --changed: awk failed (rc ${rc}) reading the change set or a selection file"
  return "${rc}"
}
venue_oracle_changed_selects() {
  local escaped pkg name path
  escaped="$(printf '%s' "$1" | sed 's/[][\\.*^$/]/\\&/g')"
  # grep -q stops at its first match: it reads a process substitution, never a pipe whose writer would die of SIGPIPE and
  # fail the whole pipeline under pipefail on a large change set (the match was lost and the run read as "nothing to run").
  local own=1
  if vo_grep -qE "^${escaped}/([^/]+|testdata/.+)$" < <(tr '\0' '\n' < "${VENUE_CHANGED_FILE}"); then
    own=0
  fi
  # The dependency closure of the package's test binary (ci/venue_oracle_closure.d/, CHAOS-9935): a
  # file directly in, or under testdata of, any package the test binary links selects every
  # test of the package. Derived by a Go test from `go list -test`, so a missing hand row cannot
  # hide a dependency. The directory is absent in a scratch tree that holds no Go module.
  if [ -d "${ROOT}/ci/venue_oracle_closure.d" ]; then
    local closure_file="${ROOT}/ci/venue_oracle_closure.d/${1//\//__}.txt"
    [ -f "${closure_file}" ] && [ -r "${closure_file}" ] || die "venue-oracles --changed: ${closure_file} is missing or not a readable regular file; run VENUE_CLOSURE_WRITE=1 go test ./internal/testsupport/venuechanged -run TestClosureFilesMatchTheTestBinaryDependencies"
    # a file that is empty, or holds a line that is not `<dir>` or `~<dir>`, was not written by the generator: refuse it
    [ -s "${closure_file}" ] || die "venue-oracles --changed: ${closure_file} is empty; regenerate it (VENUE_CLOSURE_WRITE=1 go test ./internal/testsupport/venuechanged)"
    if vo_grep -qvE '^(~[^[:cntrl:]]+|[^~[:cntrl:]][^[:cntrl:]]*)$' "${closure_file}"; then
      die "venue-oracles --changed: ${closure_file} has a line that is not a package directory or ~directory; regenerate it (VENUE_CLOSURE_WRITE=1 go test ./internal/testsupport/venuechanged)"
    fi
    # a path with an empty, "." or ".." segment, a leading slash or a trailing slash can never match a changed path
    if vo_grep -qE '^~?/|//|/$|(^|[~/])\.\.?(/|$)' "${closure_file}"; then
      die "venue-oracles --changed: ${closure_file} has a line that is not a clean relative directory (empty, dot or slash segment); regenerate it (VENUE_CLOSURE_WRITE=1 go test ./internal/testsupport/venuechanged)"
    fi
    [ "${own}" -ne 0 ] || return 0
    if [ -n "${VENUE_CHANGED_DIRS:-}" ] && vo_grep -qxFf "${closure_file}" < <(printf '%s\n' "${VENUE_CHANGED_DIRS}"); then
      return 0
    fi
    # "~dir" lines: a changed file anywhere under a directory a linked package embeds files from.
    if vo_awk 'FNR == NR { if (substr($0, 1, 1) == "~") prefix[substr($0, 2) "/"] = 1; next }
            { for (p in prefix) if (index($0, p) == 1) { found = 1; exit } }
            END { exit !found }' "${closure_file}" <(tr '\0' '\n' < "${VENUE_CHANGED_FILE}"); then
      return 0
    fi
  fi
  [ "${own}" -ne 0 ] || return 0
  [ -n "${2:-}" ] || return 1
  while IFS=$'\t' read -r pkg name path; do
    { [ "${pkg}" = "$1" ] && [ "${name}" = "$2" ]; } || continue
    escaped="$(printf '%s' "${path}" | sed 's/[][\\.*^$/]/\\&/g')"
    if vo_grep -qE "^${escaped}(/.*)?$" < <(tr '\0' '\n' < "${VENUE_CHANGED_FILE}"); then
      return 0
    fi
  done <<< "${VENUE_TRIGGER_ROWS:-}"
  return 1
}

check_venue_oracles() {
  local shard="${1:-}" shard_count="${2:-}" changed="${VENUE_CHANGED_FILE:-}"
  if [ -n "${changed}" ]; then
    [ -r "${changed}" ] || die "venue-oracles --changed: ${changed} is missing or unreadable"
  fi
  if [ -n "${changed}" ]; then
    # CHAOS-9935: in the real tree (it holds the generator of the closure files) a missing closure directory is an
    # error, never "no closure": the selection would silently drop every dependency rule and read as a green no-op.
    # A scratch tree with no Go module has neither the generator nor the directory and keeps the package and row rules.
    if [ -d "${ROOT}/internal/testsupport/venuechanged" ] && [ ! -d "${ROOT}/ci/venue_oracle_closure.d" ]; then
      die "venue-oracles --changed: ${ROOT}/ci/venue_oracle_closure.d is missing; run VENUE_CLOSURE_WRITE=1 go test ./internal/testsupport/venuechanged -run TestClosureFilesMatchTheTestBinaryDependencies"
    fi
    # The package directory of every changed path (a path under .../testdata belongs to the package
    # above it), for the closure rule of venue_oracle_changed_selects.
    VENUE_CHANGED_DIRS="$(tr '\0' '\n' < "${changed}" | awk 'NF { d = $0; sub(/\/[^\/]*$/, "", d); sub(/\/testdata(\/.*)?$/, "", d); print d }' | LC_ALL=C sort -u)"
    VENUE_TRIGGER_ROWS="$(venue_oracle_trigger_rows)" \
      || die "venue-oracles --changed: ci/venue_oracle_triggers.d/ could not be read (see above)"
  fi
  if [ -n "${shard}${shard_count}" ]; then
    case "${shard}" in ""|*[!0-9]*) die "venue-oracles SHARD must be a positive integer, got '${shard}'" ;; esac
    case "${shard_count}" in ""|*[!0-9]*) die "venue-oracles COUNT must be a positive integer, got '${shard_count}'" ;; esac
    { [ "${shard}" -ge 1 ] && [ "${shard_count}" -ge 1 ] && [ "${shard}" -le "${shard_count}" ]; } \
      || die "venue-oracles shard ${shard} is outside 1..${shard_count}"
  fi
  # `--changed FILE SHARD COUNT`: this leg's slice of the rows the change set selects (CHAOS-9935). The legs read the same
  # change set and registry, so each computes the same cost-balanced partition of the same selection; a leg whose slice is
  # empty is a plain no-op (a small change set selects fewer rows than there are legs).
  local changed_slice=""
  if [ -n "${changed}" ] && [ -n "${shard}" ]; then
    local sel_rows sel_dir sel_name
    sel_rows="$(venue_oracle_run_rows)" || die "venue-oracles --changed: the registry ci/venue_oracle_registry.d/ could not be read (see above)"
    while IFS=$'\t' read -r sel_dir sel_name; do
      [ -n "${sel_dir}" ] || continue
      if venue_oracle_changed_selects "${sel_dir}" "${sel_name}"; then
        changed_slice+="${sel_dir}"$'\t'"${sel_name}"$'\n'
      fi
    done <<< "${sel_rows}"
    local sel_count
    sel_count="$(printf '%s' "${changed_slice}" | awk 'NF' | wc -l | tr -d ' ')"
    if [ -n "${changed_slice}" ]; then
      changed_slice="$(printf '%s' "${changed_slice}" | venue_oracle_assign_stdin "${shard}" "${shard_count}")" \
        || die "venue-oracles --changed: could not plan the legs (see above)"
    fi
    printf 'venue-oracles: leg %s/%s of the change set takes %d of %d selected row(s)\n' "${shard}" "${shard_count}" \
      "$(printf '%s' "${changed_slice}" | awk 'NF' | wc -l | tr -d ' ')" "${sel_count}" >&2
  fi
  if [ -n "${changed}" ] && [ "${VENUE_CHANGED_LIST_ONLY:-}" = "--list" ]; then
    # CHAOS-7653: print the registry rows the changed paths select and run nothing (no
    # Go, jq, containers or Python needed). The registry-against-tree validation is the
    # run's own first step; the selection reads the registry rows alone, so it is fast.
    local list_dir list_name list_kind list_rows
    # The rows are read first, so a registry that cannot be read (or holds no run row) is an error: a loop fed by a process
    # substitution cannot see the failure of its producer and read it as "nothing selected".
    list_rows="$(venue_oracle_registry_rows | LC_ALL=C sort -k1,1 -k2,2)" \
      || die "venue-oracles --changed: the registry ci/venue_oracle_registry.d/ could not be read (see above)"
    [ -n "$(printf '%s\n' "${list_rows}" | awk '$3 == "run"')" ] \
      || die "venue-oracles --changed: ci/venue_oracle_registry.d/ holds no run row, so nothing could ever be selected"
    while read -r list_dir list_name list_kind; do
      if [ "${list_kind}" = run ] && venue_oracle_changed_selects "${list_dir}" "${list_name}" \
          && { [ -z "${shard}" ] || grep -qxF -- "${list_dir}"$'\t'"${list_name}" <<< "${changed_slice}"; }; then
        printf 'SELECT\t%s\t%s\n' "${list_dir}" "${list_name}"
      fi
    done <<< "${list_rows}"
    return 0
  fi
  command -v jq >/dev/null 2>&1 \
    || die "venue-oracles requires jq to read go test -json status events (a text-line grep cannot see a subtest skip or tell captured output from a real status line)"

  # The registry is what runs (CHAOS-6584): validate it against discovery and
  # against every Test*VenueOracle* by name FIRST, so a venue test nothing
  # registers fails here, before any container starts.
  check_venue_oracle_registry >&2 || die "venue-oracles: ci/venue_oracle_registry.d/ disagrees with the tree (see above)"

  local proof_dir dir kind name file names="" total=0 prev_pkg="" index
  local pkg_has_run=0 registered_runs=0 slice_file="" predicted_seconds="" leg_started
  local -a vo_dirs=() vo_names=() local_only=()
  leg_started="$(date +%s)"
  proof_dir="$(mktemp -d "${TMPDIR:-/tmp}/dev-health-venue-oracles.XXXXXX")"
  if [ -n "${shard}" ] && [ -n "${changed}" ]; then
    slice_file="${proof_dir}/slice.tsv"
    printf '%s\n' "${changed_slice}" > "${slice_file}"
  elif [ -n "${shard}" ]; then
    # This leg's rows, from the cost-balanced plan; and what the plan predicts for it.
    slice_file="${proof_dir}/slice.tsv"
    venue_oracle_assign "${shard}" "${shard_count}" > "${slice_file}" \
      || { rm -rf -- "${proof_dir}"; die "venue-oracles: could not plan the legs (see above)"; }
    read -r _ predicted_seconds < <(venue_oracle_assign 0 "${shard_count}" plan \
      | awk -F'\t' -v shard="${shard}" '$1 == "#leg" && $2 == shard { print $3, $4 }')
  fi

  # Registry rows are sorted by package, so one package's rows are adjacent.
  # A trailing sentinel row flushes the last package.
  while read -r dir name kind; do
    if [ "${dir}" != "${prev_pkg}" ]; then
      if [ -n "${prev_pkg}" ]; then
        if [ "${pkg_has_run}" -eq 0 ]; then
          # A package whose every registered test is local-only runs zero
          # comparisons while reading as covered: refuse it (on the WHOLE
          # registry, whatever this shard's slice is).
          rm -rf -- "${proof_dir}"
          die "venue-oracles: ${prev_pkg} is registered but no test there is run by this verb (local-only: ${#local_only[@]} so far) -- an oracle package that runs zero comparisons must not read as covered"
        fi
        if [ -n "${names}" ]; then
          vo_dirs+=("${ROOT}/${prev_pkg}")
          vo_names+=("${names}")
        fi
      fi
      prev_pkg="${dir}"
      names=""
      pkg_has_run=0
    fi
    case "${kind}" in
      run)
        pkg_has_run=1
        registered_runs=$((registered_runs + 1))
        if [ -n "${changed}" ]; then
          if venue_oracle_changed_selects "${dir}" "${name}" \
              && { [ -z "${shard}" ] || grep -qxF -- "${dir}"$'\t'"${name}" "${slice_file}"; }; then
            names="${names:+${names}|}${name}"
            total=$((total + 1))
          fi
        elif [ -z "${shard}" ] || grep -qxF -- "${dir}"$'\t'"${name}" "${slice_file}"; then
          names="${names:+${names}|}${name}"
          total=$((total + 1))
        fi
        ;;
      local)
        file="$(grep -lE "^func ${name}[(\[]" "${ROOT}/${dir}"/*_test.go | head -n1)"
        printf 'venue-oracles: %s in %s is marked local-only and is not run here\n' "${name}" "${file#"${ROOT}"/}" >&2
        local_only+=("${name}")
        ;;
    esac
  done < <(venue_oracle_registry_rows | LC_ALL=C sort -k1,1 -k2,2; printf '\001 \001 end\n')

  if [ "${registered_runs}" -eq 0 ]; then
    rm -rf -- "${proof_dir}"
    die "venue-oracles: the registry holds zero runnable venue-oracle tests -- the registry itself is broken, not a genuinely oracle-free tree"
  fi
  if [ "${total}" -eq 0 ] && [ -n "${changed}" ]; then
    rm -rf -- "${proof_dir}"
    printf 'venue-oracles: no changed path lies in a package with a registered venue oracle: nothing to run for this change set\n'
    return 0
  fi
  if [ "${total}" -eq 0 ]; then
    rm -rf -- "${proof_dir}"
    die "venue-oracles: shard ${shard}/${shard_count} selects zero of the ${registered_runs} registered run rows -- the matrix is wider than the registry, so this leg would read green while running nothing"
  fi

  if [ -n "${shard}" ]; then
    printf 'venue-oracles: shard %s/%s runs %d of %d registered run row(s) across %d package(s); the cost-balanced plan predicts %ss of tests for this leg (budget %ss, ci/venue_oracle_weights.d/)\n' "${shard}" "${shard_count}" "${total}" "${registered_runs}" "${#vo_dirs[@]}" "${predicted_seconds:-?}" "${VENUE_LEG_BUDGET_SECONDS:-1800}"
  elif [ -n "${changed}" ]; then
    printf 'venue-oracles: %d of %d registered run row(s) selected by the changed paths, across %d package(s)\n' "${total}" "${registered_runs}" "${#vo_dirs[@]}"
  else
    printf 'venue-oracles: %d registered test(s) across %d package(s)\n' "${total}" "${#vo_dirs[@]}"
  fi

  local rel pattern pkg_proof_dir json_log stderr_log go_test_status proof failed=0 tmo
  local -a compared=() go_only=()
  for index in "${!vo_dirs[@]}"; do
    rel="./${vo_dirs[${index}]#"${ROOT}"/}"
    pattern="^(${vo_names[${index}]})\$"
    tmo="$(job_go_test_timeout 37)" || return 2
    printf '  RUN  %s: %s\n' "${rel}" "${vo_names[${index}]//|/ }"

    pkg_proof_dir="${proof_dir}/${index}"
    mkdir -p "${pkg_proof_dir}"
    json_log="$(mktemp "${TMPDIR:-/tmp}/dev-health-venue-oracles-json.XXXXXX")"
    stderr_log="$(mktemp "${TMPDIR:-/tmp}/dev-health-venue-oracles-stderr.XXXXXX")"

    go_test_status=0
    (
      cd "${ROOT}"
      "${GO_ENV_OFF[@]}" \
        GOWORK=off \
        DEV_HEALTH_VENUE_ORACLES=1 \
        DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${pkg_proof_dir}" \
        go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" \
          -run "${pattern}" -json "${rel}"
    ) >"${json_log}" 2>"${stderr_log}" || go_test_status=$?

    # Human-readable receipt: the captured output events, in the order go
    # test emitted them -- the same text `-v` alone would have printed.
    jq -r 'select(.Action == "output") | .Output' "${json_log}" 2>/dev/null
    if [ -s "${stderr_log}" ]; then
      cat "${stderr_log}" >&2
    fi
    rm -f -- "${stderr_log}"

    if [ "${go_test_status}" -ne 0 ]; then
      failed=1
    fi

    local -a this_names=()
    IFS='|' read -ra this_names <<< "${vo_names[${index}]}"
    for name in "${this_names[@]}"; do
      if jq -e --arg name "${name}" \
          'select(.Action == "skip") | select(.Test == $name or ((.Test // "") | startswith($name + "/")))' \
          "${json_log}" >/dev/null 2>&1; then
        printf 'ERROR: venue oracle %s (or one of its subtests) reported SKIP -- a discovered venue oracle test must never skip under this verb; no real comparison ran\n' "${name}" >&2
        failed=1
      elif [ ! -f "${pkg_proof_dir}/${name}" ]; then
        printf 'ERROR: venue oracle %s did not run a real comparison (no proof file in its own package directory -- venueoracle.Diff was likely never reached)\n' "${name}" >&2
        failed=1
      else
        proof="$(cat "${pkg_proof_dir}/${name}")"
        case "${proof}" in
          executed) compared+=("${name}") ;;
          "go-only: "?*) go_only+=("${name} (${proof#go-only: })") ;;
          *)
            printf 'ERROR: venue oracle %s left an unknown proof %q\n' "${name}" "${proof}" >&2
            failed=1
            ;;
        esac
      fi
    done
    rm -f -- "${json_log}"
  done

  rm -rf -- "${proof_dir}"
  printf 'venue-oracles summary: %d discovered; %d compared both planes; %d Go-only (no Python plane); %d local-only (not run here)\n' \
    "${total}" "${#compared[@]}" "${#go_only[@]}" "${#local_only[@]}"
  for name in "${go_only[@]}"; do printf '  go-only:    %s\n' "${name}"; done
  for name in "${local_only[@]}"; do printf '  local-only: %s\n' "${name}"; done
  # The leg's test time against what the plan predicted and the budget: the log
  # says how close to the job cap a leg ran BEFORE a cap cancels it (CHAOS-6891).
  local elapsed budget="${VENUE_LEG_BUDGET_SECONDS:-1800}"
  elapsed=$(( $(date +%s) - leg_started ))
  printf 'venue-oracles: leg %ss of tests (predicted %ss, budget %ss)\n' "${elapsed}" "${predicted_seconds:-n/a}" "${budget}"
  if [ "${elapsed}" -gt "${budget}" ]; then
    printf '::warning title=venue-oracles leg over its test-time budget::%ss of tests against a %ss budget: rebalance ci/venue_oracle_weights.d/ or add a leg before the job cap cancels it\n' "${elapsed}" "${budget}"
  fi
  [ "${failed}" -eq 0 ] || return 1
}

check_build() {
  local status_before
  local status_after

  DEV_HEALTH_GO_BUILD_TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
  DEV_HEALTH_GO_BUILD_OUTPUT="$(
    mktemp -d "${DEV_HEALTH_GO_BUILD_TEMP_ROOT}/dev-health-go-build.XXXXXX"
  )"
  trap cleanup_go_build_output EXIT
  mkdir -p "${DEV_HEALTH_GO_BUILD_OUTPUT}/bin"
  status_before="${DEV_HEALTH_GO_BUILD_OUTPUT}/status.before"
  status_after="${DEV_HEALTH_GO_BUILD_OUTPUT}/status.after"
  git -C "${ROOT}" status --short --untracked-files=all > "${status_before}"

  # An explicit directory keeps single-main nested modules (including River
  # N-1) from dropping an executable into their source directory.
  run_in_modules "go build" go build -mod=readonly -trimpath \
    -o "${DEV_HEALTH_GO_BUILD_OUTPUT}/bin/" ./...

  git -C "${ROOT}" status --short --untracked-files=all > "${status_after}"
  if ! cmp -s "${status_before}" "${status_after}"; then
    printf 'go build modified the worktree:\n' >&2
    diff -u "${status_before}" "${status_after}" >&2 || true
    cleanup_go_build_output
    trap - EXIT
    return 1
  fi
  printf 'go build: worktree unchanged\n'
  cleanup_go_build_output
  trap - EXIT
}

check_contract() {
  local contract_root="${ROOT}/contracts/jobs/v1"
  local contract_base="${DEV_HEALTH_CONTRACT_BASE:-}"

  [ -d "${contract_root}" ] || die "missing job contract tree ${contract_root}"
  printf 'job contracts: validate\n'
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go run -mod=readonly -trimpath ./cmd/dho contracts \
      validate --root "${contract_root}"
  )

  if [ -n "${contract_base}" ]; then
    [ -d "${contract_base}" ] \
      || die "DEV_HEALTH_CONTRACT_BASE is not a directory: ${contract_base}"
    printf 'job contracts: compare %s\n' "${contract_base}"
    (
      cd "${ROOT}"
      "${GO_ENV_OFF[@]}" GOWORK=off go run -mod=readonly -trimpath ./cmd/dho contracts \
        compare --base "${contract_base}" --candidate "${contract_root}"
    )
  fi
}

# INTEGRATION_DENYLIST is the ONLY place an integration-tagged package may be
# excluded from check_integration. It used to be the other way around: an
# opt-IN allowlist that ran only the packages someone remembered to add. That
# shape silently excluded internal/providerfoundation's ClickHouse/Valkey
# suite, internal/workersctl's suite (which additionally did not even
# compile — see main_integration_test.go), and was structurally guaranteed to
# do it again to the next package anyone adds, because "not on the list" and
# "not written yet" look identical from the gate's point of view.
#
# The default for every integration-tagged package is now "runs in CI".
# Every key here needs a real, load-bearing reason -- "haven't gotten to it"
# or "it currently fails" are NOT valid reasons; a failing package belongs in
# the run set, failing loudly, not hidden here. Legitimate reasons look like
# "needs a live vendor credential CI does not provision."
#
# CHAOS-4730: internal/queryapi/analytics was the one entry here
# (CHAOS-4643), because its ONLY integration-tagged file at the time,
# nan_class_live_test.go, could never run in CI (it dials CLICKHOUSE_URI
# directly, which .github/workflows/go.yml's integration-shard job never
# sets) -- enrolling the package let that file skip silently on every CI
# run while reading as coverage. That premise no longer holds: the package
# now has two REAL Testcontainers-backed regression tests
# (breakdown_seeded_integration_test.go, investmentquality_seeded_integration_test.go)
# that execute for real in CI, same as every other entry in this file's
# EXPECTED_PACKAGES set. nan_class_live_test.go and
# investmentquality_live_test.go remain deliberately opt-in-live (each
# skips with a message naming the env var it needs, per the file's own
# STATUS header) -- opt-in-live-with-a-named-skip inside an otherwise real,
# executing package is the accepted pattern this repo uses elsewhere
# (mirrors internal/providersync's own live-oracle tests), not the CHAOS-4643
# defect (a package whose ENTIRE integration coverage was a permanent,
# silent skip). Removed the entry rather than leaving a stale, misleading
# denylist comment.
declare -A INTEGRATION_DENYLIST=()
declare -A INTEGRATION_SHARD_WEIGHTS=()
declare -A INTEGRATION_SHARD_BY_KEY=()
declare -A AFFECTED_KEYS=()
AFFECTED_FILTER_ACTIVE=0
declare -a INTEGRATION_SHARD_TOTALS=()
declare -a INTEGRATION_SHARD_PACKAGE_COUNTS=()
declare -a INTEGRATION_SHARD_NON_PROVIDER_COUNTS=()
INTEGRATION_SHARD_COUNT=0
PROVIDER_INTEGRATION_PACKAGE_KEY="internal/providersync"
# internal/jobs/metrics/daily is split by top-level test name the same way
# (CHAOS-8813 follow-up): one `go test` of it took 22 of a 25 minute job.
DAILY_INTEGRATION_PACKAGE_KEY="internal/jobs/metrics/daily"
# internal/workerservice is split by top-level test name the same way (CHAOS-9060):
# one `go test` of it took 24 of a 25 minute job.
WORKERSERVICE_INTEGRATION_PACKAGE_KEY="internal/workerservice"
# internal/queryapi/server is split by top-level test name the same way (CHAOS-9162):
# one `go test` of it takes 24.9 of a 25 minute job alone.
QAPISERVER_INTEGRATION_PACKAGE_KEY="internal/queryapi/server"
# internal/jobs/metrics/remaining is split by top-level test name the same way (CHAOS-9639):
# one `go test` of it takes 1,122 s alone, 21 of the 25 minutes of its isolated job.
REMAINING_INTEGRATION_PACKAGE_KEY="internal/jobs/metrics/remaining"
# Packages that get a shard of their own at their TRUE weight (CHAOS-8935).
# One `go test` starts packages in the order of its arguments, four at a time, so a long
# package that starts last starts minutes late and sets the shard's wall time, not its weight
# (CHAOS-11411: the arguments are now sorted longest weight first; an isolated package needs no order).
# internal/jobs/metrics/remaining (CHAOS-9631) runs 1,053-1,164 s on its own and
# sat in a shard with 20 other packages whose job took 22-25 of the 25 minutes.
# internal/fixturescli (CHAOS-11411) was isolated while it ran 1,145-1,168 s; the shared servers (#5197) cut it to about
# 493 s, so CHAOS-11658 took it out of this list. An isolated package needs an empty shard when its turn comes in the
# longest-weight-first order: at its true weight it would find none and the plan would fail.
INTEGRATION_ISOLATED_PACKAGE_KEYS=" ${DEV_HEALTH_GO_INTEGRATION_ISOLATED_KEYS:-internal/workerservice internal/queryapi/server internal/jobs/metrics/remaining} "
DAILY_TEST_SHARD_COUNT=0
DAILY_INTEGRATION_TEST_WEIGHT=0
DAILY_ORDINARY_TEST_WEIGHT=0
declare -A DAILY_INTEGRATION_TEST_NAMES=()
declare -A DAILY_TEST_CLASS=()
declare -A DAILY_TEST_SHARD_BY_NAME=()
declare -a DAILY_TEST_NAMES=()
declare -a DAILY_TEST_SHARD_TOTALS=()
declare -a DAILY_TEST_SHARD_COUNTS=()
WORKERSERVICE_TEST_SHARD_COUNT=0
WORKERSERVICE_INTEGRATION_TEST_WEIGHT=0
WORKERSERVICE_ORDINARY_TEST_WEIGHT=0
declare -A WORKERSERVICE_INTEGRATION_TEST_NAMES=()
declare -A WORKERSERVICE_TEST_CLASS=()
declare -A WORKERSERVICE_TEST_SHARD_BY_NAME=()
declare -a WORKERSERVICE_TEST_NAMES=()
declare -a WORKERSERVICE_TEST_SHARD_TOTALS=()
declare -a WORKERSERVICE_TEST_SHARD_COUNTS=()
QAPISERVER_TEST_SHARD_COUNT=0
QAPISERVER_INTEGRATION_TEST_WEIGHT=0
QAPISERVER_ORDINARY_TEST_WEIGHT=0
declare -A QAPISERVER_INTEGRATION_TEST_NAMES=()
declare -A QAPISERVER_TEST_CLASS=()
declare -A QAPISERVER_TEST_SHARD_BY_NAME=()
declare -a QAPISERVER_TEST_NAMES=()
declare -a QAPISERVER_TEST_SHARD_TOTALS=()
declare -a QAPISERVER_TEST_SHARD_COUNTS=()
REMAINING_TEST_SHARD_COUNT=0
REMAINING_INTEGRATION_TEST_WEIGHT=0
REMAINING_ORDINARY_TEST_WEIGHT=0
declare -A REMAINING_INTEGRATION_TEST_NAMES=()
declare -A REMAINING_TEST_CLASS=()
declare -A REMAINING_TEST_SHARD_BY_NAME=()
declare -a REMAINING_TEST_NAMES=()
declare -a REMAINING_TEST_SHARD_TOTALS=()
declare -a REMAINING_TEST_SHARD_COUNTS=()
declare -A PROVIDER_INTEGRATION_TEST_NAMES=()
declare -A PROVIDER_TEST_WEIGHTS=()
declare -A PROVIDER_TEST_CLASS=()
declare -A PROVIDER_MEASURED_WEIGHTS=()
declare -A PROVIDER_TEST_SHARD_BY_NAME=()
declare -a PROVIDER_TEST_NAMES=()
declare -a PROVIDER_TEST_SHARD_TOTALS=()
declare -a PROVIDER_TEST_SHARD_COUNTS=()
declare -a PROVIDER_TEST_SHARD_INTEGRATION_COUNTS=()
PROVIDER_TEST_SHARD_COUNT=0
PROVIDER_INTEGRATION_TEST_WEIGHT=0
PROVIDER_ORDINARY_TEST_WEIGHT=0

# scan_listed_go_tests validates the test file names `go list` returned for one package directory, then parses every
# file in ONE gotestfuncs run (go/parser, so a comment or a string can neither hide nor invent a function). It fills two
# globals keyed by source path: SCAN_TESTS (newline-separated top-level test names, file order; TestMain is not one)
# and SCAN_HOOK (the first `func init` or `func TestMain` of the file, else its first unreviewed package-level side effect). The names are validated before any file is read.
scan_listed_go_tests() {
  local label="$1" dir="$2" listed="$3" go_file
  local -a paths=()
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    check_listed_test_file "${label}" "${dir}" "${go_file}"
    paths+=("${dir}/${go_file}")
  done <<<"${listed}"
  scan_go_test_paths ${paths[@]+"${paths[@]}"}
}

check_listed_test_file() {
  local label="$1" dir="$2" go_file="$3"
  case "${go_file}" in
    */*|..*) die "go list returned unsafe ${label} test filename '${go_file}'" ;;
  esac
  case "${go_file}" in
    *[[:cntrl:]]*) die "go list returned unsafe ${label} test filename with a control byte: $(printf '%q' "${go_file}")" ;;
  esac
  [ -f "${dir}/${go_file}" ] || die "go list returned missing ${label} test file '${go_file}'"
}

# gotestfuncs_bin prints the path of ci/tools/gotestfuncs built from this tree, building it once per source and Go
# version (a stdlib-only program, build tag ignore, so it is no package of ./...).
GOTESTFUNCS_BIN=""
gotestfuncs_bin() {
  local source="${ROOT}/ci/tools/gotestfuncs/main.go" key target
  if [ -z "${GOTESTFUNCS_BIN:-}" ]; then
    [ -f "${source}" ] || die "ci/tools/gotestfuncs/main.go is missing"
    key="$(cd "${ROOT}" && { sha256sum "${source}"; "${GO_ENV_OFF[@]}" GOWORK=off go env GOVERSION; } | sha256sum | cut -c1-16)" \
      || die "could not key the gotestfuncs build"
    target="${TMPDIR:-/tmp}/dev-health-gotestfuncs-${key}"
    if [ ! -x "${target}" ]; then
      (cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go build -mod=readonly -trimpath -o "${target}.$$" "${source}") \
        || die "go build of ci/tools/gotestfuncs failed"
      mv -f "${target}.$$" "${target}"
    fi
    GOTESTFUNCS_BIN="${target}"
  fi
  printf '%s\n' "${GOTESTFUNCS_BIN}"
}

# scan_go_test_paths PATH...: the one gotestfuncs run of scan_listed_go_tests over validated paths.
scan_go_test_paths() {
  local kind path name bin scanned reviewed="${ROOT}/ci/integration_side_effect_reviewed.tsv"
  local -a reviewed_args=()
  declare -gA SCAN_TESTS=() SCAN_RUNS=() SCAN_HOOK=()
  [ "$#" -gt 0 ] || return 0
  gotestfuncs_bin >/dev/null
  bin="${GOTESTFUNCS_BIN}"
  # A package-level side effect no reviewed row covers is a hook too (CHAOS-11429); a root without the table reviews none.
  [ ! -f "${reviewed}" ] || reviewed_args=(-root "${ROOT}" -reviewed "${reviewed}")
  scanned="$(printf '%s\0' "$@" | xargs -0 "${bin}" ${reviewed_args[@]+"${reviewed_args[@]}"})" || die "gotestfuncs could not parse the listed test files"
  while IFS=$'\037' read -r kind path name; do
    case "${kind}" in
      T) SCAN_TESTS["${path}"]+="${name}"$'\n' ;;
      X) SCAN_RUNS["${path}"]+="${name}"$'\n' ;;
      H) SCAN_HOOK["${path}"]="${name}" ;;
      "") ;;
      *) die "gotestfuncs printed an unknown record '${kind}'" ;;
    esac
  done <<<"${scanned}"
}

# go_run_name_ok NAME: NAME is "Test", "Example" or "Fuzz" plus identifier characters (ASCII letters, digits, '_', or
# any non-ASCII byte of a Unicode letter or digit), so it is safe inside a `-run '^(...)$'` alternation. gotestfuncs
# prints Go identifiers only; this refuses anything else loudly.
go_run_name_ok() {
  local LC_ALL=C rest
  case "$1" in Test*|Example*|Fuzz*) ;; *) return 1 ;; esac
  rest="${1//[A-Za-z0-9_]/}"
  [[ ! "${rest}" =~ [[:print:][:cntrl:]] ]]
}

# go_test_name_ok NAME: go_run_name_ok for a "Test" name.
go_test_name_ok() {
  case "$1" in Test*) go_run_name_ok "$1" ;; *) return 1 ;; esac
}

# integration_test_files MODULE_DIR PKG...: the ONE place that decides which test files of a package belong to the
# integration run, for the packages shards and the split planners alike. A file belongs to it when
# `go list -tags=integration` lists it (TestGoFiles + XTestGoFiles) and plain `go list -e` does not: build constraints
# decide membership through go list alone; no file text is read and no file is globbed. Fills TEST_FILES_TAGGED[PKG]
# (the newline list of every test file the integration build lists, in go list order) and TEST_FILE_TAG_ONLY[path]
# (absolute path of such a file that the plain build does not list).
integration_test_files() {
  local module_dir="$1" modpath pkg import base dir go_file plain_out tagged_out
  shift
  declare -gA TEST_FILES_TAGGED=() TEST_FILE_TAG_ONLY=()
  local -A pkg_by_import=() in_plain=()
  local -a fields=()
  local template='{{.ImportPath}}{{range .TestGoFiles}}{{"\t"}}{{.}}{{end}}{{range .XTestGoFiles}}{{"\t"}}{{.}}{{end}}'

  [ "$#" -gt 0 ] || return 0
  base="${ROOT}"
  [ "${module_dir}" = "." ] || base="${ROOT}/${module_dir}"
  modpath="$(cd "${base}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -m)" \
    || die "go list -m failed in ${module_dir}"
  for pkg in "$@"; do
    case "${pkg}" in
      .|./) import="${modpath}" ;;
      *) import="${modpath}/${pkg#./}" ;;
    esac
    pkg_by_import["${import}"]="${pkg}"
  done
  # -e: a package whose every test file is tagged has no plain build; it still prints its (empty) plain file list.
  plain_out="$(cd "${base}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -e -mod=readonly -f "${template}" "$@")" \
    || die "go list failed while listing the plain test files in ${module_dir}"
  tagged_out="$(cd "${base}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -tags=integration -f "${template}" "$@")" \
    || die "go list -tags=integration failed while listing the test files in ${module_dir}"
  while IFS=$'\t' read -r -a fields; do
    [ "${#fields[@]}" -gt 0 ] || continue
    pkg="${pkg_by_import[${fields[0]}]-}"
    [ -n "${pkg}" ] || die "go list returned the unrequested package '${fields[0]}' in ${module_dir}"
    for go_file in "${fields[@]:1}"; do
      in_plain["${pkg}/${go_file}"]=1
    done
  done <<<"${plain_out}"
  while IFS=$'\t' read -r -a fields; do
    [ "${#fields[@]}" -gt 0 ] || continue
    pkg="${pkg_by_import[${fields[0]}]-}"
    [ -n "${pkg}" ] || die "go list -tags=integration returned the unrequested package '${fields[0]}' in ${module_dir}"
    dir="${base}/${pkg#./}"
    TEST_FILES_TAGGED["${pkg}"]=""
    for go_file in "${fields[@]:1}"; do
      check_listed_test_file "integration" "${dir}" "${go_file}"
      TEST_FILES_TAGGED["${pkg}"]+="${go_file}"$'\n'
      [ -n "${in_plain[${pkg}/${go_file}]+set}" ] || TEST_FILE_TAG_ONLY["${dir}/${go_file}"]=1
    done
  done <<<"${tagged_out}"
}

# split_package_hook KEY LISTED: "<key>/<file> <func>" of the first integration-tagged test file of a split package that
# declares `func init` or `func TestMain`, else an unreviewed package-level side effect (SCAN_HOOK of its scan; the class from
# integration_test_files), or nothing. Such a
# package keeps every test in its shards, as run_integration_packages keeps such a package whole.
split_package_hook() {
  local key="$1" listed="$2" go_file source_file effect=""
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    source_file="${ROOT}/${key}/${go_file}"
    if [ -n "${TEST_FILE_TAG_ONLY[${source_file}]+set}" ] && [ -n "${SCAN_HOOK[${source_file}]+set}" ]; then
      if [[ "${SCAN_HOOK[${source_file}]}" != *:* ]]; then
        printf '%s/%s %s\n' "${key}" "${go_file}" "${SCAN_HOOK[${source_file}]}"
        return 0
      fi
      [ -n "${effect}" ] || effect="${key}/${go_file} ${SCAN_HOOK[${source_file}]}"
    fi
  done <<<"${listed}"
  [ -z "${effect}" ] || printf '%s\n' "${effect}"
}

# split_test_runs_here CLASS UNFILTERED: true for a test a split shard runs (CHAOS-11407): an integration-tagged test,
# or any test of a package with a tag-only hook. Untagged tests run in the go-quality test and race legs.
split_test_runs_here() {
  [ "$1" = "integration" ] || [ -n "$2" ]
}

# split_plan_scope LABEL UNFILTERED ALL TAGGED SHARDS COUNTS: prints which tests the split shards leave to the test and
# race legs, and fails when a shard plans no test (more shards than the tests that run here).
split_plan_scope() {
  local label="$1" unfiltered="$2" all="$3" tagged="$4" shards="$5" shard
  local -n planned_counts="$6"
  if [ -n "${unfiltered}" ]; then
    printf '%s test plan: UNFILTERED %s: a tag-only hook changes the process of the untagged tests, so all %d test(s) run here\n' \
      "${label}" "${unfiltered}" "${all}"
  else
    printf '%s test plan: %d untagged test(s) run in the go-quality test and race legs, not here\n' "${label}" "$((all - tagged))"
  fi
  for ((shard = 1; shard <= shards; shard++)); do
    [ "${planned_counts[${shard}]:-0}" -gt 0 ] \
      || die "${label} test shard ${shard} plans zero tests: the manifest declares more shards than there are ${label} tests that run in a shard"
  done
}

# discover_integration_tagged_tests MODULE_DIR PKG...: for module-relative packages ("./dir") of one module, the tests a
# shard runs: the files integration_test_files classes as integration-only, parsed by gotestfuncs. Fills, keyed by PKG:
# INTEGRATION_TAGGED_TESTS (newline list of the names go test runs from those files: top-level tests, fuzz targets and
# examples with output), INTEGRATION_UNTAGGED_TESTS (the same for every other test file the integration build lists)
# and INTEGRATION_HOOK ("<key>/<file> <func>" of the first `func init` or `func TestMain` in an integration-only file:
# that hook changes the process the untagged tests run in under the tag, so the package selects every name it has).
discover_integration_tagged_tests() {
  local module_dir="$1" pkg key base dir go_file path name rel
  shift
  declare -gA INTEGRATION_TAGGED_TESTS=() INTEGRATION_UNTAGGED_TESTS=() INTEGRATION_HOOK=()
  local -A path_pkg=()
  local -a paths=()

  [ "$#" -gt 0 ] || return 0
  integration_test_files "${module_dir}" "$@"
  base="${ROOT}"
  [ "${module_dir}" = "." ] || base="${ROOT}/${module_dir}"
  for pkg in "$@"; do
    dir="${base}/${pkg#./}"
    while IFS= read -r go_file; do
      [ -n "${go_file}" ] || continue
      path_pkg["${dir}/${go_file}"]="${pkg}"
      paths+=("${dir}/${go_file}")
    done <<<"${TEST_FILES_TAGGED[${pkg}]-}"
  done
  scan_go_test_paths ${paths[@]+"${paths[@]}"}
  for path in ${paths[@]+"${paths[@]}"}; do
    pkg="${path_pkg[${path}]}"
    # A func init or TestMain names the package's hook over a package-level side effect, whatever the file order.
    if [ -n "${TEST_FILE_TAG_ONLY[${path}]+set}" ] && [ -n "${SCAN_HOOK[${path}]+set}" ] \
      && { [ -z "${INTEGRATION_HOOK[${pkg}]+set}" ] || { [[ "${INTEGRATION_HOOK[${pkg}]}" == *:* ]] && [[ "${SCAN_HOOK[${path}]}" != *:* ]]; }; }; then
      key="${module_dir}/${pkg#./}"
      key="${key#./}"
      rel="${key}/${path##*/}"
      INTEGRATION_HOOK["${pkg}"]="${rel} ${SCAN_HOOK[${path}]}"
    fi
    while IFS= read -r name; do
      [ -n "${name}" ] || continue
      go_run_name_ok "${name}" || die "integration test discovery returned unsupported top-level test name '${name}' in ${path}"
      if [ -n "${TEST_FILE_TAG_ONLY[${path}]+set}" ]; then
        INTEGRATION_TAGGED_TESTS["${pkg}"]+="${name}"$'\n'
      else
        INTEGRATION_UNTAGGED_TESTS["${pkg}"]+="${name}"$'\n'
      fi
    done <<<"${SCAN_TESTS[${path}]-}${SCAN_RUNS[${path}]-}"
  done
}

# run_integration_packages MODE MODULE_DIR LABEL PKG...: runs the PKGs of one module under the integration tag
# (CHAOS-11407). The untagged tests of these packages already run in the go-quality test legs (plain) and race legs
# (-race), which run every package on every merge_group (plan-verdict FULL); here they ran a third time. So a package
# selects, through `-run`, only the names go test runs (tests, fuzz targets, examples with output) of its
# integration-only test files. A package with a tag-only hook (INTEGRATION_HOOK) changes the process its untagged tests
# run in, so it selects every name it has (it runs whole) and skips none. The packages are packed first-fit into go
# test groups, so a shard runs one go test (-p keeps a group's packages parallel; a hook package in a separate, later
# go test added the group's wall to its own): a package joins a group only when no name it selects is a name a group
# member skips or the reverse (a shared `-run` would run that skipped test again), and while the group's regex stays
# within regex_max bytes (Linux refuses one argument over 128 KiB, MAX_ARG_STRLEN; every integration package of the root
# module in one regex is about 163 KB). Receipts: SHARD-UNFILTERED <key> <file> <func> per hook package, SHARD-TEST
# <key> <name> per selected name, and per group SHARD-GROUP <module> <n> <key>... then SHARD-REGEX <module> <regex>; a
# group after the first also gets, per earlier group <g>, SHARD-SPLIT <module> <n> <key> <g> collision <name> <member>
# or SHARD-SPLIT <module> <n> <key> <g> budget <bytes>: why its first package <key> did not join group <g>.
# warn_integration_weight_drift MODULE_DIR LOGFILE PKG...: reads the `ok <import path> <N>s` lines go test printed into
# LOGFILE and prints one `::warning title=INTEGRATION WEIGHT DRIFT::` line for each package whose measured time is more than
# 50% away from its weight in ci/go_integration_shards.d and at least 60 s on either side (CHAOS-11658). A weight is a
# placement value and a measured time includes the packages that ran beside it, so this only tells a reader a row is stale:
# it never fails, whatever the lines hold.
warn_integration_weight_drift() {
  local module_dir="$1" log="$2" pkg key weight line import_path seconds
  shift 2
  for pkg in "$@"; do
    key="${module_dir}/${pkg#./}"
    key="${key#./}"
    weight="${INTEGRATION_SHARD_WEIGHTS[${key}]:-}"
    [ -n "${weight}" ] || continue
    while IFS=$'\t' read -r import_path seconds; do
      [[ "${import_path}" == */"${pkg#./}" ]] || continue
      awk -v key="${key}" -v n="${seconds%s}" -v w="${weight}" 'BEGIN {
        d = n - w; if (d < 0) d = -d
        m = (n > w) ? n : w
        if (m >= 60 && d * 100 > 50 * w) printf "::warning title=INTEGRATION WEIGHT DRIFT::%s measured %.0fs, weight %ds (%+.0f%%)\n", key, n, w, (n - w) * 100 / w
      }'
    done < <(sed -n -E 's/^ok[[:space:]]+([^[:space:]]+)[[:space:]]+([0-9.]+s).*$/\1\t\2/p' "${log}")
  done
  return 0
}

run_integration_packages() {
  local mode="$1" module_dir="$2" label="$3" pkg key name tests=0 result=0 status groups=0 group fits why split_why size names_len regex_max=60000
  local -a unfiltered=() run_pkgs=() group_pkgs=() group_keys=() group_regex=() group_size=() group_split=()
  local -a pkgs=()
  local -A selected_in=() skipped_in=() select_names=() skip_names=() seen=()
  shift 3
  # Sorted (C locale): the same packages give the same groups and receipts in any argument order. A caller that sets
  # INTEGRATION_KEEP_ARG_ORDER=1 has already put the packages in the order go test must start them (longest first,
  # CHAOS-11411): the first-fit packs them in that order and a repeated package keeps its first place.
  if [ -n "${INTEGRATION_KEEP_ARG_ORDER:-}" ]; then
    local -A arg_seen=()
    for pkg in "$@"; do
      [ -z "${arg_seen[${pkg}]+set}" ] || continue
      arg_seen["${pkg}"]=1
      pkgs+=("${pkg}")
    done
  else
    mapfile -t pkgs < <(printf '%s\n' "$@" | LC_ALL=C sort -u)
  fi
  discover_integration_tagged_tests "${module_dir}" "${pkgs[@]}"
  # Every refusal comes before the first receipt line, so a refused run prints no plan.
  for pkg in "${pkgs[@]}"; do
    key="${module_dir}/${pkg#./}"
    key="${key#./}"
    select_names["${pkg}"]="${INTEGRATION_TAGGED_TESTS[${pkg}]-}"
    skip_names["${pkg}"]="${INTEGRATION_UNTAGGED_TESTS[${pkg}]-}"
    if [ -n "${INTEGRATION_HOOK[${pkg}]+set}" ]; then
      select_names["${pkg}"]+="${skip_names[${pkg}]}"
      skip_names["${pkg}"]=""
      [ -n "${select_names[${pkg}]}" ] \
        || die "${label}: ${key} runs in an integration shard, but declares no test, fuzz target or example with output"
    fi
    [ -n "${select_names[${pkg}]}" ] \
      || die "${label}: ${key} runs in an integration shard, but no test file that only the integration build lists declares a top-level Test"
    seen=()
    names_len=0
    while IFS= read -r name; do
      [ -n "${name}" ] || continue
      [ -z "${seen[${name}]+set}" ] || continue
      seen["${name}"]=1
      names_len=$((names_len + ${#name} + 1))
    done <<<"${select_names[${pkg}]}"
    [ $((names_len + 4)) -le "${regex_max}" ] \
      || die "${label}: the selected test names of ${key} alone exceed the ${regex_max}-byte -run budget"
  done
  for pkg in "${pkgs[@]}"; do
    key="${module_dir}/${pkg#./}"
    key="${key#./}"
    if [ -n "${INTEGRATION_HOOK[${pkg}]+set}" ]; then
      printf '  SHARD-UNFILTERED %s %s\n' "${key}" "${INTEGRATION_HOOK[${pkg}]}"
      unfiltered+=("${pkg}")
    fi
    seen=()
    while IFS= read -r name; do
      [ -n "${name}" ] || continue
      [ -z "${seen[${name}]+set}" ] || continue
      seen["${name}"]=1
      printf '  SHARD-TEST %s %s\n' "${key}" "${name}"
      tests=$((tests + 1))
    done <<<"${select_names[${pkg}]}"
    split_why=""
    for ((group = 0; group < groups; group++)); do
      fits=1 why=""
      size="${group_size[${group}]}"
      while IFS= read -r name; do
        [ -n "${name}" ] || continue
        if [ -n "${skipped_in[${group}|${name}]+set}" ]; then fits=0 why="collision ${name} ${skipped_in[${group}|${name}]}"; break; fi
        [ -n "${selected_in[${group}|${name}]+set}" ] || size=$((size + ${#name} + 1))
      done <<<"${select_names[${pkg}]}"
      if [ "${fits}" -eq 1 ] && [ "${size}" -gt "${regex_max}" ]; then fits=0 why="budget ${size}"; fi
      if [ "${fits}" -eq 1 ]; then
        while IFS= read -r name; do
          [ -n "${name}" ] || continue
          if [ -n "${selected_in[${group}|${name}]+set}" ]; then fits=0 why="collision ${name} ${selected_in[${group}|${name}]}"; break; fi
        done <<<"${skip_names[${pkg}]}"
      fi
      [ "${fits}" -eq 1 ] && break
      split_why+="  SHARD-SPLIT ${module_dir} $((groups + 1)) ${key} $((group + 1)) ${why}"$'\n'
    done
    if [ "${group}" -eq "${groups}" ]; then
      groups=$((groups + 1))
      group_pkgs[group]="" group_keys[group]="" group_regex[group]="" group_size[group]=4 group_split[group]="${split_why}"
    fi
    while IFS= read -r name; do
      [ -n "${name}" ] || continue
      [ -z "${selected_in[${group}|${name}]+set}" ] || continue
      selected_in["${group}|${name}"]="${key}"
      group_regex[group]+="${group_regex[${group}]:+|}${name}"
      group_size[group]=$((group_size[group] + ${#name} + 1))
    done <<<"${select_names[${pkg}]}"
    while IFS= read -r name; do
      [ -n "${name}" ] || continue
      [ -n "${skipped_in[${group}|${name}]+set}" ] || skipped_in["${group}|${name}"]="${key}"
    done <<<"${skip_names[${pkg}]}"
    group_pkgs[group]+="${pkg}"$'\n'
    group_keys[group]+=" ${key}"
  done
  printf '%s: %d package(s), %d selected test(s), %d unfiltered package(s), %d go test group(s)\n' \
    "${label}" "${#pkgs[@]}" "${tests}" "${#unfiltered[@]}" "${groups}"
  for ((group = 0; group < groups; group++)); do
    group_regex[group]="^(${group_regex[${group}]})\$"
    printf '  SHARD-GROUP %s %d%s\n' "${module_dir}" "$((group + 1))" "${group_keys[${group}]}"
    printf '  SHARD-REGEX %s %s\n' "${module_dir}" "${group_regex[${group}]}"
    printf '%s' "${group_split[${group}]}"
  done
  [ "${mode}" != "--dry-run" ] || return 0
  # Every go test goes through run_integration_go_test (one reaped testcontainers session per call); -run rides in run_pkgs.
  # Its stdout also goes to a file (tee) for warn_integration_weight_drift. run_integration_go_test stays in this shell:
  # its INT/TERM trap (CHAOS-10036) must be this shell's, so a TERM to this script still reaps the session. The tee is one
  # process substitution on its own descriptor; closing the descriptor ends it, and wait makes the file complete before it
  # is read. The status of go test comes straight from the function (CHAOS-11658).
  local ok_log tee_fd tee_pid
  ok_log="$(mktemp "${TMPDIR:-/tmp}/dev-health-ok.XXXXXX")"
  exec {tee_fd}> >(tee -a "${ok_log}")
  tee_pid=$!
  for ((group = 0; group < groups; group++)); do
    mapfile -t run_pkgs <<<"${group_pkgs[${group}]%$'\n'}"
    run_pkgs=(-run "${group_regex[${group}]}" "${run_pkgs[@]}")
    run_integration_go_test "${module_dir}" "${run_pkgs[@]}" >&"${tee_fd}" || { status=$?; [ "${result}" -ne 0 ] || result="${status}"; }
  done
  exec {tee_fd}>&-
  # tee ends when every writer has closed the pipe; a process go test left behind that still holds it would stall this
  # wait to the job cap (the CHAOS-11794 class). Bounded: past the bound only the tee is stopped (never the leftover
  # writer, which is not ours to name) and the drift check reads the partial log. bash cannot mark tee_fd close-on-exec,
  # so go test and its children inherit it next to their stdout: the same pipe, no new writer.
  local tee_wait="${DEV_HEALTH_GO_TEE_WAIT_SECS:-30}" tee_tick
  case "${tee_wait}" in ""|*[!0-9]*) tee_wait=30 ;; esac
  tee_wait=$((10#${tee_wait}))
  for ((tee_tick = 0; tee_tick < tee_wait * 5; tee_tick++)); do
    kill -0 "${tee_pid}" 2>/dev/null || break
    sleep 0.2
  done
  if kill -0 "${tee_pid}" 2>/dev/null; then
    kill -TERM "${tee_pid}" 2>/dev/null || true
    echo "check_go.sh: the drift check read a partial log: a process still holds go test's stdout ${tee_wait}s after go test ended" >&2
  fi
  wait "${tee_pid}" 2>/dev/null || true
  warn_integration_weight_drift "${module_dir}" "${ok_log}" "${pkgs[@]}"
  rm -f -- "${ok_log}"
  return "${result}"
}


# discover_integration_packages populates parallel module/package arrays with
# every module-relative "./pkg/dir" entry that has at least one tracked or
# untracked, non-vendor *_test.go file whose leading build-constraint comment
# names the "integration" tag. One indexed `git grep` handles tracked files;
# only the normally tiny untracked set needs per-file inspection. The previous
# grep-per-test-file scan launched thousands of processes and repeatedly hit the
# full pytest-xdist gate's fixed 120-second subprocess guard.
discover_integration_packages() {
  local module_dir go_file dir tracked_output grep_status
  declare -ga INTEGRATION_PACKAGE_MODULES=()
  declare -ga INTEGRATION_PACKAGES=()

  for module_dir in "${MODULE_DIRS[@]}"; do
    local -a found=()
    grep_status=0
    tracked_output="$(
      git -C "${ROOT}/${module_dir}" grep -l -E \
        '^//go:build.*(^|[^[:alnum:]_])integration([^[:alnum:]_]|$)' \
        -- '*_test.go'
    )" || grep_status=$?
    [ "${grep_status}" -eq 0 ] || [ "${grep_status}" -eq 1 ] \
      || die "git grep failed while discovering integration tests in ${module_dir}"
    while IFS= read -r go_file; do
      [ -n "${go_file}" ] || continue
      case "${go_file}" in
        vendor/*|*/vendor/*) continue ;;
      esac
      case "${go_file}" in */*) dir="${go_file%/*}" ;; *) dir="." ;; esac
      found+=("./${dir}")
    done < <(printf '%s\n' "${tracked_output}")
    while IFS= read -r -d '' go_file; do
      [ -f "${ROOT}/${module_dir}/${go_file}" ] || continue
      case "${go_file}" in
        vendor/*|*/vendor/*) continue ;;
      esac
      if grep -qE '^//go:build.*(^|[^[:alnum:]_])integration([^[:alnum:]_]|$)' \
        "${ROOT}/${module_dir}/${go_file}"; then
        case "${go_file}" in */*) dir="${go_file%/*}" ;; *) dir="." ;; esac
        found+=("./${dir}")
      fi
    done < <(
      git -C "${ROOT}/${module_dir}" ls-files --others --exclude-standard -z -- '*_test.go'
    )
    [ "${#found[@]}" -gt 0 ] || continue
    while IFS= read -r dir; do
      [ -n "${dir}" ] || continue
      INTEGRATION_PACKAGE_MODULES+=("${module_dir}")
      INTEGRATION_PACKAGES+=("${dir}")
    done < <(printf '%s\n' "${found[@]}" | sort -u)
  done
}

# integration_denylist_reason prints the reason for "module_dir/pkg" if it is
# denylisted, or nothing (and a false exit) if it is not.
integration_denylist_reason() {
  local key="$1"
  if [ -n "${INTEGRATION_DENYLIST[${key}]+set}" ]; then
    printf '%s' "${INTEGRATION_DENYLIST[${key}]}"
    return 0
  fi
  return 1
}

# check_integration_coverage is the guard: it recomputes discovery
# independently of check_integration's own run, prints the full plan (run vs.
# skip, with every skip's reason), and fails loudly if the denylist names a
# package discovery does not find (a stale or misspelled entry -- the
# opt-out list drifting unnoticed is exactly the failure mode this replaces,
# just on the small side of the ledger instead of the large one). It needs no
# Docker and belongs in the fast path.
check_integration_coverage() {
  discover_integration_packages
  local index module_dir pkg key reason
  local -a denylist_seen=()
  local total=0

  printf 'integration coverage: discovered packages (module: package)\n'
  for index in "${!INTEGRATION_PACKAGES[@]}"; do
    module_dir="${INTEGRATION_PACKAGE_MODULES[${index}]}"
    pkg="${INTEGRATION_PACKAGES[${index}]}"
    total=$((total + 1))
    key="${module_dir}/${pkg#./}"
    key="${key#./}"
    if reason="$(integration_denylist_reason "${key}")"; then
      denylist_seen+=("${key}")
      printf '  SKIP %s: %s\n' "${key}" "${reason}"
    else
      printf '  RUN  %s\n' "${key}"
    fi
  done

  if [ "${total}" -eq 0 ]; then
    die "integration coverage: discovered zero integration-tagged packages -- the discovery mechanism itself is almost certainly broken, not a genuinely test-free tree"
  fi

  local denylisted_key covered
  for denylisted_key in "${!INTEGRATION_DENYLIST[@]}"; do
    covered=0
    for key in "${denylist_seen[@]}"; do
      [ "${key}" = "${denylisted_key}" ] && covered=1 && break
    done
    if [ "${covered}" -ne 1 ]; then
      die "INTEGRATION_DENYLIST names '${denylisted_key}', which discover_integration_packages did not find -- stale or misspelled denylist entry"
    fi
  done

  printf 'integration coverage: %d package(s) discovered, %d denylisted, %d will run\n' \
    "${total}" "${#denylist_seen[@]}" "$((total - ${#denylist_seen[@]}))"
}

# load_integration_shard_manifest validates the manifest's syntax only. The
# package-set equality check happens in plan_integration_shards after live
# discovery, so neither the manifest nor the source scan can silently stand in
# for the other.
#
# The manifest is a DIRECTORY of row files (CHAOS-6926): one file per package,
# named for its key with "/" written "__", holding exactly one package row, plus
# `_shards.tsv` with the one `shards` row. Two PRs that touch different packages
# therefore touch different files and never conflict, where every PR used to edit
# neighbouring lines of one sorted file. A single file is still accepted as a
# manifest (the tests build small ones); it is then read as one file with any
# number of rows.
load_integration_shard_manifest() {
  local manifest="${DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST}"
  local -a files=()
  local file base as_dir=0

  INTEGRATION_SHARD_WEIGHTS=()
  INTEGRATION_SHARD_COUNT=0
  SHARDS_SEEN=0
  if [ -d "${manifest}" ]; then
    as_dir=1
    # C-locale glob order (function-local, restored on return), so the order of the
    # files and of the diagnostics does not depend on the host locale.
    local LC_ALL=C
    files=("${manifest}"/*.tsv)
    [ -e "${files[0]:-}" ] || die "integration shard manifest directory has no *.tsv files: ${manifest}"
  elif [ -f "${manifest}" ]; then
    files=("${manifest}")
  else
    die "integration shard manifest not found: ${manifest}"
  fi

  for file in "${files[@]}"; do
    base="${file##*/}"
    load_integration_shard_manifest_file "${file}" "${as_dir}" "${base}"
  done

  [ "${SHARDS_SEEN}" -eq 1 ] \
    || die "integration shard manifest must declare one 'shards' row"
  [ "${INTEGRATION_SHARD_COUNT}" -ge 2 ] \
    || die "integration shard manifest must declare at least two shards"
}

# load_integration_shard_manifest_file FILE AS_DIR BASENAME reads one manifest file.
# In directory mode a file holds exactly one package row and is named for its key
# ('_shards.tsv' holds the shards row instead), so a key cannot hide in a
# misnamed file and two PRs adding the same package collide on the file name.
load_integration_shard_manifest_file() {
  local file="$1" as_dir="$2" base="$3"
  local line_number=0 key value extra package_rows=0

  while IFS=$'\t ' read -r key value extra; do
    line_number=$((line_number + 1))
    case "${key}" in
      ""|\#*) continue ;;
    esac
    if [ -n "${extra}" ]; then
      die "integration shard manifest ${file}:${line_number} must contain exactly two fields"
    fi
    case "${value}" in
      ""|*[!0-9]*)
        die "integration shard manifest ${file}:${line_number} has non-numeric value '${value}'"
        ;;
    esac
    if [ "${value}" -le 0 ]; then
      die "integration shard manifest ${file}:${line_number} values must be positive"
    fi

    if [ "${key}" = "shards" ]; then
      if [ "${as_dir}" -eq 1 ] && [ "${base}" != "_shards.tsv" ]; then
        die "integration shard manifest ${file}:${line_number}: the 'shards' row belongs in _shards.tsv"
      fi
      [ "${SHARDS_SEEN}" -eq 0 ] \
        || die "integration shard manifest declares 'shards' more than once"
      SHARDS_SEEN=1
      INTEGRATION_SHARD_COUNT="${value}"
      continue
    fi
    if [ "${as_dir}" -eq 1 ]; then
      [ "${base}" != "_shards.tsv" ] \
        || die "integration shard manifest ${file}:${line_number}: _shards.tsv holds only the 'shards' row"
      [ "${base}" = "${key//\//__}.tsv" ] \
        || die "integration shard manifest ${file}:${line_number}: the row for '${key}' must live in ${key//\//__}.tsv"
      package_rows=$((package_rows + 1))
      [ "${package_rows}" -eq 1 ] \
        || die "integration shard manifest ${file}:${line_number}: one package row per file"
    fi
    if [ -n "${INTEGRATION_SHARD_WEIGHTS[${key}]+set}" ]; then
      die "integration shard manifest lists '${key}' more than once"
    fi
    INTEGRATION_SHARD_WEIGHTS["${key}"]="${value}"
  done < "${file}"

  if [ "${as_dir}" -eq 1 ] && [ "${base}" != "_shards.tsv" ] && [ "${package_rows}" -eq 0 ]; then
    die "integration shard manifest ${file} holds no package row"
  fi
}

# plan_integration_shards performs deterministic longest-processing-time-first
# assignment from the checked-in weights. Package paths break equal-weight
# ties; the lowest-numbered shard breaks equal-load ties. A single dominant
# package therefore stays isolated while the remaining packages balance across
# the other jobs. Every call starts with fresh source discovery and exact
# manifest equality, preserving the 25/25, zero-denylist contract independently
# in every matrix runner.
plan_integration_shards() {
  check_integration_coverage
  load_integration_shard_manifest

  local index module_dir pkg key reason manifest_key
  local runnable_count=0
  local shard selected_shard selected_total weight
  declare -A discovered_run_packages=()

  for index in "${!INTEGRATION_PACKAGES[@]}"; do
    module_dir="${INTEGRATION_PACKAGE_MODULES[${index}]}"
    pkg="${INTEGRATION_PACKAGES[${index}]}"
    key="${module_dir}/${pkg#./}"
    key="${key#./}"
    if reason="$(integration_denylist_reason "${key}")"; then
      continue
    fi
    discovered_run_packages["${key}"]=1
    runnable_count=$((runnable_count + 1))
    if [ -z "${INTEGRATION_SHARD_WEIGHTS[${key}]+set}" ]; then
      die "integration shard manifest is missing discovered package '${key}'"
    fi
  done

  for manifest_key in "${!INTEGRATION_SHARD_WEIGHTS[@]}"; do
    if [ -z "${discovered_run_packages[${manifest_key}]+set}" ]; then
      die "integration shard manifest names undiscovered or denylisted package '${manifest_key}'"
    fi
  done
  local isolated_key
  for isolated_key in ${INTEGRATION_ISOLATED_PACKAGE_KEYS}; do
    if [ -z "${discovered_run_packages[${isolated_key}]+set}" ]; then
      die "isolated integration package '${isolated_key}' matches no discovered package"
    fi
  done
  if [ "${#INTEGRATION_SHARD_WEIGHTS[@]}" -ne "${runnable_count}" ]; then
    die "integration shard manifest/package count mismatch after equality validation"
  fi
  if [ "${INTEGRATION_SHARD_COUNT}" -gt "${runnable_count}" ]; then
    die "integration shard manifest declares more shards than runnable packages"
  fi

  INTEGRATION_SHARD_BY_KEY=()
  INTEGRATION_SHARD_TOTALS=()
  INTEGRATION_SHARD_PACKAGE_COUNTS=()
  INTEGRATION_SHARD_NON_PROVIDER_COUNTS=()
  for ((shard = 1; shard <= INTEGRATION_SHARD_COUNT; shard++)); do
    INTEGRATION_SHARD_TOTALS[shard]=0
    INTEGRATION_SHARD_PACKAGE_COUNTS[shard]=0
    INTEGRATION_SHARD_NON_PROVIDER_COUNTS[shard]=0
  done

  local isolated closed_shards=" "
  while IFS=$'\t' read -r weight key; do
    isolated=0
    case "${INTEGRATION_ISOLATED_PACKAGE_KEYS}" in *" ${key} "*) isolated=1 ;; esac
    selected_shard=0
    selected_total=0
    for ((shard = 1; shard <= INTEGRATION_SHARD_COUNT; shard++)); do
      case "${closed_shards}" in *" ${shard} "*) continue ;; esac
      if [ "${isolated}" -eq 1 ] && [ "${INTEGRATION_SHARD_PACKAGE_COUNTS[${shard}]}" -ne 0 ]; then
        continue
      fi
      if [ "${selected_shard}" -eq 0 ] || [ "${INTEGRATION_SHARD_TOTALS[${shard}]}" -lt "${selected_total}" ]; then
        selected_shard="${shard}"
        selected_total="${INTEGRATION_SHARD_TOTALS[${shard}]}"
      fi
    done
    [ "${selected_shard}" -ne 0 ] \
      || die "integration shard plan has no shard left for '${key}'"
    if [ "${isolated}" -eq 1 ]; then
      closed_shards+="${selected_shard} "
    fi
    INTEGRATION_SHARD_BY_KEY["${key}"]="${selected_shard}"
    INTEGRATION_SHARD_TOTALS[selected_shard]=$((
      INTEGRATION_SHARD_TOTALS[selected_shard] + weight
    ))
    INTEGRATION_SHARD_PACKAGE_COUNTS[selected_shard]=$((
      INTEGRATION_SHARD_PACKAGE_COUNTS[selected_shard] + 1
    ))
    if [ "${key}" != "${PROVIDER_INTEGRATION_PACKAGE_KEY}" ] \
      && [ "${key}" != "${DAILY_INTEGRATION_PACKAGE_KEY}" ] \
      && [ "${key}" != "${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}" ] \
      && [ "${key}" != "${QAPISERVER_INTEGRATION_PACKAGE_KEY}" ] \
      && [ "${key}" != "${REMAINING_INTEGRATION_PACKAGE_KEY}" ]; then
      INTEGRATION_SHARD_NON_PROVIDER_COUNTS[selected_shard]=$((
        INTEGRATION_SHARD_NON_PROVIDER_COUNTS[selected_shard] + 1
      ))
    fi
  done < <(
    for key in "${!INTEGRATION_SHARD_WEIGHTS[@]}"; do
      printf '%s\t%s\n' "${INTEGRATION_SHARD_WEIGHTS[${key}]}" "${key}"
    done | LC_ALL=C sort -t $'\t' -k1,1nr -k2,2
  )

  printf 'integration shard plan: %d shard(s), %d package(s)\n' \
    "${INTEGRATION_SHARD_COUNT}" "${runnable_count}"
  for ((shard = 1; shard <= INTEGRATION_SHARD_COUNT; shard++)); do
    printf 'integration shard %d: estimated %ds, %d package(s)\n' \
      "${shard}" \
      "${INTEGRATION_SHARD_TOTALS[${shard}]}" \
      "${INTEGRATION_SHARD_PACKAGE_COUNTS[${shard}]}"
    for index in "${!INTEGRATION_PACKAGES[@]}"; do
      module_dir="${INTEGRATION_PACKAGE_MODULES[${index}]}"
      pkg="${INTEGRATION_PACKAGES[${index}]}"
      key="${module_dir}/${pkg#./}"
      key="${key#./}"
      if [ "${INTEGRATION_SHARD_BY_KEY[${key}]:-0}" -eq "${shard}" ]; then
        printf '  SHARD %d %s weight=%ss\n' \
          "${shard}" "${key}" "${INTEGRATION_SHARD_WEIGHTS[${key}]}"
      fi
    done
  done
}

# The providersync manifest owns only the number of test shards and the two
# source-derived relative weights. The complete test-name set always comes
# from the current Go package, so a checked-in list cannot silently drift.
load_providersync_test_shard_manifest() {
  local manifest="${DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST}"
  local line_number=0 key value extra
  local shards_seen=0 integration_weight_seen=0 ordinary_weight_seen=0

  [ -f "${manifest}" ] \
    || die "provider test shard manifest not found: ${manifest}"

  PROVIDER_TEST_SHARD_COUNT=0
  PROVIDER_INTEGRATION_TEST_WEIGHT=0
  PROVIDER_ORDINARY_TEST_WEIGHT=0
  while IFS=$'\t ' read -r key value extra; do
    line_number=$((line_number + 1))
    case "${key}" in
      ""|\#*) continue ;;
    esac
    if [ -n "${extra}" ]; then
      die "provider test shard manifest ${manifest}:${line_number} must contain exactly two fields"
    fi
    case "${value}" in
      ""|*[!0-9]*)
        die "provider test shard manifest ${manifest}:${line_number} has non-numeric value '${value}'"
        ;;
    esac
    if [ "${value}" -le 0 ]; then
      die "provider test shard manifest ${manifest}:${line_number} values must be positive"
    fi

    case "${key}" in
      shards)
        [ "${shards_seen}" -eq 0 ] \
          || die "provider test shard manifest declares 'shards' more than once"
        shards_seen=1
        PROVIDER_TEST_SHARD_COUNT="${value}"
        ;;
      integration-test-weight)
        [ "${integration_weight_seen}" -eq 0 ] \
          || die "provider test shard manifest declares 'integration-test-weight' more than once"
        integration_weight_seen=1
        PROVIDER_INTEGRATION_TEST_WEIGHT="${value}"
        ;;
      ordinary-test-weight)
        [ "${ordinary_weight_seen}" -eq 0 ] \
          || die "provider test shard manifest declares 'ordinary-test-weight' more than once"
        ordinary_weight_seen=1
        PROVIDER_ORDINARY_TEST_WEIGHT="${value}"
        ;;
      *)
        die "provider test shard manifest ${manifest}:${line_number} has unknown key '${key}'"
        ;;
    esac
  done < "${manifest}"

  [ "${shards_seen}" -eq 1 ] \
    || die "provider test shard manifest must declare one 'shards' row"
  [ "${integration_weight_seen}" -eq 1 ] \
    || die "provider test shard manifest must declare one 'integration-test-weight' row"
  [ "${ordinary_weight_seen}" -eq 1 ] \
    || die "provider test shard manifest must declare one 'ordinary-test-weight' row"
  [ "${PROVIDER_TEST_SHARD_COUNT}" -ge 2 ] \
    || die "provider test shard manifest must declare at least two shards"
}

# `go list` is the active-file authority for this runner's integration build.
# Reading the top-level test declarations from those files keeps shard planning
# independent of a second compile-heavy `go test -list` inside the Python xdist
# gate. The integration-vet and integration shard verbs still compile the same
# tagged package before execution.
discover_providersync_tests() {
  local files_output go_file source_file test_name integration_file
  declare -A discovered_names=()

  integration_test_files . "./${PROVIDER_INTEGRATION_PACKAGE_KEY}"
  files_output="${TEST_FILES_TAGGED[./${PROVIDER_INTEGRATION_PACKAGE_KEY}]-}"

  PROVIDER_TEST_NAMES=()
  PROVIDER_INTEGRATION_TEST_NAMES=()
  scan_listed_go_tests 'providersync' "${ROOT}/${PROVIDER_INTEGRATION_PACKAGE_KEY}" "${files_output}"
  PROVIDER_UNFILTERED="$(split_package_hook "${PROVIDER_INTEGRATION_PACKAGE_KEY}" "${files_output}")"
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    source_file="${ROOT}/${PROVIDER_INTEGRATION_PACKAGE_KEY}/${go_file}"
    integration_file=0
    if [ -n "${TEST_FILE_TAG_ONLY[${source_file}]+set}" ]; then
      integration_file=1
    fi
    while IFS= read -r test_name; do
      [ -n "${test_name}" ] || continue
      case "${test_name}" in
        Test*) go_test_name_ok "${test_name}" || die "providersync source discovery returned unsupported top-level test name '${test_name}'" ;;
        *) continue ;;
      esac
      [ -z "${discovered_names[${test_name}]+set}" ] \
        || die "providersync source discovery returned '${test_name}' more than once"
      discovered_names["${test_name}"]=1
      PROVIDER_TEST_NAMES+=("${test_name}")
      if [ "${integration_file}" -eq 1 ]; then
        PROVIDER_INTEGRATION_TEST_NAMES["${test_name}"]=1
      fi
    done <<<"${SCAN_TESTS[${source_file}]-}"
  done < <(printf '%s\n' "${files_output}")

  mapfile -t PROVIDER_TEST_NAMES < <(printf '%s\n' "${PROVIDER_TEST_NAMES[@]}" | LC_ALL=C sort)
  [ "${#PROVIDER_TEST_NAMES[@]}" -gt 0 ] \
    || die "providersync source discovery returned zero top-level tests"
  [ "${PROVIDER_TEST_SHARD_COUNT}" -le "${#PROVIDER_TEST_NAMES[@]}" ] \
    || die "provider test shard manifest declares more shards than discovered tests"
  [ "${#PROVIDER_INTEGRATION_TEST_NAMES[@]}" -gt 0 ] \
    || die "providersync source discovery returned zero integration-tagged top-level tests"

  load_providersync_measured_weights
  PROVIDER_TEST_WEIGHTS=()
  PROVIDER_TEST_CLASS=()
  for test_name in "${PROVIDER_TEST_NAMES[@]}"; do
    if [ -n "${PROVIDER_INTEGRATION_TEST_NAMES[${test_name}]+set}" ]; then
      PROVIDER_TEST_WEIGHTS["${test_name}"]="${PROVIDER_INTEGRATION_TEST_WEIGHT}"
      PROVIDER_TEST_CLASS["${test_name}"]="integration"
    else
      PROVIDER_TEST_WEIGHTS["${test_name}"]="${PROVIDER_ORDINARY_TEST_WEIGHT}"
      PROVIDER_TEST_CLASS["${test_name}"]="ordinary"
    fi
    if [ -n "${PROVIDER_MEASURED_WEIGHTS[${test_name}]+set}" ]; then
      PROVIDER_TEST_WEIGHTS["${test_name}"]="${PROVIDER_MEASURED_WEIGHTS[${test_name}]}"
    fi
  done
  for test_name in "${!PROVIDER_MEASURED_WEIGHTS[@]}"; do
    [ -n "${PROVIDER_TEST_CLASS[${test_name}]+set}" ] \
      || die "providersync test weights name '${test_name}', which is not a discovered top-level test: remove or rename the row in ${DEV_HEALTH_GO_PROVIDER_TEST_WEIGHTS}"
    split_test_runs_here "${PROVIDER_TEST_CLASS[${test_name}]}" "${PROVIDER_UNFILTERED}" \
      || die "providersync test weights name '${test_name}', an untagged test: ordinary tests do not run in shards (the go-quality test and race legs run them), so remove the row in ${DEV_HEALTH_GO_PROVIDER_TEST_WEIGHTS}"
  done
}

# ci/go_providersync_test_weights.tsv: `TestName<TAB>seconds` measured per-test wall time of the slow tests (CHAOS-11105).
# A listed test weighs its measured time in 10 ms units; an unlisted test keeps its class weight from the manifest.
load_providersync_measured_weights() {
  local weights="${DEV_HEALTH_GO_PROVIDER_TEST_WEIGHTS}"
  local line_number=0 line name seconds centis tab=$'\t'
  PROVIDER_MEASURED_WEIGHTS=()
  [ -f "${weights}" ] || die "provider test weights file not found: ${weights}"
  while IFS= read -r line || [ -n "${line}" ]; do
    line_number=$((line_number + 1))
    case "${line}" in
      ""|\#*) continue ;;
    esac
    # Split by hand: `read` with a tab IFS collapses consecutive tabs, so an empty field would pass as a missing one.
    case "${line}" in
      *"${tab}"*) ;;
      *) die "provider test weights ${weights}:${line_number} must contain exactly two tab-separated fields" ;;
    esac
    name="${line%%"${tab}"*}"
    seconds="${line#*"${tab}"}"
    case "${seconds}" in
      *"${tab}"*) die "provider test weights ${weights}:${line_number} must contain exactly two tab-separated fields" ;;
    esac
    case "${name}" in
      Test*) go_test_name_ok "${name}" || die "provider test weights ${weights}:${line_number} has unsupported test name '${name}'" ;;
      *) die "provider test weights ${weights}:${line_number} name '${name}' is not a top-level Test function" ;;
    esac
    [[ "${seconds}" =~ ^[0-9]+(\.[0-9]{1,2})?$ ]] \
      || die "provider test weights ${weights}:${line_number} seconds '${seconds}' must be a positive number with at most two decimals"
    centis="$(awk -v s="${seconds}" 'BEGIN { printf "%d", s * 100 + 0.5 }')"
    [ "${centis}" -gt 0 ] || die "provider test weights ${weights}:${line_number} seconds must be positive"
    [ -z "${PROVIDER_MEASURED_WEIGHTS[${name}]+set}" ] \
      || die "provider test weights ${weights}:${line_number} lists '${name}' more than once"
    PROVIDER_MEASURED_WEIGHTS["${name}"]="${centis}"
    if [ "${centis}" -gt "${DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET}" ]; then
      die "providersync test '${name}' measures ${seconds} s, over the shard budget of $((DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET / 100)) s (75% of the go test timeout); one test cannot be split, so speed it up"
    fi
  done < "${weights}"
}

# Deterministic longest-processing-time-first assignment balances the costly
# integration-tagged top-level tests first. Test names and lowest shard number
# are the stable tie-breakers, matching the package-level plan above.
plan_providersync_test_shards() {
  local test_name weight shard selected_shard selected_total

  load_providersync_test_shard_manifest
  discover_providersync_tests

  PROVIDER_TEST_SHARD_BY_NAME=()
  PROVIDER_TEST_SHARD_TOTALS=()
  PROVIDER_TEST_SHARD_COUNTS=()
  PROVIDER_TEST_SHARD_INTEGRATION_COUNTS=()
  for ((shard = 1; shard <= PROVIDER_TEST_SHARD_COUNT; shard++)); do
    PROVIDER_TEST_SHARD_TOTALS[shard]=0
    PROVIDER_TEST_SHARD_COUNTS[shard]=0
    PROVIDER_TEST_SHARD_INTEGRATION_COUNTS[shard]=0
  done

  while IFS=$'\t' read -r weight test_name; do
    selected_shard=1
    selected_total="${PROVIDER_TEST_SHARD_TOTALS[1]}"
    for ((shard = 2; shard <= PROVIDER_TEST_SHARD_COUNT; shard++)); do
      if [ "${PROVIDER_TEST_SHARD_TOTALS[${shard}]}" -lt "${selected_total}" ]; then
        selected_shard="${shard}"
        selected_total="${PROVIDER_TEST_SHARD_TOTALS[${shard}]}"
      fi
    done
    PROVIDER_TEST_SHARD_BY_NAME["${test_name}"]="${selected_shard}"
    PROVIDER_TEST_SHARD_TOTALS[selected_shard]=$((
      PROVIDER_TEST_SHARD_TOTALS[selected_shard] + weight
    ))
    PROVIDER_TEST_SHARD_COUNTS[selected_shard]=$((
      PROVIDER_TEST_SHARD_COUNTS[selected_shard] + 1
    ))
    if [ "${PROVIDER_TEST_CLASS[${test_name}]}" = "integration" ]; then
      PROVIDER_TEST_SHARD_INTEGRATION_COUNTS[selected_shard]=$((
        PROVIDER_TEST_SHARD_INTEGRATION_COUNTS[selected_shard] + 1
      ))
    fi
  done < <(
    for test_name in "${PROVIDER_TEST_NAMES[@]}"; do
      split_test_runs_here "${PROVIDER_TEST_CLASS[${test_name}]}" "${PROVIDER_UNFILTERED}" || continue
      printf '%s\t%s\n' "${PROVIDER_TEST_WEIGHTS[${test_name}]}" "${test_name}"
    done | LC_ALL=C sort -t $'\t' -k1,1nr -k2,2
  )

  for ((shard = 1; shard <= PROVIDER_TEST_SHARD_COUNT; shard++)); do
    if [ "${PROVIDER_TEST_SHARD_TOTALS[${shard}]}" -gt "${DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET}" ]; then
      die "providersync test shard ${shard} plans $((PROVIDER_TEST_SHARD_TOTALS[shard] / 100)) s, over the shard budget of $((DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET / 100)) s (75% of the 22 min go test timeout): raise the shard count in ${DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST} or refresh ${DEV_HEALTH_GO_PROVIDER_TEST_WEIGHTS}"
    fi
  done

  split_plan_scope "providersync" "${PROVIDER_UNFILTERED}" "${#PROVIDER_TEST_NAMES[@]}" "${#PROVIDER_INTEGRATION_TEST_NAMES[@]}" "${PROVIDER_TEST_SHARD_COUNT}" PROVIDER_TEST_SHARD_COUNTS
  printf 'providersync test plan: %d shard(s), %d top-level test(s), %d integration-tagged\n' \
    "${PROVIDER_TEST_SHARD_COUNT}" \
    "${#PROVIDER_TEST_NAMES[@]}" \
    "${#PROVIDER_INTEGRATION_TEST_NAMES[@]}"
  for ((shard = 1; shard <= PROVIDER_TEST_SHARD_COUNT; shard++)); do
    printf 'providersync test shard %d: relative weight %d, %d test(s), %d integration-tagged\n' \
      "${shard}" \
      "${PROVIDER_TEST_SHARD_TOTALS[${shard}]}" \
      "${PROVIDER_TEST_SHARD_COUNTS[${shard}]}" \
      "${PROVIDER_TEST_SHARD_INTEGRATION_COUNTS[${shard}]}"
    for test_name in "${PROVIDER_TEST_NAMES[@]}"; do
      if [ "${PROVIDER_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ]; then
        printf '  PROVIDER-SHARD %d %s weight=%d class=%s\n' \
          "${shard}" \
          "${test_name}" \
          "${PROVIDER_TEST_WEIGHTS[${test_name}]}" \
          "${PROVIDER_TEST_CLASS[${test_name}]}"
      fi
    done
  done
}

# The daily manifest owns the number of test shards and the two relative
# weights. The complete test-name set always comes from the current Go
# package (go list names the active files; their top-level Test functions are
# read from source), so a checked-in list cannot drift. Every discovered test
# is assigned to exactly one shard by construction; tests/tooling asserts the
# set equals `go test -list` of the package.
load_daily_test_shard_manifest() {
  local manifest="${DEV_HEALTH_GO_DAILY_TEST_SHARD_MANIFEST}"
  local line_number=0 key value extra
  local shards_seen=0 integration_weight_seen=0 ordinary_weight_seen=0

  [ -f "${manifest}" ] || die "daily test shard manifest not found: ${manifest}"

  DAILY_TEST_SHARD_COUNT=0
  DAILY_INTEGRATION_TEST_WEIGHT=0
  DAILY_ORDINARY_TEST_WEIGHT=0
  while IFS=$'\t ' read -r key value extra; do
    line_number=$((line_number + 1))
    case "${key}" in
      ""|\#*) continue ;;
    esac
    [ -z "${extra}" ] \
      || die "daily test shard manifest ${manifest}:${line_number} must contain exactly two fields"
    case "${value}" in
      ""|*[!0-9]*)
        die "daily test shard manifest ${manifest}:${line_number} has non-numeric value '${value}'"
        ;;
    esac
    [ "${value}" -gt 0 ] \
      || die "daily test shard manifest ${manifest}:${line_number} values must be positive"
    case "${key}" in
      shards)
        [ "${shards_seen}" -eq 0 ] || die "daily test shard manifest declares 'shards' more than once"
        shards_seen=1
        DAILY_TEST_SHARD_COUNT="${value}"
        ;;
      integration-test-weight)
        [ "${integration_weight_seen}" -eq 0 ] \
          || die "daily test shard manifest declares 'integration-test-weight' more than once"
        integration_weight_seen=1
        DAILY_INTEGRATION_TEST_WEIGHT="${value}"
        ;;
      ordinary-test-weight)
        [ "${ordinary_weight_seen}" -eq 0 ] \
          || die "daily test shard manifest declares 'ordinary-test-weight' more than once"
        ordinary_weight_seen=1
        DAILY_ORDINARY_TEST_WEIGHT="${value}"
        ;;
      *)
        die "daily test shard manifest ${manifest}:${line_number} has unknown key '${key}'"
        ;;
    esac
  done < "${manifest}"

  [ "${shards_seen}" -eq 1 ] || die "daily test shard manifest must declare one 'shards' row"
  [ "${integration_weight_seen}" -eq 1 ] \
    || die "daily test shard manifest must declare one 'integration-test-weight' row"
  [ "${ordinary_weight_seen}" -eq 1 ] \
    || die "daily test shard manifest must declare one 'ordinary-test-weight' row"
  [ "${DAILY_TEST_SHARD_COUNT}" -ge 2 ] \
    || die "daily test shard manifest must declare at least two shards"
}

discover_daily_tests() {
  local files_output go_file source_file test_name integration_file
  declare -A discovered_names=()

  integration_test_files . "./${DAILY_INTEGRATION_PACKAGE_KEY}"
  files_output="${TEST_FILES_TAGGED[./${DAILY_INTEGRATION_PACKAGE_KEY}]-}"

  DAILY_TEST_NAMES=()
  DAILY_INTEGRATION_TEST_NAMES=()
  scan_listed_go_tests 'daily' "${ROOT}/${DAILY_INTEGRATION_PACKAGE_KEY}" "${files_output}"
  DAILY_UNFILTERED="$(split_package_hook "${DAILY_INTEGRATION_PACKAGE_KEY}" "${files_output}")"
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    source_file="${ROOT}/${DAILY_INTEGRATION_PACKAGE_KEY}/${go_file}"
    integration_file=0
    if [ -n "${TEST_FILE_TAG_ONLY[${source_file}]+set}" ]; then
      integration_file=1
    fi
    while IFS= read -r test_name; do
      [ -n "${test_name}" ] || continue
      # TestMain is the process entry point, not a selectable test.
      [ "${test_name}" != "TestMain" ] || continue
      case "${test_name}" in
        Test*) go_test_name_ok "${test_name}" || die "daily source discovery returned unsupported top-level test name '${test_name}'" ;;
        *) continue ;;
      esac
      [ -z "${discovered_names[${test_name}]+set}" ] \
        || die "daily source discovery returned '${test_name}' more than once"
      discovered_names["${test_name}"]=1
      DAILY_TEST_NAMES+=("${test_name}")
      if [ "${integration_file}" -eq 1 ]; then
        DAILY_INTEGRATION_TEST_NAMES["${test_name}"]=1
      fi
    done <<<"${SCAN_TESTS[${source_file}]-}"
  done < <(printf '%s\n' "${files_output}")

  mapfile -t DAILY_TEST_NAMES < <(printf '%s\n' "${DAILY_TEST_NAMES[@]}" | LC_ALL=C sort)
  [ "${#DAILY_TEST_NAMES[@]}" -gt 0 ] || die "daily source discovery returned zero top-level tests"
  [ "${DAILY_TEST_SHARD_COUNT}" -le "${#DAILY_TEST_NAMES[@]}" ] \
    || die "daily test shard manifest declares more shards than discovered tests"
  [ "${#DAILY_INTEGRATION_TEST_NAMES[@]}" -gt 0 ] \
    || die "daily source discovery returned zero integration-tagged top-level tests"

  DAILY_TEST_CLASS=()
  for test_name in "${DAILY_TEST_NAMES[@]}"; do
    if [ -n "${DAILY_INTEGRATION_TEST_NAMES[${test_name}]+set}" ]; then
      DAILY_TEST_CLASS["${test_name}"]="integration"
    else
      DAILY_TEST_CLASS["${test_name}"]="ordinary"
    fi
  done
}

plan_daily_test_shards() {
  local test_name weight shard selected_shard selected_total

  load_daily_test_shard_manifest
  discover_daily_tests

  DAILY_TEST_SHARD_BY_NAME=()
  DAILY_TEST_SHARD_TOTALS=()
  DAILY_TEST_SHARD_COUNTS=()
  for ((shard = 1; shard <= DAILY_TEST_SHARD_COUNT; shard++)); do
    DAILY_TEST_SHARD_TOTALS[shard]=0
    DAILY_TEST_SHARD_COUNTS[shard]=0
  done

  while IFS=$'\t' read -r weight test_name; do
    selected_shard=1
    selected_total="${DAILY_TEST_SHARD_TOTALS[1]}"
    for ((shard = 2; shard <= DAILY_TEST_SHARD_COUNT; shard++)); do
      if [ "${DAILY_TEST_SHARD_TOTALS[${shard}]}" -lt "${selected_total}" ]; then
        selected_shard="${shard}"
        selected_total="${DAILY_TEST_SHARD_TOTALS[${shard}]}"
      fi
    done
    DAILY_TEST_SHARD_BY_NAME["${test_name}"]="${selected_shard}"
    DAILY_TEST_SHARD_TOTALS[selected_shard]=$((DAILY_TEST_SHARD_TOTALS[selected_shard] + weight))
    DAILY_TEST_SHARD_COUNTS[selected_shard]=$((DAILY_TEST_SHARD_COUNTS[selected_shard] + 1))
  done < <(
    for test_name in "${DAILY_TEST_NAMES[@]}"; do
      split_test_runs_here "${DAILY_TEST_CLASS[${test_name}]}" "${DAILY_UNFILTERED}" || continue
      if [ "${DAILY_TEST_CLASS[${test_name}]}" = "integration" ]; then
        weight="${DAILY_INTEGRATION_TEST_WEIGHT}"
      else
        weight="${DAILY_ORDINARY_TEST_WEIGHT}"
      fi
      printf '%s\t%s\n' "${weight}" "${test_name}"
    done | LC_ALL=C sort -t $'\t' -k1,1nr -k2,2
  )

  split_plan_scope "daily" "${DAILY_UNFILTERED}" "${#DAILY_TEST_NAMES[@]}" "${#DAILY_INTEGRATION_TEST_NAMES[@]}" "${DAILY_TEST_SHARD_COUNT}" DAILY_TEST_SHARD_COUNTS
  printf 'daily test plan: %d shard(s), %d top-level test(s), %d integration-tagged\n' \
    "${DAILY_TEST_SHARD_COUNT}" "${#DAILY_TEST_NAMES[@]}" "${#DAILY_INTEGRATION_TEST_NAMES[@]}"
  for ((shard = 1; shard <= DAILY_TEST_SHARD_COUNT; shard++)); do
    printf 'daily test shard %d: relative weight %d, %d test(s)\n' \
      "${shard}" "${DAILY_TEST_SHARD_TOTALS[${shard}]}" "${DAILY_TEST_SHARD_COUNTS[${shard}]}"
    for test_name in "${DAILY_TEST_NAMES[@]}"; do
      if [ "${DAILY_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ]; then
        printf '  DAILY-SHARD %d %s class=%s\n' "${shard}" "${test_name}" "${DAILY_TEST_CLASS[${test_name}]}"
      fi
    done
  done
}

check_daily_test_shard() {
  local shard="$1" mode="$2"
  local test_name separator="" test_regex='^('
  local selected_count=0

  if [ "${shard}" -lt 1 ] || [ "${shard}" -gt "${DAILY_TEST_SHARD_COUNT}" ]; then
    die "daily test shard ${shard} is outside 1..${DAILY_TEST_SHARD_COUNT}"
  fi

  for test_name in "${DAILY_TEST_NAMES[@]}"; do
    [ "${DAILY_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ] || continue
    printf '  DAILY-TEST-RUN %s\n' "${test_name}"
    test_regex+="${separator}${test_name}"
    separator="|"
    selected_count=$((selected_count + 1))
  done
  test_regex+=')$'

  [ "${selected_count}" -gt 0 ] || die "daily test shard ${shard} selected zero tests"
  if [ "${selected_count}" -ne "${DAILY_TEST_SHARD_COUNTS[${shard}]}" ]; then
    die "daily test shard ${shard} selected ${selected_count} tests but plan declared ${DAILY_TEST_SHARD_COUNTS[${shard}]}"
  fi
  if [ "${mode}" = "--dry-run" ]; then
    printf 'daily test shard %s: DRY RUN selected %d top-level test(s); no tests executed\n' \
      "${shard}" "${selected_count}"
    return 0
  fi

  printf 'go test daily test shard %s: %d top-level test(s)\n' "${shard}" "${selected_count}"
  local tmo
  tmo="$(job_go_test_timeout 22)" || return 2
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" -run "${test_regex}" "./${DAILY_INTEGRATION_PACKAGE_KEY}"
  )
}

# The workerservice manifest owns the number of test shards and the two relative
# weights. The complete test-name set always comes from the current Go
# package (go list names the active files; their top-level Test functions are
# read from source), so a checked-in list cannot drift. Every discovered test
# is assigned to exactly one shard by construction; tests/tooling asserts the
# set equals `go test -list` of the package.
load_workerservice_test_shard_manifest() {
  local manifest="${DEV_HEALTH_GO_WORKERSERVICE_TEST_SHARD_MANIFEST}"
  local line_number=0 key value extra
  local shards_seen=0 integration_weight_seen=0 ordinary_weight_seen=0

  [ -f "${manifest}" ] || die "workerservice test shard manifest not found: ${manifest}"

  WORKERSERVICE_TEST_SHARD_COUNT=0
  WORKERSERVICE_INTEGRATION_TEST_WEIGHT=0
  WORKERSERVICE_ORDINARY_TEST_WEIGHT=0
  while IFS=$'\t ' read -r key value extra; do
    line_number=$((line_number + 1))
    case "${key}" in
      ""|\#*) continue ;;
    esac
    [ -z "${extra}" ] \
      || die "workerservice test shard manifest ${manifest}:${line_number} must contain exactly two fields"
    case "${value}" in
      ""|*[!0-9]*)
        die "workerservice test shard manifest ${manifest}:${line_number} has non-numeric value '${value}'"
        ;;
    esac
    [ "${value}" -gt 0 ] \
      || die "workerservice test shard manifest ${manifest}:${line_number} values must be positive"
    case "${key}" in
      shards)
        [ "${shards_seen}" -eq 0 ] || die "workerservice test shard manifest declares 'shards' more than once"
        shards_seen=1
        WORKERSERVICE_TEST_SHARD_COUNT="${value}"
        ;;
      integration-test-weight)
        [ "${integration_weight_seen}" -eq 0 ] \
          || die "workerservice test shard manifest declares 'integration-test-weight' more than once"
        integration_weight_seen=1
        WORKERSERVICE_INTEGRATION_TEST_WEIGHT="${value}"
        ;;
      ordinary-test-weight)
        [ "${ordinary_weight_seen}" -eq 0 ] \
          || die "workerservice test shard manifest declares 'ordinary-test-weight' more than once"
        ordinary_weight_seen=1
        WORKERSERVICE_ORDINARY_TEST_WEIGHT="${value}"
        ;;
      *)
        die "workerservice test shard manifest ${manifest}:${line_number} has unknown key '${key}'"
        ;;
    esac
  done < "${manifest}"

  [ "${shards_seen}" -eq 1 ] || die "workerservice test shard manifest must declare one 'shards' row"
  [ "${integration_weight_seen}" -eq 1 ] \
    || die "workerservice test shard manifest must declare one 'integration-test-weight' row"
  [ "${ordinary_weight_seen}" -eq 1 ] \
    || die "workerservice test shard manifest must declare one 'ordinary-test-weight' row"
  [ "${WORKERSERVICE_TEST_SHARD_COUNT}" -ge 2 ] \
    || die "workerservice test shard manifest must declare at least two shards"
}

discover_workerservice_tests() {
  local files_output go_file source_file test_name integration_file
  declare -A discovered_names=()

  integration_test_files . "./${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}"
  files_output="${TEST_FILES_TAGGED[./${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}]-}"

  WORKERSERVICE_TEST_NAMES=()
  WORKERSERVICE_INTEGRATION_TEST_NAMES=()
  scan_listed_go_tests 'workerservice' "${ROOT}/${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}" "${files_output}"
  WORKERSERVICE_UNFILTERED="$(split_package_hook "${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}" "${files_output}")"
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    source_file="${ROOT}/${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}/${go_file}"
    integration_file=0
    if [ -n "${TEST_FILE_TAG_ONLY[${source_file}]+set}" ]; then
      integration_file=1
    fi
    while IFS= read -r test_name; do
      [ -n "${test_name}" ] || continue
      # TestMain is the process entry point, not a selectable test.
      [ "${test_name}" != "TestMain" ] || continue
      case "${test_name}" in
        Test*) go_test_name_ok "${test_name}" || die "workerservice source discovery returned unsupported top-level test name '${test_name}'" ;;
        *) continue ;;
      esac
      [ -z "${discovered_names[${test_name}]+set}" ] \
        || die "workerservice source discovery returned '${test_name}' more than once"
      discovered_names["${test_name}"]=1
      WORKERSERVICE_TEST_NAMES+=("${test_name}")
      if [ "${integration_file}" -eq 1 ]; then
        WORKERSERVICE_INTEGRATION_TEST_NAMES["${test_name}"]=1
      fi
    done <<<"${SCAN_TESTS[${source_file}]-}"
  done < <(printf '%s\n' "${files_output}")

  mapfile -t WORKERSERVICE_TEST_NAMES < <(printf '%s\n' "${WORKERSERVICE_TEST_NAMES[@]}" | LC_ALL=C sort)
  [ "${#WORKERSERVICE_TEST_NAMES[@]}" -gt 0 ] || die "workerservice source discovery returned zero top-level tests"
  [ "${WORKERSERVICE_TEST_SHARD_COUNT}" -le "${#WORKERSERVICE_TEST_NAMES[@]}" ] \
    || die "workerservice test shard manifest declares more shards than discovered tests"
  [ "${#WORKERSERVICE_INTEGRATION_TEST_NAMES[@]}" -gt 0 ] \
    || die "workerservice source discovery returned zero integration-tagged top-level tests"

  WORKERSERVICE_TEST_CLASS=()
  for test_name in "${WORKERSERVICE_TEST_NAMES[@]}"; do
    if [ -n "${WORKERSERVICE_INTEGRATION_TEST_NAMES[${test_name}]+set}" ]; then
      WORKERSERVICE_TEST_CLASS["${test_name}"]="integration"
    else
      WORKERSERVICE_TEST_CLASS["${test_name}"]="ordinary"
    fi
  done
}

plan_workerservice_test_shards() {
  local test_name weight shard selected_shard selected_total

  load_workerservice_test_shard_manifest
  discover_workerservice_tests

  WORKERSERVICE_TEST_SHARD_BY_NAME=()
  WORKERSERVICE_TEST_SHARD_TOTALS=()
  WORKERSERVICE_TEST_SHARD_COUNTS=()
  for ((shard = 1; shard <= WORKERSERVICE_TEST_SHARD_COUNT; shard++)); do
    WORKERSERVICE_TEST_SHARD_TOTALS[shard]=0
    WORKERSERVICE_TEST_SHARD_COUNTS[shard]=0
  done

  while IFS=$'\t' read -r weight test_name; do
    selected_shard=1
    selected_total="${WORKERSERVICE_TEST_SHARD_TOTALS[1]}"
    for ((shard = 2; shard <= WORKERSERVICE_TEST_SHARD_COUNT; shard++)); do
      if [ "${WORKERSERVICE_TEST_SHARD_TOTALS[${shard}]}" -lt "${selected_total}" ]; then
        selected_shard="${shard}"
        selected_total="${WORKERSERVICE_TEST_SHARD_TOTALS[${shard}]}"
      fi
    done
    WORKERSERVICE_TEST_SHARD_BY_NAME["${test_name}"]="${selected_shard}"
    WORKERSERVICE_TEST_SHARD_TOTALS[selected_shard]=$((WORKERSERVICE_TEST_SHARD_TOTALS[selected_shard] + weight))
    WORKERSERVICE_TEST_SHARD_COUNTS[selected_shard]=$((WORKERSERVICE_TEST_SHARD_COUNTS[selected_shard] + 1))
  done < <(
    for test_name in "${WORKERSERVICE_TEST_NAMES[@]}"; do
      split_test_runs_here "${WORKERSERVICE_TEST_CLASS[${test_name}]}" "${WORKERSERVICE_UNFILTERED}" || continue
      if [ "${WORKERSERVICE_TEST_CLASS[${test_name}]}" = "integration" ]; then
        weight="${WORKERSERVICE_INTEGRATION_TEST_WEIGHT}"
      else
        weight="${WORKERSERVICE_ORDINARY_TEST_WEIGHT}"
      fi
      printf '%s\t%s\n' "${weight}" "${test_name}"
    done | LC_ALL=C sort -t $'\t' -k1,1nr -k2,2
  )

  split_plan_scope "workerservice" "${WORKERSERVICE_UNFILTERED}" "${#WORKERSERVICE_TEST_NAMES[@]}" "${#WORKERSERVICE_INTEGRATION_TEST_NAMES[@]}" "${WORKERSERVICE_TEST_SHARD_COUNT}" WORKERSERVICE_TEST_SHARD_COUNTS
  printf 'workerservice test plan: %d shard(s), %d top-level test(s), %d integration-tagged\n' \
    "${WORKERSERVICE_TEST_SHARD_COUNT}" "${#WORKERSERVICE_TEST_NAMES[@]}" "${#WORKERSERVICE_INTEGRATION_TEST_NAMES[@]}"
  for ((shard = 1; shard <= WORKERSERVICE_TEST_SHARD_COUNT; shard++)); do
    printf 'workerservice test shard %d: relative weight %d, %d test(s)\n' \
      "${shard}" "${WORKERSERVICE_TEST_SHARD_TOTALS[${shard}]}" "${WORKERSERVICE_TEST_SHARD_COUNTS[${shard}]}"
    for test_name in "${WORKERSERVICE_TEST_NAMES[@]}"; do
      if [ "${WORKERSERVICE_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ]; then
        printf '  WORKERSERVICE-SHARD %d %s class=%s\n' "${shard}" "${test_name}" "${WORKERSERVICE_TEST_CLASS[${test_name}]}"
      fi
    done
  done
}

check_workerservice_test_shard() {
  local shard="$1" mode="$2"
  local test_name separator="" test_regex='^('
  local selected_count=0

  if [ "${shard}" -lt 1 ] || [ "${shard}" -gt "${WORKERSERVICE_TEST_SHARD_COUNT}" ]; then
    die "workerservice test shard ${shard} is outside 1..${WORKERSERVICE_TEST_SHARD_COUNT}"
  fi

  for test_name in "${WORKERSERVICE_TEST_NAMES[@]}"; do
    [ "${WORKERSERVICE_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ] || continue
    printf '  WORKERSERVICE-TEST-RUN %s\n' "${test_name}"
    test_regex+="${separator}${test_name}"
    separator="|"
    selected_count=$((selected_count + 1))
  done
  test_regex+=')$'

  [ "${selected_count}" -gt 0 ] || die "workerservice test shard ${shard} selected zero tests"
  if [ "${selected_count}" -ne "${WORKERSERVICE_TEST_SHARD_COUNTS[${shard}]}" ]; then
    die "workerservice test shard ${shard} selected ${selected_count} tests but plan declared ${WORKERSERVICE_TEST_SHARD_COUNTS[${shard}]}"
  fi
  if [ "${mode}" = "--dry-run" ]; then
    printf 'workerservice test shard %s: DRY RUN selected %d top-level test(s); no tests executed\n' \
      "${shard}" "${selected_count}"
    return 0
  fi

  printf 'go test workerservice test shard %s: %d top-level test(s)\n' "${shard}" "${selected_count}"
  local tmo
  tmo="$(job_go_test_timeout 22)" || return 2
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" -run "${test_regex}" "./${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}"
  )
}

# The queryapiserver manifest owns the number of test shards and the two relative
# weights. The complete test-name set always comes from the current Go
# package (go list names the active files; their top-level Test functions are
# read from source), so a checked-in list cannot drift. Every discovered test
# is assigned to exactly one shard by construction; tests/tooling asserts the
# set equals `go test -list` of the package.
load_queryapiserver_test_shard_manifest() {
  local manifest="${DEV_HEALTH_GO_QAPISERVER_TEST_SHARD_MANIFEST}"
  local line_number=0 key value extra
  local shards_seen=0 integration_weight_seen=0 ordinary_weight_seen=0

  [ -f "${manifest}" ] || die "queryapiserver test shard manifest not found: ${manifest}"

  QAPISERVER_TEST_SHARD_COUNT=0
  QAPISERVER_INTEGRATION_TEST_WEIGHT=0
  QAPISERVER_ORDINARY_TEST_WEIGHT=0
  while IFS=$'\t ' read -r key value extra; do
    line_number=$((line_number + 1))
    case "${key}" in
      ""|\#*) continue ;;
    esac
    [ -z "${extra}" ] \
      || die "queryapiserver test shard manifest ${manifest}:${line_number} must contain exactly two fields"
    case "${value}" in
      ""|*[!0-9]*)
        die "queryapiserver test shard manifest ${manifest}:${line_number} has non-numeric value '${value}'"
        ;;
    esac
    [ "${value}" -gt 0 ] \
      || die "queryapiserver test shard manifest ${manifest}:${line_number} values must be positive"
    case "${key}" in
      shards)
        [ "${shards_seen}" -eq 0 ] || die "queryapiserver test shard manifest declares 'shards' more than once"
        shards_seen=1
        QAPISERVER_TEST_SHARD_COUNT="${value}"
        ;;
      integration-test-weight)
        [ "${integration_weight_seen}" -eq 0 ] \
          || die "queryapiserver test shard manifest declares 'integration-test-weight' more than once"
        integration_weight_seen=1
        QAPISERVER_INTEGRATION_TEST_WEIGHT="${value}"
        ;;
      ordinary-test-weight)
        [ "${ordinary_weight_seen}" -eq 0 ] \
          || die "queryapiserver test shard manifest declares 'ordinary-test-weight' more than once"
        ordinary_weight_seen=1
        QAPISERVER_ORDINARY_TEST_WEIGHT="${value}"
        ;;
      *)
        die "queryapiserver test shard manifest ${manifest}:${line_number} has unknown key '${key}'"
        ;;
    esac
  done < "${manifest}"

  [ "${shards_seen}" -eq 1 ] || die "queryapiserver test shard manifest must declare one 'shards' row"
  [ "${integration_weight_seen}" -eq 1 ] \
    || die "queryapiserver test shard manifest must declare one 'integration-test-weight' row"
  [ "${ordinary_weight_seen}" -eq 1 ] \
    || die "queryapiserver test shard manifest must declare one 'ordinary-test-weight' row"
  [ "${QAPISERVER_TEST_SHARD_COUNT}" -ge 2 ] \
    || die "queryapiserver test shard manifest must declare at least two shards"
}

discover_queryapiserver_tests() {
  local files_output go_file source_file test_name integration_file
  declare -A discovered_names=()

  integration_test_files . "./${QAPISERVER_INTEGRATION_PACKAGE_KEY}"
  files_output="${TEST_FILES_TAGGED[./${QAPISERVER_INTEGRATION_PACKAGE_KEY}]-}"

  QAPISERVER_TEST_NAMES=()
  QAPISERVER_INTEGRATION_TEST_NAMES=()
  scan_listed_go_tests 'queryapiserver' "${ROOT}/${QAPISERVER_INTEGRATION_PACKAGE_KEY}" "${files_output}"
  QAPISERVER_UNFILTERED="$(split_package_hook "${QAPISERVER_INTEGRATION_PACKAGE_KEY}" "${files_output}")"
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    source_file="${ROOT}/${QAPISERVER_INTEGRATION_PACKAGE_KEY}/${go_file}"
    integration_file=0
    if [ -n "${TEST_FILE_TAG_ONLY[${source_file}]+set}" ]; then
      integration_file=1
    fi
    while IFS= read -r test_name; do
      [ -n "${test_name}" ] || continue
      # TestMain is the process entry point, not a selectable test.
      [ "${test_name}" != "TestMain" ] || continue
      case "${test_name}" in
        Test*) go_test_name_ok "${test_name}" || die "queryapiserver source discovery returned unsupported top-level test name '${test_name}'" ;;
        *) continue ;;
      esac
      [ -z "${discovered_names[${test_name}]+set}" ] \
        || die "queryapiserver source discovery returned '${test_name}' more than once"
      discovered_names["${test_name}"]=1
      QAPISERVER_TEST_NAMES+=("${test_name}")
      if [ "${integration_file}" -eq 1 ]; then
        QAPISERVER_INTEGRATION_TEST_NAMES["${test_name}"]=1
      fi
    done <<<"${SCAN_TESTS[${source_file}]-}"
  done < <(printf '%s\n' "${files_output}")

  mapfile -t QAPISERVER_TEST_NAMES < <(printf '%s\n' "${QAPISERVER_TEST_NAMES[@]}" | LC_ALL=C sort)
  [ "${#QAPISERVER_TEST_NAMES[@]}" -gt 0 ] || die "queryapiserver source discovery returned zero top-level tests"
  [ "${QAPISERVER_TEST_SHARD_COUNT}" -le "${#QAPISERVER_TEST_NAMES[@]}" ] \
    || die "queryapiserver test shard manifest declares more shards than discovered tests"
  [ "${#QAPISERVER_INTEGRATION_TEST_NAMES[@]}" -gt 0 ] \
    || die "queryapiserver source discovery returned zero integration-tagged top-level tests"

  QAPISERVER_TEST_CLASS=()
  for test_name in "${QAPISERVER_TEST_NAMES[@]}"; do
    if [ -n "${QAPISERVER_INTEGRATION_TEST_NAMES[${test_name}]+set}" ]; then
      QAPISERVER_TEST_CLASS["${test_name}"]="integration"
    else
      QAPISERVER_TEST_CLASS["${test_name}"]="ordinary"
    fi
  done
}

plan_queryapiserver_test_shards() {
  local test_name weight shard selected_shard selected_total

  load_queryapiserver_test_shard_manifest
  discover_queryapiserver_tests

  QAPISERVER_TEST_SHARD_BY_NAME=()
  QAPISERVER_TEST_SHARD_TOTALS=()
  QAPISERVER_TEST_SHARD_COUNTS=()
  for ((shard = 1; shard <= QAPISERVER_TEST_SHARD_COUNT; shard++)); do
    QAPISERVER_TEST_SHARD_TOTALS[shard]=0
    QAPISERVER_TEST_SHARD_COUNTS[shard]=0
  done

  while IFS=$'\t' read -r weight test_name; do
    selected_shard=1
    selected_total="${QAPISERVER_TEST_SHARD_TOTALS[1]}"
    for ((shard = 2; shard <= QAPISERVER_TEST_SHARD_COUNT; shard++)); do
      if [ "${QAPISERVER_TEST_SHARD_TOTALS[${shard}]}" -lt "${selected_total}" ]; then
        selected_shard="${shard}"
        selected_total="${QAPISERVER_TEST_SHARD_TOTALS[${shard}]}"
      fi
    done
    QAPISERVER_TEST_SHARD_BY_NAME["${test_name}"]="${selected_shard}"
    QAPISERVER_TEST_SHARD_TOTALS[selected_shard]=$((QAPISERVER_TEST_SHARD_TOTALS[selected_shard] + weight))
    QAPISERVER_TEST_SHARD_COUNTS[selected_shard]=$((QAPISERVER_TEST_SHARD_COUNTS[selected_shard] + 1))
  done < <(
    for test_name in "${QAPISERVER_TEST_NAMES[@]}"; do
      split_test_runs_here "${QAPISERVER_TEST_CLASS[${test_name}]}" "${QAPISERVER_UNFILTERED}" || continue
      if [ "${QAPISERVER_TEST_CLASS[${test_name}]}" = "integration" ]; then
        weight="${QAPISERVER_INTEGRATION_TEST_WEIGHT}"
      else
        weight="${QAPISERVER_ORDINARY_TEST_WEIGHT}"
      fi
      printf '%s\t%s\n' "${weight}" "${test_name}"
    done | LC_ALL=C sort -t $'\t' -k1,1nr -k2,2
  )

  split_plan_scope "queryapiserver" "${QAPISERVER_UNFILTERED}" "${#QAPISERVER_TEST_NAMES[@]}" "${#QAPISERVER_INTEGRATION_TEST_NAMES[@]}" "${QAPISERVER_TEST_SHARD_COUNT}" QAPISERVER_TEST_SHARD_COUNTS
  printf 'queryapiserver test plan: %d shard(s), %d top-level test(s), %d integration-tagged\n' \
    "${QAPISERVER_TEST_SHARD_COUNT}" "${#QAPISERVER_TEST_NAMES[@]}" "${#QAPISERVER_INTEGRATION_TEST_NAMES[@]}"
  for ((shard = 1; shard <= QAPISERVER_TEST_SHARD_COUNT; shard++)); do
    printf 'queryapiserver test shard %d: relative weight %d, %d test(s)\n' \
      "${shard}" "${QAPISERVER_TEST_SHARD_TOTALS[${shard}]}" "${QAPISERVER_TEST_SHARD_COUNTS[${shard}]}"
    for test_name in "${QAPISERVER_TEST_NAMES[@]}"; do
      if [ "${QAPISERVER_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ]; then
        printf '  QAPISERVER-SHARD %d %s class=%s\n' "${shard}" "${test_name}" "${QAPISERVER_TEST_CLASS[${test_name}]}"
      fi
    done
  done
}

check_queryapiserver_test_shard() {
  local shard="$1" mode="$2"
  local test_name separator="" test_regex='^('
  local selected_count=0

  if [ "${shard}" -lt 1 ] || [ "${shard}" -gt "${QAPISERVER_TEST_SHARD_COUNT}" ]; then
    die "queryapiserver test shard ${shard} is outside 1..${QAPISERVER_TEST_SHARD_COUNT}"
  fi

  for test_name in "${QAPISERVER_TEST_NAMES[@]}"; do
    [ "${QAPISERVER_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ] || continue
    printf '  QAPISERVER-TEST-RUN %s\n' "${test_name}"
    test_regex+="${separator}${test_name}"
    separator="|"
    selected_count=$((selected_count + 1))
  done
  test_regex+=')$'

  [ "${selected_count}" -gt 0 ] || die "queryapiserver test shard ${shard} selected zero tests"
  if [ "${selected_count}" -ne "${QAPISERVER_TEST_SHARD_COUNTS[${shard}]}" ]; then
    die "queryapiserver test shard ${shard} selected ${selected_count} tests but plan declared ${QAPISERVER_TEST_SHARD_COUNTS[${shard}]}"
  fi
  if [ "${mode}" = "--dry-run" ]; then
    printf 'queryapiserver test shard %s: DRY RUN selected %d top-level test(s); no tests executed\n' \
      "${shard}" "${selected_count}"
    return 0
  fi

  printf 'go test queryapiserver test shard %s: %d top-level test(s)\n' "${shard}" "${selected_count}"
  local tmo
  tmo="$(job_go_test_timeout 22)" || return 2
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" -run "${test_regex}" "./${QAPISERVER_INTEGRATION_PACKAGE_KEY}"
  )
}

# The internal/jobs/metrics/remaining manifest owns the number of test shards and the two relative
# weights. The complete test-name set always comes from the current Go
# package (go list names the active files; their top-level Test functions are
# read from source), so a checked-in list cannot drift. Every discovered test
# is assigned to exactly one shard by construction; tests/tooling asserts the
# set equals `go test -list` of the package.
load_remaining_test_shard_manifest() {
  local manifest="${DEV_HEALTH_GO_REMAINING_TEST_SHARD_MANIFEST}"
  local line_number=0 key value extra
  local shards_seen=0 integration_weight_seen=0 ordinary_weight_seen=0

  [ -f "${manifest}" ] || die "remaining test shard manifest not found: ${manifest}"

  REMAINING_TEST_SHARD_COUNT=0
  REMAINING_INTEGRATION_TEST_WEIGHT=0
  REMAINING_ORDINARY_TEST_WEIGHT=0
  while IFS=$'\t ' read -r key value extra; do
    line_number=$((line_number + 1))
    case "${key}" in
      ""|\#*) continue ;;
    esac
    [ -z "${extra}" ] \
      || die "remaining test shard manifest ${manifest}:${line_number} must contain exactly two fields"
    case "${value}" in
      ""|*[!0-9]*)
        die "remaining test shard manifest ${manifest}:${line_number} has non-numeric value '${value}'"
        ;;
    esac
    [ "${value}" -gt 0 ] \
      || die "remaining test shard manifest ${manifest}:${line_number} values must be positive"
    case "${key}" in
      shards)
        [ "${shards_seen}" -eq 0 ] || die "remaining test shard manifest declares 'shards' more than once"
        shards_seen=1
        REMAINING_TEST_SHARD_COUNT="${value}"
        ;;
      integration-test-weight)
        [ "${integration_weight_seen}" -eq 0 ] \
          || die "remaining test shard manifest declares 'integration-test-weight' more than once"
        integration_weight_seen=1
        REMAINING_INTEGRATION_TEST_WEIGHT="${value}"
        ;;
      ordinary-test-weight)
        [ "${ordinary_weight_seen}" -eq 0 ] \
          || die "remaining test shard manifest declares 'ordinary-test-weight' more than once"
        ordinary_weight_seen=1
        REMAINING_ORDINARY_TEST_WEIGHT="${value}"
        ;;
      *)
        die "remaining test shard manifest ${manifest}:${line_number} has unknown key '${key}'"
        ;;
    esac
  done < "${manifest}"

  [ "${shards_seen}" -eq 1 ] || die "remaining test shard manifest must declare one 'shards' row"
  [ "${integration_weight_seen}" -eq 1 ] \
    || die "remaining test shard manifest must declare one 'integration-test-weight' row"
  [ "${ordinary_weight_seen}" -eq 1 ] \
    || die "remaining test shard manifest must declare one 'ordinary-test-weight' row"
  [ "${REMAINING_TEST_SHARD_COUNT}" -ge 2 ] \
    || die "remaining test shard manifest must declare at least two shards"
}

discover_remaining_tests() {
  local files_output go_file source_file test_name integration_file
  declare -A discovered_names=()

  integration_test_files . "./${REMAINING_INTEGRATION_PACKAGE_KEY}"
  files_output="${TEST_FILES_TAGGED[./${REMAINING_INTEGRATION_PACKAGE_KEY}]-}"

  REMAINING_TEST_NAMES=()
  REMAINING_INTEGRATION_TEST_NAMES=()
  scan_listed_go_tests 'remaining' "${ROOT}/${REMAINING_INTEGRATION_PACKAGE_KEY}" "${files_output}"
  REMAINING_UNFILTERED="$(split_package_hook "${REMAINING_INTEGRATION_PACKAGE_KEY}" "${files_output}")"
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    source_file="${ROOT}/${REMAINING_INTEGRATION_PACKAGE_KEY}/${go_file}"
    integration_file=0
    if [ -n "${TEST_FILE_TAG_ONLY[${source_file}]+set}" ]; then
      integration_file=1
    fi
    while IFS= read -r test_name; do
      [ -n "${test_name}" ] || continue
      # TestMain is the process entry point, not a selectable test.
      [ "${test_name}" != "TestMain" ] || continue
      case "${test_name}" in
        Test*) go_test_name_ok "${test_name}" || die "remaining source discovery returned unsupported top-level test name '${test_name}'" ;;
        *) continue ;;
      esac
      [ -z "${discovered_names[${test_name}]+set}" ] \
        || die "remaining source discovery returned '${test_name}' more than once"
      discovered_names["${test_name}"]=1
      REMAINING_TEST_NAMES+=("${test_name}")
      if [ "${integration_file}" -eq 1 ]; then
        REMAINING_INTEGRATION_TEST_NAMES["${test_name}"]=1
      fi
    done <<<"${SCAN_TESTS[${source_file}]-}"
  done < <(printf '%s\n' "${files_output}")

  mapfile -t REMAINING_TEST_NAMES < <(printf '%s\n' "${REMAINING_TEST_NAMES[@]}" | LC_ALL=C sort)
  [ "${#REMAINING_TEST_NAMES[@]}" -gt 0 ] || die "remaining source discovery returned zero top-level tests"
  [ "${REMAINING_TEST_SHARD_COUNT}" -le "${#REMAINING_TEST_NAMES[@]}" ] \
    || die "remaining test shard manifest declares more shards than discovered tests"
  [ "${#REMAINING_INTEGRATION_TEST_NAMES[@]}" -gt 0 ] \
    || die "remaining source discovery returned zero integration-tagged top-level tests"

  REMAINING_TEST_CLASS=()
  for test_name in "${REMAINING_TEST_NAMES[@]}"; do
    if [ -n "${REMAINING_INTEGRATION_TEST_NAMES[${test_name}]+set}" ]; then
      REMAINING_TEST_CLASS["${test_name}"]="integration"
    else
      REMAINING_TEST_CLASS["${test_name}"]="ordinary"
    fi
  done
}

plan_remaining_test_shards() {
  local test_name weight shard selected_shard selected_total

  load_remaining_test_shard_manifest
  discover_remaining_tests

  REMAINING_TEST_SHARD_BY_NAME=()
  REMAINING_TEST_SHARD_TOTALS=()
  REMAINING_TEST_SHARD_COUNTS=()
  for ((shard = 1; shard <= REMAINING_TEST_SHARD_COUNT; shard++)); do
    REMAINING_TEST_SHARD_TOTALS[shard]=0
    REMAINING_TEST_SHARD_COUNTS[shard]=0
  done

  while IFS=$'\t' read -r weight test_name; do
    selected_shard=1
    selected_total="${REMAINING_TEST_SHARD_TOTALS[1]}"
    for ((shard = 2; shard <= REMAINING_TEST_SHARD_COUNT; shard++)); do
      if [ "${REMAINING_TEST_SHARD_TOTALS[${shard}]}" -lt "${selected_total}" ]; then
        selected_shard="${shard}"
        selected_total="${REMAINING_TEST_SHARD_TOTALS[${shard}]}"
      fi
    done
    REMAINING_TEST_SHARD_BY_NAME["${test_name}"]="${selected_shard}"
    REMAINING_TEST_SHARD_TOTALS[selected_shard]=$((REMAINING_TEST_SHARD_TOTALS[selected_shard] + weight))
    REMAINING_TEST_SHARD_COUNTS[selected_shard]=$((REMAINING_TEST_SHARD_COUNTS[selected_shard] + 1))
  done < <(
    for test_name in "${REMAINING_TEST_NAMES[@]}"; do
      split_test_runs_here "${REMAINING_TEST_CLASS[${test_name}]}" "${REMAINING_UNFILTERED}" || continue
      if [ "${REMAINING_TEST_CLASS[${test_name}]}" = "integration" ]; then
        weight="${REMAINING_INTEGRATION_TEST_WEIGHT}"
      else
        weight="${REMAINING_ORDINARY_TEST_WEIGHT}"
      fi
      printf '%s\t%s\n' "${weight}" "${test_name}"
    done | LC_ALL=C sort -t $'\t' -k1,1nr -k2,2
  )

  split_plan_scope "remaining" "${REMAINING_UNFILTERED}" "${#REMAINING_TEST_NAMES[@]}" "${#REMAINING_INTEGRATION_TEST_NAMES[@]}" "${REMAINING_TEST_SHARD_COUNT}" REMAINING_TEST_SHARD_COUNTS
  printf 'remaining test plan: %d shard(s), %d top-level test(s), %d integration-tagged\n' \
    "${REMAINING_TEST_SHARD_COUNT}" "${#REMAINING_TEST_NAMES[@]}" "${#REMAINING_INTEGRATION_TEST_NAMES[@]}"
  for ((shard = 1; shard <= REMAINING_TEST_SHARD_COUNT; shard++)); do
    printf 'remaining test shard %d: relative weight %d, %d test(s)\n' \
      "${shard}" "${REMAINING_TEST_SHARD_TOTALS[${shard}]}" "${REMAINING_TEST_SHARD_COUNTS[${shard}]}"
    for test_name in "${REMAINING_TEST_NAMES[@]}"; do
      if [ "${REMAINING_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ]; then
        printf '  REMAINING-SHARD %d %s class=%s\n' "${shard}" "${test_name}" "${REMAINING_TEST_CLASS[${test_name}]}"
      fi
    done
  done
}

check_remaining_test_shard() {
  local shard="$1" mode="$2"
  local test_name separator="" test_regex='^('
  local selected_count=0

  if [ "${shard}" -lt 1 ] || [ "${shard}" -gt "${REMAINING_TEST_SHARD_COUNT}" ]; then
    die "remaining test shard ${shard} is outside 1..${REMAINING_TEST_SHARD_COUNT}"
  fi

  for test_name in "${REMAINING_TEST_NAMES[@]}"; do
    [ "${REMAINING_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ] || continue
    printf '  REMAINING-TEST-RUN %s\n' "${test_name}"
    test_regex+="${separator}${test_name}"
    separator="|"
    selected_count=$((selected_count + 1))
  done
  test_regex+=')$'

  [ "${selected_count}" -gt 0 ] || die "remaining test shard ${shard} selected zero tests"
  if [ "${selected_count}" -ne "${REMAINING_TEST_SHARD_COUNTS[${shard}]}" ]; then
    die "remaining test shard ${shard} selected ${selected_count} tests but plan declared ${REMAINING_TEST_SHARD_COUNTS[${shard}]}"
  fi
  if [ "${mode}" = "--dry-run" ]; then
    printf 'remaining test shard %s: DRY RUN selected %d top-level test(s); no tests executed\n' \
      "${shard}" "${selected_count}"
    return 0
  fi

  printf 'go test remaining test shard %s: %d top-level test(s)\n' "${shard}" "${selected_count}"
  local tmo
  tmo="$(job_go_test_timeout 22)" || return 2
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" -run "${test_regex}" "./${REMAINING_INTEGRATION_PACKAGE_KEY}"
  )
}

# load_affected_selection reads INTEGRATION_AFFECTED_FILE, the output of
# cmd/cigate plan-verdict (CHAOS-9947). Unset or empty, or a FULL first line, means the
# whole suite; NONE means no package; any other first line than "AFFECTED <n>" fails. An AFFECTED key that is not a
# planned package is a failure, never an ignored line.
load_affected_selection() {
  AFFECTED_FILTER_ACTIVE=0
  AFFECTED_KEYS=()
  local file="${INTEGRATION_AFFECTED_FILE:-}" first line count=0 declared
  if [ -z "${file}" ]; then
    return 0
  fi
  [ -f "${file}" ] || die "INTEGRATION_AFFECTED_FILE not found: ${file}"
  first="$(head -n 1 "${file}")"
  case "${first}" in
    "AFFECTED "*) ;;
    "NONE")
      [ "$(wc -l < "${file}")" -le 1 ] || die "INTEGRATION_AFFECTED_FILE ${file}: a NONE selection lists no package"
      AFFECTED_FILTER_ACTIVE=1
      printf 'integration affected selection: NONE -- no integration package runs\n'
      return 0
      ;;
    "FULL "*)
      printf 'integration affected selection: %s -- running the full suite\n' "${first}"
      return 0
      ;;
    *) die "INTEGRATION_AFFECTED_FILE ${file} has an unknown first line '${first}'" ;;
  esac
  declared="${first#AFFECTED }"
  case "${declared}" in ""|*[!0-9]*) die "INTEGRATION_AFFECTED_FILE count is not numeric: '${declared}'" ;; esac
  while IFS= read -r line; do
    [ -n "${line}" ] || continue
    [ -n "${INTEGRATION_SHARD_WEIGHTS[${line}]+set}" ] \
      || die "affected selection names '${line}', which is not a planned integration package"
    [ -z "${AFFECTED_KEYS[${line}]+set}" ] \
      || die "affected selection lists '${line}' more than once"
    AFFECTED_KEYS["${line}"]=1
    count=$((count + 1))
  done < <(tail -n +2 "${file}")
  [ "${count}" -eq "${declared}" ] \
    || die "affected selection declares ${declared} package(s) but lists ${count}"
  AFFECTED_FILTER_ACTIVE=1
  printf 'integration affected selection: %d package(s)\n' "${count}"
  for line in "${!AFFECTED_KEYS[@]}"; do
    printf '  AFFECTED %s\n' "${line}"
  done | LC_ALL=C sort
}

# affected_includes KEY: true when the whole suite runs or KEY is selected.
affected_includes() {
  [ "${AFFECTED_FILTER_ACTIVE}" -eq 0 ] || [ -n "${AFFECTED_KEYS[$1]+set}" ]
}

emit_integration_shard_matrix() {
  load_affected_selection
  local matrix='{"include":[' separator="" shard key count=0
  local -a split_keys=(
    "${PROVIDER_INTEGRATION_PACKAGE_KEY}" "${DAILY_INTEGRATION_PACKAGE_KEY}"
    "${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}" "${QAPISERVER_INTEGRATION_PACKAGE_KEY}"
    "${REMAINING_INTEGRATION_PACKAGE_KEY}"
  )
  local -a split_targets=(providersync daily workerservice queryapiserver remaining)
  local -a split_counts=(
    "${PROVIDER_TEST_SHARD_COUNT}" "${DAILY_TEST_SHARD_COUNT}"
    "${WORKERSERVICE_TEST_SHARD_COUNT}" "${QAPISERVER_TEST_SHARD_COUNT}"
    "${REMAINING_TEST_SHARD_COUNT}"
  )
  local i
  for i in "${!split_targets[@]}"; do
    affected_includes "${split_keys[${i}]}" || continue
    for ((shard = 1; shard <= split_counts[i]; shard++)); do
      matrix+="${separator}{\"target\":\"${split_targets[${i}]}\",\"shard\":${shard}}"
      separator=","
      count=$((count + 1))
    done
  done
  for ((shard = 1; shard <= INTEGRATION_SHARD_COUNT; shard++)); do
    [ "${INTEGRATION_SHARD_NON_PROVIDER_COUNTS[${shard}]}" -gt 0 ] || continue
    if [ "${AFFECTED_FILTER_ACTIVE}" -eq 1 ]; then
      local hit=0
      for key in "${!INTEGRATION_SHARD_BY_KEY[@]}"; do
        [ "${INTEGRATION_SHARD_BY_KEY[${key}]}" -eq "${shard}" ] || continue
        for i in "${!split_keys[@]}"; do [ "${key}" != "${split_keys[${i}]}" ] || continue 2; done
        [ -z "${AFFECTED_KEYS[${key}]+set}" ] || hit=1
      done
      [ "${hit}" -eq 1 ] || continue
    fi
    matrix+="${separator}{\"target\":\"packages\",\"shard\":${shard}}"
    separator=","
    count=$((count + 1))
  done
  matrix+=']}'
  printf 'integration shard matrix: %s\n' "${matrix}"
  printf 'integration shard legs: %d\n' "${count}"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    printf 'matrix=%s\n' "${matrix}" >> "${GITHUB_OUTPUT}"
    printf 'legs=%d\n' "${count}" >> "${GITHUB_OUTPUT}"
  fi
}

check_integration_shard_plan() {
  plan_integration_shards
  plan_providersync_test_shards
  plan_daily_test_shards
  plan_workerservice_test_shards
  plan_queryapiserver_test_shards
  plan_remaining_test_shards
  emit_integration_shard_matrix
}

# Read the test dependencies from the production Go harness instead of copying
# digests into the workflow. Exact hosted evidence for this retry is PR #1735,
# run 31524982512, job 93891310235: Docker Hub returned 502 for this pinned
# manifest and testcontainers made no second pull attempt. The bounded pre-pull
# keeps the immutable digest contract and makes each registry attempt visible.
#
# CHAOS-4778 widened this from ClickHouse alone to every image a container-backed
# test pulls. Hosted evidence for the widening is run 33572737859, job
# go-quality: `create container: reaper: new reaper: run container: Error
# response from daemon: unauthorized: authentication required`. The image that
# failed was testcontainers-go's own reaper, which no job pre-pulled and which
# no job even names -- the library starts it before the first container of every
# test binary.
INTEGRATION_IMAGE_KEYS=(postgres clickhouse valkey reaper)

# integration_image_declaration maps a key to the constant that declares it in
# the harness. The reaper is the odd one out: it is a copy of a testcontainers-go
# constant rather than a digest we choose (see harness.go), so it is exempt from
# the digest contract below and TestReaperImageMatchesTestcontainers is what
# keeps the copy current.
integration_image_declaration() {
  case "$1" in
    postgres) printf 'PostgresImage\n' ;;
    clickhouse) printf 'ClickHouseImage\n' ;;
    valkey) printf 'ValkeyImage\n' ;;
    reaper) printf 'ReaperImage\n' ;;
    *) die "unknown test dependency image key '$1' (known: ${INTEGRATION_IMAGE_KEYS[*]})" ;;
  esac
}

# integration_image_reference_pattern echoes the ERE that a NOT-digest-pinned
# image must match. It is per-key on purpose. The previous generic
# `^[^[:space:]]+:[^[:space:]]+$` asserted only "something, a colon, something",
# which accepted `clickhouse/clickhouse-server:latest`,
# `quay.io/other/clickhouse-server:latest` and `clickhouse/other-image:26.7`
# alike -- a foreign registry so accepted then bypasses the ghcr mirror
# downstream. Relaxing ClickHouse from a digest to a tag (CHAOS-4854) was a
# change to the VERSION predicate; it should never have relaxed the IMAGE
# domain along with it.
integration_image_reference_pattern() {
  case "$1" in
    # chris's CHAOS-4854 ruling: "26.7 will pull all tags, 'matching' != matching
    # exact. It's major version MATCHING." So the repository is fixed and only
    # the 26.x version floats -- 26.7 and any patch inside it apply
    # automatically, while `latest` and a different image do not. A digest stays
    # legal so a future re-pin needs no change here.
    clickhouse)
      printf '%s\n' '^clickhouse/clickhouse-server(:26(\.[0-9]+)*|@sha256:[0-9a-f]{64})$'
      ;;
    # The reaper's identity is pinned by TestReaperImageMatchesTestcontainers
    # against testcontainers-go's own exported constant, which is a stronger
    # control than a pattern here could be. This only rejects a bare repository.
    #
    # Deliberately UNCHANGED from the pattern this function replaced, including
    # its acceptance of a digest form. Waiving the digest REQUIREMENT for a key
    # should not also forbid a digest -- a digest is strictly more pinned than a
    # tag, so refusing one here would reject an improvement. Narrowing this is
    # out of scope for CHAOS-4854, which is about ClickHouse.
    *)
      printf '%s\n' '^[^[:space:]]+:[^[:space:]]+$'
      ;;
  esac
}

integration_image_requires_digest() {
  # reaper: testcontainers-go picks its own tag, so we match the library.
  # clickhouse: tracks the 26 MAJOR by tag so minor and patch upgrades apply --
  # ruled by chris (CHAOS-4854), same policy CHAOS-4851 used for the CI service
  # containers. Returning 1 here waives only the DIGEST requirement; each key
  # still has to satisfy integration_image_reference_pattern above, which keeps
  # the repository fixed.
  case "$1" in
    reaper | clickhouse) return 1 ;;
    *) return 0 ;;
  esac
}

discover_test_dependency_image() {
  local key="$1" declaration image reference_pattern
  local -a images=()

  declaration="$(integration_image_declaration "${key}")"
  [ -f "${INTEGRATION_CONTAINER_HARNESS}" ] \
    || die "integration container harness not found: ${INTEGRATION_CONTAINER_HARNESS}"
  while IFS= read -r image; do
    [ -n "${image}" ] || continue
    images+=("${image}")
  done < <(
    sed -nE \
      "s/^[[:space:]]*${declaration}[[:space:]]*=[[:space:]]*\"([^\"]+)\".*\$/\1/p" \
      "${INTEGRATION_CONTAINER_HARNESS}"
  )

  [ "${#images[@]}" -eq 1 ] \
    || die "expected exactly one ${declaration} declaration in ${INTEGRATION_CONTAINER_HARNESS}, found ${#images[@]}"
  image="${images[0]}"
  if integration_image_requires_digest "${key}"; then
    if [[ ! "${image}" =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]]; then
      die "${declaration} must be pinned by a full sha256 digest, got '${image}'"
    fi
  else
    reference_pattern="$(integration_image_reference_pattern "${key}")"
    if [[ ! "${image}" =~ ${reference_pattern} ]]; then
      die "${declaration} must match ${reference_pattern}, got '${image}'"
    fi
  fi
  printf '%s\n' "${image}"
}

# mirrored_image applies TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX exactly as
# testcontainers-go does, so the pre-pull warms the SAME ref the tests will later
# ask for. Reusing the library's own variable rather than inventing a second one
# is deliberate: one setting redirects both, and they cannot disagree about where
# an image lives.
#
# The rule matches prependHubRegistry: an image that already names a registry is
# left alone, where "names a registry" means its first path component contains a
# dot or a colon (docker.io/x, localhost:5000/x). `postgres:18-alpine` does not
# qualify -- the colon there is a tag -- which is exactly why the check looks at
# the first component only.
mirrored_image() {
  local image="$1" prefix="${TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX:-}" first
  if [ -z "${prefix}" ]; then
    printf '%s\n' "${image}"
    return 0
  fi
  first="${image%%/*}"
  # An explicitly written `docker.io/...` ref is REFUSED rather than guessed at.
  # testcontainers-go cannot handle it coherently: ExtractRegistry normalises
  # "docker.io" to the empty fallback, so its own `registry == "docker.io"`
  # exclusion can never fire, and prependHubRegistry then builds
  # `<prefix>/docker.io/<image>` -- a ref that does not resolve. Matching that
  # would mean pre-warming a nonsense ref; diverging from it would mean this
  # script warms one image while the test pulls another. Both are worse than
  # refusing, and no pin here needs the prefix form.
  if [ "${first}" != "${image}" ] && [ "$(printf '%s' "${first}" | tr '[:upper:]' '[:lower:]')" = "docker.io" ]; then
    die "test dependency image '${image}' names docker.io explicitly. Write it without the registry (e.g. 'postgres:18-alpine@sha256:...') so TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX can redirect it; testcontainers-go mishandles the explicit form."
  fi
  if [ "${first}" != "${image}" ] && case "${first}" in *.*|*:*) true ;; *) false ;; esac; then
    printf '%s\n' "${image}"
    return 0
  fi
  printf '%s/%s\n' "${prefix%/}" "${image}"
}

prepull_one_image() {
  local key="$1" image attempt delay
  local max_attempts=3

  image="$(mirrored_image "$(discover_test_dependency_image "${key}")")"
  for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    printf 'pre-pull %s test dependency image %s (attempt %d/%d)\n' \
      "${key}" "${image}" "${attempt}" "${max_attempts}"
    if docker pull "${image}"; then
      printf 'pre-pulled %s test dependency image %s on attempt %d/%d\n' \
        "${key}" "${image}" "${attempt}" "${max_attempts}"
      return 0
    fi
    if [ "${attempt}" -eq "${max_attempts}" ]; then
      printf 'ERROR: failed to pre-pull %s test dependency image %s after %d attempts\n' \
        "${key}" "${image}" "${max_attempts}" >&2
      return 1
    fi
    delay=$((attempt * 5))
    printf 'WARN: %s image pull attempt %d/%d failed; retrying in %ds\n' \
      "${key}" "${attempt}" "${max_attempts}" "${delay}" >&2
    sleep "${delay}"
  done
}

# print_test_dependency_images emits "<key><TAB><image>" for every declared
# dependency, so anything that needs the set -- the pre-pull below, and the
# mirror workflow that copies them to ghcr.io -- reads it from ONE parser
# instead of each re-deriving it from harness.go and drifting.
print_test_dependency_images() {
  local key
  for key in "${INTEGRATION_IMAGE_KEYS[@]}"; do
    printf '%s\t%s\n' "${key}" "$(discover_test_dependency_image "${key}")"
  done
}

# check_integration_prepull warms EVERY declared dependency image.
#
# It deliberately takes no arguments. An earlier revision let a job name the
# subset it needed, which was cheaper -- go-quality starts no ClickHouse -- but
# it made the correctness of every job depend on a list a human kept in the
# workflow, and nothing checked that list against what the job actually starts.
# A codex round demonstrated the hole: `integration-prepull clickhouse` followed
# by `ci` passed every guard while leaving PostgreSQL cold, which is the exact
# defect this verb exists to prevent. Warming all four costs one extra image pull
# on go-quality and removes the entire class.
check_integration_prepull() {
  local key

  command -v docker >/dev/null 2>&1 || die "docker is required for integration-prepull"
  for key in "${INTEGRATION_IMAGE_KEYS[@]}"; do
    prepull_one_image "${key}" || return 1
  done
}

# sort_integration_shard_packages orders the caller's run_pkgs/run_keys (bash dynamic scope) longest package first:
# manifest weight descending, then key ascending in byte order. One go test starts its packages in argument order, four
# at a time, so a long package that starts last sets the shard wall (CHAOS-8935). LC_ALL=C is local to this function:
# it must not reach the go test child, which keeps the caller's locale.
sort_integration_shard_packages() {
  local i j tmp_pkg tmp_key LC_ALL=C
  for ((i = 1; i < ${#run_keys[@]}; i++)); do
    tmp_pkg="${run_pkgs[${i}]}"
    tmp_key="${run_keys[${i}]}"
    for ((j = i - 1; j >= 0; j--)); do
      if [ "${INTEGRATION_SHARD_WEIGHTS[${run_keys[${j}]}]}" -gt "${INTEGRATION_SHARD_WEIGHTS[${tmp_key}]}" ] \
        || { [ "${INTEGRATION_SHARD_WEIGHTS[${run_keys[${j}]}]}" -eq "${INTEGRATION_SHARD_WEIGHTS[${tmp_key}]}" ] \
          && [[ ! "${run_keys[${j}]}" > "${tmp_key}" ]]; }; then
        break
      fi
      run_pkgs[j + 1]="${run_pkgs[${j}]}"
      run_keys[j + 1]="${run_keys[${j}]}"
    done
    run_pkgs[j + 1]="${tmp_pkg}"
    run_keys[j + 1]="${tmp_key}"
  done
}

check_integration_package_shard() {
  local shard="$1" mode="$2"
  local index module_dir pkg key
  local selected_count=0
  local -a run_pkgs=()
  local -a run_keys=()

  if [ "${shard}" -lt 1 ] || [ "${shard}" -gt "${INTEGRATION_SHARD_COUNT}" ]; then
    die "integration package shard ${shard} is outside 1..${INTEGRATION_SHARD_COUNT}"
  fi

  for module_dir in "${MODULE_DIRS[@]}"; do
    run_pkgs=()
    run_keys=()
    for index in "${!INTEGRATION_PACKAGES[@]}"; do
      [ "${INTEGRATION_PACKAGE_MODULES[${index}]}" = "${module_dir}" ] || continue
      pkg="${INTEGRATION_PACKAGES[${index}]}"
      key="${module_dir}/${pkg#./}"
      key="${key#./}"
      [ "${key}" != "${PROVIDER_INTEGRATION_PACKAGE_KEY}" ] || continue
      [ "${key}" != "${DAILY_INTEGRATION_PACKAGE_KEY}" ] || continue
      [ "${key}" != "${WORKERSERVICE_INTEGRATION_PACKAGE_KEY}" ] || continue
      [ "${key}" != "${QAPISERVER_INTEGRATION_PACKAGE_KEY}" ] || continue
      [ "${key}" != "${REMAINING_INTEGRATION_PACKAGE_KEY}" ] || continue
      [ "${INTEGRATION_SHARD_BY_KEY[${key}]:-0}" -eq "${shard}" ] || continue
      run_pkgs+=("${pkg}")
      run_keys+=("${key}")
      selected_count=$((selected_count + 1))
    done
    [ "${#run_pkgs[@]}" -gt 0 ] || continue

    sort_integration_shard_packages

    printf 'go test integration package shard %s: %s -> %s\n' \
      "${shard}" "${module_dir}" "${run_pkgs[*]}"
    for key in "${run_keys[@]}"; do
      printf '  SHARD-RUN %s\n' "${key}"
    done
    local INTEGRATION_KEEP_ARG_ORDER=1
    run_integration_packages "${mode}" "${module_dir}" "integration package shard ${shard} (${module_dir})" "${run_pkgs[@]}"
  done

  [ "${selected_count}" -gt 0 ] \
    || die "integration package shard ${shard} selected zero packages"
  if [ "${selected_count}" -ne "${INTEGRATION_SHARD_NON_PROVIDER_COUNTS[${shard}]}" ]; then
    die "integration package shard ${shard} selected ${selected_count} packages but plan declared ${INTEGRATION_SHARD_NON_PROVIDER_COUNTS[${shard}]}"
  fi
  if [ "${mode}" = "--dry-run" ]; then
    printf 'integration package shard %s: DRY RUN selected %d package(s); no tests executed\n' \
      "${shard}" "${selected_count}"
  fi
}

check_providersync_test_shard() {
  local shard="$1" mode="$2"
  local test_name separator="" test_regex='^('
  local selected_count=0

  if [ "${shard}" -lt 1 ] || [ "${shard}" -gt "${PROVIDER_TEST_SHARD_COUNT}" ]; then
    die "providersync test shard ${shard} is outside 1..${PROVIDER_TEST_SHARD_COUNT}"
  fi

  for test_name in "${PROVIDER_TEST_NAMES[@]}"; do
    [ "${PROVIDER_TEST_SHARD_BY_NAME[${test_name}]:-0}" -eq "${shard}" ] || continue
    printf '  PROVIDER-TEST-RUN %s\n' "${test_name}"
    test_regex+="${separator}${test_name}"
    separator="|"
    selected_count=$((selected_count + 1))
  done
  test_regex+=')$'

  [ "${selected_count}" -gt 0 ] \
    || die "providersync test shard ${shard} selected zero tests"
  if [ "${selected_count}" -ne "${PROVIDER_TEST_SHARD_COUNTS[${shard}]}" ]; then
    die "providersync test shard ${shard} selected ${selected_count} tests but plan declared ${PROVIDER_TEST_SHARD_COUNTS[${shard}]}"
  fi
  if [ "${mode}" = "--dry-run" ]; then
    printf 'providersync test shard %s: DRY RUN selected %d top-level test(s); no tests executed\n' \
      "${shard}" "${selected_count}"
    return 0
  fi

  printf 'go test providersync test shard %s: %d top-level test(s)\n' \
    "${shard}" "${selected_count}"
  # CHAOS-10290: -timeout sits under the 25 min job cap so a slow shard panics with the running test names instead of a
  # silent cancel. -json goes to a file (per-test Elapsed for re-sharding); jq turns it back into the plain go test log,
  # and pipefail keeps go test's exit code.
  local json_dir="${GO_TEST_JSON_DIR:-${ROOT}/go-test-json}"
  mkdir -p "${json_dir}"
  run_providersync_bounded "${test_regex}" "${json_dir}/providersync-shard-${shard}.json"
}

check_integration_shard() {
  local target="${1:-}" shard="${2:-}" mode="${3:-}"

  case "${target}" in
    packages|providersync|daily|workerservice|queryapiserver|remaining) ;;
    *) die "integration-shard TARGET must be 'packages', 'providersync', 'daily', 'workerservice', 'queryapiserver' or 'remaining'" ;;
  esac
  case "${shard}" in
    ""|*[!0-9]*) die "integration-shard requires a numeric shard id" ;;
  esac
  case "${mode}" in
    ""|--dry-run) ;;
    *) die "integration-shard accepts only the optional --dry-run flag" ;;
  esac

  plan_integration_shards
  plan_providersync_test_shards
  plan_daily_test_shards
  plan_workerservice_test_shards
  plan_queryapiserver_test_shards
  plan_remaining_test_shards
  case "${target}" in
    packages) check_integration_package_shard "${shard}" "${mode}" ;;
    providersync) check_providersync_test_shard "${shard}" "${mode}" ;;
    daily) check_daily_test_shard "${shard}" "${mode}" ;;
    workerservice) check_workerservice_test_shard "${shard}" "${mode}" ;;
    queryapiserver) check_queryapiserver_test_shard "${shard}" "${mode}" ;;
    remaining) check_remaining_test_shard "${shard}" "${mode}" ;;
  esac
}

check_integration() {
  plan_integration_shards
  plan_providersync_test_shards
  local index module_dir pkg key reason
  local -a run_pkgs=()

  for module_dir in "${MODULE_DIRS[@]}"; do
    run_pkgs=()
    for index in "${!INTEGRATION_PACKAGES[@]}"; do
      [ "${INTEGRATION_PACKAGE_MODULES[${index}]}" = "${module_dir}" ] || continue
      pkg="${INTEGRATION_PACKAGES[${index}]}"
      key="${module_dir}/${pkg#./}"
      key="${key#./}"
      if reason="$(integration_denylist_reason "${key}")"; then
        continue
      fi
      run_pkgs+=("${pkg}")
    done
    [ "${#run_pkgs[@]}" -gt 0 ] || continue

    printf 'go test integration: %s -> %s\n' "${module_dir}" "${run_pkgs[*]}"
    run_integration_packages "" "${module_dir}" "integration (${module_dir})" "${run_pkgs[@]}"
  done
}

check_multi_replica_workers() {
  local proof_dir proof_file measured result tmo
  tmo="$(job_go_test_timeout 5)" || return 2
  proof_dir="$(mktemp -d "${TMPDIR:-/tmp}/dev-health-multi-replica.XXXXXX")"
  proof_file="${proof_dir}/measured-jobs"
  result=0
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_MULTI_REPLICA_PROOF="${proof_file}" \
      go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" \
        -run '^TestExplicitQueueMultiReplicaClaimDrainRestart$' \
        ./internal/workerservice
  ) || result=$?
  if [ "${result}" -ne 0 ]; then
    rm -rf -- "${proof_dir}"
    return "${result}"
  fi
  if [ ! -f "${proof_file}" ]; then
    rm -rf -- "${proof_dir}"
    die "multi-replica worker gate produced no measured-job proof"
  fi
  measured="$(tr -d '[:space:]' < "${proof_file}")"
  case "${measured}" in
    ""|*[!0-9]*)
      rm -rf -- "${proof_dir}"
      die "multi-replica worker gate produced an invalid measured-job proof"
      ;;
  esac
  if [ "${measured}" -le 0 ]; then
    rm -rf -- "${proof_dir}"
    die "multi-replica worker gate measured zero jobs"
  fi
  printf 'multi-replica worker gate: measured %s terminal jobs\n' "${measured}"
  rm -rf -- "${proof_dir}"
}

# check_integration_vet compiles every package under the integration tag,
# across the WHOLE tree, in every Go module, unconditionally -- no discovery,
# no denylist. check_integration_coverage's discovery answers "which packages
# does the Docker-backed run cover"; this answers the orthogonal question "does
# everything under the tag even compile", which matters independently: a
# package can compile and still be (legitimately or not) denylisted, or it can
# be in the run set and simply not build, the way
# internal/workersctl/main_integration_test.go didn't after its
# constructor's signature changed underneath it. `go vet -tags=integration
# ./...` needs no Docker and runs in seconds, so it belongs in the fast path
# rather than the Docker-backed integration job.
check_integration_vet() {
  run_in_modules "go vet -tags=integration" go vet -mod=readonly -trimpath -tags=integration ./...
}

# --- Auth Control Plane gates (CHAOS-4887) -----------------------------------
# `check_go.sh auth [STAGE]` runs the Go quality and release gates for the
# auth-service tree ALONE, so an auth-only change does not need the fleet-wide
# suite and an unrelated change does not need these. The packages are the real
# tree (cmd/auth-service, cmd/auth-migrate, internal/auth/..., the auth schema
# adapter); a new auth package under those roots is covered automatically.
AUTH_PACKAGES=(
  ./cmd/auth-service/...
  ./cmd/auth-migrate/...
  ./internal/auth/...
  ./contracts/auth/v1/...
  ./internal/audit/...
  ./internal/storage/postgres/authschema/...
  ./internal/token/...
)
# One fuzz target per parser, each its own corpus (the target's f.Add seeds plus
# any testdata/fuzz/<Target> entries) and its own proof marker. Format:
# "<package> <FuzzTarget>". check_auth_fuzz_registry fails when a Fuzz function
# exists in the tree without a row here, or a row names a function that does not.
AUTH_FUZZ_TARGETS=(
  "./internal/auth/authapi FuzzParseRefreshRequest"
  "./internal/auth/authapi FuzzDecodeAuthorizationRequest"
  "./internal/auth/contracts FuzzParseErrorEnvelope"
  "./internal/auth/contracts FuzzRevisionUnmarshal"
  "./internal/auth/contracts FuzzTimestampUnmarshal"
  "./internal/auth/contracts FuzzParseAccessToken"
  "./internal/auth/contracts FuzzParseWorkloadToken"
  "./internal/auth/edgetoken FuzzVerifyToken"
  "./internal/auth/signedtoken FuzzValidSignedToken"
  "./internal/auth/httpapi FuzzParseLimit"
  "./internal/auth/httpapi FuzzParseForwardedTrust"
  "./internal/auth/clientaddr FuzzParseTrust"
  "./internal/auth/keystore FuzzParseEd25519Key"
  "./internal/auth/keystore FuzzParseKeySetManifest"
  "./internal/auth/authconfig FuzzConfigLoad"
  "./internal/auth/identity FuzzNormalizeEmail"
  "./internal/auth/identity FuzzExternalKeyValidate"
  "./internal/auth/identity/oidc FuzzOpenState"
  "./internal/auth/identity/oidc FuzzParseJOSEHeader"
  "./internal/auth/identity/oidc FuzzValidateIDTokenClaims"
  "./internal/auth/identity/oidc FuzzParseCallbackBody"
  "./internal/auth/identity/saml FuzzParseSAMLResponse"
  "./internal/auth/identity/saml FuzzSAMLSubject"
  "./internal/auth/authorization FuzzNormalizeRequest"
  "./internal/auth/authorization FuzzEvaluateRequest"
  "./internal/auth/authorization FuzzEvaluateRepositoryGrant"
)
AUTH_FUZZTIME="${AUTH_FUZZTIME:-20s}"
AUTH_GOVULNCHECK_VERSION="v1.9.0"
# Marker the production-stack test logs once it has mounted and driven the real
# middleware stack (internal/auth/authruntime/production_stack_test.go).
AUTH_STACK_MARKER="AUTH_PRODUCTION_STACK_EXECUTED"

auth_go() {
  (cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go "$@")
}

check_auth_fmt() {
  local output pkg specs=()
  # The gated packages, one pathspec each, so a package added to AUTH_PACKAGES
  # is formatted by this gate too.
  for pkg in "${AUTH_PACKAGES[@]}"; do
    pkg="${pkg#./}"
    specs+=("${pkg%/...}/*.go")
  done
  output="$(cd "${ROOT}" && git ls-files --cached --others --exclude-standard -z -- "${specs[@]}" \
    | xargs -0 -r "${GOFMT}" -l)"
  if [ -n "${output}" ]; then
    printf 'auth gofmt: these files need formatting:\n%s\n' "${output}" >&2
    return 1
  fi
  printf 'auth gofmt: clean\n'
}

check_auth_vet() {
  printf 'auth go vet\n'
  auth_go vet -mod=readonly -trimpath "${AUTH_PACKAGES[@]}"
}

check_auth_test() {
  printf 'auth go test\n'
  auth_go test -mod=readonly -trimpath -count=1 "${AUTH_PACKAGES[@]}"
}

# check_auth_history (CHAOS-4765): the endpoint-profile inventories' source_commit must resolve, be an
# ancestor of HEAD, and every anchor line must read the same there as in the tree. It needs full git history,
# so it is built only under -tags authhistory and run only here (auth-test job, fetch-depth: 0). A shallow
# checkout, or a run that never printed the per-inventory proof marker, FAILS: it never skips.
check_auth_history() {
  local out
  printf 'auth source_commit history check\n'
  [ "$(cd "${ROOT}" && git rev-parse --is-shallow-repository)" = "false" ] \
    || die "auth history: shallow checkout; this check needs full history (actions/checkout fetch-depth: 0)"
  out="$(auth_go test -mod=readonly -trimpath -count=1 -v -tags authhistory -run '^TestEveryEndpointProfileInventoryDeclaresAVerifiableSourceCommit$' ./internal/auth/contracts 2>&1)" \
    || { printf '%s\n' "${out}" >&2; die "auth history: an inventory source_commit does not verify"; }
  printf '%s\n' "${out}"
  printf '%s\n' "${out}" | grep -q 'AUTH_SOURCE_COMMIT_VERIFIED ' \
    || die "auth history: no inventory was verified (the proof marker never printed)"
}

check_auth_race() {
  printf 'auth go test -race\n'
  auth_go test -mod=readonly -trimpath -count=1 -race "${AUTH_PACKAGES[@]}"
}

# check_auth_race_integration (CHAOS-10207): auth concurrency proofs need BOTH
# -race and a live PostgreSQL. `race` runs without the integration tag and
# go.yml's integration shards run without -race, so neither runs them.
# AUTH_RACE_PACKAGES lists the packages; append yours in the PR that adds its
# race tests. In each package every TestRace* test logs AUTH_RACE_MARKER only
# when built with -race, and the stage fails unless every one of them did.
AUTH_RACE_MARKER="AUTH_RACE_EXECUTED"
AUTH_RACE_PACKAGES=(
  ./internal/auth/session
  ./internal/auth/identity
  ./internal/auth/identity/local
  ./internal/audit
  ./internal/auth/keyrotation
)
check_auth_race_integration() {
  local pkg output want got total=0 tmo
  for pkg in "${AUTH_RACE_PACKAGES[@]}"; do
    tmo="$(job_go_test_timeout 27)" || return 2
    want="$(cd "${ROOT}" && grep -hoE '^func TestRace[A-Za-z0-9_]+' "${pkg#./}"/*_test.go | sed 's/^func //' | LC_ALL=C sort -u || true)"
    [ -n "${want}" ] || die "auth race-integration: no TestRace* test found in ${pkg}"
    output="$(mktemp)"
    printf 'auth go test -race -tags=integration %s\n' "${pkg}"
    if ! auth_go test -mod=readonly -trimpath -count=1 -race -tags=integration -v -timeout="${tmo}" "${pkg}" >"${output}" 2>&1; then
      cat "${output}" >&2
      rm -f "${output}"
      die "auth race-integration: ${pkg} failed"
    fi
    got="$(grep -oE "${AUTH_RACE_MARKER} TestRace[A-Za-z0-9_]+" "${output}" | sed "s/^${AUTH_RACE_MARKER} //" | LC_ALL=C sort -u || true)"
    rm -f "${output}"
    [ "${want}" = "${got}" ] || die "auth race-integration: ${pkg}: these race tests did not log ${AUTH_RACE_MARKER} (not run, skipped, or built without -race): $(comm -23 <(printf '%s\n' "${want}") <(printf '%s\n' "${got}") | tr '\n' ' ')"
    printf '%s %s: %s race test(s) ran with -race on a live PostgreSQL\n' "${AUTH_RACE_MARKER}" "${pkg}" "$(printf '%s\n' "${got}" | wc -l | tr -d ' ')"
    total=$((total + $(printf '%s\n' "${got}" | wc -l)))
  done
  printf 'auth race-integration: %d package(s), %d race test(s)\n' "${#AUTH_RACE_PACKAGES[@]}" "${total}"
}

check_auth_fuzz_registry() {
  local listed registered
  listed="$(cd "${ROOT}" && grep -rhoE '^func Fuzz[A-Za-z0-9_]+' \
    cmd/auth-service cmd/auth-migrate internal/auth internal/audit internal/storage/postgres/authschema --include='*_test.go' \
    | sed 's/^func //' | LC_ALL=C sort)"
  registered="$(printf '%s\n' "${AUTH_FUZZ_TARGETS[@]}" | awk '{print $2}' | LC_ALL=C sort)"
  [ -n "${registered}" ] || die "auth fuzz registry is empty: the gate would measure nothing"
  [ "${listed}" = "${registered}" ] \
    || die "auth fuzz registry and the Fuzz functions of the auth tree differ: registry=[$(printf '%s' "${registered}" | tr '\n' ' ')] tree=[$(printf '%s' "${listed}" | tr '\n' ' ')]"
  printf 'auth fuzz registry: %s targets match the tree\n' "$(printf '%s\n' "${registered}" | wc -l | tr -d ' ')"
}

# check_auth_fuzz [TARGET]: run every registered target (or only TARGET) with the
# fuzz engine for AUTH_FUZZTIME, each in its own invocation. The proof marker is
# per target and is printed only when the engine really ran ("fuzz: elapsed"
# line) and the run passed: a target satisfied by the seed corpus alone, or by
# a neighbour, prints nothing and fails.
check_auth_fuzz() {
  local only="${1:-}" entry package target output execs ran=0
  check_auth_fuzz_registry
  # A budget below the engine's baseline-coverage pass runs zero fuzz iterations and
  # would still print PASS: refuse it, and refuse a run that reports none below.
  [[ "${AUTH_FUZZTIME}" =~ ^[0-9]+s$ ]] || die "AUTH_FUZZTIME=${AUTH_FUZZTIME} must be whole seconds such as 20s"
  [ "${AUTH_FUZZTIME%s}" -ge 5 ] || die "AUTH_FUZZTIME=${AUTH_FUZZTIME} is below the 5s floor: the fuzz engine would run no iteration"
  for entry in "${AUTH_FUZZ_TARGETS[@]}"; do
    package="${entry% *}"
    target="${entry##* }"
    if [ -n "${only}" ] && [ "${only}" != "${target}" ]; then continue; fi
    output="$(mktemp)"
    if ! (cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -run '^$' \
      -fuzz "^${target}\$" -fuzztime "${AUTH_FUZZTIME}" "${package}") >"${output}" 2>&1; then
      cat "${output}" >&2
      rm -f "${output}"
      die "fuzz target ${target} failed"
    fi
    execs="$(grep -oE '^fuzz: elapsed: [^,]*, execs: [0-9]+' "${output}" | tail -n 1 | grep -oE '[0-9]+$' || true)"
    if ! grep -q '^fuzz: elapsed:' "${output}" || ! grep -q '^PASS$' "${output}" || [ "${execs:-0}" -le 0 ]; then
      cat "${output}" >&2
      rm -f "${output}"
      die "fuzz target ${target} did not run the fuzz engine to a pass"
    fi
    tail -n 3 "${output}"
    rm -f "${output}"
    printf 'auth-fuzz-proof: %s %s ran for %s (%s executions)\n' "${package}" "${target}" "${AUTH_FUZZTIME}" "${execs}"
    ran=$((ran + 1))
  done
  [ "${ran}" -gt 0 ] || die "auth fuzz: no target ran (TARGET=${only:-all})"
}

# check_auth_vuln: govulncheck over the auth packages only, pinned. ANY
# reachable finding fails the gate: there is no ignore list, so a pull request
# cannot whitelist a vulnerability.
check_auth_vuln() {
  local report
  report="$(mktemp)"
  printf 'auth govulncheck %s\n' "${AUTH_GOVULNCHECK_VERSION}"
  if ! (cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go run -mod=readonly -trimpath "golang.org/x/vuln/cmd/govulncheck@${AUTH_GOVULNCHECK_VERSION}" \
    -format json "${AUTH_PACKAGES[@]}") >"${report}" 2>"${report}.err"; then
    # govulncheck -format json exits 0 even with findings; a non-zero exit is a tool failure.
    cat "${report}.err" >&2
    rm -f "${report}" "${report}.err"
    die "govulncheck failed to run"
  fi
  rm -f "${report}.err"
  auth_go run -trimpath -mod=readonly ./cmd/cigate govulncheck <"${report}" || { rm -f "${report}"; die "auth govulncheck gate failed"; }
  rm -f "${report}"
}

# check_auth_closure: CHAOS-10131. Every module in the `go list -deps` closure of
# the two auth binaries must be a row of ci/auth_closure_allow.tsv. A ClickHouse
# driver or any other new module entering the closure fails here, not at review.
# The closure is the UNION over the shipped build configurations: the Docker
# image builds with CGO_ENABLED=0 for linux/amd64 and linux/arm64
# (docker/go-worker.Dockerfile), the CI runner with cgo on, and a `!cgo` or
# per-GOARCH file can import a module only one of them links.
# AUTH_CLOSURE_ALLOW names another allow-list (the selftest uses it).
check_auth_closure() {
  local actual="" allowed extra stale cgo arch part allow_file="${AUTH_CLOSURE_ALLOW:-${ROOT}/ci/auth_closure_allow.tsv}"
  for cgo in 0 1; do
    for arch in amd64 arm64; do
      part="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off CGO_ENABLED="${cgo}" GOOS=linux GOARCH="${arch}" go list -mod=readonly -deps \
        -f '{{if .Module}}{{.Module.Path}}{{end}}' ./cmd/auth-service ./cmd/auth-migrate)" || die "auth closure: go list failed (CGO_ENABLED=${cgo} linux/${arch})"
      actual+="${part}"$'\n'
    done
  done
  actual="$(printf '%s' "${actual}" | LC_ALL=C sort -u | sed '/^$/d')"
  [ -n "${actual}" ] || die "auth closure: go list returned no modules"
  allowed="$(awk -F'\t' '$1 !~ /^#/ && $1 != "" { print $1 }' "${allow_file}" | LC_ALL=C sort -u)"
  extra="$(LC_ALL=C comm -23 <(printf '%s\n' "${actual}") <(printf '%s\n' "${allowed}"))"
  stale="$(LC_ALL=C comm -13 <(printf '%s\n' "${actual}") <(printf '%s\n' "${allowed}"))"
  if [ -n "${stale}" ]; then
    printf 'auth closure: ci/auth_closure_allow.tsv lists modules the binaries no longer link (remove the rows):\n%s\n' "${stale}" >&2
    return 1
  fi
  if [ -n "${extra}" ]; then
    printf 'auth closure: the auth binaries link modules outside ci/auth_closure_allow.tsv:\n%s\n' "${extra}" >&2
    return 1
  fi
  printf 'auth closure: %s modules, all allowed\n' "$(printf '%s\n' "${actual}" | wc -l | tr -d ' ')"
}

# check_auth_middleware: G-69. Runs the production-stack test verbosely and
# fails unless it logged its marker: the test mounted the real middleware stack
# (built from the service's own apiServerOptions) and drove it. A green test
# name without the marker is not evidence.
check_auth_middleware() {
  local output
  output="$(mktemp)"
  if ! (cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -count=1 -v \
    -run '^TestProductionMiddlewareStackIsMountedAndDriven$' ./internal/auth/authruntime) >"${output}" 2>&1; then
    cat "${output}" >&2
    rm -f "${output}"
    die "the production middleware stack test failed"
  fi
  if ! grep -q "${AUTH_STACK_MARKER}" "${output}"; then
    cat "${output}" >&2
    rm -f "${output}"
    die "the production middleware stack test passed without logging ${AUTH_STACK_MARKER}: it did not drive the stack"
  fi
  grep "${AUTH_STACK_MARKER}" "${output}"
  rm -f "${output}"
}

# auth_relevant BASE HEAD: print relevant=true|false. A change is relevant when
# it touches a file of the auth packages' own import closure inside this module
# (computed with go list, so a change to internal/platform/secrets, which the
# service imports, counts and a change to an unrelated package does not), the
# auth contracts, the module files, or the gate's own files.
auth_relevant() {
  local base="$1" head="$2" closure changed file relevant=false
  closure="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -deps \
    -test -f '{{if not .Standard}}{{.Dir}}{{end}}' "${AUTH_PACKAGES[@]}" | sed "s|^${ROOT}/||" | LC_ALL=C sort -u)"
  [ -n "${closure}" ] || die "auth relevance: go list returned an empty closure"
  # Packages the scope selftest governs (auth-looking names, importers of the
  # schema adapter) select the gates too, as directories relative to the root.
  closure+=$'\n'"$(auth_scope_candidates | sed "s|^$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -m)/||")"
  # The exemption file's packages too: deleting one leaves a stale exemption that
  # only the scope selftest rejects, so a change under it must select the gates.
  closure+=$'\n'"$(awk -F'\t' -v m="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -m)/" '$1 !~ /^#/ && $1 != "" { sub("^" m, "", $1); print $1 }' "${ROOT}/ci/auth_gate_scope_exempt.txt")"
  # --no-renames: a file moved OUT of the auth tree must report its old path too.
  changed="$(git -C "${ROOT}" diff --name-only --no-renames "${base}" "${head}")"
  while IFS= read -r file; do
    [ -n "${file}" ] || continue
    if auth_path_relevant "${file}" "${closure}"; then
      printf 'auth relevance: %s\n' "${file}" >&2
      relevant=true
      break
    fi
  done <<<"${changed}"
  printf 'relevant=%s\n' "${relevant}"
}

auth_path_relevant() {
  local file="$1" closure="$2" dir
  case "${file}" in
    cmd/auth-service/*|cmd/auth-migrate/*|internal/auth/*|internal/audit/*|internal/storage/postgres/authschema/*|contracts/auth/*|go.mod|go.sum|ci/check_go.sh|cmd/cigate/*|internal/cigates/*|ci/auth_gate_scope_exempt.txt|ci/auth_closure_allow.tsv|ci/auth_queue_relevance.sh|ci/auth_corpus_queue_relevance.sh|ci/auth_corpus_paths.txt|.github/workflows/go-auth-service.yml)
      return 0
      ;;
  esac
  # A Go-package area path with an auth* or audit* directory segment: a package
  # that is new, deleted or moved is not in the head-tree candidate list.
  case "${file}" in
    *.go)
      [[ "${file%/*}" =~ (^|/)(auth|audit)[^/]*(/|$) ]] && return 0
      ;;
  esac
  # Any file (Go, testdata, golden, embedded asset) under a directory of the
  # import closure can change what the auth packages do or test.
  while IFS= read -r dir; do
    [ -n "${dir}" ] && [ "${dir}" != "." ] || continue
    case "${file}" in "${dir}"/*) return 0 ;; esac
  done <<<"${closure}"
  return 1
}

# check_auth_paths_selftest: the path filter's two outcomes, executed on a
# fixed closure: files inside the auth tree select, files outside do not.
check_auth_paths_selftest() {
  local closure inside outside file
  closure="$(printf 'internal/auth/authruntime\ninternal/platform/secrets\n')"
  inside=(internal/authnew/a.go internal/some/auth_helper/x.go internal/audit/audit.go internal/auth/contracts/deleted_package/x.go cmd/auth-service/main.go internal/auth/authruntime/runtime.go internal/platform/secrets/secrets.go internal/auth/authruntime/testdata/fixture.txt internal/platform/secrets/testdata/nested/golden.json contracts/auth/v1/README.md go.mod ci/check_go.sh ci/auth_queue_relevance.sh ci/auth_corpus_queue_relevance.sh ci/auth_corpus_paths.txt)
  outside=(internal/providersync/sync.go docs/index.md web/README.md internal/platform/other/x.go internal/platform/secretsx/y.go src/dev_health_ops/cli.py)
  for file in "${inside[@]}"; do
    auth_path_relevant "${file}" "${closure}" || die "auth path filter: ${file} must select the auth gates and does not"
    printf 'auth path filter: %s -> relevant\n' "${file}"
  done
  for file in "${outside[@]}"; do
    if auth_path_relevant "${file}" "${closure}"; then die "auth path filter: ${file} must NOT select the auth gates and does"; fi
    printf 'auth path filter: %s -> not relevant\n' "${file}"
  done
}

# check_auth_scope_selftest (CHAOS-10103): the auth package set is not a hand list
# a new package can miss. Every Go package of the module that LOOKS like auth (any
# path segment starting with auth or audit) or imports the auth schema adapter
# must be inside the gate (AUTH_PACKAGES) or named, with a reason, in
# ci/auth_gate_scope_exempt.txt (package<TAB>reason). An exemption that is
# gated, or that names no package, is stale and fails.
# auth_scope_candidates: the import path of every Go package of the module that
# LOOKS like auth (a path segment starting with auth or audit) or imports the
# auth schema adapter. One definition for the scope selftest AND the relevance
# filter, so a package the selftest would demand a decision for also selects the
# auth gates (a new such package must not be able to skip the very check that
# would reject it).
# auth_scope_tags: every build tag NAMED in a //go:build line of the module (minus
# the toolchain's own GOOS/GOARCH/unix/ignore constraints), derived from the repo
# so a new tag cannot hide a package from the scope check.
auth_scope_tags() {
  (cd "${ROOT}" && git ls-files -z -- '*.go' | xargs -0 -r grep -hE '^//go:build ' \
    | sed 's#^//go:build ##' | tr -c 'A-Za-z0-9_.\n' ' ' | tr ' ' '\n' \
    | grep -vxE '|unix|ignore|cgo|linux|darwin|windows|freebsd|openbsd|netbsd|js|wasip1|amd64|arm64|arm|386|wasm|go[0-9.]+' \
    | LC_ALL=C sort -u)
}

# auth_scope_candidates: discovery runs once with no tag, once with every derived
# tag, and once per tag (a `!tag` file is visible only without it; an `a && b`
# file only with both), and unions the results.
auth_scope_candidates() {
  local module tag tags all=""
  module="$(cd "${ROOT}" && "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -m)"
  tags="$(auth_scope_tags)"
  [ -n "${tags}" ] || die "auth scope: no build tag found in the module; derivation is broken"
  for tag in ${tags}; do all+="${all:+,}${tag}"; done
  {
    for set in "" "${all}" ${tags}; do
      auth_go list -mod=readonly -tags="${set}" -e -json ./... | auth_go run -trimpath -mod=readonly ./cmd/cigate scope-candidates "${module}"
    done
  } | LC_ALL=C sort -u
}

check_auth_scope_selftest() {
  local exempt="${ROOT}/ci/auth_gate_scope_exempt.txt" gated candidates missing stale pkg reason
  gated="$(auth_go list -mod=readonly "${AUTH_PACKAGES[@]}" | LC_ALL=C sort -u)"
  [ -n "${gated}" ] || die "auth scope: the gate selects no package"
  candidates="$(auth_scope_candidates)"
  [ -n "${candidates}" ] || die "auth scope: no candidate package found; the selftest would measure nothing"
  missing=""
  while IFS= read -r pkg; do
    [ -n "${pkg}" ] || continue
    printf '%s\n' "${gated}" | grep -qx -- "${pkg}" && continue
    grep -qE "^${pkg//\//\\/}	" "${exempt}" 2>/dev/null && continue
    missing+="${pkg} "
  done <<<"${candidates}"
  [ -z "${missing}" ] || die "auth scope: package(s) look like auth or import the auth schema but are neither in the gate nor exempt with a reason in ci/auth_gate_scope_exempt.txt: ${missing}"
  stale=""
  while IFS=$'\t' read -r pkg reason; do
    case "${pkg}" in ''|'#'*) continue ;; esac
    [[ "${reason}" =~ [^[:space:]] ]] || stale+="${pkg}(no reason) "
    if printf '%s\n' "${gated}" | grep -qx -- "${pkg}" || ! printf '%s\n' "${candidates}" | grep -qx -- "${pkg}"; then stale+="${pkg} "; fi
  done <"${exempt}"
  [ -z "${stale}" ] || die "auth scope: stale or reasonless exemption(s) (gated, no longer a candidate, or no reason): ${stale}"
  printf 'auth scope: %s candidate packages, all gated or exempt\n' "$(printf '%s\n' "${candidates}" | wc -l | tr -d ' ')"
}

check_auth() {
  case "${1:-all}" in
    fmt) check_auth_fmt ;;
    vet) check_auth_vet ;;
    test) check_auth_test ;;
    race) check_auth_race ;;
    race-integration) check_auth_race_integration ;;
    history) check_auth_history ;;
    fuzz) check_auth_fuzz "${2:-}" ;;
    fuzz-registry) check_auth_fuzz_registry ;;
    vuln) check_auth_vuln ;;
    middleware) check_auth_middleware ;;
    closure) check_auth_closure ;;
    relevant)
      [ "$#" -eq 3 ] || die "auth relevant accepts BASE HEAD"
      auth_relevant "$2" "$3"
      ;;
    paths-selftest) check_auth_paths_selftest ;;
    scope-selftest) check_auth_scope_selftest ;;
    all)
      check_auth_paths_selftest
      check_auth_scope_selftest
      check_auth_fmt
      check_auth_vet
      check_auth_test
      check_auth_race
      check_auth_race_integration
      check_auth_history
      check_auth_middleware
      check_auth_closure
      check_auth_fuzz
      check_auth_vuln
      ;;
    *) die "auth accepts fmt|vet|test|race|race-integration|history|fuzz [TARGET]|fuzz-registry|vuln|middleware|relevant BASE HEAD|paths-selftest|scope-selftest|all" ;;
  esac
}

discover_modules
print_modules

case "${1:-all}" in
  auth)
    shift
    check_auth "$@"
    ;;
  fmt)
    check_format
    ;;
  vet)
    check_vet
    ;;
  test)
    check_test
    ;;
  race)
    check_race
    ;;
  venue-oracles)
    if [ "${2:-}" = "--changed" ]; then
      local_usage="venue-oracles --changed accepts FILE [--list] [SHARD COUNT]"
      [ "$#" -ge 3 ] || die "${local_usage}"
      VENUE_CHANGED_FILE="$3"
      VENUE_CHANGED_LIST_ONLY=""
      shift 3
      if [ "${1:-}" = "--list" ]; then
        VENUE_CHANGED_LIST_ONLY="--list"
        shift
      fi
      { [ "$#" -eq 0 ] || [ "$#" -eq 2 ]; } || die "${local_usage}"
      check_venue_oracles "${1:-}" "${2:-}"
    else
      { [ "$#" -eq 1 ] || [ "$#" -eq 3 ]; } || die "venue-oracles accepts no arguments, or SHARD COUNT (1-based shard of COUNT), or --changed FILE [--list]"
      check_venue_oracles "${2:-}" "${3:-}"
    fi
    ;;
  race-excluded-shard-plan)
    [ "$#" -eq 3 ] || die "race-excluded-shard-plan accepts SHARD COUNT"
    race_excluded_rows "$2" "$3"
    ;;
  test-shard-plan)
    [ "$#" -eq 3 ] || die "test-shard-plan accepts SHARD COUNT"
    check_test_shard_plan "$2" "$3"
    ;;
  race-shard-plan)
    [ "$#" -eq 3 ] || die "race-shard-plan accepts SHARD COUNT"
    check_race_shard_plan "$2" "$3"
    ;;
  test-shard-wall-guard)
    [ "$#" -eq 5 ] || die "test-shard-wall-guard accepts SHARD COUNT WALL LOGFILE"
    check_test_shard_wall_guard "$2" "$3" "$4" "$5"
    ;;
  test-shard-unweighted-guard)
    [ "$#" -eq 4 ] || die "test-shard-unweighted-guard accepts SHARD COUNT LOGFILE"
    check_test_shard_unweighted_guard "$2" "$3" "$4"
    ;;
  security-fresh-receipt)
    [ "$#" -ge 3 ] || die "security-fresh-receipt accepts RAN JSON..."
    shift
    check_security_fresh_receipt "$@"
    ;;
  security-fresh-split)
    shift
    if security_fresh_active; then
      security_fresh_load
      for pkg in "$@"; do
        if [ -n "$(printf '%s\n' "${pkg}" | security_split in)" ]; then printf 'sec %s\n' "${pkg}"; else printf 'plain %s\n' "${pkg}"; fi
      done
    else
      for pkg in "$@"; do printf 'plain %s\n' "${pkg}"; done
    fi
    ;;
  venue-oracle-plan)
    [ "$#" -eq 2 ] || die "venue-oracle-plan accepts COUNT"
    check_venue_oracle_plan "$2"
    ;;
  ci-leg)
    { [ "$#" -eq 2 ] || [ "$#" -eq 4 ]; } || die "ci-leg accepts LEG, or 'test|race SHARD COUNT'"
    shift
    check_ci_leg "$@"
    ;;
  ci-leg-warm)
    [ "$#" -eq 4 ] || die "ci-leg-warm accepts test|race SHARD COUNT"
    shift
    check_ci_leg_warm "$@"
    ;;
  build)
    check_build
    ;;
  contract)
    check_contract
    ;;
  multi-replica-workers)
    [ "$#" -eq 1 ] || die "multi-replica-workers accepts no arguments"
    check_multi_replica_workers
    ;;
  integration-vet)
    check_integration_vet
    ;;
  integration-coverage)
    check_integration_coverage
    ;;
  integration-shard-plan)
    [ "$#" -eq 1 ] || die "integration-shard-plan accepts no arguments"
    check_integration_shard_plan
    ;;
  integration-images)
    [ "$#" -eq 1 ] || die "integration-images accepts no arguments"
    print_test_dependency_images
    ;;
  integration-prepull)
    [ "$#" -eq 1 ] || die "integration-prepull accepts no arguments"
    check_integration_prepull
    ;;
  ratchets)
    [ "$#" -eq 1 ] || die "ratchets accepts no arguments"
    check_ratchets
    ;;
  race-weights-rows)
    [ "$#" -eq 1 ] || die "race-weights-rows accepts no arguments"
    check_race_weights_rows
    ;;
  race-excluded-receipts)
    [ "$#" -ge 2 ] || die "race-excluded-receipts accepts go test -json file(s)"
    race_excluded_receipts_listing="$(race_excluded_rows)" || exit 2
    shift
    check_race_excluded_receipts "${race_excluded_receipts_listing}" "$@"
    ;;
  race-excluded-ran)
    [ "$#" -eq 1 ] || die "race-excluded-ran accepts no arguments"
    check_race_excluded_ran
    ;;
  integration-shard)
    if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
      die "integration-shard requires TARGET SHARD and accepts only optional --dry-run"
    fi
    check_integration_shard "$2" "$3" "${4:-}"
    ;;
  integration)
    check_integration
    ;;
  fast)
    check_format
    check_vet
    check_test
    check_build
    check_contract
    check_ratchets
    check_race_weights_rows
    check_integration_vet
    plan_integration_shards
    plan_providersync_test_shards
    check_plan_reads
    check_multi_replica_workers
    ;;
  ci)
    # Exactly the pre-CHAOS-3948 `all` behaviour: everything `all` runs
    # except the full unsharded integration suite, which stays PLAN-only
    # here on purpose. CI's go-storage-integration-plan/-shard jobs already
    # run that suite for real, sharded and parallel, with their own timeout
    # budget per shard -- running it again here, unsharded, would duplicate
    # that work and blow this job's timeout (measured ~24m for `all`'s
    # check_integration alone vs this job's 20m budget). `ci` exists so
    # go.yml's go-quality step keeps byte-for-byte the coverage it always
    # had, while `all` becomes the honest full-signal verb for local use.
    check_format
    check_vet
    check_test
    check_race
    check_build
    check_contract
    check_ratchets
    check_race_weights_rows
    check_integration_vet
    plan_integration_shards
    plan_providersync_test_shards
    check_plan_reads
    check_multi_replica_workers
    ;;
  all)
    check_format
    check_vet
    check_test
    check_race
    check_build
    check_contract
    check_ratchets
    check_race_weights_rows
    check_integration_vet
    check_integration
    check_plan_reads
    check_multi_replica_workers
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
