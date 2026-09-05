#!/usr/bin/env bash
# Run the repository's Go quality gates across the root module and every
# checked-in nested module (including the River N-1 compatibility module).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
GO_TOOLCHAIN="go1.27.0"
export GOTOOLCHAIN="${GO_TOOLCHAIN}"
# CHAOS-5224: precedence is an explicit DEV_HEALTH_GO_CACHE first, then an
# already-inherited GOCACHE, and only then the tmp fallback. The old code
# skipped straight to the tmp fallback regardless of what the caller already
# exported, silently overwriting an inherited GOCACHE and growing a THIRD Go
# build cache on bigboy's root disk (7.5G observed) alongside the two
# legitimate bind-mounted caches.
DEV_HEALTH_GO_CACHE="${DEV_HEALTH_GO_CACHE:-${GOCACHE:-${TMPDIR:-/tmp}/dev-health-go-build-cache}}"
mkdir -p "${DEV_HEALTH_GO_CACHE}"
export GOCACHE="${DEV_HEALTH_GO_CACHE}"
DEV_HEALTH_GO_BUILD_OUTPUT=""
DEV_HEALTH_GO_BUILD_TEMP_ROOT=""
DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="${DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST:-${ROOT}/ci/go_integration_shards.tsv}"
DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST="${DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST:-${ROOT}/ci/go_providersync_test_shards.tsv}"
INTEGRATION_CONTAINER_HARNESS="${ROOT}/internal/testsupport/containers/harness.go"

# --- Ambient-env scrub (CHAOS-3988). ------------------------------------------------
# This script previously had NO scrub at all -- it inherited the caller's shell
# environment wholesale into every `go` invocation below. ops/.env's direnv setup
# exports GO_PROVIDER_ROUTES=all and DEV_HEALTH_ENV=local for local `dev-hops` CLI
# convenience:
#   - internal/platform/config (config.go:58-59) reads both directly via
#     os.LookupEnv to enable the "local all-routes" preset for the Go worker's own
#     typed config, exercised anywhere `go test ./...` builds that package.
#   - live-python-oracles below shells out to `python3` (exec.Command inherits the
#     ambient environment by default -- Go does not scrub subprocess env unless the
#     caller sets cmd.Env explicitly), and the Python side's
#     _provider_route_environment() (src/dev_health_ops/workers/provider_unit_route.py:
#     107-135) treats the SAME pair as its own "local all-routes" preset, expanding
#     the full work-item family. The Go scheduler's non-GitHub branch emits
#     contributing aliases instead, so the two planners disagree and
#     internal/scheduler/sync::TestBuildScheduledPlanMatchesLivePythonPlanner goes
#     false-red -- not a real defect, an ambient-env artifact (CHAOS-3988). See
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
GO_ENV_OFF=(env -u GO_PROVIDER_ROUTES -u DEV_HEALTH_ENV -u GOFLAGS -u GOEXPERIMENT)

usage() {
  # Backticks in the literal help text document commands; they are not substitutions.
  # shellcheck disable=SC2016
  printf '%s\n' 'Usage: ci/check_go.sh [fmt|vet|test|race|live-python-oracles|build|contract|multi-replica-workers|integration-vet|integration-coverage|integration-shard-plan|integration-prepull|integration-shard|integration|fast|ci|all]

  fmt    Check gofmt without modifying files.
  vet    Run go vet ./... in every Go module.
  test   Run go test ./... in every Go module.
  race   Run go test -race ./... in every Go module.
  live-python-oracles
         Run the provider-sync, providerfoundation encryption,
         scheduled-planner, daily-metrics discovery, and sync-coverage live-Python oracle packages
         with `go test -count=1` unconditionally
         (cache lookup disabled by -count=1 itself, not by any assumption
         about cache state). Separate from `test` because that package
         executes real production Python files (src/dev_health_ops/**.py)
         at test time, which `//go:embed` cannot make part of the Go test
         cache key -- `test`'\''s bare `go test ./...` can return a stale
         cached PASS for a real change to one of those files. NOT an
         optimization opt-out: a run that skips this verb has not tested
         the oracles at all, so it MUST stay in `all`, `ci`, and `fast`
         (since it is cheap) rather than being treated as an extra,
         skippable step.
  build  Run go build ./... in every Go module.
  contract
         Validate the job contract tree and, when DEV_HEALTH_CONTRACT_BASE is
         set, reject breaking in-place changes against that directory.
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
         Validate ci/go_integration_shards.tsv against live package discovery,
         derive deterministic longest-processing-time-first shard assignments,
         print the complete assignment, and write a GitHub Actions `matrix`
         output when GITHUB_OUTPUT is set. No Docker required.
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
         the dominant internal/providersync package. `--dry-run` prints the
         exact selection without starting Docker-backed tests; CI never passes
         that option.
  integration
         Discover and run EVERY integration-tagged package'\''s suite against
         real containers, except the (small, justified) INTEGRATION_DENYLIST.
         Inclusion is the default; exclusion is the explicit, loud exception.
  fast   Run fmt, vet, test, live-python-oracles, build, contract,
         integration-vet, and the integration shard-plan checks (PLAN only --
         does not execute the integration suite; see `ci`, `all`, and
         `integration`). No race detector -- the quick local-iteration mode.
  ci     Exactly `fast` plus the race detector (CHAOS-3948): fmt, vet, test,
         race, live-python-oracles, build, contract, integration-vet, and the
         integration shard-plan checks (PLAN only, same as `fast`), plus
         multi-replica-workers. This is byte-for-byte what `all` ran before
         CHAOS-3948 -- go.yml'\''s go-quality step uses this so its coverage
         stays unchanged. CI gets its real (sharded, parallel) integration
         signal from the separate go-storage-integration-plan/-shard jobs,
         not from this step.
  all    Run fmt, vet, test, race, live-python-oracles, build, contract,
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
  run_in_modules "go vet" go vet -mod=readonly ./...
}

check_test() {
  run_in_modules "go test" go test -mod=readonly ./...
}

check_race() {
  run_in_modules "go test -race" go test -mod=readonly -race ./...
}

check_live_python_oracles() {
  # internal/providersync, internal/providerfoundation, and internal/scheduler/sync execute REAL production Python files
  # (src/dev_health_ops/**.py, via testdata/python_oracle_loader.py)
  # directly at test time -- not test fixtures, the actual functions this
  # repo ships. `//go:embed` cannot reach outside its own package
  # directory (verified directly: `go vet` rejects a `../` pattern with
  # "invalid pattern syntax"), so those Python files are structurally
  # invisible to Go's test-result cache key. A warm local `go test` cache
  # can then return a stale PASS for a real, uncommitted change to one of
  # them -- reproduced empirically (CHAOS-3162, codex adversarial review):
  # edit a live-oracle Python source with no Go file touched, run a bare
  # `go test` a second time, get `(cached)` back with no re-execution at
  # all. check_test's plain `go test ./...` above does not force a fresh
  # run and is NOT a sufficient gate for this one package on its own --
  # this step is deliberately separate and always uses -count=1, which
  # disables cache lookup entirely by design, regardless of what caused
  # the staleness.
  #
  # -count=1 lives HERE, at the verb, and not as something a caller
  # remembers to pass to `test` -- the whole defect this verb exists to
  # close is that the cache staleness is invisible from the call site (no
  # error, no warning, just a silently-stale PASS), so a solution that
  # depends on the caller already knowing to opt in reproduces the same
  # failure mode one level up. Do NOT fold this back into check_test "for
  # tidiness": that would put -count=1 (and its resulting slower, no-cache
  # `test` run) on every package in the tree instead of only the one that
  # structurally needs it, and it would make skipping this specific
  # coverage possible again by construction.
  local pair_count pair_file pair_source proof_dir proof_file
  proof_dir="$(mktemp -d "${TMPDIR:-/tmp}/dev-health-live-python-oracles.XXXXXX")"

  printf 'go test -count=1: internal/providersync (live Python oracle sources are outside the Go embed/cache boundary)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 ./internal/providersync/...
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  pair_count=0
  while IFS= read -r pair_source; do
    pair_count=$((pair_count + 1))
    pair_file="${pair_source##*/}"
    proof_file="${proof_dir}/${pair_file}"
    if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
      printf 'ERROR: live Python oracle pair %s was not executed successfully\n' \
        "${pair_file}" >&2
      rm -rf -- "${proof_dir}"
      return 1
    fi
  done < <(
    find "${ROOT}/internal/providersync/testdata/oracle_pairs" \
      -maxdepth 1 -type f -name '*.py' ! -name '_*' -print | LC_ALL=C sort
  )
  if [ "${pair_count}" -eq 0 ]; then
    printf 'ERROR: no checked-in live Python oracle pairs were discovered\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/providerfoundation (live Python encryption compatibility)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestFernetCipherMatchesLivePythonCustomSalt$' \
        ./internal/providerfoundation/...
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/providerfoundation-credentials"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: providerfoundation live Python encryption measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/scheduler/sync (live Python planner source is outside the Go embed/cache boundary)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 ./internal/scheduler/sync/...
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/scheduler-sync"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: scheduler/sync live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/daily (live Python repository discovery source is outside the Go embed/cache boundary)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestPythonDiscoverReposOracle$' \
        ./internal/jobs/metrics/daily
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/daily-metrics-discover"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: daily metrics live Python repository-discovery measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/daily (testops_risk vs live Python compute_testops_risk.py, CHAOS-4294)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestTestopsRiskComputeMatchesLivePythonProduction|TestPipelineStabilityFMAGoldenMatchesLivePython)$' \
        ./internal/jobs/metrics/daily
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/testops-risk-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: testops_risk live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Checked SEPARATELY from testops-risk-golden above (same reasoning as the
  # numerical package's sibling goldens): a single proof marker would be
  # satisfied by whichever guard happened to run, letting the other be
  # skipped, renamed, or filtered out of the -run pattern unnoticed.
  proof_file="${proof_dir}/pipeline-stability-fma-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: pipeline-stability FMA golden (CHAOS-4818 site 10) rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/aigovernance (ai_governance port vs live Python, CHAOS-4285)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestGovernanceRowsMatchLivePythonProduction$' \
        ./internal/jobs/metrics/aigovernance
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/ai-governance-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: ai_governance live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/aiimpact (ai_impact port vs live Python, CHAOS-4280)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestAIImpactMatchesLivePythonProduction|TestRepoPatternResolverMatchesLivePython)$' \
        ./internal/jobs/metrics/aiimpact
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Checked SEPARATELY, per the sibling goldens' reasoning: one shared marker
  # would be satisfied by whichever guard happened to run, letting the other be
  # skipped, renamed, or filtered out of the -run pattern unnoticed. The
  # resolver oracle is not optional -- it is the sole source of ai_impact's
  # team dimension.
  for marker in ai-impact-golden ai-impact-repo-teams-golden; do
    proof_file="${proof_dir}/${marker}"
    if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
      printf 'ERROR: ai_impact live Python oracle measurement did not occur (%s)\n' "${marker}" >&2
      rm -rf -- "${proof_dir}"
      return 1
    fi
  done

  printf 'go test -count=1: internal/jobs/metrics/workgraphedges (work_graph_edges port vs live Python, CHAOS-4286)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestWorkGraphEdgesMatchLivePythonProduction$' \
        ./internal/jobs/metrics/workgraphedges
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Own marker, checked separately, for the same reason as the siblings above:
  # a shared marker would be satisfied by whichever oracle happened to run.
  # This one compares edge_id too -- unlike ai_governance's, whose Python side
  # randomises the id -- so it is the only guard that can catch a change to the
  # _hash join or its part order.
  proof_file="${proof_dir}/work-graph-edges-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: work_graph_edges live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/aiworkflow (ai_workflow port vs live Python, CHAOS-4280/CHAOS-4286)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestAIWorkflowMatchesLivePythonProduction$' \
        ./internal/jobs/metrics/aiworkflow
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Own marker, checked separately, for the same reason as the siblings
  # above: a shared marker would be satisfied by whichever oracle happened
  # to run. This one is the ONLY guard proving Go's strongestSignal keeps
  # the FIRST maximal element on a tie against production's real
  # extract_ai_workflow_from_pull_requests, not a synthetic fixture in this
  # repo's own Go test.
  proof_file="${proof_dir}/ai-workflow-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: ai_workflow live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/testops (compute_testops.py port vs live Python, CHAOS-4294/CHAOS-4284)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestComputePipelineMetricsMatchesLivePythonProductionOnRealRow|TestComputePipelineMetricsGroupingMatchesLivePythonProduction|TestComputeTestMetricsMatchesLivePythonProductionOnRealRows|TestComputeCoverageMetricMatchesLivePythonProductionOnRealRows|TestComputePipelineMetricsAvgQueueMatchesLivePythonSum)$' \
        ./internal/jobs/metrics/testops
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # testops-pipeline-avgqueue-golden (CHAOS-4284) is checked alongside the
  # other four for the reason the numerical package's sibling goldens are:
  # one shared marker would be satisfied by whichever guard happened to run,
  # letting another be skipped, renamed, or filtered out of the -run pattern
  # unnoticed. This one specifically pins avg_queue_seconds to CPython's
  # Neumaier-compensated sum() -- the four older oracles all pass against a
  # NAIVE Go accumulation, which is how that defect survived CHAOS-4294.
  for marker in testops-pipeline-golden testops-pipeline-grouping-golden testops-test-golden testops-coverage-golden testops-pipeline-avgqueue-golden; do
    proof_file="${proof_dir}/${marker}"
    if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
      printf 'ERROR: internal/jobs/metrics/testops live Python oracle measurement did not occur (%s)\n' "${marker}" >&2
      rm -rf -- "${proof_dir}"
      return 1
    fi
  done

  printf 'go test -count=1: internal/jobs/metrics/workitemmetrics (work_item + work_item_estimate goldens vs live compute_work_items.py, CHAOS-4283)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestWorkItemGoldenMatchesLivePython$' \
        ./internal/jobs/metrics/workitemmetrics
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Checked SEPARATELY, one marker per FAMILY, for the same reason the
  # testops-risk / pipeline-stability pair above is: a single shared marker
  # would be satisfied by whichever family's guard ran, letting the other be
  # skipped, renamed, or filtered out of the -run pattern unnoticed.
  for marker in work-item-golden work-item-estimate-golden; do
    proof_file="${proof_dir}/${marker}"
    if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
      printf 'ERROR: work_item golden rot guard did not compare against live Python (%s)\n' "${marker}" >&2
      rm -rf -- "${proof_dir}"
      return 1
    fi
  done

  printf 'go test -count=1: internal/jobs/metrics/numerical (frozen numerical golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestRemainingMetricsGoldenMatchesLivePython|TestCapacityForecastGoldenMatchesLivePython|TestTeamWellbeingGoldenMatchesLivePython|TestFMAGoldenMatchesLivePython)$' \
        ./internal/jobs/metrics/numerical
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/numerical-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: numerical golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Checked SEPARATELY from the numerical golden above rather than folded into
  # one marker. Two goldens with two producers are two claims: a single proof
  # file would be satisfied by whichever guard happened to run, so the other
  # could be skipped, renamed, or filtered out of the -run pattern without the
  # lane noticing -- the same silent-degradation shape these guards exist to
  # prevent.
  proof_file="${proof_dir}/capacity-forecast-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: capacity forecast golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Same reasoning as capacity-forecast-golden above: team_wellbeing (CHAOS-4276)
  # is a distinct golden/producer and gets its own proof marker.
  proof_file="${proof_dir}/daily-wellbeing-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: team_wellbeing golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Same reasoning again: the CHAOS-4818 FMA golden (release_impact
  # ._compute_confidence, compute._percentile, compute_capacity._percentile)
  # is a fourth distinct golden/producer in this same package and gets its
  # own proof marker. hotspot_score (Go's ComputeFileHotspots) used to be a
  # fourth Python producer feeding this same fma_golden.json -- retired below
  # alongside the rest of file_hotspots'/file_risk_hotspots' live-Python rot
  # guards (CHAOS-5234/CHAOS-3092); this marker now covers only the three
  # keys TestFMAGoldenMatchesLivePython still compares
  # (fmaLiveComparedKeys).
  proof_file="${proof_dir}/fma-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: FMA golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  # internal/jobs/metrics/daily/filehotspots' three live-Python rot guards
  # (TestFileHotspotsGoldenMatchesLivePython CHAOS-4277,
  # TestFMAFollowupGoldenMatchesLivePython, TestRiskHotspotsOrderGoldenMatchesLivePython
  # CHAOS-4863) were retired here: their producers,
  # compute_file_hotspots/compute_file_risk_hotspots, were DELETED, not
  # merely un-called -- the native Go executors (FileHotspotsExecutor/
  # FileRiskHotspotsExecutor, CHAOS-4277) are the sole producers now. The
  # frozen goldens (tests/fixtures/{file_hotspots_python_golden,
  # fma_followup_golden,risk_hotspots_order_golden}.json) stay; Go's own
  # TestComputeMatchesFrozenPythonGolden-family bit-exact tests
  # (golden_full_test.go, fma_golden_test.go, risk_hotspots_order_golden_test.go)
  # are the regression guard going forward. Proving "Python still agrees
  # with itself" stops being the protection that matters once Python is no
  # longer in the loop -- same shape as the issueprlinks retirement below.

  # internal/jobs/workgraph/issueprlinks' live-Python rot guard
  # (TestIssuePRLinksGoldenMatchesLivePython, CHAOS-4757) was retired here:
  # its producer, _derive_issue_pr_links_from_dependencies, was DELETED, not
  # merely un-called -- issueprlinks is the sole producer now. The frozen
  # golden (tests/fixtures/issue_pr_links_python_golden.json) stays; Go's own
  # TestDeriveMatchesFrozenPythonGoldenExhaustively is the regression guard
  # going forward. Proving "Python still agrees with itself" stops being the
  # protection that matters once Python is no longer in the loop.

  printf 'go test -count=1: internal/jobs/metrics/daily/repouser (frozen repo_user_commit golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestRepoUserCommitGoldenMatchesLivePython|TestPysumGoldenMatchesLivePython)$' \
        ./internal/jobs/metrics/daily/repouser
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/repo-user-commit-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: repo_user_commit golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Checked SEPARATELY (same reasoning throughout this function): a single
  # proof marker would be satisfied by whichever guard happened to run.
  proof_file="${proof_dir}/pysum-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: pysum golden (CHAOS-4824) rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/daily/cicd (frozen cicd golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestCICDGoldenMatchesLivePython$' \
        ./internal/jobs/metrics/daily/cicd
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Checked SEPARATELY from repo-user-commit-golden above -- see that block's
  # comment on capacity-forecast-golden for why two goldens need two markers.
  proof_file="${proof_dir}/cicd-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: cicd golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/daily/compoundingrisk (frozen compounding_risk golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestCompoundingRiskGoldenMatchesLivePython$' \
        ./internal/jobs/metrics/daily/compoundingrisk
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, for the same reason as cicd-golden above (CHAOS-4287).
  proof_file="${proof_dir}/compounding-risk-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: compounding_risk golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/daily/reviewedges (frozen review_edges golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestReviewEdgesGoldenMatchesLivePython$' \
        ./internal/jobs/metrics/daily/reviewedges
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, for the same reason as cicd-golden above (CHAOS-4279).
  proof_file="${proof_dir}/review-edges-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: review_edges golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/daily/benchmarking (frozen benchmarking golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestBenchmarkingGoldenMatchesLivePython$' \
        ./internal/jobs/metrics/daily/benchmarking
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, for the same reason as cicd-golden above (CHAOS-4288).
  proof_file="${proof_dir}/benchmarking-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: benchmarking golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/remaining (DORA incident projection vs the live Python builder)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestGoIncidentProjectionMatchesLivePythonBuilder$' \
        ./internal/jobs/metrics/remaining
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/remaining-dora-incident-sql"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: the DORA incident projection was not compared against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  # NOTE: this -run list is itself an enumeration, and it is the SECOND place a
  # rot guard has to be remembered -- once when the test is written, again here
  # before it can ever execute. A guard missing from this list does not fail; it
  # silently never runs. TestEveryDiscoverableCorpusStillMatchesLivePython is
  # listed first because it DISCOVERS its subjects from tests/fixtures/ and so
  # covers every conforming corpus without anyone editing this line again. See
  # CHAOS-4849.
  printf 'go test -count=1: internal/jobs/workgraph/units (frozen goldens vs live Python; the first test discovers its own subjects)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestEveryDiscoverableCorpusStillMatchesLivePython|TestWorkgraphComponentsGoldenMatchesLivePython|TestConfidenceCoercionGoldenMatchesLivePython|TestInvestmentQualityGoldenMatchesLivePython|TestMaxComponentNodesGoldenMatchesLivePython|TestDecimalDigitsGoldenMatchesLivePython|TestTimeBoundsGoldenMatchesLivePython)$' \
        ./internal/jobs/workgraph/units
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, per the capacity-forecast reasoning above: this golden has a
  # different producer (work_graph/investment/components.py + work_unit_id) from
  # every other pair here, and it guards a CROSS-JOB invariant -- the same
  # work_unit_id addresses work_unit_investments (Go, once CHAOS-4441 lands) and
  # work_unit_membership (still Python until CHAOS-4282). A shared marker could
  # be satisfied by another guard while this one was filtered out of -run.
  # The DISCOVERY guard's own marker. It walks tests/fixtures for generators and
  # enforces the undiscoverable-corpus ratchet, and it SKIPS without
  # DEV_HEALTH_LIVE_PYTHON_ORACLES=1 -- at which point Go's package-level `ok`
  # counts the skip as a pass. Asserting the marker is what makes a skipped
  # discovery guard fail the gate instead of passing it silently.
  proof_file="${proof_dir}/workgraph-units-corpus-discovery"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: corpus discovery guard did not run against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  proof_file="${proof_dir}/workgraph-components-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: work-unit component golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker again: the coercion corpus has a DIFFERENT producer
  # (float()'s string branch) from the component golden above, and it is the
  # guard that caught three separate parser divergences. A shared marker could
  # be satisfied by the component guard while this one was filtered out of -run.
  proof_file="${proof_dir}/confidence-coercion-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: confidence coercion corpus did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker again: this fixture spans FOUR producers across TWO modules
  # (utils/normalization's clamp and evidence_quality_band, plus evidence's
  # _graph_density, _float_value and compute_evidence_quality), and it records
  # whether evidence._float_value still agrees with components._edge_confidence
  # -- two Python copies of one coercion that the Go port collapses into a
  # single function. clamp() in particular lives outside work_graph/investment
  # entirely, so nothing else would tell a reviewer editing it that this port
  # depends on its NaN behaviour.
  proof_file="${proof_dir}/investment-quality-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: investment evidence-quality golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, guarding a value that appears in NO source file on either
  # side: sys.get_int_max_str_digits(). It is an interpreter runtime setting,
  # so it can move with no diff in this repository and no change of CPython
  # version. Every value between the old and new limits would then be parsed by
  # one plane and refused by the other -- which, for
  # INVESTMENT_MAX_COMPONENT_NODES, means one plane splits oversized components
  # and the other does not.
  proof_file="${proof_dir}/max-component-nodes-magnitude"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: max_component_nodes magnitude golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, guarding the interpreter's Unicode category Nd set -- the
  # characters int() accepts -- and each one's decimal value. Both come from the
  # deployed interpreter's unicode data and move on a Python upgrade with no
  # diff here. The guard also covers the GENERATED Go table, because a stale
  # table alongside a fresh fixture leaves the parser on the old set with the
  # tests green.
  proof_file="${proof_dir}/python-decimal-digits"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: python decimal-digit set did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker: compute_time_bounds and _node_time_bounds, whose per-type
  # fallback chains decide the stored TimeBounds on every work unit. Does not
  # touch input_hash, so a drift here re-dates units rather than re-billing
  # categorisation.
  proof_file="${proof_dir}/time-bounds-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: time-bounds golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/investment (materialize orchestration golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestFrozenPythonGoldenStillMatchesLivePython$' \
        ./internal/jobs/investment
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker (CHAOS-4441). This golden's producers are the ORCHESTRATION
  # decisions -- rollup_subcategories_to_themes' two-different-summations shape,
  # the invalid_llm_output evidence-quality clamp and its band recomputation,
  # the LLM-vs-fallback gate order, and json.dumps' ", " separators plus
  # ensure_ascii on the audit array. None of those is covered by the units
  # goldens above: those prove the PIECES, this proves the wiring between them,
  # which is the half that had no oracle while investment.materialize's native
  # path went unwired.
  proof_file="${proof_dir}/investment-materialize-orchestration"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: investment materialize orchestration golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/pythonparity (frozen json.dumps golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestPythonJSONGoldenMatchesLivePython|TestPythonJSONInsertionOrderGoldenMatchesLivePython|TestReprBandGoldenMatchesLivePython|TestEdgeShapesGoldenMatchesLivePython|TestWhitespaceGoldenMatchesLivePython|TestClickHouseStringDecodeGoldenMatchesLivePython|TestSumGoldenMatchesLivePython)$' \
        ./internal/pythonparity
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, for the same reason the two above have theirs: a THIRD
  # distinct producer -- CPython's json.dumps over evidence.build_text_bundle's
  # payload -- and the only one whose divergence costs money rather than
  # correctness. input_hash is categorization_input_hash, the LLM
  # skip-existing key; a drifted hash matches no stored row and re-categorizes
  # every work unit on every run, silently. A shared marker could be satisfied
  # by either guard above while this one was filtered out of -run.
  proof_file="${proof_dir}/python-json-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: python json.dumps golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker again. It shares the band golden's producer but guards a
  # DIFFERENT axis: string and token spellings rather than float rendering.
  # Someone "fixing" the column with ensure_ascii=False or allow_nan=False would
  # leave the band golden green, so a shared marker would let that through.
  proof_file="${proof_dir}/evidence-json-edge-shapes-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: evidence_json edge-shapes golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, for the only fixture here whose producer is the REAL
  # APPLICATION PATH rather than a direct library call: it builds a
  # Recommendation and calls recommendation_to_record, so its bytes come out of
  # loader.py:448 itself. That makes it sensitive to a key added to or reordered
  # in the evidence dict literal, a changed rounding depth at a `value=` site,
  # or an EvidenceRef rename -- none of which the direct-json guards can see.
  proof_file="${proof_dir}/evidence-json-repr-band-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: evidence_json repr-band golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, for a producer that is neither a computation nor a
  # dependency but a set of DEFAULT ARGUMENTS. json.dumps(value) with no
  # sort_keys is a DIFFERENT reference from json.dumps(value, sort_keys=True)
  # guarded above, and the two emit different bytes for the same data --
  # recommendations/loader.py:448 writes the evidence_json column with the
  # bare form. This marker is separate because a shared one would be satisfied
  # by the sort_keys guard while this one was filtered out of -run, which is
  # exactly the substitution that makes the two look interchangeable.
  proof_file="${proof_dir}/python-json-insertion-order-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: python json.dumps insertion-order golden did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker again, and this one guards a producer that lives OUTSIDE
  # this repository: CPython's str.isspace(), i.e. the interpreter's Unicode
  # tables. A Python upgrade can move it with no diff in src/ for a reviewer to
  # notice, and pythonparity.IsSpace hard-codes the current 0x1c-0x1f delta.
  proof_file="${proof_dir}/python-whitespace-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: python whitespace predicate did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, guarding the most fragile producer here: a THIRD-PARTY
  # DEPENDENCY. clickhouse-connect decodes String columns as UTF-8 and, on
  # failure, substitutes the lowercase hex of the whole value -- two lines
  # inside its read loop, not part of its documented API. A lockfile bump moves
  # it with no diff anywhere in this repository. chquery applies that policy to
  # every String column, and those strings are hashed into input_hash and into
  # work_unit_id.
  proof_file="${proof_dir}/clickhouse-string-decode-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: clickhouse String decode policy did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker: the producer is the INTERPRETER's builtin sum(), which has
  # used Neumaier compensated summation for floats since 3.12 and was a naive
  # accumulation before. The fixture therefore depends on the interpreter
  # version with no diff in this repository, in BOTH directions -- a downgrade
  # below 3.12 would make pythonparity.Sum's compensation wrong, not merely
  # unnecessary.
  proof_file="${proof_dir}/python-sum-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: python sum() semantics did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/workgraph/edges (frozen issue<->issue edge golden vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestWorkgraphIssueEdgesGoldenMatchesLivePython|TestNumericTypeDigitTableMatchesLivePython|TestPythonLowerMatchesLivePython|TestIntMaxStrDigitsMatchesLivePython|TestPythonDecimalBlocksMatchLivePython|TestEveryRuneLowercasesLikeLivePython)$' \
        ./internal/jobs/workgraph/edges
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # A SECOND invocation, not another name in the -run pattern above: `-run`
  # selects tests within the packages named on the command line, so a guard in a
  # different package is silently never run if it is only added to the pattern.
  # That failure is invisible -- the command exits 0 having matched nothing --
  # which is why the marker check below is what actually proves it executed.
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^(TestEveryRuneMatchesLivePythonCharacterClasses|TestPythonDigitValueMatchesLivePythonForEveryDigit)$' \
        ./internal/jobs/workgraph/textrefs
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker, for the reason spelled out at capacity-forecast-golden: the
  # edge golden has a different producer from every guard above it, so sharing a
  # proof file would let this one be renamed or filtered out of the -run pattern
  # while another guard's success stood in for it.
  proof_file="${proof_dir}/workgraph-issue-edges-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: workgraph issue-edge golden rot guard did not compare against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker again, per the capacity-forecast reasoning: this guard derives
  # a Unicode property table from the live interpreter, a different producer from
  # the edge golden above it, and it is the only thing standing between a Python
  # upgrade and a silent parity break in which pipeline owns a dependency row.
  proof_file="${proof_dir}/workgraph-numeric-digit-table"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: the Numeric_Type=Digit table was not re-derived from live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker: a different Unicode property from the digit table above, and
  # the one that decides which BRANCH of the canonicalisation a row takes.
  proof_file="${proof_dir}/workgraph-python-lower"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: pythonLower was not re-derived against live str.lower()\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker: this one reads an interpreter SETTING rather than a Unicode
  # property, so it rots for a different reason from every guard above --
  # sys.set_int_max_str_digits() can change it at runtime, and it did not exist
  # before Python 3.11. A deployment that raised or lowered it would leave this
  # port disagreeing about which PR ids are convertible, in the direction that
  # mislabels a build-aborting row as an ordinary PR.
  # Its own marker: this is the guard that stops Go's unicode package being the
  # oracle for a Python-facing predicate. It derives Python's DECIMAL set (the
  # direction the digit-table guard above does not cover) and compares the two
  # planes' Unicode versions, which is how a Go-only Nd rune parsed a PR number
  # Python does not recognise.
  # Its own marker: this one enumerates EVERY code point rather than a derived
  # subset, because the two previous case guards were each blind to a one-rune
  # property they did not think to vary -- context-sensitive final sigma, then
  # Unicode version skew between x/text and the interpreter.
  proof_file="${proof_dir}/workgraph-python-lower-allrunes"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: the all-runes lowercase comparison was not run against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Its own marker: this guard covers the THREE regex character classes the text
  # extractor substitutes (\s, \w, \d), which is a different Unicode surface
  # from the case-mapping and digit-table guards above. It fails in two
  # directions for different reasons -- a rune Python accepts and Go rejects is
  # a defect, while a rune Go accepts and Python does not is version skew with a
  # pinned count -- so it also rots when either side upgrades its tables.
  # This marker carries DATA as well as the fact of execution: the two UCD
  # versions the parity claim was established against. So it is a prefix test,
  # not equality -- an undated parity claim is the thing being avoided.
  proof_file="${proof_dir}/workgraph-textrefs-charclass-allrunes"
  if [ ! -f "${proof_file}" ] || ! grep -q '^executed ucd_python=.* ucd_go=' "${proof_file}"; then
    printf 'ERROR: the text-extractor character classes were not compared against live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/workgraph-python-decimal-blocks"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: Python decimal-digit blocks were not re-derived from live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/workgraph-int-max-str-digits"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: int_max_str_digits was not read back from live Python\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/jobs/metrics/numerical/cpyrandom (recorded CPython RNG vectors vs the live interpreter)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestGoldenStillDescribesLiveCPython$' \
        ./internal/jobs/metrics/numerical/cpyrandom
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/cpython-random-golden"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: the recorded CPython RNG vectors were not re-derived from the live interpreter\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/synccoverage (live Python coverage builder is outside the Go embed/cache boundary)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 ./internal/synccoverage/...
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/synccoverage"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: sync-coverage live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/syncdispatchruntime (CHAOS-4175 native finalize_sync_run zero-unit classification vs live Python)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 ./internal/syncdispatchruntime/...
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/sync-dispatch-finalize"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: native finalize_sync_run live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # CHAOS-4198: the same ./internal/syncdispatchruntime/... run above also
  # executes TestBudgetAdmissionMathMatchesLivePython (dispatch_sync_run's
  # BudgetGuard admission math vs the same live interpreter) -- this is a
  # second proof-file check on that ALREADY-COMPLETED run, not a second test
  # invocation.
  proof_file="${proof_dir}/sync-dispatch-admission"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: native dispatch_sync_run budget-admission live Python oracle measurement did not occur\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: cmd/query-api/internal/principal (Go verifier vs a REAL Python-issued envelope + JWKS, CHAOS-4366)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestVerifierMatchesLivePythonIssuedEnvelope$' \
        ./cmd/query-api/internal/principal
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/query-api-principal-envelope"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: the Go effective-principal verifier was not compared against a real Python-issued envelope\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: cmd/dev-health-worker (build-scope parity table vs the live bridge, CHAOS-4837)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHON="${PYTHON:-python3}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestBuildScopeParityTableMatchesLivePython$' \
        ./cmd/dev-health-worker
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  # Registering the guard here is the LOAD-BEARING half of CHAOS-4837, not
  # bookkeeping. The issue/PR golden's rot guard was written first and did not
  # run AT ALL until its dispatcher entry existed -- a guard nothing invokes is
  # indistinguishable from a guard that passes. The proof marker is what makes
  # "it ran" checkable rather than assumed.
  proof_file="${proof_dir}/build-scope-parity-table"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: the build-scope parity table was not re-measured against the live bridge\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  printf 'go test -count=1: internal/pythonparity (float round/repr/format mirrors vs the live interpreter)\n'
  if ! (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_LIVE_PYTHON_ORACLES=1 \
      DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR="${proof_dir}" \
      PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
      go test -mod=readonly -count=1 \
        -run '^TestFloatTextGoldenMatchesLivePython$' \
        ./internal/pythonparity/...
  ); then
    rm -rf -- "${proof_dir}"
    return 1
  fi
  proof_file="${proof_dir}/pythonparity-float-text"
  if [ ! -f "${proof_file}" ] || [ "$(cat "${proof_file}")" != "executed" ]; then
    printf 'ERROR: the CPython float round/repr/format golden was not re-derived from the live interpreter\n' >&2
    rm -rf -- "${proof_dir}"
    return 1
  fi

  rm -rf -- "${proof_dir}"
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
  run_in_modules "go build" go build -mod=readonly \
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
    "${GO_ENV_OFF[@]}" GOWORK=off go run -mod=readonly ./cmd/worker-contractcheck \
      validate --root "${contract_root}"
  )

  if [ -n "${contract_base}" ]; then
    [ -d "${contract_base}" ] \
      || die "DEV_HEALTH_CONTRACT_BASE is not a directory: ${contract_base}"
    printf 'job contracts: compare %s\n' "${contract_base}"
    (
      cd "${ROOT}"
      "${GO_ENV_OFF[@]}" GOWORK=off go run -mod=readonly ./cmd/worker-contractcheck \
        compare --base "${contract_base}" --candidate "${contract_root}"
    )
  fi
}

# INTEGRATION_DENYLIST is the ONLY place an integration-tagged package may be
# excluded from check_integration. It used to be the other way around: an
# opt-IN allowlist that ran only the packages someone remembered to add. That
# shape silently excluded internal/providerfoundation's ClickHouse/Valkey
# suite, cmd/dev-health-workerctl's suite (which additionally did not even
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
# CHAOS-4730: cmd/query-api/internal/analytics was the one entry here
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
declare -a INTEGRATION_SHARD_TOTALS=()
declare -a INTEGRATION_SHARD_PACKAGE_COUNTS=()
declare -a INTEGRATION_SHARD_NON_PROVIDER_COUNTS=()
INTEGRATION_SHARD_COUNT=0
PROVIDER_INTEGRATION_PACKAGE_KEY="internal/providersync"
declare -A PROVIDER_INTEGRATION_TEST_NAMES=()
declare -A PROVIDER_TEST_WEIGHTS=()
declare -A PROVIDER_TEST_CLASS=()
declare -A PROVIDER_TEST_SHARD_BY_NAME=()
declare -a PROVIDER_TEST_NAMES=()
declare -a PROVIDER_TEST_SHARD_TOTALS=()
declare -a PROVIDER_TEST_SHARD_COUNTS=()
declare -a PROVIDER_TEST_SHARD_INTEGRATION_COUNTS=()
PROVIDER_TEST_SHARD_COUNT=0
PROVIDER_INTEGRATION_TEST_WEIGHT=0
PROVIDER_ORDINARY_TEST_WEIGHT=0

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
      dir="$(dirname -- "${go_file}")"
      found+=("./${dir}")
    done < <(printf '%s\n' "${tracked_output}")
    while IFS= read -r -d '' go_file; do
      [ -f "${ROOT}/${module_dir}/${go_file}" ] || continue
      case "${go_file}" in
        vendor/*|*/vendor/*) continue ;;
      esac
      if grep -qE '^//go:build.*(^|[^[:alnum:]_])integration([^[:alnum:]_]|$)' \
        "${ROOT}/${module_dir}/${go_file}"; then
        dir="$(dirname -- "${go_file}")"
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
load_integration_shard_manifest() {
  local manifest="${DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST}"
  local line_number=0 key value extra
  local shards_seen=0

  [ -f "${manifest}" ] \
    || die "integration shard manifest not found: ${manifest}"

  INTEGRATION_SHARD_WEIGHTS=()
  INTEGRATION_SHARD_COUNT=0
  while IFS=$'\t ' read -r key value extra; do
    line_number=$((line_number + 1))
    case "${key}" in
      ""|\#*) continue ;;
    esac
    if [ -n "${extra}" ]; then
      die "integration shard manifest ${manifest}:${line_number} must contain exactly two fields"
    fi
    case "${value}" in
      ""|*[!0-9]*)
        die "integration shard manifest ${manifest}:${line_number} has non-numeric value '${value}'"
        ;;
    esac
    if [ "${value}" -le 0 ]; then
      die "integration shard manifest ${manifest}:${line_number} values must be positive"
    fi

    if [ "${key}" = "shards" ]; then
      [ "${shards_seen}" -eq 0 ] \
        || die "integration shard manifest declares 'shards' more than once"
      shards_seen=1
      INTEGRATION_SHARD_COUNT="${value}"
      continue
    fi
    if [ -n "${INTEGRATION_SHARD_WEIGHTS[${key}]+set}" ]; then
      die "integration shard manifest lists '${key}' more than once"
    fi
    INTEGRATION_SHARD_WEIGHTS["${key}"]="${value}"
  done < "${manifest}"

  [ "${shards_seen}" -eq 1 ] \
    || die "integration shard manifest must declare one 'shards' row"
  [ "${INTEGRATION_SHARD_COUNT}" -ge 2 ] \
    || die "integration shard manifest must declare at least two shards"
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

  while IFS=$'\t' read -r weight key; do
    selected_shard=1
    selected_total="${INTEGRATION_SHARD_TOTALS[1]}"
    for ((shard = 2; shard <= INTEGRATION_SHARD_COUNT; shard++)); do
      if [ "${INTEGRATION_SHARD_TOTALS[${shard}]}" -lt "${selected_total}" ]; then
        selected_shard="${shard}"
        selected_total="${INTEGRATION_SHARD_TOTALS[${shard}]}"
      fi
    done
    INTEGRATION_SHARD_BY_KEY["${key}"]="${selected_shard}"
    INTEGRATION_SHARD_TOTALS[selected_shard]=$((
      INTEGRATION_SHARD_TOTALS[selected_shard] + weight
    ))
    INTEGRATION_SHARD_PACKAGE_COUNTS[selected_shard]=$((
      INTEGRATION_SHARD_PACKAGE_COUNTS[selected_shard] + 1
    ))
    if [ "${key}" != "${PROVIDER_INTEGRATION_PACKAGE_KEY}" ]; then
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

  if ! files_output="$(
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go list -mod=readonly -tags=integration \
      -f '{{range .TestGoFiles}}{{println .}}{{end}}{{range .XTestGoFiles}}{{println .}}{{end}}' \
      ./internal/providersync
  )"; then
    die "failed to discover active providersync test files with go list"
  fi

  PROVIDER_TEST_NAMES=()
  PROVIDER_INTEGRATION_TEST_NAMES=()
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    case "${go_file}" in
      */*|..*) die "go list returned unsafe providersync test filename '${go_file}'" ;;
    esac
    source_file="${ROOT}/${PROVIDER_INTEGRATION_PACKAGE_KEY}/${go_file}"
    [ -f "${source_file}" ] \
      || die "go list returned missing providersync test file '${go_file}'"
    integration_file=0
    if grep -qE '^//go:build.*(^|[^[:alnum:]_])integration([^[:alnum:]_]|$)' \
      "${source_file}"; then
      integration_file=1
    fi
    while IFS= read -r test_name; do
      [ -n "${test_name}" ] || continue
      case "${test_name}" in
        Test*[![:alnum:]_]*)
          die "providersync source discovery returned unsupported top-level test name '${test_name}'"
          ;;
        Test*) ;;
        *) continue ;;
      esac
      [ -z "${discovered_names[${test_name}]+set}" ] \
        || die "providersync source discovery returned '${test_name}' more than once"
      discovered_names["${test_name}"]=1
      PROVIDER_TEST_NAMES+=("${test_name}")
      if [ "${integration_file}" -eq 1 ]; then
        PROVIDER_INTEGRATION_TEST_NAMES["${test_name}"]=1
      fi
    done < <(
      sed -nE 's/^func[[:space:]]+(Test[A-Za-z0-9_]+)[[:space:]]*\(.*/\1/p' \
        "${source_file}"
    )
  done < <(printf '%s\n' "${files_output}")

  mapfile -t PROVIDER_TEST_NAMES < <(printf '%s\n' "${PROVIDER_TEST_NAMES[@]}" | LC_ALL=C sort)
  [ "${#PROVIDER_TEST_NAMES[@]}" -gt 0 ] \
    || die "providersync source discovery returned zero top-level tests"
  [ "${PROVIDER_TEST_SHARD_COUNT}" -le "${#PROVIDER_TEST_NAMES[@]}" ] \
    || die "provider test shard manifest declares more shards than discovered tests"
  [ "${#PROVIDER_INTEGRATION_TEST_NAMES[@]}" -gt 0 ] \
    || die "providersync source discovery returned zero integration-tagged top-level tests"

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
  done
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
      printf '%s\t%s\n' "${PROVIDER_TEST_WEIGHTS[${test_name}]}" "${test_name}"
    done | LC_ALL=C sort -t $'\t' -k1,1nr -k2,2
  )

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

emit_integration_shard_matrix() {
  local matrix='{"include":[' separator="" shard
  for ((shard = 1; shard <= PROVIDER_TEST_SHARD_COUNT; shard++)); do
    matrix+="${separator}{\"target\":\"providersync\",\"shard\":${shard}}"
    separator=","
  done
  for ((shard = 1; shard <= INTEGRATION_SHARD_COUNT; shard++)); do
    [ "${INTEGRATION_SHARD_NON_PROVIDER_COUNTS[${shard}]}" -gt 0 ] || continue
    matrix+="${separator}{\"target\":\"packages\",\"shard\":${shard}}"
    separator=","
  done
  matrix+=']}'
  printf 'integration shard matrix: %s\n' "${matrix}"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    printf 'matrix=%s\n' "${matrix}" >> "${GITHUB_OUTPUT}"
  fi
}

check_integration_shard_plan() {
  plan_integration_shards
  plan_providersync_test_shards
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
      [ "${INTEGRATION_SHARD_BY_KEY[${key}]:-0}" -eq "${shard}" ] || continue
      run_pkgs+=("${pkg}")
      run_keys+=("${key}")
      selected_count=$((selected_count + 1))
    done
    [ "${#run_pkgs[@]}" -gt 0 ] || continue

    printf 'go test integration package shard %s: %s -> %s\n' \
      "${shard}" "${module_dir}" "${run_pkgs[*]}"
    for key in "${run_keys[@]}"; do
      printf '  SHARD-RUN %s\n' "${key}"
    done
    if [ "${mode}" = "--dry-run" ]; then
      continue
    fi
    (
      cd "${ROOT}/${module_dir}"
      "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -tags=integration -count=1 -timeout=30m "${run_pkgs[@]}"
    )
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
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -tags=integration -count=1 -timeout=30m -run "${test_regex}" ./internal/providersync
  )
}

check_integration_shard() {
  local target="${1:-}" shard="${2:-}" mode="${3:-}"

  case "${target}" in
    packages|providersync) ;;
    *) die "integration-shard TARGET must be 'packages' or 'providersync'" ;;
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
  case "${target}" in
    packages) check_integration_package_shard "${shard}" "${mode}" ;;
    providersync) check_providersync_test_shard "${shard}" "${mode}" ;;
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
    (
      cd "${ROOT}/${module_dir}"
      "${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -tags=integration -count=1 -timeout=30m "${run_pkgs[@]}"
    )
  done
}

check_multi_replica_workers() {
  local proof_dir proof_file measured result
  proof_dir="$(mktemp -d "${TMPDIR:-/tmp}/dev-health-multi-replica.XXXXXX")"
  proof_file="${proof_dir}/measured-jobs"
  result=0
  (
    cd "${ROOT}"
    "${GO_ENV_OFF[@]}" \
      GOWORK=off \
      DEV_HEALTH_MULTI_REPLICA_PROOF="${proof_file}" \
      go test -mod=readonly -tags=integration -count=1 -timeout=5m \
        -run '^TestExplicitQueueMultiReplicaClaimDrainRestart$' \
        ./cmd/dev-health-worker
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
# cmd/dev-health-workerctl/main_integration_test.go didn't after its
# constructor's signature changed underneath it. `go vet -tags=integration
# ./...` needs no Docker and runs in seconds, so it belongs in the fast path
# rather than the Docker-backed integration job.
check_integration_vet() {
  run_in_modules "go vet -tags=integration" go vet -mod=readonly -tags=integration ./...
}

discover_modules
print_modules

case "${1:-all}" in
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
  live-python-oracles)
    check_live_python_oracles
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
    check_live_python_oracles
    check_build
    check_contract
    check_integration_vet
    plan_integration_shards
    plan_providersync_test_shards
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
    check_live_python_oracles
    check_build
    check_contract
    check_integration_vet
    plan_integration_shards
    plan_providersync_test_shards
    check_multi_replica_workers
    ;;
  all)
    check_format
    check_vet
    check_test
    check_race
    check_live_python_oracles
    check_build
    check_contract
    check_integration_vet
    check_integration
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
