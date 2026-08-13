#!/usr/bin/env bash
# Run the repository's Go quality gates across the root module and every
# checked-in nested module (including the River N-1 compatibility module).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
GO_TOOLCHAIN="go1.25.9"
export GOTOOLCHAIN="${GO_TOOLCHAIN}"
DEV_HEALTH_GO_CACHE="${DEV_HEALTH_GO_CACHE:-${TMPDIR:-/tmp}/dev-health-go-build-cache}"
mkdir -p "${DEV_HEALTH_GO_CACHE}"
export GOCACHE="${DEV_HEALTH_GO_CACHE}"
DEV_HEALTH_GO_BUILD_OUTPUT=""
DEV_HEALTH_GO_BUILD_TEMP_ROOT=""
DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="${DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST:-${ROOT}/ci/go_integration_shards.tsv}"
DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST="${DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST:-${ROOT}/ci/go_providersync_test_shards.tsv}"
INTEGRATION_CONTAINER_HARNESS="${ROOT}/internal/testsupport/containers/harness.go"

usage() {
  # Backticks in the literal help text document commands; they are not substitutions.
  # shellcheck disable=SC2016
  printf '%s\n' 'Usage: ci/check_go.sh [fmt|vet|test|race|live-python-oracles|build|contract|integration-vet|integration-coverage|integration-shard-plan|integration-prepull|integration-shard|integration|fast|all]

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
         the oracles at all, so it MUST stay in `all` (and `fast`, since
         it is cheap) rather than being treated as an extra, skippable
         step.
  build  Run go build ./... in every Go module.
  contract
         Validate the job contract tree and, when DEV_HEALTH_CONTRACT_BASE is
         set, reject breaking in-place changes against that directory.
  grant-advisory
         Publish the per-role grant-surface ADVISORY report into the log. Reports
         only -- it never fails the build on findings, by design. Included in
         `fast` and `all` because the report has no other delivery channel: the
         test that produces it uses t.Log, and `go test` without -v discards a
         passing package'\''s output.
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
  integration-prepull
         Pre-pull the exact pinned ClickHouse image declared by the Go test
         container harness, retrying transient registry failures at most three
         times before failing loudly. CI runs this before each real shard.
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
         integration-vet, and the integration shard-plan checks, then
         publish the grant advisory report.
  all    Run fmt, vet, test, race, live-python-oracles, build, contract,
         integration-vet, and the integration shard-plan checks, then
         publish the grant advisory report (default).'
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
      GOWORK=off "$@"
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

  printf 'go test -count=1: internal/synccoverage (live Python coverage builder is outside the Go embed/cache boundary)\n'
  if ! (
    cd "${ROOT}"
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

# check_grant_advisory publishes the per-role grant-surface ADVISORY report into
# the CI log. It is NOT a gate and cannot fail the build on findings: the command
# exits 0 even when it reports CRITICAL ones, deliberately (see
# cmd/dev-health-grantcheck's doc comment and internal/domaingrants.AdvisoryReport).
#
# It exists because the report had NO delivery channel. Its content is produced by
# TestReportCoordinatorGrantSurface through t.Log, and check_test below runs
# `go test ./...` WITHOUT -v, which discards a passing package's logs entirely. So
# CI showed a zero exit and a package-level "ok" and none of the report -- which is
# exactly the "advisory output read as a pass" failure the advisory posture exists
# to prevent. A report whose only channel is suppressed output is not a report.
#
# An EXECUTION failure (build break, analysis error) is still nonzero and still
# fails, because that means the report was not produced at all -- which is a
# different thing from the report having nothing to say.
check_grant_advisory() {
  printf '\n== grant-surface ADVISORY report (reports only; never fails on findings) ==\n'
  ( cd "${ROOT}" && GOWORK=off go run -mod=readonly ./cmd/dev-health-grantcheck -roles )
}

check_contract() {
  local contract_root="${ROOT}/contracts/jobs/v1"
  local contract_base="${DEV_HEALTH_CONTRACT_BASE:-}"

  [ -d "${contract_root}" ] || die "missing job contract tree ${contract_root}"
  printf 'job contracts: validate\n'
  (
    cd "${ROOT}"
    GOWORK=off go run -mod=readonly ./cmd/worker-contractcheck \
      validate --root "${contract_root}"
  )

  if [ -n "${contract_base}" ]; then
    [ -d "${contract_base}" ] \
      || die "DEV_HEALTH_CONTRACT_BASE is not a directory: ${contract_base}"
    printf 'job contracts: compare %s\n' "${contract_base}"
    (
      cd "${ROOT}"
      GOWORK=off go run -mod=readonly ./cmd/worker-contractcheck \
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
# names the "integration" tag. This is a repo-wide scan for the literal constraint
# every integration suite in this tree uses today
# (`//go:build integration`), checked via git's own file listing so it
# honours the same tracked/untracked/exclude-standard rules as the rest of
# this script -- not `find`, and not a hand-maintained list.
discover_integration_packages() {
  local module_dir go_file dir
  declare -ga INTEGRATION_PACKAGE_MODULES=()
  declare -ga INTEGRATION_PACKAGES=()

  for module_dir in "${MODULE_DIRS[@]}"; do
    local -a found=()
    while IFS= read -r -d '' go_file; do
      [ -f "${ROOT}/${module_dir}/${go_file}" ] || continue
      case "${go_file}" in
        vendor/*|*/vendor/*) continue ;;
      esac
      # Matches `//go:build integration` and combined constraints such as
      # `//go:build integration && !windows`; go:build lines are always a
      # single line, so a plain grep for the tag token on that line is exact
      # for this repository's usage and needs no build-constraint parser.
      if grep -qE '^//go:build.*(^|[^[:alnum:]_])integration([^[:alnum:]_]|$)' \
        "${ROOT}/${module_dir}/${go_file}"; then
        dir="$(dirname -- "${go_file}")"
        found+=("./${dir}")
      fi
    done < <(
      git -C "${ROOT}/${module_dir}" ls-files --cached --others --exclude-standard -z -- '*_test.go'
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

# Go's own test listing is the executable-name authority. Source inspection is
# used only to classify those names by cost, and only across files `go list`
# says are active for the integration build tag on this runner.
discover_providersync_tests() {
  local list_output files_output go_file source_file test_name
  declare -A discovered_names=()

  if ! list_output="$(
    cd "${ROOT}"
    GOWORK=off go test -mod=readonly -tags=integration -count=1 -run '^$' -list '^Test' ./internal/providersync
  )"; then
    die "failed to discover providersync top-level tests with go test -list"
  fi

  PROVIDER_TEST_NAMES=()
  while IFS= read -r test_name; do
    case "${test_name}" in
      Test*[![:alnum:]_]*)
        die "providersync test discovery returned unsupported top-level test name '${test_name}'"
        ;;
      Test*) ;;
      *) continue ;;
    esac
    [ -z "${discovered_names[${test_name}]+set}" ] \
      || die "providersync test discovery returned '${test_name}' more than once"
    discovered_names["${test_name}"]=1
    PROVIDER_TEST_NAMES+=("${test_name}")
  done < <(printf '%s\n' "${list_output}" | LC_ALL=C sort)

  [ "${#PROVIDER_TEST_NAMES[@]}" -gt 0 ] \
    || die "providersync test discovery returned zero top-level tests"
  [ "${PROVIDER_TEST_SHARD_COUNT}" -le "${#PROVIDER_TEST_NAMES[@]}" ] \
    || die "provider test shard manifest declares more shards than discovered tests"

  if ! files_output="$(
    cd "${ROOT}"
    GOWORK=off go list -mod=readonly -tags=integration \
      -f '{{range .TestGoFiles}}{{println .}}{{end}}{{range .XTestGoFiles}}{{println .}}{{end}}' \
      ./internal/providersync
  )"; then
    die "failed to discover active providersync test files with go list"
  fi

  PROVIDER_INTEGRATION_TEST_NAMES=()
  while IFS= read -r go_file; do
    [ -n "${go_file}" ] || continue
    case "${go_file}" in
      */*|..*) die "go list returned unsafe providersync test filename '${go_file}'" ;;
    esac
    source_file="${ROOT}/${PROVIDER_INTEGRATION_PACKAGE_KEY}/${go_file}"
    [ -f "${source_file}" ] \
      || die "go list returned missing providersync test file '${go_file}'"
    grep -qE '^//go:build.*(^|[^[:alnum:]_])integration([^[:alnum:]_]|$)' \
      "${source_file}" || continue
    while IFS= read -r test_name; do
      [ -n "${test_name}" ] || continue
      [ -z "${PROVIDER_INTEGRATION_TEST_NAMES[${test_name}]+set}" ] \
        || die "integration-tagged providersync source declares '${test_name}' more than once"
      PROVIDER_INTEGRATION_TEST_NAMES["${test_name}"]=1
    done < <(
      sed -nE 's/^func[[:space:]]+(Test[A-Za-z0-9_]+)[[:space:]]*\(.*/\1/p' \
        "${source_file}"
    )
  done <<< "${files_output}"

  [ "${#PROVIDER_INTEGRATION_TEST_NAMES[@]}" -gt 0 ] \
    || die "providersync source discovery returned zero integration-tagged top-level tests"
  for test_name in "${!PROVIDER_INTEGRATION_TEST_NAMES[@]}"; do
    [ -n "${discovered_names[${test_name}]+set}" ] \
      || die "integration-tagged providersync source test '${test_name}' is absent from go test -list"
  done

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

# Read the test dependency from the production Go harness instead of copying a
# digest into the workflow. Exact hosted evidence for this retry is PR #1735,
# run 31524982512, job 93891310235: Docker Hub returned 502 for this pinned
# manifest and testcontainers made no second pull attempt. The bounded pre-pull
# keeps the immutable digest contract and makes each registry attempt visible.
discover_pinned_clickhouse_image() {
  local image
  local -a images=()

  [ -f "${INTEGRATION_CONTAINER_HARNESS}" ] \
    || die "integration container harness not found: ${INTEGRATION_CONTAINER_HARNESS}"
  while IFS= read -r image; do
    [ -n "${image}" ] || continue
    images+=("${image}")
  done < <(
    sed -nE \
      's/^[[:space:]]*ClickHouseImage[[:space:]]*=[[:space:]]*"([^"]+)".*$/\1/p' \
      "${INTEGRATION_CONTAINER_HARNESS}"
  )

  [ "${#images[@]}" -eq 1 ] \
    || die "expected exactly one ClickHouseImage declaration in ${INTEGRATION_CONTAINER_HARNESS}, found ${#images[@]}"
  image="${images[0]}"
  if [[ ! "${image}" =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]]; then
    die "ClickHouseImage must be pinned by a full sha256 digest, got '${image}'"
  fi
  printf '%s\n' "${image}"
}

check_integration_prepull() {
  local image attempt delay
  local max_attempts=3

  command -v docker >/dev/null 2>&1 || die "docker is required for integration-prepull"
  image="$(discover_pinned_clickhouse_image)"
  for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    printf 'pre-pull pinned ClickHouse image %s (attempt %d/%d)\n' \
      "${image}" "${attempt}" "${max_attempts}"
    if docker pull "${image}"; then
      printf 'pre-pulled pinned ClickHouse image %s on attempt %d/%d\n' \
        "${image}" "${attempt}" "${max_attempts}"
      return 0
    fi
    if [ "${attempt}" -eq "${max_attempts}" ]; then
      printf 'ERROR: failed to pre-pull pinned ClickHouse image %s after %d attempts\n' \
        "${image}" "${max_attempts}" >&2
      return 1
    fi
    delay=$((attempt * 5))
    printf 'WARN: ClickHouse image pull attempt %d/%d failed; retrying in %ds\n' \
      "${attempt}" "${max_attempts}" "${delay}" >&2
    sleep "${delay}"
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
      GOWORK=off go test -mod=readonly -tags=integration -count=1 -timeout=30m "${run_pkgs[@]}"
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
    GOWORK=off go test -mod=readonly -tags=integration -count=1 -timeout=30m -run "${test_regex}" ./internal/providersync
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
      GOWORK=off go test -mod=readonly -tags=integration -count=1 -timeout=30m "${run_pkgs[@]}"
    )
  done
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
  grant-advisory)
    check_grant_advisory
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
    check_grant_advisory
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
    plan_integration_shards
    plan_providersync_test_shards
    check_grant_advisory
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
