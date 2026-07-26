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

usage() {
  cat <<'EOF'
Usage: ci/check_go.sh [fmt|vet|test|race|live-python-oracles|build|contract|integration-vet|integration-coverage|integration|fast|all]

  fmt    Check gofmt without modifying files.
  vet    Run go vet ./... in every Go module.
  test   Run go test ./... in every Go module.
  race   Run go test -race ./... in every Go module.
  live-python-oracles
         Run `go test -count=1 ./internal/providersync/...` unconditionally
         (cache lookup disabled by -count=1 itself, not by any assumption
         about cache state). Separate from `test` because that package
         executes real production Python files (src/dev_health_ops/**.py)
         at test time, which `//go:embed` cannot make part of the Go test
         cache key -- `test`'s bare `go test ./...` can return a stale
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
         passing package's output.
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
  integration
         Discover and run EVERY integration-tagged package's suite against
         real containers, except the (small, justified) INTEGRATION_DENYLIST.
         Inclusion is the default; exclusion is the explicit, loud exception.
  fast   Run fmt, vet, test, live-python-oracles, build, contract,
         integration-vet, and integration-coverage checks, then publish
         the grant advisory report.
  all    Run fmt, vet, test, race, live-python-oracles, build, contract,
         integration-vet, and integration-coverage checks, then publish
         the grant advisory report (default).
EOF
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
  # internal/providersync executes REAL production Python files
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
  printf 'go test -count=1: internal/providersync (live Python oracle sources are outside the Go embed/cache boundary)\n'
  (cd "${ROOT}" && GOWORK=off go test -mod=readonly -count=1 ./internal/providersync/...)
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

# discover_integration_packages populates INTEGRATION_PACKAGES_BY_MODULE
# (module_dir -> newline-separated list of module-relative "./pkg/dir"
# entries) with every package that has at least one tracked or untracked,
# non-vendor *_test.go file whose leading build-constraint comment names the
# "integration" tag. This is a repo-wide scan for the literal constraint
# every integration suite in this tree uses today
# (`//go:build integration`), checked via git's own file listing so it
# honours the same tracked/untracked/exclude-standard rules as the rest of
# this script -- not `find`, and not a hand-maintained list.
discover_integration_packages() {
  local module_dir go_file dir
  declare -gA INTEGRATION_PACKAGES_BY_MODULE=()

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
    if [ "${#found[@]}" -gt 0 ]; then
      INTEGRATION_PACKAGES_BY_MODULE["${module_dir}"]="$(printf '%s\n' "${found[@]}" | sort -u)"
    fi
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
  local module_dir pkg key reason
  local -a denylist_seen=()
  local total=0

  printf 'integration coverage: discovered packages (module: package)\n'
  for module_dir in "${MODULE_DIRS[@]}"; do
    [ -n "${INTEGRATION_PACKAGES_BY_MODULE[${module_dir}]:-}" ] || continue
    while IFS= read -r pkg; do
      [ -n "${pkg}" ] || continue
      total=$((total + 1))
      key="${module_dir}/${pkg#./}"
      key="${key#./}"
      if reason="$(integration_denylist_reason "${key}")"; then
        denylist_seen+=("${key}")
        printf '  SKIP %s: %s\n' "${key}" "${reason}"
      else
        printf '  RUN  %s\n' "${key}"
      fi
    done <<<"${INTEGRATION_PACKAGES_BY_MODULE[${module_dir}]}"
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

check_integration() {
  check_integration_coverage
  local module_dir pkg key reason
  local -a run_pkgs=()

  for module_dir in "${MODULE_DIRS[@]}"; do
    [ -n "${INTEGRATION_PACKAGES_BY_MODULE[${module_dir}]:-}" ] || continue
    run_pkgs=()
    while IFS= read -r pkg; do
      [ -n "${pkg}" ] || continue
      key="${module_dir}/${pkg#./}"
      key="${key#./}"
      if reason="$(integration_denylist_reason "${key}")"; then
        continue
      fi
      run_pkgs+=("${pkg}")
    done <<<"${INTEGRATION_PACKAGES_BY_MODULE[${module_dir}]}"
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
    check_integration_coverage
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
    check_integration_coverage
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
