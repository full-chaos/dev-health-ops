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

usage() {
  cat <<'EOF'
Usage: ci/check_go.sh [fmt|vet|test|race|fast|all]

  fmt    Check gofmt without modifying files.
  vet    Run go vet ./... in every Go module.
  test   Run go test ./... in every Go module.
  race   Run go test -race ./... in every Go module.
  fast   Run fmt, vet, and test (the local pre-push gate).
  all    Run fmt, vet, test, and race (the full CI gate; default).
EOF
}

die() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 2
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
  fast)
    check_format
    check_vet
    check_test
    ;;
  all)
    check_format
    check_vet
    check_test
    check_race
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
