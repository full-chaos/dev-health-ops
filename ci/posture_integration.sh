#!/usr/bin/env bash
# CHAOS-11439: list (and with --run, run) the Postgres role / grant / posture / migration integration packages a change
# must pass locally before its DELTA. The heavy shards run only in the merge queue; PR #4966 and #4980 reached it
# untested (CHAOS-11410). No CI runs this; the lane shows its output in the DELTA.
#
# THIS IS A HEURISTIC. A posture bug reached through a path outside the trigger list below still reaches the queue.
# Retire this tool and ci/posture_integration_packages.tsv when the posture / role migration work settles; review by
# 2026-12-01.
#
# Usage: posture_integration.sh [--run] [--paths FILE] [--test-runner FILE]
#   default paths: git diff --no-renames --name-only -z origin/main...HEAD (cwd repo); BASE <sha> is printed; an unresolvable
#   origin/main exits non-zero with no package line. --paths FILE: NUL-terminated change list (no BASE resolved).
# stdout (list): "BASE <sha|paths-file>" then either "POSTURE <package>" lines or "NONE no posture path changed".
# --run: "RECEIPT-HEAD <sha> clean=yes runner=<capped.sh> sha256=<hash> go=<path> go_sha256=<hash> go_version=<text>",
#   then one "RECEIPT <pkg> PASS|FAIL <secs>s rc=<n> tests=<n>" per package and "RECEIPT-SUMMARY <sha> pass=<n> fail=<n>".
#   A package is PASS only when `go test -json` exits 0, shows at least one passed test and no failed event; a package
#   with no test run is FAIL. go test runs with GOENV=off, GOFLAGS empty, GOTOOLCHAIN=local and no GIT_DIR, so the
#   caller's environment cannot make it pass without running tests. A dirty tree or an error exits non-zero. Runs
#   testcontainers: follow the D6363 caps (capped.sh) and remove your own containers afterwards. The runner is fixed.
#   --test-runner FILE is for tests only: EVERY stdout line starts SIMULATED, no line says RECEIPT, exit is 5.
set -euo pipefail

unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
list_file="${here}/posture_integration_packages.tsv"
# The runner is fixed: no environment variable replaces it, so a receipt cannot be forged by an override.
capped="/home/ubuntu/devhealth/scripts/capped.sh"

run=0
paths=""
stub=""
while [ $# -gt 0 ]; do
  case "$1" in
    --run) run=1; shift ;;
    --test-runner) [ $# -ge 2 ] || { echo "--test-runner needs a file" >&2; exit 2; }; stub="$2"; shift 2 ;;
    --paths) [ $# -ge 2 ] || { echo "--paths needs a file" >&2; exit 2; }; paths="$2"; shift 2 ;;
    *) echo "usage: posture_integration.sh [--run] [--paths FILE] [--test-runner FILE]" >&2; exit 2 ;;
  esac
done
pfx=""
[ -z "${stub}" ] || pfx="SIMULATED "

changed_tmp=""
json_tmp=""
trap 'rm -f "${changed_tmp}" "${json_tmp}"' EXIT

packages=()
while IFS= read -r line; do
  case "${line}" in "" | "#"*) continue ;; esac
  packages+=("${line}")
done < "${list_file}" || { echo "cannot read ${list_file}" >&2; exit 3; }
[ "${#packages[@]}" -gt 0 ] || { echo "empty package list" >&2; exit 3; }

if [ -n "${paths}" ]; then
  [ -f "${paths}" ] || { echo "paths file not found: ${paths}" >&2; exit 3; }
  base="paths-file"
  changed_src="${paths}"
else
  base="$(git rev-parse --verify --quiet 'origin/main^{commit}')" || { echo "origin/main does not resolve; fetch it first" >&2; exit 3; }
  changed_tmp="$(mktemp)"
  git diff --no-renames --name-only -z "${base}...HEAD" > "${changed_tmp}" || { echo "git diff against ${base} failed" >&2; exit 3; }
  changed_src="${changed_tmp}"
fi

# A non-empty list must end in NUL, like ci/integration_pr_scope.sh; an empty diff is a valid empty change.
if [ -s "${changed_src}" ] && [ "$(tail -c1 "${changed_src}" | od -An -tx1 | tr -d ' \n')" != "00" ]; then
  echo "change list is not NUL-terminated" >&2
  exit 3
fi

hit=""
while IFS= read -r -d '' path; do
  [ -n "${path}" ] || { echo "empty record in change list" >&2; exit 3; }
  [ -n "${hit}" ] && continue
  case "${path}" in
    internal/pgmigrate/sql/* | internal/pgmigrate/baseline/* | internal/rivermigrate/* | internal/testsupport/domainroledb/*) hit="${path}" ;;
    internal/storage/postgres/domain_authorization* | internal/storage/postgres/posture_* | internal/storage/postgres/role*) hit="${path}" ;;
    internal/storage/postgres/authschema/* | internal/storage/postgres/aclguard/* | internal/storage/roleprovision/*) hit="${path}" ;;
    ci/bigboy/pg-grants-check.sh | ci/posture_integration_packages.tsv | ci/posture_integration.sh) hit="${path}" ;;
  esac
  for pkg in "${packages[@]}"; do
    case "${path}" in "${pkg}"/*) hit="${path}" ;; esac
  done
done < "${changed_src}"

printf '%sBASE %s\n' "${pfx}" "${base}"
if [ -z "${hit}" ]; then
  printf '%sNONE no posture path changed\n' "${pfx}"
  exit 0
fi
for pkg in "${packages[@]}"; do printf '%sPOSTURE %s\n' "${pfx}" "${pkg}"; done
[ "${run}" -eq 1 ] || exit 0

sha="$(git rev-parse HEAD)"
status="$(git status --porcelain)" || { echo "git status failed; the tree is not known to be clean" >&2; exit 4; }
[ -z "${status}" ] || { echo "RECEIPT-HEAD ${sha} clean=no" >&2; echo "dirty tree: commit or stash first" >&2; exit 4; }

# go test runs with a fixed environment: no GOENV file, no GOFLAGS (-exec, -run, ...), no toolchain download, no GIT_DIR.
goenv=(env GOENV=off GOFLAGS= GOTOOLCHAIN=local)
go_first="$(command -v go)" || { echo "go not found on PATH" >&2; exit 3; }
# The toolchain go.mod selects (a downloaded one when the module is newer than the installed go) is resolved once with a
# fixed environment; the tests then run that exact binary with GOTOOLCHAIN=local, so no switch happens mid-run.
go_root="$(env GOENV=off GOFLAGS= GOTOOLCHAIN=auto "${go_first}" env GOROOT)" || { echo "cannot resolve the go toolchain" >&2; exit 3; }
go_path="${go_root}/bin/go"
[ -x "${go_path}" ] || { echo "go not found: ${go_path}" >&2; exit 3; }
if [ -n "${stub}" ]; then
  [ -x "${stub}" ] || { echo "test runner is not executable: ${stub}" >&2; exit 3; }
  capped="${stub}"
  printf 'SIMULATED-HEAD %s clean=yes runner=test-runner\n' "${sha}"
  rec="SIMULATED"; sum="SIMULATED-SUMMARY"
else
  [ -f "${capped}" ] && [ -x "${capped}" ] || { echo "runner not found: ${capped}" >&2; exit 3; }
  runner_sha="$(sha256sum "${capped}")" || { echo "cannot hash the runner ${capped}" >&2; exit 3; }
  go_sha="$(sha256sum "${go_path}")" || { echo "cannot hash ${go_path}" >&2; exit 3; }
  go_version="$("${goenv[@]}" "${go_path}" version)" || { echo "go version failed" >&2; exit 3; }
  printf 'RECEIPT-HEAD %s clean=yes runner=%s sha256=%s go=%s go_sha256=%s go_version="%s"\n' \
    "${sha}" "${capped}" "${runner_sha%% *}" "${go_path}" "${go_sha%% *}" "${go_version}"
  rec="RECEIPT"; sum="RECEIPT-SUMMARY"
fi
export GOMODCACHE="${GOMODCACHE:-/var/lib/oci-cache/go-mod}" GOCACHE="${GOCACHE:-/var/lib/oci-cache/go-build}" GOTMPDIR="${GOTMPDIR:-/var/lib/oci-cache/go-tmp}"
json_tmp="$(mktemp)"
pass=0
fail=0
for pkg in "${packages[@]}"; do
  start="${SECONDS}"
  rc=0
  "${goenv[@]}" "${capped}" -t 900 -m 16 -- "${go_path}" test -json -tags=integration -count=1 -p 1 "./${pkg}" > "${json_tmp}" 2>/dev/null || rc=$?
  secs=$((SECONDS - start))
  counts="$(jq -s -r '[([.[] | select(.Action == "pass" and .Test != null)] | length), ([.[] | select(.Action == "fail")] | length)] | @tsv' "${json_tmp}" 2>/dev/null)" || counts="0	1"
  tests="${counts%%$'\t'*}"
  failed="${counts##*$'\t'}"
  if [ "${rc}" -eq 0 ] && [ "${tests}" -ge 1 ] && [ "${failed}" -eq 0 ]; then
    pass=$((pass + 1))
    printf '%s %s PASS %ss rc=0 tests=%s\n' "${rec}" "${pkg}" "${secs}" "${tests}"
  else
    fail=$((fail + 1))
    printf '%s %s FAIL %ss rc=%s tests=%s failed_events=%s\n' "${rec}" "${pkg}" "${secs}" "${rc}" "${tests}" "${failed}"
  fi
done
printf '%s %s pass=%s fail=%s\n' "${sum}" "${sha}" "${pass}" "${fail}"
[ -z "${stub}" ] || { echo "a --test-runner run is not a receipt" >&2; exit 5; }
[ "${fail}" -eq 0 ]
