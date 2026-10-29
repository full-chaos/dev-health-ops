#!/bin/bash
# CHAOS-11439: list (and with --run, run) the Postgres role / grant / posture / migration integration packages a change
# must pass locally before its DELTA. The heavy shards run only in the merge queue; PR #4966 and #4980 reached it
# untested (CHAOS-11410). No CI runs this; the lane shows its output in the DELTA.
#
# THIS IS A HEURISTIC. A posture bug reached through a path outside the trigger list below still reaches the queue.
# SCOPE: the receipt resists ordinary environment and tree state. Deliberate host tampering (planted binaries, git index
# flags, files outside the repo) is out of scope.
# Retire this tool and ci/posture_integration_packages.tsv when the posture / role migration work settles; review by
# 2026-12-01.
#
# Usage: posture_integration.sh [--run] [--paths FILE] [--only PACKAGE] [--test-runner FILE]
#   default paths: git diff --no-renames --name-only -z origin/main...HEAD (cwd repo); BASE <sha> is printed; an unresolvable
#   origin/main exits non-zero with no package line. --paths FILE: NUL-terminated change list (no BASE resolved).
#   --only PACKAGE: run one listed package (the head then says packages=1: a partial run, not a full receipt).
# stdout (list): "BASE <sha|paths-file>" then either "POSTURE <package>" lines or "NONE no posture path changed".
# --run: "RECEIPT-HEAD <sha> clean=yes runner= sha256= go= go_sha256= go_version= go_env=GOOS= GOARCH= GOROOT= GOTOOLDIR=
#   script_sha256= packages=<n> packages_sha256= worktree_sha=<sha>", then one "RECEIPT <pkg> PASS|FAIL <secs>s rc=<n>
#   tests=<n>" per package and "RECEIPT-SUMMARY <sha> pass=<n> fail=<n> packages=<n> partial=<yes|no>". A package is PASS only when `go test -json`
#   exits 0, shows at least one passed test and no failed event; a package with no test run is FAIL.
#   The script re-executes itself under `env -i` with an allowlist (HOME, PATH=/usr/bin:/bin, GOENV=off, GOFLAGS empty,
#   GOWORK=off, fixed cache paths, DOCKER_HOST and a few named TESTCONTAINERS_* vars); the go toolchain runs with
#   PATH=<GOROOT>/bin:/usr/bin:/bin and GOTOOLCHAIN=local. The tests run in a fresh `git worktree add --detach <HEAD sha>`
#   under a mktemp dir, removed on every exit, so the cwd and the caller tree never feed go test. A dirty tree or an
#   error exits non-zero. Runs testcontainers: follow the D6363 caps (capped.sh) and remove your own containers
#   afterwards. The runner is fixed. --test-runner FILE is for tests only: EVERY stdout line starts SIMULATED, no line
#   says RECEIPT, exit is 5.
set -euo pipefail

if [ -z "${POSTURE_CLEAN_ENV:-}" ]; then
  # Only shell builtins run before the re-exec: nothing from the caller PATH is trusted.
  src="${BASH_SOURCE[0]}"
  dir="${src%/*}"
  [ "${dir}" != "${src}" ] || dir="."
  self="$(cd "${dir}" && pwd)/${src##*/}"
  stub_mode=0
  for a in "$@"; do
    if [ "${a}" = "--test-runner" ]; then stub_mode=1; fi
  done
  allow=("HOME=${HOME:-/nonexistent}" "PATH=/usr/bin:/bin" GOENV=off GOFLAGS= GOWORK=off POSTURE_CLEAN_ENV=1)
  # go is resolved from the caller PATH once, here; the receipt names the binary, its hash and its version. Hosted runners
  # install go through setup-go, so no toolchain location is hard-coded.
  first="$(command -v go)" || first=""
  allow+=("POSTURE_GO_FIRST=${first}")
  if [ "${stub_mode}" -eq 1 ]; then
    # A simulated run is never a receipt: it may use the caller's cache dirs (hosted CI has no fixed paths) and its TMPDIR.
    for name in GOMODCACHE GOCACHE GOTMPDIR TMPDIR; do
      if [ -n "${!name+x}" ]; then allow+=("${name}=${!name}"); fi
    done
  fi
  for name in DOCKER_HOST TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX TESTCONTAINERS_RYUK_DISABLED TESTCONTAINERS_HOST_OVERRIDE \
    TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED; do
    if [ -n "${!name+x}" ]; then allow+=("${name}=${!name}"); fi
  done
  exec /usr/bin/env -i "${allow[@]}" /bin/bash "${self}" "$@"
fi

GIT=/usr/bin/git
JQ=/usr/bin/jq
SHA=/usr/bin/sha256sum
MKTEMP=/usr/bin/mktemp
CMP=/usr/bin/cmp
RM=/bin/rm
CHMOD=/bin/chmod
self="${BASH_SOURCE[0]}"
here="${self%/*}"
list_file="${here}/posture_integration_packages.tsv"
# The runner is fixed: no environment variable replaces it, so a receipt cannot be forged by an override.
capped="/home/ubuntu/devhealth/scripts/capped.sh"

run=0
paths=""
stub=""
only=""
while [ $# -gt 0 ]; do
  case "$1" in
    --run) run=1; shift ;;
    --test-runner) [ $# -ge 2 ] || { echo "--test-runner needs a file" >&2; exit 2; }; stub="$2"; shift 2 ;;
    --paths) [ $# -ge 2 ] || { echo "--paths needs a file" >&2; exit 2; }; paths="$2"; shift 2 ;;
    --only) [ $# -ge 2 ] || { echo "--only needs a package" >&2; exit 2; }; only="$2"; shift 2 ;;
    *) echo "usage: posture_integration.sh [--run] [--paths FILE] [--only PACKAGE] [--test-runner FILE]" >&2; exit 2 ;;
  esac
done
pfx=""
[ -z "${stub}" ] || pfx="SIMULATED "

changed_tmp=""
json_tmp=""
wt_root=""
made_dirs=()
cleanup() {
  if [ -n "${wt_root}" ]; then
    "${GIT}" worktree remove --force "${wt_root}/tree" > /dev/null 2>&1 || true
    "${RM}" -rf "${wt_root}"
    "${GIT}" worktree prune > /dev/null 2>&1 || true
  fi
  "${RM}" -f "${changed_tmp}" "${json_tmp}"
  # A Go module cache is read-only: make it writable before removal. Only dirs this script created are listed.
  for d in ${made_dirs[@]+"${made_dirs[@]}"}; do
    "${CHMOD}" -R u+w "${d}" > /dev/null 2>&1 || true
    "${RM}" -rf "${d}"
  done
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP

packages=()
# The list is read strictly: it must end with a newline, and no line may hold a carriage return, a space or a tab.
[ -f "${list_file}" ] || { echo "cannot read ${list_file}" >&2; exit 3; }
if [ -s "${list_file}" ] && [ "$(tail -c1 "${list_file}" | od -An -tx1 | tr -d ' \n')" != "0a" ]; then
  echo "${list_file} does not end with a newline" >&2
  exit 3
fi
while IFS= read -r line; do
  case "${line}" in "" | "#"*) continue ;; esac
  case "${line}" in *[[:space:]]*) echo "${list_file}: a package line holds whitespace or a carriage return" >&2; exit 3 ;; esac
  packages+=("${line}")
done < "${list_file}" || { echo "cannot read ${list_file}" >&2; exit 3; }
[ "${#packages[@]}" -gt 0 ] || { echo "empty package list" >&2; exit 3; }

if [ -n "${paths}" ]; then
  [ -f "${paths}" ] || { echo "paths file not found: ${paths}" >&2; exit 3; }
  base="paths-file"
  changed_src="${paths}"
else
  base="$("${GIT}" rev-parse --verify --quiet 'origin/main^{commit}')" || { echo "origin/main does not resolve; fetch it first" >&2; exit 3; }
  changed_tmp="$("${MKTEMP}")"
  "${GIT}" diff --no-renames --name-only -z "${base}...HEAD" > "${changed_tmp}" || { echo "git diff against ${base} failed" >&2; exit 3; }
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

if [ -n "${only}" ]; then
  selected=""
  for pkg in "${packages[@]}"; do
    if [ "${pkg}" = "${only}" ]; then selected="${pkg}"; fi
  done
  [ -n "${selected}" ] || { echo "--only ${only} is not a listed package" >&2; exit 2; }
  packages=("${selected}")
fi

sha="$("${GIT}" rev-parse HEAD)"
status="$("${GIT}" status --porcelain)" || { echo "git status failed; the tree is not known to be clean" >&2; exit 4; }
if [ -n "${status}" ]; then
  if [ -n "${stub}" ]; then echo "SIMULATED-HEAD ${sha} clean=no" >&2; else echo "RECEIPT-HEAD ${sha} clean=no" >&2; fi
  echo "dirty tree: commit or stash first" >&2
  exit 4
fi

# The tests run in a fresh worktree of HEAD, so uncommitted or index-hidden edits, a parent go.work, or a fake module in
# the cwd never feed go test. The script and the list that run must be the ones committed at HEAD.
wt_root="$("${MKTEMP}" -d)"
"${GIT}" -c core.sparseCheckout=false worktree add --detach "${wt_root}/tree" "${sha}" > /dev/null 2>&1 || { echo "git worktree add failed" >&2; exit 3; }
wt="${wt_root}/tree"
# core.sparseCheckout=false on the add keeps the caller's sparse patterns out of the new worktree without writing to the
# caller's shared .git/config. Then refuse any file the index marks other than a plain cached one (sparse, skip-worktree,
# assume-unchanged), so every test committed at HEAD is on disk.
lsv="$("${GIT}" -C "${wt}" ls-files -v)" || { echo "git ls-files -v failed in the worktree" >&2; exit 3; }
while IFS= read -r entry; do
  case "${entry}" in "H "* | "") ;; *) echo "worktree index flags a file other than a plain cached one: ${entry}" >&2; exit 3 ;; esac
done <<< "${lsv}"
wt_sha="$("${GIT}" -C "${wt}" rev-parse HEAD)" || { echo "cannot read the worktree sha" >&2; exit 3; }
[ "${wt_sha}" = "${sha}" ] || { echo "worktree sha ${wt_sha} is not HEAD ${sha}" >&2; exit 3; }
"${CMP}" -s "${self}" "${wt}/ci/posture_integration.sh" || { echo "this script is not the one committed at HEAD" >&2; exit 3; }
"${CMP}" -s "${list_file}" "${wt}/ci/posture_integration_packages.tsv" || { echo "the package list is not the one committed at HEAD" >&2; exit 3; }

if [ -z "${stub}" ]; then
  [ -f "${capped}" ] && [ -x "${capped}" ] || { echo "runner not found: ${capped}" >&2; exit 3; }
fi
if [ -n "${stub}" ]; then
  gomod="${GOMODCACHE:-}"; gocache="${GOCACHE:-}"; gotmp="${GOTMPDIR:-}"
  if [ -z "${gomod}" ]; then gomod="$("${MKTEMP}" -d)"; made_dirs+=("${gomod}"); fi
  if [ -z "${gocache}" ]; then gocache="$("${MKTEMP}" -d)"; made_dirs+=("${gocache}"); fi
  if [ -z "${gotmp}" ]; then gotmp="$("${MKTEMP}" -d)"; made_dirs+=("${gotmp}"); fi
else
  gomod="/var/lib/oci-cache/go-mod"; gocache="/var/lib/oci-cache/go-build"; gotmp="/var/lib/oci-cache/go-tmp"
fi
go_first="${POSTURE_GO_FIRST:-}"
[ -n "${go_first}" ] && [ -x "${go_first}" ] || { echo "go not found: ${go_first:-<none>}" >&2; exit 3; }
common=("HOME=${HOME:-/nonexistent}" GOENV=off GOFLAGS= GOWORK=off "GOMODCACHE=${gomod}" "GOCACHE=${gocache}" "GOTMPDIR=${gotmp}")
for name in DOCKER_HOST TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX TESTCONTAINERS_RYUK_DISABLED TESTCONTAINERS_HOST_OVERRIDE \
  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED; do
  if [ -n "${!name+x}" ]; then common+=("${name}=${!name}"); fi
done
# The toolchain go.mod selects (a downloaded one when the module is newer than the installed go) is resolved once with
# GOTOOLCHAIN=auto, in the worktree; the tests then run that exact binary with GOTOOLCHAIN=local.
go_root="$(cd "${wt}" && /usr/bin/env -i "${common[@]}" "PATH=${go_first%/*}:/usr/bin:/bin" GOTOOLCHAIN=auto "${go_first}" env GOROOT)" || { echo "cannot resolve the go toolchain" >&2; exit 3; }
go_path="${go_root}/bin/go"
[ -x "${go_path}" ] || { echo "go not found: ${go_path}" >&2; exit 3; }
goenv=(/usr/bin/env -i "${common[@]}" "PATH=${go_root}/bin:/usr/bin:/bin" GOTOOLCHAIN=local)
if [ -n "${stub}" ]; then
  [ -x "${stub}" ] || { echo "test runner is not executable: ${stub}" >&2; exit 3; }
  capped="${stub}"
  printf 'SIMULATED-HEAD %s clean=yes runner=test-runner\n' "${sha}"
  rec="SIMULATED"; sum="SIMULATED-SUMMARY"
else
  runner_sha="$("${SHA}" "${capped}")" || { echo "cannot hash the runner ${capped}" >&2; exit 3; }
  go_sha="$("${SHA}" "${go_path}")" || { echo "cannot hash ${go_path}" >&2; exit 3; }
  script_sha="$("${SHA}" "${self}")" || { echo "cannot hash the script" >&2; exit 3; }
  pkgs_sha="$(printf '%s\n' "${packages[@]}" | "${SHA}")" || { echo "cannot hash the package list" >&2; exit 3; }
  go_version="$(cd "${wt}" && "${goenv[@]}" "${go_path}" version)" || { echo "go version failed" >&2; exit 3; }
  mapfile -t gv < <(cd "${wt}" && "${goenv[@]}" "${go_path}" env GOOS GOARCH GOROOT GOTOOLDIR) || { echo "go env failed" >&2; exit 3; }
  [ "${#gv[@]}" -eq 4 ] || { echo "go env GOOS GOARCH GOROOT GOTOOLDIR did not print 4 values" >&2; exit 3; }
  printf 'RECEIPT-HEAD %s clean=yes runner=%s sha256=%s go=%s go_sha256=%s go_version="%s" go_env=GOOS=%s GOARCH=%s GOROOT=%s GOTOOLDIR=%s script_sha256=%s packages=%s packages_sha256=%s worktree_sha=%s\n' \
    "${sha}" "${capped}" "${runner_sha%% *}" "${go_path}" "${go_sha%% *}" "${go_version}" "${gv[0]}" "${gv[1]}" "${gv[2]}" "${gv[3]}" \
    "${script_sha%% *}" "${#packages[@]}" "${pkgs_sha%% *}" "${wt_sha}"
  rec="RECEIPT"; sum="RECEIPT-SUMMARY"
fi
json_tmp="$("${MKTEMP}")"
pass=0
fail=0
for pkg in "${packages[@]}"; do
  start="${SECONDS}"
  rc=0
  # internal/pgmigrate takes about 705 s at host load 30-39 (CHAOS-11714), above go test's 10 m default: it alone gets
  # -timeout 20m under a 1500 s cap. Every other package keeps the default timeout and the 900 s cap.
  cap_secs=900
  timeout_flag=()
  if [ "${pkg}" = "internal/pgmigrate" ]; then cap_secs=1500; timeout_flag=(-timeout 20m); fi
  (cd "${wt}" && "${goenv[@]}" "${capped}" -t "${cap_secs}" -m 16 -- "${go_path}" test -json -tags=integration -count=1 -p 1 ${timeout_flag[@]+"${timeout_flag[@]}"} "./${pkg}") > "${json_tmp}" 2>/dev/null || rc=$?
  secs=$((SECONDS - start))
  counts="$("${JQ}" -s -r '[([.[] | select(.Action == "pass" and .Test != null)] | length), ([.[] | select(.Action == "fail")] | length)] | @tsv' "${json_tmp}" 2>/dev/null)" || counts="0	1"
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
partial="no"
[ -z "${only}" ] || partial="yes"
printf '%s %s pass=%s fail=%s packages=%s partial=%s\n' "${sum}" "${sha}" "${pass}" "${fail}" "${#packages[@]}" "${partial}"
[ -z "${stub}" ] || { echo "a --test-runner run is not a receipt" >&2; exit 5; }
[ "${fail}" -eq 0 ]
