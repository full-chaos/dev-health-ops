#!/usr/bin/env bash
# CHAOS-11439: list (and with --run, run) the Postgres role / grant / posture / migration integration packages a change
# must pass locally before its DELTA. The heavy shards run only in the merge queue; PR #4966 and #4980 reached it
# untested (CHAOS-11410). No CI runs this; the lane shows its output in the DELTA.
#
# THIS IS A HEURISTIC. A posture bug reached through a path outside the trigger list below still reaches the queue.
# Retire this tool and ci/posture_integration_packages.txt when the posture / role migration work settles; review by
# 2026-12-01.
#
# Usage: posture_integration.sh [--run] [--paths FILE]
#   default paths: git diff --name-only -z origin/main...HEAD (cwd repo); BASE <sha> is printed; an unresolvable
#   origin/main exits non-zero with no package line. --paths FILE: NUL-terminated change list (no BASE resolved).
# stdout (list): "BASE <sha|paths-file>" then either "POSTURE <package>" lines or "NONE no posture path changed".
# --run: "RECEIPT-HEAD <sha> clean=yes", then one "RECEIPT <pkg> PASS|FAIL <secs>s rc=<n>" per package and
#   "RECEIPT-SUMMARY <sha> pass=<n> fail=<n>". A dirty tree or an error exits non-zero. Runs testcontainers: follow the
#   D6363 caps (capped.sh below) and remove your own containers afterwards.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
list_file="${here}/posture_integration_packages.txt"
capped="${POSTURE_CAPPED_SH:-/home/ubuntu/devhealth/scripts/capped.sh}"

run=0
paths=""
while [ $# -gt 0 ]; do
  case "$1" in
    --run) run=1; shift ;;
    --paths) [ $# -ge 2 ] || { echo "--paths needs a file" >&2; exit 2; }; paths="$2"; shift 2 ;;
    *) echo "usage: posture_integration.sh [--run] [--paths FILE]" >&2; exit 2 ;;
  esac
done

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
  trap 'rm -f "${changed_tmp}"' EXIT
  git diff --name-only -z "${base}...HEAD" > "${changed_tmp}" || { echo "git diff against ${base} failed" >&2; exit 3; }
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
    ci/bigboy/pg-grants-check.sh | ci/posture_integration_packages.txt | ci/posture_integration.sh) hit="${path}" ;;
  esac
  for pkg in "${packages[@]}"; do
    case "${path}" in "${pkg}"/*) hit="${path}" ;; esac
  done
done < "${changed_src}"

printf 'BASE %s\n' "${base}"
if [ -z "${hit}" ]; then
  echo "NONE no posture path changed"
  exit 0
fi
for pkg in "${packages[@]}"; do printf 'POSTURE %s\n' "${pkg}"; done
[ "${run}" -eq 1 ] || exit 0

sha="$(git rev-parse HEAD)"
[ -z "$(git status --porcelain)" ] || { echo "RECEIPT-HEAD ${sha} clean=no" >&2; echo "dirty tree: commit or stash first" >&2; exit 4; }
printf 'RECEIPT-HEAD %s clean=yes\n' "${sha}"
export GOMODCACHE="${GOMODCACHE:-/var/lib/oci-cache/go-mod}" GOCACHE="${GOCACHE:-/var/lib/oci-cache/go-build}" GOTMPDIR="${GOTMPDIR:-/var/lib/oci-cache/go-tmp}"
pass=0
fail=0
for pkg in "${packages[@]}"; do
  start="${SECONDS}"
  rc=0
  "${capped}" -t 900 -m 16 -- go test -tags=integration -count=1 -p 1 "./${pkg}" > /dev/null 2>&1 || rc=$?
  secs=$((SECONDS - start))
  if [ "${rc}" -eq 0 ]; then
    pass=$((pass + 1))
    printf 'RECEIPT %s PASS %ss rc=0\n' "${pkg}" "${secs}"
  else
    fail=$((fail + 1))
    printf 'RECEIPT %s FAIL %ss rc=%s\n' "${pkg}" "${secs}" "${rc}"
  fi
done
printf 'RECEIPT-SUMMARY %s pass=%s fail=%s\n' "${sha}" "${pass}" "${fail}"
[ "${fail}" -eq 0 ]
