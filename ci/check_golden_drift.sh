#!/usr/bin/env bash
# Advisory pull-request check for frozen-Python golden drift (ci/golden_drift_registry.tsv).
# A PR that adds a column, a table or a writer guard can fail a frozen-Python golden test that only runs in the merge
# queue. This script selects the golden tests a PR can reach and runs only those.
#
# Usage:
#   check_golden_drift.sh select CHANGED_FILE   CHANGED_FILE = NUL-terminated paths (ci/go_relevant_diff.sh output).
#       stdout: "VERDICT select|none|all <reason>", then one "ROW<TAB>package<TAB>regex" per selected row.
#       FAILS OPEN: a missing, empty, unterminated or malformed list selects every row (verdict all), exit 0.
#   check_golden_drift.sh run ROWS_FILE         ROWS_FILE = the select output. Runs `go test -tags=integration -run` per row.
#       exit 0 = every row passed; 1 = a row failed or matched no test; 2 = usage or registry error.
# Pass or fail is the go test exit code only; no time is measured or asserted.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REGISTRY="${GOLDEN_DRIFT_REGISTRY:-${SCRIPT_DIR}/golden_drift_registry.tsv}"
# @MIG: the migration inputs. Any of them can add a column or a table a golden does not know.
MIG_PATTERNS=('internal/pgmigrate/sql/*' 'internal/pgmigrate/chain_manifest.tsv')
# @SELF: a change to this check itself runs every row.
SELF_PATTERNS=('ci/check_golden_drift.sh' 'ci/golden_drift_registry.tsv')

die() { echo "check_golden_drift.sh: $*" >&2; exit 2; }

load_registry() {
  [ -r "${REGISTRY}" ] || die "registry ${REGISTRY} is not readable"
  rows=()
  local line pkg re trig extra
  while IFS= read -r line || [ -n "${line}" ]; do
    case "${line}" in '' | '#'*) continue ;; esac
    IFS=$'\t' read -r pkg re trig extra <<< "${line}"
    [ -n "${pkg}" ] && [ -n "${re}" ] && [ -n "${trig}" ] && [ -z "${extra}" ] \
      || die "registry row needs exactly 3 non-empty tab separated fields: ${line}"
    rows+=("${pkg}"$'\t'"${re}"$'\t'"${trig}")
  done < "${REGISTRY}"
  [ "${#rows[@]}" -gt 0 ] || die "registry holds no row"
}

emit_all() {
  local row pkg re
  echo "VERDICT all $1"
  for row in "${rows[@]}"; do
    IFS=$'\t' read -r pkg re _ <<< "${row}"
    printf 'ROW\t%s\t%s\n' "${pkg}" "${re}"
  done
}

select_rows() {
  local list="${1:-}" paths=() path n=0 row pkg re trig pat hit picked=0
  load_registry
  if [ -z "${list}" ] || [ ! -f "${list}" ] || [ ! -r "${list}" ] || [ ! -s "${list}" ]; then
    emit_all "bad input: change list missing, unreadable or empty"
    return 0
  fi
  if [ "$(tail -c1 "${list}" | od -An -tx1 | tr -d ' \n')" != "00" ]; then
    emit_all "bad input: change list is not NUL-terminated"
    return 0
  fi
  while IFS= read -r -d '' path; do
    if [ -z "${path}" ]; then
      emit_all "bad input: empty record in change list"
      return 0
    fi
    paths+=("${path}")
    n=$((n + 1))
  done < "${list}"
  local selected=()
  for row in "${rows[@]}"; do
    IFS=$'\t' read -r pkg re trig <<< "${row}"
    hit=0
    local -a pats=()
    IFS=';' read -r -a pats <<< "${trig}"
    local -a expanded=()
    for pat in "${pats[@]}"; do
      case "${pat}" in
        @MIG) expanded+=("${MIG_PATTERNS[@]}") ;;
        @*) die "unknown trigger macro ${pat} in row ${pkg}" ;;
        *) expanded+=("${pat}") ;;
      esac
    done
    expanded+=("${SELF_PATTERNS[@]}")
    for path in "${paths[@]}"; do
      for pat in "${expanded[@]}"; do
        # shellcheck disable=SC2254
        case "${path}" in ${pat}) hit=1 ;; esac
      done
      [ "${hit}" -eq 0 ] || break
    done
    if [ "${hit}" -eq 1 ]; then
      selected+=("${row}")
      picked=$((picked + 1))
    fi
  done
  if [ "${picked}" -eq 0 ]; then
    echo "VERDICT none ${n} changed paths reach no golden row"
    return 0
  fi
  echo "VERDICT select ${picked} of ${#rows[@]} rows for ${n} changed paths"
  for row in "${selected[@]}"; do
    IFS=$'\t' read -r pkg re _ <<< "${row}"
    printf 'ROW\t%s\t%s\n' "${pkg}" "${re}"
  done
}

message() {
  cat >&2 <<MSG
::error title=FROZEN PYTHON GOLDEN DRIFT::$1 $2 differs from the frozen Python output.
The goldens are frozen and have no update command: do NOT edit a golden file.
Add the new Go-only column or guard to the test's exclusion list (or map it in the test), with the reason.
Example: internal/adminops/users_integration_test.go:185 strips 'saml_nameid_format' and 'saml_sp_name_qualifier' from to_jsonb(u) (new users columns).
A new guard that changes a status or body (500 vs 400) needs the case listed in the test as Go-only, with a ruling.
This check is advisory; the same test runs in the merge queue.
MSG
}

out=""
run_rows() {
  local file="${1:-}" pkg re kind failed=0 count=0
  [ -n "${file}" ] && [ -r "${file}" ] || die "run needs a readable ROWS_FILE"
  out="$(mktemp)"
  trap 'rm -f "${out:-}"' EXIT
  while IFS=$'\t' read -r kind pkg re || [ -n "${kind}" ]; do
    [ "${kind}" = "ROW" ] || continue
    [ -n "${pkg}" ] && [ -n "${re}" ] || die "malformed ROW line"
    count=$((count + 1))
    echo "== golden drift: ${pkg} -run ${re}"
    if ! (cd "${ROOT}" && env -u GOFLAGS -u GOEXPERIMENT GOWORK=off go test -mod=readonly -trimpath -tags=integration -count=1 -timeout=30m -v -run "${re}" "./${pkg}") 2>&1 | tee "${out}"; then
      message "${pkg}" "${re}"
      failed=1
    elif ! grep -q '^--- PASS: ' "${out}"; then
      echo "check_golden_drift.sh: -run ${re} matched no passing test in ${pkg}; fix the registry row" >&2
      message "${pkg}" "${re}"
      failed=1
    fi
  done < "${file}"
  [ "${count}" -gt 0 ] || { echo "check_golden_drift.sh: no ROW to run"; return 0; }
  return "${failed}"
}

case "${1:-}" in
  select) [ "$#" -eq 2 ] || die "select accepts CHANGED_FILE"; select_rows "$2" ;;
  run) [ "$#" -eq 2 ] || die "run accepts ROWS_FILE"; run_rows "$2" ;;
  *) die "usage: check_golden_drift.sh select CHANGED_FILE | run ROWS_FILE" ;;
esac
