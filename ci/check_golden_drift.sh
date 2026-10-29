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

# split_tabs SEP COUNT LINE: sets F1..FCOUNT. Splits on exact tabs and keeps empty fields (a `read` with a tab IFS merges
# adjacent tabs, so an empty field would be read as the next one). Refuses a line without exactly COUNT non-empty fields.
F1="" F2="" F3=""
split_tabs() {
  local count="$1" line="$2" tabs rest
  tabs="${line//[!$'\t']/}"
  [ "${#tabs}" -eq $((count - 1)) ] || die "needs exactly ${count} tab separated fields: ${line}"
  F1="${line%%$'\t'*}"
  rest="${line#*$'\t'}"
  if [ "${count}" -eq 2 ]; then
    F2="${rest}"
  else
    F2="${rest%%$'\t'*}"
    F3="${rest#*$'\t'}"
  fi
  if [ -z "${F1}" ] || [ -z "${F2}" ] || { [ "${count}" -eq 3 ] && [ -z "${F3}" ]; }; then
    die "needs ${count} non-empty fields: ${line}"
  fi
}

load_registry() {
  [ -r "${REGISTRY}" ] || die "registry ${REGISTRY} is not readable"
  rows=()
  local line
  while IFS= read -r line || [ -n "${line}" ]; do
    case "${line}" in '' | '#'*) continue ;; esac
    split_tabs 3 "${line}"
    safe_pkg "${F1}"
    rows+=("${F1}"$'\x1f'"${F2}"$'\x1f'"${F3}")
  done < "${REGISTRY}"
  [ "${#rows[@]}" -gt 0 ] || die "registry holds no row"
}

# The ROW line format lives here and only here: emit_row writes it, parse_row reads it. A line that starts with ROW and
# does not parse is a usage error, never a skipped line.
emit_row() { printf 'ROW\t%s\t%s\n' "$1" "$2"; }
ROW_PKG="" ROW_RE=""
# verdict_reason TEXT: the reason of a VERDICT line must hold a non-space character.
verdict_reason() {
  [[ "$1" =~ [^[:space:]] ]] || die "the VERDICT reason is blank"
}
# safe_pkg PKG: a package is a relative path below the repository: no absolute path, no ".." segment. The registry and
# the ROW parser apply the same rule, so select cannot emit a package that run refuses.
safe_pkg() {
  case "$1" in
    /*) die "package must be relative to the repository: $1" ;;
  esac
  case "/$1/" in
    */../*) die "package must not contain a .. segment: $1" ;;
  esac
}
parse_row() {
  split_tabs 3 "$1"
  [ "${F1}" = "ROW" ] || die "malformed ROW line: $1"
  safe_pkg "${F2}"
  ROW_PKG="${F2}"
  ROW_RE="${F3}"
}

emit_all() {
  local row pkg re
  echo "VERDICT all $1"
  for row in "${rows[@]}"; do
    pkg="${row%%$'\x1f'*}"
    re="${row#*$'\x1f'}"
    re="${re%%$'\x1f'*}"
    emit_row "${pkg}" "${re}"
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
    pkg="${row%%$'\x1f'*}"
    trig="${row##*$'\x1f'}"
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
    pkg="${row%%$'\x1f'*}"
    re="${row#*$'\x1f'}"
    re="${re%%$'\x1f'*}"
    emit_row "${pkg}" "${re}"
  done
}

message() {
  printf '%s\n' \
    "::error title=FROZEN PYTHON GOLDEN DRIFT::$1 -run $2 differs from the frozen Python output." \
    "The goldens are frozen and have no update command: do NOT edit a golden file." \
    "Add the new Go-only column or guard to the test's exclusion list (or map it in the test), with the reason." \
    "Example: internal/adminops/users_integration_test.go:185 strips 'saml_nameid_format' and 'saml_sp_name_qualifier' from to_jsonb(u) (new users columns)." \
    "A new guard that changes a status or body (500 vs 400) needs the case listed in the test as Go-only, with a ruling." \
    "This check is advisory; the same test runs in the merge queue." >&2
}

out=""
run_rows() {
  local file="${1:-}" line failed=0 rc i verdict="" verdicts=0 prefixed=0 tmo
  local -a pkgs=() res=()
  [ -n "${file}" ] && [ -r "${file}" ] || die "run needs a readable ROWS_FILE"
  trap 'rm -f "${out:-}"' EXIT
  # Bytes the shell read would drop or reinterpret are refused before it reads: a NUL byte (read drops it, so Test\0X would
  # run as TestX) and a missing final newline (select always ends its last line with one).
  [ "$(tr -d '\000' < "${file}" | wc -c)" -eq "$(wc -c < "${file}")" ] || die "NUL byte in the rows file"
  [ "$(tail -c1 "${file}" | od -An -tx1 | tr -d ' \n')" = "0a" ] || die "the rows file does not end with a newline"
  # Pass 1 reads the whole file before any test runs and accepts EXACTLY what `select` emits: one well-formed
  # ROW<TAB>package<TAB>regex line per row and exactly one "VERDICT select|none|all <reason>" line. select never prints an
  # empty line, a carriage return or any other text, so each of those exits 2 and runs nothing, as does a count of parsed
  # rows that differs from the ROW-prefixed lines (the workflow counts those with grep -c '^ROW').
  while IFS= read -r line || [ -n "${line}" ]; do
    case "${line}" in
      *$'\r'*) die "carriage return in the rows file: ${line}" ;;
      "") die "empty line in the rows file" ;;
      ROW*)
        prefixed=$((prefixed + 1))
        case "${line}" in
          ROW$'\t'*) parse_row "${line}"; pkgs+=("${ROW_PKG}"); res+=("${ROW_RE}") ;;
        esac
        ;;
      "VERDICT none "?*) verdict_reason "${line#VERDICT none }"; verdicts=$((verdicts + 1)); verdict="none" ;;
      "VERDICT select "?*) verdict_reason "${line#VERDICT select }"; verdicts=$((verdicts + 1)); verdict="rows" ;;
      "VERDICT all "?*) verdict_reason "${line#VERDICT all }"; verdicts=$((verdicts + 1)); verdict="rows" ;;
      *) die "unexpected line in the rows file: ${line}" ;;
    esac
  done < "${file}"
  [ "${verdicts}" -eq 1 ] || die "the rows file needs exactly one VERDICT line with a reason, has ${verdicts}"
  [ "${prefixed}" -eq "${#pkgs[@]}" ] || die "${prefixed} ROW-prefixed line(s) but ${#pkgs[@]} well-formed row(s): nothing runs"
  if [ "${#pkgs[@]}" -eq 0 ]; then
    [ "${verdict}" = "none" ] || die "VERDICT ${verdict} but no ROW: nothing would run"
    echo "check_golden_drift.sh: VERDICT none, no ROW to run"
    return 0
  fi
  [ "${verdict}" != "none" ] || die "VERDICT none beside ${#pkgs[@]} ROW line(s)"
  for i in "${!pkgs[@]}"; do
    echo "== golden drift: ${pkgs[i]} -run ${res[i]}"
    out="$(mktemp)"
    rc=0
    tmo="$(bash "${ROOT}/ci/check_go.sh" job-timeout 22)" || exit 2
    (cd "${ROOT}" && env -u GOFLAGS -u GOEXPERIMENT GOWORK=off go test -mod=readonly -trimpath -tags=integration -count=1 -timeout="${tmo}" -v -run "${res[i]}" "./${pkgs[i]}") >> "${out}" 2>&1 || rc=$?
    cat "${out}"
    if [ "${rc}" -ne 0 ]; then
      message "${pkgs[i]}" "${res[i]}"
      failed=1
    elif ! grep -q '^--- PASS: ' "${out}"; then
      echo "check_golden_drift.sh: -run ${res[i]} matched no passing test in ${pkgs[i]}; fix the registry row" >&2
      message "${pkgs[i]}" "${res[i]}"
      failed=1
    fi
    rm -f "${out}"
  done
  return "${failed}"
}

case "${1:-}" in
  select) [ "$#" -eq 2 ] || die "select accepts CHANGED_FILE"; select_rows "$2" ;;
  run) [ "$#" -eq 2 ] || die "run accepts ROWS_FILE"; run_rows "$2" ;;
  *) die "usage: check_golden_drift.sh select CHANGED_FILE | run ROWS_FILE" ;;
esac
