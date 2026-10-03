#!/usr/bin/env bash
# python_free_ratchet.sh -- the closed list of Go tests that still start Python (CHAOS-7384).
#
#   ci/python_free_ratchet.sh classify GO_TEST_JSON HITS_OUT [TRIPWIRE_LOG]
#   ci/python_free_ratchet.sh compare  KNOWN_TSV HITS_DIR
#   ci/python_free_ratchet.sh state    KNOWN_TSV
#
# The go-python-free job runs the integration shards AND the untagged unit tests with Python unreachable
# (ci/python_tripwire.sh).
# Tests that still start Python fail. Red must not become normal, so the failures are held to a list:
#
#   classify   reads one `go test -json` stream. A failed top-level test whose output (or whose
#              subtests' output) names the tripwire is a HIT: "<package>\t<TestName>" goes to HITS_OUT.
#              Any OTHER failure (a real regression, a build failure, a package-level failure) is
#              printed as "NON-TRIPWIRE FAILURE" and the exit status is 1. With a tripwire log it also
#              fails when the log shows a Python start by a test binary that has no HIT: a test that
#              swallowed the failure and passed is still a Python start.
#   compare    unions every HITS file in HITS_DIR and compares it with the closed list
#              (ci/python_free_known.tsv: package<TAB>TestName<TAB>CHAOS-ticket<TAB>class). Exit 0 only when
#              the HIT set equals the list. A HIT not on the list is NEW (a new Python start); a listed
#              test that did not hit is STALE (it was frozen: remove the entry in that PR). The summary
#              line prints listed, hit, new and stale. PYTHON_FREE_REPORT_ONLY=1 (the provisional first
#              run, before the list is filled in from a measured run) prints every HIT and exits 0.
#   state      the closed list's own state (CHAOS-8324): `rows=N` for a list with rows, `closed-empty` for a list that
#              has its header and no row (every listed test is frozen: the ratchet is CLOSED), and exit 2 for a file
#              without its header line (a truncated file is not an empty list). An empty list is a defined state, not
#              an absence: compare of an empty list needs the header too (else exit 2), and a scope run of it writes an
#              empty hits marker so compare sees "reported, no hit"; the full measurement (every shard and the unit leg)
#              is the check on main and still fails on any new hit.
set -euo pipefail

# STRONG marker: the shim's own line, or the nonexistent interpreter path. WEAK marker ("exit status 97", all
# a test prints when it shows only the error of a shim it ran) counts only in a package whose test binary
# the tripwire log shows starting Python: a test that merely runs `exit 97` is a real failure.
MARKER='PYTHON TRIPWIRE|python-tripwire[.][A-Za-z0-9]+/'
WEAK_MARKER='exit status 97'
# A skip whose reason says Python is missing, and does not blame the live-oracle gate (those skips are
# the gated oracles and are expected).
SKIP_MARKER='(?i)(neither python|no python|python[0-9.]*.*(not on path|not found|is missing|unavailable|cannot be)|(not on path|not found|missing).*python)'
SKIP_GATE='DEV_HEALTH_LIVE_PYTHON_ORACLE'

die() { printf 'python_free_ratchet: %s\n' "$1" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || die "jq is required"

# LIST_HEADER is the first line every closed list starts with; a file without it is truncated or not the list.
LIST_HEADER='# Closed list of Go tests that still start Python'

# list_rows prints the number of rows (non-blank, non-comment lines) of the closed list.
list_rows() {
  grep -cv -e '^#' -e '^[[:space:]]*$' "${1}" || true
}

# require_list_header dies unless the list's first line is the header: an empty list is DECLARED by a file that has its
# header and no row, so a truncated file (header gone) must never read as "empty".
require_list_header() {
  local known="${1:?require_list_header needs KNOWN_TSV}" first
  [ -f "${known}" ] || die "the closed list ${known} does not exist"
  IFS= read -r first <"${known}" || true
  case "${first}" in
    "${LIST_HEADER}"*) ;;
    *) die "the closed list ${known} does not start with its header line ('${LIST_HEADER}'): a truncated file is not an empty list" ;;
  esac
}

# state prints the closed list's state: rows=N, or closed-empty (header, no row). It dies on a list without its header.
state() {
  local known="${1:?state needs KNOWN_TSV}" rows
  require_list_header "${known}"
  rows="$(list_rows "${known}")"
  if [ "${rows}" = "0" ]; then
    printf 'closed-empty\n'
  else
    printf 'rows=%s\n' "${rows}"
  fi
}

# print_failure_output prints stdin under a NON-TRIPWIRE FAILURE line. A cause can be the first line (what a
# TestMain or a build says before a long cleanup) or the last (the assertion), so a long output keeps both
# ends: the first FAILURE_HEAD_LINES and the last FAILURE_TAIL_LINES, with a line between them that counts
# what was left out and names where the whole output is. Nothing is cut without that line.
FAILURE_HEAD_LINES=40
FAILURE_TAIL_LINES=60
print_failure_output() {
  awk -v head="${FAILURE_HEAD_LINES}" -v tail="${FAILURE_TAIL_LINES}" '
    { line[NR] = $0 }
    END {
      if (NR <= head + tail) {
        for (i = 1; i <= NR; i++) print "    | " line[i]
      } else {
        for (i = 1; i <= head; i++) print "    | " line[i]
        printf "    | ... (%d lines left out here; the whole output is in this shard'"'"'s go test stream artifact, python-free-stream-*)\n", NR - head - tail
        for (i = NR - tail + 1; i <= NR; i++) print "    | " line[i]
      }
    }'
}

classify() {
  local json="${1:?classify needs GO_TEST_JSON}" hits_out="${2:?classify needs HITS_OUT}" log="${3:-}"
  [ -s "${json}" ] || die "no go test output in ${json}: a measurement that did not happen is a failure"
  local tmp rc=0
  tmp="$(mktemp -d)"
  # Test binaries (package base names) the tripwire log shows starting Python.
  local bins='[]'
  if [ -n "${log}" ] && [ -s "${log}" ]; then
    bins="$(sed -n 's/.*parent=\([^ ]*\).*/\1/p' "${log}" | sed 's|.*/||; s|\.test$||' | sort -u | jq -R . | jq -cs .)"
  fi
  # A failed top-level test is a tripwire hit only when EVERY failed leaf under it carries the marker
  # (its own output, as go test -json attributes it): a sibling subtest that failed an ordinary assertion
  # is a real failure and must not hide behind the one that hit the tripwire.
  jq -rs --arg marker "${MARKER}" --arg weak "${WEAK_MARKER}" --argjson bins "${bins}" '
    [ .[] | select(.Package != null) ] as $events
    | ( $events | map(select(.Test != null and .Action == "fail") | {p: .Package, t: .Test}) | unique ) as $failed
    | ( $failed | map(select(. as $f | ($failed | map(select(.p == $f.p and (.t | startswith($f.t + "/")))) | length) == 0)) ) as $leaves
    | $leaves
    | map(
        . as $l
        | ( $events | map(select(.Package == $l.p and .Test == $l.t and .Action == "output") | .Output) | join("") ) as $text
        | ( $l.p | split("/") | last ) as $base
        | { p: $l.p, top: ($l.t | split("/")[0]),
            hit: (($text | test($marker)) or (($text | contains($weak)) and (($bins | index($base)) != null))) } )
    | group_by([.p, .top])[]
    | [ (if all(.[]; .hit) then "tripwire" else "other" end), .[0].p, .[0].top ] | @tsv
  ' "${json}" >"${tmp}/failed.tsv"
  # Package-level failures with no failed test under them (build error, TestMain, panic) are real failures.
  jq -rs '
    [ .[] | select(.Package != null) ] as $events
    | ( $events | map(select(.Test != null and .Action == "fail") | .Package) | unique ) as $withTest
    | $events | map(select(.Test == null and .Action == "fail") | .Package) | unique
    | map(select(. as $p | ($withTest | index($p)) == null))[]
    | ["other", ., "(package-level failure)"] | @tsv
  ' "${json}" >>"${tmp}/failed.tsv"
  # Skipped top-level tests whose reason says Python is missing.
  jq -rs --arg marker "${SKIP_MARKER}" --arg gate "${SKIP_GATE}" --arg tripwire "${MARKER}" --arg weak "${WEAK_MARKER}" --argjson bins "${bins}" '
    [ .[] | select(.Package != null and .Test != null) ] as $events
    | ( $events | map(select(.Action == "skip") | {p: .Package, t: (.Test | split("/")[0])}) | unique ) as $skipped
    | $skipped[]
    | . as $f
    | ( $events
        | map(select(.Package == $f.p and (.Test | split("/")[0]) == $f.t and .Action == "output") | .Output)
        | join("") ) as $text
    | ( $f.p | split("/") | last ) as $base
    | select(
        ((($text | test($marker)) or ($text | test($tripwire))
          or (($text | contains($weak)) and (($bins | index($base)) != null)))
         and (($text | contains($gate)) | not)))
    | ["skips-without-python", $f.p, $f.t] | @tsv
  ' "${json}" >"${tmp}/skipped.tsv"
  : >"${hits_out}"
  while IFS=$'\t' read -r kind pkg test; do
    [ -n "${kind}" ] || continue
    if [ "${kind}" = "tripwire" ]; then
      printf '%s\t%s\ttripwire\n' "${pkg}" "${test}" >>"${hits_out}"
    else
      printf 'NON-TRIPWIRE FAILURE: %s %s\n' "${pkg}" "${test}" >&2
      if [ "${test}" = "(package-level failure)" ]; then
        # No test failed, so no test's output says why: print the package's own output (what the test
        # binary printed outside a test: a TestMain's exit, a panic, "signal: killed") and the output of the
        # build that failed (go test -json reports it under ImportPath, the one the package's fail event
        # names in FailedBuild: "<pkg> [<pkg>.test]" for the package itself or its vet, "<dep>" for a
        # dependency), so a package-level failure is diagnosable from the log.
        local detail
        detail="$(jq -rs --arg p "${pkg}" '
          ( map(select(.Action == "fail" and .Package == $p and .Test == null and .FailedBuild != null) | .FailedBuild) ) as $builds
          | map(select(
              (.Package == $p and .Test == null and .Action == "output")
              or (.Action == "build-output" and .ImportPath != null and (.ImportPath as $ip | $builds | index($ip)) != null)
            ) | .Output) | join("")
        ' "${json}")" || detail=""
        if [ -n "${detail}" ]; then
          printf '%s\n' "${detail}" | print_failure_output >&2
        else
          printf '    | (the go test -json stream holds no output for this package: see the shard json artifact)\n' >&2
        fi
      else
        # The failing test's own output, so a real failure is diagnosable from the log.
        jq -rj --arg p "${pkg}" --arg t "${test}" -s '
          map(select(.Package == $p and .Test != null and (.Test | split("/")[0]) == $t and .Action == "output") | .Output) | join("")
        ' "${json}" | print_failure_output >&2 || true
      fi
      rc=1
    fi
  done <"${tmp}/failed.tsv"
  while IFS=$'\t' read -r kind pkg test; do
    [ -n "${kind}" ] || continue
    printf '%s\t%s\t%s\n' "${pkg}" "${test}" "${kind}" >>"${hits_out}"
  done <"${tmp}/skipped.tsv"
  sort -u -o "${hits_out}" "${hits_out}"
  if [ -n "${log}" ] && [ -s "${log}" ]; then
    # SWALLOWED START: every tripwire log line must be attributed to a FAILED test of the binary that started
    # Python (class tripwire) or to a test of it that SKIPPED because of the shim's failure (class
    # skips-without-python). A line whose binary has neither means the test ignored the shim's failure and
    # passed: it is named with its argv, and the run fails.
    local shim argv binary base
    while IFS=$'\t' read -r shim argv binary; do
      [ -n "${binary}" ] || continue
      base="${binary##*/}"
      base="${base%.test}"
      if ! awk -F'\t' -v b="${base}" '{ n = split($1, parts, "/"); if (parts[n] == b) found = 1 } END { exit found ? 0 : 1 }' "${hits_out}"; then
        printf 'SWALLOWED START: the test binary %s ran %s %s, but no failed test of it names the tripwire (the error was ignored and the test passed)\n' "${binary}" "${shim}" "${argv}" >&2
        rc=1
      fi
    done < <(sed -n 's/^.* shim=\([^ ]*\) argv=\(.*\) parent=\([^ ]*\)\( .*\)\{0,1\}$/\1\t\2\t\3/p' "${log}" | sort -u)
  fi
  rm -rf "${tmp}"
  printf 'python_free_ratchet: classified %s: %s tripwire hit(s)\n' "${json}" "$(wc -l <"${hits_out}" | tr -d ' ')"
  return "${rc}"
}

compare() {
  local known="${1:?compare needs KNOWN_TSV}" dir="${2:?compare needs HITS_DIR}"
  [ -f "${known}" ] || die "the closed list ${known} does not exist"
  local tmp
  tmp="$(mktemp -d)"
  local line pkg test ticket class
  : >"${tmp}/listed"
  while IFS= read -r line; do
    case "${line}" in ''|\#*) continue ;; esac
    # a blank line (only whitespace) is not a row, as in list_rows
    [ -n "${line//[[:space:]]/}" ] || continue
    IFS=$'\t' read -r pkg test ticket class <<<"${line}"
    if [ -z "${pkg}" ] || [ -z "${test}" ]; then
      die "malformed closed-list row (package<TAB>test<TAB>ticket<TAB>class): ${line}"
    fi
    case "${ticket}" in CHAOS-[0-9]*) ;; *) die "closed-list row without a CHAOS ticket: ${line}" ;; esac
    case "${class}" in tripwire|skips-without-python) ;; *) die "closed-list row without a class (tripwire or skips-without-python): ${line}" ;; esac
    printf '%s\t%s\t%s\n' "${pkg}" "${test}" "${class}" >>"${tmp}/listed"
  done <"${known}"
  sort -u -o "${tmp}/listed" "${tmp}/listed"
  : >"${tmp}/hit"
  local file found=0
  for file in "${dir}"/*; do
    [ -f "${file}" ] || continue
    found=1
    cat "${file}" >>"${tmp}/hit"
  done
  [ "${found}" = 1 ] || die "no hit files under ${dir}: the shards did not report, which is not a pass"
  # CHAOS-8324: a list with no row is a DEFINED state (the ratchet is closed) only with its header; a truncated file must
  # never read as empty. A list with rows is unchanged, and so is the provisional report-only run.
  [ -s "${tmp}/listed" ] || [ "${PYTHON_FREE_REPORT_ONLY:-}" = "1" ] || require_list_header "${known}"
  sort -u -o "${tmp}/hit" "${tmp}/hit"
  comm -13 "${tmp}/listed" "${tmp}/hit" >"${tmp}/new"
  comm -23 "${tmp}/listed" "${tmp}/hit" >"${tmp}/stale"
  local listed hit new stale
  listed="$(wc -l <"${tmp}/listed" | tr -d ' ')"
  hit="$(wc -l <"${tmp}/hit" | tr -d ' ')"
  new="$(wc -l <"${tmp}/new" | tr -d ' ')"
  stale="$(wc -l <"${tmp}/stale" | tr -d ' ')"
  if [ "${new}" -gt 0 ]; then
    printf 'NEW Python starts (not on the closed list):\n' >&2
    sed 's/^/  /' "${tmp}/new" >&2
  fi
  if [ "${stale}" -gt 0 ]; then
    printf 'STALE entries (listed, but no longer start Python: remove them in the PR that froze them):\n' >&2
    sed 's/^/  /' "${tmp}/stale" >&2
  fi
  printf 'python-free ratchet: listed=%s hit=%s new=%s stale=%s\n' "${listed}" "${hit}" "${new}" "${stale}"
  if [ "${PYTHON_FREE_REPORT_ONLY:-}" = "1" ]; then
    printf 'python-free ratchet: REPORT-ONLY (provisional list). Every tripwire hit, as closed-list rows to fill in with a ticket:\n'
    sed 's/^/HIT\t/' "${tmp}/hit"
    rm -rf "${tmp}"
    return 0
  fi
  rm -rf "${tmp}"
  [ "${new}" -eq 0 ] && [ "${stale}" -eq 0 ]
}

case "${1:-}" in
  classify) shift; classify "$@" ;;
  compare) shift; compare "$@" ;;
  state) shift; state "$@" ;;
  *) die "usage: python_free_ratchet.sh classify GO_TEST_JSON HITS_OUT [TRIPWIRE_LOG] | compare KNOWN_TSV HITS_DIR | state KNOWN_TSV" ;;
esac
