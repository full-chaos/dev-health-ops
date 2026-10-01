#!/usr/bin/env bash
# python_free_ratchet.sh -- the closed list of Go tests that still start Python (CHAOS-7384).
#
#   ci/python_free_ratchet.sh classify GO_TEST_JSON HITS_OUT [TRIPWIRE_LOG]
#   ci/python_free_ratchet.sh compare  KNOWN_TSV HITS_DIR
#
# The go-python-free job runs the integration shards with Python unreachable (ci/python_tripwire.sh).
# Tests that still start Python fail. Red must not become normal, so the failures are held to a list:
#
#   classify   reads one `go test -json` stream. A failed top-level test whose output (or whose
#              subtests' output) names the tripwire is a HIT: "<package>\t<TestName>" goes to HITS_OUT.
#              Any OTHER failure (a real regression, a build failure, a package-level failure) is
#              printed as "NON-TRIPWIRE FAILURE" and the exit status is 1. With a tripwire log it also
#              fails when the log shows a Python start by a test binary that has no HIT: a test that
#              swallowed the failure and passed is still a Python start.
#   compare    unions every HITS file in HITS_DIR and compares it with the closed list
#              (ci/python_free_known.tsv: package<TAB>TestName<TAB>CHAOS-ticket). Exit 0 only when
#              the HIT set equals the list. A HIT not on the list is NEW (a new Python start); a listed
#              test that did not hit is STALE (it was frozen: remove the entry in that PR). The summary
#              line prints listed, hit, new and stale.
set -euo pipefail

MARKER='PYTHON TRIPWIRE|python-tripwire/no-python|exit status 97'

die() { printf 'python_free_ratchet: %s\n' "$1" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || die "jq is required"

classify() {
  local json="${1:?classify needs GO_TEST_JSON}" hits_out="${2:?classify needs HITS_OUT}" log="${3:-}"
  [ -s "${json}" ] || die "no go test output in ${json}: a measurement that did not happen is a failure"
  local tmp rc=0
  tmp="$(mktemp -d)"
  # Failed top-level tests, each marked tripwire|other by its own (and its subtests') output.
  jq -rs --arg marker "${MARKER}" '
    [ .[] | select(.Package != null) ] as $events
    | ( $events | map(select(.Test != null and .Action == "fail") | {p: .Package, t: (.Test | split("/")[0])}) | unique ) as $failed
    | $failed[]
    | . as $f
    | ( $events
        | map(select(.Package == $f.p and .Test != null and (.Test | split("/")[0]) == $f.t and .Action == "output") | .Output)
        | join("") ) as $text
    | [ (if ($text | test($marker)) then "tripwire" else "other" end), $f.p, $f.t ] | @tsv
  ' "${json}" >"${tmp}/failed.tsv"
  # Package-level failures with no failed test under them (build error, TestMain, panic) are real failures.
  jq -rs '
    [ .[] | select(.Package != null) ] as $events
    | ( $events | map(select(.Test != null and .Action == "fail") | .Package) | unique ) as $withTest
    | $events | map(select(.Test == null and .Action == "fail") | .Package) | unique
    | map(select(. as $p | ($withTest | index($p)) == null))[]
    | ["other", ., "(package-level failure)"] | @tsv
  ' "${json}" >>"${tmp}/failed.tsv"
  : >"${hits_out}"
  while IFS=$'\t' read -r kind pkg test; do
    [ -n "${kind}" ] || continue
    if [ "${kind}" = "tripwire" ]; then
      printf '%s\t%s\n' "${pkg}" "${test}" >>"${hits_out}"
    else
      printf 'NON-TRIPWIRE FAILURE: %s %s\n' "${pkg}" "${test}" >&2
      rc=1
    fi
  done <"${tmp}/failed.tsv"
  sort -u -o "${hits_out}" "${hits_out}"
  if [ -n "${log}" ] && [ -s "${log}" ]; then
    # A test binary that started Python (the shim's parent) but has no HIT swallowed the failure.
    local binary base
    while IFS= read -r binary; do
      [ -n "${binary}" ] || continue
      base="${binary##*/}"
      base="${base%.test}"
      if ! awk -F'\t' -v b="${base}" '{ n = split($1, parts, "/"); if (parts[n] == b) found = 1 } END { exit found ? 0 : 1 }' "${hits_out}"; then
        printf 'UNATTRIBUTED PYTHON START: the test binary %s started Python (see the tripwire log) but no failed test names the tripwire\n' "${binary}" >&2
        rc=1
      fi
    done < <(sed -n 's/.*parent=\([^ ]*\).*/\1/p' "${log}" | sort -u)
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
  local line pkg test ticket
  : >"${tmp}/listed"
  while IFS= read -r line; do
    case "${line}" in ''|\#*) continue ;; esac
    IFS=$'\t' read -r pkg test ticket <<<"${line}"
    [ -n "${pkg}" ] && [ -n "${test}" ] || die "malformed closed-list row (package<TAB>test<TAB>ticket): ${line}"
    case "${ticket}" in CHAOS-[0-9]*) ;; *) die "closed-list row without a CHAOS ticket: ${line}" ;; esac
    printf '%s\t%s\n' "${pkg}" "${test}" >>"${tmp}/listed"
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
  rm -rf "${tmp}"
  [ "${new}" -eq 0 ] && [ "${stale}" -eq 0 ]
}

case "${1:-}" in
  classify) shift; classify "$@" ;;
  compare) shift; compare "$@" ;;
  *) die "usage: python_free_ratchet.sh classify GO_TEST_JSON HITS_OUT [TRIPWIRE_LOG] | compare KNOWN_TSV HITS_DIR" ;;
esac
