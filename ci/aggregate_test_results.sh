#!/usr/bin/env bash
# Verdict for the required `test` check in .github/workflows/test.yml.
#
# CHAOS-3482: the aggregator used to read only `needs.test-matrix.result` and
# `needs.coverage.result`, and treat `skipped` as a pass. On 2026-08-06, during
# a declared GitHub Actions outage, the `changes` job failed (and, on a rerun,
# was cancelled) in "Set up job". Both test jobs then evaluated their `if:` to
# false and reported `skipped`, the passing arm matched, and the single
# REQUIRED check reported SUCCESS on a run where zero tests executed.
# Reproduced 2 of 2 attempts -- not a rare race.
#
# The rule this file encodes: tolerate a skip only when you can NAME why it is
# legitimate. That reason never lives in the skipped job's own result -- a skip
# caused by a docs-only path filter and a skip caused by a dead upstream are
# the same literal string. It lives in `changes`: its result (did the gating
# decision happen at all?) and its `code` output (what did it decide?).
#
#   changes != success        -> FAIL. Nothing downstream can be trusted; the
#                                selection that decides what must run never
#                                completed, so the skips carry no information.
#                                This deliberately covers `cancelled` as well
#                                as `failure` (both were observed producing the
#                                false green) and `skipped`/empty.
#   test-matrix skipped       -> legitimate ONLY when the path filter actually
#                                decided against it (changes code == 'false')
#                                on an event where the filter governs. The
#                                merge queue has no base/head diff for
#                                dorny/paths-filter, and workflow_dispatch does
#                                not run the filter at all, so on both of those
#                                the job runs unconditionally and a skip is
#                                never legitimate. (workflow_dispatch was a
#                                surviving zero-test green until Codex round 1
#                                on this branch: a manual run whose filter said
#                                code=false skipped both jobs and passed.)
#   coverage skipped          -> legitimate whenever the job's own condition
#                                did not select it: on every pull_request (by
#                                design, CHAOS-2586 -- the coverage-gated suite
#                                runs at merge time and on main, not in the
#                                iterative PR loop), on a docs-only push, and
#                                on workflow_dispatch. In all three the proof
#                                comes from test-matrix, which those events do
#                                select.
#   anything else             -> FAIL, including an unrecognized or empty
#                                result string. Unknown state is not evidence.
#
# The "selected to run" predicates below MIRROR the `if:` conditions of
# test-matrix and coverage in test.yml. tests/tooling/test_aggregate_test_results.py
# pins both halves: it drives this script through the result matrix, and it
# parses test.yml to assert those `if:` conditions still say what is modelled
# here, so the two cannot drift apart silently.
set -euo pipefail

EVENT_NAME="${EVENT_NAME:-}"
CHANGES_RESULT="${CHANGES_RESULT:-}"
CHANGES_CODE="${CHANGES_CODE:-}"
MATRIX_RESULT="${MATRIX_RESULT:-}"
COVERAGE_RESULT="${COVERAGE_RESULT:-}"

printf 'event: %s\n' "${EVENT_NAME:-<empty>}"
printf 'changes result: %s (code=%s)\n' "${CHANGES_RESULT:-<empty>}" "${CHANGES_CODE:-<empty>}"
printf 'test-matrix result: %s\n' "${MATRIX_RESULT:-<empty>}"
printf 'coverage result: %s\n' "${COVERAGE_RESULT:-<empty>}"

# Plain string rather than an array: this script is exercised by pytest on
# developer macOS hosts, whose /bin/bash is 3.2, where `${#arr[@]}` on an empty
# array is an unbound-variable error under `set -u`.
FAILURES=""

add_failure() {
  FAILURES="${FAILURES}  - ${1}"$'\n'
}

gate_failed() {
  printf 'test gate failed:\n' >&2
  printf '%s' "${FAILURES}" >&2
  exit 1
}

# The gating decision itself. Checked first and on its own: when `changes` did
# not succeed, the downstream results are meaningless rather than merely
# suspicious, and reporting on them would bury the real reason.
if [[ "${CHANGES_RESULT}" != "success" ]]; then
  add_failure "changes reported '${CHANGES_RESULT:-<empty>}', not 'success' -- the job that decides what must run did not complete, so the downstream '${MATRIX_RESULT:-<empty>}'/'${COVERAGE_RESULT:-<empty>}' results are not evidence that anything was tested"
  gate_failed
fi

matrix_selected="false"
if [[ "${EVENT_NAME}" == "merge_group" ||
  "${EVENT_NAME}" == "workflow_dispatch" ||
  "${CHANGES_CODE}" == "true" ]]; then
  matrix_selected="true"
fi

coverage_selected="false"
if [[ "${EVENT_NAME}" != "pull_request" ]] &&
  [[ "${EVENT_NAME}" == "merge_group" || "${CHANGES_CODE}" == "true" ]]; then
  coverage_selected="true"
fi

# shellcheck disable=SC2317  # invoked below, twice
check_job() {
  local name="$1" result="$2" selected="$3" legitimate_skip="$4"
  case "${result}" in
    success) ;;
    skipped)
      if [[ "${selected}" == "true" ]]; then
        add_failure "${name} was skipped, but its job condition selected it to run (event=${EVENT_NAME:-<empty>}, changes code=${CHANGES_CODE:-<empty>}) -- an unexplained skip is an absence of proof, not a pass"
      else
        printf '%s skipped legitimately: %s\n' "${name}" "${legitimate_skip}"
      fi
      ;;
    *)
      add_failure "${name} reported '${result:-<empty>}'"
      ;;
  esac
}

check_job "test-matrix" "${MATRIX_RESULT}" "${matrix_selected}" \
  "no gated path changed (changes code=${CHANGES_CODE:-<empty>}) on event ${EVENT_NAME:-<empty>}"
check_job "coverage" "${COVERAGE_RESULT}" "${coverage_selected}" \
  "the coverage-gated suite does not run on event ${EVENT_NAME:-<empty>} with changes code=${CHANGES_CODE:-<empty>} (CHAOS-2586)"

if [[ -n "${FAILURES}" ]]; then
  gate_failed
fi

printf 'test gate passed\n'
