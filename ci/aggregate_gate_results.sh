#!/usr/bin/env bash
# Verdict for a required status check whose real work happens in path-filtered
# jobs: `test` (.github/workflows/test.yml), `lint` (lint.yml), and `typecheck`
# (typecheck.yml).
#
# CHAOS-3482: the `test` aggregator read only its downstream jobs' results and
# treated `skipped` as a pass. On 2026-08-06, during a declared GitHub Actions
# outage, the `changes` job failed (and, on a rerun, was cancelled) in "Set up
# job". Both test jobs then evaluated their `if:` to false and reported
# `skipped`, the passing arm matched, and the single REQUIRED check reported
# SUCCESS on a run where zero tests executed. Reproduced 2 of 2 attempts.
#
# CHAOS-3513: `lint` and `typecheck` carried the identical rule, and did it
# while their filters omitted `.github/workflows/**`. That combination has
# already put an unlinted, untypechecked change into main -- see this script's
# test file for the run IDs. Both gates now come through here.
#
# The rule: tolerate a skip only when you can NAME why it is legitimate. That
# reason never lives in the skipped job's own result -- a skip caused by a
# docs-only path filter and a skip caused by a dead upstream are the same
# literal string. It lives in `changes`: its result (did the gating decision
# happen at all?) and its `code` output (what did it decide?).
#
#   changes != success   -> FAIL. Nothing downstream can be trusted; the
#                           selection that decides what must run never
#                           completed, so the skips carry no information. This
#                           deliberately covers `cancelled` as well as
#                           `failure` (both were observed producing the false
#                           green) and `skipped`/empty.
#   job skipped          -> legitimate ONLY when that job's own `if:` provably
#                           selected against it, per its policy below. `code`
#                           must be LITERALLY 'false'; "not 'true'" is not a
#                           decision, and an undecided filter used to read as
#                           "docs-only" (CHAOS-3482 Codex round 3).
#   anything else        -> FAIL, including an unrecognized or empty result
#                           string. Unknown state is not evidence.
#
# Policies, each MIRRORING a job's `if:` expression:
#
#   path-filtered    Selected unless the filter decided against it. The merge
#                    queue has no base/head diff for dorny/paths-filter and
#                    workflow_dispatch does not run the filter at all, so on
#                    both of those the job runs unconditionally and a skip is
#                    never legitimate. Used by test-matrix, lint-job and
#                    typecheck-mypy.
#   merge-time-only  As above, but excluded outright from pull_request (by
#                    design, CHAOS-2586 -- the coverage-gated suite runs at
#                    merge time and on main, not in the iterative PR loop) and
#                    from workflow_dispatch (no filter runs there, so its
#                    condition can never select it). Used by coverage.
#
# Inputs (environment):
#   GATE_NAME        name of the required check, for messages
#   EVENT_NAME       github.event_name
#   CHANGES_RESULT   needs.changes.result
#   CHANGES_CODE     needs.changes.outputs.code
#   GATED_JOB_1..N   "<job name>|<policy>|<needs.<job>.result>", consecutive
#
# tests/tooling/test_aggregate_gate_results.py pins both halves: it drives this
# script through the result matrix for every gate, and it parses all three
# workflows to assert their `if:` conditions still say what is modelled here,
# so the two cannot drift apart silently.
set -euo pipefail

GATE_NAME="${GATE_NAME:-gate}"
EVENT_NAME="${EVENT_NAME:-}"
CHANGES_RESULT="${CHANGES_RESULT:-}"
CHANGES_CODE="${CHANGES_CODE:-}"

printf 'gate: %s\n' "${GATE_NAME}"
printf 'event: %s\n' "${EVENT_NAME:-<empty>}"
printf 'changes result: %s (code=%s)\n' "${CHANGES_RESULT:-<empty>}" "${CHANGES_CODE:-<empty>}"

# Plain string rather than an array: this script is exercised by pytest on
# developer macOS hosts, whose /bin/bash is 3.2, where `${#arr[@]}` on an empty
# array is an unbound-variable error under `set -u`.
FAILURES=""

add_failure() {
  FAILURES="${FAILURES}  - ${1}"$'\n'
}

gate_failed() {
  printf '%s gate failed:\n' "${GATE_NAME}" >&2
  printf '%s' "${FAILURES}" >&2
  exit 1
}

# The gating decision itself. Checked first and on its own: when `changes` did
# not succeed, the downstream results are meaningless rather than merely
# suspicious, and reporting on them would bury the real reason.
if [[ "${CHANGES_RESULT}" != "success" ]]; then
  add_failure "changes reported '${CHANGES_RESULT:-<empty>}', not 'success' -- the job that decides what must run did not complete, so the downstream results are not evidence that anything ran"
  gate_failed
fi

# Selection is deliberately asymmetric with the `if:` expressions it mirrors.
# Those ask "did this evaluate true?"; this asks "can the skip be EXPLAINED?",
# and only a literal `false` from the path filter explains one.
is_selected() {
  local policy="$1"

  case "${policy}" in
    merge-time-only)
      if [[ "${EVENT_NAME}" == "pull_request" || "${EVENT_NAME}" == "workflow_dispatch" ]]; then
        return 1
      fi
      if [[ "${EVENT_NAME}" == "merge_group" || "${CHANGES_CODE}" != "false" ]]; then
        return 0
      fi
      return 1
      ;;
    path-filtered)
      if [[ "${EVENT_NAME}" != "merge_group" ]] &&
        [[ "${EVENT_NAME}" != "workflow_dispatch" ]] &&
        [[ "${CHANGES_CODE}" == "false" ]]; then
        return 1
      fi
      return 0
      ;;
    *)
      add_failure "job policy '${policy}' is not recognized -- this script cannot say whether a skip would be legitimate, so it refuses to pass the gate"
      gate_failed
      ;;
  esac
}

index=1
seen_any="false"
while true; do
  spec_var="GATED_JOB_${index}"
  spec="${!spec_var:-}"
  [[ -z "${spec}" ]] && break
  seen_any="true"

  job_name="${spec%%|*}"
  rest="${spec#*|}"
  policy="${rest%%|*}"
  result="${rest#*|}"

  printf '%s result: %s\n' "${job_name}" "${result:-<empty>}"

  case "${result}" in
    success) ;;
    skipped)
      if is_selected "${policy}"; then
        add_failure "${job_name} was skipped, but its job condition selected it to run (event=${EVENT_NAME:-<empty>}, changes code=${CHANGES_CODE:-<empty>}) -- an unexplained skip is an absence of proof, not a pass"
      else
        printf '%s skipped legitimately: its condition did not select it on event %s with changes code=%s\n' \
          "${job_name}" "${EVENT_NAME:-<empty>}" "${CHANGES_CODE:-<empty>}"
      fi
      ;;
    *)
      add_failure "${job_name} reported '${result:-<empty>}'"
      ;;
  esac

  index=$((index + 1))
done

# A gate that was handed nothing to judge must not report success: that is the
# "measurement that did not happen" reading as coverage. It can only arise from
# a workflow wiring mistake, which is exactly when a green check is worst.
if [[ "${seen_any}" != "true" ]]; then
  add_failure "no GATED_JOB_<n> entries were supplied -- this gate was asked to judge nothing"
fi

if [[ -n "${FAILURES}" ]]; then
  gate_failed
fi

printf '%s gate passed\n' "${GATE_NAME}"
