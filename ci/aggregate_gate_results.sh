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
#   unconditional    The job carries no `if:` that can deselect it, so NO skip
#                    is ever legitimate. Used by CHAOS-3219 Phase 5's
#                    ask-dev-acceptance.yml, which fires only on `schedule` and
#                    `workflow_dispatch` and therefore has no path filter to
#                    consult: a scheduled run whose only job was skipped booted
#                    nothing, and "booted nothing" must be RED.
#
# GATE_HAS_SELECTOR says whether this gate HAS a `changes` job at all. It is
# not a convenience switch: the whole rule above turns on being able to name
# why a skip is legitimate, and for a filter-less workflow the answer is "it
# never is". So `false` is accepted ONLY together with the `unconditional`
# policy -- asking this script to judge a path-filtered skip with no filter
# result in hand is a wiring mistake, and it refuses rather than guessing. The
# converse is refused too: passing changes inputs alongside
# GATE_HAS_SELECTOR=false means the caller wired a selector it then told this
# script to ignore.
#
# Inputs (environment):
#   GATE_NAME          name of the required check, for messages
#   EVENT_NAME         github.event_name
#   GATE_HAS_SELECTOR  "true" (default) if a `changes` job gates this workflow
#   CHANGES_RESULT     needs.changes.result       (selector gates only)
#   CHANGES_CODE       needs.changes.outputs.code (selector gates only)
#   GATED_JOB_1..N     "<job name>|<policy>|<needs.<job>.result>", consecutive
#
# tests/tooling/test_aggregate_gate_results.py pins both halves: it drives this
# script through the result matrix for every gate, and it parses all three
# workflows to assert their `if:` conditions still say what is modelled here,
# so the two cannot drift apart silently.
set -euo pipefail

GATE_NAME="${GATE_NAME:-gate}"
EVENT_NAME="${EVENT_NAME:-}"
# `-`, not `:-`: only an UNSET variable defaults to the selector path. A
# variable set to the empty string is what a mistyped `${{ ... }}` expression
# produces in a workflow, and letting that collapse into the permissive default
# is how a wiring mistake becomes a green check. Empty falls through to the
# refusal arm below. Caught by
# test_a_non_literal_selector_declaration_is_refused, which failed against the
# `:-` this line originally had.
GATE_HAS_SELECTOR="${GATE_HAS_SELECTOR-true}"
CHANGES_RESULT="${CHANGES_RESULT:-}"
CHANGES_CODE="${CHANGES_CODE:-}"

printf 'gate: %s\n' "${GATE_NAME}"
printf 'event: %s\n' "${EVENT_NAME:-<empty>}"
printf 'selector job: %s\n' "${GATE_HAS_SELECTOR}"
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
case "${GATE_HAS_SELECTOR}" in
  true)
    if [[ "${CHANGES_RESULT}" != "success" ]]; then
      add_failure "changes reported '${CHANGES_RESULT:-<empty>}', not 'success' -- the job that decides what must run did not complete, so the downstream results are not evidence that anything ran"
      gate_failed
    fi
    ;;
  false)
    # A gate declared filter-less must not also be handed filter results: one
    # of the two statements is wrong, and this script cannot tell which.
    if [[ -n "${CHANGES_RESULT}" || -n "${CHANGES_CODE}" ]]; then
      add_failure "GATE_HAS_SELECTOR=false, but CHANGES_RESULT='${CHANGES_RESULT}' / CHANGES_CODE='${CHANGES_CODE}' were supplied -- a gate cannot both have no selector job and report one"
      gate_failed
    fi
    ;;
  *)
    add_failure "GATE_HAS_SELECTOR must be literally 'true' or 'false', not '${GATE_HAS_SELECTOR:-<empty>}' -- this script will not guess whether a skip could be legitimate"
    gate_failed
    ;;
esac

# Selection is deliberately asymmetric with the `if:` expressions it mirrors.
# Those ask "did this evaluate true?"; this asks "can the skip be EXPLAINED?",
# and only a literal `false` from the path filter explains one.
# Checked for EVERY job, whatever its result -- not only when one is skipped.
# is_selected() below runs only on the `skipped` arm, so before this existed a
# mis-wired gate (an unrecognized policy name, or a filter policy with no
# selector job) sailed through green on every run where nothing happened to
# skip, and revealed itself only on the day a skip arrived -- the one day the
# verdict actually mattered. Adversarial review 2026-08-06, LOW but reproduced:
# `GATE_HAS_SELECTOR=false GATED_JOB_1=job|path-filtered|success` passed, and
# so did a policy misspelt `uncondit1onal`.
validate_policy() {
  local job_name="$1" policy="$2"

  case "${policy}" in
    unconditional | merge-time-only | path-filtered) ;;
    *)
      add_failure "${job_name}'s policy '${policy}' is not recognized -- this script cannot say whether a skip would be legitimate, so it refuses to pass the gate"
      gate_failed
      ;;
  esac

  # The two filter-derived policies read CHANGES_CODE. With no selector job
  # there is no code to read, and "empty" is not a decision -- refuse rather
  # than let an unset variable stand in for one.
  if [[ "${GATE_HAS_SELECTOR}" != "true" && "${policy}" != "unconditional" ]]; then
    add_failure "${job_name}'s policy '${policy}' is decided by the path filter, but this gate declared GATE_HAS_SELECTOR=false -- there is no filter result to decide it with"
    gate_failed
  fi
}

is_selected() {
  local policy="$1"

  case "${policy}" in
    unconditional)
      # No `if:` can deselect the job, so a skip is never explainable.
      return 0
      ;;
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
      # Unreachable: validate_policy already refused. Kept so a future policy
      # added to one function but not the other cannot fall through silently.
      add_failure "job policy '${policy}' is not recognized -- this script cannot say whether a skip would be legitimate, so it refuses to pass the gate"
      gate_failed
      ;;
  esac
}

# How many GATED_JOB_<n> variables actually exist in the environment, versus
# how many the consecutive loop below will read. The loop stops at the first
# gap, so `GATED_JOB_1` + `GATED_JOB_3` used to judge job 1 and silently drop
# job 3 -- reproduced by adversarial review 2026-08-06 (MEDIUM):
# GATED_JOB_1=a|unconditional|success with GATED_JOB_3=b|unconditional|failure
# printed "gate passed" and exited 0. Not reachable from this repo's current
# workflows, but test.yml already ships two entries, so one typo on
# GATED_JOB_2 there would drop `coverage` from the required `test` check
# entirely. A gate that judges a subset of what it was handed, without saying
# so, is the same class of defect as one that judges nothing.
supplied=0
for spec_var in ${!GATED_JOB_@}; do
  [[ -n "${!spec_var}" ]] && supplied=$((supplied + 1))
done

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
  validate_policy "${job_name}" "${policy}"

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

# ...and a gate that judged only SOME of what it was handed must not report
# success either. `index - 1` is how many the consecutive loop consumed;
# `supplied` is how many non-empty GATED_JOB_* exist. A mismatch means the loop
# stopped at a gap and one or more jobs' results were never read at all.
judged=$((index - 1))
if [[ "${judged}" -ne "${supplied}" ]]; then
  add_failure "judged ${judged} of ${supplied} GATED_JOB_<n> entries -- the numbering has a gap, so at least one job's result was never read. Number them consecutively from 1."
fi

if [[ -n "${FAILURES}" ]]; then
  gate_failed
fi

printf '%s gate passed\n' "${GATE_NAME}"
