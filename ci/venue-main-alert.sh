#!/usr/bin/env bash
# venue-main-alert.sh -- keep ONE GitHub issue per covered workflow in step with its main-push run (CHAOS-9597, CHAOS-13243).
#
#   RUN_WORKFLOW=<name> RUN_ID=<run id> RUN_CONCLUSION=<conclusion> RUN_SHA=<sha> ci/venue-main-alert.sh
#
# COVERAGE. ci/main-red-workflows.tsv lists the workflows (default RUN_WORKFLOW is Venue oracles). The issue title is
# `main-red: <workflow> run is red on main` (search: gh issue list --search "main-red in:title"), label main-red.
# A cancelled run opens nothing for a workflow whose row says cancelled=ignore (main runs are sha-keyed and never
# cancelled by a newer push, so a cancel is manual) and never closes. The close proof for a non-venue workflow is
# the witness jobs of ci/last-venue-run.sh --workflow: a success whose gate jobs were skipped does not close.
#
# WHY. The main-push `Venue oracles` run is informational: a red run on main sat unseen
# for two days because no PR check carries it. The workflow venue-oracles-main-alert.yml
# calls this script once per completed main-push run.
#
# RED IS THE RUN'S OWN CONCLUSION. failure, startup_failure, timed_out or cancelled opens the issue, or
# comments on the open one (once per run id). No step of the run is inspected, so no kind
# of failed run can pass unseen.
#
# CLOSE ONLY ON PROVEN GREEN. A skipped run (nothing Go-relevant changed) also concludes
# success but is not a pass (Trap #408). On success the script closes the open issue only
# when ci/last-venue-run.sh says the newest executed run is THIS run and it is green; any
# other success leaves the issue open for the next executed run. The cost of that rule is
# an issue that closes later, never one that stays unseen.
#
# ONE ISSUE. The open issue that carries the label is the one issue.
#
# No Linear token is used or needed: only `gh` with the job's GITHUB_TOKEN
# (issues: write, actions: read).
#
# Env: RUN_WORKFLOW, MAIN_RED_CONFIG (tests), RUN_ID, RUN_CONCLUSION, RUN_SHA (the triggering run, required); GITHUB_REPOSITORY (or
#      REPO); VENUE_ALERT_LABEL; DRY_RUN=1 (read the API, print each write instead of doing
#      it); VENUE_ALERT_GH / VENUE_ALERT_LAST_RUN (tests: the gh command, the state script).
# Exit: 0 done or no-op; non-zero when an API read or write failed (a failed read is never
# read as "no issue" or "green").
set -euo pipefail

bad() { printf 'venue-alert: %s\n' "$1" >&2; exit 2; }
run_id="${RUN_ID:-}"
case "${run_id}" in ""|*[!0-9]*) bad "RUN_ID must be a number" ;; esac
conclusion="${RUN_CONCLUSION:-}"
case "${conclusion}" in ""|*[!a-z_]*) bad "RUN_CONCLUSION must be a lowercase word" ;; esac
sha="${RUN_SHA:-}"
case "${sha}" in ""|*[!0-9a-f]*) bad "RUN_SHA must be a hex sha" ;; esac
repo="${REPO:-${GITHUB_REPOSITORY:-}}"
[ -n "${repo}" ] || bad "set GITHUB_REPOSITORY or REPO"
workflow="${RUN_WORKFLOW:-Venue oracles}"
case "${workflow}" in ""|*[!A-Za-z0-9\ _-]*) bad "RUN_WORKFLOW must be a plain workflow name" ;; esac
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config="${MAIN_RED_CONFIG:-${here}/main-red-workflows.tsv}"
row="$(awk -F'\t' -v n="${workflow}" '$0 !~ /^#/ && $1 == n { print; exit }' "${config}")" || bad "cannot read ${config}"
if [ -z "${row}" ]; then printf 'venue-alert: workflow %s is not in %s; nothing to do\n' "${workflow}" "${config##*/}"; exit 0; fi
IFS=$'\t' read -r _ wf_file cancel_mode witnesses legacy_label <<<"${row}"
label="${VENUE_ALERT_LABEL:-main-red}"
title="main-red: ${workflow} run is red on main"
GH="${VENUE_ALERT_GH:-gh}"
last_run="${VENUE_ALERT_LAST_RUN:-${here}/last-venue-run.sh}"
run_url="https://github.com/${repo}/actions/runs/${run_id}"

log() { printf 'venue-alert: %s\n' "$*"; }

write() {
  if [ "${DRY_RUN:-0}" = "1" ]; then
    printf 'DRY-RUN: gh %s\n' "$*"
  else
    ${GH} "$@"
  fi
}

case "${conclusion}" in
  failure|startup_failure|timed_out|success) ;;
  cancelled)
    if [ "${cancel_mode}" != "red" ]; then log "run ${run_id} was cancelled; ${workflow} ignores cancels"; exit 0; fi ;;
  *) log "run ${run_id} concluded '${conclusion}'; nothing to do"; exit 0 ;;
esac

# One issue per workflow: the lowest open issue carrying the label with this exact title (or the legacy label).
issue="$(${GH} issue list -R "${repo}" --label "${label}" --state open --limit 100 --json number,title | jq -r --arg t "${title}" '[.[] | select(.title == $t) | .number] | sort | .[0] // empty')"
if [ -z "${issue}" ] && [ "${legacy_label}" != "-" ]; then
  issue="$(${GH} issue list -R "${repo}" --label "${legacy_label}" --state open --limit 1 --json number --jq '.[0].number // empty')"
fi
[ -z "${issue}" ] || log "open issue #${issue}"

if [ "${conclusion}" = "success" ]; then
  if [ -z "${issue}" ]; then
    log "run ${run_id} succeeded and no issue is open; nothing to do"
    exit 0
  fi
  state_args=(--branch main --limit 30 --repo "${repo}")
  [ "${witnesses}" = "-" ] || state_args+=(--workflow "${wf_file}" --witness "${witnesses}")
  if state_out="$(bash "${last_run}" "${state_args[@]}")"; then rc=0; else rc=$?; fi
  case "${rc}" in
    0|1|3) ;;
    *) log "could not read the main venue state (exit ${rc})"; exit "${rc}" ;;
  esac
  state="$(printf '%s\n' "${state_out}" | tail -n 1)"
  log "${state}"
  case "${state}" in
    "venue-main: GREEN last-executed=${run_id} "*)
      write issue close "${issue}" -R "${repo}" --comment "Main ${workflow} is green again: run ${run_url} at \`${sha}\` (the newest executed run; every gate job succeeded)."
      log "closed issue #${issue}"
      ;;
    *) log "run ${run_id} is not a proven executed green; issue #${issue} stays open" ;;
  esac
  exit 0
fi

failed_jobs="$(${GH} api "repos/${repo}/actions/runs/${run_id}/jobs?per_page=100" --jq '[.jobs[] | select(.conclusion == "failure" or .conclusion == "timed_out" or .conclusion == "cancelled") | .name] | join(", ")' 2>/dev/null || true)"
[ -n "${failed_jobs}" ] || failed_jobs="(none read; open the run)"
body="Workflow ${workflow}: run ${run_url} at \`${sha}\` concluded \`${conclusion}\`.

A red main run is fixed forward or reverted; no group roll starts until it is green again.
Failed jobs: ${failed_jobs}"

if [ -z "${issue}" ]; then
  write label create "${label}" -R "${repo}" --color D73A4A --description "A main-push run is red (opened by CI)" --force
  labels=(--label "${label}")
  if [ "${legacy_label}" != "-" ]; then
    write label create "${legacy_label}" -R "${repo}" --color D73A4A --description "Main venue-oracles run is red (opened by CI)" --force
    labels+=(--label "${legacy_label}")
  fi
  write issue create -R "${repo}" --title "${title}" "${labels[@]}" --body "${body}"
  log "opened the issue for run ${run_id}"
else
  seen="$(${GH} issue view "${issue}" -R "${repo}" --json body,comments --jq '[.body, (.comments[].body)] | join("\n")')"
  if printf '%s' "${seen}" | grep -Eq "actions/runs/${run_id}([^0-9]|\$)"; then
    log "run ${run_id} is already on issue #${issue}; no change"
  else
    write issue comment "${issue}" -R "${repo}" --body "Still red. ${body}"
    log "commented on issue #${issue} for run ${run_id}"
  fi
fi
