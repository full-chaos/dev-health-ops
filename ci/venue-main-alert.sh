#!/usr/bin/env bash
# venue-main-alert.sh -- keep ONE GitHub issue in step with the main venue-oracles run (CHAOS-9597).
#
#   RUN_ID=<run id> RUN_CONCLUSION=<conclusion> RUN_SHA=<sha> ci/venue-main-alert.sh
#
# WHY. The main-push `Venue oracles` run is informational: a red run on main sat unseen
# for two days because no PR check carries it. The workflow venue-oracles-main-alert.yml
# calls this script once per completed main-push run.
#
# RED IS THE RUN'S OWN CONCLUSION. failure, timed_out or cancelled opens the issue, or
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
# Env: RUN_ID, RUN_CONCLUSION, RUN_SHA (the triggering run, required); GITHUB_REPOSITORY (or
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
label="${VENUE_ALERT_LABEL:-venue-oracles-main-red}"
GH="${VENUE_ALERT_GH:-gh}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
  failure|timed_out|cancelled|success) ;;
  *) log "run ${run_id} concluded '${conclusion}'; nothing to do"; exit 0 ;;
esac

issue="$(${GH} issue list -R "${repo}" --label "${label}" --state open --limit 1 --json number --jq '.[0].number // empty')"
[ -z "${issue}" ] || log "open issue #${issue}"

if [ "${conclusion}" = "success" ]; then
  if [ -z "${issue}" ]; then
    log "run ${run_id} succeeded and no issue is open; nothing to do"
    exit 0
  fi
  if state_out="$(bash "${last_run}" --branch main --limit 30 --repo "${repo}")"; then rc=0; else rc=$?; fi
  case "${rc}" in
    0|1|3) ;;
    *) log "could not read the main venue state (exit ${rc})"; exit "${rc}" ;;
  esac
  state="$(printf '%s\n' "${state_out}" | tail -n 1)"
  log "${state}"
  case "${state}" in
    "venue-main: GREEN last-executed=${run_id} "*)
      write issue close "${issue}" -R "${repo}" --comment "Main venue-oracles is green again: run ${run_url} at \`${sha}\` (every leg's oracle step succeeded)."
      log "closed issue #${issue}"
      ;;
    *) log "run ${run_id} is not a proven executed green; issue #${issue} stays open" ;;
  esac
  exit 0
fi

body="Run ${run_url} at \`${sha}\` concluded \`${conclusion}\`.

A red main venue-oracles run is fixed forward or reverted; no group roll starts until it is green again.
Read the failing leg's log and the receipt artifact on the run."

if [ -z "${issue}" ]; then
  write label create "${label}" -R "${repo}" --color D73A4A --description "Main venue-oracles run is red (opened by CI)" --force
  write issue create -R "${repo}" --title "Main venue-oracles run is red" --label "${label}" --body "${body}"
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
