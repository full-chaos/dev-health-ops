#!/usr/bin/env bash
# venue-main-alert.sh -- keep ONE GitHub issue in step with the main venue-oracles state (CHAOS-9597).
#
#   ci/venue-main-alert.sh            (RUN_ID=<triggering workflow run id> is logged only)
#
# WHY. The main-push `Venue oracles` run is informational: a red run on main sat unseen
# for two days because no PR check carries it. The workflow venue-oracles-main-alert.yml
# calls this script on every completed main-push run.
#
# WHAT IT DOES. It acts on the STATE OF RECORD (ci/last-venue-run.sh, Trap #408: a skipped
# run is neither green nor red), never on the triggering event alone. So an event that
# arrives late, twice, out of order or for a skipped run reads the same state and changes
# nothing that is already done; a lost event is made up by the next one.
#   RED or INCOMPLETE  no open issue with the label: open one for the last executed run.
#                      An open issue: add a comment for that run, once per run id.
#   GREEN              only when that run's own conclusion is success: an open issue is
#                      commented on and closed. A run whose oracle steps all passed but
#                      which failed elsewhere (conclusion not success) closes nothing.
#   NONE               no executed run in the window: warning, no change.
#
# ONE ISSUE. The open issue that carries the label is the one issue. If a person left two
# open, the lowest number is used.
#
# No Linear token is used or needed: only `gh` with the job's GITHUB_TOKEN
# (issues: write, actions: read).
#
# Env: GITHUB_REPOSITORY (or REPO); RUN_ID (log only); VENUE_ALERT_LABEL;
#      DRY_RUN=1 (read the API, print each write instead of doing it);
#      VENUE_ALERT_GH / VENUE_ALERT_LAST_RUN (tests: the gh command, the state script).
# Exit: 0 done or no-op; non-zero when an API read or write failed (a failed read is never
# read as "no issue" or "green").
set -euo pipefail

trigger_run="${RUN_ID:-unknown}"
case "${trigger_run}" in unknown) ;; ""|*[!0-9]*) printf 'venue-alert: RUN_ID must be a number\n' >&2; exit 2 ;; esac
repo="${REPO:-${GITHUB_REPOSITORY:-}}"
[ -n "${repo}" ] || { printf 'venue-alert: set GITHUB_REPOSITORY or REPO\n' >&2; exit 2; }
label="${VENUE_ALERT_LABEL:-venue-oracles-main-red}"
title="Main venue-oracles run is red"
GH="${VENUE_ALERT_GH:-gh}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
last_run="${VENUE_ALERT_LAST_RUN:-${here}/last-venue-run.sh}"

log() { printf 'venue-alert: %s\n' "$*"; }

write() {
  if [ "${DRY_RUN:-0}" = "1" ]; then
    printf 'DRY-RUN: gh %s\n' "$*"
  else
    ${GH} "$@"
  fi
}

if state_out="$(bash "${last_run}" --branch main --limit 30 --repo "${repo}")"; then rc=0; else rc=$?; fi
case "${rc}" in
  0|1|3) ;;
  *) log "could not read the main venue state (exit ${rc})"; exit "${rc}" ;;
esac
state="$(printf '%s\n' "${state_out}" | tail -n 1)"
verdict="$(printf '%s' "${state}" | sed -n 's/^venue-main: \([A-Z]*\) .*/\1/p')"
last_executed="$(printf '%s' "${state}" | sed -n 's/.* last-executed=\([^ ]*\) .*/\1/p')"
sha="$(printf '%s' "${state}" | sed -n 's/.* sha=\([^ ]*\) .*/\1/p')"
conclusion="$(printf '%s' "${state}" | sed -n 's/.* conclusion=\([^ ]*\) .*/\1/p')"
[ -n "${verdict}" ] || { log "could not parse the state line: ${state}"; exit 2; }
log "triggered by run ${trigger_run}; ${state}"

if [ "${verdict}" = "NONE" ]; then
  echo "::warning title=venue-alert::no executed venue-oracles run in the last 30 main runs; the issue is unchanged"
  exit 0
fi
case "${last_executed}" in ""|*[!0-9]*) log "could not parse last-executed from: ${state}"; exit 2 ;; esac
run_url="https://github.com/${repo}/actions/runs/${last_executed}"

issue="$(${GH} issue list -R "${repo}" --label "${label}" --state open --json number --jq 'map(.number) | sort | .[0] // empty')"
[ -z "${issue}" ] || log "open issue #${issue}"

state_block="$(printf '%s\n' "${state_out}" | sed 's/^/    /')"

case "${verdict}" in
  RED|INCOMPLETE)
    if [ "${verdict}" = "RED" ]; then what="at least one leg's oracle step failed"; else what="some legs did not finish (timeout or cancel), so the run is not green"; fi
    body="Run ${run_url} at \`${sha}\`: ${what}.

A red or incomplete main venue-oracles run is fixed forward or reverted; no group roll starts until it is green again.
Read the failing leg's log and the receipt artifact on the run.

State of record (\`ci/last-venue-run.sh\`):

${state_block}"
    if [ -z "${issue}" ]; then
      write label create "${label}" -R "${repo}" --color D73A4A --description "Main venue-oracles run is red (opened by CI)" --force
      write issue create -R "${repo}" --title "${title}" --label "${label}" --body "${body}"
      log "opened the issue for run ${last_executed}"
    else
      seen="$(${GH} issue view "${issue}" -R "${repo}" --json body,comments --jq '[.body, (.comments[].body)] | join("\n")')"
      if printf '%s' "${seen}" | grep -Eq "actions/runs/${last_executed}([^0-9]|\$)"; then
        log "run ${last_executed} is already on issue #${issue}; no change"
      else
        write issue comment "${issue}" -R "${repo}" --body "Still red. ${body}"
        log "commented on issue #${issue} for run ${last_executed}"
      fi
    fi
    ;;
  GREEN)
    if [ -z "${issue}" ]; then
      log "green and no open issue; no change"
    elif [ "${conclusion}" != "success" ]; then
      log "oracle steps are green but run ${last_executed} concluded '${conclusion}', not success; issue #${issue} stays open"
    else
      write issue close "${issue}" -R "${repo}" --comment "Main venue-oracles is green again: run ${run_url} at \`${sha}\` (every leg's oracle step succeeded)."
      log "closed issue #${issue}"
    fi
    ;;
  *)
    log "unknown verdict ${verdict}"
    exit 2
    ;;
esac
