#!/usr/bin/env bash
# venue-main-alert.sh -- keep ONE GitHub issue in step with the main venue-oracles state (CHAOS-9597).
#
#   RUN_ID=<run id> RUN_CONCLUSION=<conclusion> RUN_SHA=<sha> ci/venue-main-alert.sh
#
# WHY. The main-push `Venue oracles` run is informational: a red run on main sat unseen
# for two days because no PR check carries it. The workflow venue-oracles-main-alert.yml
# calls this script on every completed main-push run.
#
# WHAT IT DOES. It reads the STATE OF RECORD (ci/last-venue-run.sh, Trap #408: a skipped
# run is neither green nor red) and the triggering run's own conclusion, and decides:
#   ALERT  when the last executed run is RED or INCOMPLETE, or its oracle steps are green
#          but the run itself did not conclude success (a post-oracle step failed); or when
#          the triggering run is NEWER than the last executed run and concluded failure or
#          timed_out with no executed oracle step (every leg timed out or never started).
#          No open issue with the label: open one. An open issue: add a comment for that
#          run, once per run id.
#   CLOSE  the last executed run is GREEN and concluded success, and nothing above applies:
#          an open issue is commented on and closed.
#   NOTHING  otherwise (a skipped run, a cancelled run, a late or repeated event). The
#          script acts on the state, so a late, repeated or out-of-order event changes
#          nothing that is already done, and an older failed run never alerts once a newer
#          executed run exists.
# A cancelled run is no signal (its concurrency group never cancels a main push).
#
# ONE ISSUE. The open issue that carries the label is the one issue (the lowest number if a
# person left several); any other open issue with the label is closed as a duplicate.
#
# No Linear token is used or needed: only `gh` with the job's GITHUB_TOKEN
# (issues: write, actions: read).
#
# Env: GITHUB_REPOSITORY (or REPO); RUN_ID, RUN_CONCLUSION, RUN_SHA (the triggering run);
#      VENUE_ALERT_LABEL; DRY_RUN=1 (read the API, print each write instead of doing it);
#      VENUE_ALERT_GH / VENUE_ALERT_LAST_RUN (tests: the gh command, the state script).
# Exit: 0 done or no-op; non-zero when an API read or write failed (a failed read is never
# read as "no issue" or "green").
set -euo pipefail

trigger_run="${RUN_ID:-unknown}"
case "${trigger_run}" in unknown) ;; ""|*[!0-9]*) printf 'venue-alert: RUN_ID must be a number\n' >&2; exit 2 ;; esac
trigger_conclusion="${RUN_CONCLUSION:-unknown}"
case "${trigger_conclusion}" in ""|*[!a-z_]*) printf 'venue-alert: RUN_CONCLUSION must be a lowercase word\n' >&2; exit 2 ;; esac
trigger_sha="${RUN_SHA:-unknown}"
case "${trigger_sha}" in unknown) ;; ""|*[!0-9a-f]*) printf 'venue-alert: RUN_SHA must be a hex sha\n' >&2; exit 2 ;; esac
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
log "triggered by run ${trigger_run} (${trigger_conclusion}); ${state}"

action="nothing" alert_run="" alert_sha="" alert_what=""
case "${verdict}" in
  RED)
    action="alert" alert_run="${last_executed}" alert_sha="${sha}"
    alert_what="at least one leg's oracle step failed" ;;
  INCOMPLETE)
    action="alert" alert_run="${last_executed}" alert_sha="${sha}"
    alert_what="some legs did not finish (timeout or cancel), so the run is not green" ;;
  GREEN)
    if [ "${conclusion}" = "success" ]; then
      action="close" alert_run="${last_executed}" alert_sha="${sha}"
    else
      action="alert" alert_run="${last_executed}" alert_sha="${sha}"
      alert_what="every oracle step passed but the run concluded '${conclusion}' (a later step failed)"
    fi ;;
  NONE) ;;
  *) log "unknown verdict ${verdict}"; exit 2 ;;
esac

# A triggering run newer than the last executed run that failed with no executed oracle
# step (all legs timed out or never started): the state above cannot see it.
case "${trigger_conclusion}" in
  failure|timed_out)
    if [ "${trigger_run}" != "unknown" ] && { [ "${verdict}" = "NONE" ] || [ "${trigger_run}" -gt "${last_executed}" ]; }; then
      action="alert" alert_run="${trigger_run}" alert_sha="${trigger_sha}"
      alert_what="the run concluded '${trigger_conclusion}' and no leg executed an oracle step (every leg timed out or never started)"
    fi ;;
esac

if [ "${action}" = "nothing" ]; then
  if [ "${verdict}" = "NONE" ]; then
    echo "::warning title=venue-alert::no executed venue-oracles run in the last 30 main runs; the issue is unchanged"
  else
    log "nothing to do"
  fi
  exit 0
fi

run_url="https://github.com/${repo}/actions/runs/${alert_run}"
issues="$(${GH} issue list -R "${repo}" --label "${label}" --state open --json number --jq 'map(.number) | sort | .[]')"
issue="$(printf '%s\n' "${issues}" | sed -n '1p')"
extra="$(printf '%s\n' "${issues}" | sed -n '2,$p')"
[ -z "${issue}" ] || log "open issue #${issue}"

if [ "${action}" = "alert" ]; then
  state_block="$(printf '%s\n' "${state_out}" | sed 's/^/    /')"
  body="Run ${run_url} at \`${alert_sha}\`: ${alert_what}.

A red main venue-oracles run is fixed forward or reverted; no group roll starts until it is green again.
Read the failing leg's log and the receipt artifact on the run.

State of record (\`ci/last-venue-run.sh\`):

${state_block}"
  if [ -z "${issue}" ]; then
    write label create "${label}" -R "${repo}" --color D73A4A --description "Main venue-oracles run is red (opened by CI)" --force
    write issue create -R "${repo}" --title "${title}" --label "${label}" --body "${body}"
    log "opened the issue for run ${alert_run}"
  else
    seen="$(${GH} issue view "${issue}" -R "${repo}" --json body,comments --jq '[.body, (.comments[].body)] | join("\n")')"
    if printf '%s' "${seen}" | grep -Eq "actions/runs/${alert_run}([^0-9]|\$)"; then
      log "run ${alert_run} is already on issue #${issue}; no change"
    else
      write issue comment "${issue}" -R "${repo}" --body "Still red. ${body}"
      log "commented on issue #${issue} for run ${alert_run}"
    fi
  fi
else
  if [ -z "${issue}" ]; then
    log "green and no open issue; no change"
  else
    write issue close "${issue}" -R "${repo}" --comment "Main venue-oracles is green again: run ${run_url} at \`${alert_sha}\` (every leg's oracle step succeeded)."
    log "closed issue #${issue}"
  fi
fi

for dup in ${extra}; do
  write issue close "${dup}" -R "${repo}" --reason "not planned" --comment "Duplicate: the main venue-oracles state is tracked on #${issue}."
  log "closed duplicate issue #${dup}"
done
