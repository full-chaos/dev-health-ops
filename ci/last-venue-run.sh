#!/usr/bin/env bash
# last-venue-run.sh -- what did the newest main venue-oracles run that EXECUTED say?
#
#   ci/last-venue-run.sh [--branch main] [--limit 30] [--repo OWNER/REPO] [--summary]
#
# WHY. A main-push `Venue oracles` run whose change set is not Go-relevant reports
# SUCCESS after ~30 s with the oracle step SKIPPED (run 36128253076 at c8b66f64).
# That green is not evidence about the oracles, and it was read as one. The
# definition of record (Trap #408):
#
#   GREEN MAIN VENUE = the newest run whose oracle steps EXECUTED is green (every
#   leg's oracle step succeeded), and no later EXECUTED run failed. Skipped runs
#   neither count as green nor as red: the last executed run stays the state of
#   record. A run some of whose legs did not execute (a timeout cancel) is
#   INCOMPLETE, not green.
#
# For each completed run (newest first) it reads every leg's step named
# "Run this shard's venue differential oracles ..." and classifies the run:
#   executed  every leg's step ran (conclusion success or failure)
#   skipped   no leg's step ran (the relevance decider said nothing Go changed)
#   partial   some legs ran, some did not (cancelled/timed out)
# then prints one line per run and a final verdict line:
#   venue-main: GREEN|RED|INCOMPLETE|NONE last-executed=<run id> sha=<sha> conclusion=<c> skipped-since=<n>
# Exit: 0 GREEN, 1 RED or INCOMPLETE, 3 NONE (no executed run in --limit runs),
# 2 usage / could not read the API. --summary prints the same as markdown.
#
# GENERIC MODE (CHAOS-13243): --workflow FILE --witness 'prefix;prefix' reads another workflow the same way.
# A run is classified from its JOB conclusions: the witness job-name prefixes must each match jobs and all
# matched jobs must have executed (success or failure). A cancelled run is ignored. A run with any other
# non-success conclusion is an executed RED run. A success run whose witness jobs were skipped (go-quality
# test and race legs, CHAOS-11408) is skipped; one with only some executed is partial = INCOMPLETE. Neither closes.
#
# LAST_VENUE_RUN_GH overrides the `gh` command (the tooling test feeds fixtures).
set -euo pipefail

workflow="venue-oracles.yml" witness="" branch=main limit=30 repo="${GITHUB_REPOSITORY:-full-chaos/dev-health-ops}" summary=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --branch) branch="${2:?--branch needs a value}"; shift 2 ;;
    --limit) limit="${2:?--limit needs a value}"; shift 2 ;;
    --repo) repo="${2:?--repo needs a value}"; shift 2 ;;
    --summary) summary=1; shift ;;
    --workflow) workflow="${2:?--workflow needs a value}"; shift 2 ;;
    --witness) witness="${2:?--witness needs a value}"; shift 2 ;;
    *) printf 'usage: ci/last-venue-run.sh [--branch main] [--limit N] [--repo OWNER/REPO] [--summary]\n' >&2; exit 2 ;;
  esac
done
case "${limit}" in ""|*[!0-9]*) printf 'last-venue-run: --limit must be a number\n' >&2; exit 2 ;; esac
case "${workflow}" in *[!A-Za-z0-9._-]*) printf 'last-venue-run: --workflow must be a file name\n' >&2; exit 2 ;; esac
command -v jq >/dev/null 2>&1 || { printf 'last-venue-run: jq is required\n' >&2; exit 2; }
GH="${LAST_VENUE_RUN_GH:-gh}"

api() { ${GH} api "$1"; }

runs_json="$(api "repos/${repo}/actions/workflows/${workflow}/runs?branch=${branch}&event=push&status=completed&per_page=${limit}")" \
  || { printf 'last-venue-run: could not list the venue-oracles runs\n' >&2; exit 2; }

verdict="NONE" last_id="-" last_sha="-" last_conclusion="-" skipped_since=0 found=0
lines=""
while IFS=$'\t' read -r id sha conclusion; do
  [ -n "${id}" ] || continue
  if [ -n "${witness}" ]; then
    if [ "${conclusion}" = "cancelled" ]; then continue; fi
    if [ "${conclusion}" != "success" ]; then
      total=1 ran=1 ok=0
    else
      jobs_json="$(api "repos/${repo}/actions/runs/${id}/jobs?per_page=100")" \
        || { printf 'last-venue-run: could not read the jobs of run %s\n' "${id}" >&2; exit 2; }
      read -r total ran ok < <(printf '%s' "${jobs_json}" | jq -r --arg w "${witness}" '
        ($w | split(";")) as $w | .jobs as $j
        | [$w[] as $p | [$j[] | select(.name | startswith($p)) | .conclusion]] as $m
        | "\($m | length) \($m | map(select(length > 0 and all(.[]; . == "success" or . == "failure"))) | length) \($m | map(select(length > 0 and all(.[]; . == "success"))) | length)"')
    fi
  else
  jobs_json="$(api "repos/${repo}/actions/runs/${id}/jobs?per_page=100")" \
    || { printf 'last-venue-run: could not read the jobs of run %s\n' "${id}" >&2; exit 2; }
  read -r total ran ok < <(printf '%s' "${jobs_json}" | jq -r '
    [.jobs[] | select(.name | startswith("venue-oracles"))
       | (.steps // [] | map(select(.name | startswith("Run this shard"))) | .[0].conclusion // "absent")] as $c
    | "\($c | length) \($c | map(select(. == "success" or . == "failure")) | length) \($c | map(select(. == "success")) | length)"')
  fi
  if [ "${total}" -eq 0 ]; then class="none"
  elif [ "${ran}" -eq "${total}" ]; then class="executed"
  elif [ "${ran}" -eq 0 ]; then class="skipped"
  else class="partial"
  fi
  lines="${lines}run=${id} sha=${sha:0:7} conclusion=${conclusion} class=${class} legs-green=${ok}/${total}
"
  if [ "${found}" -eq 0 ]; then
    case "${class}" in
      skipped|none) skipped_since=$((skipped_since + 1)) ;;
      executed|partial)
        found=1 last_id="${id}" last_sha="${sha}" last_conclusion="${conclusion}"
        if [ "${class}" = "partial" ]; then verdict="INCOMPLETE"
        elif [ "${ok}" -eq "${total}" ]; then verdict="GREEN"
        else verdict="RED"
        fi
        ;;
    esac
  fi
done < <(printf '%s' "${runs_json}" | jq -r '.workflow_runs[] | [.id, .head_sha, (.conclusion // "none")] | @tsv')

line="venue-main: ${verdict} last-executed=${last_id} sha=${last_sha} conclusion=${last_conclusion} skipped-since=${skipped_since}"
if [ "${summary}" -eq 1 ]; then
  printf '### Main venue-oracles state\n\n'
  printf '%s\n' "${line}"
  fence='```'
  printf '\nA run whose oracle step was skipped is NOT a pass: the last EXECUTED run is the state of record.\n\n%s\n%s%s\n' "${fence}" "${lines}" "${fence}"
else
  printf '%s' "${lines}"
  printf '%s\n' "${line}"
fi
case "${verdict}" in
  GREEN) exit 0 ;;
  RED|INCOMPLETE) exit 1 ;;
  *) exit 3 ;;
esac
