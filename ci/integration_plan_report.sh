#!/usr/bin/env bash
# Measure the Go integration plan over the most recent completed pull_request runs of go.yml: how often the plan
# selected the FULL suite, and how many storage-integration shard minutes ran in runs that a newer push cancelled.
#
# Usage: ci/integration_plan_report.sh [RUNS] [OWNER/REPO]
#   RUNS defaults to 50. One TSV row per run goes to stdout; the summary goes to stderr, so
#   `ci/integration_plan_report.sh 50 > report.tsv` keeps both. Needs gh (authenticated) and jq.
#   Cost: one REST call per 100 runs listed, plus two per run (its jobs and the plan job's log).
set -euo pipefail

runs="${1:-50}"
repo="${2:-full-chaos/dev-health-ops}"
case "${runs}" in
  '' | *[!0-9]* | 0) echo "RUNS must be a positive number, got '${runs}'" >&2; exit 2 ;;
esac
command -v gh >/dev/null 2>&1 || { echo "gh is required" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

page=1
: > "${work}/runs.tsv"
while [ "$(wc -l < "${work}/runs.tsv")" -lt "${runs}" ]; do
  gh api "repos/${repo}/actions/workflows/go.yml/runs?event=pull_request&status=completed&per_page=100&page=${page}" \
    --jq '.workflow_runs[] | [.id, (.pull_requests[0].number // "-"), .created_at, .conclusion] | @tsv' > "${work}/page.tsv"
  [ -s "${work}/page.tsv" ] || break
  cat "${work}/page.tsv" >> "${work}/runs.tsv"
  page=$((page + 1))
done

printf 'run_id\tpr\tcreated_at\tconclusion\tselection\tshard_legs\tshard_minutes\n'
total=0 full=0 shard_s=0 cancelled_s=0
while IFS=$'\t' read -r id pr created conclusion; do
  gh api "repos/${repo}/actions/runs/${id}/jobs?per_page=100" > "${work}/jobs.json"
  jq '[.jobs[] | select((.name | startswith("go-storage-integration-shard-")) and .conclusion != "skipped"
    and .started_at and .completed_at)]' "${work}/jobs.json" > "${work}/shards.json"
  legs="$(jq 'length' "${work}/shards.json")"
  seconds="$(jq '[.[] | ((.completed_at | fromdateiso8601) - (.started_at | fromdateiso8601))] | add // 0' "${work}/shards.json")"
  plan_job="$(jq -r '[.jobs[] | select(.name == "go-storage-integration-plan" and .conclusion == "success")][0].id // ""' "${work}/jobs.json")"
  selection="no plan"
  if [ -n "${plan_job}" ]; then
    line="$(gh api --allow-escape-sequences "repos/${repo}/actions/jobs/${plan_job}/logs" 2>/dev/null \
      | sed 's/\x1b\[[0-9;]*m//g' | grep -m1 'integration affected selection:' || true)"
    case "${line}" in
      *"running the full suite"*) selection="FULL ${line#*integration affected selection: FULL }" ;;
      *"package(s)"*) selection="AFFECTED $(printf '%s\n' "${line}" | sed -n 's/.*integration affected selection: \([0-9]*\) package(s).*/\1/p')" ;;
      *) selection="FULL no selection line" ;;
    esac
    selection="${selection% -- running the full suite}"
    total=$((total + 1))
    case "${selection}" in FULL*) full=$((full + 1)) ;; esac
  fi
  shard_s=$((shard_s + seconds))
  [ "${conclusion}" != "cancelled" ] || cancelled_s=$((cancelled_s + seconds))
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "${id}" "${pr}" "${created}" "${conclusion}" "${selection}" "${legs}" "$((seconds / 60))"
done < <(head -n "${runs}" "${work}/runs.tsv")

{
  printf 'runs with a plan: %d; FULL: %d' "${total}" "${full}"
  [ "${total}" -eq 0 ] || printf ' (%d%%)' "$((full * 100 / total))"
  printf '\nshard minutes: %d; in cancelled runs: %d' "$((shard_s / 60))" "$((cancelled_s / 60))"
  [ "${shard_s}" -eq 0 ] || printf ' (%d%%)' "$((cancelled_s * 100 / shard_s))"
  printf '\n'
} >&2
