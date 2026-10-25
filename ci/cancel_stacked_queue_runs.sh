#!/usr/bin/env bash
# Merge queue: cancel the workflow runs of the entries stacked on a red entry.
#
# GitHub rebuilds the entries behind an ejected entry on new group branches and starts new runs, but it leaves the runs
# on the old group branches running. Those runs are stale: they carry a discarded revision and can never merge.
# A queue run's head_branch is gh-readonly-queue/main/pr-<N>-<head_sha of the entry it is stacked on>, so the stale runs
# of one red entry are the runs whose branch suffix is its head_sha, then those whose suffix is the head_sha of such a run.
#
# Inputs (environment): REPO (owner/name), HEAD_SHA (head_sha of the red entry's run), RUN_ID (the red run, never cancelled).
# DRY_RUN=1 prints the runs and cancels nothing. Diagnostic only: always exits 0.
# The only write is POST .../actions/runs/<id>/cancel on a run that matched the strict queue branch pattern.
set -uo pipefail
export LC_ALL=C

repo="${REPO:?REPO is required}"
head_sha="${HEAD_SHA:?HEAD_SHA is required}"
self_id="${RUN_ID:?RUN_ID is required}"
dry_run="${DRY_RUN:-0}"

branch_re='^gh-readonly-queue/main/pr-[0-9]+-([0-9a-f]{40})$'
[[ "${head_sha}" =~ ^[0-9a-f]{40}$ ]] || { echo "HEAD_SHA is not a 40-char sha; nothing cancelled"; exit 0; }

max_pages=3
rows=""
for status in queued in_progress; do
  for ((p = 1; p <= max_pages; p++)); do
    page="$(gh api "repos/${repo}/actions/runs?event=merge_group&status=${status}&per_page=100&page=${p}" \
      --jq '.workflow_runs[] | [.id, .head_branch, .head_sha, .name] | @tsv')" || page=""
    [ -n "${page}" ] || break
    rows+="${page}"$'\n'
  done
done

declare -A frontier=(["${head_sha}"]=1)
declare -A chosen=()
changed=1
while [ "${changed}" = 1 ]; do
  changed=0
  while IFS=$'\t' read -r id branch sha name; do
    [ -n "${id}" ] || continue
    [ "${id}" != "${self_id}" ] || continue
    [ -z "${chosen[${id}]:-}" ] || continue
    [[ "${branch}" =~ ${branch_re} ]] || continue
    [ -n "${frontier[${BASH_REMATCH[1]}]:-}" ] || continue
    chosen["${id}"]="${name} ${branch}"
    frontier["${sha}"]=1
    changed=1
  done <<< "${rows}"
done

if [ "${#chosen[@]}" = 0 ]; then
  echo "no stale runs stacked on ${head_sha}"
  exit 0
fi

for id in "${!chosen[@]}"; do
  if [ "${dry_run}" = 1 ]; then
    echo "would cancel run ${id}: ${chosen[${id}]}"
  elif gh api -X POST "repos/${repo}/actions/runs/${id}/cancel" > /dev/null; then
    echo "cancelled run ${id}: ${chosen[${id}]}"
  else
    echo "could not cancel run ${id}: ${chosen[${id}]}"
  fi
done
exit 0
