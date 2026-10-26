#!/usr/bin/env bash
# Merge queue: cancel the workflow runs of the entries stacked on a red entry.
#
# GitHub rebuilds the entries behind an ejected entry on new group branches and starts new runs, but it leaves the runs
# on the old group branches running. Those runs are stale: they carry a discarded revision and can never merge.
# A queue run's head_branch is gh-readonly-queue/main/pr-<N>-<head_sha of the entry it is stacked on>, so the stale runs
# of one red entry are the runs whose branch suffix is its head_sha, then those whose suffix is the head_sha of such a run.
#
# Inputs (environment): REPO (owner/name), HEAD_SHA (head_sha of the red entry's run), RUN_ID (the red run, never cancelled).
# DRY_RUN=1 prints the runs and cancels nothing. Diagnostic only. Exit codes: 0 done (including nothing to cancel, a failed
# list or a failed cancel, which are logged and skipped); 1 a required input is missing; 2 the scan was truncated: the last
# page of a status was full at the page limit, so stale runs may remain (a ::warning:: line says so; what was found is cancelled).
# The only write is POST .../actions/runs/<id>/cancel on a run that matched the strict queue branch pattern.
set -uo pipefail
export LC_ALL=C

repo="${REPO:?REPO is required}"
head_sha="${HEAD_SHA:?HEAD_SHA is required}"
self_id="${RUN_ID:?RUN_ID is required}"
dry_run="${DRY_RUN:-0}"

sha_re='^[0-9a-f]{40}$'
branch_re='^gh-readonly-queue/main/pr-[0-9]+-([0-9a-f]{40})$'
[[ "${head_sha}" =~ ${sha_re} ]] || { echo "HEAD_SHA is not a 40-char sha; nothing cancelled"; exit 0; }

# Every run is validated in jq, so bash never re-splits run data: an unit separator (not whitespace) keeps empty fields in place.
# C = a queue-branch run with a valid id and head_sha; I = a run with an invalid or missing id or head_sha (skipped, never cancelled or chained).
# shellcheck disable=SC2016
select_runs='
  def clean: if type == "string" then gsub("[\u0000-\u001f]"; " ") else "" end;
  if ((.id | tostring) | test("^[0-9]+$")) and ((.head_sha | type == "string") and (.head_sha | test($sha_re)))
  then
    (.head_branch // "") as $b
    | if ($b | test($branch_re))
      then ["C", (.id | tostring), ($b | match($branch_re).captures[0].string), .head_sha, (.name | clean), $b] | join("\u001f")
      else empty end
  else ["I", ((.id // "?") | tostring | clean)] | join("\u001f") end'

max_pages=5
truncated=0
scanned=0
rows=""
for status in queued in_progress; do
  for ((p = 1; p <= max_pages; p++)); do
    page="$(gh api "repos/${repo}/actions/runs?event=merge_group&status=${status}&per_page=100&page=${p}" \
      --jq '.workflow_runs[] | {id, head_branch, head_sha, name} | tojson')" || page=""
    [ -n "${page}" ] || break
    rows+="${page}"$'\n'
    count="$(printf '%s\n' "${page}" | grep -c .)"
    scanned=$((scanned + count))
    if [ "${p}" = "${max_pages}" ] && [ "${count}" -eq 100 ]; then
      truncated=1
      echo "::warning::stale-run scan truncated: ${status} runs fill ${max_pages} pages of 100; stale runs may remain"
    fi
  done
done

parsed="$(printf '%s' "${rows}" | jq -r --arg sha_re "${sha_re}" --arg branch_re "${branch_re}" "${select_runs}")" \
  || { echo "::warning::could not parse the run list; nothing cancelled"; parsed=""; }

skipped=0
cands=()
while IFS=$'\x1f' read -r kind id suffix sha name branch; do
  case "${kind}" in
    C) cands+=("${id}"$'\x1f'"${suffix}"$'\x1f'"${sha}"$'\x1f'"${name}"$'\x1f'"${branch}") ;;
    I)
      skipped=$((skipped + 1))
      echo "::warning::skipped run ${id}: invalid or missing id or head_sha" ;;
    *) ;;
  esac
done <<< "${parsed}"

declare -A frontier=(["${head_sha}"]=1)
declare -A chosen=()
changed=1
while [ "${changed}" = 1 ]; do
  changed=0
  for cand in "${cands[@]}"; do
    IFS=$'\x1f' read -r id suffix sha name branch <<< "${cand}"
    [ "${id}" != "${self_id}" ] || continue
    [ -z "${chosen[${id}]:-}" ] || continue
    [ -n "${frontier[${suffix}]:-}" ] || continue
    chosen["${id}"]="${name} ${branch}"
    frontier["${sha}"]=1
    changed=1
  done
done

cancelled=0
for id in "${!chosen[@]}"; do
  if [ "${dry_run}" = 1 ]; then
    echo "would cancel run ${id}: ${chosen[${id}]}"
  elif gh api -X POST "repos/${repo}/actions/runs/${id}/cancel" > /dev/null; then
    cancelled=$((cancelled + 1))
    echo "cancelled run ${id}: ${chosen[${id}]}"
  else
    echo "could not cancel run ${id}: ${chosen[${id}]}"
  fi
done

truncated_word=no
[ "${truncated}" = 0 ] || truncated_word=yes
echo "scanned=${scanned} candidates=${#chosen[@]} cancelled=${cancelled} skipped_invalid=${skipped} truncated=${truncated_word}"
[ "${truncated}" = 0 ] || exit 2
exit 0
