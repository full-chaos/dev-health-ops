#!/usr/bin/env bash
# queue_tested_sha.sh <owner/repo> <sha> [workflow-file [job-prefix [required-step [min-jobs]]]]
# Exit 0 only when <workflow-file> (default go.yml) has a completed, successful merge_group run for exactly <sha>, from
# <owner/repo> itself, in which at least <min-jobs> (default 1) job(s) whose name starts with <job-prefix> (default
# go-storage-integration-shard-) ran in the latest attempt and every one of them concluded success (none skipped, cancelled
# or unfinished); the count is of DISTINCT job names, so a repeated job name counts once. With <required-step>, each of those jobs must also hold a step of exactly that name that concluded success
# (a leg skipped by its plan has the job green and that step skipped). Any other answer, including a failed API call, exits 1:
# the caller then keeps running its jobs. A failed jobs lookup for any matching run ends the helper with 1 too, even
# when another run for the same sha would be green (a run with no matching job is skipped). Fewer than two or more than six arguments exit 1.
# The sixth argument may instead be @<file>: the EXPECTED job names, one per line (the leg names of the validated leg list). The run then
# counts only when every expected name has a successful job and no other job carries the prefix: a missing, extra or padded name is not
# a tested run. An unreadable file exits 1; an empty one can never match a run.
# QUEUE_TESTED_RUN_ID_FILE, when set, receives the matched run id on exit 0.
set -uo pipefail

usage="usage: queue_tested_sha.sh <owner/repo> <sha> [workflow-file [job-prefix [required-step [min-jobs]]]]"
[ "$#" -ge 2 ] && [ "$#" -le 6 ] || { echo "${usage}" >&2; exit 1; }
repo=${1:?${usage}}
sha=${2:?${usage}}
workflow=${3:-go.yml}
prefix=${4:-go-storage-integration-shard-}
step=${5:-}
min_jobs=${6:-1}

case "${sha}" in
  *[!0-9a-f]* | "") exit 1 ;;
esac
[ "${#sha}" -eq 40 ] || exit 1
names_file=""
case "${min_jobs}" in
  @?*) names_file=${min_jobs#@}; min_jobs=1 ;;
esac
case "${min_jobs}" in
  "" | *[!0-9]*) exit 1 ;;
esac
[ "${min_jobs}" -ge 1 ] || exit 1
declare -A expected=()
if [ -n "${names_file}" ]; then
  [ -r "${names_file}" ] || exit 1
  while IFS= read -r line; do
    [ -n "${line}" ] && expected["${line}"]=1
  done < "${names_file}"
fi
for value in "${repo}" "${workflow}" "${prefix}" "${step}"; do
  case "${value}" in
    *'"'* | *\\* | *$'\n'*) exit 1 ;;
  esac
done

if [ -n "${step}" ]; then
  want='"success"|"success"'
  jobs_filter='.jobs[] | select(.name | startswith("'"${prefix}"'")) | .name + "\t" + (.conclusion | tojson) + "|" + ((.steps // []) | map(select(.name == "'"${step}"'") | .conclusion | tojson) | join(","))'
else
  want='"success"'
  jobs_filter='.jobs[] | select(.name | startswith("'"${prefix}"'")) | .name + "\t" + (.conclusion | tojson)'
fi

ids=$(gh api "repos/${repo}/actions/workflows/${workflow}/runs?event=merge_group&head_sha=${sha}&status=completed&per_page=10" \
  --jq '.workflow_runs[] | select(.event == "merge_group" and .head_sha == "'"${sha}"'" and .conclusion == "success" and .head_repository.full_name == "'"${repo}"'") | .id' 2>/dev/null) || exit 1
[ -n "${ids}" ] || exit 1

for id in ${ids}; do
  shards=$(gh api --paginate "repos/${repo}/actions/runs/${id}/jobs?per_page=100&filter=latest" \
    --jq "${jobs_filter}" 2>/dev/null) || exit 1
  [ -n "${shards}" ] || continue
  all_green=1
  unset seen
  declare -A seen=()
  while IFS=$'\t' read -r jname conclusion; do
    seen["${jname}"]=1
    if [ "${conclusion}" != "${want}" ]; then
      all_green=0
      break
    fi
  done <<<"${shards}"
  count=${#seen[@]}
  if [ -n "${names_file}" ]; then
    for jname in "${!seen[@]}"; do
      [ -n "${expected[${jname}]:-}" ] || all_green=0
    done
    for jname in "${!expected[@]}"; do
      [ -n "${seen[${jname}]:-}" ] || all_green=0
    done
  fi
  if [ "${all_green}" -eq 1 ] && [ "${count}" -ge "${min_jobs}" ]; then
    if [ -n "${QUEUE_TESTED_RUN_ID_FILE:-}" ]; then
      printf '%s\n' "${id}" > "${QUEUE_TESTED_RUN_ID_FILE}" || exit 1
    fi
    exit 0
  fi
done
exit 1
