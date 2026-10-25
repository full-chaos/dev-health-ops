#!/usr/bin/env bash
# queue_tested_sha.sh <owner/repo> <sha>
# Exit 0 only when go.yml has a completed, successful merge_group run for <sha> in which at least one integration
# shard job ran and every integration shard job concluded success (none skipped, cancelled or unfinished).
# Any other answer, including a failed API call, exits 1: the caller then keeps running the integration shards.
set -uo pipefail

repo=${1:?usage: queue_tested_sha.sh <owner/repo> <sha>}
sha=${2:?usage: queue_tested_sha.sh <owner/repo> <sha>}

ids=$(gh api "repos/${repo}/actions/workflows/go.yml/runs?event=merge_group&head_sha=${sha}&status=completed&per_page=10" \
  --jq '.workflow_runs[] | select(.head_sha == "'"${sha}"'" and .conclusion == "success") | .id' 2>/dev/null) || exit 1
[ -n "${ids}" ] || exit 1

for id in ${ids}; do
  shards=$(gh api --paginate "repos/${repo}/actions/runs/${id}/jobs?per_page=100&filter=latest" \
    --jq '.jobs[] | select(.name | startswith("go-storage-integration-shard-")) | .conclusion | tojson' 2>/dev/null) || continue
  [ -n "${shards}" ] || continue
  all_green=1
  while IFS= read -r conclusion; do
    if [ "${conclusion}" != '"success"' ]; then
      all_green=0
      break
    fi
  done <<<"${shards}"
  if [ "${all_green}" -eq 1 ]; then
    exit 0
  fi
done
exit 1
