#!/usr/bin/env bash
# queue_tested_sha.sh <owner/repo> <sha>
# Exit 0 only when go.yml has a completed, successful merge_group run for <sha>. Any other answer, including
# a failed API call, exits 1: the caller then keeps running the integration shards.
set -uo pipefail

repo=${1:?usage: queue_tested_sha.sh <owner/repo> <sha>}
sha=${2:?usage: queue_tested_sha.sh <owner/repo> <sha>}

green=$(gh api "repos/${repo}/actions/workflows/go.yml/runs?event=merge_group&head_sha=${sha}&status=completed&per_page=10" \
  --jq '[.workflow_runs[] | select(.head_sha == "'"${sha}"'" and .conclusion == "success")] | length' 2>/dev/null) || exit 1
[ "${green}" -ge 1 ] 2>/dev/null
