#!/usr/bin/env bash
# CHAOS-10700: decide whether a pull request runs the Go integration shards.
# Usage: integration_pr_scope.sh <changed-file-list>   env: PR_LABELS_JSON (JSON array of label names)
# The list is the NUL-terminated output of ci/go_relevant_diff.sh.
# stdout: "run <reason>" (let the planner pick shards) or "skip <reason>" (no shard runs).
# Any bad input (usage, unreadable labels, empty list, unterminated or empty record) exits non-zero and prints no
# verdict: the caller must then fall back to the planner (run), never to a skip.
# No path is a trigger: the shards run in the merge queue (merge_group) for every entry that is not docs-only. A PR
# that wants them on the PR itself carries ci:integration (shards) or ci:full (every heavy job), with the reason in
# the PR body, set before its push; a label change alone starts no run.
set -euo pipefail

list="${1:-}"
[ -n "${list}" ] && [ -f "${list}" ] || { echo "usage: integration_pr_scope.sh <changed-file-list>" >&2; exit 2; }

labels="$(jq -r 'if type == "array" and all(.[]; type == "string") then .[] else error("not an array of strings") end' <<< "${PR_LABELS_JSON:-[]}")" || { echo "PR_LABELS_JSON is not a JSON array" >&2; exit 3; }
while IFS= read -r label; do
  case "${label}" in
    ci:integration | ci:full) echo "run label ${label}"; exit 0 ;;
  esac
done <<< "${labels}"

[ -s "${list}" ] || { echo "empty change list" >&2; exit 3; }
[ "$(tail -c1 "${list}" | od -An -tx1 | tr -d ' \n')" = "00" ] || { echo "change list is not NUL-terminated" >&2; exit 3; }

n=0
while IFS= read -r -d '' path; do
  [ -n "${path}" ] || { echo "empty record in change list" >&2; exit 3; }
  n=$((n + 1))
done < "${list}"

echo "skip no ci:integration or ci:full label (${n} changed files); the shards run in the merge queue"
