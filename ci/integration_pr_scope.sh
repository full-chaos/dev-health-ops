#!/usr/bin/env bash
# CHAOS-10700, CHAOS-13075: decide whether a pull request runs the Go integration shards.
# Usage: integration_pr_scope.sh <changed-file-list>
#   env: PR_LABELS_JSON (JSON array of label names), PR_DRAFT (true|false), MERGE_QUEUE (the repository variable)
# The list is the NUL-terminated output of ci/go_relevant_diff.sh.
# stdout: "run <reason>" (let the planner pick shards) or "skip <draft|queue> <reason>" (no shard runs).
# Any bad input (usage, unreadable labels, a PR_DRAFT that is not true/false, empty list, unterminated or empty record)
# exits non-zero and prints no verdict: the caller must then fall back to the planner (run), never to a skip.
# A ci:integration or ci:full label runs the planner on any pull request. A ready pull request runs the planner. A
# draft skips (it runs at ready_for_review), and so does a pull request when the merge queue is enabled (the queue runs the
# shards for every entry). The go-storage-integration aggregate never reports a draft skip as success.
set -euo pipefail

list="${1:-}"
[ -n "${list}" ] && [ -f "${list}" ] || { echo "usage: integration_pr_scope.sh <changed-file-list>" >&2; exit 2; }

labels="$(jq -r 'if type == "array" and all(.[]; type == "string") then .[] else error("not an array of strings") end' <<< "${PR_LABELS_JSON:-[]}")" || { echo "PR_LABELS_JSON is not a JSON array" >&2; exit 3; }
while IFS= read -r label; do
  case "${label}" in
    ci:integration | ci:full) echo "run label ${label}"; exit 0 ;;
  esac
done <<< "${labels}"

case "${PR_DRAFT:-}" in
  true | false) ;;
  *) echo "PR_DRAFT is '${PR_DRAFT:-}', not true or false" >&2; exit 3 ;;
esac

[ -s "${list}" ] || { echo "empty change list" >&2; exit 3; }
[ "$(tail -c1 "${list}" | od -An -tx1 | tr -d ' \n')" = "00" ] || { echo "change list is not NUL-terminated" >&2; exit 3; }

n=0
while IFS= read -r -d '' path; do
  [ -n "${path}" ] || { echo "empty record in change list" >&2; exit 3; }
  n=$((n + 1))
done < "${list}"

if [ "${PR_DRAFT}" = true ]; then
  echo "skip draft a draft pull request without a ci:integration or ci:full label (${n} changed files); the shards run when it is marked ready"
elif [ "${MERGE_QUEUE:-}" = enabled ]; then
  echo "skip queue no ci:integration or ci:full label (${n} changed files); the shards run in the merge queue"
else
  echo "run ready pull request (${n} changed files); the planner selects the shards"
fi
