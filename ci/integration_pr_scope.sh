#!/usr/bin/env bash
# CHAOS-10700: decide whether a pull request runs the Go integration shards.
# Usage: integration_pr_scope.sh <changed-file-list>   env: PR_LABELS (one label per line)
# The list is the NUL-separated output of ci/go_relevant_diff.sh.
# stdout: "run <reason>" (let the planner pick shards) or "skip <reason>" (no shard runs).
# Exit non-zero on a bad input: the caller must then fall back to the planner.
# Store .go code is not a trigger: integration runs on every main push. A lane that wants the shards on its PR
# sets the label ci:integration before its push; a label change alone starts no run.
set -euo pipefail

list="${1:-}"
[ -n "${list}" ] && [ -f "${list}" ] || { echo "usage: integration_pr_scope.sh <changed-file-list>" >&2; exit 2; }

while IFS= read -r label; do
  case "${label}" in
    ci:integration | ci:full) echo "run label ${label}"; exit 0 ;;
  esac
done <<< "${PR_LABELS:-}"

n=0
while IFS= read -r -d '' path; do
  n=$((n + 1))
  case "${path}" in
    *.sql | go.mod | go.sum | */go.mod | */go.sum) echo "run trigger path ${path}"; exit 0 ;;
    migrations/* | */migrations/*) echo "run trigger path ${path}"; exit 0 ;;
    internal/chmigrate/* | internal/pgmigrate/* | internal/rivermigrate/* | internal/migrationmatrix/*) echo "run trigger path ${path}"; exit 0 ;;
    compose*.yml | */compose*.yml) echo "run trigger path ${path}"; exit 0 ;;
    .github/workflows/* | internal/cigates/* | ci/*) echo "run trigger path ${path}"; exit 0 ;;
  esac
done < "${list}"

[ "${n}" -gt 0 ] || { echo "empty change list" >&2; exit 3; }
echo "skip no trigger path and no ci:integration label in ${n} changed files"
