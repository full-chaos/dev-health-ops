#!/usr/bin/env bash
# Usage: codeql_docs_only.sh <base-sha> <head-sha>
# Prints "true" only when the pull request between the two commits is provably
# docs-only; anything else (bad input, a git error, an empty diff, any other
# path) prints "false". The paths come from git itself for exactly these two
# commits (three-dot, no rename detection so a rename lists its source too),
# never from an API that answers for a later head. A path is a docs path when
# it is docs/**, docs-data/**, any *.md, .github/docs-legacy/**, or mkdocs.yml
# at the repository root.
set -u
fail() {
  echo false
  exit 0
}
[ "$#" -eq 2 ] || fail
base=$1
head=$2
[[ $base =~ ^[0-9a-f]{40}$ && $head =~ ^[0-9a-f]{40}$ ]] || fail
# An empty diff or a git error leaves paths empty; the loop then reads one empty
# path, which is not a docs path, and fails.
paths=$(git diff --name-only --no-renames -z "$base...$head" | tr '\0' '\n')
while IFS= read -r path; do
  case "$path" in
    docs/* | docs-data/* | *.md | .github/docs-legacy/* | mkdocs.yml) ;;
    *) fail ;;
  esac
done <<<"$paths"
echo true
