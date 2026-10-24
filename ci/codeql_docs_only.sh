#!/usr/bin/env bash
# Usage: codeql_docs_only.sh <base-sha> <head-sha>
# Prints "true" only when the pull request between the two commits is provably
# docs-only; anything else (bad input, a git error, an empty diff, any other
# path) prints "false". The paths come from git itself for exactly these two
# commits (three-dot, no rename detection so a rename lists its source too),
# never from an API that answers for a later head. A path is a docs path when
# it is docs/**, docs-data/**, any *.md, .github/docs-legacy/**, or mkdocs.yml
# at the repository root, and it is not Go source or a Go module file.
set -u
fail() {
  echo false
  exit 0
}
[ "$#" -eq 2 ] || fail
base=$1
head=$2
[[ $base =~ ^[0-9a-f]{40}$ && $head =~ ^[0-9a-f]{40}$ ]] || fail
# Paths are read NUL-delimited, so a path holding a newline stays one path. The
# diff is written to a file first so a git failure (even after partial output)
# is seen and fails closed; an empty diff yields no path, which is not docs-only.
list=$(mktemp)
trap 'rm -f "$list"' EXIT
git diff --name-only --no-renames -z "$base...$head" >"$list" || fail
count=0
while IFS= read -r -d '' path; do
  count=$((count + 1))
  case "$path" in
    # Go source and module files are what CodeQL's Go leg analyzes, wherever they sit (docs/ holds a Go package too).
    *.go | go.mod | */go.mod | go.sum | */go.sum | go.work | */go.work | go.work.sum | */go.work.sum) fail ;;
    docs/* | docs-data/* | *.md | .github/docs-legacy/* | mkdocs.yml) ;;
    *) fail ;;
  esac
done <"$list"
[ "$count" -ge 1 ] || fail
echo true
