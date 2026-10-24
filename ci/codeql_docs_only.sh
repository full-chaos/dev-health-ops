#!/usr/bin/env bash
# Reads, on stdin only, a pull request's changed files and prints "true" only
# when the pull request is provably docs-only; anything else prints "false".
# Input: line 1 = the pull request's changed_files count; then one line per
# changed file: the path, a tab and the previous path for a rename (the
# previous path is absent for other changes).
# "true" needs ALL of: 1 <= count <= 3000 (the files API cap); exactly count
# file lines (a short or long list means truncation: fail closed); every path
# and every rename source is a docs path: docs/**, docs-data/**, any *.md,
# .github/docs-legacy/**, or mkdocs.yml at the repository root.
set -u
fail() {
  echo false
  exit 0
}
IFS= read -r expected || fail
case "$expected" in '' | *[!0-9]*) fail ;; esac
[ "${#expected}" -le 4 ] && [ "$expected" -ge 1 ] && [ "$expected" -le 3000 ] || fail
rows=0
while IFS= read -r row || [ -n "$row" ]; do
  rows=$((rows + 1))
  [ -n "$row" ] || fail
  IFS=$'\t' read -r -a parts <<<"$row"
  [ "${#parts[@]}" -ge 1 ] && [ "${#parts[@]}" -le 2 ] || fail
  for path in "${parts[@]}"; do
    case "$path" in
      docs/* | docs-data/* | *.md | .github/docs-legacy/* | mkdocs.yml) ;;
      *) fail ;;
    esac
  done
done
[ "$rows" -eq "$expected" ] || fail
echo true
