#!/usr/bin/env bash
# Reads the changed paths of a pull request on stdin, one per line, and prints
# "true" only when there is at least one path and every path is a docs path:
# docs/**, docs-data/**, any *.md, .github/docs-legacy/**, or mkdocs.yml at the
# repository root. Anything else, an empty list or a blank line prints "false".
set -u
count=0
while IFS= read -r path || [ -n "$path" ]; do
  case "$path" in
    docs/* | docs-data/* | *.md | .github/docs-legacy/* | mkdocs.yml) count=$((count + 1)) ;;
    *)
      echo false
      exit 0
      ;;
  esac
done
if [ "$count" -gt 0 ]; then echo true; else echo false; fi
