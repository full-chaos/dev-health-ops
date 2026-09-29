#!/usr/bin/env bash
# check-web-env-required.sh <env-names-file> <NAME>... -- exit 0 only when every NAME is present in the
# file (one env var NAME per line, as container-env-names.sh prints them). A missing name is named on
# stderr and exits 1; an unreadable or empty file exits 2, so a measurement that did not happen fails
# loudly instead of reading as "all present". NAMES only, never a value.
set -euo pipefail
file=${1:-}; shift || true
[ -n "$file" ] && [ "$#" -gt 0 ] || { echo "usage: check-web-env-required.sh <names-file> <NAME>..." >&2; exit 2; }
[ -s "$file" ] || { echo "FAIL: $file is missing or empty -- the container's env names were not read" >&2; exit 2; }
missing=0
for name in "$@"; do
  if ! grep -qx -- "$name" "$file"; then
    echo "FAIL: web env is missing $name (compose chain lacks the router overlay?)" >&2
    missing=1
  fi
done
exit "$missing"
