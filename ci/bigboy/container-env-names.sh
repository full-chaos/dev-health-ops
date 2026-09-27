#!/usr/bin/env bash
# container-env-names.sh -- the ONLY sanctioned way to read a container's environment on
# bigboy (R462/R463, CHAOS-6987). Prints the env var NAMES of one container, sorted, one
# per line. Never a value.
#
# Two layers keep values out of every pipe, file and screen:
#   1. The name is cut INSIDE docker's own Go template (`index (split . "=") 0`), so a value
#      never leaves the docker CLI process.
#   2. Every emitted line is re-checked against the env-name shape; a line that does not
#      match (a template regression, a future docker change) is replaced with the literal
#      `<non-name-line>` and the script exits 3 -- it never echoes the unexpected text.
#
# Usage: container-env-names.sh <container name or id>
# Exit: 0 names printed; 2 usage/inspect failure (named on stderr); 3 non-name line seen.
set -euo pipefail
DOCKER=${DOCKER:-docker}
c=${1:-}
[ -n "$c" ] || { echo "usage: container-env-names.sh <container>" >&2; exit 2; }

if ! raw=$("$DOCKER" inspect --type container \
    --format '{{range .Config.Env}}{{index (split . "=") 0}}{{println}}{{end}}' "$c" 2>/dev/null); then
  echo "FAIL: docker inspect failed for container '$c' (absent or not a container)" >&2
  exit 2
fi

bad=0
out=""
while IFS= read -r line; do
  [ -n "$line" ] || continue
  if [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    out+="$line"$'\n'
  else
    out+="<non-name-line>"$'\n'
    bad=1
  fi
done <<<"$raw"
unset raw
printf '%s' "$out" | sort -u
if [ "$bad" -ne 0 ]; then
  echo "FAIL: inspect produced a line that is not an env var name; its text was withheld" >&2
  exit 3
fi
