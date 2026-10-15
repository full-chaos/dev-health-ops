#!/usr/bin/env bash
# check-host-copies.sh (CHAOS-8693): are the tracked bigboy base files byte-equal to the host files the live stack still
# runs? Read-only: it reads both sides and prints one line per file. Exit 1 when a static copy differs.
#   compose.bigboy.base.yml       == $BIGBOY_ROOT/compose.yml
#   compose.bigboy.go-workers.yml == $BIGBOY_ROOT/compose/compose.go.workers.yml
# The image pin file is per-cut state, not config: it is compared on its non-comment lines and a difference is only
# reported (a cut re-pins the host file).
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
R=$(cd "${BIGBOY_ROOT:-/home/ubuntu/devhealth}" && pwd)
rc=0
same() { # <tracked> <host> <strict>
  if [ ! -f "$2" ]; then echo "MISSING host file $2"; if [ "$3" = strict ]; then rc=1; fi; return 0; fi
  if cmp -s "$1" "$2"; then echo "SAME $(sha256sum "$1" | cut -c1-12) ${1##*/}"; else echo "DIFF ${1##*/} vs $2"; if [ "$3" = strict ]; then rc=1; fi; fi
}
same "$HERE/compose.bigboy.base.yml" "$R/compose.yml" strict
same "$HERE/compose.bigboy.go-workers.yml" "$R/compose/compose.go.workers.yml" strict
host_pin="$R/compose/compose.bigboy.images.yml"
if [ -f "$host_pin" ] && diff -q <(grep -vE '^[[:space:]]*#' "$HERE/compose.bigboy.images.yml") <(grep -vE '^[[:space:]]*#' "$host_pin") >/dev/null; then
  echo "SAME pin file (non-comment lines)"
else
  echo "INFO pin file differs or is missing: it is per-cut state, re-pinned by the cut"
fi
exit "$rc"
