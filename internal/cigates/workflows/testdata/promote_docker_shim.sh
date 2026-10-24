#!/usr/bin/env bash
key() { printf '%s' "$1" | tr '/:@' '___'; }
if [ "$1 $2 $3" = "buildx imagetools inspect" ]; then
  f="${FIXTURES}/inspect_$(key "$4").json"
  [ -f "${f}" ] || exit 1
  cat "${f}"
  exit 0
fi
if [ "$1 $2 $3" = "buildx imagetools create" ]; then
  [ -z "${CREATE_FAIL:-}" ] || exit 1
  shift 3
  printf '%s\n' "$*" >>"${RECORD_FILE}"
  exit 0
fi
exit 1
