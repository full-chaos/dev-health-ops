#!/usr/bin/env bash
key() { printf '%s' "$1" | tr '/:@' '___'; }
if [ "$1" = "api" ]; then
  f="${FIXTURES}/api_$(key "$2").json"
  [ -f "${f}" ] || exit 1
  cat "${f}"
  exit 0
fi
if [ "$1" = "attestation" ] && [ "$2" = "verify" ]; then
  f="${FIXTURES}/att_$(key "${3#oci://}").json"
  [ -f "${f}" ] || exit 1
  cat "${f}"
  exit 0
fi
exit 1
