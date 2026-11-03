#!/usr/bin/env bash
# CHAOS-12372: bounded retry of a BuildKit image build whose base-image fetch failed for a reason outside the product.
# Sourced; it sets no shell option and never exits the caller.
#   registry_transient LOGFILE          prints the class (T1..T3) and returns 0 when LOGFILE holds a whole line of a transient
#                                       registry class and no permanent or product line; returns 1 otherwise
#   registry_retry_run NAME -- COMMAND  runs COMMAND at most 3 times, 10 s then 30 s apart (retry_named), and retries only
#                                       when the try's output is transient. Each try's whole output goes to the console
#                                       (stdout and stderr merged) and to a per-try file under RUNNER_TEMP or TMPDIR that is
#                                       removed at the end. Returns 0 on success, else the last try's rc.
# Classes: T1 a blob fetch of a digest-pinned image answered "not found" by the mirror (layer, config or other media type);
# T2 a 429 or 5xx from mirror.gcr.io; T3 a network error on a registry fetch. A permanent line anywhere vetoes the retry.

# shellcheck source=ci/lib/retry.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/retry.sh"

_REGISTRY_RETRY_DIR=""
_REGISTRY_RETRY_N=0
_REGISTRY_RETRY_NAME=""

registry_transient() {
  local text
  text="$(tr -d '\r' <"$1")" || return 1
  if LC_ALL=C grep -aEq '(manifest unknown|denied|unauthorized|pull access denied|repository does not exist|no match for platform in manifest|invalid reference format)|^manifest [^ ]+ not found$|^#[0-9]+ ERROR: process "|^dependency failed to start: |is unhealthy|exited \([1-9][0-9]*\)|didn.t complete successfully' <<<"${text}"; then
    return 1
  fi
  if LC_ALL=C grep -aEq '^#[0-9]+ ERROR: failed to copy: httpReadSeeker: failed open: could not fetch content descriptor sha256:[0-9a-f]{64} \(application/vnd\.(oci|docker)\.[A-Za-z0-9.+-]+\) from remote: not found$' <<<"${text}"; then
    echo T1
    return 0
  fi
  if LC_ALL=C grep -aEq '^#[0-9]+ ERROR: failed to .*unexpected status from (GET|HEAD) request to https://mirror\.gcr\.io/[^ ]*: (429 Too Many Requests|50[0234] [A-Za-z ]+)$' <<<"${text}"; then
    echo T2
    return 0
  fi
  if LC_ALL=C grep -aEq '^#[0-9]+ ERROR: failed to .*(TLS handshake timeout|i/o timeout|connection reset by peer|unexpected EOF)$' <<<"${text}"; then
    echo T3
    return 0
  fi
  return 1
}

_registry_retry_when() {
  local class
  class="$(registry_transient "${_REGISTRY_RETRY_DIR}/try-${_REGISTRY_RETRY_N}.log")" || return 1
  printf 'registry-retry: name=%s attempt=%d/3 class=%s transient registry error, retrying\n' "${_REGISTRY_RETRY_NAME}" "${_REGISTRY_RETRY_N}" "${class}" >&2
}

_registry_retry_try() {
  local st=0
  _REGISTRY_RETRY_N=$((_REGISTRY_RETRY_N + 1))
  { "$@"; } 2>&1 | tee -- "${_REGISTRY_RETRY_DIR}/try-${_REGISTRY_RETRY_N}.log"
  st="${PIPESTATUS[0]}"
  return "${st}"
}

registry_retry_run() {
  local name="${1-}" rc=0
  if [ "$#" -lt 3 ] || [ "${2}" != "--" ]; then
    printf 'registry-retry: usage: registry_retry_run NAME -- COMMAND [ARGS...]\n' >&2
    return 2
  fi
  shift 2
  _REGISTRY_RETRY_DIR="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/registry-retry.XXXXXX")" || return 1
  _REGISTRY_RETRY_N=0
  _REGISTRY_RETRY_NAME="${name}"
  RETRY_WHEN=_registry_retry_when retry_named "${name}" 3 "10 30" -- _registry_retry_try "$@" || rc=$?
  rm -rf -- "${_REGISTRY_RETRY_DIR}"
  return "${rc}"
}
