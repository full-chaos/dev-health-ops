#!/usr/bin/env bash
# Red/green coverage for ci/mirror_precheck_lib.sh (CHAOS-4928 codex round 1,
# P1 fix). Hermetic on purpose: `docker` is replaced with a shell function
# defined in THIS script, so no registry, no network, and no real image is
# ever touched. The stub only answers the two invocations the library
# actually makes -- anything else is a test bug, not silently ignored, and
# fails loud (see `docker()` below).
#
# Run directly: bash ci/mirror_precheck_lib_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=ci/mirror_precheck_lib.sh
source "${ROOT}/ci/mirror_precheck_lib.sh"

declare -A FAKE_DIGEST=()   # dest -> digest string docker would report present
declare -A FAKE_EXISTS=()   # dest -> set (any value) means --raw succeeds

docker() {
  if [ "${1:-}" = buildx ] && [ "${2:-}" = imagetools ] && [ "${3:-}" = inspect ]; then
    case "${4:-}" in
      --format)
        local dest="${6:-}"
        if [ -n "${FAKE_DIGEST[${dest}]+set}" ]; then
          printf '%s\n' "${FAKE_DIGEST[${dest}]}"
          return 0
        fi
        return 1
        ;;
      --raw)
        local dest="${5:-}"
        [ -n "${FAKE_EXISTS[${dest}]+set}" ]
        return
        ;;
    esac
  fi
  echo "TEST BUG: unstubbed docker invocation: $*" >&2
  return 99
}

failures=0
pass=0

# assert_case NAME OWNER IMAGE WANT_DECISION WANT_REASON
assert_case() {
  local name="$1" owner="$2" image="$3" want_decision="$4" want_reason="$5"
  local line status decision reason

  status=0
  line="$(mirror_image_missing "${owner}" "${image}")" || status=$?

  decision="$(printf '%s' "${line}" | grep -oE 'decision=[a-z]+' | cut -d= -f2)"
  reason="$(printf '%s' "${line}" | grep -oE 'reason=[a-z-]+' | cut -d= -f2)"

  local got_decision
  if [ "${status}" -eq 0 ]; then got_decision=present; else got_decision=missing; fi

  if [ "${got_decision}" = "${want_decision}" ] && [ "${decision}" = "${want_decision}" ] && [ "${reason}" = "${want_reason}" ]; then
    printf 'PASS %s (%s)\n' "${name}" "${line}"
    pass=$((pass + 1))
  else
    printf 'FAIL %s: want decision=%s reason=%s, got exit=%s line=%q\n' \
      "${name}" "${want_decision}" "${want_reason}" "${status}" "${line}" >&2
    failures=$((failures + 1))
  fi
}

OWNER=full-chaos

# 1. Matching digest -> present. (green: dest already carries the pin. The
# destination tag EXISTS here too -- FAKE_EXISTS is set alongside FAKE_DIGEST
# so this fixture is also valid input to the pre-fix presence-only function,
# for the red-first run described in this file's own proof, not exercised by
# the fixed library itself.)
FAKE_DIGEST['ghcr.io/full-chaos/postgres:18-alpine']='sha256:AAA1'
FAKE_EXISTS['ghcr.io/full-chaos/postgres:18-alpine']=1
assert_case "digest-match" "${OWNER}" \
  'postgres:18-alpine@sha256:AAA1' present digest-match

# 2. Stale tag: dest TAG EXISTS (FAKE_EXISTS set) but carries the OLD digest
# -- this is the exact CHAOS-4928 P1 shape (a PR bumps @sha256:OLD ->
# @sha256:NEW while the mirror still serves OLD under the same tag; a
# presence-only check sees the tag and reports present). Must read
# missing=true here, not present.
FAKE_DIGEST['ghcr.io/full-chaos/postgres:18-alpine']='sha256:AAA1'
FAKE_EXISTS['ghcr.io/full-chaos/postgres:18-alpine']=1
assert_case "stale-tag-mismatch" "${OWNER}" \
  'postgres:18-alpine@sha256:AAA2' missing digest-mismatch

# 3. Digest-pinned, destination tag does not exist at all yet.
unset 'FAKE_DIGEST[ghcr.io/full-chaos/edoburu/pgbouncer:1.25.2]'
assert_case "digest-pinned-absent" "${OWNER}" \
  'edoburu/pgbouncer:1.25.2@sha256:BBB1' missing absent

# 4. Unpinned upstream tag, destination already present -- presence-only path
# is UNCHANGED behavior (this fix only changes the digest-pinned branch).
FAKE_EXISTS['ghcr.io/full-chaos/clickhouse/clickhouse-server:26.7']=1
assert_case "unpinned-tag-present" "${OWNER}" \
  'clickhouse/clickhouse-server:26.7' present presence-only-tag-exists

# 5. Unpinned upstream tag, nothing mirrored yet.
unset 'FAKE_EXISTS[ghcr.io/full-chaos/valkey/valkey:8-alpine]'
assert_case "unpinned-tag-absent" "${OWNER}" \
  'valkey/valkey:8-alpine' missing presence-only-tag-absent

printf '\n%d passed, %d failed\n' "${pass}" "${failures}"
[ "${failures}" -eq 0 ]
