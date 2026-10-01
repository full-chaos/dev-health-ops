#!/usr/bin/env bash
# Coverage for clickhouse_http_sink in ci/lib/go_worker_fixture.sh (CHAOS-7301, r1 P1): the sink
# `dho fixtures generate` takes must name the HTTP scheme explicitly, because dho reads a
# clickhouse:// DSN as the native protocol except on port 8123. Run: bash ci/go_worker_fixture_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="${ROOT}/ci/lib/go_worker_fixture.sh"
# Load only the function under test: the pinned lint run has no follow-sources option, so sourcing
# the whole library is not analysable there, and the test needs nothing else from it.
FUNC="$(sed -n '/^clickhouse_http_sink() {/,/^}/p' "${LIB}")"
if [ -z "${FUNC}" ]; then
  echo "FAIL: clickhouse_http_sink is not defined in ${LIB}"
  exit 1
fi
eval "${FUNC}"

fail=0
expect() { # expect INPUT WANT
  local got
  got="$(clickhouse_http_sink "$1" 2>/dev/null)" || got="<refused>"
  if [ "${got}" != "$2" ]; then
    echo "FAIL: clickhouse_http_sink '$1' = '${got}', want '$2'"
    fail=1
  fi
}

expect 'clickhouse://ch:ch@127.0.0.1:8123/default' 'http://ch:ch@127.0.0.1:8123/default'
expect 'clickhouse://ch:ch@127.0.0.1:18123/default' 'http://ch:ch@127.0.0.1:18123/default'
expect 'http://ch:ch@127.0.0.1:18123/default' 'http://ch:ch@127.0.0.1:18123/default'
expect 'https://ch:ch@ch.example:8443/default' 'https://ch:ch@ch.example:8443/default'
expect 'clickhouses://ch:ch@ch.example:8443/default' '<refused>'
expect 'tcp://ch:ch@127.0.0.1:9000/default' '<refused>'
expect '127.0.0.1:8123' '<refused>'
expect '' '<refused>'

# A refused scheme must also make a calling `VAR="$(...)"` assignment stop a set -e script.
out="$(bash -c 'set -euo pipefail; eval "$1"; _="$(clickhouse_http_sink tcp://x 2>/dev/null)"; echo reached' _ "${FUNC}" 2>/dev/null || true)"
if [ "${out}" = "reached" ]; then
  echo "FAIL: a refused scheme did not stop a set -e caller"
  fail=1
fi

[ "${fail}" -eq 0 ] && echo "ok: clickhouse_http_sink"
exit "${fail}"
