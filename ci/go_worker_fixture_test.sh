#!/usr/bin/env bash
# Coverage for clickhouse_http_sink in ci/lib/go_worker_fixture.sh (CHAOS-7301, r1 P1): the sink
# `dho fixtures generate` takes must name the HTTP scheme explicitly, because dho reads a
# clickhouse:// DSN as the native protocol except on port 8123. Run: bash ci/go_worker_fixture_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="${ROOT}/ci/lib/go_worker_fixture.sh"
# Load only the function under test: the pinned lint run has no follow-sources option, so sourcing
# the whole library is not analysable there, and the test needs nothing else from it.
FUNC="$(sed -n '/^clickhouse_http_sink() {/,/^}/p;/^clickhouse_native_uri() {/,/^}/p' "${LIB}")"
for name in clickhouse_http_sink clickhouse_native_uri; do
  if ! grep -q "^${name}() {" <<<"${FUNC}"; then
    echo "FAIL: ${name} is not defined in ${LIB}"
    exit 1
  fi
done
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

expect_native() { # expect_native HTTP_URI PORT WANT
  local got
  got="$(clickhouse_native_uri "$1" "$2" 2>/dev/null)" || got="<refused>"
  if [ "${got}" != "$3" ]; then
    echo "FAIL: clickhouse_native_uri '$1' '$2' = '${got}', want '$3'"
    fail=1
  fi
}

# One address, every part honoured on the native side: the default, then an override of each part.
expect_native 'clickhouse://ch:ch@127.0.0.1:8123/default' 9000 'clickhouse://ch:ch@127.0.0.1:9000/default'
expect_native 'clickhouse://ch:ch@127.0.0.1:18123/default' 19000 'clickhouse://ch:ch@127.0.0.1:19000/default'
expect_native 'clickhouse://u2:ch@127.0.0.1:8123/default' 9000 'clickhouse://u2:ch@127.0.0.1:9000/default'
expect_native 'clickhouse://ch:p2@127.0.0.1:8123/default' 9000 'clickhouse://ch:p2@127.0.0.1:9000/default'
expect_native 'clickhouse://ch:ch@ch.internal:8123/default' 9000 'clickhouse://ch:ch@ch.internal:9000/default'
expect_native 'clickhouse://ch:ch@127.0.0.1:8123/review_db' 9000 'clickhouse://ch:ch@127.0.0.1:9000/review_db'
expect_native 'http://ch:ch@127.0.0.1:8123/review_db' 9000 'clickhouse://ch:ch@127.0.0.1:9000/review_db'
expect_native 'https://ch:ch@ch.example:8443/review_db' 9440 'clickhouses://ch:ch@ch.example:9440/review_db'
expect_native 'clickhouse://127.0.0.1:8123/default' 9000 'clickhouse://127.0.0.1:9000/default'
expect_native 'clickhouse://ch:ch@127.0.0.1:8123' 9000 'clickhouse://ch:ch@127.0.0.1:9000'
# An address that cannot be honoured on both sides is refused loudly.
expect_native 'clickhouse://ch:ch@127.0.0.1/default' 9000 '<refused>'
expect_native 'tcp://ch:ch@127.0.0.1:9000/default' 9000 '<refused>'
expect_native 'clickhouse://ch:ch@127.0.0.1:8123/default' 'nine' '<refused>'
expect_native '' 9000 '<refused>'

# The sink and the native DSN of one HTTP DSN agree on credentials, host and database.
for uri in 'clickhouse://a:b@h1:18123/db1' 'http://c:d@h2:8123/db2' 'https://e:f@h3:8443/db3'; do
  sink="$(clickhouse_http_sink "${uri}")"
  native="$(clickhouse_native_uri "${uri}" 9000)"
  if [ "${sink#*://}" != "${native#*://}" ] && [ "$(sed -E 's#^[a-z]+://([^@]*@)?([^:/]+):[0-9]+(/.*)?$#\1\2\3#' <<<"${sink}")" != "$(sed -E 's#^[a-z]+://([^@]*@)?([^:/]+):[0-9]+(/.*)?$#\1\2\3#' <<<"${native}")" ]; then
    echo "FAIL: sink '${sink}' and native '${native}' name different servers or databases"
    fail=1
  fi
done

# A refused scheme must also make a calling `VAR="$(...)"` assignment stop a set -e script.
out="$(bash -c 'set -euo pipefail; eval "$1"; _="$(clickhouse_http_sink tcp://x 2>/dev/null)"; echo reached' _ "${FUNC}" 2>/dev/null || true)"
if [ "${out}" = "reached" ]; then
  echo "FAIL: a refused scheme did not stop a set -e caller"
  fail=1
fi

[ "${fail}" -eq 0 ] && echo "ok: clickhouse_http_sink, clickhouse_native_uri"
exit "${fail}"
