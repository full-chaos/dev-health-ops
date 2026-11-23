#!/usr/bin/env bash
# CHAOS-11108: red/green coverage for the change-set reader of `ci/check_go.sh venue-oracles --changed`
# (vo_paths, venue_oracle_changed_dirs, venue_oracle_changed_selects). The functions are cut out of the real script,
# so the test cannot drift from it. Hermetic: no Go, no network.
# Run: bash ci/venue_oracle_changed_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
T="$(mktemp -d)"
trap 'rm -rf "${T}"' EXIT

awk '/^vo_grep\(\)/ { p = 1 } /^# venue_oracle_skips/ { p = 0 } p' "${ROOT}/ci/check_go.sh" > "${T}/lib.sh"
[ -s "${T}/lib.sh" ] || { printf 'FAIL could not cut the change-set reader out of ci/check_go.sh\n'; exit 1; }
die() { printf 'DIE %s\n' "$*" >&2; exit 2; }
# shellcheck disable=SC1091
. "${T}/lib.sh"

fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }

VENUE_CLOSURE_DIR=""
VENUE_TRIGGER_ROWS=""
VENUE_CHANGED_FILE="${T}/changed"
export VENUE_CLOSURE_DIR VENUE_TRIGGER_ROWS VENUE_CHANGED_FILE

same() { if [ "$1" = "$2" ]; then ok "$3"; else bad "$3: got '$1', want '$2'"; fi; }
selects() { venue_oracle_changed_selects "$1" "${2:-}"; }
expect() { # NAME WANT(selected|not) PACKAGE [TEST]
  local got=not
  if selects "$3" "${4:-}"; then got=selected; fi
  if [ "${got}" = "$2" ]; then ok "$1"; else bad "$1: ${3} is ${got}, want ${2}"; fi
}

printf 'internal/pkg/x.go\0' > "${VENUE_CHANGED_FILE}"
expect "NUL: a file in the package selects it" selected internal/pkg
printf 'internal/pkg/\nx.go\0' > "${VENUE_CHANGED_FILE}"
expect "NUL: a name with an embedded newline selects its package" selected internal/pkg
printf 'internal/pkg/sub/\nx.go\0internal/other/y.go\0' > "${VENUE_CHANGED_FILE}"
expect "NUL: a newline name in a sub-package does not select the parent" not internal/pkg
printf 'a/b\ninternal/pkg/x.go\0' > "${VENUE_CHANGED_FILE}"
expect "NUL: a newline in an earlier directory does not fake a path of the package" not internal/pkg
printf 'internal/pkg/testdata/\ngolden.json\0' > "${VENUE_CHANGED_FILE}"
expect "NUL: a newline name under testdata selects the package" selected internal/pkg
printf 'internal/pkg/x.go\0internal/pkg/\ny.go\0' > "${VENUE_CHANGED_FILE}"
dirs="$(venue_oracle_changed_dirs "${VENUE_CHANGED_FILE}" | tr '\001' '@')"
same "${dirs}" "internal/pkg" "NUL: the changed dirs hold the package once"

printf 'internal/pkg/x.go\n' > "${VENUE_CHANGED_FILE}"
expect "newline list: a file in the package selects it" selected internal/pkg
printf 'internal/pkg/testdata/a/b.json\n' > "${VENUE_CHANGED_FILE}"
expect "newline list: a file under testdata selects the package" selected internal/pkg
printf 'internal/pkg/sub/x.go\ninternal/pkgx/y.go\n' > "${VENUE_CHANGED_FILE}"
expect "newline list: a sub-package or a sibling prefix does not select it" not internal/pkg
printf 'cmd/tool/main.go\n' > "${VENUE_CHANGED_FILE}"
VENUE_TRIGGER_ROWS=$'internal/pkg\tTestA\tcmd/tool'
expect "newline list: a trigger path selects its test" selected internal/pkg TestA
expect "newline list: a trigger path does not select another test" not internal/pkg TestB

# The newline-list answer is the answer of the reader this one replaced (tr '\0' '\n').
VENUE_TRIGGER_ROWS=""
printf 'internal/pkg/x.go\ninternal/a/b.go\ninternal/pkg/testdata/g.json\nREADME.md\n' > "${VENUE_CHANGED_FILE}"
old_dirs="$(tr '\0' '\n' < "${VENUE_CHANGED_FILE}" | awk 'NF { d = $0; sub(/\/[^\/]*$/, "", d); sub(/\/testdata(\/.*)?$/, "", d); print d }' | LC_ALL=C sort -u)"
new_dirs="$(venue_oracle_changed_dirs "${VENUE_CHANGED_FILE}")"
same "${new_dirs}" "${old_dirs}" "newline list: the changed dirs equal the old reader's"
same "$(vo_paths "${VENUE_CHANGED_FILE}" | cksum)" "$(cksum < "${VENUE_CHANGED_FILE}")" "newline list: the lines are the file, byte for byte"

if [ "${fails}" -ne 0 ]; then printf '%d case(s) failed\n' "${fails}"; exit 1; fi
printf 'venue_oracle_changed_test: all cases passed\n'
