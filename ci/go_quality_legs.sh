#!/usr/bin/env bash
# The go-quality-leg matrix (ci/go_quality_legs.json), judged for one event (CHAOS-11418). A heavy (race) leg is left out
# only on a pull_request without the ci:full label; every other event, and an event this script does not know, keeps all legs.
# Usage: go_quality_legs.sh <event_name> <ci_full: true|false> [legs.json]
# stdout line 1: the matrix JSON array. stdout line 2: the race policy line (empty when every leg is kept).
set -euo pipefail
event="${1:?event name}"
ci_full="${2:?ci:full flag (true|false)}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
legs="${3:-${here}/go_quality_legs.json}"
case "${ci_full}" in true | false) ;; *) echo "go_quality_legs.sh: ci:full flag must be true or false, got '${ci_full}'" >&2; exit 2 ;; esac
if [ "${event}" = pull_request ] && [ "${ci_full}" = false ]; then
  matrix="$(jq -ce '[.[] | select((.heavy // false) | not)]' "${legs}")"
  policy="race: not run on PR by policy (runs in merge_group, on the main push and with the ci:full label)"
else
  matrix="$(jq -ce '.' "${legs}")"
  policy=""
fi
# An empty list is a wiring error, never "nothing to run": the fan-in must see a red plan job.
if ! jq -e 'type == "array" and length > 0 and any(.[]; .name == "static")' <<<"${matrix}" >/dev/null; then
  echo "go_quality_legs.sh: the leg list is empty or has no static leg" >&2
  exit 3
fi
# A repeated leg name pads the matrix: the queue-tested check counts distinct leg names, and a duplicate hides a missing leg.
dups="$(jq -r '[.[] | .name] | group_by(.) | map(select(length > 1) | .[0]) | join(", ")' "${legs}")"
if [ -n "${dups}" ]; then
  echo "go_quality_legs.sh: duplicate leg name(s) in the leg list: ${dups}" >&2
  exit 3
fi
# The whole leg file must have the shape the workflow, the shard plans and the queue-tested check rely on, whatever the event: one static
# leg with args "static" (a missing static leg is refused above, a repeated name by the duplicate check), test-K-of-N with args "test K N" for K = 1..N, race-K-of-M (heavy) with args "race K M" for K = 1..M, no other name.
# Every line of the program below but the last is one check, so a dropped check is a dropped line.
shape="$(jq -r '
  [.[] | select((.name | type) == "string")] as $l
  | [$l[] | select(.name == "static")] as $st
  | [$l[] | select(.name | test("^test-[0-9]+-of-[0-9]+$"))] as $t
  | [$l[] | select(.name | test("^race-[0-9]+-of-[0-9]+$"))] as $r
  | [$t[] | .name | capture("^test-(?<k>[0-9]+)-of-(?<n>[0-9]+)$") | {k: (.k | tonumber), n: (.n | tonumber)}] as $tk
  | [$r[] | .name | capture("^race-(?<k>[0-9]+)-of-(?<n>[0-9]+)$") | {k: (.k | tonumber), n: (.n | tonumber)}] as $rk
  | [
    (if ($l | length) != length then "a leg has no string name" else empty end),
    ($st[] | select(.args != "static") | "the static leg must have args \"static\", has \(.args | tojson)"),
    ($l[] | select(.name | test("^(static|test-[0-9]+-of-[0-9]+|race-[0-9]+-of-[0-9]+)$") | not) | "unknown leg name \(.name)"),
    ($t[] | . as $e | ($e.name | capture("^test-(?<k>[0-9]+)-of-(?<n>[0-9]+)$")) as $c | select($e.args != "test \($c.k) \($c.n)") | "leg \($e.name) has args \($e.args | tojson), want \"test \($c.k) \($c.n)\""),
    ($r[] | . as $e | ($e.name | capture("^race-(?<k>[0-9]+)-of-(?<n>[0-9]+)$")) as $c | select($e.args != "race \($c.k) \($c.n)") | "leg \($e.name) has args \($e.args | tojson), want \"race \($c.k) \($c.n)\""),
    ($r[] | select(.heavy != true) | "race leg \(.name) must be heavy"),
    (if ($tk | map(.n) | unique | length) > 1 then "test legs name more than one shard count" else empty end),
    (if ($rk | map(.n) | unique | length) > 1 then "race legs name more than one leg count" else empty end),
    (if ($tk | length) == 0 then "no test leg" else empty end),
    (if ($rk | length) == 0 then "no race leg" else empty end),
    (if ($tk | length) > 0 and (($tk | map(.n) | unique | length) == 1) and (($tk | map(.k) | sort) != [range(1; $tk[0].n + 1)]) then "test legs are not K = 1..N complete" else empty end),
    (if ($rk | length) > 0 and (($rk | map(.n) | unique | length) == 1) and (($rk | map(.k) | sort) != [range(1; $rk[0].n + 1)]) then "race legs are not K = 1..M complete" else empty end),
    empty
  ] | .[]
' "${legs}")"
if [ -n "${shape}" ]; then
  while IFS= read -r line; do echo "go_quality_legs.sh: ${line}" >&2; done <<<"${shape}"
  exit 3
fi
printf '%s\n%s\n' "${matrix}" "${policy}"
