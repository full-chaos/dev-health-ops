#!/usr/bin/env bash
# Bigboy pre-prod venue ADMIN pass 2 (gwc-corpus, CHAOS-6688): org-admin proof principal on the Fixture Org.
# Part A = every corpus-admin1..7 org-admin READ row (ids discovered at run time, zero-uuid missing cases, the
# persist-nothing POST install-url), Go plane (dho api) vs Python plane, same credential, with and without an Origin.
# Part B = the Fixture-Org WRITE sequences the corpus does not carry (R402/R406): settings PUT/POST/DELETE with a read-back
# on BOTH planes after each write and a restore to the empty state, llm-settings PUT/DELETE (validation + tier-gated,
# no key material), sync-configs DELETE / PUT repositories on a missing id. Writes run noorigin only, one plane at a time
# (go sequence, then python sequence, each ends restored). Requests are sent from inside the api container
# (network-direct to each service). Credential: bigboy-admin-proof.token (0600), never printed.
# NOT covered here, by design: a valid llm-settings PUT (stores an API key), writes to a real sync config (the Fixture Org
# has none), install-callback (needs an OAuth state), PagerDuty preflight (may call PagerDuty), llm-settings status/budget/spend
# (Go answers 404, a route gap: they are listed as EXPECTED-GAP rows so the pass shows the gap instead of hiding it).
# ABORTS before any write if the probe setting already exists on either plane (state not clean).
# HANDED TO prod-ops to run; the author never runs it. Output: <out>/table.txt (part A + part B in one table).
# CHAOS-6963/D2683: the run and table-render Python used to be inline here-documents. Bash writes a
# here-document into a pipe it also holds the read end of, so a document at or above ~400 bytes on a host
# with a small effective pipe buffer hangs the script forever (CHAOS-3362); tests/tooling's whole-ci/ scan
# caught both of this script's here-documents (7779 and 1513 bytes) the moment it landed under a tracked
# `ci/` path. Fixed by moving each one verbatim (zero logic changes -- diff is the here-document markers
# only) to its own sibling file, read from disk instead of piped: pass-bigboy-admin2-run.py,
# pass-bigboy-admin2-table.py.
set -euo pipefail; umask 077
HERE=$(cd "$(dirname "$0")" && pwd); OUT=${1:-$HERE/pass-admin2}
rm -rf "$OUT"; mkdir -p "$OUT"
export ADMIN="$(cat /home/ubuntu/devhealth/.go-api-dev/bigboy-admin-proof.token)"
docker exec -i -e ADMIN dev-health-api-1 python3 - < "$HERE/pass-bigboy-admin2-run.py"
docker exec dev-health-api-1 tar cf - -C /tmp bbpass | tar xf - -C "$OUT" --strip-components=1
docker exec dev-health-api-1 rm -rf /tmp/bbpass
python3 "$HERE/pass-bigboy-admin2-table.py" "$OUT" | tee "$OUT/table.txt"
