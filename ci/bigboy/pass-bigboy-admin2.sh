#!/usr/bin/env bash
# Bigboy pre-prod venue ADMIN pass 2 (gwc-corpus, CHAOS-6688): org-admin proof principal on the Fixture Org.
# Part A = every corpus-admin1..7 org-admin READ row (ids discovered at run time, zero-uuid missing cases, the
# persist-nothing POST install-url), Go plane (dho api) vs Python plane, same credential, with and without an Origin.
# Part B = the Fixture-Org WRITE sequences the corpus does not carry (R402/R406): settings PUT/POST/DELETE with a read-back
# on BOTH planes after each write and a restore to the empty state, llm-settings PUT/DELETE (validation + tier-gated,
# no key material), sync-configs DELETE / PUT repositories on a missing id. Writes run noorigin only, one plane at a time
# (go sequence, then python sequence, each ends restored). Requests are sent from inside the api container
# (network-direct to each service). Credential: bigboy-admin-proof.token (0600), never printed.
# CHAOS-8369 (D4566 class rules): the token reaches the in-container python on STDIN only (file redirect: no shell
# variable, no export, no `-e`, not in argv); results leave the container as ONE framed stdout stream that
# pass-bigboy-admin2-unpack.py writes under <out> on the host (nothing under /tmp in the container, no tar-out, no
# rm -rf); compose verbs only (`docker compose exec -T api`). The api container is the PYTHON plane: this whole script is
# EXPECTED-GONE at E7 (CHAOS-8364); its Go-only successor is a follow-up under 8364.
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
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${1:-$HERE/pass-admin2}; case $OUT in /*) ;; *) OUT=$PWD/$OUT ;; esac
TOKEN_FILE=/home/ubuntu/devhealth/.go-api-dev/bigboy-admin-proof.token
# no rm -rf of a path taken from argv: an existing non-empty out dir is refused, never wiped
mkdir -p "$OUT"; shopt -s nullglob dotglob; set -- "$OUT"/*; [ "$#" -eq 0 ] || { echo "REFUSED: $OUT is not empty (pass a fresh directory)" >&2; exit 3; }; shopt -u nullglob dotglob; set --
cd /home/ubuntu/devhealth   # compose project root, as bigboy-cut.sh
docker compose --env-file ops/.env exec -T api python3 -c "$(cat "$HERE/pass-bigboy-admin2-run.py")" < "$TOKEN_FILE" \
  | python3 "$HERE/pass-bigboy-admin2-unpack.py" "$OUT"
python3 "$HERE/pass-bigboy-admin2-table.py" "$OUT" | tee "$OUT/table.txt"
