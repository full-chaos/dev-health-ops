#!/usr/bin/env bash
# Bigboy pre-prod venue AUTH pass (gwc-web-ingress, CHAOS-6963, child of CHAOS-6259): the 22 D2679(b)
# session/registration/orgs-me-write/telemetry-write rows that cannot go in internal/goapiproof/restcorpus.go
# (non-idempotent, session-stateful -- login mutates login_attempts, refresh rotates a token family, register
# creates a user, etc). Same shape as pass-bigboy-admin2.sh (CHAOS-6688): a disposable identity on the Fixture
# Org, writes on ONE plane at a time with a same-plane and cross-plane read-back, restore to the pre-run state,
# ABORTS up front if a throwaway email this run would create already exists. Full design + coverage in
# pass-bigboy-auth.py's own module docstring (this wrapper only runs it) -- the logic runs on the HOST, not
# inside a container: dev-health-api-1 has no docker CLI/socket, so it cannot itself shell out to
# dev-health-postgres-1 for the SQL this pass needs (creating/reading throwaway users and tokens). HTTP is
# still network-direct to each plane's service from INSIDE the api container (one `docker exec ... python3 -c`
# per request, driven from here) -- same "bypasses ingress, proves the candidate origin" property leg 2 of
# every STEP's rest-commands.sh has. Credential for the one admin call this needs (creating an invite):
# bigboy-admin-proof.token (0600), never printed.
#
# HANDED TO prod-ops to run at the rev 193 pre-cut; the author does not run it against bigboy (same convention
# as pass-bigboy-admin2.sh). Output: <out>/table.txt (one row per request).
# Usage: pass-bigboy-auth.sh [out_dir]
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${1:-$HERE/pass-auth}
exec python3 "$HERE/pass-bigboy-auth.py" "$OUT"
