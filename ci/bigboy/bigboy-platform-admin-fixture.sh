#!/usr/bin/env bash
# bigboy venue: the platform_admin fixture and its drift-S control (CHAOS-13282; moved to the auth platform roles by CHAOS-13356).
# auth-service owns users and platform roles here (AUTH_USERS_OWNED and AUTH_PLATFORM_ROLES_OWNED are true in compose.bigboy.yml),
# so the authority for "who is a platform admin" is auth.platform_role_assignments, read with the platform role store's active rule
# (internal/auth/platformrole/rule ActiveRule: a user principal, not disabled, the assignment not expired). The legacy users table is never read.
#
#   bigboy-platform-admin-fixture.sh          seed, then prove the control: green clean, red on a planted drift, green after the clear
#   bigboy-platform-admin-fixture.sh check    the control alone, read-only: exit 0 green, exit 20 red
#
# The seed tool is `dho auth users-backfill -insert-only` (the compose one-off auth-users-backfill, users owned): it creates the auth
# users that are missing and never changes an existing one. drift-S is a user principal that holds no platform role. The control is
# green when drift-S holds no active platform role and at least one other principal holds an active platform_admin; red otherwise.
# The plant and the clear write auth.platform_role_assignments DIRECTLY (venue fixture only; no go-api route, no audit or outbox row):
# the plant inserts an active platform_admin row for drift-S, the clear deletes every row of drift-S, and a read-back count proves
# the clean value (0 rows) again. The trap clears a plant on every exit. Idempotent: a clean venue gives the same run every time.
# Inputs (names only; no value is printed):
#   BIGBOY_DRIFT_S_ID         uuid of the drift-S user principal
# The dho one-off is the compose service auth-users-backfill (the auth-service runtime login comes from compose interpolation: no env
# file, no credential here); psql runs in the postgres container as the venue's owner login.
# STOP codes: 11 precondition, 13 seed, 14 control blind to the planted drift, 15 control red on a clean state, 16 plant,
# 17 clear read-back, 20 control red (check).
set -euo pipefail
R=$(cd "${BIGBOY_ROOT:-/home/ubuntu/devhealth}" && pwd)
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$R"
for v in $(compgen -v | grep '^COMPOSE_' || true); do unset "$v"; done
DC=(docker compose -p dev-health --project-directory "$R" --env-file ops/.env --env-file _records/images.env -f "$HERE/compose.bigboy.yml")
stop() { local code=$1; shift; echo "STOP exit=$code $*"; exit "$code"; }
MODE=${1:-fixture}
[[ $MODE == fixture || $MODE == check ]] || stop 11 "usage: $0 [check]"
DS=${BIGBOY_DRIFT_S_ID:-}
[[ $DS =~ ^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$ ]] || stop 11 "BIGBOY_DRIFT_S_ID is not a uuid"
grep -q '^BIGBOY_DHO_IMAGE=' _records/images.env || stop 11 "no BIGBOY_DHO_IMAGE in _records/images.env"
psql_ro() { "${DC[@]}" exec -T postgres psql -U devhealth -d devhealth -qAt -v ON_ERROR_STOP=1 -c "SET default_transaction_read_only = on" -c "$1"; }
# One transaction that sets the audit commit guard's txid itself (authschema 0006): the guard refuses a write to the table otherwise.
psql_rw() { "${DC[@]}" exec -T postgres psql -U devhealth -d devhealth -qAt -v ON_ERROR_STOP=1 -c "BEGIN; SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true); $1; COMMIT;" >/dev/null; }
ACTIVE="p.kind = 'user' AND p.disabled_at IS NULL AND (r.expires_at IS NULL OR r.expires_at > statement_timestamp())"
ds_rows() { psql_ro "/* drift_s_rows */ SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = '$DS'"; }
# control: prints one line; returns 0 green, 1 red.
control() {
  local ds others
  ds=$(psql_ro "/* drift_s_active */ SELECT count(*) FROM auth.platform_role_assignments r JOIN auth.principals p ON p.id = r.principal_id WHERE r.principal_id = '$DS' AND $ACTIVE")
  others=$(psql_ro "/* other_active */ SELECT count(*) FROM auth.platform_role_assignments r JOIN auth.principals p ON p.id = r.principal_id WHERE r.role_key = 'platform_admin' AND r.principal_id <> '$DS' AND $ACTIVE")
  if [[ $ds == 0 && $others =~ ^[0-9]+$ && $others -ge 1 ]]; then
    echo "control: green (drift-S active platform roles $ds, other active platform_admin $others)"
    return 0
  fi
  echo "control: red (drift-S active platform roles $ds, other active platform_admin $others)"
  return 1
}

if [[ $MODE == check ]]; then
  control || stop 20 "drift-S control red: drift-S holds an active platform role, or no other principal holds platform_admin"
  exit 0
fi

[[ $(psql_ro "/* drift_s_user */ SELECT count(*) FROM auth.principals WHERE id = '$DS' AND kind = 'user'") == 1 ]] || stop 11 "drift-S is not a user principal of the auth schema"
[[ $(ds_rows) == 0 ]] || stop 11 "drift-S holds a platform role assignment row: a drift is left over; clear it (see README) and run again"

PLANTED=0
clear_drift() { psql_rw "DELETE FROM auth.platform_role_assignments WHERE principal_id = '$DS'"; }
cleanup() { if [[ $PLANTED == 1 ]]; then { clear_drift && [[ $(ds_rows) == 0 ]]; } || echo "WARN drift-S plant NOT cleared: delete the drift-S rows of auth.platform_role_assignments by hand"; fi; }
trap cleanup EXIT
dho_run() {  # dho_run <args...>
  "${DC[@]}" --profile auth run --rm --no-deps -T auth-users-backfill "$@"
}

dho_run auth users-backfill -insert-only --batch=500 --dry-run || stop 13 "users-backfill -insert-only --dry-run failed"
out=$(dho_run auth users-backfill -insert-only --batch=500) || stop 13 "users-backfill -insert-only failed"
echo "$out"
grep -qx 'platform_role_revoked=0' <<<"$out" || stop 13 "the seed revoked a platform role"

control || stop 15 "drift-S control is red before the plant"

PLANTED=1
psql_rw "INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ('$DS', 'platform_admin')" || stop 16 "the drift-S plant failed"
[[ $(ds_rows) == 1 ]] || stop 16 "the drift-S plant does not read back (want 1 row)"
if control; then stop 14 "drift-S control is green on the planted drift: the control is blind"; fi

clear_drift || stop 17 "the drift-S clear failed"
n=$(ds_rows)
[[ $n == 0 ]] || stop 17 "the drift-S clear does not read back: $n rows, want 0"
PLANTED=0
echo "clear read-back: drift-S rows 0"
control || stop 15 "drift-S control is red after the clear"
echo "platform_admin_fixture_ok"
