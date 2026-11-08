#!/usr/bin/env bash
# bigboy venue: the platform_admin fixture (CHAOS-13282). Gives every flagged superuser except drift-S an active platform_admin assignment by the
# prod path (`dho auth users-backfill`), BEFORE auth-service runs with AUTH_PLATFORM_ROLES_OWNED=true (the compose default). drift-S is the planted
# control (flag, no assignment): its is_superuser flag is off around the backfill and back on after (go-api users route, the legacy writer of the flag).
# Idempotent: a second run finds the assignments and grants nothing. Read-backs: the active platform_admin set = the flagged superusers other than
# drift-S; `users-reconcile -platform-roles-gate` shows only the drift-S platform_role_diff.
# Inputs (names only; no value is printed):
#   BIGBOY_DRIFT_S_ID         uuid of the drift-S user
#   BIGBOY_ADMIN_AUTH_HDR     file with one curl header line `Authorization: Bearer ...` of a superuser (0600, made by the operator)
#   BIGBOY_API_BASE           go-api base URL (default http://127.0.0.1:8093)
# The dho one-offs are the compose service auth-users-backfill (the auth-service runtime login comes from compose interpolation: no env file, no credential here).
# STOP codes: 11 precondition, 12 drift-S flag write, 13 backfill, 15 assignment read-back, 16 reconcile.
set -euo pipefail
R=$(cd "${BIGBOY_ROOT:-/home/ubuntu/devhealth}" && pwd)
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$R"
for v in $(compgen -v | grep '^COMPOSE_' || true); do unset "$v"; done
DC=(docker compose -p dev-health --project-directory "$R" --env-file ops/.env --env-file _records/images.env -f "$HERE/compose.bigboy.yml")
stop() { local code=$1; shift; echo "STOP exit=$code $*"; exit "$code"; }
DS=${BIGBOY_DRIFT_S_ID:-}; HDR=${BIGBOY_ADMIN_AUTH_HDR:-}; BASE=${BIGBOY_API_BASE:-http://127.0.0.1:8093}
[[ $DS =~ ^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$ ]] || stop 11 "BIGBOY_DRIFT_S_ID is not a uuid"
[[ -f $HDR && $(stat -c %a "$HDR") == 600 ]] || stop 11 "BIGBOY_ADMIN_AUTH_HDR is not a 0600 file"
grep -q '^BIGBOY_DHO_IMAGE=' _records/images.env || stop 11 "no BIGBOY_DHO_IMAGE in _records/images.env"
FLAG_OFF=0
psql_ro() { "${DC[@]}" exec -T postgres psql -U devhealth -d devhealth -qAt -v ON_ERROR_STOP=1 -c "SET default_transaction_read_only = on" -c "$1"; }
set_flag() {  # set_flag true|false
  local code
  code=$(curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PATCH -H "@$HDR" -H 'Content-Type: application/json' --data-binary "{\"is_superuser\": $1}" "$BASE/api/v1/admin/users/$DS" || echo 000)
  [[ $code == 200 ]]
}
# the drift-S flag never stays off, whatever the exit
cleanup() { if [[ $FLAG_OFF == 1 ]]; then set_flag true || echo "WARN drift-S flag NOT restored: set is_superuser true by hand"; fi; }
trap cleanup EXIT
dho_run() {  # dho_run <args...>
  "${DC[@]}" --profile auth run --rm --no-deps -T auth-users-backfill "$@"
}

FLAG_OFF=1
set_flag false || stop 12 "PATCH drift-S is_superuser false failed"
[[ $(psql_ro "SELECT is_superuser FROM public.users WHERE id = '$DS'") == f ]] || stop 12 "drift-S flag is not f"

dho_run auth users-backfill --batch=500 --dry-run || stop 13 "users-backfill --dry-run failed"
out=$(dho_run auth users-backfill --batch=500) || stop 13 "users-backfill failed"
echo "$out"
grep -qx 'platform_role_revoked=0' <<<"$out" || stop 13 "backfill revoked a platform role"

set_flag true || stop 12 "PATCH drift-S is_superuser true failed"
FLAG_OFF=0
[[ $(psql_ro "SELECT is_superuser FROM public.users WHERE id = '$DS'") == t ]] || stop 12 "drift-S flag is not t"

ACTIVE=$(psql_ro "SELECT coalesce(string_agg(a.principal_id::text, ',' ORDER BY a.principal_id), '') FROM auth.platform_role_assignments a JOIN public.users u ON u.id = a.principal_id WHERE a.role_key = 'platform_admin' AND u.is_active AND (a.expires_at IS NULL OR a.expires_at > now())")
EXPECT=$(psql_ro "SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM public.users WHERE is_superuser AND is_active AND id <> '$DS'")
[[ -n $EXPECT && $ACTIVE == "$EXPECT" ]] || stop 15 "active platform_admin set is not the flagged superusers other than drift-S"
echo "read-back: active platform_admin == flagged superusers except drift-S ($(tr ',' '\n' <<<"$ACTIVE" | wc -l) principals)"

set +e; rec=$(dho_run auth users-reconcile -platform-roles-gate 2>&1); rc=$?; set -e
echo "$rec"; echo "users-reconcile exit $rc"
if ! { grep -qx 'gated_diffs=1' <<<"$rec" && grep -qx 'diff.platform_role_diff=1' <<<"$rec" && grep -qx 'diff.no_active_platform_admin=0' <<<"$rec"; }; then
  stop 16 "users-reconcile is not exactly the drift-S platform_role_diff"
fi
echo "platform_admin_fixture_ok"
