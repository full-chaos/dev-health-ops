#!/usr/bin/env bash
# check-dho-api-ch-user.sh [users-file] -- CHAOS-7162: dho_api_ch must be declared durably and be live.
# Names only: it prints file modes, paths and user names, never the password hash or any credential.
#
# 1. FILE (before anything recreates ClickHouse): the host file that compose.bigboy.clickhouse-users.yml
#    mounts into users.d must be a regular file, world-readable (clickhouse-server runs as uid 101 in the
#    container and EXITS on a mounted users.d file it cannot read: a 0600 file owned by the host user takes
#    ClickHouse down), and shaped like a users.d declaration of dho_api_ch (a 64-hex sha256 password hash).
# 2. LIVE: `dho_api_ch` is in system.users of the running ClickHouse container, and it AUTHENTICATES with API_CH_PASSWORD. Without it go-api's every
#    ClickHouse call fails with code 516 (rev 198, after the CHAOS-7079 recreate lost the user).
#
# Exit: 0 ok; 1 the file is unusable; 2 the user is not live; 3 dho_api_ch cannot log in with API_CH_PASSWORD. CH_CONTAINER overrides the container name.
set -u
FILE=${1:-${DHO_API_CH_USERS_XML:-}}
CH=${CH_CONTAINER:-dev-health-clickhouse-1}
[ -n "$FILE" ] || { echo "CH_API_USER_FILE_FAIL: no users file given (arg 1 or DHO_API_CH_USERS_XML)" >&2; exit 1; }
[ -f "$FILE" ] && [ ! -L "$FILE" ] || { echo "CH_API_USER_FILE_FAIL: $FILE is not a regular file (a missing bind-mount source becomes a directory and ClickHouse fails to start)" >&2; exit 1; }
mode=$(stat -c %a "$FILE")
case "$mode" in
  *[4-7]) ;;
  *) echo "CH_API_USER_FILE_FAIL: $FILE is mode $mode, not world-readable: clickhouse-server (uid 101) cannot read a mounted 0600 file and exits; the file holds only a password hash, chmod 644" >&2; exit 1 ;;
esac
# Parse it, do not grep it: ClickHouse exits on a users.d file it cannot parse, and a world-readable file may hold
# only the hash and grants. The shape (one user, one 64-hex hash, no other authentication element, GRANT queries only,
# no URI) is dho_api_ch_users_shape.py's, the same one the renderer refuses on.
HERE=$(cd "$(dirname "$0")" && pwd)
reason=$(python3 "$HERE/dho_api_ch_users_shape.py" "$FILE" 2>&1 >/dev/null) || { echo "CH_API_USER_FILE_FAIL: $FILE is not a valid dho_api_ch users.d declaration: $reason" >&2; exit 1; }
echo "ch_api_user_file=ok path=$FILE mode=$mode"
count=$(docker exec "$CH" clickhouse-client -q "SELECT count() FROM system.users WHERE name='dho_api_ch'" 2>/dev/null) || count=""
[ "$count" = "1" ] || { echo "CH_API_USER_LIVE_FAIL: dho_api_ch is not in system.users of $CH (count=${count:-unreadable}); go-api would fail every ClickHouse call with code 516" >&2; exit 2; }
echo "ch_api_user_live=ok container=$CH"
# Present is not usable: prove the API's own credential authenticates. The password comes from API_CH_PASSWORD in this
# process's environment and reaches the container as CLICKHOUSE_PASSWORD through `docker exec -e NAME` (no value), so it is
# never in an argument list, and nothing here prints it.
[ -n "${API_CH_PASSWORD:-}" ] || { echo "CH_API_USER_AUTH_FAIL: API_CH_PASSWORD is not set, so dho_api_ch's login cannot be proven" >&2; exit 3; }
who=$(CLICKHOUSE_PASSWORD="$API_CH_PASSWORD" docker exec -e CLICKHOUSE_PASSWORD "$CH" clickhouse-client --user dho_api_ch -q "SELECT currentUser()" 2>/dev/null) || who=""
[ "$who" = "dho_api_ch" ] || { echo "CH_API_USER_AUTH_FAIL: logging in to $CH as dho_api_ch with API_CH_PASSWORD failed; the declared hash does not match the credential go-api uses (ClickHouse code 516)" >&2; exit 3; }
echo "ch_api_user_auth=ok container=$CH"
