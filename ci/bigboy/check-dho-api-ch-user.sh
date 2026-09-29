#!/usr/bin/env bash
# check-dho-api-ch-user.sh [users-file] -- CHAOS-7162: dho_api_ch must be declared durably and be live.
# Names only: it prints file modes, paths and user names, never the password hash or any credential.
#
# 1. FILE (before anything recreates ClickHouse): the host file that compose.bigboy.clickhouse-users.yml mounts into
#    users.d must be a regular file, world-readable (clickhouse-server runs as uid 101 in the container and EXITS on a
#    mounted users.d file it cannot read: a 0600 file owned by the host user takes ClickHouse down), and EQUAL, byte for
#    byte, to the canonical render (ci/bigboy/render-dho-api-ch-users.py from the posture manifest + API_CH_PASSWORD).
# 2. LIVE: `dho_api_ch` is in system.users of the running ClickHouse container, and it AUTHENTICATES with API_CH_PASSWORD.
#    Without it go-api's every ClickHouse call fails with code 516 (rev 198, after the CHAOS-7079 recreate lost the user).
#
# Exit: 0 ok; 1 the file is unusable (missing, not a regular file, not world-readable, or the canonical render cannot be
# computed); 2 the user is not live; 3 API_CH_PASSWORD is unset or dho_api_ch cannot log in with it; 4 the file is not the
# canonical render (checked before any live check, so a stale hand-made live user cannot mask a wrong file).
# ClickHouse is reached with `docker compose exec` on the service `clickhouse` (COMPOSE_FILE set as in bigboy-cut.sh, run from
# BIGBOY_ROOT; BIGBOY_ENV_FILE overrides ops/.env), never by container name. DHO_API_CH_POSTURE_GO overrides the manifest
# (default: this checkout's authorization.go).
set -u
FILE=${1:-${DHO_API_CH_USERS_XML:-}}
CH=clickhouse
ENVF=${BIGBOY_ENV_FILE:-ops/.env}
[ -n "$FILE" ] || { echo "CH_API_USER_FILE_FAIL: no users file given (arg 1 or DHO_API_CH_USERS_XML)" >&2; exit 1; }
[ -f "$FILE" ] && [ ! -L "$FILE" ] || { echo "CH_API_USER_FILE_FAIL: $FILE is not a regular file (a missing bind-mount source becomes a directory and ClickHouse fails to start)" >&2; exit 1; }
mode=$(stat -c %a "$FILE")
case "$mode" in
  *[4-7]) ;;
  *) echo "CH_API_USER_FILE_FAIL: $FILE is mode $mode, not world-readable: clickhouse-server (uid 101) cannot read a mounted 0600 file and exits; the file holds only a password hash, chmod 644" >&2; exit 1 ;;
esac
# The file is valid IFF it equals the canonical render, byte for byte. It is not parsed, grepped or pattern-checked:
# any comment, attribute, edit, wrong hash or malformed grant is a difference, and a difference is refused. The canonical
# render is computed here, in-process, by the same renderer that writes the file: from the posture manifest (the ops
# checkout this tool runs from, or DHO_API_CH_POSTURE_GO) and API_CH_PASSWORD from this process's environment (never an
# argument). Nothing of the two files is printed: a mismatch is reported by name only.
HERE=$(cd "$(dirname "$0")" && pwd)
POSTURE=${DHO_API_CH_POSTURE_GO:-$HERE/../../internal/storage/clickhouse/authorization.go}
[ -f "$POSTURE" ] || { echo "CH_API_USER_FILE_FAIL: the posture manifest $POSTURE is not a file, so the canonical render cannot be computed (DHO_API_CH_POSTURE_GO)" >&2; exit 1; }
[ -n "${API_CH_PASSWORD:-}" ] || { echo "CH_API_USER_AUTH_FAIL: API_CH_PASSWORD is not set, so dho_api_ch's login cannot be proven" >&2; exit 3; }
canon_dir=$(mktemp -d) || { echo "CH_API_USER_FILE_FAIL: cannot make a scratch directory for the canonical render" >&2; exit 1; }
trap 'rm -rf "$canon_dir"' EXIT
API_CH_PASSWORD="$API_CH_PASSWORD" python3 "$HERE/render-dho-api-ch-users.py" "$POSTURE" "$canon_dir/canonical.xml" >/dev/null 2>"$canon_dir/render.err" || { echo "CH_API_USER_FILE_FAIL: the canonical render could not be computed: $(head -c 300 "$canon_dir/render.err")" >&2; exit 1; }
cmp -s "$FILE" "$canon_dir/canonical.xml" || { echo "CH_API_USER_FILE_MISMATCH: $FILE is not the canonical render of the posture manifest and API_CH_PASSWORD (a comment, an edit, a malformed grant or a hash for another password all differ); regenerate it with ci/bigboy/render-dho-api-ch-users.py. ClickHouse would load whatever is mounted, and exits on a file it cannot parse" >&2; exit 4; }
echo "ch_api_user_file=ok path=$FILE mode=$mode canonical=yes"
count=$(docker compose --env-file "$ENVF" exec -T "$CH" clickhouse-client -q "SELECT count() FROM system.users WHERE name='dho_api_ch'" 2>/dev/null) || count=""
[ "$count" = "1" ] || { echo "CH_API_USER_LIVE_FAIL: dho_api_ch is not in system.users of service $CH (count=${count:-unreadable}); go-api would fail every ClickHouse call with code 516" >&2; exit 2; }
echo "ch_api_user_live=ok service=$CH"
# Present is not usable: prove the API's own credential authenticates. The password comes from API_CH_PASSWORD in this
# process's environment and reaches the container as CLICKHOUSE_PASSWORD through `docker compose exec -e NAME` (no value), so it is
# never in an argument list, and nothing here prints it.
who=$(CLICKHOUSE_PASSWORD="$API_CH_PASSWORD" docker compose --env-file "$ENVF" exec -T -e CLICKHOUSE_PASSWORD "$CH" clickhouse-client --user dho_api_ch -q "SELECT currentUser()" 2>/dev/null) || who=""
[ "$who" = "dho_api_ch" ] || { echo "CH_API_USER_AUTH_FAIL: logging in to service $CH as dho_api_ch with API_CH_PASSWORD failed; the declared hash does not match the credential go-api uses (ClickHouse code 516)" >&2; exit 3; }
echo "ch_api_user_auth=ok service=$CH"
