#!/usr/bin/env bash
# check-dho-api-ch-user.sh <users-file> <creds-file> -- CHAOS-7162 / CHAOS-8382: dho_api_ch must be declared durably and be live.
# Names only: it prints file modes, paths and user names, never the password hash or any credential.
#
# CREDENTIAL RULE (CHAOS-8382, no credential in argv, in a host child env, in a docker -e, in an env file or a host variable):
# the API password is read from <creds-file> (a 0600 file holding a line API_CH_PASSWORD=...; it is never sourced) and only
# ever travels through pipes: into sha256sum for the canonical hash, and into the stdin of `clickhouse-client --config-file
# /dev/stdin` for the login proof. No child of this script has it in its environment or argument list, and the docker CLI
# gets no -e. Scratch files live in a 0700 dir under the caller's TMPDIR (required; /tmp itself is refused).
#
# 1. FILE (before anything recreates ClickHouse): the host file that compose.bigboy.clickhouse-users.yml mounts into
#    users.d must be a regular file, world-readable (clickhouse-server runs as uid 101 in the container and EXITS on a
#    mounted users.d file it cannot read: a 0600 file owned by the host user takes ClickHouse down), and EQUAL, byte for
#    byte, to the canonical render. The renderer (ci/bigboy/render-dho-api-ch-users.py, unchanged) runs with a PLACEHOLDER
#    password (not a secret); the placeholder's hash is then swapped for the real hash, computed from the creds file by pipe.
# 2. LIVE: `dho_api_ch` is in system.users of the running ClickHouse container, and it AUTHENTICATES with the password.
#    Without it go-api's every ClickHouse call fails with code 516 (rev 198, after the CHAOS-7079 recreate lost the user).
#
# Exit: 0 ok; 1 the file is unusable (missing, not a regular file, not world-readable, or the canonical render cannot be
# computed); 2 the user is not live; 3 the creds file/API_CH_PASSWORD is missing or dho_api_ch cannot log in with it; 4 the file is not the
# canonical render (checked before any live check, so a stale hand-made live user cannot mask a wrong file).
# ClickHouse is reached with `docker compose exec` on the service `clickhouse` (COMPOSE_FILE set as in bigboy-cut.sh, run from
# BIGBOY_ROOT; BIGBOY_ENV_FILE overrides ops/.env), never by container name. DHO_API_CH_POSTURE_GO overrides the manifest
# (default: this checkout's authorization.go).
set -u
FILE=${1:-${DHO_API_CH_USERS_XML:-}}
CREDS=${2:-${DHO_API_CH_CREDS:-}}
CH=clickhouse
ENVF=${BIGBOY_ENV_FILE:-ops/.env}
[ -n "$FILE" ] || { echo "CH_API_USER_FILE_FAIL: no users file given (arg 1 or DHO_API_CH_USERS_XML)" >&2; exit 1; }
[ -f "$FILE" ] && [ ! -L "$FILE" ] || { echo "CH_API_USER_FILE_FAIL: $FILE is not a regular file (a missing bind-mount source becomes a directory and ClickHouse fails to start)" >&2; exit 1; }
mode=$(stat -c %a "$FILE")
case "$mode" in
  *[4-7]) ;;
  *) echo "CH_API_USER_FILE_FAIL: $FILE is mode $mode, not world-readable: clickhouse-server (uid 101) cannot read a mounted 0600 file and exits; the file holds only a password hash, chmod 644" >&2; exit 1 ;;
esac
# Scratch rule: TMPDIR must resolve (symlinks and `..` followed) to a directory that is NOT /tmp, /var/tmp or under them, is
# owned by the caller and is mode 0700. The scratch dir below is made only with `mktemp -p` on that resolved path.
tmp_root=""
[ -n "${TMPDIR:-}" ] && tmp_root=$(cd -P -- "$TMPDIR" 2>/dev/null && pwd -P)
[ -z "$tmp_root" ] || tmp_root=/${tmp_root#"${tmp_root%%[!/]*}"}   # pwd -P keeps a leading "//" ("//tmp/x"): collapse it to one slash
case "$tmp_root" in
  ""|/|/tmp|/tmp/*|/var/tmp|/var/tmp/*) echo "CH_API_USER_FILE_FAIL: TMPDIR must name an existing private scratch directory that resolves outside /tmp and /var/tmp (got: ${tmp_root:-unset or not a directory}): the scratch files of this check must stay off /tmp" >&2; exit 1 ;;
esac
if [ "$(stat -c '%a %u' "$tmp_root")" != "700 $(id -u)" ]; then
  echo "CH_API_USER_FILE_FAIL: TMPDIR $tmp_root must be mode 0700 and owned by the caller" >&2; exit 1
fi
HERE=$(cd "$(dirname "$0")" && pwd)
POSTURE=${DHO_API_CH_POSTURE_GO:-$HERE/../../internal/storage/clickhouse/authorization.go}
[ -f "$POSTURE" ] || { echo "CH_API_USER_FILE_FAIL: the posture manifest $POSTURE is not a file, so the canonical render cannot be computed (DHO_API_CH_POSTURE_GO)" >&2; exit 1; }
[ -f "${CREDS:-/nonexistent}" ] || { echo "CH_API_USER_AUTH_FAIL: no readable credentials file (arg 2 or DHO_API_CH_CREDS), so dho_api_ch's login cannot be proven" >&2; exit 3; }
# The password, as a pipe source only: the value of the last API_CH_PASSWORD= line, one pair of surrounding quotes removed,
# no trailing newline. The file is read, never sourced or evaluated (a value with a quote or escape inside it is not
# interpreted; such a value fails the hash or login comparison loudly, it is never silently accepted).
api_password() {
  sed -n 's/^[[:space:]]*\(export[[:space:]]\{1,\}\)\{0,1\}API_CH_PASSWORD=//p' "$CREDS" | tail -n 1 | sed -e "s/^'\(.*\)'\$/\1/" -e 's/^"\(.*\)"$/\1/' | tr -d '\r\n'
}
[ "$(api_password | wc -c)" -gt 0 ] || { echo "CH_API_USER_AUTH_FAIL: the credentials file has no API_CH_PASSWORD value, so dho_api_ch's login cannot be proven" >&2; exit 3; }
canon_dir=$(mktemp -d -p "$tmp_root") || { echo "CH_API_USER_FILE_FAIL: cannot make a scratch directory under $tmp_root for the canonical render" >&2; exit 1; }
chmod 700 "$canon_dir"
trap 'rm -rf "$canon_dir"' EXIT
# The file is valid IFF it equals the canonical render, byte for byte. It is not parsed, grepped or pattern-checked:
# any comment, attribute, edit, wrong hash or malformed grant is a difference, and a difference is refused. Nothing of the
# two files is printed: a mismatch is reported by name only.
PLACEHOLDER=placeholder-not-a-credential
placeholder_hash=$(printf %s "$PLACEHOLDER" | sha256sum | cut -d' ' -f1)
API_CH_PASSWORD="$PLACEHOLDER" python3 "$HERE/render-dho-api-ch-users.py" "$POSTURE" "$canon_dir/placeholder.xml" >/dev/null 2>"$canon_dir/render.err" || { echo "CH_API_USER_FILE_FAIL: the canonical render could not be computed: $(head -c 300 "$canon_dir/render.err")" >&2; exit 1; }
api_password | sha256sum | cut -d' ' -f1 > "$canon_dir/real.hash"
awk -v ph="$placeholder_hash" 'NR==FNR { h=$1; next } { gsub(ph, h); print }' "$canon_dir/real.hash" "$canon_dir/placeholder.xml" > "$canon_dir/canonical.xml"
cmp -s "$FILE" "$canon_dir/canonical.xml" || { echo "CH_API_USER_FILE_MISMATCH: $FILE is not the canonical render of the posture manifest and the API password (a comment, an edit, a malformed grant or a hash for another password all differ); regenerate it with ci/bigboy/render-dho-api-ch-users.py. ClickHouse would load whatever is mounted, and exits on a file it cannot parse" >&2; exit 4; }
echo "ch_api_user_file=ok path=$FILE mode=$mode canonical=yes"
count=$(docker compose --env-file "$ENVF" exec -T "$CH" clickhouse-client -q "SELECT count() FROM system.users WHERE name='dho_api_ch'" 2>/dev/null) || count=""
[ "$count" = "1" ] || { echo "CH_API_USER_LIVE_FAIL: dho_api_ch is not in system.users of service $CH (count=${count:-unreadable}); go-api would fail every ClickHouse call with code 516" >&2; exit 2; }
echo "ch_api_user_live=ok service=$CH"
# Present is not usable: prove the API's own credential authenticates. The password goes by pipe into the stdin of ONE
# `docker compose exec -T` as a clickhouse-client config file (--config-file /dev/stdin); the query is the only argument.
# Not in any argument list, not in any environment, not in a -e, not on a disk.
who=$({ printf '<clickhouse><user>dho_api_ch</user><password>'; api_password | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; printf '</password></clickhouse>'; } | docker compose --env-file "$ENVF" exec -T "$CH" clickhouse-client --config-file /dev/stdin -q "SELECT currentUser()" 2>/dev/null) || who=""
[ "$who" = "dho_api_ch" ] || { echo "CH_API_USER_AUTH_FAIL: logging in to service $CH as dho_api_ch with the API password failed; the declared hash does not match the credential go-api uses (ClickHouse code 516)" >&2; exit 3; }
echo "ch_api_user_auth=ok service=$CH"
