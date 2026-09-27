#!/usr/bin/env bash
# web-path-smoke.sh -- CHAOS-6987/R460: the ONLY sanctioned proof that a bigboy cut
# serves the real org end to end through a real browser session, not a hand-minted
# token. Logs in through web's real /api/v1/auth/login route as the bigboy admin,
# then runs web-path-smoke.py's exact Cockpit/Diagnose operations against the router.
#
# Credentials: DHO_SMOKE_ADMIN_EMAIL + DHO_SMOKE_ADMIN_PASSWORD_FILE (0600, *_FILE
# convention, CHAOS-6972) in ops/.env. FAILS LOUD (rc=1, named reason) while either
# is absent or blank -- this STEP does not run against the real org until chris adds
# both. Presence/blank check goes through compose-config-redacted.sh exclusively
# (NAME=<length> only); the password file's own PATH (not its content) is read from
# the resolved config to build the bind mount below -- a filesystem path is not a
# credential value.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
R=/home/ubuntu/devhealth
cd "$R"

BASE_COMPOSE_ARGS=(--env-file ops/.env -f compose.yml -f compose/compose.go.workers.yml \
  -f compose/compose.metrics-api.local.yml \
  -f .remember/lanes/team-lead/reconciler-sweep-override.yml \
  -f compose/compose.bigboy.images.yml -f compose/compose.bigboy.workers.yml \
  -f "$HERE/compose.bigboy.router.yml" -f "$HERE/compose.bigboy.smoke.yml")

st() { echo "STEP web-path-smoke rc=$1 $(date -u +%T)"; }

# The smoke compose file's web-smoke service requires both vars (:? in
# compose.bigboy.smoke.yml) -- `config` fails outright while either is unset,
# which IS the fail-loud signal this gate needs; a set-but-blank value is caught
# by the length check below.
if ! "$HERE/compose-config-redacted.sh" "${BASE_COMPOSE_ARGS[@]}" > /tmp/.web-smoke-redacted.$$ 2>/tmp/.web-smoke-redacted.err.$$; then
  st 1
  echo "FAIL: credential file missing -- set DHO_SMOKE_ADMIN_EMAIL and DHO_SMOKE_ADMIN_PASSWORD_FILE (0600) in ops/.env (CHAOS-6972 *_FILE convention). This STEP does not run against the real org until chris adds both." >&2
  rm -f /tmp/.web-smoke-redacted.$$ /tmp/.web-smoke-redacted.err.$$
  exit 1
fi

EMAIL_LEN=$(awk -F= '$1=="DHO_SMOKE_ADMIN_EMAIL"{print $2}' /tmp/.web-smoke-redacted.$$)
FILE_LEN=$(awk -F= '$1=="DHO_SMOKE_ADMIN_PASSWORD_FILE"{print $2}' /tmp/.web-smoke-redacted.$$)
rm -f /tmp/.web-smoke-redacted.$$ /tmp/.web-smoke-redacted.err.$$

if [ -z "${EMAIL_LEN:-}" ] || [ "$EMAIL_LEN" -le 0 ] || [ -z "${FILE_LEN:-}" ] || [ "$FILE_LEN" -le 0 ]; then
  st 1
  echo "FAIL: DHO_SMOKE_ADMIN_EMAIL or DHO_SMOKE_ADMIN_PASSWORD_FILE resolves BLANK -- an empty credential must never run this STEP. Set both in ops/.env (CHAOS-6972 *_FILE convention)." >&2
  exit 1
fi

# The password file's bind mount (${DHO_SMOKE_ADMIN_PASSWORD_FILE}:${DHO_SMOKE_ADMIN_PASSWORD_FILE}:ro)
# is already declared in compose.bigboy.smoke.yml itself -- compose's own --env-file
# substitution resolves it, so this script never has to read or re-derive that path.
docker compose "${BASE_COMPOSE_ARGS[@]}" run --rm --no-deps web-smoke
rc=$?
st "$rc"
if [ $rc -ne 0 ]; then
  echo "FAIL: web-path smoke did not pass -- see the receipt at _records/web-path-smoke-receipt.json for which check failed." >&2
fi
exit $rc
