#!/usr/bin/env bash
# web-path-smoke.sh -- CHAOS-6987/R460: the ONLY sanctioned proof that a bigboy cut
# serves the real org end to end through a real browser session, not a hand-minted
# token. Logs in through web's real /api/v1/auth/login route as the bigboy admin,
# then runs the Cockpit/Diagnose operations (`dho smoke web-path`, internal/websmoke, from the dho image) against the router.
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
export DHO_SMOKE_DIR="$HERE"
R=/home/ubuntu/devhealth
cd "$R"

BASE_COMPOSE_ARGS=(--env-file ops/.env -f compose.yml -f compose/compose.go.workers.yml \
  -f .remember/lanes/team-lead/reconciler-sweep-override.yml \
  -f compose/compose.bigboy.images.yml \
  -f "$HERE/compose.bigboy.router.yml" -f "$HERE/compose.bigboy.smoke.yml")

st() { echo "STEP web-path-smoke rc=$1 $(date -u +%T)"; }

# The edge catalog (registered GraphQL document digests) at the DEPLOYED ops sha: the smoke
# fails if web's own documents no longer hash to it. bigboy-cut.sh exports the file it
# fetched; a standalone run names the deployed sha in DHO_SMOKE_OPS_SHA instead.
if [ -z "${DHO_SMOKE_CATALOG_FILE:-}" ]; then
  if [ -z "${DHO_SMOKE_OPS_SHA:-}" ]; then
    st 1; echo "FAIL: set DHO_SMOKE_CATALOG_FILE or DHO_SMOKE_OPS_SHA (the deployed ops sha) -- the smoke cannot verify web's GraphQL documents without the edge catalog" >&2; exit 1
  fi
  DHO_SMOKE_CATALOG_FILE=$(mktemp)
  if ! gh api "repos/full-chaos/dev-health-ops/contents/contracts/graphql/v1/go_api_operations.json?ref=$DHO_SMOKE_OPS_SHA" -H 'Accept: application/vnd.github.raw' > "$DHO_SMOKE_CATALOG_FILE"; then
    st 1; echo "FAIL: could not fetch the edge catalog at $DHO_SMOKE_OPS_SHA" >&2; exit 1
  fi
fi
chmod 644 "$DHO_SMOKE_CATALOG_FILE"
export DHO_SMOKE_CATALOG_FILE

# The smoke compose file's web-smoke service requires both vars (:? in
# compose.bigboy.smoke.yml) -- `config` fails outright while either is unset,
# which IS the fail-loud signal this gate needs; a set-but-blank value is caught
# by the length check below.
RED=$(mktemp); RED_ERR=$(mktemp)  # honours TMPDIR; never a fixed /tmp name
if ! "$HERE/compose-config-redacted.sh" "${BASE_COMPOSE_ARGS[@]}" > "$RED" 2>"$RED_ERR"; then
  st 1
  cat "$RED_ERR" >&2  # compose-config-redacted.sh emits only `required variable NAME is missing` lines
  echo "FAIL: credential file missing -- set DHO_SMOKE_ADMIN_EMAIL and DHO_SMOKE_ADMIN_PASSWORD_FILE (0600) in ops/.env (CHAOS-6972 *_FILE convention). This STEP does not run against the real org until chris adds both." >&2
  rm -f "$RED" "$RED_ERR"
  exit 1
fi

EMAIL_LEN=$(awk -F= '$1=="DHO_SMOKE_ADMIN_EMAIL"{print $2}' "$RED")
FILE_LEN=$(awk -F= '$1=="DHO_SMOKE_ADMIN_PASSWORD_FILE"{print $2}' "$RED")
rm -f "$RED" "$RED_ERR"

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
if [ $rc -eq 3 ]; then
  echo "KNOWN-GAP: every check passed except named KNOWN-MISSING operations (see routing-ops.txt) -- not a clean pass" >&2
elif [ $rc -ne 0 ]; then
  echo "FAIL: web-path smoke did not pass -- see the receipt at _records/web-path-smoke-receipt.json for which check failed." >&2
fi
exit $rc
