#!/usr/bin/env bash
# bigboy: the prod river-migrate hook's own verb (`dho migrate river --apply-and-check`, applies the role posture GRANTs incl. the api role) from the
# pinned dho image (_records/bigboy-pins.env). bigboy-hook-check.sh's `migrate upgrade --river=false` does NOT apply grants; without this step bigboy's PG role grants lag the
# posture (found 09-25: devhealth_api lacked SELECT on tier_limits -> Go PUT sync-configs/{id}/repositories 500 permission denied). Redacted output.
# CHAOS-8371 (D4566 class rules): no credential is read, held or passed by this script. dho runs as the compose one-off `venue-river`
# (compose.bigboy.yml, profile venue); compose interpolates the DSN from ops/.env itself. No printenv out of the api container, no -e, no
# bare `docker run`: compose verbs only.
set -u
R=$(cd "${BIGBOY_ROOT:-/home/ubuntu/devhealth}" && pwd)
DHO=$(grep -oE 'ghcr.io/full-chaos/dev-health-go-dho@sha256:[0-9a-f]{64}' $R/_records/bigboy-pins.env | head -1)
[[ -n $DHO ]] || { echo "no dho image in _records/bigboy-pins.env"; exit 3; }
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$R"   # compose project root (bigboy-cut.sh runs from here too)
export BIGBOY_ROOT=$R BIGBOY_TOOLS_DIR=$HERE
unset $(compgen -e | grep '^COMPOSE_') 2>/dev/null   # CHAOS-9962: the file, project, env files and profiles come from DC only
DC=(docker compose -p dev-health --project-directory "$R" --env-file "$R/ops/.env" --env-file "$R/_records/bigboy-pins.env" -f "$HERE/compose.bigboy.yml")
export BIGBOY_OPERATOR_IMAGE="${BIGBOY_OPERATOR_IMAGE:-$DHO}"   # by hand: the worker overlay only has to interpolate; this one-off runs --no-deps and starts no worker
"${DC[@]}" --profile venue run --rm --no-deps -T venue-river migrate river --apply-and-check 2>&1 | sed -E 's#(://[^:/@]*:)[^@/]*@#\1<redacted>@#g' | tail -8 | cut -c1-200
rc=${PIPESTATUS[0]}
echo "river_apply_rc=$rc image=${DHO##*@}"
exit "$rc"   # a failed apply must fail this step (was: the echo's status, always 0)
