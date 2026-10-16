#!/usr/bin/env bash
# bigboy: the prod river-migrate hook's own verb (`dho migrate river --apply-and-check`, applies the role posture GRANTs incl. the api role) from the
# overlay's pinned dho image. bigboy-hook-check.sh's `migrate upgrade --river=false` does NOT apply grants; without this step bigboy's PG role grants lag the
# posture (found 09-25: devhealth_api lacked SELECT on tier_limits -> Go PUT sync-configs/{id}/repositories 500 permission denied). Redacted output.
# CHAOS-8371 (D4566 class rules): no credential is read, held or passed by this script. dho runs as the compose one-off `venue-river`
# (compose.bigboy.river-apply.yml); compose interpolates the DSN from ops/.env itself. No printenv out of the api container, no -e, no
# bare `docker run`: compose verbs only.
set -u
R=${BIGBOY_ROOT:-/home/ubuntu/devhealth}
DHO=$(grep -oE 'ghcr.io/full-chaos/dev-health-go-dho@sha256:[0-9a-f]{64}' $R/_records/compose.bigboy.images.yml | head -1)
[[ -n $DHO ]] || { echo "no dho image in the overlay"; exit 3; }
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$R"   # compose project root (bigboy-cut.sh runs from here too)
export BIGBOY_ROOT=$R BIGBOY_TOOLS_DIR=$HERE   # CHAOS-8693: the chain, project, env file and profile come from ci/bigboy/stack.files
export RIVER_DHO_IMAGE=$DHO   # an image reference, not a credential
export BIGBOY_OPERATOR_IMAGE="${BIGBOY_OPERATOR_IMAGE:-$DHO}"   # by hand: the worker overlay only has to interpolate; this one-off runs --no-deps and starts no worker
"$HERE/stack.sh" -s river-apply run venue-river migrate river --apply-and-check 2>&1 | sed -E 's#(://[^:/@]*:)[^@/]*@#\1<redacted>@#g' | tail -8 | cut -c1-200
rc=${PIPESTATUS[0]}
echo "river_apply_rc=$rc image=${DHO##*@}"
exit "$rc"   # a failed apply must fail this step (was: the echo's status, always 0)
