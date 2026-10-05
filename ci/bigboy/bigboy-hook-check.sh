#!/usr/bin/env bash
# bigboy-hook-check.sh (CHAOS-6641): run the SAME verb the prod migrate hook Job runs -- `dho migrate upgrade --river=false`
# from the pinned OPERATOR image, with the env shape the prod Job gets (native ClickHouse DSN, MIGRATION_DATABASE_URI,
# contract 2, celery cutover flag) -- against the bigboy stores. bigboy's compose `migrate` service runs `dho migrate upgrade --river`
# from the dho image (the cut's own migrate step); this check is the hook's verb (`--river=false`) from the OPERATOR image, the image the prod Job runs. nothing printed but step names/exit codes. usage: bigboy-hook-check.sh [record-dir]
# CHAOS-8371 (D4566 class rules): no credential is read, held or passed by this script. The operator runs as the compose one-off
# `venue-hook` (compose.bigboy.hook-check.yml); compose interpolates both DSNs from ops/.env itself. No printenv out of a
# container, no script-written env file, no bare `docker run`: compose verbs only.
set -euo pipefail; umask 077
R=/home/ubuntu/devhealth
REC=${1:-$R/_records/bigboy-a2b9bf79}; SHA=$(cat "$REC/sha.txt")   # the pinned ops sha (Trap #328: derived, not typed)
OP=ghcr.io/full-chaos/dev-health-go-operator@$(docker buildx imagetools inspect ghcr.io/full-chaos/dev-health-go-operator:sha-${SHA:0:7} --format '{{json .Manifest}}' | jq -r .digest)
[[ $OP == *@sha256:* ]] || { echo "no operator image for sha-${SHA:0:7}"; exit 3; }
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$R"   # compose project root (bigboy-cut.sh runs from here too)
export COMPOSE_FILE="${COMPOSE_FILE:-compose.yml}:$HERE/compose.bigboy.hook-check.yml"   # by hand: the stack file supplies the network
export HOOK_OPERATOR_IMAGE=$OP   # an image reference, not a credential
HC=(docker compose --env-file ops/.env --profile venue run --rm --no-deps -T venue-hook)
RC=0
OUT1=$("${HC[@]}" migrate upgrade --river=false 2>&1) || RC1=$?
echo "$OUT1" | grep -E '"msg":"migrate step|"action"|"error"|step_failed' | sed -E 's/"time":"[^"]*",//' | cut -c1-200
echo "hook_upgrade_rc=${RC1:-0} image=${OP##*@}"
# Step 3 of the hook (the one that failed on prod rev 182) exercised on its own, read-only: native DSN against ClickHouse.
# (CHAOS-6641: bigboy lacked alembic 0066 until 03:26Z 09-25; applied through the pinned migrate path with DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1.)
OUT2=$("${HC[@]}" migrate clickhouse status --check 2>&1) || RC2=$?
echo "$OUT2" | grep -oE '"(state|error|code|applied|pending)"[^,}]{0,60}' | head -6
echo "ch_status_check_rc=${RC2:-0}"
[[ ${RC2:-0} -eq 0 ]] || RC=1          # the native-DSN ClickHouse step is the hard gate
[[ ${RC1:-0} -eq 0 ]] || RC=1          # the full hook verb is a gate too (bigboy alembic heads 0066+0138 since 09-25 03:26Z)
exit $RC
