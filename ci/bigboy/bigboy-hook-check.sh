#!/usr/bin/env bash
# bigboy-hook-check.sh (CHAOS-6641): run the SAME verb the prod migrate hook Job runs -- `dho migrate upgrade --river=false`
# from the pinned OPERATOR image, with the env shape the prod Job gets (native ClickHouse DSN, MIGRATION_DATABASE_URI,
# contract 2, celery cutover flag) -- against the bigboy stores. bigboy's compose `migrate` service is the Python chain
# (`dev_health_ops.cli migrate postgres && migrate clickhouse`, HTTP :8123) and never exercises this path. Secrets by env
# file (0600, tmp) only; nothing printed but step names/exit codes. usage: bigboy-hook-check.sh [record-dir]
set -euo pipefail; umask 077
R=/home/ubuntu/devhealth
REC=${1:-$R/_records/bigboy-a2b9bf79}; SHA=$(cat "$REC/sha.txt")   # the pinned ops sha (Trap #328: derived, not typed)
OP=ghcr.io/full-chaos/dev-health-go-operator@$(docker buildx imagetools inspect ghcr.io/full-chaos/dev-health-go-operator:sha-${SHA:0:7} --format '{{json .Manifest}}' | jq -r .digest)
[[ $OP == *@sha256:* ]] || { echo "no operator image for sha-${SHA:0:7}"; exit 3; }
ENVF=$(mktemp); trap 'rm -f "$ENVF"' EXIT
PW=$(docker exec dev-health-api-1 printenv POSTGRES_PASSWORD); CHU=$(docker exec dev-health-api-1 printenv CLICKHOUSE_USER); CHP=$(docker exec dev-health-api-1 printenv CLICKHOUSE_PASSWORD)
{ echo "MIGRATION_DATABASE_URI=postgresql://devhealth:${PW}@postgres:5432/devhealth?sslmode=disable"
  echo "CLICKHOUSE_URI=clickhouse://${CHU}:${CHP}@clickhouse:9000/default"
  echo "OPERATIONAL_ORDERING_CONTRACT=2"; echo "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"; } > "$ENVF"
RC=0
OUT1=$(docker run --rm --network dev-health_dev-health --env-file "$ENVF" "$OP" migrate upgrade --river=false 2>&1) || RC1=$?
echo "$OUT1" | grep -E '"msg":"migrate step|"action"|"error"|step_failed' | sed -E 's/"time":"[^"]*",//' | cut -c1-200
echo "hook_upgrade_rc=${RC1:-0} image=${OP##*@}"
# Step 3 of the hook (the one that failed on prod rev 182) exercised on its own, read-only: native DSN against ClickHouse.
# (CHAOS-6641: bigboy lacked alembic 0066 until 03:26Z 09-25; applied through the pinned migrate path with DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1.)
OUT2=$(docker run --rm --network dev-health_dev-health --env-file "$ENVF" "$OP" migrate clickhouse status --check 2>&1) || RC2=$?
echo "$OUT2" | grep -oE '"(state|error|code|applied|pending)"[^,}]{0,60}' | head -6
echo "ch_status_check_rc=${RC2:-0}"
[[ ${RC2:-0} -eq 0 ]] || RC=1          # the native-DSN ClickHouse step is the hard gate
[[ ${RC1:-0} -eq 0 ]] || RC=1          # the full hook verb is a gate too (bigboy alembic heads 0066+0138 since 09-25 03:26Z)
exit $RC
