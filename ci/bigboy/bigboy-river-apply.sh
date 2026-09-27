#!/usr/bin/env bash
# bigboy: the prod river-migrate hook's own verb (`dho migrate river --apply-and-check`, applies the role posture GRANTs incl. the api role) from the
# overlay's pinned dho image. bigboy-hook-check.sh's `migrate upgrade --river=false` does NOT apply grants; without this step bigboy's PG role grants lag the
# posture (found 09-25: devhealth_api lacked SELECT on tier_limits -> Go PUT sync-configs/{id}/repositories 500 permission denied). Secrets by env only, redacted output.
set -u
R=/home/ubuntu/devhealth; NET=dev-health_dev-health
DHO=$(grep -oE 'ghcr.io/full-chaos/dev-health-go-dho@sha256:[0-9a-f]{64}' $R/compose/compose.bigboy.images.yml | head -1)
[[ -n $DHO ]] || { echo "no dho image in the overlay"; exit 3; }
PGPW=$(docker exec dev-health-api-1 printenv POSTGRES_PASSWORD)
docker run --rm --network "$NET" --entrypoint /usr/local/bin/dho \
  -e MIGRATION_DATABASE_URI="postgresql://devhealth:${PGPW}@postgres:5432/devhealth?sslmode=disable" \
  -e RIVER_DATABASE_SCHEMA=river -e RIVER_DOMAIN_DATABASE_ROLE=devhealth_domain -e RIVER_QUEUE_DATABASE_ROLE=devhealth_queue \
  -e RIVER_COORDINATOR_DATABASE_ROLE=devhealth_coordinator -e API_DATABASE_ROLE=devhealth_api \
  "$DHO" migrate river --apply-and-check 2>&1 | sed -E 's#(://[^:/@]*:)[^@/]*@#\1<redacted>@#g' | tail -8 | cut -c1-200
echo "river_apply_rc=${PIPESTATUS[0]} image=${DHO##*@}"
