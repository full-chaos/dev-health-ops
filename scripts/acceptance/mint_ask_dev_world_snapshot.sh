#!/usr/bin/env bash
# CHAOS-3463: mint (or re-mint) the ask-dev-world.v1 snapshot + WORLD_DIGEST.
#
# This is the ONE-OFF operator flow. Ordinary acceptance boots never run it --
# they run `dev-hops fixtures world-restore` (wired into
# run_ask_dev_compose.sh), which restores the artifact this script produced
# and VERIFIES the pin rather than minting it.
#
# Why a mint step exists at all
# -----------------------------
# Cross-generation WORLD_DIGEST reproducibility is declared-blocked
# (world.json's `cross_generation_digest_status`, CHAOS-3432): two
# independent `fixtures world` runs do NOT produce the same digest, so
# regenerating the world on every boot can never match a pinned digest.
# SINGLE-generation pinning IS proven. So the world is generated exactly
# once, snapshotted, and every boot restores those same bytes -- which puts
# every boot inside the regime that is proven.
#
# What it does
# ------------
#   1. creates + migrates a pair of scratch databases inside the ALREADY
#      RUNNING acceptance stack;
#   2. runs `fixtures world` against them (the scratch guard is satisfied
#      honestly -- the database really is named *_scratch and really is
#      disposable);
#   3. `fixtures world-snapshot` -- derives the written-table set by diffing
#      the generated scratch databases against the stack's own
#      freshly-migrated serving databases, and writes the artifact;
#   4. `fixtures world-restore --mint-digest` -- restores into the serving
#      databases, runs the round-trip row-count oracle, and re-pins
#      WORLD_DIGEST from the RESTORED state (the state every boot verifies);
#   5. asserts the scratch generation's digest and the restored digest are
#      identical -- a differential proof that the snapshot round trip is
#      lossless, rather than a claim that it is.
#
# Preconditions (checked, not assumed): the acceptance Compose project must
# already be up with FRESH volumes (`down --volumes` then `up`), and its
# serving databases must be untouched -- `world-restore`'s own emptiness
# precondition enforces the second half of that and fails closed.
#
#   docker compose --project-name dev-health-ask-dev-acceptance \
#     --project-directory <ops> -f <ops>/compose.yml \
#     -f <ops>/tests/acceptance/compose.ask-dev.yml \
#     --profile ask-dev-acceptance up -d --build --wait \
#     postgres pgbouncer clickhouse valkey migrate ask-dev-scripted-openai api
#
# The artifact is written into the repo (tests/acceptance/world/
# ask-dev-world.v1/snapshot/) and is meant to be committed alongside the
# re-minted WORLD_DIGEST -- the two are only ever valid as a pair.
set -euo pipefail

# Same compose-interpolation hardening run_ask_dev_compose.sh performs, and for
# the same reason: `docker compose` interpolates every `${VAR}` in compose.yml
# and the acceptance overlay against THIS process's environment, so a
# direnv-loaded ops/.env silently redirects this flow at a different database.
# test_mint_script_hardens_the_same_env_as_the_launcher (see
# tests/acceptance/test_ask_dev_compose.py) asserts this list is identical to
# the launcher's, so the two cannot drift apart.
unset \
  POSTGRES_DB CLICKHOUSE_DB LOG_LEVEL \
  MIGRATION_DATABASE_URI MIGRATION_DATABASE_URI_FILE \
  RIVER_DATABASE_SCHEMA \
  RIVER_DOMAIN_DATABASE_ROLE RIVER_DOMAIN_DATABASE_PASSWORD \
  RIVER_QUEUE_DATABASE_ROLE RIVER_QUEUE_DATABASE_PASSWORD \
  RIVER_COORDINATOR_DATABASE_ROLE RIVER_COORDINATOR_DATABASE_PASSWORD \
  SETTINGS_ENCRYPTION_KEY \
  STRIPE_SECRET_KEY STRIPE_WEBHOOK_SECRET STRIPE_PRICE_ID_TEAM STRIPE_PRICE_ID_ENTERPRISE \
  LICENSE_PRIVATE_KEY \
  EMAIL_PROVIDER EMAIL_API_KEY EMAIL_FROM_ADDRESS \
  OTEL_ENABLED OTEL_EXPORTER_OTLP_ENDPOINT OTEL_SERVICE_NAME OTEL_METRIC_EXPORT_INTERVAL \
  SENTRY_DSN SENTRY_ENVIRONMENT SENTRY_TRACES_RATE SENTRY_SEND_PII \
  WORKER_CONCURRENCY WORKER_HEAVY_CONCURRENCY \
  WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED WORKER_GITHUB_REPO_METADATA_ENABLED \
  WORKER_GITLAB_REPO_METADATA_ENABLED WORKER_GITLAB_INCIDENTS_ENABLED \
  DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER \
  BUGSINK_BASE_URL BUGSINK_CREATE_SUPERUSER \
  ASK_DEV_QUA_SHADOW_ENABLED ASK_DEV_QUA_COMMIT_ENABLED
# ^ CHAOS-3532: cleared here too, and this script deliberately has NO
# opt-in to turn them back on. ops/.env exports both for the dev stack and
# direnv carries them into every ops shell, so a mint run from a developer
# shell would otherwise boot an ARMED stack -- and a snapshot minted from
# one bakes QUA-influenced state into the fixture world every future
# acceptance run then restores. Minting must never be armed; running an
# armed stack is what run_ask_dev_compose.sh's ASK_DEV_ACCEPTANCE_QUA knob
# is for.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ops_root="$(cd -- "${script_dir}/../.." && pwd)"
project_name="${ASK_DEV_ACCEPTANCE_PROJECT_NAME:-dev-health-ask-dev-acceptance}"
world_dir="${ops_root}/tests/acceptance/world/ask-dev-world.v1"
container_world_dir="/app/tests/acceptance/world/ask-dev-world.v1"
manifest="${container_world_dir}/world.json"

ch_scratch="clickhouse://ch:ch@clickhouse:8123/ask_dev_world_scratch"
pg_scratch="postgresql+asyncpg://postgres:postgres@postgres:5432/ask_dev_world_scratch"
# The databases the acceptance API actually serves. `fixtures world` refuses
# these by name and MUST keep refusing them -- only `world-restore`, which is
# insert-only and gated on ENVIRONMENT=acceptance plus an empty target, ever
# writes here.
ch_serving="clickhouse://ch:ch@clickhouse:8123/default"
pg_serving="postgresql+asyncpg://postgres:postgres@postgres:5432/postgres"

# Compose interpolates EVERY `${VAR}` in both files before it will run any
# command, including ones belonging to services this flow never builds or
# starts. `web` declares `${ASK_DEV_WEB_CONTEXT:?}` and `bugsink` declares
# `${BUGSINK_SECRET_KEY:?}`, so without these even `compose ps` exits 1 with an
# interpolation error (observed). Neither service is started here; these values
# exist only to let interpolation succeed.
export ASK_DEV_WEB_CONTEXT="${ASK_DEV_WEB_CONTEXT:-${ops_root}}"
export BUGSINK_SECRET_KEY="${BUGSINK_SECRET_KEY:-ask-dev-acceptance-unused}"
export ASK_DEV_ACCEPTANCE_API_PORT="${ASK_DEV_ACCEPTANCE_API_PORT:-18080}"

compose=(
  docker compose
  --project-name "${project_name}"
  --project-directory "${ops_root}"
  -f "${ops_root}/compose.yml"
  -f "${ops_root}/tests/acceptance/compose.ask-dev.yml"
  --profile ask-dev-acceptance
)

if ! "${compose[@]}" ps --status running --services | grep -qx api; then
  echo "mint: the acceptance stack's api service is not running." >&2
  echo "mint: bring the stack up with FRESH volumes first (see this script's header)." >&2
  exit 69
fi

# CHAOS-3544: refuse to mint unless the running api container is serving
# THIS checkout.
#
# `fixtures world` runs INSIDE the api container (see the exec calls below),
# so the world is generated by whatever code that container reads -- not by
# the checkout this script was invoked from. Those differ more easily than
# they look: compose bind-mounts the PROJECT DIRECTORY at /app, so a stack
# booted from one worktree keeps serving that worktree's source even when
# this script is run from another. A stale image is the other way in.
#
# The header used to merely DOCUMENT `up -d --build` as a prerequisite, which
# is a dead guard by construction: nothing checked it, and skipping it is
# invisible.
#
# The failure that motivated this is the worst outcome this ticket has: on
# 2026-08-07 the running stack was bound to a DIFFERENT worktree, so the
# container still read the pre-CHAOS-3432 generator (history to the edge of a
# 90-day TTL) while the operator's own checkout held the fix. Minting then
# would have regenerated the decaying world, snapshotted it, re-pinned
# WORLD_DIGEST, printed "mint: done" -- and shipped a snapshot that fails its
# own content oracle again within days, with a fresh digest making it look
# deliberate. Measured directly, from a checkout that HAD the fix:
#     container has TTL cap: False
#     container has old literal: True
#
# Compares a source signature rather than probing one symbol: any drift
# between the invoking checkout and the running container is a mint that does not
# mean what its operator thinks it means.
echo "mint: verifying the api container is serving this checkout"
# Kept as a plain newline-delimited list, and interpolated into the
# container-side python as a literal rather than through bash array
# quoting: `${array[*]@Q}` is bash 5 syntax and macOS ships bash 3.2, so
# the first version of this guard died with "bad substitution" -- a guard
# that cannot run is worse than none, because it reads as coverage.
fixture_sources="dev_health_ops/fixtures/generators/interactions.py
dev_health_ops/fixtures/ttl_horizon.py
dev_health_ops/fixtures/world_snapshot.py
dev_health_ops/fixtures/world.py"

host_signature="$(
  for rel in ${fixture_sources}; do
    shasum -a 256 "${ops_root}/src/${rel}"
  done | awk '{print $1}' | shasum -a 256 | awk '{print $1}'
)"
container_signature="$(
  "${compose[@]}" exec -T api python -c "
import hashlib, pathlib, sys
import dev_health_ops
root = pathlib.Path(dev_health_ops.__file__).resolve().parent.parent
parts = [
    hashlib.sha256((root / rel).read_bytes()).hexdigest()
    for rel in sys.argv[1].split()
]
print(hashlib.sha256(('\n'.join(parts) + '\n').encode()).hexdigest())
" "${fixture_sources}" | tr -d '\r'
)"
if [[ "${host_signature}" != "${container_signature}" ]]; then
  echo "mint: REFUSING -- the api container is not serving this checkout." >&2
  echo "mint:   host      ${host_signature}" >&2
  echo "mint:   container ${container_signature}" >&2
  echo "mint: The world is generated INSIDE the container, so minting now would" >&2
  echo "mint: snapshot code you are not looking at. Rebuild first:" >&2
  echo "mint:   docker compose --project-name dev-health-ask-dev-acceptance \\" >&2
  echo "mint:     --project-directory ${ops_root} -f ${ops_root}/compose.yml \\" >&2
  echo "mint:     -f ${ops_root}/tests/acceptance/compose.ask-dev.yml \\" >&2
  echo "mint:     --profile ask-dev-acceptance up -d --build --wait api" >&2
  exit 70
fi
echo "mint: api container matches this checkout (${host_signature:0:12})"

echo "mint: creating scratch databases"
"${compose[@]}" exec -T postgres psql -U postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS ask_dev_world_scratch;" \
  -c "CREATE DATABASE ask_dev_world_scratch;"
# Single-line `python -c`, never a heredoc: `docker compose exec -T … python -`
# reading a heredoc off this script's stdin hangs indefinitely when the script
# runs non-interactively (observed -- the mint sat on the round-trip step for as
# long as it was left running).
"${compose[@]}" exec -T api python -c 'import clickhouse_connect; c = clickhouse_connect.get_client(host="clickhouse", port=8123, username="ch", password="ch"); c.command("DROP DATABASE IF EXISTS ask_dev_world_scratch"); c.command("CREATE DATABASE ask_dev_world_scratch")'

echo "mint: migrating scratch databases"
"${compose[@]}" exec -T \
  -e POSTGRES_URI="${pg_scratch}" \
  -e DATABASE_URI="${pg_scratch}" \
  -e CLICKHOUSE_URI="${ch_scratch}" \
  -e DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 \
  api sh -c 'dev-hops migrate postgres && dev-hops migrate clickhouse'

echo "mint: generating ask-dev-world.v1 (once) into the scratch databases"
"${compose[@]}" exec -T api dev-hops fixtures world \
  --manifest "${manifest}" \
  --sink "${ch_scratch}" \
  --postgres-uri "${pg_scratch}" \
  --digest-path /tmp/WORLD_DIGEST_generated

echo "mint: snapshotting the generated world"
"${compose[@]}" exec -T api dev-hops fixtures world-snapshot \
  --manifest "${manifest}" \
  --sink "${ch_scratch}" \
  --postgres-uri "${pg_scratch}" \
  --baseline-sink "${ch_serving}" \
  --baseline-postgres-uri "${pg_serving}" \
  --out /tmp/world-snapshot

echo "mint: restoring into the serving databases and re-pinning WORLD_DIGEST"
"${compose[@]}" exec -T api dev-hops fixtures world-restore \
  --manifest "${manifest}" \
  --sink "${ch_serving}" \
  --postgres-uri "${pg_serving}" \
  --snapshot /tmp/world-snapshot \
  --digest-path "${container_world_dir}/WORLD_DIGEST" \
  --generated-digest-path /tmp/WORLD_DIGEST_generated \
  --mint-digest

# The differential proof: if the restore were lossy in any digested column,
# these two would differ. Asserting it here means a lossy round trip can never
# be minted into the pin and then "verified" against itself forever.
echo "mint: proving the snapshot round trip is lossless"
"${compose[@]}" exec -T api python \
  /app/scripts/acceptance/assert_world_snapshot_round_trip.py \
  --generated /tmp/WORLD_DIGEST_generated \
  --restored "${container_world_dir}/WORLD_DIGEST"

# CHAOS-3463 credential contract: a snapshot may only be minted if the
# credentials it FREEZES actually authenticate. Runs against the stack's public
# API from the host, and includes a wrong-password negative control -- an API
# that accepted anything would otherwise satisfy every positive check.
echo "mint: proving the corpus contract principals can log in"
"${ops_root}/.venv/bin/python" \
  "${ops_root}/scripts/acceptance/assert_world_principals_can_log_in.py" \
  --api-url "http://127.0.0.1:${ASK_DEV_ACCEPTANCE_API_PORT}" \
  --manifest "${world_dir}/world.json"

# Copy into a sibling directory and swap only once the replacement is
# complete. Codex adversarial review round 3 (MEDIUM, confirmed): this used to
# `rm -rf` the committed snapshot BEFORE `docker compose cp` had produced its
# replacement, so any container/disk/copy failure after every proof had passed
# destroyed the existing artifact and left a checkout with a pin and no
# snapshot -- unbootable, and with any uncommitted artifact gone.
echo "mint: copying the snapshot artifact into the repo"
staging="${world_dir}/.snapshot.incoming"
rm -rf "${staging}"
"${compose[@]}" cp api:/tmp/world-snapshot "${staging}"
if [[ ! -f "${staging}/manifest.json" ]]; then
  echo "mint: the copied snapshot has no manifest.json -- refusing to replace" >&2
  echo "mint: the existing artifact. Staged copy left at ${staging}." >&2
  exit 1
fi
rm -rf "${world_dir}/snapshot"
mv "${staging}" "${world_dir}/snapshot"
"${compose[@]}" cp "api:${container_world_dir}/WORLD_DIGEST" "${world_dir}/WORLD_DIGEST"

echo "mint: done. Commit ${world_dir#"${ops_root}/"}/snapshot AND"
echo "mint: ${world_dir#"${ops_root}/"}/WORLD_DIGEST together -- they are only valid as a pair."
du -sh "${world_dir}/snapshot"
