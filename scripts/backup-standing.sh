#!/usr/bin/env bash
# scripts/backup-standing.sh — CHAOS-4084
#
# Commits the backup recipe validated 2026-08-22 (see
# docs/operate/maintain/backup-and-restore.md) as a script, so the next
# backup does not have to be reconstructed from artifact archaeology.
#
# Dumps the STANDING dev-health stack's Postgres and ClickHouse state into
# dev-health/backups/<timestamp>/ (one directory per run, timestamped so
# runs never collide):
#   - Postgres: `pg_dumpall` piped through gzip -> postgres-all-<ts>.sql.gz,
#     plus a postgres-dumpall-<ts>.log capturing pg_dumpall's own stderr.
#   - ClickHouse: one native `BACKUP DATABASE ... TO File(...)` per
#     database (system/information_schema excluded), copied out via
#     `docker cp` and cleaned up container-side ->
#     clickhouse-<db>-<ts>.zip.
#
# Every artifact is verified after the fact (never trust a zero exit code
# alone): gzip -t + the pg_dumpall header/footer + a CREATE DATABASE count
# reconciled against a live, non-template database count; unzip -t per
# ClickHouse zip. sha256 digests are printed and written to
# SHA256SUMS for every artifact so a caller can pin/compare.
#
# Idempotent: refuses to write into an existing, non-empty backup
# directory unless --force is passed. Every artifact name carries this
# run's own timestamp, so --force never actually overwrites a prior run's
# files (it never truly is destructive) -- it only lifts the guard against
# proceeding into a directory a caller may not have expected to be
# non-empty (a stale --out-dir, or a same-second re-run colliding on the
# default timestamped directory).
#
# Usage:
#   scripts/backup-standing.sh [--force] [--out-dir DIR]
#
# Env overrides (defaults match compose.yml / ops/.env):
#   BACKUP_PG_CONTAINER   (default dev-health-postgres-1)
#   BACKUP_CH_CONTAINER   (default dev-health-clickhouse-1)
#   POSTGRES_USER         (default devhealth)
#   CLICKHOUSE_USER       (default ch)
#   CLICKHOUSE_PASSWORD   (default ch)
#   BACKUP_OUT_ROOT       (default <dev-health-root>/backups)
set -euo pipefail

FORCE=0
OUT_DIR_OVERRIDE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=1
      shift
      ;;
    --out-dir)
      OUT_DIR_OVERRIDE="${2:?--out-dir requires a value}"
      shift 2
      ;;
    -h | --help)
      sed -n '2,41p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "backup-standing.sh: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

command -v docker >/dev/null 2>&1 || {
  echo "backup-standing.sh: docker is required" >&2
  exit 1
}

# Repo-root / dev-health-root resolution: this script lives in the ops
# repo, but the STANDING stack (compose.yml, the containers this script
# talks to) is defined one level above the repo root -- NOT one level
# above this script's own directory. A plain checkout has ops/ directly
# under dev-health/, but a linked worktree (ops/worktrees/ops/<branch>/)
# is nested deeper, so "one level up from this script" is wrong there.
# `git --path-format=absolute --git-common-dir` resolves the ORIGINAL
# repo's .git regardless of which worktree is running this script (mirrors
# acr's scripts/trial/common.sh resolve_dev_health_root convention);
# ../.. from that is this repo's true root, and one more level up is
# dev-health/. Validated by checking the landmark file (compose.yml)
# actually exists there, never assumed from path arithmetic alone.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
dev_health_root=""
git_common_dir="$(cd "$script_dir" && git rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
if [[ -n "$git_common_dir" ]]; then
  # git_common_dir is the ORIGINAL repo's <repo_root>/.git regardless of
  # which linked worktree is running this script, so .git/.. is that
  # repo's root and .git/../.. is one level above it (dev-health/).
  dev_health_root="$(cd "$git_common_dir/../.." 2>/dev/null && pwd -P || true)"
fi
if [[ -z "$dev_health_root" ]]; then
  # git unavailable: fall back to a fixed two-levels-up from this script,
  # correct only for a plain, non-worktree checkout (scripts/ -> repo
  # root -> dev-health/).
  dev_health_root="$(cd "$script_dir/../.." 2>/dev/null && pwd -P || true)"
fi
if [[ -z "$dev_health_root" || ! -f "$dev_health_root/compose.yml" ]]; then
  echo "backup-standing.sh: could not resolve dev-health/ (expected compose.yml one level above the ops repo root, got dev_health_root=${dev_health_root:-<empty>}) -- run this from a normal ops checkout or linked worktree, not a detached copy" >&2
  exit 1
fi

PG_CONTAINER="${BACKUP_PG_CONTAINER:-dev-health-postgres-1}"
CH_CONTAINER="${BACKUP_CH_CONTAINER:-dev-health-clickhouse-1}"
PG_USER="${POSTGRES_USER:-devhealth}"
CH_USER="${CLICKHOUSE_USER:-ch}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-ch}"
OUT_ROOT="${BACKUP_OUT_ROOT:-$dev_health_root/backups}"

for c in "$PG_CONTAINER" "$CH_CONTAINER"; do
  running="$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null || true)"
  if [[ "$running" != "true" ]]; then
    echo "backup-standing.sh: container not running: $c" >&2
    exit 1
  fi
done

TS="$(date -u +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR_OVERRIDE:-$OUT_ROOT/$TS}"

if [[ -d "$OUT_DIR" && -n "$(ls -A "$OUT_DIR" 2>/dev/null)" && "$FORCE" -ne 1 ]]; then
  echo "backup-standing.sh: $OUT_DIR already exists and is non-empty -- refusing to proceed (pass --force to write into it anyway; every artifact name carries this run's own timestamp, so nothing gets overwritten)" >&2
  exit 1
fi
mkdir -p "$OUT_DIR"

echo "backup-standing.sh: writing to $OUT_DIR"

# --- Postgres ---
pg_dump_file="$OUT_DIR/postgres-all-$TS.sql.gz"
pg_log_file="$OUT_DIR/postgres-dumpall-$TS.log"
echo "backup-standing.sh: pg_dumpall ($PG_CONTAINER, user=$PG_USER)..."
docker exec "$PG_CONTAINER" pg_dumpall -U "$PG_USER" 2>"$pg_log_file" | gzip >"$pg_dump_file"

# --- ClickHouse ---
echo "backup-standing.sh: listing ClickHouse databases ($CH_CONTAINER)..."
mapfile -t ch_dbs < <(docker exec "$CH_CONTAINER" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" -q \
  "SELECT name FROM system.databases WHERE name NOT IN ('system','information_schema','INFORMATION_SCHEMA') ORDER BY name")

if [[ "${#ch_dbs[@]}" -eq 0 ]]; then
  echo "backup-standing.sh: WARNING no ClickHouse databases found besides system -- nothing to back up there" >&2
fi

ch_zips=()
for db in "${ch_dbs[@]}"; do
  container_zip="${db}-${TS}.zip"
  local_zip="$OUT_DIR/clickhouse-${db}-${TS}.zip"
  echo "backup-standing.sh: ClickHouse BACKUP DATABASE \`$db\`..."
  docker exec "$CH_CONTAINER" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" -q \
    "BACKUP DATABASE \`${db}\` TO File('${container_zip}')" >/dev/null
  docker cp "$CH_CONTAINER:/var/lib/clickhouse/backups/$container_zip" "$local_zip"
  # Clean container-side copy -- allowed_path is a shared namespace across
  # every run, so a leftover here would collide on any db name reused
  # after a full timestamp second (extremely unlikely, but free to avoid).
  docker exec "$CH_CONTAINER" rm -f "/var/lib/clickhouse/backups/$container_zip"
  ch_zips+=("$local_zip")
done

# --- Verify (never trust a zero exit code alone) ---
echo "backup-standing.sh: verifying..."
gzip -t "$pg_dump_file" || {
  echo "backup-standing.sh: VERIFY FAILED: $pg_dump_file is not a valid gzip" >&2
  exit 1
}
# Decompressed to ONE temp file rather than piped repeatedly through
# head/tail/grep: a truncating consumer (head -N) closes its end of the
# pipe as soon as it has enough lines, which sends gzip a SIGPIPE and
# gives it a nonzero exit -- under `set -o pipefail` that nonzero exit
# becomes the pipeline's own reported status even though the downstream
# grep genuinely matched, producing a false VERIFY FAILED. Decompressing
# once, to a real file, sidesteps that entirely.
pg_plain_file="$OUT_DIR/.postgres-all-$TS.sql.plain"
gzip -dc "$pg_dump_file" >"$pg_plain_file"
if ! head -5 "$pg_plain_file" | grep -q "PostgreSQL database cluster dump"; then
  echo "backup-standing.sh: VERIFY FAILED: $pg_dump_file is missing the pg_dumpall header -- dump likely incomplete or corrupt" >&2
  exit 1
fi
if ! tail -5 "$pg_plain_file" | grep -q "PostgreSQL database cluster dump complete"; then
  echo "backup-standing.sh: VERIFY FAILED: $pg_dump_file is missing the pg_dumpall completion footer -- dump likely truncated" >&2
  exit 1
fi
dumped_db_count="$(grep -c '^CREATE DATABASE ' "$pg_plain_file")"
rm -f "$pg_plain_file"
# pg_dumpall never emits a CREATE DATABASE for `postgres` itself -- every
# target cluster already has it by construction, so dumpall assumes it
# rather than recreating it (live-verified: it dumps postgres' CONTENTS
# via a \connect block, just never its CREATE DATABASE). Excluded from
# the live count for the same reason, not because it went unbacked-up.
live_db_count="$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -tAc "SELECT count(*) FROM pg_database WHERE datistemplate = false AND datname <> 'postgres'")"
live_db_count="${live_db_count//[[:space:]]/}"
if [[ "$dumped_db_count" -ne "$live_db_count" ]]; then
  echo "backup-standing.sh: VERIFY FAILED: dump has $dumped_db_count CREATE DATABASE statements, live cluster has $live_db_count non-template databases -- see $pg_log_file" >&2
  exit 1
fi
echo "backup-standing.sh: postgres OK ($dumped_db_count databases, header+footer present)"

for zip in "${ch_zips[@]}"; do
  unzip -t "$zip" >/dev/null || {
    echo "backup-standing.sh: VERIFY FAILED: $zip failed unzip -t" >&2
    exit 1
  }
done
echo "backup-standing.sh: clickhouse OK (${#ch_zips[@]} database(s) verified)"

# --- Digests ---
echo "backup-standing.sh: sha256 digests:"
(
  cd "$OUT_DIR"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- * | tee SHA256SUMS
  else
    shasum -a 256 -- * | tee SHA256SUMS
  fi
)

echo "backup-standing.sh: done -- $OUT_DIR"
