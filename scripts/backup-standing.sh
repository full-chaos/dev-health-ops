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
# reconciled against a live, non-template, connectable database count;
# unzip -t per ClickHouse zip. sha256 digests are printed and written to
# SHA256SUMS-<ts> for every artifact this run produced so a caller can
# pin/compare.
#
# `pg_dumpall` embeds role definitions, including password hashes, by
# design (this recipe never adds --no-role-passwords -- the validated
# 2026-08-22 recipe did not, and this is a credential-bearing artifact on
# purpose, not an oversight). `umask 077` below is the actual control:
# every file/directory this script creates is owner-only from the moment
# it exists, never briefly world-readable under a default 022 umask.
#
# Idempotent: refuses to write into an existing, non-empty backup
# directory unless --force is passed, and the existence check itself is
# atomic (a bare `mkdir`, which fails if the directory already exists --
# not a separate check-then-create, which would race two concurrent
# invocations targeting the same default timestamped directory). Every
# artifact name (including this run's own SHA256SUMS-<ts>) carries this
# run's timestamp, so --force does not overwrite a PRIOR run's files --
# it only lifts the atomicity guard for a directory a caller may not have
# expected to be non-empty (a stale --out-dir, or a --force retry after a
# failed run left partial artifacts behind).
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
#
# Requires a bash with `local`/arrays (bash >= 3.2 is fine; this script
# deliberately avoids `mapfile`/`readarray`, unavailable in stock macOS
# /bin/bash 3.2). Run with an explicit modern bash if `/bin/bash` on your
# machine is the ancient stock one and `bash` on PATH is not (`command -v
# bash` should resolve to a >= 4.x install; Homebrew's bash package
# provides one).
set -euo pipefail
umask 077

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
      sed -n '2,59p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
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

# PID-suffixed (codex xhigh re-review finding, PR #1885 round 2): a
# bare second-resolution timestamp collides on a same-second --force
# retry (a failed run immediately retried with --force), which would
# truncate the FIRST run's dump/zips/SHA256SUMS-<ts> under the SAME
# filenames -- contradicting the whole point of --force being "lifts the
# directory guard" rather than "may overwrite a run's own files". `$$` is
# unique per invocation (a retry is a new process), so two runs can never
# collide on a filename even inside the same second, --force or not.
TS="$(date -u +%Y%m%d-%H%M%S)-$$"
OUT_DIR="${OUT_DIR_OVERRIDE:-$OUT_ROOT/$TS}"

mkdir -p "$OUT_ROOT"
if [[ "$FORCE" -eq 1 ]]; then
  mkdir -p "$OUT_DIR"
else
  # Atomic collision guard: `mkdir` (no -p) fails if OUT_DIR already
  # exists, so two invocations racing on the SAME default timestamped
  # directory cannot both pass a separate check-then-create and then both
  # proceed to write the same filenames -- the loser's `mkdir` fails here,
  # before either has written anything.
  if ! mkdir "$OUT_DIR" 2>/dev/null; then
    echo "backup-standing.sh: $OUT_DIR already exists -- refusing to proceed (pass --force to write into it anyway; every artifact name, including SHA256SUMS-$TS, carries this run's own timestamp, so a PRIOR run's files are never overwritten)" >&2
    exit 1
  fi
fi

echo "backup-standing.sh: writing to $OUT_DIR"

# --- Postgres ---
pg_dump_file="$OUT_DIR/postgres-all-$TS.sql.gz"
pg_log_file="$OUT_DIR/postgres-dumpall-$TS.log"
echo "backup-standing.sh: pg_dumpall ($PG_CONTAINER, user=$PG_USER)..."
docker exec "$PG_CONTAINER" pg_dumpall -U "$PG_USER" 2>"$pg_log_file" | gzip >"$pg_dump_file"

# --- ClickHouse ---
echo "backup-standing.sh: listing ClickHouse databases ($CH_CONTAINER)..."
# Written to a real file and the PRODUCING command's own exit status
# checked explicitly -- NOT `mapfile -t arr < <(cmd)`. Process
# substitution runs `cmd` asynchronously; mapfile only sees whatever
# `cmd` wrote and returns success regardless of `cmd`'s own exit code, so
# a failing `docker exec ... clickhouse-client` (bad credentials,
# container hiccup) would silently leave `ch_dbs` empty and this script
# would go on to produce and VERIFY a Postgres-only backup as a clean
# success -- exactly the silent-data-loss shape this script exists to
# prevent (codex xhigh review, PR #1885). A plain `while read` loop below
# is also more portable: `mapfile`/`readarray` do not exist in stock
# macOS /bin/bash 3.2 at all.
ch_db_list_file="$OUT_DIR/.clickhouse-databases.txt"
trap 'rm -f "$ch_db_list_file"' EXIT
if ! docker exec "$CH_CONTAINER" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" -q \
  "SELECT name FROM system.databases WHERE name NOT IN ('system','information_schema','INFORMATION_SCHEMA') ORDER BY name" \
  >"$ch_db_list_file"; then
  echo "backup-standing.sh: VERIFY FAILED: ClickHouse database listing failed ($CH_CONTAINER) -- refusing to produce a Postgres-only backup silently" >&2
  exit 1
fi
ch_dbs=()
while IFS= read -r db; do
  [[ -n "$db" ]] && ch_dbs+=("$db")
done <"$ch_db_list_file"
rm -f "$ch_db_list_file"
trap - EXIT

if [[ "${#ch_dbs[@]}" -eq 0 ]]; then
  echo "backup-standing.sh: WARNING no ClickHouse databases found besides system -- the query ran successfully and genuinely returned nothing to back up there" >&2
fi

ch_zips=()
for db in "${ch_dbs[@]}"; do
  container_zip="${db}-${TS}.zip"
  local_zip="$OUT_DIR/clickhouse-${db}-${TS}.zip"
  echo "backup-standing.sh: ClickHouse BACKUP DATABASE \`$db\`..."
  docker exec "$CH_CONTAINER" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" -q \
    "BACKUP DATABASE \`${db}\` TO File('${container_zip}')" >/dev/null
  docker cp "$CH_CONTAINER:/var/lib/clickhouse/backups/$container_zip" "$local_zip"
  # `docker cp` preserves the SOURCE file's mode from inside the
  # container (live-verified: it lands owner-rw/group-r, 640, regardless
  # of this shell's own umask) -- umask only governs files THIS script
  # creates directly. Enforced explicitly here so every artifact in the
  # output directory ends up owner-only, matching the pg_dumpall file's
  # actual protection rather than only appearing to via umask.
  chmod 600 "$local_zip"
  # Clean container-side copy -- allowed_path is a shared namespace across
  # every run, so a leftover here would collide on any db name reused
  # after a full timestamp second (extremely unlikely, but free to avoid).
  # Best-effort: an already-successful docker cp above means the backup
  # is safe either way, this is tidiness, not correctness.
  if ! docker exec "$CH_CONTAINER" rm -f "/var/lib/clickhouse/backups/$container_zip"; then
    # Non-fatal (the local copy via docker cp above already succeeded,
    # so the backup itself is not lost) but LOUD, never silent -- a
    # bare `|| true` here (codex xhigh re-review finding, PR #1885
    # round 2) would leave a sensitive backup zip sitting in the
    # container's persistent volume indefinitely while this script
    # still reports a clean overall success.
    echo "backup-standing.sh: WARNING container-side cleanup failed for $db (/var/lib/clickhouse/backups/$container_zip left on $CH_CONTAINER) -- the local copy at $local_zip is unaffected, but remove the container-side leftover manually" >&2
  fi
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
# once, to a real file, sidesteps that entirely. Trap-cleaned so a
# plaintext copy of a credential-bearing pg_dumpall (it embeds role
# password hashes by design, see the umask note above) never survives a
# verify failure on disk -- every `exit 1` path below this point would
# otherwise leave it behind.
pg_plain_file="$OUT_DIR/.postgres-all-$TS.sql.plain"
trap 'rm -f "$pg_plain_file"' EXIT
gzip -dc "$pg_dump_file" >"$pg_plain_file"
if ! head -5 "$pg_plain_file" | grep -q "PostgreSQL database cluster dump"; then
  echo "backup-standing.sh: VERIFY FAILED: $pg_dump_file is missing the pg_dumpall header -- dump likely incomplete or corrupt" >&2
  exit 1
fi
if ! tail -5 "$pg_plain_file" | grep -q "PostgreSQL database cluster dump complete"; then
  echo "backup-standing.sh: VERIFY FAILED: $pg_dump_file is missing the pg_dumpall completion footer -- dump likely truncated" >&2
  exit 1
fi
# Anchored to the exact statement shape pg_dumpall emits (WITH TEMPLATE =
# ... at end of line), not a bare `^CREATE DATABASE ` prefix -- a data
# row inside a later COPY block could theoretically start with that exact
# literal text and inflate the count on a bare prefix match. `|| true` +
# explicit fallback: `grep -c` exits 1 (not just "0 matches") when the
# count is genuinely zero, which under `set -e` would abort the script
# before the count is ever compared -- a cluster with a single database
# is a real, if unusual, case this must not crash on.
dumped_db_count="$(grep -c '^CREATE DATABASE .* WITH TEMPLATE = ' "$pg_plain_file" || true)"
dumped_db_count="${dumped_db_count:-0}"
rm -f "$pg_plain_file"
trap - EXIT
# Mirrors pg_dumpall's OWN selection criteria exactly (verified against
# src/bin/pg_dump/pg_dumpall.c's dumpDatabases(), not inferred): `SELECT
# datname FROM pg_database WHERE datallowconn AND datconnlimit != -2`,
# then template0 is always skipped (even if somehow datallowconn), and
# template1/postgres are assumed to already exist on the target so they
# get NO CREATE DATABASE statement (a \connect block instead) -- codex
# xhigh re-review, PR #1885 round 2, correctly flagged that the FIRST fix
# used `datistemplate = false` instead, which only happened to agree with
# this on an ordinary cluster (template1 has datistemplate=true there by
# coincidence) and would silently disagree on a cluster with a
# non-standard IS_TEMPLATE flag on some other database. datistemplate is
# not part of pg_dumpall's real criteria at all; do not reintroduce it.
live_db_count="$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -tAc "SELECT count(*) FROM pg_database WHERE datallowconn AND datconnlimit <> -2 AND datname NOT IN ('template0','template1','postgres')")"
live_db_count="${live_db_count//[[:space:]]/}"
if [[ "$dumped_db_count" -ne "$live_db_count" ]]; then
  echo "backup-standing.sh: VERIFY FAILED: dump has $dumped_db_count CREATE DATABASE statements, live cluster has $live_db_count non-template, connectable databases -- see $pg_log_file" >&2
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
# Named with this run's OWN timestamp (SHA256SUMS-<ts>, never a bare
# SHA256SUMS) and scoped to exactly the files this invocation produced
# (never a bare `*` glob) -- a bare name/glob would, under --force into a
# non-empty directory, silently absorb a PRIOR run's files into this
# run's manifest and overwrite that prior run's own SHA256SUMS with a
# merged one.
sha_file="SHA256SUMS-$TS"
this_run_artifacts=("$(basename "$pg_dump_file")" "$(basename "$pg_log_file")")
for zip in "${ch_zips[@]}"; do
  this_run_artifacts+=("$(basename "$zip")")
done
echo "backup-standing.sh: sha256 digests:"
(
  cd "$OUT_DIR"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "${this_run_artifacts[@]}" | tee "$sha_file"
  else
    shasum -a 256 -- "${this_run_artifacts[@]}" | tee "$sha_file"
  fi
)

echo "backup-standing.sh: done -- $OUT_DIR"
