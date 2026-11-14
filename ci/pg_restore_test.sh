#!/usr/bin/env bash
# CHAOS-13373: restore test of one PostgreSQL custom-format dump (the nightly CronJob's <ts>.dump), run by hand on
# bigboy (docker). Never against prod, never kind. It starts a scratch PostgreSQL from PG_IMAGE (the server image the
# dump came from), restores the dump into a fresh database, and checks:
#   sha256      the dump matches <dump>.sha256 when that file is next to it
#   restore     pg_restore --no-owner --no-acl --exit-on-error into restore_check succeeds
#   stamp       restore_check.public.alembic_version holds at least one revision
#   reference   DHO_IMAGE builds an empty database to its head, and that head is the dump's stamp
#   schema      CHAOS-4467: every relation, column (name, type, not null) and index (name, definition) the reference
#               holds exists in restore_check. alembic_version alone is not trusted: a dump stamped at a revision
#               whose tables it lacks fails here, before any migration
#   rows        per table, the rows of the dump's COPY stream = count(*) in restore_check (see rows_check)
#   migrate     DHO_HEAD_IMAGE (default DHO_IMAGE) `migrate postgres upgrade` on restore_check succeeds
#   analyze     ANALYZE river.river_job when the table exists (docs/operate/maintain/backup-and-restore.md)
# One PASS/FAIL line per check, then RESTORE-TEST PASS or RESTORE-TEST FAIL (exit 0 / 1; 2 = bad input).
#
# Inputs (environment):
#   DUMP            path of the .dump file
#   PG_IMAGE        the PostgreSQL image, digest-pinned (postgresql.image of the release, e.g. postgres:18-alpine@sha256:...)
#   DHO_IMAGE       the dho (operator) image whose PostgreSQL head is the dump's stamp: the image the release ran when
#                   the dump was taken (repo@sha256:...)
#   DHO_HEAD_IMAGE  optional: the dho image to migrate restore_check to its head with (default DHO_IMAGE)
#   OUT_DIR         optional: where the schema lists, counts and logs go (default: a new dir under TMPDIR)
#   AUTH_DATABASE_SCHEMA, RIVER_DATABASE_SCHEMA  optional: as the release's migrate Job (default auth, river)
# The scratch password is generated here and never printed; the port is published on 127.0.0.1 only; the container
# and network carry a run-unique name and are removed on exit.
set -uo pipefail

die() { printf 'pg_restore_test: %s\n' "$*" >&2; exit 2; }
digest_re='@sha256:[0-9a-f]{64}$'

[ -n "${DUMP:-}" ] && [ -f "${DUMP}" ] && [ -r "${DUMP}" ] || die "DUMP must name a readable dump file"
[[ "${PG_IMAGE:-}" =~ ${digest_re} ]] || die "PG_IMAGE must be digest-pinned (...@sha256:<64 hex>)"
[[ "${DHO_IMAGE:-}" =~ ${digest_re} ]] || die "DHO_IMAGE must be digest-pinned (...@sha256:<64 hex>)"
DHO_HEAD_IMAGE="${DHO_HEAD_IMAGE:-${DHO_IMAGE}}"
[[ "${DHO_HEAD_IMAGE}" =~ ${digest_re} ]] || die "DHO_HEAD_IMAGE must be digest-pinned (...@sha256:<64 hex>)"
command -v docker >/dev/null || die "docker is not on PATH"
AUTH_SCHEMA="${AUTH_DATABASE_SCHEMA:-auth}"
RIVER_SCHEMA="${RIVER_DATABASE_SCHEMA:-river}"

DUMP_ABS="$(cd "$(dirname "${DUMP}")" && pwd)/$(basename "${DUMP}")"
OUT="${OUT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pgrt.XXXXXX")}"
mkdir -p "${OUT}" || die "cannot create OUT_DIR ${OUT}"
TAG="pgrt-$$-$(date +%s)"
PG="${TAG}-pg"
NET="${TAG}-net"
PGRT_PASSWORD="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
[ "${#PGRT_PASSWORD}" -eq 48 ] || die "could not generate the scratch password"
export PGRT_PASSWORD

# shellcheck disable=SC2329 # cleanup runs from the EXIT trap
cleanup() {
  docker rm -f "${PG}" >/dev/null 2>&1
  docker network rm "${NET}" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT
trap 'exit 1' INT TERM

failed=0
pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; failed=$((failed + 1)); }
finish() {
  printf 'out_dir=%s\n' "${OUT}"
  if [ "${failed}" -eq 0 ]; then
    printf 'RESTORE-TEST PASS\n'
    exit 0
  fi
  printf 'RESTORE-TEST FAIL (%d failed)\n' "${failed}"
  exit 1
}
redact() { sed "s/${PGRT_PASSWORD}/<redacted>/g"; }

# psql over the local socket as postgres (the image trusts its own socket); -At, stop on the first error.
psqlx() { docker exec "${PG}" psql -X -q -v ON_ERROR_STOP=1 -U postgres -At "$@"; }

# dho_upgrade <image> <db> <log>: `dho migrate postgres upgrade` against one scratch database, with the settings the
# migrate Job runs with. The DSN goes by name (-e NAME), so it is on no command line.
dho_upgrade() {
  local image="$1" db="$2" log="$3" rc
  MIGRATION_DATABASE_URI="postgres://postgres:${PGRT_PASSWORD}@${PG}:5432/${db}?sslmode=disable" \
    docker run --rm --network "${NET}" --read-only --cap-drop ALL --security-opt no-new-privileges \
    -e MIGRATION_DATABASE_URI -e DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1 \
    -e "RIVER_DATABASE_SCHEMA=${RIVER_SCHEMA}" -e "AUTH_DATABASE_SCHEMA=${AUTH_SCHEMA}" \
    "${image}" migrate postgres upgrade >"${log}.raw" 2>&1
  rc=$?
  redact <"${log}.raw" >"${log}"
  rm -f "${log}.raw"
  return "${rc}"
}

# The relations, columns and indexes of the non-system schemas, one normalised line each.
schema_lines() {
  local db="$1" sys="n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg\_%'"
  psqlx -d "${db}" -c "SELECT 'relation ' || c.relkind::text || ' ' || n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p','v','m','S') AND ${sys}" &&
    psqlx -d "${db}" -c "SELECT 'column ' || n.nspname || '.' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod) || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r','p','v','m') AND ${sys}" &&
    psqlx -d "${db}" -c "SELECT 'index ' || schemaname || '.' || indexname || ' ' || indexdef FROM pg_indexes n WHERE n.schemaname NOT IN ('pg_catalog','information_schema') AND n.schemaname NOT LIKE 'pg\_%'"
}

# rows_check: the dump's own data stream is the source of truth for what the source database held, since pg_dump
# reads every table in one snapshot. In COPY text format each row is exactly one line (a newline inside a value is
# written as \n), so counting the lines between "COPY <table> ..." and "\." counts that table's rows as dumped,
# independent of the restore. Each count must equal count(*) in restore_check. Run before the migration, which may
# change rows on purpose.
rows_check() {
  local name want got bad=0 n=0
  docker exec "${PG}" pg_restore --data-only --file=- /in/dump.dump 2>"${OUT}/rows-stream.err" |
    awk '/^COPY / && !incopy { name = $0; sub(/^COPY /, "", name); sub(/ \(.*$/, "", name); sub(/ FROM stdin;$/, "", name); n = 0; incopy = 1; next }
         incopy && $0 == "\\." { print name "\t" n; incopy = 0; next }
         incopy { n++ }' >"${OUT}/rows-dump.tsv"
  if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    fail "rows: pg_restore --data-only could not read the dump (${OUT}/rows-stream.err)"
    return
  fi
  : >"${OUT}/rows-restore.tsv"
  while IFS=$'\t' read -r name want; do
    n=$((n + 1))
    got="$(psqlx -d restore_check -c "SELECT count(*) FROM ${name}" 2>&1)" || got="error: ${got}"
    printf '%s\t%s\n' "${name}" "${got}" >>"${OUT}/rows-restore.tsv"
    if [ "${got}" != "${want}" ]; then
      bad=$((bad + 1))
      printf '  rows %s: dump %s, restore %s\n' "${name}" "${want}" "${got}"
    fi
  done <"${OUT}/rows-dump.tsv"
  if [ "${n}" -eq 0 ]; then
    fail "rows: the dump holds no table data"
  elif [ "${bad}" -eq 0 ]; then
    pass "rows: ${n} tables, every count(*) equals the dump's COPY rows (${OUT}/rows-dump.tsv)"
  else
    fail "rows: ${bad} of ${n} tables differ from the dump"
  fi
}

printf 'pg_restore_test: dump=%s bytes=%s run=%s out=%s\n' "${DUMP_ABS}" "$(wc -c <"${DUMP_ABS}" | tr -d ' ')" "${TAG}" "${OUT}"

# sha256
sum="$(sha256sum "${DUMP_ABS}" | cut -d ' ' -f 1)"
if [ -f "${DUMP_ABS}.sha256" ]; then
  if [ "$(cut -d ' ' -f 1 "${DUMP_ABS}.sha256")" = "${sum}" ]; then
    pass "sha256: ${sum:0:12} matches $(basename "${DUMP_ABS}").sha256"
  else
    fail "sha256: ${sum:0:12} does not match $(basename "${DUMP_ABS}").sha256"
  fi
else
  pass "sha256: ${sum:0:12} (no .sha256 file next to the dump; nothing to compare)"
fi

# scratch server
docker network create "${NET}" >/dev/null || die "docker network create failed"
POSTGRES_PASSWORD="${PGRT_PASSWORD}" docker run -d --name "${PG}" --network "${NET}" -p 127.0.0.1::5432 -e POSTGRES_PASSWORD \
  -v "${DUMP_ABS}:/in/dump.dump:ro" "${PG_IMAGE}" >/dev/null || die "the scratch PostgreSQL did not start"
# The image's init server listens on its socket only, then restarts; wait for TCP plus a real query.
ready=0
for _ in $(seq 1 90); do
  if PGPASSWORD="${PGRT_PASSWORD}" docker exec -e PGPASSWORD "${PG}" psql -X -q -h 127.0.0.1 -U postgres -At -c 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
[ "${ready}" -eq 1 ] || { docker logs "${PG}" 2>&1 | tail -20 | redact >&2; die "the scratch PostgreSQL accepted no connection in 180 s"; }
printf 'scratch: %s on 127.0.0.1:%s\n' "${PG}" "$(sed -n '1s/.*://p' <<<"$(docker port "${PG}" 5432/tcp)")"

# restore
for db in restore_check reference_check; do
  psqlx -d postgres -c "CREATE DATABASE ${db}" || die "could not create the scratch database ${db}"
done
if docker exec "${PG}" pg_restore --no-owner --no-acl --exit-on-error -U postgres --dbname=restore_check /in/dump.dump \
  >"${OUT}/restore.log" 2>&1; then
  pass "restore: pg_restore --no-owner --no-acl into restore_check"
else
  fail "restore: pg_restore failed: $(tail -3 "${OUT}/restore.log" | tr '\n' ' ')"
  finish
fi

# stamp
psqlx -d restore_check -c 'SELECT version_num FROM public.alembic_version ORDER BY 1' >"${OUT}/stamp-restore.txt" 2>&1 || true
if grep -q . "${OUT}/stamp-restore.txt" && ! grep -q ERROR "${OUT}/stamp-restore.txt"; then
  pass "stamp: alembic_version = $(tr '\n' ' ' <"${OUT}/stamp-restore.txt")"
else
  fail "stamp: restore_check has no alembic_version revision ($(head -1 "${OUT}/stamp-restore.txt"))"
  finish
fi

# reference
if dho_upgrade "${DHO_IMAGE}" reference_check "${OUT}/reference-upgrade.log"; then
  psqlx -d reference_check -c 'SELECT version_num FROM public.alembic_version ORDER BY 1' >"${OUT}/stamp-reference.txt"
  if cmp -s "${OUT}/stamp-reference.txt" "${OUT}/stamp-restore.txt"; then
    pass "reference: ${DHO_IMAGE##*@} builds an empty database to the dump's stamp"
  else
    fail "reference: DHO_IMAGE head is $(tr '\n' ' ' <"${OUT}/stamp-reference.txt"), the dump's stamp is $(tr '\n' ' ' <"${OUT}/stamp-restore.txt"); pass the dho image the release ran when the dump was taken"
    finish
  fi
else
  fail "reference: dho migrate postgres upgrade on an empty database failed: $(tail -2 "${OUT}/reference-upgrade.log" | tr '\n' ' ')"
  finish
fi

# schema (CHAOS-4467)
docker exec "${PG}" pg_dump -U postgres --schema-only reference_check >"${OUT}/schema-reference.sql" 2>&1
docker exec "${PG}" pg_dump -U postgres --schema-only restore_check >"${OUT}/schema-restore.sql" 2>&1
if schema_lines reference_check | LC_ALL=C sort -u >"${OUT}/schema-reference.txt" &&
  schema_lines restore_check | LC_ALL=C sort -u >"${OUT}/schema-restore.txt"; then
  LC_ALL=C comm -23 "${OUT}/schema-reference.txt" "${OUT}/schema-restore.txt" >"${OUT}/schema-missing.txt"
  missing="$(grep -c . "${OUT}/schema-missing.txt")"
  if [ "${missing}" -eq 0 ]; then
    pass "schema: all $(grep -c . "${OUT}/schema-reference.txt") reference relations, columns and indexes exist in restore_check"
  else
    fail "schema: ${missing} reference objects are missing from restore_check although alembic_version claims them (${OUT}/schema-missing.txt):"
    head -20 "${OUT}/schema-missing.txt" | sed 's/^/  missing /'
  fi
else
  fail "schema: could not read the catalogs"
fi

# rows
rows_check

# migrate
if dho_upgrade "${DHO_HEAD_IMAGE}" restore_check "${OUT}/restore-upgrade.log"; then
  pass "migrate: ${DHO_HEAD_IMAGE##*@} migrate postgres upgrade on restore_check; alembic_version now $(psqlx -d restore_check -c 'SELECT version_num FROM public.alembic_version ORDER BY 1' | tr '\n' ' ')"
else
  fail "migrate: dho migrate postgres upgrade on restore_check failed: $(tail -2 "${OUT}/restore-upgrade.log" | tr '\n' ' ')"
fi

# analyze
if [ "$(psqlx -d restore_check -c "SELECT to_regclass('river.river_job') IS NOT NULL")" = "t" ]; then
  if psqlx -d restore_check -c 'ANALYZE river.river_job'; then
    pass "analyze: ANALYZE river.river_job"
  else
    fail "analyze: ANALYZE river.river_job failed"
  fi
else
  pass "analyze: no river.river_job in the dump; nothing to analyze"
fi

finish
