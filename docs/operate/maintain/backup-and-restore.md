---
page_id: op-backup
summary: Back up and restore data, configuration references, and required cryptographic material without copying live secrets into documentation.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Back up and restore

Define the recovery point and time objectives, then cover:

- primary and analytical data stores;
- migration and schema state;
- configuration and deployment manifests;
- encrypted secret-manager records and key dependencies;
- queue or job state when required for safe recovery;
- external provider installation identifiers;
- retention, encryption, access, and deletion.

Test restore in isolation. Verify schema, API health, worker progress, source coverage, and a representative product query before declaring recovery complete.

## Local standing-stack backup (CHAOS-4084)

For the local/dev standing stack (the `compose.yml` Postgres and ClickHouse
containers most day-to-day work runs against), use
[`scripts/backup-standing.sh`](https://github.com/full-chaos/dev-health-ops/blob/main/scripts/backup-standing.sh)
rather than reconstructing the recipe by hand:

```bash
scripts/backup-standing.sh
```

This writes a timestamped directory under `dev-health/backups/<ts>/`
(sibling to this repo, one level above `ops/`) containing:

- `postgres-all-<ts>.sql.gz` — `pg_dumpall`, gzip-compressed, plus a
  `postgres-dumpall-<ts>.log` capturing `pg_dumpall`'s own stderr;
- `clickhouse-<db>-<ts>.zip` — one native `BACKUP DATABASE <db> TO
  File(...)` per ClickHouse database (`system`/`information_schema`
  excluded), copied out of the container and cleaned up container-side;
- `SHA256SUMS` — a digest of every artifact in the run, so a caller can
  pin or compare a specific backup without re-downloading it.

The script verifies every artifact after writing it (a zero exit code
alone is never proof a backup is usable): `gzip -t` plus the `pg_dumpall`
header/footer plus a `CREATE DATABASE` count reconciled against the live
cluster's own non-template database count; `unzip -t` per ClickHouse zip.
A verification failure exits nonzero and names exactly what failed.

It refuses to write into an existing, non-empty output directory unless
`--force` is passed — every artifact name already carries its own run
timestamp, so `--force` never overwrites a prior run's files; it only
lifts the guard against writing into a directory that unexpectedly
already has content (a stale `--out-dir`, or a same-second re-run). Run
`scripts/backup-standing.sh --help` for the full option/environment-variable
reference (container names, credentials, and the output root are all
overridable; defaults match `compose.yml`/`ops/.env`).

**Purpose and posture (per chris, 2026-08-20/23):** these backups exist
primarily to FRONT-LOAD test/trial environments with realistic captured
state instead of building fixtures from scratch, and secondarily as
disaster recovery if a local Docker VM resize or restart clobbers the
standing stack. Restoring from an existing dump is not a substitute for
waiting out a live measurement run that depends on the standing stack, and
an old dump can predate schema/data writes a newer restore target expects
— check the timestamp against what has been written since before
restoring, and never compare measurement runs taken against two different
restored datasets as though they were the same baseline.

Restore is manual (not yet scripted, see CHAOS-4091 for a future
ephemeral-cluster snapshot-restore path). **Restore into an isolated
target (a scratch/ephemeral stack), never back onto the live standing
stack** — matches the general "Test restore in isolation" guidance above
and the isolation posture in
[databases-and-storage.md](../configure/databases-and-storage.md).

Postgres (against the `compose.yml` container, host/port/credentials
default to `localhost:5432`, `POSTGRES_USER`/`POSTGRES_PASSWORD` from
`ops/.env` — `devhealth`/`devhealth` unless overridden):

```bash
gunzip -c postgres-all-<ts>.sql.gz \
  | PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -p 5432 \
      -U "$POSTGRES_USER" -d postgres --set ON_ERROR_STOP=1
```

`--set ON_ERROR_STOP=1` is required — without it `psql` keeps going past a
failed statement and a partial, silently-corrupt restore looks identical
to a clean one in the output.

ClickHouse (against the `compose.yml` container,
`CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` from `ops/.env` — `ch`/`ch` unless
overridden), per database:

```bash
docker cp clickhouse-<db>-<ts>.zip \
  dev-health-clickhouse-1:/var/lib/clickhouse/backups/<db>-<ts>.zip
docker exec dev-health-clickhouse-1 clickhouse-client \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  -q "RESTORE DATABASE \`<db>\` FROM File('<db>-<ts>.zip')"
```

Verify with a real query per store, not by inference — see "Test restore
in isolation" above.
