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
- `SHA256SUMS-<ts>` — a digest of every artifact THIS run produced, so a
  caller can pin or compare a specific backup without re-downloading it
  (named per-run, not a bare `SHA256SUMS`, so `--force` into a directory
  that already has a prior run's files never overwrites that prior run's
  own manifest).

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

Both snippets below are self-contained — they source `ops/.env` for
credentials rather than assuming the values are already in your shell
(they will not be, in a clean shell, and both commands would otherwise
silently run with empty credentials, since `source`-ing a missing/wrong
path is not fail-fast). **Run these from inside the backup directory
itself** (`dev-health/backups/<ts>/`, so the bare `<ts>`/`<db>` filenames
below resolve directly) — `../../ops/.env` from there is
`dev-health/ops/.env`, the real credentials file:

Postgres (against the `compose.yml` container at `localhost:5432`):

```bash
set -a; source ../../ops/.env; set +a  # POSTGRES_USER, POSTGRES_PASSWORD
set -o pipefail
gunzip -c postgres-all-<ts>.sql.gz \
  | PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -p 5432 \
      -U "$POSTGRES_USER" -d postgres --set ON_ERROR_STOP=1
```

`--set ON_ERROR_STOP=1` and `set -o pipefail` are both required — without
`ON_ERROR_STOP`, `psql` keeps going past a failed statement; without
`pipefail`, a truncated/corrupt `.gz` file makes `gunzip` fail but still
feeds `psql` whatever partial SQL prefix it managed to decompress, which
can exit 0 having silently restored only part of the dump.

ClickHouse (against the `compose.yml` container), per database:

```bash
set -a; source ../../ops/.env; set +a  # CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
docker cp clickhouse-<db>-<ts>.zip \
  dev-health-clickhouse-1:/var/lib/clickhouse/backups/<db>-<ts>.zip
docker exec dev-health-clickhouse-1 clickhouse-client \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  -q "RESTORE DATABASE \`<db>\` FROM File('<db>-<ts>.zip')"
```

Verify with a real query per store, not by inference — see "Test restore
in isolation" above.

## After any Postgres restore: ANALYZE `river.river_job` (CHAOS-5615)

A restore leaves Postgres's planner statistics stale for the restored
tables, `river.river_job` included. On 2026-09-11, a restore left 11,428
`available` `metrics` rows queued; the Go worker's queued-contract-version
startup check queries this table on every restart, and against stale
statistics it chose a plan that missed its own timeout budget, aborting
readiness (13 restarts on the affected deployment) until `ANALYZE
river.river_job` and a wider `--health-check-timeout` were applied by
hand. Run

```sql
ANALYZE river.river_job;
```

against the queue-control database immediately after restoring it,
before restarting the Go worker/reconciler/scheduler fleet — this is
cheap (a statistics-only pass, no table rewrite or lock beyond a normal
`ANALYZE`) and is not optional when the restored table has a
nontrivial backlog.

## Auth schema backup, restore and PITR drill (CHAOS-11232)

`scripts/drill-auth-pitr.sh` rehearses backup, restore and point-in-time
recovery of the `auth` schema on a throwaway compose project
(`drill-11232`, one Postgres 18 container at a time, bind mounts only,
teardown is `compose down` with no volume removal). It never touches the
standing stack and never runs against production. The driver is
`TestDrillAuthPITR` (`internal/audit/drill_pitr_integration_test.go`,
build tag `integration`, skipped unless `DRILL_PHASE` is set); every write
goes through `audit.Commit` as the runtime role.

Flow: migrate, write A, `pg_basebackup`, write B, snapshot T (time and
state), write C (includes revocations), snapshot F, archive the WAL. Then
two copies: PITR to T (`recovery_target_time`) and restore to the end of the
WAL. Run with `DRILL_DIR=<new dir> scripts/drill-auth-pitr.sh`; evidence
(snapshots, verify and post JSON, `summary.txt`) lands in `$DRILL_DIR/evidence`.

Checks and result line (`DRILL RESULT: PASS|GAP|FAIL`):

- FAIL when a credential revoked at or before T is not revoked on the copy,
  when any revision is below its value at T, or when a duplicate
  (aggregate type, aggregate id, revision) key exists after new writes.
- GAP when the copy is correct as of T but later work is gone: revocations
  made after T that are no longer revoked, or outbox keys that the restored
  timeline re-issues with different content than the pre-restore timeline
  published.
- PASS when all of those counts are zero.

Result of the 2026-10-24 run on the dev stack: restore to the end of the WAL
= PASS (state identical to the source, digests equal). PITR to T = GAP:
revisions never went below T and no duplicate key appeared, new writes
continued at exactly revision + 1, and every credential revoked before T
stayed revoked, but PITR dropped 30 revisions published after T, 2
revocations made after T came back (credentials usable again), and 6
(type, id, revision) keys were re-issued with different content.

What this means for an operator: a PITR to an earlier time is a rollback of
the auth plane. Treat every credential revoked after the target time as
live again and revoke it again, and tell downstream consumers that
revisions above the restored value will be re-issued. The fix that makes
this safe (a restore fence that moves revision counters forward and keeps
revocations) is tracked in CHAOS-11271; until it lands, prefer restore to
the end of the WAL, and never PITR past a revocation you cannot redo.

Interim rule (D6323, CHAOS-11271), verbatim: "until 11271 rolls, no PITR of the auth/identity DB to any point before end of WAL without acp attest + chris's word; end-of-WAL restore is safe."
