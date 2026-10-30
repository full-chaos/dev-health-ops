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
goes through `audit.Commit` as the runtime role, except seeding one principal as the database owner.

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
= PASS (row counts and the compared digests equal to the source). PITR to T = GAP:
revisions never went below T and no duplicate key appeared, new writes
continued at exactly revision + 1, and every credential revoked before T
stayed revoked, but PITR dropped 30 revisions published after T, 2
revocations made after T came back (credentials usable again), and 6
(type, id, revision) keys were re-issued with different content.

The same drill with `DRILL_FENCE=1` rehearses the restore fence (below): the source is initialised once
(`restore-fence init`), the end-of-WAL copy is restored first and `restore-fence export` runs against it, the PITR
copy starts read-only, `restore-fence status` says not fenced, `apply` runs as a dry run and then with `-apply`, the
read-only default is reset, and the post phase must show no revocation after T alive, no key re-issued with different
content, every aggregate continuing at `max(restored revision, floor) + 1` and exactly one `restore_fence_state` row
for the live timeline. Run it with `DRILL_FENCE=1 DRILL_DIR=<new dir> scripts/drill-auth-pitr.sh`; the result line
is `DRILL RESULT: PASS (b revokes-after-T-back=0, c reissued-different=0)`.

How `b` is counted: in the unfenced drill it is every credential revoked (or principal disabled) after T that is not revoked on
the PITR copy, absent rows included (a credential created and revoked after T does not exist on the copy). In the fenced drill
only rows that are present on the copy and still active count; the absent ones are reported separately
(`absent_after_restore` in `post-T.json`) and must equal what `apply` reports as absent: an absent row cannot authenticate.
The drill also revokes a credential and disables a principal that were created before T, so the `apply` must re-apply at
least one revocation (`revocations_reapplied`), which proves the re-apply path and not only the rows the copy lacks.

From CHAOS-11889 the drill also proves the effect, not only the markers. Before T it creates two workloads, each with a live
bootstrap credential; after T it revokes the credential of one and disables the principal of the other. On each restored copy
it then presents both secrets to `workload.Store.ResolveBootstrap`, the verifier the auth service uses at the workload-token
exchange, as the unprivileged runtime role: the end-of-WAL copy and the fenced PITR copy must refuse both (`revoked`,
`disabled`); on the unfenced PITR copy the drill records how many are still accepted, which is the gap itself
(`attempt-T-pitr.json`, `revoked_access_accepted`). The drill also creates one row in each of `public.refresh_tokens`,
`public.internal_service_credentials` and `public.external_ingest_tokens` before T and revokes it after T: those come back
unfenced (`legacy_revokes_after_target_that_came_back` is 3, and they count in `b`) and stay revoked after the fenced apply.
So the unfenced baseline `b` is larger than in the 2026-10-29 run below. The attempt secrets live in
`attempt-secrets.json` of the evidence directory (mode 0600, values made for the drill; nothing prints or passes them as an argument).

Result of the fenced drill (CHAOS-11271), 2026-10-29 on bigboy at e86f591c: `DRILL RESULT: PASS (b revokes-after-T-back=0,
c reissued-different=0)`; post-T.json keys_reissued_with_different_content=0, revokes_after_target_that_came_back=0,
absent_after_restore=2 (equal to the apply's absent count), fence_rows_for_live_timeline=1; the apply raised 6 floors and
re-applied 2 revocations. The unfenced baseline of the same tip: `DRILL RESULT: GAP (b revokes-after-T-back=4,
c reissued-different=6)`. Evidence: /var/lib/oci-cache/lane-scratch/fcdev/drill-11271b-d/evidence (fenced) and
drill-11271b-c/evidence (baseline).

Without the fence a PITR to an earlier time is a rollback of the auth plane: every credential revoked after the target
time is live again and revisions above the restored value are re-issued with other content. The fence below removes
both; follow it for every restore, promote or failover of the auth database.

### Restoring the auth schema to a point in time (the restore fence)

Order (nothing serves traffic until the last step):

1. Stop auth-service (scale it to zero). Nothing may write to the auth database while it is restored.
2. Restore the database to the target time (`recovery_target_time`) with `default_transaction_read_only = 'on'` in
   `postgresql.auto.conf` before it is promoted, so no client or job can write before the fence runs.
3. Restore a second, shadow copy of the same base backup to the END of the WAL archive (no recovery target) and keep it
   away from clients. It is the source of everything the target time dropped.
4. Export the manifest from the shadow copy with the migration role (the command refuses the runtime role):
   `auth-migrate restore-fence export -out manifest.json` (`AUTH_MIGRATION_DATABASE_URI`, `AUTH_RUNTIME_DATABASE_ROLE`
   and `AUTH_DATABASE_SCHEMA` in the environment; no secret on a command line). The manifest holds ids, states and
   revision floors, never a credential, a hash or a token.
5. Apply it to the restored copy: first the dry run, `auth-migrate restore-fence apply -manifest manifest.json` (it writes
   nothing and prints the counts per table), then `... apply -manifest manifest.json -apply`. The migration DSN of the
   apply carries `options='-c default_transaction_read_only=off'` so the apply can write although the copy is read-only.
   One transaction does everything: the revision floors, every revocation made after the target time, and the fence row
   for the new timeline. It is idempotent: a second run for the same timeline and manifest changes nothing.
   Set `RESTORE_FENCE_SOURCE_URI` (or `RESTORE_FENCE_SOURCE_URI_FILE`) to the shadow copy's migration DSN for both runs:
   the apply then copies the provider token revocations queued after the target time from the shadow copy into the
   restored copy, in the same transaction, as pending work. It refuses (and writes nothing) when the shadow copy is on
   another timeline than the export or lacks a revocation the manifest names. Environment only, never a flag. Without it
   the revocations are only named (`requeue_provider_revocation`).
6. Act on the `operator-action` lines the apply prints (ids only): users whose password may have changed after the
   target (their older password is live again: force a reset, ticket 11834); `provider_revocation_requeued`: with the
   PagerDuty client id configured on the API, disconnect or reconnect the credential of each named revocation once
   (`SELECT org_id, credential_name FROM public.provider_oauth_revocations WHERE id = ...`), which sends the queued
   revoke (a setup revoke is sent by any disconnect or callback of its org); `provider_revocation_requeued_blocked`:
   the credential was connected after the target, so the restored copy has no credential row and a disconnect does not
   send the revoke (it answers `disconnected: true` anyway); these wait for the revocation worker (the ticket the line
   names); `requeue_provider_revocation`: tokens the apply could only name (revocations with no source set, provider
   credentials it deleted): revoke them at the provider, then reconnect.
7. Reset the read-only default: `ALTER SYSTEM RESET default_transaction_read_only;` then `SELECT pg_reload_conf();`.
8. Start auth-service. Its readiness check `restore_fence` must be true (`/readyz`).
9. `auth-migrate restore-fence status` prints `fenced=true` and the timeline (and the stored operator actions).

What the apply puts back, so that nothing removed after the target time stays alive: sessions, refresh credentials,
credential registry, signing keys, resource grants, memberships, service accounts, workloads, workload bindings,
principals and organizations disabled, consumed link credentials and consumed refresh credentials, replaced link
credentials, a pending invite the source no longer holds pending (it ends revoked, never accepted), SAML replay evidence, every SSO flow state (users start the flow again),
and the legacy public tables: refresh tokens, service and ingest credentials, webhook bindings, invites, users (disabled,
token version, and a user the source deleted is disabled, never deleted), membership mirror rows, single-use reset and
verification tokens, and SAML replays. Revision floors are raised past everything the source reached, so no
`(aggregate, id, revision)` key is issued twice with different content.

Which command, when:

| Situation | Command |
|---|---|
| A new database that is not a restore (first roll of the fence, a new environment, a fresh dev stack) | `restore-fence init`, once per database timeline. Dev compose runs `init -if-absent` as the `auth-fence-init` one-shot; `-if-absent` is for compose and dev only, never on a restore and never in production automation. On a timeline above 1 it refuses unless `-timeline N` names the live timeline. |
| PITR, promote, failover, a restore from a base backup | `export` from the shadow end-of-WAL copy, then `apply` (dry run first). A planned promote that lost nothing makes `apply` a no-op that opens readiness. |
| A logical restore (pg_dump and pg_restore into a new cluster) | The probe cannot see it (a new cluster starts at timeline 1). Run the fence by hand before traffic: `export` against the source (or its shadow restore), then `apply` and `apply -apply` on the new cluster. Never `init`. |
| A restore of a backup taken before the fence migration existed | `apply`, never `init`. |
| A restored volume that is on timeline 1 (a file copy of an old volume) | `apply`, never `init`: init cannot detect it. |
| The WAL after the target time is gone (no shadow copy possible) | `apply -blind-margin N` (instead of `-manifest`). It raises every revision by N, which consumers read as a gap, and it is fail closed: it removes EVERY legacy reset and verification token, EVERY unconsumed link credential and EVERY pending invite (users ask for a new link, admins re-invite) because there is no proof of what was consumed after the target. The apply prints the counts. It cannot redo revocations: redo them by hand. |

Known gaps (follow-up 11878 and the tickets it names): an unlink of an external identity, login lockout counters
(11848), a revoked platform role assignment (11846) and the legacy organization, IP allowlist and integration flags
(11872) are not carried; and the legacy invite table, the margin-specific blind-margin idempotency and a strict
manifest parse are open (11878). Until those ship, redo such changes by hand after a restore and check them.

First roll of the fence (D6466), in this order: (1) the auth-migrate Job (migration 0035), (2) a one-off
`restore-fence init` Job against the live database (it writes the first fence row for the live timeline; run it once
and read `status`), (3) the auth-service image. If the image lands before the init, the new pods stay not ready and
the old pods keep serving; that is fail closed.

Interim rule (D6323, CHAOS-11271), verbatim: "until 11271 rolls, no PITR of the auth/identity DB to any point before end of WAL without acp attest + chris's word; end-of-WAL restore is safe."
