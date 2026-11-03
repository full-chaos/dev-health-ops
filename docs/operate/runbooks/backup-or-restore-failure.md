---
page_id: op-rb-backup
summary: Recover when a backup is incomplete, unreadable, unverified, or a restore does not produce a healthy platform.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Backup or restore failure

1. Preserve the failed artifact, logs, manifest, encryption context, and recovery target.
2. Determine whether the failure is capture, transfer, retention, key access, integrity, compatibility, or restore verification.
3. Do not overwrite the last known good recovery copy.
4. Retry into an isolated target using the approved procedure.
5. Verify schema, data integrity, API, workers, source coverage, and representative queries.
6. Record the remaining recovery-point gap.

Escalate immediately when no verified recovery copy remains or encryption keys are unavailable.

## The auth database: `restore_fence` not ready, or `restore-fence apply` failed

After a restore, promote or failover of the auth database, auth-service stays not ready (readiness check `restore_fence`)
until `auth-migrate restore-fence apply` has run for the live timeline. This is by design (fail closed); do not bypass it.
The procedure is in [Restoring the auth schema to a point in time](../maintain/backup-and-restore.md#restoring-the-auth-schema-to-a-point-in-time-the-restore-fence).

1. `auth-migrate restore-fence status` (migration role): `fenced=false` means no apply has run for this timeline; `fenced=true` with a
   not-ready service means look at the service logs (`reason=fence_state_unreadable` is a failed read, `timeline_not_fenced` a missing row).
2. Apply failed: it rolls back whole and leaves readiness closed. Read the message (a table name and a SQLSTATE, never a value). A
   missing privilege is listed by table name; the migration role must be able to write the auth and the legacy public tables.
3. Re-run `export` from a FRESH shadow copy (the end-of-WAL restore of the same base backup) and apply again: apply is idempotent
   (the same timeline and manifest changes nothing), and a different manifest applies on top. Dry run first (no `-apply`).
4. The WAL after the target time is gone: `apply -blind-margin N` instead of `-manifest`. It raises every revision (consumers read a
   jump) and is fail closed: it removes every legacy reset and verification token, every unconsumed link credential and every pending
   invite. It does NOT redo revocations: redo each credential, session and grant revoked after the target time by hand.
5. Do not run `init` to open readiness on a restored database: it would mark the timeline fenced without applying any revocation
   (a restore of a pre-fence backup, a logical restore and a volume copied on timeline 1 all use `apply`).
6. Known gaps until follow-up 11878 ships (and tickets 11848, 11846, 11872): an external identity unlink, login lockout counters, a revoked
   platform role assignment, the legacy organization, IP allowlist and integration flags, and a legacy pending invite the source deleted
   are not carried; check and redo them by hand.
