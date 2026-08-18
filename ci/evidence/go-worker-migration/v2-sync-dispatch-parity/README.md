# v2 sync-dispatch parity

This artifact records one sanitized local comparison of the Python and Go
sync-dispatch observers. The coordinator opened one read-only PostgreSQL
`REPEATABLE READ` transaction, exported its snapshot, imported that snapshot
in Python, and ran the Go observer through the exporting transaction. Both
readers used the same database-clock cutoff and bounded limit.

The comparison includes only the versioned candidate digest and aggregate
fields. Candidate IDs are inputs to the digest but are never printed or stored.
Database URIs, snapshot IDs, tenant data, payloads, claim tokens, and driver
errors are excluded.

The versioned candidate predicate mirrors the active production Celery claim
window: pending rows due at the cutoff, with no live claim, whose kind has a
persisted, unpaused `celery` transport route. Paused kinds, River-routed kinds,
and kinds without a persisted route are excluded before ordering and limiting.
The predicate version is `sync_dispatch_active_celery_due_v2`; the digest byte
framing is unchanged at `sync_dispatch_candidate_digest_v1`.

The comparison was produced by `cmd/dev-health-sync-parity` together with
`scripts/worker/observe_sync_dispatch_parity.py`, run from the repository root
with `POSTGRES_URI` or `DATABASE_URI` set:

```bash
go run ./cmd/dev-health-sync-parity --limit 100
```

**Both were removed under CHAOS-3875** once this capture was taken; the
one-shot oracle had served its purpose and is no longer reproducible from the
working tree. Recover them from history at the commit recorded in
[`capture.json`](capture.json) if the comparison ever needs to be re-run.

The command exited nonzero on configuration failure, observation failure, or a
mismatch. A `match` proves that the two read-only implementations observed the
same bounded candidate window under this protocol. It does not prove claim,
publish, handler, scheduler, performance, or production-canary parity.

[`capture.json`](capture.json) records the safe result from the current local
production-equivalent dataset. It is comparison-harness evidence only; the
worktree contained the implementation under review, so it is not a
release-artifact attestation.
