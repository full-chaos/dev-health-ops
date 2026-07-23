# Canary release proof (CHAOS-3053)

`scripts/worker/canary_release_proof.py` is a default-off evaluator. It does
not start workers, change a route, scale Compose, or write to either database.
The caller first runs Celery and Go against the same bounded dataset and run
scope, captures redacted observations, and then invokes:

```bash
python scripts/worker/canary_release_proof.py \
  --celery-observation /secure/celery.json \
  --go-observation /secure/go.json \
  --output docs/architecture/evidence/go-worker-migration/v3-canary-release-proof/candidate.json
```

Each observation has exact, redacted digest fields for input/output/state and
idempotency, an authoritative `worker_job_routes` snapshot, lag/error/resource
values, and the actual rollback quiescence predicates. The route chain is
Celery `G` → River canary `G+1` → restored Celery `G+2`; the two observations
must carry the same completed rollback operation/evidence record. That record
binds kind, source and restored route, zero pending/claimed outbox and running
semantic/River jobs, passed external quiescence, and observed post-rollback
Celery service. The evaluator loads only the checked-in job registry and
migration state (recording their SHA-256 digests); arbitrary replacements fail
closed. `investment.dispatch` and every route without that checked-in
capability fail closed.

The artifact contains hashed scope fingerprints and build revisions/digests,
not payloads, IDs, tenant identifiers, DSNs, or credentials. The checked-in v3
threshold document is deliberately unapproved because the v0 baseline has
`parity_thresholds_approved=false`, is not authoritative for canary use, has
unresolved observability gaps, and keeps its production-canary gate blocked.
It defines absolute error ceilings as well as deltas, and zero baseline CPU or
memory allows only zero Go use. A local run records its measurements but reports
`thresholds_unapproved`; it cannot make a lag/error/resource threshold proof.
On any failed rerun the evaluator removes only the requested output file, so a
stale passing candidate cannot survive. Every local artifact remains
explicitly release-ineligible. Two independently attested, stable production
releases, reviewed thresholds, route quiescence, and a completed rollback
remain required external evidence; do not fill them in by hand or treat a
local result as production proof.
