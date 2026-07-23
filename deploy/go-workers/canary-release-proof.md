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
idempotency, route generation, lag/error/resource values, quiescence counts,
and a completed rollback observation. The evaluator checks the checked-in job
registry plus migration state; only an executable `river_canary` route with
Celery rollback is eligible. `investment.dispatch` and every route without
that checked-in capability fail closed.

The artifact contains hashed scope fingerprints and build revisions/digests,
not payloads, IDs, tenant identifiers, DSNs, or credentials. A passing local
artifact remains explicitly release-ineligible. Two independently attested,
stable production releases, reviewed thresholds, route quiescence, and a
completed rollback remain required external evidence; do not fill them in by
hand or treat a local result as production proof.
