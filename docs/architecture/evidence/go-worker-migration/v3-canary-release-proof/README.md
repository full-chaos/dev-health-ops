# v3 canary release proof

This version defines a fail-closed local or preproduction comparison protocol;
it is not a release attestation. Observations use
[`observation.schema.json`](observation.schema.json), and candidate output uses
[`artifact.schema.json`](artifact.schema.json).

The evaluator pins its registry, migration state, baseline, and this
[`parity-thresholds.json`](parity-thresholds.json) document from the checked-in
repository root. Its candidate output records the SHA-256 digest of every
document it actually evaluated. Command-line replacements are rejected.

Route evidence must originate from `worker_job_routes`: the Celery baseline is
generation `G`, the River canary is `G+1`, and a successful rollback restores
Celery at `G+2`. One shared rollback operation/evidence record is present in
both observations. It binds the kind, source and restored routes, completed
and successful state, zero outbox/semantic/River work, a passed external
quiescer, and post-rollback Celery service observation. It is corroboration of
one operation, not a second, self-asserted Celery rollback.

The threshold values are intentionally **not approved**. They include bounded
lag deltas, absolute Celery and Go error ceilings, and resource multipliers;
a zero Celery resource baseline accepts only a zero Go value. The v0 baseline's
`parity_thresholds_approved` and `authoritative_for_canary` are false, it has
unresolved observability gaps, and its production-canary gate is blocked. The
evaluator therefore records comparison measurements but returns
`thresholds_unapproved` and makes no lag/error/resource threshold proof. It
will continue to do so until both the baseline and this document contain
explicit review and approval and the baseline is authoritative, gap-free, and
canary-ready.

No local artifact can satisfy CHAOS-3053 acceptance. Two independently
attested stable production releases, with reviewed thresholds and the durable
route-control evidence, remain required. Do not add a candidate artifact by
hand or infer its release status from a passing comparison.
