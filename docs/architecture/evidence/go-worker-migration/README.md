# Go worker migration evidence

This directory holds sanitized, versioned evidence for the Celery-to-Go worker
migration. Evidence versions describe the comparison protocol, not a product or
database schema version.

| Version | Purpose | Current state |
|---|---|---|
| [`v0-celery-baseline`](v0-celery-baseline/README.md) | Capture the current Celery reliability, latency, resource, and deploy baseline | Local one-shot resources and an instrumentation gap are recorded; production values are not yet recorded |
| [`v1-river-spike`](v1-river-spike/README.md) | Record River, PgBouncer, Python enqueue, version, and licensing compatibility | Harness complete: direct PostgreSQL GO; session mode unverified; PollOnly-only and Python client NO-GO; N/N-1, load, interop, and crash evidence recorded |

## Evidence rules

- Commit authored summaries, query text, exact non-secret version pins, and
  structured results only.
- Do not commit raw logs, database dumps, DSNs, access tokens, organization or
  repository identifiers, payload bodies, or unredacted observability exports.
- Every result identifies whether it came from a local harness, a
  production-like environment, or production. Local evidence is never promoted
  to production evidence by inference.
- An empty query result is recorded as `missing`, not as a numeric zero.
- Corrections may amend an existing artifact with an explanation. A changed
  measurement contract creates the next evidence-version directory.

Phase 1 foundation work has a conditional GO with direct PostgreSQL queue
control as a hard prerequisite, a session-mode endpoint that separately passes
the same matrix, or a separately verified cancellation plane that closes the
PollOnly blocker. The first production canary remains blocked until the v0
production capture is populated and reviewed.
