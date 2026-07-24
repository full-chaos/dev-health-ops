# Go worker migration evidence

This directory holds sanitized, versioned evidence for the Celery-to-Go worker
migration. Evidence versions describe the comparison protocol, not a product or
database schema version.

| Version | Purpose | Current state |
|---|---|---|
| [`v0-celery-baseline`](v0-celery-baseline/README.md) | Capture the current Celery reliability, latency, resource, and deploy baseline | Five-minute production-equivalent local capture recorded; production-canary authority remains false |
| [`v1-river-spike`](v1-river-spike/README.md) | Record River, PgBouncer, Python enqueue, version, and licensing compatibility | Harness complete: direct PostgreSQL GO; session mode unverified; PollOnly-only and Python client NO-GO; N/N-1, load, interop, and crash evidence recorded |
| [`v2-sync-dispatch-parity`](v2-sync-dispatch-parity/README.md) | Compare Python and Go sync-dispatch observations at one exported PostgreSQL snapshot | Local production-equivalent dataset matched; mutation and canary authority remain false |
| [`v3-canary-release-proof`](v3-canary-release-proof/README.md) | Define the pinned, redacted canary comparison and rollback evidence protocol | Thresholds and production attestation are unapproved; no CHAOS-3053 acceptance |

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

Phase 1 foundation work is complete with direct PostgreSQL queue control as a
hard prerequisite. A session-mode endpoint still requires the same matrix, and
PollOnly cannot become the sole queue-control path without a separately
verified cancellation plane. The first production canary remains blocked
until observability gaps and promotion thresholds in the v0 capture are
reviewed.
