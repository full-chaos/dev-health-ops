---
page_id: int-submit
summary: Validate, submit, and poll a bounded Customer Push batch.
content_type: task-guide
owner: platform-api
source_of_truth:
  - current external-ingest REST routes
  - docs/customer-push-ingestion/examples.md
  - docs/customer-push-ingestion/ci-cd.md
applicability: current
lifecycle: active
---

# Submit records

1. Build one `external-ingest.v1` envelope for one registered source instance.
2. Include a stable idempotency key, optional time window, and between 1 and the current server-reported maximum records.
3. Call `POST /api/v1/external-ingest/validate` before production submission.
4. Submit to `POST /api/v1/external-ingest/batches` with the source-bound bearer token.
5. Retain the returned `ingestionId`.
6. Poll `GET /api/v1/external-ingest/batches/{ingestionId}` until `completed`, `partial`, or `failed`.
7. Inspect per-record errors and bounded recompute state before declaring the data available.

The CLI equivalent is `dev-hops push batch <file> --poll`. Do not log the bearer token or full customer payload.
