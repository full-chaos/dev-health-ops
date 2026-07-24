---
page_id: ref-api-errors
summary: Common response classes and the rules for bounded result and retry handling.
content_type: api-reference
owner: platform-api
applicability: current
lifecycle: active
---

# Errors, pagination, and rate limits

## Response classes

- `400`: invalid request, schema, enum, field, or batch limit.
- `401`: missing or invalid authentication.
- `403`: authenticated but unauthorized, wrong scope, source mismatch, or ownership conflict.
- `404`: unknown supported resource or schema version.
- `409`: state or idempotency conflict.
- `413`: request body exceeds the supported limit.
- `429`: rate or cost limit.
- `5xx`: transient platform, dependency, stream, queue, or storage failure unless the endpoint documents a terminal state.

Use the structured error code and request or ingestion identifier when provided; do not parse prose messages as a contract.

## Pagination and limits

Use endpoint or schema discovery for current maximum date range, bucket, top-N, node, edge, record, and body limits. Preserve cursors or continuation state exactly. Bound concurrency and retry with jitter, and stop retrying non-transient `4xx` responses.
