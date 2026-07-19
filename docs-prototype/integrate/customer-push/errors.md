---
page_id: int-errors
summary: Separate envelope rejection, per-record rejection, ownership, idempotency, stream, and processing failures.
content_type: troubleshooting
owner: platform-api
applicability: current
lifecycle: active
---

# Handle validation and delivery errors

## Whole-request failures

Check schema version, batch size, body size, authentication scope, exact source binding, source ownership, and idempotency conflict.

## Per-record failures

A `partial` result can contain accepted and rejected records. Use each record's external ID and error code to correct the source export. Common classes include unknown kind, forbidden extra field, invalid enum or timestamp, unsupported kind for system, and record outside the registered source instance.

## Delivery failures

`stream_unavailable` is recoverable because the accepted payload is already durable. Retry the same payload with the same key. Repeated processing failures require the batch ID, attempt, worker status, sanitized error summary, and source details.

See [Integration troubleshooting](../troubleshooting/index.md) for authentication, validation, and rate-limit recovery.
