---
page_id: int-idempotency
summary: Reuse a Customer Push idempotency key only for the same normalized payload.
content_type: concept
owner: platform-api
source_of_truth:
  - current external-ingest idempotency implementation
applicability: current
lifecycle: active
---

# Use idempotency and retries

A batch is identified by organization, source system, source instance, and idempotency key. Keys do not expire.

| Outcome | Meaning |
| --- | --- |
| New | No batch exists for the key; the payload is accepted and queued. |
| Replay | The same key and canonical payload already exist; the current status is returned without duplicate processing. |
| Conflict | The key exists with a different payload; the original is preserved and the request is rejected. |
| Recoverable retry | The same key and payload are reused for a supported recoverable state. |

Generate a deterministic key from the source and export window. Retry network or recoverable delivery failures with the same key and unchanged payload. Use a new key when the logical batch changes.
