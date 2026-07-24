---
page_id: int-observe
summary: Monitor batch status, rejection counts, attempts, stream state, bounded recompute, and product freshness.
content_type: task-guide
owner: platform-api
applicability: current
lifecycle: active
---

# Observe source delivery

Track, at minimum:

- source system and instance;
- idempotency key and ingestion ID;
- accepted, processing, completed, partial, failed, or recoverable stream state;
- items received, accepted, and rejected;
- attempts and last transition time;
- per-record error codes without sensitive payloads;
- bounded recompute status and job identifiers;
- downstream freshness or coverage confirmation.

Alert on sustained age or terminal failure, not every short retry. Do not treat `accepted` as completed ingestion.
