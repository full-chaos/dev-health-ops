---
page_id: int-delivery-fail
summary: Recover transient delivery, processing, provider-rate, stream, queue, or sink failures without duplicate writes.
content_type: troubleshooting
owner: platform-api
applicability: current
lifecycle: active
---

# Delivery, retry, and rate-limit problems

1. Identify the durable acceptance boundary and current status.
2. Retry Customer Push with the same idempotency key and unchanged payload only for a supported recoverable state.
3. Respect provider retry guidance and budget signals.
4. Stop concurrent replay or retry amplification.
5. Check stream, queue, worker, sink, and bounded recompute progress.
6. Verify downstream coverage before closing.

Escalate platform-level queue or storage failure to operations with identifiers and sanitized errors.
