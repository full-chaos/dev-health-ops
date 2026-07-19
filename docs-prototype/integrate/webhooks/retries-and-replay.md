---
page_id: int-wh-retry
summary: Handle provider redelivery and internal retries without duplicate side effects.
content_type: task-guide
owner: platform-api
applicability: current
lifecycle: active
---

# Handle retries and replay

1. Retain the provider delivery identifier and event type.
2. Verify the signature on every delivery, including replay.
3. Deduplicate through the current idempotency store before dispatching side effects.
4. Return the provider-appropriate success response only after the handler's durable acceptance boundary.
5. Retry transient internal failures with bounded backoff.
6. Reconcile repeated or missing deliveries with the managed synchronization path.

Do not use Customer Push idempotency semantics for provider webhook events; each surface has its own contract.
