---
page_id: con-debug
summary: Debug a request or job with correlated, tenant-safe, redacted evidence.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Debug and observe development environments

1. Reproduce in the smallest isolated environment.
2. Retain route or command, organization, source, scope, time, request or job ID, and revision.
3. Inspect API, worker, queue, provider, and store signals along the same path.
4. Confirm tenant scope and latest-row or idempotency behavior.
5. Reduce to a deterministic fixture or failing test where possible.
6. Redact credentials, authorization headers, signed URLs, and sensitive payloads.
7. Verify the fix against the original evidence and a regression case.
