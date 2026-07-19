---
page_id: int-test
summary: Test an integration with isolated fixtures, live-like permissions, deterministic replay, and failure evidence.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Test an integration

Cover:

- minimum and denied permissions;
- source discovery and pagination;
- incremental and backfill windows;
- duplicate delivery and idempotency;
- missing, malformed, deleted, and unsupported records;
- provider rate limits and outages;
- queue, worker, and sink failure;
- tenant isolation;
- reconciliation and coverage reporting;
- credential rotation and revocation.

Use a dedicated test organization and source. Never mix synthetic fixture rows into a live customer organization without an explicit safe test plan.
