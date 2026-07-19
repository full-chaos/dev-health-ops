---
page_id: int-source
summary: Register one Customer Push source and create a source-bound write credential.
content_type: task-guide
owner: platform-api
source_of_truth:
  - current /api/v1/admin/customer-push source and token routes
  - docs/customer-push-ingestion/setup-guide.md
applicability: current
lifecycle: active
---

# Register a source and credential

Registration and token management require an authenticated organization administrator.

1. Register the intended `system` and `instance` through the supported administration UI or `/api/v1/admin/customer-push/sources` route.
2. Resolve any active managed-sync ownership conflict before continuing.
3. Create a source-bound token with only the required scopes, commonly `ingest:write` and `ingest:status`.
4. Store the plaintext `fcpush_…` token immediately; it is returned only at creation or rotation.
5. Record the exact registered instance string. Accept-time token/source matching is case-sensitive.
6. Validate a sample payload before the first write.

Rotation is an immediate cutover: the old token stops working when the replacement is issued. Update the secret consumer and verify it before broad submission.
