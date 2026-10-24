---
page_id: int-webhooks
summary: Configure signed provider webhooks for Full Chaos-managed synchronization.
content_type: landing
owner: platform-api
source_of_truth:
  - current webhook routes and handlers
applicability: current
lifecycle: active
---

# Use webhooks

Provider webhooks notify the managed synchronization path. They are not Customer Push batches.

- [Configure a webhook](configure.md)
- [Verify signatures](verify-signatures.md)
- [Handle retries and replay](retries-and-replay.md)

Current provider routes are `/api/v1/webhooks/github` and `/api/v1/webhooks/pagerduty/{binding_id}`. GitLab and Jira have no webhook route; use the Customer Push API or the supported provider connection for them. SaaS billing webhooks are an operator/billing concern and are not part of this integration guide.
