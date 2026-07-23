---
page_id: int-webhooks
summary: Configure signed provider webhooks for Full Chaos-managed synchronization.
content_type: landing
owner: platform-api
source_of_truth:
  - current webhook routes and handlers
  - docs/webhooks.md
applicability: current
lifecycle: active
---

# Use webhooks

Provider webhooks notify the managed synchronization path. They are not Customer Push batches.

- [Configure a webhook](configure.md)
- [Verify signatures](verify-signatures.md)
- [Handle retries and replay](retries-and-replay.md)

Current provider routes include `/api/v1/webhooks/github`, `/api/v1/webhooks/gitlab`, and `/api/v1/webhooks/jira`. SaaS billing webhooks are an operator/billing concern and are not part of this integration guide.
