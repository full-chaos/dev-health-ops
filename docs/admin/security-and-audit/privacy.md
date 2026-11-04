---
page_id: admin-privacy
summary: Review product telemetry and data-handling choices at the workspace boundary.
content_type: concept
owner: platform-product
source_of_truth:
  - current telemetry and privacy controls
applicability: current
lifecycle: active
---

# Privacy and data-handling responsibilities

Before enabling telemetry, model-assisted features, or a new data source, identify:

- data categories collected or transmitted;
- workspace and provider boundaries;
- retention and deletion behavior;
- access roles and intended audience;
- external processors or model providers;
- whether prompt, source, or customer-sensitive content is included;
- the supported control for disabling or changing the behavior.

Document the current product behavior, not an aspirational policy. Escalate deployment-level encryption, logging, backup, and incident questions to platform operations and security.

## Ask Dev data boundary

Ask Dev conversations are stored by the Ask Dev service (ACR), not by the Dev Health API. The service keeps the conversation, each question, the stored result, the event log and any feedback.

- **Retention.** A conversation has a retention tier of 0 days or 30 days. With 0 days it expires 24 hours after its last turn; with 30 days, 30 days after its last turn. When no tier is chosen, 0 days applies. The web app does not request the 30-day tier. Other values are refused. Expired conversations are not served on any read after their expiry. The Ask Dev service removes them only where its own retention cleanup job is turned on (`ACR_ASK_DEV_RETENTION_ENABLED` in the Ask Dev service, not a Dev Health variable); it is off by default. Older Ask Dev rows that still sit in the Dev Health PostgreSQL database (`public.dev_conversations`) are a separate store, purged by Dev Health's daily cleanup at their stored expiry; see [Repair stranded Ask Dev conversations](../../operate/run/ask-dev-rollout.md).
- **Audit records.** A receipt keeps input and result digests and the request id. A content-free monthly usage count exists. Cost is not metered.
- **Deleting.** A user can delete their own conversations and no one else's. Deleting closes the conversation at once and leaves a closure record that holds no question, answer or result. Content is removed on the next retention cleanup, where that job is on.
- **Organization removal.** Purging an organization deletes its Ask Dev rows; later calls with the old credentials are refused.

This page does not state that conversation content is or is not used for model training: no such statement is enforced in the product code. Ask your platform owner for the provider's terms for the model your organization uses.
