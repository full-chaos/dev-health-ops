---
page_id: int-wh-config
summary: Configure a provider webhook URL, events, and secret for the current managed-sync handler.
content_type: task-guide
owner: platform-api
applicability: current
lifecycle: active
---

# Configure a webhook

1. Connect the provider through the supported workspace administration path first.
2. Choose the current provider endpoint for GitHub, GitLab, or Jira.
3. Generate a strong secret and store it in the deployment secret manager.
4. Configure the same secret for the handler and provider webhook.
5. Select only the event families supported by the current handler.
6. Send the provider test event.
7. Verify signature acceptance, idempotency, queueing, and downstream progress.

Use the exact public application origin and TLS path. Do not put a reusable secret in a screenshot or example URL.
