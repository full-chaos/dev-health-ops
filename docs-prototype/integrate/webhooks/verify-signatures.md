---
page_id: int-wh-verify
summary: Verify provider-specific webhook signatures before parsing or dispatching an event.
content_type: task-guide
owner: platform-api
applicability: current
lifecycle: active
---

# Verify webhook signatures

The handler must validate the provider-specific secret or signature over the raw request according to the current implementation.

- GitHub uses the configured webhook secret and HMAC signature validation.
- GitLab uses the configured secret token header.
- Jira uses its supported configured secret path when enabled.

Reject missing or invalid signatures before processing. Compare safely, rotate secrets through a controlled overlap or cutover plan, and never log the secret or complete authorization headers.
