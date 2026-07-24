---
page_id: op-logs
summary: Collect structured, correlated, redacted logs with enough context to diagnose a request or job.
content_type: reference
owner: platform-operations
applicability: current
lifecycle: active
---

# Logs

Logs should identify service, environment, organization or workspace identifier, provider, operation, request or job identifier, status, duration, retry, and sanitized error.

Do not log:

- tokens, passwords, private keys, session cookies, authorization headers, or signed URLs;
- unredacted customer payloads or prompt content;
- full database connection strings;
- unnecessary personal data.

Use correlation identifiers to connect API, worker, queue, provider, and storage events.
