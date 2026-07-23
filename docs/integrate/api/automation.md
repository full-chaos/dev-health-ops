---
page_id: int-automation
summary: Automate a supported API workflow with bounded scope, retries, provenance, and failure handling.
content_type: task-guide
owner: platform-api
applicability: current
lifecycle: active
---

# Automate supported workflows

- Pin the API contract or schema revision used by the integration.
- Discover current limits instead of hardcoding them where a discovery route exists.
- Use deterministic idempotency for writes.
- Bound date ranges, result size, concurrency, and retries.
- Preserve request, organization, source, run, and output provenance.
- Distinguish transient failure, authorization failure, validation failure, and unsupported behavior.
- Stop retry amplification during provider or platform degradation.

Test automation against a non-production organization and representative fixture before broad rollout.
