---
page_id: op-external
summary: Configure email, model, provider, and other external services with explicit failure and spend limits.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/email-setup.md
  - current model and provider runtime configuration
applicability: current
lifecycle: active
---

# Model and external-service configuration

For each external service, document:

- endpoint and supported region or host;
- credential owner and secret location;
- timeout, retry, rate, circuit-breaker, and spend limits;
- data categories transmitted;
- health, error, and usage signals;
- safe degradation behavior;
- rotation and disable procedure.

Verify with a bounded non-sensitive request before enabling broad workload. Do not present a configured model or email provider as healthy until the application path succeeds.
