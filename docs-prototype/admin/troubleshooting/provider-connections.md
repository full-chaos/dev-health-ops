---
page_id: admin-provider-fail
summary: Diagnose provider authentication, namespace visibility, callback, and repository-coverage failures.
content_type: troubleshooting
owner: platform-product
applicability: current
lifecycle: active
---

# Provider connection failures

1. Confirm the provider host and connection type.
2. Check credential expiry, revocation, installation state, and required scope without exposing the value.
3. Verify callback and redirect configuration for delegated flows.
4. Verify the intended organization, group, project, or repository is visible to the connection.
5. Retry one bounded authorization or synchronization action.
6. Escalate repeated rate-limit, worker, queue, or storage failures to operations.
