---
page_id: int-auth-fail
summary: Diagnose the wrong credential type, missing scope, source mismatch, expiry, revocation, or organization context.
content_type: troubleshooting
owner: platform-api
applicability: current
lifecycle: active
---

# Authentication and authorization errors

1. Identify the API surface and expected credential type.
2. Confirm expiry, revocation, source binding, and required scope.
3. Confirm organization and workspace context.
4. For Customer Push, compare the envelope source instance with the exact registered token binding.
5. For provider webhooks, verify the configured signature secret independently from connector credentials.
6. Reproduce one least-privilege request after correction.

Rotate a potentially exposed credential; do not paste it into diagnostic output.
