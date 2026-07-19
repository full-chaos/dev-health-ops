---
page_id: admin-auth
summary: Diagnose sign-in, SSO, membership, role, and authorization failures.
content_type: troubleshooting
owner: platform-product
applicability: current
lifecycle: active
---

# Authentication and role problems

1. Confirm the user is signing into the intended workspace and identity provider.
2. Check workspace membership and the current role.
3. For SSO, verify issuer, audience, callback, and group or role mapping.
4. Reproduce with a second account that has the expected role.
5. Distinguish authentication failure from authorization failure.
6. Retain the route, time, correlation or request identifier, and sanitized message.

Do not solve an authorization defect by granting owner access permanently.
