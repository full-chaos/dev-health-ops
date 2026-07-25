---
page_id: admin-roles
summary: Grant the minimum product access required and verify SSO or role changes safely.
content_type: task-guide
owner: platform-product
source_of_truth:
  - docs/sso-setup.md
  - current authentication and authorization implementation
applicability: current
lifecycle: active
---

# Manage roles and access

1. Confirm the intended workspace and supported role model.
2. Grant the least privilege required for the documented task.
3. When SSO is enabled, verify issuer, audience, callback, and group or role mapping against the current deployment configuration.
4. Test sign-in and one representative authorized action with a non-owner account.
5. Test that an unauthorized account remains blocked.
6. Retain sanitized audit evidence and a rollback path.

Do not troubleshoot access by sharing session tokens or elevating every user to an owner role.
