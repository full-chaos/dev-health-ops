---
page_id: admin-roles
summary: Grant the minimum product access required and verify SSO or role changes safely.
content_type: task-guide
owner: platform-product
source_of_truth:
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

## Ask Dev entity access

Ask Dev requests are served by Context Fabric, which re-reads the user's live access from Dev Health on every call and refuses with 403 `feature_not_enabled` when `ask_dev` is off. Context Fabric reauthorizes every subject ID in a request against the caller's live organization and repository grant. A page hint or a browser-provided ID never widens that grant.

Missing and unauthorized stable IDs intentionally return the same
existence-neutral result. Administrators should not expect the response to
confirm that an entity belongs to another workspace. Resolve access problems by
checking the user's workspace membership, effective role (including
impersonation), and the workspace's current source catalog; do not test with a
cross-workspace ID or broaden the scope to the organization.
