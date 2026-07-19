---
page_id: admin-credentials
summary: Rotate or revoke a provider credential without losing the evidence needed to verify recovery.
content_type: task-guide
owner: platform-product
applicability: current
lifecycle: active
---

# Rotate or revoke provider credentials

1. Identify the workspace, provider, connection, owner, and dependent synchronization jobs.
2. Create the replacement credential with the minimum required access.
3. Update the supported secret or connection field without exposing the value in logs or screenshots.
4. Verify authentication and one bounded synchronization.
5. Revoke the old credential only after recovery is confirmed.
6. Record the rotation time, verifier, and any coverage gap.

For a suspected compromise, follow the security incident process rather than a routine rotation sequence.
