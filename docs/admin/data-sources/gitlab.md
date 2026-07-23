---
page_id: admin-gitlab
summary: Connect GitLab with the minimum scopes and verify accessible groups and projects.
content_type: task-guide
owner: platform-product
source_of_truth:
  - docs/connectors/gitlab-permissions.md
  - current GitLab provider implementation
applicability: current
lifecycle: active
---

# Connect GitLab

1. Confirm the GitLab host and whether the deployment supports GitLab.com or the intended self-managed instance.
2. Create or select a credential with only the scopes required by the current connector.
3. Add the connection through the workspace administration surface.
4. Verify the intended groups and projects are visible to the credential.
5. Start or wait for synchronization and check coverage.
6. Remove unused broad scopes after verification.

A successful authentication response does not prove that every required project is visible. Check namespace membership and token scope separately.
