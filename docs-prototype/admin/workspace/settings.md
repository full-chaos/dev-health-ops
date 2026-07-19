---
page_id: admin-workspace-settings
summary: Change supported workspace settings and verify their effect without exposing deployment secrets.
content_type: task-guide
owner: platform-product
source_of_truth:
  - current /settings and /org/admin product surfaces
applicability: current
lifecycle: active
---

# Configure workspace settings

1. Open **Admin** or **Settings** in the intended workspace.
2. Review the current value and its workspace-wide effect.
3. Change one supported setting at a time.
4. Save and verify the affected product workflow with a non-sensitive example.
5. Record the old and new value, reviewer, and rollback condition for high-impact changes.

Do not store provider secrets, private keys, or infrastructure credentials in general workspace fields. A setting that requires an environment variable or secret manager belongs under [Configure the platform](../../operate/configure/index.md).
