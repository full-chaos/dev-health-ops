---
page_id: admin-github
summary: Connect GitHub with the supported app or credential path and verify repository coverage.
content_type: task-guide
owner: platform-product
source_of_truth:
  - docs/user-guide/github-app-auth.md
  - docs/user-guide/github-app-setup.md
  - current GitHub App implementation
applicability: current
lifecycle: active
---

# Connect GitHub

Prefer the supported GitHub App flow when the deployment exposes it.

1. Open the workspace **Connections** or GitHub integration surface.
2. Start the installation or authorization flow from the product.
3. Install the app for the intended organization and select the minimum repositories required.
4. Return through the configured callback and confirm the installation is associated with the correct workspace.
5. Start or wait for synchronization.
6. Verify repository coverage and freshness before using product results.

Keep app IDs, client secrets, private keys, and installation credentials in the approved secret store. Do not paste them into the UI unless the supported flow explicitly requires that field.
