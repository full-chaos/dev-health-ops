---
page_id: use-sign-in-switch-organization
summary: Change the organization you work in when your account belongs to more than one.
content_type: task-guide
owner: documentation
source_of_truth:
  - internal/api/session/routes.go
  - internal/api/session/session.go
applicability: current
lifecycle: active
---

# Switch organization

The workspace card at the top of the navigation shows the organization you work in. It shows the organization name, and under it either "Organization workspace" or, when your account has only one organization, "Organization workspace · only one on this account". It also shows the data state of the organization: "Data through" a date, "Has data", "No data yet", or "Data status unavailable".

## Switch

1. Open the organization list in the workspace card.
2. Choose another organization.

Expected result: the page reloads in the other organization. Every view, report and setting now belongs to it.

Things to know:

- The list holds only organizations you are a member of. With one organization the list is disabled.
- Your role can differ between organizations. See [Permissions and availability](../troubleshooting/permissions-and-availability.md).
- If the switch fails, the card shows "Could not switch organization". Nothing changed. Try again.
- If the list cannot load, the card shows "Could not load organizations".
- You cannot switch organization while [viewing as another user](../../admin/security-and-audit/impersonation.md).

To be added to another organization, ask an administrator of that organization to [invite you](../../admin/teams-and-identities/manage-org-members.md).
