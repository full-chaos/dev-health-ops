---
page_id: admin-impersonation
summary: View the product as another user, know what is limited while you do, and stop.
content_type: task-guide
owner: platform-product
source_of_truth:
  - internal/api/session/session.go
applicability: current
lifecycle: active
---

# View the product as another user

Impersonation lets a platform administrator see the product as another user sees it. Use it to reproduce a problem the user reports.

## Know who can use it

Only a platform administrator can start it. The **Impersonate User** button on the user page is not shown when:

- you are not a platform administrator,
- the user is you,
- the user is a platform administrator or has the `admin` role, or
- your session is already impersonating, or your session is limited and the deployment enforces platform limits.

## Start

1. Open the user in the platform user list.
2. Select **Impersonate User**.

Expected result: the product opens in a new tab on the dashboard. If the browser blocks the new tab, it opens in the same tab. If the start is not confirmed, you stay as yourself and see an error.

## While you impersonate

- A banner reads "Viewing as" and the user's email address. If the address is not known, it reads "Viewing as another user". It shows in every tab.
- A start or a stop in one tab applies to all your tabs.
- You cannot switch organization.
- Ask Dev is hidden.
- Budget writes are unavailable. Some organization write controls are hidden.

## Stop

Select **Stop Impersonating** in the banner. You return to your own organization. The impersonation tab closes, or, if the browser keeps it open, opens the platform admin area.
