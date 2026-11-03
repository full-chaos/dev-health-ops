---
page_id: admin-org-members
summary: Invite a person, change a member's role, remove a member and transfer ownership of an organization, and what each refusal means.
content_type: task-guide
owner: platform-product
source_of_truth:
  - internal/apiservice/admin/orgs.go
  - internal/apiservice/admin/invites.go
  - internal/auth/membership/membership.go
  - internal/auth/invite/invite.go
applicability: current
lifecycle: active
---

# Manage organization members

Use this page to invite people to an organization, change a role, remove a member and move ownership. The roles,
states and refusals are in [Organization roles, members and invites](../../reference/auth/org-member-roles.md).

## Before you start

- You are an owner or an admin member of the organization, or an active superuser.
- Only a superuser may change a member who is a superuser.
- The organization is active. A suspended organization refuses every member and invite write.
- Pick the least role that does the job. `viewer` and `member` cannot change members. `admin` can. Only an `owner` can delete the organization.

## Invite a person

1. Send `POST /api/v1/admin/orgs/{org_id}/invites` with the person's `email` and a `role`.
2. Give the person the invite. It is valid for 72 hours by default, and the token works once.
3. The person signs in with an account whose email address is verified and equals the invite address, then accepts.

If the same address already has a pending invite, revoke it or wait for it to expire. A person who is already a member cannot accept.

## Change a role

1. Send `PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}` with the new `role`.
2. The answer is the updated membership. The same role again changes nothing.

A role outside `viewer`, `member`, `admin` and `owner` is refused. You cannot take the last owner out of the `owner` role.

## Remove a member

1. Send `DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}`.
2. The membership ends and the member's sessions in the organization end.

You cannot remove the last owner of an organization. Transfer ownership first.

## Transfer ownership

1. Make the new owner an active member.
2. Send `POST /api/v1/admin/orgs/{org_id}/transfer-ownership` with `new_owner_user_id`.
3. The current owner becomes `admin`. The target becomes `owner`.

The call takes the source from the organization's current owner. A target that is not an active member is refused.

## When a call is refused

| You see | Do this |
| --- | --- |
| `admin_required`, `superuser_required` | Use an owner or admin account. Ask a superuser to change a superuser member. |
| `last_owner` | Name another owner first, then repeat. |
| `already_member` | The person is a member. Change the role instead. |
| `membership_suspended`, `organization_suspended` | Ask an operator to end the suspension. |
| `invite_invalid` | The token is unknown, used, revoked or expired. Send a new invite. |
| `email_unverified`, `email_mismatch` | The account must verify the address the invite names. |
| `state_changed` | The row moved while the call ran. Read the member list and repeat. |

Check the result with `GET /api/v1/admin/orgs/{org_id}/members`. Do not widen a role to work around a refusal.
Every change leaves an audit row. For platform authority, see [Platform administrator](../../reference/auth/platform-admin.md).
