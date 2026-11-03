---
page_id: admin-grant-repository-access
summary: Give a member access to one repository or to every repository of an organization, with an optional expiry, then change or end it, and what each refusal means.
content_type: task-guide
owner: platform-product
source_of_truth:
  - internal/auth/grant/grant.go
  - internal/auth/grant/store.go
  - internal/auth/authorization/grant.go
  - internal/auth/authapi/provisioning_grant.go
applicability: current
lifecycle: active
---

# Grant repository access

Use a repository grant to give a member a role on one repository, or on every repository of the organization. The
[repository grants reference](../../reference/auth/resource-grants.md) has the full rules.
{: .fc-page-lede }

## Before you start

- You are an owner or an admin member of the organization, or an active superuser.
- The person is an active member of the organization. A grant does not add a member: see [Manage organization members](manage-org-members.md).
- The organization is active.
- You hold the session issuer service credential or the go-api workload token. The three routes are dark: no go-api route calls them yet, and no route lists grants yet. Keep the `grant_id` of every answer, because you need it to change or end the grant.

## The first grant narrows the member

A member with no grant in the organization acts by the membership role. After the first grant of any state, the
member acts only by the live grants. Before you give a member a grant, decide the full set of repositories the member
needs, or give `*`, so that the member does not lose access to a repository the grant does not name.

Connecting a repository or syncing one never creates a grant. A credential rotation never widens one.

## Give access to one repository

1. Send `POST /internal/v1/grants/create` with the organization, your principal, the member's principal, a `role` of `viewer`, `member` or `admin`, and the key in lowercase `owner/repo`.
2. Keep the `grant_id` and the `revision` of the answer. The answer does not repeat the key.

```bash
# $AUTH_HEADER_FILE holds one line: Authorization: Bearer <credential>. Keep it mode 0600.
curl -sS -X POST "$AUTH_SERVICE_URL/internal/v1/grants/create" \
  -H @"$AUTH_HEADER_FILE" \
  -H "Content-Type: application/json" \
  -d '{"organization_id":"<org-id>","actor_principal_id":"<your-principal-id>","principal_id":"<member-principal-id>","role":"viewer","resource_key":"<owner>/<repo>"}'
```

## Give access to every repository

Send the same request with `"resource_key":"*"`. The key `*` is the only organization-wide key. No default grant exists:
nothing creates a `*` grant for you.

## Set an expiry

Add `"expires_at":"<RFC 3339 timestamp>"` to the create, or to an update. The time must lie in the future. At that
time the grant stops counting and the member loses what only that grant gave. To remove an expiry, send an update with
`"clear_expiry":true`.

## Change a grant

1. Send `POST /internal/v1/grants/update` with the organization, your principal, the `grant_id`, and a new `role`, a new `expires_at` or `clear_expiry`.
2. Add `expected_revision` with the revision you read, so that a change made by someone else refuses yours.

You cannot change the key or the member. Revoke the grant and create a new one.

## End a grant

1. Send `POST /internal/v1/grants/revoke` with the organization, your principal and the `grant_id`.
2. The answer has the status `revoked`. The row stays as history. A new grant of the same member, role and key is a new row.

If you revoke the last grant of a member, the member still has a grant row, so the membership default does not come
back. Give a new grant when the member needs access again.

## Refusals

| Status | Reason | What to do |
| --- | --- | --- |
| 400 | `resource_key_invalid` | Use lowercase `owner/repo` or `*`. |
| 400 | `role_invalid` | Use `viewer`, `member` or `admin`. |
| 400 | `expiry_not_future` | Give a time after now. |
| 400 | `nothing_to_change` | Name a role or an expiry, and not `expires_at` with `clear_expiry`. |
| 403 | `admin_required`, `principal_deactivated`, `superuser_required` | Act as an owner, an admin or a superuser. Only a superuser may name a superuser. |
| 404 | `grant_not_found` | Check the `grant_id` and the organization. A grant of another organization gives the same answer. |
| 409 | `grantee_not_member` | Add the person to the organization first. |
| 409 | `already_granted` | A live grant of the same member, role and key exists. Update it. |
| 409 | `grant_revoked` | Create a new grant. |
| 409 | `revision_conflict` | Read the grant again, then repeat with its revision. |
| 409 | `organization_suspended` | Reactivate the organization first. |
| 409 | `users_not_owned` | Auth-service does not own users in this deployment. |

Every write leaves one audit row and one provisioning event. See [Repository grants](../../reference/auth/resource-grants.md#life-of-a-grant).
