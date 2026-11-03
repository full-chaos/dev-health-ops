---
page_id: ref-auth-org-member-roles
summary: The organization roles, the states of a membership and an invite, the four member writes and the refusals they give, and which table decides.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/vocabulary/roles.v1.json
  - internal/auth/membership/membership.go
  - internal/auth/invite/invite.go
  - internal/auth/authapi/provisioning.go
  - internal/apiservice/admin/orgs.go
applicability: current
lifecycle: active
---

# Organization roles, members and invites

An organization has members. Each member holds one organization role. This page lists the roles, the states of a
membership and of an invite, the writes that change them, and the refusals those writes give.
{: .fc-page-lede }

For the steps an administrator follows, see [Manage organization members](../../admin/teams-and-identities/manage-org-members.md).
For platform authority, which is a different thing, see [Platform administrator](platform-admin.md).

## Which table decides

`public.users`, `public.organizations` and `public.memberships` are the authoritative records (D6651). The `auth`
schema keeps copies of the shared columns so that its own tables can point at them. A copy never decides an access
question.

- A `public.memberships` row means the member is active, with the role on that row.
- With no public row, the status on the `auth.memberships` row counts. An `active` auth row with no public row counts as
  `removed`.
- Every member write changes the public row, the auth row, the audit row, the provisioning event and the revision in
  one commit, or none of them.
- `auth.users` also holds the data only auth-service uses: credentials, password state and lockout state. These have
  no public copy.

## Organization roles

The closed list is `contracts/auth/v1/vocabulary/roles.v1.json`. A role holds an explicit action list. No role
inherits another and no role holds a wildcard. The [policy catalog](policy-catalog.md) lists every action.

| Role | What it adds |
| --- | --- |
| `viewer` | Read actions: organization, members, teams, settings, git, work items, metrics, analytics, billing and license reads, and `acr.context.read`. |
| `member` | The `viewer` actions and `analytics.export`. |
| `admin` | The `member` actions and the write and manage actions: members (`members.invite`, `members.manage`), teams, settings, organization write, integrations, git and work-item sync, metric compute, `billing.manage`. |
| `owner` | The `admin` actions and `org.delete`. |

- An organization role holds organization actions only. A platform role never gives an organization role, and the reverse.
- An organization owner is not a platform administrator. See [Platform administrator](platform-admin.md).
- One user can be a member of several organizations, with a different role in each.

## Membership states

| State | Meaning |
| --- | --- |
| `invited` | The pair has an invite and no membership yet. |
| `active` | The member holds the role and acts in the organization. |
| `suspended` | The membership exists and the role is kept, but the member does not act. |
| `removed` | The membership ended. |

Only active members act in an organization. An organization that is suspended refuses member and invite writes.

## Member writes

go-api serves the administrator routes below. With `AUTH_PROXY_ORG_PROVISIONING=true` go-api sends each write to the
auth-service route in the same row, and it writes only after auth-service answers. See
[Auth service configuration](configuration.md) for the switch, the credential and the route bodies.

| go-api route | Body | auth-service route |
| --- | --- | --- |
| `POST /api/v1/admin/orgs/{org_id}/members` | `user_id`, optional `role` (default `member`) | `memberships/add` |
| `PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}` | `role` | `memberships/role` |
| `DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}` | none | `memberships/remove` |
| `POST /api/v1/admin/orgs/{org_id}/transfer-ownership` | `new_owner_user_id` | `memberships/transfer` |

Rules of the writes:

- Add: an absent, invited or removed pair becomes `active` with the role. An active pair is refused. A suspended pair is refused.
- Change of role: the pair must be active. The same role again is a no-op, and a change that would leave the organization with no owner is refused. Both hold with the switch on (auth-service) and off (go-api): a repeated role writes nothing and leaves `updated_at` as it was, and the last owner's role change is refused with 400 "Cannot remove the last owner of an organization". The other transition rules apply only with `AUTH_PROXY_ORG_PROVISIONING=true`.
- Remove: an active or a suspended membership becomes `removed`.
- Transfer: go-api takes the organization's current owner as the source. The source becomes `admin` and the target
  becomes `owner`. The target must be an active member.
- No write leaves an organization with no active owner. A change of role or a removal of the last active owner is refused (`last_owner`).
  An organization that already has no owner stays usable, and naming an owner is the way out.
- A removal ends the member's sessions in that organization.

Who may act, checked before anything is read and again inside the write after its locks: an active superuser, or
an owner or admin member of the organization. Only a superuser actor may change a member who is a superuser.

## Invites

An invite names an email address and a role. The token is single purpose and is stored as a SHA-256 digest only. It
is consumed once.

| State | Meaning |
| --- | --- |
| `pending` | The invite can be accepted until its expiry. |
| `accepted` | Used. It stays accepted. |
| `revoked` | Ended by an administrator. A revoke is monotonic and never a way back. |

- The default lifetime is 72 hours. A requested lifetime is 1 hour to 30 days (`ttl_invalid` outside).
- An invite is still valid at the instant it expires.
- An address must be a normalized address (`email_invalid`). A second pending invite for the same address is refused (`invite_pending`).
- To accept, the account must be active, its email address verified (`email_unverified`) and equal to the invite address
  after strip and lower-casing (`email_mismatch`).
- A token that is unknown, accepted, revoked or expired gives one refusal, `invite_invalid`, so a caller learns nothing about which.
- A revoke of a revoked invite is a no-op. A revoke of an accepted or expired invite is `invite_not_pending`.

## Refusals

| Status | `reason_code` | Meaning |
| --- | --- | --- |
| 400 | `role_invalid` | The role is not one of the four organization roles. |
| 400 | `invite_invalid`, `email_invalid`, `ttl_invalid` | The invite token, address or lifetime is not usable. |
| 403 | `principal_deactivated`, `admin_required`, `superuser_required` | The actor rule refused. |
| 403 | `email_unverified`, `email_mismatch` | An accept by an account that does not fit the invite. |
| 404 | `membership_not_found`, `organization_not_found`, `principal_not_found`, `invite_not_found` | The row is not there. |
| 409 | `already_member`, `membership_suspended` | An add on an existing pair. |
| 409 | `last_owner`, `not_owner`, `target_not_member` | An owner rule. |
| 409 | `organization_suspended` | The organization is suspended. |
| 409 | `invite_pending`, `invite_not_pending` | An invite state rule. |
| 409 | `state_changed` | The row moved while the write ran. |
| 409 | `users_not_owned` | auth-service does not own users. |

Each write puts one audit row and one provisioning event in its commit. The audit events are
`auth.membership.added`, `auth.membership.role_changed`, `auth.membership.ownership_transferred` and
`auth.membership.removed`. For the outbox, see [Audit outbox](../../contribute/architecture/auth-control-plane-audit-outbox.md).
