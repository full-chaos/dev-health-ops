---
page_id: ref-auth-resource-grants
summary: The repository grant, its key, roles and expiry, how a grant is created, changed and ended, how the decision reads grants and the membership default, and the refusals of the three write routes.
content_type: reference
owner: engineering
source_of_truth:
  - internal/auth/grant/grant.go
  - internal/auth/grant/store.go
  - internal/auth/grantend/grantend.go
  - internal/auth/authorization/grant.go
  - internal/auth/authapi/provisioning_grant.go
  - contracts/auth/v1/endpoint-profiles.auth-service.json
applicability: current
lifecycle: active
---

# Resource grants

A repository grant lets one member of an organization act on one repository, or on every repository of the
organization, with a role and an optional expiry. This page lists what a grant holds, how it ends, how the decision
reads it, and what the write routes accept and refuse.
{: .fc-page-lede }

For the steps an administrator follows, see [Grant access to repositories](../../admin/teams-and-identities/grant-repository-access.md).
For the organization roles a membership holds, see [Organization roles, members and invites](org-member-roles.md).

## Which table decides

Grants live in `auth.resource_grants`. They are auth-native data: there is no public copy. The grantee is a member, and
`public.memberships` decides whether the member is active (D6651). A grant never makes a person a member.

## What a grant holds

| Field | Rule |
| --- | --- |
| Organization | A grant belongs to one organization. A grant of another organization never applies. |
| Principal | The grantee. It must be an active member of the organization when the grant is made. |
| Resource kind | `repository`. This is the only kind. |
| Repository key | One repository path, or the explicit organization-wide `*`. See [Key format](#key-format). |
| Role | One of the roles in [Roles](#roles). |
| Expiry | Optional. It must lie in the future when it is set. |
| Origin | `api` or `backfill`. An `api` grant names the principal that made it. |

An inventory of connected repositories never creates a grant. A credential rotation or a refresh never widens one.

## Roles

| Role |
| --- |
| `viewer` |
| `member` |
| `admin` |

`owner` is organization-wide authority and is never held through a grant. The actions each role holds are in the
[policy catalog](policy-catalog.md).

## Life of a grant

- A grant is live until it is revoked. A live grant with an expiry stops counting at that time; the row stays.
- A revoke marks the grant revoked and never deletes it. A revoke of a revoked grant changes nothing.
- An update changes the role or the expiry of a live grant. The principal and the key never change: revoke the grant and make a new one. An update that changes nothing writes nothing. An update of a revoked grant is refused.
- A create over a principal, role and key whose only row has expired ends that row and makes the new one in the same transaction. A create over a live row of the same principal, role and key is refused as `already_granted`. A revoked row is history, and a create makes a new row.
- Removing a member and deprovisioning an organization end the live grants of the pair or of the organization through the same code as a revoke, so each of them writes its audit row and its event.
- Each write changes the grant row, one audit row, one provisioning event and the revision in one commit, or none of them. The events are `resource_grant.created`, `resource_grant.updated`, `resource_grant.revoked` and `resource_grant.expired`. The audit rows are `auth.resource_grant.created`, `auth.resource_grant.updated`, `auth.resource_grant.revoked` and `auth.resource_grant.expired`.

## How the decision reads grants

A repository action needs standing in the organization, which is an active membership or a service account row, and then
one of these:

- **A live grant.** A grant of the organization whose role holds the action, whose key is the requested repository or `*`, and that is neither revoked nor expired. The reason is `allowed_by_repository_grant`.
- **The membership default.** A member who has no grant row of any state in the organization holds the repository actions of the membership role, and an owner reads as `admin`. The reason is `allowed_by_membership_default`.

One grant row of any state, for any repository, ends the membership default for that member in that organization. From
then on the member holds only the live grants. The first grant a member receives therefore narrows the member to the
keys it names. A service account never uses the default: with no live grant it is denied with `grant_absent`.

## Repository id

A provider rename or transfer changes the path of a repository and keeps its id. A repository grant therefore holds the
stable id (`resource_repo_id`, `<provider>:<number>`, for example `github:42`) beside the path.

- A grant with an id matches on the id only. The path is display. The decision turns the live path of the request into an
  id with `auth.repository_identities` (organization, provider, provider id, current path), so a renamed repository keeps its
  grant, and a new repository that reuses an old path does not inherit one.
- A grant with no id (a legacy row, or a path that no identity resolved when it was made) matches on the path, as before.
  The wildcard `*` never holds an id.
- A grant that holds an id and a request path with no single identity row is denied with `repository_unresolved`. The next
  run of the sync resolves it. A path with two identity rows is also unresolved.
- A grant create stores the id when exactly one identity row of the organization has the path.
- `auth.repository_identities` is a copy. ClickHouse `repos` (`provider_repo_id`) stays the source. The operator verb
  `dho auth repo-identities-sync` copies it and gives every live grant that has no id the id of its path. It is idempotent,
  runs after `auth-migrate`, and prints counts and the ids of the grants still without an id, never a key.

The decision gives one of these reasons when it denies: `grant_expired`, `grant_revoked`, `repository_not_granted`,
`repository_unresolved`, `grant_absent` or `grant_read_failed`. An allow is bound to 30 seconds. The other decision reasons are in the
[policy catalog](policy-catalog.md).

## Write routes

The three routes are dark: no go-api route calls them yet, and no read or list route for grants is on the main branch.
They are served with the other provisioning routes (`AUTH_MEMBERSHIP_ROUTES`), they need `AUTH_USERS_OWNED`, and
they take the session issuer service credential or the go-api workload token. Their rows are in the
[endpoint profiles](endpoint-profiles.md).

| Route | Body members |
| --- | --- |
| `POST /internal/v1/grants/create` | `organization_id`, `actor_principal_id`, `principal_id`, `role`, `resource_key`, optional `expires_at`. |
| `POST /internal/v1/grants/update` | `organization_id`, `actor_principal_id`, `grant_id`, optional `role`, `expires_at`, `clear_expiry`, `expected_revision`. |
| `POST /internal/v1/grants/revoke` | `organization_id`, `actor_principal_id`, `grant_id`, optional `expected_revision`. |

A body with any other member is refused as malformed. The expiry is an RFC 3339 timestamp. `expires_at` and
`clear_expiry` together change nothing and are refused. `expected_revision` is the revision of the grant: a stale one
is refused.

The actor is an active superuser, or an active owner or admin member of the organization in the body. The rule runs
before the verb and again inside it after its locks. A non-superuser actor may not name a superuser as grantee.

The answer is the same for all three: `grant_id`, `organization_id`, `principal_id`, `role`, `expires_at` when set,
`status` and `revision`. The status is `revoked` first, then `expired`, else `active`. No answer repeats the repository key.

| Status | Reason | When |
| --- | --- | --- |
| 400 | `malformed` | The body is not a strict object of the members above. |
| 400 | `role_invalid` | The role is not in the table above. |
| 400 | `resource_key_invalid` | The key does not match the pattern. |
| 400 | `expiry_not_future` | The expiry is not in the future. |
| 400 | `nothing_to_change` | An update names no role and no expiry, or both `expires_at` and `clear_expiry`. |
| 403 | `admin_required`, `principal_deactivated`, `superuser_required` | The actor rule refuses. A grant that does not exist and a grant of another organization answer an actor the same way. |
| 404 | `grant_not_found`, `organization_not_found`, `principal_not_found` | The row is not there, for an actor the rule allows. |
| 409 | `users_not_owned` | Users are not owned by auth-service. |
| 409 | `organization_suspended` | The organization is suspended. |
| 409 | `grantee_not_member` | The grantee is not an active member of the organization. |
| 409 | `already_granted` | A live grant of the same principal, role and key exists. |
| 409 | `grant_revoked` | The grant is revoked. |
| 409 | `revision_conflict` | `expected_revision` is not the grant's revision. |
| 503 | `store_unavailable` | The store or a read it needs failed. Nothing was written. |

There is no go-api `audit_logs` row for a grant write. The auth audit row and the provisioning event are the record.

## Key format

The key is `*` for every repository of the organization, or a lowercase repository path of 2 to 20 segments, such as
`owner/repo` or `group/subgroup/repo`, in at most 255 bytes. Every segment but the last is a group name of 1 to 100
characters that starts and ends with a letter or a digit and never starts with a dot. The last segment is the
repository name; it may start with a dot (`.github`), but `.` and `..` are refused. A segment holds lowercase letters,
digits, `.`, `_` and `-`. The pattern is the same in the request contract, in the database check and in the code:

```text
^(?:[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?/(?:[a-z0-9]|[a-z0-9.][a-z0-9._-]{0,98}[a-z0-9])|\*)$
```

A key that does not match is refused as `resource_key_invalid`. A grant is bound to its organization, so two
organizations that hold the same path never share a grant.
