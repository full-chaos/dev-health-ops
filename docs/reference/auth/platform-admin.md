---
page_id: ref-auth-platform-admin
summary: What the platform_admin role holds, how it differs from an organization role, the two sources of platform authority, how impersonation scopes it, and the operator verbs that change it.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/vocabulary/roles.v1.json
  - internal/auth/platformrole/platformrole.go
  - internal/auth/platformrole/rule/
  - internal/api/policy/scope.go
  - internal/auth/usersbackfill/platformrole_command.go
applicability: current
lifecycle: active
---

# Platform administrator

A platform administrator acts on the platform, not inside one organization. This page says what the role holds, where
the platform sees it, and how it stays apart from organization roles.
{: .fc-page-lede }

## Roles

| Role | Scope | Holds |
| --- | --- | --- |
| `platform_admin` | platform | `admin.orgs`, `admin.users`, `admin.impersonate`, `admin.billing`, `admin.flags`, `admin.audit`, `admin.telemetry`, `admin.stats`. |
| `platform_owner` | platform | No action. It is a marker. An owner acts only through a `platform_admin` row it also holds. |

The closed list is `contracts/auth/v1/vocabulary/roles.v1.json`.

## Rules that do not change

- A platform role holds platform actions only. An organization role never comes from a platform role. A platform role never comes from an organization role.
- An organization owner or admin is not a platform administrator, and the reverse.
- Only a user principal holds a platform role. A service account or a workload does not.
- Granting or revoking `platform_admin` needs the platform action `admin.users` now, held by the granter at the time of the call.
- A revoke that would leave no active platform administrator is refused.
- `platform_owner` is granted and revoked only by `auth-migrate`. The API refuses it and writes a `denied` audit row. With `AUTH_PLATFORM_ROLES_OWNED=false`, the `dho auth` write verbs `platform-role-grant` and `platform-role-revoke` refuse (exit 3) before they open the store, so they write no `denied` audit row.

## Two sources of authority

`AUTH_PLATFORM_ROLES_OWNED` picks the source of platform authority (`internal/auth/platformrole/rule/`).

| Switch | The caller is a platform administrator when |
| --- | --- |
| off (default) | `users.is_superuser` is true in the public schema. |
| `true` | The caller holds `platform_admin` now in `auth.platform_role_assignments`. go-api and query-api read this in the same statement as the users read, so a failed auth read fails the request. |

The chart renders the switch from `authService.platformRolesOwned`, and it needs users owned. The flip and its rollback
are in [Auth platform roles flip and rollback](../../operate/runbooks/auth-platform-roles-flip-and-rollback.md). While the switch is off, `dho auth platform-role-grant` and `platform-role-revoke`
refuse. The public user record is the authority for who the user is (D6651). The platform role assignment is
auth-native data.

## Impersonation

A caller who holds the platform action `admin.impersonate` can start an impersonation session on a target user
(`POST /api/v1/admin/impersonate`).

- The start is refused for yourself, for a user who is not active, for a superuser, and for a user with no organization membership.
- While the session is live, an organization-scoped admin route acts in the target's organization. Without a session it
  acts in the organization claim of the caller's token. An `X-Org-Id` header does not move a superuser.
- `EffectiveOrgID` (`internal/api/policy/scope.go`) decides this on every request, after a live check of the session. Nothing carries the session between requests.
- The session honors only a caller who holds `admin.impersonate` at that request. A session changes the organization of the route. It adds no role.

## Operator verbs

| Verb | Use |
| --- | --- |
| `dho auth platform-role-list` | List assignments and whether each is active now. Read only. Prints ids only. |
| `dho auth platform-role-grant -principal ID -role platform_admin -granted-by ID [-expires-at RFC3339]` | Grant. The granter must hold the action now. |
| `dho auth platform-role-revoke -principal ID -role platform_admin -revoked-by ID` | Revoke. The revoker must hold the action now. Never the last active `platform_admin`. |
| `auth-migrate platform-role-bootstrap -principal ID -confirm` | Break-glass grant when no active platform administrator exists. The audit reason code is `restore_fence_bootstrap`. |

The token claim `is_superuser` can be up to 60 minutes old after a flip. No server decision trusts it. See
[Auth authentication and authorization](authn-authz.md#platform-authority) for the full read order, and
[Auth action map](action-map.md) for the routes each action guards.
