---
page_id: ref-auth-policy-catalog
summary: The closed action and role catalog (actions.v1, roles.v1) - every action with its scope, risk class and cache class, which role holds which action, the rules that the loader and the profile gate enforce, and the other vocabularies that depend on it.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/vocabulary/actions.v1.json
  - contracts/auth/v1/vocabulary/roles.v1.json
  - contracts/auth/v1/vocabulary/reason-codes.json
  - contracts/auth/v1/vocabulary/workload-registry.json
  - contracts/auth/v1/README.md
  - internal/auth/policycatalog/catalog.go
  - internal/auth/contracts/endpoint_auth_profile_selector_gate.go
  - internal/auth/platformrole/platformrole.go
applicability: current
lifecycle: active
---

# Policy catalog reference

The catalog is the closed list of actions and roles of the Auth Control Plane. The version, the actions and the roles are in `contracts/auth/v1/vocabulary/actions.v1.json` and `roles.v1.json`. An action or role that is not in the catalog is refused everywhere.
{: .fc-page-lede }

The JSON files are the single source. `internal/auth/policycatalog` embeds them, parses them and validates them at start. A defect panics the process, so a bad catalog never serves. See also [authentication and authorization](authn-authz.md), [action map](action-map.md) and [obligations](obligations.md).

## Actions

Names are dotted and namespaced. A bare single segment is not an action (the pattern is `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`). No action means "all actions".

28 actions are organization actions. 10 are platform actions. 25 are high risk. 13 are low risk.

| Action | Scope | Resource kind | Risk class | Cache class |
| --- | --- | --- | --- | --- |
| `analytics.read` | organization | organization | low | `decision_bound` |
| `analytics.export` | organization | organization | high | `none` |
| `metrics.read` | organization | organization | low | `decision_bound` |
| `metrics.compute` | organization | organization | high | `none` |
| `work_items.read` | organization | organization | low | `decision_bound` |
| `work_items.sync` | organization | organization | high | `none` |
| `git.read` | organization | organization | low | `resource_revision_bound` |
| `git.sync` | organization | organization | high | `none` |
| `teams.read` | organization | organization | low | `decision_bound` |
| `teams.write` | organization | organization | high | `none` |
| `settings.read` | organization | organization | low | `decision_bound` |
| `settings.write` | organization | organization | high | `none` |
| `reports.write` | organization | organization | high | `none` |
| `integrations.read` | organization | organization | low | `decision_bound` |
| `integrations.write` | organization | organization | high | `none` |
| `members.read` | organization | organization | low | `decision_bound` |
| `members.invite` | organization | organization | high | `none` |
| `members.manage` | organization | organization | high | `none` |
| `members.update` | organization | organization | high | `none` |
| `org.read` | organization | organization | low | `decision_bound` |
| `org.write` | organization | organization | high | `none` |
| `org.delete` | organization | organization | high | `none` |
| `billing.read` | organization | organization | low | `decision_bound` |
| `billing.manage` | organization | organization | high | `none` |
| `license.read` | organization | organization | low | `decision_bound` |
| `acr.context.read` | organization | repository | low | `resource_revision_bound` |
| `acr.evidence.read` | organization | repository | low | `resource_revision_bound` |
| `acr.credential.issue` | organization | organization | high | `none` |
| `acr.credential.revoke` | organization | organization | high | `none` |
| `sso.write` | organization | organization | high | `none` |
| `admin.users` | platform | platform | high | `none` |
| `admin.orgs` | platform | platform | high | `none` |
| `admin.impersonate` | platform | platform | high | `none` |
| `authz.decide` | platform | platform | high | `none` |
| `admin.billing` | platform | platform | high | `none` |
| `admin.flags` | platform | platform | high | `none` |
| `admin.audit` | platform | platform | high | `none` |
| `admin.telemetry` | platform | platform | high | `none` |
| `credentials.project` | platform | platform | high | `none` |
| `admin.stats` | platform | platform | high | `none` |

Source: `contracts/auth/v1/vocabulary/actions.v1.json`. `resource_kind` equals `scope` for every action except `acr.context.read` and `acr.evidence.read`, whose resource is a repository. The kind set is `platform`, `organization` and `repository`; a scope is `platform` or `organization` only.

### Risk class and cache class

| Cache class | Actions |
| --- | --- |
| `none` | All 25 high-risk actions. A decision for these is never reused. |
| `decision_bound` | The low-risk actions except `git.read`, `acr.context.read` and `acr.evidence.read`. |
| `resource_revision_bound` | `git.read`, `acr.context.read` and `acr.evidence.read`. |

The two cached classes are the allow-cache classes of TRD section 14. The catalog files do not define them further. A cached allow is bound to the policy revision of its decision and ends at the decision's `expires_at` (`authorization_decision.v1`, see [obligations](obligations.md#decision)).

A high-risk action must have cache class `none`. The loader refuses a catalog that breaks this rule (`internal/auth/policycatalog/catalog.go`, `Load`). The README of the contracts directory points to TRD section 14 for the allow-cache classes.

## Roles

| Role | Scope | Actions held |
| --- | --- | --- |
| `viewer` | organization | 15 of 40 |
| `member` | organization | 16 of 40 |
| `admin` | organization | 28 of 40 |
| `owner` | organization | 29 of 40 |
| `platform_admin` | platform | 8 of 40 |
| `platform_owner` | platform | 0 of 40 |

Each role lists its actions explicitly. There is no inheritance, no wildcard and no all-actions role. Which role holds which action:

| Action | `viewer` | `member` | `admin` | `owner` | `platform_admin` |
| --- | --- | --- | --- | --- | --- |
| `analytics.read` | yes | yes | yes | yes |  |
| `analytics.export` |  | yes | yes | yes |  |
| `metrics.read` | yes | yes | yes | yes |  |
| `metrics.compute` |  |  | yes | yes |  |
| `work_items.read` | yes | yes | yes | yes |  |
| `work_items.sync` |  |  | yes | yes |  |
| `git.read` | yes | yes | yes | yes |  |
| `git.sync` |  |  | yes | yes |  |
| `teams.read` | yes | yes | yes | yes |  |
| `teams.write` |  |  | yes | yes |  |
| `settings.read` | yes | yes | yes | yes |  |
| `settings.write` |  |  | yes | yes |  |
| `reports.write` | yes | yes | yes | yes |  |
| `integrations.read` |  |  | yes | yes |  |
| `integrations.write` |  |  | yes | yes |  |
| `members.read` | yes | yes | yes | yes |  |
| `members.invite` |  |  | yes | yes |  |
| `members.manage` |  |  | yes | yes |  |
| `members.update` |  |  |  |  |  |
| `org.read` | yes | yes | yes | yes |  |
| `org.write` |  |  | yes | yes |  |
| `org.delete` |  |  |  | yes |  |
| `billing.read` | yes | yes | yes | yes |  |
| `billing.manage` |  |  | yes | yes |  |
| `license.read` | yes | yes | yes | yes |  |
| `acr.context.read` | yes | yes | yes | yes |  |
| `acr.evidence.read` |  |  |  |  |  |
| `acr.credential.issue` | yes | yes | yes | yes |  |
| `acr.credential.revoke` | yes | yes | yes | yes |  |
| `sso.write` |  |  | yes | yes |  |
| `admin.users` |  |  |  |  | yes |
| `admin.orgs` |  |  |  |  | yes |
| `admin.impersonate` |  |  |  |  | yes |
| `authz.decide` |  |  |  |  |  |
| `admin.billing` |  |  |  |  | yes |
| `admin.flags` |  |  |  |  | yes |
| `admin.audit` |  |  |  |  | yes |
| `admin.telemetry` |  |  |  |  | yes |
| `credentials.project` |  |  |  |  |  |
| `admin.stats` |  |  |  |  | yes |

No role holds `members.update`, `authz.decide`, `credentials.project`, `acr.evidence.read` or `acr.credential.issue` yet, so the decision core denies them to every role until a roles change grants them. `acr.context.read` is held by the roles that hold `git.read` (catalog version 9); the decision core serves a repository request by the membership default or a repository grant, so a role that holds the action is allowed by default.

Every action names `impersonation` (`denied` or `allowed`): whether a delegated session may exercise it. `acr.context.read` is `allowed` (a read, low risk). `acr.evidence.read` is `allowed` (a read, low risk). `members.update`, `authz.decide` and `acr.credential.issue` are `denied`. A delegated session gets it only when the actor role holds it: `viewer`, `member`, `admin` and `owner` do.

Source: `contracts/auth/v1/vocabulary/roles.v1.json`. The role keys `viewer`, `member`, `admin` and `owner` are the organization roles that `membership.v1` uses. `platform_admin` and `platform_owner` are platform roles and are not membership roles. `platform_owner` holds no action: it is a marker that only the operator grants (`auth-migrate platform-owner-grant`), and an owner acts through a `platform_admin` row it also holds.

## Rules the loader enforces

`policycatalog.Load` refuses the catalog (`ErrInvalid`) when any of these hold:

- A file has a duplicate object member, an unknown member, or trailing data.
- `schema_version` is not `actions.v1` and `roles.v1`, or the two `catalog_version` values differ or are below 1.
- An action name does not match the pattern, is repeated, or has an unknown scope, resource kind, risk class or cache class.
- A high-risk action is cached.
- A role name does not match `^[a-z][a-z0-9_]*$`, is repeated, or is one of the reserved names `superuser`, `root`, `all`, `any`, `everyone`.
- A role lists an unknown action, lists one twice, or lists an action of the other scope.
- A role holds every action.

`Allows(role, action)` is true only for a known role that lists a known action.

## Rules the profile gate enforces

`CheckActionCachePolicy` (`internal/auth/contracts/endpoint_auth_profile_selector_gate.go`) joins an endpoint profile to the catalog:

- A profile that caches must name an action.
- The action must be in the catalog. There is no exemption list: a cached profile for an action that is not a catalog row is refused.
- A profile that caches a high-risk action is refused.
- The profile's cache class must be `none` or the catalog's class for the action.

## Who reads the catalog

| Reader | Use |
| --- | --- |
| `internal/auth/platformrole` | `Authority.Allows(action)` is true only for a platform action held by a held platform role. The grant action is `admin.users`. |
| `internal/auth/workload` | The workload registry may list only catalog actions in `allowed_actions`, and no wildcard. |
| `internal/auth/contracts` | The endpoint-profile gate (above). |

go-api and query-api request handlers do not read the catalog. See [action map](action-map.md#what-enforces-today).

### Workload registry

`contracts/auth/v1/vocabulary/workload-registry.json` seeds the platform workloads. `allowed_actions` are catalog actions. A row with no audience is a registry identity only and is never resolved by the token exchange. No workload holds an allowed action today.

| Workload | Audiences | Allowed actions |
| --- | --- | --- |
| `api` | `auth-service` | none |
| `auth-service` | none | none |
| `acr-api` | `auth-service`, `dev-health-api` | none |
| `web` | none | none |
| `query-api` | `auth-service` | none |

## Related closed vocabularies

`contracts/auth/v1/vocabulary/reason-codes.json` closes three wire fields:

| Field | Values |
| --- | --- |
| `authorization_decision.v1` `reason_code` | 26 values; see [reason codes](reason-codes.md) |
| `audit-event.v1` `attributes.reason_code` | 65 values; the file lists them |
| `revoked_reason` of a credential | `compromised`, `domain_revoked`, `org_deprovisioned`, `owner_request` |

A value is added only with the pull request of the producer that emits it. `error.v1` `reason_code` is not governed by this file. It is shape-only.

## Version history

From `contracts/auth/v1/README.md`:

- Version 1: the closed action and role catalogs.
- Version 2: every action gets a `risk_class` and a `cache_class`.
- Version 3: `billing.read`, `billing.manage` and `license.read`. They are held as the billing and licensing routes allow today. They are catalog-only: `/auth/me` does not serve them until a web-reviewed change does.
- Version 4: `acr.context.read` (organization scope, repository resource kind, low risk, `resource_revision_bound`), `members.update` (organization, high, `none`) and `authz.decide` (platform, high, `none`). The resource-kind set gains `repository`. The cached-action exemption list is gone.
- Version 5: the role `platform_owner` (platform scope, no action). No action changed, so no decision changes.
- Version 6: every action gets a required `impersonation` member (`denied` or `allowed`), and the action `admin.impersonate`.
- Version 8: `acr.context.read` is `allowed` under a delegated session. No role changed, so no decision changes.
- Version 9: `viewer`, `member`, `admin` and `owner` hold `acr.context.read`, as they hold `git.read`. `platform_admin` and `platform_owner` do not. No action row changed.
- Version 10: `credentials.project` (platform, high, `none`, `denied`), the action the acr-api workload row lists for the acr credential events route. No role holds it, so no decision changes.
- Version 11: `acr.evidence.read` (organization scope, repository resource kind, low risk, `resource_revision_bound`) and `acr.credential.issue` (organization, organization, high, `none`). No role holds either, so the decision core denies both to every role until a roles change grants them.
- Version 12: `reports.write` (organization scope, organization resource kind, high risk, `none`, impersonation `denied`), held by `viewer`, `member`, `admin` and `owner`. It covers the five saved-report GraphQL writes. Those writes check only the caller's organization today, never a role (`requireMutationOrg`, `internal/queryapi/graph/reports_mutations.go`), so every organization role holds it. It is catalog-only: `/auth/me` does not serve it. Narrowing it to a smaller role set is a separate decision.
- Version 13: an optional `repository_scope` member on an action (`filtered` or `none`, absent is `none`). `filtered` on `acr.context.read`, `acr.evidence.read`, `analytics.read`, `analytics.export`, `metrics.read`, `work_items.read` and `git.read` (the last five were set back to `none` in version 14): an allow of these carries the repositories the principal may read, as the `repository_scope` (`organization`) or the `repository_allowlist` obligation (see [obligations](obligations.md)). A census requires the member on every action whose id ends in `.read` or `.export` and on every repository-kind action. No role changed.
- Version 14: `repository_scope` is `none` on `analytics.read`, `analytics.export`, `metrics.read`, `work_items.read` and `git.read`. Product rule 2: no role-based redaction of metrics data in an organization. Every active member reads all metrics data of the organization, and a repository grant never narrows these five reads: an allow of them carries the tenant scope alone. `acr.context.read` and `acr.evidence.read` stay `filtered`: repository grants scope ACR only. No role changed.
- Version 15: `platform_admin: allowed` on `analytics.read`, `metrics.read`, `git.read`, `work_items.read` and `reports.write`. Product rule 1: a platform admin with no membership row gets the legacy answer, and the legacy query-api path admits a superuser to these reads and to the saved-report writes in any organization. The flagged allow also holds in a suspended organization (see below). No role changed.
- Version 16: `platform_admin: allowed` on `billing.read` and `license.read`. The legacy go-api billing and licensing handlers admit an `admin.billing` holder (or a `users.is_superuser` user) with no membership row on every row that asks these two actions. The flagged allow also holds in an organization with no organization row, because the legacy paths admit a superuser there without reading the organization (see below). No role changed.
- Version 17: `viewer`, `member`, `admin` and `owner` hold `acr.evidence.read` and `acr.credential.issue`, the same answers as the legacy web path, which lets every organization member read ACR evidence and issue an ACR credential (device approval and consent); `acr.credential.issue` stays impersonation `denied`. No platform role holds either action.
- Version 18: `acr.credential.issue` is `repository_scope` `filtered`, because device approval and consent take their repository scopes from this decision, and it carries `platform_admin: allowed`. Product rule 1: a platform admin with no membership row gets the legacy answer, and the legacy web routes let it approve a device and give consent in the organization of its session; its allow is org-wide (`repository_scope` `organization`). A repository allow that covers every repository in fact (the membership default with no grant row, or a live wildcard grant) now carries `repository_scope` `organization` instead of the one asked key, for `acr.context.read`, `acr.evidence.read` and `acr.credential.issue` alike; an allow by exact grants still lists its keys. The ACR repository reads carry no platform-admin flag: the legacy path refuses them to a platform admin with no membership row in its own organization. No role changed.
- Version 19: `platform_admin: allowed` on `org.read` and `settings.read`, and the new action `acr.credential.revoke` (organization, organization, high, `none`, impersonation `denied`, `platform_admin: allowed`) held by `viewer`, `member`, `admin` and `owner`. Product rule 1: a platform admin with no membership row gets the legacy answer on `GET /api/v1/orgs/me`, `GET /api/v1/telemetry/status` and `POST /api/v1/acr/credentials/{credential_id}/revoke`; every organization member reaches the same routes today. Which credentials a member may revoke stays with acr.
- Version 20: the new action `sso.write` (organization, organization, high, `none`, impersonation `denied`, no `platform_admin`) held by `admin` and `owner`, for create, update, delete, activate and deactivate of an SSO provider. Product rule 1: the legacy provider writes judged the token role alone (`user.role not in ("owner", "admin")`, sso/router.py at 64efc37bb3^: create :262, patch :333, delete :383, activate :408, deactivate :434); a platform admin with no membership row carries role member and no organization there, so it got 403 "Admin access required". `sso.write` has no `platform_admin` flag, so the answer is the same. `org.write` is unchanged.

## Platform admin on organization actions

An organization action can carry `platform_admin: allowed` (absent is `denied`). A principal whose platform role holds `admin.orgs` is then allowed the action in any active or suspended organization, or in an organization with no organization row, also with no membership row. The allow has the owner's obligations: the tenant scope only. The internal reason is `allowed_by_platform_admin`, and the wire code is `allowed_by_role` (see [reason codes](reason-codes.md)).

The order matters. For a holder on a flagged action, the platform-admin rule comes before the organization status check when the organization is suspended or has no organization row (unknown), because the legacy paths give a superuser the same answer there as in an active organization: no legacy guard reads the organization's status for a holder. A removed organization is still denied with `organization_inactive`. A member without `admin.orgs` in a suspended or unknown organization is still denied with `organization_inactive`. An unflagged organization action is still denied to a holder in an unknown organization.

The flag is on three sets of actions:

- The organization actions of the go-api `policy.Admin` and `policy.AdminOrg` rows, which admit an `admin.orgs` holder before they read the membership: `billing.manage`, `integrations.read`, `integrations.write`, `members.invite`, `members.manage`, `settings.write` and `teams.write`.
- The query-api actions whose legacy path admits a superuser in any organization: `analytics.read`, `metrics.read`, `git.read`, `work_items.read` and `reports.write`. `analytics.export` is not flagged.
- The go-api billing and license reads, whose `policy.Authenticated` rows reach handlers that admit an `admin.billing` holder before any membership read: `billing.read` and `license.read`. The flag keys on `admin.orgs`; a unit test holds that every platform role with `admin.orgs` also holds `admin.billing`.

A test in `internal/auth/contracts` holds the flag equal to those three sets. It also fails when a profile row outside them asks a flagged action, or when a flagged action is `repository_scope: filtered` or has a redaction rule. A second test fails when a profile row or GraphQL field asks an unflagged organization action. The one exception is `org.read` on query-api `GET /buildinfo`, which never asks the decision point.

## Platform actions

The catalog holds 10 platform actions: `admin.users`, `admin.orgs`, `admin.impersonate`, `authz.decide`, `admin.billing`, `admin.flags`, `admin.audit`, `admin.telemetry`, `credentials.project` and `admin.stats`. Each is high risk with cache class `none`. `platform_admin` holds 8 of them. `authz.decide` and `credentials.project` are held by no role.
