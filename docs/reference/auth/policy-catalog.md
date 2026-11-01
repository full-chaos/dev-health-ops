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

25 actions are organization actions. 3 are platform actions. 16 are high risk. 12 are low risk.

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
| `admin.users` | platform | platform | high | `none` |
| `admin.orgs` | platform | platform | high | `none` |
| `authz.decide` | platform | platform | high | `none` |

Source: `contracts/auth/v1/vocabulary/actions.v1.json`. `resource_kind` equals `scope` for every action except `acr.context.read`, whose resource is a repository. The kind set is `platform`, `organization` and `repository`; a scope is `platform` or `organization` only.

### Risk class and cache class

| Cache class | Actions |
| --- | --- |
| `none` | All 16 high-risk actions. A decision for these is never reused. |
| `decision_bound` | The low-risk actions except `git.read` and `acr.context.read`. |
| `resource_revision_bound` | `git.read` and `acr.context.read`. |

The two cached classes are the allow-cache classes of TRD section 14. The catalog files do not define them further. A cached allow is bound to the policy revision of its decision and ends at the decision's `expires_at` (`authorization_decision.v1`, see [obligations](obligations.md#decision)).

A high-risk action must have cache class `none`. The loader refuses a catalog that breaks this rule (`internal/auth/policycatalog/catalog.go`, `Load`). The README of the contracts directory points to TRD section 14 for the allow-cache classes.

## Roles

| Role | Scope | Actions held |
| --- | --- | --- |
| `viewer` | organization | 10 of 28 |
| `member` | organization | 11 of 28 |
| `admin` | organization | 22 of 28 |
| `owner` | organization | 23 of 28 |
| `platform_admin` | platform | 2 of 28 |
| `platform_owner` | platform | 0 of 28 |

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
| `acr.context.read` |  |  |  |  |  |
| `admin.users` |  |  |  |  | yes |
| `admin.orgs` |  |  |  |  | yes |
| `authz.decide` |  |  |  |  |  |

No role holds `members.update`, `acr.context.read` or `authz.decide` yet, so the decision core denies them to every role until a roles change grants them.

Every action names `impersonation` (`denied` or `allowed`): whether a delegated session may exercise it. `acr.context.read` is `allowed` (a read, low risk). `members.update` and `authz.decide` are `denied`. Until a role holds `acr.context.read`, no delegated session can use it.

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
| `query-api` | none | none |

## Related closed vocabularies

`contracts/auth/v1/vocabulary/reason-codes.json` closes three wire fields:

| Field | Values |
| --- | --- |
| `authorization_decision.v1` `reason_code` | `allowed_by_repository_grant`, `grant_absent`, `repository_not_granted`, `platform_role_absent` |
| `audit-event.v1` `attributes.reason_code` | 61 values; the file lists them |
| `revoked_reason` of a credential | `compromised`, `owner_request` |

A value is added only with the pull request of the producer that emits it. `error.v1` `reason_code` is not governed by this file. It is shape-only.

## Version history

From `contracts/auth/v1/README.md`:

- Version 1: the closed action and role catalogs (CHAOS-10637).
- Version 2: every action gets a `risk_class` and a `cache_class` (CHAOS-10641).
- Version 3: `billing.read`, `billing.manage` and `license.read` (CHAOS-11272). They are held as the billing and licensing routes allow today. They are catalog-only: `/auth/me` does not serve them until a web-reviewed change does.
- Version 4: `acr.context.read` (organization scope, repository resource kind, low risk, `resource_revision_bound`), `members.update` (organization, high, `none`) and `authz.decide` (platform, high, `none`) (CHAOS-10774). The resource-kind set gains `repository`. The cached-action exemption list is gone.
- Version 5: the role `platform_owner` (platform scope, no action) (CHAOS-12294). No action changed, so no decision changes.
- Version 6: every action gets a required `impersonation` member (`denied` or `allowed`), and the action `admin.impersonate` (CHAOS-10987).
- Version 8: `acr.context.read` is `allowed` under a delegated session (CHAOS-12401). No role changed, so no decision changes.

## Planned, not built

The catalog on `main` has no platform action other than `admin.users`, `admin.orgs` and `authz.decide`. The superuser-inherited sites in go-api have no explicit platform action yet. CHAOS-11847 plans `admin.billing`, `admin.flags`, `admin.audit`, `admin.impersonate`, `admin.telemetry` and `admin.stats`. CHAOS-10987 (delegated session) also changes the catalog. Neither change is on `main`, and the version number each will take is not fixed here.
