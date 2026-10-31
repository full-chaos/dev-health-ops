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

The catalog is the closed list of actions and roles of the Auth Control Plane. It is catalog version 3 on `main`: 25 actions and 5 roles. An action or role that is not in the catalog is refused everywhere.
{: .fc-page-lede }

The JSON files are the single source. `internal/auth/policycatalog` embeds them, parses them and validates them at start. A defect panics the process, so a bad catalog never serves. See also [authentication and authorization](authn-authz.md), [action map](action-map.md) and [obligations](obligations.md).

## Actions

Names are dotted and namespaced. A bare single segment is not an action (the pattern is `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`). No action means "all actions".

23 actions are organization actions. 2 are platform actions. 14 are high risk. 11 are low risk.

| Action | Scope | Risk class | Cache class |
| --- | --- | --- | --- |
| `analytics.read` | organization | low | `decision_bound` |
| `analytics.export` | organization | high | `none` |
| `metrics.read` | organization | low | `decision_bound` |
| `metrics.compute` | organization | high | `none` |
| `work_items.read` | organization | low | `decision_bound` |
| `work_items.sync` | organization | high | `none` |
| `git.read` | organization | low | `resource_revision_bound` |
| `git.sync` | organization | high | `none` |
| `teams.read` | organization | low | `decision_bound` |
| `teams.write` | organization | high | `none` |
| `settings.read` | organization | low | `decision_bound` |
| `settings.write` | organization | high | `none` |
| `integrations.read` | organization | low | `decision_bound` |
| `integrations.write` | organization | high | `none` |
| `members.read` | organization | low | `decision_bound` |
| `members.invite` | organization | high | `none` |
| `members.manage` | organization | high | `none` |
| `org.read` | organization | low | `decision_bound` |
| `org.write` | organization | high | `none` |
| `org.delete` | organization | high | `none` |
| `billing.read` | organization | low | `decision_bound` |
| `billing.manage` | organization | high | `none` |
| `license.read` | organization | low | `decision_bound` |
| `admin.users` | platform | high | `none` |
| `admin.orgs` | platform | high | `none` |

Source: `contracts/auth/v1/vocabulary/actions.v1.json`. In this catalog `resource_kind` equals `scope` for every action.

### Risk class and cache class

| Cache class | Actions |
| --- | --- |
| `none` | All 14 high-risk actions. A decision for these is never reused. |
| `decision_bound` | The low-risk actions except `git.read`. |
| `resource_revision_bound` | `git.read` only. |

The two cached classes are the allow-cache classes of TRD section 14. The catalog files do not define them further. A cached allow is bound to the policy revision of its decision and ends at the decision's `expires_at` (`authorization_decision.v1`, see [obligations](obligations.md#decision)).

A high-risk action must have cache class `none`. The loader refuses a catalog that breaks this rule (`internal/auth/policycatalog/catalog.go`, `Load`). The README of the contracts directory points to TRD section 14 for the allow-cache classes.

## Roles

| Role | Scope | Actions held |
| --- | --- | --- |
| `viewer` | organization | 10 of 25 |
| `member` | organization | 11 of 25 |
| `admin` | organization | 22 of 25 |
| `owner` | organization | 23 of 25 |
| `platform_admin` | platform | 2 of 25 |

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
| `org.read` | yes | yes | yes | yes |  |
| `org.write` |  |  | yes | yes |  |
| `org.delete` |  |  |  | yes |  |
| `billing.read` | yes | yes | yes | yes |  |
| `billing.manage` |  |  | yes | yes |  |
| `license.read` | yes | yes | yes | yes |  |
| `admin.users` |  |  |  |  | yes |
| `admin.orgs` |  |  |  |  | yes |

Source: `contracts/auth/v1/vocabulary/roles.v1.json`. The role keys `viewer`, `member`, `admin` and `owner` are the organization roles that `membership.v1` uses. `platform_admin` is a platform role and is not a membership role.

## Rules the loader enforces

`policycatalog.Load` refuses the catalog (`ErrInvalid`) when any of these hold:

- A file has a duplicate object member, an unknown member, or trailing data.
- `schema_version` is not `actions.v1` and `roles.v1`, or the two `catalog_version` values differ or are below 1.
- An action name does not match the pattern, is repeated, or has an unknown scope, risk class or cache class.
- A high-risk action is cached.
- A role name does not match `^[a-z][a-z0-9_]*$`, is repeated, or is one of the reserved names `superuser`, `root`, `all`, `any`, `everyone`.
- A role lists an unknown action, lists one twice, or lists an action of the other scope.
- A role holds every action.

`Allows(role, action)` is true only for a known role that lists a known action.

## Rules the profile gate enforces

`CheckActionCachePolicy` (`internal/auth/contracts/endpoint_auth_profile_selector_gate.go`) joins an endpoint profile to the catalog:

- A profile that caches must name an action.
- The action must be in the catalog. One action is exempt today, `acr.context.read`, which is not catalogued yet (CHAOS-10774).
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

## Planned, not built

The catalog on `main` has no platform action other than `admin.users` and `admin.orgs`. The superuser-inherited sites in go-api have no explicit platform action yet. CHAOS-11847 plans `admin.billing`, `admin.flags`, `admin.audit`, `admin.impersonate`, `admin.telemetry` and `admin.stats`. CHAOS-10987 (delegated session) also changes the catalog. Neither change is on `main`, and the version number each will take is not fixed here. CHAOS-10774 plans to catalogue the `acr.*` actions with risk and cache classes.
