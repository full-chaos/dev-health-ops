---
page_id: ref-auth-action-map
summary: Which catalog action each go-api route, query-api route and GraphQL root field maps to, what is declared as a gap and why, and what actually enforces access today.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/endpoint-profiles.go-api.json
  - contracts/auth/v1/endpoint-profiles.query-api.json
  - contracts/auth/v1/endpoint-profiles.auth-service.json
  - contracts/auth/v1/graphql-field-actions.json
  - contracts/auth/v1/endpoint-profile.schema.json
  - internal/queryapi/graph/field_action_map_test.go
  - internal/queryapi/graph/operation_org_guard.go
  - contracts/graphql/v1/schema.graphql
  - internal/api/policy/guard.go
applicability: current
lifecycle: active
---

# Action map reference

The action map joins each route and GraphQL field to a catalog action from the [policy catalog](policy-catalog.md). It is data. It states the mapping. It is not the check that runs on a request.
{: .fc-page-lede }

## What enforces today

| Surface | What a request meets | Catalog action checked? |
| --- | --- | --- |
| go-api | One of six route levels from `internal/api/policy/guard.go`, then the handler's own checks. See [authentication and authorization](authn-authz.md#route-levels). | No |
| query-api REST | A valid credential and an organization (`authenticateRESTRequest`). | No |
| query-api GraphQL | A valid credential, then `OperationOrgGuard` for the `orgId` argument. | No |
| auth-service | The credential class and scope of the route. | No |

The rows are held to source by Go tests. The profile rows are checked against the served routes. The GraphQL map is checked against the served schema and against handler source (`internal/queryapi/graph/field_action_map_test.go`). Nothing in the serving path reads the rows, so a runtime change does not fail a row. CHAOS-11042 plans a check that drives each route and field through the real handler.

!!! warning "Do not read a mapped action as an allow"
    The `billing.read` routes are at the `Authenticated` level today, so any valid token passes the route level. The catalog gives `billing.read` to every organization role, so the two agree here. For other routes the level and the catalog can differ, and the row says so in its `gaps`. Read the route level to know what is enforced.

## Row fields

Each profile row (`contracts/auth/v1/endpoint-profile.schema.json`) has an `action` field with three states:

| State | Meaning |
| --- | --- |
| absent | Not applicable. The surface has nothing to say (all 33 public go-api rows). |
| a catalog action id | The catalog action the route maps to. |
| `null` (go-api) or `"unknown"` (query-api and the GraphQL map) | Undetermined. The row's `gaps` must hold an entry that begins with `action:`. The query-api and GraphQL gap text also says that no consumer may treat the row as allow. |

## go-api

242 rows: 53 map to one of 13 catalog actions, 156 have no action with a stated gap, and 33 (the public rows) have no action field.

### Routes with an action

| Action | Method and route | Route level today | Tenant requirement |
| --- | --- | --- | --- |
| `teams.read` | `GET /api/v1/admin/teams` | AdminOrg | `org_scoped` |
| `teams.read` | `GET /api/v1/admin/teams/{team_id}` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/teams` | AdminOrg | `org_scoped` |
| `teams.write` | `DELETE /api/v1/admin/teams/{team_id}` | AdminOrg | `org_scoped` |
| `teams.write` | `PATCH /api/v1/admin/teams/{team_id}` | AdminOrg | `org_scoped` |
| `settings.read` | `GET /api/v1/admin/settings/{category}` | Admin | `org_scoped` |
| `settings.read` | `GET /api/v1/admin/settings/{category}/{key}` | Admin | `org_scoped` |
| `settings.write` | `POST /api/v1/admin/settings` | Admin | `org_scoped` |
| `settings.write` | `DELETE /api/v1/admin/settings/{category}/{key}` | Admin | `org_scoped` |
| `settings.write` | `PUT /api/v1/admin/settings/{category}/{key}` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/pagerduty/services` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/pagerduty/status` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/{integration_id}` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/{integration_id}/datasets` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/{integration_id}/sources` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/github/install-callback` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/api-token` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/authorize` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/callback` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/client-credentials` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/disconnect` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate` | Admin | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/integrations/{integration_id}` | AdminOrg | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/integrations/{integration_id}/datasets` | AdminOrg | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}` | AdminOrg | `org_scoped` |
| `members.read` | `GET /api/v1/admin/orgs/{org_id}/members` | Admin | `org_scoped` |
| `members.invite` | `POST /api/v1/admin/orgs/{org_id}/invites` | Admin | `org_scoped` |
| `members.manage` | `POST /api/v1/admin/orgs/{org_id}/members` | Admin | `org_scoped` |
| `members.manage` | `DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}` | Admin | `org_scoped` |
| `members.manage` | `PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}` | Admin | `org_scoped` |
| `members.manage` | `POST /api/v1/admin/orgs/{org_id}/transfer-ownership` | Admin | `org_scoped` |
| `billing.read` | `GET /api/v1/billing/invoices` | Authenticated | `org_scoped` |
| `billing.read` | `GET /api/v1/billing/invoices/{invoice_id}` | Authenticated | `org_scoped` |
| `billing.read` | `GET /api/v1/billing/subscriptions` | Authenticated | `org_scoped` |
| `billing.read` | `GET /api/v1/billing/subscriptions/history` | Authenticated | `org_scoped` |
| `billing.read` | `GET /api/v1/billing/subscriptions/list` | Authenticated | `org_scoped` |
| `billing.manage` | `POST /api/v1/billing/invoices/{invoice_id}/void` | Admin | `org_scoped` |
| `billing.manage` | `POST /api/v1/billing/subscriptions/cancel` | Admin | `org_scoped` |
| `billing.manage` | `POST /api/v1/billing/subscriptions/change-plan` | Admin | `org_scoped` |
| `billing.manage` | `POST /api/v1/billing/subscriptions/reactivate` | Admin | `org_scoped` |
| `license.read` | `GET /api/v1/billing/entitlements/{org_id}` | Authenticated | `org_scoped` |
| `license.read` | `GET /api/v1/licensing/entitlements/{org_id}` | Authenticated | `org_scoped` |
| `admin.orgs` | `GET /api/v1/admin/orgs` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `POST /api/v1/admin/orgs` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `DELETE /api/v1/admin/orgs/{org_id}` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `GET /api/v1/admin/orgs/{org_id}` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `PATCH /api/v1/admin/orgs/{org_id}` | Superuser | `cross_org_superuser_only` |

Source: `contracts/auth/v1/endpoint-profiles.go-api.json`. "Route level today" is the `policy.*` validator named in the row.

These 12 catalog actions have no go-api route that maps to them: `analytics.read`, `analytics.export`, `metrics.read`, `metrics.compute`, `work_items.read`, `work_items.sync`, `git.read`, `git.sync`, `org.read`, `org.write`, `org.delete`, `admin.users`. No query-api row or GraphQL field maps to a catalog action either (see below).

### Routes with no action

156 protected rows carry `action: null` and a gap. The gap text falls into these groups:

| Reason | Rows |
| --- | --- |
| no catalog action covers the route's resource | 129 |
| superuser-only handler; the catalog has no platform action for it | 11 |
| a check exists but is not proven equal to one catalog action | 11 |
| other (see the row) | 5 |

The first group names a resource that the catalog has no action for. The resources, taken from the gap text, with the number of rows for each:

| Resource | Rows |
| --- | --- |
| LLM settings | 14 |
| sync configuration | 11 |
| the session and onboarding routes | 10 |
| SSO provider management; no settings action is mapped because the handler check is not proven to equal one | 9 |
| credential | 6 |
| ingest token | 6 |
| retention policy | 6 |
| user | 6 |
| IP allowlist entry | 5 |
| audit log | 5 |
| ingest source | 5 |
| feature override | 4 |
| the internal service entitlement and configuration reads | 4 |
| impersonation session | 3 |
| backfill job | 2 |
| feature flag | 2 |
| identity | 2 |
| ingest batch | 2 |
| ingest schema | 2 |
| team drift change | 2 |
| the payment provider webhook; the signature is its only credential | 2 |
| GitHub App install state | 1 |
| IP allowlist check | 1 |
| PagerDuty dataset scope preflight | 1 |
| auto-import capability table | 1 |
| integration backfill trigger | 1 |
| integration source | 1 |
| integration sync trigger | 1 |
| platform stats | 1 |
| setup status | 1 |
| sync run | 1 |
| sync run units | 1 |
| sync targets | 1 |
| team import | 1 |
| team member discovery (provider call) | 1 |
| team member inference (provider call) | 1 |
| team members | 1 |
| team members (inferred) | 1 |
| the build identity read | 1 |
| the incident provider webhook; the binding signature is its only credential | 1 |
| the source provider webhook; the signature is its only credential | 1 |
| the sync configuration domain | 1 |

Each gap entry says "this leaf does not extend the vocabulary": the profile leaf that wrote the rows recorded the missing action and did not add one.

Superuser-only handlers have no platform action to map to, because the catalog has `admin.users` and `admin.orgs` only. CHAOS-11847 plans explicit platform actions for them.

## query-api

45 rows: 37 protected and 8 public. Every protected row has `action: "unknown"` with a declared gap tracked in CHAOS-11042. The rows state what each surface accepts, not what each resolver enforces. query-api has no single authorization chokepoint, so per-resolver checks are not profiled.

| Method and route | Class | Accepted credential classes |
| --- | --- | --- |
| `api_route /api/v1/` | public | none |
| `GET /api/v1/drilldown/issues` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/drilldown/issues` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/drilldown/prs` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/drilldown/prs` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/explain` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/explain` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/filters/options` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/flame` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/flame/aggregated` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/heatmap` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/home` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/home` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/investment` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/investment` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/investment/explain` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/investment/sunburst` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/meta` | public | none |
| `GET /api/v1/opportunities` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/opportunities` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/people` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/people/{person_id}/drilldown/issues` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/people/{person_id}/drilldown/prs` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/people/{person_id}/metric` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/people/{person_id}/summary` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/quadrant` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/sankey` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/sankey` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /api/v1/work-units` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/work-units` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /api/v1/work-units/{work_unit_id}/explain` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `GET /buildinfo` | protected | `effective_principal_envelope` |
| `GET /graphql` | protected | `ops_access_token_hs256`, `user_access_token` |
| `POST /graphql` | protected | `ops_access_token_hs256`, `user_access_token` |
| `api_route /healthz` | public | none |
| `GET /healthz` | public | none |
| `GET /metrics` | public | none |
| `POST /query` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /query` | protected | none |
| `POST /query/proof` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /query/proof-write` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `POST /query/run-operation` | protected | `effective_principal_envelope`, `ops_access_token_hs256`, `user_access_token` |
| `api_route /readyz` | public | none |
| `GET /readyz` | public | none |
| `GET /registry` | public | none |

Source: `contracts/auth/v1/endpoint-profiles.query-api.json`.

## auth-service

The 48 auth-service rows have no `action` field. See the route table in [authentication and authorization](authn-authz.md#auth-service).

## GraphQL field map

`contracts/auth/v1/graphql-field-actions.json` lists every served root field: 51 fields (46 `Query`, 5 `Mutation`). The served schema (`contracts/graphql/v1/schema.graphql`) has a `Query` and a `Mutation` type and no `Subscription` type. The guard test `internal/queryapi/graph/field_action_map_test.go` fails when a served field has no row or has more than one, when a row names a field that is not served, when the map's `catalog_version` differs from the catalog's, when an action is not `unknown`, when an `unknown` has no gap that names CHAOS-11042, or when a statement fails its source proof.

Every row has `action: "unknown"`. query-api enforces no catalog action on a field today. Each row has a gap that names CHAOS-11042 and says no consumer may treat the row as allow.

### Which organization a field uses

| Source of the organization | Fields |
| --- | --- |
| The `orgId` argument, checked by `OperationOrgGuard` against the credential's organization (44 fields) | `catalog`, `analytics`, `productTelemetryDashboard`, `home`, `workGraphEdges`, `pr`, `workGraphFlow`, `workGraphArtifacts`, `featureFlags`, `featureFlagEvents`, `workItemTeamAttributions`, `workUnitTeamAttributions`, `securityAlerts`, `securityOverview`, `savedReports`, `savedReport`, `reportRuns`, `capacityForecast`, `capacityForecasts`, `throughputForecast`, `operatingReview`, `sourceHealth`, `busFactor`, `compoundingRisk`, `testopsRisk`, `testopsJobFailures`, `coverageBaselines`, `coverageScopeBaseline`, `recommendations`, `experiments`, `aiImpactSummary`, `aiComparison`, `aiReviewLoad`, `aiRiskBreakdown`, `aiOpportunities`, `aiGovernanceSummary`, `aiWorkflowDrilldown`, `aiAttributedPrs`, `aiAttributionOverview` and the mutations `createSavedReport`, `updateSavedReport`, `deleteSavedReport`, `cloneSavedReport`, `triggerReport` |
| The credential's organization (`claims.OrgID`); the field takes no `orgId` (6 fields) | `dataHealth`, `complexityTimeseries`, `hotspots`, `cognitiveLoad`, `reviewEdges`, `improveOpportunities` |
| None; a platform field | `productTelemetryPlatformDashboard` |

### Refusals before a field runs

For a field with an `orgId` argument, `OperationOrgGuard` answers the whole operation (data null) with one of these messages:

| Message | When |
| --- | --- |
| `A valid organization ID is required` | An `orgId` that is empty, padded or not a string. |
| `Only one organization may be queried per operation` | Differing `orgId` values in one operation. |
| `Authorization required` | The caller has no organization. |
| `Access denied: cannot query org` | The `orgId` is not the caller's organization. A verified, non-impersonating superuser with an organization may name another, and the operation then runs against it. |

The guard reads only fields' own arguments named `orgId` or `org_id`. An organization id inside an input object is not seen by the guard.

### Fields with their own check

7 rows say "no catalog action covers this field" instead of "not enforced": `Query.productTelemetryPlatformDashboard`, `Query.dataHealth`, `Mutation.createSavedReport`, `Mutation.updateSavedReport`, `Mutation.deleteSavedReport`, `Mutation.cloneSavedReport`, `Mutation.triggerReport`.

- `productTelemetryPlatformDashboard` is refused with `AUTHORIZATION_ERROR` unless `producttelemetry.RequirePlatformAdmin` passes the caller.
- `dataHealth` is refused with `AUTHORIZATION_ERROR` unless `datahealth.RequireOperator` passes the caller.
- The five saved-report mutations apply `requireMutationOrg` after the guard: `Authorization required` when the caller has no organization, `Access denied` when the `orgId` is not the caller's organization.
- `complexityTimeseries` answers `AUTHORIZATION_ERROR` for a request with no organization.

Source for this section: the `disclosure_behavior` and `gaps` members of `contracts/auth/v1/graphql-field-actions.json`.

## Planned, not built

| Item | Ticket |
| --- | --- |
| Check each query-api route and GraphQL field against live behaviour. | CHAOS-11042 |
| Explicit platform actions for the superuser-only handlers. Not in the catalog on `main`. | CHAOS-11847 |
| Catalog the `acr.*` actions and delete the cached-action exemption. | CHAOS-10774 |
| Replace the route levels with decisions from the policy decision point. | CHAOS-10841 |

The rows in this page are as on `main`. A pending catalog change will change the action and gap text of the affected rows.
