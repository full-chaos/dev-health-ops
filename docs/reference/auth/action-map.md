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
  - internal/queryapi/graph/field_actions.go
  - internal/queryapi/server/route_actions.go
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

The rows are held to source by Go tests. The profile rows are checked against the served routes. The GraphQL map is checked against the served schema and against handler source (`internal/queryapi/graph/field_action_map_test.go`). Nothing in the serving path reads the rows, so a runtime change does not fail a row. A planned check drives each route and field through the real handler. The query-api and GraphQL actions are held to two Go tables (`internal/queryapi/server/route_actions.go`, `internal/queryapi/graph/field_actions.go`) and to a parity cell that compares each action's catalog role grants with what today's guard admits.

!!! warning "Do not read a mapped action as an allow"
    The `billing.read` routes are at the `Authenticated` level today, so any valid token passes the route level. The catalog gives `billing.read` to every organization role, so the two agree here. For other routes the level and the catalog can differ, and the row says so in its `gaps`. Read the route level to know what is enforced.

## Row fields

Each profile row (`contracts/auth/v1/endpoint-profile.schema.json`) has an `action` field with three states:

| State | Meaning |
| --- | --- |
| absent | Not applicable. The surface has nothing to say (all 33 public go-api rows). |
| a catalog action id | The catalog action the route maps to. |
| `null` (go-api) | Undetermined. The row's `gaps` must hold an entry that begins with `action:`. |
| `"unknown"` (query-api and the GraphQL map) | Not allowed. Every protected query-api row and every GraphQL field names a catalog action. |

## go-api

252 rows: 158 map to one of 19 catalog actions, 59 have no action with a stated gap, and 33 (the public rows) have no action field. 2 public rows carry `action: null`.

### Routes with an action

| Action | Method and route | Route level today | Tenant requirement |
| --- | --- | --- | --- |
| `teams.write` | `GET /api/v1/admin/identities` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/identities` | AdminOrg | `org_scoped` |
| `teams.write` | `GET /api/v1/admin/teams` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/teams` | AdminOrg | `org_scoped` |
| `teams.write` | `DELETE /api/v1/admin/teams/{team_id}` | AdminOrg | `org_scoped` |
| `teams.write` | `GET /api/v1/admin/teams/{team_id}` | AdminOrg | `org_scoped` |
| `teams.write` | `PATCH /api/v1/admin/teams/{team_id}` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/teams/{team_id}` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/teams/{team_id}/approve-changes` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/teams/{team_id}/confirm-inferred-members` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/teams/{team_id}/confirm-members` | AdminOrg | `org_scoped` |
| `teams.write` | `GET /api/v1/admin/teams/{team_id}/discover-members` | AdminOrg | `org_scoped` |
| `teams.write` | `POST /api/v1/admin/teams/{team_id}/dismiss-changes` | AdminOrg | `org_scoped` |
| `teams.write` | `GET /api/v1/admin/teams/{team_id}/infer-members` | AdminOrg | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/ip-allowlist` | Admin | `org_scoped` |
| `settings.write` | `POST /api/v1/admin/ip-allowlist` | Admin | `org_scoped` |
| `settings.write` | `DELETE /api/v1/admin/ip-allowlist/{entry_id}` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/ip-allowlist/{entry_id}` | Admin | `org_scoped` |
| `settings.write` | `PATCH /api/v1/admin/ip-allowlist/{entry_id}` | Admin | `org_scoped` |
| `settings.write` | `POST /api/v1/admin/ip-allowlist/{entry_id}` | Admin | `org_scoped` |
| `settings.write` | `DELETE /api/v1/admin/llm-settings` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings` | Admin | `org_scoped` |
| `settings.write` | `PUT /api/v1/admin/llm-settings` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings/budget` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings/models` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings/own-model` | Admin | `org_scoped` |
| `settings.write` | `PUT /api/v1/admin/llm-settings/own-model` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings/prompt-cache` | Admin | `org_scoped` |
| `settings.write` | `PUT /api/v1/admin/llm-settings/prompt-cache` | Admin | `org_scoped` |
| `settings.write` | `POST /api/v1/admin/llm-settings/readiness` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings/service-model` | Admin | `org_scoped` |
| `settings.write` | `PUT /api/v1/admin/llm-settings/service-model` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings/spend` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/llm-settings/status` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/retention-policies` | Admin | `org_scoped` |
| `settings.write` | `POST /api/v1/admin/retention-policies` | Admin | `org_scoped` |
| `settings.write` | `DELETE /api/v1/admin/retention-policies/{policy_id}` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/retention-policies/{policy_id}` | Admin | `org_scoped` |
| `settings.write` | `PATCH /api/v1/admin/retention-policies/{policy_id}` | Admin | `org_scoped` |
| `settings.write` | `POST /api/v1/admin/retention-policies/{policy_id}/execute` | Admin | `org_scoped` |
| `settings.write` | `POST /api/v1/admin/settings` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/settings/{category}` | Admin | `org_scoped` |
| `settings.write` | `DELETE /api/v1/admin/settings/{category}/{key}` | Admin | `org_scoped` |
| `settings.write` | `GET /api/v1/admin/settings/{category}/{key}` | Admin | `org_scoped` |
| `settings.write` | `PUT /api/v1/admin/settings/{category}/{key}` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/backfill-jobs` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/backfill-jobs/{job_id}` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/credentials` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/credentials/{provider}/{name}` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/batches/{ingestion_id}` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/schemas` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/schemas/{schema_version}` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/sources` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/sources/{source_id}` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/sources/{source_id}/batches` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/sources/{source_id}/tokens` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/customer-push/tokens` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/pagerduty/services` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/pagerduty/status` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}` | Admin | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/{integration_id}` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/{integration_id}/datasets` | AdminOrg | `org_scoped` |
| `integrations.read` | `POST /api/v1/admin/integrations/{integration_id}/discover` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/integrations/{integration_id}/sources` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-configs` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-configs/auto-import-capabilities` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-configs/{config_id}` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-configs/{config_id}/coverage` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-configs/{config_id}/jobs` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-configs/{config_id}/repositories` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-runs/{run_id}` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-runs/{run_id}/units` | AdminOrg | `org_scoped` |
| `integrations.read` | `GET /api/v1/admin/sync-targets` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/credentials` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/credentials/test` | AdminOrg | `org_scoped` |
| `integrations.write` | `DELETE /api/v1/admin/credentials/{provider}/{name}` | Admin | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/credentials/{provider}/{name}` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/customer-push/sources` | Admin | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/customer-push/sources/{source_id}` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/customer-push/sources/{source_id}/tokens` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/customer-push/sources/{source_id}/validate` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/customer-push/tokens` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/customer-push/tokens/{token_id}/revoke` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/customer-push/tokens/{token_id}/rotate` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/github/install-callback` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/github/install-url` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/api-token` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/authorize` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/callback` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/client-credentials` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/disconnect` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/preflight` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke` | Admin | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate` | Admin | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/integrations/{integration_id}` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/{integration_id}/backfill` | AdminOrg | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/integrations/{integration_id}/datasets` | AdminOrg | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/integrations/{integration_id}/sync` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/sync-configs` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/sync-configs/batch` | AdminOrg | `org_scoped` |
| `integrations.write` | `DELETE /api/v1/admin/sync-configs/{config_id}` | AdminOrg | `org_scoped` |
| `integrations.write` | `PATCH /api/v1/admin/sync-configs/{config_id}` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/sync-configs/{config_id}/backfill` | AdminOrg | `org_scoped` |
| `integrations.write` | `PUT /api/v1/admin/sync-configs/{config_id}/repositories` | AdminOrg | `org_scoped` |
| `integrations.write` | `POST /api/v1/admin/sync-configs/{config_id}/trigger` | AdminOrg | `org_scoped` |
| `members.invite` | `POST /api/v1/admin/orgs/{org_id}/invites` | Admin | `org_scoped` |
| `members.manage` | `GET /api/v1/admin/orgs/{org_id}/members` | Admin | `org_scoped` |
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
| `org.read` | `GET /api/v1/orgs/me` | Authenticated | `org_scoped` |
| `org.write` | `PATCH /api/v1/orgs/me` | Authenticated | `org_scoped` |
| `acr.credential.revoke` | `POST /api/v1/acr/credentials/{credential_id}/revoke` | Authenticated | `org_scoped` |
| `admin.users` | `POST /api/v1/admin/service-accounts/keys/{credential_id}/revoke` | Superuser | `cross_org_superuser_only` |
| `admin.users` | `GET /api/v1/admin/service-accounts/{service_account_id}/keys` | Superuser | `cross_org_superuser_only` |
| `admin.users` | `POST /api/v1/admin/service-accounts/{service_account_id}/keys` | Superuser | `cross_org_superuser_only` |
| `admin.users` | `POST /api/v1/admin/service-accounts/{service_account_id}/keys/rotate` | Superuser | `cross_org_superuser_only` |
| `admin.users` | `GET /api/v1/admin/users` | Admin | `org_scoped` |
| `admin.users` | `POST /api/v1/admin/users` | Admin | `org_scoped` |
| `admin.users` | `DELETE /api/v1/admin/users/{user_id}` | Admin | `org_scoped` |
| `admin.users` | `GET /api/v1/admin/users/{user_id}` | Admin | `org_scoped` |
| `admin.users` | `PATCH /api/v1/admin/users/{user_id}` | Admin | `org_scoped` |
| `admin.users` | `POST /api/v1/admin/users/{user_id}/password` | Admin | `org_scoped` |
| `admin.orgs` | `GET /api/v1/admin/orgs` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `POST /api/v1/admin/orgs` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `DELETE /api/v1/admin/orgs/{org_id}` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `GET /api/v1/admin/orgs/{org_id}` | Superuser | `cross_org_superuser_only` |
| `admin.orgs` | `PATCH /api/v1/admin/orgs/{org_id}` | Superuser | `cross_org_superuser_only` |
| `admin.impersonate` | `POST /api/v1/admin/impersonate` | Superuser | `cross_org_superuser_only` |
| `admin.impersonate` | `POST /api/v1/admin/impersonate/handoff/consume` | Superuser | `cross_org_superuser_only` |
| `admin.impersonate` | `POST /api/v1/admin/impersonate/stop` | Superuser | `cross_org_superuser_only` |
| `admin.billing` | `POST /api/v1/billing/plans/pull-stripe` | Superuser | `cross_org_superuser_only` |
| `admin.flags` | `GET /api/v1/admin/feature-flags` | Superuser | `cross_org_superuser_only` |
| `admin.flags` | `PATCH /api/v1/admin/feature-flags/{flag_id}` | Superuser | `cross_org_superuser_only` |
| `admin.flags` | `GET /api/v1/admin/orgs/{org_id}/feature-overrides` | Superuser | `cross_org_superuser_only` |
| `admin.flags` | `POST /api/v1/admin/orgs/{org_id}/feature-overrides` | Superuser | `cross_org_superuser_only` |
| `admin.flags` | `DELETE /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}` | Superuser | `cross_org_superuser_only` |
| `admin.flags` | `PATCH /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}` | Superuser | `cross_org_superuser_only` |
| `admin.audit` | `GET /api/v1/admin/platform/audit-logs` | Superuser | `cross_org_superuser_only` |
| `admin.stats` | `GET /api/v1/admin/platform/stats` | Superuser | `cross_org_superuser_only` |
| `admin.telemetry` | `POST /api/v1/telemetry/report` | Superuser | `org_scoped` |

Source: `contracts/auth/v1/endpoint-profiles.go-api.json`. "Route level today" is the `policy.*` validator named in the row.

These 19 catalog actions have no go-api route that maps to them: `analytics.read`, `analytics.export`, `metrics.read`, `metrics.compute`, `work_items.read`, `work_items.sync`, `git.read`, `git.sync`, `teams.read`, `settings.read`, `reports.write`, `members.read`, `members.update`, `org.delete`, `acr.context.read`, `acr.evidence.read`, `acr.credential.issue`, `authz.decide`, `credentials.project`. Of these, query-api rows map to `analytics.read`, `metrics.read`, `work_items.read`, `git.read` (see below).

### Routes with no action

59 protected rows carry `action: null` and a gap (54 on the api listener, 4 on the internal listener, 1 on the billing-edge listener). The gap text falls into these groups:

| Reason | Rows |
| --- | --- |
| no catalog action covers the route's resource | 37 |
| a check exists but is not proven equal to one catalog action | 8 |
| other (see the row) | 14 |

The first group names a resource that the catalog has no action for. The resources, taken from the gap text, with the number of rows for each:

| Resource | Rows |
| --- | --- |
| the session and onboarding routes | 10 |
| SSO provider management; no settings action is mapped because the handler check is not proven to equal one | 9 |
| audit log | 4 |
| the internal service entitlement and configuration reads | 4 |
| user | 4 |
| the payment provider webhook; the signature is its only credential | 2 |
| setup status | 1 |
| the build identity read | 1 |
| the incident provider webhook; the binding signature is its only credential | 1 |
| the source provider webhook; the signature is its only credential | 1 |

Each gap entry says "this leaf does not extend the vocabulary": the profile leaf that wrote the rows recorded the missing action and did not add one.

## query-api

45 rows: 37 protected and 8 public. Every protected row names a catalog action; the public rows name none. Each action is the one whose catalog role grants allow exactly what today's guard allows (parity): every organization role holds `analytics.read`, `metrics.read`, `work_items.read`, `git.read` and `org.read`, and today's guard reads the caller's organization and no role. query-api enforces none of these actions yet; the rows state a mapping for a later decision request, not a check. query-api has no single authorization chokepoint, so per-resolver checks are not profiled.

The six dispatcher rows (`/graphql`, `/query` on the query and internal listeners and on the mcp listener, `/query/proof`, `/query/run-operation`) carry the minimum action the legacy guard requires at the route, `analytics.read`; the action of each root field, in the GraphQL field map below, decides per operation. `/buildinfo` is `org.read` (legacy admits any verified credential) and `/query/proof-write` is `analytics.read` (legacy requires an allowlisted organization and no role).

**Deliberate change candidates, not made here:** `/buildinfo` to `admin.telemetry`; `/query/proof-write` to `settings.write`; the MCP caller class refuses an identity with the admin, owner or operator role while the catalog grants `analytics.read` to admin and owner (`POST /query` on the mcp listener).

| Method and route | Class | Action | Accepted credential classes |
| --- | --- | --- | --- |
| `api_route /api/v1/` | public | none | none |
| `GET /api/v1/drilldown/issues` | protected | `work_items.read` | `user_access_token` |
| `POST /api/v1/drilldown/issues` | protected | `work_items.read` | `user_access_token` |
| `GET /api/v1/drilldown/prs` | protected | `git.read` | `user_access_token` |
| `POST /api/v1/drilldown/prs` | protected | `git.read` | `user_access_token` |
| `GET /api/v1/explain` | protected | `metrics.read` | `user_access_token` |
| `POST /api/v1/explain` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/filters/options` | protected | `analytics.read` | `user_access_token` |
| `GET /api/v1/flame` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/flame/aggregated` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/heatmap` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/home` | protected | `analytics.read` | `user_access_token` |
| `POST /api/v1/home` | protected | `analytics.read` | `user_access_token` |
| `GET /api/v1/investment` | protected | `analytics.read` | `user_access_token` |
| `POST /api/v1/investment` | protected | `analytics.read` | `user_access_token` |
| `POST /api/v1/investment/explain` | protected | `analytics.read` | `user_access_token` |
| `GET /api/v1/investment/sunburst` | protected | `analytics.read` | `user_access_token` |
| `GET /api/v1/meta` | public | none | none |
| `GET /api/v1/opportunities` | protected | `analytics.read` | `user_access_token` |
| `POST /api/v1/opportunities` | protected | `analytics.read` | `user_access_token` |
| `GET /api/v1/people` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/people/{person_id}/drilldown/issues` | protected | `work_items.read` | `user_access_token` |
| `GET /api/v1/people/{person_id}/drilldown/prs` | protected | `git.read` | `user_access_token` |
| `GET /api/v1/people/{person_id}/metric` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/people/{person_id}/summary` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/quadrant` | protected | `metrics.read` | `user_access_token` |
| `GET /api/v1/sankey` | protected | `analytics.read` | `user_access_token` |
| `POST /api/v1/sankey` | protected | `analytics.read` | `user_access_token` |
| `GET /api/v1/work-units` | protected | `analytics.read` | `user_access_token` |
| `POST /api/v1/work-units` | protected | `analytics.read` | `user_access_token` |
| `POST /api/v1/work-units/{work_unit_id}/explain` | protected | `analytics.read` | `user_access_token` |
| `GET /buildinfo` | protected | `org.read` | `user_access_token` (a platform admin only) |
| `GET /graphql` | protected | `analytics.read` | `user_access_token` |
| `POST /graphql` | protected | `analytics.read` | `user_access_token` |
| `api_route /healthz` | public | none | none |
| `GET /healthz` | public | none | none |
| `GET /metrics` | public | none | none |
| `POST /query` | protected | `analytics.read` | `user_access_token` |
| `POST /query` | protected | `analytics.read` | none |
| `POST /query/proof` | protected | `analytics.read` | `user_access_token` |
| `POST /query/proof-write` | protected | `analytics.read` | `user_access_token` |
| `POST /query/run-operation` | protected | `analytics.read` | `user_access_token` |
| `api_route /readyz` | public | none | none |
| `GET /readyz` | public | none | none |
| `GET /registry` | public | none | none |

Source: `contracts/auth/v1/endpoint-profiles.query-api.json`.

## auth-service

The 48 auth-service rows have no `action` field. See the route table in [authentication and authorization](authn-authz.md#auth-service).

## GraphQL field map

`contracts/auth/v1/graphql-field-actions.json` lists every served root field: 51 fields (46 `Query`, 5 `Mutation`). The served schema (`contracts/graphql/v1/schema.graphql`) has a `Query` and a `Mutation` type and no `Subscription` type. The guard test `internal/queryapi/graph/field_action_map_test.go` fails when a served field has no row or has more than one, when a row names a field that is not served, when the map's `catalog_version` differs from the catalog's, when an action is empty, `unknown`, not in the catalog or not the field action table's, when a platform action sits on a field whose resolver is not `none`, when an action row still carries an `action:` gap, or when a statement fails its source proof.

Every field names a catalog action, the same one `internal/queryapi/graph/field_actions.go` holds. query-api enforces none of them today. Each action allows exactly what today's guard allows (parity):

| Action | Fields |
| --- | --- |
| `admin.telemetry` (1) | `Query.productTelemetryPlatformDashboard` |
| `analytics.read` (28) | `Query.catalog`, `Query.analytics`, `Query.productTelemetryDashboard`, `Query.home`, `Query.featureFlags`, `Query.featureFlagEvents`, `Query.securityAlerts`, `Query.securityOverview`, `Query.savedReports`, `Query.savedReport`, `Query.reportRuns`, `Query.capacityForecast`, `Query.capacityForecasts`, `Query.throughputForecast`, `Query.operatingReview`, `Query.sourceHealth`, `Query.recommendations`, `Query.experiments`, `Query.aiImpactSummary`, `Query.aiComparison`, `Query.aiReviewLoad`, `Query.aiRiskBreakdown`, `Query.aiOpportunities`, `Query.improveOpportunities`, `Query.aiGovernanceSummary`, `Query.aiWorkflowDrilldown`, `Query.aiAttributedPrs`, `Query.aiAttributionOverview` |
| `git.read` (5) | `Query.workGraphEdges`, `Query.pr`, `Query.workGraphFlow`, `Query.workGraphArtifacts`, `Query.reviewEdges` |
| `integrations.read` (1) | `Query.dataHealth` |
| `metrics.read` (9) | `Query.busFactor`, `Query.compoundingRisk`, `Query.testopsRisk`, `Query.testopsJobFailures`, `Query.coverageBaselines`, `Query.coverageScopeBaseline`, `Query.complexityTimeseries`, `Query.hotspots`, `Query.cognitiveLoad` |
| `reports.write` (5) | `Mutation.createSavedReport`, `Mutation.updateSavedReport`, `Mutation.deleteSavedReport`, `Mutation.cloneSavedReport`, `Mutation.triggerReport` |
| `work_items.read` (2) | `Query.workItemTeamAttributions`, `Query.workUnitTeamAttributions` |

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

7 fields have their own check: `Query.productTelemetryPlatformDashboard`, `Query.dataHealth`, `Mutation.createSavedReport`, `Mutation.updateSavedReport`, `Mutation.deleteSavedReport`, `Mutation.cloneSavedReport`, `Mutation.triggerReport`.

- `productTelemetryPlatformDashboard` is refused with `AUTHORIZATION_ERROR` unless `producttelemetry.RequirePlatformAdmin` passes the caller, which requires `admin.telemetry` outside an impersonation: its action is `admin.telemetry`, an exact match.
- `dataHealth` is refused with `AUTHORIZATION_ERROR` unless `datahealth.RequireOperator` passes the caller: a holder of `admin.stats`, or the role admin, owner or operator. Its action is `integrations.read`, which the organization roles admin and owner hold. The `operator` role and `admin.stats` holders have no organization role in the catalog; that difference is a deliberate change candidate.
- The five saved-report mutations apply `requireMutationOrg` after the guard: `Authorization required` when the caller has no organization, `Access denied` when the `orgId` is not the caller's organization. No role is read, so every organization role can write them today: their action is the new `reports.write` (catalog version 12), held by viewer, member, admin and owner.
- `sourceHealth` has no role gate, only the organization guard, so its action is `analytics.read`. Narrowing it to `integrations.read` (admin and owner) is a deliberate change candidate, not made here.
- `complexityTimeseries` answers `AUTHORIZATION_ERROR` for a request with no organization.

Source for this section: the `disclosure_behavior` and `gaps` members of `contracts/auth/v1/graphql-field-actions.json`.

## Planned, not built

| Item | Status |
| --- | --- |
| Check each query-api route and GraphQL field against live behaviour. | Planned |
| Narrow, widen or split any query-api action away from today's guard (the deliberate change candidates above). | Decided later, on its own |
| Explicit platform actions for the superuser-only handlers. Not in the catalog on `main`. | Planned |
| Replace the route levels with decisions from the policy decision point. | Planned |

The rows in this page are as on `main`. A pending catalog change will change the action and gap text of the affected rows.
