---
page_id: op-rb-auth-pdp-outage-rollback
summary: Recognise a policy decision point outage on go-api (503 on switched-on namespaces), confirm it, and roll the enforcement switch back without changing policy data.
content_type: runbook
owner: platform-operations
source_of_truth:
  - internal/api/policy/enforce.go
  - internal/api/policy/enforcecounters.go
  - internal/api/policy/guard.go
  - internal/api/policy/enforce_test.go
  - internal/apiservice/enforcer.go
  - internal/authzenforce/authzenforce.go
  - internal/queryapi/pdp/pdp.go
  - internal/queryapi/pdp/operation.go
  - internal/queryapi/server/pdp_gate.go
  - internal/queryapi/server/graphql_enforce.go
  - internal/queryapi/graph/operation_enforce.go
  - alerts/rules.yml
  - deploy/helm/dev-health/values.yaml
  - deploy/helm/dev-health/templates/_helpers.tpl
  - deploy/helm/dev-health/templates/go-api-deployment.yaml
  - docs/reference/auth/readiness-and-slo.md
applicability: current
lifecycle: active
---

# Auth PDP outage and rollback

When a go-api namespace is switched on, the policy decision point (PDP) decides its routes. If go-api gets no verdict, it refuses the request with a `503`. It does not fall back to the legacy guard. The rollback is to switch the namespace off and roll go-api.
{: .fc-page-lede }

!!! warning "Status: no namespace is switched on by default"
    The chart value `authService.enforce.goApi` is an empty list (`deploy/helm/dev-health/values.yaml`, `enforce:` block), so on a deployment that never set it, no route is enforced and this page does not apply. Turning a namespace on is a separate decision. This page states only what exists on `main` at `5cb1eb16`. Where a thing does not exist, the page says so.

## What an outage looks like

Only requests of a switched-on namespace are affected. The guard asks the PDP after authentication (`internal/api/policy/guard.go:155`, `Guard.wrap`). The decision is made in `Enforcer.decide` (`internal/api/policy/enforce.go:134`). A route with no action in its profile row, and every namespace that is off, keep the legacy answer.

| Symptom | Where it comes from |
| --- | --- |
| `503` with the body `{"detail":"Authorization service temporarily unavailable"}` and the header `Retry-After: 5` | `writeEnforceRefusal`, `internal/api/policy/enforce.go:244-248`; constants at `enforce.go:25` and `:27` |
| The WARN log `api enforcement: no verdict, the route is refused`, with attributes `namespace` and `reason` | `enforce.go:160` |
| The WARN log `api enforcement: the allow carries an obligation go-api does not carry out, no verdict, the route is refused`, with attribute `namespace` | `enforce.go:166` |
| `dev_health_authz_enforcement_decisions_total{namespace,service="go-api",outcome="error_deny"}` rises | `internal/api/policy/enforcecounters.go:12`; `enforce.go:159`, `:165` |

A `503` comes from any of these, all counted `error_deny`: the PDP is unreachable, answers late, answers a malformed body, rate limits the caller, or panics inside the client call; or the allow carries an obligation go-api does not carry out (`enforce.go:164`, `obligationsCarriedOut` at `:210`). The `reason` attribute of the first WARN line is the local reason of the client, for example `pdp_unreachable`, `pdp_timeout` or `pdp_rate_limited` (the closed set is `LocalReasons` in `pep/decision.go`, module `github.com/full-chaos/dev-health-go` v0.45.0, pinned at `go.mod:17`). A call has a 2 s timeout (`enforce.go:24`, `:102`).

An allow with an obligation is not an outage of the PDP. It is a policy-data or code gap: see the last bullet of [Rule out other causes](#rule-out-other-causes).

A request is not counted `error_deny` when the PDP answers deny. That is a `403` (`enforce.go:255`), body `Admin access required`, `Superuser access required` or `Access denied`, counted `outcome="deny"`. It is not an outage.

### A second `503`: go-api cannot ask at all

If a namespace is on and go-api has no decision point client (a missing `AUTH_PROXY_URL` or workload input), the start is not refused. Every request of the switched-on namespaces gets `503` with `{"detail":{"message":"Authorization service temporarily unavailable","reason_code":"authz_enforce_no_decision_point"}}` and no `Retry-After` (`enforce.go:249-252`, constant at `:28`). Check, in `internal/apiservice/enforcer.go:46-79`:

- one go-api ERROR line at start, `authz enforcement: a namespace is switched on but go-api cannot ask the decision point; its requests are refused with a 503` (`enforcer.go:72`), with attributes `env` and `variable` (the name of the missing variable, never a value);
- the gauge `dev_health_authz_enforcement_config_invalid{service="go-api"}` is 1 (`enforcer.go:21`, set at `:74`; documented at `docs/reference/auth/readiness-and-slo.md:78`).

The gauge is set once at start. It is 0 when go-api can ask, so a PDP outage does not set it.

## Confirm

1. Read `dev_health_authz_enforcement_enabled{namespace,service="go-api"}` (`internal/authzenforce/authzenforce.go:128`, `:140-151`). A series at 1 is a switched-on namespace; the `503` can only come from those.
2. Read `dev_health_authz_enforcement_decisions_total{outcome="error_deny"}` by `namespace`. A rising share against `allow` and `deny` is the outage signal. The counter is lazy: no series means no event yet.
3. Read the go-api WARN lines above for the `reason`. `pdp_unreachable`, `pdp_timeout` or `pdp_unavailable` point at auth-service or the network; `pdp_rate_limited` points at the auth-service limits.
4. Check auth-service: `/readyz` is 200 (`docs/reference/auth/readiness-and-slo.md:26`). The PDP route is `POST /v1/authorize` (`internal/auth/authapi/authorize.go:28`).
5. Two auth-service limits can cause `pdp_rate_limited`: the per-pod server limit, default 20 requests per second (`defaultRateLimitPerSecond`, `internal/auth/authconfig/config.go:175`, read at `:484`), and the per-caller limit (`PDPCallerLimit`, `internal/auth/authapi/authorize_limit.go:23`; the zero value sets none).

What does not exist on `main`:

- No alert rule watches the enforcement metrics. `alerts/rules.yml` has no `dev_health_authz_enforcement` series, so nothing pages on a PDP outage of go-api. The `auth_service_rate_limit` group and `AuthServiceHighServerErrorRate` (`alerts/rules.yml:954`) watch auth-service itself.
- No dashboard shows the enforcement series. `deploy/signoz/dashboards/` has none for them.
- No `dho` or other command shows or flips the switch. The switch is one environment variable, read once at start.
- No automatic fallback to the legacy guard. A `503` stays a `503` until the PDP answers or the namespace is switched off.

### Rule out other causes

- A `403` is a PDP deny, not an outage. Triage the reason with [Auth shadow PDP triage](auth-shadow-pdp-triage.md).
- A delegated (impersonation) session's read and a delegated session's bearer with no organization keep the legacy answer and are never refused with a `503`. A request whose served organization is no UUID is judged on the token organization (its handler still refuses the value), so in an outage it is a `503`. A request with no user access token cannot reach an enforced row (authentication refuses it first); if one does, it is refused `403` and counted `error_deny`. A request that serves another organization than the token's (path `{org_id}` or `org_id` query) is judged on the token organization, so in an outage it is a `503` like any other enforced request. A token with no organization on an organization row is asked `admin.orgs`, so in an outage it is a `503` too (a platform admin included), except on an `AdminOrg` row, which refuses it `403` `Organization context required` with no question (`decideOrgless` in `enforce.go`). The callers that keep the legacy answer, and the `AdminOrg` refusal, are counted in `dev_health_authz_enforcement_unjudged_total{route,reason}` with reason `impersonation`, `delegated_orgless` or `org_required` (`enforcecounters.go`).
- An allow with a `redaction` or `step_up` obligation, or a `minimum_assurance` other than `aal1` on a read, is a `503` for every such request, however healthy the PDP is. So is a `repository_allowlist` or `repository_scope` allow: no route is listed with no repository dimension (`goAPIRoutesWithoutRepositoryDimension` is empty, `enforce.go:52`). The only log signal is the second WARN line above.

## Roll back

Rollback switches the namespace off. It does not touch policy data. The switch holds no policy rows (`internal/authzenforce/authzenforce.go:1-4`); it is read once at start (`authzenforce.Load`, `:100`, called from `enforcer.go:47`), so go-api must roll.

1. In the values of the release, remove the namespace from `authService.enforce.goApi` (`deploy/helm/dev-health/values.yaml`, the `enforce:` block). To switch every namespace off, set `goApi: []`. An empty list renders no variable (`deploy/helm/dev-health/templates/go-api-deployment.yaml:301-304`; `dev-health.authzEnforceGoApi` in `deploy/helm/dev-health/templates/_helpers.tpl:923`). The variable the chart renders is `GO_API_AUTHZ_ENFORCE`, one comma list of catalog namespaces; the variable name is `Service.Env` for go-api (`authzenforce.go:36`).
2. Run `helm upgrade` with the changed values, as in the [platform roles runbook](auth-platform-roles-flip-and-rollback.md#rollback). Wait for the go-api rollout to finish.
3. Do not set `GO_API_AUTHZ_ENFORCE` through `goApi.extraEnv`: the chart refuses it (`deploy/helm/dev-health/auth_enforce_test.go:60-61`).
4. Read back: `dev_health_authz_enforcement_enabled{namespace="<name>",service="go-api"}` is 0 for the namespace you removed, `dev_health_authz_enforcement_ignored_items{service="go-api"}` is 0, and the `503` and the `error_deny` counter stop for it. `dev_health_authz_enforcement_config_invalid` is 0 once no namespace is on.

A bad list value never stops go-api: an item that is not a catalog namespace, an empty item, a repeat or `*` is ignored, its namespace stays off, and one ERROR line `authz enforcement switch: items ignored, their namespaces stay off` is logged (`authzenforce.go:97-116`). A typo in the name of a namespace you wanted off therefore leaves the others as they were: read the gauge, not the list.

### Rollback cannot give back revoked access

The enforced answer and the legacy answer rest on the same live rows (membership, user state). Switching a namespace off returns the route to the legacy guard, which reads those rows. It does not restore a role, a membership or a user that was removed while the cell was on.

The test is `TestRollbackCannotReactivateRevokedAccess` (`internal/api/policy/enforce_test.go:652`). It revokes a caller two ways (membership removed, user deactivated), flips the cell off, and asserts the request is not served (`enforce_test.go:668-670`). The test runs over an in-memory access store and a stubbed PDP, not a real database or auth-service. The rollback does not write to any policy table, because the switch is a process-local value with no write path.

### After the rollback

- The legacy guard decides the routes again. Shadow comparison (`AUTH_SHADOW_PDP`) is separate and unchanged: see [Auth shadow PDP triage](auth-shadow-pdp-triage.md).
- Before the next turn-on: the `unjudged` count of the row must be 0 for the window its leaf names (`docs/reference/auth/readiness-and-slo.md:76`), and auth-service must list go-api's workload principal in `AUTH_AUTHORIZE_CALLER_SUBJECTS` (`docs/reference/auth/authn-authz.md:162`).
- Field reference for the switch: [Enforcement switch](../../reference/auth/authn-authz.md#enforcement-switch).

## Before a turn-on: platform role parity read

Before a go-api or query-api enforcement cell turns on (F7, F8, F9), run the read in `deploy/scripts/auth_preflip_platform_role_parity.sql` once, read-only, against the production database (`BEGIN TRANSACTION READ ONLY;`, the file, `ROLLBACK;`). Record its output on the flip ticket. The flip goes on only when every count except `superusers` is 0. A non-zero `superusers_without_platform_admin` or `platform_admin_not_superuser` means go-api's superuser flag and the decision point's platform role disagree for that user; a non-zero `platform_admin_on_disabled_copy_of_active_user` means the decision point would admit a platform admin that go-api refuses (product rule 1). Fix the data first, then read again. This is a point-in-time read, not a start refusal and not a wait. A cell in `internal/api/policy` runs the same file.

## query-api

query-api has its own PDP gate. It is separate from go-api: other variables, other metrics, and one more state (the mode). This switch gives no organization and no membership: a caller with no organization keeps the legacy answer.

### The three switches

| Variable | Values | What it does |
| --- | --- | --- |
| `QUERY_API_PDP_MODE` | `off`, `shadow`, `enforce`; unset or empty is `off` | Builds the PDP client. `off` builds none and asks nothing. `shadow` and `enforce` build it. |
| `QUERY_API_AUTHZ_ENFORCE` | comma list of catalog action namespaces; unset is none | The cells of the REST routes and of the GraphQL root fields of a user access token. A route or root is enforced only while the cell of its action's namespace is on, whatever the mode. |
| `QUERY_API_HEADERS_AUTHZ_ENFORCE` | the same list form; unset is none | The cells of the GraphQL operations of the `X-DH-Internal-*` header carrier (the MCP listener, `/query` on the internal listener). |

Both lists are read once at start, so query-api must roll for any change. A bad list item (not a namespace, empty, repeat or `*`) is ignored, its namespace stays off, and one ERROR is logged. A bad `QUERY_API_PDP_MODE` value, or `shadow` or `enforce` with a missing input, never stops the start: one ERROR names the variable, `dev_health_query_api_pdp_config_invalid` is 1, and every request that would ask the PDP is refused as unavailable.

The mode and the cells combine:

| Mode | Cell on | Cell off |
| --- | --- | --- |
| `off` | `503` (log reason `authz_enforce_no_decision_point`): the cell has no client | legacy answer, nothing asked |
| `shadow` | the PDP decision is the answer | legacy answer; the PDP is asked in the background and never changes the answer |
| `enforce` | the PDP decision is the answer | `503` (log reason `authz_enforce_cell_off`), nothing asked |

`enforce` never acts as `shadow`. A user caller's route or root whose cell is off is refused. Do not take a cell out while the mode is `enforce`, unless the same roll also lowers the mode.

### What the caller sees

| Plane | PDP deny | No verdict, or an obligation not carried out |
| --- | --- | --- |
| REST routes (`/api/v1/*`) | `403` `{"detail":"Not authorized"}` | `503` `{"detail":"Authorization unavailable"}` |
| `/graphql` and `/query` | HTTP 200, one error, message `Not authorized`, `extensions.code` `FORBIDDEN` | HTTP `503`, one error, message `Authorization unavailable`, `extensions.code` `SERVICE_UNAVAILABLE`, `extensions.reason` the closed reason |
| MCP listener | HTTP `403`, the same error body | HTTP `503`, the same error body |

A refusal covers the whole GraphQL operation, with no data: one deny among the roots refuses all of them, before any resolver runs. The log line `query-api: the PDP refused the request` names the closed reason only, never a value. The closed reasons the gate makes itself are `authz_enforce_cell_off`, `authz_enforce_no_decision_point`, `authz_enforce_no_principal` (an enforced root of the header carrier, which has no principal the PDP can judge), `authz_enforce_no_scope` (an operation whose ingress set no scope, with the cell of one of its roots on), `tenant_scope_mismatch`, `repository_scope_absent`, `pdp_unknown_obligation` and `allowlist_route_unsupported`. Other reasons come from the PDP client, for example `pdp_unreachable` or `pdp_timeout`. A call has a 2 s timeout.

An allow is served only if query-api carries out every obligation. A `redaction` or `step_up` obligation, a `minimum_assurance` above `aal1`, or a repository allowlist on a route or root with no narrowing is a `503`, however healthy the PDP is. That is a policy-data or code gap, not an outage.

### What stays on the legacy path

- The HS256 edge token (`ops_access_token_hs256`) and the effective-principal envelope (`effective_principal_envelope`) never ask the PDP in any mode. Each accepted call counts in `dev_health_query_api_pdp_exempt_total{class,action}`.
- A go-api impersonation session carried by the admin's own token keeps the legacy answer on GraphQL.
- A superuser with no organization keeps the legacy answer and asks nothing. It counts `result="no_org"`.
- A root whose action is not organization-scoped (the platform actions) keeps its own legacy decision.
- A GraphQL operation with every root cell off runs as before, in every mode except `enforce` for a user caller (see the table above).

### Metrics to read

- `dev_health_query_api_pdp_enabled`: 1 while the gate is built in `shadow` or `enforce`, else 0.
- `dev_health_query_api_pdp_config_invalid`: 1 when the mode is bad, an input is missing, or a cell is on with no client. Set once at start.
- `dev_health_query_api_pdp_results_total{mode,result,action}`: `mode` is `enforce` or `invalid`. `result` is `allow`, `deny`, `unavailable`, `not_configured`, `not_carried_out`, `no_decision_point` or `no_org`. The counter is lazy: no series means no event yet. In `shadow` mode the asks count on `dev_health_api_shadow_results_total` and `dev_health_api_shadow_mismatch_total`, not here.
- `dev_health_query_api_pdp_exempt_total{class,action}`.
- `dev_health_authz_enforcement_enabled{namespace,service="query-api"}`: one series per cell that is on. `dev_health_authz_enforcement_ignored_items{service="query-api"}`: items the list ignored.

Reading the refusals: `result="unavailable"` is no verdict (PDP down, timeout, rate limit, or an operation with no scope or principal). `result="no_decision_point"` is a cell on with no client. `result="not_configured"` is a bad config (`mode="invalid"`) or, in `mode="enforce"`, a cell that is off. `result="not_carried_out"` is an allow with an obligation query-api does not carry out. `result="deny"` is a `403` and is not an outage. The alerts are `QueryAPIPDPConfigInvalid` and `QueryAPIPDPRefusing` in `alerts/rules.yml`; see [Dashboards and alerts](../observe/dashboards-and-alerts.md#query-api-pdp-gate).

### Roll back

Rollback returns the routes to the legacy guard. It does not touch policy data. Both lists and the mode are read once at start, so do all of these in one roll of query-api:

1. Remove the namespaces from `QUERY_API_AUTHZ_ENFORCE` and from `QUERY_API_HEADERS_AUTHZ_ENFORCE`. An empty or unset list is no namespace.
2. Set `QUERY_API_PDP_MODE=shadow`. Shadow asks beside the legacy answer and never changes it. Do not leave the mode at `enforce` with the cells off: that refuses every user caller's organization route with `503` (`authz_enforce_cell_off`).
3. Roll query-api and wait for the rollout to finish.
4. Read back: `dev_health_authz_enforcement_enabled{service="query-api"}` is 0 for each namespace you removed, `dev_health_query_api_pdp_config_invalid` is 0, and no new `result` of `unavailable`, `no_decision_point`, `not_carried_out` or `deny` counts.

Setting only `QUERY_API_PDP_MODE=shadow` while a cell stays on does not roll back that namespace: a cell on is decided in every mode. Setting `QUERY_API_PDP_MODE=off` while a cell stays on refuses that namespace's routes with `503`.

As for go-api, rollback cannot give back access removed while the cell was on: the enforced answer and the legacy answer rest on the same live rows.
