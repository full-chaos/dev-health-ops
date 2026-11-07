---
page_id: ref-auth-authn-authz
summary: How a request is authenticated and authorized today in go-api, query-api and auth-service - the order of the checks, the route levels, organization scope, impersonation, platform authority and the failure answers - and what is planned but not built.
content_type: reference
owner: engineering
source_of_truth:
  - internal/api/policy/guard.go
  - internal/api/policy/enforce.go
  - internal/api/policy/platform_decision.go
  - internal/authzenforce/authzenforce.go
  - internal/api/policy/scope.go
  - internal/api/policy/principalonce.go
  - internal/api/policy/principal.go
  - internal/api/policy/accessroute.go
  - internal/auth/accessverify/accessverify.go
  - internal/queryapi/server/rest_error_response.go
  - internal/queryapi/graph/operation_org_guard.go
  - internal/auth/platformrole/platformrole.go
  - contracts/auth/v1/credential-classes.json
  - contracts/auth/v1/endpoint-profiles.go-api.json
  - contracts/auth/v1/endpoint-profiles.query-api.json
  - contracts/auth/v1/endpoint-profiles.auth-service.json
applicability: current
lifecycle: active
---

# Authentication and authorization reference

This page describes the Go services as they are on `main`. It does not describe the removed Python api. Every statement names the file that proves it.
{: .fc-page-lede }

Three pages go with this one: [policy catalog](policy-catalog.md) (the closed list of actions and roles), [action map](action-map.md) (which route or GraphQL field maps to which action) and [obligations](obligations.md) (the decision contract). The per-surface rows are in the three `endpoint-profiles.*.json` files under `contracts/auth/v1/`.

!!! warning "The action catalog is not the enforcement mechanism today"
    go-api and query-api do not check a catalog action on a request. go-api enforces one of six route levels (see [Route levels](#route-levels)). query-api enforces a valid credential and the caller's organization. The catalog and the action column of the profile rows state the mapping that later enforcement is built on. See [action map](action-map.md#what-enforces-today).

## Services and credentials

| Service | Profile rows | Protected | Public | File |
| --- | --- | --- | --- | --- |
| go-api (`dev-health-ops-api`, plus 3 rows of `dev-health-ops-billing-edge`) | 242 | 209 | 33 | `contracts/auth/v1/endpoint-profiles.go-api.json` |
| query-api (`dev-health-query-api`) | 45 | 37 | 8 | `contracts/auth/v1/endpoint-profiles.query-api.json` |
| auth-service (`dev-health-auth-service`) | 48 | 24 | 24 | `contracts/auth/v1/endpoint-profiles.auth-service.json` |

A route with no profile row fails CI. The Go tests that check the rows are `internal/apiservice/endpoint_profile_rows_test.go`, `internal/queryapi/server/` and `internal/auth/contracts/endpoint_profiles_*_test.go`.

`contracts/auth/v1/credential-classes.json` lists 37 credential classes by `class_id` (29 active, 6 optional_feature, 2 unconsumed; principal types: 17 human, 8 external_source, 5 workload, 3 service_account, 3 infrastructure, 1 client_credential). The class names are the closed vocabulary that every profile row uses in `accepted_credential_classes`. The class page [credential classes](credential-classes.md) is a Python-era record. The JSON is authoritative.

!!! note "Status field versus profile rows"
    The `status` of `user_access_token` in `credential-classes.json` is `unconsumed`. The profile rows of go-api, query-api and auth-service list it as an accepted class. Read the profile rows for what a route accepts.

### Accepted credential classes by service

Counts are the number of profile rows that list the class.

go-api:

| Class | Rows |
| --- | --- |
| `user_access_token` | 196 |
| `external_push_ingest_token` | 5 |
| `internal_svc_acr_token` | 4 |
| `org_context_header` | 4 |
| `workload_token` | 4 |
| `social_oauth_exchange` | 3 |
| `sso_saml_exchange` | 3 |
| `ops_refresh_token` | 2 |
| `sso_oidc_exchange` | 2 |
| `stripe_webhook_signature` | 2 |
| `email_verification_token` | 1 |
| `github_webhook_signature` | 1 |
| `pagerduty_webhook_signature` | 1 |
| `password_reset_token` | 1 |

query-api:

| Class | Rows |
| --- | --- |
| `user_access_token` | 35 |
| `effective_principal_envelope` | 34 |

auth-service:

| Class | Rows |
| --- | --- |
| `internal_svc_authsess_token` | 13 |
| `workload_token` | 11 |
| `user_access_token` | 6 |
| `refresh_credential` | 3 |
| `sso_oidc_exchange` | 2 |
| `kubernetes_sa_token_exchange` | 1 |
| `social_oauth_exchange` | 1 |
| `sso_saml_exchange` | 1 |
| `workload_bootstrap_credential` | 1 |

## Access tokens

go-api and query-api take a bearer access token. Four access-token shapes exist. The route's profile row names which shape it accepts. The validator picks one verifier from the row and the token's JOSE header (`alg`, and `typ` where the row names a `typ`). It never tries several (`internal/auth/accessverify/accessverify.go`).

| Shape | Class | Header | Used by |
| --- | --- | --- | --- |
| Legacy edge token | `ops_access_token_hs256` | `alg` HS256 | none: the class is `retired`, every row refuses it (`alg_not_allowed`) |
| User access token | `user_access_token` | `alg` EdDSA, `typ` `at+jwt`, audience `dev-health-api` | go-api (196 rows), query-api only when `AUTH_ACCESS_TOKEN_ISSUER` is set, auth-service lists the class on 6 session, identity and change-password rows, but the validator text of those rows describes an HS256 token only |
| Workload token | `workload_token` | `typ` `workload+jwt` | go-api (4 rows), auth-service internal routes |
| Effective principal envelope | `effective_principal_envelope` | `alg` EdDSA | query-api |

Sources: `class_disambiguator` and `access_token_validation` in `endpoint-profiles.go-api.json`; the validator text of `endpoint-profiles.query-api.json`; `internal/queryapi/server/rest_error_response.go` (`authenticateRESTRequest`).

After the signature check, go-api reads the live `users` row and refuses the token when the user is missing, is not active, or has a `token_version` that differs from the claim. It then checks the session (`internal/api/policy/principal.go`, `CheckClaims`). A store failure is never treated as a pass.

Token audiences and the classes that may carry them are in `contracts/auth/v1/vocabulary/audiences.json`:

| Audience | Token classes | Services |
| --- | --- | --- |
| `dev-health-api` | `user_access_token`, `workload_token` | `dev-health-ops-api`, `dev-health-query-api` |
| `auth-service` | `user_access_token`, `workload_token` | `dev-health-auth-service` |
| `acr-api` | `user_access_token` | `dev-health-acr-api` |
| `acr-mcp` | `user_access_token` | `dev-health-acr-mcp` |
| `query-api` | `effective_principal_envelope` | `dev-health-query-api` |

## Request order in go-api

For an api-listener route the checks run in this order (`internal/api/policy/principalonce.go`, `Layers`; `internal/api/policy/guard.go`, `Wrap`):

1. **Principal.** The one step that reads the `Authorization` header. It decides the caller once, from the route's profile row, and stores the decision. A caller can be anonymous for a stated reason (no credential, webhook row, refused value). `OrgScope` and `Impersonation` never read the header.
2. **OrgScope.** Resolves the organization of the request (see [Organization scope](#organization-scope)).
3. **Impersonation.** Applies an active impersonation session (see [Impersonation](#impersonation)).
4. **Route level.** The guard of the route (see [Route levels](#route-levels)).

A route in `signatureOnlyRoutes` (`internal/apiservice/scopeexempt.go`) skips steps 2 and 3. Its handler checks a signature first.

### Route levels

`internal/api/policy/guard.go` defines six levels. Counts below are protected go-api rows whose primary validator names the level.

| Level | Rule | Rows |
| --- | --- | --- |
| `Public` | No credential check. | not counted (public rows: 33) |
| `Authenticated` | A valid access token. | 45 |
| `Admin` | A valid token, and the role is `owner` or `admin`, or the user is a superuser. The role is read from the membership row of the token's organization, not from the role claim. | 81 |
| `AdminOrg` | `Admin`, and the effective organization is not empty. The effective organization is the caller's own, or the target organization while an impersonation session is active. | 50 |
| `Superuser` | The user is a superuser. | 16 |
| `Optional` | The caller is authenticated when a usable bearer token is sent. A refused token does not refuse the request. | 2 |

15 protected rows use another validator: a push token, a webhook signature, a workload token check, or the handler's own check. The row's `primary_validator` says which.

A caller with no organization and no superuser flag gets `403 Organization context required`, unless the route is in `OrglessMayReach` (`internal/api/policy/orgless.go`).

### Enforcement switch

A switch per action namespace and service lets the policy decision point decide a route instead of the route level (`internal/authzenforce`, `internal/api/policy/enforce.go`). It is off for every namespace by default and is read once at start.

- go-api reads `GO_API_AUTHZ_ENFORCE`, a comma list of catalog namespaces (the chart renders it from `authService.enforce.goApi`). An item that is not a namespace, a repeat, an empty item and `*` are ignored and stay off; the start is never refused for them (one ERROR line, and the `dev_health_authz_enforcement_ignored_items` gauge).
- query-api reads `QUERY_API_AUTHZ_ENFORCE` the same way, for its REST routes (`/api/v1/*`) only. A listed namespace needs the PDP client of `QUERY_API_PDP_MODE`; without it the namespace's routes answer `503`. An allow is served only as the scope its obligations name; see [configuration](configuration.md#pdp-gate-query-api).
- For a route whose profile row names an action in an enforced namespace, the decision replaces the route level. Allow passes; deny answers `403` with the body the route level gives (`Admin access required`, `Superuser access required`, `Access denied` at the Authenticated level); no verdict (the decision point is down, answers late, answers a malformed body, or an allow carries an obligation go-api does not carry out) answers `503` with `Retry-After`. There is no fallback to the legacy check on a failure.
- An allow serves only when go-api carries out every obligation it carries: a `tenant_scope` that is the request's organization (never on a platform route, whose request names no organization), and a `repository_allowlist` or the `repository_scope` `organization` only on a route listed with no repository dimension (`goAPIRoutesWithoutRepositoryDimension` in `enforce.go`; a route not listed has one). `redaction`, `minimum_assurance` and `step_up` are never carried out. One obligation not carried out makes the allow no verdict: `503` with `Retry-After`, counted `error_deny`.
- A caller with a live go-api impersonation row (the admin's own bearer while an `impersonation_sessions` row is in force) is never asked of the decision point: it would judge that bearer as the admin in the admin's organization, not as the delegation. Its read (`GET`, `HEAD`, `OPTIONS`, the rule of `RefuseImpersonation`, `internal/api/policy/impersonation_readonly.go`) of a row that is not a platform wrap keeps the legacy answer, served as the target organization as with the cell off (product rule 1), counted unjudged `impersonation`. Its platform questions (`HoldsPlatform`, the admin-or and platform wraps' action) keep main's answer too: that read asks the decision point nothing. Any other request, and every request to a platform wrap, is refused with `403` `Not allowed in an impersonation session`, counted `impersonation_refused` (writes as built: a documented flaw, not a product rule). The stop route (`POST /api/v1/admin/impersonate/stop`) is the exception: the admin acts on their own session, and the decision point decides it.
- With the `admin` namespace on, a route open to an organization admin or to a holder of a platform action (the `/api/v1/admin/users` routes, mounted `WrapAdminOr`) asks one batch: the route's platform action, and `members.manage` in the token's organization when the token names one. Either allow passes. A missing or failed answer, or an allow with an obligation go-api does not carry out, answers `503` with `Retry-After`. The rows take part once their profile rows name `admin.users`.
- With the `admin` namespace on, a platform action of an admin-or or platform wrap, and of a handler that acts across organizations, is asked of the decision point for a caller it can judge (`HoldsPlatform`, `internal/api/policy/platform_decision.go`): the user list and create, the user and organization scope checks, the impersonation status and the telemetry report. Each action is asked at most once per request, and a platform route's own answer is reused. No verdict answers `503` with `Retry-After`. A legacy HS256 caller, whom the decision point cannot judge, keeps main's answer from go-api's own set. A live go-api impersonation row keeps main's answer on rows the decision point does not decide.
- With the `admin` namespace on, go-api holds no local check stricter than the legacy path for a platform admin (product rule 1): the guard's `User.IsAdmin`, its organization-less reach and its membership read are as with the namespace off, and a platform admin with no membership row gets the same answer.
- A delegated session's own bearer (a token with an `act` claim) is decided by the decision point like any other caller. Its allow carries `minimum_assurance`, which go-api does not carry out, so on an enforced row it is no verdict (`503`).
- The decision point judges the caller's token organization; `X-Org-Id` never changes it. On a row whose handler serves an organization that a path segment or an `org_id` query parameter names (`internal/api/policy/servedorg.go`: the rows whose profile resolver is `organization_from_path`, that is the `{org_id}` members, invites and entitlements rows, and the rows whose resolver is `organization_from_query_or_session`, that is the billing invoice and subscription rows), that organization must be the token organization, compared as parsed UUIDs (upper case, braces, `urn:uuid:` and no dashes are the same id). Another organization, or a token with none, is refused with `403` and the route level's body before any decision point call, counted `org_mismatch`. The check runs before the unjudged check, so a legacy HS256 token and a superuser are refused too. A value that parses as no UUID keeps the legacy answer (the handler refuses it) and is counted unjudged `no_org`. The shadow records the same request as a mismatch of class `intentional` with no decision point call, and never changes the answer.
- A caller the decision point cannot judge keeps the legacy answer: a legacy HS256 token, an organization route with no organization, an impersonation read. The request is counted in `dev_health_authz_enforcement_unjudged_total` by route and reason (`hs256`, `no_org`, `impersonation`).
- A route with no action in its row is never enforced. Authentication and the token organization's membership read still run first.
- Rollback is removing the namespace from the list and rolling go-api. The switch holds no policy data, and the legacy path reads the same live membership, user and session rows, so a revoked caller stays refused.
- go-api needs a way to ask: `AUTH_PROXY_URL`, and a workload credential (`AUTH_WORKLOAD_SUBJECT_FILE` or `AUTH_WORKLOAD_BOOTSTRAP_FILE`, and `AUTH_WORKLOAD_ISSUER`). A namespace on with one of them missing does not stop the start: go-api logs one ERROR naming the missing variable, sets `dev_health_authz_enforcement_config_invalid` to 1, and refuses every request of the switched-on namespaces with a `503` whose detail carries `reason_code` `authz_enforce_no_decision_point`; the rest of go-api serves. auth-service must list go-api's workload principal in `AUTH_AUTHORIZE_CALLER_SUBJECTS`. The switch and the shadow (`AUTH_SHADOW_PDP`) ask through one decision point client, which uses the workload token of the provisioning or session client when go-api built one.

### Organization scope

`internal/api/policy/scope.go`, `OrgScope`:

- With an `X-Org-Id` header, the header is accepted only for a superuser, for the caller's own organization, or for an organization where the caller has a membership. Any other value is `403` before the route runs.
- With no header, the caller's own organization is the scope.
- An anonymous or refused caller passes with no organization. The route's own guard then answers `401`.

Each go-api row carries `tenant_requirement`: `org_scoped` (185), `none` (41), `cross_org_superuser_only` (16).

### Impersonation

A superuser with a live impersonation session acts in the target organization. `EffectiveOrgID` in `internal/api/policy/scope.go` decides this on every request. It reads the session after a live check. It never carries the session between requests. At the `Admin` and `AdminOrg` levels the guard swaps the caller's organization for the effective one before the admin test.

Each go-api row carries `impersonation_policy`: `subject_to_impersonation_override` (175), `exempt_no_org_context` (31), `not_applicable_non_main_app` (3), unset (33, the public rows).

## Platform authority

A superuser is a caller with platform authority. Two sources exist, chosen by `AUTH_PLATFORM_ROLES_OWNED` (`internal/auth/platformrole/rule/`):

- Off (the default): `users.is_superuser` in the public schema.
- On: the caller holds the `platform_admin` role now, in `auth.platform_role_assignments`. go-api reads this in the same statement as the users read, so a failed auth read fails the read (`internal/api/policy/store.go`, `PlatformRoleRead`).

A platform role holds platform actions only. An organization role never comes from a platform role, and a platform role never comes from an organization role (`internal/auth/platformrole/platformrole.go`). Granting or revoking a platform role needs the platform action `admin.users` now. A revoke that leaves no active platform administrator is refused. A break-glass bootstrap grant uses the audit reason code `restore_fence_bootstrap`.

## query-api

query-api authenticates every protected REST route with `authenticateRESTRequest` (`internal/queryapi/server/rest_error_response.go`). It has no per-route action check. Its 37 protected rows all have `action: unknown`, with a declared gap.

| Condition | Answer |
| --- | --- |
| No `Authorization` header | 401 `Not authenticated` |
| Header is not `Bearer` plus one token | 401 `Invalid authorization header` |
| Token refused by its verifier | 401 `Invalid or expired token` |
| Valid edge or user token, no organization, caller is not a superuser | 403 `Organization context required` |
| Users-row store unavailable | 503 |
| Other store failure | 500 |

For GraphQL, `OperationOrgGuard` (`internal/queryapi/graph/operation_org_guard.go`) reads every `orgId` argument before any field runs. It refuses an operation that names an invalid id, more than one organization, or an organization other than the caller's. A verified superuser who is not impersonating may name another organization, and the operation then runs against it. The per-field map is in [action map](action-map.md#graphql-field-map).

## auth-service

auth-service has 48 profile rows. Its routes do not carry an `action` field. They are guarded by credential class and, for the internal routes, by scope.

| Method and route | Class | Accepted credential classes | Tenant requirement |
| --- | --- | --- | --- |
| `POST /v1/token/refresh` | protected | `refresh_credential` | `none` |
| `GET /v1/sessions/current` | protected | `user_access_token` | `none` |
| `POST /v1/sessions/revoke` | protected | `user_access_token` | `none` |
| `POST /v1/sessions/revoke-all` | protected | `user_access_token` | `none` |
| `POST /v1/identities/social` | protected | `user_access_token` | `none` |
| `DELETE /v1/identities/social/{id}` | protected | `user_access_token` | `none` |
| `POST /api/v1/auth/login` | public | none | `unset` |
| `POST /api/v1/auth/refresh` | protected | `refresh_credential` | `none` |
| `POST /api/v1/auth/logout` | protected | `refresh_credential` | `none` |
| `POST /api/v1/auth/validate` | protected | `user_access_token` (body field `token`) | `none` |
| `POST /api/v1/auth/switch-org` | protected | `user_access_token` | `none` |
| `POST /api/v1/auth/social-login` | protected | `social_oauth_exchange` | `none` |
| `POST /internal/v1/sessions` | protected | `internal_svc_authsess_token`, `workload_token` | `none` |
| `POST /internal/v1/sessions/state` | protected | `internal_svc_authsess_token`, `workload_token` | `none` |
| `POST /internal/v1/sessions/delegated` | protected | `internal_svc_authsess_token`, `workload_token` | `none` |
| `POST /internal/v1/sessions/delegated/end` | protected | `internal_svc_authsess_token`, `workload_token` | `none` |
| `POST /internal/v1/memberships/add` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/memberships/role` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/memberships/remove` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/memberships/transfer` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/invites/create` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/invites/accept` | protected | `internal_svc_authsess_token`, `workload_token` | `none` |
| `POST /internal/v1/invites/revoke` | protected | `internal_svc_authsess_token` | `org_scoped` |
| `POST /internal/v1/organizations/deprovision` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/organizations/reactivate` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/organizations/suspend` | protected | `internal_svc_authsess_token` | `cross_org_superuser_only` |
| `POST /internal/v1/organizations/state` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/principals/get` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/principals/list` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/organizations/get` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/organizations/list` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/memberships/get` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/memberships/list` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/grants/get` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/grants/list` | protected | `internal_svc_authsess_token`, `workload_token` | `cross_org_superuser_only` |
| `POST /internal/v1/organizations/rename` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/create` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/disable` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/enable` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/owner` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/keys/issue` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/keys/rotate` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/keys/revoke` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/service-accounts/keys/list` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/grants/create` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/grants/update` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/grants/revoke` | protected | `internal_svc_authsess_token`, `workload_token` | `org_scoped` |
| `POST /internal/v1/token/exchange` | protected | `kubernetes_sa_token_exchange`, `workload_bootstrap_credential` | `none` |
| `GET /api/v1/auth/verify` | public | none | `none` |
| `POST /api/v1/auth/register` | public | none | `none` |
| `POST /api/v1/auth/resend-verification` | public | none | `none` |
| `POST /api/v1/auth/forgot-password` | public | none | `none` |
| `POST /api/v1/auth/reset-password` | public | none | `none` |
| `POST /api/v1/auth/change-password` | protected | `user_access_token` | `none` |
| `GET /healthz` | public | none | `unset` |
| `GET /readyz` | public | none | `unset` |
| `GET /metrics` | public | none | `unset` |
| `HEAD /healthz` | public | none | `unset` |
| `HEAD /readyz` | public | none | `unset` |
| `HEAD /metrics` | public | none | `unset` |
| `POST /api/v1/auth/oidc/{provider_id}/authorize` | public | none | `unset` |
| `POST /api/v1/auth/oidc/{provider_id}/callback` | public | `sso_oidc_exchange` | `unset` |
| `POST /api/v1/auth/oauth/{provider_id}/authorize` | public | none | `unset` |
| `GET /api/v1/auth/oauth/{provider_id}/authorize` | public | none | `unset` |
| `POST /api/v1/auth/oauth/{provider_id}/callback` | public | `sso_oidc_exchange` | `unset` |
| `GET /api/v1/auth/saml/{provider_id}/metadata` | public | none | `unset` |
| `POST /api/v1/auth/saml/{provider_id}/initiate` | public | none | `unset` |
| `POST /api/v1/auth/saml/{provider_id}/acs` | public | `sso_saml_exchange` | `unset` |
| `GET /.well-known/jwks.json` | public | none | `unset` |
| `HEAD /.well-known/jwks.json` | public | none | `unset` |
| `GET /.well-known/openid-configuration` | public | none | `unset` |
| `HEAD /.well-known/openid-configuration` | public | none | `unset` |

Source: `contracts/auth/v1/endpoint-profiles.auth-service.json`. The OpenAPI document for the public session routes is `contracts/auth/v1/openapi/auth-v1.openapi.json`.

## Planned, not built

| Item | Status |
| --- | --- |
| Policy decision point routes `POST /v1/authorize` and `POST /v1/authorize:batch`. No Go route serves them. | Planned |
| Actor chain, delegation limits and obligations in the decision point. | Planned |
| Delegated (impersonation) session with an actor chain. | Planned |
| Explicit platform actions for the superuser-inherited sites (`admin.billing`, `admin.flags`, `admin.audit`, `admin.impersonate`, `admin.telemetry`, `admin.stats`). Not in the catalog on `main`. | Planned |
| Server actions and route handlers in web use single decisions. | Planned |
| Check of the query-api rows and the GraphQL map against live behaviour. | Planned |

The catalog described here is version 4. Changes from the tickets above will change the version. Check `contracts/auth/v1/vocabulary/actions.v1.json` for the current one.
