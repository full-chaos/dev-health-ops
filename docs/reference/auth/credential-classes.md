# Credential-class inventory

!!! info "Machine-readable contract"
    This page is prose over
    [`contracts/auth/v1/credential-classes.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.json)
    (schema: [`credential-classes.schema.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.schema.json)).
    The JSON is authoritative; this page must stay in sync with it. Every fact
    below carries a `repo/path:line` anchor in the JSON — this page does not
    repeat every anchor inline, it summarizes.

## Scope

This page lists the credential classes that dev-health services mint or
validate today. It covers the three repositories that handle credentials:

- **ops** (`dev-health-ops`): the Go api, auth-service, query-api and workers
- **web** (`dev-health-web`): Next.js 16 / Auth.js
- **acr** (`dev-health-acr`): Go `acr-api` + `acr-mcp`

The inventory is the closed vocabulary that the
[endpoint profiles](endpoint-profiles.md) and the CI gate key on: a class
missing here cannot be named by a profile row. One hazard runs through it: a
generic decoder that sees every bearer credential before the validator that
owns it. [Credentials that reach a validator that is not their own](#credentials-that-reach-a-validator-that-is-not-their-own)
shows where that happens today.

## Reading this table

`status` values:

- **active** — minted and validated today.
- **unconsumed** — minted/stored but no validator was found anywhere in
  ops/web/acr.
- **legacy_permissive** — the validator silently no-ops when its config is
  unset (fails **open**, not closed). Flagged per the engineering
  guardrails' explicit ban on "missing config means auth disabled in
  production."
- **optional_feature** — gated behind a license entitlement.
- **retired** — no endpoint-profile row may accept the class. The contracts
  gate `TestNoEndpointProfileRowAcceptsARetiredClass` refuses a row that lists
  one. The class stays in the registry so refusal reasons and audit records
  can still name it.

| Class | Status | Principal type | Permissive-when-unconfigured |
| --- | --- | --- | --- |
| `auth_js_browser_session` | active | human | |
| `ops_access_token_hs256` | retired | human | |
| `ops_refresh_token` | retired | human | no endpoint-profile row accepts it; go-api's refresh and logout rows list `refresh_credential` |
| `social_oauth_exchange` | active | human | |
| `sso_saml_exchange` | optional_feature | human | |
| `sso_oidc_exchange` | optional_feature | human | |
| `org_context_header` | active | human | |
| `impersonation_session` | active | human | |
| `internal_svc_acr_token` | **retired** | service_account | no route accepts it; the service-credentials verbs are gone |
| `internal_svc_worker_token` | **retired** | service_account | no route accepts it; the service-credentials verbs are gone |
| `acr_client_credential` | active | client_credential | |
| `acr_device_flow_code` | active | human | |
| `acr_browser_refresh_token` | optional_feature | human | |
| `acr_web_assertion` | active | workload | |
| `acr_workload_identity_exchange` | active | workload | |
| `external_push_ingest_token` | active | external_source | |
| `stripe_webhook_signature` | active | external_source | |
| `github_webhook_signature` | active | external_source | |
| `gitlab_webhook_signature` | **retired** | external_source | no route accepts it; the GitLab intake route is removed |
| `jira_webhook_signature` | **retired** | external_source | no route accepts it; the Jira intake route is removed |
| `kubernetes_sa_token_exchange` | optional_feature | workload | |
| `github_app_installation_token` | active | external_source | |
| `pagerduty_oauth_credential` | active | external_source | |
| `llm_api_keys` | active | infrastructure | |
| `email_verification_token` | active | human | |
| `password_reset_token` | active | human | |
| `org_invite_token` | active | human | |
| `license_key` | active | infrastructure | |
| `infrastructure_deployment_credentials` | active | infrastructure | |
| `effective_principal_envelope` | retired | human | no endpoint-profile row accepts it; query-api refuses it on every route |
| `refresh_credential` | active | human | |
| `user_access_token` | **unconsumed** | human | |
| `internal_svc_authsess_token` | **retired** | service_account | no route accepts it; the service-credentials verbs are gone |
| `link_credential` | active | human | |
| `pagerduty_webhook_signature` | active | external_source | |
| `workload_token` | optional_feature | workload | |
| `workload_bootstrap_credential` | optional_feature | workload | |
| `service_account_key` | **unconsumed** | service_account | issued by the key routes; no route accepts it |
| `service_account_token` | **unconsumed** | service_account | contract only; nothing mints or verifies it yet |
| `oauth_authorization_code` | **unconsumed** | human | minted at consent; the token endpoint that redeems it is not built |
| `oauth_consent_csrf_token` | active | human | minted at describe, checked at approve and deny |

**30 classes** (31 with `effective_principal_envelope`, below). 5 were not in the minimum list and were discovered
during this inventory (`new_vs_ticket: true` in the JSON):
`internal_svc_worker_token`,
`acr_workload_identity_exchange`, `org_invite_token`, `license_key`, and the
distinction between `sso_saml_exchange`/`sso_oidc_exchange` as two classes
rather than one.

**`effective_principal_envelope`.** query-api, a sixth deployed
app, was outside the ops/web/acr inventories, and the signed envelope it
verifies was a credential class the vocabulary did not name. It is now class 31
and query-api's three surfaces have rows in
`contracts/auth/v1/endpoint-profiles.query-api.json`. G-61 position: the class
has no issuer in Go (the minter is removed and the class is retired); query-api only
verifies and refuses it, so no exception to one active issuer per token class is needed.

**`refresh_credential`.** The opaque refresh credential of the
auth-service session domain (TRD section 11): `dhr1_` plus 32 random bytes,
stored as its SHA-256 only, single use with a 30 s grace and family reuse
revocation. It is class 32. auth-service is its only issuer and its only
validator (`POST /v1/token/refresh`); its four API routes and three operator
routes have rows in `contracts/auth/v1/endpoint-profiles.auth-service.json`.
It is not `ops_refresh_token`: that class is go-api's retired HS256 refresh JWT, and
`refresh_credential` is auth-service's own.

**`internal_svc_authsess_token`.** The go-api service credential
(`svc_authsess_` plus 32 random bytes, service `auth-session-issuer`). Its first
scope, `sessions:issue`, is retired: `POST /internal/v1/sessions`,
`/internal/v1/sessions/state` and `/internal/v1/sessions/delegated{,/end}` take
only the go-api workload token and refuse this credential from the bearer text,
counted in `auth.session.retired_credential_refusals{operation}`. It shares the
`internal_service_credentials` table with `svc_acr_` and `svc_worker_`, which
carry other prefixes, services and scopes. No route takes it, so no validator
accepts it, and the `service-credentials` verbs that minted, rotated, listed and
revoked its rows are gone. Its second scope, `memberships:write`, is retired: no service
may hold it, nothing mints it, and the internal provisioning routes
(`/internal/v1/memberships/*`, `/internal/v1/invites/*`, `/internal/v1/organizations/*`,
`/internal/v1/service-accounts/*`, `/internal/v1/grants/*`) refuse this credential
and take the go-api workload token only. The route and
go-api's four compatibility routes (`/api/v1/auth/{login,refresh,logout,social-login}`)
have rows in `contracts/auth/v1/endpoint-profiles.auth-service.json`. The
provisioning audit rows name the actor, the request id and the calling
credential (`caller_principal_id` and `workload_jti` of the go-api workload token).

Four read scopes name the versioned read API: `principals:read`,
`organizations:read`, `memberships:read` and `grants:read`. A read scope is never
satisfied by a write scope (`contracts.ReadScopeAllows`). No credential of this
class holds one: the read routes (`/internal/v1/{principals,organizations,memberships,grants}/{get,list}`)
refuse this credential and take the go-api workload token only.

**`org_invite_token`.** The invite link token: `signedtoken.Build`
over the invite id (32 lowercase hex characters, a dot, 64 lowercase hex
characters), stored only as its SHA-256 digest. go-api mints it
(`internal/apiservice/admin/invites.go`) and so does auth-service's invite
aggregate behind `POST /internal/v1/invites/create`, which answers it once; go-api
(`internal/api/session/invites.go`) and the auth-service invite aggregate validate
it.

**`link_credential`.** The one-time e-mail link token of the
auth-service local flows (e-mail verification, password reset): 32 random bytes,
base64url, stored as its SHA-256 only in `link_credentials`, bound to one
purpose, single use, expiry by the database clock (24 h verification, 1 h
reset). auth-service verify and reset accept only this class, never the go-api
signed tokens (`email_verification_token`, `password_reset_token`), so a go-api
link issued before the move to auth-service lapses and the user asks for a new one. A
password change does not delete other outstanding links; they lapse at expiry.
A reset link redeems only while the user's `public.users.token_version` still
equals the value it was issued against (`issued_token_version`); every password
write of either service raises that counter, so a reset link issued before it
reads as unknown afterwards.
Purpose binding is proven by `TestVerifyAndResetRedeemOnlyTheirOwnPurpose` and
`TestLinkCredentialOfAnotherPurposeIsNotRedeemedBothWays`. Its routes get
profile rows when they are mounted.


**`pagerduty_webhook_signature`.** The inbound delivery signature
of `POST /api/v1/webhooks/pagerduty/{binding_id}`: `X-PagerDuty-Signature`, a
comma-separated list of at most 8 `v1=<hex>` HMAC-SHA256 values over the raw
body, checked against the secret stored encrypted on the addressed binding row.
It is distinct from `pagerduty_oauth_credential`, the outbound OAuth credential
that ops sends to PagerDuty.
**`user_access_token`.** The EdDSA user access token of TRD
section 11, the successor of `ops_access_token_hs256`. It is class 33. Its wire
contract is `access-token.v1`: header {alg EdDSA, typ at+jwt, kid}; claims iss,
aud (one of `dev-health-api`, `auth-service`, `acr-api`, `acr-mcp`), sub, principal_type, sid, jti,
iat, nbf, exp, optional org_id and scope. It carries no role, email, profile,
superuser bit or token version: authorization is checked per request. Its
lifetime is at most 60 min, the ceiling of the retired HS256 token. The class is
`unconsumed`: nothing mints or verifies it yet.
The audiences `acr-api` and `acr-mcp` (scopes `acr:api` and `acr:mcp`) are ACR's: acr-api accepts `acr-api` only and acr-mcp `acr-mcp`
only. go-api, query-api and auth-service refuse both. No ops platform token
carries one: the session token settings (`AUTH_JWT_AUDIENCE`, go-api's
`JWT_AUDIENCE`), the envelope verifier and both minters refuse them. The
mint that will issue them is a separate one not built yet.

**`workload_token`.** The EdDSA workload token of TRD section 11
for one internal service caller. Its wire contract is `workload-token.v1`:
header {alg EdDSA, typ `workload+jwt`, kid}; claims iss, aud (exactly one
service), sub, principal_type `workload`, jti, iat, nbf, exp; no `org_id`, no
`sid`, no `scope`; lifetime at most 300 seconds. Its typ separates it from the
user access token (`at+jwt`). The auth-service workload-token exchange mints it
(`POST /internal/v1/token/exchange`, mounted only with `AUTH_WORKLOAD_EXCHANGE=true`). The document parser `ParseWorkloadToken` is under
`FuzzParseWorkloadToken`. auth-service's session issue route
(`POST /internal/v1/sessions`) also accepts it, for audience `auth-service` and
the one workload principal in `AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT` (the token
has no action claim, so that subject is the `sessions.issue` binding); with the
variable unset the route refuses it. The internal provisioning routes (mounted
by `AUTH_MEMBERSHIP_ROUTES`) accept it on the same terms and the same subject,
so one go-api token covers both; their audit rows name it by
`caller_principal_id` and `workload_jti`.

**`kubernetes_sa_token_exchange`.** The Kubernetes projected ServiceAccount
token a workload presents to auth-service's workload-token exchange
(`POST /internal/v1/token/exchange`, form field `subject_token`, RFC 8693). The
Kubernetes TokenReview API is its only authenticator, for the fixed audience
`dev-health-workload-exchange`: after a successful review its `exp` claim is
read unverified, only to cap the issued token's lifetime, and it is never
stored. The exchange answers a `workload_token`. The route is mounted only with
`AUTH_WORKLOAD_EXCHANGE=true`, so the class is `optional_feature`.

**`workload_bootstrap_credential`.** The Compose and self-hosted bootstrap
credential of one workload (ACP-ADR-06 decision 2): `fcwlb_` and 32 random
bytes as unpadded base64url, read by the workload from a restricted secret file
and presented to auth-service's workload-token exchange as the RFC 8693
`subject_token` with `subject_token_type`
`urn:dev-health:params:oauth:token-type:workload-bootstrap`. It is accepted
there and nowhere else: as an `Authorization` bearer it opens no auth-service
route, and go-api refuses its prefix before any validator. The registry
(`auth.credential_registry`) keeps the SHA-256 digest of the whole value, never
the value. A value of another shape is refused before any store read; a
revoked or expired credential, a disabled workload, or an audience outside
both the workload's and the credential's audiences gets the exchange's one
refusal. An issued `workload_token` never outlives the credential's expiry, and
its `auth.workload.token_issued` audit row names the credential row id. The
route serves this subject only with `AUTH_WORKLOAD_BOOTSTRAP=true`, so the
class is `optional_feature`. The operator creates, rotates, revokes and
disables a credential with `auth-migrate workloads bootstrap-create`,
`bootstrap-rotate`, `bootstrap-revoke` and `bootstrap-disable` (migration role;
see the [bootstrap credential runbook](../../operate/runbooks/auth-workload-bootstrap-credential.md)).

**`service_account_key`.** The key of an organization's service
account: `fcsak_` and 32 random bytes as unpadded base64url. The store verbs
exist in `internal/auth/serviceaccount`, and the routes
`/internal/v1/service-accounts/keys/*` issue, rotate, revoke and list keys.
No route accepts a key. A key authenticates nowhere yet,
so the class is `unconsumed`. It is no bearer: go-api and the auth-service
session issue route refuse its prefix before any validator. The registry
(`auth.credential_registry`) keeps the SHA-256 digest of the whole value, never
the value. Each key change is one `service_account.key_changed` provisioning
event that names the credential row id.

**`service_account_token`.** The EdDSA token the exchange of a
service account key will issue. Its wire contract is `service-account-token.v1`:
header {alg EdDSA, typ `sa+jwt`, kid}; claims iss, aud (exactly `dev-health-api`),
sub (the account), principal_type `service_account`, org_id (the account's
organization), cid (the key row id), jti, iat, nbf, exp; no `sid`, no `scope`,
no audience list; lifetime at most 300 seconds and never past the key's expiry
(`ServiceAccountTokenLifetime`). Its typ separates it from the user access
token (`at+jwt`) and the workload token (`workload+jwt`). Only the contract and
its signed golden corpus (`golden/service-account-token`) exist: nothing mints
it, no verifier reads it and no endpoint row accepts it, so the class is
`unconsumed`. The key itself stays the exchange's subject and is never a bearer.
Its issue will write the append-only audit event
`auth.service_account.token_issued`, naming the credential row id, never the
key value.

**`oauth_authorization_code` and `oauth_consent_csrf_token`.**
The two secrets of auth-service's OAuth 2.1 authorization server (ADR-13).
`POST /oauth/authorize`, action `describe`, binds an authorization request to
the bearer's web session once and returns the consent anti-CSRF token:
`fcocs_` and 32 random bytes as unpadded base64url. Action `approve` or `deny`
must carry it in the form body field `csrf_token`, with the same web session
and principal; one decision spends it, and it ends with its request (10
minutes). `approve` mints the authorization code, `fcoac_` and 32 random bytes,
inside the consent's audit commit, and returns it once on the client's exact
redirect. It is single use, lives 60 seconds and is bound to the client, the
redirect, the RFC 8707 resource and the S256 challenge. auth-service keeps the
SHA-256 of each on `auth.oauth_authorization_requests`, never the value; no
audit row holds either. Neither is a bearer, and no route takes either as
one. The code is `unconsumed` until the token endpoint (O4)
redeems it. The anti-CSRF token is `active`: describe mints it and
approve and deny check it. The routes mount with `AUTH_TOKEN_ISSUER_URL` set;
no ingress or go-api proxy path reaches them from outside.

**`acr_browser_refresh_token`.** The refresh token of the ACR
first-party browser client: `acrrt_` and 32 random bytes as
unpadded base64url. The Ask Dev web browser gets it from the authorization code
exchange of acr-api's OAuth server and presents it only as a form body field:
`refresh_token` on `POST /token` (`grant_type=refresh_token`), `token` on
`POST /revoke` (RFC 7009). It is never a bearer and opens no route by itself.
acr keeps its SHA-256 only (`acr.browser_refresh_tokens`), in a token family
bound to the user's organization, the browser client and the approving web
session. It is single use: each refresh issues a successor and a new 10-minute
`acr_client_credential` of class browser. The same consumed token presented
again within 30 s, while its successor is unused, gets that same successor (an
HMAC of the presented token over a stored random seed, so no token is stored);
any other reuse revokes the whole family. The family ends 12 h after login and
no refresh extends it; an ended family is purged after a grace. acr-api serves
it only with a browser client configured, so the class is `optional_feature`.

## Credentials that reach a validator that is not their own

go-api reads the `Authorization` header in one place: the Principal step
(`Scope.Principal`, `internal/api/policy/principalonce.go:112`, which decides
in `Scope.decide`, `:125`). It checks the credential's own shape before any
decoder runs:

- Its refusal table (`earlyRefusals`, `internal/api/policy/principalonce.go:39-49`)
  refuses a bearer whose prefix names a domain credential or a secret
  (`svc_`, `fcpush_`, `fcacr_`, `whsec_`, `fcwlb_`, `fcsak_`, `fcoac_`,
  `fcocs_`) before any validator or store read.
- A route whose endpoint-profile row does not accept an access token sends no
  bearer to the access-token check.
- The access-token check (`accessverify.Validator.Validate`,
  `internal/auth/accessverify/accessverify.go:133`) reads only the JOSE header
  `alg` and refuses every alg but EdDSA, the retired HS256 included.

The org scope (`Scope.OrgScope`, `internal/api/policy/scope.go:128`) and the
impersonation scope (`Scope.Impersonation`, `:221`) read only the Principal
decision (`principalFrom`, `:105`), never the header. The org scope skips the
`/api/v1/internal/acr/` prefix (`acrInternalPrefix`, `:98`); those routes
accept only the acr-api workload token (`routeAuth.authenticate`,
`internal/apiservice/acr/internalauth.go:99`).

So no class reaches a validator it does not own: no validator in
`contracts/auth/v1/credential-classes.json` sets
`foreign_class_validator: true`. The Python `OrgIdMiddleware` did: it ran every
bearer through the user-JWT decoder, except under the `/api/v1/internal/acr/`
prefix. That code is gone, and its validator entries are removed from the
registry:

| Credential class | What go-api does with it today |
| --- | --- |
| `internal_svc_acr_token` (`svc_acr_*`) | The internal acr routes answer the uniform 401 before any credential read; every other route refuses the `svc_` prefix in the Principal step. |
| `internal_svc_worker_token` (`svc_worker_*`) | No route accepts it; the Principal step refuses the `svc_` prefix. |
| `external_push_ingest_token` (`fcpush_*`) | The Principal step refuses the `fcpush_` prefix; only the External Push routes read it (`requireIngestScope`, `internal/api/externalingest/auth.go:81`). |
| `ops_refresh_token` (a refresh JWT) | It is a body field, not a bearer. auth-service refuses a JWT-shaped `refresh_token` before any session read (`refuseLegacyRefresh`, `internal/auth/authapi/compat.go:504`). |

### A positive counter-example worth copying: acr's dispatch

`acr`'s `Authenticator.MiddlewareFor` dispatches on the *credential's own
header* before choosing a decoder, and explicitly **rejects** a request that
carries both `X-ACR-Web-Assertion` and `Authorization`
(in the acr repo) rather than silently
preferring one. This is the shape a class-aware gate should generalize.

## Retired: the legacy ingest path

The legacy `POST /api/v1/ingest/*` routes are retired. go-api
mounts none of them and reads none of their settings
(`INGEST_API_KEYS`, `INGEST_SIGNING_SECRET`,
`DEV_HEALTH_LEGACY_INGEST_ENABLED`, `DEV_HEALTH_LEGACY_INGEST_SUNSET`), so each
path answers 404. Their deployment-wide key carried no organization and the
handler wrote the body's `org_id` as given: retiring the routes removed that
cross-tenant write path. Its two credential classes (the deployment-wide API
key and the HMAC body signature) are deleted from the registry, the wire
schemas and the stored-class CHECK (auth migration 0036).

The current signature-only routes accept Stripe, GitHub, and PagerDuty signatures. GitLab and Jira intake routes are absent. See [Billing and worker boundaries](billing-and-worker-boundaries.md) for matching rules, refusal behavior, and enforcing tests.

## Retired credential: `svc_worker_*`

The class is retired. The entire `dho service-credentials` command group is absent, including create, rotate, list, and revoke. Each verb returns an unknown-command error with exit 2. Stored rows do not grant operator authority.

Enforced by: `TestRetiredVerbGroupsAreUnknownCommands` and `TestRetiredVerbGroupsAreNotInTheTree` in `cmd/dho/retired_verbs_test.go`.

The worker operator CLI uses its fixed operator principal and database role checks. It does not validate `svc_worker_` bearers or check `workers:read` or `workers:operate` scopes. See [Worker and queue authorization](billing-and-worker-boundaries.md#worker-and-queue-authorization).

Enforced by: `internal/joboperator/auth_test.go` and `internal/workersctl/operator_authority_inventory_test.go`.

## Infrastructure and exchange classes

- **`infrastructure_deployment_credentials`** (DB/Kubernetes/TLS/deployment
  identities) are provisioned by the Helm chart
  (`deploy/helm/dev-health/templates/secret.yaml`, `deploy/helm/dev-health/templates/serviceaccount.yaml`).
  They are deployment inputs, not application tokens.
- **`acr_workload_identity_exchange`** accepts a Kubernetes ServiceAccount JWT as
  its subject token; that token is validated by
  `github.com/full-chaos/dev-health-go/authverify`.

Each class has its anchors and unknown fields recorded in the `gaps` array of
[`credential-classes.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.json).

## query-api: the MCP caller-class listener

query-api serves the hosted MCP's validated free-form GraphQL query on a
separate listener (`--mcp-addr` / `QUERY_API_MCP_ADDR`, chart
`queryApi.mcp.port`, default 8092) with **no token**: the caller states the
org in the four `X-DH-Internal-*` identity headers, and the network is the
control. Any `Authorization` header is refused there, and so is a header set
that claims superuser, impersonation or an operator role. Only query
operations over an allowlist of root fields are served, with depth, alias,
complexity and ClickHouse read limits. No Ingress may route to the MCP
Service (`<release>-query-api-mcp`; the chart refuses an Ingress path to it),
and the chart's NetworkPolicy admits only the pod selectors in
`queryApi.mcp.allowedFrom` (acr's api pods); an empty list denies the port.
A pod that can reach the port can read any org through the allowlisted
fields; it cannot write.

## Go api: the internal listener

The Go api serves `/api/v1/internal/acr/*` behind two controls: the network,
and the acr-api workload token on every route (the only bearer accepted; a
`svc_acr_` service credential gets the uniform 401 before any credential read). Those
routes are mounted only on the
internal listener (`--api-internal-addr` / `DEV_HEALTH_API_INTERNAL_ADDR`,
chart `goApi.internal.port`, default 8091), never on the public api listener,
which answers them 404. No Ingress may route to the internal Service
(`<release>-go-api-internal`; the chart refuses an Ingress path to it), and the
chart's NetworkPolicy admits only the pod selectors in
`goApi.internal.allowedFrom` (acr's api pods); an empty list denies the port.
