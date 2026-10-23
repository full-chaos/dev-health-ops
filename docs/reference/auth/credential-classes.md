# Credential-class inventory (CHAOS-3273 Wave 0 freeze)

> **Note:** The Python api (FastAPI and Strawberry) that this page inventories was removed. This page is a record of that Python surface, not of the Go api.

!!! info "Machine-readable contract"
    This page is prose over
    [`contracts/auth/v1/credential-classes.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.json)
    (schema: [`credential-classes.schema.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.schema.json)).
    The JSON is authoritative; this page must stay in sync with it. Every fact
    below carries a `repo/path:line` anchor in the JSON — this page does not
    repeat every anchor inline, it summarizes.

## Why this exists

dev-health is building a dedicated Go `auth-service` that will become
authoritative for principals, sessions, tokens, orgs, memberships, roles,
service accounts, workload identities, grants, authz decisions, and audit.
Before any authority moves, this inventory freezes the **current** state
across the three repos that mint or validate credentials today:

- **ops** (`dev-health-ops`) — Python FastAPI + Go workers
- **web** (`dev-health-web`) — Next.js 16 / Auth.js
- **acr** (`dev-health-acr`) — Go `acr-api` + `acr-mcp`

The motivating defect is **CHAOS-3271**: ops' global `OrgIdMiddleware` ran
*every* `Authorization: Bearer` value through the user-JWT decoder, so a
valid opaque `svc_acr_*` service credential was parsed as a JWT and threw
`"Not enough segments"`. That specific crash is fixed (PR #1338) — a path
prefix exemption for `/api/v1/internal/acr/*` — but the underlying pattern
(one generic decoder sees every bearer credential class before the intended
validator does) is not closed. This inventory is the closed vocabulary a
later CI gate keys on: **a class missing here is a class the gate can never
catch.**

Design docs (read the guardrails at minimum):

- [ADR — Implement the Auth Control Plane in Go](https://linear.app/fullchaos/document/adr-implement-the-auth-control-plane-in-go-c056c6f30e86)
- [Engineering Guardrails](https://linear.app/fullchaos/document/engineering-guardrails-go-native-dev-health-auth-control-plane-974bdb93843a)
- [TRD](https://linear.app/fullchaos/document/trd-go-native-dev-health-auth-control-plane-527b6cfb2b0f)
- [PRD](https://linear.app/fullchaos/document/prd-go-native-dev-health-auth-control-plane-cd91f1f85d1f)
- [CHAOS-3273](https://linear.app/fullchaos/issue/CHAOS-3273)

## Reading this table

`status` values:

- **active** — minted and validated on the paths inspected this pass.
- **unconsumed** — minted/stored but no validator was found anywhere in
  ops/web/acr.
- **legacy_permissive** — the validator silently no-ops when its config is
  unset (fails **open**, not closed). Flagged per the engineering
  guardrails' explicit ban on "missing config means auth disabled in
  production."
- **optional_feature** — gated behind a license entitlement or an optional
  Python import.

| Class | Status | Principal type | Permissive-when-unconfigured |
| --- | --- | --- | --- |
| `auth_js_browser_session` | active | human | |
| `ops_access_token_hs256` | active | human | |
| `ops_refresh_token` | active | human | |
| `social_oauth_exchange` | active | human | |
| `sso_saml_exchange` | optional_feature | human | |
| `sso_oidc_exchange` | optional_feature | human | |
| `org_context_header` | active | human | |
| `impersonation_session` | active | human | |
| `internal_svc_acr_token` | active | service_account | |
| `internal_svc_worker_token` | **unconsumed** | service_account | |
| `worker_operational_bridge_token` | **unconsumed** | infrastructure | |
| `acr_client_credential` | active | client_credential | |
| `acr_device_flow_code` | active | human | |
| `acr_web_assertion` | active | workload | |
| `acr_workload_identity_exchange` | active | workload | |
| `external_push_ingest_token` | active | external_source | |
| `legacy_ingest_api_key` | **legacy_permissive** | external_source | **YES** |
| `legacy_ingest_hmac_signature` | **legacy_permissive** | external_source | **YES** |
| `stripe_webhook_signature` | active | external_source | |
| `github_webhook_signature` | active | external_source | |
| `gitlab_webhook_signature` | active | external_source | |
| `jira_webhook_signature` | active | external_source | |
| `github_app_installation_token` | active | external_source | |
| `pagerduty_oauth_credential` | active | external_source | |
| `llm_api_keys` | active | infrastructure | |
| `email_verification_token` | active | human | |
| `password_reset_token` | active | human | |
| `org_invite_token` | active | human | |
| `license_key` | active | infrastructure | |
| `infrastructure_deployment_credentials` | active | infrastructure | |
| `effective_principal_envelope` | active | human | |
| `refresh_credential` | active | human | |
| `user_access_token` | **unconsumed** | human | |
| `internal_svc_authsess_token` | active | service_account | |
| `link_credential` | active | human | |
| `pagerduty_webhook_signature` | active | external_source | |

**30 classes at the Wave 0 freeze** (31 with `effective_principal_envelope`, added by CHAOS-4737 below). 6 were not in CHAOS-3273's minimum list and were discovered
during this inventory (`new_vs_ticket: true` in the JSON):
`internal_svc_worker_token`, `worker_operational_bridge_token`,
`acr_workload_identity_exchange`, `org_invite_token`, `license_key`, and the
distinction between `sso_saml_exchange`/`sso_oidc_exchange` as two classes
rather than one.

**`effective_principal_envelope` (CHAOS-4737).** query-api, a sixth deployed
app, was outside the ops/web/acr inventories, and the signed envelope it
verifies was a credential class the vocabulary did not name. It is now class 31
and query-api's three surfaces have rows in
`contracts/auth/v1/endpoint-profiles.query-api.json`. G-61 position: the class
has exactly one issuer, the edge (`internal/envelopemint`); query-api only
verifies, so no exception to one active issuer per token class is needed.

**`refresh_credential` (CHAOS-10228).** The opaque refresh credential of the
auth-service session domain (TRD section 11): `dhr1_` plus 32 random bytes,
stored as its SHA-256 only, single use with a 30 s grace and family reuse
revocation. It is class 32. auth-service is its only issuer and its only
validator (`POST /v1/token/refresh`); its four API routes and three operator
routes have rows in `contracts/auth/v1/endpoint-profiles.auth-service.json`.
It does not replace `ops_refresh_token` yet: go-api still mints that one until
the session cutover.

**`internal_svc_authsess_token` (CHAOS-10465).** The go-api service credential
(`svc_authsess_` plus 32 random bytes, service `auth-session-issuer`, scope
`sessions:issue`) that `POST /internal/v1/sessions` accepts. It shares the
`internal_service_credentials` table with `svc_acr_` and `svc_worker_`, which
carry other prefixes, services and scopes. auth-service is its only validator
(`servicecred.PG.Check`); the verbs of `dev-hops service-credentials` mint,
rotate and revoke it. The route and go-api's four compatibility routes
(`/api/v1/auth/{login,refresh,logout,social-login}`) have rows in
`contracts/auth/v1/endpoint-profiles.auth-service.json`.

**`link_credential` (CHAOS-10688).** The one-time e-mail link token of the
auth-service local flows (e-mail verification, password reset): 32 random bytes,
base64url, stored as its SHA-256 only in `link_credentials`, bound to one
purpose, single use, expiry by the database clock (24 h verification, 1 h
reset). auth-service verify and reset accept only this class, never the go-api
signed tokens (`email_verification_token`, `password_reset_token`), so a go-api
link issued before the cutover lapses and the user asks for a new one. A
password change does not delete other outstanding links; they lapse at expiry.
Purpose binding is proven by `TestVerifyAndResetRedeemOnlyTheirOwnPurpose` and
`TestLinkCredentialOfAnotherPurposeIsNotRedeemedBothWays`. Its routes get
profile rows when they are mounted.


**`pagerduty_webhook_signature` (CHAOS-10707).** The inbound delivery signature
of `POST /api/v1/webhooks/pagerduty/{binding_id}`: `X-PagerDuty-Signature`, a
comma-separated list of at most 8 `v1=<hex>` HMAC-SHA256 values over the raw
body, checked against the secret stored encrypted on the addressed binding row.
It is distinct from `pagerduty_oauth_credential`, the outbound OAuth credential
that ops sends to PagerDuty.
**`user_access_token` (CHAOS-10442).** The EdDSA user access token of TRD
section 11, the successor of `ops_access_token_hs256`. It is class 33. Its wire
contract is `access-token.v1`: header {alg EdDSA, typ at+jwt, kid}; claims iss,
aud (one of `dev-health-api`, `auth-service`), sub, principal_type, sid, jti,
iat, nbf, exp, optional org_id and scope. It carries no role, email, profile,
superuser bit or token version: authorization is checked per request. Its
lifetime is at most the 60 min HS256-compat ceiling. The class is
`unconsumed`: nothing mints or verifies it yet.

## The CHAOS-3271 class: credentials that reach a validator that is not their own

The org scope (`Scope.OrgScope`, `internal/api/policy/scope.go:128`) reads every
request's `authorization` header through `Scope.headerAuthenticate`
(`internal/api/policy/scope.go:103-120`), which calls
`Authenticator.Authenticate` (`internal/api/policy/principal.go:109`) and
decodes it as a user access JWT — **regardless of what the token actually
is** — except for one explicit path-prefix exemption (`acrInternalPrefix`,
`internal/api/policy/scope.go:97-98`, checked at `:130`):

```go
if strings.HasPrefix(r.URL.Path, acrInternalPrefix) {
	next.ServeHTTP(w, r)
	return
}
```

A non-JWT bearer value does not fail the request: `Authenticator.Authenticate`
returns a refusal, and `Scope.headerAuthenticate` treats a refusal as no user
(`internal/api/policy/scope.go:116-118`), so the request proceeds as anonymous
through the org scope and its *real* validator still gets a turn. **But the
credential still transited a validator it does not own**, which is exactly the
shape CHAOS-3271 warns about. Every class below hits
this:

| Credential class | Route(s) | Exempted from the path prefix? |
| --- | --- | --- |
| `internal_svc_worker_token` (`svc_worker_*`) | none found (see below) | **No** — no HTTP validator exists for it anywhere, so if one were ever wired up at a non-`/api/v1/internal/acr/` path it would silently degrade to anonymous first. |
| `worker_operational_bridge_token` (`WORKER_OPERATIONAL_BRIDGE_TOKEN`) | `/api/internal/worker-operational` (CHAOS-6240 deleted `/internal/worker`'s only routes along with worker_metrics.py; `/api/internal/worker-sync` was deleted separately, CHAOS-6243) | **No.** None of these paths start with `/api/v1/internal/acr/`. |
| `external_push_ingest_token` (`fcpush_*`) | External Push data-plane endpoints | **No.** The module docstring for `external_ingest/auth.py` says as much: *"OrgIdMiddleware only understands user JWTs and takes its anonymous pass-through branch for an `fcpush_...` bearer"* — meaning the decode attempt still happens, it just fails harmlessly. |

Only `internal_svc_acr_token` (`svc_acr_*`) got the fix. The fix is a path
exemption, not a credential-class dispatch — so it only protects whatever
happens to be mounted under `/api/v1/internal/acr/*` today. Any new route
for `svc_worker_*`, the worker-bridge tokens, or any future opaque credential
mounted **outside** that one path prefix inherits the original bug's shape
by default. **The durable fix is credential-class-aware dispatch (check the
token's own shape/prefix before choosing a decoder), not a growing exemption
list of path prefixes.**

### A positive counter-example worth copying: acr's dispatch

`acr`'s `Authenticator.MiddlewareFor` dispatches on the *credential's own
header* before choosing a decoder, and explicitly **rejects** a request that
carries both `X-ACR-Web-Assertion` and `Authorization`
(in the acr repo) rather than silently
preferring one. This is the shape a class-aware gate should generalize.

## Permissive-when-unconfigured: the legacy ingest path

The legacy ingest handler (`internal/api/legacyingest/auth.go`) is permissive
only in a development environment. Its checks work like this:

- If `INGEST_API_KEYS` and `INGEST_SIGNING_SECRET` are both unset outside a
  development environment, the request gets a 401 (fail closed;
  `internal/api/legacyingest/auth.go:155-159`).
- If only one of them is set, only that check runs: an unset key list skips
  the API-key check, and an unset signing secret skips the HMAC signature
  check (`internal/api/legacyingest/auth.go:160-195`).

The engineering guardrails say: "missing config must never mean auth
disabled in production." The webhook validators in
`internal/api/webhookintake/` (`github.go` HMAC, `gitlab.go` token compare,
`jira.go` HMAC) and the Stripe webhook
(`internal/api/billing/webhook.go:60-84`, signature check in
`internal/api/billing/webhooksig.go`) also fail closed: each answers 500
when its secret is unset. The legacy ingest path is the one place where an
unset single credential skips its own check, so it stays a gap to review.

## Unconsumed credential: `svc_worker_*`

`WORKER_OPERATOR_TOKEN_PREFIX = "svc_worker_"` is defined and minted through
the exact same code path as `svc_acr_*`
(`internal/apiservice/admin/servicecredentials.go`), with
its own scope set (`workers:read`, `workers:operate`,
`internal/apiservice/admin/servicecredentials.go:36-44`) and a full CLI lifecycle
(create/list/rotate/revoke). No HTTP route in ops, web, or acr was found
that validates a `svc_worker_`-prefixed bearer token — `internal/apiservice/acr/internalauth.go:57-71`
`extractToken` only accepts the `svc_acr_` prefix. Either this is
scaffolding for a not-yet-wired consumer, or a consumer exists in the Go
worker binaries or `dev-health-go` outside this lane's assigned read scope.
Flagged rather than guessed.

## Not anchored this pass

- **`infrastructure_deployment_credentials`** (DB/Kubernetes/TLS/deployment
  identities) — only confirmed the deployment manifests exist
  (`deploy/helm/dev-health/templates/secret.yaml`, `deploy/helm/dev-health/templates/serviceaccount.yaml`);
  no application-code read/validate path was traced. This is the
  least-verified class in the inventory.
- **`acr_workload_identity_exchange`**'s subject-token (Kubernetes
  ServiceAccount JWT) validator lives in `github.com/full-chaos/dev-health-go/authverify`
  — a fourth repo outside this lane's assigned scope (ops/web/acr only).
- **`sso_saml_exchange`** / **`sso_oidc_exchange`** — the router's gating
  (`@require_feature("sso")`) and schemas are anchored; the actual
  SAML-signature/OIDC-code verification logic in
  `internal/api/sso/` was not read this pass.
- **`license_key`** — whether `LicenseValidator`/`licensing/gating.py` fails
  open or closed when `LICENSE_PUBLIC_KEY` or the license string itself is
  unset was not traced to its call site.
- **`internal_svc_acr_token`**'s outbound caller (where in the acr repo a
  `svc_acr_*` token is actually sent as `Authorization: Bearer`) was not
  located — acr's `internal/auth/` is inbound-credential-focused.
- **`pagerduty_oauth_credential`** — the Python-side
  `oauth.py`/`oauth_lifecycle.py`/`oauth_storage.py`/`oauth_authorization_store.py`
  were only grepped for filenames, not read line by line.
- **`legacy_ingest_api_key`/`legacy_ingest_hmac_signature`** — the current
  route table mounted behind `validate_ingest_auth` was not enumerated, so
  whether this permissive path is reachable in production is unconfirmed.

Every gap above is also recorded per-class in the `gaps` array of
[`credential-classes.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.json).
No field in the JSON was guessed from a name; an unanchored fact is `null`
with a `gaps` entry, never a filled-in guess.

## mkdocs navigation

This page is declared under the existing explicit `Reference:` nav block in
`mkdocs.yml` (nav is explicit here, not `awesome-pages`) — see the `Auth:`
entry alongside `API`, `GraphQL`, `CLI`, etc.

## query-api: the MCP caller-class listener (CHAOS-7085)

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

## Go api: the internal listener (CHAOS-7181)

The Go api serves `/api/v1/internal/acr/*` behind two controls: the network,
and a `svc_acr_` bearer check on every route (CHAOS-9993; health and
entitlements port `api/internal/acr.py`, llm-config adds its own scope). Those
routes are mounted only on the
internal listener (`--api-internal-addr` / `DEV_HEALTH_API_INTERNAL_ADDR`,
chart `goApi.internal.port`, default 8091), never on the public api listener,
which answers them 404. No Ingress may route to the internal Service
(`<release>-go-api-internal`; the chart refuses an Ingress path to it), and the
chart's NetworkPolicy admits only the pod selectors in
`goApi.internal.allowedFrom` (acr's api pods); an empty list denies the port.
