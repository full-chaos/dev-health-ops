# Endpoint authentication profiles (CHAOS-3273 Wave 0 freeze)

!!! info "Machine-readable contract"
    This page is prose over
    [`contracts/auth/v1/endpoint-profiles.ops.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/endpoint-profiles.ops.json)
    (schema: [`endpoint-profile.schema.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/endpoint-profile.schema.json)),
    generated from independent source discovery in
    [`ci/discover_ops_routes.py`](https://github.com/full-chaos/dev-health-ops/blob/main/ci/discover_ops_routes.py).
    The JSON is authoritative; every row carries a `file:line` anchor. This
    page summarizes; it does not repeat every anchor inline.

## Why this exists

Guardrail G-1: **a route without a registered profile fails CI and may not
ship.** This is the second half of Wave 0's inventory (see
[credential-classes.md](credential-classes.md) for the first half, ops'
credential-class vocabulary) — every REST route and GraphQL resolver in
`dev-health-ops`, classified `protected`/`public`, with its accepted
credential class(es) and every validator that can observe that credential
before (or instead of) its intended one. A later lane builds the CI gate
(L3) that checks new routes against this inventory; a route missing here is
a route that gate can never guard.

The motivating defect is **CHAOS-3271**: ops' global `OrgIdMiddleware` ran
every `Authorization: Bearer` value through the user-JWT decoder, so a valid
opaque `svc_acr_*` credential got parsed as a JWT. It was fixed in PR #1338 by
exempting that route family from the generic decoder.

The general question that defect raises — which validators can observe a
credential they do not own — is a property of the middleware stack rather than
of any single route, so every row records it explicitly in
`reachable_validators`. That makes it a checkable fact per surface instead of an
argument, which is the point of carrying it in the inventory at all; see
[Reachable-but-not-owner findings](#reachable-but-not-owner-findings) below.

## Scope and counts

`ci/discover_ops_routes.py` independently re-derives every REST decorator
(`@router.<method>(...)`, `@app.<method>(...)`, including `api_route` and any
router-alias name, not just literal `router`/`app`) and every
`@strawberry.field`/`@strawberry.mutation` resolver under
`src/dev_health_ops/api`, then resolves each REST route's full mount path by
walking the `include_router` graph from each `FastAPI()` root.

**303 REST decorator surfaces + 58 GraphQL resolvers = 361 total.**

!!! warning "Known gap: GraphQL subscriptions are not covered"
    Discovery matches `@strawberry.field` and `@strawberry.mutation` only, so
    the three `@strawberry.subscription` resolvers in
    `api/graphql/subscriptions.py` (`:60`, `:97`, `:134`) are **neither
    discovered nor profiled**, even though they are mounted on the served
    schema (`api/graphql/schema.py:1028`). The 361 above **excludes** them.
    They *are* authenticated: all three go through the same connect-time
    context path as every other GraphQL surface (`schema.py`'s
    `context_getter` → `get_context` → `GraphQLContext.__post_init__`), which
    runs before the websocket is accepted. So this is a gap in what the
    inventory covers, not an unauthenticated surface.
    Tracked as **CHAOS-4761**, which replaces source-text discovery with
    enumeration from the served application and schema objects — the set a
    pattern can miss, an object cannot. Until that lands, a green run of this
    gate says nothing about those three surfaces.

!!! warning "Discrepancy vs. the orchestrator's 279"
    The lane brief's baseline count (279 `@router|@app.<method>(` matches)
    undercounts by exactly **24**: two files use router-alias names the
    baseline's regex doesn't match — `status_router` in
    `api/external_ingest/status.py` (2 routes) and `sso_router` in
    `api/auth/sso/router.py` (17 routes) — plus 5
    `@router.api_route(...)`/`@app.api_route(...)` registrations across the
    tree (`2 + 17 + 5 = 24`). `303 - 279 = 24`. The GraphQL count (58) matched
    exactly. **True total is 361 surfaces, not the ~337 estimate.**

### Two deployed apps

Routes are served by **two separate `FastAPI()` instances**, not one:

- `dev-health-ops-api` — `src/dev_health_ops/api/main.py`, the main app. Full
  middleware stack registered in `api/_middleware.py`: on the request path,
  `CorrelationIdMiddleware` → `OrgIdMiddleware` → `ImpersonationMiddleware` →
  `SlowAPIMiddleware` → `OriginValidationMiddleware` (CSRF) →
  `GraphQLQuerySizeLimitMiddleware` → `SecurityHeadersMiddleware` →
  `CORSMiddleware` → the route.
- `dev-health-ops-billing-edge` — `src/dev_health_ops/api/billing_edge.py`, a
  **separately deployed** app (`deploy/helm/dev-health/templates/billing-edge-deployment.yaml`)
  with **zero shared middleware** — no `OrgIdMiddleware`, no CORS, no CSRF.
  It registers exactly three routes: `POST /api/v1/billing/webhooks/stripe`
  (forwards into `billing/router.py`'s `stripe_webhook`, the SAME handler the
  main app also serves at the same path — two independent surfaces, two rows),
  `GET/HEAD /health`, and a catch-all `/{path:path}` that 404s everything
  else.

Every row's `service` field says which app serves it. `reachable_validators`
is `[]` for billing-edge rows because that app shares no middleware with the
main app to be reachable through.

## Classification summary

| | Count |
| --- | --- |
| Protected | 339 |
| Public | 22 |
| Unclassifiable in this pass | 0 |
| Not discovered (GraphQL subscriptions, CHAOS-4761) | 3 |

Every protected row carries `accepted_credential_classes` drawn from
[credential-classes.json](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.json)'s
closed vocabulary — validated: no row uses a `class_id` outside that list,
and no protected row has an empty credential-class list. Every public row
carries a non-null `public_rationale`.

Per the lane brief's explicit method: `accepted_credential_classes` and
`reachable_validators` are derived from the **middleware stack + mount
path**, not from reading every route body. The remaining fields (`action`,
`resource_resolver`, `entitlement_requirement`, cache class) are filled where
cheaply determinable at the family level and `null` with a `gaps` note
otherwise — a stated reason, not a guess, per the brief's standing rule.

## Reachable-but-not-owner findings

Every row's `reachable_validators` array records each validator that can
observe a credential before (or instead of) the route's intended one, with
`reachable_but_not_owner: true` marking the CHAOS-3271 class. The per-family
analysis of what that currently reaches is maintained in the Auth Control
Plane project in Linear rather than here, and lands in this page as the
cases it describes are closed.

## Gap verdicts (lane brief's four required investigations)

1. **`api/services/sso.py` validators, anchored.**
   SAML: `_validate_saml_signature` (`sso.py:796-812`) uses `signxml.XMLVerifier`
   against the provider's stored certificate; raises if no certificate is
   configured (fails **closed**). OIDC: `_validate_id_token` (`sso.py:1017-1044`)
   fetches the signing key via `PyJWKClient` and calls `jwt.decode` with an
   explicit RS/ES algorithm allowlist and required claims (`exp`/`iat`/`iss`/`aud`);
   raises if `issuer`/`client_id`/`jwks_uri` are missing (fails **closed**).
2. **`api/middleware/impersonation.py`, read line-by-line.** See the
   reachable-but-not-owner finding above — no path exemption, unverified JWT
   peek on every request.
3. **`validate_ingest_auth` reachability: LIVE in production routing.**
   Reachable via exactly 6 REST routes in `api/ingest/router.py`
   (`POST /api/v1/ingest/{commits,pull-requests,work-items,deployments,incidents,telemetry}`,
   lines 26/53/80/107/134/161) — a distinct, unrelated `IngestAuthContext`
   also exists in `api/external_ingest/auth.py`; the two must not be
   conflated. `validate_ingest_auth` (`api/ingest/auth.py:81-111`) is
   **permissive when unconfigured**: an unset `INGEST_API_KEYS` skips the
   API-key check entirely, an unset `INGEST_SIGNING_SECRET` skips the HMAC
   check entirely. No manifest under `deploy/` in this repo sets either
   variable — could not verify whether either is injected out-of-band (a
   secrets manager not checked into this repo), so this is reported as a
   confirmed **live, reachable, currently-permissive shape**, not a confirmed
   open exploit in a specific deployed environment.
4. **`licensing/gating.py` fails CLOSED, not open.** `LicenseManager.initialize`
   (`gating.py:230-241`) only constructs a validator/payload `if public_key:`
   — with `LICENSE_PUBLIC_KEY`/`LICENSE_KEY` unset, `_license_payload` stays
   `None`. `LicenseManager.tier` (`gating.py:302-304`) then defaults to
   `LicenseTier.COMMUNITY`, the **lowest** tier, and
   `get_features_for_tier` (`licensing/registry.py:42-53`) only enables
   features whose `min_tier <= COMMUNITY` — every `TEAM`/`ENTERPRISE`-gated
   feature and every explicit-purchase feature (`EXPLICIT_PURCHASE_FEATURES`)
   is denied. Unconfigured licensing degrades to the minimum tier, not to
   unrestricted access.

## Addendum: `svc_worker_*` — mint-only scaffolding, unwired to any HTTP transport

Per the orchestrator's addendum (escalated from L0): `svc_worker_*` can be
minted/rotated/revoked via CLI (`service_credentials.py`,
`models/internal_service_credential.py:14`), and it **does** have a real,
DB-backed validator — `internal/joboperator/auth.go` (ops repo Go tree, this
worktree's own HEAD, not the versioned `dev-health-go` dependency, so no
version-pin caveat applies here): the token-shape regex at line 54
(`^svc_worker_[A-Za-z0-9_-]{32,256}$`) and `Authenticator.Authenticate`
(lines ~119-160) hash-match against `public.internal_service_credentials`
with `revoked_at`/`expires_at` checks and a `last_used_at` touch.

**But nothing calls it.** Searched `cmd/` (all 12 Go binaries) and every
`internal/` package outside `internal/joboperator/` itself for
`joboperator.`, `NewAuthenticator`, `ScopeWorkerRead`, `ScopeWorkerOperate`,
`WorkerOperatorService` — zero hits except `internal/syncreconciler`'s test
files, which reference an unrelated `joboperator` symbol
(`NewDirectPostgresBackend`). `internal/joboperator/` itself contains no
`http.`/`Handler`/`mux.`/`ServeHTTP` reference at all — no HTTP transport
exists in this package. Also searched `dev-health-go` at both pinned tags
(`ops/go.mod:8` → `v0.6.1`, `acr/go.mod:5` → `v0.5.5`) via
`git grep -n svc_worker` against each tag directly (never the unpinned
working tree) — zero hits in either. **Verdict: `svc_worker_*` is real,
DB-live scaffolding with no HTTP consumer in ops, acr, or dev-health-go at
either pinned version — exercised only by `internal/joboperator`'s own
integration test suite.** Not part of `endpoint-profiles.ops.json` (no route
uses it); recorded here because it's a Go-side fact this lane was asked to
resolve, not a REST/GraphQL surface.

## GraphQL resolvers

All 58 `@strawberry.field`/`@strawberry.mutation` resolvers require an
authenticated context: `GraphQLContext.__post_init__`
(`api/graphql/context.py:74-82`) raises `AuthorizationError` unless `org_id`
is set (or the user is a superuser), and `org_id` is only ever populated from
`OrgIdMiddleware`'s validated-JWT-plus-membership contextvar — there is no
path to a usable context for an unauthenticated request. Per-field/mutation
`action`/`resource_resolver`/entitlement scoping was **not** individually
re-derived for all 58 resolvers in this pass (`gaps` on every GraphQL row
says so explicitly) — that is a follow-up, not a guess filled in here.

## What a surface ISSUES: the `issued_credential` field

Every other field on a profile row describes what a route **accepts**. That is
only half of an auth control plane. A surface that **mints** a credential is an
*issuer*, and the guardrails treat issuers as their own governed thing: **G-12**
(signing keys are issuer-only — a key that verifies must never also sign) and
**G-61** (exactly one active issuer per token class). Neither guardrail is
checkable from an accept-side-only inventory: it would show a class being
validated everywhere and never show where that class is born.

`issued_credential` is an array on each row, added after the ops/web/acr
inventories were frozen. It is **optional** in `endpoint-profile.v1`, so a row
untouched since the freeze is *absent*, not wrong. Its values are distinct and
must not be conflated:

| Value | Meaning |
|---|---|
| non-empty array | This surface mints these credentials. Each entry anchors the **mint site** — the line that signs or creates the credential, not the line that sends it. |
| `[]` | The surface was **assessed** and positively issues nothing. |
| `null` | Issuance was **not determined** in this pass. MUST be paired with a `gaps` entry, exactly like a null anchor. |
| absent | The repo's inventory pass predates the field. |

Each entry carries `class_id` (the same closed vocabulary as
`accepted_credential_classes` — a minted credential with no registered class is
a finding, not a value to invent), `direction`
(`outbound_to_dependency` when the credential authenticates *this* service's own
call to a dependency and the caller never sees it; `returned_to_caller` when it
is handed back in a body, a header, or a `Set-Cookie`), the mint-site `anchor`,
and where determinable `issuer` / `audience` / `algorithm` /
`lifetime_seconds` / `key_source` / `verified_by`. **`key_source` is an env var
NAME, a JWKS path, or a key-management surface — never a key value.**

`verified_by` is the one field allowed to point outside this repo, because the
whole point of an issued credential is that something else validates it; it is
written repo-qualified (`acr:internal/auth/web_assertion.go:96`). Anchors in the
`anchor` object stay relative to the repo that owns the inventory file.

**Backfill discipline.** A row gets `issued_credential` only where issuance was
*proven* by tracing the route handler through to the mint site. The known Wave 0
case is dev-health-web, which mints an `acr_web_assertion` on three of its
routes (see the web repo's `docs/auth-endpoint-profiles.md`). **No ops row is
backfilled yet.** ops has obvious issuer candidates — the login and SSO-callback
routes that return `ops_access_token_hs256` / `ops_refresh_token`, and the
device-flow surfaces — but "obvious" is a family pattern, and this inventory
does not record family patterns as facts. They are tracked as an explicit gap
rather than guessed.

## Contributor guide: registering a new route or credential class

1. **New route, existing credential class.** Add a row to
   `contracts/auth/v1/endpoint-profiles.ops.json` with `id`, `route`/`method`
   (or `graphql_field_name`), `source` anchor, `classification`, and — if
   protected — `accepted_credential_classes` from
   `contracts/auth/v1/credential-classes.json`'s closed vocabulary plus
   `reachable_validators` derived from where your router mounts (which
   middleware sees it, per [Two deployed apps](#two-deployed-apps) and
   [Reachable-but-not-owner findings](#reachable-but-not-owner-findings)
   above). Re-run `python3 ci/discover_ops_routes.py` first — it independently
   re-derives every surface, so your new route must appear in its output with
   the SAME method/path/file/line before you hand-add the profile row for it.
2. **New credential class.** It must exist in
   `contracts/auth/v1/credential-classes.json` first (that file's schema and
   contributor rules are L0's responsibility, see
   [credential-classes.md](credential-classes.md)) — do not invent a
   `class_id` in an endpoint profile that isn't already registered there.
3. **Public route.** Set `classification: "public"` and a non-empty
   `public_rationale` — every public row must justify why no credential is
   required, not just assert it.
4. **Anything you can't determine.** Leave the field `null` and add a
   `gaps` entry naming what you searched and why it's undetermined. A stated
   reason is correct; an unstated guess is a defect (see this file's own
   `gaps` entries for the pattern to follow — e.g. billing/router.py's
   per-route Depends() was not individually re-verified for the ~20 routes
   beyond `pull-stripe` and the two stripe-webhook rows).
5. **A route that MINTS a credential.** Set `issued_credential` and anchor the
   mint site, not the send site — see
   [What a surface ISSUES](#what-a-surface-issues-the-issued_credential-field)
   above. If you traced it and it mints nothing, write `[]`; if you did not
   trace it, write `null` and add the `gaps` entry. Do not leave the field
   absent on a row you touched.
6. **CI gate.** A later lane (L3) enforces this contract the same way
   `ci/check_transitional_inventory.py` enforces the transitional-workload
   inventory: independent re-discovery, unowned-surface detection, and
   staleness/content-drift checks on every anchor. `ci/discover_ops_routes.py`
   is that gate's discovery half.
