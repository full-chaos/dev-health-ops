# Credential-class inventory (CHAOS-3273 Wave 0 freeze)

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
| `worker_operational_bridge_token` | active | infrastructure | |
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

**30 classes.** 6 were not in CHAOS-3273's minimum list and were discovered
during this inventory (`new_vs_ticket: true` in the JSON):
`internal_svc_worker_token`, `worker_operational_bridge_token`,
`acr_workload_identity_exchange`, `org_invite_token`, `license_key`, and the
distinction between `sso_saml_exchange`/`sso_oidc_exchange` as two classes
rather than one.

## The CHAOS-3271 class: credentials that reach a validator that is not their own

`OrgIdMiddleware` scans every request for an `authorization` header
(`src/dev_health_ops/api/middleware/__init__.py:39-40`) and, if present,
decodes it as a user access JWT
(`get_authenticated_user_from_headers` → `authenticate_access_token`,
same file, lines 36-50) — **regardless of what the token actually is** —
except for one explicit path-prefix exemption:

```python
if path.startswith("/api/v1/internal/acr/"):
    # These routes authenticate opaque ACR service credentials themselves.
    await self.app(scope, receive, send)
    return
```

`src/dev_health_ops/api/middleware/__init__.py:85-89`

Post-fix, a non-JWT bearer value no longer crashes: `AuthService.validate_token`
catches `InvalidTokenError` and returns `None`
(`src/dev_health_ops/api/services/auth.py:336-338`), so the request proceeds
as anonymous through `OrgIdMiddleware` and its *real* validator still gets a
turn. **But the credential still transited a validator it does not own**,
which is exactly the shape CHAOS-3271 warns about. Every class below hits
this:

| Credential class | Route(s) | Exempted from the path prefix? |
| --- | --- | --- |
| `internal_svc_worker_token` (`svc_worker_*`) | none found (see below) | **No** — no HTTP validator exists for it anywhere, so if one were ever wired up at a non-`/api/v1/internal/acr/` path it would silently degrade to anonymous first. |
| `worker_operational_bridge_token` (`WORKER_OPERATIONAL_BRIDGE_TOKEN`/`WORKER_METRIC_REPAIR_TOKEN`/`WORKER_WORKGRAPH_REPAIR_TOKEN`) | `/api/internal/worker-operational`, `/internal/worker`, `/internal/worker/workgraph/v1`, `/api/internal/worker-sync` | **No.** None of these paths start with `/api/v1/internal/acr/`. |
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
(`internal/auth/web_assertion_middleware.go:11-16`) rather than silently
preferring one. This is the shape a class-aware gate should generalize.

## Permissive-when-unconfigured: the legacy ingest path

`src/dev_health_ops/api/ingest/auth.py` documents its own guardrail
violation in its module docstring: *"All layers degrade gracefully when not
configured (permissive mode for development)."*

- `_get_api_keys()` returns `[]` when `INGEST_API_KEYS` is unset, and
  `validate_ingest_auth` only checks the key `if valid_keys:` — so an unset
  env var **skips the check entirely**, not fails it
  (`src/dev_health_ops/api/ingest/auth.py:24-32,97-100`).
- Same shape for the HMAC signature: unset `INGEST_SIGNING_SECRET` skips
  `_verify_signature` entirely (`src/dev_health_ops/api/ingest/auth.py:35-37,102-106`).

This is precisely what the engineering guardrails prohibit: "missing config
must never mean auth disabled in production." Every webhook validator in
`src/dev_health_ops/api/webhooks/auth.py` (GitHub, GitLab, Jira) and the
Stripe webhook (`src/dev_health_ops/api/billing/stripe_client.py:46-50`)
gets this right — they raise `500`/`RuntimeError` when their secret is
unset, i.e. **fail closed**. The legacy ingest path is the one place that
doesn't. This inventory did not confirm which routes are still mounted
behind `validate_ingest_auth` in the current route table — that is the next
step before deciding whether this is a live exposure or dead code (see gaps
below).

## Unconsumed credential: `svc_worker_*`

`WORKER_OPERATOR_TOKEN_PREFIX = "svc_worker_"` is defined and minted through
the exact same code path as `svc_acr_*`
(`src/dev_health_ops/models/internal_service_credential.py:14,17-23`), with
its own scope set (`workers:read`, `workers:operate`,
`src/dev_health_ops/service_credentials.py:16`) and a full CLI lifecycle
(create/list/rotate/revoke). No HTTP route in ops, web, or acr was found
that validates a `svc_worker_`-prefixed bearer token — `internal/acr.py`'s
`_extract_token` only accepts the `svc_acr_` prefix. Either this is
scaffolding for a not-yet-wired consumer, or a consumer exists in the Go
worker binaries or `dev-health-go` outside this lane's assigned read scope.
Flagged rather than guessed.

## Not anchored this pass

- **`infrastructure_deployment_credentials`** (DB/Kubernetes/TLS/deployment
  identities) — only confirmed the deployment manifests exist
  (`deploy/kubernetes/secrets.yaml`, `deploy/helm/dev-health/templates/serviceaccount.yaml`);
  no application-code read/validate path was traced. This is the
  least-verified class in the inventory.
- **`acr_workload_identity_exchange`**'s subject-token (Kubernetes
  ServiceAccount JWT) validator lives in `github.com/full-chaos/dev-health-go/authverify`
  — a fourth repo outside this lane's assigned scope (ops/web/acr only).
- **`sso_saml_exchange`** / **`sso_oidc_exchange`** — the router's gating
  (`@require_feature("sso")`) and schemas are anchored; the actual
  SAML-signature/OIDC-code verification logic in
  `src/dev_health_ops/api/services/sso.py` was not read this pass.
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
