# ci/bigboy — tracked bigboy venue tooling

Tracked home of the bigboy-cut.sh script family and the bigboy-specific compose overlays
(CHAOS-6964, CHAOS-6977). The host's own operational copies live in `_records/` on the bigboy
host and are kept byte-identical to this directory by hand at every change (there is no automated
sync yet).

## Compose invocation (CHAOS-6987, D2724/D2726/D2728)

Every `docker compose` invocation against this stack MUST pass `--env-file ops/.env` explicitly.
Docker Compose's own default `.env` auto-load only reads a `.env` at the **project root**, never
`ops/.env` -- so any compose-level `${VAR}` substitution referencing a var that only lives in
`ops/.env` (e.g. `query-api`'s `GO_API_EDGE_JWT_SECRET: ${JWT_SECRET_KEY}`) silently resolves to a
blank string with a `"variable is not set"` warning easy to miss, not a hard failure. `bigboy-cut.sh`
passes it on every invocation; do the same for any manual `docker compose up` against this overlay.

The full invocation (from `/home/ubuntu/devhealth`):

```
export BIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@<digest>
docker compose --env-file ops/.env \
  -f compose.yml -f compose/compose.go.workers.yml -f compose/compose.metrics-api.local.yml \
  -f .remember/lanes/team-lead/reconciler-sweep-override.yml \
  -f compose/compose.bigboy.images.yml -f compose/compose.bigboy.workers.yml \
  -f compose/compose.bigboy.router.yml \
  up -d --no-deps --no-build <service...>
```

`compose/compose.bigboy.router.yml` (this repo's tracked copy also lives here as
`ci/bigboy/compose.bigboy.router.yml`) is REQUIRED in that `-f` list from now on -- omitting it
silently drops the plane-split router (see below) and every request falls back to the pre-CHAOS-6987
"everything through the Python api" behavior.

## Plane-split router (CHAOS-6987, D2724)

Bigboy has no k8s Ingress, so unlike prod it had no mechanism to route go-served REST paths
(`ingress.goApiPaths`/`ingress.queryApiPaths` in deploy's `values.prod.yaml`) to `go-api`/`query-api`
directly -- every request went through the Python `api` service, which 500s unconditionally on any
route whose Python body has been deleted (CHAOS-6241 class). This surfaced as a real P1: chris's
real-org dashboard showing "no data" through the `commanderkeen.dev` Cloudflare tunnel into bigboy.

Fix: `generate-plane-split-router.py` derives a traefik file-provider dynamic config (routers +
services, one per plane) straight from `ingress.goApiPaths`/`ingress.queryApiPaths` -- never
hand-maintained. `compose.bigboy.router.yml` adds traefik's `--providers.file.directory` flag +
volume mount for that generated file, and points `web`'s `BACKEND_URL` at the router
(`http://traefik:3000`) instead of straight at `api:8000`, so both browser-origin traffic
(`commanderkeen.dev`) and web's own server-side calls get the same split.

Regenerate after every `ingress.goApiPaths`/`ingress.queryApiPaths` change:

```
python3 ci/bigboy/generate-plane-split-router.py <deploy-repo>/values.prod.yaml --format dynamic \
  > .traefik-dynamic/planes.yml
```

then recreate ONLY `traefik` (file-provider watch picks it up live, no restart strictly required,
but a clean recreate after a real deploy-repo change is the safe default).

### D2733 regression + fix: `go-api-paths`/`query-api-paths` must match `Host(traefik)` ONLY

The first version of this router matched `Host(commanderkeen.dev) || Host(www.commanderkeen.dev)
|| Host(traefik)` on ALL three routers, on the theory that browser-origin and web's own
server-side traffic needed the identical split. That let a real browser's OWN client-side request
for any REST path in these lists (e.g. clicking an "Explain" button, opening a panel) bypass `web`
entirely -- and with it, `proxy.ts`, the ONLY place that turns the NextAuth session cookie into an
`Authorization: Bearer` header (`apiClient`'s `getServerAuthHeaders` deliberately returns `{}` in
the browser: "the backend remains the authentication boundary"). The request landed on
go-api/query-api with no auth carrier at all -- a real, reproducible P1 (CHAOS-6987 follow-up,
D2733): confirmed live, a bare unauthenticated request straight to traefik with
`Host: commanderkeen.dev` 401'd `/api/v1/work-units`, `/api/v1/investment` and `/api/v1/home`
*identically* -- proving it was never route-specific, only a question of whether that page's data
happened to be fetched server-side (`apiClient` DOES attach auth there) or client-side (it never
does, by design).

Fixed: `HOST_RULE = "Host(\`traefik\`)"` for `go-api-paths`/`query-api-paths` too, matching
`api-internal-catchall`'s already-correct pattern. A real browser request on `commanderkeen.dev`
now always lands on `web`'s own pre-existing docker-label router first (unaffected, since it has
no competing `Path` restriction), which runs `proxy.ts` and re-issues the request server-side via
`BACKEND_URL=http://traefik:3000` -- arriving back here as `Host: traefik`, now correctly
authenticated. Server-side calls (already `Host: traefik`, already carrying a real Authorization
header) are unaffected. Verified post-fix: the same bare-request probe now gets a 303 to
`/auth/signin` (proxy.ts's own designed unauthenticated redirect), and a real Authorization header
sent directly with `Host: traefik` still reaches `plane=go` correctly.

## query-api env: flags + secret refs (CHAOS-6967(b), CHAOS-6987/D2728)

`generate-query-api-enabled-flags.py` derives BOTH classes of env var query-api needs from
`ops.queryApi.extraEnv` in deploy's `values.prod.yaml`:
- `GO_API_*_ENABLED` literal flags (unchanged since CHAOS-6967(b)).
- `valueFrom.secretKeyRef`-backed names (`GO_API_EDGE_JWT_SECRET`, `LLM_PROVIDER`, `OPENAI_API_KEY`,
  `LLM_MODEL`, `SETTINGS_ENCRYPTION_KEY`), emitted as a compose `${KEY}` substitution using the same
  key name `ops/.env` already uses for every other service.

D2728 found the exact same silent-drift class that CHAOS-6967(b) named for the flags ALSO applied
to the secret refs: `GO_API_EDGE_JWT_SECRET` (and, once the generator was extended to check for it,
`LLM_PROVIDER`/`OPENAI_API_KEY`/`LLM_MODEL`/`SETTINGS_ENCRYPTION_KEY` too) were simply never set on
query-api at all -- a real user's JWT was rejected outright with no way to serve real dashboard data.

`bigboy-cut.sh`'s `query-api-enabled-flags-current` STEP now does two checks, both required for
`DEPLOY_CHECKOUT=<deploy repo worktree>`:
1. Every NAME the generator emits (flags + secret refs) is present in the checked-in
   `compose/compose.bigboy.images.yml` -- fails loud (STEP rc=1) naming exactly which name(s) are
   missing, rather than a silent 404/401 surfacing hours later through a real user.
2. Every secret-ref NAME resolves to a non-blank value through the LIVE, `--env-file`-substituted
   compose config -- fails loud (separate STEP `query-api-secret-refs-nonblank`, rc=1) if any
   resolves blank, which is exactly what a missing `--env-file ops/.env` produces.

## Suspected gap 4: ruled a mis-aimed probe (D2732), not a defect

A direct POST of a real end-user JWT straight to `query-api:8090/query` 401s. That is
`internal_auth.go`'s `authenticateInternalRequest` working as designed: `/query` accepts only the
internal-listener identity headers or a Bearer **envelope** token (Ed25519 JWKS at
`GO_API_ENVELOPE_JWKS_PATH`) -- never a raw end-user HS256 JWT. `GO_API_EDGE_JWT_SECRET` (D2728)
governs the browser-reachable `/api/v1/*` REST routes only, not `/query`. Confirmed live:
query-api's own log shows `envelope.rejected reason=bad_signature kid=""` for that probe, i.e. the
designed rejection, not a hang or a silent failure. The `*_database_configured`/`*_clickhouse_configured`
startup-log fields that looked suspicious are a parity question against prod's own query-api env
block (`values.prod.yaml`), not evidence of a defect on their own.

The real GraphQL path for a real user is through the edge (Python `api` `/graphql`, or web), which
either forwards an enabled operation with an envelope-signed dispatch or serves it itself.
Verified live: the bigboy routeswitch ledger (`dho goapi routing status`) shows 47/55 catalog
operations enabled (mode=canary, rollout=100; 8 MISSING -- the saved-reports family plus
testopsRisk); one real edge-forwarded op (`featureFlags`, the exact registered document text, a
real user JWT through Python api's `/graphql`) returned 200, `X-Dev-Health-Plane: go`, and real
rows for the real org; and the edge's own fallback behavior on a query-api rejection
(`go_api_dispatcher.py`) is confirmed LOUD by source -- `_forward_to_go` has no silent-fallback
branch, any non-200 goes through `_go_failed` (logged, counted, a typed GraphQL error), never a
quiet retry on Python.

## Web-path smoke (CHAOS-6987/R460)

`web-path-smoke.sh` (+ `web-path-smoke.py`, `compose.bigboy.smoke.yml`) is the one proof that
closes the loop for a REAL browser session, not a hand-minted token: it logs in through web's own
`/api/v1/auth/login` route as the bigboy admin, then issues the EXACT operations web's Cockpit
(`/api/v1/home`, `/api/v1/investment`, `/api/v1/opportunities` REST threads) and Diagnose
(`complexityTimeseries` GraphQL, `COMPLEXITY_TIMESERIES_QUERY` verbatim from
`web/src/lib/graphql/queries.ts`) pages send, through the plane-split router -- asserting 200 +
`X-Dev-Health-Plane: go` + non-empty rows for each, and writing a receipt (`_records/bigboy-<sha8>/
web-path-smoke-receipt.json` from `bigboy-cut.sh`, structural facts only -- status/plane/row
counts, never a token or a body value).

Credentials: `DHO_SMOKE_ADMIN_EMAIL` + `DHO_SMOKE_ADMIN_PASSWORD_FILE` (0600, the `*_FILE`
convention, CHAOS-6972) in `ops/.env`. **This STEP fails loud (rc=1, named reason) while either is
absent or resolves blank** -- presence/blank is checked through `compose-config-redacted.sh`
exclusively (NAME=length only), never a raw read of `ops/.env`. It does not run against the real
org until chris adds both entries.

## D2734: `web` needs `AUTH_URL` too -- same parity class as D2728, on `web` instead of `query-api`

A real user's logout redirected to `http://[::]:3000/` (Node's own listen bind) instead of
anywhere useful. `trustHost: true` is already set in `web/src/lib/auth.ts`, but Auth.js's default
callback-url cookie is computed from `AUTH_URL`/`NEXTAUTH_URL` when set, not purely from the
request's Host header -- and neither was set on bigboy's `web` container at all. Prod's own web
block (`deploy/values.prod.yaml:1015-1016`) sets `AUTH_URL: https://www.fullchaos.dev` explicitly,
with its own doc comment: "without it web derives its origin from the request URL... every
[Origin-checked] approval is refused" -- the exact same drift-without-a-generator-check failure
CHAOS-6987 already exists to close, just on a different service. Fixed: `AUTH_URL:
https://www.commanderkeen.dev` added to `web`'s environment in `compose.bigboy.router.yml`.
Verified live: `GET /api/auth/session` through the router now sets
`__Secure-authjs.callback-url=https%3A%2F%2Fwww.commanderkeen.dev`, not the bind address.

Prod's web `extraEnv` has five other entries (`ACR_API_ORIGIN`, `ACR_WEB_ASSERTION_*`) -- all
in-cluster-DNS/private-key-mount ACR wiring, which bigboy does not run (acr stays off bigboy per
standing rule) and would need its own volume/secret plumbing regardless. `AUTH_URL` is the only
bigboy-relevant entry in that list, so it is hand-maintained here with a citing comment rather than
adding a second full YAML-parsing generator for a single name; re-derive by hand from
`values.prod.yaml`'s web block if this list ever grows a second bigboy-relevant entry.
