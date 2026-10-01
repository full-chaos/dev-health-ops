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

### One path, one backend

The generator REFUSES (exit 4, nothing emitted) a values file in which a path the Python api is
given and a path a Go plane is given can match the same request. On bigboy the Go plane would win
by priority while prod's ingress controller picks by its own rule order, so bigboy would prove a
route prod may not have. The check is on the paths the rules match, not on the text of an entry:

- It reads every Python source: `ops.ingress.pythonAllowList`, each host's own `pythonAllowList`
  list (prod's in-cluster host carries one; only prod routes it, and a flip that forgets it is
  half a flip), and the local-only paths this router adds (`BIGBOY_LOCAL_PYTHON_PATHS`).
- A `Prefix` is read at its widest: every path that starts with its text. That is what
  ingress-nginx renders for a Prefix on a host in regex mode, and it contains this router's own
  reading (the path, or anything under it).
- A Go plane wildcard (`{param}`, `[^/]+`) never crosses a `/`, so the answer is exact.
- An allow-list entry may name its backend: `service: query-api`. The router then sends that
  path to query-api and leaves it out of the Python rule, and the entry counts as a Go plane path
  in this check, on whichever list it is: the same path still on Python on another list is half
  a change and is refused. Such an entry must be ANCHORED (`ImplementationSpecific`,
  `/literal$`, no `.` or `..` segment): the one shape that is one path on every host. An
  `Exact` or `Prefix` entry is refused: the chart reads those by the host they are on.
- A second refusal, `one path, a backend that differs by host`. This router has one host and
  prod has several, so it can prove the backend of such a path only when every host that
  serves the api gives it that backend. Each host of `ops.ingress.hosts` must be one of two
  kinds: a host that carries the entry on the allow-list it uses (the shared one if it opts in
  with `pythonAllowList: true`, or its own) and has no path of its own beside `/`; or a web
  host (all its paths go to `web`). Any other host is refused. A list that no host uses is not
  read: the chart renders nothing from it.
- This generator does not read the chart's rules a second time for such an entry: a shape it
  would have to read the way the chart does is refused. The shape in use passes: a public api
  host on the shared list, an in-cluster api host with its own list, a web host.
- One more refusal, `one path, two Ingress objects`: an entry that names query-api while
  `ingress.goApiPaths` or `ingress.queryApiPaths` also claims the path. Prod renders the
  allow-list in one Ingress object and the path tables in others, and its ingress admission
  denies two live objects for one host and path.
- An allow-list entry of a shape the ops chart does not accept (an unanchored
  `ImplementationSpecific`, an unknown `pathType`) is refused too, not read as a literal.

A Go plane path with a character that needs a regex escape (a dot, for one) is refused as well
(exit 5): its rule cannot be written in the router file, the file would not load, and traefik would
keep the old router with no failing step.

The refusal names the list, the entry and the Go plane path. A values change that moves a path
to a Go plane takes it off every Python list in the same commit. A local-only path that a Go
plane starts to serve is removed from `BIGBOY_LOCAL_PYTHON_PATHS` in this repo first.

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
`DEPLOY_SHA=<pinned deploy commit>` (see "Pinned deploy values" below):
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

CHAOS-7190: it also sends the three seeded read documents (`HOME_QUERY`, `RECOMMENDATIONS_QUERY`,
`WORK_ITEM_TEAM_ATTRIBUTIONS_QUERY`, verbatim from the same file) and asserts 200 + `X-Dev-Health-Plane:
go` + no GraphQL errors + the expected `data.<op>` shape; a Python-plane answer fails as `<op>_plane_python`.
The web source mounted at `./web/src` must be the deployed web build (it must define those constants).

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

## Drift checks added after D2735/D2736 (CHAOS-6987 follow-through)

Every check below fails loud with a named finding; none can pass on an empty or unread input.

- **Names-only container env reader** (`container-env-names.sh`, R462/R463): the only sanctioned way
  to read a container's environment on bigboy. The name is cut inside docker's own Go template, so a
  value never leaves the docker CLI; any line that is not an env-var name is withheld and exits 3.
- **Router host scope** (`tests/tooling/test_bigboy_plane_split_router.py`): every rule the generator
  can emit (file-provider and label form) and the checked-in example must be exactly
  `Host(`traefik`)` or `Host(`traefik`) && PathRegexp(...)`. A public hostname, a second Host
  matcher, or an `||` that lets a rule match without the Host clause fails (the D2735 shape).
- **Live router current** (`bigboy-cut.sh` STEP `router-current`, needs `DEPLOY_SHA`): the live
  `.traefik-dynamic/planes.yml` must be byte-identical to the generator's output for the pinned
  deploy sha (see "Pinned deploy values" below).
- **Web env parity** (`check-web-env-parity.py`, STEP `web-env-parity`): every prod `ops.web.env` /
  `ops.web.extraEnv` NAME must be classified (overlay vs venue-local base) -- a new prod name fails,
  named; overlay names (`BACKEND_URL`, `AUTH_URL`) must be in `compose.bigboy.router.yml` with the
  expected public values; the running web container (names via `container-env-names.sh`) must carry
  every prod name. `AUTH_URL` stays hand-maintained per the lead's ruling; this check is what makes
  the hand list loud.
- **Routing-ledger parity** (`routing-ops.txt` + `check-routing-parity.py`, STEPs `routing-enable` /
  `routing-parity`): `routing-ops.txt` is the list of GraphQL operations enabled on prod. Each cut
  enables whatever listed operation bigboy lacks (`dho goapi routing enable`, envelope minted inside
  `venue-tools`, fixture org), then a fresh `status -json` must match: listed ops reachable, nothing
  unlisted reachable, every row canary/100/go. `testopsRisk` is `KNOWN-MISSING CHAOS-6993` (the
  enable proof gate needs a bigboy go-api-prove run; do not bypass it): the STEP exits 3, never 0,
  and fails once it becomes reachable so the marker is removed.
- **Web-path smoke, real browser session** (`web-path-smoke.py`): now logs in through Auth.js
  (csrf + Credentials callback) with the PUBLIC Host header on traefik, so every call takes the
  browser's path through `web`'s proxy.ts. Checks: unauthenticated public-host request lands on web
  (303, no plane header); callback-url cookie and sign-out redirect carry the public origin; backend
  `/health`; Cockpit threads; `filters/options`; `investment/explain`; `work-units`
  (`include_textual=true`); `drilldown/prs` feeding a real `flame` entity; GraphQL
  `complexityTimeseries`, `workGraphFlow` (no empty `nodeType`) and `testopsRisk`; the
  `/testops/risk` page. GraphQL documents are derived from web source through urql's formatDocument
  transform and must hash to the edge catalog's registered digests at the deployed sha
  (`DHO_SMOKE_CATALOG_FILE`, or `DHO_SMOKE_OPS_SHA` for a standalone run). Exit 3 = passed except
  named KNOWN-MISSING checks (read from `routing-ops.txt`).

### Running the web-path smoke

- **Inside a cut:** `bigboy-cut.sh` fetches the edge catalog at the cut's sha into
  `_records/bigboy-<sha8>.catalog.json` and exports it as `DHO_SMOKE_CATALOG_FILE`. Nothing else to set.
- **Standalone (no cut):** set `DHO_SMOKE_OPS_SHA` to the ops sha bigboy is running (the last cut's
  full sha). The script fetches `src/dev_health_ops/api/graphql/go_api_operations.json` at that sha
  with `gh api`. Without `DHO_SMOKE_CATALOG_FILE` or `DHO_SMOKE_OPS_SHA` it fails with exit 1 and names
  both variables. Example: `DHO_SMOKE_OPS_SHA=<full sha> bash ci/bigboy/web-path-smoke.sh`.
- Both need `DHO_SMOKE_ADMIN_EMAIL` and `DHO_SMOKE_ADMIN_PASSWORD_FILE` in `ops/.env`.
- Exit codes: 0 pass; 1 fail (named); 2 refused target (`DHO_SMOKE_BASE_URL` outside the allowlist);
  3 passed except named KNOWN-MISSING checks.

## GraphQL prove harness (CHAOS-6993, partial)

`bigboy-graphql-prove.sh <full ops sha> [--go-edge]` + `compose.bigboy.prove.yml` run the prove leg of
prod's STEP 216 (`pod-r216.sh`) on the compose stack, from `venue-prove`. The edge is the caller's
explicit choice: by default the Python edge (`localhost:8000`, api's network namespace: the prover's
Python-reference mode); with `--go-edge` the ROUTED `/graphql` (`http://traefik:3000/graphql`, which
the plane-split router sends to query-api once the pinned deploy values list `/graphql` in
`ingress.queryApiPaths`, CHAOS-6263: the prover's Go-edge mode, `dho goapi prove -go-edge`). In
Go-edge mode every proof is the candidate alone, and the prover refuses by name if a Python plane
still answers the routed `/graphql`. The steps:

1. refuse unless `venue-prove`'s image is `go-api-tools:sha-<sha>` (prover build skew);
2. derive the local org read-only (the single org of the local admin account; never printed);
3. `dho goapi routing repoint` every row to the running build (provenance only; prove refuses stale rows);
4. `dho goapi prove` for the local org -- read-only against org data, writes proof receipts only;
   both credentials are minted in process (envelope key loaded from the mounted file inside the
   container; `JWT_SECRET_KEY` by compose substitution from `ops/.env`);
5. `dho goapi routing enable` for each `KNOWN-MISSING` op in `routing-ops.txt`, then the parity check.

Full output names the org and stays under the devhealth-root `_records/bigboy-<sha8>/graphql-prove-<ts>/`.
`BIGBOY_ROOT` (default `/home/ubuntu/devhealth`) is the running tree the harness works in, the same
parameter `bigboy-cut.sh` reads.

First live run (4f014a9d): repoint 49 rows (47 changed), prove attempted=250 executed=183
PROVEN_GO_ONLY=178. **testopsRisk is still not provable here**: it has no routing row, so prove
refuses it (`operation_not_routed_to_go`). An unrouted op can only be proven as a `shadow` row through
query-api's measurement route `POST /query/proof`, which query-api registers only when BOTH are set:

- `DEV_HEALTH_ENV` = a declared non-production posture (e.g. `bigboy`; never `prod`/`production`)
- `GO_API_PROOF_ROUTE_ENABLED=true`

Bigboy's query-api sets neither (same posture as prod). The remaining sequence is an **operator
step**, one sitting, and this lane's permission classifier denied it (not attempted):

1. set both names on query-api, recreate query-api only; probe that `/query/proof` is not reachable
   through the public host (it must land on web) nor through `Host: traefik`;
2. `dho goapi routing disable -operations testopsRisk -mode shadow -apply ...`, then repoint;
3. `dho goapi prove ... -proof-url http://query-api:8090/query/proof`;
4. `dho goapi routing enable -operations testopsRisk -mode canary ...`;
5. remove both names, recreate query-api, check with `container-env-names.sh` that both are gone,
   re-run the 12-path plane readback, and expect `check-routing-parity.py` to report 50/50 once the
   `KNOWN-MISSING` marker is removed from `routing-ops.txt`.

If a step fails, revert step 1 first. No named limit in `goserved_ledger.json` is used for testopsRisk.

## Pinned deploy values (R467)

Deploy values can run AHEAD of the running build: a deploy PR that routes new paths to Go can merge
before the ops change that serves them is in any image (rev 195's SSO group: deploy main routed 5
SSO paths while bigboy still ran 4f014a9d). So the values source is an explicit input per cut:

- `DEPLOY_SHA` (in the round's `round.env` and recorded in `_records/bigboy-<sha8>.deploy-sha.txt`),
  read from `DEPLOY_REPO` (default the devhealth-root `deploy` checkout) with `git show` -- never a
  working tree, never "deploy main".
- STEP `deploy-pin`: `generate-plane-split-router.py --deploy-repo --deploy-sha --expect-ops-sha $NEW
  --values-out ...` REFUSES (rc=3) when that deploy commit's `vendor/dev-health-ops` pin is not the
  build the cut runs. Every values-driven STEP (router-current, query-api flags, web-env parity,
  route-coverage) reads the file it wrote. Without `DEPLOY_SHA` those STEPs report rc=2 SKIPPED.
- STEP `route-coverage` (`check-route-coverage.py`): one `ROUTEPROBE` request per routed path to each
  Go plane; net/http's method-aware mux answers 405 (or 401 behind auth) for a registered path and
  404 for none, before any handler runs. A 404 fails the STEP by name.

Limits, stated plainly: the vendor-pin check cannot tell a deploy commit whose values are ahead of
its own vendor pin (deploy main at R467 vendored 4f014a9d too), and the ROUTEPROBE check cannot tell
a registered stub from a real handler (bigboy's 4f014a9d go-api answers the OIDC paths with 405
because a placeholder is registered). The control for R467 is choosing `DEPLOY_SHA` = the deploy
commit that was rolled (or proven) with this build -- today **8028ff17** for ops 4f014a9d.

Regenerate the live router from a pinned sha:
`generate-plane-split-router.py --deploy-repo /home/ubuntu/devhealth/deploy --deploy-sha 8028ff17
--expect-ops-sha 4f014a9d9babfff17b6ca71a33778f2f69d30737 --format dynamic > .traefik-dynamic/planes.yml`
(write to a temp file and rename; traefik hot-reloads).

## Billing host on go-api; the Python billing-edge is retired (CHAOS-7055)

Prod serves the Stripe webhook host from go-api's billing-edge listener (:8010) and has no Python
`billing-edge`. `compose.bigboy.billing-edge.yml` does the same here: go-api gains
`--api-billing-edge-addr=:8010` and a traefik router for `Host(billing.localhost)` (the rule and entrypoint the
Python service carried) to port 8010; `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` and `LICENSE_PRIVATE_KEY`
reach go-api by name from `ops/.env` (never written in the file; unset, the listener's `/health` names the
missing one with a 503); the Python `billing-edge` service moves to the profile `retired-billing-edge`, which
nothing enables. The file is in `bigboy-cut.sh`'s `COMPOSE_FILE` chain before the router (the router stays
last), so go-api is never recreated without them.

Applying it recreates go-api, so it goes in with a cut, never on its own while go-api is in use, and the
checkout the cut's scripts read (line 39's list) is pulled to the merge right before that cut, not earlier. ORDER:
(1) name the Python container and its state (`docker compose ps billing-edge`), (2) remove it with compose verbs
(`docker compose ... rm -sf billing-edge`; it is base-file drift, the cut has no rm step; one approved line, run
on the lead's GO), (3) run the cut. The go-api router is `billing-go` so the old container's router `billing` is
never redefined while both exist. All three values are present in `ops/.env` on this host (checked by name and length only), so the listener starts
configured; whether go-api starts with them EMPTY was not executed (compose.go.workers.yml's go-api comment says its
`/health` then answers 503 naming the missing one). Read the rendered chain only through
`compose-config-redacted.sh`. Proof: the billing host answers from go-api :8010 through traefik, and
`docker compose ps` no longer lists `billing-edge`.
