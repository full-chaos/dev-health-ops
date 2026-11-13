# Endpoint authentication profiles

!!! info "Machine-readable contract"
    This page is prose over the Go-owned profile rows:
    [`endpoint-profiles.auth-service.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/endpoint-profiles.auth-service.json)
    [`endpoint-profiles.query-api.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/endpoint-profiles.query-api.json)
    and
    [`endpoint-profiles.go-api.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/endpoint-profiles.go-api.json)
    (schema: [`endpoint-profile.schema.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/endpoint-profile.schema.json)),
    checked by the Go tests in `internal/auth/contracts/` (`endpoint_profiles_*_test.go`)
    `internal/queryapi/server/` and `internal/apiservice/` (`endpoint_profile_rows_test.go`). Each row
    carries a `file:line` anchor. This page summarizes; it does not repeat every
    anchor inline.

## Why this exists

A route without a registered profile fails CI and may not ship. The profile
rows classify every REST route and GraphQL resolver of the Go services as
`protected` or `public`, and record its accepted credential class(es) and every
validator that can observe that credential before (or instead of) its intended
one. The credential classes come from
[credential-classes.md](credential-classes.md). A route missing from the rows is
a route the gate cannot guard.

The question "which validators can observe a credential they do not own" is a
property of the middleware stack rather than of any single route, so every row
records it explicitly in `reachable_validators`. That makes it a checkable fact
per surface instead of an argument; see
[Earlier auth gates and foreign-class validators](#earlier-auth-gates-and-foreign-class-validators) below.

## Scope and counts

Each service has its own profile file. The row counts are:

| Service | Protected | Public | Rows |
| --- | --- | --- | --- |
| auth-service | 67 | 28 | 95 |
| go-api | 211 | 33 | 244 |
| query-api | 37 | 8 | 45 |

!!! warning "What a green run does and does not tell you"
    The Go row tests check that every served route has a row and every row has a
    served route. The judgement in each row (`classification`,
    `accepted_credential_classes`, which validator is the intended one) is
    checked for consistency, vocabulary and anchor validity, never for being
    right about what the code does. `exposure` is an asserted boundary: nothing
    here can verify whether a route the app mounts is reachable from outside.

### Deployed apps

Routes are served by the deployed Go app `dev-health-ops-api`. The go-api billing-edge
listener serves the billing host. `service` is a per-deployed-app field.

- `dev-health-ops-api` — `cmd/dho/main.go` via `internal/apiservice/service.go`, the main app. The Go
  transport stack is built by `httpapi.NewServer` (`internal/auth/httpapi/server.go:481`) around the
  middleware that `NewServer` in `internal/apiservice/service.go` passes: on the request path, the
  request id (outermost), the access log and panic recovery, then the app middleware in the order
  given, including the build stamp, the unhandled-error shape, the org scope and impersonation policy from
  `internal/api/policy/scope.go` when a database is configured, the origin check (CSRF), security
  headers and CORS, then the route (with its own rate limit, body bound, deadline and recovery).

Every row's `service` field says which app serves it. `reachable_validators`
is derived from the middleware that app mounts. A route listed in `signatureOnlyRoutes`
(`internal/apiservice/scopeexempt.go`) skips the org scope and impersonation policy, so its row lists no
reachable validators; its handler's signature check is the first check.

## Classification summary

Every protected row carries `accepted_credential_classes` drawn from
[credential-classes.json](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/credential-classes.json)'s
closed vocabulary: no row uses a `class_id` outside that list, and no protected
row has an empty credential-class list. Every public row carries a non-null
`public_rationale`.

By method, `accepted_credential_classes` and
`reachable_validators` are derived from the **middleware stack + mount
path**, not from reading every route body. The remaining fields (`action`,
`resource_resolver`, `entitlement_requirement`, cache class) are filled where
cheaply determinable at the family level and `null` with a `gaps` note
otherwise: a stated reason, not a guess.

## Absent versus null

On the advisory fields (`action`, `resource_resolver`,
`current_state_cache_behavior`, `entitlement_requirement`,
`disclosure_behavior`, `token_shape`) the two states differ (decision sheet B10 shape b):

| State | Meaning | Gate |
| --- | --- | --- |
| absent | Not applicable. Code evidence shows the route has nothing to say. | none |
| `null` | Undetermined. | The row's `gaps` must contain an entry that begins with `<field>:` (for example `action: not traced`). An entry for another field, or prose that merely mentions the field, does not excuse it. |

Never infer "absent" from silence: a missing note is not an examination. This
matches `issued_credential` and `exposure`, which already work this way.

## Earlier auth gates and foreign-class validators

Every entry in a row's `reachable_validators` array carries two required
booleans. They answer two different questions, so they are never summed:

- `foreign_class_validator: true` (the defect shape): a validator
  that does not own a credential's class can observe or try to parse that
  credential before (or regardless of) the route's intended validator. This is
  the number to watch.
- `earlier_auth_gate: true` (a design property): an earlier gate of the same
  class runs before the route's own check. It is defence in depth, recorded so
  that it stays visible.

The schema refuses an entry that sets both, an entry that leaves out either
field, an entry with the retired name `reachable_but_not_owner`, and an entry
that is not the intended validator but sets both to false. Both false is
allowed only on the route's intended validator.

Worked examples:

- Earlier auth gate (ops): `Scope.OrgScope` (`internal/api/policy/scope.go:128`)
  and `Scope.Impersonation` (`internal/api/policy/scope.go:221`) run around
  every api-listener route. They read the caller from the `Scope.Principal`
  decision only (`principalFrom`, `internal/api/policy/scope.go:102-105`); they never read the
  `Authorization` header. The `Principal` step
  (`internal/api/policy/principalonce.go:112`) sends a credential that the
  refusal table or the route's row rules out to no validator.
- Earlier auth gate (web): the session and organization gate of the web
  server's request proxy runs before each route's own check.
- Foreign-class validator: the Python `OrgIdMiddleware` ran every
  `Authorization` bearer (an opaque ACR service credential included) through
  the user-JWT decoder. That code is gone. Today no row in ops,
  web or acr sets `foreign_class_validator: true`.

`contracts/auth/v1/credential-classes.json` lists the validators of each
credential class with the same two fields, but only on a validator that has a
code anchor. A validator with a null anchor (its code was not found or is gone,
four of them retired Python validators) carries neither field: an unverified
`false` would read as a measured 0. The schema lets only an unanchored entry
leave the fields out, and the count names every such entry instead of counting
it.

The count of foreign-class validators, with the entries it does not count named
in its output (run at the ops repository root; add the web and acr profile files
for the platform-wide count):

```sh
jq -n '[inputs] as $d | {foreign_class_validator: ([$d[] | .. | objects | select(.foreign_class_validator == true)] | length), not_counted_unanchored: [$d[] | .classes[]? | .class_id as $c | (.validators // []) | to_entries[] | select(.value | has("foreign_class_validator") | not) | "\($c) validators[\(.key)]"]}' contracts/auth/v1/endpoint-profiles.*.json contracts/auth/v1/credential-classes.json
```

`ci/auth-contracts-ajv/run.test.mjs` runs this command text from this page:
on this repository it answers 0 and names every unanchored validator; on a
fixture with one planted foreign-class entry it answers 1.

## Retired worker service credentials

No endpoint profile accepts the retired `internal_svc_worker_token` class. The worker operator CLI uses a fixed operator principal and database role checks. It does not check service-token scopes. The `dho service-credentials` command group is absent.

Enforced by: `TestNoEndpointProfileRowAcceptsARetiredClass`, `internal/joboperator/auth_test.go`, and `cmd/dho/retired_verbs_test.go`. See [Billing and worker boundaries](billing-and-worker-boundaries.md) for the current controls and limits.

## GraphQL resolvers

All GraphQL resolvers require an authenticated context: the query-api GraphQL
layer refuses a request without an organization (or superuser) context, and the
organization is only populated from the validated credential plus membership.
Per-field `action`/`resource_resolver`/entitlement scoping is not individually
derived for every resolver (`gaps` on every GraphQL row says so explicitly);
those fields are `null` with a `gaps` entry, never a guess.

### query-api per-field action map

The served query-api GraphQL root fields (Query and Mutation of
`contracts/graphql/v1/schema.graphql`) each have exactly one row in
[`graphql-field-actions.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/graphql-field-actions.json):
its `action`, which is `unknown` with an `action:` gap: query-api enforces no
action of the closed catalog (`vocabulary/actions.v1.json`) on a field today. The query-api REST rows carry
the same `action`, `resource_resolver`, `current_state_cache_behavior`,
`impersonation_policy` and `disclosure_behavior` fields.

A row asserts only what a source proof in the Go guards states. Every other
value is `unknown` with a gap that names the check and says it is a declared
gap; no consumer may treat `unknown` as allow, and no field is `null`. The
guards `internal/queryapi/graph/field_action_map_test.go` and
`internal/queryapi/server/endpoint_profile_action_fields_test.go` fail on a
served field with no row or with two, a row for a field nothing serves, an
action outside the catalog, a `null`, an `unknown` without its gap, a statement
no registered claim states, an action with no registered proof, and a claim whose proof is missing from the body of the declaration it names (comments are not read).

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
| `null` | Issuance was **not determined**. MUST be paired with a `gaps` entry, exactly like a null anchor. |
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
*proven* by tracing the route handler through to the mint site. The known
case is dev-health-web, which mints an `acr_web_assertion` on three of its
routes (see the web repo's auth-endpoint-profiles page). **No ops row is
backfilled yet.** ops has obvious issuer candidates — the login and SSO-callback
routes that return `ops_access_token_hs256` / `ops_refresh_token`, and the
device-flow surfaces — but "obvious" is a family pattern, and this inventory
does not record family patterns as facts. They are tracked as an explicit gap
rather than guessed.

## Access-token validation: `access_token_validation`

A row that accepts a bearer access token says how that token is validated.
`access_token_validation` is an object with one member, `legacy_hs256`, a
required boolean:

| Accepted access-token class | `access_token_validation` |
|---|---|
| `ops_access_token_hs256` (the legacy HS256 token) | required, `legacy_hs256: true` |
| `user_access_token` only (the platform EdDSA token) | required, `legacy_hs256: false` |
| neither | forbidden |

The opt-in is explicit: a validator never infers HS256 from the class list
alone, and a route that does not accept the HS256 class cannot opt in. The
accepted classes are the ones in `accepted_credential_classes`; the object
does not repeat them. The field and its rules are defined once, in
`endpoint-profile.schema.json` (`$defs/endpointProfile/properties/access_token_validation`
and `$defs/accessTokenValidationRules`); `endpoint-auth-profile.v1` references
both. The wire profile also holds the HS256 class's pins to algorithm `HS256`.

No row accepts `ops_access_token_hs256`: the class is `retired` in
`credential-classes.json`, and every Bearer row sets `legacy_hs256: false`.
The profile-routed validation middleware picks the HS256 verifier only on a
row that opts in, so it refuses an HS256 bearer on every row with reason
`alg_not_allowed`. The contracts gate
`TestNoEndpointProfileRowAcceptsARetiredClass` refuses a row that lists a
retired class or sets `legacy_hs256: true`.

## Wrong-class gate

`TestWrongClassGateGoAPI` (`internal/apiservice/wrong_class_gate_test.go`)
sends every go-api row, on the listener that its row names, every credential
class of `credential-classes.json` that the row does not accept. The requests
go through the production listeners. The rows come from the embedded profile
file. Each class's carrier (header or body field) comes from its markers. A
row that is not mounted fails, and a marker kind with no carrier rule fails.
A wrong class must give the same answer as a garbage credential in the same
carrier (or no credential, when the carrier is not `Authorization`). It must
cause no signing-key read and no store read. It must not make the shadow ask
the PDP. On a protected row the answer must be a 4xx. A known divergence is
one row of `ci/auth_exception_ledger.tsv`, anchored on the gate, with an open
ticket. It excuses the 4xx rule on the rows that it names, and nothing else.
When a named row no longer diverges, the gate fails until the ledger row is
removed.

`TestWrongClassGateQueryAPI` (`internal/queryapi/server/wrong_class_gate_test.go`)
applies the same rules to the query-api rows, public rows included, on every
listener a row names. The REST rows run on their production route builders.
The `/query` route set and two REST builders need a live ClickHouse at build
time, so their rows run on the listener test stack instead. The census line
names the stack of each row.

`TestWrongClassGateAuthService` (`internal/auth/authruntime/wrong_class_gate_test.go`)
does the same for every auth-service row, on the API, operator and internal
listeners. The API and internal listeners are built with the route builders
that `configureWithRoutes` calls, in its order, with in-memory rate-limit
counters; `TestWrongClassGateAuthServiceRigMatchesConfigure` fails when the
routes or the calls differ from the production build. The store is a listener
that counts each connection. A wrong class must cause no store connection and
no key-set read that a garbage credential does not cause, and none at all on a
protected row. auth-service builds no shadow:
`TestWrongClassGateAuthServiceHasNoShadow` fails when a package of the build
other than `internal/api/policy` uses a policy shadow. Its ledger rows say
which rule they excuse (the 4xx rule, or the 4xx rule and the protected-row
read rule) on the rows that they name. A row whose accepted classes have no
accept control (a provider token, an OIDC code, a SAML response) is named in
the gate's pinned not-measured list; a row that joins or leaves it fails the
gate until the list changes.

## `delegated_bearer`

An auth-service row that `authenticateWith` gates has `delegated_bearer`:
does a delegated session's bearer pass this route's gate. The value is
`allowed` on `GET /v1/sessions/current` and `POST /v1/sessions/revoke`, and
`denied` on the other four rows. The test
`TestDelegatedBearerRowsEqualTheGoAllowlist` holds the values equal to the Go
allowlist. A row that `authenticateWith` does not reach omits the field.

A go-api row that the Guard gates for a user bearer also has
`delegated_bearer`. The Guard reads the values at start, keyed by the route
template, and refuses a delegated session's bearer on a `denied` row with
`403 {"detail":"Not allowed in an impersonation session"}`. A route with no
value refuses it too. The rule: `allowed` only outside `/api/v1/admin`, never
with an action the catalog marks `impersonation: denied`, and only for a `GET`
or `HEAD` or an action the catalog marks `allowed`. Every other row is
`denied`, the writes with no action included: a support need comes as a row
with a catalog action. The deny applies to every delegated bearer,
`platform_owner` too, until 6.12. `POST /api/v1/auth/logout` reads its
optional bearer outside the Guard and takes it to a write (the refresh revoke
and the audit line), so its handler applies the Guard's refusal
(`Guard.RefuseDelegated`) and its row is `denied`. `POST /api/v1/auth/validate`
reads a token in the body, writes nothing and omits the field. The test
`TestDelegatedBearerRowsFollowTheRule` holds the values equal to the rule;
`TestEveryBearerReadOutsideTheGuardIsNamed` fails on a bearer read outside the
Guard that is not named, and on a named reader that takes the bearer to a write
without `Guard.RefuseDelegated`.

This field is not `impersonation_policy`. That field says whether go-api's
org-context override reaches a route.

## One registration per line

A row addresses its surface as `file:line`, so two route registrations written
on the same line cannot both be owned by a row. In `internal/queryapi/server`
and `internal/platform/health`, every `Handle`/`HandleFunc` call and every
route-table `Pattern:` field sits on its own line.
`TestNoTwoRouteRegistrationsShareALine` fails closed with
`MULTIPLE REGISTRATIONS ON ONE LINE` and names both routes. The schema has no
column field; this rule is the chosen alternative. Register each route by a
direct `Handle`/`HandleFunc` call: using either method as a value (an alias)
is refused too, because a call through the alias would escape the scan.
The scan matches by name (`Handle`, `HandleFunc`, and `Pattern:` fields of the
route-table types), so a registration through a wrapper with another name is
not seen; add such a wrapper's name to the scan when one is introduced.

## `source_commit`: what is verified and what is not

Every `contracts/auth/v1/endpoint-profiles.*.json` carries a top-level
`source_commit`, the commit its `file:line` anchors were derived against. The
`auth-test` job (`ci/check_go.sh auth history`, full git history) enforces:

- **Reachable.** `source_commit` is a 40-hex id that resolves in the repo and is
  an ancestor of `HEAD`. A shallow checkout fails the check and says so; it is
  the one benign cause.
- **Not stale.** Every anchor in this repo (a `path`/`line` anchor or a row's
  `source` `file`/`line`) holds in one of two ways: the line reads the same at
  `source_commit` as in the tree under test at the same line number, or the
  tree line's exact text occurs exactly once in that file at `source_commit`
  and exactly once in the tree (a pure line shift, for example after a squash
  merge). Text that did not exist at `source_commit`, or that occurs twice or
  more on either side, fails. An ancestor check alone would pass a stale base,
  so this half is what catches an inventory whose anchors changed after it was
  derived.

- **Declared rewrite.** A change that rewrites the text of an anchored line on
  purpose lists it in the optional top-level `rewritten_anchors`, one
  `{file, line, was, now}` per anchor. For an anchor with an entry, `now` must be
  the tree text at that line and `was` must occur exactly once in that file at
  `source_commit`; every other anchor keeps the rule above. An entry whose tree
  line also occurs at `source_commit` (it rewrites nothing), an entry that
  matches no anchor, an entry listed twice and an entry that is not exactly those
  four keys all fail, so the list is empty again at the next re-pin of
  `source_commit`. `source_commit` is still an ancestor of `HEAD`, always.

Not verified: that the anchored line is the right symbol (the rows gate in
`internal/queryapi/server` owns that), and anchors that name another repo.
When you move an anchor, bump `source_commit` to a commit on `main` at which
every anchor line holds, and empty `rewritten_anchors`.

## Contributor guide: registering a new route or credential class

1. **New route, existing credential class.** Add a row to the profile file of the
   service that serves it (`contracts/auth/v1/endpoint-profiles.query-api.json`,
   `contracts/auth/v1/endpoint-profiles.auth-service.json` or
   `contracts/auth/v1/endpoint-profiles.go-api.json`) with `id`, `route`/`method`
   (or `graphql_field_name`), `source` anchor, `classification`, and — if
   protected — `accepted_credential_classes` from
   `contracts/auth/v1/credential-classes.json`'s closed vocabulary plus
   `reachable_validators` derived from where your router mounts (which
   middleware sees it, per [Deployed apps](#deployed-apps) and
   [Earlier auth gates and foreign-class validators](#earlier-auth-gates-and-foreign-class-validators)
   above). Then run the Go row tests (`internal/queryapi/server/endpoint_profile_rows_test.go`,
   `internal/auth/contracts/endpoint_profiles_query_api_test.go` and
   `endpoint_profiles_auth_service_test.go`): they fail when the route and its
   profile row disagree.
2. **New credential class.** It must exist in
   `contracts/auth/v1/credential-classes.json` first (that file's schema and
   contributor rules are in
   [credential-classes.md](credential-classes.md)) — do not invent a
   `class_id` in an endpoint profile that isn't already registered there.
3. **Public route.** Set `classification: "public"` and a non-empty
   `public_rationale` — every public row must justify why no credential is
   required, not just assert it.
4. **Anything you can't determine.** Leave the field `null` and add a
   `gaps` entry that **begins with `<field>:`** and says what you searched and why
   it's undetermined. Where the code shows the field does not apply (a health
   check has no `action`), leave the field **absent**; never write `null` for
   "not applicable". See [Absent versus null](#absent-versus-null). A stated
   reason is correct; an unstated guess is a defect (see this file's own
   `gaps` entries for the pattern to follow).
5. **A route that MINTS a credential.** Set `issued_credential` and anchor the
   mint site, not the send site — see
   [What a surface ISSUES](#what-a-surface-issues-the-issued_credential-field)
   above. If you traced it and it mints nothing, write `[]`; if you did not
   trace it, write `null` and add the `gaps` entry. Do not leave the field
   absent on a row you touched.
6. **CI gate.** A planned CI gate enforces this contract with independent
   re-discovery, unowned-surface detection, and staleness/content-drift checks
   on every anchor.
