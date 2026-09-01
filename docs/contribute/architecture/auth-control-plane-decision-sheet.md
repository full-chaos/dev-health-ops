---
page_id: con-auth-control-plane-decision-sheet
summary: One-pass ratification sheet for the twelve Auth Control Plane ADR drafts — recommendation, alternatives considered, and risk for each — plus the contract changes and named residual risks that need a ruling rather than an implementation.
content_type: architecture
owner: engineering
source_of_truth:
  - docs/contribute/architecture/auth-control-plane-adrs.md (the ADR drafts this sheet ratifies)
  - contracts/auth/v1/ (the frozen Wave 0 inventory and schema)
applicability: current
lifecycle: active
---

# Auth Control Plane decision sheet

Every draft in [Auth Control Plane ADRs](auth-control-plane-adrs.md) is at status
**Proposed**. This sheet exists so all of them can move to **Accepted** in a
single pass: one recommendation each, the alternatives that were actually
considered, and the risk of taking the recommendation. Wave 0's freeze is
defined as *all drafts Proposed + this sheet delivered + a ratification pass*.
{: .fc-page-lede }

!!! info "How to use this"
    **Part A** is twelve ADR decisions. **Part B** is nine contract changes that
    need a yes/no, not a design. **Part C** is the residual risks that stay open
    whatever is decided — they need to be *acknowledged*, not resolved, and
    naming them is the deliverable. **Part D** lists the work this sheet
    believes needs tickets.

    Rejecting a recommendation is a legitimate outcome throughout. What this
    sheet is built to prevent is a recommendation passing **silently**.

!!! success "G-1 is enforced, not intended"
    The Wave 0 CI gates are built and committed — ops
    `ci/check_endpoint_profiles.py` (`0a7ff7ccd`) and acr's Go equivalent — and
    both were verified to **reject seeded violations** rather than merely to
    pass on clean input: dropped rows, off-vocabulary class ids, drifted
    anchors, and an unexplained null. Several recommendations below assume that
    enforcement exists; it does. Note the gate's stricter-than-prose rule: a
    `gaps` entry must **name the field it excuses**
    (`_gaps_mentions`, `:459`), so one honest gap cannot launder every null in
    the row.

---

## Part A — the twelve ADRs

### ACP-ADR-01 — Go dependency selection

| | |
|---|---|
| **Recommend** | **Accept.** Adopt `golang-jwt/jwt/v5` with `cmd/query-api/internal/principal/verifier.go` promoted to the canonical verification pattern; `dev-health-go/authverify` for JWKS; `net/http`+`ServeMux`; Ed25519/EdDSA for every asymmetric platform token; `pgx/v5`. **Defer OAuth/OIDC, SAML, and password hashing to a named Wave 1 addendum.** |
| **Alternatives** | (a) Select all six now — rejected: three of them had no candidate evaluated in Wave 0, and inventing one in an ADR is how an unreviewed dependency becomes a decision. (b) A third-party router — rejected: TRD §6 wants strict method+path registration, which `ServeMux` gives, and `query-api` already runs on it. (c) RS256 — rejected: `Ed25519JWKSVerifier` is Ed25519-only by design and the platform's other asymmetric key is already Ed25519. |
| **Risk of accepting** | **Low.** The main risk is the deferral: if the Wave 1 addendum slips, Wave 2 (human identity parity) has no SAML/OIDC library and stalls. Mitigation: the addendum is a Wave 1 exit criterion, not a Wave 2 entry criterion. |
| **Risk of rejecting** | Wave 1 starts with no pinned security dependencies, and the first PR picks them implicitly. |

### ACP-ADR-02 — Signing-key custody and the KMS adapter

| | |
|---|---|
| **Recommend** | **Accept.** Custody behind an `internal/keystore` interface; KMS or secret store in production; **`web`'s file contract (`O_NOFOLLOW`, regular file, size-bounded, `mode & 0o077 == 0`, algorithm type check) promoted to the platform requirement** for self-hosted; direct-value signing secrets prohibited, deprecating the inline-PEM `GO_API_ENVELOPE_PRIVATE_KEY`; every key gets a `kid` and a JWKS entry from day one. |
| **Alternatives** | (a) Keep env-var custody and revisit later — rejected: the `api` process currently holds **two** minting keys, so "later" is the point at which the blast radius has already been inherited by the new architecture. (b) Standardise on inline PEM instead of files — rejected: TRD §6 requires direct values and `_FILE` sources to be mutually exclusive, and `web` already implements the harder, safer half. |
| **Risk of accepting** | **Medium.** Naming no KMS keeps the decision cheap but means the adapter is written against an unknown. A `kid` on every key including compatibility issuers is real work in Wave 1 for a benefit that only appears at the first rotation. |
| **Risk of rejecting** | G-18 overlap rotation stays unrepresentable (HS256 has no `kid`), and the two Ed25519 keys keep two different custody postures for the same job — which is how a rotation runbook ends up covering one and not the other. |
| **Note** | This ADR does **not** fix `JWT_SECRET_KEY`. That is Wave 3's asymmetric-issuer work. The ADR's job is to ensure the replacement does not inherit the posture. |

### ACP-ADR-03 — Token TTLs

| | |
|---|---|
| **Recommend** | **Accept with one item flagged.** 10-minute access token (target), 60-minute compatibility, 7-day rolling refresh **unchanged**, 5-minute workload token, 15-minute delegated session, **60-second envelope with the per-call override removed**, 30-second ACR assertion unchanged, ≤30-second decision cache. |
| **Alternatives** | (a) Narrow refresh below 7 days — rejected: TRD §11 permits it and nothing in Wave 0 argues for it; changing a user-visible session length without a reason is churn. (b) 10-minute workload token (the wide end of TRD's range) — rejected: workloads re-exchange cheaply and have no user-facing cost. (c) Leave the envelope override in place — rejected, and this is the flagged item: **a security bound any call site may widen is not a bound.** |
| **Risk of accepting** | **Medium, concentrated in one number.** The 10-minute access token is TRD's target adopted **on principle, not on load evidence** — no refresh-rate measurement at 60 minutes was taken in Wave 0. If refresh volume is higher than expected, 10 minutes is a availability problem discovered late. |
| **Mitigation** | G-74 load tests precede cutover and are the falsification point. Treat 10 minutes as provisional until then and say so in Wave 3's exit criteria. |
| **Risk of rejecting** | The envelope keeps a widenable TTL, which is the only control on a token carrying a full permission set. |

### ACP-ADR-04 — Schema ownership and aggregate transition

| | |
|---|---|
| **Recommend** | **Accept.** Separate schema and role (not a separate cluster); `auth-migrate` as a separate binary with no runtime auto-migration; aggregate-by-aggregate transfer, **sessions first**; each transfer removes the Python write path in the same change; destructive cleanup only under G-63. |
| **Alternatives** | (a) Separate database cluster — rejected: it breaks the single-transaction state-plus-outbox write G-53 requires, which is the mechanism that makes provisioning events trustworthy. (b) Principals first — rejected: principals are the longest-lived state and therefore the most expensive rollback; sessions expire on their own, which makes the first transfer the cheapest one to get wrong. |
| **Risk of accepting** | **Medium.** Sharing an instance means an auth-service incident and an ops-API incident can share a failure domain. Accepted deliberately in exchange for transactional integrity. |
| **Open, and this needs a ruling** | Whether the auth schema joins the existing ops alembic lineage or starts a second one. The ops lineage has head values pinned as literals across many test files, so a shared lineage makes every auth migration a cross-cutting change. **Recommend a second lineage**; flagged here rather than decided because it affects tooling beyond auth. |

### ACP-ADR-05 — Policy representation and revisions

| | |
|---|---|
| **Recommend** | **Accept.** Closed, versioned action vocabulary where unknown denies; policy as data, not code; monotonic `policy_revision` bound into every allow-cache key; explicit action names at call sites; vocabulary derived from the inventory where anchored and from source review where not. |
| **Alternatives** | (a) Adopt an external policy engine now — rejected, consistent with TRD §25: it would fix the representation before the decision contract is stable. (b) An expression language in v1 — rejected by TRD §14 and by G-26; an expression that can be written can be written wrong, and nothing denies by default. |
| **Risk of accepting** | **Medium.** The action vocabulary is the largest unbounded piece of work in the program and it cannot be generated from the inventory: per-route action/resource/entitlement was **not derivable for all 59 GraphQL resolvers**, and ~17 of billing's ~20 routes were classified by family pattern with a gaps note. Those are hand-review items, and hand review at that scale is where a wrong action name enters and is never noticed. |
| **Mitigation** | Treat the action vocabulary as a contract with its own gate — closed, anchored, and re-discovered from source — exactly as the credential vocabulary was. |

### ACP-ADR-06 — Compose bootstrap and Kubernetes federation

| | |
|---|---|
| **Recommend** | **Accept, including the retirement.** TokenReview-based Kubernetes federation as already implemented; named file-based bootstrap accepted only at the exchange endpoint; one workload principal per service or worker class with exact audiences; **`svc_worker_*` retired rather than migrated**; infrastructure identity stays an independent layer. |
| **Alternatives** | (a) Build an exchange for `svc_worker_*` — rejected: it has a real DB-backed validator (`internal/joboperator/auth.go:54`) with **zero HTTP wiring** anywhere and no reference at either pinned `dev-health-go` tag. Building an exchange for a class with no caller is work that also creates a validator the CI gate would then have to encode. (b) Replace the live TokenReview call with a local JWT decode for latency — rejected: it converts a current-state check into a token claim, which is G-30 exactly. |
| **Risk of accepting** | **Low–medium.** The retirement is the only real risk and it is bounded: G-63 still requires evidence before removal, so "retire" means "start the evidence clock", not "delete now". |
| **Declared gap** | The acr-side outbound `svc_acr_*` caller was not traced in Wave 0, so whether `svc_acr_*` migrates in Wave 8 or waits for ADR-08 is genuinely open. |

### ACP-ADR-07 — Entitlement snapshot and event contract

| | |
|---|---|
| **Recommend** | **Accept.** Billing and licensing stay authoritative; a bounded `entitlement_snapshots` projection with source revision and expiry; **entitlement is a decision input, never a token claim**, deprecating `tier` and `licensed_features` from the envelope at its next `v` bump; both gates independent; unavailable entitlement fails closed; `agent_context_runtime` stays independent of ACR scope and repository grant. |
| **Alternatives** | (a) Leave `tier`/`licensed_features` in the envelope — rejected: G-14 names entitlement in a token specifically, and this is the newest security-sensitive code path in the platform, so it sets the pattern the Go work will copy. (b) Make the control plane the entitlement authority — rejected by G-6 and G-9; it creates a second source of truth for billing state. |
| **Risk of accepting** | **Low.** Removing claims from the envelope is a `v` bump, and the verifier already **rejects an unrecognised `v`** by design (`principal/verifier.go:99-105`), so the migration is mechanically safe. The consumer reads only `org_id` today, so nothing breaks. |
| **Compensating control until the bump** | The envelope's 60-second TTL (ADR-03). Recorded so the interim state is a decision rather than an oversight. |

### ACP-ADR-08 — ACR lifecycle projection boundary

| | |
|---|---|
| **Recommend** | **Accept, and adopt the last clause as a platform requirement.** ACR keeps validator authority; the control plane projects lifecycle metadata only; the web assertion is preserved exactly and a generic workload JWT is explicitly **not** a drop-in replacement; `acr-mcp` never sees platform credentials; repository-grant enforcement stays per-operation; **ACR's single-dispatch pattern becomes the platform requirement for the CHAOS-3271-class fix in ops.** |
| **Alternatives** | (a) Migrate ACR's validators into the control plane — rejected: ACR is the only component in the platform with **zero** foreign reachable validators (0 of 16 rows, against 59 in ops), on the hardest protocol. Moving it is spending the program's risk budget on the part that works. (b) Replace the web assertion with a workload JWT for uniformity — rejected by G-41, and the ADR says so explicitly so a later wave cannot "simplify" it away. |
| **Risk of accepting** | **Low.** The clause with real cost is the last one: porting single dispatch into ops touches 59 surfaces' worth of middleware behaviour. That cost is the point — it is the CHAOS-3271 fix, and it is a port of working code rather than novel design. |
| **Declared gap** | ACR's own issued credentials (device flow, `oauth/token`) were **not** backfilled into `issued_credential` in Wave 0. They are obvious candidates, and "obvious" is a family pattern, which this inventory does not record as fact. |

### ACP-ADR-09 — External Push lifecycle projection boundary

| | |
|---|---|
| **Recommend** | **Accept.** `fcpush_*` keeps its domain validator and source/org/scope binding; **legacy ingest becomes fail-closed unconditionally with the permissive branch removed, not environment-gated**; removal of the six legacy routes stays an external-contract decision; the `IngestAuthContext` name collision is resolved by renaming in its own change; signature, scope, gating, validation, and idempotency stay separate checks. |
| **Alternatives** | (a) Gate the permissive branch behind an environment flag — rejected: G-47 requires permissive mode to be *impossible* to enable in staging or production, and a flag that can be set is not impossible. This is the same objection §8.3 of the threat model raises against `GRAPHQL_AUTH_REQUIRED`. (b) Remove the six routes now — evidence-favoured (zero external producers in a 30-day window across 10.5M prod spans) but **not an engineering decision**; see Part C. |
| **Risk of accepting** | **Low.** Fail-closed on a route family with no observed producer has near-zero blast radius. The residual risk is entirely in the *removal* question, which this ADR deliberately does not decide. |
| **Declared gap** | Whether the legacy HMAC path bounds replay with a timestamp or nonce was not traced. |

### ACP-ADR-10 — MFA and step-up

| | |
|---|---|
| **Recommend** | **Accept.** Wave 2 ships the assurance **vocabulary and enforcement path**, not a factor; **until a step-up factor exists, G-51's high-risk list is DENIED during impersonation, not warned on**; factor selection (TOTP / WebAuthn) deferred to a named addendum; enterprise SSO does not substitute for MFA. |
| **Alternatives** | (a) Ship TOTP in Wave 2 — rejected: it front-loads a product decision (self-hosted deployments may have no second channel) into an architecture wave. (b) Leave G-51's list warned-on until step-up exists — rejected: "denied or step-up gated" with no step-up available has exactly one compliant reading. |
| **Risk of accepting** | **Medium, and it is a product risk rather than a security one.** Denying role elevation, credential operations, billing changes, and service-account management **during impersonation** will break support workflows that rely on them today. That breakage is the guardrail working as designed, but it is a user-visible change that support should hear about before Wave 2, not during it. |
| **Relevance you may not expect** | The 150 Server Actions are concentrated in `src/lib/admin/server/` and `src/lib/billing/actions/` — G-51's list almost exactly — and in Next.js 16.2.12 none is protected by a layout guard or path middleware. So this ADR's enforcement point and the Server Action guard sweep are the **same** surface approached from two directions. |

### ACP-ADR-11 — SCIM and enterprise deprovisioning

| | |
|---|---|
| **Recommend** | **Accept.** SCIM deferred beyond Wave 4 and not a prerequisite for any authority transfer; **Wave 4 ships the deprovisioning semantics SCIM would need** — revisioned status, transactional outbox, idempotent revision-keyed consumers, deny-then-clean ordering, monotonic revocation; revocation propagation is an SLO with a metric; provisioning events carry stable ids, not profiles. |
| **Alternatives** | (a) Ship SCIM in Wave 4 — rejected: SCIM is a provisioning transport, and shipping it before the aggregates it drives creates a second provisioning authority (G-9). (b) Defer the semantics too — rejected: they are required by G-24/G-53/G-54/G-55/G-58 regardless of whether SCIM ever ships, so deferring them defers guardrail compliance, not a feature. |
| **Risk of accepting** | **Low technically, potentially high commercially.** Enterprise customers with SSO commonly expect SCIM, and "deferred beyond Wave 4" may collide with a sales commitment this sheet cannot see. |

### ACP-ADR-12 — A single authentication seam for `query-api`

| | |
|---|---|
| **Status** | **Not one of TRD §26's eleven.** Raised by the threat model §2.9. |
| **Recommend** | **Accept.** Authenticate at one seam (directive, operation middleware, or generated wrapper — implementation choice deferred to Wave 6); public operations are an explicit allowlist on that seam, never the absence of a check; **new operations default to org-scoped-from-claims with no `orgID` argument**; a gate seeds a resolver with no hand-rolled check and proves it is still rejected; per-resolver checks are removed only after the seam is proven, in a separate change. |
| **Alternatives** | (a) Keep per-resolver checks and rely on code review — rejected: this is precisely the arrangement that produced CHAOS-3271, and 40 of 50 resolvers are still unwritten, so the exposure is almost entirely in the future. (b) Do nothing until a resolver is found unauthenticated — rejected: an unauthenticated resolver is not detectable by any test, gate, type error, or compiler diagnostic that exists, so "until found" may mean "in production". |
| **Risk of accepting** | **Low.** The overlap approach (seam first, per-resolver removal second) means there is no window where either control is absent. The 10 existing correct resolvers are untouched until the seam is proven. |
| **Risk of rejecting** | Each of the remaining 40 resolvers is an independent opportunity to omit a check that nothing verifies. This is the one recommendation in this sheet whose cost of rejection compounds with time. |

---

## Part B — contract changes needing a ruling

These are changes to `contracts/auth/v1/`. **They are recommendations only — this
document edits no contract.** Each is a yes/no, not a design.

| # | Change | Why | Cost |
|---|---|---|---|
| B1 | **Split `reachable_but_not_owner`** into `foreign_class_validator` and `earlier_auth_gate` | One flag currently spans two phenomena: a foreign validator parsing a class it does not own (ops, 59 rows — a real CHAOS-3271 finding) and a legitimate session pre-gate (web, 15 rows — defense in depth). A cross-repo sum over them would be a proxy number. | Re-tag 74 rows; schema change |
| B2 | **Register `effective_principal_envelope`** in the closed credential vocabulary | An EdDSA JWT carrying the effective principal, minted by ops, verified by Go, is **not one of the 30 classes**. The vocabulary is closed, so the gate certifies its absence and reports zero out-of-vocabulary ids while doing so. | One class entry + its four required facts |
| B3 | **Add `dev-health-query-api` to the `service` enum** and profile `/query`, `/healthz`, `/readyz` | A sixth deployed application. The enum is closed (correctly, per G-26), so its three surfaces are currently unprofilable rather than unprofiled. | Enum value + 3 rows (see B9 on granularity) |
| B4 | **Set `exposure.reachability: "unknown"` + a `gaps` entry naming `exposure` on all 395 rows** | Absent reads as "not applicable"; `unknown` reads as "nobody has checked", which is the true state for 395 of 395. The gate already **requires** the gaps entry (`ci/check_endpoint_profiles.py:328-348`), so this is populating a field the gate is waiting for, not a new rule. | Bulk edit; no schema change |
| B5 | **Correct the Server Action count to 150** | Measured: 20 file-level `"use server"` modules, 150 top-level `export async function`, zero other top-level exports, no inline directives. The recorded figure was ~98. | Text correction; changes coverage arithmetic |
| B6 | **Add `guard_site` to Server Action rows**, predicate **"rejects without a session"** | The predicate has to be *does a rejecting check run in this surface's own call chain*, not *is an auth call present*: a `calls_auth` boolean would record a clean result for code that calls the auth helper and then continues without a session. | Schema field + per-row tracing |
| B7 | **Record the assessed Next.js version** as a top-level field | §2.5's conclusions are properties of 16.2.12's dispatch implementation. An upgrade must re-open them mechanically, not by memory. | One top-level field |
| B8 | **Note `GRAPHQL_AUTH_REQUIRED` on every ops GraphQL row** | 58 rows describe a per-operation posture that one environment variable overrides globally. | 58-row note |
| B9 | **Profile `query-api` per resolver, not as one `/query` row** | The auth check is per resolver (§2.9), so one row would assert a uniform posture the code does not have — the same granularity argument that made `billing_edge` a separate row from the main app's identical path. | Depends on B3 |
| B10 | **Disambiguate `null` — "not applicable" vs "we did not find out"** | See B10 below; this one needs a choice between four shapes, not a yes/no. | varies by shape |
| B11 | **Lift the gaps field-naming rule into the schema description** | The rule that a `gaps` entry must name the field it excuses currently exists **only in `ci/check_endpoint_profiles.py`** — not in the schema text, not in the docs. A reader of the contract cannot find the real rule. | One description string |
| B12 | **Validate the whole document against the schema, not just its top level** | Both gates hand-roll their checks and validate only the top level, so a row with `primary_validator: 17` — an integer where the schema permits `object`/`null` — **passes CI**. Reproduced independently and by codex. The gates are honest about the scope (the message says "top level"), but a contract enforced only at its top level is not enforced. **Library constraint, and the obvious pick is wrong:** the schema declares draft 2020-12 and acr's `go.mod` already carries three candidates — use `github.com/google/jsonschema-go`, whose `doc.go` states it supports "draft 2020-12 and draft-07" and that other drafts are "not supported". **Never `xeipuuv/gojsonschema`: it maxes at Draft 7** (`draft.go:30-33`) and would silently accept what it cannot interpret — reproducing the exact defect being fixed. `invopop/jsonschema` is a schema generator, not a validator. | Full validation in both gates + a seeded shape test; no new dependency |

### B10 in full — `null` carries two meanings and only convention separates them

This is a flaw in the contract, surfaced by building the gate against it, and it
is the wave's running theme in one more habitat: **a representation that looks
like it carries information and does not.**

* On `anchor`, `issued_credential`, and `exposure`, `null` means **"undetermined
  in this pass"** and requires a paired `gaps` entry. The gate enforces this,
  and enforces it well — the entry must name the field it excuses, so an
  unrelated gap cannot launder an unrelated null.
* On the advisory fields — `action`, `resource_resolver`,
  `current_state_cache_behavior`, `entitlement_requirement`,
  `disclosure_behavior`, `token_shape` — `null` overwhelmingly means **"not
  applicable"**. A health-check route genuinely has no action.

Re-measured on the ops inventory after CHAOS-4761 (370 rows): **all 370
rows** carry at least one advisory null, and **272** of them carry no `gaps`
entry at all — and those 272 are **correct**. So the ambiguity is not a corner case; it is the normal state of
every row in the file.

The gate scoped its unstated-null check to the first group only. That was the
right call — the alternatives were failing the real tree or inventing 272 gaps
entries that would say nothing — but the consequence is that **neither the
contract nor a human reader of the JSON can tell "nothing to say here" from "we
did not find out"**, and the distinction is exactly the one this whole inventory
exists to preserve.

Four shapes, with the trade-off each makes:

| Shape | What it buys | What it costs |
|---|---|---|
| **(a)** A distinct sentinel for not-applicable (e.g. `"n/a"`) | The absence of a `gaps` entry becomes meaningful **everywhere**; the gate can drop its hardcoded field list | Touches many rows; a magic string in a typed field |
| **(b)** Make the advisory fields **optional**: *absent* = not-applicable, *null* = undetermined | Cheap, no sentinel, and it **matches how `issued_credential`'s absent-vs-null already works** — one idiom instead of two | Absent-vs-null is a subtle distinction that JSON tooling erases easily; needs the docs to say it loudly |
| **(c)** Per-field MUST/advisory metadata in the schema, gate derives strictness | The gate stops hardcoding a list, so adding a field cannot silently escape the null rule | Most schema work; introduces schema-about-schema |
| **(d)** Accept it explicitly, documented, with the gate's scoping as the stated compromise | Zero cost now | The ambiguity stays, and a later reader inherits it without inheriting this paragraph |

**Recommendation: (b).** It is the cheapest of the three real fixes, it removes
a second idiom rather than adding one, and the platform already has a working
precedent for absent-vs-null in the same file. **(d) is a legitimate choice**
and is strictly better than choosing nothing — but it should be chosen, and
written into the schema description, rather than arrived at by the gate's
scoping being the only record.

!!! note "Not fixed in this wave"
    The schema and the gate belong to other lanes. This sheet raises the design
    question and recommends; it edits neither.

---

## Part C — residual risks: acknowledge, do not resolve

These stay open whatever is ratified. Naming them **is** the deliverable; a
residual risk that nobody wrote down is indistinguishable from one nobody found.

### C1 — Edge path-map not under version control / not gate-readable

Public reachability is defined by the edge path-map, not by app-level route
mounting: two routers mounted identically in the same application can have
opposite exposure, so mounting is not evidence either way. The schema now has an
`exposure` object to capture the distinction.

**No repository in this wave's read set contains the edge path-map.** The
in-repo "edge dispatcher" is GraphQL routing to `query-api` — a different thing,
and it must not be cited as evidence of public routing. Until the real path-map's
location is known:

* `exposure.source` stays **required**, and values are explicitly **asserted,
  not verified**;
* a legitimate `source` is an edge/ingress path-map artifact or a dated,
  executed probe with its result — never a repository file that merely mounts
  the route;
* everything else is `unknown` with a `gaps` entry, and **neither direction is
  ever inferred from route mounting** — an inference rule that did would have
  classified the ingest routes as public, which is exactly backwards.

This is the **third** instance this wave of "an artifact asserted to satisfy a
contract, with nothing executing to check it", after the closed 2-value `service`
enum and the two self-describing `schema_deviation_note` keys. Three in one wave
is a pattern. `exposure` is the instance where the unverified assertion **is a
security boundary**, and a security boundary that exists only as prose is
indistinguishable, to every future reader, from one that is enforced.

**Needs from chris:** where the real path-map lives — cloudflared ingress, host
reverse proxy, or Cloudflare rules. If a location comes back, the gate lane gets
an addendum to read it and C1 downgrades from residual risk to work item.

### C2 — `GRAPHQL_AUTH_REQUIRED` is a single-variable global auth kill switch

`api/graphql/app.py:44-50`. Fails closed by default — correct — but it is the
only gate on the chokepoint at `:84-85`, so one variable set to one value
disables authentication for **every GraphQL operation in ops**. No gate, no
alert, no telemetry would notice it being flipped. Production state is **not
known to this document** and is being checked separately; the mechanism is the
finding regardless.

Recommended compensating control is observability, not removal — local GraphiQL
development is a real need. A startup log line, a readiness-check field, and a
metric make "auth is off" a fact the platform reports about itself. Compare
`principal.Verifier`, which **refuses to construct** with an unset issuer rather
than running in a silently weaker mode.

### C3 — The 30-day producer window bounds a claim; it does not close it

Zero external requests across all six legacy ingest routes in a 30-day window
over 10.5M prod spans. That does **not** exclude a quarterly batch, an annual
reconciliation, or a customer integration on their calendar rather than ours.
Write the window with the number, permanently. This is why removal is chris's
call as an external contract, not an engineering inference from telemetry.

### C4 — Accept-and-warn makes arrival telemetry unsafe as an effect measure

Legacy `/api/v1/ingest` returns 202 even when the downstream stream is
unavailable. Any G-63 "measured zero use" gate, any retirement decision, and any
revocation-propagation SLO must measure the **effect**, not the arrival. Moot at
zero traffic today; structural in general.

### C6 — Three signing keys, two of them in one process

`JWT_SECRET_KEY` (HS256, mints and validates) and `GO_API_ENVELOPE_PRIVATE_KEY`
(Ed25519, mints envelopes) are both held by the `api` service. A single
compromise of that process yields the ability to mint user access tokens **and**
principal envelopes carrying arbitrary permissions and a superuser bit. This is
the baseline the program is measured against; it is not fixed by any ADR here,
it is fixed by Waves 3 and 4 completing.

---

## A property worth preserving: the gate makes a softened record unpublishable

This was not designed, and it is the strongest evidence that the Wave 0
machinery does what it claims.

The CI gate and the disclosure policy were built by different lanes for
different reasons. They turn out to be mutually reinforcing. A profile row for a
protected surface whose guard does not reject **cannot be made vague**: omit the
row and the unowned-surface check fires, because every discovered surface must
have one; write the row without disclosing the gap and the unstated-null check
fires, because it requires the `gaps` entry to name the field and say why no
rejecting check exists.

The consequence is that **a disclosure policy of "describe a weakness only once
its fix has landed" cannot be satisfied by softening the record — only by fixing
the defect.** The inventory cannot be quietly made more flattering than the code,
because the gate rejects the flattering version.

Preserve this property in anything that replaces or extends the gate. A check
that can be talked into accepting a vague row would let a laundered inventory
ship *and* be called policy-compliant, which is worse than having no policy: it
converts an unfixed defect into a documented, reviewed, apparently-clean record.

---

## Part D — believed to need tickets

Filed under the **Auth Control Plane** project. This sheet recommends; it files
nothing.

| Item | Kind | Rationale |
|---|---|---|
| Register `effective_principal_envelope` as a credential class | Inventory gap | B2 — a live signed platform credential outside a closed vocabulary |
| Add `dev-health-query-api` to the service enum and profile its surfaces | Inventory gap | B3/B9 — a sixth deployed app with three unprofilable surfaces |
| Correct the Server Action population to 150 and sweep with the rejecting predicate | Coverage | B5/B6 — the sweep is in progress; the count correction is not |
| Split `reachable_but_not_owner` into two fields | Contract defect | B1 — one flag spanning two phenomena, already producing incomparable rows |
| Rename the duplicate `IngestAuthContext` | Latent defect | Same name, opposite trust semantics, one package tree; the type checker is satisfied either way |
| Make `GRAPHQL_AUTH_REQUIRED` state observable | Hardening | C2 — a global kill switch nothing reports on |
| Move `GO_API_ENVELOPE_PRIVATE_KEY` to a `_FILE` source with web's file contract | Hardening | ADR-02 — two custody postures for one requirement |
| Close a latent trust path in the impersonation middleware | Latent defect | Tracked in the Auth Control Plane project; detail is held under the fix-first disclosure policy |
| Establish `query-api`'s single auth seam | Structural | ACP-ADR-12 — 40 unwritten resolvers, each an opportunity to omit a check nothing verifies |
| Decide remove-vs-retain on the six legacy ingest routes | External contract | C3 — chris's call, not an engineering inference |
| Disambiguate `null` in the profile schema (B10) | Contract defect | All 370 ops rows carry an advisory null; 272 carry no gaps entry and are correct — the contract cannot express why |
| Lift the gaps field-naming rule into the schema description (B11) | Documentation | The real rule lives only in the gate; a reader of the contract cannot find it |

---

## Related reading

* Auth Control Plane threat model — maintained in the Auth Control Plane project in Linear, not in this repository.
* [Auth Control Plane ADRs](auth-control-plane-adrs.md)
* [Credential classes](../../reference/auth/credential-classes.md)
* [Endpoint authentication profiles](../../reference/auth/endpoint-profiles.md)
