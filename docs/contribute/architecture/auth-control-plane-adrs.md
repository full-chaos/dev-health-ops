---
page_id: con-auth-control-plane-adrs
summary: The eleven ADRs TRD section 26 requires before Auth Control Plane cutover, plus one proposed by the threat model, drafted at status Proposed — each stating its decision, what it supersedes, what it deliberately leaves open, and the anchored evidence from the Wave 0 inventory that constrains it.
content_type: architecture
owner: engineering
source_of_truth:
  - contracts/auth/v1/credential-classes.json (closed credential vocabulary)
  - contracts/auth/v1/endpoint-profiles.ops.json (frozen ops surface inventory)
  - src/dev_health_ops/api/services/auth.py (current HS256 issuer, TTLs, superuser verification)
  - src/dev_health_ops/api/graphql/principal_envelope.py (current EdDSA envelope issuer)
  - cmd/query-api/internal/principal (current EdDSA verifier, the prior art for ADR-01)
applicability: current
lifecycle: active
---

# Auth Control Plane ADRs (Wave 0 drafts)

TRD §26 requires eleven ADRs before cutover. All eleven are drafted here at
status **Proposed**. None is Accepted; ratification is a single pass over the
[decision sheet](auth-control-plane-decision-sheet.md), which carries the
recommendation, the alternatives considered, and the risk for each.
{: .fc-page-lede }

!!! info "What is and is not settled"
    The ADR *"Implement the Auth Control Plane in Go"* is **Accepted** and sits
    outside this set. It supersedes any Python-first language in the PRD or TRD;
    **every other authority, protocol, migration, and guardrail decision in
    those documents stays in force.** These eleven ADRs decide the things that
    ADR deliberately left open.

    Guardrail references (`G-n`) are to *Engineering Guardrails — Go-native Dev
    Health Auth Control Plane*. Where an ADR cites a `file:line`, that anchor was
    read during Wave 0; where it cites nothing, the ADR says so and names the gap.

!!! success "The inventory contract these ADRs build on is enforced"
    `ci/check_endpoint_profiles.py` (`0a7ff7ccd`) and acr's Go equivalent are
    committed and **verified to reject seeded violations** — dropped rows,
    off-vocabulary class ids, drifted anchors, and an unexplained null — not
    merely to pass on clean input. **G-1 is enforced rather than intended**, and
    every ADR below that says "closed vocabulary, unknown denies" has a working
    precedent rather than an aspiration.

    The gate also requires a `gaps` entry to **name the field it excuses**
    (`_gaps_mentions`, `:459`) — stricter than the contract prose, and
    deliberately so: without it, one honest gaps entry would launder every null
    in the row. Carry that shape into every later contract. An escape hatch must
    be as specific as the thing it excuses, or it becomes an exemption.

## Index

| ID | Title | Status | Blocks |
|---|---|---|---|
| ACP-ADR-01 | Go dependency selection for the auth service | Proposed | Wave 1 |
| ACP-ADR-02 | Signing-key custody and the production KMS adapter | Proposed | Wave 1, Wave 3 |
| ACP-ADR-03 | Access, refresh, and workload token TTLs | Proposed | Wave 3 |
| ACP-ADR-04 | Auth schema ownership and aggregate transition | Proposed | Wave 4 |
| ACP-ADR-05 | Policy representation and revision strategy | Proposed | Wave 5 |
| ACP-ADR-06 | Compose bootstrap and Kubernetes federation | Proposed | Wave 8 |
| ACP-ADR-07 | Entitlement snapshot and event contract | Proposed | Wave 5 |
| ACP-ADR-08 | ACR lifecycle projection boundary | Proposed | Wave 9 |
| ACP-ADR-09 | External Push lifecycle projection boundary | Proposed | Wave 9 |
| ACP-ADR-10 | MFA and step-up requirements | Proposed | Wave 2 |
| ACP-ADR-11 | SCIM and enterprise deprovisioning scope | Proposed | Wave 4 |
| ACP-ADR-12 | A single authentication seam for `query-api` | Proposed | Wave 6 |

ACP-ADR-12 is **not** one of TRD §26's eleven. It is proposed by the Wave 0
threat model (§2.9) because the migration removes a control that exists today
and replaces it with per-site discipline. Ratify or reject it alongside the
others; rejecting it is a legitimate outcome, but doing so silently is not.

---

## ACP-ADR-01 — Go dependency selection for the auth service

**Status:** Proposed · **Decides:** TRD §26.1 · **Guardrails:** G-13, G-73, plus the ADR's "standard library first" rule

### Context

The accepted Go ADR requires standard library first, no hand-rolled
cryptographic primitives, and explicit review and pinning of every security
dependency. It does not name the libraries. Six choices are needed: HTTP router
and middleware, JOSE/JWT, OAuth 2.0/OIDC client, SAML, password hashing, and
PostgreSQL driver plus migration tooling.

The platform is not starting from zero. Two independent JWT verification
implementations already exist and one of them is materially better than the
other:

* `cmd/query-api/internal/principal/verifier.go:62-109` uses
  `github.com/golang-jwt/jwt/v5` and pins issuer, audience, `exp` (via
  `WithExpirationRequired`, so an `exp`-less token is rejected rather than
  treated as non-expiring), the algorithm set (`WithValidMethods(["EdDSA"])`,
  evaluated **before** a key is resolved so an alg-confusion attempt never
  reaches key material), a required `kid`, and a claim-schema version with its
  own rejection telemetry. It additionally **refuses to construct** with an
  empty issuer or audience (`:39-45`) because `jwt.WithIssuer("")` silently
  disables issuer checking — a footgun caught once, at construction, rather
  than relied on at every call site.
* `acr:internal/auth/` uses opaque credential lookup plus Ed25519 assertion
  verification with a replay cache and body-digest binding.

`ops/go.mod` pins `dev-health-go v0.6.1`; `acr/go.mod` pins `v0.5.5`. The
`authverify` package is **byte-identical** across those two tags (verified by
diff), but the pins diverge in general, so any shared verifier must be anchored
at the tag its consumer pins.

### Decision

Select, and record here as pinned versions at Wave 1 start:

1. **JOSE/JWT: `github.com/golang-jwt/jwt/v5`**, with
   `cmd/query-api/internal/principal/verifier.go` promoted to the **canonical
   verification pattern** — every field it pins is mandatory for every platform
   token verifier, and the empty-issuer construction guard is mandatory.
2. **JWKS: `dev-health-go/authverify`**, extended rather than re-implemented.
   Its `Ed25519JWKSVerifier` is Ed25519-only by design, which matches the
   asymmetric algorithm decision below.
3. **Router:** `net/http` with `http.ServeMux` and explicit method-and-path
   registration. No third-party router in Wave 1. Rationale: TRD §6 requires
   strict route/method registration, and `query-api` already runs on plain
   `ServeMux`.
4. **Algorithm: Ed25519/EdDSA for every asymmetric platform token.** Not RS256.
   This is already the reconciled platform position and the JWKS verifier
   enforces it.
5. **PostgreSQL:** `pgx/v5` (already in use, `cmd/query-api` builds a
   `pgxpool.Pool`). Migrations continue through the repository's existing
   alembic-owned schema until ADR-04 transfers ownership.
6. **OAuth/OIDC, SAML, and password hashing: DEFERRED to a named Wave 1
   addendum.** No candidate for these three was evaluated in Wave 0, and this
   ADR does not invent one. The current Python implementations
   (`api/sso.py:796-812` signxml SAML signature; `:1017-1044` PyJWKClient OIDC
   `id_token` with an algorithm allowlist) are both **confirmed fail-closed**
   and remain the behavioural reference.

### Supersedes

Nothing. This is the first dependency decision for `auth-service`.

### Leaves open

The three deferred selections above, and whether the shared verifier lives in
`dev-health-go` (versioned, divergent pins, read-only in this wave) or in an
ops-local package. A defect found in `dev-health-go` is ticketed, never fixed
in-wave.

---

## ACP-ADR-02 — Signing-key custody and the production KMS adapter

**Status:** Proposed · **Decides:** TRD §26.2 · **Guardrails:** G-11, G-12, G-16, G-17, G-18, G-19, G-62

### Context

The platform currently has **three signing keys with three different custody
postures**, documented in the threat model §7.1:

| Key | Algorithm | Custody | Forge capability |
|---|---|---|---|
| `JWT_SECRET_KEY` | **HS256, symmetric** | direct env value | **mints and validates** |
| `GO_API_ENVELOPE_PRIVATE_KEY` | Ed25519 | direct env value, inline PEM | mints envelopes |
| `ACR_WEB_ASSERTION_KEY_FILE` | Ed25519 | file path, `O_NOFOLLOW`, size-bounded, mode-checked | mints assertions |

Two facts constrain this decision more than any preference:

* **`JWT_SECRET_KEY` is G-11's named anti-pattern, live.** One symmetric secret
  both mints (`api/services/auth.py:202`, `:228`, `:258`) and validates
  (`:302`). HS256 has no `kid`, so an overlap window has nowhere to live and
  G-18 rotation is not representable at all.
* **The `api` process holds two minting keys.** Per
  `deploy/go-api/compose-query-api.yml`, the envelope private key is the `api`
  service's. A single compromise of the Python API process yields the ability
  to mint both user access tokens and effective-principal envelopes carrying
  arbitrary permissions and a superuser bit.

### Decision

1. **Custody interface first, adapters behind it.** `internal/keystore/` exposes
   sign and public-JWKS operations only. No caller outside `internal/token/`
   may reach it (TRD §5 package rules).
2. **Production: an external KMS or secret store holds private material**;
   application tables carry public metadata and a reference only.
3. **Self-hosted and Compose: file-based custody, and the file contract is
   `web`'s, promoted to a platform requirement** — `O_NOFOLLOW`, regular file,
   size-bounded, `mode & 0o077 == 0`, and a type check that the parsed key is
   the expected algorithm. `ACR_WEB_ASSERTION_KEY_FILE` already implements
   exactly this and is the reference.
4. **Direct-value secret env vars are prohibited for signing material.** This
   deprecates the inline-PEM form of `GO_API_ENVELOPE_PRIVATE_KEY` in favour of
   a `_FILE` variant. TRD §6 already requires direct values and `_FILE` sources
   to be mutually exclusive; today the platform holds one of each for the same
   job.
5. **Every key gets a `kid` and a JWKS entry from day one**, including any
   compatibility issuer, so that G-18 overlap and G-19 bounded-refresh are
   representable before they are needed.
6. **Rotation runbooks are a Wave 1 deliverable, planned and emergency
   separately**, per G-18 and G-62. A compromised key is never re-enabled by a
   rollback.

### Supersedes

The implicit "an env var is adequate custody" position, which no document ever
stated and every deployment currently relies on.

### Leaves open

Which KMS. No cloud or vendor commitment is made here, only that the adapter
boundary exists and that the file-based path is a first-class supported
deployment, not a development shortcut. **Declared gap:** no rotation runbook
for `JWT_SECRET_KEY` or `GO_API_ENVELOPE_PRIVATE_KEY` was found in Wave 0's
read set.

---

## ACP-ADR-03 — Access, refresh, and workload token TTLs

**Status:** Proposed · **Decides:** TRD §26.3 · **Guardrails:** G-14, G-15, G-30, G-31, G-57, G-60

### Context

Measured current values:

| Credential | TTL | Anchor |
|---|---|---|
| ops access token (HS256) | **60 minutes** | `api/services/auth.py:98` |
| ops refresh token | **7 days** | `api/services/auth.py:99` |
| effective-principal envelope | **60 seconds** (default, per-call overridable) | `api/graphql/principal_envelope.py:96`, `:179` |
| ACR web assertion | **30 seconds**, request-bound | `web:src/lib/acr/assertion.ts:30-53` |

TRD §11 targets a 10-minute access token after migration while preserving
60 minutes during compatibility, a 5–10 minute workload token, and a seven-day
rolling refresh "unless a TTL ADR narrows it". This is that ADR.

The envelope's 60 seconds is the load-bearing number in the current system: it
is the only bound on a token that carries a **complete resolved permission
set**, an entitlement list, and a superuser bit (threat model §7.3).

### Decision

| Class | TTL | Notes |
|---|---|---|
| User access token (target) | **10 minutes** | TRD §11's target, adopted |
| User access token (compatibility) | 60 minutes | HS256 only, on explicitly compatibility-enabled profiles, retired on telemetry |
| Refresh credential | **7 days rolling**, single-use, family-linked | unchanged; reuse detection revokes the family |
| Workload token | **5 minutes** | the narrow end of TRD's range; workloads re-exchange cheaply |
| Delegated/impersonation session | **15 minutes**, independently revocable | G-52 requires shorter than the base session |
| Effective-principal envelope | **60 seconds, and the per-call override is removed** | a security bound that any call site may widen is not a bound |
| ACR web assertion | **30 seconds**, unchanged | request-bound and replay-cached; do not weaken |
| Decision cache (low-risk reads) | **≤30 seconds**, never extended on dependency outage | TRD §14 and G-60 |

**A TTL is a revocation window, and it is only a bound when it is the *shortest*
control.** Where a shorter control exists — the ACR assertion's body-digest
binding and replay cache — it is the real bound and the TTL is defense in depth.
Where no other control exists — the envelope — the TTL is the only thing between
a stale permission set and enforcement, and it must not be overridable.

### Supersedes

TRD §11's "compatible initial seven-day rolling behavior unless TTL ADR narrows
it" — this ADR declines to narrow it, and says so explicitly so the question is
closed rather than open.

### Leaves open

Whether the 10-minute access token is reachable without a refresh-storm problem
at current session volumes. **Declared gap:** no measurement of refresh rate at
60 minutes was taken in Wave 0, so the 10-minute figure is TRD's target adopted
on principle, not on load evidence. G-74 load tests precede cutover and are the
place this gets falsified.

---

## ACP-ADR-04 — Auth schema ownership and aggregate transition

**Status:** Proposed · **Decides:** TRD §26.4 · **Guardrails:** G-9, G-24, G-53, G-55, G-58, G-63, G-64

### Context

TRD §9 specifies an auth-owned PostgreSQL schema and role. Today, auth state
lives in the Ops application schema, and the ops repository has an alembic
migration lineage with head pins asserted as literals in many test files — so a
migration that shifts the head is a cross-cutting change, not a local one.

G-9 permits exactly one mutation authority per object at any point in the
migration. G-64 forbids an undocumented direct-table fallback after transfer.

### Decision

1. **A separate schema with a separate database role**, not a separate database
   cluster, for Wave 1. This keeps transactional outbox writes (G-53) in one
   transaction with the state change, which a cross-cluster split would break.
2. **`auth-migrate` is a separate binary and the runtime never auto-migrates**
   (TRD §20). The runtime role owns no DDL.
3. **Aggregate-by-aggregate transfer with an explicit routing switch per
   aggregate**, in this order: sessions and refresh families → principals and
   users → organizations and memberships → platform roles → service accounts →
   workloads → grants. Sessions first because they are the shortest-lived state
   and therefore the cheapest rollback.
4. **Each transfer removes the Python write path in the same change** that
   makes Go authoritative. A compatibility *read* is permitted and named; a
   compatibility *write* is not (G-9).
5. **Destructive cleanup only under G-63**: compatibility readers deployed
   first, telemetry at zero for two stable release cycles, reconciliation proof,
   tested rollback, explicit approval.

### Supersedes

Nothing. Extends TRD §9 with an order and a rollback unit.

### Leaves open

Whether the auth schema lives in the same PostgreSQL instance as the Ops
application schema in production. Also open: whether the ops alembic lineage
gains the auth schema or a second lineage is created. **Both are Wave 1
decisions and both are noted as unresolved rather than assumed.**

---

## ACP-ADR-05 — Policy representation and revision strategy

**Status:** Proposed · **Decides:** TRD §26.5 · **Guardrails:** G-26, G-27, G-24, G-29, G-31

### Context

The current authorization model is a static role-to-permission map in Python
plus ad hoc checks. TRD §14 specifies a constrained model — platform RBAC,
organization RBAC, explicit resource grants, workload audience/action limits,
status checks, delegation restrictions, entitlement predicates, and risk/cache
classes — and explicitly excludes tenant-authored policy code from the first
engine.

The Wave 0 inventory found that per-route action, resource, and entitlement were
**not derivable** for all 58 GraphQL resolvers (context-level auth is anchored;
per-field scoping was not re-derived) and that roughly 17 of billing's ~20
routes were classified by family pattern with a gaps note. So the action
vocabulary cannot be generated from the inventory alone.

### Decision

1. **Actions are a registered, versioned, closed vocabulary.** Unknown action,
   role, grant type, condition, or entitlement key **denies** (G-26). Same
   closed-vocabulary discipline as the credential classes, and for the same
   reason: an unknown value must fail, not be tolerated. The mechanism is not
   hypothetical — `ci/check_endpoint_profiles.py` already enforces exactly this
   for the credential vocabulary and is verified to reject an off-vocabulary
   id, so the action vocabulary is a second instance of a working gate rather
   than a new one.
2. **Policy is data, not code, in the first engine.** Roles map to action sets;
   grants bind principal + organization + resource + action set + effect +
   conditions. No expression language.
3. **`policy_revision` is monotonic and appears in every decision**, and every
   allow-cache key binds it along with membership, grant, and entitlement
   revisions (G-31). A revision bump invalidates by construction rather than by
   an explicit purge.
4. **Action names are explicit at call sites** — `members.invite`,
   `acr.context.read`, `billing.subscription.manage`. New code may not use an
   `is_admin`-style check as the final decision (G-27).
5. **The action vocabulary is derived from the endpoint-profile inventory where
   the inventory is anchored, and from source review where it is not.** The
   GraphQL per-field gap and the billing family-pattern gap are named work
   items, not silent omissions.

### Supersedes

The static Python role-permission map, once Wave 5 shadow-mode mismatch reaches
zero. Not before.

### Leaves open

Whether an external policy engine is adopted later. TRD §25 defers this behind
the decision contracts; this ADR keeps that door open by making the decision
contract — not the engine — the stable interface.

---

## ACP-ADR-06 — Compose bootstrap and Kubernetes federation

**Status:** Proposed · **Decides:** TRD §26.6 · **Guardrails:** G-33, G-34, G-36, G-37, G-59

### Context

Wave 0 established the workload-identity ground truth:

* `acr_workload_identity_exchange` is validated by
  `authverify.KubernetesTokenReviewValidator` via a **live Kubernetes
  TokenReview call — never a local JWT decode**. Audience and trust domain are
  deployment environment variables; namespace and service-account identity come
  from TokenReview's own response. This is the strongest workload identity in
  the platform and it already exists.
* `svc_worker_*` is **mint-only scaffolding**: a real DB-backed validator exists
  at `internal/joboperator/auth.go:54` with **zero HTTP wiring** anywhere, and
  nothing at either pinned `dev-health-go` tag references it. It is mintable,
  rotatable and revocable by CLI, and reachable by nothing.

### Decision

1. **Kubernetes: TokenReview-based federation, as already implemented.** Exact
   issuer, audience, namespace, and service account are validated; the
   deployment binding is part of the workload registration. Do not replace a
   live TokenReview with a local JWT decode for latency.
2. **Compose and self-hosted: named bootstrap credentials in restricted files**,
   hashed server-side, accepted **only** at the exchange endpoint (G-36). A
   bootstrap secret never authenticates a resource route in target state.
3. **One workload principal per service or materially distinct worker class**
   (G-33), each with exact audiences and an allowlisted action set (G-34). No
   universal internal credential — the platform's most repeated anti-pattern.
4. **`svc_worker_*` is retired, not migrated.** It has no consumer. The CI gate
   must not encode a validator that nothing reaches, and Wave 8 should remove
   the mint path rather than build an exchange for a class with no caller.
   Retirement still follows G-63's evidence rule.
5. **Infrastructure identity stays independent** (G-37). A workload token does
   not replace least-privilege database roles, TLS, or network policy — and per
   the threat model §1.1, the private-network boundary is a real control that
   the platform is currently relying on without being able to verify it.

### Supersedes

Any plan to give `svc_worker_*` a resource-route validator.

### Leaves open

Whether `svc_acr_*` migrates to workload exchange in Wave 8 or waits for
ADR-08's projection boundary. **Declared gap:** the acr-side outbound
`svc_acr_*` caller was not traced in Wave 0.

---

## ACP-ADR-07 — Entitlement snapshot and event contract

**Status:** Proposed · **Decides:** TRD §26.7 · **Guardrails:** G-6, G-9, G-14, G-45, G-59

### Context

`licensing/gating.py` is **confirmed fail-closed**: unconfigured licensing
resolves to the COMMUNITY tier rather than to unrestricted
(`gating.py:230-241`, `:302-304`, `registry.py:42-53`). Billing and licensing
remain the entitlement authority; the control plane consumes a projection.

The complication is already in the tree: the effective-principal envelope
carries `tier` and `licensed_features[]` **as signed token claims**
(`cmd/query-api/internal/principal/claims.go:38-39`). That is entitlement
travelling inside a credential, which G-14 forbids by name.

### Decision

1. **Billing and licensing remain authoritative.** The control plane holds a
   bounded `entitlement_snapshots` projection with a source revision and an
   expiry, never a second source of truth (G-9).
2. **Entitlement is an input to a decision, never a claim in a token.** The
   authorization decision may carry an entitlement-derived obligation; the
   access token may not carry the entitlement. This deprecates `tier` and
   `licensed_features` from the envelope's claim schema at its next `v` bump.
3. **Entitlement and authorization are independent gates, both of which must
   pass** (G-6). A paid entitlement grants no action; a role grants no product.
4. **Unavailable or expired entitlement state fails closed for gated actions**
   (G-59), and the COMMUNITY-tier fallback is the model for what "closed" means
   here: a defined minimum, not a denial of service.
5. **`agent_context_runtime` stays a product entitlement, independent of ACR
   credential scope and repository grant** (G-45). All three gates remain
   separate.

### Supersedes

The envelope's carriage of `tier` and `licensed_features`, at its next schema
version. Until then the 60-second TTL (ADR-03) is the compensating control and
is recorded as such.

### Leaves open

The snapshot's refresh mechanism — event-driven from billing versus pull with a
bounded TTL. Both satisfy the guardrails; the choice is an availability
trade-off that Wave 5 should measure rather than assume.

---

## ACP-ADR-08 — ACR lifecycle projection boundary

**Status:** Proposed · **Decides:** TRD §26.8 · **Guardrails:** G-40, G-41, G-42, G-43, G-44, G-9

### Context

ACR is the part of the platform that already works, and the threat model says so
with numbers: **0 of 16 acr rows** carry a foreign reachable validator, against
59 in ops. `Authenticator.MiddlewareFor` is genuine single dispatch
(`acr:internal/auth/middleware.go:100-109`), the web-assertion path rejects any
request carrying both `X-ACR-Web-Assertion` and `Authorization`
(`web_assertion_middleware.go:11-15`), and a credential-store outage returns 503
rather than passing through (`middleware.go:117-119`).

The risk in this ADR is therefore not "how do we fix ACR". It is "how do we
avoid breaking ACR while giving the control plane visibility into it".

### Decision

1. **ACR remains the authoritative validator for `fcacr_*`, device state, and
   the request-bound web assertion.** The control plane projects lifecycle
   *metadata* — owner, class, audiences, scopes, status, rotation lineage — and
   never becomes a second validator or a second writer (G-9).
2. **The web assertion is preserved exactly**: Ed25519, exact issuer/audience/
   key, short-lived, bound to method, escaped path and body digest, populated
   from server-derived subject/org/repository-scopes/permission, and restricted
   from credential administration (G-41). **A generic workload JWT is not a
   drop-in replacement**, and this ADR states that so a later wave cannot
   "simplify" it away.
3. **`acr-mcp` never receives platform session, workload, or administration
   credentials** (G-43). It remains a local STDIO client of hosted ACR.
4. **Repository-grant enforcement stays per-operation in ACR** (G-42). Evidence
   and packet identifiers never bypass authorization, and unknown, malformed,
   foreign, deleted, and unauthorized handles remain non-disclosing.
5. **ACR's single-dispatch pattern is promoted to the platform requirement** for
   the CHAOS-3271-class fix in ops. Ops has 59 surfaces where a foreign
   validator observes the credential first; ACR has zero, on a harder protocol.
   The fix is not novel design work — it is porting a working implementation.

### Supersedes

Any reading of "integrate ACR into the control plane" that implies moving ACR's
validators. It moves metadata, not authority.

### Leaves open

**Declared gap:** ACR's issued credentials were not backfilled into the
inventory's `issued_credential` field in Wave 0 — device flow and `oauth/token`
are obvious candidates, and "obvious" is a family pattern, which this inventory
does not record as fact. Also open: acr's storage-table lifecycle model.

---

## ACP-ADR-09 — External Push lifecycle projection boundary

**Status:** Proposed · **Decides:** TRD §26.9 · **Guardrails:** G-46, G-47, G-48, G-9

### Context

Two ingestion subsystems exist and they must not be conflated — an error this
wave came close to making, and refused:

* **External Push** (`fcpush_*`) has its own validator, scope checks, and source
  binding (`api/external_ingest/auth.py`, `require_ingest_scope`,
  `require_matching_source`), and deliberately diverges to 503 where the legacy
  router returns 202. Nothing observed shows it skipping a check.
* **Legacy ingest** treats unset configuration as auth-disabled
  (`api/ingest/auth.py:98-105`) — G-47's named violation, tracked as CHAOS-4720,
  private-network-only per the threat model §1.1, with **zero external producers
  observed in a 30-day window** across 10.5M prod spans.

They also share a symbol name with opposite trust semantics: `IngestAuthContext`
is a permissive `Annotated[dict, Depends(...)]` at `api/ingest/auth.py:151` and
a fail-closed dataclass with `org_id` and `scopes` at
`api/external_ingest/auth.py:65`.

### Decision

1. **`fcpush_*` keeps its domain validator and its source/organization/scope
   binding** (G-46). The control plane projects lifecycle metadata only; a
   user or workload token never substitutes for an ingestion-source credential
   on the data plane.
2. **Legacy ingest becomes fail-closed unconditionally**, with the permissive
   branch removed rather than environment-gated. G-47 requires that permissive
   mode be *impossible* to enable accidentally in staging or production; a flag
   that can be unset is not impossible.
3. **Removal of the six legacy routes is evidence-favoured but is an external
   contract decision**, not an engineering one. Thirty days of zero traffic does
   not exclude a quarterly or annual producer. The window is recorded with the
   number, permanently.
4. **The `IngestAuthContext` name collision is resolved by renaming**, in its
   own change, before either subsystem is migrated. An import fixed in the wrong
   direction silently swaps fail-closed for permissive and the type checker is
   satisfied either way.
5. **Signature, scope, gating, payload validation, and idempotency stay separate
   checks** (G-48). Failure of one is never success of another.

### Supersedes

Nothing. Records the ingest boundary the inventory established.

### Leaves open

The remove-versus-retain decision on the six legacy routes. **Declared gap:**
whether the legacy HMAC path bounds replay with a timestamp or nonce was not
traced in Wave 0.

---

## ACP-ADR-10 — MFA and step-up requirements

**Status:** Proposed · **Decides:** TRD §26.10 · **Guardrails:** G-29, G-30, G-51, G-52

### Context

No MFA exists today. The principal contract already reserves the vocabulary —
`authentication.methods`, `assurance` (`aal1` in TRD §8's example) — and
decisions may carry a step-up obligation, but nothing issues or enforces one.

G-51 requires that a specific list of high-risk actions be **denied or step-up
gated during impersonation**: role elevation, credential issuance/rotation/
revocation, signing-key operations, billing changes, organization deletion,
service-account management, security-policy changes, and nested impersonation.
Without step-up, the only compliant option for that list is **denied**.

The relevant surface is larger than it looks. The 150 Server Actions
(threat model §11.3) are concentrated in `src/lib/admin/server/` and
`src/lib/billing/actions/` — administrative and billing mutations, which is
G-51's list almost exactly — and in Next.js 16.2.12 none of them is protected by
a layout guard or by path middleware (§2.5).

### Decision

1. **Wave 2 ships the assurance *vocabulary and enforcement path*, not an MFA
   factor.** `assurance` is carried in the principal, decisions may return a
   step-up obligation, and PEPs must enforce an unmet obligation as a denial
   (G-29). This makes the contract complete before a factor exists.
2. **Until a step-up factor ships, G-51's list is DENIED during impersonation**,
   not warned on. This is the only compliant reading of G-51 in the absence of
   step-up, and it is written down so the absence is a decision rather than an
   oversight.
3. **MFA factor selection (TOTP, WebAuthn, or both) is deferred to a named
   post-Wave-2 addendum**, gated on a product decision about self-hosted
   deployments that may have no second channel.
4. **Enterprise SSO does not substitute for MFA.** An assertion from an identity
   provider carries whatever assurance that provider asserts; the platform
   records it and does not upgrade it.

### Supersedes

The implicit position that MFA is out of scope because it does not exist. It is
in scope as a contract in Wave 2 and as a factor later.

### Leaves open

The factor. Also open: whether step-up is required for high-risk actions outside
impersonation — G-30 requires *current state* there, which is a weaker and
separately satisfiable requirement.

---

## ACP-ADR-11 — SCIM and enterprise deprovisioning scope

**Status:** Proposed · **Decides:** TRD §26.11 · **Guardrails:** G-24, G-53, G-54, G-55, G-58, G-68

### Context

SSO exists — SAML and OIDC, both confirmed fail-closed (`api/sso.py:796-812`,
`:1017-1044`). SCIM does not. Enterprise customers who have SSO generally expect
deprovisioning to follow, and today deactivation is a manual administrative
action.

G-55 sets the ordering that any deprovisioning implementation must respect:
disabling first **denies new authorization and revokes active access**; physical
deletion and PII cleanup happen later under retention policy.

### Decision

1. **SCIM 2.0 is deferred beyond Wave 4** and is not a prerequisite for any
   authority transfer. It is a provisioning *transport*; the aggregates it would
   drive are the ones Wave 4 transfers, so it must follow rather than lead.
2. **Wave 4 ships the deprovisioning *semantics* SCIM would need**: revisioned
   membership and principal status (G-24), transactional outbox events (G-53),
   idempotent consumers keyed by aggregate and revision (G-54), deny-then-clean
   ordering (G-55), and monotonic revocation that no retry, rollback, stale
   event, or same-identifier recreation can reverse (G-58).
3. **Deprovisioning is bounded and measurable.** "Revocation propagates within
   N seconds" is an SLO with a metric behind it (TRD §19 lists revocation
   propagation lag), not a statement in a document.
4. **Provisioning events carry stable identifiers and necessary state, not full
   profiles or email addresses** unless a consumer explicitly requires and is
   authorized for them (G-68).
5. **JIT provisioning from SSO follows configured policy and emits audit and
   provisioning revisions**, so that a later SCIM implementation reconciles
   against the same event stream rather than a parallel one.

### Supersedes

Nothing.

### Leaves open

Whether SCIM ships at all, and for which identity providers. This ADR's position
is that the decision costs nothing to defer **provided** Wave 4 ships the
semantics above — and that shipping SCIM before them would create a second
provisioning authority, violating G-9.

---

## ACP-ADR-12 — A single authentication seam for `query-api`

**Status:** Proposed · **Decides:** nothing in TRD §26 — **raised by the Wave 0 threat model §2.9** · **Guardrails:** G-1, G-3, G-4, G-8, G-69

### Context

Python's ops GraphQL surface authenticates at **one chokepoint**:
`api/graphql/app.py:84-85`, inside `get_context`, firing before
`OrgIdAuthExtension` and before `org_id` is resolved. A resolver cannot be
reached unauthenticated, and a resolver author cannot forget the check because
there is no per-resolver check to forget.

That chokepoint is governed by `GRAPHQL_AUTH_REQUIRED`, which is **enforced by
default; verified unset on prod 2026-09-01** (CHAOS-4743 tracks making the
setting observable — a startup log line and a gauge — so its state is visible
rather than inferred). The property this ADR relies on is therefore the
deployed one, not merely the coded one.

`cmd/query-api/internal/graph/schema.resolvers.go` does not have this property.
It declares **50** query resolvers, **10** implemented and the rest stubs, and
each of the 10 re-implements `authctx.FromContext(ctx)` + `claims.OrgID == ""`
+ (sometimes) `claims.OrgID != orgID` **by hand** (`:123`, `:186`, `:222`,
`:253`, `:292`, `:408`, `:454`, `:480`, `:524`, `:568`).

All ten are correct today. The risk is the arrangement, not the code: as the
remaining 40 resolvers land, an omitted check produces a resolver that is simply
unauthenticated, and **no test, gate, type error, or compiler diagnostic
notices**. This is the CHAOS-3271 shape — a missing chokepoint compensated for
by per-site discipline — reappearing inside the migration that exists to
eliminate it.

A second observation constrains the design. The ten implemented resolvers use
two idioms: six take an `orgID` argument and compare it to `claims.OrgID`; four
(`ComplexityTimeseries`, `Hotspots`, `CognitiveLoad`, `ReviewEdges`) take **no
`orgID` argument at all** and pass `claims.OrgID` straight through
(`schema.resolvers.go:453-470`). The second idiom makes a caller-supplied org
**unrepresentable** rather than merely rejected — G-4 enforced by the type
signature. Which idiom is available is decided by the **SDL**, not the resolver
author.

### Decision

1. **`query-api` enforces authentication at a single seam**, not per resolver.
   The seam runs after `principal.Verifier.Verify` and before any resolver, and
   a resolver reached without a verified principal is impossible rather than
   merely unlikely. Three implementations satisfy this and the choice is a Wave
   6 implementation detail: a gqlgen **directive** applied schema-wide, an
   **operation middleware** on the executable schema, or a **generated wrapper**
   the resolver stubs are emitted into.
2. **Public operations are an explicit allowlist on that seam**, declared in the
   SDL or the route profile — never the absence of a check. G-1 requires a route
   to declare its profile; the same rule applies to a resolver.
3. **New operations default to org-scoped-from-claims (idiom B).** An `orgID`
   argument is added to the SDL only where a superuser cross-org query is an
   intended, profiled capability, and such an operation carries a current-state
   check rather than a claim comparison (threat model §3.2, G-30).
4. **A gate proves the seam fires.** Following the Wave 0 precedent, the test
   seeds a resolver with no hand-rolled check and asserts it is **still**
   rejected. A seam whose failure path never executes is indistinguishable from
   one that cannot fire (G-69), which is exactly how the per-resolver
   arrangement would pass review today.
5. **Existing per-resolver checks are removed only after the seam is proven**,
   in a separate change, so the two controls overlap rather than hand over.

### Supersedes

Nothing formally. In practice it supersedes the unstated assumption that
per-resolver checks are an adequate substitute for the Python chokepoint they
replace.

### Leaves open

Which of the three mechanisms. Also open: whether the same seam should carry the
action name for ADR-05's decision call, which would make G-1's profile
requirement and G-27's explicit-action requirement satisfiable in one place
rather than two. That is attractive and unproven; it is recorded as a
possibility, not a decision.

---

## Related reading

* Auth Control Plane threat model — the seven threat classes and the anchored
  evidence these ADRs answer. Maintained in the Auth Control Plane project in
  Linear, not in this repository.
* [Auth Control Plane decision sheet](auth-control-plane-decision-sheet.md) —
  recommendation, alternatives considered, and risk per ADR, for one-pass
  ratification.
* [Credential classes](../../reference/auth/credential-classes.md)
* [Endpoint authentication profiles](../../reference/auth/endpoint-profiles.md)
