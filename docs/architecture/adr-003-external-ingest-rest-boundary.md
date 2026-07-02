# ADR-003: External-ingest REST boundary, token scopes, and ownership model

## Status

Accepted. This is the CHAOS-2690 epic's backend ADR (flagged as unowned by
the epic's cross-ticket recon). It covers decisions that span multiple
sub-issues; the REST contract layer (this document's "REST, not GraphQL"
section) ships in CHAOS-2691, while the token/ownership bodies described
below ship in CHAOS-2696/2712/2695 — this ADR records the agreed model so
those tickets implement against a fixed shape rather than re-deriving it.

## Context

CHAOS-2690 adds a customer-facing ingestion path: external customers push
git/work-item data into FullChaos via HTTP instead of FullChaos's connectors
pulling it. This needs (1) a stable public contract surface, (2) a
credential/scope model distinct from session auth, and (3) a policy for what
happens when a customer's pushed data would collide with FullChaos's own
managed sync for the same repo/project/team.

## Decision 1: REST, not GraphQL, for the data plane

The ingestion endpoints (`/api/v1/external-ingest/*`) and their admin
counterparts (`/api/v1/admin/customer-push/*`) are plain REST, snake_case on
the admin side and camelCase on the data-plane wire contract — **no GraphQL
mutations are added for this epic** (master-spec CC25).

Rationale:

- External customers integrate via CI scripts, cron jobs, and simple HTTP
  clients (curl, a thin CLI) — a REST/JSON contract with a documented error
  envelope is the lowest-friction integration surface for that audience,
  matching how GitHub/Stripe/most ingestion-style APIs are consumed.
- The existing GraphQL schema (`api/graphql/schema.py`) is primarily a
  query surface for the web app's own read paths; the earlier "verified
  query-only schema" claim in this ADR's early drafts was **incorrect** —
  `api/graphql/schema.py:814` defines a `Mutation` root with five
  saved-report mutations. The REST-only decision here is grounded in the
  epic's own scope (a stable external contract, not another GraphQL
  surface), not in an absent mutation root.
- Precedent: the legacy `/api/v1/ingest` router and `api/product_telemetry/`
  are both REST. External-ingest follows the same shape rather than
  introducing a third pattern.

The data plane and the admin plane deliberately use **two different
response conventions** — the data plane's `{"error": {code, message,
errors?}}` envelope (`api/external_ingest/errors.py`, CHAOS-2691 D3) versus
the admin plane's house `HTTPException`/snake_case convention. This is not
an oversight: the data plane is a documented public contract consumed by
customer code; the admin plane is consumed by FullChaos's own web app and
can evolve with the rest of the internal API.

## Decision 2: Token scopes

Three scopes: `schema:read`, `ingest:write`, `ingest:status`. `ingest:write`
requires a source-bound token (a token minted against one registered
`external_ingest_sources` row); `schema:read`/`ingest:status` do not.
Provider-specific scopes are reserved but not implemented in v1 (no-op).

`IngestAuthContext(org_id, scopes, token_id, source)` is the single shape
threaded through every endpoint via `require_ingest_scope(scope)`
(`api/external_ingest/auth.py`). CHAOS-2691 ships this dependency's
signature and an **interim** body (see Decision 4); CHAOS-2696 mints real
tokens (table `external_ingest_tokens`, format `fcpush_` +
`secrets.token_urlsafe(32)`, sha256-hashed at rest); CHAOS-2712 swaps the
dependency body to real resolution without changing the shape or any
`Depends(...)` call site.

## Decision 3: One-active-owner policy

A customer should not be able to push data for a repo/project/team that
FullChaos's own managed sync already owns — that would produce silently
diverging or duplicated rows. The policy (master-spec CC5/CC14):

- **Per-provider matching**, not bare `external_id` equality — verified in
  code that a bare-equality check fails open for 3 of 4 providers (GitHub's
  `integration_sources.external_id` is `owner/repo`; GitLab's is the
  *numeric* project ID with the human-readable path living in
  `full_name`/metadata; Jira matches on `project_id`/`project_key`/config
  name; Linear matches on a team UUID or the literal `"linear"` org-wide
  placeholder). Each provider gets its own matching rule (see
  `sync/discovery.py:107,123,133` and `api/admin/routers/sync.py:535,775-820`
  for the verified shapes).
- **Registration-time resolution is stored**: matching runs once when a
  customer registers a source; the matched row's id persists as
  `external_ingest_sources.matched_integration_source_id`. If the match hits
  an **enabled** managed source, registration is rejected
  (`409 source_owned_by_fullchaos_sync`); if the match is disabled,
  registration succeeds and the match is recorded.
- **Accept-time re-check is authoritative**: every `POST /batches` re-checks
  the stored match plus the two indexed exact matches, so a managed source
  created or re-enabled *after* registration is still caught
  (`403 source_owned_by_fullchaos_sync` via `resolve_effective_mode()`,
  owned by CHAOS-2695's `ownership.py`).
- Instance-level ownership is a **hard XOR** (a given repo/project/team is
  owned by exactly one of {managed sync, customer push} at a time);
  provider-level (e.g. "this org has GitHub sync enabled at all") is a
  **soft warning** surfaced in the registration UX, not an enforcement gate.

### Residual risk: Linear team-scoped ownership cannot be exactly resolved

A managed Linear `IntegrationSource` stores a **team UUID**, not the
human-readable team key (`CHAOS`) that `source.instance` uses for
external-ingest. Equating the two requires a Linear API call this ingestion
path does not make. The per-provider matching rule therefore falls back to
`full_name`/`name` equality plus the org-wide `"linear"` placeholder rule
(any enabled Linear source with `metadata_.org_wide_placeholder == true` or
`external_id == 'linear'` owns **all** Linear instances for the org) — this
is intentionally conservative (favors false-positive conflicts over
false-negative silent divergence) but is not a byte-for-byte guarantee.
**Operational guidance**: disable managed Linear sync for a team before
enabling customer push for that same team's work items; do not rely on the
matching rule alone to prevent a Linear collision.

## Decision 4: Interim auth is a mechanically-gated stopgap, not a real credential system

CHAOS-2691 ships `api/external_ingest/auth.py::require_ingest_scope` with a
body that accepts **any** bearer token + `X-Org-Id` header combination —
solely so the REST contract is end-to-end testable before CHAOS-2696/2712
land real token issuance and validation. This is gated, not silently
insecure:

- The dependency **hard-fails `503 auth_not_configured`** unless the
  deployment sets `EXTERNAL_INGEST_INSECURE_AUTH=1` — a flag intended only
  for local compose/CI, never a deployed environment.
- Every request accepted under the flag logs a WARNING naming the org_id,
  so interim-mode traffic is visible in ops before CHAOS-2696/2712 land.
- **CHAOS-2696/2712 landing (real DB-backed `IngestToken` validation) is a
  hard pre-GA blocker** for this epic, not routine follow-up work. Merging
  the `chaos-2690-external-ingest` integration branch to `main` remains
  additionally gated on CHAOS-2712.

## Consequences

- Sibling tickets import fixed shapes from this ADR + CHAOS-2691's
  `schemas.py`/`auth.py`/`errors.py` rather than re-deriving the contract,
  keeping the epic's many parallel sub-issues from diverging.
- The Linear residual risk above is a known, documented gap — not a defect
  to silently patch over — until a future ticket adds a live Linear API
  lookup to the matching rule (not scoped to CHAOS-2690 v1).
- Interim auth is real, working code, not a mock — which is precisely why
  it needs the mechanical off-switch: a working-but-insecure code path is
  more dangerous than an obviously-fake one if a deploy gate slips.
