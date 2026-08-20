# Implementation Brief: CHAOS-2714 — Web setup screens for customer-push source onboarding

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. The §4.1 admin contract is RATIFIED with backend ownership assigned: sources/tokens
>    CRUD = CHAOS-2696 (wave 1); batches list/detail + schemas proxies = CHAOS-2694
>    (wave 2); validate + console-push proxies = CHAOS-2695 (wave 4). This issue lands in
>    wave 3: sources/tokens/batches screens wire live; Validate/Push screens are built
>    against MSW mocks and live-verified in wave 4.
> 2. Field-shape corrections to §4.1's sketches (TS types must mirror the ops Pydantic
>    schemas): source rows use **`id`** (not `source_id`) per 2696's `IngestSourceResponse`;
>    token rows have no `status` or `last_result` field — derive
>    `active/revoked/expired/never_used` CLIENT-SIDE from `revoked_at`/`expires_at`/
>    `last_used_at` (drop `last_result` in v1); token prefix is **`fcpush_`** (not
>    `fchp_live_`); batch `status` enum is `accepted|stream_unavailable|processing|
>    completed|partial|failed` (drop `rejected`/`ignored_unsupported_event` — webhook-
>    addendum vocabulary, not batch statuses); `error_summary` is a JSON object
>    (`{total_rejected, stored_rejections, truncated, top_codes}`), not a string;
>    `record_counts_by_kind` field name is `record_counts` (JSON column, kinds are
>    versioned strings); `recompute_status` values come from 2699's 0034 columns — pinned enum `not_applicable|pending|dispatched|skipped_no_scope|failed` (CC21); surfaced in GET batch detail only after 2699 lands (wave 3).
> 3. Batch list filters supported server-side in v1: status, producer, from, to,
>    limit/offset. **No `record_kind` query param** — filter client-side from
>    `record_counts` if needed.
> 4. Source `instance` is repo/project-grain (CC5): registration form copy = "repository
>    full name (owner/repo)" for GitHub/GitLab, project key for Jira, team key for Linear
>    (not `github.com/acme`-style org URLs). Sources also expose `webhook_mode`
>    (`disabled|customer_relay`; `fullchaos_hosted` renders disabled/"coming soon").
> 5-OVERRULED (post-critique, CC25 product decision): **Screen 5 is VALIDATE-ONLY in
>    v1.** D6's console-push proxy (`POST .../sources/{id}/batches`,
>    producer="web-console") is CUT from v1 → v2 follow-up; the ingestion write path
>    stays exclusively token-authed. Keep the validate proxy
>    (`POST .../sources/{id}/validate`). Drop the console-push ROUTE_LIMITS entry, the
>    "Push this payload" CTA, and the "console" leg of D8's producer heuristic (keep the
>    classifier's fallthrough). D6's original text below is VOID:
> ~~5. D6 (console push, producer="web-console", 10/hour proxy limit) RATIFIED pending the
>    epic owner's product veto (tracked as an open question — build behind the pinned
>    contract).~~
> 6. Wave-3 hot-file note: this issue is web-repo-only; no ops files.

Repo: `dev-health-web`, worktree `/Users/chris/projects/full-chaos/dev-health/web/.claude/worktrees/chaos-2690-integration`, branch `chaos-2690-external-ingest`.

Parent epic: CHAOS-2690. Sibling issues referenced throughout: CHAOS-2691 (REST contract), CHAOS-2692 (schema discovery), CHAOS-2694 (status/rejections), CHAOS-2696 (source registration), CHAOS-2712 (auth/credential lifecycle), CHAOS-2713 (CI/CD examples), CHAOS-2701 (docs), CHAOS-2715 (webhook-assisted, out of scope here beyond a disabled/beta card).

All file paths below are verified against the current worktree state (read directly, not assumed from the plan docs).

---

## 1. Scope

Build the 7 web screens from the CHAOS-2714 issue description and the design doc (`web/docs/customer-push-ingestion-setup-design.md`, addendum `ops/docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md`), as an **extension of the existing `/org/admin/integrations` tree**, not a new top-level flow:

1. **Source setup landing** — mode-choice cards (managed sync vs customer push) on `[provider]/page.tsx`, plus a list of existing customer-push sources for that provider.
2. **Create customer-push source** — `.../customer-push/new`.
3. **Credential creation** (one-time token display) — `.../customer-push/[source_id]/credentials/new`.
4. **Runner setup examples** (GitHub Actions / GitLab Runner / Docker / cURL / webhook relay tabs) — `.../customer-push/[source_id]/examples`.
5. **Validate first payload** — `.../customer-push/[source_id]/validate`.
6. **Ingest status list + drilldown** — `.../customer-push/[source_id]/batches` and `.../batches/[ingestion_id]`.
7. **Credential management** (list/rotate/revoke) — `.../customer-push/[source_id]/credentials`.

Plus a source overview page (`.../customer-push/[source_id]`) that screen 1 CTAs and screen-2 creation redirect into — this is implied by the design doc's route list but not separately numbered; it is required for navigation to make sense (see Design Decision D1).

This brief also specifies the **new admin-session-authenticated backend endpoints** the web screens need (Design Decision D2) because none of the sibling backend issues (2691/2692/2694/2696/2712) define a session-authenticated surface — they only define the token-authenticated customer-facing `/api/v1/external-ingest/*` API. This is a genuine cross-issue gap; the contract below is the resolution and should be handed to whichever agent implements 2696/2712/2694 on the ops side.

## 2. Out of scope

- Any ops-side (`dev-health-ops`) implementation. This brief specifies the *contract* the web needs (Section 4) but the coding agent for this issue should stub/mock it (MSW handlers) and treat the real backend as a dependency (see Section 9). If the backend endpoints already exist by the time this is implemented, wire to them as specified; do not redesign the contract without re-checking this brief's Design Decisions.
- FullChaos-hosted webhook endpoint (Screen 7 "FullChaos-hosted webhook screen" in the design doc / CHAOS-2715) — only the customer-owned-relay tab content (static docs/example) ships now, marked `Experimental`. No relay backend, no source-scoped webhook secret UI.
- GraphQL for any of this (CHAOS-2690 is non-GraphQL v1; do not touch `schema.graphql`).
- `dev-hops push export <provider>` CLI implementation (CHAOS-2700) — the examples screen only *displays* the commands.
- Actual CI/CD example authoring for CHAOS-2713's docs/repo templates — reuse the same snippet source (Section 6) but do not duplicate ownership of that issue's docs deliverables.
- Provider-specific ingest scopes (`ingest:github`, `ingest:gitlab`, ...) — plan explicitly defers these; UI reserves no interactive control for them (see D7).
- Tier/license gating UI unless `_request.ts`'s existing `feature_not_licensed` error shape is returned by the backend — no new gating logic to build, it's inherited for free (recon confirmed).

## 3. Design decisions

**D1. Add a `[source_id]` overview/detail page, not just leaf tab pages.**
Rationale: the design doc's route list includes `.../customer-push/[source_id]` as a bare route with no numbered screen — it's the landing point after creating a source and the natural back-link target from every leaf screen. Mirrors the existing `/org/admin/sync/[configId]/page.tsx` precedent (dynamic detail page importing sub-widgets) exactly. Build it as a lightweight summary: source system/instance/mode/enabled state, a one-active-owner conflict banner if applicable, and 4 link-cards to Credentials / Examples / Validate / Batches.

**D2. New admin-session-authenticated REST surface `/api/v1/admin/customer-push/*`, separate from the token-authenticated `/api/v1/external-ingest/*` customer surface.**
Rationale: the web admin UI authenticates via NextAuth JWT session + `X-Org-Id` (see `getSessionContext()` in `src/lib/admin/server/_shared.ts`) — it never holds an ingest token. The customer-facing `/api/v1/external-ingest/*` endpoints (CHAOS-2691) are token-scoped (`schema:read`/`ingest:write`/`ingest:status`) per CHAOS-2712's auth model ("missing or invalid token returns 401"), so the browser session cannot call them directly. Concretely this requires a new ops router `src/dev_health_ops/api/admin/routers/customer_push.py` (mirroring `routers/credentials.py`'s exact shape: `get_admin_org_id` dependency, one router per domain, included in `api/admin/router.py`) exposing:
- Source CRUD (org-scoped, backed by CHAOS-2696's source registration table)
- Token issuance/rotation/revocation (org-scoped, backed by CHAOS-2712's credential model; plaintext token returned once, matching the `RefreshToken` hashed-storage precedent from ops recon)
- A read-only proxy over CHAOS-2694's status/rejections store, scoped by session `org_id` instead of a token
- A schema-discovery proxy over CHAOS-2692's registry (so the in-product Validate screen doesn't require the customer to paste a secret token into the browser)
- A validate proxy and a "push sample from console" proxy (see D6)
Full contract in Section 4. **This is a cross-issue interface decision** — flag to whoever picks up CHAOS-2696/2712/2694 that this router is required; do not let them build a token-only surface and leave the web team blocked.

**D3. `custom` is a customer-push-only pseudo-provider, not a new managed-sync `Provider`.**
Rationale: `src/lib/admin/types.ts`'s `Provider` union (`"github"|"gitlab"|"jira"|"linear"|"launchdarkly"`) and `PROVIDER_SYNC_TARGETS` drive managed-sync credential/sync-config forms (`IntegrationForm.tsx`, `ProviderForms.tsx`). Adding `"custom"` there would imply a managed-sync flow that doesn't exist. Instead: extend only the page-local `PROVIDERS: Record<string,string>` map in `src/app/(app)/org/admin/integrations/[provider]/page.tsx` (already documented as a separate, intentionally-drifted map per `docs/providers-integration.md`) with `custom: "Custom"`, and gate the managed-sync-only blocks (`GitHubAppConnect`, `ProviderCredentialsList`) to not render when `provider === "custom"`. `/org/admin/integrations/custom` shows only the customer-push mode card + source list. Introduce a new, additive type `CustomerPushSystem = Provider | "custom"` in `types.ts` for customer-push-specific interfaces — do not touch `Provider` itself.

**D4. `launchdarkly` does not get a customer-push mode card.** LaunchDarkly is not in the epic's v1 record-kind/source-system list (github/gitlab/jira/linear/custom). Provider detail page renders only the managed-sync form for `launchdarkly`, unchanged from today.

**D5. Separate routes per screen (not a `?tab=` query param), matching the design doc's primary recommendation.** Rationale: batch-detail deep links need to be printable by `dev-hops push batch --poll` (CLI prints a status URL) and referenceable from future ingest-failure notifications — the design doc explicitly says "prefer separate routes if the implementation needs deep links from docs, CLI output, email alerts". A `?tab=` param would still work for deep-linking but separate routes give cleaner `generateMetadata`/breadcrumbs and match the existing `/org/admin/sync/[configId]` + `/org/admin/sync/new` precedent. Do not build a tab-query-param version.

**D6. [VOID — OVERRULED post-critique (CC25): Screen 5 is validate-only in v1; console-push moved to v2 follow-ups. Retained below for the v2 design record only.]** ~~The in-product Validate screen (Screen 5) can push a payload directly from the browser via the admin-session proxy, not just show CLI commands.~~ Rationale: CHAOS-2701's acceptance criterion ("Customer can validate and push a sample payload") and the design doc's explicit "Push this payload" CTA both imply an in-product push path. Since the browser never holds a customer ingest token (D2), this must go through the new `POST /api/v1/admin/customer-push/sources/{source_id}/batches` proxy (Section 4), which the ops side implements by resolving org_id from the session and internally reusing the same accept/enqueue code path as the token-authed `POST /external-ingest/batches`. The server MUST stamp `source.producer = "web-console"` (override any client-supplied value) so console-originated batches are distinguishable from CI/relay/CLI batches in the batch list (feeds the "producer" filter in Screen 6). Idempotency key is generated client-side as `web-console-{source_id}-{uuid}` per push. Add a `ROUTE_LIMITS` entry in `src/proxy.ts` for this path (10/hour per user, mirroring the existing `POST /api/v1/admin/credentials/test-connection` limit) since it's a real side-effecting write reachable from the browser.

**D7. Provider-specific scopes (`ingest:github` etc.) render as visibly-disabled checkboxes with a "Coming soon" tooltip, not hidden entirely.** Rationale: CHAOS-2714's acceptance criteria say "Screens make authorization and credential scope visible, not hidden in docs only" — showing the full intended scope model (even disabled) sets correct customer expectations without shipping non-functional controls. Do not wire them to any state; `CustomerPushTokenCreate.scopes` only ever contains the 3 v1 scopes.

**D8. Producer-type bucketing (CLI/CI/relay/API) is a client-side heuristic, not a backend enum.** Rationale: CHAOS-2691's batch envelope only carries a free-text `source.producer` string (e.g. `"dev-hops-cli"`, `"web-console"`) plus `producerVersion` — no sibling issue defines a `producer_type` categorical field. Implement `classifyProducer(producer: string): "cli" | "ci" | "relay" | "api" | "console"` in `src/lib/customer-push/producer.ts` as a pure function (prefix/substring match: `dev-hops` → cli, `github-actions`/`gitlab-ci`/`.ci.` → ci, `relay` → relay, `web-console` → console, else → api) and use it only for the Screen 6 filter chips and badge coloring — never send a derived value back to the API. Flag to CHAOS-2696/2691 owners that a first-class `producer_type` enum would be a cleaner v2 fix; not blocking for v1.

**D9. Token value is never persisted client-side beyond the reveal-once render.** The one-time token panel holds the plaintext in local component `useState` only, cleared on unmount/navigation; it is never written to `localStorage`/`sessionStorage`/URL. `CustomerPushTokenCreateResponse.token` must not be logged (add a lint-disable-free guard: never pass the response object as-is into `console.*` or a toast that echoes full objects).

**D10. Design-lint `no-raw-id-in-jsx` handling for `source_id`/`ingestion_id`/`token_id`.** These are not work-graph entities so `resolveEntityLabel`/`EntityLabel` don't apply (confirmed: that helper resolves contributor/work-item graph nodes only). Render as a truncated monospace value (`{id.slice(0, 8)}…`) with the full UUID in a `title` attribute and a copy-to-clipboard icon button, and add a `// design-lint-disable-next-line no-raw-id-in-jsx -- source/ingestion identifiers are the primary key the customer must correlate against their CI logs, no human label exists` comment at each render site. Do not attempt to invent a fake human-readable label.

**D11. Reuse existing status-badge and job-history table patterns instead of inventing new visual primitives.** `SyncStatusBadge.tsx`/`ConnectionStatus.tsx` (badge), `SyncJobHistory.tsx` (paginated status table w/ row-click-to-detail), and `SyncRunDetailLive.tsx` (client polling component: `POLL_INTERVAL_MS=3500`, `MAX_POLL_DURATION_MS=10*60*1000`, `testMode` prop that renders static data and never polls) are the direct precedents for the batch list/detail screens. Copy their poll-until-terminal pattern verbatim for `CustomerPushBatchDetailLive.tsx`; do not hand-roll a new polling loop.

**D12. New admin nav entry.** Do not add a new top-level `AdminSidebar.tsx` entry — customer-push lives entirely under the existing "Integrations" nav item, reached via the provider detail page. Plain `<Link href="...">`, no `withFilterParam` (confirmed: admin routes are not filter-scoped; zero existing usage under `src/components/admin/`).

## 4. API / schema sketches

### 4.1 New backend contract (ops side — dependency, not built by this issue)

Base path `/api/v1/admin/customer-push` (same admin prefix/auth pattern as `/api/v1/admin/credentials`: `Depends(get_admin_org_id)`, i.e. requires an admin/owner JWT session, not an ingest token).

```
GET    /api/v1/admin/customer-push/sources?system=github
POST   /api/v1/admin/customer-push/sources
GET    /api/v1/admin/customer-push/sources/{source_id}
PATCH  /api/v1/admin/customer-push/sources/{source_id}        # enabled toggle, display name

GET    /api/v1/admin/customer-push/sources/{source_id}/tokens
POST   /api/v1/admin/customer-push/sources/{source_id}/tokens          # -> plaintext token ONCE
POST   /api/v1/admin/customer-push/tokens/{token_id}/rotate            # -> plaintext token ONCE
POST   /api/v1/admin/customer-push/tokens/{token_id}/revoke

GET    /api/v1/admin/customer-push/sources/{source_id}/batches?status=&producer=&record_kind=&from=&to=&limit=&offset=
GET    /api/v1/admin/customer-push/batches/{ingestion_id}              # includes rejected_records[]

GET    /api/v1/admin/customer-push/schemas
GET    /api/v1/admin/customer-push/schemas/{schema_version}
POST   /api/v1/admin/customer-push/sources/{source_id}/validate        # body = batch envelope, no idempotency key required
POST   /api/v1/admin/customer-push/sources/{source_id}/batches         # console-push proxy, see D6; server stamps producer="web-console"
```

Pydantic-shaped response bodies (illustrative; final field names must match whatever CHAOS-2696/2712/2694 land on — confirm before wiring, per web-admin recon's warning that `types.ts` is a hand-maintained, non-codegen'd mirror):

```jsonc
// CustomerPushSource
{
  "source_id": "uuid",
  "org_id": "uuid",
  "system": "github",              // "github" | "gitlab" | "jira" | "linear" | "custom"
  "instance": "github.com/acme",
  "display_name": "Acme GitHub",
  "mode": "customer_push",         // "fullchaos_sync" | "customer_push" | "disabled"
  "enabled": true,
  "conflicting_managed_sync": false, // true if a fullchaos_sync config already owns this instance
  "created_at": "2026-06-25T00:00:00Z",
  "updated_at": "2026-06-25T00:00:00Z"
}

// CustomerPushToken (list item — never includes the secret)
{
  "token_id": "uuid",
  "name": "CI runner",
  "source_id": "uuid | null",       // null = org-wide, not bound to one source
  "scopes": ["schema:read", "ingest:write", "ingest:status"],
  "status": "active",               // "active" | "rotated" | "revoked" | "expired" | "never_used"
  "last_used_at": "2026-06-25T00:00:00Z | null",
  "last_result": "accepted | rejected | null",
  "expires_at": "2027-01-01T00:00:00Z | null",
  "created_at": "2026-06-25T00:00:00Z"
}

// CustomerPushTokenCreateResponse (create/rotate response only — one-time)
{
  "token_id": "uuid",
  "token": "fchp_live_...",          // plaintext, shown once, never returned again
  "name": "CI runner",
  "source_id": "uuid | null",
  "scopes": ["schema:read", "ingest:write", "ingest:status"],
  "expires_at": "2027-01-01T00:00:00Z | null"
}

// CustomerPushBatchSummary (list row)
{
  "ingestion_id": "uuid",
  "source_id": "uuid",
  "producer": "dev-hops-cli",
  "window_started_at": "2026-06-25T00:00:00Z",
  "window_ended_at": "2026-06-26T00:00:00Z",
  "status": "processing",   // accepted|processing|completed|partial|failed|rejected|ignored_unsupported_event
  "items_received": 500,
  "items_accepted": 492,
  "items_rejected": 8,
  "created_at": "2026-06-26T00:01:00Z",
  "completed_at": "2026-06-26T00:03:00Z | null"
}

// CustomerPushBatchDetail extends Summary with:
{
  "schema_version": "external-ingest.v1",
  "record_counts_by_kind": { "pull_request": 120, "review": 80 },
  "recompute_status": "not_applicable | pending | dispatched | skipped_no_scope | failed",
  "error_summary": "string | null",
  "rejected_records": [
    { "index": 12, "kind": "pull_request", "external_id": "PR#88", "code": "missing_external_id", "path": "records[12].externalId", "message": "externalId is required" }
  ]
}

// ValidateResponse (mirrors POST /external-ingest/validate shape from CHAOS-2691)
{
  "valid": false,
  "items_accepted": 487,
  "items_rejected": 13,
  "errors": [ { "index": 12, "kind": "pull_request", "code": "missing_external_id", "path": "records[12].externalId", "message": "externalId is required" } ]
}
```

### 4.2 New TypeScript types — `src/lib/admin/types.ts` additions

Append near the existing `IntegrationCredential`/`SyncConfig` sections, following the file's own "mirrors dev-health-ops/api/admin/schemas.py" convention:

```ts
// ---- Customer Push (CHAOS-2690/2714) ----

export type CustomerPushSystem = Provider | "custom";
export type CustomerPushMode = "fullchaos_sync" | "customer_push" | "disabled";
export type CustomerPushBatchStatus =
    | "accepted" | "processing" | "completed" | "partial" | "failed"
    | "rejected" | "ignored_unsupported_event";
export type CustomerPushTokenStatus = "active" | "rotated" | "revoked" | "expired" | "never_used";
export type CustomerPushScope = "schema:read" | "ingest:write" | "ingest:status";

export interface CustomerPushSource {
    source_id: string;
    org_id: string;
    system: CustomerPushSystem;
    instance: string;
    display_name: string;
    mode: CustomerPushMode;
    enabled: boolean;
    conflicting_managed_sync: boolean;
    created_at: string;
    updated_at: string;
}

export interface CustomerPushSourceCreate {
    system: CustomerPushSystem;
    instance: string;
    display_name: string;
}

export interface CustomerPushToken {
    token_id: string;
    name: string;
    source_id: string | null;
    scopes: CustomerPushScope[];
    status: CustomerPushTokenStatus;
    last_used_at: string | null;
    last_result: "accepted" | "rejected" | null;
    expires_at: string | null;
    created_at: string;
}

export interface CustomerPushTokenCreate {
    name: string;
    source_id?: string | null;
    scopes: CustomerPushScope[];
    expires_at?: string | null;
}

export interface CustomerPushTokenCreateResponse {
    token_id: string;
    token: string;
    name: string;
    source_id: string | null;
    scopes: CustomerPushScope[];
    expires_at: string | null;
}

export interface CustomerPushBatchSummary {
    ingestion_id: string;
    source_id: string;
    producer: string;
    window_started_at: string;
    window_ended_at: string;
    status: CustomerPushBatchStatus;
    items_received: number;
    items_accepted: number;
    items_rejected: number;
    created_at: string;
    completed_at: string | null;
}

export interface CustomerPushRejectedRecord {
    index: number;
    kind: string;
    external_id: string | null;
    code: string;
    path: string;
    message: string;
}

export interface CustomerPushBatchDetail extends CustomerPushBatchSummary {
    schema_version: string;
    record_counts_by_kind: Record<string, number>;
    recompute_status: "not_applicable" | "pending" | "dispatched" | "skipped_no_scope" | "failed"; // pinned epic-wide (CC21, 2699's 0034)
    error_summary: string | null;
    rejected_records: CustomerPushRejectedRecord[];
}

export interface CustomerPushValidateResponse {
    valid: boolean;
    items_accepted: number;
    items_rejected: number;
    errors: CustomerPushRejectedRecord[];
}

export interface CustomerPushSchemaListEntry {
    schema_version: string;
    record_kinds: string[];
}

export interface CustomerPushSchemaListResponse {
    schemas: CustomerPushSchemaListEntry[];
}
```

### 4.3 API client — `src/lib/admin/api/customer-push.ts` (new file)

Follow the exact shape of `src/lib/admin/api/credentials.ts` — thin `request<T>()` wrappers, no business logic:

```ts
import { request } from "./_request";
import type {
    CustomerPushSource, CustomerPushSourceCreate, CustomerPushToken,
    CustomerPushTokenCreate, CustomerPushTokenCreateResponse,
    CustomerPushBatchSummary, CustomerPushBatchDetail,
    CustomerPushValidateResponse, CustomerPushSchemaListResponse,
} from "../types";

export const customerPushApi = {
    listSources: (system?: string, token?: string, orgId?: string) =>
        request<CustomerPushSource[]>(
            `/customer-push/sources${system ? `?system=${encodeURIComponent(system)}` : ""}`,
            {}, token, orgId,
        ),
    createSource: (data: CustomerPushSourceCreate, token?: string, orgId?: string) =>
        request<CustomerPushSource>("/customer-push/sources", { method: "POST", body: JSON.stringify(data) }, token, orgId),
    getSource: (sourceId: string, token?: string, orgId?: string) =>
        request<CustomerPushSource>(`/customer-push/sources/${sourceId}`, {}, token, orgId),
    updateSource: (sourceId: string, data: Partial<Pick<CustomerPushSource, "enabled" | "display_name">>, token?: string, orgId?: string) =>
        request<CustomerPushSource>(`/customer-push/sources/${sourceId}`, { method: "PATCH", body: JSON.stringify(data) }, token, orgId),

    listTokens: (sourceId: string, token?: string, orgId?: string) =>
        request<CustomerPushToken[]>(`/customer-push/sources/${sourceId}/tokens`, {}, token, orgId),
    createToken: (sourceId: string, data: CustomerPushTokenCreate, token?: string, orgId?: string) =>
        request<CustomerPushTokenCreateResponse>(`/customer-push/sources/${sourceId}/tokens`, { method: "POST", body: JSON.stringify(data) }, token, orgId),
    rotateToken: (tokenId: string, token?: string, orgId?: string) =>
        request<CustomerPushTokenCreateResponse>(`/customer-push/tokens/${tokenId}/rotate`, { method: "POST" }, token, orgId),
    revokeToken: (tokenId: string, token?: string, orgId?: string) =>
        request<void>(`/customer-push/tokens/${tokenId}/revoke`, { method: "POST" }, token, orgId),

    listBatches: (sourceId: string, params: Record<string, string | undefined>, token?: string, orgId?: string) => {
        const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v) as [string, string][]).toString();
        return request<CustomerPushBatchSummary[]>(`/customer-push/sources/${sourceId}/batches${qs ? `?${qs}` : ""}`, {}, token, orgId);
    },
    getBatch: (ingestionId: string, token?: string, orgId?: string) =>
        request<CustomerPushBatchDetail>(`/customer-push/batches/${ingestionId}`, {}, token, orgId),

    listSchemas: (token?: string, orgId?: string) =>
        request<CustomerPushSchemaListResponse>("/customer-push/schemas", {}, token, orgId),
    getSchema: (version: string, token?: string, orgId?: string) =>
        request<Record<string, unknown>>(`/customer-push/schemas/${encodeURIComponent(version)}`, {}, token, orgId),

    validate: (sourceId: string, envelope: unknown, token?: string, orgId?: string) =>
        request<CustomerPushValidateResponse>(`/customer-push/sources/${sourceId}/validate`, { method: "POST", body: JSON.stringify(envelope) }, token, orgId),
    pushFromConsole: (sourceId: string, envelope: unknown, token?: string, orgId?: string) =>
        request<{ ingestion_id: string; status: string; items_received: number }>(
            `/customer-push/sources/${sourceId}/batches`, { method: "POST", body: JSON.stringify(envelope) }, token, orgId,
        ),
};
```

Register in `src/lib/admin/api.ts`: import `customerPushApi` and add `customerPush: customerPushApi,` to the `adminApi` object.

### 4.4 Server Actions — `src/lib/admin/server/customer-push.ts` (new file)

Follow `src/lib/admin/server/credentials.ts` exactly: `"use server"`, `getSessionContext()` + `withErrorHandling()` + `revalidatePath()` after mutations. One function per API client method above (`listCustomerPushSources`, `createCustomerPushSource`, `updateCustomerPushSource`, `listCustomerPushTokens`, `createCustomerPushToken`, `rotateCustomerPushToken`, `revokeCustomerPushToken`, `listCustomerPushBatches`, `getCustomerPushBatch`, `listCustomerPushSchemas`, `getCustomerPushSchema`, `validateCustomerPushPayload`, `pushCustomerPushPayloadFromConsole`). `revalidatePath("/org/admin/integrations", "page")` after source/token mutations only (not after read-only validate/batches calls).

Add `export * from "./server/customer-push";` to `src/lib/admin/server.ts` barrel.

## 5. Files to create/modify

All paths relative to `/Users/chris/projects/full-chaos/dev-health/web/.claude/worktrees/chaos-2690-integration`.

**Modify:**
- `src/lib/admin/types.ts` — add types from §4.2
- `src/lib/admin/api.ts` — register `customerPush: customerPushApi`
- `src/lib/admin/server.ts` — re-export `./server/customer-push`
- `src/app/(app)/org/admin/integrations/[provider]/page.tsx` — add `custom: "Custom"` to local `PROVIDERS` map; render `<ModeCards provider={provider} sources={...} />` above/alongside `ProviderCredentialsList`; skip `GitHubAppConnect`/`ProviderCredentialsList` when `provider === "custom"`
- `src/proxy.ts` — add `ROUTE_LIMITS` entry for `POST /api/v1/admin/customer-push/sources/:id/batches` (console-push, D6) and for `POST /api/v1/admin/customer-push/sources/:id/tokens` + `.../rotate` (token issuance abuse guard, 20/hour per user)
- `tests/mocks/handlers.ts` — add `*/api/v1/admin/customer-push/*` handlers (§7)

**Create — library:**
- `src/lib/admin/api/customer-push.ts`
- `src/lib/admin/server/customer-push.ts`
- `src/lib/customer-push/producer.ts` — `classifyProducer()` (D8)
- `src/lib/customer-push/examples.ts` — `buildExampleSnippets({ apiUrl, sourceSystem, sourceInstance, tokenPlaceholder }): ExampleTab[]` pure function generating the 5 tab contents (GitHub Actions/GitLab/Docker/cURL/webhook-relay), reusing the literal YAML/bash blocks from the design doc verbatim (do not paraphrase — they must match CHAOS-2713's eventual docs exactly)

**Create — routes (App Router):**
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/new/page.tsx`
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/[source_id]/page.tsx`
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/[source_id]/credentials/page.tsx`
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/[source_id]/credentials/new/page.tsx`
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/[source_id]/examples/page.tsx`
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/[source_id]/validate/page.tsx`
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/[source_id]/batches/page.tsx`
- `src/app/(app)/org/admin/integrations/[provider]/customer-push/[source_id]/batches/[ingestion_id]/page.tsx`

**Create — components** (`src/components/admin/integrations/customer-push/`):
- `ModeCards.tsx` — screen 1 mode-choice cards (client, static; managed-sync CTA links to existing `ProviderCredentialsList` add-connection flow, customer-push CTA links to `.../customer-push/new` or `.../customer-push` list if sources exist)
- `CustomerPushSourceList.tsx` — list of sources per provider with status pill + link to `[source_id]`
- `CreateCustomerPushSourceForm.tsx` — screen 2 form (client), calls `createCustomerPushSource`, shows the duplicate-active-source / one-active-owner validation copy verbatim from the design doc
- `CustomerPushSourceOverview.tsx` — `[source_id]` landing content (D1)
- `CustomerPushTokenList.tsx` — screen 7 table (rotate/revoke actions, mirrors `CredentialCard.tsx` confirm-dialog pattern)
- `CreateCustomerPushTokenForm.tsx` — screen 3 form (scope checkboxes incl. disabled provider-specific ones per D7)
- `TokenRevealPanel.tsx` — one-time token display + copy button (D9)
- `SetupExamplesTabs.tsx` — screen 4 tabs, consumes `buildExampleSnippets()`
- `ValidatePayloadPanel.tsx` — screen 5 (paste/upload/sample modes + results table + "Push this payload" CTA wired to `pushCustomerPushPayloadFromConsole`, D6)
- `CustomerPushStatusBadge.tsx` — badge for the 7 batch statuses (mirrors `SyncStatusBadge.tsx`)
- `CustomerPushBatchList.tsx` — screen 6 list (mirrors `SyncJobHistory.tsx`; filters: source is implicit via route, status/producer/record-kind/date-range as query params)
- `CustomerPushBatchDetailLive.tsx` — screen 6 drilldown (mirrors `SyncRunDetailLive.tsx` poll pattern, D11; `testMode` prop)
- `RejectedRecordsTable.tsx` — index/kind/external_id/code/path/message table used by both Validate and batch-detail screens

**Create — unit tests** (co-located `*.test.tsx`, one per component above, Vitest + Testing Library convention already used repo-wide) and:
- `src/lib/customer-push/producer.test.ts`
- `src/lib/customer-push/examples.test.ts`
- `src/lib/admin/api/__tests__/customer-push.test.ts` (mirrors existing `src/lib/admin/api/__tests__/` pattern — check that directory for the exact mocking convention before writing)

**Create — e2e:**
- `tests/admin-customer-push.spec.ts` (see §7)

## 6. Screen-by-screen empty/loading/error states (hard requirement — issue AC)

Per screen, the following states must exist (empty state only renders when the list is genuinely empty — mirrors the memory rule "Empty states only when no data"; never a placeholder over real-but-zero data without checking it's truly absent):

1. **Landing/mode cards**: no loading state needed (SSR data); if `credentialsResult.error`/source-list fetch errors, render the existing red error banner pattern from `[provider]/page.tsx`. Empty state = "No customer-push sources yet" only when `sources.length === 0` for that provider.
2. **Create source**: client-side pending state on submit button (`isPending` from `useTransition`, matching `CredentialCard.tsx`); server validation errors (duplicate active source / one-active-owner) rendered inline above the form, using the exact copy from the design doc.
3. **Credential creation**: token reveal panel has no "empty" state (it only renders post-creation); loading = submit button `disabled` + "Creating..." label; error = inline banner with `AdminApiError.detail`.
4. **Examples tabs**: no network call (pure client render from `buildExampleSnippets()`) — no loading/error/empty states needed, only the always-populated 5 tabs.
5. **Validate**: 3 sub-states — idle (no payload yet, sample/paste/upload chooser visible), valid result, invalid result (error table). "Push this payload" CTA only enabled when `valid === true`. Loading = spinner on validate button. Error (network/5xx) = toast + inline banner, distinct from `valid: false` (a schema-invalid payload is not an API error).
6. **Batch list**: loading = skeleton rows (or reuse `SyncJobHistory`'s `jobs.length === 0` "No sync history available" copy adapted to "No batches yet — push your first payload from Validate or a CI job."); this empty state should link to both `.../validate` and `.../examples`. Error = red banner, same pattern as source list.
7. **Batch detail**: loading = skeleton while `initialSummary` resolves server-side (RSC), then live-polls per D11; terminal states (`completed`/`failed`/`rejected`/`ignored_unsupported_event`) stop polling (`isTerminalSyncStatus`-equivalent helper — add `isTerminalCustomerPushStatus()` to `src/lib/customer-push/producer.ts` or a new `status.ts`). Rejected-records table empty state = "No rejected records" (green, not red) when `items_rejected === 0`.
8. **Credential management list**: empty state = "No credentials yet for this source" with CTA to `.../credentials/new`; per-row loading = disabled rotate/revoke buttons during their own `useTransition`.

## 7. Test-mode mocks (`tests/mocks/handlers.ts`)

Add handlers immediately after the existing `*/api/v1/admin/sync-configs*` block (~line 2876), using the exact wildcard `http.get/post/patch("*/api/v1/admin/customer-push/...")` MSW v2 pattern already used. Seed module-level mutable arrays (`MOCK_CUSTOMER_PUSH_SOURCES`, `MOCK_CUSTOMER_PUSH_TOKENS`, `MOCK_CUSTOMER_PUSH_BATCHES`) exactly like `MOCK_CREDENTIALS`/`MOCK_SYNC_CONFIGS`. **Vocabulary must mirror the real backend exactly once it lands** — status strings (`accepted|processing|completed|partial|failed|rejected|ignored_unsupported_event`), scope strings (`schema:read|ingest:write|ingest:status`), and field casing (`snake_case`, matching every other admin endpoint in this file — NOT `camelCase` like the customer-facing `/external-ingest` envelope, which is Pydantic-alias-camelCase per the plan doc's JSON examples). This casing split (admin=`snake_case`, external-ingest=`camelCase`) is a real, deliberate inconsistency inherited from the existing codebase convention (`/api/v1/admin/*` responses are all snake_case; `/api/v1/external-ingest/*` request/response examples in the plan doc are camelCase) — do not "fix" it by picking one casing for both.

Minimum handlers required for the e2e spec in §8 to pass:
```
GET    */api/v1/admin/customer-push/sources
POST   */api/v1/admin/customer-push/sources
GET    */api/v1/admin/customer-push/sources/:id
PATCH  */api/v1/admin/customer-push/sources/:id
GET    */api/v1/admin/customer-push/sources/:id/tokens
POST   */api/v1/admin/customer-push/sources/:id/tokens
POST   */api/v1/admin/customer-push/tokens/:id/rotate
POST   */api/v1/admin/customer-push/tokens/:id/revoke
GET    */api/v1/admin/customer-push/sources/:id/batches
GET    */api/v1/admin/customer-push/batches/:id
POST   */api/v1/admin/customer-push/sources/:id/validate
POST   */api/v1/admin/customer-push/sources/:id/batches
GET    */api/v1/admin/customer-push/schemas
```

## 8. Playwright e2e specs

New file `tests/admin-customer-push.spec.ts`, following `tests/admin-integrations.spec.ts` conventions exactly (`page.goto`, `getByRole("heading", ...)`, `getByRole("button", { name: ... })`, toast text assertions, `#id` locators for form fields). Minimum coverage:

1. Provider detail page (`/org/admin/integrations/github`) renders both "Managed sync" and "Customer push" mode cards.
2. `/org/admin/integrations/custom` renders only the "Customer push" card (no managed-sync form/heading) — regression guard for D3/D4.
3. Create-source flow: fill system/instance/display name, submit, redirected to `/org/admin/integrations/github/customer-push/{source_id}`, overview page shows the new source.
4. Duplicate/conflicting source shows the one-active-owner validation message (mock a 409/400 with the design-doc copy) instead of a generic error.
5. Credential creation flow: submit form, one-time token panel renders exactly once, "Copy" button copies (assert `navigator.clipboard` via Playwright's clipboard permission grant), navigating away and back to the credentials list shows the credential WITHOUT the plaintext token (only masked/metadata fields).
6. Rotate token: click rotate, confirm dialog, new one-time token panel renders; revoke: click revoke, confirm, row status becomes "revoked", rotate/revoke buttons become disabled.
7. Examples tabs: all 5 tabs (GitHub Actions/GitLab Runner/Docker/cURL/Webhook relay) render and are clickable; assert the cURL tab contains the real `/api/v1/external-ingest/batches` path string (regression guard against accidentally pointing examples at the admin proxy path).
8. Validate screen: paste an intentionally-invalid sample payload, submit, assert the rejected-record error table renders with index/kind/path/message columns; paste/select a valid sample, assert "Push this payload" becomes enabled, click it, assert redirect/toast to the new batch's status.
9. Batch list: empty state text when no batches; seeded-batches state shows status badges and links each row to `.../batches/{ingestion_id}`.
10. Batch detail: renders rejected-records table with correct empty-state copy when `items_rejected === 0`; use `testMode` prop (or a mocked terminal-status fixture) so the spec does not depend on real polling timing.

## 9. Dependencies on other sub-issues

- **CHAOS-2696** (source registration) and **CHAOS-2712** (auth/credential lifecycle) — must additionally implement the `/api/v1/admin/customer-push/sources*` and `/api/v1/admin/customer-push/*tokens*` endpoints from §4.1 (D2), not just the token-authed customer surface their own issue text describes. **Flag this explicitly when those issues are picked up.**
- **CHAOS-2694** (ingest status/rejected-record diagnostics) — must additionally implement the `/api/v1/admin/customer-push/sources/{id}/batches` and `/api/v1/admin/customer-push/batches/{id}` read proxies (D2), reusing its Postgres status/rejections tables scoped by session org_id instead of token org_id.
- **CHAOS-2691/2692** (REST contract + schema discovery) — the console-push (D6) and validate proxies internally call the same `validate()`/accept-batch code paths these issues build; the admin router should import and call them directly (same-process function call), not HTTP-loopback to `/api/v1/external-ingest/*`.
- **CHAOS-2700** (dev-hops push CLI) — not a blocking dependency for the web UI itself (examples screen only displays static command text), but the exact CLI flags shown in the examples tabs (`--api-url`, `--token`, `--org`, `--poll`, `dev-hops push validate ... --schema`) must match whatever CHAOS-2700 actually ships; re-verify the literal flag names against CHAOS-2700's implementation before merging, since the design doc's snippets are illustrative/not yet verified against real CLI code (CLI doesn't exist yet per recon-cli.md).
- **CHAOS-2713** (CI/CD examples) — this issue's docs deliverables should reuse `src/lib/customer-push/examples.ts` as source-of-truth content if that doc lives in the web repo, or the ops-repo docs should be kept byte-identical to it manually; flag drift risk either way.

No blocking dependency prevents starting CHAOS-2714 now: build all UI against the MSW mocks in §7, wire to real endpoints once the ops side lands. The whole surface is inert without those endpoints (every screen will show "Failed to load" via `AdminApiError` until they exist), so live end-to-end verification (§11) cannot fully complete until CHAOS-2696/2712/2694 ship their admin-router additions — call this out explicitly when reporting done.

## 10. Gate commands

```bash
cd /Users/chris/projects/full-chaos/dev-health/web/.claude/worktrees/chaos-2690-integration

# format
pnpm format:check:changed

# quality (audit + codegen:check + lint + typecheck)
pnpm audit --audit-level=high --prod
pnpm codegen:check      # should be a no-op — this issue touches zero GraphQL files
pnpm lint
pnpm typecheck

# unit
pnpm test:unit

# design lint (advisory today, but run it — new screens introduce the highest no-raw-id-in-jsx / cta-from-registry risk in the codebase)
pnpm design-lint

# targeted e2e (avoid a full 10+min e2e run while iterating)
npx playwright install chromium   # first run only
pnpm exec playwright test tests/admin-customer-push.spec.ts

# full e2e gate before calling done (matches ci/run_tests.sh e2e tier)
bash ci/run_tests.sh e2e
```

Per project memory: run the LITERAL CI commands (`ci/run_tests.sh format`, `ci/run_tests.sh quality`, `ci/run_tests.sh unit`) rather than ad hoc subsets, to avoid partial-gate false-greens burning CI cycles:
```bash
bash ci/run_tests.sh format
bash ci/run_tests.sh quality
bash ci/run_tests.sh unit
```

## 11. Live verification procedure

Since the ops-side admin endpoints (§4.1) are a dependency and may not exist yet when this issue is implemented, live verification has two tiers:

**Tier 1 — UI-only, against MSW mocks (always runnable):**
```bash
cd /Users/chris/projects/full-chaos/dev-health/web/.claude/worktrees/chaos-2690-integration
pnpm dev   # next dev, port 3000 — but the mock harness expects tests/mocks/http-server.ts on :8000
# In a second terminal:
node --experimental-strip-types tests/mocks/http-server.ts &   # confirm actual start command in package.json "test:e2e" pretest hooks before relying on this
```
Then use Playwright MCP (or `pnpm exec playwright test tests/admin-customer-push.spec.ts --headed`) to click through all 7 screens against the mock backend and visually confirm dark-theme rendering (screenshot each screen — hard rule: no bright borders/sizing drift, verify in the running app, don't assume from code).

**Tier 2 — against a real ops backend (only once CHAOS-2696/2712/2694 land the `/api/v1/admin/customer-push/*` router):**
```bash
# ops side, from ops worktree, host dev-hops per "Live-validate via host dev-hops" convention:
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
.venv/bin/dev-hops migrate postgres upgrade --db "$POSTGRES_URI"
.venv/bin/uvicorn dev_health_ops.api.main:app --reload --port 8000   # or existing compose 'api' service

# web side, point BACKEND_URL at the real ops instance:
cd /Users/chris/projects/full-chaos/dev-health/web/.claude/worktrees/chaos-2690-integration
BACKEND_URL=http://127.0.0.1:8000 pnpm dev
```
Manual walkthrough against a real (test) org: create a customer-push source for `github`/`github.com/<test-org>`, create a token, copy it, confirm `curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8000/api/v1/external-ingest/schemas` succeeds with that token (cross-checks D2's contract — token issued via the admin proxy must actually work against the real customer-facing endpoint), push a sample payload via the Validate screen's "Push this payload" CTA, confirm it shows up in the Batch list with `producer = "web-console"`, open the drilldown and confirm it polls to a terminal state.

Do not run this against the shared dev `default` ClickHouse/Postgres database — use a scratch DB per the `ops/ci/local_validate.sh` isolation convention if any ops-side work is done alongside this to unblock testing.

## 12. Risks

- **Biggest risk: D2's admin-proxy contract is invented by this brief, not sourced from an authoritative backend design doc.** If CHAOS-2696/2712/2694 land with different endpoint names/shapes, this issue's TS types/API client/mocks will need a follow-up patch. Mitigate by keeping the mismatch surface small: all backend calls go through `src/lib/admin/api/customer-push.ts`, a single file to update.
- **`custom` pseudo-provider (D3) touches a page-local map that's already documented as drifted from `types.ts`** — a future refactor that unifies `PROVIDER_META`/`PROVIDERS` could silently break the "no managed-sync form for custom" gate if not re-checked.
- **Console-push proxy (D6) is a real write path reachable from the browser** — needs the `ROUTE_LIMITS` entry (done in scope) and needs the backend to actually stamp `producer="web-console"` to avoid customer confusion about where a batch came from; if the backend implementer skips that stamp, the producer-classification heuristic (D8) will misclassify these as "api".
- **One-time token display (D9) has no existing precedent in this codebase** — highest-scrutiny code path for a security review; the PR should get the codex-adversarial-review pass (project convention: gate + codex per changeset) with specific attention to token persistence/logging.
- **Design-lint `no-raw-id-in-jsx` false-negatives**: the lint rule is AST/string-scan based and advisory-only today — a raw UUID slipping through unflagged into a toast or error string (not JSX) would not be caught; manual review needed for toast/alert copy in addition to JSX.
- **CLI flag names in the examples tabs (D-dependency on CHAOS-2700) are speculative** — CHAOS-2700 doesn't exist yet (confirmed via recon-cli.md), so the examples screen's copy-pasteable commands may drift from the real CLI surface; treat as a known follow-up, not a blocker, but do not let it ship silently wrong — add a code comment in `examples.ts` noting it must be re-verified once CHAOS-2700 lands.
- **Playwright e2e clipboard assertions require `browserName: chromium` + `permissions: ["clipboard-read", "clipboard-write"]`** in the Playwright context — confirm `playwright.config.ts` grants this or the "Copy" button test (§8.5) will need to fall back to asserting the token's presence in the DOM rather than real clipboard content.
