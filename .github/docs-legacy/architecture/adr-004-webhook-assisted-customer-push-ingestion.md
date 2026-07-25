# ADR-004: Webhook-Assisted Customer-Push Ingestion

## Status

Accepted (v1: customer relay only, beta/experimental).

## Context

CHAOS-2690 introduces `POST /api/v1/external-ingest/batches` as the durable, REST-based
path for customers to push developer-health source facts into FullChaos without granting
FullChaos long-lived provider credentials. Once that path exists, the natural follow-up
question is whether provider webhooks (GitHub/GitLab/Jira push notifications) should also
feed the platform, and if so, how.

This ADR evaluates three architectural options for webhook-assisted ingestion and ratifies
a v1 recommendation, a "must not foreclose" contract for source-registration and
idempotency fields that later sub-issues depend on, and the provider-by-provider
feasibility analysis backing the recommendation.

This evaluation does not ship any production code. It ratifies a decision doc, an
illustrative example, and a set of follow-up issue specs. See "Non-goals" below.

### Existing webhook path (verified against code)

`dev-health-ops` already mounts a webhook router at `/api/v1/webhooks/*`
(`src/dev_health_ops/api/webhooks/router.py`):

- `/api/v1/webhooks/github`, `/api/v1/webhooks/gitlab`, `/api/v1/webhooks/jira`,
  `/api/v1/webhooks/health`.
- Validates a provider-specific signature/token (`src/dev_health_ops/api/webhooks/auth.py`),
  parses provider headers into a canonical `WebhookEvent`
  (`src/dev_health_ops/api/webhooks/models.py`), dispatches a Celery task, and returns
  immediately.

This path is a low-stakes internal hint mechanism, not a customer-durability primitive.
Verified limitations, all confirmed directly against the current worktree:

- **Global, environment-wide secrets.** `auth.py:_get_github_webhook_secret` /
  `_get_gitlab_webhook_token` / `_get_jira_webhook_secret` read
  `GITHUB_WEBHOOK_SECRET` / `GITLAB_WEBHOOK_TOKEN` / `JIRA_WEBHOOK_SECRET` from the
  process environment — one value per provider, for the whole deployment. There is no
  per-org or per-source secret.
- **Best-effort dispatch.** `router.py:_dispatch_webhook_task` (lines 39–68) wraps
  `process_webhook_event.delay(...)` in a bare `try/except Exception` that logs and
  swallows any dispatch failure so the webhook endpoint can still return 200/202. An event
  lost here leaves no customer-visible trace.
- **Weak idempotency.** `workers/system_webhooks.py:_is_duplicate_delivery` (line 105) is a
  presence check against a 24-hour TTL cache key (`webhook_delivery:{provider}:{delivery_id}`,
  `ttl_seconds=86400`) with no payload-hash comparison — it cannot distinguish "same
  delivery replayed" from "different payload reusing an old delivery id" the way the
  external-ingest idempotency contract (same key + different hash → `409`) requires.
- **No Linear support.** `WebhookProvider` (`models.py:18`) has exactly three members —
  `GITHUB`, `GITLAB`, `JIRA` — and there is no concept of `source_id` or org/source mapping
  anywhere in the router or models.

### Relevant customer-push contracts (from the core plan + master-spec)

- Batch envelope, idempotency, and status model are defined in
  `docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md` and pinned by
  `docs/superpowers/plans/2026-07-01-chaos-2690-implementation/master-spec.md` (CC1–CC29).
- Batch status vocabulary (CC12, this ADR's authority for the status section below):
  `accepted → (stream_unavailable) → processing → completed | partial | failed`.
- Batch identity: `org_id + source_system + source_instance + idempotency_key`.
- Auth tables are `external_ingest_sources` / `external_ingest_tokens` (CC14), and
  `external_ingest_sources` already reserves `webhook_mode TEXT NOT NULL DEFAULT 'disabled'`
  and `webhook_secret_id UUID NULL` in migration `0032_add_customer_push_ingest_auth.py`
  (CHAOS-2696) — see "Must-not-foreclose contract" below for how those columns are used.

## Decision

**v1 ships Option A only: a customer-owned webhook relay, marked beta/experimental.** No
FullChaos-hosted webhook endpoint ships in v1, and the existing `/api/v1/webhooks/*` router
is not repurposed for customer-push ingestion.

A customer relay is simply another **producer** of the existing
`POST /api/v1/external-ingest/batches` contract — structurally identical to
`dev-hops push batch` or a CI runner. It receives a provider webhook, normalizes the
event(s) into `external-ingest.v1` records, derives a stable `idempotencyKey` (see
derivation rules below), and calls the same batch endpoint every other producer uses. No
new ingest code path, no new idempotency mechanism, no new status model.

```text
GitHub/GitLab/Jira webhook
  → customer-owned relay
  → normalize to external-ingest.v1 records
  → POST /api/v1/external-ingest/batches   (source.producer = "webhook-relay")
  → same durable stream / status / rejection path as any other producer
```

### Why Option A

- FullChaos never touches provider webhook secrets or raw payloads unless the customer
  explicitly chooses to relay them.
- Tenant/source authorization is just an ordinary ingest token (`ingest:write` scope) —
  reuses `external_ingest_tokens`, no new auth primitive.
- Customers keep full control over their own provider webhook security posture.
- The relay is a thin, disposable, customer-hosted component; FullChaos ships the pattern
  and an illustrative sketch, not a production dependency.

## Provider feasibility

| Provider | v1 webhook role | Events to support first | Design notes |
|---|---|---|---|
| GitHub | Acceleration only, not source of truth | `push`, `pull_request` (extend later to `pull_request_review`, `check_run`/`check_suite`, `deployment`/`deployment_status`, `issues`/`issue_comment`) | `X-GitHub-Delivery` is the idempotency key (see below); signed via `X-Hub-Signature-256`; payloads capped at 25 MB, so large pushes need reconciliation. |
| GitLab | Good for MR/pipeline acceleration; push needs reconciliation | Merge Request Hook, Pipeline Hook (Push Hook only as a secondary hint) | Push event payloads include only the newest 20 commits when a push exceeds 20 commits, with `total_commits_count` as the only truncation signal — push webhooks alone are insufficient for commit ingestion. No universal delivery GUID; idempotency key must be derived (see below). |
| Jira | Useful for work-item transition hints; backfill remains required | `jira:issue_created`, `jira:issue_updated`, `jira:issue_deleted` | `changelog` array (transition detail) is present only on `jira:issue_updated`. Webhooks larger than 25 MB are silently dropped by Jira — do not rely on webhook-only ingestion. |
| Linear | Deferred | N/A | `dev-health-ops` already has a native Linear provider; the existing webhook router has no Linear support at all (`WebhookProvider` enum verified above); do not add Linear webhook support until it can reuse the external-ingest schema path and tenant-scoped source registration. |

**Reconciliation is mandatory for every webhook-enabled source, not optional** (see
"Reconciliation schedule" below) — every provider's webhook payloads are lossy in a
documented way (size caps, commit-count truncation, delivery failures), so a webhook-only
source will silently drift from the provider's true state without a periodic batch
reconciliation job.

## Rejected/deferred alternatives

### Option B: FullChaos-hosted webhook endpoint — deferred, not rejected

```text
GitHub/GitLab/Jira webhook
  → /api/v1/webhooks/customer/{source_id}/{provider}
  → verify source-scoped secret
  → normalize to external-ingest records
  → durable status path
```

Deferred pending three preconditions that do not exist yet:

1. **Source-scoped webhook secrets.** Today's model is one global secret per provider
   (verified above). Hosting customer webhooks requires a secret per `external_ingest_sources`
   row, hashed (not the customer's plaintext) so FullChaos can verify an inbound HMAC without
   ever needing to send the secret anywhere itself.
2. **Durable delivery diagnostics.** The existing best-effort dispatch and 24h-TTL-only
   idempotency check are acceptable for internal low-stakes hints; they are not acceptable
   for a contract customers depend on for data completeness.
3. **Replay protection** beyond a bare presence check.

This is a sequencing risk, not a technical dead end: building Option B before the
ingest-token/source model (CHAOS-2712) exists would mean storing per-source provider
webhook secrets with no source-registration table to hang them off yet.

### Option C: repurpose the existing `/api/v1/webhooks/{provider}` endpoints — rejected outright

Not deferred — rejected. Verified reasons (see "Existing webhook path" above for line
references):

- Secrets are single global env vars, not org/source-scoped — one leaked customer webhook
  secret would let anyone forge events for every org on the platform.
- Dispatch is deliberately best-effort; an event can be silently lost with no customer
  visibility.
- Idempotency is a presence-only TTL check with no payload-hash comparison, which does not
  satisfy the external-ingest same-key/different-hash → `409` contract.
- `WebhookProvider` has no `linear` member and the router has no `source_id`/org mapping
  concept at all.

Repurposing this router would require rewriting nearly all of it, at which point it is not
"repurposing" — it is building Option B under a different name without its preconditions
met.

## Non-goals for v1

- FullChaos-hosted webhook endpoint (Option B).
- Any Linear webhook support.
- Any direct write from a webhook handler (relay or, later, hosted) to metric or sink
  tables — a webhook is always just another **producer** of `external-ingest.v1` batches,
  preserving the Connectors → Processors → Sinks → Metrics boundary the whole epic is
  built on.
- Modifying or repurposing `src/dev_health_ops/api/webhooks/*` — that router keeps serving
  its existing internal-hint purpose, untouched.

## Must-not-foreclose contract (hand off to CHAOS-2712 / 2713 / 2714)

These are the fields and derivation rules that CHAOS-2712's source-registration schema,
CHAOS-2713's CI/CD example tabs, and CHAOS-2714's web screens must accommodate, even though
this evaluation does not implement them.

### 1. `webhookMode` field on the source-registration model (owned by CHAOS-2712)

```json
{
  "sourceId": "uuid",
  "orgId": "uuid",
  "system": "github",
  "instance": "acme/api",
  "mode": "customer_push",
  "enabled": true,
  "webhookMode": "disabled"
}
```

- Enum: `"disabled" | "customer_relay" | "fullchaos_hosted"`. Default `"disabled"`.
- `"fullchaos_hosted"` **must be accepted by the schema** in v1 (so the enum never needs a
  breaking migration later) but **rejected at the API layer** with `400`
  (`"fullchaos_hosted webhook mode is not available yet"`) until Option B ships. Do not let
  the UI or API silently no-op on an unsupported value.
- This is a two-layer contract, and both layers matter — do not collapse it to just one:
  the Pydantic/JSON-Schema **type** for `webhookMode` is the full 3-value enum above (so a
  request body containing `"fullchaos_hosted"` passes schema validation, i.e. no `422`), but
  the **router's business-logic check** rejects that value with `400` before it is persisted
  or acted on. Master-spec CC14's terser phrasing — "API accepts `webhook_mode ∈ {disabled,
  customer_relay}`, 400s on `fullchaos_hosted`" — describes this same two-layer behavior, not
  a narrower one: "accepts" there means "processes as a live, non-rejected setting," not
  "is the only value the field type permits." CHAOS-2712 must implement both layers, not
  just the outer schema type — a schema that merely narrows the enum to two values would
  force a breaking migration/schema change to add `fullchaos_hosted` later, which is exactly
  what reserving the value now is meant to avoid.
- `webhookMode` is independent of `mode` (`fullchaos_sync | customer_push | disabled`) — the
  "who owns sync" axis and the "does this source have a webhook accelerator" axis must not
  be conflated.
- Persistence: `external_ingest_sources.webhook_mode TEXT NOT NULL DEFAULT 'disabled'` is
  already reserved by migration `0032_add_customer_push_ingest_auth.py` (CHAOS-2696) — no
  further migration needed for this field.

### 2. Source-scoped webhook secret reference (owned by CHAOS-2712, needed only for `fullchaos_hosted`)

Not needed for v1 — `customer_relay` mode uses an ordinary ingest token; no provider secret
ever touches FullChaos.

```json
{
  "webhookSecretId": "uuid | null"
}
```

- `external_ingest_sources.webhook_secret_id UUID NULL` is already reserved by migration
  `0032` — no further migration needed to add the column.
- When Option B is eventually built, `webhookSecretId` must reference a **hashed** secret
  row (like `RefreshToken.token_hash` in `src/dev_health_ops/models/refresh_token.py`), not
  a **Fernet-encrypted, reversible** one like `IntegrationCredential.credentials_encrypted`
  in `src/dev_health_ops/models/settings.py`. `IntegrationCredential` exists to let
  FullChaos decrypt and present outbound provider credentials when calling a provider's
  API; a webhook secret is inbound-only — FullChaos verifies an HMAC against it and never
  needs to send it anywhere, so it should never be stored in reversibly-decryptable form.

### 3. Delivery idempotency-key derivation rules (documentation only — consumed by relay authors and CHAOS-2713's relay example tab)

No new server-side idempotency mechanism is introduced. A relay derives the
`idempotencyKey` string it puts in the existing batch envelope as follows:

| Provider | Idempotency key derivation |
|---|---|
| GitHub | `X-GitHub-Delivery` header value, verbatim. Already a UUID. Per [GitHub's webhook delivery-failure documentation](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries), **GitHub does not automatically redeliver failed webhook deliveries** — a dropped delivery is permanently lost unless the source's mandatory reconciliation batch (see "Reconciliation schedule" below) later picks up the same change through a scheduled export. This makes reconciliation load-bearing for GitHub specifically, not just a defense-in-depth nicety. Manual redeliveries triggered via the GitHub UI/API should **not** be assumed to reuse the original `X-GitHub-Delivery` value (unverified either way) — a relay must not rely on delivery-ID stability across a manual redelivery; record-level identity (`externalId` + `updatedAt`, see the note on batch-level vs. record-level idempotency just below this table) is the dedup backstop for that case, independent of whatever value ends up as the batch's `idempotencyKey`. |
| GitLab | GitLab webhook deliveries carry provider-issued delivery-identity headers (per [GitLab webhook events documentation](https://docs.gitlab.com/ee/user/project/integrations/webhooks.html)): `Idempotency-Key` ("unique ID consistent across webhook retries"), `webhook-id` ("unique message ID consistent across webhook retries"), `webhook-timestamp`, `webhook-signature`, `X-Gitlab-Event-UUID`, and `X-Gitlab-Webhook-UUID`. UI-triggered resends reuse the same `Idempotency-Key`, which is exactly the delivery-identity property a relay needs — this is GitLab's equivalent of GitHub's `X-GitHub-Delivery` and should be preferred over any payload-derived formula. Use this **preference ladder**, taking the first header present on the request: <br>1. `Idempotency-Key` header value, verbatim. <br>2. `webhook-id` header value, verbatim. <br>3. `X-Gitlab-Event-UUID` header value, verbatim. <br>4. **Legacy fallback only** — if none of the above headers are present (older self-managed GitLab instances that predate them), fall back to a payload-derived formula, and note that **the available fields differ per event type** — there is no single blanket formula that fits every event. `event_header` below is the `X-Gitlab-Event` header value; `project_id` is `object_attributes.project_id` or top-level `project.id`, whichever the event provides. Per the two v1 first-wave events (see "Provider feasibility"): <br>&nbsp;&nbsp;• **Merge Request Hook** — `object_attributes` documents both `action` and `updated_at`, so derive `sha256(f"{event_header}:{object_kind}:{object_attributes.iid}:{object_attributes.action}:{object_attributes.updated_at}:{project_id}")`. <br>&nbsp;&nbsp;• **Pipeline Hook** — `object_attributes` has no `action`/`updated_at` field; it has `status`, `created_at`, and `finished_at` instead. Derive `sha256(f"{event_header}:{object_kind}:{object_attributes.id}:{object_attributes.status}:{object_attributes.finished_at or object_attributes.created_at}:{project_id}")`. <br>&nbsp;&nbsp;**Do not** reuse either legacy formula verbatim for a GitLab event type not listed here (e.g. Push Hook, Job Hook, when added later) — confirm that event's actual payload fields against current GitLab webhook documentation first, since assuming `action`/`updated_at` exist universally is exactly the mistake this fallback tier corrects. The relay must compute whichever tier applies itself before calling `/batches`. |
| Jira | Derive as `sha256(f"{webhookEvent}:{issue.id}:{timestamp}:{changelog.id or ''}")`. `changelog.id` is present only on `jira:issue_updated`; use an empty string for `jira:issue_created`/`jira:issue_deleted`. |
| Linear | N/A — deferred, no derivation rule needed for v1. |

The batch-level `idempotencyKey` in the core plan's envelope is a single string per
`POST /batches` call, and batch identity is
`org_id + source_system + source_instance + idempotencyKey`. A relay that forwards one
webhook event per HTTP call as a one-record batch uses the derived key above directly as
`idempotencyKey`. A relay that buffers multiple webhook events into one batch (recommended
for volume) must derive a *batch-level* key instead, e.g.
`sha256(":".join(sorted(per_event_keys)))`, and should still include per-event identifiers
in each record's own `externalId`/`updatedAt` fields so record-level idempotency
(`org_id + source_system + source_instance + record_kind + external_id + updated_at/hash`)
still functions independently of how events were batched.

### 4. Webhook status surfaced through the existing batch status model (owned by CHAOS-2712/2714)

No parallel "webhook delivery status" concept is introduced. A relay-forwarded webhook is
just a `POST /batches` call and gets the same `ingestionId`/status lifecycle
(`accepted → processing → completed | partial | failed`, per master-spec CC12) as any other
producer. The only webhook-specific addition is a **producer-type tag** so the status UI
(CHAOS-2714 Screen 6) can filter/label it distinctly:

```json
{
  "source": {
    "producer": "webhook-relay"
  }
}
```

`source.producer` already exists as a free-text string in the core plan's envelope (example
value `"dev-hops-cli"`). This adds `"webhook-relay"` to the *documented* set of expected
values alongside `"dev-hops-cli"`, `"ci-runner"`, `"custom"` — a documentation-only addition,
not a schema change, since the field is unconstrained free text (confirm with CHAOS-2712's
implementer that it stays that way, so new producer types never require a migration). No new
status enum values are needed.

## Reconciliation schedule

Every webhook-enabled source must have a reconciliation schedule; webhook-only ingestion is
not supported for any provider in v1:

- **GitHub**: hourly-or-daily batch reconciliation via `dev-hops push batch`/export,
  cadence depending on customer volume (large pushes exceed the 25 MB webhook payload cap).
  This is not just a size-cap mitigation: GitHub does not automatically redeliver failed
  webhook deliveries (see the idempotency-key table above), so reconciliation is the *only*
  recovery path for a delivery GitHub attempted but the relay never durably received — there
  is no provider-side retry to fall back on.
- **GitLab**: hourly-or-daily batch reconciliation, because push events truncate to the
  newest 20 commits for pushes over that size.
- **Jira**: daily work-item and transition reconciliation (webhooks over 25 MB are dropped
  silently).
- **Linear**: N/A — webhook support deferred entirely for v1.

CHAOS-2713's CI/CD example tabs are the intended home for the concrete cron/scheduling
guidance (see "Follow-up issues" below).

## Relay example sketch

An illustrative, non-production GitHub relay lives at
[`docs/examples/webhook-relay/github_relay.py`](../examples/webhook-relay/github_relay.py),
with usage notes in
[`docs/examples/webhook-relay/README.md`](../examples/webhook-relay/README.md). It verifies
GitHub's HMAC signature, normalizes a `pull_request` event into an `external-ingest.v1`
record, derives the idempotency key from `X-GitHub-Delivery`, and POSTs a one-record batch.
It is intentionally minimal (one event type) — its purpose is to prove the envelope and
idempotency-key shape, not to be a shippable relay. It is not imported by
`dev_health_ops`, has no test suite, and is not part of the Python package.
CHAOS-2713's "Webhook relay" setup-example tab should link to or fork this file rather than
re-deriving its own envelope shape.

## Follow-up issues

To be filed against the CHAOS team, parented under CHAOS-2690, once this ADR is reviewed and
accepted. Not filed as part of writing this ADR — see "Risks" below.

1. **Add webhookMode + webhook secret fields to source-registration schema** — CHAOS-2712's
   source-registration API surface must expose `webhookMode` (enum `disabled |
   customer_relay | fullchaos_hosted`, with `fullchaos_hosted` accepted by the schema but
   400'd at the API layer) and `webhookSecretId` (nullable) per the "Must-not-foreclose"
   contract above. The underlying columns are already reserved in migration `0032`, so this
   is API/schema work only, not a new migration. Blocked by: CHAOS-2712.
2. **[Deferred] FullChaos-hosted webhook endpoint (Option B)** — not scheduled. File as a
   Backlog-priority placeholder so it is not silently forgotten but also not implied to be
   near-term. Description: Option B pros/cons and its precondition list (source-scoped
   secrets, durable delivery diagnostics, replay protection) from this ADR. Priority: Low.
   No target cycle.

Not filed as separate issues (already absorbed by existing scope, confirmed against
CHAOS-2713's brief during this evaluation):

- **Webhook relay setup-example tab** — CHAOS-2713's brief already lists a "Webhook relay"
  tab among its CI/CD example tabs (GitHub Actions, GitLab Runner, Generic Docker, cURL,
  Webhook relay). CHAOS-2713 should link/fork `docs/examples/webhook-relay/github_relay.py`
  produced here rather than re-deriving its own example.
- **Reconciliation scheduling guidance** — folds into CHAOS-2713's CI/CD example tabs
  (cron cadence baked into the GitHub Actions/GitLab Runner templates), not a distinct
  Linear issue, per the "Reconciliation schedule" section above.
- **Idempotency-key derivation docs** — the per-provider table above is the complete
  specification; it is copied into CHAOS-2713's relay example tab content directly rather
  than needing a separate documentation issue, since this ADR is the durable reference.
- **Linear webhook feasibility** — explicitly deferred with no scoped follow-up; revisit
  only if/when Linear webhook support is separately prioritized.

## Risks

- **Plan-doc drift**: the provider-feasibility claims about payload size caps, GitLab's
  20-commit truncation, and Jira's 25 MB webhook cap are carried from the addendum plan doc
  (`docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md`); they were
  not independently re-verified against live GitHub/GitLab/Jira API documentation in this
  pass (no network access to those hosts was exercised here). The code-level claims in this
  ADR (secrets, dispatch, idempotency, provider enum) were independently re-verified against
  the current worktree, per the citations above. Two claims in the idempotency-key table above
  are exceptions, independently verified against live provider documentation during review:
  the GitLab delivery-header preference ladder, confirmed against
  [GitLab's webhook events documentation](https://docs.gitlab.com/ee/user/project/integrations/webhooks.html)
  (an earlier draft of this ADR incorrectly rejected a header-based approach for GitLab as
  unverifiable — corrected once the header names were confirmed against that page); and the
  GitHub no-auto-redelivery claim, confirmed against
  [GitHub's handling-failed-webhook-deliveries documentation](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries)
  (an earlier draft incorrectly asserted GitHub *retries* failed deliveries with the same
  delivery ID — GitHub does not auto-retry at all, which is corrected in the table and makes
  reconciliation load-bearing for GitHub rather than defense-in-depth only).
- **Coordination risk with CHAOS-2712**: if CHAOS-2712 lands its source-registration API
  surface before reading this ADR's `webhookMode`/`webhookSecretId` field spec, it risks
  designing an inconsistent shape even though the underlying DDL columns are already
  reserved. Sequencing recommendation: CHAOS-2712's implementer should read this ADR before
  finalizing the source-registration API contract.

## Consequences

- v1 customer-push ingestion supports webhook acceleration only through a customer-owned
  relay pattern; FullChaos does not operate a hosted webhook endpoint for customer sources.
- The relay pattern requires zero new backend code — it reuses the existing
  `POST /api/v1/external-ingest/batches` contract, an ordinary `ingest:write` token, and the
  existing status/rejection model.
- `external_ingest_sources.webhook_mode`/`webhook_secret_id`, already reserved by migration
  `0032`, are the only schema surface this decision touches; CHAOS-2712 wires them into its
  API without a follow-on migration.
- The existing `/api/v1/webhooks/*` router is unaffected and continues serving its narrower,
  best-effort internal-hint purpose.
- Every webhook-enabled source requires a reconciliation schedule; there is no
  webhook-only ingestion mode for any provider in v1.
