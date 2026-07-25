# Implementation Brief: CHAOS-2715 — Evaluate webhook-assisted customer-push ingestion

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. ADR renumbered: **adr-004** (`docs/architecture/adr-004-webhook-assisted-customer-
>    push.md`). adr-003 is taken by CHAOS-2691's external-ingest boundary ADR (wave 1).
> 2. The must-not-foreclose columns are PRE-RESERVED NOW: CHAOS-2696's migration 0032 adds
>    `webhook_mode TEXT NOT NULL DEFAULT 'disabled'` and `webhook_secret_id UUID NULL` to
>    **`external_ingest_sources`** (canonical table name). API accepts
>    `webhook_mode ∈ {disabled, customer_relay}` and 400s on `fullchaos_hosted` — exactly
>    per §1 of the hand-off contract; no later migration needed.
> 3. Follow-up issues #3 (relay setup-example tab) and #5 (reconciliation scheduling
>    guidance) are ABSORBED by CHAOS-2713's scope (decided now, not deferred to ADR
>    review). File the remaining follow-ups (Option B build-out, Linear webhook support,
>    DLQ replay interplay) only after ADR review, as planned.
> 4. Status vocabulary aligned to CC12: `accepted → processing → completed | partial |
>    failed` (this brief's `processed` → `completed`); webhook-originated batches get the
>    same enum, distinguished only by `source.producer = "webhook-relay"` (free-text
>    producer CONFIRMED — 2691's schema keeps it unconstrained).
> 5. Batch identity/idempotency derivation rules stand, with `source.instance` at
>    repo/project grain (CC5) — a relay for `github.com/acme` fans out per-repo batches
>    (instance = `owner/repo`), matching the ownership model.

Parent epic: CHAOS-2690 External customer-push ingestion API
Issue type: **Evaluation / spike**. The deliverable is a written recommendation (ADR) + an
illustrative relay sketch (non-shipped example) + filed follow-up issues. **No production
code, no new API routes, no new DB tables, no new Celery tasks ship under this issue.**
Any implementation-shaped work this evaluation surfaces (source-scoped webhook secrets,
`webhookMode` field, delivery-idempotency helpers, actual relay package) is explicitly
out of scope here and must become its own follow-up issue(s) created at the end of this
work, per the issue's own acceptance criteria.

This brief is written so a Sonnet coding agent can execute CHAOS-2715 end-to-end without
re-deriving the recommendation — the recommendation is already fully formed in the addendum
plan doc (`docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md`).
The job here is to (a) verify that recommendation against the real codebase, (b) ratify it
as a committed ADR in `docs/architecture/`, (c) produce the relay sketch, and (d) file the
follow-up issues with concrete scope so later sub-issues (2712/2713/2714) don't have to
re-derive the webhook data-model requirements.

---

## Scope

1. **Write ADR**: `ops/docs/architecture/adr-003-webhook-assisted-customer-push-ingestion.md`
   (see "Files to create" for exact numbering check). Content: ratifies Option A (customer
   relay, beta) as the v1 recommendation, defers Option B (FullChaos-hosted), rejects Option C
   (repurposing `/api/v1/webhooks/*`). Must include:
   - Provider-by-provider feasibility table (GitHub / GitLab / Jira / Linear) with recommended
     v1 role, events, and required reconciliation cadence.
   - The "must not foreclose" contract (below) that CHAOS-2712/2713/2714 must design against.
   - Webhook status model mapped onto the existing batch status enum.
   - Explicit non-goals for v1 (FullChaos-hosted endpoint, Linear webhooks, any direct
     metric-table write from a webhook handler).
2. **Relay example sketch**: a single illustrative, non-production Python script showing the
   customer-relay pattern for GitHub (`push` + `pull_request` events → normalize →
   `POST /api/v1/external-ingest/batches`). Lives under `docs/examples/webhook-relay/` (see
   "Files to create"). This is a *documentation artifact*, not a package — no tests, no
   pyproject entry, no import from `src/dev_health_ops/`. It must reuse the exact
   `external-ingest.v1` envelope/record shapes from the core plan doc, not an invented shape.
3. **Follow-up issue specs**: draft (in the ADR's final section, and mirrored in this brief's
   "Follow-up issues to file") the exact titles/descriptions for the issues that must be
   created if webhook support is accepted. Actual `linear-cli issues create` calls happen
   when this evaluation issue is worked for real (out of scope for *this* read-only analysis
   task, and also out of scope for the initial ADR-writing pass — file them as the last step
   of executing CHAOS-2715, only after the ADR is reviewed/accepted, per acceptance criteria
   "Follow-up implementation issues are created if webhook support is accepted").
4. **Verify claims against code**: every factual claim carried over from the addendum plan doc
   (webhook router shape, auth mechanism, idempotency mechanism, best-effort dispatch) must be
   spot-checked against the actual files in this evaluation pass — do not cite the plan doc
   as ground truth without independent confirmation. (This brief has already done this
   verification once; re-verify if the plan doc or code has drifted since 2026-06-28.)

## Out of scope

- Any new FastAPI router, Pydantic schema, Celery task, Alembic/ClickHouse migration, or
  admin endpoint. `webhookMode`, source-scoped webhook secrets, and delivery-idempotency
  storage are **specified** here (so CHAOS-2712 doesn't have to re-derive them) but **built**
  in CHAOS-2712, not CHAOS-2715.
- FullChaos-hosted webhook endpoint (Option B) implementation — explicitly deferred past v1;
  do not scaffold `/api/v1/webhooks/customer/{source_id}/{provider}` in this issue.
- Repurposing or modifying the existing `/api/v1/webhooks/{provider}` router — Option C is
  rejected; do not touch `src/dev_health_ops/api/webhooks/*` under this issue.
- Linear webhook feasibility beyond "defer" — no design work, no event mapping.
- Any change to `dev-hops push` CLI, `/org/admin/integrations` UI, or CI/CD example tabs —
  those are CHAOS-2713/2714's surfaces; this issue only specifies the webhook-relay tab's
  *content requirements*, not its implementation.
- Web/Linear/Postgres/ClickHouse writes of any kind (this is a docs-only deliverable).

## Design decisions

Each decision is already load-bearing in the addendum plan and independently confirmed
against code in this pass; treat as settled unless new evidence contradicts it.

1. **v1 recommendation = Option A (customer-owned relay), marked beta/experimental.**
   Rationale: FullChaos never touches provider webhook secrets or raw payloads unless the
   customer opts in; reuses the exact same `/api/v1/external-ingest/batches` contract as
   CLI/CI producers, so no new ingest code path is needed — only documentation + an ingest
   token with `ingest:write` scope.
2. **Option B (FullChaos-hosted) is deferred, not rejected**, pending source-scoped secrets,
   durable delivery-diagnostics, and replay protection existing first. Rationale: building it
   now means storing per-source provider webhook secrets before the ingest-token/source
   model (CHAOS-2712) even exists — sequencing risk, not a technical dead end.
3. **Option C (repurpose `/api/v1/webhooks/{provider}`) is rejected outright**, not deferred.
   Rationale, confirmed in code (`src/dev_health_ops/api/webhooks/auth.py`,
   `src/dev_health_ops/api/webhooks/router.py`):
   - Secrets are single global env vars (`GITHUB_WEBHOOK_SECRET`, `GITLAB_WEBHOOK_TOKEN`,
     `JIRA_WEBHOOK_SECRET`) — no org/source scoping, so one leaked customer webhook secret
     would let anyone forge events for every org.
   - Dispatch is deliberately best-effort (`_dispatch_webhook_task` swallows any
     `process_webhook_event.delay(...)` exception and still returns 200/202) — acceptable for
     internal low-stakes hints, unacceptable for customer-durability guarantees.
   - Idempotency is a 24h Redis TTL presence check (`workers/system_webhooks.py:_is_duplicate_delivery`)
     with no payload-hash comparison — does not satisfy the plan's same-key/different-hash → 409
     contract.
   - `WebhookProvider` enum has no `linear` member and the router has no concept of
     `source_id`/org mapping at all.
4. **Webhook payloads never write metrics or sink tables directly, in any option.** Rationale:
   preserves the Connectors → Processors → Sinks → Metrics boundary the whole epic is built
   on; a webhook handler (relay or, later, hosted) is just another *producer* of
   `external-ingest.v1` batches, structurally identical to the CLI/CI producer.
5. **Reconciliation is mandatory for every webhook-enabled source, not optional.** Rationale:
   every provider's webhook payloads are lossy in a documented way (GitHub 25MB payload cap;
   GitLab push events cap at newest 20 commits with `total_commits_count` as the only signal
   of truncation; Jira webhooks >25MB are dropped silently; all providers can silently fail
   delivery). A webhook-only source without a reconciliation schedule will silently drift.
   Concretely: GitHub/GitLab hourly-or-daily batch depending on customer volume, Jira daily,
   Linear N/A (deferred).
6. **`webhookMode` is a property of the *source*, not the *token*.** Rationality: a source can
   have zero, one relay, or (later) one hosted webhook feeding it, independent of how many
   ingest tokens exist for that source; modeling it on the token would force a webhook-relay
   token to be re-issued every time the mode changes.
7. **Delivery idempotency for webhook-originated batches reuses the *same* `idempotencyKey`
   field on the existing `POST /batches` envelope — no new idempotency mechanism.** Rationale:
   the relay is just another producer; introducing a second idempotency primitive
   (`X-Idempotency-Key` header vs body field, or a webhook-specific dedup table) would fork the
   idempotency model the core plan already defines (`org_id + source_system + source_instance +
   idempotencyKey`). The relay's job is to *derive* a stable `idempotencyKey` from the
   provider's own delivery identifier (see per-provider derivation rules below) and put it in
   the batch envelope like any other producer.
8. **This evaluation does not select a canonical relay implementation language/runtime.**
   Rationale: out of scope per the issue (a written recommendation, not code); the sketch is
   illustrative Python only because `dev-hops` and the rest of the CI examples are Python-first,
   not because Python is mandated for customer relays.
9. **ADR lands in `ops/docs/architecture/`, not `docs/superpowers/plans/`.** Rationale: user's
   own house rule ("document architecture decisions in ops/docs/architecture in the same
   changeset") and the repo's existing convention — `docs/superpowers/plans/*.md` are
   pre-implementation planning documents (this epic's own plan docs live there), while
   `docs/architecture/adr-*.md` is the durable, indexed decision record
   (see `docs/architecture/adr-001-canonical-provider-pattern.md`,
   `docs/architecture/adr-002-investment-period-components.md`). CHAOS-2715's output is a
   ratified decision about a durable boundary (webhooks vs REST-push), which is exactly what
   the ADR series is for.

## "Must not foreclose" contract (hand off to CHAOS-2712 / 2713 / 2714)

This is the concrete, load-bearing output of the evaluation — the fields/keys that
CHAOS-2712's source-registration schema, CHAOS-2713's CI/CD example tabs, and CHAOS-2714's
web screens must accommodate even though CHAOS-2715 does not implement them.

### 1. `webhookMode` field on the source-registration model (owned by CHAOS-2712)

Extend the source-registration JSON shape from the core plan doc:

```json
{
  "sourceId": "uuid",
  "orgId": "uuid",
  "system": "github",
  "instance": "github.com/acme",
  "mode": "customer_push",
  "enabled": true,
  "webhookMode": "disabled"
}
```

- `webhookMode` enum: `"disabled" | "customer_relay" | "fullchaos_hosted"`.
- Default: `"disabled"`.
- `"fullchaos_hosted"` value must be **accepted by the schema in v1** (so the enum doesn't need
  a breaking migration later) but **rejected at the API layer** with a 400
  (`"fullchaos_hosted webhook mode is not available yet"`) until Option B ships — do not let the
  UI or API silently no-op on an unsupported value.
- `webhookMode` is independent of `mode` (`fullchaos_sync | customer_push | disabled`); do not
  conflate the "who owns sync" axis with the "does this source have a webhook accelerator" axis.
- Persistence: this belongs on whatever table CHAOS-2712 creates for source registration
  (not designed yet as of this evaluation — confirmed no such table exists today). Add
  `webhook_mode TEXT NOT NULL DEFAULT 'disabled'` to that table's DDL when CHAOS-2712 authors it.

### 2. Source-scoped webhook secret reference (owned by CHAOS-2712, only needed for `fullchaos_hosted`)

Not needed for v1 (`customer_relay` mode uses an ordinary ingest token, no provider secret
touches FullChaos at all). Reserve the field so `fullchaos_hosted` doesn't need a schema
migration later:

```json
{
  "webhookSecretId": "uuid | null"
}
```

- `webhookSecretId` is nullable, references a **hashed-or-encrypted secret row** — NOT
  `IntegrationCredential` (that model is for outbound, reversibly-encrypted provider
  credentials FullChaos decrypts to call the provider's API; a webhook secret is inbound,
  used only to verify an HMAC/token FullChaos never needs to send anywhere, so it should be
  **hashed** like `RefreshToken.token_hash`, not Fernet-encrypted).
- Do not build this table/column under CHAOS-2715. Document the requirement in the ADR so
  CHAOS-2712 adds it in the same migration as the source-registration table, rather than a
  second migration later.

### 3. Delivery idempotency-key derivation rules (documentation only, consumed by relay authors and CHAOS-2713's relay example tab)

No new server-side idempotency mechanism (decision #7). What CHAOS-2715 must specify precisely
is how a relay derives the `idempotencyKey` string it puts in the existing batch envelope:

| Provider | Idempotency key derivation |
|---|---|
| GitHub | `X-GitHub-Delivery` header value, verbatim (already a UUID; globally unique per delivery attempt from GitHub's side — note GitHub *retries* re-send the same delivery ID, so this is correct as an idempotency key, not per-retry-unique) |
| GitLab | No universal delivery GUID across all GitLab webhook payload types. Derive as `sha256(f"{event_header}:{object_kind}:{object_id}:{action}:{updated_at}:{project_id}")` where `event_header` is the `X-Gitlab-Event` header value. Relay must compute this itself before calling `/batches`. |
| Jira | Derive as `sha256(f"{webhookEvent}:{issue.id}:{timestamp}:{changelog.id or ''}")`. `changelog.id` is only present on `jira:issue_updated`; use empty string for `jira:issue_created`/`jira:issue_deleted`. |
| Linear | N/A — deferred, no derivation rule needed for v1. |

Note: the batch-level `idempotencyKey` in the core plan's envelope is a single string per
`POST /batches` call, and the plan's batch identity is
`org_id + source_system + source_instance + idempotencyKey`. A relay that receives one webhook
event per HTTP call and forwards it as a one-record batch should use the derived key above
directly as `idempotencyKey`. A relay that buffers multiple webhook events into one batch
(recommended for volume) must derive a *batch-level* key, e.g.
`sha256(":".join(sorted(per_event_keys)))`, and should still include per-event identifiers in
each record's own `externalId`/`updatedAt` fields so record-level idempotency
(`org_id + source_system + source_instance + record_kind + external_id + updated_at/hash`,
per the core plan) still functions independently of how events were batched.

### 4. Webhook status surfaced through the existing batch status model (owned by CHAOS-2712/2714)

Do not introduce a parallel "webhook delivery status" concept. A relay-forwarded webhook is
just a `POST /batches` call and gets the same `ingestionId`/status lifecycle
(`accepted → processing → partial|failed|processed`) as any other producer. The only
webhook-specific status addition needed is a **producer-type tag** so the status UI
(CHAOS-2714 Screen 6) can filter/label it distinctly:

```json
{
  "source": {
    "producer": "webhook-relay"
  }
}
```

- `source.producer` already exists in the core plan's envelope (example value `"dev-hops-cli"`).
  Extend the *documented* enum of expected values to include `"webhook-relay"` alongside
  `"dev-hops-cli"`, `"ci-runner"`, `"custom"` — this is a documentation-only addition (the field
  is already a free-text string in the schema), not a schema change. Confirm with CHAOS-2712's
  implementer that `source.producer` remains unconstrained/free-text (recommended) rather than
  a closed enum, so new producer types never require a schema migration.
- No new status enum values are needed; `partial`/`failed`/`processed`/etc. from the core plan
  already cover webhook-originated batches.

## API / DDL / schema sketches

No new API routes or DDL ship under CHAOS-2715. The sketches below are the **complete content
to paste into the ADR**, so a later reader (or CHAOS-2712's implementer) has the exact shapes
without re-deriving them.

### ADR frontmatter/skeleton

```markdown
# ADR-003: Webhook-Assisted Customer-Push Ingestion

## Status

Accepted (v1: customer relay only, beta).

## Context

<pull from "Design decisions" #1-#4 above>

## Decision

v1 ships **Option A only**: customer-owned webhook relay, beta/experimental, using the
existing `/api/v1/external-ingest/batches` contract as-is. No FullChaos-hosted webhook
endpoint, no repurposing of `/api/v1/webhooks/*`.

## Provider feasibility

<table from plan-webhooks.md Provider feasibility section, re-verified>

## Must-not-foreclose contract

<copy "Must not foreclose" section above verbatim>

## Rejected/deferred alternatives

<Option B and Option C rationale from "Design decisions" #2, #3>

## Follow-up issues

<list from "Follow-up issues to file" below>
```

### Relay example sketch (illustrative only — goes in `docs/examples/webhook-relay/github_relay.py`)

```python
"""Illustrative customer-relay example for GitHub webhooks -> FullChaos external-ingest.

NOT production code. NOT imported by dev_health_ops. Demonstrates the minimum shape
a customer-owned relay needs: verify GitHub's signature, normalize a subset of events
into external-ingest.v1 records, derive a stable idempotency key, and POST a batch.

Run: FULLCHAOS_INGEST_TOKEN=... FULLCHAOS_API_URL=... GITHUB_WEBHOOK_SECRET=... \
     uvicorn github_relay:app --port 8080
"""
import hashlib
import hmac
import os
import time
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request

app = FastAPI()

GITHUB_WEBHOOK_SECRET = os.environ["GITHUB_WEBHOOK_SECRET"]
FULLCHAOS_API_URL = os.environ["FULLCHAOS_API_URL"]
FULLCHAOS_INGEST_TOKEN = os.environ["FULLCHAOS_INGEST_TOKEN"]
FULLCHAOS_ORG_ID = os.environ["FULLCHAOS_ORG_ID"]


def _verify_signature(raw_body: bytes, signature_header: str | None) -> None:
    if not signature_header or not signature_header.startswith("sha256="):
        raise HTTPException(status_code=401, detail="missing signature")
    expected = hmac.new(GITHUB_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(f"sha256={expected}", signature_header):
        raise HTTPException(status_code=401, detail="bad signature")


def _normalize_pull_request(payload: dict[str, Any]) -> dict[str, Any]:
    pr = payload["pull_request"]
    return {
        "kind": "pull_request.v1",
        "externalId": str(pr["id"]),
        "updatedAt": pr["updated_at"],
        "repository": payload["repository"]["full_name"],
        "number": pr["number"],
        "title": pr["title"],
        "state": pr["state"],
        "authorExternalId": str(pr["user"]["id"]),
        "createdAt": pr["created_at"],
        "mergedAt": pr.get("merged_at"),
        "closedAt": pr.get("closed_at"),
    }


@app.post("/github/webhook")
async def github_webhook(
    request: Request,
    x_hub_signature_256: str | None = Header(default=None),
    x_github_delivery: str | None = Header(default=None),
    x_github_event: str | None = Header(default=None),
) -> dict[str, str]:
    raw_body = await request.body()
    _verify_signature(raw_body, x_hub_signature_256)
    if not x_github_delivery:
        raise HTTPException(status_code=400, detail="missing X-GitHub-Delivery")

    payload = await request.json()
    if x_github_event != "pull_request":
        # Illustrative relay only handles one event type; a real relay would
        # dispatch on x_github_event and normalize each supported kind.
        return {"status": "ignored", "event": x_github_event or "unknown"}

    record = _normalize_pull_request(payload)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    batch = {
        "schemaVersion": "external-ingest.v1",
        "idempotencyKey": x_github_delivery,  # GitHub delivery IDs are stable across retries
        "source": {
            "type": "customer_push",
            "system": "github",
            "instance": payload["repository"]["full_name"].split("/")[0] + ".github.com",
            "producer": "webhook-relay",
            "producerVersion": "0.1.0",
        },
        "window": {"startedAt": now, "endedAt": now},
        "records": [record],
    }

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{FULLCHAOS_API_URL}/api/v1/external-ingest/batches",
            headers={"Authorization": f"Bearer {FULLCHAOS_INGEST_TOKEN}"},
            json=batch,
        )
    # A production relay must retry on 503/429 with backoff and persist a local
    # dead-letter queue for anything that keeps failing -- omitted here for brevity.
    resp.raise_for_status()
    return {"status": "forwarded", "ingestionId": resp.json()["ingestionId"]}
```

Keep this sketch intentionally minimal (one event type) — its purpose is to prove the
envelope/idempotency-key shape, not to be a shippable relay. CHAOS-2713's "Webhook relay"
setup-example tab should link to (or fork) this file, not re-derive its own envelope shape.

## Files to create/modify

All under `/Users/chris/projects/full-chaos/dev-health/ops` (worktree
`chaos-2690-integration` or whichever branch actually executes CHAOS-2715 — confirm the
current ADR number isn't taken by checking `ls docs/architecture/adr*.md` at execution time,
since 2690's other sub-issues may land ADR-003 first; renumber if so):

- `ops/docs/architecture/adr-003-webhook-assisted-customer-push-ingestion.md` — new, the ADR
  (see skeleton above).
- `ops/docs/examples/webhook-relay/github_relay.py` — new, illustrative relay sketch.
- `ops/docs/examples/webhook-relay/README.md` — new, 1-2 paragraphs: what this is (illustrative
  only), how to run it, explicit "NOT production code, NOT covered by CI" disclaimer, link back
  to the ADR.
- `ops/docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md` — do NOT
  edit; it's the input to this evaluation, keep it as historical planning record. The ADR is
  the new durable artifact; don't fork the source of truth across two docs long-term, but don't
  delete/rewrite the plan doc either (out of scope, and other sub-issues reference it by path).
- No changes to `src/dev_health_ops/**`, `tests/**`, `web/**`, no Alembic revision, no
  ClickHouse migration script. If a coding agent executing this issue finds itself about to
  add any of those, it has scope-crept past what CHAOS-2715 is — stop and file a follow-up
  issue instead (see below).

## Follow-up issues to file (only after ADR is reviewed/accepted)

File these against CHAOS team, parented under CHAOS-2690, once the ADR lands. Do not create
them speculatively before the ADR is written — their scope depends on the ADR being the
committed reference.

1. **"Add webhookMode + webhook secret fields to source-registration schema"** — depends on
   CHAOS-2712 (source registration/token model) landing its base table first; adds
   `webhook_mode` column (default `'disabled'`) and nullable `webhook_secret_id` reference in
   the same migration if CHAOS-2712 hasn't shipped yet, or as a follow-on migration if it has.
   Blocked by: CHAOS-2712.
2. **"Document + validate idempotency-key derivation for webhook relays"** — adds the
   per-provider derivation table from this brief into customer-facing docs (likely
   `docs/customer-push-ingestion-setup-design.md` webhook relay tab, owned by web repo, or an
   ops-side `docs/` customer guide), plus a small conformance test the relay sketch itself can
   run against `POST /validate`. Blocked by: CHAOS-2690 Phase 1 (validate endpoint must exist).
3. **"Webhook relay setup-example tab (CHAOS-2713 scope)"** — cross-reference only; confirm
   CHAOS-2713's existing scope already covers this (it does, per the addendum plan's "Screen 4:
   Setup examples" tab list), so this may not need a *new* issue — note in the ADR that
   CHAOS-2713 should link/fork `docs/examples/webhook-relay/github_relay.py` rather than
   re-deriving its own example.
4. **"[Deferred] FullChaos-hosted webhook endpoint (Option B)"** — explicitly NOT scheduled;
   file as a Backlog-priority placeholder issue only, description = the Option B pros/cons and
   its precondition list (source-scoped secrets, durable delivery diagnostics, replay
   protection) from the ADR, so it isn't silently forgotten but also isn't implied to be
   near-term. Priority: Low. No target cycle.
5. **"Reconciliation scheduling guidance for webhook-enabled sources"** — cross-reference:
   confirm whether `dev-hops push export {github,gitlab,jira}` (CHAOS-2690 Phase 3/CLI scope)
   already covers the reconciliation batch job, or whether a distinct scheduling primitive is
   needed (e.g., a recommended cron cadence baked into the CI/CD example templates). Likely
   folds into CHAOS-2713 rather than a new issue — decide during ADR review, only file if
   distinct work is identified.

## Test plan

This issue produces no executable production code, so there is no unit/integration test
suite to add under `tests/`. The only "test" surface:

- **Manual smoke-check of the relay sketch** (not CI-gated): run the sketch locally against a
  disposable/scratch ClickHouse+Postgres dev stack once CHAOS-2690 Phase 1 (`POST /batches`,
  `POST /validate`) is live, using a real or synthetic GitHub webhook payload
  (`gh api repos/{owner}/{repo}/hooks` deliveries, or a captured sample payload), confirm:
  - signature verification rejects a tampered payload (401),
  - a valid payload round-trips to `202 Accepted` with an `ingestionId`,
  - re-sending the identical webhook delivery (same `X-GitHub-Delivery`) is idempotent (same
    `ingestionId`/status returned, no duplicate `pull_request` record created).
  This smoke check is **only runnable after CHAOS-2690 Phase 1/2 ship** — if this evaluation
  issue is worked before those land, downgrade to a `httpx`-mocked dry run of the envelope
  shape only (assert the JSON matches the core plan's schema, no live call).
- **Doc review, not test automation**: the ADR's provider-feasibility table and
  "must-not-foreclose" contract are reviewed by a human/second agent (Codex per house rule) for
  internal consistency with the core + addendum plan docs, not exercised by pytest.
- No `@pytest.mark.clickhouse` tests are needed or appropriate for this issue.

## Gate commands

No code changes ship, so the usual ops/web CI gates (`bash ci/local_validate.sh`,
`mypy --install-types --non-interactive .`, `ci/run_tests.sh format/quality/unit`, Playwright
e2e) are **not applicable** to this issue's diff (docs + one non-imported example script under
`docs/examples/`). If the execution agent's diff touches anything under `src/`, `tests/`, or
`web/src`, that is a signal of scope creep — stop and re-scope rather than running the full
gate suite to "make it pass."

Recommended lightweight check only:
- `python -m py_compile docs/examples/webhook-relay/github_relay.py` — confirms the sketch is
  at least syntactically valid Python 3, since it's meant to be copy-pasteable, without pulling
  it into the pytest suite or pyproject dependency graph (it uses `httpx`/`fastapi`, both
  already pyproject deps, but should NOT gain its own `pyproject.toml`/package — it's a doc
  example, not a distributable relay).
- Markdown link check (manual): confirm the ADR's cross-references to
  `docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md`,
  `docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md`, and
  `docs/architecture/adr-001-canonical-provider-pattern.md` (style precedent) resolve.

## Live verification procedure

Not applicable in the traditional sense (no server-side change to verify against a running
compose stack). The closest live verification is the smoke-check described in "Test plan":
once CHAOS-2690 Phase 1/2 are merged and the dev compose stack is up, run the relay sketch
against a real `POST /api/v1/external-ingest/batches` call using an ingest token minted via
whatever token-issuance path CHAOS-2712 ships (CLI or admin API), and confirm via
`GET /api/v1/external-ingest/batches/{ingestion_id}` that status transitions as expected. Do
not attempt this before Phase 1/2 exist — there is nothing to call yet.

## Dependencies on other sub-issues

- **Informational input, not a hard blocker**: this evaluation can be executed independently
  by reading the plan docs and code (as this brief did) — it does not require CHAOS-2712/2713/
  2714 to be merged first.
- **Output feeds forward into**: CHAOS-2712 (must add `webhook_mode`/`webhook_secret_id` to
  whatever source-registration DDL it authors), CHAOS-2713 (webhook-relay setup-example tab
  should fork/link the sketch produced here, not re-derive it), CHAOS-2714 (web UI's
  "webhook-assisted badge" on Screen 1 and producer-type filter on Screen 6 should reflect the
  `webhookMode` enum and `source.producer: "webhook-relay"` value defined here).
- **Sequencing recommendation**: do CHAOS-2715 early (it's cheap, doc-only) so CHAOS-2712's
  author has the `webhookMode`/`webhook_secret_id` field spec in hand before writing the
  source-registration migration, avoiding a second migration later.

## Risks

- **Scope creep into implementation.** The issue's own acceptance criteria and this brief both
  say "written recommendation + follow-up issues, not code" — but the FOCUS also asks for a
  relay "example sketch," which is a code artifact. Risk: an executing agent treats the sketch
  as a starting point and keeps building it into a real package (adding tests, a pyproject
  entry, CI coverage). Mitigate by keeping it a single illustrative file with an explicit
  disclaimer and no test suite, per "Files to create."
- **ADR numbering collision.** Only `adr-001` and `adr-002` exist as of this analysis
  (2026-07-01); another in-flight CHAOS-2690 sub-issue could claim `adr-003` first. The
  executing agent must re-check `ls docs/architecture/adr-*.md` at execution time and
  renumber if needed, rather than assuming `003` is free.
- **Plan-doc drift.** This brief's provider-feasibility claims (GitHub delivery ID stability,
  GitLab 20-commit truncation, Jira 25MB webhook cap) are carried from the addendum plan doc
  and were not independently re-verified against live GitHub/GitLab/Jira API docs in this pass
  (network access to those docs' hosts was not exercised) — flag in the ADR as "carried from
  addendum plan doc, provider docs not re-fetched," and re-verify against
  `develop.sentry.dev`-style official provider docs if the ADR reviewer wants a stronger
  citation than the plan doc itself.
- **Coordination risk with CHAOS-2712.** If CHAOS-2712 is implemented before this evaluation's
  `webhookMode`/`webhook_secret_id` fields are specified and handed off, its source-registration
  migration will ship without them, forcing a second migration. The "Dependencies" section
  above recommends sequencing to avoid this, but there's no hard technical blocker enforcing
  the order — this is a coordination risk, not a code risk.
- **Follow-up issues filed too early or with wrong scope.** The issue's acceptance criteria
  say follow-ups are created "if webhook support is accepted" — i.e., after ADR review, not
  as part of writing the ADR. An executing agent that files them immediately risks creating
  Linear noise for a recommendation that hasn't been reviewed yet.
