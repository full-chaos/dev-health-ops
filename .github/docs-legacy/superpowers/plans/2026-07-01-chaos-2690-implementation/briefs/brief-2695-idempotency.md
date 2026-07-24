# CHAOS-2695 Implementation Brief: Idempotency + Source-Ownership Policy

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. **Zero migrations in this issue.** The DB primitives land elsewhere: sources table =
>    CHAOS-2696's 0032 (canonical name **`external_ingest_sources`** — matches this brief);
>    `payload_hash`, unique idempotency index, `attempts`, and the `stream_unavailable`
>    status value = CHAOS-2694's 0033. §6.3's migration sketch is VOID.
> 2. **RETRYABLE_STATUSES = {"stream_unavailable", "failed"}** (2697 D11 amendment
>    RATIFIED): same key + same hash + status `failed` → fresh accept, SAME ingestion_id,
>    `attempts += 1`, status reset to `accepted`, re-enqueue. Different hash → still 409.
>    **POST-CRITIQUE (CC13): + stale-accepted recovery** — same key+hash against a batch
>    in `accepted`/`processing` whose `updated_at` is older than
>    `EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES=15` → treat as RETRY (attempts+=1, payload
>    row refreshed, re-enqueue SAME ingestion_id); younger → REPLAY. Closes the
>    crash-before-XADD / stream-trim fail-open where `accepted` was unrecoverable.
> 3. Error shape/codes: use CHAOS-2691's `ExternalIngestError` envelope
>    (`{"error": {code, message}}`) — NOT `HTTPException(detail={...})`. Renames:
>    `idempotency_key_conflict` → **`idempotency_conflict`**;
>    `ingest_stream_unavailable` → **`stream_unavailable`**. Keep
>    `source_not_registered` / `source_disabled` / `source_owned_by_fullchaos_sync` (403).
> 4. D8 (REPLAY → 200 OK with full status envelope) is RATIFIED. D10's instance GRAIN
>    (repo/project) is RATIFIED, but **its exact-match-vs-external_id mechanism is
>    OVERRULED post-critique (master-spec CC5)**: `integration_sources.external_id` is
>    `owner/repo` only for GitHub — GitLab stores the NUMERIC project_id (path lives in
>    `full_name` / `metadata_.path_with_namespace`; sync/discovery.py:123-133), Jira
>    stores `sync_options.project_id|project_key`-or-config-name, Linear stores a team
>    UUID or the literal `"linear"` org-wide placeholder that owns ALL teams
>    (api/admin/routers/sync.py:775-820). `ownership.py` must implement CC5's
>    per-provider matching (org- AND provider-scoped): github external_id|full_name;
>    gitlab full_name|metadata path|external_id; jira external_id|full_name; linear
>    external_id|full_name|name + org-wide placeholder rule; custom never conflicts.
>    Registration stores the match as
>    `external_ingest_sources.matched_integration_source_id` (0032); accept-time =
>    indexed is_enabled check on the stored id + the exact indexed matches + linear
>    org-wide check (catches later-created managed sources). The body's "confirmed live
>    at sync/discovery.py:249" claim was a mis-verification (that line is the generic
>    INSERT, not evidence the match works). Canonical instance strings unchanged:
>    github/gitlab = repo full name (`acme/api`), jira = project key, linear = team key,
>    custom = stable slug. `source.instance` == `repository.v1.externalId` for git
>    systems.
> 5. Scope += **final router-flow ownership** (CC22): this issue (wave 4) rewires
>    `POST /batches` to the full sequence (ownership → idempotency-first-write → payload
>    row via 2693's `payload_store` → commit → pointer enqueue → RETRY handling), and
>    factors an `accept_batch_core()` reused by the admin proxy it ships:
>    `POST /api/v1/admin/customer-push/sources/{id}/validate`.
>    **POST-CRITIQUE (CC25, product decision): the console-push proxy
>    (`POST .../sources/{id}/batches`, producer="web-console") is CUT from v1** — moved
>    to the v2 follow-up list; web Screen 5 is validate-only.
>    **POST-CRITIQUE (CC22): the payload write is `payload_store.upsert_payload()`** —
>    SELECT-by-PK-then-UPDATE-or-INSERT in the same accept txn (no ON CONFLICT); the
>    idempotency row's unique index is the serialization point; the true concurrent
>    same-key race → `503 ingest_temporarily_unavailable` (now in CC16's vocabulary;
>    CLI retries all 503s).
> 6. §5's "provider value" open point is settled: provider = source system, provenance via
>    ClickHouse `source_id` column (CC8, owned by 2698). Repo UUID seed = provider full
>    name (`owner/repo`), NOT a URL (CC4).

Epic: CHAOS-2690 External customer-push ingestion API
Sub-issue: CHAOS-2695 "Idempotency and source ownership policy"
Repo: `dev-health-ops`, worktree `chaos-2690-integration` (branch `chaos-2690-external-ingest`)

This brief is self-contained: a coding agent should be able to implement CHAOS-2695
end-to-end from this document without re-deriving design decisions. Where this issue's
code depends on tables/endpoints owned by sibling sub-issues (CHAOS-2691/2693/2694/2696/
2697/2698/2712), the exact interface contract this issue requires from them is spelled
out explicitly.

---

## 1. Scope

CHAOS-2695 owns:

1. **Batch idempotency policy**: identity key, payload canonicalization, hash
   comparison, and the exact NEW / REPLAY / CONFLICT / RETRY decision logic for
   `POST /api/v1/external-ingest/batches`.
2. **Source-ownership policy**: the "one active mode per (org, system, instance)"
   rule — `fullchaos_sync | customer_push | disabled | unclaimed` — and the shared
   resolver function used at *both* enforcement points (source registration,
   CHAOS-2696; and batch accept, CHAOS-2691).
3. **Record-level identity/versioning contract**: how `org_id + source_system +
   source_instance + record_kind + external_id` maps onto each ClickHouse sink's
   existing ReplacingMergeTree `ORDER BY` + version-column dedup, so CHAOS-2697/2698
   don't have to invent per-record dedup logic — they inherit it for free by
   following the rules in §5.
4. **Exact error codes/messages** for every idempotency and ownership failure mode
   (§7), to be raised by CHAOS-2691's router and CHAOS-2696's registration endpoint.
5. New modules: `src/dev_health_ops/external_ingest/idempotency.py` and
   `src/dev_health_ops/external_ingest/ownership.py`, plus the SQLAlchemy models and
   (coordinated) Alembic migration those modules need.

Out of scope (owned by sibling issues, referenced here only as an interface
contract):

- The REST router itself, request/response Pydantic schemas, OpenAPI wiring
  (CHAOS-2691).
- The Valkey/Redis stream writer, DLQ, `503`-on-unavailable behavior itself
  (CHAOS-2693) — this issue only defines *when* the router must call the stream
  writer and what to do with the batch row if it fails.
- The `external_ingest_batches` / `external_ingest_rejections` status-store CRUD
  surface and `GET /batches/{id}` handler (CHAOS-2694) — this issue defines the
  columns *it* needs on `external_ingest_batches` (`payload_hash`) and the exact
  status-transition semantics around idempotent replay, but CHAOS-2694 owns the
  general read/update API for the table.
- The `external_ingest_sources` registration CRUD endpoints and admin UI wiring
  (CHAOS-2696) — this issue defines the table shape and the ownership-conflict
  resolver function CHAOS-2696 must call before allowing a `customer_push`
  registration.
- Token issuance, scopes, hashing, rotation, revocation, audit logging
  (CHAOS-2712) — this issue only requires that whatever auth dependency CHAOS-2712
  builds hands the router a resolved `org_id` (and, if scoped, a specific
  `source_id`) *before* the router calls into this issue's idempotency/ownership
  code.
- Worker normalization/sink-writing (CHAOS-2697/2698) beyond the versioning
  contract in §5.
- Bounded recompute (CHAOS-2699), CLI (CHAOS-2700), webhooks (CHAOS-2715), web UI
  (CHAOS-2714).

---

## 2. Out of scope (explicit non-goals for this issue)

- No TTL/expiry on idempotency keys. Unlike the legacy `/api/v1/ingest`
  (`check_idempotency`, 24h Redis `SET NX EX 86400`) and the webhook delivery
  cache (`workers/system_webhooks.py`, 24h TTL), CHAOS-2690 requires durable
  dedup ("Reprocessing must be safe"), so idempotency keys are unique **forever**
  per `(org_id, source_system, source_instance)`. This is a deliberate deviation
  from both existing precedents — do not "fix" it back to a TTL cache.
- No cross-source or cross-org idempotency-key namespacing beyond
  `(org_id, source_system, source_instance, idempotency_key)`. A customer reusing
  the same string key across two different `source.instance` values is a
  different logical batch and is NOT a conflict.
- No automatic reconciliation/merge of `fullchaos_sync` and `customer_push` data
  for the same instance. Mixed ownership is always rejected (`403`), never
  auto-resolved. (Epic non-goal, restated here because this issue is the
  enforcement point.)
- No changes to `src/dev_health_ops/api/admin/routers/sync.py` or the existing
  `Integration`/`IntegrationSource` provisioning flow. Ownership detection reads
  those tables; it does not write to them (see §6 "why derive, don't migrate").

---

## 3. Design decisions

Each decision is opinionated and final for this issue; only the ones flagged
**[CROSS-CUTTING]** need epic-owner confirmation because they constrain
CHAOS-2696/2714's UI copy or CHAOS-2691's schema.

1. **Payload hash = SHA-256 over the canonicalized, *validated* envelope, not the
   raw request bytes.** Compute the hash from the Pydantic-validated model
   (`envelope.model_dump(mode="json")`) after CHAOS-2691's schema validation has
   already run, not from raw JSON text. Rationale: canonicalization must be
   immune to field order, insignificant whitespace, numeric formatting
   (`1` vs `1.0`), and timestamp representation (`...Z` vs `...+00:00` — verified
   both normalize to the same string under Pydantic v2's `mode="json"` dump).
   Hashing post-validation gets all of this for free instead of hand-rolling a
   JSON canonicalizer.
2. **Canonical serialization = `json.dumps(model_dump, sort_keys=True,
   separators=(",", ":"), ensure_ascii=True)`, encoded UTF-8, then
   `hashlib.sha256(...).hexdigest()`.** `sort_keys=True` handles field-order
   invariance (recursively, including inside each record's `data` object);
   `separators=(",", ":")` removes whitespace variance. This is the entire
   canonicalization algorithm — no custom deep-sort code needed because
   `sort_keys` in the stdlib `json` module is already recursive.
3. **The `records` array is treated as position-significant — it is NOT sorted
   before hashing.** Rationale: records are heterogeneous (9 different kinds),
   sorting would require inventing a synthetic cross-kind comparison key, and
   `dev-hops push export`/CLI-generated payloads are already deterministically
   ordered, so reordering-tolerance has no real customer benefit for the
   complexity it adds. A customer who regenerates an export non-deterministically
   and gets a spurious `409` should fix their export determinism, not rely on the
   platform to paper over it.
4. **The full envelope (including `idempotencyKey` and `source`) is hashed, not
   just `records`.** Simpler and harmless: for a hash comparison to be reached at
   all, `idempotencyKey`/`org_id`/`source.system`/`source.instance` already match
   by construction (they're the lookup key), so including them in the hash changes
   nothing except defending against a hypothetical future bug where the lookup key
   comparison is loosened.
5. **Atomic insert-or-detect via ORM `INSERT` + `IntegrityError`, not
   `postgresql.dialects.insert(...).on_conflict_do_nothing()`.** Rationale: the
   codebase's Postgres-model unit tests run against `sqlite:///:memory:` (see
   `tests/test_batch_storage.py`, `tests/test_backfill_fanout.py`, etc. — no
   `@pytest.mark.clickhouse`-style Postgres marker exists), and
   `sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_nothing()` compiles
   to Postgres-only SQL that cannot run against SQLite. `session.add()` +
   `session.flush()` + `except IntegrityError` is dialect-portable, testable
   without a live Postgres, and is the standard SQLAlchemy insert-or-select race
   pattern. Only downside (one extra round trip on the conflict path) is
   irrelevant at ingest QPS.
6. **`resolve_batch_idempotency(...)` must be the FIRST database write in the
   request handler**, before any other write in the same session, because its
   internal conflict handling calls `session.rollback()` (SQLite/SQLAlchemy
   rollback discards the *whole* session's uncommitted state, not just the
   conflicting insert). Document this loudly in the docstring and call it out in
   the CHAOS-2691 router's implementation notes.
7. **Idempotency outcomes are a 4-way enum: `NEW`, `REPLAY`, `CONFLICT`, `RETRY`**
   — not the 3-way (`same key+same hash → existing status`, `same key+different
   hash → 409`) the plan doc literally states. The 4th outcome, `RETRY`, exists
   because the plan's own `503`-on-stream-unavailable requirement creates a state
   the plan doesn't address: a batch row can be durably persisted with
   `status="stream_unavailable"` (stream write failed after the Postgres insert
   committed) and the client is expected to retry with the *same* idempotency key.
   Naively treating that retry as `REPLAY` would return the stale
   `stream_unavailable` status forever without ever re-attempting the enqueue.
   `RETRY` = same key + same hash + existing row's `status` is in
   `RETRYABLE_STATUSES = {"stream_unavailable", "failed"}` (+ stale accepted/processing >15 min, header item 2) → the router re-attempts the
   stream enqueue using the *existing* `ingestion_id`, rather than creating a new
   batch row or returning a stale terminal-looking status. See §4 for the router
   integration contract.
8. **`REPLAY` returns `200 OK` (not `202`), with the full current-status envelope**
   (same shape as `GET /batches/{id}`: `ingestionId, status, itemsReceived,
   itemsAccepted, itemsRejected, source, window`), not the narrower 202-accepted
   shape from the plan's example. **[CROSS-CUTTING — confirm with CHAOS-2691/2700
   owners]** Rationale: a replayed batch may already be `completed`/`partial`, and
   returning only `{ingestionId, status:"accepted", itemsReceived, stream}` would
   be actively misleading (claims "accepted" when it may be long done). `200` vs
   `202` also gives `dev-hops push batch --poll` (CHAOS-2700) a way to short-circuit
   polling in one round trip. If the epic owner insists on the plan's literal
   `202` shape for all non-conflict outcomes, that's a one-line change in the
   router — flag, don't silently deviate without noting it in the PR description.
9. **Record-level identity is *not* a separate dedup table.** It is entirely
   delegated to each target ClickHouse table's existing `ReplacingMergeTree`
   `ORDER BY` + version-column convention (§5). The plan's phrase "external_id +
   updated_at/hash" is resolved as: use the **ingest worker's processing
   timestamp** as the version column (matching `WorkItem`/`WorkItemStatusTransition`/
   etc.'s existing `last_synced: datetime = field(default_factory=lambda:
   datetime.now(timezone.utc))` convention, confirmed live in
   `ClickHouseMetricsSink.write_work_items` which stamps `synced_at =
   datetime.now(timezone.utc)` at write time) — **not** a customer-supplied
   `updated_at`/hash. Customer-supplied `updated_at` per record is validated for
   sanity (CHAOS-2697's job) but is not the RMT version column. This keeps
   external-ingest's dedup semantics IDENTICAL to native sync's, so CHAOS-2698
   needs zero new dedup code — it inherits FINAL/argMax discipline automatically
   by using the same dataclasses.
10. **`source_instance` must use the same identifier grain as the existing
    `IntegrationSource.external_id`** for that provider — i.e. `owner/repo` full
    name for GitHub/GitLab (confirmed live: `api/admin/routers/sync.py:559-570`,
    `sync/discovery.py:249`), `project_key`/`team_id`/Linear-workspace-scoped key
    for Jira/Linear — **not** the org/group-level strings shown in the plan's own
    example (`"instance": "github.com/acme"`) or the web design doc's Screen 2
    examples (`gitlab.com/group/project`, Jira cloud URL). **[CROSS-CUTTING —
    this contradicts both plan docs' literal examples; confirm with CHAOS-2696/
    2714 owners before those issues lock in UI copy.]** Rationale: the
    one-active-owner conflict check (§6) needs an **exact string match** against
    `IntegrationSource.external_id` to detect "FullChaos sync already owns this
    repo." An org-level string like `github.com/acme` has no row to match against
    in `integration_sources` (which is per-repo), so the conflict check would
    silently never fire for the exact scenario the epic's non-goal list calls
    out as forbidden ("mixed FullChaos-sync and customer-push ownership"). If the
    epic owner wants org/group-level `source_instance` for UX reasons, the
    ownership resolver in §6 needs a *prefix-match* fallback against
    `integration_sources.full_name`/`metadata.owner`, which is fuzzier and a
    real design escalation — this brief implements the exact-match version and
    flags the alternative as a decision needed, not a silent choice.
11. **Ownership state has one authoritative table going forward
    (`external_ingest_sources`), but existing `fullchaos_sync` ownership is
    *derived*, not migrated.** Do not backfill a mirror row into
    `external_ingest_sources` for every existing `Integration`/`IntegrationSource`.
    Instead, `resolve_effective_mode()` (§6) falls back to querying
    `integration_sources`/`integrations` directly when no explicit
    `external_ingest_sources` row exists. Rationale: touching every existing
    provider-connect code path (`api/admin/routers/sync.py`) to dual-write into a
    new table is a much larger, riskier change than a read-time derivation, and
    is unnecessary since `integration_sources` already has everything needed
    (`org_id`, `provider`, `external_id`, `is_enabled`) plus `integrations.is_active`.
12. **Ownership is enforced at both points named in the issue, using the SAME
    resolver function**, not two independent implementations:
    - **Registration time** (CHAOS-2696's `POST` endpoint for registering a
      `customer_push` source): reject with `409 source_owned_by_fullchaos_sync`
      if `resolve_effective_mode(...) == "fullchaos_sync"`.
    - **Batch accept time** (CHAOS-2691's `POST /batches`, via CHAOS-2696's
      registered-source lookup): reject with `403 source_owned_by_fullchaos_sync`
      if the resolved mode for the batch's declared `source.system`/
      `source.instance` is `"fullchaos_sync"`, even if a `customer_push` row
      exists and is `enabled=True` in `external_ingest_sources` — this is
      deliberate defense-in-depth against the race where a FullChaos-managed sync
      gets connected to the *same* repo *after* a customer_push source was
      registered (nothing today prevents that on the `api/admin/routers/sync.py`
      side, since it doesn't know about `external_ingest_sources`).
13. **Error response shape**: `HTTPException(status_code=X, detail={"code": "...",
    "message": "..."})`, matching the codebase's existing dict-`detail` convention
    (`api/_errors.py`'s rate-limit/validation handlers already return
    `{"detail": {"message": ...}}`). This is a *superset* — adding `"code"` as a
    stable machine-readable field customers/CLI can branch on — not a break from
    the existing shape.

---

## 4. Router integration contract (for CHAOS-2691/2693 to implement against)

Pseudocode for `POST /api/v1/external-ingest/batches`, showing exactly where this
issue's functions are called:

```python
# 1. Auth (CHAOS-2712) resolves org_id + token scopes. Must have 'ingest:write'.
# 2. CHAOS-2691 validates the envelope with the Pydantic schema -> `envelope`.
#    Malformed body / unsupported schemaVersion -> 400 BEFORE reaching this issue's code.

# 3. Source ownership + registration check (CHAOS-2696 lookup + this issue's resolver):
source = lookup_external_ingest_source(db, org_id=org_id,
                                        system=envelope.source.system,
                                        instance=envelope.source.instance)
mode = resolve_effective_mode(db, org_id=org_id, system=envelope.source.system,
                               instance=envelope.source.instance)
if mode == "unclaimed":
    raise ingest_error(403, "source_not_registered", ...)
if mode == "disabled":
    raise ingest_error(403, "source_disabled", ...)
if mode == "fullchaos_sync":
    raise ingest_error(403, "source_owned_by_fullchaos_sync", ...)
# mode == "customer_push" -> proceed.

# 4. Idempotency resolution (THIS issue, §6/§7) -- FIRST write in this session (decision 6):
payload_hash = compute_payload_hash(envelope)
outcome = resolve_batch_idempotency(
    db, org_id=org_id, source_system=envelope.source.system,
    source_instance=envelope.source.instance,
    idempotency_key=envelope.idempotencyKey, payload_hash=payload_hash,
    schema_version=envelope.schemaVersion,
    window_started_at=envelope.window.startedAt, window_ended_at=envelope.window.endedAt,
    items_received=len(envelope.records),
)
await db.commit()  # commit-before-raise: batch row must survive even if step 5 raises 503/409

if outcome.kind == "conflict":
    raise ingest_error(409, "idempotency_key_conflict", ...)
if outcome.kind == "replay":
    return replay_response(outcome.batch)  # 200, decision 8

# outcome.kind in ("new", "retry") -> attempt/re-attempt stream enqueue (CHAOS-2693):
try:
    stream_write(outcome.batch.id, envelope)
except StreamUnavailableError:
    await mark_batch_stream_unavailable(db, outcome.batch.id)  # commit-before-raise again
    raise ingest_error(503, "ingest_stream_unavailable", ...)

if outcome.kind == "retry":
    await mark_batch_accepted(db, outcome.batch.id)  # clear stream_unavailable -> accepted
return accepted_response(outcome.batch)  # 202
```

`ingest_error(status, code, message)` is a thin helper this issue provides:
`raise HTTPException(status_code=status, detail={"code": code, "message": message})`.

---

## 5. Record-level identity/versioning ↔ sink dedup interplay (for CHAOS-2697/2698)

No new table or check is required. The rule, per record kind, is:

| record kind | internal dataclass | ClickHouse table | `ORDER BY` (post-027) | version column |
|---|---|---|---|---|
| `repository.v1` | `Repo` (`models/git.py`) | `repos` | `(org_id, id)` | none — RMT keyed by deterministic `id` |
| `identity.v1` | untyped dict row | `identities` | `(org_id, canonical_id)` | implicit (`insert_identities`) |
| `team.v1` | untyped dict row | `teams` | `(org_id, id)`, `FINAL`-read | implicit (`insert_teams`) |
| `work_item.v1` | `WorkItem` (`models/work_items.py`) | `work_items` | `(org_id, repo_id, work_item_id)` | `last_synced` — **stamped by the sink at write time** (`synced_at = datetime.now(timezone.utc)` in `ClickHouseMetricsSink.write_work_items`), not customer-supplied |
| `work_item_transition.v1` | `WorkItemStatusTransition` | `work_item_transitions` | `(repo_id, work_item_id, occurred_at)` + semantic-dedup subquery (`metrics/sinks/clickhouse/idempotency.py`, columns incl. `from_status`/`to_status`/`actor`) | `last_synced`, read via `semantic_deduped_subquery(...)`, **never raw `SELECT *`** |
| `work_item_dependency.v1` | `WorkItemDependency` | (see migration `011_work_item_extras.sql`) | `(source_work_item_id, target_work_item_id, relationship_type)` | `last_synced` (dataclass field, `default_factory=now`) |
| `pull_request.v1` | `GitPullRequest` (`models/git.py`) | `git_pull_requests` | `(org_id, repo_id, number)` | sink-stamped, same convention |
| `review.v1` | `GitPullRequestReview` | `git_pull_request_reviews` | per `027_...py` catalog | sink-stamped |
| `commit.v1` | `GitCommit` | `git_commits` | `(org_id, repo_id, hash)` | sink-stamped (commits are content-addressed by `hash`, so replays are naturally idempotent even without a version column mattering) |

**External ID → deterministic internal ID rule** (needed because a `repository.v1`
record's `externalId` is a customer-supplied string like `"acme/api"`, but `Repo.id`
in ClickHouse is a UUID): CHAOS-2697's normalizer MUST derive `Repo.id` via the
SAME function native sync uses — `get_repo_uuid_from_repo(repo_identifier)`
(`models/git.py:72`) — applied to the canonical `f"{source.system}:{envelope.source.instance}/{record.externalId}"`-shaped string (exact composition TBD by CHAOS-2697, but it MUST be deterministic and MUST collide with the native-sync-derived UUID for the same logical repo if `source.system`+`externalId` match what native sync would have produced — otherwise a customer push and a later FullChaos sync of the *same* repo create two disjoint `repos` rows instead of deduping, defeating the entire "same product model" architecture goal). **This is a CHAOS-2697 implementation detail, not this issue's code, but it is the single highest-risk correctness gap in the epic and is called out here because it is invisible until someone tests repo dedup across a mode switch.**

**Provider value**: `Repo.provider` today only recognizes
`github|gitlab|local|synthetic` (`models/git.py:180-185`). CHAOS-2697/2698 need
either a new `provider="customer_push"` value or (preferred, since `source.system`
already carries `github`/`gitlab`/`jira`/`linear`/`custom`) to keep
`Repo.provider = envelope.source.system` and add a separate boolean/enum
"ingested via customer push" marker column if provenance needs to be
distinguishable from native sync later. **Not this issue's decision to finalize**
(flagged for CHAOS-2697/2698), but the ownership-resolution logic in §6 does NOT
depend on `Repo.provider` at all — it only reads `integration_sources`/
`integrations`, so this choice doesn't block CHAOS-2695's own code.

---

## 6. API / DDL / schema sketches

### 6.1 `external_ingest_sources` (Postgres, new — CHAOS-2696 primary owner, this
issue specifies the shape needed for ownership resolution)

```python
# src/dev_health_ops/models/external_ingest.py

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import Boolean, DateTime, Index, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class ExternalIngestSourceMode(str, Enum):
    FULLCHAOS_SYNC = "fullchaos_sync"
    CUSTOMER_PUSH = "customer_push"
    DISABLED = "disabled"


class ExternalIngestSource(Base):
    """Explicit ownership registration for (org, system, instance).

    Only customer_push (and explicit disabled) rows are written here in v1;
    fullchaos_sync ownership is DERIVED at read time from
    integrations/integration_sources (see
    dev_health_ops.external_ingest.ownership.resolve_effective_mode), not
    mirrored into this table. See CHAOS-2695 brief decision 11.
    """

    __tablename__ = "external_ingest_sources"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    system: Mapped[str] = mapped_column(Text, nullable=False)
    instance: Mapped[str] = mapped_column(Text, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False, default="customer_push")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id", "system", "instance",
            name="uq_external_ingest_sources_identity",
        ),
        Index("ix_external_ingest_sources_org_system", "org_id", "system"),
    )
```

### 6.2 `external_ingest_batches` (Postgres — CHAOS-2694 primary owner; this issue
requires the `payload_hash` column and the unique constraint below)

```python
class ExternalIngestBatch(Base):
    __tablename__ = "external_ingest_batches"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)  # == ingestionId
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    source_instance: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(Text, nullable=False)  # sha256 hex, 64 chars
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    window_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    window_ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="accepted")
    items_received: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_accepted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_rejected: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "org_id", "source_system", "source_instance", "idempotency_key",
            name="uq_external_ingest_batches_identity",
        ),
        Index("ix_external_ingest_batches_org_status", "org_id", "status"),
    )
```

Valid `status` values (str, not a DB enum — matches `Integration`/`SyncRun` house
style of plain `Text`/`String` status columns validated in Python):
`accepted`, `stream_unavailable`, `processing`, `partial`, `completed`, `failed`.
`RETRYABLE_STATUSES = {"stream_unavailable", "failed"}` + stale-accepted rule (header item 2, post-critique).

**Coordination note**: CHAOS-2694 owns this table's migration. If CHAOS-2694 lands
first without `payload_hash`, add it via a small guarded follow-up migration
(`_add_column_if_missing`, same pattern as `route_family_attribution` in `0031`) —
do not block on strict landing order. If this issue (CHAOS-2695) lands first, its
migration creates the table with `payload_hash` included from the start and
CHAOS-2694 extends it (e.g. `error_summary` detail, whatever it needs).

### 6.3 Alembic migration `0032_external_ingest_idempotency_ownership.py`

```python
"""Add external_ingest_sources and external_ingest_batches (CHAOS-2695).

Idempotency + one-active-owner policy for customer-push ingestion (CHAOS-2690).
external_ingest_sources is the explicit ownership registry for customer_push /
disabled source instances; fullchaos_sync ownership is derived at read time from
integrations/integration_sources (see
dev_health_ops.external_ingest.ownership.resolve_effective_mode) rather than
mirrored here. external_ingest_batches is the durable idempotency ledger: one row
per (org_id, source_system, source_instance, idempotency_key), storing a SHA-256
payload_hash so retried batches can be distinguished from conflicting ones (same
key, different payload -> 409).

Guarded per the 0025/0031 create-if-missing convention so a partially-applied
migration can be re-run safely.

Revision ID: 0032
Revises: 0031
Create Date: 2026-07-01 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0032"
down_revision: str | None = "0031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_SOURCES_TABLE = "external_ingest_sources"
_BATCHES_TABLE = "external_ingest_batches"


def upgrade() -> None:
    if not _table_exists(_SOURCES_TABLE):
        op.create_table(
            _SOURCES_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("system", sa.Text(), nullable=False),
            sa.Column("instance", sa.Text(), nullable=False),
            sa.Column("mode", sa.Text(), nullable=False, server_default="customer_push"),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
    _create_index_if_missing("ix_external_ingest_sources_org_system", _SOURCES_TABLE, ["org_id", "system"])
    _create_unique_if_missing(
        "uq_external_ingest_sources_identity", _SOURCES_TABLE, ["org_id", "system", "instance"]
    )

    if not _table_exists(_BATCHES_TABLE):
        op.create_table(
            _BATCHES_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("source_instance", sa.Text(), nullable=False),
            sa.Column("idempotency_key", sa.Text(), nullable=False),
            sa.Column("payload_hash", sa.Text(), nullable=False),
            sa.Column("schema_version", sa.Text(), nullable=False),
            sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("status", sa.Text(), nullable=False, server_default="accepted"),
            sa.Column("items_received", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("items_accepted", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("items_rejected", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("error_summary", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
    _add_column_if_missing(_BATCHES_TABLE, sa.Column("payload_hash", sa.Text(), nullable=True))
    _create_index_if_missing("ix_external_ingest_batches_org_status", _BATCHES_TABLE, ["org_id", "status"])
    _create_unique_if_missing(
        "uq_external_ingest_batches_identity", _BATCHES_TABLE,
        ["org_id", "source_system", "source_instance", "idempotency_key"],
    )


def downgrade() -> None:
    if _table_exists(_BATCHES_TABLE):
        op.drop_table(_BATCHES_TABLE)
    if _table_exists(_SOURCES_TABLE):
        op.drop_table(_SOURCES_TABLE)


# ... _table_exists / _column_names / _add_column_if_missing / _create_index_if_missing
# copied verbatim from 0031; add _create_unique_if_missing using
# sa.inspect(bind).get_unique_constraints(table_name) the same way.
```

**Coordination risk**: whichever of CHAOS-2694 / 2695 / 2696 merges first claims
revision `0032`; the next one rebases to `0033` and updates `down_revision`. Do
not pre-claim `0032` in a long-lived branch — check `alembic heads` immediately
before writing the migration file.

### 6.4 `src/dev_health_ops/external_ingest/idempotency.py`

```python
"""Batch idempotency resolution for external customer-push ingestion (CHAOS-2695).

See docs/architecture/external-ingest-idempotency-ownership.md for the full
policy writeup (canonicalization algorithm, outcome semantics, ownership
interplay). This module implements the algorithm; it does not own the
external_ingest_batches table's general CRUD (see CHAOS-2694's status.py).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dev_health_ops.models.external_ingest import ExternalIngestBatch

RETRYABLE_STATUSES = frozenset({"stream_unavailable", "failed"})  # header item 2; +
# stale-accepted/processing (updated_at older than EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES=15)
# is ALSO treated as RETRY — see header item 2 (post-critique CC13).


class IdempotencyOutcomeKind(str, Enum):
    NEW = "new"
    REPLAY = "replay"
    CONFLICT = "conflict"
    RETRY = "retry"


@dataclass(frozen=True)
class IdempotencyOutcome:
    kind: IdempotencyOutcomeKind
    batch: ExternalIngestBatch


def compute_payload_hash(envelope: BaseModel) -> str:
    """SHA-256 hex digest of the canonicalized, schema-validated envelope.

    MUST be called on the already-validated Pydantic model (post
    schema-version/shape validation in CHAOS-2691's router), not on raw
    request bytes -- canonicalization relies on Pydantic's mode="json" dump
    to normalize field order, whitespace, and timestamp formatting.
    """
    canonical = json.dumps(
        envelope.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def resolve_batch_idempotency(
    session: Session,
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    idempotency_key: str,
    payload_hash: str,
    schema_version: str,
    window_started_at: datetime | None,
    window_ended_at: datetime | None,
    items_received: int,
) -> IdempotencyOutcome:
    """Resolve NEW / REPLAY / CONFLICT / RETRY for a batch identity.

    MUST be the first write in the caller's session (see CHAOS-2695 brief
    decision 6): on conflict this rolls back the whole session before
    re-querying.
    """
    batch = ExternalIngestBatch(
        org_id=org_id,
        source_system=source_system,
        source_instance=source_instance,
        idempotency_key=idempotency_key,
        payload_hash=payload_hash,
        schema_version=schema_version,
        window_started_at=window_started_at,
        window_ended_at=window_ended_at,
        status="accepted",
        items_received=items_received,
    )
    session.add(batch)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        existing = (
            session.query(ExternalIngestBatch)
            .filter(
                ExternalIngestBatch.org_id == org_id,
                ExternalIngestBatch.source_system == source_system,
                ExternalIngestBatch.source_instance == source_instance,
                ExternalIngestBatch.idempotency_key == idempotency_key,
            )
            .one_or_none()
        )
        if existing is None:
            # Sub-millisecond true race: the conflicting insert hasn't
            # committed yet (or was itself rolled back). Treat as
            # transiently unavailable; the client's idempotency-safe retry
            # will resolve it on the next attempt.
            raise IngestTemporarilyUnavailableError(
                "Concurrent write in progress for this idempotency key; retry."
            ) from None
        if existing.payload_hash == payload_hash:
            if existing.status in RETRYABLE_STATUSES:
                return IdempotencyOutcome(IdempotencyOutcomeKind.RETRY, existing)
            return IdempotencyOutcome(IdempotencyOutcomeKind.REPLAY, existing)
        return IdempotencyOutcome(IdempotencyOutcomeKind.CONFLICT, existing)
    return IdempotencyOutcome(IdempotencyOutcomeKind.NEW, batch)


class IngestTemporarilyUnavailableError(RuntimeError):
    """Raised on the sub-millisecond true-concurrency race; maps to 503."""
```

(Async variant `resolve_batch_idempotency_async` mirrors this using
`AsyncSession`/`await session.flush()` for the real FastAPI request path — the sync
version above is what unit tests exercise directly against `sqlite:///:memory:`;
CHAOS-2691's router uses the async version against the real async Postgres session.
Keep both bodies in lockstep — a thin `async def` wrapper duplicating the same
try/except over an `AsyncSession` is acceptable duplication here, matching how
`db.py` already keeps sync/async session helpers side by side.)

### 6.5 `src/dev_health_ops/external_ingest/ownership.py`

```python
"""One-active-owner resolution for external customer-push ingestion (CHAOS-2695).

fullchaos_sync ownership is DERIVED from integrations/integration_sources (native
sync's existing tables) rather than mirrored into external_ingest_sources -- see
CHAOS-2695 brief decision 11.
"""
from __future__ import annotations

from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from dev_health_ops.models.external_ingest import ExternalIngestSource
from dev_health_ops.models.integrations import Integration, IntegrationSource

EffectiveMode = Literal["fullchaos_sync", "customer_push", "disabled", "unclaimed"]


def resolve_effective_mode(
    session: Session, *, org_id: str, system: str, instance: str
) -> EffectiveMode:
    """Resolve the single active ingestion owner for (org, system, instance).

    `instance` MUST be the same identifier grain as
    IntegrationSource.external_id for `system` (repo full_name for
    github/gitlab, project_key/team key for jira/linear) -- see CHAOS-2695
    brief decision 10. Precedence:
      1. Explicit external_ingest_sources row (customer_push registration or
         explicit disable) wins if present.
      2. Otherwise, an enabled IntegrationSource + active Integration for the
         same (org, provider=system, external_id=instance) implies
         fullchaos_sync (legacy/native-sync ownership, never explicitly
         registered here).
      3. Otherwise, unclaimed.
    """
    explicit = session.execute(
        select(ExternalIngestSource).where(
            ExternalIngestSource.org_id == org_id,
            ExternalIngestSource.system == system,
            ExternalIngestSource.instance == instance,
        )
    ).scalar_one_or_none()
    if explicit is not None:
        if not explicit.enabled:
            return "disabled"
        return explicit.mode  # "customer_push" or "disabled" (mode column, decision 12)

    fullchaos_owned = session.execute(
        select(IntegrationSource.id)
        .join(Integration, Integration.id == IntegrationSource.integration_id)
        .where(
            IntegrationSource.org_id == org_id,
            IntegrationSource.provider == system,
            IntegrationSource.external_id == instance,
            IntegrationSource.is_enabled.is_(True),
            Integration.is_active.is_(True),
        )
        .limit(1)
    ).scalar_one_or_none()
    if fullchaos_owned is not None:
        return "fullchaos_sync"

    return "unclaimed"


def check_registration_conflict(
    session: Session, *, org_id: str, system: str, instance: str
) -> bool:
    """True if registering a customer_push source here would create mixed
    ownership. CHAOS-2696's registration endpoint calls this before writing
    a new external_ingest_sources row.
    """
    return resolve_effective_mode(session, org_id=org_id, system=system, instance=instance) == "fullchaos_sync"
```

---

## 7. Error codes (exact HTTP status, `code`, and message template)

All raised as `HTTPException(status_code=<status>, detail={"code": "<code>",
"message": "<message>"})`.

| # | Scenario | HTTP | `code` | `message` template | Raised by |
|---|---|---|---|---|---|
| 1 | Batch replay: same idempotency key, same payload hash | 200 | n/a | full current-status envelope body, no error | CHAOS-2691 router, using this issue's `REPLAY` outcome |
| 2 | Batch conflict: same idempotency key, different payload hash | 409 | `idempotency_key_conflict` | `"Idempotency key '{key}' was already used for source '{system}:{instance}' with a different payload. Use a new idempotencyKey, or retry with the exact original payload to get the cached status."` | this issue (`CONFLICT` outcome) |
| 3 | True concurrent-write race (sub-ms) | 503 | `ingest_temporarily_unavailable` | `"A concurrent request for the same idempotency key is in progress. Retry."` | this issue (`IngestTemporarilyUnavailableError`) |
| 4 | Stream enqueue failed (new or retry) | 503 | `ingest_stream_unavailable` | `"The durable ingest stream is temporarily unavailable. The batch was recorded as '{ingestionId}'; retry with the same idempotencyKey once available."` | CHAOS-2693, batch row already updated to `stream_unavailable` by this issue's helper |
| 5 | `source.system`/`source.instance` never registered | 403 | `source_not_registered` | `"No ingest source is registered for system='{system}' instance='{instance}' in this organization. Register it under /org/admin/integrations before pushing."` | this issue (`resolve_effective_mode == "unclaimed"`), called from CHAOS-2691 router |
| 6 | Source registered but disabled | 403 | `source_disabled` | `"Ingest source '{system}:{instance}' is disabled for this organization."` | this issue, `resolve_effective_mode == "disabled"` |
| 7 | Source owned by FullChaos-managed sync (batch-accept time) | 403 | `source_owned_by_fullchaos_sync` | `"Source '{system}:{instance}' is currently managed by FullChaos-hosted sync. Disable managed sync for this source before pushing customer data, or contact support."` | this issue, `resolve_effective_mode == "fullchaos_sync"` |
| 8 | Source owned by FullChaos-managed sync (registration time) | 409 | `source_owned_by_fullchaos_sync` | same message as #7 | this issue's `check_registration_conflict`, called from CHAOS-2696's registration endpoint |
| 9 | Duplicate active customer_push registration | 409 | `source_already_registered` | `"An ingest source is already registered for system='{system}' instance='{instance}'."` | CHAOS-2696 (uses this issue's unique constraint / `resolve_effective_mode == "customer_push"`) |

Codes NOT owned by this issue but that MUST reuse the same
`{"code","message"}` shape for consistency (listed for the router author's
awareness, not implemented here): `invalid_envelope` (400), `unsupported_schema_version`
(400), `payload_too_large` (413), `invalid_token` (401), `insufficient_scope` (403),
`batch_not_found` (404).

---

## 8. Files to create/modify

Create:
- `src/dev_health_ops/models/external_ingest.py` — `ExternalIngestSource`,
  `ExternalIngestSourceMode`, `ExternalIngestBatch` (§6.1, §6.2). If CHAOS-2694/2696
  already created a models file for their halves when this lands, merge into it
  instead of creating a duplicate — check `git log`/open PRs before creating.
- `src/dev_health_ops/alembic/versions/0032_external_ingest_idempotency_ownership.py`
  (§6.3) — check `alembic heads` first; rename to whatever the next free revision
  actually is if 2694/2696 landed a migration first.
- `src/dev_health_ops/external_ingest/__init__.py`
- `src/dev_health_ops/external_ingest/idempotency.py` (§6.4)
- `src/dev_health_ops/external_ingest/ownership.py` (§6.5)
- `docs/architecture/external-ingest-idempotency-ownership.md` — durable record of
  this policy (house rule: document decisions in the same changeset). Should
  summarize §3/§5/§6/§7 in prose + the table from §5, cross-linking to
  `docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md`.
- `tests/external_ingest/__init__.py`
- `tests/external_ingest/test_idempotency.py`
- `tests/external_ingest/test_ownership.py`

Modify (coordinate with sibling-issue authors; do not silently overwrite):
- `src/dev_health_ops/api/external_ingest/router.py` (CHAOS-2691) — wire in
  §4's call sequence.
- `src/dev_health_ops/api/external_ingest/errors.py` (CHAOS-2691's planned module
  path) — add the `ingest_error(...)` helper and the error codes from §7.
- CHAOS-2696's source-registration endpoint — call `check_registration_conflict`.

---

## 9. Test plan

### Unit tests (no live DB required — `sqlite:///:memory:`, matching
`tests/test_batch_storage.py`'s pattern; NOT `@pytest.mark.clickhouse`, this is
pure-Postgres-model logic)

`tests/external_ingest/test_idempotency.py`:
- `test_canonicalization_field_order_invariant` — two envelopes with the same
  logical content but different JSON key order in a hand-built dict (bypass
  Pydantic's own reordering by constructing via `model_construct` or by
  round-tripping through `model_validate` on differently-ordered raw dicts)
  produce identical `compute_payload_hash`.
- `test_canonicalization_whitespace_invariant` — parsing from a pretty-printed
  vs minified JSON string of the same content produces identical hashes.
- `test_canonicalization_timestamp_format_invariant` — `"...Z"` vs `"...+00:00"`
  in `window.startedAt` produce identical hashes (verified live: Pydantic v2
  `model_dump(mode="json")` normalizes both to `"...Z"`).
- `test_new_batch_returns_new_outcome`.
- `test_replay_same_key_same_hash_returns_replay_outcome_and_existing_row`.
- `test_conflict_same_key_different_hash_returns_conflict_outcome`.
- `test_retry_outcome_when_existing_status_is_stream_unavailable`.
- `test_replay_not_retry_when_existing_status_is_terminal` (e.g. `completed`
  must NOT be treated as `RETRY`).
- `test_unique_constraint_scoped_to_org_system_instance` — same
  `idempotency_key` string across two different `source_instance` values for the
  same org creates two independent `NEW` rows (not a conflict).
- `test_true_race_raises_temporarily_unavailable` — simulate by inserting a row
  with a matching identity AFTER the `IntegrityError` fires but engineer the
  `existing is None` branch directly (unit-test the branch, not the actual race,
  since a real race is not deterministically reproducible in a single-threaded
  test — assert the function raises `IngestTemporarilyUnavailableError` when the
  post-conflict `SELECT` returns nothing, via a monkeypatched/mocked query).

`tests/external_ingest/test_ownership.py`:
- `test_unclaimed_when_no_rows_anywhere`.
- `test_customer_push_when_explicit_row_enabled`.
- `test_disabled_when_explicit_row_disabled`.
- `test_fullchaos_sync_derived_from_integration_sources` — seed
  `Integration(is_active=True)` + `IntegrationSource(is_enabled=True,
  provider="github", external_id="acme/api")`, assert
  `resolve_effective_mode(..., system="github", instance="acme/api") ==
  "fullchaos_sync"`.
- `test_fullchaos_sync_not_derived_when_integration_inactive` — same seed but
  `Integration.is_active=False` → `"unclaimed"`.
- `test_fullchaos_sync_not_derived_when_source_disabled` —
  `IntegrationSource.is_enabled=False` → `"unclaimed"`.
- `test_explicit_row_wins_over_legacy_derivation` — seed BOTH an
  `ExternalIngestSource(mode="customer_push", enabled=True)` row and a
  conflicting active `IntegrationSource` for the same identity; assert the
  resolver returns `"customer_push"` (explicit row precedence, per §6.5
  docstring) — and separately assert `check_registration_conflict` would have
  blocked this state from being CREATED in the first place (test the guard, not
  just the resolver, since the guard is what should prevent this scenario from
  occurring in practice).
- `test_registration_conflict_blocks_when_fullchaos_owns`.
- `test_registration_conflict_allows_when_unclaimed`.

### Integration tests (still sqlite, but exercise the migration model definitions
directly via `Base.metadata.create_all(engine)` to catch column/constraint typos)
- `tests/external_ingest/test_external_ingest_models.py::test_unique_constraint_enforced_at_db_level`
  — assert a raw duplicate insert raises `IntegrityError` at the DB layer, not
  just in application logic (guards against the constraint name typo class of
  bug).

### Live-DB tests
None required for this issue specifically — idempotency/ownership logic is pure
Postgres-model logic covered by sqlite-backed unit tests per house convention (no
`@pytest.mark.clickhouse` tests needed here). If CHAOS-2694/2698 want an
end-to-end live-Postgres+ClickHouse test of a full batch replay including sink
writes, that belongs in their test suites, not this issue's — but flag to them
that this issue's `RETRY` outcome (stream-unavailable-then-retry) is the one
scenario worth an explicit integration test somewhere in the epic, since it's the
one behavior that's easy to regress by "simplifying" back to a 3-way
NEW/REPLAY/CONFLICT model.

### mypy
`compute_payload_hash`/`resolve_batch_idempotency`/`resolve_effective_mode` must
be fully typed (no `Any` returns) — this module is exactly the kind of new,
small, pure-logic module `mypy --install-types --non-interactive .` should catch
100% cleanly; do not add `# type: ignore` without a comment explaining why.

---

## 10. Gate commands

Run from `/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`:

```bash
# fresh venv per worktree pattern (house rule)
uv sync --all-extras --dev

# format + lint + typecheck + full unit suite + live-ClickHouse argMax proof,
# isolated scratch DB (per-issue name to avoid clobbering a parallel agent's run)
SCRATCH_DB=ci_local_validate_chaos2695 bash ci/local_validate.sh

# mypy alone, literal CI invocation (redundant with local_validate.sh but
# matches the CI-parity house rule of running the LITERAL command)
.venv/bin/mypy --install-types --non-interactive .

# targeted: just this issue's new tests, fast loop
.venv/bin/pytest tests/external_ingest/ -v
```

No `@pytest.mark.clickhouse` tests are added by this issue, so
`SKIP_CLICKHOUSE=1 bash ci/local_validate.sh` is also a valid fast pre-check while
iterating, but run the full (non-skip) gate before calling the issue done, since
`local_validate.sh`'s live-ClickHouse stage also re-validates that this issue's
new Alembic migration + any Postgres schema changes don't break `dev-hops migrate
clickhouse status --check` wiring indirectly (schema_migrations tracking is
independent of Postgres, but the script's overall health-check flow should still
pass clean).

---

## 11. Live verification procedure (dev compose stack)

This issue has no HTTP endpoints of its own (those are CHAOS-2691/2696's), so live
verification is at the Python-module level plus a Postgres round-trip against the
real dev Postgres container — do NOT run migrations against it per the task's
constraints; use a scratch schema/DB the same way `local_validate.sh` uses a
scratch ClickHouse DB.

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration

# 1. Apply the new migration to a SCRATCH Postgres database only (never the
#    dev-health-postgres-1 container's default 'devhealth' DB):
docker exec dev-health-postgres-1 psql -U devhealth -c \
  "CREATE DATABASE ci_local_validate_chaos2695_pg;" 2>&1 | tail -5

POSTGRES_URI="postgresql+asyncpg://devhealth:devhealth@localhost:5432/ci_local_validate_chaos2695_pg" \
  .venv/bin/dev-hops migrate postgres upgrade

# 2. Verify the two tables + constraints exist:
docker exec dev-health-postgres-1 psql -U devhealth -d ci_local_validate_chaos2695_pg -c \
  "\d external_ingest_sources" -c "\d external_ingest_batches"

# 3. Exercise resolve_batch_idempotency / resolve_effective_mode against the
#    real scratch DB (not sqlite) to catch any Postgres-specific constraint
#    behavior sqlite's laxer typing might have hidden:
POSTGRES_URI="postgresql://devhealth:devhealth@localhost:5432/ci_local_validate_chaos2695_pg" \
  .venv/bin/python - <<'PY'
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from dev_health_ops.external_ingest.idempotency import resolve_batch_idempotency, compute_payload_hash
from datetime import datetime, timezone

engine = create_engine("postgresql://devhealth:devhealth@localhost:5432/ci_local_validate_chaos2695_pg")
with Session(engine) as s:
    o1 = resolve_batch_idempotency(s, org_id="org1", source_system="github",
        source_instance="acme/api", idempotency_key="k1", payload_hash="h1",
        schema_version="external-ingest.v1", window_started_at=datetime.now(timezone.utc),
        window_ended_at=datetime.now(timezone.utc), items_received=5)
    s.commit()
    print("first:", o1.kind, o1.batch.id)

    o2 = resolve_batch_idempotency(s, org_id="org1", source_system="github",
        source_instance="acme/api", idempotency_key="k1", payload_hash="h1",
        schema_version="external-ingest.v1", window_started_at=datetime.now(timezone.utc),
        window_ended_at=datetime.now(timezone.utc), items_received=5)
    s.commit()
    print("replay (expect REPLAY, same id):", o2.kind, o2.batch.id == o1.batch.id)

    o3 = resolve_batch_idempotency(s, org_id="org1", source_system="github",
        source_instance="acme/api", idempotency_key="k1", payload_hash="DIFFERENT",
        schema_version="external-ingest.v1", window_started_at=datetime.now(timezone.utc),
        window_ended_at=datetime.now(timezone.utc), items_received=5)
    s.commit()
    print("conflict (expect CONFLICT):", o3.kind)
PY

# 4. Tear down the scratch DB:
docker exec dev-health-postgres-1 psql -U devhealth -c \
  "DROP DATABASE ci_local_validate_chaos2695_pg;"
```

Expected output: `first: new <uuid>`, `replay (expect REPLAY, same id): True`,
`conflict (expect CONFLICT): conflict`.

---

## 12. Dependencies on other sub-issues

- **CHAOS-2691** (REST contract): router must call this issue's functions in the
  exact sequence in §4, and must run Pydantic envelope validation (400/schema
  errors) BEFORE calling `compute_payload_hash`/`resolve_batch_idempotency`
  (hashing requires an already-validated model per decision 1).
- **CHAOS-2693** (durable stream): must expose a `StreamUnavailableError` (or
  equivalent) this issue's router integration can catch to trigger the
  `stream_unavailable` status transition (§4 step 5) — confirm the exact
  exception type/import path with CHAOS-2693's implementer.
- **CHAOS-2694** (status store): owns `external_ingest_batches`'s general
  CRUD/`GET /batches/{id}` handler; must accommodate the `payload_hash` column
  and the `stream_unavailable` status value in its status-transition state
  machine (it is a legitimate non-terminal status a batch can be updated FROM by
  a later successful retry, not just a dead end).
- **CHAOS-2696** (source registration): owns the registration endpoint but must
  call `check_registration_conflict` before creating an
  `ExternalIngestSource(mode="customer_push")` row, and must use the same
  `source_instance` grain (decision 10) as this issue's ownership resolver
  expects.
- **CHAOS-2697** (worker normalization): must derive `Repo.id` via
  `get_repo_uuid_from_repo` (§5) so a customer-pushed repo dedupes against a
  prior/future native-sync row for the same repo.
- **CHAOS-2698** (sink writes): inherits record-level dedup for free per §5 as
  long as it uses the existing dataclasses/sink methods unmodified; no new dedup
  code needed on their end.
- **CHAOS-2712** (auth/tokens): must resolve `org_id` before the router reaches
  this issue's code; if tokens are source-scoped (not just org-scoped), CHAOS-2712
  should also expose whether the token's source binding matches the batch's
  declared `source.system`/`source.instance` — that check is logically adjacent
  to but NOT implemented by this issue (this issue only checks *mode ownership*,
  not *token-to-source binding*); flag to CHAOS-2712/2691 owners that both checks
  must run (mode ownership from this issue AND token-source binding from
  CHAOS-2712) before a batch is accepted.

---

## 13. Risks

1. **Migration revision-number collision** (§6.3) — three sub-issues
   (CHAOS-2694/2695/2696) each plausibly want to claim Postgres revision `0032`.
   Mitigate by checking `alembic heads` immediately before writing the migration
   file and by keeping this issue's migration mergeable independently (its own
   guarded `create_table` calls don't hard-fail if the table already exists from
   a sibling PR that landed first).
2. **`source_instance` granularity mismatch with the plan docs' own examples**
   (decision 10) — if the epic owner insists on org/group-level instance strings
   for UX reasons (matching the web design doc's `github.com/acme` example), the
   exact-match ownership conflict check in §6.5 will need a fuzzier prefix-match
   fallback against `IntegrationSource.full_name`/`metadata.owner`, which is a
   real design change, not a copy-paste fix. Flagged as
   **[CROSS-CUTTING]** in decision 10 — needs explicit sign-off before CHAOS-2696/
   2714 lock in their schemas/copy.
3. **`RETRY` outcome is easy to "simplify away."** A reviewer unfamiliar with the
   `503`-on-stream-unavailable requirement may look at the 4-way outcome enum and
   "simplify" it back to the plan's literal 3-way description, silently
   reintroducing a bug where a client that retried after a `503` gets a
   permanently stale `stream_unavailable` status instead of a fresh accept
   attempt. Mitigated by the explicit `test_retry_outcome_when_existing_status_is_stream_unavailable`
   test and this brief's decision 7 rationale — cite this brief in the PR
   description.
4. **`resolve_batch_idempotency` rollback semantics require callers to treat it
   as the first write in the session** (decision 6). If CHAOS-2691's router later
   grows a "write an audit-log row before checking idempotency" step (plausible,
   since CHAOS-2712 wants audit logging on every ingest request), that audit row
   would be silently discarded by this function's internal `session.rollback()`
   on the conflict path — exactly the CHAOS-2498 "emit-then-raise rollback"
   failure class from house rules, but triggered by an internal rollback rather
   than an exception escaping to `get_postgres_session`. Any future audit-log
   write in the same request MUST either happen in a separate session/connection
   or happen AFTER this function returns (and be committed via the
   commit-before-raise pattern if the outcome subsequently raises an
   HTTPException). Document this constraint prominently and re-flag it in
   CHAOS-2712's brief/PR review.
5. **Repo-ID determinism across customer-push and native-sync** (§5's
   `get_repo_uuid_from_repo` note) is the highest-severity *silent* correctness
   risk in the whole epic — if CHAOS-2697 gets the composition string wrong, a
   customer switching from customer-push to managed sync (or vice versa) for the
   same repo silently creates a duplicate `repos` row instead of the "same
   product model" the epic promises. This issue flags it; CHAOS-2697 must action
   it. Recommend an explicit epic-level acceptance test (cross-issue, likely
   owned by whoever does epic-level integration testing) that pushes a repo via
   customer_push, then connects the same repo via managed sync, and asserts a
   single `repos` row results.
6. **sqlite-vs-Postgres divergence in `IntegrityError` triggering.** SQLite
   enforces unique constraints slightly differently under certain isolation
   levels/pragma settings than Postgres; the unit tests in §9 should be
   considered a fast correctness check, not a substitute for the live-Postgres
   verification in §11 — run §11 at least once before considering the issue
   done, even though it's not part of the automated gate.
