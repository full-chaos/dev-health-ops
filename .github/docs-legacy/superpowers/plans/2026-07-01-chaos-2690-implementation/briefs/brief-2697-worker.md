# Implementation Brief: CHAOS-2697 External ingest worker normalization

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. **D5 is REVERSED**: do NOT set `Repo.provider = "customer_push"`. Provider stays the
>    source system (`github|gitlab|custom`) — a customer_push provider value would flip
>    `provider` on RMT replace for the same repo row and break provider-branching readers.
>    Provenance = new nullable ClickHouse `source_id UUID` column (CHAOS-2698's migration
>    065), stamped with the registered source's UUID on every pushed row (CC8).
> 2. **D2's queue/compose wiring is REASSIGNED to CHAOS-2693** (single Celery-wiring owner,
>    CC20): queue name is `external-ingest` (hyphen), worker `worker-external-ingest`,
>    consumer subclass + Celery task + beat + compose all land in 2693 (wave 3). This issue
>    (wave 4) ships ONLY `external_ingest/{validate-ext,normalize,processor,status_reporter}`
>    — no workers/config.py, compose.yml, or consumer-class changes.
> 3. **D9/D10 are SUPERSEDED by the reclaim ladder (CC11)**: 2693 adds XPENDING/XCLAIM
>    reclaim to `StreamConsumer` (default-off; enabled for external-ingest,
>    `max_deliveries=5`). Worker keeps its bounded in-process retry (3×, 2s/4s/8s) for sink
>    transients; when exhausted it RAISES a transient error (entry left unACKed → reclaimed
>    later) instead of DLQ+ACK-ing itself. `PermanentProcessingError`
>    (from `external_ingest/errors.py`, owned by 2693) → DLQ immediately. `processor.py`
>    additionally exposes `mark_batch_failed(ingestion_id, org_id, reason)` for the
>    consumer's max-deliveries give-up path.
> 4. **D1 amended**: the Pydantic models live in `api/external_ingest/schemas.py`
>    (CHAOS-2691, wave 1) — this issue does NOT define record models. `external_ingest/
>    validate.py` (created thin by 2691) is extended here with deep per-record validation +
>    the kind×system matrix + `record_outside_source_instance` checks (CC6), importing
>    2691's `RECORD_KIND_MODELS`. `mappings.py`'s RecordEnvelope sketch is VOID (2691's
>    wrapper with field name **`payload`**, not `data`, is canonical).
> 5. Wire-schema sketches below defer to 2691's canonical field sets; `work_item.v1` takes
>    `externalKey` (provider-native), and namespaced `work_item_id` derivation
>    (`jira:`/`linear:`/`gh:`/`ghpr:`/`gitlab:#`/`gitlab:!`) happens via CHAOS-2698's
>    `external_ingest/ids.py` (CC7). Repo UUID seed = repo full name, not URL (CC4).
> 6. `process_batch` signature pinned (CC23):
>    `process_batch(*, ingestion_id, org_id, source_system, source_instance, schema_version) -> int`.
>    Stream entry fields pinned per CC9. Payload fetched via 2693's `payload_store`.
> 7. **D8 amended**: recompute is dispatched via CHAOS-2699's
>    `schedule_or_coalesce(*, org_id, source_system, source_instance, ingestion_id,
>    repo_ids, team_ids, window_start, window_end, record_kinds)` (primitives — map 2698's
>    `AffectedScope` fields into kwargs); do not hand-roll `.delay()` fan-out here.
> 8. work_item family restricted to systems {jira, github, gitlab, linear}; `custom` batches
>    may not contain work_item kinds in v1 (per-record `unsupported_kind_for_system`).
> 9. Live-ClickHouse per-kind round-trip tests are OWNED BY CHAOS-2698; this issue keeps
>    fakeredis + mocked-sink integration tests; full-stack e2e is CHAOS-2702.
> 10. D11's failed-batch resubmission amendment is RATIFIED (2695 implements;
>     `attempts` column in 2694's 0033).
> 11. **POST-CRITIQUE (CC17): `external_ingest/validate.py` is IMPORTED UNCHANGED** —
>     it is created COMPLETE by 2691 in wave 1; this issue does NOT create or extend it.
>     If worker-side validation needs anything beyond it, that logic lives in
>     normalize.py/processor.py, not validate.py.
> 12. **POST-CRITIQUE (CC11): before processing any entry (fresh or reclaimed), check
>     batch status — terminal (completed|partial|failed) → ACK + skip** (idempotent-skip
>     guard; the consumer-side check is 2693's, but processor.py must tolerate being
>     invoked twice and keep status upserts replay-safe).

Repo: `dev-health-ops`, worktree `/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration` (branch `chaos-2690-external-ingest`).

Parent epic: CHAOS-2690. Sibling issues referenced throughout: CHAOS-2691 (REST contract/schemas), CHAOS-2692 (schema registry/JSON Schema export), CHAOS-2693 (durable stream+DLQ), CHAOS-2694 (status/rejection persistence), CHAOS-2695 (idempotency/ownership policy), CHAOS-2696 (source registration/token scopes), CHAOS-2698 (sink writes), CHAOS-2699 (bounded recomputation planner), CHAOS-2700 (dev-hops push CLI), CHAOS-2702 (e2e test).

As of this brief, **nothing under `src/dev_health_ops/external_ingest/` or `src/dev_health_ops/api/external_ingest/` exists yet** — the branch only has the two plan docs committed (`git log` shows a single docs commit). This brief assumes CHAOS-2697 may be implemented before, after, or concurrently with its siblings, so every cross-issue dependency is expressed as an explicit interface (Protocol / dataclass) that the other issue's implementation must satisfy, not as a hard import of code that doesn't exist yet.

---

## Scope

Build the worker-side normalization pipeline that turns an accepted, durably-enqueued `external-ingest.v1` batch into provider-neutral internal records, persists them through existing sinks, and reports outcome — per the issue's literal file list plus the runtime/queue wiring needed to actually run it:

1. `src/dev_health_ops/external_ingest/validate.py` — full (worker-side) structural + business validation of a batch envelope and its records, independent of whatever light validation CHAOS-2691's `POST /batches` performs at accept time. This is the **single source of truth** for record-kind validation (see Design Decision D1) — CHAOS-2691/2692 must import from here, not re-implement.
2. `src/dev_health_ops/external_ingest/mappings.py` — per-record-kind Pydantic v2 models (the wire shape) plus pure mapping functions from validated wire records to internal dataclasses/rows (`WorkItem`, `WorkItemStatusTransition`, `WorkItemDependency`, `Repo`, `GitCommit`, `GitPullRequest`, `GitPullRequestReview`, and dict rows for `team`/`identity`).
3. `src/dev_health_ops/external_ingest/normalize.py` — orchestrates validate → map → org_id-stamp for one batch, returns `(normalized_by_kind, rejections, affected_scope)`. No I/O (no ClickHouse/Postgres calls) — pure and unit-testable.
4. `src/dev_health_ops/external_ingest/processor.py` — the impure orchestrator: takes a raw stream entry payload, calls `normalize.process_batch(...)`, writes accepted records through existing sink methods, calls the (interface-only, see D6/D7) status-reporter and recompute-enqueue hooks, and returns a result the Celery task uses for logging/metrics.
5. `src/dev_health_ops/external_ingest/status_reporter.py` — a small `Protocol` (not an implementation) that CHAOS-2694 will implement against Postgres. Needed so `processor.py` has something concrete to call and something concrete to inject in tests.
6. `src/dev_health_ops/external_ingest/recompute.py` — `AffectedScope` dataclass + a **minimal working implementation** of bounded recompute enqueue (calls existing `dispatch_investment_materialize_partitioned` / `run_daily_metrics` tasks scoped by `repo_ids`/`org_id`/date window — see D8), which CHAOS-2699 will replace/extend with cross-batch coalescing and its own observability. Ship something real now rather than a stub, since the acceptance criteria of both 2690 and 2699 require recompute to actually be scoped, not full-org.
7. Worker runtime: a new Celery task (`run_external_ingest_consumer`) subclassing the shared `StreamConsumer` base, a new dedicated Celery queue + compose worker service, and a beat schedule entry (see D2).
8. Unit tests for all of the above; a `@pytest.mark.clickhouse` live-DB round-trip test proving each of the 9 v1 record kinds lands in the correct ClickHouse table with correct dedup semantics.

## Out of scope (owned by sibling issues — do not build here)

- `src/dev_health_ops/api/external_ingest/router.py`, `schemas.py` (CHAOS-2691) — the FastAPI accept-path Pydantic models. `external_ingest/validate.py` (this issue) is what 2691 should import for record-kind validation; 2691 owns only envelope-level (schemaVersion/idempotencyKey/source/max-records) checks at accept time.
- `src/dev_health_ops/api/external_ingest/streams.py`, DLQ stream naming (CHAOS-2693) — this issue's Celery task consumes from streams named by 2693's convention (`external-ingest:<org_id>:batches`, `external-ingest:<org_id>:dlq`) but does not define stream-write helpers.
- Postgres `external_ingest_batches` / `external_ingest_rejections` tables, alembic migration, and `GET /batches/{id}` (CHAOS-2694) — this issue only defines the `StatusReporter` Protocol it calls through.
- New/expanded `IngestToken` model, source registration, scope enforcement (CHAOS-2696) — the worker trusts that anything it dequeues was already auth'd/scope-checked at accept time; it does not re-check tokens.
- The exact idempotency **conflict** (409) semantics and one-active-owner enforcement (CHAOS-2695) live at the accept-path; this issue's job is to be safe to **re-run** on the same batch (replay-safety), not to decide whether a resubmission is a conflict.
- `dev-hops push` CLI (CHAOS-2700), CI/CD examples, docs, web UI (CHAOS-2711/2713/2714), webhook-assisted ingestion (CHAOS-2715), full E2E test (CHAOS-2702, though this issue's live-ClickHouse test is a prerequisite for that E2E).
- Any new sink methods beyond what's listed in D5 as "confirmed missing" — CHAOS-2698 owns adding sink helpers and the ClickHouse-row-verification test *suite*; this issue calls the sink methods that already exist and, if it discovers a real gap while wiring a kind, adds the minimal helper and flags it to 2698 rather than blocking (see D5).

---

## Design decisions

**D1. Validation lives once, in `external_ingest/validate.py`, using Pydantic v2 models — not a hand-rolled JSON-Schema validator.**
Rationale: the codebase is Pydantic-everywhere (FastAPI request models, `pydantic.mypy` plugin already enabled). Pydantic v2's `model_json_schema()` can generate the JSON Schema CHAOS-2692's `GET /schemas/{version}` endpoint needs to serve, so schema-for-customers and validation-for-real can never drift (this mirrors the project's existing "web schema.graphql is generated, never hand-written" convention). CHAOS-2691's accept-time validation and CHAOS-2692's schema export should both import from this module rather than re-implementing. Flag this to those issue owners explicitly (cross-cutting, see `decisionsNeeded`).

**D2. Worker runtime = Celery task subclassing `StreamConsumer`, on its own dedicated queue+worker, not a standalone always-on process.**
Rationale: `src/dev_health_ops/api/_stream_consumer.py` already encodes two hard-won production fixes (blocking-read `socket_timeout=None`, bounded backoff on `XREADGROUP` failure — see its docstring) that a hand-rolled loop would very likely reintroduce. The existing `worker-ingest` container proves the "beat-triggered, bounded-iteration Celery task on a concurrency=1 queue" pattern works in this compose topology. **Do not share the existing `ingest` queue/`worker-ingest` container.** That queue already serves the legacy `/api/v1/ingest` consumer and product-telemetry consumer; customer-push batches can be large and bursty (up to the max-batch-size CHAOS-2691 defines) and a slow/backlogged external-ingest consumer must not starve those unrelated features out of their single concurrency=1 slot. Add a new queue `external_ingest` and a new compose service `worker-external-ingest` (concurrency=1, same shape as `worker-ingest`).

**D3. Worker always re-runs full validation on every record — never trusts the accept-time check.**
Rationale: explicitly required by the plan doc's Worker section ("Run full validation") and by this issue's FOCUS. Concretely: CHAOS-2691's accept path only validates the *envelope* (cheap, synchronous, blocks the HTTP response); per-record kind/field validation is deferred to the worker so a slow customer payload with 10k records doesn't hold the HTTP connection open. This also defends against stream-payload corruption between accept and consume (defense in depth, matches "verify reachability" project convention of not trusting upstream claims).

**D4. Cross-record references (repo, PR, work item) are resolved by deterministic derivation, never by DB lookup.**
Rationale: `pull_request.v1`, `review.v1`, and `commit.v1` records reference a repository via `data.repoExternalId`. The normalizer computes `repo_id = get_repo_uuid_from_repo(repoExternalId)` — the *exact* SHA256-derived UUID function `models/git.py:72` already uses for `fullchaos_sync`-owned repos — rather than looking up an existing `repos` row. This guarantees the same logical repo gets the same `repo_id` regardless of whether the matching `repository.v1` record has already been processed, is in the same batch, or arrives in a later batch, and regardless of stream redelivery order. It also means a customer-push repo automatically joins the "one active owner" identity space a `fullchaos_sync` repo would have used for the same `repoExternalId` string — **this is intentional and required** so switching a source from `fullchaos_sync` to `customer_push` (or vice versa, subject to CHAOS-2695's one-active-owner enforcement) doesn't fork the repo's row history. `work_item_dependency.v1` references are just strings (`source_work_item_id`/`target_work_item_id`) and are **not** required to resolve within the same batch — a dependency may legitimately point at a work item ingested in an earlier or later batch; validate.py checks only that the reference is a non-empty string, not that it currently exists.

**D5. Repository ownership marker: set `Repo.provider = "customer_push"` — no migration needed.**
Rationale: verified `repos.provider` is a free-text `String` column with no CHECK constraint or ClickHouse Enum type (`storage/clickhouse.py`, migration `028_repos_provider.sql` — plain `String DEFAULT 'unknown'`). This resolves a real ambiguity the recon flagged ("no `customer_push` value exists yet") for zero schema cost. Do not add a new column for this in v1; CHAOS-2695's one-active-owner enforcement is a **Postgres-side source-registration** concern (CHAOS-2696's `source` table `mode` field), not something this ClickHouse column needs to gate.
All 9 kinds map onto **existing** sink methods with no missing helpers identified during this recon:
| kind | sink method | client |
|---|---|---|
| `repository.v1` | `ClickHouseStore.insert_repo(repo: Repo)` — **called once per record, not batched** | async `ClickHouseStore` |
| `identity.v1` | `ClickHouseStore.insert_identities(list[dict])` | async `ClickHouseStore` |
| `team.v1` | `ClickHouseStore.insert_teams(list[dict])` | async `ClickHouseStore` |
| `work_item.v1` | `ClickHouseMetricsSink.write_work_items(Sequence[WorkItem])` | sync `ClickHouseMetricsSink` |
| `work_item_transition.v1` | `ClickHouseMetricsSink.write_work_item_transitions(Sequence[WorkItemStatusTransition])` | sync `ClickHouseMetricsSink` |
| `work_item_dependency.v1` | `ClickHouseMetricsSink.write_work_item_dependencies(Sequence[WorkItemDependency])` | sync `ClickHouseMetricsSink` |
| `pull_request.v1` | `ClickHouseStore.insert_git_pull_requests(Sequence[GitPullRequest \| dict])` | async `ClickHouseStore` |
| `review.v1` | `ClickHouseStore.insert_git_pull_request_reviews(list[GitPullRequestReview])` | async `ClickHouseStore` |
| `commit.v1` | `ClickHouseStore.insert_git_commit_data(Sequence[GitCommit \| dict])` | async `ClickHouseStore` |

Verified: despite its type hint saying `list[GitPullRequestReview]`, `insert_git_pull_request_reviews` (`storage/clickhouse.py:906`) already has an `isinstance(item, dict)` branch identical to its siblings — **all git-family `insert_*` methods accept plain dict rows**, so `mappings.py` can emit dicts uniformly for the git-family kinds instead of constructing real ORM instances (simpler, avoids importing/instantiating SQLAlchemy `Base` subclasses outside a session). No sink change needed for any of the 9 kinds based on this recon; if implementation surfaces a real gap anyway, make the **minimal** change inline and post a one-line note to CHAOS-2698 rather than blocking this issue on it.

**D6. `processor.py` calls **both** async (`ClickHouseStore`) and sync (`ClickHouseMetricsSink`) clients from one Celery task.**
Rationale: confirmed in `recon-models-sinks.md` — there is no single "the sink," there are two client classes with different construction and different `org_id` semantics. `ClickHouseStore` auto-injects `org_id` from `store.org_id` for any row missing it; `ClickHouseMetricsSink.write_work_items`/`write_work_item_transitions` read `org_id` **directly off each record with no fallback** (dict path does `item["org_id"]`, a `KeyError` if absent). Decision: **always stamp `org_id` explicitly on every normalized record in `normalize.py`**, never rely on `ClickHouseStore`'s auto-injection — this is the safer, uniform contract and avoids a `KeyError` footgun on the metrics-sink path. Since `WorkItem`/`WorkItemStatusTransition`/`WorkItemDependency` are frozen dataclasses, stamping means `dataclasses.replace(item, org_id=org_id)` after mapping (or pass `org_id` directly into the constructor from `mappings.py`, which is simpler — do this).
Concretely: `processor.py` constructs `store = create_store(dsn, "clickhouse"); store.org_id = org_id` (async, `async with store:`) for the git-family kinds, and `sink = create_sink(dsn); sink.ensure_schema()` (sync) for the work-item-family kinds, run inside `asyncio.to_thread`/`run_async` per the existing `workers/async_runner.py` bridge convention (the whole Celery task body is sync; `ClickHouseStore` calls need `asyncio.run`/`run_async`, matching `ingest/persist.py`'s `asyncio.run(persist_items(...))` pattern).

**D7. Status-reporting is injected through a `Protocol`, not a hard dependency on CHAOS-2694's tables.**
Rationale: lets this issue merge and be fully unit-tested (with an in-memory fake) independent of build order relative to CHAOS-2694. `status_reporter.py` defines:
```python
class StatusReporter(Protocol):
    async def mark_processing(self, ingestion_id: str, org_id: str) -> None: ...
    async def record_rejections(self, ingestion_id: str, org_id: str, rejections: list[RejectedRecord]) -> None: ...
    async def complete(
        self, ingestion_id: str, org_id: str, *,
        status: Literal["completed", "partial", "failed"],
        items_accepted: int, items_rejected: int,
        error_summary: str | None = None,
    ) -> None: ...
```
`processor.py` accepts `status_reporter: StatusReporter | None = None` and falls back to a `LoggingStatusReporter` (logs at info/warning, no persistence) when `None` — this keeps the worker runnable (and its own tests green) before CHAOS-2694 lands, and CHAOS-2694 wires its Postgres-backed implementation in at the Celery-task construction site (`workers/external_ingest_tasks.py`, one import + one constructor call — see Files section). **Every `StatusReporter` write that must survive a subsequent exception (`mark_processing` before the risky sink-write section; the terminal `complete()` call before re-raising on a system failure) must itself commit-before-raise per the CHAOS-2498 pattern** — call this out explicitly in `status_reporter.py`'s docstring so CHAOS-2694's Postgres implementation doesn't reintroduce that bug class.

**D8. Bounded recompute: implement a real (if simple) scoped enqueue now, using existing tasks — don't stub it out.**
Rationale: `recon-celery-metrics.md` confirms `dispatch_investment_materialize_partitioned` (kwargs: `repo_ids`, `team_ids`, `from_date`, `to_date`, `org_id`, `force`) and `run_daily_metrics` (kwargs: `db_url`, `day`, `repo_id`, `org_id`, ...) **already implement scoped-vs-full recompute** by presence/absence of scope kwargs — this is exactly the "bounded recomputation planner" CHAOS-2699 is titled for, already half-built. `external_ingest/recompute.py` should:
1. Accumulate an `AffectedScope` dataclass during `normalize.process_batch()` (org_id, `repo_ids: set[uuid.UUID]`, `team_ids: set[str]`, `min_ts`/`max_ts: datetime`, `record_kinds: set[str]`, `source_systems`/`source_instances: set[str]`).
2. After a successful (or partial) sink-write pass, call `enqueue_bounded_recompute(scope)`, which fires (best-effort, wrapped in narrow `try/except Exception` per the house convention in `post_sync_dispatch.py`, so a dispatch hiccup never fails the ingest path):
   - `run_daily_metrics.delay(db_url=..., day=<one call per day in [min_ts.date(), max_ts.date()]>, repo_id=None, org_id=scope.org_id)` when `repo_ids` is non-empty — or, for anything spanning >1 day/repo, prefer chaining through `dispatch_daily_metrics_partitioned`-style batching if the day range exceeds ~7 days (avoid a message-per-day storm for large backfills; cap and log a warning beyond that).
   - `dispatch_investment_materialize_partitioned.delay(repo_ids=list(scope.repo_ids), team_ids=list(scope.team_ids), from_date=scope.min_ts.date(), to_date=scope.max_ts.date(), org_id=scope.org_id, force=False)` when `work_item*` kinds are present in `scope.record_kinds`.
   - `run_work_graph_build.delay(repo_id=..., org_id=scope.org_id, from_date=scope.min_ts.date(), to_date=scope.max_ts.date())` when `pull_request.v1`/`review.v1`/`commit.v1`/`work_item*` kinds are present.
3. CHAOS-2699 will very likely refactor step 2 into a shared "AffectedScope → task fan-out" planner reused by both post-sync dispatch and external-ingest (the plan's own module list doesn't specify a location) — leave a `# TODO(CHAOS-2699): generalize into a shared planner` comment at the call site so the follow-up is discoverable.
Per `reference_celery_signature_contract.md`: pin these `.delay()` kwargs with `inspect.signature(task.run)` in a unit test (see Test plan) so drift in the target tasks' signatures fails CI here instead of silently no-op'ing recompute.

**D9. Partial-failure semantics: per-record rejection, never a whole-batch abort, for data-shape problems; the *entry* is retried in-process (not via stream redelivery) for transient infra problems.**
Rationale (see full reasoning in "Redelivery and retry semantics" below): the shared `StreamConsumer.handle_entries` default ACKs every entry after exactly one processing attempt regardless of outcome (verified in `_stream_consumer.py:194-218`) and the consume loop always reads with `id=">"` (new messages only, verified `:252-258`) — there is **no XCLAIM/XAUTOCLAIM reclaim of pending entries anywhere in this codebase**, so "leave it unacked, let the stream redeliver it" is not actually a working mechanism here despite looking like one. Given that, this issue's `ExternalIngestStreamConsumer.handle_entries` override must not rely on redelivery for infra-transient failures (ClickHouse/Postgres connection errors); instead:
   - Per-record validation/mapping failures → always non-fatal, recorded as a `RejectedRecord`, processing continues for the rest of the batch. Cap detailed rejection rows at 1000 per batch (matches the "bounded rejected-record diagnostics" wording in the plan doc's Error store section) with `rejections_truncated: bool` surfaced in the batch summary if more were dropped.
   - Sink-write failures on an already-validated chunk (ClickHouse/Postgres down) → treated as a **system** failure, not a data failure. Retry the failing write in-process up to 3 times with 2s/4s/8s backoff (mirrors `task_default_retry_delay`/`task_max_retries` conventions elsewhere) before giving up.
   - If retries are exhausted: mark the batch `status="failed"` via the status reporter (commit-before-raise), route the entry to the `external-ingest:<org_id>:dlq` stream (CHAOS-2693's DLQ), and **still ACK it** — leaving it unacked provides no real benefit (see above) and would silently wedge the consumer group. This is a deliberate, documented divergence from "true at-least-once redelivery"; durability instead comes from (a) the persisted `failed` status making the failure visible/actionable and (b) safe customer resubmission (see D10).
   - The Celery task itself should still surface a non-zero-ish result/log line when any entry in the poll ended `failed`, so `monitor_queue_depths`-style alerting has something to key off if CHAOS-2699/observability work wants to build an alert later — do not swallow this silently at the task-return level even though the HTTP-facing behavior (DLQ + status write) is unaffected.

**D10. Replay-safety, not exactly-once: idempotent upserts at the sink layer are the actual durability guarantee.**
Rationale: Redis Streams + Celery beat give **at-least-once** delivery of "this batch needs processing," never exactly-once. This issue's worker must therefore be safe to run twice on the identical batch. It already is, by construction, **as long as every sink write is a `ReplacingMergeTree` upsert on the same natural key**: `repos (org_id, id)`, `git_commits (org_id, repo_id, hash)`, `git_pull_requests (org_id, repo_id, number)`, `git_pull_request_reviews (org_id, repo_id, number, review_id)`, `identities (org_id, canonical_id)`, `teams (org_id, id)`, `work_items (org_id, repo_id, work_item_id)`, `work_item_dependencies (org_id, source_work_item_id, target_work_item_id, relationship_type)` — reprocessing the same batch produces identical keys and ClickHouse's replace-on-merge semantics converge to one row. **Exception: `work_item_transitions`** — its ORDER BY includes `occurred_at` but is not fully semantically deduped by the ORDER BY key alone (per `metrics/sinks/clickhouse/idempotency.py`); a naive reprocessing that re-derives a fresh `last_synced` on retry is fine because the *semantic* dedup (`semantic_deduped_subquery`, grouped on `org_id, repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor`, `max(last_synced)`) is a **read-time** concern already handled by existing query code — this worker does not need to do anything extra here beyond writing rows normally, but the live-DB test (Test plan) must assert readers see one transition, not N, after reprocessing.
Status-store idempotency (Postgres, owned by CHAOS-2694) must mirror this: writes should be **upsert-by-`ingestion_id`**, not insert-only, so double-processing updates counts to the same final values instead of duplicating a status row. Flag this explicitly to CHAOS-2694 (cross-cutting).

**D11. Recovery path for a `failed` batch is customer resubmission with the same idempotency key — this requires an amendment to CHAOS-2695's idempotency rule.**
CHAOS-2695's stated rule ("same idempotency key + same payload hash returns the existing ingest status") is correct for `completed`/`partial`/`processing`, but if taken literally for `failed` it strands the customer: resubmitting the identical payload after a transient ClickHouse outage would just return the stale `failed` status forever, with no way to trigger a fresh attempt short of minting a new `idempotencyKey` (which defeats the purpose of idempotency keys for scheduled CI jobs that reuse a deterministic key like `acme-github-prs-2026-06-26T00:00:00Z`). **Recommended amendment**: when the existing status for a given `(org_id, source_system, source_instance, idempotency_key)` is `failed`, a resubmission with the same payload hash should be treated as a **fresh accept** — reset to `status="accepted"`, re-enqueue on the stream, keep the same `ingestion_id` (don't mint a new one; simpler for CLI polling) and increment an `attempt` counter. This is a decision this issue's implementer cannot make unilaterally (it changes CHAOS-2694's status-store write path and CHAOS-2695's accept-path idempotency check) — flagged in `decisionsNeeded`.

---

## Wire schema sketches (per v1 record kind)

These do not exist anywhere yet (confirmed no schema in either plan doc, no code on branch) — this is genuinely new design, built to match the internal models exactly. All record envelopes share this wrapper (validated by `mappings.RecordEnvelope`, itself nested inside CHAOS-2691's batch envelope `records: list[RecordEnvelope]`):

```python
# external_ingest/mappings.py
from __future__ import annotations
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field

RecordKind = Literal[
    "repository.v1", "identity.v1", "team.v1",
    "work_item.v1", "work_item_transition.v1", "work_item_dependency.v1",
    "pull_request.v1", "review.v1", "commit.v1",
]

class RecordEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: RecordKind
    externalId: str = Field(min_length=1, max_length=512)
    data: dict  # validated per-kind below via a discriminated re-parse
```

### `repository.v1` → `Repo`
```python
class RepositoryV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repo: str = Field(min_length=1)          # human-facing URL, stored as repos.repo
    ref: str | None = None
    tags: list[str] = Field(default_factory=list)
    settings: dict = Field(default_factory=dict)
```
Mapping: `repo_id = get_repo_uuid_from_repo(record.externalId)`; `Repo(id=repo_id, repo=data.repo, ref=data.ref, provider="customer_push", repo_tags=data.tags, settings=data.settings)`. **`externalId` is the identity string fed to the UUID derivation — `data.repo` is display-only.** Validation rule: reject if `externalId` is empty/whitespace-only (`code=missing_external_id`).

### `identity.v1` → `insert_identities` dict row
```python
class IdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    displayName: str | None = None
    email: str | None = None
    providerIdentities: dict[str, list[str]] = Field(default_factory=dict)
    teamIds: list[str] = Field(default_factory=list)
    isActive: bool = True
    updatedAt: datetime
```
Mapping: `{"canonical_id": externalId, "org_id": org_id, "display_name": ..., "email": ..., "provider_identities": json.dumps(data.providerIdentities), "team_ids": data.teamIds, "is_active": int(data.isActive), "updated_at": data.updatedAt}`. `identity_uuid` is auto-derived by `insert_identities` itself — do not set it.

### `team.v1` → `insert_teams` dict row
```python
class TeamV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1)
    description: str | None = None
    members: list[str] = Field(default_factory=list)
    updatedAt: datetime
```
Mapping: `{"id": externalId, "org_id": org_id, "name": ..., "description": ..., "members": ..., "updated_at": ..., "provider": "customer_push", "is_active": 1}`. `team_uuid` auto-derived.

### `work_item.v1` → `WorkItem`
```python
class WorkItemV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field(min_length=1)
    type: WorkItemType = "unknown"                 # reuse Literal from models.work_items
    status: WorkItemStatusCategory = "unknown"
    statusRaw: str | None = None
    repoExternalId: str | None = None               # optional link to a repository.v1
    projectKey: str | None = None
    projectId: str | None = None
    projectName: str | None = None
    assignees: list[str] = Field(default_factory=list)
    reporter: str | None = None
    createdAt: datetime
    updatedAt: datetime
    startedAt: datetime | None = None
    completedAt: datetime | None = None
    closedAt: datetime | None = None
    labels: list[str] = Field(default_factory=list)
    storyPoints: float | None = None
    url: str | None = None
```
Mapping: `provider` is **not** a per-record field — it's derived from the batch envelope's `source.system`, validated against `{"jira","github","gitlab","linear"}` (see D-provider-note below). `work_item_id = externalId` **as supplied by the customer verbatim** — recommend (in the JSON-Schema description/docs, CHAOS-2692's job) the customer use the same `<provider>:<native-id>` convention native syncs use (e.g. `jira:ABC-123`), but do not enforce a specific prefix format here, only non-empty. `repo_id = get_repo_uuid_from_repo(repoExternalId)` when present, else `None`.
**Provider constraint (new v1 rule, resolves a real gap)**: `WorkItem.provider`/`WorkItemStatusTransition.provider` are `Literal["jira","github","gitlab","linear"]` in `models/work_items.py` — mypy will reject anything else, and nothing in the plan docs addresses this. **Decision: reject (per-record, code=`unsupported_provider`) any `work_item.v1`/`work_item_transition.v1` record whose batch `source.system` is not one of those four literal values.** This is consistent with the plan's non-goal of "custom arbitrary event blob ingestion" — v1 customer-push work items must map onto a known provider taxonomy; a 5th "generic" bucket is future work, not v1.

### `work_item_transition.v1` → `WorkItemStatusTransition`
```python
class WorkItemTransitionV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workItemId: str = Field(min_length=1)   # must equal a work_item.v1 externalId (cross-batch OK, not validated for existence)
    occurredAt: datetime
    fromStatusRaw: str | None = None
    toStatusRaw: str | None = None
    fromStatus: WorkItemStatusCategory = "unknown"
    toStatus: WorkItemStatusCategory = "unknown"
    actor: str | None = None
```
`externalId` for this kind is a synthetic composite the customer controls (recommend docs suggest `f"{workItemId}@{occurredAt.isoformat()}"`) — it is not itself persisted (no natural externalId column on `work_item_transitions`), only used for rejection-diagnostics addressing.

### `work_item_dependency.v1` → `WorkItemDependency`
```python
class WorkItemDependencyV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sourceWorkItemId: str = Field(min_length=1)
    targetWorkItemId: str = Field(min_length=1)
    relationshipType: str = Field(min_length=1)   # e.g. "blocks", "relates_to" — passthrough, no enum in the internal model either
```

### `pull_request.v1` → `GitPullRequest`
```python
class PullRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repoExternalId: str = Field(min_length=1)
    number: int = Field(gt=0)
    title: str | None = None
    body: str | None = None
    state: str | None = None
    authorName: str | None = None
    authorEmail: str | None = None
    createdAt: datetime
    mergedAt: datetime | None = None
    closedAt: datetime | None = None
    headBranch: str | None = None
    baseBranch: str | None = None
    additions: int | None = None
    deletions: int | None = None
    changedFiles: int | None = None
    firstReviewAt: datetime | None = None
    firstCommentAt: datetime | None = None
    changesRequestedCount: int = 0
    reviewsCount: int = 0
    commentsCount: int = 0
```
`externalId` convention (docs/schema example, not enforced beyond non-empty): `f"{repoExternalId}#{number}"`. `repo_id = get_repo_uuid_from_repo(repoExternalId)`.

### `review.v1` → `GitPullRequestReview`
```python
class ReviewV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repoExternalId: str = Field(min_length=1)
    prNumber: int = Field(gt=0)
    reviewId: str = Field(min_length=1)
    reviewer: str = Field(min_length=1)
    state: str = Field(min_length=1)   # raw passthrough — no normalized enum in the internal model either, confirmed
    submittedAt: datetime
```
`externalId` convention: `f"{repoExternalId}#{prNumber}#{reviewId}"`.

### `commit.v1` → `GitCommit`
```python
class CommitV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repoExternalId: str = Field(min_length=1)
    hash: str = Field(min_length=7, max_length=64)
    message: str | None = None
    authorName: str | None = None
    authorEmail: str | None = None
    authorWhen: datetime
    committerName: str | None = None
    committerEmail: str | None = None
    committerWhen: datetime
    parents: int = 0
```
`externalId` convention: `f"{repoExternalId}@{hash}"`.

---

## Internal interfaces (concrete, copy-pasteable)

```python
# src/dev_health_ops/external_ingest/normalize.py
from __future__ import annotations
import dataclasses
import uuid
from datetime import datetime

@dataclasses.dataclass
class RejectedRecord:
    index: int
    kind: str
    external_id: str | None
    code: str          # e.g. "missing_external_id", "unsupported_provider", "invalid_field"
    message: str
    path: str | None = None   # e.g. "data.createdAt"

@dataclasses.dataclass
class AffectedScope:
    org_id: str
    source_systems: set[str] = dataclasses.field(default_factory=set)
    source_instances: set[str] = dataclasses.field(default_factory=set)
    repo_ids: set[uuid.UUID] = dataclasses.field(default_factory=set)
    team_ids: set[str] = dataclasses.field(default_factory=set)
    record_kinds: set[str] = dataclasses.field(default_factory=set)
    min_ts: datetime | None = None
    max_ts: datetime | None = None

    def observe(self, *, ts: datetime | None, repo_id: uuid.UUID | None = None, team_id: str | None = None) -> None:
        if repo_id is not None:
            self.repo_ids.add(repo_id)
        if team_id is not None:
            self.team_ids.add(team_id)
        if ts is not None:
            self.min_ts = ts if self.min_ts is None else min(self.min_ts, ts)
            self.max_ts = ts if self.max_ts is None else max(self.max_ts, ts)

@dataclasses.dataclass
class NormalizedBatch:
    org_id: str
    ingestion_id: str
    repos: list       # Repo
    identities: list  # dict rows
    teams: list       # dict rows
    work_items: list       # WorkItem
    work_item_transitions: list   # WorkItemStatusTransition
    work_item_dependencies: list  # WorkItemDependency
    pull_requests: list    # GitPullRequest
    reviews: list           # GitPullRequestReview
    commits: list            # GitCommit
    rejections: list[RejectedRecord]
    scope: AffectedScope
    items_received: int

def process_batch(
    *, org_id: str, ingestion_id: str, source_system: str,
    records: list[dict],   # already-parsed JSON, one per RecordEnvelope
) -> NormalizedBatch: ...
```

```python
# src/dev_health_ops/external_ingest/status_reporter.py
from __future__ import annotations
from typing import Literal, Protocol
from .normalize import RejectedRecord

class StatusReporter(Protocol):
    async def mark_processing(self, *, ingestion_id: str, org_id: str) -> None: ...
    async def record_rejections(self, *, ingestion_id: str, org_id: str, rejections: list[RejectedRecord]) -> None: ...
    async def complete(
        self, *, ingestion_id: str, org_id: str,
        status: Literal["completed", "partial", "failed"],
        items_accepted: int, items_rejected: int,
        error_summary: str | None = None,
    ) -> None: ...

class LoggingStatusReporter:
    """No-op-but-logs default used until CHAOS-2694's Postgres-backed reporter is wired in."""
    ...
```

```python
# src/dev_health_ops/external_ingest/processor.py
from __future__ import annotations

async def process_stream_entry(
    entry_payload: dict,           # {"ingestion_id", "org_id", "source_system", "source_instance",
                                    #  "schema_version", "idempotency_key", "payload": "<json records array>"}
    *,
    clickhouse_dsn: str,
    status_reporter: "StatusReporter | None" = None,
    recompute_enqueue=None,        # defaults to external_ingest.recompute.enqueue_bounded_recompute
) -> "ProcessResult": ...
```

```python
# src/dev_health_ops/external_ingest/recompute.py
from __future__ import annotations
from .normalize import AffectedScope

def enqueue_bounded_recompute(scope: AffectedScope) -> None:
    """Best-effort; never raises. See Design Decision D8."""
    ...
```

---

## Redelivery and retry semantics (explicit answer to the FOCUS question)

- **Delivery guarantee from the stream**: at-least-once at the "this ingestion_id needs a processing attempt" granularity. A crash mid-processing before ACK, or an XREADGROUP timeout, can cause the same entry to be delivered again on a future poll **only if it was never ACKed and something reclaims it** — and nothing in this codebase reclaims pending entries today (confirmed: no `XCLAIM`/`XAUTOCLAIM` call anywhere; `StreamConsumer.consume()` always reads `id=">"`). So in practice: **the shared base class's current behavior already ACKs every entry unconditionally after one attempt** (success or DLQ) — this issue's `ExternalIngestStreamConsumer` should keep that shape (don't fight the base class), and treat true redelivery as **not available**.
- **Given that, durability = (persisted status + safe customer resubmission)**, not stream-level redelivery. This is the correct interpretation of the plan's "Reprocessing must be safe" requirement — "reprocessing" means a *customer* reprocessing (same idempotency key resubmit) or a *manual replay* (an operator re-driving the batch from the DLQ payload, e.g. via a future `dev-hops push replay <ingestion_id>` — not in v1 scope, note as a natural follow-up for CHAOS-2699/2694), not automatic silent stream redelivery.
- **Effective exactly-once semantics come from ReplacingMergeTree upserts at the sink layer** (D10), not from anything at the stream/Celery layer. Document this explicitly in the new `docs/architecture/external-ingest-worker.md` doc (see Files section) so nobody later "fixes" the ACK-always behavior expecting stream-level exactly-once — it was never the durability mechanism.
- **In-process retry (3 attempts, 2s/4s/8s) is for transient infra failures only** (ClickHouse/Postgres connection errors caught by type, e.g. `clickhouse_connect.driver.exceptions.Error`, `ConnectionError`, `TimeoutError`, `asyncpg`/SQLAlchemy `OperationalError`) — never for validation failures, which are always immediate per-record rejections.

---

## Worker runtime, queue, and beat wiring — files to create/modify

### New files (this issue)
- `src/dev_health_ops/external_ingest/__init__.py`
- `src/dev_health_ops/external_ingest/mappings.py` — Pydantic wire models + pure `map_*` functions (one per kind).
- `src/dev_health_ops/external_ingest/validate.py` — `validate_record(kind, external_id, data, *, source_system) -> RejectedRecord | None`, `validate_envelope(...)` re-exported for CHAOS-2691.
- `src/dev_health_ops/external_ingest/normalize.py` — `process_batch(...)`, `RejectedRecord`, `AffectedScope`, `NormalizedBatch` (as sketched above).
- `src/dev_health_ops/external_ingest/status_reporter.py` — `StatusReporter` Protocol + `LoggingStatusReporter`.
- `src/dev_health_ops/external_ingest/recompute.py` — `enqueue_bounded_recompute(scope)`.
- `src/dev_health_ops/external_ingest/processor.py` — `process_stream_entry(...)`, `ProcessResult` dataclass.
- `src/dev_health_ops/api/external_ingest/consumer.py` — `ExternalIngestStreamConsumer(StreamConsumer)`, subclassing exactly like `IngestStreamConsumer`/`ProductTelemetryStreamConsumer`; overrides `handle_entries` to process one full batch per stream entry (not flattened items) and to ACK-after-DLQ-on-terminal-failure per D9. Lives under `api/external_ingest/` (not `external_ingest/`) to mirror the existing `api/ingest/consumer.py` vs `api/ingest/persist.py` split — the API-adjacent module owns stream mechanics, the domain package owns pure normalization. **Coordinate the exact path with whoever implements CHAOS-2693**, since `api/external_ingest/` is nominally that issue's directory; if 2693 lands first, add this file there instead of creating a parallel structure.
- `src/dev_health_ops/workers/external_ingest_tasks.py` — the Celery task `run_external_ingest_consumer`, constructing the real `PostgresStatusReporter` if CHAOS-2694 has landed (import guarded / lazy) else falling back to `LoggingStatusReporter`.

### Modified files (this issue)
- `src/dev_health_ops/workers/config.py`:
  - Add `"external_ingest": {}` to `task_queues`.
  - Add `"dev_health_ops.workers.tasks.run_external_ingest_consumer"` to `late_ack_excluded_tasks` (matches `run_ingest_consumer`/`run_product_telemetry_consumer` precedent — these self-looping stream-drain tasks are explicitly exempted from `acks_late`).
  - Add beat entry:
    ```python
    "process-external-ingest-streams": {
        "task": "dev_health_ops.workers.tasks.run_external_ingest_consumer",
        "schedule": stream_consumer_schedule_seconds,
        "kwargs": {"max_iterations": stream_consumer_max_iterations},
        "options": {"queue": "external_ingest", "expires": stream_consumer_expires_seconds},
    },
    ```
- `src/dev_health_ops/workers/tasks.py` — re-export `run_external_ingest_consumer` per the existing flat-namespace convention.
- `compose.yml` — new service `worker-external-ingest` (copy `worker-ingest`'s shape exactly, `-Q external_ingest --concurrency=1`); update the topology comment block above `worker: &worker-base` to mention it.
- `tests/test_compose_config.py` — no code change needed if the new queue is added to both `task_queues` and some compose `-Q` list, but re-run it locally to confirm (`test_compose_workers_cover_every_celery_queue` will fail otherwise).
- `docs/architecture/external-ingest-worker.md` (new) — document D2 (queue isolation rationale), D9/D10/D11 (ACK-always + upsert-based durability, no stream redelivery), and the two-ClickHouse-client split (D6), per the "document decisions in the same changeset" house rule. Link from `docs/architecture/data-pipeline.md` if that file has a "see also" section (check when writing).

### Files this issue depends on but does not create (interface contracts only)
- `src/dev_health_ops/api/external_ingest/streams.py` (CHAOS-2693) — must produce stream entries matching the payload shape `processor.process_stream_entry` expects (see Internal interfaces above): `{"ingestion_id", "org_id", "source_system", "source_instance", "schema_version", "idempotency_key", "payload": "<json>"}`, on stream key `external-ingest:<org_id>:batches`, DLQ `external-ingest:<org_id>:dlq`. **Confirm this exact field set with 2693's implementer before writing `processor.py`'s parsing code** — it's this issue's single biggest external-contract risk (see Risks).
- `dev_health_ops.external_ingest.status_reporter.PostgresStatusReporter` (CHAOS-2694) — must satisfy the `StatusReporter` Protocol above.

---

## Test plan

### Unit tests (no live services), `tests/external_ingest/`
- `test_mappings.py` — one test per kind: valid wire dict → correct internal dataclass/dict fields (assert exact field values, not just "no exception"); one invalid-shape test per kind (missing required field → `RejectedRecord` with expected `code`).
- `test_validate.py` — `work_item.v1`/`work_item_transition.v1` rejected when `source_system` not in `{jira,github,gitlab,linear}` (`code=unsupported_provider`); max-batch-size passthrough (validate.py should not itself enforce max-records — that's CHAOS-2691's job at accept time — assert this module doesn't duplicate that check, to avoid two systems disagreeing on the limit).
- `test_normalize.py` — `process_batch` with a mixed valid/invalid batch: assert accepted records normalize correctly, rejected records appear in `rejections` with correct `index`, and `AffectedScope` accumulates `repo_ids`/`min_ts`/`max_ts` correctly across kinds. Assert `repo_id` for a `pull_request.v1` referencing `repoExternalId="X"` equals `get_repo_uuid_from_repo("X")` bit-for-bit — this is the load-bearing dedup-safety property from D4.
- `test_processor.py` — mock `ClickHouseStore`/`ClickHouseMetricsSink` (patch `create_store`/`create_sink`); assert:
  - happy path calls each expected sink method once with the right rows;
  - a per-record validation failure does not prevent other records/kinds from being written (partial-failure);
  - a simulated `ConnectionError` from a sink call triggers exactly 3 attempts with the documented backoff (use `monkeypatch` on `time.sleep`/`asyncio.sleep`, same style as `tests/test_ingest_consumer_backoff.py`) then calls `status_reporter.complete(status="failed", ...)`.
  - `status_reporter.mark_processing` is called (and "committed", i.e. the fake records a commit flag) **before** any sink write is attempted.
- `test_recompute.py` — assert `enqueue_bounded_recompute` calls `run_daily_metrics.delay`/`dispatch_investment_materialize_partitioned.delay`/`run_work_graph_build.delay` with kwargs matching `inspect.signature(task.run)` for each (per `reference_celery_signature_contract.md` — copy the pattern from `tests/test_dispatch_outbox.py`); assert a raised exception inside enqueue is swallowed (best-effort) and logged, never propagates.
- `test_consumer.py` (in `tests/api/external_ingest/` or alongside CHAOS-2693's stream tests) — `ExternalIngestStreamConsumer.handle_entries` ACKs the entry both on success and on terminal system failure (never leaves it unacked — assert `xack` is always called), and calls `move_to_dlq` only on terminal failure.

### Live-ClickHouse tests (`@pytest.mark.clickhouse`, `tests/test_external_ingest_worker_live.py`)
Model on `tests/test_rmt_org_id_dedup_live.py` and `tests/test_work_unit_attribution_live.py`. Requires `CLICKHOUSE_URI` env var; skip via `pytest.mark.skipif` otherwise. Must be added to the marker-exclusion filters already present in **both** `unit_tests()` and `ci_tests()` in `ci/run_tests.sh` (verify — the `clickhouse` marker filter is a blanket `-m "not benchmark and not clickhouse"`, so a new file automatically inherits the exclusion, no per-file registration needed beyond using the marker).
- Build a synthetic `external-ingest.v1` batch covering all 9 kinds (reuse the JSON-Schema examples CHAOS-2692 will produce, or hand-write fixtures here first and hand them to 2692 — recommend the latter since this issue's tests will exist before 2692's schema examples).
- Run `normalize.process_batch(...)` → `processor.process_stream_entry(...)`-equivalent direct calls into the sink-writing helpers (don't require the actual Celery/stream machinery for this test — call the persistence layer directly, matching how `test_rmt_org_id_dedup_live.py` calls `ClickHouseMetricsSink` directly).
- Assert each of the 9 target tables has exactly the expected row(s), reading with `FINAL`/`argMax` per the org_id join-predicate house rule.
- **Replay-safety assertion (the load-bearing test for D10)**: run the exact same batch through the pipeline a second time, then `OPTIMIZE TABLE <t> FINAL` on each of the 9 tables, and assert row counts are unchanged (no duplicates) — this is the single test that actually proves "reprocessing is safe," not just "writes succeed."
- **`work_item_transitions` semantic-dedup assertion**: two reprocessing runs with a deliberately different `last_synced`-implying timestamp (simulate a retry) must still read as ONE transition via `semantic_deduped_subquery`, not two.
- **Repo identity-continuity assertion (D4)**: insert a `repository.v1` and a `pull_request.v1` referencing it via `repoExternalId` in the SAME batch but with `records` order = PR-before-repo; assert both resolve to the identical `repo_id` and the PR row's `repo_id` is queryable (no FK violation, since ClickHouse has no real FK enforcement, but the join must still return the row).

### mypy
`WorkItemV1`/`work_item.v1` provider-Literal handling is the highest-risk mypy spot (D-provider-note) — run `mypy` locally on `external_ingest/` before pushing; the `Literal` narrowing from a runtime `source_system: str` check needs an explicit `cast()` or a small `_PROVIDER_LITERALS: frozenset[str]` guard function, not a bare `if x in {...}` (mypy won't narrow a `str` to a `Literal` from a set-membership check against a `frozenset[str]` — use `typing.get_args(WorkItemProvider)` combined with an explicit `if provider not in get_args(WorkItemProvider): raise/reject` followed by `cast(WorkItemProvider, provider)`).

---

## Gate commands (run from the worktree root, in this order)

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration

# 1. Fast local gates (format/lint/types/full unit tier + isolated live-CH stage on a scratch db)
SCRATCH_DB=ci_local_validate_chaos2697 bash ci/local_validate.sh

# 2. mypy, standalone (also run inside local_validate.sh, but run explicitly if iterating on external_ingest/ only)
.venv/bin/mypy --install-types --non-interactive src/dev_health_ops/external_ingest src/dev_health_ops/api/external_ingest src/dev_health_ops/workers/external_ingest_tasks.py

# 3. Targeted unit tests while iterating (fast feedback, before the full gate)
.venv/bin/pytest tests/external_ingest -v --tb=short

# 4. Live-ClickHouse round trip (explicit, since default CI/unit tiers filter this marker out)
CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/ci_local_validate_chaos2697 \
  .venv/bin/pytest tests/test_external_ingest_worker_live.py -v -m clickhouse

# 5. Compose/queue lockstep guard (fails fast if external_ingest queue isn't in some -Q list)
.venv/bin/pytest tests/test_compose_config.py -v
```
Use a **per-issue** `SCRATCH_DB` name (`ci_local_validate_chaos2697`) rather than the script's default, per house convention for parallel-agent isolation — never point `CLICKHOUSE_URI` at the `default` database.

---

## Live verification procedure (against the dev compose stack)

1. Bring up the stack (already-running dev compose is fine; only start what's missing): `docker compose up -d valkey clickhouse postgres api worker-external-ingest beat` (the last two won't exist until this issue's compose.yml change is applied).
2. Confirm the new queue is live: `docker exec dev-health-valkey-1 valkey-cli -n 1 LLEN external-ingest:<org_id>:batches` should return `0` initially (per queue-architecture memory: `llen` = health check primitive).
3. Manually XADD a synthetic batch entry matching CHAOS-2693's payload shape directly via `valkey-cli` (bypassing the not-yet-built API layer, since this issue can be verified before 2691/2693 land):
   ```bash
   docker exec dev-health-valkey-1 valkey-cli -n 1 XADD external-ingest:<org_id>:batches '*' \
     ingestion_id "$(uuidgen)" org_id "<org_id>" source_system github source_instance "github.com/acme" \
     schema_version external-ingest.v1 idempotency_key "manual-verify-1" \
     payload '{"records":[{"kind":"repository.v1","externalId":"github.com/acme/api","data":{"repo":"https://github.com/acme/api"}}]}'
   ```
4. Wait one beat cycle (~30s) or force it: `docker exec dev-health-worker-external-ingest-1 celery -A dev_health_ops.workers.celery_app call dev_health_ops.workers.tasks.run_external_ingest_consumer --kwargs '{"max_iterations": 1}'`.
5. Verify the row landed: `docker exec dev-health-clickhouse-1 clickhouse-client --query "SELECT id, org_id, provider FROM repos FINAL WHERE org_id = '<org_id>'"` — expect `provider = 'customer_push'` and `id` equal to the Python-computed `get_repo_uuid_from_repo("github.com/acme/api")`.
6. Verify no duplicate on replay: re-run step 3 with the identical entry, wait a beat cycle, `OPTIMIZE TABLE repos FINAL`, re-run the query in step 5 — still exactly 1 row.
7. Verify recompute enqueue: `docker exec dev-health-worker-1 celery -A dev_health_ops.workers.celery_app inspect active` (or check `run_daily_metrics`/`dispatch_investment_materialize_partitioned` task logs) shows a task fired scoped to the single repo, not a full-org sweep.

---

## Dependencies on other sub-issues

- **CHAOS-2693** (stream/DLQ): hard interface dependency — this issue's consumer parses whatever payload shape 2693's `streams.py` writes. Confirm field names before finalizing `processor.process_stream_entry`'s parsing code. If 2693 hasn't landed, this issue can still be built/tested (unit tests mock the entry payload directly; the live-ClickHouse test calls `normalize`/`processor` functions directly, bypassing the actual stream).
- **CHAOS-2694** (status/rejections): soft dependency via the `StatusReporter` Protocol — this issue ships with `LoggingStatusReporter` as a placeholder; CHAOS-2694 wires in `PostgresStatusReporter` at the Celery-task construction site (one-line change in `workers/external_ingest_tasks.py`).
- **CHAOS-2695** (idempotency/ownership): D11 flags a needed amendment to its stated rule for `failed`-status resubmission — coordinate before 2695 is marked done, since its current acceptance criteria don't mention a `failed` case.
- **CHAOS-2696** (source registration/tokens): none at the worker layer — the worker trusts accept-time auth.
- **CHAOS-2698** (sink writes): overlapping-by-design — D5 lists the confirmed method mapping; this issue calls those methods directly rather than waiting for 2698, and flags any genuinely-missing helper discovered during implementation (none currently identified) rather than blocking. 2698's own acceptance criteria ("Tests verify ClickHouse rows for each v1 kind") substantially overlaps with this issue's live-DB test — recommend the two issues' owners coordinate to avoid writing the same test twice; this brief's Test plan assumes **this issue writes the live round-trip test**, and 2698 either extends it or is folded into it.
- **CHAOS-2699** (bounded recomputation planner): D8 ships a real (not stubbed) scoped-recompute call using existing tasks; 2699 is expected to generalize/harden it (cross-batch coalescing, its own observability/status-visibility acceptance criteria). This issue's `recompute.py` is the seed 2699 refactors, not a throwaway stub.
- **CHAOS-2691/2692**: D1 asks both to import validation/schema logic from this issue's `validate.py`/`mappings.py` rather than duplicating it — flag to those owners.

---

## Risks

1. **Stream payload contract with CHAOS-2693 is unconfirmed** (no code exists yet on either side) — the biggest single integration risk. Mitigate by defining the exact dict shape in this brief (above) and getting it confirmed/committed before both issues' implementers diverge.
2. **`WorkItemProvider` Literal mismatch** — customer-push work items outside `{jira,github,gitlab,linear}` are rejected in v1; if a design partner needs a 5th "generic" provider bucket, that's a model change (`models/work_items.py`) touching every consumer of `WorkItem.provider`, not a small addition — flag early if this constraint is a blocker for a real customer.
3. **No true stream redelivery (D9)** is a real durability gap relative to what "durable Valkey/Redis stream" sounds like it promises. It's the correct call given the existing `StreamConsumer` base class's actual behavior, but if a future requirement demands true redelivery, that's a change to shared infra (`_stream_consumer.py`) affecting the legacy ingest and product-telemetry consumers too — bigger blast radius than this issue alone. Documented in `docs/architecture/external-ingest-worker.md` so it's a conscious, visible tradeoff.
4. **`worker-external-ingest` is a new compose service** — adds to an already-large worker topology (`worker`, `worker-ingest`, `worker-heavy`, now `worker-external-ingest`) and to `tests/test_compose_config.py`'s coverage checks; low risk but touches shared deploy config, verify with the team that a 4th worker container is acceptable rather than reusing `worker-ingest`'s queue (D2's tradeoff).
5. **`insert_repo` is per-record, not batched** — a batch with hundreds of `repository.v1` records (unlikely in practice; customers push facts about repos they already know, typically few per org) would serialize N awaited calls. Not a correctness risk, a latency one; note as a follow-up if CHAOS-2691's max-batch-size ends up large and repo-heavy.
6. **Recompute fan-out volume**: D8's per-day `run_daily_metrics.delay` loop could message-storm on a large historical backfill batch (e.g. a first-time customer pushing 90 days of history in one batch) — the brief caps this at ~7 days before warning/preferring the partitioned dispatcher, but the exact threshold needs real-world tuning once CHAOS-2700's CLI/export tooling defines typical batch time-window sizes.
