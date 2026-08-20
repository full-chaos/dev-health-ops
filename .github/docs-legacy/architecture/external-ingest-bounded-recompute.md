# Bounded recomputation planner (CHAOS-2699)

Part of the [customer-push ingestion epic](adr-003-external-ingest-rest-boundary.md)
(CHAOS-2690). A customer-pushed batch that lands new git/work-item facts
must trigger the *existing* metrics/work-graph/investment pipeline for the
affected slice -- but never a full-org recompute, and never so eagerly that
a burst of small batches fires the pipeline once per batch. This doc
records the D1-D15 design decisions behind
`src/dev_health_ops/external_ingest/recompute.py`,
`recompute_status.py`, and `workers/external_ingest_recompute.py`.

## The planner is a free function, not a class (D1)

Mirrors the documented house preference (`workers/queues.py` -- the
`SyncDispatchPolicy` class was designed but deliberately never built) and
the working analog `_dispatch_post_sync_tasks`
(`workers/post_sync_dispatch.py`). `plan_recompute()` is pure (scope in,
plan out, no I/O); `dispatch_recompute()` is the impure Celery-signature
builder; `schedule_or_coalesce()` is the public debounce seam.

## Public seam is primitives, not a shared dataclass (CC21)

`RecomputeScope` is internal to `recompute.py`. The function every other
sub-issue calls is:

```python
schedule_or_coalesce(
    *, org_id, source_system, source_instance, ingestion_id,
    repo_ids: set[str], team_ids: set[str],
    window_start: datetime | None, window_end: datetime | None,
    record_kinds: set[str],
)
```

This decouples CHAOS-2699 from CHAOS-2697/2698's own scope-accumulation
data structures -- 2697 maps whatever `AffectedScope` it builds while
normalizing a batch into these kwargs at the end of processing. No sibling
issue imports `RecomputeScope` or calls `RecomputeScope.merge_record()`.

## Debounce via Valkey SETNX, not a Celery beat poll (D3)

Debounce here needs "coalesce writes that land within N seconds, fire
once" -- a SETNX-guarded scheduled flush needs no new beat entry, unlike
the existing 30s-cadence stream-consumer beat pattern (which drains a
*stream*, a different shape of problem). Two keys per
`(org_id, source_system, source_instance)`:

- `external-ingest:recompute:pending:{org}:{system}:{instance}` -- the
  accumulating scope blob (JSON), widened (union of repo/team/kind sets,
  min/max window) on every call within the debounce window.
- `external-ingest:recompute:scheduled:{org}:{system}:{instance}` -- a
  SETNX guard, TTL == the debounce window. Only the caller that acquires it
  schedules the flush; every other caller within the window just widens the
  pending blob.

Client is a plain non-blocking Valkey client (`_get_redis_client()`, same
shape as `api/product_telemetry/streams.py:get_redis_client()`) -- **not**
`get_consumer_redis_client()`, which is reserved for blocking `XREADGROUP`
reads.

**Atomicity (post-adversarial-review fix):** the pending-blob merge and the
guard's SETNX are performed inside a single Valkey `WATCH`/`MULTI`
optimistic transaction (`_MAX_COALESCE_RETRIES` retries on `WatchError`),
not a plain `GET` -> merge -> `SET`. Two truly concurrent
`schedule_or_coalesce()` calls for the same debounce key would otherwise
race: both read the same starting blob, and the second `SET` silently
clobbers the first caller's widened result (and its ingestion_id), even
though only one guard was acquired and scheduled. `WATCH` aborts the
transaction if the pending key changed since the read, forcing a retry
against the now-current value --
`tests/test_external_ingest_recompute_debounce.py::test_concurrent_callers_do_not_lose_either_ingestion_id`
proves both ingestion ids survive a simulated race. Similarly, the flush
task (below) reads the pending blob via `GETDEL` (one atomic command)
rather than a separate `GET` then `DELETE`, and deliberately never
explicitly deletes the guard key -- see "Flush task cleanup is atomic and
guard-preserving" below.

**Never-silently-drop guarantee:** if Valkey is unavailable (no
`REDIS_URL`, a connection error, or any other exception talking to it),
`schedule_or_coalesce()` degrades to an *immediate synchronous*
`dispatch_and_persist_scope()` call for just that one batch's own scope.
Durability of the *batch itself* is CHAOS-2693's job (503 on stream
enqueue failure); durability of *recompute triggering* here is
best-effort-but-never-silent.

**Known gap (Risk 4, accepted for v1):** the pending blob's TTL
(`max(debounce_seconds * 4, 300s)`) is generous but Valkey maxmemory
eviction under load (DB 1 is shared with streams/impersonation-cache/rate-
limiter) could still drop a scheduled flush's blob between the guard SETNX
and the flush firing. The flush task tolerates a missing blob gracefully
(log + no-op `{"status": "no_pending_scope"}`) rather than raising -- this
under-triggers recompute in that rare case rather than ever blocking
ingestion.

**Flush task cleanup is atomic and guard-preserving (post-adversarial-review
fix):** the original implementation did a plain `GET` then two separate
`DELETE`s (pending blob + guard). That left a window: if the guard's own
TTL expired slightly before this countdown-delayed task actually ran
(broker/worker latency, not a clock mismatch -- the guard TTL and the
countdown are set to the same value but fire independently), a *new*
`schedule_or_coalesce()` call could land in between this task's `GET` and
its `DELETE`, writing a fresher blob (with a new ingestion_id folded in)
that the old task would then delete without ever having read --
recompute for that later batch would be silently lost, and the new guard
it had just acquired would be deleted out from under it too. The fix has
two parts: (1) the flush task reads the pending blob via `GETDEL` -- one
atomic Valkey command, so whatever it reads is exactly what it clears,
with no gap; (2) the flush task never explicitly deletes the guard key at
all -- its own `ex=seconds` TTL governs its lifecycle, so a
`schedule_or_coalesce()` call that lands while the guard is still valid
correctly coalesces into the *current* flush's blob (read atomically via
the same `GETDEL`, since Valkey serializes commands) instead of racing a
separate cleanup step for a key this task has no reason to touch.
`tests/test_external_ingest_recompute_flush_task.py` covers both: the
GETDEL leaves the key empty in one step, and the guard key survives a
flush run untouched.

## Hard invariant: never call investment materialize with empty scope (D4)

`dispatch_investment_materialize_partitioned(repo_ids=None, team_ids=None)`
resolves to `fetch_work_graph_edges(repo_ids=None, org_id=...)` --
**every** repo's work-graph edges for the whole org, unbounded by time
(`work_graph/investment/materialize.py`). That is exactly the "full-org
recompute" the acceptance criteria forbid by default.

`dispatch_recompute()` never calls
`dispatch_investment_materialize_partitioned` unless the plan's
(capped) `repo_ids` or `team_ids` is non-empty; otherwise
`recompute_status = "skipped_no_scope"` for that portion of the plan (the
top-level status only becomes `"skipped_no_scope"` if *nothing at all* got
dispatched -- see "Status derivation" below).

## Per-repo fan-out, single investment call (D5)

`run_daily_metrics` / `run_work_graph_build` only accept a single
`repo_id`, never a list (`workers/metrics_daily.py`,
`workers/work_graph_tasks.py`). For N affected repos, `dispatch_recompute`
fans out N independent `run_daily_metrics -> run_work_graph_build` chains
(`celery.chain`, immutable links), capped at
`EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS` (default 25).
`dispatch_investment_materialize_partitioned` is the one task that accepts
`repo_ids`/`team_ids` lists directly -- called **once** per flush with the
full capped list, never per repo.

## Never reused: `dispatch_daily_metrics_partitioned` / `run_daily_metrics_batch` (D6)

Two disqualifying findings, both verified in `workers/metrics_partitioned.py`:

- `dispatch_daily_metrics_partitioned` discovers repos via
  `SELECT id FROM repos` with **no `org_id` filter** -- not tenant-scoped,
  cannot be reused for a bounded single-org recompute.
- `run_daily_metrics_batch` gates on a `daily_batch` checkpoint and
  **skips entirely** if `(org_id, repo_id, day)` is already COMPLETED --
  exactly what happens after the normal daily beat cycle. A customer push
  arriving later the same day would be silently swallowed until the next
  calendar day.

`dispatch_recompute()` and its test suite assert these two task names (plus
`run_dora_metrics`/`run_complexity_job` -- deferred-v1 kinds per the
kind-routing table below) are never referenced anywhere in
`recompute.py` (negative-space test).

## Record-kind -> job mapping (D7)

| Category | Kinds | Jobs |
|---|---|---|
| `_GIT_KINDS` | `pull_request.v1`, `review.v1`, `commit.v1` | daily metrics + work graph build (per repo) + investment materialize |
| `_WORK_ITEM_KINDS` | `work_item.v1`, `work_item_transition.v1`, `work_item_dependency.v1` | daily metrics + work graph build (per repo, or org+day fallback -- D8) + investment materialize |
| `_TEAM_KINDS` | `identity.v1`, `team.v1` | investment materialize (`team_ids` only) -- daily/work-graph NOT re-run |
| `_REPO_ONLY_KINDS` | `repository.v1` | none -- `recompute_status` stays `not_applicable` |

## D8 org-wide day-bounded fallback: ratified, capped

Work items may have no repo dimension (`WorkItem.repo_id` is nullable --
Jira-native issues have no repo linkage). If `_WORK_ITEM_KINDS` are present
and the accumulated `repo_ids` is empty, `plan_recompute` falls back to
**one** `run_daily_metrics(repo_id=None, day=..., backfill_days=...,
org_id=...)` call -- org-wide but still day/window-bounded by the batch's
own (capped) window, never all-time history. `run_work_graph_build` is
deliberately skipped in this fallback (`repo_id=None` there means "all
repos, 30-day trailing window", ignoring the tighter batch window).

Never taken if `repo_ids` is non-empty (per-repo chains always win); never
taken for git-only kinds with an empty repo scope either -- that
combination is structurally impossible (PR/review/commit records always
carry a resolved repo), and the defensive path is simply "dispatch
nothing", not an org-wide fallback outside the kind category it was
designed for.

## Guardrail caps are env-configurable (D9)

- `EXTERNAL_INGEST_RECOMPUTE_DEBOUNCE_SECONDS` (default `45`).
- `EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS` (default `14`) -- caps
  `run_daily_metrics(backfill_days=...)` and the `work_graph_build`/
  investment `from_date`/`to_date` window. Clamped to
  `[window_end.date() - (cap-1)days, window_end.date()]`; `capped_days`
  is recorded (`recompute_scope.cappedDays`) for operator visibility.
- `EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS` (default `25`) -- caps
  per-repo chain fan-out (stable-sorted, first N). Beyond the cap,
  `capped_repos = true`; the next natural daily beat cycle still catches
  the remaining repos on its normal schedule (accepted v1 limitation, not
  data loss).

## Debounce key granularity: `(org_id, source_system, source_instance)` (D10)

Not per-ingestion, not per-repo. A customer's CI relay typically pushes
many small batches for the *same* source instance in quick succession
(e.g. one batch per PR webhook relay); coalescing at that granularity gets
the biggest win. Different source instances (`github.com/acme` vs.
`gitlab.com/acme`) debounce independently since their repo/team scopes are
disjoint.

## Status persistence: direct SQL, no `postgres` pytest marker (D11)

Per the epic-wide synthesizer reconciliation (CHAOS-2694's sqlite-portable
direct-SQL convention wins over this issue's original proposal): all reads
and writes in `recompute_status.py` go through
`session.execute(text(...), params)`. SQL is dialect-portable -- no
`RETURNING`, no `ON CONFLICT`, and (unlike the brief's original sketch) no
`ANY(:array)` either: `record_recompute_dispatch()` loops over
`ingestion_ids` and issues one `UPDATE ... WHERE ingestion_id = :id` per
coalesced ingestion, matching `status.py`'s own per-row-UPDATE convention
and staying sqlite-compatible. Unit tests run entirely on sqlite via
`Base.metadata.create_all()`; there is no live-Postgres pytest tier for
this module, only the manual live-verification runbook below.

`ExternalIngestBatch`'s new columns declare `server_default=` (not the
ORM's Python-side `default=`), matching migration 0034's
`server_default="not_applicable"` -- but every INSERT `status.py` issues
still passes the recompute columns *explicitly* (never relies on the
server default at insert time), per the status-store doc's established
"explicit bind params, never rely on server_default" convention.

## `emit-then-raise` (D12)

`record_recompute_dispatch()` commits before returning. It is the last
write in the flush task's request path; a subsequent unrelated failure in
the calling Celery task (e.g. an outer retry) must never roll back an
already-decided recompute outcome.

## Recompute dispatch failures never fail ingestion (D13)

`dispatch_recompute()` wraps its entire body in `try/except Exception`
and returns `RecomputeDispatchResult(status="failed", ...)` rather than
raising. `dispatch_and_persist_scope()` (used by both the flush task and
the Valkey-unavailable synchronous fallback) additionally wraps its own
Postgres write in `try/except`, logging rather than raising -- a
status-persistence hiccup must not surface as a task failure after the
Celery jobs have already been durably dispatched (or correctly skipped).

## Idempotent Celery kwargs contract, pinned by test (D14)

`tests/test_external_ingest_recompute_dispatch.py` asserts every kwargs
dict `dispatch_recompute()` builds for `run_daily_metrics`,
`run_work_graph_build`, and `dispatch_investment_materialize_partitioned`
is a subset of `inspect.signature(<task>.run).parameters` -- mocking
`.delay()`/`.apply_async()` alone hides kwarg drift (the task names are
plain strings in `recompute.py`, resolved only by Celery's routing at
publish time, so nothing else catches a renamed/removed kwarg).

## `celery_task_id` is nullable (post-adversarial-review fix)

A `chain(daily_sig, build_sig).apply_async()` result's `.parent` (the
daily-metrics leg's own `AsyncResult`) is not guaranteed to be populated in
every Celery/broker configuration; `dispatch_recompute()` already handles
this defensively (`daily_id = async_result.parent.id if async_result.parent
is not None else None`), but the first cut of migration 0034 and the
`ExternalIngestRecomputeJob` model declared `celery_task_id` `NOT NULL`.
Since the Celery dispatch has *already succeeded* by the time a job record
is built, inserting that record with an unknown id must not fail -- a
`NOT NULL` violation there would raise inside `record_recompute_dispatch()`,
which `dispatch_and_persist_scope()` catches and merely logs, silently
rolling back the **entire** status/job-log write (not just the one job
row) even though the metric tasks were genuinely dispatched. `celery_task_id`
is now nullable in both the migration and the ORM model;
`test_external_ingest_recompute_dispatch.py::test_per_repo_chain_with_no_parent_result_still_dispatches_with_none_task_id`
and
`test_external_ingest_recompute_status_sql.py::test_record_recompute_dispatch_persists_job_with_none_task_id`
cover the `parent=None` path end to end (build stage, then persistence).

## Celery wiring: single owner, this module stays out (D15 / CC20)

The flush task (`flush_external_ingest_recompute`,
`workers/external_ingest_recompute.py`) declares `queue="default"` and its
full dotted name directly on the `@celery_app.task(...)` decorator. It is
**not** re-exported from `workers/tasks.py` and **not** added to
`workers/config.py`'s `late_ack_excluded_tasks` -- both are hot files
CHAOS-2693 owns exclusively this wave. See the `INTEGRATOR TODO` comment at
the top of `external_ingest_recompute.py` for the exact one-line addition
CHAOS-2693 (or whoever merges this PR) makes to `late_ack_excluded_tasks`
at integration time. No `task_queues`/compose change is needed -- it
reuses the existing `default` queue's worker coverage.

**Adversarial-review finding, refuted (task not registered by a fresh
worker):** the review correctly observed that `flush_external_ingest_
recompute` is absent from `celery_app.tasks` after importing only
`dev_health_ops.workers.tasks` -- it only registers once
`workers/external_ingest_recompute.py` itself is imported. This is the
*expected* state of this PR taken in isolation, not an oversight: CC20
pins Celery wiring (including which modules a worker process imports at
startup) as CHAOS-2693's sole responsibility this wave, and the
HOT-FILE RULE this issue was built under explicitly forbids editing
`workers/tasks.py`/`workers/config.py` in this PR. The exact one-line fix
is already documented in the `INTEGRATOR TODO` comment above and is called
out again in this PR's description for CHAOS-2693 (or the wave integrator)
to apply at merge time -- landing it here would both violate the
HOT-FILE RULE and very likely merge-conflict with CHAOS-2693's own
concurrent edits to the same two files.

## Production wiring into the external-ingest worker (out of scope here)

**Adversarial-review finding, refuted (no production caller extracts scope
or calls `schedule_or_coalesce`):** correct as observed, and explicitly
out of scope for CHAOS-2699 per the brief's own boundary: "the durable
stream/worker entrypoint that calls the planner at the end of batch
processing" and "`RecomputeScope` *accumulation* during record
processing" are owned by CHAOS-2697/CHAOS-2698, not this issue. The
Dependencies section is explicit about the direction: CHAOS-2697 is a
*hard dependency on* CHAOS-2699 ("must call `schedule_or_coalesce(scope)`
once at the end of batch processing... this brief defines that exact
interface; 2697's implementer should treat `external_ingest/recompute.py`
as already-specified"), not the reverse. CHAOS-2697 has not landed as of
this PR (parallel wave-3/4 work) -- there is no worker code yet for this
issue to call into. `mark_recompute_pending()` is similarly an optional
seam documented for CHAOS-2697 to adopt, not a self-contained feature of
this module. Wiring this seam into the real worker is CHAOS-2697's
acceptance criteria, not this issue's.

## Status derivation

`recompute_status` on `external_ingest_batches` (enum pinned epic-wide:
`not_applicable | pending | dispatched | skipped_no_scope | failed`,
`server_default 'not_applicable'`):

- `not_applicable` -- the DB default; also the explicit outcome for
  `repository.v1`-only batches (D7) or any batch whose scope never even
  reaches a flush.
- `pending` -- optional, best-effort (`mark_recompute_pending()`,
  `recompute_status.py`): a batch mid-debounce-window, if the caller
  (CHAOS-2697's worker) chooses to call it alongside its own status write.
  `schedule_or_coalesce()` itself takes no `session` parameter (primitives-
  only public seam, CC21) so it cannot write this transition on its own.
- `dispatched` -- at least one Celery job was successfully enqueued this
  flush (daily/work-graph chains and/or the investment call).
- `skipped_no_scope` -- a flush ran, decided recompute *was* warranted by
  kind, but ended up with nothing dispatchable (the structurally-
  impossible git-only-no-repo defensive case, or D4's investment skip when
  it's the only thing that would have fired).
- `failed` -- `dispatch_recompute()` caught an exception building/firing
  signatures.

## `GET /batches/{ingestion_id}` response block (CC21, extends CHAOS-2694's `status.py`)

```json
{
  "recompute": {
    "status": "dispatched",
    "scope": {
      "repoIds": ["..."], "teamIds": [],
      "windowStartedAt": "2026-06-25T00:00:00Z",
      "windowEndedAt": "2026-06-26T00:00:00Z",
      "cappedDays": false, "cappedRepos": false
    },
    "dispatchedAt": "2026-06-26T00:01:15Z",
    "completedAt": "2026-06-26T00:01:15Z",
    "error": null,
    "jobs": [
      {"task": "run_daily_metrics", "taskId": "...", "queue": "metrics", "repoId": "..."},
      {"task": "run_work_graph_build", "taskId": "...", "queue": "metrics", "repoId": "..."},
      {"task": "dispatch_investment_materialize_partitioned", "taskId": "...", "queue": "default", "repoId": null}
    ]
  }
}
```

`jobs` is read from `external_ingest_recompute_jobs`, joined by exact
`(org_id, source_system, source_instance, dispatched_at)` match against the
batch's own `recompute_dispatched_at` -- a flush coalesces N ingestion_ids,
so there is no per-job FK to a single `external_ingest_batches` row; every
job a single flush inserts shares the identical `dispatched_at` timestamp,
which is what makes the join possible.

## Live verification runbook

1. **Migration 0034 up/down/up** against a scratch Postgres DB: `psql \d
   external_ingest_batches` shows the 5 new columns +
   `external_ingest_recompute_jobs`; `downgrade()` drops them cleanly;
   `upgrade()` again is a no-op-safe re-create.
2. **Debounce**, against dev Valkey with a throwaway test org: call
   `schedule_or_coalesce(...)` twice within the window for the same
   `(org, system, instance)` -- `valkey-cli GET
   external-ingest:recompute:pending:...` shows one widened blob;
   `valkey-cli TTL external-ingest:recompute:scheduled:...` proves the
   guard key exists and only the first call scheduled a flush.
3. **Dispatch shapes**, with Celery in eager/mock mode -- confirm chain
   length, caps (25 repos / 14 days), and the empty-scope investment
   refusal, without dispatching real metric jobs against dev data.
4. **GET response**, via a local `uvicorn` against the scratch Postgres --
   confirm the `recompute` block renders with the pinned enum and the
   `jobs` array population.

Full end-to-end (`POST /batches` -> worker -> `GET /batches/{id}` showing
`recompute.status: "dispatched"`) is blocked until CHAOS-2693/2694/2696/
2697/2698 all land; owned by CHAOS-2702's E2E test once available.
