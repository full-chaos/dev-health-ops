# CHAOS-2699 — Bounded recomputation planner — Implementation Brief

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. Migration renumbered **0034** (`down_revision="0033"`; 0032=2696 auth, 0033=2694
>    status store — fixed chain, no runtime reconciliation needed).
> 2. **No new `postgres` pytest marker** (2694's sqlite-portable direct-SQL convention wins
>    epic-wide). Unit tests run on sqlite via Base.metadata; live-Postgres checks are
>    manual runbook steps.
> 3. **No workers/config.py, compose.yml, or workers/tasks.py edits**: the flush task
>    (`dev_health_ops.workers.tasks.flush_external_ingest_recompute`, name pinned) declares
>    `queue="default"` in its decorator; its `late_ack_excluded_tasks` line is added by
>    CHAOS-2693 (single Celery-wiring owner, CC20). Skip the flat-namespace re-export.
> 4. **Public seam is primitives, not a shared dataclass** (decouples from 2697/2698):
>    `schedule_or_coalesce(*, org_id, source_system, source_instance, ingestion_id,
>    repo_ids: set, team_ids: set, window_start, window_end, record_kinds: set)`.
>    `RecomputeScope` stays internal to this module; CHAOS-2697 maps 2698's AffectedScope
>    into these kwargs (no `merge_record()` calls by siblings).
> 5. decisionsNeeded resolved: D8 org-wide-day-bounded fallback RATIFIED but only within
>    the caps (skip + `recompute_status="skipped_no_scope"` beyond them); investment/LLM
>    materialize STAYS in the debounced flush, scoped, `force=False`, never with both
>    repo_ids and team_ids empty (D4 hard invariant stands).
> 6. Landing wave: 3 (needs 2694's 0033 table for the ALTER; consumed by 2697 in wave 4).
> 7. **POST-CRITIQUE (CC21): this issue ALSO owns surfacing recompute_status** —
>    extend the GET batch-detail response block in 2694's `api/external_ingest/status.py`
>    (deliberate cross-wave file touch; 2694 ships wave 2 with no recompute references).
>    Enum pinned EPIC-WIDE: `not_applicable | pending | dispatched | skipped_no_scope |
>    failed` (0034 server_default 'not_applicable'). brief-2714's
>    queued/running/completed union was wrong and has been corrected.

Sub-issue of CHAOS-2690 (External customer-push ingestion API). Owns Phase 4
of `docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md`
("Metric recomputation" section) plus the recompute-visibility slice of the
`2026-06-28` webhooks/setup addendum (Screen 6 "Ingest status" drilldown:
"recompute status").

Repo: `ops` (`src/dev_health_ops`). No web work in this issue.

At time of writing (2026-07-01), **nothing** in CHAOS-2690 is implemented yet
(`src/dev_health_ops/external_ingest/` and `src/dev_health_ops/api/external_ingest/`
do not exist). This brief is written to be buildable in parallel with, and to
define the exact interface contract against, the sibling issues it depends on.

---

## Scope

1. A pure-function **bounded recompute planner**: given an accumulated
   affected-scope (org, source system/instance, repo ids, team ids, record
   kinds, min/max occurred-at), decide which existing metric Celery tasks to
   enqueue, with what kwargs, capped to sane windows/fan-out.
2. A **debounce/coalescing layer** so N small batches landing within a short
   window for the same (org, source_system, source_instance) collapse into
   one recompute dispatch instead of firing per-batch.
3. **Enqueue** via existing Celery queues/tasks — no new queue, no new
   compose worker.
4. **Recompute status persistence** (direct SQL, Postgres) + the response
   shape that `GET /api/v1/external-ingest/batches/{ingestion_id}` (owned by
   CHAOS-2694) must merge in.
5. **Guardrails**: never full-org recompute by default; cap recompute window
   (days) and repo fan-out; explicit skip when scope can't be bounded.
6. Unit tests (mocked Celery signatures, sqlite-backed SQL helpers) +
   `@pytest.mark.clickhouse` tests are NOT needed here (this module never
   touches ClickHouse directly — it only enqueues tasks that do).

## Out of scope

- The status/rejection tables themselves (`external_ingest_batches`,
  `external_ingest_rejections`) — owned by **CHAOS-2694**. This issue adds
  columns to `external_ingest_batches` and a new companion table, but the
  base table's migration is 2694's to land.
- Normalization, sink writes, and the `RecomputeScope` *accumulation* during
  record processing — owned by **CHAOS-2697**/**CHAOS-2698**. This issue only
  defines the accumulator dataclass/API they must call into.
- The durable stream/worker entrypoint that calls the planner at the end of
  batch processing — owned by **CHAOS-2693**/**CHAOS-2697** (the worker
  task). This issue defines the single call site contract
  (`flush_or_schedule_recompute(...)`) that worker code must invoke.
- Web ingest-status "recompute status" UI — owned by **CHAOS-2714**. This
  issue only defines the JSON shape it will consume.
- `deployment.v1`/`incident.v1`/DORA-triggering kinds — deferred v1 kinds
  per the epic; DORA recompute (`run_dora_metrics`) is therefore **never**
  dispatched by this planner in v1 (see Design decision D6).
- File-content-driven complexity recompute (`run_complexity_job`) — v1
  customer-push kinds carry no file diffs/blame data (per
  `reference_complexity_needs_file_contents.md`), so it is deliberately
  never dispatched here (D6).
- A generic "force full recompute" admin action — flagged as a
  `decisionsNeeded` item, not built in v1.

---

## Design decisions

**D1. The planner is a free function, not a class.** Mirrors the documented
house preference (`workers/queues.py:23-25` — CHAOS-2284 "SyncDispatchPolicy"
was designed but deliberately never built as a class) and the working analog
`_dispatch_post_sync_tasks` (`workers/post_sync_dispatch.py`). New module
`src/dev_health_ops/external_ingest/recompute.py` exports `plan_recompute()`
and `dispatch_recompute()` as plain functions over a frozen dataclass.

**D2. Scope accumulates during worker processing, not by re-querying
ClickHouse.** The worker (CHAOS-2697/2698) already derives the deterministic
`repo_id` (`get_repo_uuid_from_repo`, `models/git.py:72`) and stamps `org_id`
per record while normalizing. Re-deriving scope by querying ClickHouse after
the fact would duplicate that logic and risk drift. `RecomputeScope` is a
running accumulator the worker updates once per successfully-persisted
record; this issue defines and owns that dataclass, sibling issues call
`RecomputeScope.merge_record(...)`.

**D3. Debounce via Valkey (`REDIS_URL`, DB 1), not a Celery beat poll.**
There is already a 30s-cadence beat pattern (`stream_consumer_schedule_seconds`)
for *draining a stream*, but debounce here needs "coalesce writes that land
within N seconds, fire once" — a SETNX-guarded scheduled flush is a better
fit and needs no new beat entry. Reuses the same `REDIS_URL`/DB-1 Valkey
instance already shared by streams/impersonation-cache/rate-limiter (per
`recon-streams.md`), via a plain non-blocking client (same shape as
`api/product_telemetry/streams.py:get_redis_client()` — NOT
`get_consumer_redis_client()`, which is reserved for blocking `XREADGROUP`
reads only).

**D4. Never call a metrics task with an unresolved repo/team scope when
`from_date`/`to_date` are also being passed unscoped.** `dispatch_investment_materialize_partitioned`
resolves `repo_ids=None, team_ids=None` → `_resolve_repo_ids` returns `None`
→ `fetch_work_graph_edges(repo_ids=None, org_id=...)` fetches **every repo's
work-graph edges for the whole org, unbounded by time** (verified:
`work_graph/investment/materialize.py:1058-1068`; `from_date`/`to_date` do
not gate the edge-discovery query, only downstream chunk kwargs). That is
exactly the "full-org recompute" the acceptance criteria forbid by default.
**Hard invariant: `dispatch_recompute()` must never call
`dispatch_investment_materialize_partitioned` unless `repo_ids` or `team_ids`
is non-empty.** If a batch's resolved scope has neither, skip investment
recompute entirely and record `recompute_status = "skipped_no_scope"`.

**D5. `run_daily_metrics`/`run_work_graph_build`/`run_dora_metrics` only
accept a single `repo_id`, never a list** (verified:
`workers/metrics_daily.py:104`, `workers/work_graph_tasks.py:83`,
`workers/metrics_extra.py:96`). For N affected repos, fan out N independent
`run_daily_metrics -> run_work_graph_build` chains (one per repo, via
`celery.group`), capped at `EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS`
(default 25). `dispatch_investment_materialize_partitioned` is the one task
in the chain that *does* accept `repo_ids`/`team_ids` lists directly — call
it once per flush with the full (capped) list, not per repo.

**D6. Do NOT reuse `run_daily_metrics_batch`/`dispatch_daily_metrics_partitioned`
for push-triggered recompute.** Two disqualifying findings, both verified in
`workers/metrics_partitioned.py`:
  - `dispatch_daily_metrics_partitioned` discovers repos via
    `SELECT id FROM repos` with **no `org_id` filter at all** — it is not
    tenant-scoped and cannot be reused as-is for a bounded, single-org
    recompute.
  - `run_daily_metrics_batch` gates on `metrics.checkpoints.is_completed(org_id,
    repo_id, "daily_batch", day)` and **skips entirely** if that
    (org, repo, day) checkpoint is already COMPLETED — which is exactly what
    happens after the normal daily beat cycle runs. A customer push arriving
    later the same day would be silently swallowed: metrics would not
    reflect the new data until the next calendar day. This checkpoint
    mechanism is designed for backfill resume-on-failure, not
    "recompute because new facts landed."
  Instead use the plain, ungated `run_daily_metrics` task per repo
  (`skip_finalize=False` default runs its own IC finalize — no separate
  finalize task needed, unlike the partitioned/chord path).

**D7. Record-kind → job mapping**, mirroring `_GIT_TARGETS`/`_WORK_ITEM_TARGETS`
(`workers/task_utils.py:257-258`) but for the 9 v1 record kinds:

| Category | Kinds | Jobs |
|---|---|---|
| `_GIT_KINDS` | `pull_request.v1`, `review.v1`, `commit.v1` | daily metrics + work graph build (per repo) + investment materialize |
| `_WORK_ITEM_KINDS` | `work_item.v1`, `work_item_transition.v1`, `work_item_dependency.v1` | daily metrics + work graph build (per repo, or org+day fallback — see D8) + investment materialize |
| `_TEAM_KINDS` | `identity.v1`, `team.v1` | investment materialize (`team_ids` only) — daily metrics/work-graph are NOT re-run for identity/team-only batches (they don't consume identity edits directly; re-running provides no benefit until the next natural cycle — accepted limitation, not a bug) |
| `_REPO_ONLY_KINDS` | `repository.v1` | none — repo registration alone doesn't feed any metric job; scope is still recorded for completeness |

`_RECOMPUTE_TRIGGER_KINDS = _GIT_KINDS | _WORK_ITEM_KINDS`.

**D8. Work items may have no repo dimension** (`WorkItem.repo_id: uuid.UUID
| None = None` — verified `models/work_items.py:56`; Jira-native issues have
no repo linkage). If `_WORK_ITEM_KINDS` are present and the accumulated
`repo_ids` set is **empty** (not just small), fall back to ONE
`run_daily_metrics(repo_id=None, day=..., backfill_days=..., org_id=...)`
call (org-wide but still day/window-bounded — this is judged acceptable
against "no full-org recompute": it is bounded by the batch's own window,
not by history). Skip `run_work_graph_build` in this fallback (repo_id=None
there means "all repos, 30-day trailing window", ignoring the tighter batch
window — not useful here). Never take this fallback if `repo_ids` is
non-empty; prefer the per-repo path (D5) whenever any repo scope exists.

**D9. Guardrail caps are env-configurable, not hardcoded**, following
`PROVIDER_SYNC_QUEUES_ENABLED`/`workers/config.py:_env_int`-style read
convention:
  - `EXTERNAL_INGEST_RECOMPUTE_DEBOUNCE_SECONDS` (default `45`) — between the
    two existing 30s stream-consumer beat cadences; long enough to coalesce a
    burst of small CLI batches, short enough that customers see status
    change quickly.
  - `EXTERNAL_INGEST_RECOMPUTE_MAX_BACKFILL_DAYS` (default `14`) — caps
    `run_daily_metrics(backfill_days=...)`; a customer backfilling 2 years of
    history in one push must not multiply into thousands of per-repo/day
    ClickHouse scans. Window is clamped to `[max_ts.date() - (cap-1)days,
    max_ts.date()]`; the *actual requested* window is still recorded in
    `recompute_scope` for operator visibility, with `recompute_capped_days =
    true`.
  - `EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS` (default `25`) — caps
    per-repo chain fan-out (D5). Beyond the cap, dispatch only the first N
    (stable-sorted) repo ids, set `recompute_capped_repos = true`, log a
    warning. The next natural daily beat cycle (unaffected by this cap) will
    still catch the remaining repos on its normal schedule — this is an
    accepted v1 limitation for the pathological "one batch touches >25
    distinct repos" case, not a silent data-loss bug.
  Large/historical backfills beyond the cap require a manual
  `dispatch_investment_materialize_partitioned`/`dev-hops backfill` run
  (existing tooling) — call this out in the recompute-status `error_summary`
  when capping occurs (`"window clamped to last 14 days; run a manual
  backfill for older data"`).

**D10. Debounce key granularity is `(org_id, source_system,
source_instance)`**, not per-ingestion or per-repo. Rationale: a customer's
CI relay typically pushes many small batches for the *same* source instance
in quick succession (e.g. one batch per PR webhook relay); coalescing at
that granularity gets the biggest win. Different source instances (e.g.
`github.com/acme` vs `gitlab.com/acme`) debounce independently since their
repo/team scopes are disjoint.

**D11. Status persistence uses direct parameterized SQL, not a new ORM
model**, per the core plan's explicit instruction ("Use direct SQL for API
persistence/status queries... avoid adding ORM-only paths") — this is a
deliberate divergence from the rest of the codebase's SQLAlchemy-ORM Postgres
convention (flagged in `recon-persistence-migrations.md`) and CHAOS-2699 must
follow the same convention CHAOS-2694 uses for consistency across the
feature. Alembic migration DDL still uses `op.*` (schema tooling, not ORM
runtime models); only *query-time* access is raw SQL (`sqlalchemy.text()`
via `get_postgres_session`/`get_postgres_session_sync`).

**D12. `emit-then-raise` / commit-before-raise applies here.** Recompute
status writes happen in the same worker task as the final ingestion-status
update. If a downstream commit needs to survive an exception path (e.g. we
record `recompute_status="failed"` and then re-raise for Celery retry),
`await session.commit()` must be called explicitly before raising — mirrors
CHAOS-2498 (`reference_emit_then_raise_rollback.md`); `get_postgres_session`
rolls back on **any** exception including one raised after an add-only
helper.

**D13. Recompute dispatch failures must never fail the ingestion.** Mirrors
the documented `try/except Exception: logger.exception(...)` convention for
best-effort non-critical dispatches (`post_sync_dispatch.py:250-269`,
`recon-celery-metrics.md` gotcha 9). If `dispatch_recompute()` raises, the
worker task must catch it, mark `recompute_status="failed"`, record
`error_summary`, and still finalize the ingestion's own `status` (accepted
data must not be held hostage by a metrics-dispatch hiccup).

**D14. Idempotent Celery kwargs contract, pinned by test.** Per
`reference_celery_signature_contract.md` and `tests/test_dispatch_outbox.py`'s
`inspect.signature` pattern: a unit test must assert every kwargs dict this
module builds for `run_daily_metrics`, `run_work_graph_build`,
`dispatch_investment_materialize_partitioned` is a subset of
`inspect.signature(<task>.run).parameters` (mocking `.delay`/`apply_async`
hides kwarg drift otherwise — this exact class of bug clobbered ops#847).

**D15. New dispatcher task added to `late_ack_excluded_tasks`.** The flush
task (`flush_external_ingest_recompute`) is a short-lived dispatcher (builds
signatures, doesn't do heavy compute) analogous to
`dispatch_investment_materialize_partitioned`/`dispatch_daily_metrics_partitioned`,
both already in `late_ack_excluded_tasks` (`workers/config.py:36-48`). Add it
there and route it to `queue="default"` (the same queue those two dispatchers
use) — no new queue, no compose changes, `tests/test_compose_config.py`
stays green untouched.

---

## API / DDL / schema sketches

### `RecomputeScope` accumulator (interface contract for CHAOS-2697/2698)

```python
# src/dev_health_ops/external_ingest/recompute.py
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime

_GIT_KINDS = frozenset({"pull_request.v1", "review.v1", "commit.v1"})
_WORK_ITEM_KINDS = frozenset(
    {"work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"}
)
_TEAM_KINDS = frozenset({"identity.v1", "team.v1"})
_REPO_ONLY_KINDS = frozenset({"repository.v1"})
_RECOMPUTE_TRIGGER_KINDS = _GIT_KINDS | _WORK_ITEM_KINDS


@dataclass(frozen=True)
class RecomputeScope:
    """Affected scope accumulated while normalizing/persisting one batch.

    Built incrementally by the external-ingest worker (CHAOS-2697/2698) via
    ``merge_record`` — one call per successfully-persisted record. Never
    re-derived from ClickHouse; the worker already knows the resolved
    repo_id/org_id per record at persist time.
    """

    org_id: str
    source_system: str
    source_instance: str
    repo_ids: frozenset[str] = field(default_factory=frozenset)
    team_ids: frozenset[str] = field(default_factory=frozenset)
    record_kinds: frozenset[str] = field(default_factory=frozenset)
    min_occurred_at: datetime | None = None
    max_occurred_at: datetime | None = None
    record_count: int = 0

    def merge_record(
        self,
        *,
        record_kind: str,
        occurred_at: datetime,
        repo_id: str | None = None,
        team_id: str | None = None,
    ) -> "RecomputeScope":
        return replace(
            self,
            repo_ids=self.repo_ids | ({repo_id} if repo_id else set()),
            team_ids=self.team_ids | ({team_id} if team_id else set()),
            record_kinds=self.record_kinds | {record_kind},
            min_occurred_at=(
                occurred_at
                if self.min_occurred_at is None
                else min(self.min_occurred_at, occurred_at)
            ),
            max_occurred_at=(
                occurred_at
                if self.max_occurred_at is None
                else max(self.max_occurred_at, occurred_at)
            ),
            record_count=self.record_count + 1,
        )

    @classmethod
    def empty(cls, *, org_id: str, source_system: str, source_instance: str) -> "RecomputeScope":
        return cls(org_id=org_id, source_system=source_system, source_instance=source_instance)
```

### Planner + dispatcher

```python
@dataclass(frozen=True)
class RecomputePlan:
    org_id: str
    trigger: bool  # False => nothing to dispatch (e.g. repository.v1-only)
    repo_ids: list[str]
    team_ids: list[str]
    day: str | None          # ISO date, for run_daily_metrics
    backfill_days: int | None
    from_date: str | None    # ISO datetime, for work_graph_build / investment
    to_date: str | None
    capped_days: bool
    capped_repos: bool
    fallback_org_wide_daily: bool  # D8
    skip_investment_no_scope: bool  # D4


def plan_recompute(scope: RecomputeScope) -> RecomputePlan:
    """Pure function: scope -> bounded plan. No I/O, no Celery calls."""
    ...  # implements D5-D9 clamping + D7 kind routing; unit-testable without mocks


@dataclass(frozen=True)
class RecomputeDispatchResult:
    status: str  # "dispatched" | "skipped_no_scope" | "not_applicable" | "failed"
    jobs: list[dict]  # [{"task": str, "task_id": str, "queue": str}]
    capped_days: bool
    capped_repos: bool
    error: str | None = None


def dispatch_recompute(plan: RecomputePlan) -> RecomputeDispatchResult:
    """Impure: builds and fires Celery signatures per D5/D6. Never raises —
    catches and returns status='failed' (D13)."""
    ...


def schedule_or_coalesce(
    scope: RecomputeScope, *, debounce_seconds: int | None = None
) -> None:
    """Called once per finished batch by the external-ingest worker (D3/D10).

    Writes/merges the pending scope blob into Valkey under
    ``external-ingest:recompute:pending:{org_id}:{source_system}:{source_instance}``
    and, iff a SETNX guard key
    ``external-ingest:recompute:scheduled:{org_id}:{source_system}:{source_instance}``
    is newly acquired, schedules
    ``flush_external_ingest_recompute.apply_async(countdown=debounce_seconds,
    kwargs={"org_id":..., "source_system":..., "source_instance":...})``.
    If Valkey is unavailable, degrade to an IMMEDIATE synchronous
    ``dispatch_recompute(plan_recompute(scope))`` call (never silently drop
    recompute — durability of the *batch* is 2693's job via 503; durability
    of *recompute triggering* here is best-effort-but-never-silent).
    """
```

### New Celery task

```python
# src/dev_health_ops/workers/external_ingest_tasks.py (new file, re-exported
# from workers/tasks.py per the flat-namespace convention)

@celery_app.task(
    bind=True,
    max_retries=3,
    queue="default",
    name="dev_health_ops.workers.tasks.flush_external_ingest_recompute",
)
def flush_external_ingest_recompute(
    self,
    org_id: str,
    source_system: str,
    source_instance: str,
) -> dict:
    """Debounce flush: read+clear the pending Valkey blob, plan, dispatch,
    persist recompute status via direct SQL (D11), update
    external_ingest_batches rows in the flushed window."""
```

Register in `workers/config.py`:
```python
late_ack_excluded_tasks = (
    ...,
    "dev_health_ops.workers.tasks.flush_external_ingest_recompute",
)
```
No `task_queues`/compose changes — reuses `"default"`, already covered by the
`worker` container's `-Q` list.

### Postgres DDL (direct SQL, Alembic migration)

**Dependency note**: this migration ALTERs `external_ingest_batches`, which
is created by CHAOS-2694's own migration. Chain `down_revision` onto
whichever revision CHAOS-2694 actually lands as — placeholder below assumes
2694 lands as `"0032"` and this lands as `"0033"`; **verify the actual head
via `dev-hops migrate postgres heads` before writing the real file**, since
parallel sub-issue work racing for migration numbers is a known integration
risk (see Risks).

```python
# src/dev_health_ops/alembic/versions/0033_add_external_ingest_recompute.py
"""Add recompute-visibility columns + job log for customer-push ingestion
(CHAOS-2699).

Extends CHAOS-2694's ``external_ingest_batches`` table with a summary of the
bounded recompute triggered for that batch, plus a companion table logging
each individual Celery dispatch (a single flush can fan out to N per-repo
chains + one investment-materialize call).

Guarded per the 0025/0030/0031 create-if-missing convention so a partial
rerun resumes cleanly.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "0033"
down_revision: str | None = "0032"  # <-- verify against CHAOS-2694's actual revision
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_BATCHES_TABLE = "external_ingest_batches"
_JOBS_TABLE = "external_ingest_recompute_jobs"


def upgrade() -> None:
    _add_column_if_missing(
        _BATCHES_TABLE,
        sa.Column(
            "recompute_status",
            sa.Text(),
            nullable=False,
            server_default="not_applicable",
        ),
    )
    _add_column_if_missing(
        _BATCHES_TABLE, sa.Column("recompute_scope", JSONB(), nullable=True)
    )
    _add_column_if_missing(
        _BATCHES_TABLE,
        sa.Column("recompute_dispatched_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        _BATCHES_TABLE,
        sa.Column("recompute_completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        _BATCHES_TABLE, sa.Column("recompute_error", sa.Text(), nullable=True)
    )

    if not _table_exists(_JOBS_TABLE):
        op.create_table(
            _JOBS_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("source_instance", sa.Text(), nullable=False),
            # FK-less by design (mirrors provider_rate_limit_observations,
            # migration 0031): a flush coalesces N ingestions, so there is no
            # single ingestion_id owner; batches touched by a flush are
            # recorded on their own recompute_scope/recompute_status columns
            # instead, keyed by (org_id, source_system, source_instance) +
            # time range.
            sa.Column("celery_task_name", sa.Text(), nullable=False),
            sa.Column("celery_task_id", sa.Text(), nullable=False),
            sa.Column("queue", sa.Text(), nullable=False),
            sa.Column("repo_id", sa.Text(), nullable=True),
            sa.Column("status", sa.Text(), nullable=False, server_default="dispatched"),
            sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
    _create_index_if_missing(
        "ix_external_ingest_recompute_jobs_scope",
        _JOBS_TABLE,
        ["org_id", "source_system", "source_instance", "dispatched_at"],
    )


def downgrade() -> None:
    if _table_exists(_JOBS_TABLE):
        op.drop_table(_JOBS_TABLE)
    for col in (
        "recompute_status",
        "recompute_scope",
        "recompute_dispatched_at",
        "recompute_completed_at",
        "recompute_error",
    ):
        if _table_exists(_BATCHES_TABLE) and col in _column_names(_BATCHES_TABLE):
            op.drop_column(_BATCHES_TABLE, col)


# _table_exists / _column_names / _add_column_if_missing / _create_index_if_missing:
# copy verbatim from 0031_add_provider_rate_limit_observations.py (same helpers).
```

### Direct-SQL status read/write helpers

```python
# src/dev_health_ops/external_ingest/recompute_status.py
from sqlalchemy import text

async def record_recompute_dispatch(
    session, *, org_id: str, ingestion_ids: list[str], scope: RecomputeScope,
    result: RecomputeDispatchResult,
) -> None:
    await session.execute(
        text(
            """
            UPDATE external_ingest_batches
            SET recompute_status = :status,
                recompute_scope = CAST(:scope AS JSONB),
                recompute_dispatched_at = now(),
                recompute_error = :error
            WHERE org_id = :org_id AND ingestion_id = ANY(:ingestion_ids)
            """
        ),
        {
            "status": result.status,
            "scope": json.dumps(_scope_to_json(scope, result)),
            "error": result.error,
            "org_id": org_id,
            "ingestion_ids": ingestion_ids,
        },
    )
    for job in result.jobs:
        await session.execute(
            text(
                """
                INSERT INTO external_ingest_recompute_jobs
                    (id, org_id, source_system, source_instance,
                     celery_task_name, celery_task_id, queue, repo_id,
                     status, dispatched_at)
                VALUES
                    (gen_random_uuid(), :org_id, :source_system, :source_instance,
                     :task_name, :task_id, :queue, :repo_id, 'dispatched', now())
                """
            ),
            {"org_id": org_id, **job},
        )
    # D12: commit BEFORE any caller re-raises on a subsequent step.
    await session.commit()
```

`GET /api/v1/external-ingest/batches/{ingestion_id}` (CHAOS-2694) response
gains:
```json
{
  "...": "...",
  "recompute": {
    "status": "dispatched",
    "scope": {
      "repoIds": ["..."],
      "teamIds": [],
      "windowStartedAt": "2026-06-25T00:00:00Z",
      "windowEndedAt": "2026-06-26T00:00:00Z",
      "cappedDays": false,
      "cappedRepos": false
    },
    "dispatchedAt": "2026-06-26T00:01:15Z",
    "jobs": [
      {"task": "run_daily_metrics", "taskId": "...", "queue": "metrics"},
      {"task": "run_work_graph_build", "taskId": "...", "queue": "metrics"},
      {"task": "dispatch_investment_materialize_partitioned", "taskId": "...", "queue": "default"}
    ]
  }
}
```
`status` enum: `not_applicable | pending | dispatched | skipped_no_scope | failed`.

---

## Files to create/modify

- `src/dev_health_ops/external_ingest/recompute.py` — new: `RecomputeScope`,
  `RecomputePlan`, `plan_recompute()`, `dispatch_recompute()`,
  `schedule_or_coalesce()`, kind constants (D7).
- `src/dev_health_ops/external_ingest/recompute_status.py` — new: direct-SQL
  read/write helpers (D11/D12).
- `src/dev_health_ops/external_ingest/__init__.py` — new, if not already
  created by CHAOS-2697 (package init; coordinate to avoid a conflicting
  empty-file PR race).
- `src/dev_health_ops/workers/external_ingest_tasks.py` — new:
  `flush_external_ingest_recompute` Celery task.
- `src/dev_health_ops/workers/tasks.py` — modify: re-export
  `flush_external_ingest_recompute` (flat-namespace convention).
- `src/dev_health_ops/workers/config.py` — modify: add task name to
  `late_ack_excluded_tasks` (D15).
- `src/dev_health_ops/alembic/versions/0033_add_external_ingest_recompute.py`
  — new (revision number to be confirmed against actual head at
  implementation time).
- `ops/docs/architecture/external-ingest-bounded-recompute.md` — new: record
  D1-D15 as the durable architecture decision doc in the same changeset
  (per house rule: document decisions in `ops/docs/architecture/*`, not only
  `.remember/`).
- `tests/test_external_ingest_recompute_planner.py` — new: pure `plan_recompute()`
  unit tests (no mocks needed — pure function).
- `tests/test_external_ingest_recompute_dispatch.py` — new: `dispatch_recompute()`
  tests, mocking `celery_app.signature`/`chain`/`group`/`send_task` exactly
  like `tests/test_post_sync_investment_dispatch.py`/`test_dispatch_outbox.py`
  (D14 kwargs-contract assertions).
- `tests/test_external_ingest_recompute_debounce.py` — new: `schedule_or_coalesce()`
  against a fake/`fakeredis`-style Valkey client (see Test plan).
- `tests/test_external_ingest_recompute_status_sql.py` — new: direct-SQL
  helpers against sqlite `:memory:`-compatible raw SQL... **caveat**: raw
  `text()` SQL with Postgres-specific `JSONB`/`gen_random_uuid()`/`ANY(:array)`
  is not sqlite-portable (unlike the ORM-based `test_dispatch_outbox.py`
  fixture). Mark this file `@pytest.mark.clickhouse`-adjacent but actually
  needs **Postgres**, not ClickHouse — there is no existing `@pytest.mark.postgres`
  marker in this repo (confirmed: `pytest.ini` only registers `benchmark` and
  `clickhouse`). **Decision: reuse the `clickhouse` marker's sibling
  convention by adding a new `postgres` marker** (register in `pytest.ini`,
  exclude via `-m "not benchmark and not clickhouse and not postgres"` in
  both `unit_tests()` and `ci_tests()` in `ci/run_tests.sh`) — flag this as a
  cross-cutting change other CHAOS-2690 sub-issues (2694, 2696) will also
  need, since they're the first Postgres-direct-SQL modules in this codebase
  requiring a live DB in tests. **Escalate this specific point** — see
  `decisionsNeeded`.

---

## Test plan

### Unit (pure, no live services)

- `plan_recompute()`:
  - `_GIT_KINDS` only, single repo → per-repo chain plan, no fallback.
  - `_WORK_ITEM_KINDS` only, empty `repo_ids` → `fallback_org_wide_daily=True`,
    `work_graph_build` skipped (D8).
  - `_TEAM_KINDS` only → `trigger=False` for daily/work-graph, investment plan
    with `team_ids` only.
  - `_REPO_ONLY_KINDS` only → `trigger=False` entirely, `not_applicable`.
  - Mixed kinds, both `repo_ids` and `team_ids` populated → per-repo chains +
    ONE investment call with the union list.
  - Window spanning 40 days → `capped_days=True`, clamped to 14 (default env).
  - 60 distinct repo_ids → `capped_repos=True`, only first 25 dispatched.
  - Zero `repo_ids` AND zero `team_ids` with `_GIT_KINDS`/`_WORK_ITEM_KINDS`
    present but no work-item fallback triggerable (i.e. git-only kinds with
    no repo — should be structurally impossible since PR/review/commit
    always carry repo, but assert the defensive skip path anyway) →
    `skip_investment_no_scope=True` (D4 hard invariant test).

- `dispatch_recompute()`:
  - Mock `celery_app.signature`/`chain`/`group`.apply_async` exactly like
    `tests/test_post_sync_investment_dispatch.py`. Assert:
    - `run_daily_metrics` kwargs subset of `inspect.signature(run_daily_metrics.run).parameters` (D14).
    - `run_work_graph_build` and `dispatch_investment_materialize_partitioned`
      likewise.
    - `dispatch_investment_materialize_partitioned` is called exactly ONCE
      per flush regardless of repo count (D5), with the full capped
      `repo_ids`/`team_ids` lists.
    - `run_dora_metrics` / `run_complexity_job` are NEVER referenced anywhere
      in this module (D6 negative-space test — grep-assert on the module
      source or assert the mock was never called).
  - Exception inside signature-building → returns `RecomputeDispatchResult(status="failed", ...)`, never raises (D13).

- `schedule_or_coalesce()` (fakeredis or a minimal hand-rolled fake client
  implementing `get/set(nx=..., ex=...)/delete`):
  - First call for a scope key acquires the SETNX guard and schedules
    `flush_external_ingest_recompute.apply_async(countdown=45, ...)` (mock
    `.apply_async`).
  - Second call within the debounce window does NOT re-schedule (guard
    already held), but DOES widen the pending blob's min/max timestamps.
  - Simulated Valkey connection error → falls back to synchronous
    `dispatch_recompute(plan_recompute(scope))` inline (never silently
    drops).

### Live-DB (`@pytest.mark.postgres`, new marker — see decisionsNeeded)

- Apply migration `0033` against a scratch Postgres DB (or whatever
  fixture CHAOS-2694 establishes for `external_ingest_batches`); round-trip
  `record_recompute_dispatch()` and assert the row + job log persist and
  read back correctly, including `JSONB` round-trip of `recompute_scope`.
- Only runnable once CHAOS-2694's migration exists in the branch; until
  then, this test file should be written but may `pytest.skip` with a clear
  reason if `external_ingest_batches` doesn't exist yet (detect via
  `sa.inspect(engine).has_table(...)` at fixture setup, not an import-time
  guess).

No `@pytest.mark.clickhouse` tests are needed for this issue — the planner
never touches ClickHouse directly (it only builds/dispatches Celery
signatures for tasks that do; those tasks' own ClickHouse behavior is
already covered by existing tests for `run_daily_metrics`/`run_work_graph_build`/
`dispatch_investment_materialize_partitioned`).

---

## Gate commands

```bash
# ops — from the worktree root
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration

# Per-issue scratch DB isolation (avoid clobbering another agent's scratch run)
SCRATCH_DB=ci_local_validate_2699 bash ci/local_validate.sh

# mypy, literal CI command
mypy --install-types --non-interactive .

# targeted unit runs while iterating
pytest tests/test_external_ingest_recompute_planner.py \
       tests/test_external_ingest_recompute_dispatch.py \
       tests/test_external_ingest_recompute_debounce.py -v

# once the new `postgres` marker exists and a scratch PG is up:
pytest tests/test_external_ingest_recompute_status_sql.py -m postgres -v

# queue/compose lockstep guard (should stay green — no new queue added)
pytest tests/test_compose_config.py -v

# celery kwargs-contract guard
pytest tests/test_external_ingest_recompute_dispatch.py -k signature -v
```

No web gate commands apply to this issue (no `dev-health-web` files
touched). CHAOS-2714 (web setup screens) will need `ci/run_tests.sh
format/quality/unit` plus a targeted Playwright spec against the
`recompute` field shape defined above once it consumes it.

---

## Live verification procedure

Since CHAOS-2693/2694/2696/2697/2698 are all unimplemented as of this
writing, full end-to-end live verification (`POST /batches` → worker →
`GET /batches/{id}` showing `recompute.status: "dispatched"`) is **blocked**
until those land. Two verification tiers:

**Tier 1 — planner/dispatcher in isolation (buildable now, no dependencies):**
```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
.venv/bin/python -c "
from datetime import datetime, timezone
from dev_health_ops.external_ingest.recompute import RecomputeScope, plan_recompute, dispatch_recompute

scope = RecomputeScope.empty(org_id='<real-org-uuid>', source_system='github', source_instance='github.com/acme')
scope = scope.merge_record(record_kind='pull_request.v1', occurred_at=datetime.now(timezone.utc), repo_id='<real-repo-uuid-from-repos-table>')
plan = plan_recompute(scope)
print(plan)
result = dispatch_recompute(plan)
print(result)
"
# Then confirm real Celery task IDs actually ran:
docker exec dev-health-clickhouse-1 clickhouse-client -q "SELECT org_id, repo_id, day, computed_at FROM user_metrics WHERE org_id = '<real-org-uuid>' ORDER BY computed_at DESC LIMIT 5"
```
Use a real `org_id`/`repo_id` already present in the dev compose ClickHouse
(`docker exec dev-health-clickhouse-1 clickhouse-client -q "SELECT id, org_id, full_name FROM repos LIMIT 5"`)
per `feedback_verify_with_real_data_first.md` — don't invent UUIDs.

**Tier 2 — debounce behavior against real Valkey:**
```bash
docker exec dev-health-valkey-1 valkey-cli -n 1 KEYS "external-ingest:recompute:*"
# fire schedule_or_coalesce() twice in <45s from a python -c snippet, confirm
# only ONE guard key + ONE widened pending blob, via:
docker exec dev-health-valkey-1 valkey-cli -n 1 GET "external-ingest:recompute:pending:<org>:<system>:<instance>"
docker exec dev-health-valkey-1 valkey-cli -n 1 TTL "external-ingest:recompute:scheduled:<org>:<system>:<instance>"
```

**Tier 3 — full E2E** (after CHAOS-2693/2694/2696/2697/2698 merge): owned by
CHAOS-2702 (E2E customer-push ingestion test); this issue's planner should
be re-verified as part of that E2E once available, confirming
`GET /api/v1/external-ingest/batches/{id}` actually surfaces the `recompute`
block end-to-end.

---

## Dependencies on other sub-issues

- **CHAOS-2694** (Ingest status and rejected-record diagnostics) — HARD
  dependency: owns `external_ingest_batches` base table/migration that this
  issue's migration ALTERs, and owns the `GET /batches/{ingestion_id}`
  endpoint that must merge in the `recompute` response block defined here.
- **CHAOS-2697** (External ingest worker normalization) — HARD dependency:
  must call `RecomputeScope.merge_record(...)` per persisted record and
  `schedule_or_coalesce(scope)` once at the end of batch processing. This
  brief defines that exact interface; 2697's implementer should treat
  `external_ingest/recompute.py` as already-specified.
- **CHAOS-2698** (External ingest sink writes) — soft dependency: the
  resolved `repo_id` used for scope accumulation must match the
  `get_repo_uuid_from_repo` derivation 2698 uses for sink writes, or scope
  and persisted data will disagree about which repo was touched.
- **CHAOS-2693** (Durable stream and DLQ) — soft dependency: the worker task
  that calls into this module runs on whatever queue/consumer 2693
  establishes; no direct code dependency but affects where
  `schedule_or_coalesce()` is invoked from.
- **CHAOS-2696** (Source registration and token scopes) — no code
  dependency; only relevant in that `source_system`/`source_instance` values
  used as debounce/scope keys here must match whatever registration
  normalizes them to (e.g. case sensitivity of `github.com/acme`).
- **CHAOS-2714** (Web setup screens) — downstream consumer of the
  `recompute` JSON shape defined here; no blocking dependency in this
  direction.

---

## Risks

1. **Migration-number race.** Multiple CHAOS-2690 sub-issues (2694, 2699,
   possibly 2695/2696) will each want to add Alembic migrations in parallel
   worktrees. `down_revision` chains will collide unless sequenced/rebased
   carefully. Mitigate by running `dev-hops migrate postgres heads` at
   actual implementation time (not now) and treating the `"0032"`/`"0033"`
   numbers in this brief as placeholders, not final.
2. **New `postgres` pytest marker is a cross-cutting change** touching
   `pytest.ini` and `ci/run_tests.sh` shared by the whole repo, not just this
   issue — if CHAOS-2694 lands first and already introduces it (likely, since
   2694 owns the base table and has the same live-DB test need), CHAOS-2699
   should reuse theirs rather than add a second one. Coordinate order of
   landing; don't duplicate.
3. **D8's org-wide daily-metrics fallback** (work-item batch with no repo
   scope) is a deliberate, bounded exception to "no full-org recompute by
   default." If product/architecture disagrees with treating "day-bounded,
   all-repos" as acceptable, this needs to become a hard skip instead
   (documented `decisionsNeeded` below).
4. **Debounce blob loss on Valkey restart/eviction** (maxmemory eviction
   under load, since DB 1 is shared with streams/cache/rate-limiter) could
   drop a scheduled flush silently. The synchronous fallback-on-error path
   (D3) only covers connection *errors*, not silent key eviction between the
   guard SETNX and the scheduled flush firing. Low likelihood (TTL is short,
   45s) but worth a follow-up: the flush task itself should tolerate an
   empty/missing pending blob gracefully (log + no-op) rather than assuming
   it's always present when scheduled.
5. **`recompute_status` on `external_ingest_batches` assumes one flush maps
   cleanly onto one-or-more ingestion rows.** Since debounce coalesces across
   multiple `ingestion_id`s (D10 groups by source instance, not by
   ingestion), a flush's `UPDATE ... WHERE ingestion_id = ANY(:ids)` must be
   given the exact set of ingestion_ids that contributed to the coalesced
   scope — this requires the worker (2697) to also track *which ingestion
   ids* fed into a given `RecomputeScope`, which isn't part of the
   `RecomputeScope` dataclass as scoped above (it only tracks facts, not
   ingestion provenance). **This is a real gap**: either extend
   `RecomputeScope` with an `ingestion_ids: frozenset[str]` field (cheap,
   recommended) or accept that `recompute_status` is only approximately
   accurate for coalesced batches. Recommend the former; flagged as a
   required addition during implementation, not left as-is.
6. **Investment-materialize LLM cost**: `dispatch_investment_materialize_partitioned`
   invokes an LLM provider per component (`work_graph/investment/materialize.py`).
   Debounced push-triggered recompute could increase LLM spend materially
   under sustained customer-push traffic vs. the current once-daily
   post-sync trigger. Not blocking for v1 but worth flagging to whoever owns
   cost/budget guardrails (`sync/budget_guard.py` exists for provider sync,
   not for this path) — no budget gate currently applies to this trigger.

---

## decisionsNeeded (cross-cutting, escalate to epic owner)

1. Should the `postgres` pytest marker (needed by 2699 AND 2694 for
   direct-SQL status tables) be introduced once, centrally, as part of
   whichever of 2694/2699 lands first — and who owns updating
   `pytest.ini`/`ci/run_tests.sh`?
2. Is D8's "day-bounded, all-repos-in-org" fallback for repo-less work-item
   batches acceptable as "not full-org recompute," or does product want a
   hard skip (no recompute at all) until repo attribution exists for
   Jira-native work items?
3. Should investment-materialize recompute (LLM-cost-bearing) be excluded
   from customer-push-triggered debounce entirely in v1, and only continue
   to run on the existing once-daily/post-sync cadence, to avoid uncapped
   LLM spend from a new, higher-frequency trigger? (Risk 6.)
4. Confirm final Alembic revision numbers for 2694/2699 migrations once both
   are actually being implemented (Risk 1) — needs whoever merges first to
   own updating the other's `down_revision`.
