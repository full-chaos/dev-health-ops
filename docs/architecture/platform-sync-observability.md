# Design Doc: Platform (Superadmin) Sync Observability + Non-Impersonation Re-run

_Date: 2026-06-12_
_Status: Proposed_
_Scope: dev-health-ops (backend), dev-health-web (frontend)_

## One Sentence

Give platform admins a single cross-org view of every organization's sync/job status and last error, and let them re-run a job for any org/user by passing explicit target IDs — without impersonating.

## Problem

Today, sync/job status and error logs are only visible **inside a single org's context**, so a platform admin has to **impersonate** each org to see whether its syncs are healthy. Worse, "global" jobs (LLM investment materialization, work-graph builds) run platform-wide but record **no queryable status** — their outcome lives only in SignOz traces/logs, which are hard to navigate. There is no operational answer to "which orgs' syncs are failing right now, and why?" and no clean way to kick a job for a specific org/user.

## Key Finding: most of this already exists

Two facts reframe the work as **surfacing + filling gaps**, not building from scratch.

### 1. Sync status/errors are already persisted in Postgres, already org-tagged

| Model (`models/settings.py`, `models/backfill.py`, `models/reports.py`) | Relevant fields |
|---|---|
| `SyncConfiguration` | `last_sync_at`, `last_sync_success`, `last_sync_error`, `last_sync_stats`, `org_id` |
| `ScheduledJob` (`models/settings.py:363-454`) | `last_run_at`, `last_run_status`, `last_run_duration_seconds`, `last_run_error`, `run_count`, `failure_count` |
| `JobRun` (`models/settings.py:485-535`) | `status`, `started_at`, `completed_at`, `duration_seconds`, `result`, `error`, `error_traceback`, `triggered_by` (scheduler/manual/webhook) |
| `BackfillJob` (`models/backfill.py:12-50`) | `status`, `error_message`, `celery_task_id`, chunk counters |
| `ReportRun` (`models/reports.py:174-246`) | `status`, `error_message`, `celery_task_id`, report metadata |

Existing read paths: `api/admin/routers/platform.py` (aggregate stats, `require_superuser`), `api/admin/routers/sync.py` (per-config `JobRun` history). These are org-filtered, not cross-org.

### 2. Workers already run on explicit `org_id` — impersonation was never required to execute

Sync/metrics/work-graph Celery tasks accept `org_id` (and config/report IDs) as **explicit kwargs** and re-load org-scoped rows with `WHERE org_id = …`. `workers/org_guard.organization_exists_sync()` validates the org before dispatch. Workers never read the caller's web session. The Reports "Run Now" path (`trigger_report` GraphQL mutation → `ReportRun(triggered_by="api")` → `apply_async(queue="reports")`) is the canonical in-repo template. Impersonation is therefore purely a **read-path** convenience for per-org UIs — execution already takes explicit targets.

### 3. On-Demand Trigger Endpoints

Admins can trigger syncs, backfills, and reports using these endpoints:
- POST `/api/v1/admin/sync-configs/{config_id}/trigger` (`api/admin/routers/sync.py:850-983`): Triggers a sync configuration run.
- POST `/api/v1/admin/sync-configs/backfill` (`api/admin/routers/sync.py:986-1045`): Initiates a backfill job.
- `triggerReport` mutation (`api/graphql/schema.py:720-727`): Triggers a report run.


## Gaps (verified)

1. **No cross-org read surface.** All status queries are `require_org_id`-scoped; only `product_telemetry` is `require_platform_admin` cross-org.
2. **Global jobs record nothing queryable.** `workers/work_graph_tasks.py::run_investment_materialize` and `run_work_graph_build` only `logger.info/exception` and return a dict — **no `JobRun` row**. Their status/errors exist only in SignOz/logs. This is the core navigability pain.
3. **`JobRun` has no actor or trace correlation.** `triggered_by` is a coarse string (`scheduler`/`manual`/`webhook`); there is no `actor_user_id`, `target_user_id`, or `trace_id`/`correlation_id`. Cannot answer "which admin re-ran this" or deep-link a run to its SignOz trace.
4. **`JobRun` is FK-bound to `ScheduledJob`** (`job_id` non-null FK), so it cannot represent ad-hoc or global runs that are not scheduled jobs.

## Storage Decision: Postgres (existing) + SignOz as drill-down — not a new ClickHouse table, not SignOz-as-source-of-truth

The run-record-as-source-of-truth pattern is what mature OSS job tooling converges on (django-celery-results durable rows; Hatchet `v1_runs_olap`/`v1_statuses_olap`; Temporal UI run list + event-history drawer + gated write actions; django-rq staff-only requeue). Dev-health already has the relational run record — we extend it.

- **Do not add a ClickHouse table for sync status.** It would duplicate the Postgres truth, and ClickHouse's append-only/eventual-consistency model is wrong for "current status / did-it-fail?" transactional reads. ClickHouse stays the analytics backend. (SignOz's own docs say raw ClickHouse SQL is for dashboards, not transactional reads.)
- **Do not make SignOz the source of truth.** It is external infra (OTLP collector `signoz-otel-collector:4317`), has no in-code query path, and is already "hard to navigate." Use it as a **deep-link drill-down target** keyed by `trace_id`/`correlation_id`. BugSink/Sentry already captures exception detail and can be linked by event id.
- **Do extend the existing Postgres run records** to close gaps 2–4, then read them cross-org.

So: "ClickHouse or SignOz?" → **Postgres (already there) + SignOz/BugSink deep-link.**

## Design

### Track A — Make every job a first-class run record

Generalize the run record so global/ad-hoc jobs report status the same way syncs do.

- Relax `JobRun.job_id` to nullable and add a discriminator `job_kind` (e.g. `sync`, `backfill`, `investment_materialize`, `work_graph_build`, `report`) + optional `job_key`.
- Add columns: `org_id` (indexed), `actor_user_id` (nullable, the triggering superadmin), `target_user_id` (nullable), `trace_id`/`correlation_id` (nullable, for SignOz/BugSink deep-link).
- Have `run_investment_materialize` and `run_work_graph_build` open/close a `JobRun` (running → success/failed with `error`/`error_traceback`), mirroring `sync_runtime.run_sync_config`.
- Alembic migration (Postgres). No analytics/ClickHouse change.

_Alternative considered:_ register the globals as `ScheduledJob` rows so they emit `JobRun`s unchanged (smaller migration, but conflates "scheduled" with "global/ad-hoc"). Decision pending (see Open Decisions #1).

### Track B — Cross-org Platform read view

- Backend: a `require_platform_admin` GraphQL resolver (mirror `resolvers/product_telemetry.py`) returning, per org → per job/config: last status, last error excerpt, last run time, `failure_count`, last `trace_id`. Cross-org via superuser bypass; **no impersonation**.
- Frontend: `/superadmin/syncs` page (mirror `/superadmin/product-telemetry`), with org → config drill-down reusing the existing `JobRun` history endpoint, plus a "View in SignOz / BugSink" deep-link. urql client, consistent with existing superadmin pages.

### Track C — Re-run without impersonation

Mirror `trigger_report`. A `require_platform_admin` mutation `rerunSync(orgId, configId)` (and `rerunGlobalJob(jobKey, orgId?)`) that:

1. validates superuser + `organization_exists`,
2. creates a `JobRun` with `actor_user_id = <superadmin>`, `triggered_by = "platform_admin"`, target org/user,
3. enqueues the existing worker with explicit `org_id` (already supported),
4. writes an `AuditService` entry with the real (admin) user id.

Strictly better than impersonation-based re-runs: no session/JWT swap, and a complete "who ran what, for whom, why" audit trail that impersonation does not currently produce.

## Security Guardrails

- All new read/write surfaces gated by `require_platform_admin` / `require_superuser`.
- Re-run never escalates into a target user's session; it runs under service identity with explicit target IDs.
- Every re-run writes an audit log with the real admin id (reuse `AuditService`, which already supports impersonation metadata).
- Error excerpts shown in the cross-org view should be truncated; full traceback stays behind the drill-down / observability link.

## Open Decisions

1. **Run-record model:** generalize `JobRun` (`job_kind` + nullable FK; cleaner) vs. register globals as `ScheduledJob`s (smaller migration). Recommendation: generalize.
2. **Read path:** new GraphQL resolver (consistent with `product_telemetry`) vs. extend REST `platform.py`. Recommendation: GraphQL.
3. **Drill-down target:** SignOz trace deep-link, BugSink issue deep-link, or both — requires adding `org_id`/`job_run_id` span/log attributes (not currently enriched).
4. **Sequencing:** read-only view (A+B) first, then re-run (C); or A/B/C together.

## References

- Existing impersonation ADR: `ops/docs/architecture/middleware-impersonation.md`
- Dual-DB contract: `ops/docs/architecture/database-architecture.md`
- Models: `models/settings.py` (`SyncConfiguration`, `ScheduledJob`, `JobRun`), `models/backfill.py`
- Trigger template: `api/graphql/resolvers/reports.py`, `api/admin/routers/sync.py`
- Global jobs lacking run records: `workers/work_graph_tasks.py`
- Cross-org guard template: `api/graphql/authz.py` (`require_platform_admin`), `api/graphql/resolvers/product_telemetry.py`
