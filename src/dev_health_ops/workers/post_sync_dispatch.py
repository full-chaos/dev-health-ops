from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any

from dev_health_ops.models import SyncRun, SyncRunUnit, SyncRunUnitStatus

# CHAOS-3093 (PR2b): _DORA_TARGETS (deployments/cicd/incidents) and this
# module's own logger fed the now-deleted dispatch decisions in
# _dispatch_post_sync_tasks -- removed with them.


@dataclass(frozen=True)
class PostSyncDispatchPayload:
    provider: str
    sync_targets: list[str]
    org_id: str
    from_date: str | None
    to_date: str | None
    work_graph_from_date: str | None
    work_graph_to_date: str | None
    # CHAOS-4323: "should the post-sync team-autoimport task dispatch at all" --
    # true when ANY of the three independent auto_import_teams/_projects/_members
    # flags is set, not just auto_import_teams. Name kept for callers/tests; the
    # task itself (team_autoimport.run_post_sync_team_autoimport) re-reads
    # sync_options and honours each flag independently once dispatched.
    auto_import_teams: bool


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_post_sync_dispatch_payload(
    session: Any, sync_run_id: str | uuid.UUID
) -> PostSyncDispatchPayload | None:
    from dev_health_ops.sync.planner import map_datasets_to_legacy_targets

    run_uuid = uuid.UUID(str(sync_run_id))
    run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
    if run is None:
        return None

    units = (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == run_uuid)
        .order_by(SyncRunUnit.id)
        .all()
    )
    successful_by_provider: dict[str, set[str]] = {}
    successful_units: list[SyncRunUnit] = []
    for unit in units:
        if unit.status != SyncRunUnitStatus.SUCCESS.value:
            continue
        provider = str(unit.provider)
        successful_by_provider.setdefault(provider, set()).add(str(unit.dataset_key))
        successful_units.append(unit)
    if not successful_units:
        return None

    legacy_targets: set[str] = set()
    for provider, dataset_keys in successful_by_provider.items():
        legacy_targets.update(map_datasets_to_legacy_targets(provider, dataset_keys))
    if not legacy_targets:
        return None

    covered_since: datetime | None = None
    covered_before: datetime | None = None
    any_unbounded_lower = any(u.since_at is None for u in successful_units)
    any_unbounded_upper = any(u.before_at is None for u in successful_units)
    if not any_unbounded_lower:
        since_values = [
            _as_aware(u.since_at) for u in successful_units if u.since_at is not None
        ]
        covered_since = min(since_values)
    if not any_unbounded_upper:
        before_values = [
            _as_aware(u.before_at) for u in successful_units if u.before_at is not None
        ]
        covered_before = max(before_values)

    from_date_str = (
        covered_since.date().isoformat() if covered_since is not None else None
    )
    to_date_str = (
        covered_before.date().isoformat() if covered_before is not None else None
    )
    work_graph_from_date_str = (
        datetime.combine(
            covered_since.date(),
            time.min,
            tzinfo=timezone.utc,
        ).isoformat()
        if covered_since is not None
        else None
    )
    work_graph_to_date_str = (
        datetime.combine(
            covered_before.date() + timedelta(days=1),
            time.min,
            tzinfo=timezone.utc,
        ).isoformat()
        if covered_before is not None
        else None
    )

    from dev_health_ops.sync.trigger_routing import (
        canonical_sync_config_for_sync_run,
    )
    from dev_health_ops.workers.team_autoimport_categories import (
        import_categories_from_sync_options,
    )

    canonical_config = canonical_sync_config_for_sync_run(session, run)
    auto_import_teams = (
        any(import_categories_from_sync_options(canonical_config.sync_options).values())
        if canonical_config is not None
        else False
    )

    return PostSyncDispatchPayload(
        provider=next(iter(successful_by_provider), "unknown"),
        sync_targets=sorted(legacy_targets),
        org_id=str(run.org_id),
        from_date=from_date_str,
        to_date=to_date_str,
        work_graph_from_date=work_graph_from_date_str,
        work_graph_to_date=work_graph_to_date_str,
        auto_import_teams=auto_import_teams,
    )


def _dispatch_post_sync_tasks(
    *,
    provider: str,
    sync_targets: list[str],
    org_id: str,
    metrics_day: str | None = None,
    metrics_backfill_days: int | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    work_graph_from_date: str | None = None,
    work_graph_to_date: str | None = None,
    auto_import_teams: bool = False,
    sync_run_id: str | None = None,
) -> None:
    # metrics_day/metrics_backfill_days/from_date/to_date fed ONLY the
    # run_complexity_job single-day-vs-historical dispatch decision deleted
    # below (CHAOS-3093, PR2b) -- their from_date/to_date-derived computation
    # is deleted with it. The parameters stay in this function's signature
    # (its caller, sync_units.py, is untouched in this PR) but are no longer
    # read here.

    # run_complexity_job writes file_complexity_snapshots, which the native Go
    # daily worker's FileRiskHotspotsExecutor reads
    # (internal/jobs/metrics/daily/filehotspots) -- CHAOS-5234/CHAOS-3092
    # deleted job_daily.py's own Python reader (_load_complexity_map_for_repo)
    # once the family went fully native, but the freshness dependency itself
    # is unchanged, just on the Go side now. Chaining complexity -> daily
    # guarantees the daily risk/hotspot rows reflect the just-synced file
    # contents instead of the previous cycle's snapshot -- important for a newly
    # onboarded org's first daily run, which would otherwise show zero complexity.
    # CHAOS-5254: run_daily_metrics no longer participates in THIS chain --
    # the prod Celery workers/Beat that would have consumed it are stopped
    # (CHAOS-4026), so the only currently-CONSUMED daily metrics execution
    # path is the Go scheduler/reconciler's own cadence + the HTTP bridge
    # (api/internal/worker_metrics.py) into run_daily_metrics_job. CHAOS-3093
    # deleted the Celery task itself (workers/metrics_daily.py) outright --
    # its other dispatch source, external_ingest/recompute.py's webhook-
    # triggered send_task, is a known pre-existing follow-up (not this file's
    # concern; tracked for cleanup alongside CHAOS-4427). Complexity is still
    # chained ahead of build/materialize below for the same freshness reason
    # as always.
    #
    # Trade-off (CHAOS review #1078): as the chain head, a *terminal* complexity
    # failure (after its 3 internal retries) aborts the rest of the chain
    # (build/materialize), so the /investment refresh is skipped for that
    # sync. This is accepted as fresh-or-nothing: terminal complexity failures are
    # rare and usually stem from ClickHouse being unavailable, in which case the
    # downstream steps would fail anyway. If hard isolation is ever
    # needed, decouple via a non-raising wrapper task rather than link_error.
    # Historical backfills must not enqueue complexity implicitly: run_complexity_db_job
    # scans CURRENT persisted file contents/blame, so writing it across a
    # historical window (or a past single day) would fabricate a flat/incorrect
    # historical complexity trend rather than reflecting real historical state
    # (CHAOS-2888). Complexity is safe to enqueue only for a current single-day
    # sync: backfill_days in (None, 1) and day absent or == utc_today().
    #
    # CHAOS-3093 (PR2b): the whole `run_complexity_job` dispatch-decision block
    # that used to live here (the single-day-vs-historical check, the kwargs
    # it built, and the "skipping" warning for the historical case) is deleted
    # outright, not just its `celery_app.send_task` call -- `run_complexity_job`
    # itself was deleted as a celery_task definition under CHAOS-4439 (an
    # earlier, independent ticket), so this entire block has computed a
    # decision nothing has acted on since then.

    # `run_work_graph_build` (the Celery-dispatched Python build task) was
    # deleted under CHAOS-4924, and `dispatch_investment_materialize_partitioned`
    # (workers.work_graph_tasks) under CHAOS-3093 -- prod Celery has been
    # stopped since 2026-08-19 (CHAOS-4026), so neither ever fired anywhere in
    # production, and investment.materialize's route is river-only
    # (rollback_route=none in migration-state.json): the Go plane is the only
    # live orchestrator now. For work-graph builds specifically, the Go worker
    # already creates `workgraph.build` requests after a sync on its own,
    # independent of this dispatcher -- `cmd/dev-health-worker/sync_dispatch.go:
    # 273-310`'s `workGraphPostSyncWriter.StartRequestTx`, registered live at
    # `sync_dispatch.go:455`. Nothing replaces either deleted link; the Go
    # writer already is the replacement.

    # CHAOS-3093 (PR2b): run_dora_metrics was ALSO deleted as a celery_task
    # definition under CHAOS-4439 (the same ticket that deleted
    # run_complexity_job above) -- this `has_git or has_dora` dispatch
    # decision and its `celery_app.send_task` have been computing a decision
    # nothing has acted on since then, deleted outright with it.
    #
    # Post-sync team auto-import (CHAOS-2647) used to be dispatched from here
    # as a separate credential-resolving "sync" task, gated on the canonical
    # config's `auto_import_teams`. That `celery_app.send_task` call is
    # deleted too -- Celery has had zero consumers since CHAOS-4026, and the
    # REAL live trigger for run_post_sync_team_autoimport today is the HTTP
    # compatibility bridge (api/internal/worker_sync.py's /team-autoimport,
    # called from teamCatalogAutoimportBridge.TeamAutoImport on the Go side;
    # see team_autoimport.py's own module docstring for the full caller
    # enumeration) -- this was always a second, redundant, dead-into-the-void
    # dispatch attempt alongside that real path, not the real path itself.
    #
    # With every dispatch decision in this function now deleted, this is a
    # deliberate no-op: every parameter (provider/sync_targets/org_id/
    # metrics_day/metrics_backfill_days/from_date/to_date/
    # work_graph_from_date/work_graph_to_date/auto_import_teams/sync_run_id)
    # is accepted but unread, left in the signature rather than trimmed,
    # since this function's sole caller (sync_units.py) is untouched in this
    # PR. Flagged to team-lead: candidate for full deletion (function +
    # call site) in a follow-up once sync_units.py is back in scope.
    pass
