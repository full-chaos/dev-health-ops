from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any

from celery import chain

from dev_health_ops.models import SyncRun, SyncRunUnit, SyncRunUnitStatus
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _GIT_TARGETS, _WORK_ITEM_TARGETS

# DORA (deployment frequency, lead time, change-failure-rate, MTTR) is computed
# from synced deployments/CI/incidents in ClickHouse. These targets can be
# scheduled independently of git/prs (e.g. a deployments-only sync config), so a
# post-sync DORA recompute must fire on any of them, not only on git.
_DORA_TARGETS = {"deployments", "cicd", "incidents"}

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PostSyncDispatchPayload:
    provider: str
    sync_targets: list[str]
    org_id: str
    from_date: str | None
    to_date: str | None
    work_graph_from_date: str | None
    work_graph_to_date: str | None
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

    canonical_config = canonical_sync_config_for_sync_run(session, run)
    auto_import_teams = (
        bool((canonical_config.sync_options or {}).get("auto_import_teams"))
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
    target_set = set(sync_targets)
    has_git = bool(target_set & _GIT_TARGETS)
    has_work_items = bool(target_set & _WORK_ITEM_TARGETS)
    has_dora = bool(target_set & _DORA_TARGETS)
    dispatched: list[str] = []

    daily_metrics_kwargs: dict[str, Any] = {"org_id": org_id}
    if metrics_day is not None:
        daily_metrics_kwargs["day"] = metrics_day
    if metrics_backfill_days is not None:
        daily_metrics_kwargs["backfill_days"] = metrics_backfill_days

    if has_git or has_work_items:
        dispatched.append("run_daily_metrics")

    # run_complexity_job writes file_complexity_snapshots, which run_daily_metrics
    # reads (job_daily._load_complexity_map_for_repo). Chaining complexity ->
    # daily guarantees the daily risk/hotspot rows reflect the just-synced file
    # contents instead of the previous cycle's snapshot — important for a newly
    # onboarded org's first daily run, which would otherwise show zero complexity.
    #
    # Trade-off (CHAOS review #1078): as the chain head, a *terminal* complexity
    # failure (after its 3 internal retries) aborts the rest of the chain
    # (daily/build/materialize), so the /investment refresh is skipped for that
    # sync. This is accepted as fresh-or-nothing: terminal complexity failures are
    # rare and usually stem from ClickHouse being unavailable, in which case the
    # downstream steps would fail anyway; daily also degrades gracefully on a
    # missing snapshot, so the next cycle recovers. If hard isolation is ever
    # needed, decouple via a non-raising wrapper task rather than link_error.
    complexity_sig = None
    if has_git:
        complexity_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_complexity_job",
            kwargs={"org_id": org_id},
            queue="metrics",
            immutable=True,
        )
        dispatched.append("run_complexity_job")

    if has_git or has_work_items:
        build_kwargs: dict[str, Any] = {"org_id": org_id}
        graph_from_date = work_graph_from_date or from_date
        if graph_from_date is not None:
            build_kwargs["from_date"] = graph_from_date
        graph_to_date = work_graph_to_date or to_date
        if graph_to_date is not None:
            build_kwargs["to_date"] = graph_to_date

        materialize_kwargs: dict[str, Any] = {"org_id": org_id}
        if from_date is not None:
            materialize_kwargs["from_date"] = from_date
        if to_date is not None:
            materialize_kwargs["to_date"] = to_date

        # Every link below the chain head must be immutable so a parent's return
        # value is not injected as a positional arg into the next task.
        build_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_work_graph_build",
            kwargs=build_kwargs,
            queue="metrics",
            immutable=True,
        )
        materialize_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned",
            kwargs=materialize_kwargs,
            queue="default",
            immutable=True,
        )
        daily_metrics_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_daily_metrics",
            kwargs=daily_metrics_kwargs,
            queue="metrics",
            immutable=True,
        )
        chain_sigs = [daily_metrics_sig, build_sig, materialize_sig]
        if complexity_sig is not None:
            chain_sigs.insert(0, complexity_sig)
        chain(*chain_sigs).apply_async()
        dispatched.append("run_work_graph_build")
        dispatched.append("dispatch_investment_materialize_partitioned")

    if has_git or has_dora:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_dora_metrics",
            kwargs={"org_id": org_id},
            queue="metrics",
        )
        dispatched.append("run_dora_metrics")

    if dispatched:
        logger.info(
            "Post-sync dispatch for config org_id=%s provider=%s targets=%s: %s",
            org_id,
            provider,
            sync_targets,
            dispatched,
        )

    # Post-sync team auto-import (CHAOS-2647): restore the legacy per-config-run
    # refresh of team/project/member attribution on the unitized path. Dispatched
    # as a separate credential-resolving "sync" task (this relay has no credentials)
    # and gated on the canonical config's ``auto_import_teams``. Best-effort: a
    # dispatch failure must never break post-sync metric fan-out.
    if auto_import_teams and sync_run_id:
        try:
            celery_app.send_task(
                "dev_health_ops.workers.tasks.run_post_sync_team_autoimport",
                kwargs={"sync_run_id": sync_run_id},
                queue="sync",
            )
        except Exception:
            logger.exception(
                "Post-sync team auto-import dispatch failed for "
                "org_id=%s sync_run_id=%s",
                org_id,
                sync_run_id,
            )
