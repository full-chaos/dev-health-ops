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


# CHAOS-3093 (PR2b): _dispatch_post_sync_tasks is deleted outright. It had
# already had every one of its celery_app.send_task dispatch decisions
# deleted earlier in this same PR (run_complexity_job, run_dora_metrics,
# run_post_sync_team_autoimport -- each dead-into-the-void, zero Celery
# consumers since CHAOS-4026), leaving a `pass`-bodied no-op. Re-checked
# repo-wide at delete time (team-lead's "delete the function AND the call"
# instruction assumed sync_units.py called it -- it does not; no production
# code calls it at all, confirmed by grep for `_dispatch_post_sync_tasks(`
# repo-wide: the only other hits are the function's own definition, a
# docstring in processors/sync.py explicitly noting it is NOT called there,
# and comments/test names already cleaned up earlier in this PR). No call
# site exists anywhere to remove.
