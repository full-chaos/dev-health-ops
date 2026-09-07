from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any

from dev_health_ops.db import get_postgres_session_sync


def run_backfill_via_planner(
    integration_id: str,
    since: date | datetime,
    before: date | datetime,
    *,
    org_id: str,
    source_ids: tuple[str, ...] | None = None,
    dataset_keys: tuple[str, ...] | None = None,
    triggered_by: str,
) -> dict[str, Any]:
    # Lazy imports: backfill is imported during sync.planner init via
    # backfill.chunker, so importing planner/sync_units at module top creates a
    # circular import. Import them at call time instead.
    from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
    from dev_health_ops.workers.sync_units import dispatch_sync_run

    with get_postgres_session_sync() as session:
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                integration_id=integration_id,
                org_id=org_id,
                mode="backfill",
                triggered_by=triggered_by,
                source_ids=source_ids,
                dataset_keys=dataset_keys,
                since=_as_utc_datetime(since, end_of_day=False),
                before=_as_utc_datetime(before, end_of_day=True),
            ),
        )

    if not plan.dispatch_required:
        return {
            "status": "disabled",
            "mode": "backfill",
            "integration_id": integration_id,
            "org_id": org_id,
            "sync_run_id": plan.sync_run_id,
            "unit_count": plan.total_units,
            "unit_ids": list(plan.unit_ids),
            "reason": plan.terminal_reason,
            "since": _as_utc_datetime(since, end_of_day=False).isoformat(),
            "before": _as_utc_datetime(before, end_of_day=True).isoformat(),
        }

    dispatch_result = dispatch_sync_run(plan.sync_run_id)
    return {
        "status": "success",
        "mode": "backfill",
        "integration_id": integration_id,
        "org_id": org_id,
        "sync_run_id": plan.sync_run_id,
        "unit_count": plan.total_units,
        "unit_ids": list(plan.unit_ids),
        "dispatch": dispatch_result,
        "since": _as_utc_datetime(since, end_of_day=False).isoformat(),
        "before": _as_utc_datetime(before, end_of_day=True).isoformat(),
    }


def _as_utc_datetime(value: date | datetime, *, end_of_day: bool) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    boundary = time.max if end_of_day else time.min
    return datetime.combine(value, boundary, tzinfo=timezone.utc)
