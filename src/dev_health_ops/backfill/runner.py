from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import date, datetime, time, timezone
from typing import Any

from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.metrics.job_work_items import run_work_items_sync_job
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.workers.reference_discovery import _verify_reference_readback
from dev_health_ops.workers.task_utils import _jira_query_options
from dev_health_ops.workers.team_autoimport import run_team_autoimport_strict

from .chunker import chunk_date_range

ProgressCallback = Callable[[int, int, date, date], None]


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


def _run_strict_reference_discovery_for_backfill(
    *,
    provider: str,
    org_id: str,
    credentials: dict[str, Any] | None,
    sync_options: dict[str, Any],
    sync_config_id: str,
    since: date,
    before: date,
    window_count: int,
    analytics_db_url: str | None,
) -> dict[str, Any] | None:
    if not sync_options.get("auto_import_teams"):
        return None
    summary = run_team_autoimport_strict(
        provider=provider,
        org_id=org_id,
        credentials=credentials or {},
        scope={
            "mode": "backfill",
            "sync_config_id": sync_config_id,
            "sync_options": dict(sync_options),
            "window_count": window_count,
            "since": since.isoformat(),
            "before": before.isoformat(),
        },
        analytics_db_url=analytics_db_url,
    )
    if analytics_db_url is not None:
        _verify_reference_readback(
            org_id=org_id,
            provider=provider,
            summary=summary,
            analytics_db_url=analytics_db_url,
        )
    return summary


def run_backfill_for_config(
    *,
    db_url: str,
    sync_config_id: str,
    org_id: str | None = None,
    since: date,
    before: date,
    sink: str = "clickhouse",
    chunk_days: int = 7,
    progress_cb: ProgressCallback | None = None,
    credentials: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config_uuid = uuid.UUID(sync_config_id)
    with get_postgres_session_sync() as session:
        # The sync configuration owns its tenant, so the org is derived from the
        # config id — callers do not need to pass --org. When an org_id IS given
        # it is treated as an assertion: a mismatch is an explicit error rather
        # than a silent "not found" (the previous behaviour filtered on both id
        # AND org_id, so a wrong/empty --org just looked like a missing config).
        config = (
            session.query(SyncConfiguration)
            .filter(SyncConfiguration.id == config_uuid)
            .one_or_none()
        )
        if config is None:
            raise ValueError(f"Sync configuration not found: {sync_config_id}")

        resolved_org_id = str(config.org_id)
        if org_id and org_id != resolved_org_id:
            raise ValueError(
                f"Org mismatch: --org {org_id} does not own sync config "
                f"{sync_config_id} (owned by {resolved_org_id})"
            )
        org_id = resolved_org_id

        provider = str(config.provider or "").strip().lower()
        sync_options = dict(config.sync_options or {})
        sync_targets = [str(t) for t in (config.sync_targets or [])]

    windows = chunk_date_range(since=since, before=before, chunk_days=chunk_days)

    reference_discovery = _run_strict_reference_discovery_for_backfill(
        provider=provider,
        org_id=org_id,
        credentials=credentials,
        sync_options=sync_options,
        sync_config_id=sync_config_id,
        since=since,
        before=before,
        window_count=len(windows),
        analytics_db_url=db_url,
    )

    for idx, (window_since, window_before) in enumerate(windows, start=1):
        if progress_cb is not None:
            progress_cb(idx, len(windows), window_since, window_before)

        backfill_days = (window_before - window_since).days + 1
        jira_project_keys, jira_jql, jira_fetch_all = _jira_query_options(sync_options)
        github_sync_targets = sync_targets or ["work-items"]
        run_work_items_sync_job(
            db_url=db_url,
            day=window_before,
            backfill_days=backfill_days,
            provider=provider,
            sink=sink,
            repo_name=sync_options.get("repo"),
            search_pattern=sync_options.get("search"),
            org_id=org_id,
            credentials=credentials,
            jira_project_keys=jira_project_keys if provider == "jira" else None,
            jira_jql=jira_jql if provider == "jira" else None,
            jira_fetch_all=jira_fetch_all if provider == "jira" else None,
            # CHAOS-646: only ingest PRs as work items when the PRS target is
            # enabled (None would let the github provider fall back to the
            # GITHUB_INCLUDE_PRS env default, PRs ON). Mirrors the unitized path
            # (processors/dataset_adapters._work_item_kwargs).
            include_issues=(
                ("work-items" in github_sync_targets) if provider == "github" else None
            ),
            include_pull_requests=(
                ("prs" in github_sync_targets) if provider == "github" else None
            ),
        )

    result = {
        "status": "success",
        "provider": provider,
        "sync_config_id": sync_config_id,
        "org_id": org_id,
        "window_count": len(windows),
        "since": since.isoformat(),
        "before": before.isoformat(),
    }
    if reference_discovery is not None:
        result["team_autoimport"] = reference_discovery
    return result
