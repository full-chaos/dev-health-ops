from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import date
from typing import Any

from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.metrics.job_work_items import run_work_items_sync_job
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.workers.task_utils import _jira_query_options

from .chunker import chunk_date_range

ProgressCallback = Callable[[int, int, date, date], None]


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

    windows = chunk_date_range(since=since, before=before, chunk_days=chunk_days)

    for idx, (window_since, window_before) in enumerate(windows, start=1):
        if progress_cb is not None:
            progress_cb(idx, len(windows), window_since, window_before)

        backfill_days = (window_before - window_since).days + 1
        jira_project_keys, jira_jql, jira_fetch_all = _jira_query_options(sync_options)
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
        )

    return {
        "status": "success",
        "provider": provider,
        "sync_config_id": sync_config_id,
        "org_id": org_id,
        "window_count": len(windows),
        "since": since.isoformat(),
        "before": before.isoformat(),
    }
