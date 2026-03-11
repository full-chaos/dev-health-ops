from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import date
from typing import Any

from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.metrics.job_work_items import run_work_items_sync_job
from dev_health_ops.models.settings import SyncConfiguration

from .chunker import chunk_date_range

ProgressCallback = Callable[[int, int, date, date], None]


def run_backfill_for_config(
    *,
    db_url: str,
    sync_config_id: str,
    org_id: str,
    since: date,
    before: date,
    sink: str = "clickhouse",
    chunk_days: int = 7,
    progress_cb: ProgressCallback | None = None,
) -> dict[str, Any]:
    config_uuid = uuid.UUID(sync_config_id)
    with get_postgres_session_sync() as session:
        config = (
            session.query(SyncConfiguration)
            .filter(
                SyncConfiguration.id == config_uuid,
                SyncConfiguration.org_id == org_id,
            )
            .one_or_none()
        )
        if config is None:
            raise ValueError(f"Sync configuration not found: {sync_config_id}")

        provider = str(config.provider or "").strip().lower()
        sync_options = dict(config.sync_options or {})

    windows = chunk_date_range(since=since, before=before, chunk_days=chunk_days)

    for idx, (window_since, window_before) in enumerate(windows, start=1):
        if progress_cb is not None:
            progress_cb(idx, len(windows), window_since, window_before)

        backfill_days = (window_before - window_since).days + 1
        run_work_items_sync_job(
            db_url=db_url,
            day=window_before,
            backfill_days=backfill_days,
            provider=provider,
            sink=sink,
            repo_name=sync_options.get("repo"),
            search_pattern=sync_options.get("search"),
            org_id=org_id,
        )

    return {
        "status": "success",
        "provider": provider,
        "sync_config_id": sync_config_id,
        "window_count": len(windows),
        "since": since.isoformat(),
        "before": before.isoformat(),
    }
