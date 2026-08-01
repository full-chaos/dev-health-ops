from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import case, select

from dev_health_ops.api.services.sync_coverage import (
    HISTORY_LOOKBACK_DAYS,
    rebuild_sync_coverage_projection,
)
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


async def _refresh_sync_coverage(
    *,
    limit: int = 100,
) -> dict[str, Any]:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        stmt = (
            select(SyncConfiguration)
            .outerjoin(
                SyncCoverageProjection,
                (SyncCoverageProjection.org_id == SyncConfiguration.org_id)
                & (SyncCoverageProjection.sync_config_id == SyncConfiguration.id)
                & (
                    SyncCoverageProjection.history_lookback_days
                    == HISTORY_LOOKBACK_DAYS
                ),
            )
            .order_by(
                case(
                    (SyncCoverageProjection.invalidated_at.is_not(None), 0),
                    (SyncCoverageProjection.id.is_(None), 1),
                    else_=2,
                ),
                SyncCoverageProjection.updated_at.asc().nullsfirst(),
                SyncConfiguration.id,
            )
        )
        config_keys = [
            (str(config.org_id), config.id)
            for config in (await session.execute(stmt.limit(limit))).scalars().all()
        ]

    refreshed = 0
    failed = 0
    for org_id, config_id in config_keys:
        try:
            async with get_postgres_session() as session:
                config = await session.get(SyncConfiguration, config_id)
                if config is None or str(config.org_id) != org_id:
                    continue
                await rebuild_sync_coverage_projection(
                    session,
                    org_id,
                    config,
                    lookback_days=HISTORY_LOOKBACK_DAYS,
                )
            refreshed += 1
        except Exception:
            failed += 1
            logger.exception(
                "sync_coverage_projection_refresh_failed",
                extra={"org_id": org_id, "sync_config_id": str(config_id)},
            )
    return {"status": "completed", "refreshed": refreshed, "failed": failed}


@celery_app.task(
    queue="sync",
    name="dev_health_ops.workers.tasks.refresh_sync_coverage_projections",
)
def refresh_sync_coverage_projections(limit: int = 100) -> dict[str, Any]:
    """Periodic safety net that keeps exact coverage projections warm."""

    return run_async(_refresh_sync_coverage(limit=limit))
