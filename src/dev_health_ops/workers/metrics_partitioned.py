from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from celery import chord

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url, _invalidate_metrics_cache

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    queue="default",
    name="dev_health_ops.workers.tasks.dispatch_daily_metrics_partitioned",
)
def dispatch_daily_metrics_partitioned(
    self,
    org_id: str | None = None,
    db_url: str | None = None,
    day: str | None = None,
    backfill_days: int = 1,
    batch_size: int = 5,
    sink: str = "auto",
    provider: str = "auto",
) -> dict:
    """Orchestrator: discover repos, partition into batches, fan out via chord.

    For each day in the backfill range, dispatches a chord of
    ``run_daily_metrics_batch`` tasks with a ``run_daily_metrics_finalize_task``
    callback.

    Args:
        db_url: Database connection string (defaults to env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        batch_size: Number of repos per batch task
        sink: Sink type (auto|clickhouse)
        provider: Work item provider (auto|all|jira|github|gitlab|none)
        org_id: Organization scope

    Returns:
        dict with dispatched count, batch_count, and days
    """
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    db_url = db_url or _get_db_url()
    org_id = org_id or "default"
    target_day = date.fromisoformat(day) if day else utc_today()

    logger.info(
        "dispatch_daily_metrics_partitioned: day=%s backfill=%d batch_size=%d",
        target_day.isoformat(),
        backfill_days,
        batch_size,
    )

    try:
        ch_sink = ClickHouseMetricsSink(db_url)
        rows = ch_sink.client.query("SELECT id FROM repos").result_rows
        repo_ids = [str(row[0]) for row in rows]
    except Exception as exc:
        logger.exception("Failed to discover repos for partitioned dispatch: %s", exc)
        return {"status": "error", "error": str(exc)}

    if not repo_ids:
        logger.warning("No repos found — nothing to dispatch")
        return {"status": "no_repos", "dispatched": 0}

    batches = [
        repo_ids[i : i + batch_size] for i in range(0, len(repo_ids), batch_size)
    ]

    days_list = [target_day - timedelta(days=i) for i in range(backfill_days)]

    total_dispatched = 0
    for d in days_list:
        day_iso = d.isoformat()
        chord(
            [
                run_daily_metrics_batch.s(
                    repo_ids=[str(rid) for rid in batch],
                    day=day_iso,
                    db_url=db_url,
                    sink=sink,
                    provider=provider,
                    org_id=org_id,
                )
                for batch in batches
            ],
            run_daily_metrics_finalize_task.s(
                day=day_iso,
                db_url=db_url,
                sink=sink,
                org_id=org_id,
            ),
        )()
        total_dispatched += len(batches)

    logger.info(
        "dispatch_daily_metrics_partitioned: dispatched %d batches across %d days",
        total_dispatched,
        len(days_list),
    )

    return {
        "status": "dispatched",
        "repo_count": len(repo_ids),
        "batch_count": len(batches),
        "days": len(days_list),
        "total_dispatched": total_dispatched,
    }


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_daily_metrics_batch",
)
def run_daily_metrics_batch(
    self,
    repo_ids: list[str],
    day: str,
    org_id: str | None = None,
    db_url: str | None = None,
    sink: str = "auto",
    provider: str = "auto",
) -> dict:
    """Worker: compute daily metrics for a batch of repos (single day).

    Processes each repo independently so one failure does not kill the batch.
    Uses checkpoint CRUD to track progress and skip already-completed repos.

    Args:
        repo_ids: List of repository UUID strings
        day: Target day as ISO string
        db_url: Database connection string
        sink: Sink type
        provider: Work item provider
        org_id: Organization scope

    Returns:
        dict with per-repo results
    """
    from datetime import time

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.metrics.checkpoints import (
        is_completed,
        mark_completed,
        mark_failed,
        mark_running,
    )
    from dev_health_ops.metrics.job_daily import run_daily_metrics_job

    db_url = db_url or _get_db_url()
    org_id = org_id or "default"
    target_day = date.fromisoformat(day)
    checkpoint_day = datetime.combine(target_day, time.min, tzinfo=timezone.utc)

    results: dict[str, Any] = {}

    for repo_id in repo_ids:
        repo_id_uuid = uuid.UUID(repo_id)
        try:
            with get_postgres_session_sync() as session:
                if is_completed(
                    session, org_id, repo_id_uuid, "daily_batch", checkpoint_day
                ):
                    logger.info(
                        "Skipping already-completed repo %s for day %s",
                        repo_id,
                        day,
                    )
                    results[repo_id] = {
                        "status": "skipped",
                        "reason": "already_completed",
                    }
                    continue

                checkpoint = mark_running(
                    session,
                    org_id,
                    repo_id_uuid,
                    "daily_batch",
                    checkpoint_day,
                    self.request.id,
                )
                checkpoint_id = checkpoint.id

            run_async(
                run_daily_metrics_job(
                    db_url=db_url,
                    day=target_day,
                    backfill_days=1,
                    repo_id=repo_id_uuid,
                    skip_finalize=True,
                    sink=sink,
                    provider=provider,
                    org_id=org_id,
                )
            )

            with get_postgres_session_sync() as session:
                mark_completed(session, checkpoint_id)

            results[repo_id] = {"status": "success"}

        except Exception as exc:
            logger.exception(
                "run_daily_metrics_batch failed for repo %s day %s: %s",
                repo_id,
                day,
                exc,
            )
            try:
                with get_postgres_session_sync() as session:
                    mark_failed(session, checkpoint_id, str(exc))
            except Exception as mark_exc:
                logger.error("Failed to mark checkpoint as failed: %s", mark_exc)

            results[repo_id] = {"status": "failed", "error": str(exc)}

    return {
        "day": day,
        "repo_count": len(repo_ids),
        "results": results,
    }


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_daily_metrics_finalize_task",
)
def run_daily_metrics_finalize_task(
    self,
    batch_results: list,
    day: str,
    org_id: str | None = None,
    db_url: str | None = None,
    sink: str = "auto",
) -> dict:
    """Chord callback: finalize daily metrics after all batches complete.

    Runs the finalize step (rollups, aggregations) and invalidates caches.
    Named with ``_task`` suffix to avoid collision with
    ``job_daily.run_daily_metrics_finalize``.

    Args:
        batch_results: List of results from header tasks (chord callback arg)
        day: Target day as ISO string
        db_url: Database connection string
        sink: Sink type
        org_id: Organization scope

    Returns:
        dict with finalize status
    """
    from datetime import time

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.metrics.checkpoints import (
        mark_completed,
        mark_failed,
        mark_running,
    )
    from dev_health_ops.metrics.job_daily import (
        run_daily_metrics_finalize as _run_finalize,
    )

    db_url = db_url or _get_db_url()
    org_id = org_id or "default"
    target_day = date.fromisoformat(day)
    checkpoint_day = datetime.combine(target_day, time.min, tzinfo=timezone.utc)

    logger.info(
        "run_daily_metrics_finalize_task: day=%s batches=%d",
        day,
        len(batch_results) if batch_results else 0,
    )

    checkpoint_id = None
    try:
        with get_postgres_session_sync() as session:
            checkpoint = mark_running(
                session, org_id, None, "daily_finalize", checkpoint_day, self.request.id
            )
            checkpoint_id = checkpoint.id

        run_async(
            _run_finalize(
                db_url=db_url,
                day=target_day,
                org_id=org_id,
                sink=sink,
            )
        )

        _invalidate_metrics_cache(day, org_id)

        if checkpoint_id is not None:
            with get_postgres_session_sync() as session:
                mark_completed(session, checkpoint_id)

        return {
            "status": "success",
            "day": day,
            "batches_received": len(batch_results) if batch_results else 0,
        }

    except Exception as exc:
        logger.exception(
            "run_daily_metrics_finalize_task failed for day %s: %s", day, exc
        )

        if checkpoint_id is not None:
            try:
                with get_postgres_session_sync() as session:
                    mark_failed(session, checkpoint_id, str(exc))
            except Exception as mark_exc:
                logger.error(
                    "Failed to mark finalize checkpoint as failed: %s", mark_exc
                )

        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))
