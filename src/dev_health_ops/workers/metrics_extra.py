from __future__ import annotations

import logging
import uuid
from datetime import date

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_complexity_job",
)
def run_complexity_job(
    self,
    db_url: str | None = None,
    day: str | None = None,
    backfill_days: int = 1,
    repo_id: str | None = None,
    search_pattern: str | None = None,
    language_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    max_files: int | None = None,
    org_id: str | None = None,
) -> dict:
    """
    Compute code complexity metrics from ClickHouse git_files/git_blame.

    Analyzes file contents already synced to the database — no local
    repository checkout required.

    Args:
        db_url: ClickHouse connection string (defaults to CLICKHOUSE_URI env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        repo_id: Optional repository UUID to filter
        search_pattern: Repo name glob pattern (e.g. "org/*")
        language_globs: Include language globs (e.g. ["*.py", "*.ts"])
        exclude_globs: Exclude path globs (e.g. ["*/tests/*"])
        max_files: Limit number of files scanned per repo
        org_id: Organization scope

    Returns:
        dict with job status and summary
    """
    from dev_health_ops.metrics.job_complexity_db import run_complexity_db_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else utc_today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting complexity analysis task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        search_pattern or str(parsed_repo_id) or "all",
    )

    try:
        result = run_complexity_db_job(
            repo_id=parsed_repo_id,
            db_url=db_url,
            date=target_day,
            backfill_days=backfill_days,
            language_globs=language_globs,
            max_files=max_files,
            search_pattern=search_pattern,
            exclude_globs=exclude_globs,
            org_id=org_id or "",
        )
        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
            "exit_code": result,
        }
    except Exception as exc:
        logger.exception("Complexity analysis task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_dora_metrics",
)
def run_dora_metrics(
    self,
    db_url: str | None = None,
    day: str | None = None,
    backfill_days: int = 1,
    repo_id: str | None = None,
    repo_name: str | None = None,
    sink: str = "auto",
    metrics: str | None = None,
    interval: str = "daily",
    org_id: str | None = None,
) -> dict:
    """
    Compute and persist DORA metrics asynchronously.

    Args:
            db_url: Database connection string (defaults to DATABASE_URI env)
            day: Target day as ISO string (defaults to today)
            backfill_days: Number of days to backfill
            repo_id: Optional repository UUID to filter
            repo_name: Optional repository name to filter
            sink: Sink type (auto|clickhouse|mongo|sqlite|postgres|both)
            metrics: Specific metrics to compute (optional)
            interval: Metric interval (daily|weekly|monthly)
            org_id: Organization scope

    Returns:
            dict with job status and summary
    """
    from dev_health_ops.metrics.job_dora import run_dora_metrics_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else utc_today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting DORA metrics task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        repo_name or str(parsed_repo_id) or "all",
    )

    try:
        run_dora_metrics_job(
            db_url=db_url,
            day=target_day,
            backfill_days=backfill_days,
            repo_id=parsed_repo_id,
            repo_name=repo_name,
            sink=sink,
            metrics=metrics,
            interval=interval,
            org_id=org_id or "",
        )

        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
        }
    except Exception as exc:
        logger.exception("DORA metrics task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
