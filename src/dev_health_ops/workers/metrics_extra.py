from __future__ import annotations

import logging
import uuid
from datetime import date

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.org_guard import organization_exists_sync
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
            "status": "success" if result == 0 else "no_data",
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
    queue="default",
    name="dev_health_ops.workers.tasks.dispatch_complexity_job",
)
def dispatch_complexity_job(
    self,
    db_url: str | None = None,
    day: str | None = None,
) -> dict:
    """Fan out ``run_complexity_job`` per active org on a daily cadence (CHAOS-2850).

    ``run_complexity_job`` previously only ran chained after a git sync
    (post_sync_dispatch.py), so an org with infrequent syncs left
    ``repo_complexity_daily`` stale for days at a time. Because
    ``complexity_delta`` (compounding_risk.py) reads a trailing 30-day
    window from that table, a stale table reads as a flat trend even
    though nothing about the compute itself is wrong. This dispatcher
    gives complexity an independent daily floor cadence -- the same
    fan-out shape as ``dispatch_release_impact`` /
    ``dispatch_membership_backfill`` -- so every active org's complexity
    refreshes at least once per day regardless of sync activity.

    Args:
        db_url: ClickHouse connection string (defaults to CLICKHOUSE_URI env)
        day: Target day as ISO string (defaults to today, resolved per task)

    Returns:
        dict with the list of dispatched org_ids
    """
    from dev_health_ops.workers.recommendations_tasks import _discover_active_org_ids

    try:
        # strict=True: a Postgres enumeration failure must RAISE (not
        # collapse to ["default"]) so the once-daily run retries instead of
        # silently reporting a clean empty success while computing zero orgs.
        org_ids = _discover_active_org_ids(strict=True)
    except Exception as exc:
        logger.exception("dispatch_complexity_job failed to enumerate orgs")
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))

    dispatched: list[str] = []
    for org_id in org_ids:
        run_complexity_job.apply_async(
            kwargs={"db_url": db_url, "day": day, "org_id": org_id},
            queue="metrics",
        )
        dispatched.append(org_id)

    logger.info("Complexity dispatch: dispatched=%d organizations", len(dispatched))
    return {"dispatched": dispatched}


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


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_release_impact_job",
)
def run_release_impact_job(
    self,
    db_url: str | None = None,
    day: str | None = None,
    backfill_days: int = 1,
    recomputation_window_days: int = 7,
    org_id: str | None = None,
) -> dict:
    """Compute and persist release_impact_daily metrics asynchronously.

    Materializes the ``release_impact_daily`` table (read by the
    ``/feature-flags`` release-reliability cards) from the
    ``telemetry_signal_bucket`` and ``deployments`` tables. Without this
    scheduled task the compute only ran via the ``dev-hops metrics
    release-impact`` CLI, so live orgs saw flat-zero cards (CHAOS-2381).

    Args:
        db_url: ClickHouse connection string (defaults to CLICKHOUSE_URI env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        recomputation_window_days: Days to recompute per run (late-data window)
        org_id: Organization scope

    Returns:
        dict with job status and number of records written
    """
    from dev_health_ops.metrics.job_release_impact import (
        run_release_impact_job as _run_release_impact_job,
    )

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else utc_today()

    logger.info(
        "Starting release-impact metrics task: day=%s backfill=%d org=%s",
        target_day.isoformat(),
        backfill_days,
        org_id or "all",
    )

    if org_id:
        from dev_health_ops.db import get_postgres_session_sync

        with get_postgres_session_sync() as session:
            if not organization_exists_sync(session, org_id):
                logger.info(
                    "Skipping release-impact task for deleted org_id=%s", org_id
                )
                return {
                    "status": "skipped",
                    "reason": "organization_not_found",
                    "day": target_day.isoformat(),
                }

    try:
        written = run_async(
            _run_release_impact_job(
                db_url=db_url,
                day=target_day,
                backfill_days=backfill_days,
                recomputation_window_days=recomputation_window_days,
                org_id=org_id or "",
            )
        )

        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
            "records_written": written,
        }
    except Exception as exc:
        logger.exception("Release-impact metrics task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="default",
    name="dev_health_ops.workers.tasks.dispatch_release_impact",
)
def dispatch_release_impact(
    self,
    db_url: str | None = None,
    day: str | None = None,
    backfill_days: int = 1,
    recomputation_window_days: int = 7,
) -> dict:
    """Fan out ``run_release_impact_job`` per active organization.

    ``compute_release_impact_daily`` filters telemetry with
    ``WHERE org_id = {org_id:String}``, so a single global run with a blank
    ``org_id`` would match zero rows for every real (UUID-scoped) tenant and
    leave ``release_impact_daily`` empty (CHAOS-2381). This dispatcher
    enumerates active organizations and dispatches one per-org compute, the
    same fan-out shape used by the other scheduled metrics dispatchers.

    Args:
        db_url: ClickHouse connection string (defaults to CLICKHOUSE_URI env)
        day: Target day as ISO string (defaults to today, resolved per task)
        backfill_days: Number of days to backfill
        recomputation_window_days: Days to recompute per run (late-data window)

    Returns:
        dict with the list of dispatched org_ids and a skipped count
    """
    from sqlalchemy import select

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.users import Organization

    dispatched: list[str] = []
    skipped = 0

    try:
        with get_postgres_session_sync() as session:
            org_ids = [
                str(row[0])
                for row in session.execute(
                    select(Organization.id).where(Organization.is_active.is_(True))
                ).all()
            ]
    except Exception as exc:
        # A transient Postgres/enumeration failure (DB outage, migration skew,
        # permissions) at the once-daily scheduled run must NOT report success
        # while computing zero orgs — that leaves every tenant with stale,
        # flat-zero release-reliability cards for ~24h with no failure signal
        # to page on (CHAOS-2381). Re-raise via Celery's retry machinery so the
        # task is retried for transient errors and ultimately surfaces as a
        # FAILED task (not a silent empty success) once retries are exhausted.
        logger.exception("dispatch_release_impact failed to enumerate orgs")
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))

    for org_id in org_ids:
        run_release_impact_job.apply_async(
            kwargs={
                "db_url": db_url,
                "day": day,
                "backfill_days": backfill_days,
                "recomputation_window_days": recomputation_window_days,
                "org_id": org_id,
            },
            queue="metrics",
        )
        dispatched.append(org_id)

    logger.info(
        "Release-impact dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}
