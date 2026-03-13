from __future__ import annotations

import logging
from datetime import date

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)

@celery_app.task(bind=True, max_retries=2, queue="metrics", name="dev_health_ops.workers.tasks.sync_teams_to_analytics")
def sync_teams_to_analytics(self, org_id: str | None = None) -> dict:
    from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

    try:
        count = bridge_teams_to_clickhouse(org_id=org_id)
        return {"status": "success", "teams_synced": count}
    except Exception as exc:
        logger.exception("sync_teams_to_analytics failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))

@celery_app.task(bind=True, max_retries=2, queue="metrics", name="dev_health_ops.workers.tasks.run_capacity_forecast_job")
def run_capacity_forecast_job(
    self,
    db_url: str | None = None,
    team_id: str | None = None,
    work_scope_id: str | None = None,
    target_items: int | None = None,
    target_date: str | None = None,
    history_days: int = 90,
    simulations: int = 10000,
    all_teams: bool = False,
) -> dict:
    """
    Run capacity forecasting job asynchronously.

    Args:
        db_url: Database connection string (defaults to DATABASE_URI env)
        team_id: Optional team UUID to forecast
        work_scope_id: Optional work scope UUID to forecast
        target_items: Optional target item count for forecast
        target_date: Optional target date as ISO string
        history_days: Number of historical days to analyze (default 90)
        simulations: Number of Monte Carlo simulations (default 10000)
        all_teams: If True, forecast for all teams

    Returns:
        dict with job status and forecast count
    """
    from dev_health_ops.metrics.job_capacity import run_capacity_forecast

    db_url = db_url or _get_db_url()
    parsed_target_date = date.fromisoformat(target_date) if target_date else None

    logger.info(
        "Starting capacity forecast task: team=%s scope=%s all_teams=%s",
        team_id,
        work_scope_id,
        all_teams,
    )

    try:
        results = run_async(
            run_capacity_forecast(
                db_url=db_url,
                team_id=team_id,
                work_scope_id=work_scope_id,
                target_items=target_items,
                target_date=parsed_target_date,
                history_days=history_days,
                simulations=simulations,
                all_teams=all_teams,
                persist=True,
            )
        )

        return {"status": "success", "forecasts": len(results)}
    except Exception as exc:
        logger.exception("Capacity forecast task failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))
