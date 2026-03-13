from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)

@celery_app.task(bind=True, max_retries=3, queue="metrics", name="dev_health_ops.workers.tasks.run_work_graph_build")
def run_work_graph_build(
    self,
    db_url: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    repo_id: str | None = None,
    heuristic_window: int = 7,
    heuristic_confidence: float = 0.3,
    org_id: str = "",
) -> dict:
    """Build work graph from evidence.

    Args:
        db_url: Database connection string
        from_date: Start date (ISO format, defaults to 30 days ago)
        to_date: End date (ISO format, defaults to now)
        repo_id: Optional repository UUID to filter
        heuristic_window: Days window for heuristics
        heuristic_confidence: Confidence threshold for heuristics

    Returns:
        dict with build status and edge count
    """
    from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder

    db_url = db_url or _get_db_url()
    now = datetime.now(timezone.utc)

    # Parse dates
    if to_date:
        parsed_to = datetime.fromisoformat(to_date)
    else:
        parsed_to = now

    if from_date:
        parsed_from = datetime.fromisoformat(from_date)
    else:
        parsed_from = parsed_to - timedelta(days=30)

    # Parse repo_id
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting work graph build task: from=%s to=%s repo=%s",
        parsed_from.isoformat(),
        parsed_to.isoformat(),
        repo_id or "all",
    )

    try:
        config = BuildConfig(
            dsn=db_url,
            from_date=parsed_from,
            to_date=parsed_to,
            repo_id=parsed_repo_id,
            heuristic_days_window=heuristic_window,
            heuristic_confidence=heuristic_confidence,
            org_id=org_id,
        )
        builder = WorkGraphBuilder(config)
        try:
            result = builder.build()
            return {"status": "success", "edges": result}
        finally:
            builder.close()
    except Exception as exc:
        logger.exception("Work graph build task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))

@celery_app.task(bind=True, max_retries=2, queue="metrics", name="dev_health_ops.workers.tasks.run_investment_materialize")
def run_investment_materialize(
    self,
    db_url: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    window_days: int = 30,
    repo_ids: list[str] | None = None,
    team_ids: list[str] | None = None,
    llm_provider: str = "auto",
    llm_model: str | None = None,
    force: bool = False,
) -> dict:
    """Materialize investment distributions from work graph.

    Args:
        db_url: Database connection string
        from_date: Start date (ISO format)
        to_date: End date (ISO format)
        window_days: Days window for default date range
        repo_ids: Optional list of repository IDs to filter
        team_ids: Optional list of team IDs to filter
        llm_provider: LLM provider (auto|openai|anthropic)
        llm_model: Optional specific LLM model
        force: Force recomputation even if cached

    Returns:
        dict with materialization status and stats
    """

    from dev_health_ops.work_graph.investment.materialize import (
        MaterializeConfig,
        materialize_investments,
    )

    db_url = db_url or _get_db_url()
    now = datetime.now(timezone.utc)

    # Parse to_date
    if to_date:
        parsed_to = datetime.combine(
            date.fromisoformat(to_date) + timedelta(days=1),
            dt_time.min,
            tzinfo=timezone.utc,
        )
    else:
        parsed_to = now

    # Parse from_date
    if from_date:
        parsed_from = datetime.combine(
            date.fromisoformat(from_date),
            dt_time.min,
            tzinfo=timezone.utc,
        )
    else:
        parsed_from = parsed_to - timedelta(days=window_days)

    logger.info(
        "Starting investment materialize task: from=%s to=%s repos=%s teams=%s",
        parsed_from.isoformat(),
        parsed_to.isoformat(),
        repo_ids or "all",
        team_ids or "all",
    )

    try:
        config = MaterializeConfig(
            dsn=db_url,
            from_ts=parsed_from,
            to_ts=parsed_to,
            repo_ids=repo_ids,
            llm_provider=llm_provider,
            persist_evidence_snippets=False,
            llm_model=llm_model,
            team_ids=team_ids,
            force=force,
        )
        stats = run_async(materialize_investments(config))
        return {"status": "success", "stats": stats}
    except Exception as exc:
        logger.exception("Investment materialize task failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))
