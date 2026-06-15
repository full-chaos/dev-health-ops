from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time

from celery import chain

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_work_graph_build",
)
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


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_investment_materialize",
)
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
    org_id: str = "",
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
        org_id: Organization scope for work-graph/investment queries

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
            persist_evidence_snippets=True,
            llm_model=llm_model,
            team_ids=team_ids,
            force=force,
            org_id=org_id or None,
        )
        stats = run_async(materialize_investments(config))
        return {"status": "success", "stats": stats}
    except Exception as exc:
        logger.exception("Investment materialize task failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


def _orgs_with_work_graph_data(db_url: str, org_ids: list[str]) -> set[str]:
    """Return the subset of ``org_ids`` that have any ``work_graph_edges`` rows.

    Scopes the daily floor-cadence fan-out to orgs that actually have a work
    graph, so empty / never-synced orgs are not churned every night. Sourced
    from ClickHouse ``work_graph_edges`` — the exact input that the
    build->materialize chain reads — mirroring how the recommendations job
    scopes to tables that hold data (``_discover_team_ids``). On any ClickHouse
    error, returns all candidate orgs (fail open): the per-org chain is itself a
    no-op for an empty org, so a transient probe failure must not silently skip
    the safety net it exists to provide.
    """
    if not org_ids:
        return set()
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    try:
        sink = ClickHouseMetricsSink(db_url)
        rows = sink.client.query(
            """
            SELECT DISTINCT org_id
            FROM work_graph_edges
            WHERE org_id IN %(org_ids)s
            """,
            parameters={"org_ids": org_ids},
        ).result_rows
        return {str(row[0]) for row in rows if row and row[0]}
    except Exception:
        logger.warning(
            "work_graph_edges org-with-data probe failed; "
            "falling back to all candidate orgs",
            exc_info=True,
        )
        return set(org_ids)


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="default",
    name="dev_health_ops.workers.tasks.dispatch_investment_materialize",
)
def dispatch_investment_materialize(
    self,
    db_url: str | None = None,
) -> dict:
    """Daily floor-cadence safety net for ``work_unit_membership`` (CHAOS-2439).

    Investment materialization (which populates ``work_unit_membership``, read by
    the work-graph theme/subcategory filter) is otherwise EVENT-DRIVEN ONLY — it
    runs post-sync via the ``run_work_graph_build`` -> ``run_investment_materialize``
    chain in ``_dispatch_post_sync_tasks``. Idle-sync orgs and the post-deploy
    window therefore leave membership empty, stranding theme filters in the
    ``MEMBERSHIP_NOT_MATERIALIZED`` degraded state indefinitely (CHAOS-2427 #925).

    This dispatcher fans out one per-org job at a fixed daily cadence, queuing the
    SAME immutable ``build -> materialize`` chain used post-sync. The chain (not
    two independent dispatches) guarantees materialize only starts after the
    build *succeeds*, so concurrent metrics workers cannot race materialize ahead
    of a stale/empty graph — the race CHAOS-2374 avoided. It is idempotent and
    safe to coexist with the post-sync dispatch: materialization is keyed on
    ``computed_at`` (ReplacingMergeTree) and re-running simply refreshes the
    latest run.

    Org selection mirrors the other daily fan-out dispatchers
    (``_discover_active_org_ids`` — active orgs from Postgres, the source of
    truth, ``["default"]`` fallback for single-tenant installs), then narrows to
    orgs that actually have ``work_graph_edges`` so empty orgs are not churned.

    Returns:
        dict with the list of dispatched org_ids and a skipped count.
    """
    from dev_health_ops.workers.recommendations_tasks import _discover_active_org_ids

    db_url = db_url or _get_db_url()

    try:
        candidate_org_ids = _discover_active_org_ids()
    except Exception as exc:
        # A transient Postgres/enumeration failure at the once-daily run must not
        # report success while computing zero orgs (mirrors dispatch_release_impact):
        # retry, then surface as a FAILED task rather than a silent empty success.
        logger.exception("dispatch_investment_materialize failed to enumerate orgs")
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))

    with_data = _orgs_with_work_graph_data(db_url, candidate_org_ids)

    dispatched: list[str] = []
    skipped = 0
    for org_id in candidate_org_ids:
        if org_id not in with_data:
            skipped += 1
            continue
        # Mirror the post-sync immutable chain exactly (CHAOS-2374): build FIRST,
        # then materialize, on the metrics queue. ``.si()``-equivalent immutability
        # keeps the build's return value out of materialize's positional args.
        build_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_work_graph_build",
            kwargs={"org_id": org_id},
            queue="metrics",
        )
        materialize_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_investment_materialize",
            kwargs={"org_id": org_id},
            queue="metrics",
            immutable=True,
        )
        chain(build_sig, materialize_sig).apply_async()
        dispatched.append(org_id)

    logger.info(
        "Investment materialize dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}
