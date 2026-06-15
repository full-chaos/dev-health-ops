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


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_membership_backfill",
)
def run_membership_backfill(
    self,
    db_url: str | None = None,
    org_id: str = "",
    repo_ids: list[str] | None = None,
) -> dict:
    """Project work_unit_membership from EXISTING work_unit_investments — NO LLM.

    The cheap daily counterpart to ``run_investment_materialize``: instead of
    re-running LLM categorization (cost + category drift), it rebuilds the
    work-graph components and re-emits ``work_unit_membership`` rows from the
    theme/subcategory distributions ALREADY persisted by the post-sync LLM
    materializer. Units whose current component hash has no persisted
    categorization (churned components) receive TOMBSTONE rows (category='',
    weight=0, is_dominant=0) so their stale prior-run membership rows are
    superseded and the nodes stop matching any theme/subcategory filter.  See
    ``work_graph.investment.backfill`` for the full contract (CHAOS-2439).

    Args:
        db_url: Database connection string (defaults to env).
        org_id: Organization scope for work-graph/investment queries.
        repo_ids: Optional repo filter.

    Returns:
        dict with backfill status and stats.
    """
    from dev_health_ops.work_graph.investment.backfill import (
        MembershipBackfillConfig,
        backfill_memberships,
    )

    db_url = db_url or _get_db_url()
    try:
        config = MembershipBackfillConfig(
            dsn=db_url,
            org_id=org_id or None,
            repo_ids=repo_ids,
        )
        stats = backfill_memberships(config)
        return {"status": "success", "stats": stats}
    except Exception as exc:
        logger.exception("Membership backfill task failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="default",
    name="dev_health_ops.workers.tasks.dispatch_membership_backfill",
)
def dispatch_membership_backfill(
    self,
    db_url: str | None = None,
) -> dict:
    """Daily floor-cadence safety net for ``work_unit_membership`` (CHAOS-2439).

    ``work_unit_membership`` (read by the work-graph theme/subcategory filter) is
    otherwise populated EVENT-DRIVEN ONLY — post-sync via the
    ``run_work_graph_build`` -> ``run_investment_materialize`` (LLM) chain in
    ``_dispatch_post_sync_tasks``. Idle-sync orgs and the post-deploy window
    therefore leave membership empty, stranding theme filters in the
    ``MEMBERSHIP_NOT_MATERIALIZED`` degraded state (CHAOS-2427 #925).

    The daily job must NOT re-run LLM materialization (cost + category drift), so
    it fans out a CHEAP, no-LLM chain per active org:
    ``run_work_graph_build`` -> ``run_membership_backfill``. The build refreshes
    ``work_graph_edges`` (NO LLM); the backfill then PROJECTS membership from the
    theme/subcategory distributions already persisted in ``work_unit_investments``
    by the post-sync LLM path. The post-sync full ``build -> materialize`` (LLM)
    chain is UNCHANGED and still categorizes new data.

    The chain (not two independent dispatches) guarantees the backfill only runs
    after the build *succeeds*, so it never projects against a stale/empty graph
    — the race CHAOS-2374 avoided. It is idempotent and safe to coexist with the
    post-sync dispatch: membership is keyed on ``computed_at`` (ReplacingMergeTree)
    and the resolver's per-node latest-run guard supersedes stale rows.

    GATING: dispatched for EVERY active org — deliberately NOT gated on
    ``work_graph_edges`` existence (that is the build's OUTPUT; gating on it would
    permanently skip the very tenants the safety net must repair). The build is a
    cheap no-op for an org with no source data and the backfill short-circuits on
    zero components, so fanning out to all active orgs is correct and cheap.

    Org selection mirrors the other daily fan-out dispatchers
    (``_discover_active_org_ids`` — active orgs from Postgres, ``["default"]``
    fallback only for the positively-detected single-tenant case) with
    ``strict=True`` so a Postgres outage RAISES and triggers retry rather than
    silently dispatching zero orgs as a clean success.

    Returns:
        dict with the list of dispatched org_ids.
    """
    from dev_health_ops.workers.recommendations_tasks import _discover_active_org_ids

    db_url = db_url or _get_db_url()

    try:
        # strict=True: a Postgres enumeration failure must RAISE (not collapse to
        # ["default"]) so the once-daily run retries instead of reporting a clean
        # empty-success on a multi-tenant DB outage (CHAOS-2439).
        candidate_org_ids = _discover_active_org_ids(strict=True)
    except Exception as exc:
        logger.exception("dispatch_membership_backfill failed to enumerate orgs")
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))

    dispatched: list[str] = []
    for org_id in candidate_org_ids:
        # Immutable chain (CHAOS-2374): build FIRST (refreshes edges, NO LLM),
        # then the cheap no-LLM membership projection. ``.si()``-equivalent
        # immutability keeps the build's return value out of the backfill's args.
        # Forward the resolved ``db_url`` to BOTH children so an explicit override
        # (manual/backfill: dispatch_membership_backfill(db_url=...)) targets the
        # requested ClickHouse, not the workers' ambient instance (the children
        # otherwise default to _get_db_url()). The scheduled path passes the same
        # value _get_db_url() already resolves, so behaviour is unchanged when no
        # override is supplied (CHAOS-2439 review).
        build_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_work_graph_build",
            kwargs={"db_url": db_url, "org_id": org_id},
            queue="metrics",
        )
        backfill_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_membership_backfill",
            kwargs={"db_url": db_url, "org_id": org_id},
            queue="metrics",
            immutable=True,
        )
        chain(build_sig, backfill_sig).apply_async()
        dispatched.append(org_id)

    logger.info("Membership backfill dispatch: dispatched=%d", len(dispatched))
    return {"dispatched": dispatched}
