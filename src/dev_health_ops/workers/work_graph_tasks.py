from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from typing import Any

from celery import chain, chord

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)


def _llm_concurrency(value: object | None = None) -> int:
    import os

    raw = value if value is not None else os.getenv("INVESTMENT_LLM_CONCURRENCY", "5")
    try:
        concurrency = int(str(raw))
    except (TypeError, ValueError):
        concurrency = 5
    return max(1, concurrency)


def _investment_chunk_size(value: object | None = None) -> int:
    import os

    raw = (
        value
        if value is not None
        else os.getenv("INVESTMENT_MATERIALIZE_CHUNK_SIZE", "25")
    )
    try:
        chunk_size = int(str(raw))
    except (TypeError, ValueError):
        chunk_size = 25
    return max(1, chunk_size)


def _parse_materialize_window(
    *,
    from_date: str | None,
    to_date: str | None,
    window_days: int,
) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if to_date:
        parsed_to = datetime.combine(
            date.fromisoformat(to_date) + timedelta(days=1),
            dt_time.min,
            tzinfo=timezone.utc,
        )
    else:
        parsed_to = now

    if from_date:
        parsed_from = datetime.combine(
            date.fromisoformat(from_date),
            dt_time.min,
            tzinfo=timezone.utc,
        )
    else:
        parsed_from = parsed_to - timedelta(days=window_days)
    return parsed_from, parsed_to


def _investment_chunk_scope_id(run_id: str, chunk_index: int) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL, f"dev-health:investment:{run_id}:{chunk_index}"
    )


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
    llm_concurrency: int | None = None,
    force: bool = False,
    org_id: str = "",
    allow_unscoped: bool = False,
    llm_batch_mode: str | None = None,
    llm_batch_min_items: int | None = None,
    llm_batch_poll_interval_seconds: float | None = None,
    llm_batch_timeout_seconds: float | None = None,
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
        llm_concurrency: Maximum concurrent LLM categorizations
        force: Force recomputation even if cached
        org_id: Organization scope for work-graph/investment queries
        allow_unscoped: Allow real LLM materialization without an org scope

    Returns:
        dict with materialization status and stats
    """

    from dev_health_ops.llm import LLMAuthError, LLMError, resolve_provider_name
    from dev_health_ops.llm.credentials import resolve_llm_credentials
    from dev_health_ops.work_graph.investment.materialize import (
        MaterializeConfig,
        materialize_investments,
        resolve_llm_batch_min_items,
        resolve_llm_batch_mode,
        resolve_llm_batch_poll_interval_seconds,
        resolve_llm_batch_timeout_seconds,
    )

    db_url = db_url or _get_db_url()
    parsed_from, parsed_to = _parse_materialize_window(
        from_date=from_date,
        to_date=to_date,
        window_days=window_days,
    )

    logger.info(
        "Starting investment materialize task: from=%s to=%s repos=%s teams=%s",
        parsed_from.isoformat(),
        parsed_to.isoformat(),
        repo_ids or "all",
        team_ids or "all",
    )

    try:
        resolved_provider = resolve_provider_name(llm_provider, org_id=org_id or None)
        llm_credentials = resolve_llm_credentials(
            resolved_provider, org_id=org_id or None
        )
        config = MaterializeConfig(
            dsn=db_url,
            from_ts=parsed_from,
            to_ts=parsed_to,
            repo_ids=repo_ids,
            llm_provider=resolved_provider,
            persist_evidence_snippets=True,
            llm_model=llm_model,
            llm_api_key=llm_credentials.api_key,
            llm_base_url=llm_credentials.base_url,
            llm_concurrency=_llm_concurrency(llm_concurrency),
            team_ids=team_ids,
            force=force,
            org_id=org_id or None,
            allow_unscoped=allow_unscoped,
            llm_batch_mode=resolve_llm_batch_mode(llm_batch_mode),
            llm_batch_min_items=resolve_llm_batch_min_items(llm_batch_min_items),
            llm_batch_poll_interval_seconds=resolve_llm_batch_poll_interval_seconds(
                llm_batch_poll_interval_seconds
            ),
            llm_batch_timeout_seconds=resolve_llm_batch_timeout_seconds(
                llm_batch_timeout_seconds
            ),
        )
        stats = run_async(materialize_investments(config))
        return {"status": "success", "stats": stats}
    except LLMAuthError as exc:
        logger.error("Investment materialize task failed with LLM auth error: %s", exc)
        raise
    except LLMError as exc:
        logger.error(
            "Investment materialize task failed with classified LLM error: %s", exc
        )
        raise
    except Exception as exc:
        logger.exception("Investment materialize task failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_investment_materialize_chunk",
)
def run_investment_materialize_chunk(
    self,
    db_url: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    window_days: int = 30,
    repo_ids: list[str] | None = None,
    team_ids: list[str] | None = None,
    llm_provider: str = "auto",
    llm_model: str | None = None,
    llm_concurrency: int | None = None,
    force: bool = False,
    org_id: str = "",
    allow_unscoped: bool = False,
    run_id: str = "",
    computed_at: str = "",
    component_indexes: list[int] | None = None,
    chunk_index: int = 0,
    llm_batch_mode: str | None = None,
    llm_batch_min_items: int | None = None,
    llm_batch_poll_interval_seconds: float | None = None,
    llm_batch_timeout_seconds: float | None = None,
    max_component_nodes: int | None = None,
) -> dict:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.llm import LLMAuthError, LLMError, resolve_provider_name
    from dev_health_ops.llm.credentials import resolve_llm_credentials
    from dev_health_ops.metrics.checkpoints import (
        is_completed,
        mark_completed,
        mark_failed,
        mark_running,
    )
    from dev_health_ops.work_graph.investment.materialize import (
        MaterializeConfig,
        materialize_investments,
        resolve_llm_batch_min_items,
        resolve_llm_batch_mode,
        resolve_llm_batch_poll_interval_seconds,
        resolve_llm_batch_timeout_seconds,
    )

    db_url = db_url or _get_db_url()
    parsed_from, parsed_to = _parse_materialize_window(
        from_date=from_date,
        to_date=to_date,
        window_days=window_days,
    )
    shared_computed_at = datetime.fromisoformat(computed_at)
    if shared_computed_at.tzinfo is None:
        shared_computed_at = shared_computed_at.replace(tzinfo=timezone.utc)
    run_id = run_id or uuid.uuid4().hex
    checkpoint_scope = _investment_chunk_scope_id(run_id, chunk_index)
    checkpoint_id: uuid.UUID | None = None

    try:
        with get_postgres_session_sync() as session:
            if is_completed(
                session,
                org_id or "default",
                checkpoint_scope,
                "investment_materialize_chunk",
                shared_computed_at,
            ):
                return {
                    "status": "skipped",
                    "reason": "already_completed",
                    "chunk_index": chunk_index,
                    "records": 0,
                    "quotes": 0,
                }
            checkpoint = mark_running(
                session,
                org_id or "default",
                checkpoint_scope,
                "investment_materialize_chunk",
                shared_computed_at,
                str(self.request.id or "unknown"),
            )
            checkpoint_id = uuid.UUID(str(checkpoint.id))

        resolved_provider = resolve_provider_name(llm_provider, org_id=org_id or None)
        llm_credentials = resolve_llm_credentials(
            resolved_provider, org_id=org_id or None
        )
        config = MaterializeConfig(
            dsn=db_url,
            from_ts=parsed_from,
            to_ts=parsed_to,
            repo_ids=repo_ids,
            llm_provider=resolved_provider,
            persist_evidence_snippets=True,
            llm_model=llm_model,
            llm_api_key=llm_credentials.api_key,
            llm_base_url=llm_credentials.base_url,
            llm_concurrency=_llm_concurrency(llm_concurrency),
            team_ids=team_ids,
            force=force,
            org_id=org_id or None,
            allow_unscoped=allow_unscoped,
            run_id=run_id,
            computed_at=shared_computed_at,
            component_indexes=component_indexes,
            chunk_index=chunk_index,
            llm_batch_mode=resolve_llm_batch_mode(llm_batch_mode),
            llm_batch_min_items=resolve_llm_batch_min_items(llm_batch_min_items),
            llm_batch_poll_interval_seconds=resolve_llm_batch_poll_interval_seconds(
                llm_batch_poll_interval_seconds
            ),
            llm_batch_timeout_seconds=resolve_llm_batch_timeout_seconds(
                llm_batch_timeout_seconds
            ),
            # Frozen by the dispatcher: chunk workers must split components with
            # the SAME cap the dispatcher enumerated with, or component_indexes
            # would name different work units (CHAOS-2775 codex round 2).
            max_component_nodes=max_component_nodes,
        )
        stats = run_async(materialize_investments(config))

        if checkpoint_id is not None:
            with get_postgres_session_sync() as session:
                mark_completed(session, checkpoint_id)
        return {"status": "success", "chunk_index": chunk_index, "stats": stats}
    except (LLMAuthError, LLMError):
        if checkpoint_id is not None:
            try:
                with get_postgres_session_sync() as session:
                    mark_failed(session, checkpoint_id, "classified LLM failure")
            except Exception:
                logger.exception("Failed to mark investment chunk checkpoint failed")
        raise
    except Exception as exc:
        if checkpoint_id is not None:
            try:
                with get_postgres_session_sync() as session:
                    mark_failed(session, checkpoint_id, str(exc))
            except Exception:
                logger.exception("Failed to mark investment chunk checkpoint failed")
        logger.exception("Investment materialize chunk failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="metrics",
    name="dev_health_ops.workers.tasks.finalize_investment_materialize_partitioned",
)
def finalize_investment_materialize_partitioned(
    self,
    chunk_results: list,
    db_url: str | None = None,
    org_id: str = "",
    run_id: str = "",
    run_membership_backfill_after: bool = False,
) -> dict:
    db_url = db_url or _get_db_url()
    totals: dict[str, Any] = {
        "status": "success",
        "run_id": run_id,
        "chunks": len(chunk_results or []),
        "records": 0,
        "quotes": 0,
        "skipped_existing": 0,
        "llm_calls": 0,
        "llm_input_tokens": 0,
        "llm_output_tokens": 0,
        "llm_failures": 0,
        "llm_failure_counts": {},
        "oversized_components": 0,
        "dropped_edges": 0,
        "dropped_nodes": 0,
    }
    try:
        for result in chunk_results or []:
            stats = result.get("stats", {}) if isinstance(result, dict) else {}
            for key in (
                "records",
                "quotes",
                "skipped_existing",
                "llm_calls",
                "llm_input_tokens",
                "llm_output_tokens",
                "llm_failures",
            ):
                totals[key] += int(stats.get(key, 0) or 0)
            # Split stats are aggregated by MAX, not summed: every chunk
            # rebuilds the FULL component list (then materializes only its
            # component_indexes slice), so each chunk reports the same
            # graph-wide split counters — summing would multiply them by the
            # chunk count (CHAOS-2775 codex round 2).
            for key in ("oversized_components", "dropped_edges", "dropped_nodes"):
                totals[key] = max(totals[key], int(stats.get(key, 0) or 0))
            for failure_class, count in dict(
                stats.get("llm_failure_counts", {})
            ).items():
                current = totals["llm_failure_counts"].get(failure_class, 0)
                totals["llm_failure_counts"][failure_class] = current + int(count or 0)

        if run_membership_backfill_after:
            from dev_health_ops.work_graph.investment.backfill import (
                MembershipBackfillConfig,
                backfill_memberships,
            )

            totals["membership"] = backfill_memberships(
                MembershipBackfillConfig(
                    dsn=db_url, org_id=org_id or None, repo_ids=None
                )
            )
        return totals
    except Exception as exc:
        logger.exception("Investment materialize partition finalizer failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


@celery_app.task(
    bind=True,
    queue="default",
    name="dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned",
)
def dispatch_investment_materialize_partitioned(
    self,
    db_url: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    window_days: int = 30,
    repo_ids: list[str] | None = None,
    team_ids: list[str] | None = None,
    llm_provider: str = "auto",
    llm_model: str | None = None,
    llm_concurrency: int | None = None,
    force: bool = False,
    org_id: str = "",
    allow_unscoped: bool = False,
    chunk_size: int | None = None,
    llm_batch_mode: str | None = None,
    llm_batch_min_items: int | None = None,
    llm_batch_poll_interval_seconds: float | None = None,
    llm_batch_timeout_seconds: float | None = None,
) -> dict:
    from dev_health_ops.metrics.sinks.factory import create_sink
    from dev_health_ops.work_graph.investment.constants import (
        resolve_max_component_nodes,
    )
    from dev_health_ops.work_graph.investment.materialize import (
        _build_components,
        _resolve_repo_ids,
        resolve_llm_batch_min_items,
        resolve_llm_batch_mode,
        resolve_llm_batch_poll_interval_seconds,
        resolve_llm_batch_timeout_seconds,
    )
    from dev_health_ops.work_graph.investment.queries import fetch_work_graph_edges

    # Hoisted to guarantee definite assignment on every return path (CodeQL).
    #
    # CHAOS-2776: gate the finalizer's membership projection on SCOPE, not window.
    # The finalizer runs ``backfill_memberships`` (see
    # ``work_graph.investment.backfill`` module docstring for the CHAOS-2433
    # run-marker protocol), which is ALWAYS full-coverage BY CONSTRUCTION: it
    # iterates the FULL current work graph and projects from the latest persisted
    # investments per unit (argMax(computed_at)), independent of any materialize
    # window. ``from_date``/``to_date`` only bound which units get NEW LLM
    # investment rows; they do NOT bound projection coverage. So running the
    # projection after a WINDOWED org-wide materialize is safe and correct — it
    # republishes a full-coverage org-wide completion marker at >= the newest
    # investment clock, re-arming the read-path stale-generation guard
    # (CHAOS-2764, api/queries/investment_membership_scope.py).
    #
    # The old ``... or from_date or to_date`` gate broke the post-sync path: the
    # dispatcher (post_sync_dispatch.py) ALWAYS forwards the sync window as
    # from_date/to_date, so the finalizer NEVER projected after a post-sync
    # materialize. The guard then disarmed (investments newer than the marker,
    # scope_mode='unscoped_fallback') until the next daily 03:30 org-wide
    # projection — which the next sync immediately disarmed again, flooding the
    # Investment charts with stale work-unit generations (~18x effort inflation).
    #
    # Only repo/team-SCOPED runs must still skip publishing the org-wide marker: a
    # scoped projection would only cover in-scope units and blank every other
    # repo's membership for unscoped reads.
    run_membership = not (repo_ids or team_ids)

    # Resolve the component-size cap ONCE and freeze it for the whole
    # partitioned run: chunk workers rebuild the component list from a fresh
    # fetch, and component_indexes are positional — if a chunk worker resolved
    # a different INVESTMENT_MAX_COMPONENT_NODES from its own env, the split
    # would differ and index N would name a different work unit
    # (CHAOS-2775 codex round 2).
    frozen_max_component_nodes = resolve_max_component_nodes()

    db_url = db_url or _get_db_url()
    sink = create_sink(db_url)
    try:
        sink.ensure_schema()
        resolved_repo_ids = _resolve_repo_ids(
            sink, repo_ids, team_ids, config_org_id=org_id or ""
        )
        edges = fetch_work_graph_edges(
            sink, repo_ids=resolved_repo_ids, org_id=org_id or ""
        )
        components = _build_components(
            edges, max_component_nodes=frozen_max_component_nodes
        )
    finally:
        sink.close()

    if not components:
        return {"status": "no_components", "dispatched": 0, "chunks": 0}

    size = _investment_chunk_size(chunk_size)
    indexes = list(range(len(components)))
    chunks = [indexes[i : i + size] for i in range(0, len(indexes), size)]
    run_id = uuid.uuid4().hex
    computed_at = datetime.now(timezone.utc).isoformat()
    resolved_llm_batch_mode = resolve_llm_batch_mode(llm_batch_mode)
    resolved_llm_batch_min_items = resolve_llm_batch_min_items(llm_batch_min_items)
    resolved_llm_batch_poll_interval_seconds = resolve_llm_batch_poll_interval_seconds(
        llm_batch_poll_interval_seconds
    )
    resolved_llm_batch_timeout_seconds = resolve_llm_batch_timeout_seconds(
        llm_batch_timeout_seconds
    )

    header = []
    for chunk_index, chunk_indexes in enumerate(chunks):
        chunk_kwargs = {
            "db_url": db_url,
            "from_date": from_date,
            "to_date": to_date,
            "window_days": window_days,
            "repo_ids": repo_ids,
            "team_ids": team_ids,
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "llm_concurrency": llm_concurrency,
            "llm_batch_mode": resolved_llm_batch_mode,
            "llm_batch_min_items": resolved_llm_batch_min_items,
            "llm_batch_poll_interval_seconds": resolved_llm_batch_poll_interval_seconds,
            "llm_batch_timeout_seconds": resolved_llm_batch_timeout_seconds,
            "force": force,
            "org_id": org_id,
            "run_id": run_id,
            "computed_at": computed_at,
            "component_indexes": chunk_indexes,
            "chunk_index": chunk_index,
            "max_component_nodes": frozen_max_component_nodes,
        }
        if allow_unscoped:
            chunk_kwargs["allow_unscoped"] = True
        header.append(
            celery_app.signature(
                "dev_health_ops.workers.tasks.run_investment_materialize_chunk",
                kwargs=chunk_kwargs,
                queue="metrics",
            )
        )
    callback = celery_app.signature(
        "dev_health_ops.workers.tasks.finalize_investment_materialize_partitioned",
        kwargs={
            "db_url": db_url,
            "org_id": org_id,
            "run_id": run_id,
            "run_membership_backfill_after": run_membership,
        },
        queue="metrics",
    )
    chord(header, callback).apply_async()
    return {
        "status": "dispatched",
        "components": len(components),
        "chunks": len(chunks),
        "run_id": run_id,
        "computed_at": computed_at,
        "membership_in_finalizer": run_membership,
    }


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

    The SOLE writer of ``work_unit_membership`` rows AND the completion marker
    (CHAOS-2433 round-3 finding #2). Instead of re-running LLM categorization
    (cost + category drift), it rebuilds the work-graph components and re-emits
    ``work_unit_membership`` rows from the theme/subcategory distributions ALREADY
    persisted in ``work_unit_investments`` by the LLM materializer. It iterates
    the FULL current work graph (NOT a time window), so its org-wide completion
    marker is legitimately FULL-COVERAGE — unlike a date-windowed materialize,
    which is why the materializer no longer writes membership/markers.

    Runs in BOTH chains:
      - post-sync: build -> materialize (LLM, investments only) -> THIS (projects
        membership + marker from the just-persisted investments).
      - daily: build -> THIS (full-coverage projection / floor cadence).

    Uses the run_id / completion-marker protocol (CHAOS-2433): all membership
    rows are written first, then the completion marker is written last with a
    COMPLETION-time timestamp (now(), captured at marker write — not run start —
    so an overlapping run that finishes later wins argMax). Units whose current
    component hash has no persisted categorization (churned components) are
    skipped; the run_id protocol makes those nodes invisible without tombstones.
    See ``work_graph.investment.backfill`` for the full contract (CHAOS-2439/2433).

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
    """Daily floor-cadence projection of ``work_unit_membership`` (CHAOS-2439/2433).

    ``work_unit_membership`` (read by the work-graph theme/subcategory filter) is
    written EXCLUSIVELY by the no-LLM projection (``run_membership_backfill``),
    which runs post-sync (build -> materialize -> project) AND on this daily
    floor cadence. Idle-sync orgs and the post-deploy window would otherwise leave
    membership empty, stranding theme filters in the ``MEMBERSHIP_NOT_MATERIALIZED``
    degraded state (CHAOS-2427 #925) — this daily job repairs them.

    The daily job must NOT re-run LLM materialization (cost + category drift), so
    it fans out a CHEAP, no-LLM chain per active org:
    ``run_work_graph_build`` -> ``run_membership_backfill``. The build refreshes
    ``work_graph_edges`` (NO LLM); the projection then re-emits membership from the
    theme/subcategory distributions already persisted in ``work_unit_investments``
    by the post-sync LLM materializer, with FULL current-component coverage.

    The chain guarantees the projection only runs after the build *succeeds*, so it
    never projects against a stale/empty graph. The projection writes a complete
    run via the run_id / completion-marker protocol (CHAOS-2433) with a
    completion-time marker timestamp; readers always see the most recently
    completed full-coverage run.

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
        # Immutable chain: build FIRST (refreshes edges, NO LLM), then the cheap
        # no-LLM membership projection. Immutable chain (.s() + immutable=True)
        # keeps the build's return value out of the backfill's args.
        # Forward the resolved ``db_url`` to BOTH children so an explicit override
        # (manual dispatch_membership_backfill(db_url=...)) targets the requested
        # ClickHouse, not the workers' ambient instance. The scheduled path passes
        # the same value _get_db_url() already resolves, so behaviour is unchanged
        # when no override is supplied (CHAOS-2439 review).
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
