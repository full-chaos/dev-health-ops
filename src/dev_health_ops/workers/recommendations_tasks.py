from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)


def _discover_active_org_ids() -> list[str]:
    """Return the IDs of all active organisations (Postgres source of truth).

    Mirrors how ``dispatch_scheduled_metrics`` scopes work to live orgs. Falls
    back to ``["default"]`` (single-tenant / community installs) when the
    organizations table is empty or unavailable.
    """
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.users import Organization

    try:
        with get_postgres_session_sync() as session:
            rows = (
                session.query(Organization.id)
                .filter(Organization.is_active.is_(True))
                .all()
            )
        org_ids = [str(row[0]) for row in rows]
    except Exception:
        logger.exception("Failed to enumerate active organizations")
        org_ids = []

    return org_ids or ["default"]


def _discover_team_ids(client: Any, org_id: str) -> list[str]:
    """Return team IDs with recent activity for ``org_id`` from ClickHouse.

    Sourced from ``work_item_metrics_daily`` — the same table that feeds the
    recommendation snapshot signals — so we only evaluate teams that have data.
    """
    query = """
        SELECT DISTINCT team_id
        FROM work_item_metrics_daily
        WHERE day >= today() - 30
          AND team_id != ''
    """
    params: dict[str, str] = {}
    if org_id and org_id != "default":
        query += " AND org_id = %(org_id)s"
        params["org_id"] = org_id

    result = client.query(query, parameters=params)
    return [str(row[0]) for row in (result.result_rows or []) if row[0]]


def _compute_recommendations_for_org(
    org_id: str,
    db_url: str,
    window: int,
    now: datetime,
    team_id: str | None = None,
) -> int:
    """Run the RuleEngine for every team in ``org_id`` and persist results.

    Returns the number of fired recommendations written.
    """
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.recommendations import registry as recommendations_registry
    from dev_health_ops.recommendations.engine import RuleEngine
    from dev_health_ops.recommendations.loader import (
        ClickHouseMetricsLoader,
        recommendation_to_record,
    )
    from dev_health_ops.recommendations.snapshot import RecommendationRecord

    sink = ClickHouseMetricsSink(dsn=db_url)
    try:
        team_ids = [team_id] if team_id else _discover_team_ids(sink.client, org_id)
        if not team_ids:
            logger.info("No teams with recent activity for org_id=%s", org_id)
            return 0

        loader = ClickHouseMetricsLoader(client=sink.client, org_id=org_id)
        engine = RuleEngine(registry=recommendations_registry, loader=loader, now=now)

        records: list[RecommendationRecord] = []
        for tid in team_ids:
            try:
                recommendations = engine.evaluate_all(
                    team_id=tid, window=window, org_id=org_id
                )
            except Exception:
                logger.exception(
                    "Recommendations evaluation failed for org=%s team=%s",
                    org_id,
                    tid,
                )
                continue
            records.extend(recommendation_to_record(rec) for rec in recommendations)

        if records:
            sink.write_recommendations(records)

        logger.info(
            "recommendations job: org=%s teams=%d fired=%d window=%dd",
            org_id,
            len(team_ids),
            len(records),
            window,
        )
        return len(records)
    finally:
        sink.close()


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_recommendations_job",
)
def run_recommendations_job(
    self,
    org_id: str | None = None,
    db_url: str | None = None,
    window: int = 14,
    team_id: str | None = None,
) -> dict:
    """Compute rule-based recommendations for every active org + team.

    This is the scheduled live path for ``recommendations_daily`` — without it
    the table is only ever written by the manual ``dev-hops recommendations
    compute`` CLI, leaving real orgs' home RankedSignals, the
    ``recommendations(team, window)`` GraphQL query, and the Operating Review
    empty (CHAOS-2373).

    Args:
        org_id: Restrict to a single org. When ``None``, enumerate all active
            organisations from Postgres.
        db_url: ClickHouse connection string (defaults to CLICKHOUSE_URI env).
        window: Evaluation window in days (default 14).
        team_id: Restrict to a single team. When ``None``, discover all teams
            with recent activity for each org from ClickHouse.

    Returns:
        dict with job status and per-org fired counts.
    """
    db_url = db_url or _get_db_url()
    now = datetime.now(timezone.utc)

    org_ids = [org_id] if org_id else _discover_active_org_ids()

    results: dict[str, int] = {}
    total_fired = 0
    try:
        for oid in org_ids:
            fired = _compute_recommendations_for_org(
                org_id=oid,
                db_url=db_url,
                window=window,
                now=now,
                team_id=team_id,
            )
            results[oid] = fired
            total_fired += fired
    except Exception as exc:
        logger.exception("Recommendations job failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))

    return {
        "status": "success",
        "orgs": len(org_ids),
        "fired": total_fired,
        "per_org": results,
    }
