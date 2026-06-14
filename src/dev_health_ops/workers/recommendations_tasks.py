from __future__ import annotations

import logging
from datetime import date, datetime, time, timezone
from typing import Any

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)

# Checkpoint metric_type written by run_daily_metrics_finalize_task once all
# daily-metrics batches for an (org, day) have completed. Recommendations gate
# on this so a premature beat run never evaluates against partial metric tables.
_FINALIZE_METRIC_TYPE = "daily_finalize"


def _daily_metrics_ready(org_id: str, day: Any) -> bool:
    """Return False only when daily metrics for ``org_id``/``day`` are *in flight*.

    The race the gate guards against: the partitioned daily-metrics chord
    (``dispatch_daily_metrics_partitioned``) dispatches batch tasks
    asynchronously and writes a ``daily_finalize`` checkpoint only once every
    batch has finished. If recommendations evaluate while that finalize is still
    RUNNING/FAILED, they read partial metric tables and persist misleading rows
    for today (CHAOS-2373).

    Semantics:

    * A ``daily_finalize`` checkpoint exists for today but is **not COMPLETED**
      → metrics are demonstrably mid-flight → **skip** (return ``False``).
    * No checkpoint, or a COMPLETED checkpoint → **proceed**. (Absence means the
      chord path is not driving this org today; we have no positive evidence of
      partial data, and the daily run self-corrects via tombstones.)
    * The ``"default"`` sentinel and any read error → **proceed** (fail open;
      a checkpoint glitch must never permanently wedge the pipeline).
    """
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.metrics.checkpoints import get_checkpoint
    from dev_health_ops.models.checkpoints import CheckpointStatus

    if org_id == "default":
        return True

    checkpoint_day = datetime.combine(day, time.min, tzinfo=timezone.utc)
    try:
        with get_postgres_session_sync() as session:
            checkpoint = get_checkpoint(
                session, org_id, None, _FINALIZE_METRIC_TYPE, checkpoint_day
            )
            if checkpoint is None:
                return True
            return checkpoint.status == CheckpointStatus.COMPLETED
    except Exception:
        logger.exception(
            "Failed to read daily_finalize checkpoint for org=%s day=%s; "
            "treating as ready",
            org_id,
            day,
        )
        return True


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
    """Run the RuleEngine for every team in ``org_id`` and persist full state.

    Persists the *complete* rule state per team (fired recommendations **and**
    explicit ``fired=False`` tombstones for rules that no longer fire) so a
    recovered signal is cleared instead of lingering (CHAOS-2373).

    Returns the number of *fired* recommendations written (tombstones excluded).
    """
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.recommendations import registry as recommendations_registry
    from dev_health_ops.recommendations.engine import RuleEngine
    from dev_health_ops.recommendations.loader import ClickHouseMetricsLoader
    from dev_health_ops.recommendations.snapshot import RecommendationRecord

    if not _daily_metrics_ready(org_id, now.date()):
        logger.info(
            "Daily metrics not finalized for org=%s day=%s; skipping recommendations",
            org_id,
            now.date(),
        )
        return 0

    sink = ClickHouseMetricsSink(dsn=db_url)
    try:
        team_ids = [team_id] if team_id else _discover_team_ids(sink.client, org_id)
        if not team_ids:
            logger.info("No teams with recent activity for org_id=%s", org_id)
            return 0

        loader = ClickHouseMetricsLoader(client=sink.client, org_id=org_id)
        engine = RuleEngine(registry=recommendations_registry, loader=loader, now=now)

        records: list[RecommendationRecord] = []
        fired_count = 0
        for tid in team_ids:
            try:
                team_records = engine.evaluate_state(
                    team_id=tid, window=window, org_id=org_id
                )
            except Exception:
                logger.exception(
                    "Recommendations evaluation failed for org=%s team=%s",
                    org_id,
                    tid,
                )
                continue
            records.extend(team_records)
            fired_count += sum(1 for r in team_records if r.fired)

        if records:
            sink.write_recommendations(records)

        logger.info(
            "recommendations job: org=%s teams=%d fired=%d rows=%d window=%dd",
            org_id,
            len(team_ids),
            fired_count,
            len(records),
            window,
        )
        return fired_count
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
    as_of: str | None = None,
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
        as_of: ISO date (``YYYY-MM-DD``) of the finalized metrics partition the
            run should evaluate against. The finalize callback passes the exact
            ``day`` it finalized so the readiness gate, ``window_end`` and the
            written tombstones all key on that partition rather than wall-clock
            ``today`` — correct across UTC-midnight finalizes and backfills.
            When ``None`` (beat backstop), today (UTC) is used.

    Returns:
        dict with job status and per-org fired counts.
    """
    db_url = db_url or _get_db_url()
    if as_of:
        # Anchor evaluation to the finalized partition's end-of-day so the
        # engine derives window_end == as_of (RuleEngine uses now.date()).
        as_of_day = date.fromisoformat(as_of)
        now = datetime(
            as_of_day.year, as_of_day.month, as_of_day.day, tzinfo=timezone.utc
        )
    else:
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
