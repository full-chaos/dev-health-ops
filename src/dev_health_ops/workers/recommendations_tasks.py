from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)


class RecommendationsTeamFailure(Exception):
    """Raised when one or more teams fail to evaluate during a scheduled run.

    Surfacing this (instead of swallowing the per-team error and returning
    success) lets Celery retry the job and lets monitoring alert: a silently
    skipped team writes no ``fired=False`` tombstone, so stale fired guidance
    would otherwise linger while the task reported success (CHAOS-2373).
    """

    def __init__(self, org_id: str, failed_teams: list[str], total_teams: int) -> None:
        self.org_id = org_id
        self.failed_teams = failed_teams
        self.total_teams = total_teams
        super().__init__(
            f"Recommendations evaluation failed for {len(failed_teams)}/"
            f"{total_teams} team(s) in org={org_id}: {failed_teams}"
        )


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


def _discover_active_org_ids(strict: bool = False) -> list[str]:
    """Return the IDs of all active organisations (Postgres source of truth).

    Mirrors how ``dispatch_scheduled_metrics`` scopes work to live orgs. Falls
    back to ``["default"]`` (single-tenant / community installs) when the
    organizations table is empty or unavailable.

    When ``strict=True``, a Postgres enumeration failure RAISES instead of
    collapsing to the ``["default"]`` fallback, so a once-daily job retries on
    a DB outage rather than silently dispatching zero orgs as a clean success
    (CHAOS-2439).
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
        if strict:
            raise
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
    as_of_day: date,
    team_id: str | None = None,
) -> int:
    """Run the RuleEngine for every team in ``org_id`` and persist full state.

    Persists the *complete* rule state per team (fired recommendations **and**
    explicit ``fired=False`` tombstones for rules that no longer fire) so a
    recovered signal is cleared instead of lingering (CHAOS-2373).

    ``now`` is anchored to the *day after* the finalized partition (``as_of_day``)
    so the engine derives ``window_end == as_of_day + 1`` and — because the
    ClickHouse loader treats ``window_end`` as **exclusive** (``day < %(end)s``)
    — the just-finalized ``as_of_day`` partition is actually *read* rather than
    skipped (CHAOS-2373 round-2). ``as_of_day`` is passed separately so the
    readiness gate keys on the finalized partition's checkpoint, not on
    ``now.date()``.

    Returns the number of *fired* recommendations written (tombstones excluded).
    """
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.recommendations import registry as recommendations_registry
    from dev_health_ops.recommendations.engine import RuleEngine
    from dev_health_ops.recommendations.loader import ClickHouseMetricsLoader
    from dev_health_ops.recommendations.snapshot import RecommendationRecord

    if not _daily_metrics_ready(org_id, as_of_day):
        logger.info(
            "Daily metrics not finalized for org=%s day=%s; skipping recommendations",
            org_id,
            as_of_day,
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
        failed_teams: list[str] = []
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
                failed_teams.append(tid)
                continue
            records.extend(team_records)
            fired_count += sum(1 for r in team_records if r.fired)

        # Persist the state we DID compute before surfacing the failure, so the
        # teams that evaluated cleanly get fresh tombstones this run.
        if records:
            # CHAOS-2398: stamp every record with the actual wall-clock write
            # time. The engine derives both window_end AND computed_at from
            # ``now``, but on the as_of path ``now`` == as_of_day + 1 — a
            # constant across re-runs of the same finalized day. Two runs would
            # then write rows with an identical computed_at, and neither the
            # read-side two-stage ``argMax(fired, computed_at)`` nor the
            # ``ReplacingMergeTree(computed_at)`` version could deterministically
            # pick the latest, so a recovered signal might not clear. A single
            # monotonic write timestamp per run (later runs strictly newer) makes
            # the most recent write always win, while window_end stays a pure
            # function of as_of. True retries rewrite identical content under a
            # newer stamp — idempotent in effect, deterministic in winner.
            write_ts = datetime.now(timezone.utc)
            records = [replace(record, computed_at=write_ts) for record in records]
            sink.write_recommendations(records)

        logger.info(
            "recommendations job: org=%s teams=%d failed=%d fired=%d rows=%d window=%dd",
            org_id,
            len(team_ids),
            len(failed_teams),
            fired_count,
            len(records),
            window,
        )

        # Fail loudly on any per-team failure. A swallowed loader/rule error
        # writes no fired=False tombstone for that team, so stale fired guidance
        # would linger while the task reported success and monitoring/retries
        # saw nothing wrong (CHAOS-2373 round-2). Raising marks the job failed so
        # Celery retries it and operators can alert on the failure.
        if failed_teams:
            raise RecommendationsTeamFailure(
                org_id=org_id, failed_teams=failed_teams, total_teams=len(team_ids)
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
            ``day`` it finalized so the readiness gate keys on that partition's
            checkpoint and the engine's exclusive ``window_end`` is set to
            ``as_of + 1`` — which *includes* the finalized ``as_of`` partition
            (the loader filters ``day < window_end``). Correct across
            UTC-midnight finalizes and backfills. When ``None`` (beat backstop),
            today (UTC) is used and today's not-yet-finalized partition is
            naturally excluded.

    Returns:
        dict with job status and per-org fired counts.
    """
    db_url = db_url or _get_db_url()
    if as_of:
        # The finalized partition is ``as_of_day``; the readiness gate keys on
        # it. The engine derives its exclusive ``window_end`` from ``now.date()``
        # and the loader filters ``day < window_end``, so to *include* the
        # just-finalized ``as_of_day`` partition we anchor ``now`` to the day
        # AFTER it (CHAOS-2373 round-2). Anchoring to ``as_of_day`` itself made
        # ``window_end == as_of_day`` and silently excluded the partition the
        # finalize had just written.
        as_of_day = date.fromisoformat(as_of)
        now = datetime.combine(
            as_of_day + timedelta(days=1), time.min, tzinfo=timezone.utc
        )
    else:
        now = datetime.now(timezone.utc)
        as_of_day = now.date()

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
                as_of_day=as_of_day,
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
