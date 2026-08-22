from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Any

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


# Go's durable daily-metrics completion state (CHAOS-4066).
#
# History: this gate used to read a Postgres ``metric_checkpoints`` row with
# ``metric_type='daily_finalize'``, written only by the Celery chord's
# ``run_daily_metrics_finalize_task``. That task's sole trigger was the
# ``run-daily-metrics`` beat entry, unreachable since Celery Beat stopped on
# 2026-08-19, and CHAOS-4026 (#1846) deleted the task itself. The checkpoint
# has had ZERO writers since 2026-08-19, so "no checkpoint -> proceed" fired
# on every call and the gate was permanently vacuous.
#
# Go owns the daily-metrics cadence now, and its completion state is a
# different table. One ``daily_metrics_runs`` row exists per
# ``(org_id, target_day, generation)`` (UNIQUE ``uq_daily_metrics_run_generation``,
# src/dev_health_ops/alembic/versions/0057_add_daily_metrics_river_state.py:55-58),
# and ``finalization_status`` is the durable completion fact:
#
# * ``'pending'`` at insert, for both writers
#   (internal/jobs/metrics/daily/postgres.go:109-114 and :176-181);
# * ``'running'`` once a finalizer holds the lease (same file:770-775), which
#   is only claimable after EVERY partition of the run succeeded (:735-742);
# * ``'succeeded'`` only when the finalize step committed (:830-836), or when
#   the run terminalizes as an explicit no-work outcome -- an organization with
#   no repositories sets ``status='no_repositories'`` AND
#   ``finalization_status='succeeded'`` together (:452-454), so a legitimate
#   empty day proceeds rather than blocking forever;
# * ``'failed'`` when a claimed finalize is released (:866-875). River retries
#   the finalize job, so this converges; until it does, the metric tables are
#   demonstrably incomplete.
#
# "Latest generation for the org/day" is ordered by ``created_at``, NOT by the
# ``generation`` string. ``generation`` is an opaque ``varchar(64)`` written by
# two disjoint, non-comparable producers:
#
#   * the nightly fan-out writes
#     ``"fixed-schedule:daily_metrics_fanout:" + <occurrence RFC3339 UTC>``
#     (internal/scheduler/fixed/producers.go:433-434), and
#   * every post-sync dispatch writes ``"post-sync:" + <sync run id>``
#     (cmd/dev-health-worker/sync_dispatch.go:38).
#
# Both land on the same ``target_day`` -- the fan-out uses the occurrence's
# scheduled day (producers.go:433, 486) and post-sync uses the sync's
# ``to``/now day (internal/syncdispatchruntime/native_post_sync.go:389-393, 409-410) --
# so several generations per (org, day) are ordinary, and ordering them
# lexicographically by ``generation`` would compare ``"fixed-schedule:..."``
# against ``"post-sync:..."``, which means nothing. ``created_at`` is stamped
# by the Go store at insert (``store.now().UTC()``, postgres.go:109 and :176),
# so the newest row is the authoritative run for that day. Gating on the
# newest one only -- rather than on "any run not yet finalized" -- is also
# what keeps the gate from wedging: a superseded generation abandoned in
# ``'pending'``/``'running'`` (CHAOS-3997 strands are real) cannot block a day
# whose newer run has finalized. ``generation`` is a tiebreaker for
# determinism only, never a semantic order.
_LATEST_DAILY_METRICS_RUN_SQL = """
    SELECT finalization_status
    FROM daily_metrics_runs
    WHERE org_id = CAST(:org_id AS uuid)
      AND target_day = CAST(:target_day AS date)
    ORDER BY created_at DESC, generation DESC
    LIMIT 1
"""

_FINALIZATION_SUCCEEDED = "succeeded"


def _daily_metrics_ready(org_id: str, day: Any) -> bool:
    """Return False only when Go's daily metrics for ``org_id``/``day`` are unfinished.

    The race the gate guards against: Go dispatches a daily-metrics run's
    repository partitions asynchronously and finalizes only once every one of
    them has succeeded. If recommendations evaluate before that, they read
    partial metric tables and persist misleading fired/tombstone rows for the
    day (CHAOS-2373). The fixed-schedule inventory states the requirement
    directly -- ``recommendations_daily_fanout`` is the "safety net behind the
    finalize-gated primary trigger. The producer must preserve the in-flight
    daily_finalize skip" (internal/scheduler/fixed/inventory.go:110-112).

    Semantics (see the ``_LATEST_DAILY_METRICS_RUN_SQL`` comment above for the
    Go-side citation of every status value):

    * The authoritative (most recently created) ``daily_metrics_runs`` row for
      ``(org_id, day)`` has ``finalization_status = 'succeeded'`` -> the day's
      metrics are durably complete -> **proceed**.
    * That row exists with any other ``finalization_status`` -> the run is
      demonstrably unfinished -> **skip** (return ``False``). The fence is
      per-day, so a day that never finalizes cannot wedge later days: the next
      occurrence keys on its own ``target_day``.
    * No row at all -> **proceed**. Absence means Go recorded no run for this
      org/day; there is no positive evidence of partial data, and the daily run
      self-corrects via tombstones. (This is the one branch that behaves like
      the dead checkpoint read did -- deliberately, because turning absence
      into a block would stop recommendations for every org whose day Go never
      dispatched.)
    * The ``"default"`` sentinel -> **proceed**. ``daily_metrics_runs.org_id``
      is typed ``uuid``, so the single-tenant sentinel is unrepresentable in
      this table by construction (the same reason Go's fan-out refuses to
      invent one, internal/scheduler/fixed/producers.go:607-616).
    * Any read error -> **proceed** (fail open; a database glitch must never
      permanently wedge the pipeline).
    """
    from sqlalchemy import text

    from dev_health_ops.db import get_postgres_session_sync

    if org_id == "default":
        return True

    target_day = day.date() if isinstance(day, datetime) else day
    try:
        with get_postgres_session_sync() as session:
            finalization_status = session.execute(
                text(_LATEST_DAILY_METRICS_RUN_SQL),
                {"org_id": str(org_id), "target_day": target_day.isoformat()},
            ).scalar_one_or_none()
    except Exception:
        logger.exception(
            "Failed to read daily_metrics_runs for org=%s day=%s; treating as ready",
            org_id,
            day,
        )
        return True

    if finalization_status is None or finalization_status == _FINALIZATION_SUCCEEDED:
        return True
    logger.info(
        "Daily metrics run for org=%s day=%s has finalization_status=%s; "
        "metrics are not final",
        org_id,
        day,
        finalization_status,
    )
    return False


def _discover_team_ids(client: Any, org_id: str) -> list[str]:
    """Return team IDs with recent activity for ``org_id`` from ClickHouse.

    Sourced from ``work_item_metrics_daily`` — the same table that feeds the
    recommendation snapshot signals — so we only evaluate teams that have data.
    """
    query = """
        SELECT DISTINCT team_id
        FROM work_item_metrics_daily FINAL
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
    readiness gate keys on the day whose ``daily_metrics_runs`` row it must
    check, not on ``now.date()``.

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


# CHAOS-4026 (2026-08-21): the beat-scheduled ``run_recommendations_job``
# Celery task (and its ``_discover_active_org_ids`` all-org fan-out) were
# deleted -- Go's `recommendations_daily` fixed schedule now owns the
# periodic cadence and Celery Beat has not scheduled this since the
# 2026-08-19 stop. ``_compute_recommendations_for_org`` above stays: it is
# still the live, per-org compute invoked synchronously by the dormant-Go
# operational bridge (api/internal/worker_metrics.py::_run_recommendations),
# and by nothing beat-shaped any more.
