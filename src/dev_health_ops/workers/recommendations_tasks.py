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
# WHICH run is authoritative for a day is the whole difficulty, and getting it
# wrong fails in one of two directions -- always-passing (the bug being fixed)
# or always-blocking (a live org wedged behind an abandoned row).
#
# Two disjoint producers write ``generation`` for the same (org, target_day):
#
#   * the nightly fan-out writes
#     ``"fixed-schedule:daily_metrics_fanout:" + <occurrence RFC3339 UTC>``
#     (internal/scheduler/fixed/producers.go:433-434, prefix constant
#     ``scheduledFanoutGenerationPrefix`` at
#     internal/jobs/metrics/daily/postgres.go:25). It is the ORG-WIDE run: the
#     heavy worker discovers every repository for the organization from
#     ClickHouse (``RepositoryIDs``, internal/jobs/metrics/daily/clickhouse.go:28-40,
#     dispatched at daily.go:188-194) and materializes the partitions from that
#     snapshot (postgres.go:402-470).
#   * every post-sync dispatch writes ``"post-sync:" + <sync run id>``
#     (cmd/dev-health-worker/sync_dispatch.go:36-41) with
#     ``RepositoryIDs: plan.RepositoryIDs`` -- only the repositories that sync
#     actually touched (internal/syncdispatchruntime/native_post_sync.go:403-410).
#     Its Python partitions recompute exactly those repo ids
#     (src/dev_health_ops/api/internal/worker_metrics.py:870-881).
#
# Only the fan-out establishes that the day is complete for the whole
# organization, so only fan-out generations are authoritative here. Ranking
# both families together by recency would let a one-repository post-sync run
# that finalized at 01:20 mask the org-wide 01:00 fan-out still writing
# partitions at 02:00 -- the exact partial read this gate exists to prevent.
# A post-sync run is therefore not authoritative in EITHER direction: it can
# neither certify the day nor block it. That is not lost coverage; the dead
# checkpoint this replaces was never written by a post-sync run either, and
# blocking on one would skip recommendations for any org that happened to be
# syncing at 02:00.
#
# Among fan-out generations the newest by ``created_at`` wins. ``created_at``
# is stamped by the Go store at insert (``store.now().UTC()``, postgres.go:176),
# so it is durable insertion order rather than intended-schedule order -- a
# catch-up occurrence is created long after the time embedded in its
# generation string. Taking only the newest is what stops a superseded
# generation abandoned in ``'pending'``/``'running'`` (CHAOS-3997 strands are
# real) from wedging a day whose newer fan-out finalized. ``generation`` is a
# tiebreaker for determinism only, never a semantic order.
#
# Runs whose own ``status`` is terminal-without-success are excluded rather
# than treated as in-flight. ``'canceled'`` is permitted by
# ``ck_daily_metrics_run_status`` (alembic 0057, widened by 0095) and Go's
# store refuses to claim dispatch or finalize for a run that is not
# ``'running'`` (postgres.go:377-379, :750-752), so such a row can NEVER reach
# ``finalization_status='succeeded'``; counting it as in-flight would block its
# target day forever. Neither value has a production writer for daily runs
# today -- excluding them is a wedge that cannot form, not a behaviour change.
#
# CHAOS-4073 item 1 -- investigated, deliberately NOT acted on here. Absence
# can also mean "the org-wide dispatch was REJECTED", not just "nothing to
# do": ``DailyMetricsFanoutProducer.Produce``
# (internal/scheduler/fixed/producers.go:415-459) wraps every organization's
# dispatch in its own savepoint (``startOrganization``, :461-495), and a
# permanent per-organization error (``permanentForOrganization``, :674-680)
# rolls that org's savepoint back and leaves NO ``daily_metrics_runs`` row
# for it -- indistinguishable, to the query below, from an org Go never
# dispatched at all.
#
# Two gaps stack, and either alone is enough to leave this branch unfixed
# for now (both filed as CHAOS-4074):
#
# 1. No PER-ORGANIZATION durable trace exists. The fan-out only counts
#    rejections (``rejected++``, producers.go:453) and records the fact in
#    AGGREGATE, on the occurrence as a whole, via
#    ``Outcome.Degraded = DegradedRejectedOrganizations``
#    ("organizations_rejected", producers.go:36,456,653) ->
#    ``fixed_schedule_occurrences.degraded_reason`` (alembic
#    0100_add_fixed_schedule_degraded_reason.py). That says "some org was
#    rejected tonight", never which one.
# 2. Even that aggregate signal is NOT READABLE from here. This gate runs as
#    the Python semantic-DB session (``get_postgres_session_sync``, bound to
#    ``POSTGRES_URI``), which production scopes to the ``devhealth_domain``
#    role (docs/operate/install/production.md:98,
#    deploy/go-workers/compose-go-workers.yml:234,337). Under the
#    domain/coordinator Postgres role split,
#    ``fixed_schedule_occurrences`` is coordinator-EXCLUSIVE by design
#    (internal/storage/postgres/domain_authorization.go's coordinatorPosture
#    doc: "adding fixed_schedule_occurrences to the domain role would
#    destroy" the boundary that keeps provider-sync/domain code out of
#    control-plane tables). A query against it from this process would not
#    return a coarser answer -- it would raise ``permission denied`` on
#    every call that reaches it, which is every ordinary "no row yet"
#    evaluation, converting the common case into constant, alert-drowning
#    noise on the very fail-open path CHAOS-4073 item 2 just made loud.
#
# Widening the domain role's grant, or querying with different credentials
# this process does not have, are both off the table -- one weakens a
# deliberate security boundary, the other doesn't exist in production today.
# Manufacturing a heuristic in its place is exactly what the ticket warns
# against. So this section stays a comment, not code: the gate's absence
# branch is unchanged, and CHAOS-4074 now tracks BOTH the missing
# per-organization grain and the missing domain-readable surface a real fix
# needs (e.g. Go persisting the rejection fact somewhere the domain role can
# already read, such as ``daily_metrics_runs`` itself).
_SCHEDULED_FANOUT_GENERATION_PREFIX = "fixed-schedule:daily_metrics_fanout:"

_LATEST_DAILY_METRICS_RUN_SQL = """
    SELECT finalization_status
    FROM daily_metrics_runs
    WHERE org_id = CAST(:org_id AS uuid)
      AND target_day = CAST(:target_day AS date)
      AND starts_with(generation, :fanout_prefix)
      AND status NOT IN ('canceled', 'failed')
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
    daily_finalize skip" (internal/scheduler/fixed/inventory.go:110-111).

    Semantics (see the ``_LATEST_DAILY_METRICS_RUN_SQL`` comment above for the
    Go-side citation of every status value):

    * The authoritative run for ``(org_id, day)`` -- the most recently created
      non-abandoned *fan-out* generation -- has
      ``finalization_status = 'succeeded'`` -> the day is durably complete for
      the whole organization -> **proceed**.
    * It exists with any other ``finalization_status`` -> the org-wide run is
      demonstrably unfinished -> **skip** (return ``False``). The fence is
      per-day, so a day that never finalizes cannot wedge later days: the next
      occurrence keys on its own ``target_day``.
    * No such row -> **proceed**. Absence means Go recorded no org-wide run
      for this org/day; there is no positive evidence of partial data, and
      the daily run self-corrects via tombstones. (This is the one branch
      that behaves like the dead checkpoint read did -- deliberately,
      because turning absence into a block would stop recommendations for
      every org whose day Go never dispatched. CHAOS-4073 item 1 investigated
      whether a rejected dispatch (see the comment above
      ``_SCHEDULED_FANOUT_GENERATION_PREFIX``) could be told apart from
      legitimate absence here; it deliberately stays unresolved -- see that
      comment for why acting on it would need either a security-boundary
      change or upstream Go work, both tracked in CHAOS-4074, rather than
      anything this function alone can safely do.)
    * The ``"default"`` sentinel -> **proceed**. ``daily_metrics_runs.org_id``
      is typed ``uuid``, so the single-tenant sentinel is unrepresentable in
      this table by construction (the same reason Go's fan-out refuses to
      invent one, internal/scheduler/fixed/producers.go:607-616).
    * Any read error -> **proceed, loudly** (CHAOS-4073 item 2 owner ruling).
      Fail-open stays -- fail-closed would wire an unknown gate-error rate
      directly to an org-wide recommendations wedge with no tombstones
      (CHAOS-2373) -- but the exception path now also increments
      ``RECOMMENDATIONS_READINESS_GATE_FAIL_OPEN_TOTAL`` (alertable, see
      ``alerts/rules.yml``'s ``RecommendationsReadinessGateFailingOpen``) and
      stays at ``logger.exception`` (ERROR) rather than being downgraded:
      this codebase's Sentry `LoggingIntegration` only turns ERROR+ records
      into events (``event_level=logging.ERROR``, src/dev_health_ops/sentry.py),
      and there are zero manual ``capture_exception`` call sites anywhere in
      this codebase to fall back on -- log-level IS the only automatic
      incident-surfacing path here. Choosing to proceed anyway is a policy
      decision, not a reclassification of the failure as benign, so the log
      stays ERROR-shaped while the *behaviour* stays fail-open. The same
      measurement-fails-toward-fine failure mode that let CHAOS-4066 sit
      vacuous is exactly what this guards against.
    """
    from sqlalchemy import text

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.metrics.prometheus import (
        RECOMMENDATIONS_READINESS_GATE_FAIL_OPEN_TOTAL,
    )

    if org_id == "default":
        return True

    target_day = day.date() if isinstance(day, datetime) else day
    try:
        with get_postgres_session_sync() as session:
            finalization_status = session.execute(
                text(_LATEST_DAILY_METRICS_RUN_SQL),
                {
                    "org_id": str(org_id),
                    "target_day": target_day.isoformat(),
                    "fanout_prefix": _SCHEDULED_FANOUT_GENERATION_PREFIX,
                },
            ).scalar_one_or_none()
    except Exception as exc:
        RECOMMENDATIONS_READINESS_GATE_FAIL_OPEN_TOTAL.labels(
            exception_type=type(exc).__name__
        ).inc()
        # ERROR, not WARNING: this codebase's Sentry LoggingIntegration only
        # turns ERROR+ records into events (event_level=logging.ERROR,
        # src/dev_health_ops/sentry.py), and nothing here calls
        # capture_exception directly -- downgrading the level would silently
        # drop this failure from Sentry with no other automatic path to see
        # it. Proceeding anyway (fail-open, CHAOS-4073 item 2 owner ruling)
        # is a policy choice about what the gate DOES, not about how loudly
        # the underlying read failure gets reported.
        logger.exception(
            "Failed to read daily metrics readiness state for org=%s day=%s: "
            "%s; treating as ready (fail-open, CHAOS-4073 item 2)",
            org_id,
            day,
            exc,
        )
        return True

    if finalization_status is None or finalization_status == _FINALIZATION_SUCCEEDED:
        return True
    logger.info(
        "Daily metrics fan-out run for org=%s day=%s has finalization_status=%s; "
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
