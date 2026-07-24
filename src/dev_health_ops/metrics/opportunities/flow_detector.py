"""Flow / Improve opportunity detector (CHAOS-2218, Phase 1).

This module is **read-only** — it queries precomputed ClickHouse rows and
returns scored :class:`~.models.ImproveOpportunity` objects.  It never writes
to storage; persistence is deferred to a future phase.

Architecture
------------
- Two ``GROUP BY`` queries run in parallel via :func:`asyncio.gather`:
  one over ``repo_metrics_daily`` (repo entities),
  one over ``work_item_metrics_daily`` (team entities).
- Seven threshold rule functions are applied to the aggregated rows.
- ``org_id`` is always injected via :class:`OrgScopedQuery` — the detector
  never mixes rows from different organisations.
- Per-rule ``try/except`` ensures a single bad row never blanks the result.
- On total failure the detector logs and returns ``[]``.

Column names are verified against ClickHouse migrations:
- ``repo_metrics_daily``:  pr_first_review_p50_hours, pr_rework_ratio,
  rework_churn_ratio_30d, change_failure_rate, total_loc_touched (001, 004)
- ``work_item_metrics_daily``:  cycle_time_p50_hours, wip_congestion_ratio,
  items_completed, defect_intro_rate (001, 002)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from dev_health_ops.clickhouse_dedup import dedup_from
from dev_health_ops.metrics.opportunities.models import (
    FlowScopeInput,
    ImproveOpportunity,
    ImproveOpportunityKind,
)
from dev_health_ops.metrics.opportunities.scoring import (
    clamp,
    score_delta,
    score_ratio,
    stable_opportunity_id,
)
from dev_health_ops.metrics.query_builder import OrgScopedQuery

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds (blueprint values)
# ---------------------------------------------------------------------------

_REVIEW_LATENCY_THRESHOLD_HOURS = 24.0  # p50 first-review > 24 h
_CYCLE_TIME_THRESHOLD_HOURS = 120.0  # p50 cycle > 5 days
_REWORK_RATIO_THRESHOLD = 0.20  # > 20 % of PRs are rework
_WIP_CONGESTION_THRESHOLD = 0.40  # congestion ratio > 40 %
_LOW_THROUGHPUT_THRESHOLD = 2.0  # items_completed < 2 over window
_HIGH_CHURN_THRESHOLD = 0.30  # rework_churn_ratio_30d > 30 %
_CHANGE_FAILURE_THRESHOLD = 0.15  # change_failure_rate > 15 %

# Minimum distinct days of data required for a row to be considered
_MIN_DATA_DAYS = 5

# ---------------------------------------------------------------------------
# Recommended actions per kind (static map — same strings as
# _METRIC_SUGGESTED_EXPERIMENTS in services/opportunities.py but tied to a
# scored entity rather than a metric key).
# ---------------------------------------------------------------------------

_RECOMMENDED_ACTIONS: dict[ImproveOpportunityKind, str] = {
    ImproveOpportunityKind.HIGH_REVIEW_LATENCY: (
        "Reserve a daily review block for PRs waiting longest for first response."
    ),
    ImproveOpportunityKind.SLOW_CYCLE_TIME: (
        "Trace the oldest active items to their current waiting state."
    ),
    ImproveOpportunityKind.HIGH_REWORK: (
        "Compare reopened or rewritten work against its original acceptance criteria."
    ),
    ImproveOpportunityKind.HIGH_WIP: (
        "Set a short-term WIP limit and finish active items before starting more."
    ),
    ImproveOpportunityKind.LOW_THROUGHPUT: (
        "Audit recently completed items for the smallest repeatable delivery pattern."
    ),
    ImproveOpportunityKind.HIGH_CHURN: (
        "Review the files with the largest churn increase for unclear ownership or scope."
    ),
    ImproveOpportunityKind.HIGH_CHANGE_FAILURE: (
        "Review recent failed changes for the earliest detectable signal before release."
    ),
}


def _severity(score: float) -> str:
    """Map a normalised score to a severity label."""
    if score >= 0.75:
        return "high"
    if score >= 0.50:
        return "medium"
    return "low"


def _make_opportunity(
    *,
    kind: ImproveOpportunityKind,
    entity_type: str,
    entity_id: str,
    title: str,
    rationale: str,
    score: float,
    evidence_refs: list[str],
) -> ImproveOpportunity:
    clamped = clamp(score)
    return ImproveOpportunity(
        opportunity_id=stable_opportunity_id(kind, entity_id, None),
        kind=kind,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_display_name=None,
        title=title,
        rationale=rationale,
        score=clamped,
        severity=_severity(clamped),
        evidence_refs=evidence_refs,
        recommended_action=_RECOMMENDED_ACTIONS[kind],
    )


# ---------------------------------------------------------------------------
# Rule functions — each returns an ImproveOpportunity | None
# ---------------------------------------------------------------------------


def _rule_high_review_latency(
    row: dict[str, Any],
) -> ImproveOpportunity | None:
    """Fire when ``pr_first_review_p50_hours > threshold``."""
    value = _float_or_none(row.get("pr_first_review_p50_hours"))
    if value is None or value <= _REVIEW_LATENCY_THRESHOLD_HOURS:
        return None
    entity_id = str(row["entity_id"])
    return _make_opportunity(
        kind=ImproveOpportunityKind.HIGH_REVIEW_LATENCY,
        entity_type="repo",
        entity_id=entity_id,
        title=f"High review latency in {entity_id}",
        rationale=(
            f"Median first-review time was {value:.1f} h over the last "
            f"{row.get('window_days', 30)} days "
            f"(threshold: {_REVIEW_LATENCY_THRESHOLD_HOURS:.0f} h)."
        ),
        score=score_ratio(value, _REVIEW_LATENCY_THRESHOLD_HOURS),
        evidence_refs=[f"repo_metrics_daily:pr_first_review_p50_hours:{entity_id}"],
    )


def _rule_slow_cycle_time(
    row: dict[str, Any],
) -> ImproveOpportunity | None:
    """Fire when ``cycle_time_p50_hours > threshold``."""
    value = _float_or_none(row.get("cycle_time_p50_hours"))
    if value is None or value <= _CYCLE_TIME_THRESHOLD_HOURS:
        return None
    entity_id = str(row["entity_id"])
    return _make_opportunity(
        kind=ImproveOpportunityKind.SLOW_CYCLE_TIME,
        entity_type="team",
        entity_id=entity_id,
        title=f"Slow cycle time for {entity_id}",
        rationale=(
            f"Median cycle time was {value:.1f} h over the last "
            f"{row.get('window_days', 30)} days "
            f"(threshold: {_CYCLE_TIME_THRESHOLD_HOURS:.0f} h)."
        ),
        score=score_ratio(value, _CYCLE_TIME_THRESHOLD_HOURS),
        evidence_refs=[f"work_item_metrics_daily:cycle_time_p50_hours:{entity_id}"],
    )


def _rule_high_rework(
    row: dict[str, Any],
) -> ImproveOpportunity | None:
    """Fire when ``pr_rework_ratio > threshold``."""
    value = _float_or_none(row.get("pr_rework_ratio"))
    if value is None or value <= _REWORK_RATIO_THRESHOLD:
        return None
    entity_id = str(row["entity_id"])
    return _make_opportunity(
        kind=ImproveOpportunityKind.HIGH_REWORK,
        entity_type="repo",
        entity_id=entity_id,
        title=f"High rework ratio in {entity_id}",
        rationale=(
            f"PR rework ratio was {value:.0%} over the last "
            f"{row.get('window_days', 30)} days "
            f"(threshold: {_REWORK_RATIO_THRESHOLD:.0%})."
        ),
        score=score_ratio(value, _REWORK_RATIO_THRESHOLD),
        evidence_refs=[f"repo_metrics_daily:pr_rework_ratio:{entity_id}"],
    )


def _rule_high_wip(
    row: dict[str, Any],
) -> ImproveOpportunity | None:
    """Fire when ``wip_congestion_ratio > threshold``."""
    value = _float_or_none(row.get("wip_congestion_ratio"))
    if value is None or value <= _WIP_CONGESTION_THRESHOLD:
        return None
    entity_id = str(row["entity_id"])
    return _make_opportunity(
        kind=ImproveOpportunityKind.HIGH_WIP,
        entity_type="team",
        entity_id=entity_id,
        title=f"High WIP congestion for {entity_id}",
        rationale=(
            f"WIP congestion ratio was {value:.0%} over the last "
            f"{row.get('window_days', 30)} days "
            f"(threshold: {_WIP_CONGESTION_THRESHOLD:.0%})."
        ),
        score=score_ratio(value, _WIP_CONGESTION_THRESHOLD),
        evidence_refs=[f"work_item_metrics_daily:wip_congestion_ratio:{entity_id}"],
    )


def _rule_low_throughput(
    row: dict[str, Any],
) -> ImproveOpportunity | None:
    """Fire when ``items_completed < threshold`` (low throughput signal)."""
    value = _float_or_none(row.get("items_completed"))
    if value is None or value >= _LOW_THROUGHPUT_THRESHOLD:
        return None
    entity_id = str(row["entity_id"])
    # score: distance below threshold (lower value → higher score)
    gap = max(0.0, _LOW_THROUGHPUT_THRESHOLD - value)
    return _make_opportunity(
        kind=ImproveOpportunityKind.LOW_THROUGHPUT,
        entity_type="team",
        entity_id=entity_id,
        title=f"Low throughput for {entity_id}",
        rationale=(
            f"Only {value:.0f} items were completed over the last "
            f"{row.get('window_days', 30)} days "
            f"(threshold: {_LOW_THROUGHPUT_THRESHOLD:.0f})."
        ),
        score=score_delta(gap, _LOW_THROUGHPUT_THRESHOLD),
        evidence_refs=[f"work_item_metrics_daily:items_completed:{entity_id}"],
    )


def _rule_high_churn(
    row: dict[str, Any],
) -> ImproveOpportunity | None:
    """Fire when ``rework_churn_ratio_30d > threshold``."""
    value = _float_or_none(row.get("rework_churn_ratio_30d"))
    if value is None or value <= _HIGH_CHURN_THRESHOLD:
        return None
    entity_id = str(row["entity_id"])
    return _make_opportunity(
        kind=ImproveOpportunityKind.HIGH_CHURN,
        entity_type="repo",
        entity_id=entity_id,
        title=f"High rework churn in {entity_id}",
        rationale=(
            f"Rework churn ratio was {value:.0%} over the last 30 days "
            f"(threshold: {_HIGH_CHURN_THRESHOLD:.0%})."
        ),
        score=score_ratio(value, _HIGH_CHURN_THRESHOLD),
        evidence_refs=[f"repo_metrics_daily:rework_churn_ratio_30d:{entity_id}"],
    )


def _rule_high_change_failure(
    row: dict[str, Any],
) -> ImproveOpportunity | None:
    """Fire when ``change_failure_rate > threshold``."""
    value = _float_or_none(row.get("change_failure_rate"))
    if value is None or value <= _CHANGE_FAILURE_THRESHOLD:
        return None
    entity_id = str(row["entity_id"])
    return _make_opportunity(
        kind=ImproveOpportunityKind.HIGH_CHANGE_FAILURE,
        entity_type="repo",
        entity_id=entity_id,
        title=f"High change failure rate in {entity_id}",
        rationale=(
            f"Change failure rate was {value:.0%} over the last "
            f"{row.get('window_days', 30)} days "
            f"(threshold: {_CHANGE_FAILURE_THRESHOLD:.0%})."
        ),
        score=score_ratio(value, _CHANGE_FAILURE_THRESHOLD),
        evidence_refs=[f"repo_metrics_daily:change_failure_rate:{entity_id}"],
    )


# ---------------------------------------------------------------------------
# Rule registry — maps to the correct entity_type columns
# ---------------------------------------------------------------------------

_REPO_RULES = [
    _rule_high_review_latency,
    _rule_high_rework,
    _rule_high_churn,
    _rule_high_change_failure,
]

_TEAM_RULES = [
    _rule_slow_cycle_time,
    _rule_high_wip,
    _rule_low_throughput,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class FlowOpportunityDetector:
    """Rule-based flow / improve opportunity detector.

    This class is **read-only**.  It queries precomputed ClickHouse rows
    produced by the daily metrics jobs and returns scored
    :class:`~.models.ImproveOpportunity` objects.

    Parameters
    ----------
    client:
        A ClickHouse-connect (or compatible) client instance.
    """

    def __init__(self, client: Any) -> None:
        self.client = client

    async def detect(
        self,
        org_id: str,
        scope: FlowScopeInput | None = None,
        *,
        limit: int = 10,
        window_days: int = 30,
    ) -> list[ImproveOpportunity]:
        """Detect and score flow opportunities.

        Runs two ClickHouse GROUP BY queries in parallel (repo metrics and
        team/work-item metrics), applies threshold rules, and returns the
        top *limit* opportunities sorted by score descending.

        Returns ``[]`` on total failure (individual rule failures are
        logged and skipped).
        """
        bounded_limit = max(1, min(limit, 100))
        try:
            repo_rows, team_rows = await asyncio.gather(
                self._load_repo_rows(org_id, scope, window_days),
                self._load_team_rows(org_id, scope, window_days),
            )
        except Exception:
            logger.exception(
                "FlowOpportunityDetector: failed to load metric rows for org=%s",
                org_id,
            )
            return []

        opportunities: list[ImproveOpportunity] = []
        opportunities.extend(
            self._apply_rules(repo_rows, _REPO_RULES, window_days=window_days)
        )
        opportunities.extend(
            self._apply_rules(team_rows, _TEAM_RULES, window_days=window_days)
        )
        opportunities.sort(key=lambda o: o.score, reverse=True)
        return opportunities[:bounded_limit]

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    async def _load_repo_rows(
        self,
        org_id: str,
        scope: FlowScopeInput | None,
        window_days: int,
    ) -> list[dict[str, Any]]:
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {"window_days": window_days}
        filters: list[str] = ["day >= today() - {window_days:UInt32}"]

        if scope is not None and scope.repo_id is not None:
            params["repo_id"] = str(scope.repo_id)
            filters.append("repo_id = {repo_id:UUID}")

        org_scope = OrgScopedQuery(org_id)
        params = org_scope.inject(params)
        org_expr = org_scope.expression()
        if org_expr:
            filters.append(org_expr)

        where_clause = " AND ".join(filters)
        query = f"""
        SELECT
            toString(repo_id) AS entity_id,
            uniqExact(day) AS data_days,
            avg(pr_first_review_p50_hours) AS pr_first_review_p50_hours,
            avg(pr_rework_ratio) AS pr_rework_ratio,
            avg(rework_churn_ratio_30d) AS rework_churn_ratio_30d,
            avg(change_failure_rate) AS change_failure_rate,
            sum(total_loc_touched) AS total_loc_touched
        FROM {dedup_from("repo_metrics_daily")}
        WHERE {where_clause}
        GROUP BY repo_id
        HAVING data_days >= {_MIN_DATA_DAYS}
        ORDER BY data_days DESC
        LIMIT 500
        """
        rows = await query_dicts(self.client, query, params)
        for row in rows:
            row["window_days"] = window_days
        return rows

    async def _load_team_rows(
        self,
        org_id: str,
        scope: FlowScopeInput | None,
        window_days: int,
    ) -> list[dict[str, Any]]:
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {"window_days": window_days}
        filters: list[str] = ["day >= today() - {window_days:UInt32}"]

        if scope is not None and scope.team_id is not None:
            params["team_id"] = scope.team_id
            filters.append("team_id = {team_id:String}")

        org_scope = OrgScopedQuery(org_id)
        params = org_scope.inject(params)
        org_expr = org_scope.expression()
        if org_expr:
            filters.append(org_expr)

        where_clause = " AND ".join(filters)
        query = f"""
        SELECT
            team_id AS entity_id,
            uniqExact(day) AS data_days,
            avg(cycle_time_p50_hours) AS cycle_time_p50_hours,
            avg(wip_congestion_ratio) AS wip_congestion_ratio,
            sum(items_completed) AS items_completed,
            avg(defect_intro_rate) AS defect_intro_rate
        FROM {dedup_from("work_item_metrics_daily")}
        WHERE {where_clause}
          AND team_id != ''
        GROUP BY team_id
        HAVING data_days >= {_MIN_DATA_DAYS}
        ORDER BY data_days DESC
        LIMIT 500
        """
        rows = await query_dicts(self.client, query, params)
        for row in rows:
            row["window_days"] = window_days
        return rows

    # ------------------------------------------------------------------
    # Rule application
    # ------------------------------------------------------------------

    def _apply_rules(
        self,
        rows: list[dict[str, Any]],
        rules: list[Any],
        *,
        window_days: int,
    ) -> list[ImproveOpportunity]:
        results: list[ImproveOpportunity] = []
        for row in rows:
            for rule in rules:
                try:
                    opp = rule(row)
                    if opp is not None:
                        results.append(opp)
                except Exception:
                    logger.exception(
                        "FlowOpportunityDetector: rule %s failed for row=%r",
                        getattr(rule, "__name__", rule),
                        row,
                    )
        return results
