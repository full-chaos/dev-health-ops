from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Literal

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from ..models.filters import MetricFilter
from ..models.schemas import (
    ConstraintCard,
    ConstraintEvidence,
    Coverage,
    EventItem,
    Freshness,
    HomeDataConfidence,
    HomeHealthState,
    HomeLimitingFactor,
    HomeResponse,
    HomeSignal,
    MetricDelta,
    ReworkThemeAllocation,
    ScopeEntityRef,
    SparkPoint,
    SummarySentence,
)
from ..queries.client import clickhouse_client, query_dicts
from ..queries.explain import fetch_metric_driver_delta
from ..queries.freshness import fetch_coverage, fetch_last_ingested_at
from ..queries.metrics import (
    fetch_blocked_hours,
    fetch_metric_series,
    fetch_metric_value,
    fetch_rework_theme_allocation,
)
from ..utils import delta_pct, safe_float, safe_transform
from .cache import TTLCache
from .filtering import (
    filter_cache_key,
    scope_filter_for_metric,
    time_window,
    work_category_filter,
)

logger = logging.getLogger(__name__)

SignalDirection = Literal["up", "down", "flat"]
SignalSeverity = Literal["critical", "high", "medium", "low"]
SignalConfidence = Literal["high", "medium", "low"]
SignalCategory = Literal["delivery", "durability", "wellbeing", "dynamics", "ai"]

_METRICS = [
    {
        "metric": "cycle_time",
        "label": "Cycle Time",
        "unit": "days",
        "table": "work_item_metrics_daily",
        "column": "cycle_time_p50_hours",
        "aggregator": "avg",
        "transform": lambda v: v / 24.0,
        "scope": "team",
    },
    {
        "metric": "review_latency",
        "label": "Review Latency",
        "unit": "hours",
        "table": "repo_metrics_daily",
        "column": "pr_first_review_p50_hours",
        "aggregator": "avg",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "throughput",
        "label": "Throughput",
        "unit": "items",
        "table": "work_item_metrics_daily",
        "column": "items_completed",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "team",
    },
    {
        "metric": "deploy_freq",
        "label": "Deploy Frequency",
        "unit": "deploys",
        "table": "deploy_metrics_daily",
        "column": "deployments_count",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "churn",
        "label": "Code Churn",
        "unit": "loc",
        "table": "repo_metrics_daily",
        "column": "total_loc_touched",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "wip_saturation",
        "label": "WIP Saturation",
        "unit": "%",
        "table": "work_item_metrics_daily",
        "column": "wip_congestion_ratio",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "team",
    },
    {
        "metric": "blocked_work",
        "label": "Blocked Work",
        "unit": "hours",
        "table": "work_item_state_durations_daily",
        "column": "duration_hours",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "team",
    },
    {
        "metric": "change_failure_rate",
        "label": "Change Failure Rate",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "change_failure_rate",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
    {
        "metric": "rework_ratio",
        "label": "Rework Ratio",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "rework_churn_ratio_30d",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
    {
        "metric": "pr_rework_ratio",
        "label": "PR Rework Ratio",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "pr_rework_ratio",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
    {
        "metric": "ci_success",
        "label": "CI Success Rate",
        "unit": "%",
        "table": "cicd_metrics_daily",
        "column": "success_rate",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
]

_METRIC_CATEGORIES: dict[str, SignalCategory] = {
    "cycle_time": "delivery",
    "review_latency": "dynamics",
    "throughput": "delivery",
    "deploy_freq": "delivery",
    "churn": "durability",
    "wip_saturation": "dynamics",
    "blocked_work": "delivery",
    "change_failure_rate": "durability",
    "rework_ratio": "durability",
    "pr_rework_ratio": "durability",
    "ci_success": "durability",
    "compounding_risk": "durability",
}

_LOWER_IS_BETTER = {
    "cycle_time",
    "review_latency",
    "churn",
    "wip_saturation",
    "blocked_work",
    "change_failure_rate",
    "rework_ratio",
    "pr_rework_ratio",
    "compounding_risk",
}

# Two-stage resolution so a *recovered* signal stops showing (CHAOS-2373):
#   1. inner: argMax(..., computed_at) collapses re-runs of the SAME window_end.
#   2. outer: argMax(..., window_end) keeps ONLY the latest window_end (as-of
#      day) per (org, team, rule). The scheduled job writes the full rule state
#      — fired rows AND explicit fired=false tombstones — at window_end=today,
#      so the most recent as-of dominates and a stale fired row from an earlier
#      day is superseded instead of lingering in-range.
# ``{team_filter}`` is substituted with an extra inner-WHERE predicate.
_RECOMMENDATIONS_SQL = """
    SELECT
        team_id,
        org_id,
        rule_id,
        argMax(latest_fired,             window_end) AS latest_fired,
        argMax(latest_severity,          window_end) AS latest_severity,
        argMax(latest_title,             window_end) AS latest_title,
        argMax(latest_rationale,         window_end) AS latest_rationale,
        argMax(latest_success_criterion, window_end) AS latest_success_criterion,
        argMax(latest_evidence_json,     window_end) AS latest_evidence_json,
        argMax(latest_window_start,      window_end) AS latest_window_start,
        max(window_end)                              AS latest_window_end,
        argMax(latest_computed_at,       window_end) AS latest_computed_at
    FROM (
        SELECT
            team_id,
            org_id,
            rule_id,
            window_end,
            argMax(fired,               computed_at) AS latest_fired,
            argMax(severity,            computed_at) AS latest_severity,
            argMax(title,               computed_at) AS latest_title,
            argMax(rationale,           computed_at) AS latest_rationale,
            argMax(success_criterion,   computed_at) AS latest_success_criterion,
            argMax(evidence_json,       computed_at) AS latest_evidence_json,
            argMax(window_start,        computed_at) AS latest_window_start,
            max(computed_at)                         AS latest_computed_at
        FROM recommendations_daily
        WHERE org_id = {org_id:String}
          AND window_end >= {window_start:Date}
          AND window_end <= {window_end:Date}
          {team_filter}
        GROUP BY org_id, team_id, rule_id, window_end
    )
    GROUP BY org_id, team_id, rule_id
    HAVING latest_fired = true
    ORDER BY latest_window_end DESC, latest_severity DESC, rule_id
    LIMIT 10
"""

_COMPOUNDING_RISK_SQL = """
    SELECT
        scope,
        scope_id,
        tupleElement(latest_row, 1) AS score,
        tupleElement(latest_row, 2) AS severity,
        tupleElement(latest_row, 3) AS latest_computed_at
    FROM (
        SELECT
            scope,
            scope_id,
            argMax(tuple(compounding_risk, severity, computed_at), computed_at) AS latest_row
        FROM compounding_risk_daily
        WHERE org_id = {org_id:String}
          AND day = (
              SELECT maxOrNull(day)
              FROM (
                  SELECT
                      day,
                      count() AS row_count,
                      countIf(tupleElement(latest_row, 1) IS NULL) AS missing_scores
                  FROM (
                      SELECT
                          day,
                          scope,
                          scope_id,
                          argMax(tuple(compounding_risk), computed_at) AS latest_row
                      FROM compounding_risk_daily
                      WHERE org_id = {org_id:String}
                        AND day >= {start_day:Date}
                        AND day < {end_day:Date}
                      {latest_scope_filter}
                      GROUP BY day, scope, scope_id
                  )
                  GROUP BY day
              )
              WHERE row_count > 0 AND missing_scores = 0
          )
"""


def _spark_points(rows: list[dict[str, Any]], transform) -> list[SparkPoint]:
    points = []
    for row in rows:
        value = safe_float(row.get("value"))
        points.append(SparkPoint(ts=row["day"], value=safe_transform(transform, value)))
    return points


def _direction(pct_change: float) -> str:
    if pct_change > 0:
        return "rose"
    if pct_change < 0:
        return "fell"
    return "held steady"


def _format_delta(delta_pct: float) -> str:
    return f"{abs(delta_pct):.0f}%"


def _primary_scope_label(filters: MetricFilter) -> str:
    if filters.scope.ids:
        return ",".join(filters.scope.ids)
    return filters.scope.level


def _signal_direction(delta: float) -> SignalDirection:
    if delta > 1:
        return "up"
    if delta < -1:
        return "down"
    return "flat"


def _prior_value(current: float, delta: float) -> float | None:
    if abs(delta + 100.0) < 0.0001:
        return None
    return current / (1.0 + (delta / 100.0))


def _metric_impact(metric: str, delta: float) -> float:
    direction = _signal_direction(delta)
    if direction == "flat":
        return 0.0
    magnitude = abs(delta)
    worsened = (metric in _LOWER_IS_BETTER and delta > 0) or (
        metric not in _LOWER_IS_BETTER and delta < 0
    )
    return magnitude if worsened else magnitude * 0.35


def _severity_for_impact(impact: float) -> SignalSeverity:
    if impact >= 60:
        return "critical"
    if impact >= 35:
        return "high"
    if impact >= 15:
        return "medium"
    return "low"


def _severity_rank(severity: str) -> int:
    return {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(severity, 0)


def _confidence_from_evidence(
    evidence_count: int, coverage_pct: float | None
) -> SignalConfidence:
    coverage = coverage_pct or 0.0
    if evidence_count >= 7 and coverage >= 75:
        return "high"
    if evidence_count >= 2 and coverage >= 40:
        return "medium"
    return "low"


def _format_value(value: float, unit: str | None = None) -> str:
    suffix = f" {unit}" if unit else ""
    if abs(value) >= 100 or float(value).is_integer():
        return f"{value:.0f}{suffix}"
    return f"{value:.1f}{suffix}"


def _format_delta_value(delta: float | None) -> str | None:
    if delta is None:
        return None
    return f"{delta:+.0f}%"


def _evidence_link(metric: str, filters: MetricFilter) -> str:
    return (
        f"/api/v1/explain?metric={metric}"
        f"&scope_type={filters.scope.level}"
        f"&scope_id={_primary_scope_id(filters)}"
        f"&range_days={filters.time.range_days}"
        f"&compare_days={filters.time.compare_days}"
    )


def _action_for_metric(metric: str) -> str:
    actions = {
        "cycle_time": "Inspect the slowest stage and rebalance active work before adding scope.",
        "review_latency": "Rebalance reviewer rotation and clear stale review queues.",
        "throughput": "Review WIP and dependency queues before changing delivery commitments.",
        "deploy_freq": "Check release blockers and restore the smallest safe deployment path.",
        "churn": "Inspect hotspots and stabilize rework loops before expanding the change set.",
        "wip_saturation": "Set a short-term WIP limit and finish active items before starting more.",
        "blocked_work": "Triage blocked items by owner and unblock the oldest high-impact queue first.",
        "change_failure_rate": "Inspect recent failed changes and tighten pre-release checks around the common failure mode.",
        "rework_ratio": "Review reopened or rewritten work and pick one root-cause experiment.",
        "ci_success": "Inspect failing pipelines and restore the most common broken check first.",
        "compounding_risk": "Inspect the highest-risk scope and reduce the strongest component before adding scope.",
    }
    return actions.get(
        metric,
        "Inspect supporting evidence and choose one reversible operating experiment.",
    )


def _why_for_metric(metric: str, label: str, direction: str) -> str:
    movement = {
        "up": "rising",
        "down": "falling",
        "flat": "flat",
    }[direction]
    reasons = {
        "cycle_time": "longer cycle time suggests delivery work may spend more time waiting than moving.",
        "review_latency": "review queues shape how quickly teams can learn from completed work.",
        "throughput": "throughput movement changes the team's ability to keep commitments credible.",
        "deploy_freq": "deployment cadence suggests whether finished work can reach users smoothly.",
        "churn": "higher churn suggests effort may be cycling through rework rather than durable progress.",
        "wip_saturation": "saturation suggests active work may exceed the team's coordination capacity.",
        "blocked_work": "blocked time suggests dependencies may be consuming delivery capacity.",
        "change_failure_rate": "failed changes suggest reliability work may be competing with delivery.",
        "rework_ratio": "rework suggests unclear requirements or fragile implementation paths may be taxing focus.",
        "ci_success": "CI health suggests whether the delivery path is dependable.",
        "compounding_risk": "compounding risk combines churn, complexity, ownership, and review pressure into one persisted signal.",
    }
    reason = reasons.get(
        metric, f"{label} movement suggests an operating signal to inspect."
    )
    return f"{label} appears {movement}; {reason}"


def _coverage_pct_from_coverage(coverage: dict[str, Any]) -> float | None:
    values = [safe_float(value) for value in coverage.values() if value is not None]
    if not values:
        return None
    return sum(values) / len(values)


def build_data_confidence(
    *, coverage: dict[str, Any], sources: dict[str, str]
) -> HomeDataConfidence:
    coverage_pct = _coverage_pct_from_coverage(coverage)
    connected = sorted(source for source, status in sources.items() if status == "ok")
    missing = sorted(source for source, status in sources.items() if status != "ok")
    if coverage_pct is not None and coverage_pct >= 75 and not missing:
        level = "high"
    elif coverage_pct is not None and coverage_pct >= 40 and connected:
        level = "medium"
    else:
        level = "low"

    caveats: list[str] = []
    if missing:
        caveats.append("Some source freshness checks are missing or stale.")
    if coverage_pct is None:
        caveats.append("Coverage could not be computed from available lineage fields.")
    elif coverage_pct < 60:
        caveats.append(
            "Coverage appears partial; treat cockpit signals as directional."
        )

    return HomeDataConfidence(
        level=level,
        coverage_pct=coverage_pct,
        connected_sources=connected,
        missing_sources=missing,
        caveats=caveats,
    )


def build_metric_signals(
    deltas: list[MetricDelta],
    filters: MetricFilter,
    data_confidence: HomeDataConfidence,
) -> list[HomeSignal]:
    signals: list[HomeSignal] = []
    for delta in deltas:
        direction = _signal_direction(delta.delta_pct)
        evidence_count = len(delta.spark)
        impact = _metric_impact(delta.metric, delta.delta_pct)
        signals.append(
            HomeSignal(
                id=f"metric:{delta.metric}",
                title=f"{delta.label} appears {direction}",
                metric=delta.metric,
                current_value=_format_value(delta.value, delta.unit),
                prior_value=(
                    _format_value(prior, delta.unit)
                    if (prior := _prior_value(delta.value, delta.delta_pct)) is not None
                    else None
                ),
                delta=_format_delta_value(delta.delta_pct),
                direction=direction,
                severity=_severity_for_impact(impact),
                confidence=_confidence_from_evidence(
                    evidence_count, data_confidence.coverage_pct
                ),
                affected_scope=_primary_scope_label(filters),
                evidence_count=evidence_count,
                why_it_matters=_why_for_metric(delta.metric, delta.label, direction),
                recommended_action=_action_for_metric(delta.metric),
                evidence_ref=_evidence_link(delta.metric, filters),
                category=_METRIC_CATEGORIES.get(delta.metric, "delivery"),
            )
        )
    return _rank_signals(signals)


def _rank_signals(signals: list[HomeSignal]) -> list[HomeSignal]:
    def _delta_magnitude(signal: HomeSignal) -> float:
        if not signal.delta:
            return 0.0
        try:
            return abs(float(signal.delta.rstrip("%")))
        except ValueError:
            return 0.0

    return sorted(
        signals,
        key=lambda signal: (
            _severity_rank(signal.severity),
            _delta_magnitude(signal),
            signal.evidence_count,
        ),
        reverse=True,
    )


def build_health_state(
    signals: list[HomeSignal],
    data_confidence: HomeDataConfidence,
    as_of: datetime | None,
) -> HomeHealthState:
    top = signals[0] if signals else None
    if top is None:
        status = "watch" if data_confidence.level == "low" else "healthy"
        return HomeHealthState(
            status=status,
            headline="Cockpit signals appear sparse",
            summary="Available data suggests watching coverage before making operating changes.",
            as_of=as_of,
        )

    if top.severity == "critical":
        status = "critical"
    elif top.severity == "high":
        status = "at_risk"
    elif top.severity == "medium" or data_confidence.level == "low":
        status = "watch"
    else:
        status = "healthy"

    return HomeHealthState(
        status=status,
        headline=f"{top.title} across {top.affected_scope}",
        summary=f"The strongest signal suggests {top.why_it_matters}",
        as_of=as_of,
    )


def build_limiting_factor(signals: list[HomeSignal]) -> HomeLimitingFactor:
    if not signals:
        return HomeLimitingFactor()
    top = signals[0]
    return HomeLimitingFactor(
        claim=f"{top.title} appears to be the current limiting factor.",
        why_it_matters=top.why_it_matters,
        recommended_action=top.recommended_action,
        confidence=top.confidence,
        evidence_ref=top.evidence_ref,
    )


def _parse_recommendation_evidence(raw: str | list[Any] | None) -> list[dict[str, Any]]:
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return []
    else:
        parsed = raw
    return [item for item in parsed if isinstance(item, dict)]


def _recommendation_signal(
    row: dict[str, Any], filters: MetricFilter, data_confidence: HomeDataConfidence
) -> HomeSignal | None:
    title = str(row.get("latest_title") or row.get("title") or "").strip()
    if not title:
        return None
    raw_severity = str(row.get("latest_severity") or row.get("severity") or "warning")
    severity: SignalSeverity = "critical" if raw_severity == "critical" else "medium"
    evidence = _parse_recommendation_evidence(
        row.get("latest_evidence_json") or row.get("evidence_json")
    )
    rule_id = str(row.get("rule_id") or "recommendation")
    team_id = str(row.get("team_id") or _primary_scope_label(filters))
    return HomeSignal(
        id=f"recommendation:{rule_id}:{team_id}",
        title=title,
        metric=rule_id,
        current_value=_format_value(float(len(evidence)), "refs"),
        prior_value=None,
        delta=None,
        direction="flat",
        severity=severity,
        confidence=_confidence_from_evidence(
            len(evidence), data_confidence.coverage_pct
        ),
        affected_scope=team_id,
        evidence_count=len(evidence),
        why_it_matters=str(
            row.get("latest_rationale")
            or row.get("rationale")
            or "A persisted recommendation suggests this operating pattern needs attention."
        ),
        recommended_action=str(
            row.get("latest_success_criterion")
            or row.get("success_criterion")
            or "Choose one reversible experiment and inspect the evidence trail."
        ),
        evidence_ref=(
            f"/api/graphql?query=recommendations&team={team_id}" if team_id else None
        ),
        category="dynamics",
    )


_BARE_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _looks_like_uuid(value: str) -> bool:
    """Return True when *value* is a bare UUID that must not appear as a label (A8)."""
    return bool(_BARE_UUID_RE.match(value.strip()))


async def _resolve_scope_labels(
    sink: BaseMetricsSink,
    *,
    org_id: str,
    rows: list[dict[str, Any]],
) -> dict[str, str]:
    """Return {scope_id: display_name} for the given risk rows.

    Queries the repos / teams tables to resolve human-readable labels.
    Best-effort: on any failure returns an empty dict so callers apply a
    controlled fallback rather than surfacing raw IDs (A8 / B7).
    """
    repo_ids = [
        str(r["scope_id"])
        for r in rows
        if str(r.get("scope") or "") == "repo" and r.get("scope_id")
    ]
    team_ids = [
        str(r["scope_id"])
        for r in rows
        if str(r.get("scope") or "") == "team" and r.get("scope_id")
    ]

    label_map: dict[str, str] = {}

    if repo_ids:
        try:
            repo_rows = await query_dicts(
                sink,
                """
                SELECT toString(id) AS scope_id, repo AS display_name
                FROM repos
                WHERE org_id = {org_id:String}
                  AND toString(id) IN {scope_ids:Array(String)}
                """,
                {"org_id": org_id, "scope_ids": repo_ids},
            )
            label_map.update(
                {
                    r["scope_id"]: r.get("display_name") or r["scope_id"]
                    for r in repo_rows
                }
            )
        except Exception:
            logger.warning("Could not resolve repo labels for risk signals")

    if team_ids:
        try:
            team_rows = await query_dicts(
                sink,
                """
                SELECT toString(id) AS scope_id, name AS display_name
                FROM teams
                WHERE org_id = {org_id:String}
                """,
                {"org_id": org_id},
            )
            label_map.update(
                {
                    r["scope_id"]: r.get("display_name") or r["scope_id"]
                    for r in team_rows
                }
            )
        except Exception:
            logger.warning("Could not resolve team labels for risk signals")

    return label_map


def _risk_signal(
    row: dict[str, Any],
    filters: MetricFilter,
    data_confidence: HomeDataConfidence,
    *,
    scope_display_name: str | None = None,
) -> HomeSignal | None:
    score = row.get("score")
    if score is None:
        return None
    current_value = safe_float(score) * 100.0
    raw_severity = str(row.get("severity") or "low").lower()
    severity_map: dict[str, SignalSeverity] = {
        "high": "high",
        "elevated": "medium",
        "low": "low",
        "unknown": "low",
    }
    severity: SignalSeverity = severity_map.get(raw_severity, "low")
    scope_id = str(row.get("scope_id") or "").strip()
    scope_type = str(row.get("scope") or filters.scope.level or "repo").strip()

    # B7: controlled empty/flat state — absent scope_id → suppress signal
    if not scope_id:
        return None

    # A8: no bare UUID in any label or headline field
    entity_name = scope_display_name
    if not entity_name or _looks_like_uuid(entity_name):
        # Display name unresolved — controlled flat state; do not surface raw id
        return None

    # affected_scope is DISTINCT from the entity named in the title
    affected_scope = f"{scope_type}s"

    return HomeSignal(
        id=f"risk:{scope_type}:{scope_id}",
        title=f"Compounding risk appears {raw_severity} for {entity_name}",
        metric="compounding_risk",
        current_value=_format_value(current_value, "%"),
        prior_value=None,
        delta=None,
        direction="flat",
        severity=severity,
        confidence=_confidence_from_evidence(1, data_confidence.coverage_pct),
        affected_scope=affected_scope,
        evidence_count=1,
        why_it_matters=_why_for_metric("compounding_risk", "Compounding Risk", "flat"),
        recommended_action=_action_for_metric("compounding_risk"),
        evidence_ref=None,
        category="durability",
        scope_entity=ScopeEntityRef(id=scope_id, display_name=entity_name),
    )


async def _fetch_recommendation_signals(
    sink: BaseMetricsSink,
    *,
    filters: MetricFilter,
    start_day: date,
    end_day: date,
    org_id: str,
    data_confidence: HomeDataConfidence,
) -> list[HomeSignal]:
    if filters.scope.level != "team" or not filters.scope.ids:
        return []
    params: dict[str, Any] = {
        "org_id": org_id,
        "window_start": start_day,
        "window_end": end_day,
        "team_ids": filters.scope.ids,
    }
    query = _RECOMMENDATIONS_SQL.replace(
        "{team_filter}", "AND team_id IN {team_ids:Array(String)}"
    )
    try:
        rows = await query_dicts(sink, query, params)
    except Exception:
        logger.exception("Failed to fetch home recommendation signals")
        return []
    return [
        signal
        for row in rows
        if (signal := _recommendation_signal(row, filters, data_confidence)) is not None
    ]


async def _fetch_risk_signals(
    sink: BaseMetricsSink,
    *,
    filters: MetricFilter,
    start_day: date,
    end_day: date,
    org_id: str,
    data_confidence: HomeDataConfidence,
) -> list[HomeSignal]:
    latest_scope_filter = ""
    params: dict[str, Any] = {
        "org_id": org_id,
        "start_day": start_day,
        "end_day": end_day,
    }
    if filters.scope.level in {"team", "repo"} and filters.scope.ids:
        latest_scope_filter = """
                AND scope = {scope:String}
                AND scope_id IN {scope_ids:Array(String)}
        """
    query = _COMPOUNDING_RISK_SQL.replace("{latest_scope_filter}", latest_scope_filter)
    if filters.scope.level in {"team", "repo"} and filters.scope.ids:
        query += """
      AND scope = {scope:String}
      AND scope_id IN {scope_ids:Array(String)}
        """
        params["scope"] = filters.scope.level
        params["scope_ids"] = filters.scope.ids
    query += """
        GROUP BY scope, scope_id
    )
    ORDER BY score DESC NULLS LAST
    LIMIT 5
    """
    try:
        rows = await query_dicts(sink, query, params)
    except Exception:
        logger.exception("Failed to fetch home compounding risk signals")
        return []
    if not rows:
        return []

    # Resolve scope_id → human display name so labels never contain bare IDs (A8)
    label_map = await _resolve_scope_labels(sink, org_id=org_id, rows=rows)

    return [
        signal
        for row in rows
        if (
            signal := _risk_signal(
                row,
                filters,
                data_confidence,
                scope_display_name=label_map.get(str(row.get("scope_id") or "")),
            )
        )
        is not None
    ]


async def _metric_deltas(
    sink: BaseMetricsSink,
    filters: MetricFilter,
    start_day: date,
    end_day: date,
    compare_start: date,
    compare_end: date,
    org_id: str = "",
) -> list[MetricDelta]:

    async def _compute_one(metric: dict[str, Any]) -> MetricDelta:
        scope_filter, scope_params = await scope_filter_for_metric(
            sink, metric_scope=metric["scope"], filters=filters
        )

        if metric["metric"] == "blocked_work":
            (current_value, current_series), (previous_value, _) = await asyncio.gather(
                fetch_blocked_hours(
                    sink,
                    start_day=start_day,
                    end_day=end_day,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    org_id=org_id,
                ),
                fetch_blocked_hours(
                    sink,
                    start_day=compare_start,
                    end_day=compare_end,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    org_id=org_id,
                ),
            )
            current_value = safe_float(current_value)
            previous_value = safe_float(previous_value)
            spark = _spark_points(current_series, metric["transform"])
        else:
            current_value, previous_value, series = await asyncio.gather(
                fetch_metric_value(
                    sink,
                    table=metric["table"],
                    column=metric["column"],
                    start_day=start_day,
                    end_day=end_day,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    aggregator=metric["aggregator"],
                    org_id=org_id,
                ),
                fetch_metric_value(
                    sink,
                    table=metric["table"],
                    column=metric["column"],
                    start_day=compare_start,
                    end_day=compare_end,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    aggregator=metric["aggregator"],
                    org_id=org_id,
                ),
                fetch_metric_series(
                    sink,
                    table=metric["table"],
                    column=metric["column"],
                    start_day=start_day,
                    end_day=end_day,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    aggregator=metric["aggregator"],
                    org_id=org_id,
                ),
            )
            current_value = safe_float(current_value)
            previous_value = safe_float(previous_value)
            spark = _spark_points(series, metric["transform"])

        pct_change = safe_float(delta_pct(current_value, previous_value))
        return MetricDelta(
            metric=metric["metric"],
            label=metric["label"],
            value=safe_transform(metric["transform"], current_value),
            unit=metric["unit"],
            delta_pct=pct_change,
            spark=spark,
        )

    return list(await asyncio.gather(*[_compute_one(m) for m in _METRICS]))


def _select_constraint(deltas: list[MetricDelta]) -> MetricDelta:
    if not deltas:
        return MetricDelta(
            metric="cycle_time",
            label="Cycle Time",
            value=0.0,
            unit="days",
            delta_pct=0.0,
            spark=[],
        )
    return sorted(deltas, key=lambda d: d.delta_pct)[-1]


async def build_home_response(
    *,
    db_url: str,
    filters: MetricFilter,
    cache: TTLCache,
    org_id: str = "",
) -> HomeResponse:
    cache_key = filter_cache_key("home", org_id, filters)
    cached = cache.get(cache_key)
    if cached is not None:
        return HomeResponse.model_validate(cached)

    start_day, end_day, compare_start, compare_end = time_window(filters)

    async with clickhouse_client(db_url) as sink:
        allocation_scope = "repo" if filters.scope.level == "repo" else "team"
        (
            allocation_scope_filter,
            allocation_scope_params,
        ) = await scope_filter_for_metric(
            sink,
            metric_scope=allocation_scope,
            filters=filters,
            org_id=org_id,
        )
        allocation_category_filter, allocation_category_params = work_category_filter(
            filters
        )

        (
            last_ingested,
            coverage,
            deltas,
            rework_theme_allocation_rows,
        ) = await asyncio.gather(
            fetch_last_ingested_at(sink, org_id=org_id),
            fetch_coverage(
                sink,
                start_day=start_day,
                end_day=end_day,
                org_id=org_id,
            ),
            _metric_deltas(
                sink,
                filters,
                start_day,
                end_day,
                compare_start,
                compare_end,
                org_id=org_id,
            ),
            fetch_rework_theme_allocation(
                sink,
                start_day=start_day,
                end_day=end_day,
                scope_filter=allocation_scope_filter,
                scope_params=allocation_scope_params,
                work_category_filter=allocation_category_filter,
                work_category_params=allocation_category_params,
                org_id=org_id,
            ),
        )
        rework_theme_allocation = [
            ReworkThemeAllocation(
                **{
                    **row,
                    # Sanitize non-finite floats (NaN/inf) so the JSON response
                    # stays spec-compliant, matching every other numeric field
                    # in this payload.
                    "allocation": safe_float(row.get("allocation")),
                    "allocation_pct": safe_float(row.get("allocation_pct")),
                }
            )
            for row in rework_theme_allocation_rows
        ]

        sources = {
            "github": "ok" if last_ingested else "down",
            "gitlab": "ok" if last_ingested else "down",
            "jira": "ok" if last_ingested else "down",
            "ci": "ok" if last_ingested else "down",
        }
        data_confidence = build_data_confidence(coverage=coverage, sources=sources)
        metric_signals = build_metric_signals(deltas, filters, data_confidence)
        recommendation_signals, risk_signals = await asyncio.gather(
            _fetch_recommendation_signals(
                sink,
                filters=filters,
                start_day=start_day,
                end_day=end_day,
                org_id=org_id,
                data_confidence=data_confidence,
            ),
            _fetch_risk_signals(
                sink,
                filters=filters,
                start_day=start_day,
                end_day=end_day,
                org_id=org_id,
                data_confidence=data_confidence,
            ),
        )
        signals = _rank_signals(metric_signals + recommendation_signals + risk_signals)
        health_state = build_health_state(signals, data_confidence, last_ingested)
        limiting_factor = build_limiting_factor(signals)

        summary_sentences: list[SummarySentence] = []
        top_delta = max(deltas, key=lambda d: abs(d.delta_pct), default=None)
        if top_delta:
            scope_filter, scope_params = await scope_filter_for_metric(
                sink, metric_scope=_metric_scope(top_delta.metric), filters=filters
            )

            driver_rows = await fetch_metric_driver_delta(
                sink,
                table=_metric_table(top_delta.metric),
                column=_metric_column(top_delta.metric),
                group_by=_metric_group(top_delta.metric),
                start_day=start_day,
                end_day=end_day,
                compare_start=compare_start,
                compare_end=compare_end,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            driver_labels = ", ".join(
                [str(row.get("id")) for row in driver_rows if row.get("id")] or []
            )
            driver_text = f" driven by {driver_labels}." if driver_labels else "."
            summary_sentences.append(
                SummarySentence(
                    id="s1",
                    text=(
                        f"{top_delta.label} {_direction(top_delta.delta_pct)} "
                        f"{_format_delta(top_delta.delta_pct)}{driver_text}"
                    ),
                    evidence_link=(
                        f"/api/v1/explain?metric={top_delta.metric}"
                        f"&scope_type={filters.scope.level}"
                        f"&scope_id={_primary_scope_id(filters)}"
                        f"&range_days={filters.time.range_days}"
                        f"&compare_days={filters.time.compare_days}"
                    ),
                )
            )

        constraint_metric = _select_constraint(deltas)
        constraint = ConstraintCard(
            title=f"This week's constraint: {constraint_metric.label}",
            claim=(
                f"{constraint_metric.label} {_direction(constraint_metric.delta_pct)} "
                f"{_format_delta(constraint_metric.delta_pct)} over the last {filters.time.range_days} days."
            ),
            evidence=[
                ConstraintEvidence(
                    label=f"Drill into {constraint_metric.label}",
                    link=(
                        f"/api/v1/explain?metric={constraint_metric.metric}"
                        f"&scope_type={filters.scope.level}"
                        f"&scope_id={_primary_scope_id(filters)}"
                        f"&range_days={filters.time.range_days}"
                        f"&compare_days={filters.time.compare_days}"
                    ),
                )
            ],
            experiments=[
                "Rebalance reviewer rotation to reduce queueing.",
                "Set WIP limits per team and auto-alert at saturation.",
            ],
        )

        events: list[EventItem] = []
        for delta in deltas:
            if abs(delta.delta_pct) >= 25:
                events.append(
                    EventItem(
                        ts=datetime.now(timezone.utc),
                        type="regression" if delta.delta_pct > 0 else "spike",
                        text=(
                            f"{delta.label} shifted {delta.delta_pct:.0f}% "
                            f"over the last {filters.time.range_days} days."
                        ),
                        link=(
                            f"/api/v1/explain?metric={delta.metric}"
                            f"&scope_type={filters.scope.level}"
                            f"&scope_id={_primary_scope_id(filters)}"
                            f"&range_days={filters.time.range_days}"
                            f"&compare_days={filters.time.compare_days}"
                        ),
                    )
                )

        tiles = {
            "understand": {
                "title": "Understand",
                "subtitle": "Flow stages",
                "link": "/explore?view=understand",
            },
            "measure": {
                "title": "Measure",
                "subtitle": "Coverage & freshness",
                "link": "/explore?view=measure",
            },
            "align": {
                "title": "Align",
                "subtitle": "Investment mix",
                "link": "/investment",
            },
            "execute": {
                "title": "Execute",
                "subtitle": "Top opportunities",
                "link": "/opportunities",
            },
        }

        response = HomeResponse(
            freshness=Freshness(
                last_ingested_at=last_ingested,
                sources=sources,
                coverage=Coverage(**coverage),
            ),
            deltas=deltas,
            rework_theme_allocation=rework_theme_allocation,
            summary=summary_sentences,
            tiles=tiles,
            constraint=constraint,
            events=events,
            health_state=health_state,
            signals=signals,
            limiting_factor=limiting_factor,
            data_confidence=data_confidence,
        )

    cache.set(cache_key, response.model_dump(mode="json"))
    return response


def _metric_table(metric: str) -> str:
    for cfg in _METRICS:
        if cfg.get("metric") != metric:
            continue
        table = cfg.get("table")
        if isinstance(table, str):
            return table
    return "repo_metrics_daily"


def _metric_column(metric: str) -> str:
    for cfg in _METRICS:
        if cfg.get("metric") != metric:
            continue
        column = cfg.get("column")
        if isinstance(column, str):
            return column
    return "pr_first_review_p50_hours"


def _metric_group(metric: str) -> str:
    if metric in {"cycle_time", "throughput", "wip_saturation", "blocked_work"}:
        return "team_id"
    return "repo_id"


def _metric_scope(metric: str) -> str:
    if metric in {"cycle_time", "throughput", "wip_saturation", "blocked_work"}:
        return "team"
    return "repo"


def _primary_scope_id(filters: MetricFilter) -> str:
    if filters.scope.ids:
        return filters.scope.ids[0]
    return ""
