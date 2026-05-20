"""Weekly Engineering Operating Review metrics.

This module is intentionally framework-free: it computes the review payload from
daily rollup rows and exposes ClickHouse query text for API callers.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Literal

DeltaStatus = Literal["changed", "improved", "worsened", "unchanged"]


@dataclass(frozen=True)
class MetricDelta:
    value: float
    prior_value: float
    absolute: float
    percent: float | None
    status: DeltaStatus


@dataclass(frozen=True)
class OperatingReviewMetric:
    key: str
    label: str
    value: float
    unit: str
    delta: MetricDelta


@dataclass(frozen=True)
class OperatingReviewSection:
    key: str
    title: str
    metrics: list[OperatingReviewMetric]
    changed: list[str]
    improved: list[str]
    worsened: list[str]

    def metric(self, key: str) -> OperatingReviewMetric:
        for metric in self.metrics:
            if metric.key == key:
                return metric
        raise KeyError(key)


@dataclass(frozen=True)
class OperatingReview:
    org_id: str
    team_id: str
    week_start: date
    prior_week_start: date
    sections: list[OperatingReviewSection]
    recommendations: list[str]
    recommendations_empty_state: str

    def section(self, key: str) -> OperatingReviewSection:
        for section in self.sections:
            if section.key == key:
                return section
        raise KeyError(key)


@dataclass(frozen=True)
class OperatingReviewRows:
    work_items: Sequence[Mapping[str, Any]] = field(default_factory=list)
    state_durations: Sequence[Mapping[str, Any]] = field(default_factory=list)
    repo_metrics: Sequence[Mapping[str, Any]] = field(default_factory=list)
    hotspots: Sequence[Mapping[str, Any]] = field(default_factory=list)
    complexity: Sequence[Mapping[str, Any]] = field(default_factory=list)
    deployments: Sequence[Mapping[str, Any]] = field(default_factory=list)
    incidents: Sequence[Mapping[str, Any]] = field(default_factory=list)
    investment: Sequence[Mapping[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class OperatingReviewQuery:
    key: str
    sql: str


LOWER_IS_BETTER = "lower"
HIGHER_IS_BETTER = "higher"
NEUTRAL = "neutral"


def week_bounds(week_start: date) -> tuple[date, date]:
    return week_start, week_start + timedelta(days=7)


def prior_week_start(week_start: date) -> date:
    return week_start - timedelta(days=7)


def build_operating_review_queries() -> list[OperatingReviewQuery]:
    """Return ClickHouse queries for daily rollups used by the review.

    Parameters expected by every query: org_id, team_id, start, end.
    Queries select the latest append-only value per day/dimension with argMax.
    """

    return [
        OperatingReviewQuery(
            "work_items",
            """
            SELECT
              day,
              sum(items_started) AS items_started,
              sum(items_completed) AS items_completed,
              max(wip_count_end_of_day) AS wip_count_end_of_day,
              avg(cycle_time_p50_hours) AS cycle_time_p50_hours,
              avg(cycle_time_p90_hours) AS cycle_time_p90_hours,
              avg(wip_age_p50_hours) AS wip_age_p50_hours,
              avg(wip_age_p90_hours) AS wip_age_p90_hours
            FROM (
              SELECT
                day,
                provider,
                work_scope_id,
                argMax(items_started, computed_at) AS items_started,
                argMax(items_completed, computed_at) AS items_completed,
                argMax(wip_count_end_of_day, computed_at) AS wip_count_end_of_day,
                argMax(cycle_time_p50_hours, computed_at) AS cycle_time_p50_hours,
                argMax(cycle_time_p90_hours, computed_at) AS cycle_time_p90_hours,
                argMax(wip_age_p50_hours, computed_at) AS wip_age_p50_hours,
                argMax(wip_age_p90_hours, computed_at) AS wip_age_p90_hours
              FROM work_item_metrics_daily
              WHERE org_id = %(org_id)s
                AND team_id = %(team_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, provider, work_scope_id
            )
            GROUP BY day
            ORDER BY day
            """,
        ),
        OperatingReviewQuery(
            "state_durations",
            """
            SELECT
              status,
              sum(items_touched) AS items_touched,
              avg(duration_hours) AS duration_hours,
              avg(avg_wip) AS avg_wip
            FROM (
              SELECT
                day,
                provider,
                work_scope_id,
                status,
                argMax(duration_hours, computed_at) AS duration_hours,
                argMax(items_touched, computed_at) AS items_touched,
                argMax(avg_wip, computed_at) AS avg_wip
              FROM work_item_state_duration_daily
              WHERE org_id = %(org_id)s
                AND team_id = %(team_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, provider, work_scope_id, status
            )
            GROUP BY status
            """,
        ),
        OperatingReviewQuery(
            "repo_metrics",
            """
            SELECT
              sum(prs_merged) AS prs_merged,
              avg(pr_first_review_p50_hours) AS pr_first_review_p50_hours,
              avg(single_owner_file_ratio_30d) AS single_owner_file_ratio_30d,
              avg(code_ownership_gini) AS code_ownership_gini,
              min(bus_factor) AS bus_factor,
              avg(change_failure_rate) AS change_failure_rate,
              avg(mttr_hours) AS mttr_hours
            FROM (
              SELECT
                day,
                repo_id,
                argMax(prs_merged, computed_at) AS prs_merged,
                argMax(pr_first_review_p50_hours, computed_at) AS pr_first_review_p50_hours,
                argMax(single_owner_file_ratio_30d, computed_at) AS single_owner_file_ratio_30d,
                argMax(code_ownership_gini, computed_at) AS code_ownership_gini,
                argMax(bus_factor, computed_at) AS bus_factor,
                argMax(change_failure_rate, computed_at) AS change_failure_rate,
                argMax(mttr_hours, computed_at) AS mttr_hours
              FROM repo_metrics_daily
              WHERE org_id = %(org_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, repo_id
            )
            """,
        ),
        OperatingReviewQuery(
            "hotspots",
            """
            SELECT avg(risk_score) AS risk_score, count() AS hotspots_count
            FROM (
              SELECT
                day,
                repo_id,
                file_path,
                argMax(risk_score, computed_at) AS risk_score
              FROM file_hotspot_daily
              WHERE org_id = %(org_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, repo_id, file_path
            )
            WHERE risk_score > 0
            """,
        ),
        OperatingReviewQuery(
            "complexity",
            """
            SELECT avg(cyclomatic_per_kloc) AS cyclomatic_per_kloc
            FROM (
              SELECT
                day,
                repo_id,
                argMax(cyclomatic_per_kloc, computed_at) AS cyclomatic_per_kloc
              FROM repo_complexity_daily
              WHERE org_id = %(org_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, repo_id
            )
            """,
        ),
        OperatingReviewQuery(
            "deployments",
            """
            SELECT
              sum(deployments_count) AS deployments_count,
              sum(failed_deployments_count) AS failed_deployments_count
            FROM (
              SELECT
                day,
                repo_id,
                argMax(deployments_count, computed_at) AS deployments_count,
                argMax(failed_deployments_count, computed_at) AS failed_deployments_count
              FROM deploy_metrics_daily
              WHERE org_id = %(org_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, repo_id
            )
            """,
        ),
        OperatingReviewQuery(
            "incidents",
            """
            SELECT sum(incidents_count) AS incidents_count, avg(mttr_p50_hours) AS mttr_p50_hours
            FROM (
              SELECT
                day,
                repo_id,
                argMax(incidents_count, computed_at) AS incidents_count,
                argMax(mttr_p50_hours, computed_at) AS mttr_p50_hours
              FROM incident_metrics_daily
              WHERE org_id = %(org_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, repo_id
            )
            """,
        ),
        OperatingReviewQuery(
            "investment",
            """
            SELECT investment_area, sum(delivery_units) AS delivery_units
            FROM (
              SELECT
                day,
                repo_id,
                investment_area,
                project_stream,
                argMax(delivery_units, computed_at) AS delivery_units
              FROM investment_metrics_daily
              WHERE org_id = %(org_id)s
                AND team_id = %(team_id)s
                AND day >= %(start)s AND day < %(end)s
              GROUP BY day, repo_id, investment_area, project_stream
            )
            GROUP BY investment_area
            """,
        ),
    ]


def compute_operating_review(
    *,
    org_id: str,
    team_id: str,
    week_start: date,
    current: OperatingReviewRows,
    prior: OperatingReviewRows,
) -> OperatingReview:
    """Compute the weekly review payload from current/prior rollup rows."""

    sections = [
        _delivery_section(current, prior),
        _bottleneck_section(current, prior),
        _risk_section(current, prior),
        _reliability_section(current, prior),
        _investment_section(current, prior),
    ]
    return OperatingReview(
        org_id=org_id,
        team_id=team_id,
        week_start=week_start,
        prior_week_start=prior_week_start(week_start),
        sections=sections,
        recommendations=[],
        recommendations_empty_state="No operating review rules are configured.",
    )


def _delivery_section(
    current: OperatingReviewRows, prior: OperatingReviewRows
) -> OperatingReviewSection:
    return _section(
        key="delivery_movement",
        title="Delivery movement",
        metrics=[
            _metric(
                "cycle_time_p50_hours",
                "Cycle time p50",
                _avg(current.work_items, "cycle_time_p50_hours"),
                _avg(prior.work_items, "cycle_time_p50_hours"),
                "hours",
                LOWER_IS_BETTER,
            ),
            _metric(
                "throughput",
                "Throughput",
                _sum(current.work_items, "items_completed"),
                _sum(prior.work_items, "items_completed"),
                "items completed",
                HIGHER_IS_BETTER,
            ),
            _metric(
                "wip_count",
                "WIP",
                _max(current.work_items, "wip_count_end_of_day"),
                _max(prior.work_items, "wip_count_end_of_day"),
                "items",
                LOWER_IS_BETTER,
            ),
        ],
    )


def _bottleneck_section(
    current: OperatingReviewRows, prior: OperatingReviewRows
) -> OperatingReviewSection:
    return _section(
        key="bottleneck",
        title="Bottleneck",
        metrics=[
            _metric(
                "state_duration_hours",
                "State duration",
                _weighted_avg(
                    current.state_durations, "duration_hours", "items_touched"
                ),
                _weighted_avg(prior.state_durations, "duration_hours", "items_touched"),
                "hours",
                LOWER_IS_BETTER,
            ),
            _metric(
                "review_latency_hours",
                "Review latency",
                _avg(current.repo_metrics, "pr_first_review_p50_hours"),
                _avg(prior.repo_metrics, "pr_first_review_p50_hours"),
                "hours",
                LOWER_IS_BETTER,
            ),
            _metric(
                "wip_age_p90_hours",
                "WIP age p90",
                _avg(current.work_items, "wip_age_p90_hours"),
                _avg(prior.work_items, "wip_age_p90_hours"),
                "hours",
                LOWER_IS_BETTER,
            ),
        ],
    )


def _risk_section(
    current: OperatingReviewRows, prior: OperatingReviewRows
) -> OperatingReviewSection:
    return _section(
        key="risk",
        title="Risk",
        metrics=[
            _metric(
                "hotspot_risk_score",
                "Hotspot risk",
                _avg(current.hotspots, "risk_score"),
                _avg(prior.hotspots, "risk_score"),
                "score",
                LOWER_IS_BETTER,
            ),
            _metric(
                "ownership_concentration",
                "Ownership concentration",
                _avg(current.repo_metrics, "single_owner_file_ratio_30d"),
                _avg(prior.repo_metrics, "single_owner_file_ratio_30d"),
                "ratio",
                LOWER_IS_BETTER,
            ),
            _metric(
                "complexity_per_kloc",
                "Complexity",
                _avg(current.complexity, "cyclomatic_per_kloc"),
                _avg(prior.complexity, "cyclomatic_per_kloc"),
                "cyclomatic/KLOC",
                LOWER_IS_BETTER,
            ),
            _metric(
                "bus_factor",
                "Bus factor",
                _min(current.repo_metrics, "bus_factor"),
                _min(prior.repo_metrics, "bus_factor"),
                "people",
                HIGHER_IS_BETTER,
            ),
        ],
    )


def _reliability_section(
    current: OperatingReviewRows, prior: OperatingReviewRows
) -> OperatingReviewSection:
    return _section(
        key="reliability",
        title="Reliability",
        metrics=[
            _metric(
                "deployments_count",
                "Deployments",
                _sum(current.deployments, "deployments_count"),
                _sum(prior.deployments, "deployments_count"),
                "deployments",
                HIGHER_IS_BETTER,
            ),
            _metric(
                "change_failure_rate",
                "Change failure rate",
                _change_failure_rate(current),
                _change_failure_rate(prior),
                "ratio",
                LOWER_IS_BETTER,
            ),
            _metric(
                "incidents_count",
                "Incidents",
                _sum(current.incidents, "incidents_count"),
                _sum(prior.incidents, "incidents_count"),
                "incidents",
                LOWER_IS_BETTER,
            ),
            _metric(
                "mttr_hours",
                "MTTR",
                _first_non_zero(
                    _avg(current.incidents, "mttr_p50_hours"),
                    _avg(current.repo_metrics, "mttr_hours"),
                ),
                _first_non_zero(
                    _avg(prior.incidents, "mttr_p50_hours"),
                    _avg(prior.repo_metrics, "mttr_hours"),
                ),
                "hours",
                LOWER_IS_BETTER,
            ),
        ],
    )


def _investment_section(
    current: OperatingReviewRows, prior: OperatingReviewRows
) -> OperatingReviewSection:
    current_units = _investment_units(current.investment)
    prior_units = _investment_units(prior.investment)
    return _section(
        key="investment",
        title="Investment",
        metrics=[
            _metric(
                "ktlo_units",
                "KTLO",
                current_units["ktlo"],
                prior_units["ktlo"],
                "delivery units",
                LOWER_IS_BETTER,
            ),
            _metric(
                "new_value_units",
                "New value",
                current_units["new_value"],
                prior_units["new_value"],
                "delivery units",
                HIGHER_IS_BETTER,
            ),
            _metric(
                "security_units",
                "Security",
                current_units["security"],
                prior_units["security"],
                "delivery units",
                NEUTRAL,
            ),
            _metric(
                "infra_units",
                "Infra",
                current_units["infra"],
                prior_units["infra"],
                "delivery units",
                NEUTRAL,
            ),
        ],
    )


def _section(
    *, key: str, title: str, metrics: list[OperatingReviewMetric]
) -> OperatingReviewSection:
    changed: list[str] = []
    improved: list[str] = []
    worsened: list[str] = []
    for metric in metrics:
        summary = _delta_summary(metric)
        if metric.delta.status == "changed":
            changed.append(summary)
        elif metric.delta.status == "improved":
            improved.append(summary)
        elif metric.delta.status == "worsened":
            worsened.append(summary)
    return OperatingReviewSection(
        key=key,
        title=title,
        metrics=metrics,
        changed=changed,
        improved=improved,
        worsened=worsened,
    )


def _metric(
    key: str,
    label: str,
    value: float,
    prior: float,
    unit: str,
    direction: str,
) -> OperatingReviewMetric:
    delta_value = value - prior
    if prior == 0:
        percent = None if delta_value else 0.0
    else:
        percent = delta_value / abs(prior) * 100.0

    if abs(delta_value) < 0.000001:
        status: DeltaStatus = "unchanged"
    elif direction == HIGHER_IS_BETTER:
        status = "improved" if delta_value > 0 else "worsened"
    elif direction == LOWER_IS_BETTER:
        status = "improved" if delta_value < 0 else "worsened"
    else:
        status = "changed"

    return OperatingReviewMetric(
        key=key,
        label=label,
        value=value,
        unit=unit,
        delta=MetricDelta(
            value=value,
            prior_value=prior,
            absolute=delta_value,
            percent=percent,
            status=status,
        ),
    )


def _delta_summary(metric: OperatingReviewMetric) -> str:
    direction = {
        "changed": "changed",
        "improved": "improved",
        "worsened": "worsened",
        "unchanged": "did not change",
    }[metric.delta.status]
    return f"{metric.label} {direction} by {metric.delta.absolute:+.1f} {metric.unit}"


def _value(row: Mapping[str, Any], key: str) -> float | None:
    raw = row.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _present_values(rows: Iterable[Mapping[str, Any]], key: str) -> list[float]:
    return [value for row in rows if (value := _value(row, key)) is not None]


def _sum(rows: Iterable[Mapping[str, Any]], key: str) -> float:
    return sum(_present_values(rows, key))


def _avg(rows: Iterable[Mapping[str, Any]], key: str) -> float:
    values = _present_values(rows, key)
    if not values:
        return 0.0
    return sum(values) / len(values)


def _max(rows: Iterable[Mapping[str, Any]], key: str) -> float:
    values = _present_values(rows, key)
    return max(values) if values else 0.0


def _min(rows: Iterable[Mapping[str, Any]], key: str) -> float:
    values = _present_values(rows, key)
    return min(values) if values else 0.0


def _weighted_avg(
    rows: Iterable[Mapping[str, Any]], value_key: str, weight_key: str
) -> float:
    total = 0.0
    total_weight = 0.0
    for row in rows:
        value = _value(row, value_key)
        if value is None:
            continue
        weight = _value(row, weight_key) or 1.0
        total += value * weight
        total_weight += weight
    return total / total_weight if total_weight else 0.0


def _change_failure_rate(rows: OperatingReviewRows) -> float:
    deployments = _sum(rows.deployments, "deployments_count")
    failed = _sum(rows.deployments, "failed_deployments_count")
    if deployments > 0:
        return failed / deployments
    return _avg(rows.repo_metrics, "change_failure_rate")


def _first_non_zero(*values: float) -> float:
    for value in values:
        if value != 0:
            return value
    return 0.0


def _investment_units(rows: Iterable[Mapping[str, Any]]) -> dict[str, float]:
    units = {"ktlo": 0.0, "new_value": 0.0, "security": 0.0, "infra": 0.0}
    for row in rows:
        area = str(row.get("investment_area", "")).strip().lower()
        key = _investment_key(area)
        if key is not None:
            units[key] += _value(row, "delivery_units") or 0.0
    return units


def _investment_key(area: str) -> str | None:
    normalized = area.replace("_", " ").replace("/", " ")
    if normalized in {"ktlo", "maintenance", "maintenance tech debt"}:
        return "ktlo"
    if normalized in {"new value", "feature delivery", "features"}:
        return "new_value"
    if normalized in {"security", "risk security"}:
        return "security"
    if normalized in {"infra", "infrastructure", "operational support"}:
        return "infra"
    return None
