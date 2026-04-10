"""Pre-built report template library."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class ChartTemplate:
    chart_type: str
    metric: str
    title: str
    group_by: str | None = None


@dataclass(frozen=True)
class ReportTemplate:
    report_type: str
    sections: tuple[str, ...]
    default_metrics: tuple[str, ...]
    charts: tuple[ChartTemplate, ...]
    default_comparison: str | None


TEMPLATE_LIBRARY = {
    "weekly_health": ReportTemplate(
        report_type="weekly_health",
        sections=("summary", "delivery", "quality", "testops", "wellbeing"),
        default_metrics=(
            "items_completed",
            "cycle_time_p50_hours",
            "success_rate",
            "flake_rate",
            "line_coverage_pct",
            "after_hours_commit_ratio",
        ),
        charts=(
            ChartTemplate("scorecard", "items_completed", "Weekly throughput"),
            ChartTemplate("trend_delta", "cycle_time_p50_hours", "Cycle time change"),
            ChartTemplate("line", "flake_rate", "Flaky test trend", "day"),
            ChartTemplate(
                "line", "after_hours_commit_ratio", "After-hours trend", "day"
            ),
        ),
        default_comparison="prior_week",
    ),
    "monthly_review": ReportTemplate(
        report_type="monthly_review",
        sections=(
            "summary",
            "delivery",
            "quality",
            "testops",
            "wellbeing",
            "benchmarks",
            "trends",
        ),
        default_metrics=(
            "items_completed",
            "cycle_time_p50_hours",
            "lead_time_p50_hours",
            "success_rate",
            "flake_rate",
            "line_coverage_pct",
            "weekend_commit_ratio",
        ),
        charts=(
            ChartTemplate(
                "line", "items_completed", "Monthly throughput trend", "week"
            ),
            ChartTemplate("line", "cycle_time_p50_hours", "Cycle time trend", "week"),
            ChartTemplate("line", "success_rate", "Pipeline success trend", "week"),
            ChartTemplate("line", "line_coverage_pct", "Coverage trend", "week"),
        ),
        default_comparison="prior_month",
    ),
    "quality_trend": ReportTemplate(
        report_type="quality_trend",
        sections=("summary", "quality", "testops"),
        default_metrics=(
            "flake_rate",
            "failure_rate",
            "pass_rate",
            "line_coverage_pct",
            "coverage_regression_count",
        ),
        charts=(
            ChartTemplate("line", "flake_rate", "Flake rate trend", "day"),
            ChartTemplate("line", "failure_rate", "Failure rate trend", "day"),
            ChartTemplate("line", "line_coverage_pct", "Coverage trend", "day"),
        ),
        default_comparison="prior_period",
    ),
    "ci_stability": ReportTemplate(
        report_type="ci_stability",
        sections=("summary", "testops", "quality"),
        default_metrics=(
            "success_rate",
            "median_duration_seconds",
            "avg_queue_seconds",
            "rerun_rate",
            "failure_rate",
        ),
        charts=(
            ChartTemplate("scorecard", "success_rate", "CI success rate"),
            ChartTemplate(
                "line", "median_duration_seconds", "Pipeline duration trend", "day"
            ),
            ChartTemplate("line", "avg_queue_seconds", "Queue time trend", "day"),
            ChartTemplate("line", "rerun_rate", "Rerun trend", "day"),
        ),
        default_comparison="prior_period",
    ),
}


def get_template(report_type: str | None) -> ReportTemplate | None:
    if report_type is None:
        return None
    return TEMPLATE_LIBRARY.get(report_type)


def default_time_range(report_type: str | None, *, today: date) -> tuple[date, date]:
    if report_type == "monthly_review":
        end = today.replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)
        return start, end
    if report_type == "ci_stability":
        return today - timedelta(days=13), today
    if report_type == "quality_trend":
        return today - timedelta(days=29), today
    return today - timedelta(days=6), today
