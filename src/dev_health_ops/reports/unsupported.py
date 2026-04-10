from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.reports.charts import ChartResult


@dataclass(frozen=True)
class MetricAvailability:
    metric: str
    available: bool
    reason: str | None


def check_metric_availability(
    requested_metrics: list[str],
    chart_results: list[ChartResult],
) -> list[MetricAvailability]:
    available_metrics = {cr.spec.metric for cr in chart_results if not cr.empty}
    all_metrics = {cr.spec.metric for cr in chart_results}
    result: list[MetricAvailability] = []
    for metric in requested_metrics:
        if metric in available_metrics:
            result.append(
                MetricAvailability(metric=metric, available=True, reason=None)
            )
        elif metric in all_metrics:
            result.append(
                MetricAvailability(
                    metric=metric, available=False, reason="time_range_empty"
                )
            )
        else:
            result.append(
                MetricAvailability(
                    metric=metric, available=False, reason="not_computed"
                )
            )
    return result


def generate_availability_notice(unavailable: list[MetricAvailability]) -> str:
    if not unavailable:
        return ""
    lines = [
        "> **Note:** Some requested metrics have limited data in this report window:\n"
    ]
    for ma in unavailable:
        reason_text = {
            "time_range_empty": "no data in selected time range",
            "not_computed": "metric not yet computed",
            "no_data": "no data available",
        }.get(ma.reason or "", ma.reason or "unavailable")
        lines.append(f"> - **{ma.metric}**: {reason_text}")
    return "\n".join(lines)
