"""Backfill metrics diagnostics (CHAOS-2888 Workstream C).

Read-only ClickHouse aggregation feeding
``BackfillJobResponse.metrics_diagnostics`` for the backfill-job detail
endpoint (``GET /backfill-jobs/{job_id}``). Reuses the fixed reason-name
vocabulary from ``metrics/compounding_risk.py`` (Workstream B) so the API
surface and the metrics job report identical missing-input semantics.

ClickHouse analytics reads only -- no Postgres shortcuts, no persistence.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Protocol

from dev_health_ops.metrics.compounding_risk import (
    REASON_MISSING_COMPLEXITY_DELTA,
    REASON_MISSING_OWNERSHIP_SIGNAL,
    REASON_MISSING_REVIEW_LATENCY,
    REASON_MISSING_REWORK_CHURN,
)

from ..schemas.backfill import (
    BackfillMetricsDiagnostics,
    BackfillMetricsDiagnosticsBucket,
    BackfillMetricsDiagnosticsDay,
)

_REASON_KEYS: tuple[str, ...] = (
    REASON_MISSING_REWORK_CHURN,
    REASON_MISSING_COMPLEXITY_DELTA,
    REASON_MISSING_REVIEW_LATENCY,
    REASON_MISSING_OWNERSHIP_SIGNAL,
)


class MetricsDiagnosticsSink(Protocol):
    """Structural sink contract -- the same one ``compounding_risk.py``'s
    ``load_repo_complexity_delta_30d`` already uses (``ClickHouseMetricsSink``
    in production, a canned fake in tests).
    """

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]: ...


def _day_range(start: date, end: date) -> list[date]:
    if end < start:
        return []
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _rows_per_day_query(table: str) -> str:
    # DISTINCT repo_id rather than a raw row count: both tables are
    # append-only MergeTree (no dedup engine), so a recomputed day would
    # otherwise double-count. "Rows" here means "repos with data for this day".
    return f"""
        SELECT day, count(DISTINCT repo_id) AS row_count
        FROM {table}
        WHERE org_id = {{org_id:String}}
          AND day >= {{start:Date}} AND day <= {{end:Date}}
        GROUP BY day
    """


_COMPOUNDING_RISK_QUERY = """
    WITH latest AS (
        SELECT
            day,
            scope,
            scope_id,
            argMax(compounding_risk, computed_at) AS compounding_risk,
            argMax(severity, computed_at) AS severity,
            argMax(rework_churn, computed_at) AS rework_churn,
            argMax(complexity_delta, computed_at) AS complexity_delta,
            argMax(review_latency_p90h, computed_at) AS review_latency_p90h,
            argMax(single_owner_ratio, computed_at) AS single_owner_ratio,
            argMax(ownership_gini, computed_at) AS ownership_gini
        FROM compounding_risk_daily
        WHERE org_id = {org_id:String}
          AND day >= {start:Date} AND day <= {end:Date}
          -- Rows are persisted per (day, scope, scope_id): 'repo' rows and
          -- 'team' rows both cover the same org/day, so counting both scopes
          -- here would double-count. Diagnostics report the repo-scope view;
          -- the DTO has no scope split (CHAOS-2888 Workstream C review fix).
          AND scope = 'repo'
        GROUP BY day, scope, scope_id
    )
    SELECT
        day,
        count() AS total_rows,
        countIf(compounding_risk IS NOT NULL) AS non_null_rows,
        countIf(severity = 'unknown') AS unknown_rows,
        countIf(rework_churn IS NULL) AS missing_rework_churn,
        countIf(complexity_delta IS NULL) AS missing_complexity_delta,
        countIf(review_latency_p90h IS NULL) AS missing_review_latency,
        countIf(single_owner_ratio IS NULL AND ownership_gini IS NULL)
            AS missing_ownership_signal
    FROM latest
    GROUP BY day
"""


def _empty_bucket() -> dict[str, Any]:
    return {
        "repo_metrics_rows": 0,
        "repo_complexity_rows": 0,
        "compounding_risk_rows": 0,
        "compounding_risk_non_null_rows": 0,
        "compounding_risk_unknown_rows": 0,
        "reason_counts": dict.fromkeys(_REASON_KEYS, 0),
    }


def _int_rows_by_day(rows: list[dict[str, Any]]) -> dict[date, int]:
    return {
        row["day"]: int(row.get("row_count") or 0)
        for row in rows
        if isinstance(row.get("day"), date)
    }


def build_backfill_metrics_diagnostics(
    sink: MetricsDiagnosticsSink,
    *,
    org_id: str,
    range_start: date,
    range_end: date,
) -> BackfillMetricsDiagnostics:
    """Aggregate ClickHouse row counts and missing-input reasons for a backfill window.

    Reads ``repo_metrics_daily``, ``repo_complexity_daily``, and
    ``compounding_risk_daily`` directly. Per-day rows are required by the
    CHAOS-2888 shared diagnostics contract because the underlying failure
    mode (historical complexity unsupported) is window-specific.
    """
    params = {"org_id": org_id, "start": range_start, "end": range_end}

    metrics_by_day = _int_rows_by_day(
        sink.query_dicts(_rows_per_day_query("repo_metrics_daily"), params)
    )
    complexity_by_day = _int_rows_by_day(
        sink.query_dicts(_rows_per_day_query("repo_complexity_daily"), params)
    )
    risk_by_day: dict[date, dict[str, Any]] = {
        row["day"]: row
        for row in sink.query_dicts(_COMPOUNDING_RISK_QUERY, params)
        if isinstance(row.get("day"), date)
    }

    per_day: list[BackfillMetricsDiagnosticsDay] = []
    agg = _empty_bucket()

    for day in _day_range(range_start, range_end):
        bucket = _empty_bucket()
        bucket["repo_metrics_rows"] = metrics_by_day.get(day, 0)
        bucket["repo_complexity_rows"] = complexity_by_day.get(day, 0)

        risk_row = risk_by_day.get(day)
        if risk_row is not None:
            bucket["compounding_risk_rows"] = int(risk_row.get("total_rows") or 0)
            bucket["compounding_risk_non_null_rows"] = int(
                risk_row.get("non_null_rows") or 0
            )
            bucket["compounding_risk_unknown_rows"] = int(
                risk_row.get("unknown_rows") or 0
            )
            bucket["reason_counts"] = {
                REASON_MISSING_REWORK_CHURN: int(
                    risk_row.get("missing_rework_churn") or 0
                ),
                REASON_MISSING_COMPLEXITY_DELTA: int(
                    risk_row.get("missing_complexity_delta") or 0
                ),
                REASON_MISSING_REVIEW_LATENCY: int(
                    risk_row.get("missing_review_latency") or 0
                ),
                REASON_MISSING_OWNERSHIP_SIGNAL: int(
                    risk_row.get("missing_ownership_signal") or 0
                ),
            }

        agg["repo_metrics_rows"] += bucket["repo_metrics_rows"]
        agg["repo_complexity_rows"] += bucket["repo_complexity_rows"]
        agg["compounding_risk_rows"] += bucket["compounding_risk_rows"]
        agg["compounding_risk_non_null_rows"] += bucket[
            "compounding_risk_non_null_rows"
        ]
        agg["compounding_risk_unknown_rows"] += bucket["compounding_risk_unknown_rows"]
        for reason, count in bucket["reason_counts"].items():
            agg["reason_counts"][reason] += count

        per_day.append(BackfillMetricsDiagnosticsDay(day=day, **bucket))

    return BackfillMetricsDiagnostics(
        range_start=range_start,
        range_end=range_end,
        aggregate=BackfillMetricsDiagnosticsBucket(**agg),
        per_day=per_day,
    )


__all__ = [
    "MetricsDiagnosticsSink",
    "build_backfill_metrics_diagnostics",
]
