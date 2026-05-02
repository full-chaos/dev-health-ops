from __future__ import annotations

from datetime import date, datetime
from typing import Any

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts


async def fetch_investment_flow_items(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    limit: int,
    org_id: str = "",
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
            theme_kv.1 AS source,
            ifNull(r.repo, toString(repo_id)) AS target,
            sum(theme_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(theme_distribution_json AS Array(Tuple(String, Float32))) AS theme_kv
        WHERE from_ts < %(end_ts)s AND to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
            {scope_filter}
        GROUP BY source, target
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    params = {"start_ts": start_ts, "end_ts": end_ts, "limit": limit}
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(sink, query, params)


async def fetch_expense_counts(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
            sum(new_items_count) AS new_items,
            sum(new_bugs_count) AS new_bugs,
            sum(items_completed * bug_completed_ratio) AS bug_completed_estimate
        FROM work_item_metrics_daily
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND org_id = %(org_id)s
            {scope_filter}
    """
    params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(sink, query, params)


async def fetch_expense_abandoned(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
            countIf(status = 'canceled') AS canceled_items
        FROM work_item_cycle_times
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND org_id = %(org_id)s
            {scope_filter}
    """
    params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(sink, query, params)


async def fetch_state_status_counts(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
            status,
            sum(items_touched) AS items_touched
        FROM work_item_state_durations_daily
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND org_id = %(org_id)s
            {scope_filter}
        GROUP BY status
        ORDER BY items_touched DESC
    """
    params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(sink, query, params)


async def fetch_hotspot_rows(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    limit: int,
    org_id: str = "",
) -> list[dict[str, Any]]:
    query = f"""
        WITH
            (
                SELECT quantileExact(0.7)(churn) FROM (
                    SELECT
                        sum(metrics.churn) AS churn
                    FROM file_metrics_daily AS metrics
                    INNER JOIN repos AS r ON r.id = metrics.repo_id
                    WHERE metrics.day >= %(start_day)s AND metrics.day < %(end_day)s
                        AND metrics.org_id = %(org_id)s
                        {scope_filter}
                    GROUP BY r.repo, metrics.path
                )
            ) AS churn_hi,
            (
                SELECT quantileExact(0.3)(churn) FROM (
                    SELECT
                        sum(metrics.churn) AS churn
                    FROM file_metrics_daily AS metrics
                    INNER JOIN repos AS r ON r.id = metrics.repo_id
                    WHERE metrics.day >= %(start_day)s AND metrics.day < %(end_day)s
                        AND metrics.org_id = %(org_id)s
                        {scope_filter}
                    GROUP BY r.repo, metrics.path
                )
            ) AS churn_mid
        SELECT
            r.repo AS repo,
            if(
                position(metrics.path, '/') > 0,
                arrayElement(splitByChar('/', metrics.path), 1),
                '(root)'
            ) AS directory,
            metrics.path AS file_path,
            multiIf(
                sum(metrics.churn) >= churn_hi,
                'refactor',
                sum(metrics.churn) >= churn_mid,
                'fix',
                'feature'
            ) AS change_type,
            sum(metrics.churn) AS churn
        FROM file_metrics_daily AS metrics
        INNER JOIN repos AS r
            ON r.id = metrics.repo_id
        WHERE metrics.day >= %(start_day)s AND metrics.day < %(end_day)s
          AND metrics.path != ''
          AND metrics.org_id = %(org_id)s
            {scope_filter}
        GROUP BY repo, directory, file_path
        ORDER BY churn DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(sink, query, params)
