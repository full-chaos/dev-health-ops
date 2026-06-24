from __future__ import annotations

from datetime import date
from typing import Any

from dev_health_ops.investment_taxonomy import SUBCATEGORIES, THEMES, theme_of

from .client import query_dicts


def _date_params(start_day: date, end_day: date) -> dict[str, Any]:
    return {"start_day": start_day, "end_day": end_day}


# CHAOS-2377 / CHAOS-2645: daily-rollup tables the sync job re-writes per run.
# work_item_metrics_daily and work_item_user_metrics_daily are
# ReplacingMergeTree(computed_at) (migration 055); work_item_state_durations_daily
# stays MergeTree. In all cases a top-level sum() double-counts re-runs/backfills,
# so the table read is wrapped in a per-key argMax(..., computed_at) dedup subquery
# before aggregating (the FINAL-equivalent for the metric-config read path).
_DEDUP_BY_COMPUTED_AT: dict[str, tuple[str, ...]] = {
    "work_item_state_durations_daily": (
        "day",
        "provider",
        "work_scope_id",
        "team_id",
        "status",
    ),
    "work_item_metrics_daily": (
        "day",
        "provider",
        "work_scope_id",
        "team_id",
    ),
    "work_item_user_metrics_daily": (
        "day",
        "provider",
        "work_scope_id",
        "user_identity",
    ),
}


def _metric_from_clause(
    *,
    table: str,
    column: str,
    scope_filter: str,
    start_param: str = "start_day",
    end_param: str = "end_day",
) -> str:
    """Return the FROM source for a metric read.

    For most tables this is just the raw table name. For ReplacingMergeTree-by-
    ``computed_at`` daily tables (those in ``_DEDUP_BY_COMPUTED_AT``) the table
    is wrapped in a subquery that collapses to the latest ``computed_at`` per
    natural key — so a top-level ``sum(column)`` counts each key once instead of
    once per re-run. ``org_id`` stays filtered in the inner ``WHERE``.

    ``start_param`` / ``end_param`` name the bound query params so a single
    subquery shape can serve both the current and comparison windows (the
    driver-delta CTE pair reuses this with ``compare_start`` / ``compare_end``).
    """
    natural_key = _DEDUP_BY_COMPUTED_AT.get(table)
    if natural_key is None:
        return table
    key_columns = ",\n                ".join(natural_key)
    return f"""(
            SELECT
                {key_columns},
                argMax({column}, computed_at) AS {column}
            FROM {table}
            WHERE day >= %({start_param})s AND day < %({end_param})s
            {scope_filter}
              AND org_id = %(org_id)s
            GROUP BY {", ".join(natural_key)}
        )"""


async def fetch_metric_series(
    client: Any,
    *,
    table: str,
    column: str,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    aggregator: str,
    org_id: str = "",
) -> list[dict[str, Any]]:
    value_expression = _metric_value_expression(
        table=table, column=column, aggregator=aggregator
    )
    if table in _DEDUP_BY_COMPUTED_AT:
        # Dedup re-run rows under the outer sum() (CHAOS-2377). The subquery
        # filters day/scope/org_id and emits one row per natural key with the
        # latest computed_at, so we group only by day on the outer query.
        from_clause = _metric_from_clause(
            table=table, column=column, scope_filter=scope_filter
        )
        query = f"""
        SELECT
            day,
            {value_expression} AS value
        FROM {from_clause}
        GROUP BY day
        ORDER BY day
    """
    else:
        query = f"""
        SELECT
            day,
            {value_expression} AS value
        FROM {table}
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {scope_filter}
          AND org_id = %(org_id)s
        GROUP BY day
        ORDER BY day
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(client, query, params)


async def fetch_metric_value(
    client: Any,
    *,
    table: str,
    column: str,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    aggregator: str,
    org_id: str = "",
) -> float:
    value_expression = _metric_value_expression(
        table=table, column=column, aggregator=aggregator
    )
    if table in _DEDUP_BY_COMPUTED_AT:
        # Dedup re-run rows under the outer sum() (CHAOS-2377). Without this the
        # /explain blocked_work headline (current + comparison) inflates by the
        # number of duplicate daily runs/backfills for the same natural key.
        from_clause = _metric_from_clause(
            table=table, column=column, scope_filter=scope_filter
        )
        query = f"""
        SELECT
            {value_expression} AS value
        FROM {from_clause}
    """
    else:
        query = f"""
        SELECT
            {value_expression} AS value
        FROM {table}
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {scope_filter}
          AND org_id = %(org_id)s
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params["org_id"] = org_id
    rows = await query_dicts(client, query, params)
    if not rows:
        return 0.0
    value = rows[0].get("value")
    return float(value or 0.0)


def _metric_value_expression(*, table: str, column: str, aggregator: str) -> str:
    if table == "repo_metrics_daily" and column == "pr_rework_ratio":
        return "SUM(pr_rework_ratio * prs_merged) / NULLIF(SUM(prs_merged), 0)"
    return f"{aggregator}({column})"


async def fetch_blocked_hours(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
) -> tuple[float, list[dict[str, Any]]]:
    # CHAOS-2377: the daily job appends a fresh row per run to this plain
    # MergeTree, so a re-run/backfill of the same day leaves duplicate rows.
    # Dedup with argMax(duration_hours, computed_at) over the full natural key
    # before summing, matching the operating_review reader; summing raw rows
    # would inflate the blocked-hours panel on every re-run.
    query = f"""
        SELECT
            day,
            sum(duration_hours) AS value
        FROM (
            SELECT
                day,
                provider,
                work_scope_id,
                team_id,
                status,
                argMax(duration_hours, computed_at) AS duration_hours
            FROM work_item_state_durations_daily
            WHERE day >= %(start_day)s AND day < %(end_day)s
              AND status = 'blocked'
            {scope_filter}
              AND org_id = %(org_id)s
            GROUP BY day, provider, work_scope_id, team_id, status
        )
        GROUP BY day
        ORDER BY day
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params["org_id"] = org_id
    rows = await query_dicts(client, query, params)
    total = sum(float(row.get("value") or 0.0) for row in rows)
    return total, rows


_INVESTMENT_THEME_LABELS = {
    "feature_delivery": "Feature Delivery",
    "operational": "Operational / Support",
    "maintenance": "Maintenance / Tech Debt",
    "quality": "Quality / Reliability",
    "risk": "Risk / Security",
}


def canonical_investment_theme_sql(column: str = "investment_area") -> str:
    mappings: dict[str, str] = {theme: theme for theme in THEMES}
    mappings.update(
        {subcategory: theme_of(subcategory) for subcategory in SUBCATEGORIES}
    )
    for subcategory in SUBCATEGORIES:
        leaf = subcategory.rsplit(".", 1)[-1]
        mappings.setdefault(leaf, theme_of(subcategory))

    clauses = []
    for raw_key, theme in sorted(mappings.items()):
        clauses.append(f"lowerUTF8({column}) = '{raw_key}'")
        clauses.append(f"'{theme}'")
    return f"multiIf({', '.join(clauses)}, '')"


async def fetch_rework_theme_allocation(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    work_category_filter: str = "",
    work_category_params: dict[str, Any] | None = None,
    org_id: str = "",
) -> list[dict[str, Any]]:
    canonical_theme_expr = canonical_investment_theme_sql("investment_area")
    query = f"""
        SELECT
            canonical_theme AS theme,
            sum(work_items_completed) AS allocation,
            sum(prs_merged) AS prs_merged,
            sum(churn_loc) AS churn_loc
        FROM (
            SELECT
                day,
                repo_id,
                team_id,
                {canonical_theme_expr} AS canonical_theme,
                project_stream,
                argMax(work_items_completed, computed_at) AS work_items_completed,
                argMax(prs_merged, computed_at) AS prs_merged,
                argMax(churn_loc, computed_at) AS churn_loc
            FROM investment_metrics_daily
            WHERE day >= %(start_day)s AND day < %(end_day)s
            {scope_filter}
            {work_category_filter}
              AND org_id = %(org_id)s
            GROUP BY day, repo_id, team_id, canonical_theme, project_stream
        )
        WHERE canonical_theme != ''
        GROUP BY canonical_theme
        ORDER BY allocation DESC
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params.update(work_category_params or {})
    params["org_id"] = org_id
    rows = await query_dicts(client, query, params)
    rows = [row for row in rows if str(row.get("theme") or "") in THEMES]
    total = sum(float(row.get("allocation") or 0.0) for row in rows)
    allocations: list[dict[str, Any]] = []
    for row in rows:
        theme = str(row.get("theme") or "")
        allocation = float(row.get("allocation") or 0.0)
        allocations.append(
            {
                "theme": theme,
                "label": _INVESTMENT_THEME_LABELS[theme],
                "allocation": allocation,
                "allocation_pct": (allocation / total * 100.0) if total else 0.0,
                "prs_merged": int(row.get("prs_merged") or 0),
                "churn_loc": int(row.get("churn_loc") or 0),
            }
        )
    return allocations
