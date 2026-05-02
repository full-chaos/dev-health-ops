from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
from typing import Any

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.storage import detect_db_type

ROLLING_WINDOWS = (7, 30)
ROLLING_TABLE_SPECS = [
    {
        "table": "repo_metrics_daily",
        "date_column": "day",
        "group_by": ("repo_id",),
        "sum_metrics": ("commits_count", "prs_merged", "total_loc_touched"),
        "avg_metrics": ("avg_commit_size_loc",),
        "p50_metrics": ("median_pr_cycle_hours",),
    },
    {
        "table": "user_metrics_daily",
        "date_column": "day",
        "group_by": ("repo_id", "author_email"),
        "sum_metrics": ("loc_touched", "delivery_units"),
        "avg_metrics": ("avg_commit_size_loc",),
        "p50_metrics": ("cycle_p50_hours",),
    },
    {
        "table": "work_item_metrics_daily",
        "date_column": "day",
        "group_by": ("provider", "work_scope_id", "team_id"),
        "sum_metrics": ("items_started", "items_completed"),
        "avg_metrics": (),
        "p50_metrics": ("cycle_time_p50_hours",),
    },
]


def _window_start(as_of: date, window_days: int) -> date:
    return as_of - timedelta(days=max(1, int(window_days)) - 1)


def _query_dicts_clickhouse(
    client: Any, query: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _fetch_table_presence_clickhouse(
    client: Any, tables: Iterable[str]
) -> dict[str, bool]:
    table_list = list(tables)
    if not table_list:
        return {}
    rows = _query_dicts_clickhouse(
        client,
        """
        SELECT name
        FROM system.tables
        WHERE database = currentDatabase()
          AND name IN %(tables)s
        """,
        {"tables": table_list},
    )
    present = {row.get("name") for row in rows}
    return {table: table in present for table in table_list}


def _count_rows_clickhouse(
    client: Any, table: str, date_column: str, start: date, end: date
) -> int:
    rows = _query_dicts_clickhouse(
        client,
        f"""
        SELECT count() AS count
        FROM {table}
        WHERE {date_column} >= toDate(%(start)s)
          AND {date_column} <= toDate(%(end)s)
        """,
        {"start": start.isoformat(), "end": end.isoformat()},
    )
    if not rows:
        return 0
    return int(rows[0].get("count") or 0)


def _sum_monotonicity_clickhouse(
    client: Any,
    table: str,
    date_column: str,
    group_by: Sequence[str],
    metric: str,
    start_short: date,
    start_long: date,
    end: date,
) -> dict[str, Any]:
    group_cols = ", ".join(group_by)
    rows = _query_dicts_clickhouse(
        client,
        f"""
        WITH
          toDate(%(start_short)s) AS start_short,
          toDate(%(start_long)s) AS start_long,
          toDate(%(end)s) AS end_day
        SELECT
          count() AS group_count,
          sumIf(1, sum_short > sum_long) AS drift_count,
          max(sum_short - sum_long) AS max_delta
        FROM (
          SELECT
            {group_cols},
            sumIf({metric}, {date_column} >= start_short AND {date_column} <= end_day) AS sum_short,
            sumIf({metric}, {date_column} >= start_long AND {date_column} <= end_day) AS sum_long
          FROM {table}
          WHERE {date_column} >= start_long AND {date_column} <= end_day
          GROUP BY {group_cols}
        )
        """,
        {
            "start_short": start_short.isoformat(),
            "start_long": start_long.isoformat(),
            "end": end.isoformat(),
        },
    )
    if not rows:
        return {"group_count": 0, "drift_count": 0, "max_delta": 0.0}
    row = rows[0]
    return {
        "group_count": int(row.get("group_count") or 0),
        "drift_count": int(row.get("drift_count") or 0),
        "max_delta": float(row.get("max_delta") or 0.0),
    }


def _avg_check_clickhouse(
    client: Any,
    table: str,
    date_column: str,
    group_by: Sequence[str],
    metric: str,
    agg_kind: str,
    start_long: date,
    end: date,
) -> dict[str, Any]:
    group_cols = ", ".join(group_by)
    if agg_kind == "p50":
        agg_expr = f"quantile(0.5)(if({metric} IS NOT NULL, {metric}, NULL))"
    else:
        agg_expr = f"avg({metric})"
    rows = _query_dicts_clickhouse(
        client,
        f"""
        WITH
          toDate(%(start_long)s) AS start_long,
          toDate(%(end)s) AS end_day
        SELECT
          count() AS group_count,
          sumIf(1, sample_count = 0) AS no_samples,
          sumIf(1, sample_count > 0 AND not isFinite(avg_val)) AS non_finite
        FROM (
          SELECT
            {group_cols},
            countIf({metric} IS NOT NULL) AS sample_count,
            {agg_expr} AS avg_val
          FROM {table}
          WHERE {date_column} >= start_long AND {date_column} <= end_day
          GROUP BY {group_cols}
        )
        """,
        {"start_long": start_long.isoformat(), "end": end.isoformat()},
    )
    if not rows:
        return {"group_count": 0, "no_samples": 0, "non_finite": 0}
    row = rows[0]
    return {
        "group_count": int(row.get("group_count") or 0),
        "no_samples": int(row.get("no_samples") or 0),
        "non_finite": int(row.get("non_finite") or 0),
    }


def run_rolling_aggregates_audit(*, db_url: str, as_of: date) -> dict[str, Any]:
    backend = detect_db_type(db_url)
    windows = sorted(set(int(w) for w in ROLLING_WINDOWS))
    short_window = windows[0]
    long_window = windows[-1]
    start_short = _window_start(as_of, short_window)
    start_long = _window_start(as_of, long_window)

    tables = [str(spec["table"]) for spec in ROLLING_TABLE_SPECS]
    report: dict[str, Any] = {
        "as_of": as_of,
        "windows": windows,
        "tables": {},
        "overall_ok": True,
    }

    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. Only ClickHouse is supported (CHAOS-641). "
            "Set CLICKHOUSE_URI and use a clickhouse:// connection string."
        )
    sink = ClickHouseMetricsSink(db_url)
    client = sink.client
    try:
        presence = _fetch_table_presence_clickhouse(client, tables)
        for spec in ROLLING_TABLE_SPECS:
            _populate_table_report(
                report=report,
                presence=presence,
                spec=spec,
                client=client,
                start_short=start_short,
                start_long=start_long,
                end=as_of,
            )
    finally:
        sink.close()

    report["overall_ok"] = all(
        entry.get("status") in {"ok", "no_data"} for entry in report["tables"].values()
    )
    return report


def _populate_table_report(
    *,
    report: dict[str, Any],
    presence: dict[str, bool],
    spec: dict[str, Any],
    client: Any,
    start_short: date,
    start_long: date,
    end: date,
) -> None:
    table = spec["table"]
    date_column = spec["date_column"]
    group_by = spec["group_by"]
    sum_metrics = spec.get("sum_metrics", ())
    avg_metrics = spec.get("avg_metrics", ())
    p50_metrics = spec.get("p50_metrics", ())

    entry: dict[str, Any] = {
        "status": "missing",
        "rows": 0,
        "sum_metrics": {},
        "avg_metrics": {},
        "p50_metrics": {},
        "issues": [],
    }

    if not presence.get(table):
        entry["issues"].append("missing_table")
        report["tables"][table] = entry
        return

    row_count = _count_rows_clickhouse(client, table, date_column, start_long, end)

    entry["rows"] = row_count
    if row_count == 0:
        entry["status"] = "no_data"
        report["tables"][table] = entry
        return

    for metric in sum_metrics:
        stats = _sum_monotonicity_clickhouse(
            client,
            table,
            date_column,
            group_by,
            metric,
            start_short,
            start_long,
            end,
        )
        entry["sum_metrics"][metric] = stats
        drift_count = stats.get("drift_count", 0)
        group_count = stats.get("group_count", 0)
        if drift_count:
            entry["issues"].append(f"sum:{metric} drift={drift_count}/{group_count}")

    for metric in avg_metrics:
        stats = _avg_check_clickhouse(
            client,
            table,
            date_column,
            group_by,
            metric,
            "avg",
            start_long,
            end,
        )
        entry["avg_metrics"][metric] = stats
        _append_avg_issues(entry, metric, stats, label="avg")

    for metric in p50_metrics:
        stats = _avg_check_clickhouse(
            client,
            table,
            date_column,
            group_by,
            metric,
            "p50",
            start_long,
            end,
        )
        entry["p50_metrics"][metric] = stats
        _append_avg_issues(entry, metric, stats, label="p50")

    entry["status"] = "drift" if entry["issues"] else "ok"
    report["tables"][table] = entry


def _append_avg_issues(
    entry: dict[str, Any], metric: str, stats: dict[str, Any], label: str
) -> None:
    no_samples = stats.get("no_samples", 0)
    non_finite = stats.get("non_finite", 0)
    group_count = stats.get("group_count", 0)
    if no_samples:
        entry["issues"].append(
            f"{label}:{metric} no_samples={no_samples}/{group_count}"
        )
    if non_finite:
        entry["issues"].append(
            f"{label}:{metric} non_finite={non_finite}/{group_count}"
        )


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _row(cells: list[str]) -> str:
        padded = [cell.ljust(widths[idx]) for idx, cell in enumerate(cells)]
        return f"| {' | '.join(padded)} |"

    lines = [_row(headers)]
    lines.append(_row(["-" * w for w in widths]))
    for row in rows:
        lines.append(_row(row))
    return "\n".join(lines)


def format_rolling_aggregates_table(report: dict[str, Any]) -> str:
    as_of = report.get("as_of")
    windows = report.get("windows", [])
    if isinstance(as_of, date):
        as_of_label = as_of.isoformat()
    else:
        as_of_label = str(as_of or "-")

    header = f"Rolling aggregates as of {as_of_label} (windows: {', '.join(f'{w}d' for w in windows)})"

    rows: list[list[str]] = []
    tables = report.get("tables", {})
    for table, entry in tables.items():
        issues = entry.get("issues") or []
        status = entry.get("status", "missing")
        row_count = entry.get("rows")
        rows.append(
            [
                table,
                "n/a" if status == "missing" else str(row_count or 0),
                status,
                ", ".join(issues) if issues else "-",
            ]
        )

    table_block = _render_table(["table", "rows", "status", "issues"], rows)
    return "\n".join([header, table_block])


def _serialize_report(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _serialize_report(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_serialize_report(val) for val in value]
    if isinstance(value, date):
        return value.isoformat()
    return value


def format_rolling_aggregates_json(report: dict[str, Any]) -> str:
    return json.dumps(_serialize_report(report), indent=2, sort_keys=True)


def rolling_aggregates_failed(report: dict[str, Any]) -> bool:
    tables = report.get("tables", {})
    for entry in tables.values():
        if entry.get("status") in {"drift", "missing"}:
            return True
    return False
