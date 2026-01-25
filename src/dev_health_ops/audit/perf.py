from __future__ import annotations

import argparse
import logging
from typing import Any, Dict, List, Optional

from dev_health_ops.storage import detect_db_type

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD_MS = 1000
DEFAULT_LOOKBACK_MINUTES = 60
DEFAULT_LIMIT = 20
MAX_QUERY_LENGTH = 500


def _result_rows(result: Any) -> List[Any]:
    return list(getattr(result, "result_rows", []) or [])


def _column_names(result: Any) -> List[str]:
    return list(getattr(result, "column_names", []) or [])


def _truncate_query(query: str, max_length: int = MAX_QUERY_LENGTH) -> str:
    normalized = " ".join((query or "").split())
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[: max_length - 3]}..."


def _describe_query_log_columns(client: Any) -> List[str]:
    result = client.query("DESCRIBE TABLE system.query_log")
    return [row[0] for row in _result_rows(result) if row]


def _get_setting_value(client: Any, name: str) -> Optional[str]:
    result = client.query(
        "SELECT value FROM system.settings WHERE name = {name:String}",
        parameters={"name": name},
    )
    rows = _result_rows(result)
    if not rows:
        return None
    return str(rows[0][0])


def _pick_time_column(columns: List[str]) -> Optional[str]:
    for candidate in ("event_time", "query_start_time", "event_time_microseconds"):
        if candidate in columns:
            return candidate
    return None


def run_perf_audit(
    db_url: str,
    threshold_ms: int = DEFAULT_THRESHOLD_MS,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
    limit: int = DEFAULT_LIMIT,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "status": "unchecked",
        "db_backend": None,
        "db_error": None,
        "threshold_ms": int(threshold_ms),
        "lookback_minutes": int(lookback_minutes),
        "limit": int(limit),
        "log_settings": {},
        "slow_queries": [],
        "notes": [],
    }

    try:
        backend = detect_db_type(db_url)
    except ValueError as exc:
        report["db_error"] = str(exc)
        return report

    report["db_backend"] = backend
    if backend != "clickhouse":
        report["db_error"] = f"unsupported backend: {backend}"
        return report

    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(dsn=db_url)
    except Exception as exc:
        report["db_error"] = f"connection failed: {exc}"
        return report

    try:
        try:
            columns = _describe_query_log_columns(client)
        except Exception as exc:
            report["db_error"] = f"query_log unavailable: {exc}"
            return report

        if "query_duration_ms" not in columns:
            report["db_error"] = "query_log missing query_duration_ms"
            return report

        log_queries = _get_setting_value(client, "log_queries")
        log_min_ms = _get_setting_value(client, "log_queries_min_query_duration_ms")
        if log_queries is not None:
            report["log_settings"]["log_queries"] = log_queries
        if log_min_ms is not None:
            report["log_settings"]["log_queries_min_query_duration_ms"] = log_min_ms

        if log_queries == "0":
            report["db_error"] = "query_log disabled (log_queries=0)"
            return report

        time_column = _pick_time_column(columns)
        if not time_column:
            report["notes"].append("query_log missing time column")

        select_columns = ["query_duration_ms"]
        if time_column:
            select_columns.insert(0, time_column)
        for optional in (
            "query_kind",
            "query",
            "read_rows",
            "read_bytes",
            "result_rows",
            "result_bytes",
            "memory_usage",
            "exception",
        ):
            if optional in columns:
                select_columns.append(optional)

        filters = [f"query_duration_ms >= {int(threshold_ms)}"]
        if time_column:
            filters.append(
                f"{time_column} >= now() - INTERVAL {int(lookback_minutes)} MINUTE"
            )
        if "type" in columns:
            filters.append("type = 'QueryFinish'")
        if "is_initial_query" in columns:
            filters.append("is_initial_query = 1")

        where_clause = " AND ".join(filters)
        sql = "SELECT " + ", ".join(select_columns) + " FROM system.query_log"
        if where_clause:
            sql += " WHERE " + where_clause
        sql += " ORDER BY query_duration_ms DESC"
        sql += f" LIMIT {int(limit)}"

        result = client.query(sql)
        col_names = _column_names(result)
        rows = _result_rows(result)

        slow_queries = []
        for row in rows:
            entry = dict(zip(col_names, row))
            query_text = entry.get("query", "")
            slow_queries.append(
                {
                    "duration_ms": entry.get("query_duration_ms"),
                    "event_time": entry.get(time_column) if time_column else None,
                    "query_kind": entry.get("query_kind"),
                    "read_rows": entry.get("read_rows"),
                    "read_bytes": entry.get("read_bytes"),
                    "result_rows": entry.get("result_rows"),
                    "result_bytes": entry.get("result_bytes"),
                    "memory_usage": entry.get("memory_usage"),
                    "exception": entry.get("exception"),
                    "query": _truncate_query(str(query_text)),
                }
            )

        report["slow_queries"] = slow_queries
        report["status"] = "slow" if slow_queries else "ok"
        return report
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning("Failed to close ClickHouse client: %s", exc)


def format_perf_report(report: Dict[str, Any]) -> str:
    status = report.get("status", "unchecked")
    backend = report.get("db_backend") or "unknown"
    lines = [f"status: {status}", f"backend: {backend}"]

    db_error = report.get("db_error")
    if db_error:
        lines.append(f"db_error: {db_error}")

    lines.append(f"threshold_ms: {report.get('threshold_ms')}")
    lines.append(f"lookback_minutes: {report.get('lookback_minutes')}")
    lines.append(f"limit: {report.get('limit')}")

    log_settings = report.get("log_settings") or {}
    if log_settings:
        lines.append("log_settings:")
        for key in sorted(log_settings):
            lines.append(f"- {key}: {log_settings[key]}")

    notes = report.get("notes") or []
    if notes:
        lines.append("notes:")
        for note in notes:
            lines.append(f"- {note}")

    slow_queries = report.get("slow_queries") or []
    if slow_queries:
        lines.append("slow_queries:")
        for entry in slow_queries:
            duration = entry.get("duration_ms")
            event_time = entry.get("event_time")
            query_kind = entry.get("query_kind")
            query = entry.get("query")
            summary_parts = [
                str(duration) if duration is not None else "?",
                "ms",
            ]
            if event_time:
                summary_parts.append(str(event_time))
            if query_kind:
                summary_parts.append(str(query_kind))
            summary_parts.append(query or "")
            lines.append("- " + " ".join(summary_parts).strip())
    else:
        lines.append("slow_queries: none")

    return "\n".join(lines)


def register_commands(audit_subparsers: argparse._SubParsersAction) -> None:
    audit_perf = audit_subparsers.add_parser(
        "perf", help="Audit database query performance."
    )
    audit_perf.add_argument("--db", required=True, help="Database connection string.")
    audit_perf.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_THRESHOLD_MS,
        help=f"Slow query threshold in ms (default: {DEFAULT_THRESHOLD_MS}).",
    )
    audit_perf.add_argument(
        "--lookback",
        type=int,
        default=DEFAULT_LOOKBACK_MINUTES,
        help=f"Lookback in minutes (default: {DEFAULT_LOOKBACK_MINUTES}).",
    )
    audit_perf.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max queries to show (default: {DEFAULT_LIMIT}).",
    )
    audit_perf.set_defaults(func=_cmd_audit_perf)


def _cmd_audit_perf(ns: argparse.Namespace) -> int:
    logger = logging.getLogger(__name__)
    try:
        report = run_perf_audit(
            db_url=ns.db,
            threshold_ms=ns.threshold,
            lookback_minutes=ns.lookback,
            limit=ns.limit,
        )
        print(format_perf_report(report))
        return 0 if report.get("status") == "ok" else 1
    except Exception as e:
        logger.error(f"Performance audit failed: {e}")
        return 1
