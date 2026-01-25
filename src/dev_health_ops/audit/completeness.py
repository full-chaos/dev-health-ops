from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

REQUIRED_PROVIDERS = ("jira", "github", "gitlab")
AUDIT_PROVIDERS = ("jira", "github", "gitlab", "synthetic")
SOURCE_KEYS = ("github", "gitlab", "unknown")
REQUIRED_GIT_TABLES = ("git_commits", "git_pull_requests")
OPTIONAL_GIT_TABLES = ("deployments", "incidents", "ci_pipeline_runs")
ALL_GIT_TABLES = REQUIRED_GIT_TABLES + OPTIONAL_GIT_TABLES


def build_window(
    days: int, now: Optional[datetime] = None
) -> Tuple[datetime, datetime]:
    window_days = max(1, int(days))
    end = now or datetime.now(timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    start = end - timedelta(days=window_days)
    return start, end


def build_work_items_query() -> str:
    return """
    SELECT
      provider,
      countIf(updated_at >= {start:DateTime} AND updated_at < {end:DateTime}) AS count,
      max(last_synced) AS last_synced
    FROM work_items
    GROUP BY provider
    """


def build_transitions_query() -> str:
    return """
    SELECT
      provider,
      countIf(occurred_at >= {start:DateTime} AND occurred_at < {end:DateTime}) AS count,
      max(last_synced) AS last_synced
    FROM work_item_transitions
    GROUP BY provider
    """


def build_git_commits_query() -> str:
    return """
    SELECT
      repo_id,
      countIf(committer_when >= {start:DateTime} AND committer_when < {end:DateTime}) AS count,
      max(last_synced) AS last_synced
    FROM git_commits
    GROUP BY repo_id
    """


def build_git_pull_requests_query() -> str:
    return """
    SELECT
      repo_id,
      countIf(
        (created_at >= {start:DateTime} AND created_at < {end:DateTime})
        OR (merged_at IS NOT NULL AND merged_at >= {start:DateTime} AND merged_at < {end:DateTime})
      ) AS count,
      max(last_synced) AS last_synced
    FROM git_pull_requests
    GROUP BY repo_id
    """


def build_deployments_query() -> str:
    return """
    SELECT
      repo_id,
      countIf(
        coalesce(deployed_at, started_at) >= {start:DateTime}
        AND coalesce(deployed_at, started_at) < {end:DateTime}
      ) AS count,
      max(last_synced) AS last_synced
    FROM deployments
    GROUP BY repo_id
    """


def build_incidents_query() -> str:
    return """
    SELECT
      repo_id,
      countIf(started_at >= {start:DateTime} AND started_at < {end:DateTime}) AS count,
      max(last_synced) AS last_synced
    FROM incidents
    GROUP BY repo_id
    """


def build_ci_pipeline_runs_query() -> str:
    return """
    SELECT
      repo_id,
      countIf(started_at >= {start:DateTime} AND started_at < {end:DateTime}) AS count,
      max(last_synced) AS last_synced
    FROM ci_pipeline_runs
    GROUP BY repo_id
    """


def _safe_json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return None


def infer_repo_source(settings_value: Any, tags_value: Any) -> str:
    settings = _safe_json_loads(settings_value) or {}
    source = str((settings or {}).get("source") or "").strip().lower()
    if source in {"github", "gitlab"}:
        return source

    tags = _safe_json_loads(tags_value) or []
    if isinstance(tags, list):
        for tag in tags:
            candidate = str(tag).strip().lower()
            if candidate in {"github", "gitlab"}:
                return candidate

    return "unknown"


def build_repo_source_map(repo_rows: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for row in repo_rows:
        repo_id = row.get("id")
        if not repo_id:
            continue
        mapping[str(repo_id)] = infer_repo_source(row.get("settings"), row.get("tags"))
    return mapping


def _naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _coerce_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _max_dt(first: Optional[datetime], second: Any) -> Optional[datetime]:
    second_dt = _coerce_dt(second)
    if first is None:
        return second_dt
    if second_dt is None:
        return first
    return max(first, second_dt)


def _build_stat(count: Any, last_synced: Any, window_start: datetime) -> Dict[str, Any]:
    count_value = int(count or 0)
    last_synced_dt = _coerce_dt(last_synced)
    stale = last_synced_dt is None or last_synced_dt < window_start
    ok = count_value > 0 and not stale
    return {
        "count": count_value,
        "last_synced": last_synced_dt,
        "stale": stale,
        "ok": ok,
    }


def _empty_stat(window_start: datetime) -> Dict[str, Any]:
    return _build_stat(0, None, window_start)


def _build_stats_by_provider(
    rows: Iterable[Dict[str, Any]], window_start: datetime
) -> Dict[str, Dict[str, Any]]:
    stats: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        provider = str(row.get("provider") or "").strip().lower()
        if not provider:
            continue
        stats[provider] = _build_stat(
            row.get("count"), row.get("last_synced"), window_start
        )
    return stats


def _aggregate_git_rows_by_source(
    rows: Iterable[Dict[str, Any]],
    repo_sources: Dict[str, str],
    window_start: datetime,
) -> Dict[str, Dict[str, Any]]:
    buckets = {source: {"count": 0, "last_synced": None} for source in SOURCE_KEYS}
    for row in rows:
        repo_id = row.get("repo_id")
        if not repo_id:
            continue
        source = repo_sources.get(str(repo_id), "unknown")
        if source not in buckets:
            source = "unknown"
        bucket = buckets[source]
        bucket["count"] += int(row.get("count") or 0)
        bucket["last_synced"] = _max_dt(bucket["last_synced"], row.get("last_synced"))
    return {
        source: _build_stat(values["count"], values["last_synced"], window_start)
        for source, values in buckets.items()
    }


def _stat_ok(stat: Optional[Dict[str, Any]]) -> bool:
    return bool(stat and stat.get("ok"))


def _stat_issues(stat: Optional[Dict[str, Any]], label: str) -> List[str]:
    if stat is None:
        return [f"{label}_table_missing"]
    issues: List[str] = []
    if stat.get("count", 0) <= 0:
        issues.append(f"{label}_missing")
    if stat.get("stale"):
        issues.append(f"{label}_stale")
    return issues


def compile_report(
    *,
    window_start: datetime,
    window_end: datetime,
    window_days: int,
    work_item_rows: Iterable[Dict[str, Any]],
    transition_rows: Iterable[Dict[str, Any]],
    repo_rows: Iterable[Dict[str, Any]],
    git_rows_by_table: Dict[str, Iterable[Dict[str, Any]]],
    present_tables: Dict[str, bool],
) -> Dict[str, Any]:
    repo_sources = build_repo_source_map(repo_rows)
    work_items = _build_stats_by_provider(work_item_rows, window_start)
    transitions = _build_stats_by_provider(transition_rows, window_start)

    git_sources: Dict[str, Dict[str, Optional[Dict[str, Any]]]] = {
        source: {} for source in SOURCE_KEYS
    }
    for table in ALL_GIT_TABLES:
        if not present_tables.get(table, False):
            for source in SOURCE_KEYS:
                git_sources[source][table] = None
            continue
        rows = git_rows_by_table.get(table, [])
        by_source = _aggregate_git_rows_by_source(rows, repo_sources, window_start)
        for source in SOURCE_KEYS:
            git_sources[source][table] = by_source.get(
                source, _empty_stat(window_start)
            )

    providers: Dict[str, Dict[str, Any]] = {}
    for provider in AUDIT_PROVIDERS:
        wi_stat = work_items.get(provider, _empty_stat(window_start))
        tr_stat = transitions.get(provider, _empty_stat(window_start))
        issues: List[str] = []
        wi_ok = _stat_ok(wi_stat)
        tr_ok = _stat_ok(tr_stat)
        if not wi_ok:
            issues.extend(_stat_issues(wi_stat, "work_items"))
        if not tr_ok:
            issues.extend(_stat_issues(tr_stat, "transitions"))

        git_facts = None
        git_ok = True
        if provider in {"github", "gitlab"}:
            git_facts = git_sources.get(provider, {})
            commit_stat = (
                git_facts.get("git_commits")
                if present_tables.get("git_commits", False)
                else None
            )
            pr_stat = (
                git_facts.get("git_pull_requests")
                if present_tables.get("git_pull_requests", False)
                else None
            )
            commit_ok = _stat_ok(commit_stat)
            pr_ok = _stat_ok(pr_stat)
            git_ok = commit_ok or pr_ok
            if not git_ok:
                issues.extend(_stat_issues(commit_stat, "git_commits"))
                issues.extend(_stat_issues(pr_stat, "git_pull_requests"))
                issues.append("git_activity_missing")

        provider_ok = wi_ok and tr_ok and (git_ok if provider != "jira" else True)
        providers[provider] = {
            "work_items": wi_stat,
            "transitions": tr_stat,
            "git_facts": git_facts,
            "ok": provider_ok,
            "issues": issues,
        }

    overall_ok = all(providers[p]["ok"] for p in REQUIRED_PROVIDERS)

    missing_tables = [table for table, present in present_tables.items() if not present]

    return {
        "window": {"days": window_days, "start": window_start, "end": window_end},
        "providers": providers,
        "git_sources": git_sources,
        "missing_tables": missing_tables,
        "overall_ok": overall_ok,
    }


def _query_dicts(
    client: Any, query: str, parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _fetch_table_presence(client: Any, tables: Iterable[str]) -> Dict[str, bool]:
    table_list = list(tables)
    if not table_list:
        return {}
    rows = _query_dicts(
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


def run_completeness_audit(*, db_url: str, days: int) -> Dict[str, Any]:
    window_start, window_end = build_window(days)
    params = {
        "start": _naive_utc(window_start),
        "end": _naive_utc(window_end),
    }

    sink = ClickHouseMetricsSink(db_url)
    client = sink.client
    try:
        tables = {
            "repos",
            "work_items",
            "work_item_transitions",
            *ALL_GIT_TABLES,
        }
        present_tables = _fetch_table_presence(client, tables)

        repo_rows: List[Dict[str, Any]] = []
        if present_tables.get("repos", False):
            repo_rows = _query_dicts(
                client,
                "SELECT id, settings, tags FROM repos",
                {},
            )

        work_item_rows: List[Dict[str, Any]] = []
        if present_tables.get("work_items", False):
            work_item_rows = _query_dicts(client, build_work_items_query(), params)

        transition_rows: List[Dict[str, Any]] = []
        if present_tables.get("work_item_transitions", False):
            transition_rows = _query_dicts(client, build_transitions_query(), params)

        git_rows_by_table: Dict[str, Iterable[Dict[str, Any]]] = {}
        if present_tables.get("git_commits", False):
            git_rows_by_table["git_commits"] = _query_dicts(
                client, build_git_commits_query(), params
            )
        if present_tables.get("git_pull_requests", False):
            git_rows_by_table["git_pull_requests"] = _query_dicts(
                client, build_git_pull_requests_query(), params
            )
        if present_tables.get("deployments", False):
            git_rows_by_table["deployments"] = _query_dicts(
                client, build_deployments_query(), params
            )
        if present_tables.get("incidents", False):
            git_rows_by_table["incidents"] = _query_dicts(
                client, build_incidents_query(), params
            )
        if present_tables.get("ci_pipeline_runs", False):
            git_rows_by_table["ci_pipeline_runs"] = _query_dicts(
                client, build_ci_pipeline_runs_query(), params
            )

        return compile_report(
            window_start=window_start,
            window_end=window_end,
            window_days=max(1, int(days)),
            work_item_rows=work_item_rows,
            transition_rows=transition_rows,
            repo_rows=repo_rows,
            git_rows_by_table=git_rows_by_table,
            present_tables=present_tables,
        )
    finally:
        sink.close()


def _format_dt(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _render_table(headers: List[str], rows: List[List[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _row(cells: List[str]) -> str:
        padded = [cell.ljust(widths[idx]) for idx, cell in enumerate(cells)]
        return f"| {' | '.join(padded)} |"

    lines = [_row(headers)]
    lines.append(_row(["-" * w for w in widths]))
    for row in rows:
        lines.append(_row(row))
    return "\n".join(lines)


def format_completeness_table(report: Dict[str, Any]) -> str:
    window = report.get("window") or {}
    days = window.get("days")
    start = _format_dt(window.get("start"))
    end = _format_dt(window.get("end"))

    providers = report.get("providers", {})
    provider_rows: List[List[str]] = []
    for provider in AUDIT_PROVIDERS:
        entry = providers.get(provider, {})
        work_items = entry.get("work_items") or {}
        transitions = entry.get("transitions") or {}
        issues = entry.get("issues") or []
        provider_rows.append(
            [
                provider,
                str(work_items.get("count", 0)),
                _format_dt(work_items.get("last_synced")),
                str(transitions.get("count", 0)),
                _format_dt(transitions.get("last_synced")),
                "ok" if entry.get("ok") else "missing",
                ", ".join(issues) if issues else "-",
            ]
        )

    table_sections = [
        f"Completeness window: last {days} days ({start} to {end})",
        _render_table(
            [
                "provider",
                "work_items",
                "work_items_last_synced",
                "transitions",
                "transitions_last_synced",
                "status",
                "issues",
            ],
            provider_rows,
        ),
    ]

    git_sources = report.get("git_sources", {})
    git_rows: List[List[str]] = []
    for source in SOURCE_KEYS:
        source_entry = git_sources.get(source, {})

        def _cell(table: str) -> str:
            stat = source_entry.get(table)
            if stat is None:
                return "n/a"
            return f"{stat.get('count', 0)} ({_format_dt(stat.get('last_synced'))})"

        commit_ok = _stat_ok(source_entry.get("git_commits"))
        pr_ok = _stat_ok(source_entry.get("git_pull_requests"))
        git_status = "ok" if (commit_ok or pr_ok) else "missing"
        git_rows.append(
            [
                source,
                _cell("git_commits"),
                _cell("git_pull_requests"),
                _cell("deployments"),
                _cell("incidents"),
                _cell("ci_pipeline_runs"),
                git_status,
            ]
        )

    table_sections.extend(
        [
            "",
            _render_table(
                [
                    "source",
                    "commits",
                    "pull_requests",
                    "deployments",
                    "incidents",
                    "ci_pipeline_runs",
                    "status",
                ],
                git_rows,
            ),
        ]
    )

    overall = "ok" if report.get("overall_ok") else "missing"
    table_sections.append("")
    table_sections.append(f"Overall: {overall}")
    return "\n".join(table_sections)


def _serialize_report(value: Any) -> Any:
    if isinstance(value, datetime):
        return _format_dt(value)
    if isinstance(value, dict):
        return {key: _serialize_report(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_serialize_report(val) for val in value]
    return value


def format_completeness_json(report: Dict[str, Any]) -> str:
    return json.dumps(_serialize_report(report), indent=2, sort_keys=True)


def completeness_failed(report: Dict[str, Any]) -> bool:
    return not bool(report.get("overall_ok"))


def register_commands(audit_subparsers: argparse._SubParsersAction) -> None:
    audit_completeness = audit_subparsers.add_parser(
        "completeness", help="Audit data completeness and freshness."
    )
    audit_completeness.add_argument(
        "--db", required=True, help="Database connection string."
    )
    audit_completeness.add_argument(
        "--days", type=int, default=7, help="Window in days (default: 7)."
    )
    audit_completeness.add_argument(
        "--format", choices=["table", "json"], default="table", help="Output format."
    )
    audit_completeness.set_defaults(func=_cmd_audit_completeness)


def _cmd_audit_completeness(ns: argparse.Namespace) -> int:
    import logging

    logger = logging.getLogger(__name__)
    try:
        report = run_completeness_audit(db_url=ns.db, days=ns.days)
        if ns.format == "json":
            print(format_completeness_json(report))
        else:
            print(format_completeness_table(report))
        return 0
    except Exception as e:
        logger.error(f"Completeness audit failed: {e}")
        return 1
