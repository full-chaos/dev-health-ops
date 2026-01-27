from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
import logging
from typing import Any, Dict, List, Literal, Optional, Tuple, cast

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from ..models.filters import MetricFilter, SankeyContext
from ..models.schemas import SankeyLink, SankeyNode, SankeyResponse
from ..queries.client import clickhouse_client, query_dicts
from ..queries.sankey import (
    fetch_expense_abandoned,
    fetch_expense_counts,
    fetch_hotspot_rows,
    fetch_investment_flow_items,
    fetch_state_status_counts,
)
from ..queries.scopes import build_scope_filter_multi
from .filtering import resolve_repo_filter_ids, time_window

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SankeyDefinition:
    label: str
    description: str
    unit: str


SANKEY_DEFINITIONS: Dict[str, SankeyDefinition] = {
    "investment": SankeyDefinition(
        label="Investment flow",
        description=(
            "Where effort allocates across initiatives, areas, issue types, and work items."
        ),
        unit="items",
    ),
    "expense": SankeyDefinition(
        label="Investment expense",
        description="How planned effort converts into unplanned work, rework, and rewrites.",
        unit="items",
    ),
    "state": SankeyDefinition(
        label="State flow",
        description="Execution paths that reveal stalls, loops, and retry patterns.",
        unit="items",
    ),
    "hotspot": SankeyDefinition(
        label="Code hotspot flow",
        description="Where change concentrates from repos to files and change intent.",
        unit="changes",
    ),
}

MAX_INVESTMENT_ITEMS = 60
MAX_HOTSPOT_ROWS = 150


def _apply_window_to_filters(
    filters: MetricFilter,
    window_start: Optional[date],
    window_end: Optional[date],
) -> MetricFilter:
    if not window_start and not window_end:
        return filters
    payload = (
        filters.model_dump(mode="json")
        if hasattr(filters, "model_dump")
        else filters.dict()
    )
    time_payload = payload.get("time") or {}
    if window_start:
        time_payload["start_date"] = window_start
    if window_end:
        time_payload["end_date"] = window_end
    if window_start and window_end:
        delta_days = max(1, (window_end - window_start).days)
        time_payload["range_days"] = delta_days
    payload["time"] = time_payload
    return MetricFilter(**payload)


def _normalize_label(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def _add_edge(
    edges: Dict[Tuple[str, str], float],
    source: str,
    target: str,
    value: float,
) -> None:
    if value <= 0:
        return
    key = (source, target)
    edges[key] = edges.get(key, 0.0) + value


def _touch_node(
    nodes: Dict[str, SankeyNode],
    name: str,
    group: Optional[str],
) -> None:
    if name in nodes:
        return
    nodes[name] = SankeyNode(name=name, group=group)


def _links_from_edges(edges: Dict[Tuple[str, str], float]) -> List[SankeyLink]:
    links = [
        SankeyLink(source=source, target=target, value=value)
        for (source, target), value in edges.items()
        if value > 0
    ]
    links.sort(key=lambda link: link.value, reverse=True)
    return links


async def _tables_present(sink: BaseMetricsSink, tables: List[str]) -> bool:
    if not tables:
        return True
    try:
        rows = await query_dicts(
            sink,
            """
            SELECT name
            FROM system.tables
            WHERE database = currentDatabase()
                AND name IN %(tables)s
            """,
            {"tables": tables},
        )
    except Exception as exc:
        logger.warning("Sankey table lookup failed: %s", exc)
        return False
    present = {row.get("name") for row in rows}
    missing = [table for table in tables if table not in present]
    if missing:
        logger.info("Sankey tables missing: %s", ", ".join(missing))
        return False
    return True


async def _columns_present(
    sink: BaseMetricsSink,
    table: str,
    columns: List[str],
) -> bool:
    if not columns:
        return True
    try:
        rows = await query_dicts(
            sink,
            """
            SELECT name
            FROM system.columns
            WHERE database = currentDatabase()
                AND table = %(table)s
                AND name IN %(columns)s
            """,
            {"table": table, "columns": columns},
        )
    except Exception as exc:
        logger.warning("Sankey column lookup failed for %s: %s", table, exc)
        return False
    present = {row.get("name") for row in rows}
    missing = [column for column in columns if column not in present]
    if missing:
        logger.info("Sankey columns missing for %s: %s", table, ", ".join(missing))
        return False
    return True


async def _repo_scope_filter(
    sink: BaseMetricsSink,
    filters: MetricFilter,
    repo_column: str = "repo_id",
) -> Tuple[str, Dict[str, Any]]:
    repo_ids = await resolve_repo_filter_ids(sink, filters)
    if not repo_ids:
        return "", {}
    return build_scope_filter_multi("repo", repo_ids, repo_column=repo_column)


def _team_scope_filter(
    filters: MetricFilter,
    team_column: str = "team_id",
) -> Tuple[str, Dict[str, Any]]:
    if filters.scope.level != "team" or not filters.scope.ids:
        return "", {}
    return build_scope_filter_multi("team", filters.scope.ids, team_column=team_column)


def _category_theme_filters(filters: MetricFilter) -> List[str]:
    raw_categories = filters.why.work_category or []
    themes: List[str] = []
    for category in raw_categories:
        if not category:
            continue
        category_str = str(category).strip()
        if not category_str:
            continue
        if "." in category_str:
            theme = category_str.split(".", 1)[0]
        else:
            theme = category_str
        if theme:
            themes.append(theme)
    return list(dict.fromkeys(themes))


def _work_scope_filter(
    filters: MetricFilter,
    work_scope_column: str = "work_scope_id",
) -> Tuple[str, Dict[str, Any]]:
    scope_ids: List[str] = []
    if filters.scope.level == "repo":
        scope_ids.extend(filters.scope.ids)
    if filters.what.repos:
        scope_ids.extend(filters.what.repos)
    scope_ids = [scope_id for scope_id in scope_ids if scope_id]
    if not scope_ids:
        return "", {}
    return f" AND {work_scope_column} IN %(work_scope_ids)s", {
        "work_scope_ids": scope_ids
    }


async def _build_investment_flow(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    if sink.backend_type == "clickhouse":
        if not await _tables_present(sink, ["work_unit_investments"]):
            return [], []
        if not await _columns_present(
            sink,
            "work_unit_investments",
            [
                "theme_distribution_json",
                "effort_value",
                "from_ts",
                "to_ts",
                "repo_id",
            ],
        ):
            return [], []

    repo_filter, repo_params = await _repo_scope_filter(
        sink, filters, repo_column="repo_id"
    )
    theme_filters = _category_theme_filters(filters)
    category_filter = " AND theme_kv.1 IN %(themes)s" if theme_filters else ""
    scope_filter = f"{repo_filter}{category_filter}"
    scope_params = {**repo_params}
    if theme_filters:
        scope_params["themes"] = theme_filters
    window_start = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    window_end = datetime.combine(end_day, time.min, tzinfo=timezone.utc)
    rows = await fetch_investment_flow_items(
        sink,
        start_ts=window_start,
        end_ts=window_end,
        scope_filter=scope_filter,
        scope_params=scope_params,
        limit=MAX_INVESTMENT_ITEMS,
    )
    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}
    for row in rows:
        value = float(row.get("value") or 0.0)
        if value <= 0:
            continue
        source = _normalize_label(row.get("source"), "Unassigned")
        target = _normalize_label(row.get("target"), "Other")

        _touch_node(nodes, source, "initiative")
        _touch_node(nodes, target, "project")

        _add_edge(edges, source, target, value)

    return list(nodes.values()), _links_from_edges(edges)


async def _build_expense_flow(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    if sink.backend_type == "clickhouse":
        if not await _tables_present(
            sink, ["work_item_metrics_daily", "work_item_cycle_times"]
        ):
            return [], []
        if not await _columns_present(
            sink,
            "work_item_metrics_daily",
            [
                "day",
                "new_items_count",
                "new_bugs_count",
                "items_completed",
                "bug_completed_ratio",
                "team_id",
                "work_scope_id",
            ],
        ):
            return [], []
        if not await _columns_present(
            sink,
            "work_item_cycle_times",
            ["day", "status", "team_id", "work_scope_id"],
        ):
            return [], []

    team_filter, team_params = _team_scope_filter(
        filters, team_column="ifNull(nullIf(team_id, ''), 'unassigned')"
    )
    work_scope_filter, work_scope_params = _work_scope_filter(
        filters, work_scope_column="work_scope_id"
    )
    scope_filter = f"{team_filter}{work_scope_filter}"
    scope_params = {**team_params, **work_scope_params}
    rows = await fetch_expense_counts(
        sink,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
    )
    if not rows:
        return [], []
    row = rows[0]
    new_bugs = float(row.get("new_bugs") or 0.0)
    bug_completed = float(row.get("bug_completed_estimate") or 0.0)

    abandoned_rows = await fetch_expense_abandoned(
        sink,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
    )
    canceled_items = 0.0
    if abandoned_rows:
        canceled_items = float(abandoned_rows[0].get("canceled_items") or 0.0)

    unplanned = max(0.0, new_bugs)
    rework = max(0.0, min(unplanned, bug_completed))
    abandoned = max(0.0, min(rework, canceled_items))

    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}

    _touch_node(nodes, "Planned work", "planned")
    _touch_node(nodes, "Unplanned work", "unplanned")
    _touch_node(nodes, "Rework", "rework")
    _touch_node(nodes, "Abandonment / rewrite", "abandonment")

    _add_edge(edges, "Planned work", "Unplanned work", unplanned)
    _add_edge(edges, "Unplanned work", "Rework", rework)
    _add_edge(edges, "Rework", "Abandonment / rewrite", abandoned)

    return list(nodes.values()), _links_from_edges(edges)


async def _build_state_flow(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    if sink.backend_type == "clickhouse":
        if not await _tables_present(sink, ["work_item_state_durations_daily"]):
            return [], []
        if not await _columns_present(
            sink,
            "work_item_state_durations_daily",
            ["day", "status", "items_touched", "team_id", "work_scope_id"],
        ):
            return [], []

    team_filter, team_params = _team_scope_filter(
        filters, team_column="ifNull(nullIf(team_id, ''), 'unassigned')"
    )
    work_scope_filter, work_scope_params = _work_scope_filter(
        filters, work_scope_column="work_scope_id"
    )
    scope_filter = f"{team_filter}{work_scope_filter}"
    scope_params = {**team_params, **work_scope_params}
    rows = await fetch_state_status_counts(
        sink,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
    )
    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}

    status_counts: Dict[str, float] = {}
    for row in rows:
        value = float(row.get("items_touched") or 0.0)
        if value <= 0:
            continue
        status_raw = _normalize_label(row.get("status"), "unknown").lower()
        status_counts[status_raw] = status_counts.get(status_raw, 0.0) + value

    status_labels = {
        "backlog": "Backlog",
        "todo": "Todo",
        "in_progress": "In Progress",
        "in_review": "In Review",
        "blocked": "Blocked",
        "done": "Done",
        "canceled": "Canceled",
        "unknown": "Unknown",
    }

    def _label(status: str) -> str:
        return status_labels.get(status, status.replace("_", " ").title())

    backlog = status_counts.get("backlog", 0.0)
    todo = status_counts.get("todo", 0.0)
    in_progress = status_counts.get("in_progress", 0.0)
    in_review = status_counts.get("in_review", 0.0)
    done = status_counts.get("done", 0.0)
    blocked = status_counts.get("blocked", 0.0)
    canceled = status_counts.get("canceled", 0.0)

    flow_backlog = min(backlog, todo)
    flow_todo = min(todo, in_progress)
    blocked_flow = min(in_progress, blocked)
    remaining = max(0.0, in_progress - blocked_flow)
    review_flow = min(remaining, in_review)
    remaining = max(0.0, remaining - review_flow)
    canceled_flow = min(remaining, canceled)
    done_flow = min(review_flow, done)

    for status in status_counts:
        _touch_node(nodes, _label(status), "state")

    _add_edge(edges, _label("backlog"), _label("todo"), flow_backlog)
    _add_edge(edges, _label("todo"), _label("in_progress"), flow_todo)
    _add_edge(edges, _label("in_progress"), _label("blocked"), blocked_flow)
    _add_edge(edges, _label("in_progress"), _label("in_review"), review_flow)
    _add_edge(edges, _label("in_progress"), _label("canceled"), canceled_flow)
    _add_edge(edges, _label("in_review"), _label("done"), done_flow)

    return list(nodes.values()), _links_from_edges(edges)


async def _build_hotspot_flow(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    if sink.backend_type == "clickhouse":
        if not await _tables_present(sink, ["file_metrics_daily", "repos"]):
            return [], []
        if not await _columns_present(
            sink,
            "file_metrics_daily",
            ["repo_id", "day", "path", "churn"],
        ):
            return [], []
        if not await _columns_present(sink, "repos", ["id", "repo"]):
            return [], []

    scope_filter, scope_params = await _repo_scope_filter(
        sink, filters, repo_column="metrics.repo_id"
    )
    rows = await fetch_hotspot_rows(
        sink,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
        limit=MAX_HOTSPOT_ROWS,
    )
    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}
    for row in rows:
        churn = float(row.get("churn") or 0.0)
        if churn <= 0:
            continue
        repo = _normalize_label(row.get("repo"), "Unknown repo")
        directory = _normalize_label(row.get("directory"), "(root)")
        file_path = _normalize_label(row.get("file_path"), "unknown file")
        change_type = _normalize_label(row.get("change_type"), "feature")

        directory_label = f"{repo} / {directory}"
        file_label = f"{repo} / {file_path}"

        _touch_node(nodes, repo, "repo")
        _touch_node(nodes, directory_label, "directory")
        _touch_node(nodes, file_label, "file")
        _touch_node(nodes, change_type, "change_type")

        _add_edge(edges, repo, directory_label, churn)
        _add_edge(edges, directory_label, file_label, churn)
        _add_edge(edges, file_label, change_type, churn)

    return list(nodes.values()), _links_from_edges(edges)


async def build_sankey_response(
    *,
    db_url: str,
    mode: str,
    filters: MetricFilter,
    context: Optional[SankeyContext] = None,
    window_start: Optional[date] = None,
    window_end: Optional[date] = None,
) -> SankeyResponse:
    if mode == "hotpot":
        mode = "hotspot"
    definition = SANKEY_DEFINITIONS.get(mode)
    if definition is None:
        raise ValueError(f"Unknown sankey mode: {mode}")

    resolved_filters = _apply_window_to_filters(filters, window_start, window_end)
    start_day, end_day, _, _ = time_window(resolved_filters)

    async with clickhouse_client(db_url) as sink:
        if mode == "investment":
            nodes, links = await _build_investment_flow(
                sink,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        elif mode == "expense":
            nodes, links = await _build_expense_flow(
                sink,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        elif mode == "state":
            nodes, links = await _build_state_flow(
                sink,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        elif mode == "hotspot":
            nodes, links = await _build_hotspot_flow(
                sink,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        else:
            nodes, links = [], []

    return SankeyResponse(
        mode=cast(Literal["investment", "expense", "state", "hotspot"], mode),
        nodes=nodes,
        links=links,
        unit=definition.unit,
        label=definition.label,
        description=definition.description,
    )
