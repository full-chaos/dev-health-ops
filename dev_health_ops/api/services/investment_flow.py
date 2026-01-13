from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Dict, List, Optional

from ..models.filters import MetricFilter
from ..models.schemas import SankeyLink, SankeyNode, SankeyResponse
from ..queries.client import clickhouse_client
from ..queries.investment import (
    fetch_investment_subcategory_edges,
    fetch_investment_team_edges,
)
from ..queries.scopes import build_scope_filter_multi
from .filtering import resolve_repo_filter_ids, time_window
from .investment import _columns_present, _split_category_filters, _tables_present


def _title_case(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title()


def _format_subcategory_label(subcategory_key: str) -> str:
    if "." not in subcategory_key:
        return _title_case(subcategory_key)
    theme, sub = subcategory_key.split(".", 1)
    return f"{_title_case(theme)} · {_title_case(sub)}"


async def build_investment_flow_response(
    *,
    db_url: str,
    filters: MetricFilter,
    theme: Optional[str] = None,
) -> SankeyResponse:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)

    theme_filters, subcategory_filters = _split_category_filters(filters)
    if theme:
        theme_filters = [theme]

    async with clickhouse_client(db_url) as client:
        if not await _tables_present(client, ["work_unit_investments"]):
            return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)

        # Check for required columns
        required_cols = [
            "from_ts",
            "to_ts",
            "repo_id",
            "effort_value",
            "subcategory_distribution_json",
        ]
        if not await _columns_present(client, "work_unit_investments", required_cols):
            return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)

        scope_filter, scope_params = "", {}
        if filters.scope.level in {"team", "repo"}:
            repo_ids = await resolve_repo_filter_ids(client, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )

        # 1. Fetch both sets of edges
        repo_rows = await fetch_investment_subcategory_edges(
            client,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )

        team_rows = await fetch_investment_team_edges(
            client,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )

    # 2. Calculate stats
    def get_stats(rows):
        total_val = sum(row["value"] for row in rows)
        if total_val == 0:
            return 0.0, 0
        assigned_val = sum(
            row["value"] for row in rows if row["target"] != "unassigned"
        )
        distinct_targets = len({
            row["target"] for row in rows if row["target"] != "unassigned"
        })
        return assigned_val / total_val, distinct_targets

    team_coverage, distinct_team_targets = get_stats(team_rows)
    repo_coverage, distinct_repo_targets = get_stats(repo_rows)

    # 3. Decision Logic
    # Thresholds: Prefer Team if cov >= 0.7 and targets >= 2. Else Repo if cov >= 0.7 and targets >= 2.
    rows_to_use = []

    if distinct_team_targets >= 2 and team_coverage >= 0.70:
        chosen_mode = "team"
        rows_to_use = team_rows
    elif distinct_repo_targets >= 2 and repo_coverage >= 0.70:
        chosen_mode = "repo_scope"
        rows_to_use = repo_rows
    else:
        # Fallback to source distribution only if neither qualifies
        chosen_mode = "fallback"
        rows_to_use = repo_rows  # Use repo rows to get source distribution

    nodes_by_name: Dict[str, SankeyNode] = {}
    links: List[SankeyLink] = []

    for row in rows_to_use:
        source_key = str(row.get("source") or "")
        target = str(row.get("target") or "")
        value = float(row.get("value") or 0.0)
        if not source_key or not target or value <= 0:
            continue

        source_label = _format_subcategory_label(source_key)
        if source_label not in nodes_by_name:
            nodes_by_name[source_label] = SankeyNode(
                name=source_label, group="subcategory", value=0.0
            )
        nodes_by_name[source_label].value = (
            nodes_by_name[source_label].value or 0.0
        ) + value

        if chosen_mode != "fallback":
            target_group = "team" if chosen_mode == "team" else "repo"
            if target not in nodes_by_name:
                nodes_by_name[target] = SankeyNode(
                    name=target, group=target_group, value=0.0
                )
            nodes_by_name[target].value = (nodes_by_name[target].value or 0.0) + value
            links.append(SankeyLink(source=source_label, target=target, value=value))

    label = "Investment allocation"
    if chosen_mode == "team":
        label = "Subcategory → Team"
    elif chosen_mode == "repo_scope":
        label = "Subcategory → Repo scope"

    return SankeyResponse(
        mode="investment",
        nodes=list(nodes_by_name.values()),
        links=links,
        unit=None,
        label=label,
        description="Dynamic allocation target based on coverage metrics.",
        team_coverage=team_coverage,
        repo_coverage=repo_coverage,
        distinct_team_targets=distinct_team_targets,
        distinct_repo_targets=distinct_repo_targets,
        chosen_mode=chosen_mode,
    )
