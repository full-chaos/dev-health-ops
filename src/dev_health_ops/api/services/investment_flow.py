from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Dict, List, Optional

from ..models.filters import MetricFilter
from ..models.schemas import SankeyLink, SankeyNode, SankeyResponse
from ..queries.client import clickhouse_client
from ..queries.investment import (
    fetch_investment_repo_team_edges,
    fetch_investment_subcategory_edges,
    fetch_investment_team_edges,
    fetch_investment_team_category_repo_edges,
    fetch_investment_team_subcategory_repo_edges,
    fetch_investment_unassigned_counts,
)
from ..queries.scopes import build_scope_filter_multi
from .filtering import resolve_repo_filter_ids, time_window
from .investment import _columns_present, _split_category_filters, _tables_present
from dev_health_ops.investment_taxonomy import THEMES


def _title_case(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title()


def _format_subcategory_label(subcategory_key: str) -> str:
    if "." not in subcategory_key:
        return _title_case(subcategory_key)
    theme, sub = subcategory_key.split(".", 1)
    return f"{_title_case(theme)} · {_title_case(sub)}"


THEME_LABELS = {
    "feature_delivery": "Feature Delivery",
    "operational": "Operational / Support",
    "maintenance": "Maintenance / Tech Debt",
    "quality": "Quality / Reliability",
    "risk": "Risk / Security",
}
THEME_KEYS_BY_LABEL = {label.lower(): key for key, label in THEME_LABELS.items()}

UNASSIGNED_TEAM = "unassigned"
UNASSIGNED_REPO = "unassigned"
UNASSIGNED_TEAM_LABEL = "Unassigned team"
UNASSIGNED_REPO_LABEL = "Unassigned repo"
OTHER_REPOS_LABEL = "Other repos"


def _format_theme_label(theme_key: str) -> str:
    key = str(theme_key or "").strip().lower()
    if key in THEME_LABELS:
        return THEME_LABELS[key]
    return _title_case(theme_key)


def _normalize_theme_key(theme_key: Optional[str]) -> Optional[str]:
    if theme_key is None:
        return None
    raw = str(theme_key).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered in THEMES:
        return lowered
    if lowered in THEME_KEYS_BY_LABEL:
        return THEME_KEYS_BY_LABEL[lowered]
    return None


def _get_repo_rollup_map(
    rows: List[Dict[str, object]], top_n_repos: int
) -> Dict[str, str]:
    repo_totals: Dict[str, float] = {}
    for row in rows:
        repo = str(row.get("repo") or UNASSIGNED_REPO)
        if repo == UNASSIGNED_REPO:
            continue
        value = float(row.get("value") or 0.0)
        if value <= 0:
            continue
        repo_totals[repo] = repo_totals.get(repo, 0.0) + value
    top_repos = {
        repo
        for repo, _ in sorted(
            repo_totals.items(), key=lambda item: item[1], reverse=True
        )[: max(1, top_n_repos)]
    }
    rollup_map: Dict[str, str] = {}
    for repo in repo_totals:
        rollup_map[repo] = repo if repo in top_repos else OTHER_REPOS_LABEL
    return rollup_map


def _build_team_burden_sankey(
    rows: List[Dict[str, object]],
    *,
    category_key: str,
    category_label_fn,
    top_n_repos: int,
) -> tuple[List[SankeyNode], List[SankeyLink]]:
    nodes_by_name: Dict[str, SankeyNode] = {}
    link_totals: Dict[tuple[str, str], float] = {}
    incoming: Dict[str, float] = {}
    outgoing: Dict[str, float] = {}

    def add_node(name: str, group: str) -> None:
        if name not in nodes_by_name:
            nodes_by_name[name] = SankeyNode(name=name, group=group, value=0.0)

    rollup_map = _get_repo_rollup_map(rows, top_n_repos)

    for row in rows:
        team_raw = str(row.get("team") or UNASSIGNED_TEAM)
        team_label = UNASSIGNED_TEAM_LABEL if team_raw == UNASSIGNED_TEAM else team_raw
        category_raw = str(row.get(category_key) or "")
        repo_raw = str(row.get("repo") or UNASSIGNED_REPO)
        value = float(row.get("value") or 0.0)
        if not category_raw or value <= 0:
            continue

        repo_label = (
            UNASSIGNED_REPO_LABEL
            if repo_raw == UNASSIGNED_REPO
            else rollup_map.get(repo_raw, repo_raw)
        )
        category_label = category_label_fn(category_raw)

        add_node(team_label, "team")
        add_node(
            category_label, "category" if category_key == "category" else "subcategory"
        )
        add_node(repo_label, "repo")

        link_totals[(team_label, category_label)] = (
            link_totals.get((team_label, category_label), 0.0) + value
        )
        link_totals[(category_label, repo_label)] = (
            link_totals.get((category_label, repo_label), 0.0) + value
        )

    links: List[SankeyLink] = []
    for (source, target), value in link_totals.items():
        if value <= 0:
            continue
        links.append(SankeyLink(source=source, target=target, value=value))
        outgoing[source] = outgoing.get(source, 0.0) + value
        incoming[target] = incoming.get(target, 0.0) + value

    for name, node in nodes_by_name.items():
        node.value = max(incoming.get(name, 0.0), outgoing.get(name, 0.0))

    return list(nodes_by_name.values()), links


def _build_team_theme_subcategory_repo_sankey(
    rows: List[Dict[str, object]],
    *,
    top_n_repos: int,
) -> tuple[List[SankeyNode], List[SankeyLink]]:
    nodes_by_name: Dict[str, SankeyNode] = {}
    link_totals: Dict[tuple[str, str], float] = {}
    incoming: Dict[str, float] = {}
    outgoing: Dict[str, float] = {}

    def add_node(name: str, group: str) -> None:
        if name not in nodes_by_name:
            nodes_by_name[name] = SankeyNode(name=name, group=group, value=0.0)

    rollup_map = _get_repo_rollup_map(rows, top_n_repos)

    for row in rows:
        team_raw = str(row.get("team") or UNASSIGNED_TEAM)
        team_label = UNASSIGNED_TEAM_LABEL if team_raw == UNASSIGNED_TEAM else team_raw
        subcategory_raw = str(row.get("subcategory") or "")
        repo_raw = str(row.get("repo") or UNASSIGNED_REPO)
        value = float(row.get("value") or 0.0)

        if not subcategory_raw or value <= 0:
            continue

        # Parse Theme and Subcategory from "theme.subcategory"
        if "." in subcategory_raw:
            theme_key, sub_key = subcategory_raw.split(".", 1)
            theme_label = _format_theme_label(theme_key)
            subcategory_label = _title_case(sub_key)
        else:
            theme_label = _title_case(subcategory_raw)  # Fallback if no dot
            subcategory_label = (
                subcategory_raw  # Should ideally not happen if data is strict
            )

        repo_label = (
            UNASSIGNED_REPO_LABEL
            if repo_raw == UNASSIGNED_REPO
            else rollup_map.get(repo_raw, repo_raw)
        )

        add_node(team_label, "team")
        add_node(theme_label, "category")
        add_node(subcategory_label, "subcategory")
        add_node(repo_label, "repo")

        # Team -> Theme
        link_totals[(team_label, theme_label)] = (
            link_totals.get((team_label, theme_label), 0.0) + value
        )
        # Theme -> Subcategory
        link_totals[(theme_label, subcategory_label)] = (
            link_totals.get((theme_label, subcategory_label), 0.0) + value
        )
        # Subcategory -> Repo
        link_totals[(subcategory_label, repo_label)] = (
            link_totals.get((subcategory_label, repo_label), 0.0) + value
        )

    links: List[SankeyLink] = []
    for (source, target), value in link_totals.items():
        if value <= 0:
            continue
        links.append(SankeyLink(source=source, target=target, value=value))
        outgoing[source] = outgoing.get(source, 0.0) + value
        incoming[target] = incoming.get(target, 0.0) + value

    for name, node in nodes_by_name.items():
        node.value = max(incoming.get(name, 0.0), outgoing.get(name, 0.0))

    return list(nodes_by_name.values()), links


async def build_investment_flow_response(
    *,
    db_url: str,
    filters: MetricFilter,
    theme: Optional[str] = None,
    flow_mode: Optional[str] = None,
    drill_category: Optional[str] = None,
    top_n_repos: int = 12,
) -> SankeyResponse:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)

    theme_filters, subcategory_filters = _split_category_filters(filters)
    if theme:
        theme_filters = [theme]
    normalized_drill = _normalize_theme_key(drill_category)

    if flow_mode in {
        "team_category_repo",
        "team_subcategory_repo",
        "team_category_subcategory_repo",
    }:
        if flow_mode == "team_subcategory_repo" and not normalized_drill:
            raise ValueError("drill_category is required for team_subcategory_repo")
        if normalized_drill:
            theme_filters = [normalized_drill]
        top_n_repos = max(1, int(top_n_repos or 1))

        async with clickhouse_client(db_url) as sink:
            if sink.backend_type == "clickhouse":
                if not await _tables_present(sink, ["work_unit_investments"]):
                    return SankeyResponse(
                        mode="investment", nodes=[], links=[], unit=None
                    )

                required_cols = [
                    "from_ts",
                    "to_ts",
                    "repo_id",
                    "effort_value",
                    "subcategory_distribution_json",
                    "structural_evidence_json",
                ]
                if not await _columns_present(
                    sink, "work_unit_investments", required_cols
                ):
                    return SankeyResponse(
                        mode="investment", nodes=[], links=[], unit=None
                    )

            scope_filter, scope_params = "", {}
            if filters.scope.level in {"team", "repo"}:
                repo_ids = await resolve_repo_filter_ids(sink, filters)
                scope_filter, scope_params = build_scope_filter_multi(
                    "repo", repo_ids, repo_column="repo_id"
                )

            if flow_mode == "team_category_repo":
                rows = await fetch_investment_team_category_repo_edges(
                    sink,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    themes=theme_filters or None,
                    subcategories=subcategory_filters or None,
                )
                nodes, links = _build_team_burden_sankey(
                    rows,
                    category_key="category",
                    category_label_fn=_format_theme_label,
                    top_n_repos=top_n_repos,
                )
                label = "Team → Category → Repo"
                description = "Team burden flow with category rollups."
            elif flow_mode == "team_category_subcategory_repo":
                rows = await fetch_investment_team_subcategory_repo_edges(
                    sink,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    themes=theme_filters or None,
                    subcategories=subcategory_filters or None,
                )
                nodes, links = _build_team_theme_subcategory_repo_sankey(
                    rows,
                    top_n_repos=top_n_repos,
                )
                label = "Team → Category → Subcategory → Repo"
                description = "Full 4-level team burden flow."
            else:
                rows = await fetch_investment_team_subcategory_repo_edges(
                    sink,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    scope_filter=scope_filter,
                    scope_params=scope_params,
                    themes=theme_filters or None,
                    subcategories=subcategory_filters or None,
                )
                nodes, links = _build_team_burden_sankey(
                    rows,
                    category_key="subcategory",
                    category_label_fn=_format_subcategory_label,
                    top_n_repos=top_n_repos,
                )
                label = "Team → Subcategory → Repo"
                description = f"Showing subcategories within {_format_theme_label(normalized_drill or '')}."

            unassigned_counts = await fetch_investment_unassigned_counts(
                sink,
                start_ts=start_ts,
                end_ts=end_ts,
                scope_filter=scope_filter,
                scope_params=scope_params,
                themes=theme_filters or None,
                subcategories=subcategory_filters or None,
            )

        total_value = sum(float(row.get("value") or 0.0) for row in rows)
        assigned_team_value = sum(
            float(row.get("value") or 0.0)
            for row in rows
            if str(row.get("team") or UNASSIGNED_TEAM) != UNASSIGNED_TEAM
        )
        assigned_repo_value = sum(
            float(row.get("value") or 0.0)
            for row in rows
            if str(row.get("repo") or UNASSIGNED_REPO) != UNASSIGNED_REPO
        )
        team_coverage = assigned_team_value / total_value if total_value > 0 else 0.0
        repo_coverage = assigned_repo_value / total_value if total_value > 0 else 0.0
        distinct_team_targets = len(
            {
                str(row.get("team"))
                for row in rows
                if str(row.get("team") or UNASSIGNED_TEAM) != UNASSIGNED_TEAM
            }
        )
        distinct_repo_targets = len(
            {
                str(row.get("repo"))
                for row in rows
                if str(row.get("repo") or UNASSIGNED_REPO) != UNASSIGNED_REPO
            }
        )

        return SankeyResponse(
            mode="investment",
            nodes=nodes,
            links=links,
            unit=None,
            label=label,
            description=description,
            team_coverage=team_coverage,
            repo_coverage=repo_coverage,
            distinct_team_targets=distinct_team_targets,
            distinct_repo_targets=distinct_repo_targets,
            chosen_mode=flow_mode,
            coverage={"team_coverage": team_coverage, "repo_coverage": repo_coverage},
            unassigned_reasons=unassigned_counts,
            flow_mode=flow_mode,
            drill_category=normalized_drill,
            top_n_repos=top_n_repos,
        )

    async with clickhouse_client(db_url) as sink:
        if sink.backend_type == "clickhouse":
            if not await _tables_present(sink, ["work_unit_investments"]):
                return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)

            # Check for required columns
            required_cols = [
                "from_ts",
                "to_ts",
                "repo_id",
                "effort_value",
                "subcategory_distribution_json",
            ]
            if not await _columns_present(sink, "work_unit_investments", required_cols):
                return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)

        scope_filter, scope_params = "", {}
        if filters.scope.level in {"team", "repo"}:
            repo_ids = await resolve_repo_filter_ids(sink, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )

        # 1. Fetch both sets of edges
        repo_rows = await fetch_investment_subcategory_edges(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )

        team_rows = await fetch_investment_team_edges(
            sink,
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
        distinct_targets = len(
            {row["target"] for row in rows if row["target"] != "unassigned"}
        )
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


async def build_investment_repo_team_flow_response(
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

    async with clickhouse_client(db_url) as sink:
        if sink.backend_type == "clickhouse":
            if not await _tables_present(sink, ["work_unit_investments"]):
                return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)

            required_cols = [
                "from_ts",
                "to_ts",
                "repo_id",
                "effort_value",
                "subcategory_distribution_json",
                "structural_evidence_json",
            ]
            if not await _columns_present(sink, "work_unit_investments", required_cols):
                return SankeyResponse(mode="investment", nodes=[], links=[], unit=None)

        scope_filter, scope_params = "", {}
        if filters.scope.level in {"team", "repo"}:
            repo_ids = await resolve_repo_filter_ids(sink, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )

        rows = await fetch_investment_repo_team_edges(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter=scope_filter,
            scope_params=scope_params,
            themes=theme_filters or None,
            subcategories=subcategory_filters or None,
        )

    nodes_by_name: Dict[str, SankeyNode] = {}
    links: List[SankeyLink] = []

    def add_node(name: str, group: str) -> None:
        if name not in nodes_by_name:
            nodes_by_name[name] = SankeyNode(name=name, group=group, value=0.0)

    for row in rows:
        sub_key = str(row.get("subcategory") or "")
        if not sub_key:
            continue
        value = float(row.get("value") or 0.0)
        if value <= 0:
            continue
        source_label = _format_subcategory_label(sub_key)
        add_node(source_label, "subcategory")
        nodes_by_name[source_label].value = (
            nodes_by_name[source_label].value or 0.0
        ) + value

        repo_label = str(row.get("repo") or "unassigned")
        team_label = str(row.get("team") or "").strip() or "unassigned"

        if repo_label == "unassigned" and team_label != "unassigned":
            add_node(team_label, "team")
            nodes_by_name[team_label].value = (
                nodes_by_name[team_label].value or 0.0
            ) + value
            links.append(
                SankeyLink(source=source_label, target=team_label, value=value)
            )
            continue

        add_node(repo_label, "repo")
        nodes_by_name[repo_label].value = (
            nodes_by_name[repo_label].value or 0.0
        ) + value
        links.append(SankeyLink(source=source_label, target=repo_label, value=value))

        if team_label != "unassigned":
            add_node(team_label, "team")
            nodes_by_name[team_label].value = (
                nodes_by_name[team_label].value or 0.0
            ) + value
            links.append(SankeyLink(source=repo_label, target=team_label, value=value))

    return SankeyResponse(
        mode="investment",
        nodes=list(nodes_by_name.values()),
        links=links,
        unit=None,
        label="Subcategory → Repo → Team",
        description="Allocation flow with repo-to-team mapping from work items.",
        chosen_mode="repo_team",
    )
