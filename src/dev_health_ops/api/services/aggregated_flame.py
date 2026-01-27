"""Service layer for aggregated flame graph responses."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from ..models.schemas import (
    AggregatedFlameMeta,
    AggregatedFlameNode,
    AggregatedFlameResponse,
    ApproximationInfo,
)
from ..queries.client import clickhouse_client
from ..queries.aggregated_flame import (
    fetch_code_hotspots,
    fetch_cycle_breakdown,
    fetch_cycle_milestones,
    fetch_repo_names,
    fetch_throughput,
    fetch_throughput_by_type,
)


# Canonical status ordering for cycle breakdown display
_CYCLE_STATUS_ORDER = [
    "backlog",
    "ready",
    "in_progress",
    "in progress",
    "coding",
    "review",
    "in_review",
    "waiting",
    "blocked",
    "testing",
    "qa",
    "deploy",
    "deployed",
    "done",
    "closed",
    "resolved",
]


def _status_sort_key(status: str) -> int:
    """Return sort key for status ordering."""
    lower = status.lower().strip()
    try:
        return _CYCLE_STATUS_ORDER.index(lower)
    except ValueError:
        # Unknown statuses go at end, sorted alphabetically
        return len(_CYCLE_STATUS_ORDER) + ord(lower[0]) if lower else 999


def _sanitize_label(name: Optional[str]) -> str:
    """Ensure non-null label names."""
    if not name or not name.strip():
        return "(unknown)"
    return name.strip()


def _build_cycle_breakdown_tree(
    rows: List[Dict[str, Any]],
) -> AggregatedFlameNode:
    """
    Build hierarchical tree for cycle-time breakdown.

    Structure:
    - root (total time)
      - Active Work (in_progress, coding)
      - Review (review, in_review)
      - Waiting (backlog, ready, waiting)
      - Blocked
      - Other (remaining statuses)
    """
    # Categorize statuses
    categories: Dict[str, List[Dict[str, Any]]] = {
        "Active Work": [],
        "Review": [],
        "Waiting": [],
        "Blocked": [],
        "Other": [],
    }

    for row in rows:
        status = (row.get("status") or "").lower().strip()
        hours = float(row.get("total_hours") or 0)
        if hours <= 0:
            continue

        if status in ("in_progress", "in progress", "coding", "development"):
            categories["Active Work"].append(row)
        elif status in ("review", "in_review", "code review", "pr_review"):
            categories["Review"].append(row)
        elif status in ("backlog", "ready", "waiting", "queue", "pending"):
            categories["Waiting"].append(row)
        elif status in ("blocked", "on_hold", "on hold"):
            categories["Blocked"].append(row)
        else:
            categories["Other"].append(row)

    # Build category nodes
    category_nodes: List[AggregatedFlameNode] = []
    for category_name, category_rows in categories.items():
        if not category_rows:
            continue

        children = [
            AggregatedFlameNode(
                name=_sanitize_label(row.get("status")),
                value=float(row.get("total_hours") or 0),
                children=[],
            )
            for row in sorted(category_rows, key=lambda r: -(r.get("total_hours") or 0))
        ]

        category_value = sum(child.value for child in children)
        category_nodes.append(
            AggregatedFlameNode(
                name=category_name,
                value=category_value,
                children=children,
            )
        )

    # Sort categories by value descending
    category_nodes.sort(key=lambda n: -n.value)

    # Calculate root total
    root_value = sum(node.value for node in category_nodes)

    return AggregatedFlameNode(
        name="Cycle Time",
        value=root_value,
        children=category_nodes,
    )


def _build_code_hotspots_tree(
    rows: List[Dict[str, Any]],
    repo_names: Dict[str, str],
) -> AggregatedFlameNode:
    """
    Build hierarchical tree for code hotspots.

    Structure:
    - root (total churn)
      - repo name
        - directory
          - file
    """
    # Group by repo, then build path hierarchy
    repo_files: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        repo_id = row.get("repo_id") or "(unknown)"
        if repo_id not in repo_files:
            repo_files[repo_id] = []
        repo_files[repo_id].append(row)

    repo_nodes: List[AggregatedFlameNode] = []
    for repo_id, files in repo_files.items():
        repo_name = repo_names.get(repo_id) or repo_id

        # Build path tree for this repo
        path_tree: Dict[str, Any] = {}
        for file_row in files:
            file_path = file_row.get("file_path") or "(unknown)"
            churn = float(file_row.get("total_churn") or 0)

            # Split path into segments
            segments = [s for s in file_path.split("/") if s]
            if not segments:
                segments = [file_path]

            # Insert into tree
            current = path_tree
            for i, segment in enumerate(segments):
                if segment not in current:
                    current[segment] = {"_value": 0, "_children": {}}
                if i == len(segments) - 1:
                    # Leaf node - add churn value
                    current[segment]["_value"] += churn
                else:
                    current = current[segment]["_children"]

        def _tree_to_node(name: str, tree_entry: Dict[str, Any]) -> AggregatedFlameNode:
            """Recursively convert path tree to AggregatedFlameNode."""
            children = [
                _tree_to_node(child_name, child_tree)
                for child_name, child_tree in tree_entry.get("_children", {}).items()
            ]
            # Sort children by value descending
            children.sort(key=lambda n: -n.value)

            leaf_value = tree_entry.get("_value", 0)
            children_value = sum(c.value for c in children)
            total_value = leaf_value + children_value

            return AggregatedFlameNode(
                name=_sanitize_label(name),
                value=total_value,
                children=children,
            )

        # Build nodes from path tree
        dir_nodes: List[AggregatedFlameNode] = []
        for dir_name, dir_tree in path_tree.items():
            dir_nodes.append(_tree_to_node(dir_name, dir_tree))

        # Sort by value descending
        dir_nodes.sort(key=lambda n: -n.value)

        repo_value = sum(node.value for node in dir_nodes)
        repo_nodes.append(
            AggregatedFlameNode(
                name=_sanitize_label(repo_name),
                value=repo_value,
                children=dir_nodes,
            )
        )

    # Sort repos by value descending
    repo_nodes.sort(key=lambda n: -n.value)

    root_value = sum(node.value for node in repo_nodes)
    return AggregatedFlameNode(
        name="Code Churn",
        value=root_value,
        children=repo_nodes,
    )


def _build_throughput_tree(
    rows: List[Dict[str, Any]],
) -> AggregatedFlameNode:
    """
    Build hierarchical tree for throughput.

    Structure:
    - root (total items)
      - By Work Type (feature, bug, chore, etc.)
        - By Team/Repo
    """
    # Group by work type
    type_groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        w_type = row.get("work_type") or "unclassified"
        if w_type not in type_groups:
            type_groups[w_type] = []
        type_groups[w_type].append(row)

    type_nodes: List[AggregatedFlameNode] = []
    for w_type, type_rows in type_groups.items():
        children = [
            AggregatedFlameNode(
                name=_sanitize_label(
                    row.get("team_name") or row.get("repo_name") or row.get("team_id")
                ),
                value=float(row.get("items_completed") or row.get("throughput") or 0),
                children=[],
            )
            for row in sorted(
                type_rows,
                key=lambda r: -(r.get("items_completed") or r.get("throughput") or 0),
            )
        ]
        type_value = sum(child.value for child in children)
        type_nodes.append(
            AggregatedFlameNode(
                name=w_type.capitalize(),
                value=type_value,
                children=children,
            )
        )

    # Sort types by value descending
    type_nodes.sort(key=lambda n: -n.value)
    root_value = sum(node.value for node in type_nodes)

    return AggregatedFlameNode(
        name="Work Delivered",
        value=root_value,
        children=type_nodes,
    )


async def build_aggregated_flame_response(
    *,
    db_url: str,
    mode: Literal["cycle_breakdown", "code_hotspots", "throughput"],
    start_day: date,
    end_day: date,
    team_id: Optional[str] = None,
    repo_id: Optional[str] = None,
    provider: Optional[str] = None,
    work_scope_id: Optional[str] = None,
    limit: int = 500,
    min_value: int = 1,
) -> AggregatedFlameResponse:
    """
    Build an aggregated flame graph response for the given mode.
    """
    notes: List[str] = []
    filters_used: Dict[str, Any] = {}
    approximation = ApproximationInfo()

    async with clickhouse_client(db_url) as sink:
        if mode == "cycle_breakdown":
            rows = await fetch_cycle_breakdown(
                sink,
                start_day=start_day,
                end_day=end_day,
                team_id=team_id,
                provider=provider,
                work_scope_id=work_scope_id,
            )

            if not rows:
                # Approximation fallback: use milestones if state transition data is missing
                rows = await fetch_cycle_milestones(
                    sink,
                    start_day=start_day,
                    end_day=end_day,
                    team_id=team_id,
                    provider=provider,
                    work_scope_id=work_scope_id,
                )
                if rows:
                    notes.append(
                        "Detailed state transition data unavailable. Using milestone-based approximation."
                    )
                    approximation.used = True
                    approximation.method = "milestones"
                    # Map 'milestone' to 'status' for _build_cycle_breakdown_tree
                    rows = [
                        {
                            "status": r["milestone"],
                            "total_hours": float(r["avg_hours"] * r["total_items"]),
                            "total_items": r["total_items"],
                        }
                        for r in rows
                    ]
                    root = _build_cycle_breakdown_tree(rows)
                else:
                    root = AggregatedFlameNode(name="Cycle Time", value=0, children=[])
            else:
                root = _build_cycle_breakdown_tree(rows)

            if team_id:
                filters_used["team_id"] = team_id
            if provider:
                filters_used["provider"] = provider
            if work_scope_id:
                filters_used["work_scope_id"] = work_scope_id

            return AggregatedFlameResponse(
                mode="cycle_breakdown",
                unit="hours",
                root=root,
                meta=AggregatedFlameMeta(
                    window_start=start_day,
                    window_end=end_day,
                    filters=filters_used,
                    notes=notes,
                    approximation=approximation,
                ),
            )

        elif mode == "code_hotspots":
            rows = await fetch_code_hotspots(
                sink,
                start_day=start_day,
                end_day=end_day,
                repo_id=repo_id,
                limit=limit,
                min_churn=min_value,
            )

            if not rows:
                notes.append("No file churn data available for this window/scope.")
                root = AggregatedFlameNode(name="Code Churn", value=0, children=[])
                repo_names = {}
            else:
                repo_ids: List[str] = [
                    str(row.get("repo_id")) for row in rows if row.get("repo_id")
                ]
                repo_names = await fetch_repo_names(sink, repo_ids=repo_ids)
                root = _build_code_hotspots_tree(rows, repo_names)

            if repo_id:
                filters_used["repo_id"] = repo_id

            return AggregatedFlameResponse(
                mode="code_hotspots",
                unit="loc",
                root=root,
                meta=AggregatedFlameMeta(
                    window_start=start_day,
                    window_end=end_day,
                    filters=filters_used,
                    notes=notes,
                    approximation=approximation,
                ),
            )

        else:  # throughput
            rows = await fetch_throughput_by_type(
                sink,
                start_day=start_day,
                end_day=end_day,
                team_id=team_id,
                repo_id=repo_id,
                limit=limit,
            )

            if not rows:
                # Fallback to simple throughput if typed data missing
                rows = await fetch_throughput(
                    sink,
                    start_day=start_day,
                    end_day=end_day,
                    team_id=team_id,
                    repo_id=repo_id,
                    limit=limit,
                )
                notes.append(
                    "Work type classification unavailable. Using team-based grouping."
                )
                approximation.used = True
                approximation.method = "inferred"

            if not rows:
                notes.append("No throughput data available for this window/scope.")
                root = AggregatedFlameNode(name="Work Delivered", value=0, children=[])
            else:
                root = _build_throughput_tree(rows)

            if team_id:
                filters_used["team_id"] = team_id
            if repo_id:
                filters_used["repo_id"] = repo_id

            return AggregatedFlameResponse(
                mode="throughput",
                unit="items",
                root=root,
                meta=AggregatedFlameMeta(
                    window_start=start_day,
                    window_end=end_day,
                    filters=filters_used,
                    notes=notes,
                    approximation=approximation,
                ),
            )
