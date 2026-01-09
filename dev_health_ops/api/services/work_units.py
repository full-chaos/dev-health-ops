from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from analytics.work_units import (
    WorkUnitConfig,
    apply_textual_modifiers,
    compute_confidence,
    compute_structural_scores,
    compute_text_agreement,
    compute_textual_modifiers,
    confidence_band,
    load_work_unit_config,
    work_unit_id,
)
from ..models.filters import MetricFilter
from ..models.schemas import (
    WorkUnitConfidence,
    WorkUnitEvidence,
    WorkUnitEffort,
    WorkUnitSignal,
    WorkUnitTimeRange,
)
from ..queries.client import clickhouse_client
from ..queries.work_units import (
    fetch_commit_churn,
    fetch_commits,
    fetch_pull_requests,
    fetch_work_graph_edges,
    fetch_work_item_active_hours,
    fetch_work_items,
)
from .filtering import resolve_repo_filter_ids, time_window

NodeKey = Tuple[str, str]

_CONFIG: Optional[WorkUnitConfig] = None


def _config() -> WorkUnitConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = load_work_unit_config()
    return _CONFIG


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_pr_id(pr_id: str) -> Tuple[Optional[str], Optional[int]]:
    try:
        repo_part, number_part = pr_id.split("#pr", 1)
        return repo_part, int(number_part)
    except (ValueError, AttributeError):
        return None, None


def _parse_commit_id(commit_id: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        repo_part, hash_part = commit_id.split("@", 1)
        return repo_part, hash_part
    except (ValueError, AttributeError):
        return None, None


def _node_time_bounds(node_type: str, data: Dict[str, object]) -> Tuple[Optional[datetime], Optional[datetime]]:
    if node_type == "issue":
        start = _ensure_utc(data.get("created_at"))  # type: ignore[arg-type]
        end = _ensure_utc(data.get("completed_at"))  # type: ignore[arg-type]
        end = end or _ensure_utc(data.get("updated_at"))  # type: ignore[arg-type]
        return start, end or start
    if node_type == "pr":
        start = _ensure_utc(data.get("created_at"))  # type: ignore[arg-type]
        end = _ensure_utc(data.get("merged_at"))  # type: ignore[arg-type]
        end = end or _ensure_utc(data.get("closed_at"))  # type: ignore[arg-type]
        return start, end or start
    if node_type == "commit":
        when = _ensure_utc(data.get("author_when"))  # type: ignore[arg-type]
        when = when or _ensure_utc(data.get("committer_when"))  # type: ignore[arg-type]
        return when, when
    return None, None


def _temporal_score(
    *,
    start: datetime,
    end: datetime,
    config: WorkUnitConfig,
) -> float:
    span_days = max(0.0, (end - start).total_seconds() / 86400.0)
    window_days = max(1.0, float(config.temporal_window_days))
    return max(0.0, 1.0 - (span_days / window_days))


def _graph_density(node_count: int, edge_count: int) -> float:
    if node_count <= 1:
        return 1.0
    possible = node_count * (node_count - 1) / 2.0
    if possible <= 0:
        return 0.0
    return min(1.0, edge_count / possible)


def _effort_from_work_unit(
    *,
    issue_ids: Iterable[str],
    pr_ids: Iterable[str],
    commit_ids: Iterable[str],
    pr_churn: Dict[str, float],
    commit_churn: Dict[str, float],
    active_hours: Dict[str, float],
) -> WorkUnitEffort:
    commit_total = sum(commit_churn.get(cid, 0.0) for cid in commit_ids)
    if commit_total > 0:
        return WorkUnitEffort(metric="churn_loc", value=float(commit_total))
    pr_total = sum(pr_churn.get(pid, 0.0) for pid in pr_ids)
    if pr_total > 0:
        return WorkUnitEffort(metric="churn_loc", value=float(pr_total))
    active_total = sum(active_hours.get(wid, 0.0) for wid in issue_ids)
    if active_total > 0:
        return WorkUnitEffort(metric="active_hours", value=float(active_total))
    return WorkUnitEffort(metric="churn_loc", value=0.0)


def _edge_confidence(edges: Iterable[Dict[str, object]]) -> float:
    values = [float(edge.get("confidence") or 0.0) for edge in edges]
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def _build_components(edges: List[Dict[str, object]]) -> List[Tuple[List[NodeKey], List[Dict[str, object]]]]:
    adjacency: Dict[NodeKey, List[NodeKey]] = {}
    edges_by_node: Dict[NodeKey, List[Dict[str, object]]] = {}

    for edge in edges:
        source = (str(edge.get("source_type")), str(edge.get("source_id")))
        target = (str(edge.get("target_type")), str(edge.get("target_id")))
        adjacency.setdefault(source, []).append(target)
        adjacency.setdefault(target, []).append(source)
        edges_by_node.setdefault(source, []).append(edge)
        edges_by_node.setdefault(target, []).append(edge)

    visited: set[NodeKey] = set()
    components: List[Tuple[List[NodeKey], List[Dict[str, object]]]] = []

    for node in adjacency:
        if node in visited:
            continue
        stack = [node]
        visited.add(node)
        component_nodes: List[NodeKey] = []
        component_edges: Dict[str, Dict[str, object]] = {}
        while stack:
            current = stack.pop()
            component_nodes.append(current)
            for edge in edges_by_node.get(current, []):
                edge_id = str(edge.get("edge_id") or "")
                if edge_id and edge_id not in component_edges:
                    component_edges[edge_id] = edge
            for neighbor in adjacency.get(current, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        components.append((component_nodes, list(component_edges.values())))
    return components


async def build_work_unit_signals(
    *,
    db_url: str,
    filters: MetricFilter,
    limit: int = 200,
) -> List[WorkUnitSignal]:
    config = _config()
    start_day, end_day, _, _ = time_window(filters)
    window_start = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    window_end = datetime.combine(end_day, time.min, tzinfo=timezone.utc)

    async with clickhouse_client(db_url) as client:
        repo_ids = await resolve_repo_filter_ids(client, filters)
        edge_limit = max(50000, limit * 200)
        edges = await fetch_work_graph_edges(
            client,
            repo_ids=repo_ids or None,
            limit=edge_limit,
        )

        components = _build_components(edges)
        if not components:
            return []

        issue_ids = {node_id for node_type, node_id in _flatten_nodes(components) if node_type == "issue"}
        pr_ids = {node_id for node_type, node_id in _flatten_nodes(components) if node_type == "pr"}
        commit_ids = {node_id for node_type, node_id in _flatten_nodes(components) if node_type == "commit"}

        work_items = await fetch_work_items(client, work_item_ids=issue_ids)
        active_hours = await fetch_work_item_active_hours(client, work_item_ids=issue_ids)

        repo_prs = _group_prs_by_repo(pr_ids)
        prs = await fetch_pull_requests(client, repo_numbers=repo_prs)

        repo_commits = _group_commits_by_repo(commit_ids)
        commits = await fetch_commits(client, repo_commits=repo_commits)
        commit_churn = await fetch_commit_churn(client, repo_commits=repo_commits)

    work_item_map = {str(item.get("work_item_id")): item for item in work_items}
    pr_map = _map_prs(prs)
    commit_map = _map_commits(commits)
    pr_churn = _pr_churn_map(prs)

    results: List[WorkUnitSignal] = []
    for nodes, component_edges in components:
        unit_nodes = list(dict.fromkeys(nodes))
        issue_node_ids = [node_id for node_type, node_id in unit_nodes if node_type == "issue"]
        pr_node_ids = [node_id for node_type, node_id in unit_nodes if node_type == "pr"]
        commit_node_ids = [node_id for node_type, node_id in unit_nodes if node_type == "commit"]

        structural_scores, structural_evidence = compute_structural_scores(
            _count_work_item_types(issue_node_ids, work_item_map),
            config,
        )

        texts_by_source = _collect_texts(issue_node_ids, pr_node_ids, commit_node_ids, work_item_map, pr_map, commit_map)
        modifiers, textual_evidence = compute_textual_modifiers(texts_by_source, config)
        final_scores = apply_textual_modifiers(structural_scores, modifiers, config.categories)

        text_agreement = compute_text_agreement(structural_scores, modifiers, config)
        density_score = _graph_density(len(unit_nodes), len(component_edges))
        provenance_score = _edge_confidence(component_edges)

        temporal_start, temporal_end, temporal_fallback = _component_time_bounds(
            unit_nodes,
            work_item_map,
            pr_map,
            commit_map,
            window_start,
            window_end,
        )
        temporal_score = (
            config.temporal_fallback
            if temporal_fallback
            else _temporal_score(start=temporal_start, end=temporal_end, config=config)
        )

        confidence_value = compute_confidence(
            provenance_score=provenance_score,
            temporal_score=temporal_score,
            density_score=density_score,
            text_agreement=text_agreement,
            config=config,
        )

        if temporal_end < window_start or temporal_start >= window_end:
            continue

        effort = _effort_from_work_unit(
            issue_ids=issue_node_ids,
            pr_ids=pr_node_ids,
            commit_ids=commit_node_ids,
            pr_churn=pr_churn,
            commit_churn=commit_churn,
            active_hours=active_hours,
        )

        evidence = WorkUnitEvidence(
            structural=structural_evidence
            + [
                {
                    "type": "graph_density",
                    "nodes": len(unit_nodes),
                    "edges": len(component_edges),
                    "value": density_score,
                },
                {
                    "type": "provenance",
                    "edges": len(component_edges),
                    "value": provenance_score,
                },
            ],
            temporal=[
                {
                    "type": "time_range",
                    "start": temporal_start.isoformat(),
                    "end": temporal_end.isoformat(),
                    "span_days": max(
                        0.0, (temporal_end - temporal_start).total_seconds() / 86400.0
                    ),
                    "score": temporal_score,
                    "window_days": config.temporal_window_days,
                }
            ],
            textual=textual_evidence,
        )

        results.append(
            WorkUnitSignal(
                work_unit_id=work_unit_id(unit_nodes),
                time_range=WorkUnitTimeRange(start=temporal_start, end=temporal_end),
                effort=effort,
                categories=final_scores,
                confidence=WorkUnitConfidence(
                    value=confidence_value,
                    band=confidence_band(confidence_value),
                ),
                evidence=evidence,
            )
        )

    results.sort(key=lambda item: item.effort.value, reverse=True)
    return results[: max(1, int(limit))]


def _flatten_nodes(
    components: List[Tuple[List[NodeKey], List[Dict[str, object]]]]
) -> List[NodeKey]:
    nodes: List[NodeKey] = []
    for node_list, _ in components:
        nodes.extend(node_list)
    return nodes


def _group_prs_by_repo(pr_ids: Iterable[str]) -> Dict[str, List[int]]:
    repo_map: Dict[str, List[int]] = {}
    for pr_id in pr_ids:
        repo_id, number = _parse_pr_id(pr_id)
        if repo_id and number is not None:
            repo_map.setdefault(repo_id, []).append(number)
    return repo_map


def _group_commits_by_repo(commit_ids: Iterable[str]) -> Dict[str, List[str]]:
    repo_map: Dict[str, List[str]] = {}
    for commit_id in commit_ids:
        repo_id, commit_hash = _parse_commit_id(commit_id)
        if repo_id and commit_hash:
            repo_map.setdefault(repo_id, []).append(commit_hash)
    return repo_map


def _map_prs(prs: Sequence[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    mapped: Dict[str, Dict[str, object]] = {}
    for pr in prs:
        repo_id = str(pr.get("repo_id") or "")
        number = pr.get("number")
        if not repo_id or number is None:
            continue
        pr_id = f"{repo_id}#pr{number}"
        mapped[pr_id] = pr
    return mapped


def _map_commits(commits: Sequence[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    mapped: Dict[str, Dict[str, object]] = {}
    for commit in commits:
        repo_id = str(commit.get("repo_id") or "")
        commit_hash = str(commit.get("hash") or "")
        if not repo_id or not commit_hash:
            continue
        commit_id = f"{repo_id}@{commit_hash}"
        mapped[commit_id] = commit
    return mapped


def _pr_churn_map(prs: Sequence[Dict[str, object]]) -> Dict[str, float]:
    churn: Dict[str, float] = {}
    for pr in prs:
        repo_id = str(pr.get("repo_id") or "")
        number = pr.get("number")
        if not repo_id or number is None:
            continue
        pr_id = f"{repo_id}#pr{number}"
        additions = float(pr.get("additions") or 0.0)
        deletions = float(pr.get("deletions") or 0.0)
        churn[pr_id] = additions + deletions
    return churn


def _count_work_item_types(
    issue_ids: Iterable[str],
    work_item_map: Dict[str, Dict[str, object]],
) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for work_item_id in issue_ids:
        item = work_item_map.get(work_item_id)
        if not item:
            continue
        work_type = str(item.get("type") or "unknown")
        counts[work_type] = counts.get(work_type, 0) + 1
    return counts


def _collect_texts(
    issue_ids: Iterable[str],
    pr_ids: Iterable[str],
    commit_ids: Iterable[str],
    work_item_map: Dict[str, Dict[str, object]],
    pr_map: Dict[str, Dict[str, object]],
    commit_map: Dict[str, Dict[str, object]],
) -> Dict[str, List[str]]:
    texts: Dict[str, List[str]] = {}

    for issue_id in issue_ids:
        item = work_item_map.get(issue_id)
        title = (item or {}).get("title")
        if title:
            texts.setdefault("issue_title", []).append(str(title))
        description = (item or {}).get("description")
        if description:
            texts.setdefault("issue_description", []).append(str(description))

    for pr_id in pr_ids:
        pr = pr_map.get(pr_id)
        title = (pr or {}).get("title")
        if title:
            texts.setdefault("pr_title", []).append(str(title))
        description = (pr or {}).get("body")
        if description:
            texts.setdefault("pr_description", []).append(str(description))

    for commit_id in commit_ids:
        commit = commit_map.get(commit_id)
        message = (commit or {}).get("message")
        if message:
            texts.setdefault("commit_message", []).append(str(message))

    return texts


def _component_time_bounds(
    nodes: List[NodeKey],
    work_item_map: Dict[str, Dict[str, object]],
    pr_map: Dict[str, Dict[str, object]],
    commit_map: Dict[str, Dict[str, object]],
    window_start: datetime,
    window_end: datetime,
) -> Tuple[datetime, datetime, bool]:
    starts: List[datetime] = []
    ends: List[datetime] = []
    for node_type, node_id in nodes:
        data: Dict[str, object] = {}
        if node_type == "issue":
            data = work_item_map.get(node_id, {})
        elif node_type == "pr":
            data = pr_map.get(node_id, {})
        elif node_type == "commit":
            data = commit_map.get(node_id, {})
        start, end = _node_time_bounds(node_type, data)
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    if not starts or not ends:
        return window_start, window_end, True
    return min(starts), max(ends), False
