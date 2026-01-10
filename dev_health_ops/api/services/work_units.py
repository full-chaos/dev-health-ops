from __future__ import annotations

from datetime import datetime, time, timezone
import logging
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from analytics.work_units import (
    WorkUnitConfig,
    compute_evidence_quality,
    compute_subcategory_scores,
    evidence_quality_band,
    merge_subcategory_vectors,
    rollup_subcategories_to_themes,
    work_unit_id,
)
from ..models.filters import MetricFilter
from ..models.schemas import (
    EvidenceQuality,
    InvestmentBreakdown,
    WorkUnitEvidence,
    WorkUnitEffort,
    WorkUnitInvestment,
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
from .investment_categorizer import categorize_investment_texts

NodeKey = Tuple[str, str]

_CONFIG: Optional[WorkUnitConfig] = None
logger = logging.getLogger(__name__)


def _config() -> WorkUnitConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = WorkUnitConfig()
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


def _node_time_bounds(
    node_type: str, data: Dict[str, object]
) -> Tuple[Optional[datetime], Optional[datetime]]:
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


def _build_components(
    edges: List[Dict[str, object]],
) -> List[Tuple[List[NodeKey], List[Dict[str, object]]]]:
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


async def build_work_unit_investments(
    *,
    db_url: str,
    filters: MetricFilter,
    limit: int = 200,
    include_text: bool = True,
    llm_provider: str = "auto",
) -> List[WorkUnitInvestment]:
    config = _config()
    start_day, end_day, _, _ = time_window(filters)
    window_start = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    window_end = datetime.combine(end_day, time.min, tzinfo=timezone.utc)

    async with clickhouse_client(db_url) as client:
        repo_ids = await resolve_repo_filter_ids(client, filters)
        edge_limit = max(50000, limit * 200)
        logger.debug(
            "WorkUnit investments query repo_ids=%s edge_limit=%s",
            len(repo_ids or []),
            edge_limit,
        )
        edges = await fetch_work_graph_edges(
            client,
            repo_ids=repo_ids or None,
            limit=edge_limit,
        )
        logger.debug("WorkUnit investments fetched edges=%s", len(edges))

        components = _build_components(edges)
        logger.debug("WorkUnit investments components=%s", len(components))
        if not components:
            return []

        issue_ids = {
            node_id
            for node_type, node_id in _flatten_nodes(components)
            if node_type == "issue"
        }
        pr_ids = {
            node_id
            for node_type, node_id in _flatten_nodes(components)
            if node_type == "pr"
        }
        commit_ids = {
            node_id
            for node_type, node_id in _flatten_nodes(components)
            if node_type == "commit"
        }

        work_items = await fetch_work_items(client, work_item_ids=issue_ids)
        active_hours = await fetch_work_item_active_hours(
            client, work_item_ids=issue_ids
        )

        repo_prs = _group_prs_by_repo(pr_ids)
        prs = await fetch_pull_requests(client, repo_numbers=repo_prs)

        repo_commits = _group_commits_by_repo(commit_ids)
        commits = await fetch_commits(client, repo_commits=repo_commits)
        commit_churn = await fetch_commit_churn(client, repo_commits=repo_commits)
        logger.debug(
            "WorkUnit investments issue_ids=%s pr_ids=%s commit_ids=%s work_items=%s prs=%s commits=%s churn=%s",
            len(issue_ids),
            len(pr_ids),
            len(commit_ids),
            len(work_items),
            len(prs),
            len(commits),
            len(commit_churn),
        )

    work_item_map = {str(item.get("work_item_id")): item for item in work_items}
    pr_map = _map_prs(prs)
    commit_map = _map_commits(commits)
    pr_churn = _pr_churn_map(prs)

    results: List[WorkUnitInvestment] = []
    for nodes, component_edges in components:
        unit_nodes = list(dict.fromkeys(nodes))
        issue_node_ids = [
            node_id for node_type, node_id in unit_nodes if node_type == "issue"
        ]
        pr_node_ids = [
            node_id for node_type, node_id in unit_nodes if node_type == "pr"
        ]
        commit_node_ids = [
            node_id for node_type, node_id in unit_nodes if node_type == "commit"
        ]

        type_counts = _count_work_item_types(issue_node_ids, work_item_map)
        metadata_scores, metadata_evidence = compute_subcategory_scores(type_counts)

        texts_by_source: Dict[str, List[str]] = {}
        llm_result = None
        if include_text:
            texts_by_source = _collect_texts(
                issue_node_ids,
                pr_node_ids,
                commit_node_ids,
                work_item_map,
                pr_map,
                commit_map,
            )
            if _count_text_sources(texts_by_source) > 0:
                llm_result = await categorize_investment_texts(
                    texts_by_source,
                    llm_provider=llm_provider,
                )

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

        subcategory_vector = merge_subcategory_vectors(
            primary=llm_result.subcategories if llm_result else None,
            secondary=metadata_scores,
            primary_weight=config.text_weight if llm_result else 0.0,
        )
        theme_vector = rollup_subcategories_to_themes(subcategory_vector)
        text_source_count = _count_text_sources(texts_by_source)
        evidence_quality_value = compute_evidence_quality(
            text_source_count=text_source_count,
            metadata_present=bool(type_counts),
            density_score=density_score,
            provenance_score=provenance_score,
            temporal_score=temporal_score,
        )
        evidence_quality = EvidenceQuality(
            value=evidence_quality_value,
            band=evidence_quality_band(evidence_quality_value),
        )

        contextual_evidence: List[Dict[str, object]] = [
            {
                "type": "time_range",
                "start": temporal_start.isoformat(),
                "end": temporal_end.isoformat(),
                "span_days": max(
                    0.0, (temporal_end - temporal_start).total_seconds() / 86400.0
                ),
                "score": temporal_score,
                "window_days": config.temporal_window_days,
            },
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
        ]
        repo_ids = _collect_repo_ids(component_edges)
        if repo_ids:
            contextual_evidence.append({"type": "repo_scope", "repo_ids": repo_ids})

        textual_evidence = _build_textual_evidence(
            texts_by_source, llm_result.textual_evidence if llm_result else []
        )
        if llm_result and llm_result.uncertainty:
            textual_evidence.extend(llm_result.uncertainty)

        evidence = WorkUnitEvidence(
            textual=textual_evidence,
            structural=metadata_evidence,
            contextual=contextual_evidence,
        )

        results.append(
            WorkUnitInvestment(
                work_unit_id=work_unit_id(unit_nodes),
                time_range=WorkUnitTimeRange(start=temporal_start, end=temporal_end),
                effort=effort,
                investment=InvestmentBreakdown(
                    themes=theme_vector,
                    subcategories=subcategory_vector,
                ),
                evidence_quality=evidence_quality,
                evidence=evidence,
            )
        )

    results.sort(key=lambda item: item.effort.value, reverse=True)
    logger.debug(
        "WorkUnit investments returning count=%s", len(results[: max(1, int(limit))])
    )
    return results[: max(1, int(limit))]


def _flatten_nodes(
    components: List[Tuple[List[NodeKey], List[Dict[str, object]]]],
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


def _count_text_sources(texts_by_source: Dict[str, List[str]]) -> int:
    return sum(1 for texts in texts_by_source.values() if any(t.strip() for t in texts))


def _build_textual_evidence(
    texts_by_source: Dict[str, List[str]],
    llm_evidence: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    evidence: List[Dict[str, object]] = []
    for source, texts in texts_by_source.items():
        if texts:
            evidence.append({"type": "text_source", "source": source, "count": len(texts)})
    for entry in llm_evidence:
        if isinstance(entry, dict):
            evidence.append(entry)
    return evidence


def _collect_repo_ids(edges: List[Dict[str, object]]) -> List[str]:
    repo_ids = {
        str(edge.get("repo_id") or "")
        for edge in edges
        if edge.get("repo_id")
    }
    return sorted(repo_id for repo_id in repo_ids if repo_id)


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
