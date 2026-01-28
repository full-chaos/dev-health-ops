"""Materialize work unit investment categorization into sinks."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from dev_health_ops.llm import get_provider
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.schemas import (
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.work_graph.ids import parse_commit_from_id, parse_pr_from_id
from dev_health_ops.work_graph.investment.categorize import (
    categorize_text_bundle,
    fallback_outcome,
)
from dev_health_ops.work_graph.investment.constants import MIN_EVIDENCE_CHARS
from dev_health_ops.work_graph.investment.evidence import (
    build_text_bundle,
    compute_evidence_quality,
    compute_time_bounds,
)
from dev_health_ops.work_graph.investment.queries import (
    fetch_commit_churn,
    fetch_commits,
    fetch_parent_titles,
    fetch_pull_requests,
    fetch_work_graph_edges,
    fetch_work_item_active_hours,
    fetch_work_items,
    resolve_repo_ids_for_teams,
)
from dev_health_ops.work_graph.investment.utils import (
    evidence_quality_band,
    rollup_subcategories_to_themes,
    work_unit_id,
)

logger = logging.getLogger(__name__)

NodeKey = Tuple[str, str]


@dataclass(frozen=True)
class MaterializeConfig:
    dsn: str
    from_ts: datetime
    to_ts: datetime
    repo_ids: Optional[List[str]]
    llm_provider: str
    persist_evidence_snippets: bool
    llm_model: Optional[str]
    force: bool = False
    team_ids: Optional[List[str]] = None


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
        repo_id, number = parse_pr_from_id(pr_id)
        if repo_id and number is not None:
            repo_map.setdefault(str(repo_id), []).append(number)
    return repo_map


def _group_commits_by_repo(commit_ids: Iterable[str]) -> Dict[str, List[str]]:
    repo_map: Dict[str, List[str]] = {}
    for commit_id in commit_ids:
        repo_id, commit_hash = parse_commit_from_id(commit_id)
        if repo_id and commit_hash:
            repo_map.setdefault(str(repo_id), []).append(commit_hash)
    return repo_map


def _map_prs(prs: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    mapped: Dict[str, Dict[str, object]] = {}
    for pr in prs:
        repo_id = str(pr.get("repo_id") or "")
        number = pr.get("number")
        if not repo_id or number is None:
            continue
        pr_id = f"{repo_id}#pr{number}"
        mapped[pr_id] = pr
    return mapped


def _map_commits(commits: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    mapped: Dict[str, Dict[str, object]] = {}
    for commit in commits:
        repo_id = str(commit.get("repo_id") or "")
        commit_hash = str(commit.get("hash") or "")
        if not repo_id or not commit_hash:
            continue
        commit_id = f"{repo_id}@{commit_hash}"
        mapped[commit_id] = commit
    return mapped


def _pr_churn_map(prs: Iterable[Dict[str, object]]) -> Dict[str, float]:
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


def _effort_from_work_unit(
    *,
    issue_ids: Iterable[str],
    pr_ids: Iterable[str],
    commit_ids: Iterable[str],
    pr_churn: Dict[str, float],
    commit_churn: Dict[str, float],
    active_hours: Dict[str, float],
) -> Tuple[str, float]:
    commit_total = sum(commit_churn.get(cid, 0.0) for cid in commit_ids)
    if commit_total > 0:
        return "churn_loc", float(commit_total)
    pr_total = sum(pr_churn.get(pid, 0.0) for pid in pr_ids)
    if pr_total > 0:
        return "churn_loc", float(pr_total)
    active_total = sum(active_hours.get(wid, 0.0) for wid in issue_ids)
    if active_total > 0:
        return "active_hours", float(active_total)
    return "churn_loc", 0.0


def _collect_repo_ids(edges: List[Dict[str, object]]) -> List[str]:
    repo_ids = {str(edge.get("repo_id") or "") for edge in edges if edge.get("repo_id")}
    return sorted(repo_id for repo_id in repo_ids if repo_id)


def _parse_repo_id(repo_id: Optional[str]) -> Optional[uuid.UUID]:
    if not repo_id:
        return None
    try:
        return uuid.UUID(str(repo_id))
    except Exception:
        return None


def _collect_provider(
    work_item_ids: Iterable[str],
    work_item_map: Dict[str, Dict[str, object]],
) -> Optional[str]:
    providers = {
        str(work_item_map.get(item_id, {}).get("provider") or "")
        for item_id in work_item_ids
        if work_item_map.get(item_id, {}).get("provider")
    }
    providers = {provider for provider in providers if provider}
    if len(providers) == 1:
        return next(iter(providers))
    return None


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _first_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return text.strip()


def _resolve_work_unit_label(
    *,
    issue_ids: Iterable[str],
    pr_ids: Iterable[str],
    commit_ids: Iterable[str],
    work_item_map: Dict[str, Dict[str, object]],
    pr_map: Dict[str, Dict[str, object]],
    commit_map: Dict[str, Dict[str, object]],
) -> Tuple[Optional[str], Optional[str]]:
    for issue_id in sorted(issue_ids):
        item = work_item_map.get(issue_id) or {}
        title = _clean_text(item.get("title"))
        if title:
            item_type = _clean_text(item.get("type")) or "issue"
            return item_type, title

    for pr_id in sorted(pr_ids):
        pr = pr_map.get(pr_id) or {}
        title = _clean_text(pr.get("title"))
        if title:
            return "pr", title

    for commit_id in sorted(commit_ids):
        commit = commit_map.get(commit_id) or {}
        message = _clean_text(commit.get("message"))
        if message:
            return "commit", _first_line(message)

    if issue_ids:
        for issue_id in sorted(issue_ids):
            item = work_item_map.get(issue_id) or {}
            item_type = _clean_text(item.get("type"))
            return (item_type or "issue"), None
        return "issue", None
    if pr_ids:
        return "pr", None
    if commit_ids:
        return "commit", None
    return None, None


def _resolve_repo_ids(
    sink: BaseMetricsSink,
    repo_ids: Optional[List[str]],
    team_ids: Optional[List[str]],
) -> Optional[List[str]]:
    if repo_ids:
        return repo_ids
    if team_ids:
        return resolve_repo_ids_for_teams(sink, team_ids=team_ids)
    return None


async def materialize_investments(config: MaterializeConfig) -> Dict[str, int]:
    sink = create_sink(config.dsn)
    provider_instance = None
    try:
        sink.ensure_schema()

        # Initialize LLM provider once (reusing connection pool)
        provider_instance = get_provider(config.llm_provider, model=config.llm_model)

        repo_ids = _resolve_repo_ids(sink, config.repo_ids, config.team_ids)
        edges = fetch_work_graph_edges(sink, repo_ids=repo_ids)
        components = _build_components(edges)
        if not components:
            logger.info(
                "No work graph components found for investment materialization."
            )
            return {"components": 0, "records": 0, "quotes": 0}

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

        work_items = fetch_work_items(sink, work_item_ids=issue_ids)
        active_hours = fetch_work_item_active_hours(sink, work_item_ids=issue_ids)
        repo_prs = _group_prs_by_repo(pr_ids)
        prs = fetch_pull_requests(sink, repo_numbers=repo_prs)
        repo_commits = _group_commits_by_repo(commit_ids)
        commits = fetch_commits(sink, repo_commits=repo_commits)
        commit_churn = fetch_commit_churn(sink, repo_commits=repo_commits)

        work_item_map = {str(item.get("work_item_id")): item for item in work_items}
        pr_map = _map_prs(prs)
        commit_map = _map_commits(commits)
        pr_churn = _pr_churn_map(prs)

        parent_ids = {
            str(item.get("parent_id") or "")
            for item in work_items
            if item.get("parent_id")
        }
        epic_ids = {
            str(item.get("epic_id") or "") for item in work_items if item.get("epic_id")
        }
        parent_titles = fetch_parent_titles(sink, work_item_ids=parent_ids)
        epic_titles = fetch_parent_titles(sink, work_item_ids=epic_ids)

        records: List[WorkUnitInvestmentRecord] = []
        quote_records: List[WorkUnitInvestmentEvidenceQuoteRecord] = []
        run_id = uuid.uuid4().hex
        computed_at = datetime.now(timezone.utc)
        model_version = config.llm_model or config.llm_provider

        logger.info(
            "Materializing investments for %d components (run_id=%s)",
            len(components),
            run_id,
        )
        for idx, (nodes, component_edges) in enumerate(components):
            if idx % 10 == 0:
                logger.info("Processing component %d/%d", idx, len(components))
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

            bounds = compute_time_bounds(unit_nodes, work_item_map, pr_map, commit_map)
            if bounds is None:
                continue
            if bounds.end < config.from_ts or bounds.start >= config.to_ts:
                continue

            unit_id = work_unit_id(unit_nodes)
            bundle = build_text_bundle(
                issue_ids=issue_node_ids,
                pr_ids=pr_node_ids,
                commit_ids=commit_node_ids,
                work_item_map=work_item_map,
                pr_map=pr_map,
                commit_map=commit_map,
                parent_titles=parent_titles,
                epic_titles=epic_titles,
                work_unit_id=unit_id,
            )

            if bundle.text_char_count < MIN_EVIDENCE_CHARS:
                outcome = fallback_outcome("insufficient_evidence")
            elif bundle.text_source_count == 0:
                outcome = fallback_outcome("no_text_sources")
            else:
                logger.info("Calling categorize_text_bundle for unit %s", unit_id)
                outcome = await categorize_text_bundle(
                    bundle,
                    llm_provider=config.llm_provider,
                    llm_model=config.llm_model,
                    provider=provider_instance,
                )
                logger.info("categorize_text_bundle returned status=%s", outcome.status)

            theme_distribution = rollup_subcategories_to_themes(outcome.subcategories)
            evidence_quality_value = compute_evidence_quality(
                text_bundle=bundle,
                nodes_count=len(unit_nodes),
                edges=component_edges,
            )
            if outcome.status == "invalid_llm_output":
                evidence_quality_value = min(float(evidence_quality_value), 0.3)
            evidence_band = evidence_quality_band(float(evidence_quality_value))

            effort_metric, effort_value = _effort_from_work_unit(
                issue_ids=issue_node_ids,
                pr_ids=pr_node_ids,
                commit_ids=commit_node_ids,
                pr_churn=pr_churn,
                commit_churn=commit_churn,
                active_hours=active_hours,
            )

            structural_evidence = {
                "issues": sorted(issue_node_ids),
                "prs": sorted(pr_node_ids),
                "commits": sorted(commit_node_ids),
                "edges": sorted(
                    edge_id
                    for edge_id in (
                        str(edge.get("edge_id") or "") for edge in component_edges
                    )
                    if edge_id
                ),
            }

            repo_id = None
            repo_candidates = _collect_repo_ids(component_edges)
            if len(repo_candidates) == 1:
                repo_id = _parse_repo_id(repo_candidates[0])

            provider = _collect_provider(issue_node_ids, work_item_map)
            work_unit_type, work_unit_name = _resolve_work_unit_label(
                issue_ids=issue_node_ids,
                pr_ids=pr_node_ids,
                commit_ids=commit_node_ids,
                work_item_map=work_item_map,
                pr_map=pr_map,
                commit_map=commit_map,
            )

            records.append(
                WorkUnitInvestmentRecord(
                    work_unit_id=unit_id,
                    work_unit_type=work_unit_type,
                    work_unit_name=work_unit_name,
                    from_ts=bounds.start,
                    to_ts=bounds.end,
                    repo_id=repo_id,
                    provider=provider,
                    effort_metric=effort_metric,
                    effort_value=effort_value,
                    theme_distribution_json=theme_distribution,
                    subcategory_distribution_json=outcome.subcategories,
                    structural_evidence_json=json.dumps(structural_evidence),
                    evidence_quality=evidence_quality_value,
                    evidence_quality_band=evidence_band,
                    categorization_status=outcome.status,
                    categorization_errors_json=json.dumps(outcome.errors),
                    categorization_model_version=model_version,
                    categorization_input_hash=bundle.input_hash,
                    categorization_run_id=run_id,
                    computed_at=computed_at,
                )
            )

            if config.persist_evidence_snippets and outcome.evidence_quotes:
                for quote in outcome.evidence_quotes:
                    quote_records.append(
                        WorkUnitInvestmentEvidenceQuoteRecord(
                            work_unit_id=unit_id,
                            quote=quote.quote,
                            source_type=quote.source_type,
                            source_id=quote.source_id,
                            computed_at=computed_at,
                            categorization_run_id=run_id,
                        )
                    )

        logger.info("Finished component loop, writing %d records to sink", len(records))
        if records:
            sink.write_work_unit_investments(records)
        if quote_records:
            sink.write_work_unit_investment_quotes(quote_records)
        logger.info("Sink write complete")

        return {
            "components": len(components),
            "records": len(records),
            "quotes": len(quote_records),
        }
    finally:
        sink.close()
        if provider_instance:
            await provider_instance.aclose()
