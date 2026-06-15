"""No-LLM membership backfill (CHAOS-2439).

The daily scheduled job must NOT re-run LLM categorization (cost + category
drift). Instead it cheaply PROJECTS ``work_unit_membership`` from the theme /
subcategory distributions ALREADY persisted in ``work_unit_investments`` by the
post-sync LLM materializer.

Per org, with NO categorizer/LLM call:

1. Rebuild the work-graph connected components from ``work_graph_edges`` (reuse
   ``_build_components`` + ``work_unit_id`` so the unit hashing matches the
   materializer exactly).
2. Read the LATEST ``work_unit_investments`` row per ``work_unit_id`` (argMax on
   ``computed_at``, the same latest-per-unit semantics as
   ``api/queries/work_unit_investments.py``) for those unit ids to recover the
   persisted ``theme_distribution_json`` / ``subcategory_distribution_json``.
3. Project membership rows via the SHARED ``build_membership_records`` helper, so
   the rows are byte-for-byte identical to what the LLM materializer would emit
   for the same distributions. A fresh ``computed_at`` is stamped; the resolver's
   per-node latest-run guard then supersedes any stale rows.
4. Write via ``sink.write_work_unit_memberships``.

SKIP-ON-CHURNED-COMPONENT: a unit whose CURRENT component hash has no matching
``work_unit_investments`` row is skipped. That happens when edges churned since
the last categorization (the node moved to a new component, so its new
``work_unit_id`` was never categorized). Those units are intentionally left to
the post-sync full ``build -> run_investment_materialize`` (LLM) chain, which
runs on new data and categorizes the new component. The backfill only refreshes
membership for components that ALREADY have categorization — it never invents
categories.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.metrics.schemas import WorkUnitMembershipRecord
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.work_graph.investment.membership import (
    NodeKey,
    build_membership_records,
)
from dev_health_ops.work_graph.investment.queries import fetch_work_graph_edges
from dev_health_ops.work_graph.investment.utils import work_unit_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MembershipBackfillConfig:
    dsn: str
    org_id: str | None = None
    repo_ids: list[str] | None = None


def _build_components_for_backfill(
    edges: list[dict[str, Any]],
) -> list[list[NodeKey]]:
    """Connected-component node lists from work_graph_edges.

    Mirrors ``materialize._build_components`` (same union-find over
    source/target endpoints) but returns only the per-component node lists — the
    backfill does not need the component edges. Kept local to avoid importing the
    heavy ``materialize`` module (and its LLM provider deps) into the worker.
    """
    adjacency: dict[NodeKey, list[NodeKey]] = {}
    for edge in edges:
        source = (str(edge.get("source_type")), str(edge.get("source_id")))
        target = (str(edge.get("target_type")), str(edge.get("target_id")))
        adjacency.setdefault(source, []).append(target)
        adjacency.setdefault(target, []).append(source)

    visited: set[NodeKey] = set()
    components: list[list[NodeKey]] = []
    for node in adjacency:
        if node in visited:
            continue
        stack = [node]
        visited.add(node)
        component_nodes: list[NodeKey] = []
        while stack:
            current = stack.pop()
            component_nodes.append(current)
            for neighbor in adjacency.get(current, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        components.append(component_nodes)
    return components


def _fetch_latest_distributions(
    sink: BaseMetricsSink,
    *,
    work_unit_ids: list[str],
    org_id: str,
) -> dict[str, dict[str, Any]]:
    """Return {work_unit_id -> {theme/ subcategory distribution + status}}.

    Reads the LATEST row per ``work_unit_id`` (argMax on ``computed_at``), the
    same latest-per-unit semantics as ``api/queries/work_unit_investments.py``.
    Org-scoped. Returns only the unit ids that actually have a row.
    """
    if not work_unit_ids:
        return {}
    query = """
        SELECT
            work_unit_id,
            argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
            argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution_json,
            argMax(categorization_status, computed_at) AS categorization_status
        FROM work_unit_investments
        WHERE org_id = %(org_id)s
          AND work_unit_id IN %(work_unit_ids)s
        GROUP BY org_id, work_unit_id
    """
    rows = sink.query_dicts(
        query,
        {"org_id": org_id, "work_unit_ids": work_unit_ids},
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        wid = str(row.get("work_unit_id") or "")
        if wid:
            result[wid] = row
    return result


def _as_distribution(value: Any) -> dict[str, float]:
    """Coerce a ClickHouse Map(String, Float64) cell into a plain dict."""
    if isinstance(value, dict):
        return {str(k): float(v) for k, v in value.items()}
    return {}


def backfill_memberships(config: MembershipBackfillConfig) -> dict[str, int]:
    """Project work_unit_membership from existing work_unit_investments (no LLM).

    Returns a stats dict: components seen, units matched (had a persisted
    categorization), units skipped (churned component / never categorized), and
    membership rows written.
    """
    sink = create_sink(config.dsn)
    org_id = config.org_id or ""
    try:
        sink.ensure_schema()

        edges = fetch_work_graph_edges(sink, repo_ids=config.repo_ids, org_id=org_id)
        components = _build_components_for_backfill(edges)
        if not components:
            logger.info(
                "Membership backfill: no work graph components for org=%s", org_id
            )
            return {
                "components": 0,
                "matched": 0,
                "skipped": 0,
                "memberships": 0,
            }

        # Map each current work_unit_id -> its node list.
        unit_nodes_by_id: dict[str, list[NodeKey]] = {}
        for nodes in components:
            unit_nodes = list(dict.fromkeys(nodes))
            uid = work_unit_id(unit_nodes)
            # Multiple components cannot collide on the hash (sha256 of sorted
            # node tokens); last-writer is fine if they somehow do.
            unit_nodes_by_id[uid] = unit_nodes

        distributions = _fetch_latest_distributions(
            sink,
            work_unit_ids=list(unit_nodes_by_id.keys()),
            org_id=org_id,
        )

        computed_at = datetime.now(timezone.utc)
        membership_records: list[WorkUnitMembershipRecord] = []
        matched = 0
        skipped = 0
        for uid, unit_nodes in unit_nodes_by_id.items():
            persisted = distributions.get(uid)
            if persisted is None:
                # SKIP-ON-CHURNED-COMPONENT: this component hash was never
                # categorized (edges churned since last LLM run). The post-sync
                # build->materialize(LLM) chain covers it; the backfill never
                # invents categories.
                skipped += 1
                continue
            matched += 1
            membership_records.extend(
                build_membership_records(
                    unit_nodes=unit_nodes,
                    work_unit_id=uid,
                    theme_distribution=_as_distribution(
                        persisted.get("theme_distribution_json")
                    ),
                    subcategory_distribution=_as_distribution(
                        persisted.get("subcategory_distribution_json")
                    ),
                    categorization_status=str(
                        persisted.get("categorization_status") or ""
                    ),
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )

        if membership_records:
            sink.write_work_unit_memberships(membership_records)

        logger.info(
            "Membership backfill org=%s: components=%d matched=%d skipped=%d "
            "memberships=%d (no LLM)",
            org_id,
            len(components),
            matched,
            skipped,
            len(membership_records),
        )
        return {
            "components": len(components),
            "matched": matched,
            "skipped": skipped,
            "memberships": len(membership_records),
        }
    finally:
        sink.close()
