"""Resolver for work graph edge queries."""

from __future__ import annotations

import logging
import re
from typing import Any

from dev_health_ops.api.services.identity import looks_like_uuid

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.inputs import WorkGraphEdgeFilterInput
from ..models.outputs import (
    PageInfo,
    WorkGraphEdgeResult,
    WorkGraphEdgesResult,
    WorkGraphEdgeType,
    WorkGraphNodeType,
    WorkGraphProvenance,
)

logger = logging.getLogger(__name__)

_OPAQUE_HEX_ID_RE = re.compile(r"^[0-9a-f]{24,}$", re.IGNORECASE)


def _map_node_type(value: str) -> WorkGraphNodeType:
    try:
        return WorkGraphNodeType(value.lower())
    except ValueError:
        return WorkGraphNodeType.ISSUE


def _map_edge_type(value: str) -> WorkGraphEdgeType:
    try:
        return WorkGraphEdgeType(value.lower())
    except ValueError:
        return WorkGraphEdgeType.RELATES


def _map_provenance(value: str) -> WorkGraphProvenance:
    try:
        return WorkGraphProvenance(value.lower())
    except ValueError:
        return WorkGraphProvenance.HEURISTIC


def _display_name_for(entity_id: str) -> str | None:
    """A7/A8: pass through human-readable ids; return None for bare UUIDs.

    Non-UUID identifiers (e.g. PROJ-123, INC-001, deploy-xyz) are already
    human-readable and are surfaced verbatim. UUID-style identifiers that
    cannot be resolved server-side return None so the client renders a
    controlled Unresolved badge rather than leaking a raw UUID.
    """
    raw = str(entity_id).strip()
    if not raw:
        return None
    return None if looks_like_uuid(raw) or _OPAQUE_HEX_ID_RE.match(raw) else raw


def _row_to_edge(row: dict[str, Any]) -> WorkGraphEdgeResult:
    source_id = str(row.get("source_id", ""))
    target_id = str(row.get("target_id", ""))
    return WorkGraphEdgeResult(
        edge_id=str(row.get("edge_id", "")),
        source_type=_map_node_type(str(row.get("source_type", "issue"))),
        source_id=source_id,
        source_display_name=_display_name_for(source_id),
        target_type=_map_node_type(str(row.get("target_type", "issue"))),
        target_id=target_id,
        target_display_name=_display_name_for(target_id),
        edge_type=_map_edge_type(str(row.get("edge_type", "relates"))),
        provenance=_map_provenance(str(row.get("provenance", "heuristic"))),
        confidence=float(row.get("confidence", 0.0)),
        evidence=str(row.get("evidence", "")),
        repo_id=str(row.get("repo_id")) if row.get("repo_id") else None,
        provider=str(row.get("provider")) if row.get("provider") else None,
    )


async def resolve_work_graph_edges(
    context: GraphQLContext,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> WorkGraphEdgesResult:
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    limit = filters.limit if filters else 1000
    params: dict[str, Any] = {"limit": int(limit), "org_id": org_id}
    where_clauses: list[str] = ["org_id = %(org_id)s"]

    if filters:
        if filters.repo_ids:
            where_clauses.append("repo_id IN %(repo_ids)s")
            params["repo_ids"] = filters.repo_ids

        if filters.source_type:
            where_clauses.append("source_type = %(source_type)s")
            params["source_type"] = filters.source_type.value

        if filters.target_type:
            where_clauses.append("target_type = %(target_type)s")
            params["target_type"] = filters.target_type.value

        if filters.edge_type:
            where_clauses.append("edge_type = %(edge_type)s")
            params["edge_type"] = filters.edge_type.value

        if filters.node_id:
            where_clauses.append("(source_id = %(node_id)s OR target_id = %(node_id)s)")
            params["node_id"] = filters.node_id

    where_sql = f"WHERE {' AND '.join(where_clauses)}"

    query = f"""
        SELECT
            edge_id,
            source_type,
            source_id,
            target_type,
            target_id,
            edge_type,
            toString(repo_id) AS repo_id,
            provider,
            provenance,
            confidence,
            evidence
        FROM work_graph_edges
        {where_sql}
        LIMIT %(limit)s
    """

    rows = await query_dicts(client, query, params)
    edges = [_row_to_edge(row) for row in rows]

    return WorkGraphEdgesResult(
        edges=edges,
        total_count=len(edges),
        page_info=PageInfo(
            has_next_page=len(edges) == limit,
            has_previous_page=False,
            start_cursor=edges[0].edge_id if edges else None,
            end_cursor=edges[-1].edge_id if edges else None,
        ),
    )
