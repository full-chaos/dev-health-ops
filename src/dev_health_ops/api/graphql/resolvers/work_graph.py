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

# PR ids stored in work_graph_edges use the format "{repo_uuid}#pr{number}".
# This pattern is not a bare UUID so it slips past looks_like_uuid(), but it
# is not human-readable either — it must be resolved to the PR title.
_PR_EDGE_ID_RE = re.compile(
    r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})#pr(\d+)$",
    re.IGNORECASE,
)

# Incident status → customer-facing label.  Unknown statuses fall back to a
# neutral "Incident" label so raw enum strings never reach customer copy.
_INCIDENT_STATUS_LABELS: dict[str, str] = {
    "open": "Open",
    "triggered": "Triggered",
    "acknowledged": "Acknowledged",
    "investigating": "Investigating",
    "resolved": "Resolved",
    "closed": "Closed",
}


def _incident_label(status: str) -> str:
    """Map a raw incident status string to a normalised customer-facing label."""
    return _INCIDENT_STATUS_LABELS.get(status.lower(), "Incident")


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


def _display_name_for(
    entity_id: str, resolved: dict[str, str] | None = None
) -> str | None:
    """A7/A8: pass through human-readable ids; return None for unresolvable UUIDs.

    Resolution priority:
    1. Lookup-resolved name (from batch lookup) — takes precedence.
    2. Human-readable pass-through for non-UUID, non-hex identifiers
       (e.g. PROJ-123, INC-001, deploy-xyz).
    3. None for bare UUIDs, opaque hex strings, and UUID-based PR ids that
       were not resolved — the client renders a controlled Unresolved badge
       rather than leaking a raw UUID (A8).
    """
    raw = str(entity_id).strip()
    if not raw:
        return None
    # Lookup-resolved names (from batch lookup) take precedence.
    if resolved and raw in resolved:
        return resolved[raw]
    # UUID-based PR ids are not human-readable even though they do not match
    # the bare-UUID regex (they carry a "#pr{N}" suffix).
    if _PR_EDGE_ID_RE.match(raw):
        return None
    # Bare UUIDs and opaque hex strings are not human-readable.
    if looks_like_uuid(raw) or _OPAQUE_HEX_ID_RE.match(raw):
        return None
    # Human-readable id — pass through verbatim.
    return raw


async def _batch_resolve_display_names(
    client: Any,
    org_id: str,
    rows: list[dict[str, Any]],
) -> dict[str, str]:
    """Resolve display names for UUID-derived ids in one query per entity type.

    Collects unresolved source/target ids across the edge page, grouped by
    entity type, then issues ONE ClickHouse query per type (no N+1).
    org_id is included in every join predicate to prevent cross-tenant leaks.

    Returns a mapping {entity_id -> display_name} for all successfully
    resolved ids.  Any ids absent from the returned dict remain unresolved
    and will surface as None (→ client Unresolved badge).
    """
    from dev_health_ops.api.queries.client import query_dicts

    resolved: dict[str, str] = {}

    # Collect ids that need lookup, partitioned by entity type.
    pr_ids: set[str] = set()  # "{uuid}#pr{N}" format
    deployment_ids: set[str] = set()  # bare UUID deployment ids
    incident_ids: set[str] = set()  # bare UUID incident ids

    for row in rows:
        for id_field, type_field in (
            ("source_id", "source_type"),
            ("target_id", "target_type"),
        ):
            entity_id = str(row.get(id_field) or "").strip()
            entity_type = str(row.get(type_field) or "").lower()
            if not entity_id:
                continue

            is_pr_format = bool(_PR_EDGE_ID_RE.match(entity_id))
            is_bare_uuid = looks_like_uuid(entity_id)
            is_opaque_hex = bool(_OPAQUE_HEX_ID_RE.match(entity_id))

            # Opaque hex ids (feature_flag hashes, etc.) are not resolvable.
            if is_opaque_hex:
                continue
            # Only collect ids that need a table lookup.
            if not (is_pr_format or is_bare_uuid):
                continue

            if is_pr_format or entity_type == "pr":
                pr_ids.add(entity_id)
            elif entity_type == "deployment" and is_bare_uuid:
                deployment_ids.add(entity_id)
            elif entity_type == "incident" and is_bare_uuid:
                incident_ids.add(entity_id)

    # --- PRs: one query against git_pull_requests -------------------------
    if pr_ids:
        # Only "{uuid}#pr{N}" ids can be resolved; bare UUID pr ids cannot.
        pr_lookups: dict[str, tuple[str, int]] = {}
        repo_uuids: set[str] = set()
        for pr_id in pr_ids:
            m = _PR_EDGE_ID_RE.match(pr_id)
            if m:
                repo_uuid = m.group(1).lower()
                pr_num = int(m.group(2))
                pr_lookups[pr_id] = (repo_uuid, pr_num)
                repo_uuids.add(repo_uuid)

        if pr_lookups and repo_uuids:
            pr_numbers = sorted({pr_num for _, pr_num in pr_lookups.values()})
            try:
                pr_rows = await query_dicts(
                    client,
                    """
                    SELECT toString(repo_id) AS repo_id, number, title
                    FROM git_pull_requests FINAL
                    WHERE org_id = %(org_id)s
                      AND toString(repo_id) IN %(repo_ids)s
                      AND number IN %(pr_numbers)s
                    """,
                    {
                        "org_id": org_id,
                        "repo_ids": sorted(repo_uuids),
                        "pr_numbers": pr_numbers,
                    },
                )
                # Build (repo_id_lower, number) → title lookup.
                pr_title_map: dict[tuple[str, int], str] = {}
                for r in pr_rows:
                    repo_id = str(r.get("repo_id") or "").lower()
                    number = int(r.get("number") or 0)
                    # Keep variable name distinct from the outer loop's `resolved_title`
                    # to avoid mypy seeing str | None rebind the earlier `str` annotation.
                    row_title = str(r.get("title") or "").strip()
                    if repo_id and number and row_title:
                        pr_title_map[(repo_id, number)] = row_title

                for pr_id, (repo_uuid, pr_num) in pr_lookups.items():
                    resolved_title: str | None = pr_title_map.get((repo_uuid, pr_num))
                    if resolved_title:
                        resolved[pr_id] = resolved_title
            except Exception:
                logger.warning("PR display-name lookup failed", exc_info=True)

    # --- Deployments: one query against deployments -----------------------
    if deployment_ids:
        dep_ids = sorted(deployment_ids)
        try:
            dep_rows = await query_dicts(
                client,
                """
                SELECT deployment_id, environment
                FROM deployments FINAL
                WHERE org_id = %(org_id)s
                  AND deployment_id IN %(dep_ids)s
                """,
                {"org_id": org_id, "dep_ids": dep_ids},
            )
            for r in dep_rows:
                dep_id = str(r.get("deployment_id") or "")
                env = str(r.get("environment") or "").strip()
                # Only store a label when we have a meaningful environment string.
                # Empty env → omit from resolved so _display_name_for returns None
                # (Unresolved badge) rather than leaking the raw UUID (A8).
                if dep_id and env:
                    resolved[dep_id] = f"{env} deploy"
        except Exception:
            logger.warning("Deployment display-name lookup failed", exc_info=True)

    # --- Incidents: one query against incidents ---------------------------
    if incident_ids:
        inc_ids = sorted(incident_ids)
        try:
            inc_rows = await query_dicts(
                client,
                """
                SELECT incident_id, status
                FROM incidents FINAL
                WHERE org_id = %(org_id)s
                  AND incident_id IN %(inc_ids)s
                """,
                {"org_id": org_id, "inc_ids": inc_ids},
            )
            for r in inc_rows:
                inc_id = str(r.get("incident_id") or "")
                status = str(r.get("status") or "").strip()
                # Empty status → omit from resolved (→ Unresolved badge, not raw UUID).
                # Known statuses are normalised to customer-facing labels; unknown
                # statuses map to the neutral "Incident" label via _incident_label().
                if inc_id and status:
                    resolved[inc_id] = f"incident ({_incident_label(status)})"
        except Exception:
            logger.warning("Incident display-name lookup failed", exc_info=True)

    return resolved


def _row_to_edge(
    row: dict[str, Any], resolved: dict[str, str] | None = None
) -> WorkGraphEdgeResult:
    source_id = str(row.get("source_id", ""))
    target_id = str(row.get("target_id", ""))
    return WorkGraphEdgeResult(
        edge_id=str(row.get("edge_id", "")),
        source_type=_map_node_type(str(row.get("source_type", "issue"))),
        source_id=source_id,
        source_display_name=_display_name_for(source_id, resolved),
        target_type=_map_node_type(str(row.get("target_type", "issue"))),
        target_id=target_id,
        target_display_name=_display_name_for(target_id, resolved),
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
    resolved = await _batch_resolve_display_names(client, org_id, rows)
    edges = [_row_to_edge(row, resolved) for row in rows]

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
