"""Bounded, tenant-scoped ``work_graph_neighbors.v1`` application service."""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Protocol

from dev_health_ops.api.queries.client import query_dicts

from .contracts import DevScope, ScopeResolutionOutcome
from .entitlement import AskDevEntitlementAuthorizer
from .scope_service import ScopeResolutionService, ScopeResolveRequest

SCHEMA_VERSION = "work_graph_neighbors.v1"
QUERY_VERSION = "work-graph-neighbors.v1"
MAX_ROOT_REFS = 20
MAX_NEIGHBORS = 25
MAX_TIMEOUT_SECONDS = 15.0
ALLOWED_NODE_TYPES = frozenset(
    {"issue", "pr", "commit", "file", "deployment", "incident"}
)
ALLOWED_RELATIONSHIP_TYPES = frozenset(
    {
        "references",
        "implements",
        "contains",
        "touches",
        "blocks",
        "is_blocked_by",
        "relates",
        "is_related_to",
        "duplicates",
        "is_duplicate_of",
        "parent_of",
        "child_of",
    }
)
_OPAQUE_RE = re.compile(r"^(?:[0-9a-f]{24,}|[0-9a-f-]{36})(?:#pr\d+)?$", re.I)
_RELATIONSHIP_NORMALIZATION = {
    "blocked_by": "is_blocked_by",
    "relates_to": "relates",
    "duplicate": "duplicates",
    "parent": "parent_of",
    "child": "child_of",
}


class GraphDirection(StrEnum):
    INCOMING = "incoming"
    OUTGOING = "outgoing"
    BOTH = "both"


class WorkGraphResultState(StrEnum):
    COMPLETE = "complete"
    EMPTY = "empty"
    PARTIAL = "partial"


@dataclass(frozen=True, slots=True)
class WorkGraphRootRef:
    node_type: str
    node_id: str

    def __post_init__(self) -> None:
        node_type = self.node_type.strip().lower()
        node_id = self.node_id.strip()
        if node_type not in ALLOWED_NODE_TYPES:
            raise ValueError("Unsupported work-graph root node type")
        if not node_id or len(node_id) > 512:
            raise ValueError("Work-graph root IDs must contain 1 to 512 characters")
        object.__setattr__(self, "node_type", node_type)
        object.__setattr__(self, "node_id", node_id)


@dataclass(frozen=True, slots=True)
class WorkGraphNeighborsRequest:
    scope_request: ScopeResolveRequest
    root_refs: tuple[WorkGraphRootRef, ...]
    relationship_types: tuple[str, ...]
    direction: GraphDirection = GraphDirection.BOTH
    limit: int = MAX_NEIGHBORS
    depth: int = 1
    timeout_seconds: float = MAX_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if not self.root_refs or len(self.root_refs) > MAX_ROOT_REFS:
            raise ValueError(f"Work-graph roots must contain 1 to {MAX_ROOT_REFS} refs")
        if len(set(self.root_refs)) != len(self.root_refs):
            raise ValueError("Work-graph roots must be unique")
        relationships = tuple(
            _RELATIONSHIP_NORMALIZATION.get(
                value.strip().lower(), value.strip().lower()
            )
            for value in self.relationship_types
        )
        if not relationships or not set(relationships) <= ALLOWED_RELATIONSHIP_TYPES:
            raise ValueError("Unsupported work-graph relationship type")
        if len(set(relationships)) != len(relationships):
            raise ValueError("Work-graph relationship types must be unique")
        if self.depth != 1:
            raise ValueError("Ask Dev V1 work-graph depth is fixed to one")
        if self.limit < 1 or self.limit > MAX_NEIGHBORS:
            raise ValueError(f"Work-graph limit must be between 1 and {MAX_NEIGHBORS}")
        if self.timeout_seconds <= 0 or self.timeout_seconds > MAX_TIMEOUT_SECONDS:
            raise ValueError(
                f"Work-graph timeout must be between 0 and {MAX_TIMEOUT_SECONDS} seconds"
            )
        object.__setattr__(self, "relationship_types", relationships)


@dataclass(frozen=True, slots=True)
class WorkGraphNeighborNode:
    node_type: str
    node_id: str
    display_label: str
    resolution_state: str
    repository_id: str | None


@dataclass(frozen=True, slots=True)
class WorkGraphNeighborEdge:
    edge_id: str
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    relationship_type: str
    direction: GraphDirection
    provenance: str
    confidence: float
    source_ref_id: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class WorkGraphSourceRef:
    ref_id: str
    source_table: str
    source_version: str
    watermark: datetime | None
    query_version: str = QUERY_VERSION


@dataclass(frozen=True, slots=True)
class WorkGraphNeighborsResult:
    schema_version: str
    state: WorkGraphResultState
    nodes: tuple[WorkGraphNeighborNode, ...]
    edges: tuple[WorkGraphNeighborEdge, ...]
    source_refs: tuple[WorkGraphSourceRef, ...]
    warnings: tuple[str, ...]
    total_count: int
    returned_count: int
    truncated: bool
    depth: int
    query_version: str
    watermark: datetime | None


@dataclass(frozen=True, slots=True)
class WorkGraphRawEdge:
    edge_id: str
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    relationship_type: str
    repository_id: str | None
    provenance: str
    confidence: float
    observed_at: datetime
    source_table: str
    source_version: str
    source_watermark: datetime | None
    source_label: str | None = None
    target_label: str | None = None


class WorkGraphNeighborSource(Protocol):
    async def fetch(
        self,
        *,
        org_id: str,
        scope: DevScope,
        roots: tuple[WorkGraphRootRef, ...],
        relationship_types: tuple[str, ...],
        direction: GraphDirection,
        limit: int,
    ) -> tuple[WorkGraphRawEdge, ...]: ...


class ClickHouseWorkGraphNeighborSource:
    """Read only persisted graph/dependency edges; never derive new relationships."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def fetch(
        self,
        *,
        org_id: str,
        scope: DevScope,
        roots: tuple[WorkGraphRootRef, ...],
        relationship_types: tuple[str, ...],
        direction: GraphDirection,
        limit: int,
    ) -> tuple[WorkGraphRawEdge, ...]:
        root_pairs = [(root.node_type, root.node_id) for root in roots]
        params: dict[str, Any] = {
            "org_id": org_id,
            "root_pairs": root_pairs,
            "relationship_types": list(relationship_types),
            "limit": limit + 1,
        }
        repo_clause = ""
        if scope.repositories:
            params["repo_ids"] = list(scope.repositories)
            repo_clause = " AND (repo_id IS NULL OR toString(repo_id) IN %(repo_ids)s)"
        direction_clause = _direction_clause(direction)
        graph_rows = await query_dicts(
            self._client,
            f"""
            SELECT edge_id, source_type, source_id, target_type, target_id,
                   edge_type AS relationship_type, toString(repo_id) AS repository_id,
                   provenance, confidence, discovered_at AS observed_at,
                   last_synced AS source_watermark
            FROM work_graph_edges FINAL
            WHERE org_id = %(org_id)s
              AND edge_type IN %(relationship_types)s
              AND ({direction_clause})
              {repo_clause}
            ORDER BY edge_type, source_type, source_id, target_type, target_id, edge_id
            LIMIT %(limit)s
            """,
            params,
        )
        dependency_types = [
            value
            for value in relationship_types
            if value
            in {
                "blocks",
                "is_blocked_by",
                "relates",
                "duplicates",
                "parent_of",
                "child_of",
            }
        ]
        dependency_rows: list[dict[str, Any]] = []
        if dependency_types:
            dependency_params = dict(params)
            dependency_params["dependency_types"] = _dependency_source_types(
                dependency_types
            )
            dependency_rows = await query_dicts(
                self._client,
                f"""
                SELECT source_work_item_id AS source_id,
                       target_work_item_id AS target_id,
                       relationship_type,
                       last_synced AS observed_at,
                       last_synced AS source_watermark
                FROM work_item_dependencies FINAL
                WHERE org_id = %(org_id)s
                  AND relationship_type IN %(dependency_types)s
                  AND ({_dependency_direction_clause(direction)})
                ORDER BY relationship_type, source_work_item_id, target_work_item_id
                LIMIT %(limit)s
                """,
                dependency_params,
            )
        labels = await self._issue_labels(
            org_id,
            {
                str(row.get(field) or "")
                for row in (*graph_rows, *dependency_rows)
                for field in ("source_id", "target_id")
                if row.get(field)
            },
        )
        rows = [self._graph_row(row, labels) for row in graph_rows]
        rows.extend(self._dependency_row(row, labels) for row in dependency_rows)
        return tuple(rows)

    async def _issue_labels(self, org_id: str, ids: set[str]) -> dict[str, str]:
        if not ids:
            return {}
        rows = await query_dicts(
            self._client,
            """
            SELECT work_item_id, argMax(title, last_synced) AS title
            FROM work_items
            WHERE org_id = %(org_id)s AND work_item_id IN %(ids)s
            GROUP BY work_item_id
            """,
            {"org_id": org_id, "ids": sorted(ids)},
        )
        return {
            str(row["work_item_id"]): str(row["title"])
            for row in rows
            if row.get("work_item_id") and row.get("title")
        }

    @staticmethod
    def _graph_row(row: dict[str, Any], labels: dict[str, str]) -> WorkGraphRawEdge:
        return WorkGraphRawEdge(
            edge_id=str(row["edge_id"]),
            source_type=str(row["source_type"]).lower(),
            source_id=str(row["source_id"]),
            target_type=str(row["target_type"]).lower(),
            target_id=str(row["target_id"]),
            relationship_type=str(row["relationship_type"]).lower(),
            repository_id=str(row.get("repository_id") or "") or None,
            provenance=str(row.get("provenance") or "persisted"),
            confidence=float(row.get("confidence") or 0),
            observed_at=_aware(row.get("observed_at")),
            source_table="work_graph_edges",
            source_version="work-graph-edges.v1",
            source_watermark=_aware_optional(row.get("source_watermark")),
            source_label=labels.get(str(row["source_id"])),
            target_label=labels.get(str(row["target_id"])),
        )

    @staticmethod
    def _dependency_row(
        row: dict[str, Any], labels: dict[str, str]
    ) -> WorkGraphRawEdge:
        source_id = str(row["source_id"])
        target_id = str(row["target_id"])
        relationship = _RELATIONSHIP_NORMALIZATION.get(
            str(row["relationship_type"]).lower(),
            str(row["relationship_type"]).lower(),
        )
        edge_id = hashlib.sha256(
            f"issue:{source_id}:{relationship}:issue:{target_id}".encode()
        ).hexdigest()
        return WorkGraphRawEdge(
            edge_id=edge_id,
            source_type="issue",
            source_id=source_id,
            target_type="issue",
            target_id=target_id,
            relationship_type=relationship,
            repository_id=None,
            provenance="native_dependency",
            confidence=1.0,
            observed_at=_aware(row.get("observed_at")),
            source_table="work_item_dependencies",
            source_version="work-item-dependencies.v1",
            source_watermark=_aware_optional(row.get("source_watermark")),
            source_label=labels.get(source_id),
            target_label=labels.get(target_id),
        )


class WorkGraphNeighborsService:
    def __init__(
        self,
        source: WorkGraphNeighborSource,
        entitlement: AskDevEntitlementAuthorizer,
        authorizer: ScopeResolutionService,
    ) -> None:
        self._source = source
        self._entitlement = entitlement
        self._authorizer = authorizer

    async def neighbors(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        request: WorkGraphNeighborsRequest,
    ) -> WorkGraphNeighborsResult:
        if not org_id or not permission_fingerprint:
            raise ValueError("Tenant and permission fingerprint are required")
        await self._entitlement.require(org_id)
        resolution = await self._authorizer.resolve_contract(
            org_id, permission_fingerprint, request.scope_request
        )
        scope = resolution.resolved_scope
        if scope is None or resolution.outcome not in {
            ScopeResolutionOutcome.EXACT,
            ScopeResolutionOutcome.FILTERED,
            ScopeResolutionOutcome.INHERITED,
        }:
            return _empty_result(resolution.warnings)
        _validate_roots(scope, request.root_refs)
        raw = await asyncio.wait_for(
            self._source.fetch(
                org_id=org_id,
                scope=scope,
                roots=request.root_refs,
                relationship_types=request.relationship_types,
                direction=request.direction,
                limit=request.limit,
            ),
            timeout=request.timeout_seconds,
        )
        deduped = {edge.edge_id: edge for edge in raw}
        ordered = sorted(deduped.values(), key=_edge_order)
        total_count = len(ordered)
        selected = ordered[: request.limit]
        source_refs = _source_refs(selected)
        nodes = _nodes(selected)
        truncated = total_count > len(selected)
        return WorkGraphNeighborsResult(
            schema_version=SCHEMA_VERSION,
            state=WorkGraphResultState.PARTIAL
            if truncated
            else WorkGraphResultState.COMPLETE
            if selected
            else WorkGraphResultState.EMPTY,
            nodes=nodes,
            edges=tuple(_edge(edge, request.direction) for edge in selected),
            source_refs=source_refs,
            warnings=("result_truncated",) if truncated else (),
            total_count=total_count,
            returned_count=len(selected),
            truncated=truncated,
            depth=1,
            query_version=QUERY_VERSION,
            watermark=max(
                (ref.watermark for ref in source_refs if ref.watermark is not None),
                default=None,
            ),
        )


def _validate_roots(scope: DevScope, roots: tuple[WorkGraphRootRef, ...]) -> None:
    if scope.direct_scope.value == "organization":
        return
    authorized = set(scope.repositories) | {
        entity.entity_id for entity in scope.entity_refs
    }
    if not {root.node_id for root in roots} <= authorized:
        raise ValueError("Work-graph root is outside the resolved scope")


def _empty_result(warnings: list[str]) -> WorkGraphNeighborsResult:
    return WorkGraphNeighborsResult(
        SCHEMA_VERSION,
        WorkGraphResultState.EMPTY,
        (),
        (),
        (),
        tuple(warnings),
        0,
        0,
        False,
        1,
        QUERY_VERSION,
        None,
    )


def _edge_order(edge: WorkGraphRawEdge) -> tuple[str, ...]:
    return (
        edge.relationship_type,
        edge.source_type,
        edge.source_id,
        edge.target_type,
        edge.target_id,
        edge.edge_id,
    )


def _edge(edge: WorkGraphRawEdge, direction: GraphDirection) -> WorkGraphNeighborEdge:
    ref_id = f"graph-source:{edge.source_table}"
    return WorkGraphNeighborEdge(
        edge.edge_id,
        edge.source_type,
        edge.source_id,
        edge.target_type,
        edge.target_id,
        edge.relationship_type,
        direction,
        edge.provenance,
        edge.confidence,
        ref_id,
        edge.observed_at,
    )


def _source_refs(edges: list[WorkGraphRawEdge]) -> tuple[WorkGraphSourceRef, ...]:
    refs: dict[str, WorkGraphSourceRef] = {}
    for edge in edges:
        ref_id = f"graph-source:{edge.source_table}"
        previous = refs.get(ref_id)
        watermark = (
            max(
                value
                for value in (
                    edge.source_watermark,
                    previous.watermark if previous else None,
                )
                if value is not None
            )
            if edge.source_watermark is not None or (previous and previous.watermark)
            else None
        )
        refs[ref_id] = WorkGraphSourceRef(
            ref_id,
            edge.source_table,
            edge.source_version,
            watermark,
        )
    return tuple(sorted(refs.values(), key=lambda ref: ref.ref_id))


def _nodes(edges: list[WorkGraphRawEdge]) -> tuple[WorkGraphNeighborNode, ...]:
    values: dict[tuple[str, str], WorkGraphNeighborNode] = {}
    for edge in edges:
        for node_type, node_id, label in (
            (edge.source_type, edge.source_id, edge.source_label),
            (edge.target_type, edge.target_id, edge.target_label),
        ):
            resolved_label = label or (
                node_id if not _OPAQUE_RE.match(node_id) else None
            )
            values[(node_type, node_id)] = WorkGraphNeighborNode(
                node_type,
                node_id,
                resolved_label or f"Unresolved {node_type.replace('_', ' ')}",
                "resolved" if resolved_label else "unresolved",
                edge.repository_id,
            )
    return tuple(
        sorted(
            values.values(),
            key=lambda node: (
                node.display_label.casefold(),
                node.node_type,
                node.node_id,
            ),
        )
    )


def _direction_clause(direction: GraphDirection) -> str:
    outgoing = "(source_type, source_id) IN %(root_pairs)s"
    incoming = "(target_type, target_id) IN %(root_pairs)s"
    if direction is GraphDirection.OUTGOING:
        return outgoing
    if direction is GraphDirection.INCOMING:
        return incoming
    return f"{outgoing} OR {incoming}"


def _dependency_direction_clause(direction: GraphDirection) -> str:
    outgoing = "('issue', source_work_item_id) IN %(root_pairs)s"
    incoming = "('issue', target_work_item_id) IN %(root_pairs)s"
    if direction is GraphDirection.OUTGOING:
        return outgoing
    if direction is GraphDirection.INCOMING:
        return incoming
    return f"{outgoing} OR {incoming}"


def _dependency_source_types(values: list[str]) -> list[str]:
    reverse = {
        "is_blocked_by": "blocked_by",
        "relates": "relates_to",
        "duplicates": "duplicates",
        "parent_of": "parent",
        "child_of": "child",
    }
    return sorted(
        set(values) | {reverse[value] for value in values if value in reverse}
    )


def _aware(value: object) -> datetime:
    result = _aware_optional(value)
    return result or datetime.now(timezone.utc)


def _aware_optional(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
