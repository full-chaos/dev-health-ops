"""Typed GraphQL projections for bounded Ask Dev work-graph neighbors."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

import strawberry

from .dev_evidence import DevEvidenceScopeInput


@strawberry.enum
class DevWorkGraphDirection(Enum):
    INCOMING = "incoming"
    OUTGOING = "outgoing"
    BOTH = "both"


@strawberry.input
class DevWorkGraphRootRefInput:
    node_type: str
    node_id: strawberry.ID


@strawberry.input
class DevWorkGraphNeighborsInput:
    scope: DevEvidenceScopeInput
    root_refs: list[DevWorkGraphRootRefInput]
    relationship_types: list[str]
    direction: DevWorkGraphDirection = DevWorkGraphDirection.BOTH
    limit: int = 25
    depth: int = 1


@strawberry.type
class DevWorkGraphNeighborNode:
    node_type: str
    node_id: strawberry.ID
    display_label: str
    resolution_state: str
    repository_id: strawberry.ID | None


@strawberry.type
class DevWorkGraphNeighborEdge:
    edge_id: strawberry.ID
    source_type: str
    source_id: strawberry.ID
    target_type: str
    target_id: strawberry.ID
    relationship_type: str
    direction: DevWorkGraphDirection
    provenance: str
    confidence: float
    evidence_ref_ids: list[strawberry.ID]
    observed_at: datetime
    freshness: str


@strawberry.type
class DevWorkGraphSourceRef:
    source_table: str
    source_version: str
    watermark: datetime | None
    query_version: str


@strawberry.type
class DevWorkGraphNeighborsResult:
    schema_version: str
    state: str
    nodes: list[DevWorkGraphNeighborNode]
    edges: list[DevWorkGraphNeighborEdge]
    source_refs: list[DevWorkGraphSourceRef]
    warnings: list[str]
    total_count: int
    returned_count: int
    truncated: bool
    depth: int
    query_version: str
    watermark: datetime | None
