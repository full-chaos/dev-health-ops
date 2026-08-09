"""CHAOS-3617: the Graphiti-backed shadow investigation arm.

The first implementation that tests the CHAOS-3498 graph hypothesis itself
rather than one-shot extraction reliability. It ingests canonical Dev Health
and ACR structured records into an isolated trial graph, reads a bounded,
authorized neighbourhood back out, and emits the frozen CHAOS-3615
``ask_dev_investigation_packet.v1``.

Four constraints hold everywhere in this package:

* **shadow-only** — nothing here is reachable from a user-visible Ask Dev
  path, from ACR, or from MCP. Both flags default off
  (:mod:`.flags`);
* **Graphiti is under evaluation, not approved** — it is an optional
  dependency extra and is imported lazily, never at module scope
  (:mod:`.backend`);
* **structured records stay structured** — the structured write path makes
  no model call and stores no prose (:mod:`.projection`, :mod:`.backend`);
* **removable** — deleting this package breaks exactly two registration
  points, the optional extra and the ``EXTERNAL_DERIVED_STORES`` entry.

Reading order: :mod:`.vocabulary` (what can exist), :mod:`.records` (what is
ingested), :mod:`.identity` (how things are addressed and partitioned),
:mod:`.projection` (records to nodes and edges), :mod:`.backend` and
:mod:`.store` (the Graphiti/FalkorDB binding), :mod:`.readback` (bounded
authorized traversal), :mod:`.packet_builder` (the emitted contract).

Architecture note, including the backend-choice justification:
``docs/contribute/architecture/context-fabric-graph-arm.md``.
"""

from __future__ import annotations

from .budgets import DEFAULT_BUDGETS, TrialBudgets
from .flags import (
    GRAPH_PROJECTION_FLAG,
    GRAPH_READ_FLAG,
    graph_projection_enabled,
    graph_read_enabled,
    trial_store_config,
)
from .identity import partition_for_org
from .projection import PROJECTION_VERSION, GraphProjection, build_projection
from .readback import QUERY_VERSION, InvestigationReadout, ProjectionGraphReader
from .records import (
    AliasRecord,
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
    RelationshipRecord,
    UnstructuredDocumentRecord,
)
from .vocabulary import AliasKind, GraphEntityKind, GraphObservationKind
from .watermark import IndexWatermark

__all__ = [
    "DEFAULT_BUDGETS",
    "GRAPH_PROJECTION_FLAG",
    "GRAPH_READ_FLAG",
    "PROJECTION_VERSION",
    "QUERY_VERSION",
    "AliasKind",
    "AliasRecord",
    "CanonicalRef",
    "EntityRecord",
    "GraphEntityKind",
    "GraphObservationKind",
    "GraphProjection",
    "IndexWatermark",
    "IngestionBatch",
    "InvestigationReadout",
    "ObservationRecord",
    "ProjectionGraphReader",
    "RelationshipRecord",
    "TrialBudgets",
    "UnstructuredDocumentRecord",
    "build_projection",
    "graph_projection_enabled",
    "graph_read_enabled",
    "partition_for_org",
    "trial_store_config",
]
