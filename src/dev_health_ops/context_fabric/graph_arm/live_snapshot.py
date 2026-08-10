"""Issue 3689 (adapter PR): the live-store equivalent of ``build_projection``.

``cohort_discovery.discover_cohort`` (issue 3645/3667, D's module) takes
``nodes: Sequence[GraphNode]``/``edges: Sequence[GraphEdge]`` -- exactly
what ``build_projection`` returns from an in-memory ``IngestionBatch``. That
is the ONLY place :class:`~.projection.GraphNode`/
:class:`~.projection.GraphEdge` were ever constructed until this module:
nothing in the arm previously reconstructed them from a live partition, so
``discover_cohort`` had never actually run against production data --
:func:`~.query_service.mechanism_for` could select
``SUBJECTLESS_COHORT_DISCOVERY`` but nothing could feed it.

:func:`_live_graph_snapshot` closes that, by reading the SAME partition-scoped
queries every other live reader already uses
(:data:`~.readback._ENTITY_QUERY`/:data:`~.readback._OBSERVATION_QUERY`/
:data:`~.readback._EDGE_QUERY`) and reconstructing ``GraphNode``/
``GraphEdge`` instances that are equal, field-for-field, to what
``build_projection`` would have produced for the same batch -- proven, not
assumed, by ``test_chaos_3689_live_snapshot.py``'s differential oracle,
which writes a real fixture through the real write path and compares this
module's reconstruction against ``build_projection``'s in-memory output for
the identical batch.

**uuid is derived, never read back.** Neither ``_ENTITY_QUERY`` nor
``_EDGE_QUERY`` exposes a node's or edge's own uuid as a column (there is no
such column to add: FalkorDB's internal node id is not the arm's uuid).
Every uuid here is recomputed via :mod:`.identity`'s own deterministic
functions (``node_uuid``/``observation_uuid``/``relationship_uuid``) from
data the queries DO return -- the exact functions ``projection.py`` uses on
write, so a correct reconstruction produces the identical uuid the write
side minted, which the oracle asserts directly rather than assuming.

**Two known, pre-existing, out-of-scope gaps**, neither introduced by this
module and both documented (with reasons) in the oracle's own exclusion
list rather than silently tolerated:

* ``AliasRecord.provider`` -- ``backend.to_graphiti_nodes`` flattens an
  entity's aliases into ``cf_alias_{kind}`` VALUE-only strings and never
  persists ``provider`` at all. No live reader can recover what the write
  side never wrote; this module reconstructs every alias with
  ``provider=None``.
* ``outcome``/``supersedes``/``prior_attempt_ids`` on an observation's
  ``attributes`` -- these fold into ``node.attributes`` on the trial's
  in-memory side (``projection._observation_node``), which means they are
  written to the store as ``cf_attr_outcome``/etc. via ``to_graphiti_nodes``'s
  generic attribute loop -- but none of the three is a member of
  :data:`~.projection.READBACK_ATTRIBUTE_KEYS`, so ``_attributes_from_row``
  (the SAME helper every live reader already uses) cannot recover them
  either. This is a pre-existing gap in the declared read-back vocabulary,
  not something this adapter PR widens -- ``READBACK_ATTRIBUTE_KEYS`` is a
  shared, cross-lane vocabulary and growing it is a separate decision.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.api.dev.contracts_v2.base import SourceClass

from . import identity
from .backend import parse_triple_fact
from .projection import GraphEdge, GraphNode
from .readback import (
    _ALIAS_SEPARATOR,
    _EDGE_QUERY,
    _ENTITY_QUERY,
    _OBSERVATION_QUERY,
    _as_datetime,
    _as_optional_datetime,
    _attributes_from_row,
    _rows,
)
from .records import AliasRecord
from .vocabulary import AliasKind, GraphEntityKind, GraphObservationKind

#: Empty, matching subject_resolution.py's own convention: the one function
#: this module exports is package-private (CHAOS-3617's "no caller-supplied
#: partition parameter" guard -- test_chaos_3617_no_caller_supplied_
#: partition.py -- forces every new live-data helper taking a raw partition
#: string to be non-public), so there is nothing for __all__ to name.
__all__: list[str] = []

#: ``{kind.value}`` column suffix -> AliasKind, mirroring backend.py's
#: ``cf_alias_{kind.value}`` write convention exactly.
_ALIAS_KIND_BY_COLUMN: dict[str, AliasKind] = {
    f"alias_{kind.value}": kind for kind in AliasKind
}


def _split_multivalued(raw: object) -> tuple[str, ...]:
    """A comma-joined multi-valued property, split.

    Empty/``None`` becomes ``()`` -- "the property was never set" and "it was
    set to an empty string" must not be distinguishable outcomes, matching
    every other multi-valued property this arm reads back
    (``cf_repository_ids``, ``cf_subject_canonical_ids``).
    """

    if not raw:
        return ()
    return tuple(item for item in str(raw).split(",") if item)


def _aliases_from_row(record: dict[str, Any]) -> tuple[AliasRecord, ...]:
    """Every alias on one entity row, reassembled from its ``cf_alias_*``
    columns. ``provider`` is always ``None`` -- see the module docstring's
    "known gap": the write side never persists it.

    Sorted so the reconstruction is a function of the stored data, not of
    dict/column iteration order -- the oracle compares this against an
    in-memory reference built by iterating ``EntityRecord.aliases`` in
    a fixed, author-written order, and an unsorted reconstruction would
    report drift that is really nondeterminism.
    """

    aliases: list[AliasRecord] = []
    for column, kind in _ALIAS_KIND_BY_COLUMN.items():
        raw = record.get(column)
        if not raw:
            continue
        for value in str(raw).split(_ALIAS_SEPARATOR):
            if value:
                aliases.append(AliasRecord(kind=kind, value=value, provider=None))
    return tuple(sorted(aliases, key=lambda alias: (alias.kind.value, alias.value)))


async def _live_graph_snapshot(
    driver: Any, org_id: str, partition: str
) -> tuple[tuple[GraphNode, ...], tuple[GraphEdge, ...]]:
    """Reconstruct this partition's entities, observations and edges as
    :class:`~.projection.GraphNode`/:class:`~.projection.GraphEdge`.

    ``org_id``/``partition`` are supplied by the caller (mirroring
    :class:`~.readback.LiveGraphReader`), never read back per-row: both are
    already known before this function is called, and a caller cannot use a
    read-back value as an authorization claim any more than a query
    parameter can (see ``identity.assert_partition_matches_org``, which
    every store-derived caller already runs before reaching here).

    Edges whose source or target canonical id has no corresponding entity
    row are skipped, not raised on: ``_EDGE_QUERY`` matches ``Entity``-
    labelled nodes on both ends unconditionally, and this arm's own schema
    never connects a RELATES_TO edge to anything but an entity, so this
    should not happen against a partition this arm wrote -- but a
    ``GraphEdge`` cannot be constructed without a real
    :class:`~.vocabulary.GraphEntityKind` for both ends, and skipping is
    honest where inventing one would not be.
    """

    entity_rows = await _rows(driver, _ENTITY_QUERY, partition=partition)
    observation_rows = await _rows(driver, _OBSERVATION_QUERY, partition=partition)
    edge_rows = await _rows(driver, _EDGE_QUERY, partition=partition)

    entity_kinds: dict[str, GraphEntityKind] = {}
    nodes: list[GraphNode] = []

    for record in entity_rows:
        kind = GraphEntityKind(record["entity_kind"])
        canonical_id = record["canonical_id"]
        entity_kinds[canonical_id] = kind
        nodes.append(
            GraphNode(
                uuid=identity.node_uuid(org_id, kind, canonical_id),
                org_id=org_id,
                partition=partition,
                entity_kind=kind,
                observation_kind=None,
                canonical_id=canonical_id,
                display_label=record["display_label"],
                source_class=SourceClass(record["source_class"]),
                observed_at=_as_datetime(record["observed_at"]),
                aliases=_aliases_from_row(record),
                attributes=_attributes_from_row(record),
                repository_ids=_split_multivalued(record.get("repository_ids")),
                valid_from=_as_optional_datetime(record.get("valid_from")),
                valid_to=_as_optional_datetime(record.get("valid_to")),
            )
        )

    for record in observation_rows:
        obs_kind = GraphObservationKind(record["observation_kind"])
        if obs_kind is GraphObservationKind.DOCUMENT:
            # An approved document (issue 3632's write side) is written as
            # an observation-shaped node -- deliberately, so it is
            # BM25/vector-searchable through the same mechanism as any
            # other observation -- but it is represented in-memory as an
            # UnstructuredDocumentRecord, and GraphProjection.nodes never
            # contains one (documents live in .approved_documents, a
            # structurally distinct field/type). A faithful reconstruction
            # of what build_projection's .nodes actually contains must
            # exclude it the same way, or this function's output would
            # disagree with its own docstring's claim for every partition
            # that has ever written a document -- confirmed by the
            # differential oracle, which fails on this exact case without
            # this guard.
            continue
        canonical_id = record["canonical_id"]
        nodes.append(
            GraphNode(
                uuid=identity.observation_uuid(org_id, obs_kind, canonical_id),
                org_id=org_id,
                partition=partition,
                entity_kind=None,
                observation_kind=obs_kind,
                canonical_id=canonical_id,
                display_label=record["title"],
                source_class=SourceClass(record["source_class"]),
                observed_at=_as_datetime(record["observed_at"]),
                aliases=(),
                attributes=_attributes_from_row(record),
                repository_ids=_split_multivalued(record.get("repository_ids")),
                valid_from=_as_optional_datetime(record.get("valid_from")),
                valid_to=_as_optional_datetime(record.get("valid_to")),
            )
        )

    edges: list[GraphEdge] = []
    for record in edge_rows:
        source_id, relationship, target_id = parse_triple_fact(record["fact"])
        source_kind = entity_kinds.get(source_id)
        target_kind = entity_kinds.get(target_id)
        if source_kind is None or target_kind is None:
            continue
        contributor_count = record.get("contributor_count")
        edges.append(
            GraphEdge(
                uuid=identity.relationship_uuid(
                    org_id,
                    relationship.value,
                    source_kind,
                    source_id,
                    target_kind,
                    target_id,
                ),
                org_id=org_id,
                partition=partition,
                relationship=relationship,
                source_uuid=identity.node_uuid(org_id, source_kind, source_id),
                source_kind=source_kind,
                source_canonical_id=source_id,
                target_uuid=identity.node_uuid(org_id, target_kind, target_id),
                target_kind=target_kind,
                target_canonical_id=target_id,
                source_class=SourceClass(record["source_class"]),
                observed_at=_as_datetime(record["observed_at"]),
                valid_from=_as_optional_datetime(record.get("valid_from")),
                valid_to=_as_optional_datetime(record.get("valid_to")),
                contributor_count=(
                    int(contributor_count) if contributor_count is not None else None
                ),
                observation_ids=_split_multivalued(record.get("observation_ids")),
            )
        )

    return tuple(nodes), tuple(edges)
