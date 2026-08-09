"""CHAOS-3617: canonical records -> a backend-neutral graph projection.

This module is **pure**: no graph client, no network, no clock, no model.
It takes an :class:`~.records.IngestionBatch` and returns a
:class:`GraphProjection` — nodes and edges with deterministic identities —
which ``backend`` then writes to Graphiti verbatim. Splitting it out this
way is what makes the arm's most important properties testable without a
live store: identity preservation, direction preservation, tenant isolation
and the no-prose rule are all decided here.

Three rules are enforced rather than documented.

**No prose.** :func:`build_projection` rejects any attribute value that is a
string longer than :data:`MAX_ATTRIBUTE_CHARS`, and rejects attribute keys
outside ``[a-z][a-z0-9_]*``. There is nowhere in a structured record for a
sentence to live, so "structured records must not be converted to
hand-authored prose" cannot be violated by an adapter that means well and
writes a nice summary. Unstructured documents are carried through untouched
in a separate collection that the structured writer never reads.

**No reversed relationships.** Every relationship record is checked against
the frozen ``RELATIONSHIP_ALLOWLIST``'s declared canonical orientation
before it becomes an edge. A record stating ``TEAM -owned_by_team-> PROJECT``
is rejected at ingestion. The alternative — storing it and letting
``LineageHop.validate_direction_matches_allowlist`` catch it at emission —
would mean the graph contained a lie that only some queries surfaced.

**No dangling endpoints.** A relationship whose source or target entity is
not in the batch and not already declared is rejected. An edge to an entity
nobody declared is how a path acquires a node the authorization filter never
saw.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    RELATIONSHIP_ALLOWLIST,
    RelationshipType,
    SubjectMatchSignal,
)

from . import identity
from .budgets import DEFAULT_BUDGETS, TrialBudgets
from .records import (
    AliasRecord,
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
    RelationshipRecord,
    UnstructuredDocumentRecord,
    validate_batch_org,
)
from .vocabulary import (
    AliasKind,
    GraphEntityKind,
    GraphObservationKind,
    entity_kind_to_subject_kind,
)

__all__ = [
    "ALIAS_SIGNAL",
    "MAX_ATTRIBUTE_CHARS",
    "PROJECTION_VERSION",
    "GraphEdge",
    "GraphNode",
    "GraphProjection",
    "ProjectionError",
    "build_projection",
]

#: Bumped whenever the record -> node/edge mapping changes in a way that
#: makes an existing store's contents no longer reproducible from the same
#: inputs. Emitted on the packet as ``versions.projection_version``, so a
#: recorded trial run can always be tied back to the mapping that produced
#: it. Must satisfy ``PlatformVersionToken``.
PROJECTION_VERSION = "graph_arm_projection.v1"

#: The longest string a structured attribute value may be. 256 characters is
#: comfortably more than any identifier, status token or provider key, and
#: comfortably less than a sentence anyone would call a summary.
MAX_ATTRIBUTE_CHARS = 256

_ATTRIBUTE_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

#: How an alias kind becomes a match signal the packet can cite. Total over
#: :class:`AliasKind` — checked by :func:`_validate_alias_signal_totality` at
#: import time, because an unmapped alias kind would silently degrade to the
#: weak ``FUZZY_LABEL`` signal, which the contract refuses to accept as the
#: sole basis for a subject commitment.
ALIAS_SIGNAL: Mapping[AliasKind, SubjectMatchSignal] = {
    AliasKind.ALIAS: SubjectMatchSignal.ALIAS,
    AliasKind.ACRONYM: SubjectMatchSignal.ACRONYM,
    AliasKind.PREVIOUS_NAME: SubjectMatchSignal.PREVIOUS_NAME,
    AliasKind.PROVIDER_IDENTIFIER: SubjectMatchSignal.PROVIDER_IDENTIFIER,
}


class ProjectionError(ValueError):
    """A structured record could not be projected, and why."""


@dataclass(frozen=True, slots=True)
class GraphNode:
    """One node, addressed deterministically, with its canonical id intact.

    ``uuid`` is a storage address (see :mod:`.identity`); ``canonical_id`` is
    the identity. Both are stored, and every read path recovers the canonical
    id from the node rather than parsing the uuid.
    """

    uuid: str
    org_id: str
    partition: str
    entity_kind: GraphEntityKind | None
    observation_kind: GraphObservationKind | None
    canonical_id: str
    display_label: str
    source_class: SourceClass
    observed_at: datetime
    aliases: tuple[AliasRecord, ...] = ()
    attributes: Mapping[str, str | int | float | bool | None] = field(
        default_factory=dict
    )
    repository_ids: tuple[str, ...] = ()
    valid_from: datetime | None = None
    valid_to: datetime | None = None

    def __post_init__(self) -> None:
        if (self.entity_kind is None) == (self.observation_kind is None):
            raise ProjectionError(
                f"node {self.canonical_id!r} must be exactly one of an entity "
                "or an observation; a node that is both could be traversed as "
                "a lineage endpoint and cited as evidence for itself"
            )

    @property
    def is_entity(self) -> bool:
        return self.entity_kind is not None


@dataclass(frozen=True, slots=True)
class GraphEdge:
    """One relationship edge, stored in the allowlist's canonical orientation.

    There is no ``direction`` field. The store holds relationships one way —
    the canonical way — and *traversal* direction is decided at read time and
    recorded on the emitted ``LineageHop``. Storing a direction alongside the
    endpoints would make "reversed" representable in the data, which is
    exactly the fault mode this arm must never exhibit.
    """

    uuid: str
    org_id: str
    partition: str
    relationship: RelationshipType
    source_uuid: str
    source_kind: GraphEntityKind
    source_canonical_id: str
    target_uuid: str
    target_kind: GraphEntityKind
    target_canonical_id: str
    source_class: SourceClass
    observed_at: datetime
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    contributor_count: int | None = None
    observation_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GraphProjection:
    """Everything one batch becomes, plus what it deliberately did not."""

    org_id: str
    partition: str
    projection_version: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    #: Observation node uuid -> the entity node uuids it was observed on.
    #: Internal attachment, never emitted as a ``LineageHop``.
    observation_attachments: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    #: Approved unstructured documents, untouched. Carried so a later
    #: extraction pass has them; the structured writer never reads this.
    approved_documents: tuple[UnstructuredDocumentRecord, ...] = ()
    #: Documents that arrived unapproved and were dropped before extraction
    #: could see them, by canonical id. Recorded rather than silently
    #: discarded so a reproduction can prove the drop happened.
    rejected_document_ids: tuple[str, ...] = ()
    truncated: bool = False
    truncation_detail: str = ""

    def entity_nodes(self) -> tuple[GraphNode, ...]:
        return tuple(node for node in self.nodes if node.is_entity)

    def observation_nodes(self) -> tuple[GraphNode, ...]:
        return tuple(node for node in self.nodes if not node.is_entity)


def _validate_attributes(
    where: str, attributes: Mapping[str, str | int | float | bool | None]
) -> None:
    for key, value in attributes.items():
        if not _ATTRIBUTE_KEY.fullmatch(key):
            raise ProjectionError(
                f"{where} declares attribute key {key!r}; structured "
                "attribute keys are snake_case tokens, so a key cannot carry "
                "authored text either"
            )
        if isinstance(value, str) and len(value) > MAX_ATTRIBUTE_CHARS:
            raise ProjectionError(
                f"{where} attribute {key!r} is {len(value)} characters, over "
                f"the {MAX_ATTRIBUTE_CHARS}-character structured-value bound. "
                "Structured records are ingested as structured facts; prose "
                "belongs in an approved unstructured document, not in an "
                "attribute"
            )


def _validate_label(where: str, label: str) -> None:
    if not label.strip():
        raise ProjectionError(f"{where} has an empty display label")
    if len(label) > MAX_ATTRIBUTE_CHARS:
        raise ProjectionError(
            f"{where} label is {len(label)} characters, over the "
            f"{MAX_ATTRIBUTE_CHARS}-character bound; a label that long is a "
            "sentence, and a structured record does not author sentences"
        )


def _check_orientation(record: RelationshipRecord) -> None:
    orientation = RELATIONSHIP_ALLOWLIST[record.relationship]
    source_kind = entity_kind_to_subject_kind(record.source.kind)
    target_kind = entity_kind_to_subject_kind(record.target.kind)
    if source_kind is None or target_kind is None:
        raise ProjectionError(
            f"relationship {record.relationship} connects "
            f"{record.source.kind} -> {record.target.kind}; the organization "
            "partition root is not a relationship endpoint"
        )
    if not orientation.permits(source_kind, target_kind):
        raise ProjectionError(
            f"relationship {record.source.canonical_id} "
            f"-[{record.relationship}]-> {record.target.canonical_id} "
            f"contradicts the frozen canonical orientation "
            f"({orientation.canonical_reading}); {source_kind} -> "
            f"{target_kind} is not a declared ordering. Storing it would put "
            "a reversed relationship in the graph, which no read path could "
            "then distinguish from a true one"
        )
    if (
        record.source.kind == record.target.kind
        and record.source.canonical_id == record.target.canonical_id
    ):
        raise ProjectionError(
            f"relationship {record.relationship} on "
            f"{record.source.canonical_id} points at itself; a self-loop "
            "explains nothing and inflates path recall"
        )


def _entity_node(record: EntityRecord, partition: str) -> GraphNode:
    where = f"entity {record.canonical_id!r}"
    _validate_label(where, record.display_label)
    _validate_attributes(where, record.attributes)
    return GraphNode(
        uuid=identity.node_uuid(record.org_id, record.kind, record.canonical_id),
        org_id=record.org_id,
        partition=partition,
        entity_kind=record.kind,
        observation_kind=None,
        canonical_id=record.canonical_id,
        display_label=record.display_label,
        source_class=record.source_class,
        observed_at=record.observed_at,
        aliases=record.aliases,
        attributes=dict(record.attributes),
        repository_ids=record.repository_ids,
        valid_from=record.valid_from,
        valid_to=record.valid_to,
    )


def _observation_node(record: ObservationRecord, partition: str) -> GraphNode:
    where = f"observation {record.canonical_id!r}"
    _validate_label(where, record.title)
    _validate_attributes(where, record.attributes)
    attributes: dict[str, str | int | float | bool | None] = dict(record.attributes)
    if record.outcome is not None:
        if len(record.outcome) > MAX_ATTRIBUTE_CHARS:
            raise ProjectionError(
                f"{where} outcome is {len(record.outcome)} characters, over "
                f"the {MAX_ATTRIBUTE_CHARS}-character bound; an outcome is a "
                "source-asserted token, not a write-up"
            )
        attributes["outcome"] = record.outcome
    if record.supersedes:
        attributes["supersedes"] = ",".join(sorted(record.supersedes))
    if record.prior_attempt_ids:
        attributes["prior_attempt_ids"] = ",".join(sorted(record.prior_attempt_ids))
    _validate_attributes(where, attributes)
    return GraphNode(
        uuid=identity.observation_uuid(record.org_id, record.kind, record.canonical_id),
        org_id=record.org_id,
        partition=partition,
        entity_kind=None,
        observation_kind=record.kind,
        canonical_id=record.canonical_id,
        display_label=record.title,
        source_class=record.source_class,
        observed_at=record.observed_at,
        attributes=attributes,
        repository_ids=record.repository_ids,
    )


def _key(ref: CanonicalRef) -> tuple[GraphEntityKind, str]:
    return (ref.kind, ref.canonical_id)


def build_projection(
    batch: IngestionBatch, *, budgets: TrialBudgets = DEFAULT_BUDGETS
) -> GraphProjection:
    """Project one organization's structured records into nodes and edges.

    Order of checks is deliberate and each one is a fault mode:

    1. **org homogeneity** — a foreign record never reaches the store at all;
    2. **record budget** — a batch over budget truncates *and says so*,
       rather than being written half-way with no record of where it stopped;
    3. **duplicate canonical ids** — the same canonical id declared twice
       with different labels is a genuine ambiguity, not something to
       last-write-wins;
    4. **orientation** — see :func:`_check_orientation`;
    5. **dangling endpoints** — an edge to an undeclared entity;
    6. **observation attachment** — an observation attached to nothing.
    """

    validate_batch_org(batch)

    truncated = False
    truncation_detail = ""
    outcome = budgets.check_ingest_records(batch.record_count())
    if not outcome.within_budget:
        truncated = True
        truncation_detail = outcome.detail

    partition = identity.partition_for_org(batch.org_id)

    nodes: list[GraphNode] = []
    entity_index: dict[tuple[GraphEntityKind, str], GraphNode] = {}
    for record in batch.entities:
        node = _entity_node(record, partition)
        key = (record.kind, record.canonical_id)
        existing = entity_index.get(key)
        if existing is not None:
            if existing.display_label != node.display_label:
                raise ProjectionError(
                    f"entity {record.canonical_id!r} ({record.kind}) is "
                    f"declared twice with different labels "
                    f"({existing.display_label!r} and {node.display_label!r}); "
                    "silently keeping one would make the emitted label depend "
                    "on ingestion order"
                )
            continue
        entity_index[key] = node
        nodes.append(node)

    observation_index: dict[str, GraphNode] = {}
    attachments: dict[str, tuple[str, ...]] = {}
    for observation in batch.observations:
        if not observation.subjects:
            raise ProjectionError(
                f"observation {observation.canonical_id!r} names no subject "
                "entity; unattached evidence displaces lineage rather than "
                "adding to it, and the packet's evidence index refuses it"
            )
        node = _observation_node(observation, partition)
        if observation.canonical_id in observation_index:
            continue
        missing = [
            subject.canonical_id
            for subject in observation.subjects
            if _key(subject) not in entity_index
        ]
        if missing:
            raise ProjectionError(
                f"observation {observation.canonical_id!r} is about entities "
                f"the batch never declared: {sorted(missing)}"
            )
        observation_index[observation.canonical_id] = node
        nodes.append(node)
        attachments[node.uuid] = tuple(
            entity_index[_key(subject)].uuid for subject in observation.subjects
        )

    edges: list[GraphEdge] = []
    seen_edges: set[str] = set()
    for relationship_record in batch.relationships:
        _check_orientation(relationship_record)
        for ref, side in (
            (relationship_record.source, "source"),
            (relationship_record.target, "target"),
        ):
            if _key(ref) not in entity_index:
                raise ProjectionError(
                    f"relationship {relationship_record.relationship} names a {side} "
                    f"entity the batch never declared: {ref.kind}/"
                    f"{ref.canonical_id}. An edge to an undeclared entity is "
                    "how a path acquires a node the authorization filter "
                    "never saw"
                )
        unknown_observations = sorted(
            set(relationship_record.observation_ids) - set(observation_index)
        )
        if unknown_observations:
            raise ProjectionError(
                f"relationship {relationship_record.relationship} between "
                f"{relationship_record.source.canonical_id} and {relationship_record.target.canonical_id} "
                f"cites observations the batch never declared: "
                f"{unknown_observations}"
            )
        if (
            relationship_record.contributor_count is not None
            and relationship_record.contributor_count < 0
        ):
            raise ProjectionError(
                f"relationship {relationship_record.relationship} declares a negative "
                "contributor_count"
            )
        source_node = entity_index[_key(relationship_record.source)]
        target_node = entity_index[_key(relationship_record.target)]
        edge_uuid = identity.relationship_uuid(
            relationship_record.org_id,
            relationship_record.relationship.value,
            relationship_record.source.kind,
            relationship_record.source.canonical_id,
            relationship_record.target.kind,
            relationship_record.target.canonical_id,
        )
        if edge_uuid in seen_edges:
            continue
        seen_edges.add(edge_uuid)
        edges.append(
            GraphEdge(
                uuid=edge_uuid,
                org_id=relationship_record.org_id,
                partition=partition,
                relationship=relationship_record.relationship,
                source_uuid=source_node.uuid,
                source_kind=relationship_record.source.kind,
                source_canonical_id=relationship_record.source.canonical_id,
                target_uuid=target_node.uuid,
                target_kind=relationship_record.target.kind,
                target_canonical_id=relationship_record.target.canonical_id,
                source_class=relationship_record.source_class,
                observed_at=relationship_record.observed_at,
                valid_from=relationship_record.valid_from,
                valid_to=relationship_record.valid_to,
                contributor_count=relationship_record.contributor_count,
                observation_ids=relationship_record.observation_ids,
            )
        )

    approved = tuple(document for document in batch.documents if document.approved)
    rejected = tuple(
        document.canonical_id for document in batch.documents if not document.approved
    )

    return GraphProjection(
        org_id=batch.org_id,
        partition=partition,
        projection_version=PROJECTION_VERSION,
        nodes=tuple(nodes),
        edges=tuple(edges),
        observation_attachments=attachments,
        approved_documents=approved,
        rejected_document_ids=rejected,
        truncated=truncated,
        truncation_detail=truncation_detail,
    )


def _validate_alias_signal_totality() -> None:
    missing = sorted(kind.value for kind in AliasKind if kind not in ALIAS_SIGNAL)
    if missing:
        raise RuntimeError(
            "these alias kinds have no match signal and would silently "
            f"degrade to the weak FUZZY_LABEL signal: {missing}"
        )


_validate_alias_signal_totality()
