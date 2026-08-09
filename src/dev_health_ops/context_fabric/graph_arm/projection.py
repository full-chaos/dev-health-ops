"""CHAOS-3617: canonical records -> a backend-neutral graph projection.

This module is **pure**: no graph client, no network, no clock, no model.
It takes an :class:`~.records.IngestionBatch` and returns a
:class:`GraphProjection` — nodes and edges with deterministic identities —
which ``backend`` then writes to Graphiti verbatim. Splitting it out this
way is what makes the arm's most important properties testable without a
live store: identity preservation, direction preservation, tenant isolation
and the no-prose rule are all decided here.

Three rules are enforced rather than documented.

**No ARM-AUTHORED prose.** This is the corrected, and narrower, statement of
the rule; the earlier one ("there is nowhere for a sentence to live") was
false and adversarial review demonstrated it.

What is true: the arm never *composes* text. Every textual value it stores is
a verbatim copy of a field the source record supplied — ``display_label``,
``title``, ``outcome``, alias values — or a rejection. Nothing here formats,
concatenates, templates or summarises, and ``EntityEdge.fact`` is a
three-token rendering of canonical identifiers
(:func:`~.backend.triple_fact`). That is the property the issue's rule
actually needs: no adapter can "help" by writing a nice summary of a
structured record.

What is NOT true, and must not be claimed: that prose cannot *transit*.
``display_label`` and ``title`` are source-supplied free text bounded only at
:data:`MAX_ATTRIBUTE_CHARS` characters, so a source system whose project name
is a sentence — or contains a person's name — will have that stored and
carried into the packet. Those values are **untrusted evidence**, exactly
like any other retrieved content, and narrowing them to an identifier
grammar would reject legitimate provider labels (``fullchaos/auth-gateway``,
``Nightfall Migration``).

Attribute *values* are bounded and attribute *keys* must match
``[a-z][a-z0-9_]*``, which keeps the structured attribute map from becoming a
second free-text channel. Unstructured documents travel a separate
collection that the structured writer never reads.

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
    EVIDENCE_HANDLE_PATTERN,
    SOURCE_EVIDENCE_ENTITY_ATTRIBUTE,
    SOURCE_EVIDENCE_HANDLE_ATTRIBUTE,
    SOURCE_EVIDENCE_ID_ATTRIBUTE,
    AliasKind,
    GraphEntityKind,
    GraphObservationKind,
    entity_kind_to_subject_kind,
)

__all__ = [
    "ALIAS_SIGNAL",
    "MAX_ATTRIBUTE_CHARS",
    "READBACK_ATTRIBUTE_KEYS",
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

#: The attribute keys the arm commits to reading BACK out of the store.
#:
#: Deliberately a closed, declared list rather than "whatever properties the
#: node has". The live reader names its columns, so a query that returned an
#: open property map would either drag the embedding vectors back with it or
#: silently vary by what happened to be written — and the differential oracle
#: can only compare fields both readers agree exist.
#:
#: Writing an attribute outside this list is legal and lossless in the store;
#: it simply is not read. ``test_chaos_3617_structured_ingestion`` fails if
#: the corpus adapter writes a key that is not here, so "stored but silently
#: unreadable" is a build failure rather than a capability that quietly does
#: not work.
READBACK_ATTRIBUTE_KEYS: tuple[str, ...] = (
    "corpus_is_adversarial",
    "corpus_state",
    "corpus_trust",
    "declared_status",
    # A canonical measurement, carried verbatim. The arm cites these; it
    # never computes, aggregates or derives a number from them, and
    # ``test_chaos_3617_measurements`` enforces that structurally.
    "measurement_basis",
    "measurement_cohort_median",
    "measurement_evidence_slug",
    "measurement_metric",
    "measurement_unit",
    "measurement_value",
    # CHAOS-3627. The source-issued evidence handle and the canonical id it
    # was issued for. Read back because provenance that does not survive the
    # round trip is provenance the packet cannot cite: the builder mints its
    # own handle only where the source issued none, so a key that failed to
    # read back would silently restore the re-minting this fixed.
    SOURCE_EVIDENCE_ENTITY_ATTRIBUTE,
    SOURCE_EVIDENCE_HANDLE_ATTRIBUTE,
    SOURCE_EVIDENCE_ID_ATTRIBUTE,
    "superseded_by",
)

_ATTRIBUTE_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

#: A source-issued handle must satisfy the frozen contract's grammar before it
#: is stored. Refused at ingestion rather than repaired: a handle is an
#: identity, and an arm that trimmed, lowercased or re-derived a malformed one
#: would be inventing a different record's identity out of a broken one.
_EVIDENCE_HANDLE = re.compile(f"^{EVIDENCE_HANDLE_PATTERN}$")

#: Bytes the storage encoding uses to join multiple values into one attribute
#: string: US (0x1f) for alias lists, "," for repository ids, supersession
#: chains and prior-attempt chains.
#:
#: A source value containing one of these does not round trip -- adversarial
#: verification reproduced an alias containing US coming back as TWO aliases,
#: one of which no source ever supplied. That is worse than losing the value:
#: it manufactures one, and a later alias search would match a string nobody
#: wrote.
#:
#: Refused, not escaped, for the same reason organization ids are refused
#: rather than normalised: an escaping scheme is a second encoding to keep in
#: sync, and the first time it drifts the failure is silent and looks like
#: data. These bytes do not occur in real provider identifiers or labels.
_UNIT_SEPARATOR = "\x1f"
_LIST_SEPARATORS: tuple[tuple[str, str], ...] = (
    (_UNIT_SEPARATOR, "unit separator (0x1f)"),
    (",", "comma"),
)


#: C0 control characters, minus the unit separator handled above with its own
#: message. Refused for two independent reasons, either of which is enough:
#:
#: 1. the live store silently DROPS NUL from stored values, so the two readers
#:    disagree about what a source supplied -- the same "copied verbatim" lie
#:    the separator bytes told, with no error anywhere;
#: 2. :mod:`.identity` joins its hash inputs on NUL precisely so that
#:    ``("a", "b:c")`` and ``("a:b", "c")`` cannot collide. A canonical id
#:    containing NUL defeats exactly that guarantee, and two different
#:    relationships can then be addressed identically.
#:
#: No provider identifier or human label contains a C0 control character, so
#: this refuses nothing real.
_CONTROL_CHARACTERS = frozenset(chr(code) for code in range(0x20)) | {chr(0x7F)}


def _reject_control_characters(where: str, field: str, value: str) -> None:
    """Raise if a value carries a C0 control character."""

    found = sorted(_CONTROL_CHARACTERS & set(value))
    if not found:
        return
    codes = ", ".join(f"0x{ord(char):02x}" for char in found)
    raise ProjectionError(
        f"{where} {field} contains control characters ({codes}). The live "
        "store drops some of them silently -- so the two readers disagree "
        "about what the source supplied, with no error anywhere -- and NUL "
        "additionally defeats the NUL-separated hash inputs identity.py "
        "relies on to keep two different relationships from sharing one "
        "address. Refused rather than stripped: stripping is the silent "
        "rewrite this exists to prevent"
    )


def _reject_separator_bytes(where: str, field: str, value: str) -> None:
    """Raise if a value carries a byte the storage encoding joins on.

    Separators are checked BEFORE control characters, and the order matters
    for the error message rather than the outcome: US (0x1f) is both, and
    "this is the byte we join on" tells the reader the actual mechanism,
    where "this is a control character" would leave them guessing why.
    """

    for separator, name in _LIST_SEPARATORS:
        if separator in value:
            raise ProjectionError(
                f"{where} {field} contains a {name}, which is the byte the "
                "graph arm joins multi-valued attributes on. Storing it would "
                "split one source value into several on read -- inventing a "
                "value no source supplied -- so it is refused rather than "
                "escaped: an escaping scheme is a second encoding to keep in "
                "sync, and its first drift would look like data"
            )
    _reject_control_characters(where, field, value)


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

    # There is deliberately no ``truncated`` flag. There used to be, and it
    # was the whole defect: an over-budget batch set it and then projected
    # everything anyway, so the flag described a truncation that never
    # happened. A projection is now all-or-nothing -- over budget raises --
    # and a field that could only ever hold its default would be one more
    # thing a reader has to check and can never learn anything from.

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


def _validate_source_evidence(
    where: str, attributes: Mapping[str, str | int | float | bool | None]
) -> None:
    """Refuse a source-issued handle the arm could not honestly cite.

    Both halves or neither. A handle with no id is a citation the builder
    cannot attribute to a record, and an id with no handle is a record the
    builder would then mint its own handle for while believing it had one --
    the re-minting CHAOS-3627 exists to stop, restored by a half-populated
    pair rather than by a code change anyone would notice.
    """

    handle = attributes.get(SOURCE_EVIDENCE_HANDLE_ATTRIBUTE)
    source_id = attributes.get(SOURCE_EVIDENCE_ID_ATTRIBUTE)
    source_entity = attributes.get(SOURCE_EVIDENCE_ENTITY_ATTRIBUTE)
    if handle is None and source_id is None and source_entity is None:
        return
    if handle is not None and (not isinstance(source_entity, str) or not source_entity):
        raise ProjectionError(
            f"{where} carries a source-issued evidence handle with no "
            f"{SOURCE_EVIDENCE_ENTITY_ATTRIBUTE}. The entry describing this "
            "record must name the entity the RECORD is about, and the arm "
            "will not re-derive that from whichever observation happens to "
            "cite it"
        )
    if handle is None or source_id is None:
        raise ProjectionError(
            f"{where} declares one half of the source evidence pair "
            f"({SOURCE_EVIDENCE_HANDLE_ATTRIBUTE}="
            f"{handle!r}, {SOURCE_EVIDENCE_ID_ATTRIBUTE}={source_id!r}). A "
            "handle the packet cannot attribute to a record, or a record "
            "whose handle went missing, is provenance the arm would have to "
            "invent the other half of"
        )
    if not isinstance(handle, str) or not _EVIDENCE_HANDLE.fullmatch(handle):
        raise ProjectionError(
            f"{where} declares source evidence handle {handle!r}, which is "
            "not the frozen contract's EvidenceHandle grammar. A handle is an "
            "identity: repairing one here would attribute this record to "
            "whatever the repaired string happened to name"
        )
    if not isinstance(source_id, str) or not source_id:
        raise ProjectionError(
            f"{where} declares source evidence id {source_id!r}; the handle "
            "must name the canonical record it was issued for"
        )


def _validate_label(where: str, label: str) -> None:
    _reject_control_characters(where, "label", label)
    if not label.strip():
        raise ProjectionError(f"{where} has an empty display label")
    if len(label) > MAX_ATTRIBUTE_CHARS:
        raise ProjectionError(
            f"{where} label is {len(label)} characters, over the "
            f"{MAX_ATTRIBUTE_CHARS}-character bound. This bounds SIZE, not "
            "content: a source-supplied label is copied verbatim and may be "
            "a sentence. What the arm guarantees is that it never composes "
            "one -- see the module docstring"
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
    _reject_control_characters(where, "canonical_id", record.canonical_id)
    _validate_label(where, record.display_label)
    _validate_attributes(where, record.attributes)
    for alias in record.aliases:
        _reject_separator_bytes(where, f"alias {alias.kind.value}", alias.value)
    for repository_id in record.repository_ids:
        _reject_separator_bytes(where, "repository_ids", repository_id)
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
    _reject_control_characters(where, "canonical_id", record.canonical_id)
    _validate_label(where, record.title)
    _validate_attributes(where, record.attributes)
    for repository_id in record.repository_ids:
        _reject_separator_bytes(where, "repository_ids", repository_id)
    for superseded in record.supersedes:
        _reject_separator_bytes(where, "supersedes", superseded)
    for attempt in record.prior_attempt_ids:
        _reject_separator_bytes(where, "prior_attempt_ids", attempt)
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
    _validate_source_evidence(where, attributes)
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
    2. **record budget** — a batch over budget is REFUSED. It used to be
       annotated and then projected in full, which bounded nothing;
    3. **duplicate canonical ids** — the same canonical id declared twice
       with different labels is a genuine ambiguity, not something to
       last-write-wins;
    4. **orientation** — see :func:`_check_orientation`;
    5. **dangling endpoints** — an edge to an undeclared entity;
    6. **observation attachment** — an observation attached to nothing.
    """

    validate_batch_org(batch)

    outcome = budgets.check_ingest_records(batch.record_count())
    if not outcome.within_budget:
        # REFUSE, do not annotate. Adversarial review found this setting
        # ``truncated=True`` and then projecting the whole batch anyway: a
        # one-record budget still wrote all 19 nodes and 10 edges, so the
        # advertised work bound bounded nothing and the flag described a
        # truncation that never happened.
        #
        # Refusing rather than slicing is deliberate. A batch is a connected
        # world -- relationships reference entities, observations reference
        # subjects -- so any slice this function chose would drop endpoints
        # its own validators then reject, or worse, silently change which
        # entities exist. The caller knows how to narrow a batch; this
        # function does not.
        raise ProjectionError(
            f"{outcome.detail}; refusing to project. A batch is a connected "
            "world, so there is no slice this function could take without "
            "changing which entities exist -- narrow the batch at the reader, "
            "or raise max_ingest_records deliberately"
        )

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
            # CHAOS-3627 fix round 2, codex medium 3. This used to ``continue``
            # -- silently keeping the first record and discarding the second.
            # Refuse-don't-sanitize applies to identifiers exactly as it does
            # to values: a batch asserting two different records under one
            # canonical id is telling the arm something contradictory, and
            # picking one is the arm inventing an answer.
            #
            # It also became load-bearing. The fallback mint now discriminates
            # records BY canonical id, so its collision-freedom rests on this
            # index being injective. A silent discard here would let two
            # distinct same-kind records share an id, lose one before the mint
            # ever saw it, and leave the duplicate-handle refusal unable to
            # protect the case it exists for.
            raise ProjectionError(
                f"observation {observation.canonical_id!r} is declared twice "
                "in one batch. A canonical id names one record; keeping the "
                "first and discarding the second would drop a record no "
                "reader could then know existed, and the arm's own evidence "
                "mint identifies records by this id"
            )
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
    )


def _validate_alias_signal_totality() -> None:
    missing = sorted(kind.value for kind in AliasKind if kind not in ALIAS_SIGNAL)
    if missing:
        raise RuntimeError(
            "these alias kinds have no match signal and would silently "
            f"degrade to the weak FUZZY_LABEL signal: {missing}"
        )


_validate_alias_signal_totality()
