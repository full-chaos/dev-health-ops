"""CHAOS-3617: the graph arm's own closed vocabularies.

Three vocabularies live here, and the boundary between the first two is the
single most load-bearing modelling decision in the arm.

**Entity kinds are exactly the wire's subject kinds.** ``GraphEntityKind``
is ``InvestigationSubjectKind`` plus one partition-root member,
:data:`GraphEntityKind.ORGANIZATION`, that never reaches the wire. Nothing
else may be an entity, because a ``LineageHop``'s endpoints are typed
``InvestigationSubjectKind``: a node kind the packet cannot name is a node
kind that can never appear in a path, and inventing one would mean the arm
had built lineage it is structurally unable to explain.

**Everything else is an observation, not an entity.** Incidents,
deployments, releases, CI runs, reviews, commits, decisions, documents and
ACR agent episodes are all ingested — the issue requires them — but they
attach to entities as :class:`GraphObservationKind` nodes and surface in the
packet as ``InvestigationEvidenceEntry`` items, never as hop endpoints. That
is not a limitation being worked around; it is the frozen contract's own
shape. "Incident context association" means *a path between entities whose
evidence is an incident*, not *a hop that lands on an incident*.

**There are no person ENTITIES, and no person-derived rankings.** That is
the precise claim, narrowed after adversarial review showed the looser one
("a person identity cannot reach the graph") to be false.

What is true: no ``GraphEntityKind`` and no ``GraphObservationKind`` names an
individual, so a person can never be a node, a traversal endpoint, a cohort
member or a ranked subject. Contribution and membership are ingested as
team-level association with a contributor *count* (see
``records.RelationshipRecord.contributor_count``), never as person nodes.
``allowlists.TRIAL_SOURCE_RATIONALE`` records the matching read-side rule for
``COGNITIVE_LOAD``: "team-level only ... because no person subject kind
exists".

What is NOT true: that a person's *name* cannot appear. Source-supplied
labels, titles and outcomes are copied verbatim, so a review titled "Ada
Lovelace requested changes" is stored and carried into the packet as
untrusted evidence. Nothing ranks it, nothing aggregates it per person, and
no structure in this arm can express a person as a subject — and the
corpus's ``zero_person_level_ranking`` oracle scores the downstream behaviour
that this vocabulary alone cannot guarantee.
"""

from __future__ import annotations

from enum import StrEnum

from dev_health_ops.api.dev.investigation_contract import InvestigationSubjectKind

__all__ = [
    "ALL_ALIAS_KINDS",
    "ALL_GRAPH_ENTITY_KINDS",
    "ALL_GRAPH_OBSERVATION_KINDS",
    "EMITTABLE_ENTITY_KINDS",
    "EVIDENCE_HANDLE_PATTERN",
    "SOURCE_EVIDENCE_ENTITY_ATTRIBUTE",
    "SOURCE_EVIDENCE_HANDLE_ATTRIBUTE",
    "SOURCE_EVIDENCE_ID_ATTRIBUTE",
    "AliasKind",
    "GraphEntityKind",
    "GraphObservationKind",
    "entity_kind_to_subject_kind",
]

#: The attribute an ingested observation carries the **source-issued**
#: evidence handle in, and the canonical id that handle was issued for.
#:
#: CHAOS-3627. Provenance is not a formatting question: a handle identifies
#: one source record, so an arm that re-mints one has changed the identity of
#: the evidence it is presenting and no consumer can join it back to the
#: system that issued it. The pair is carried rather than the handle alone
#: because an observation may cite a record it is not itself — a canonical
#: measurement names the record that evidences the number — and "which record
#: is this handle about" is then a question the packet can answer without
#: re-deriving anything.
#:
#: Absent on any source that issues no handle of its own. That is a real
#: case, not a fallback for convenience: the arm's own fixtures have no
#: issuing source, and for them the platform's ``EvidenceReferenceSigner``
#: mints the handle exactly as it always did.
SOURCE_EVIDENCE_HANDLE_ATTRIBUTE = "source_evidence_handle"
SOURCE_EVIDENCE_ID_ATTRIBUTE = "source_evidence_id"

#: The entity the source record the handle names is **about**.
#:
#: The third member of the pair, added in CHAOS-3627's fix round after both
#: reviewers measured the same defect: an observation that CITES a record can
#: be reached when the record itself is not, and the builder was describing
#: the entry with the citing observation's subject while carrying the
#: record's handle. 33 of 96 packets in the arm's own sweep -- 115 of 291 in
#: the verifier's -- carried at least one entry whose ``entity_id``
#: contradicted the world record its handle named, and the corpus oracle is
#: structurally blind to it: it compares the handle's world record against
#: the grant and the packet's entity against the world, never the two against
#: each other.
#:
#: Carried rather than derived because the arm must not re-derive what a
#: record is about -- that is the source's statement, and re-deriving it is
#: the same class of mistake as re-minting the handle.
SOURCE_EVIDENCE_ENTITY_ATTRIBUTE = "source_evidence_entity_id"

#: The frozen contract's ``EvidenceHandle`` grammar: ``ev1_`` and 40 lowercase
#: hex characters. Repeated here so an ingested handle can be refused at the
#: door rather than at packet validation, where the record that carried it is
#: no longer in scope and the error names only the packet.
EVIDENCE_HANDLE_PATTERN = r"ev1_[0-9a-f]{40}"


class GraphEntityKind(StrEnum):
    """Node kinds that may be endpoints of a relationship.

    Every member except :data:`ORGANIZATION` maps 1:1 onto an
    ``InvestigationSubjectKind``. ``ORGANIZATION`` exists so the projection
    has an explicit partition root to hang authorization on, and is filtered
    out of every emitted packet by
    :func:`entity_kind_to_subject_kind` returning ``None`` for it.
    """

    ORGANIZATION = "organization"
    TEAM = "team"
    PROJECT = "project"
    PORTFOLIO = "portfolio"
    INITIATIVE = "initiative"
    REPOSITORY = "repository"
    SERVICE = "service"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    DEPENDENCY = "dependency"


ALL_GRAPH_ENTITY_KINDS: tuple[GraphEntityKind, ...] = tuple(GraphEntityKind)

#: Entity kinds that may appear on the wire, i.e. every kind except the
#: partition root.
EMITTABLE_ENTITY_KINDS: frozenset[GraphEntityKind] = frozenset(
    kind for kind in GraphEntityKind if kind is not GraphEntityKind.ORGANIZATION
)


def entity_kind_to_subject_kind(
    kind: GraphEntityKind,
) -> InvestigationSubjectKind | None:
    """The wire subject kind for a graph entity kind, or ``None`` if unemittable.

    Deliberately a lookup by *value* against the frozen enum rather than a
    hand-maintained mapping: a member added to :class:`GraphEntityKind`
    without a matching ``InvestigationSubjectKind`` returns ``None`` here and
    is caught by ``validate_vocabularies`` at import time, instead of
    silently becoming a node kind no packet can describe.
    """

    if kind is GraphEntityKind.ORGANIZATION:
        return None
    return InvestigationSubjectKind(kind.value)


class GraphObservationKind(StrEnum):
    """What was observed *about* entities. Never a relationship endpoint.

    These are the structured records the issue requires ingested — reviews,
    CI outcomes, deployments, releases, incidents, decisions, documents and
    the whole ACR agent-episode family — and they carry the evidence
    identity a packet cites. An observation node connects to entities via
    ``OBSERVED_ON`` internal edges that are never emitted as
    ``LineageHop``s; what reaches the wire is the evidence handle.
    """

    COMMIT = "commit"
    REVIEW = "review"
    CI_RUN = "ci_run"
    TEST_REPORT = "test_report"
    DEPLOYMENT = "deployment"
    RELEASE = "release"
    INCIDENT = "incident"
    STATUS_CHANGE = "status_change"
    DECISION = "decision"
    DOCUMENT = "document"
    AGENT_EPISODE = "agent_episode"
    AGENT_TASK = "agent_task"
    AGENT_ARTIFACT = "agent_artifact"
    AGENT_OUTCOME = "agent_outcome"
    #: A canonical service's measurement, ingested so the arm can CITE the
    #: number. Deliberately its own kind: a measurement is not a status
    #: change or a review, and filing it under one of those would make it
    #: indistinguishable from the structured records the arm reasons about
    #: structurally — which is the boundary the whole trial rests on.
    MEASUREMENT = "measurement"


ALL_GRAPH_OBSERVATION_KINDS: tuple[GraphObservationKind, ...] = tuple(
    GraphObservationKind
)


class AliasKind(StrEnum):
    """How an alternative name for an entity came to exist.

    Each member maps onto a ``SubjectMatchSignal`` the packet can cite, so an
    alias-driven match can always state *which* signal matched rather than
    collapsing to the weak ``FUZZY_LABEL``. That mapping lives in
    ``projection.ALIAS_SIGNAL`` rather than here to keep this module free of
    wire imports beyond the subject-kind enum.
    """

    ALIAS = "alias"
    ACRONYM = "acronym"
    PREVIOUS_NAME = "previous_name"
    PROVIDER_IDENTIFIER = "provider_identifier"


ALL_ALIAS_KINDS: tuple[AliasKind, ...] = tuple(AliasKind)


def validate_vocabularies() -> None:
    """Raise unless every emittable entity kind has a wire subject kind.

    Called at import time. The failure this prevents is silent and
    expensive: a node kind the graph can hold but the packet cannot name
    would produce paths that are dropped at emission, so recall would look
    like a traversal problem rather than a vocabulary one.
    """

    wire_kinds = {kind.value for kind in InvestigationSubjectKind}
    unmappable = sorted(
        kind.value for kind in EMITTABLE_ENTITY_KINDS if kind.value not in wire_kinds
    )
    if unmappable:
        raise RuntimeError(
            "these graph entity kinds have no InvestigationSubjectKind and "
            f"could never appear in an emitted lineage path: {unmappable}"
        )
    unreachable = sorted(
        kind.value
        for kind in InvestigationSubjectKind
        if kind.value not in {item.value for item in EMITTABLE_ENTITY_KINDS}
    )
    if unreachable:
        raise RuntimeError(
            "the packet can name these subject kinds but the graph has no "
            f"entity kind for them, so they can never be discovered: {unreachable}"
        )
    overlap = sorted(
        {kind.value for kind in GraphEntityKind}
        & {kind.value for kind in GraphObservationKind}
    )
    if overlap:
        raise RuntimeError(
            "these names are both an entity kind and an observation kind, so "
            f"a node's identity would depend on which enum read it: {overlap}"
        )


validate_vocabularies()
