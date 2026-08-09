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
    "AliasKind",
    "GraphEntityKind",
    "GraphObservationKind",
    "entity_kind_to_subject_kind",
]


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
