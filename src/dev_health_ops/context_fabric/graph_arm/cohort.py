"""CHAOS-3617 PR2: bounded comparison-cohort construction.

Answers "which subjects belong in this comparison, and why" for a committed
subject, using only edges already in the graph.

**Every member states a basis, and the basis is a path.** The frozen contract
refuses a cohort member carrying neither evidence nor an explicit
no-evidence classification, because an unjustified member is how an unrelated
project joins a comparison unnoticed. This builder goes further and requires
the *reason* to be a real traversal: a peer is included because it shares an
owning team, a portfolio, an initiative or a dependency with the subject —
never because it happened to be nearby.

**The comparison must be possible, not merely populated.** A cohort-bearing
shape needs at least two members and at least one comparison dimension, and
this builder derives the dimensions from what the cohort's members actually
have in common rather than asserting a fixed list. A cohort that names a
dimension nothing supports is a comparison the reader has to perform
themselves — the "dashboard redirect" fault in cohort clothing.

**Exclusions are results.** A subject considered and rejected is recorded
with a reason, because "why is X not in this comparison" is a question the
packet exists to answer and an absence answers nothing.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from dev_health_ops.api.dev.investigation_contract import (
    CohortExclusionReason,
    CohortInclusionBasis,
    ComparisonDimension,
    RelationshipType,
)

from .projection import GraphEdge
from .vocabulary import GraphEntityKind

__all__ = [
    "ANCHOR_RELATIONSHIPS",
    "COHORT_BEARING_RELATIONSHIPS",
    "PEER_RELATIONSHIPS",
    "CohortCandidate",
    "CohortEntryMode",
    "CohortExclusionRecord",
    "CohortProposal",
    "build_cohort",
]


class CohortEntryMode(StrEnum):
    """How the arm arrived at this cohort.

    Two modes, and telling them apart is load-bearing rather than
    descriptive: they differ in whether the packet has a *subject* at all.

    ``SUBJECT_ANCHORED`` is :func:`build_cohort` -- a subject was resolved
    from the question and its peers were walked out from there, so every
    member's rationale is "shares X with that subject" and the packet commits
    to the subject.

    ``SCOPE_ENUMERATED`` is :func:`~.cohort_discovery.discover_cohort` -- the
    question named no subject, because a cohort question has none to name
    ("which teams are struggling" contains no reference to resolve). The
    members were enumerated from the principal's authorized scope, so a
    member's rationale is what it shares with the OTHER MEMBERS, and the
    packet commits to no subject. The emitter must NOT manufacture one: the
    frozen contract's only vocabulary for a committed candidate's match
    signal describes matching a reference the question supplied, and there
    was no reference. A pivot invented to satisfy the shape would put a
    fabricated ``exact_canonical_id`` match into a scored packet.

    Carried on the proposal rather than inferred from an empty
    ``subject_id``, because "" is also what a bug produces and the packet
    builder relaxes a real guard on the strength of this value.
    """

    SUBJECT_ANCHORED = "subject_anchored"
    SCOPE_ENUMERATED = "scope_enumerated"


#: Edges to a shared *anchor*: subject -> anchor <- peer. The anchor is the
#: team, portfolio or initiative both sides hang off, and it is named in the
#: member's rationale so a reader can see which one.
#:
#: Deliberately small. Every entry answers "these two are alike in a way a
#: reader would accept as a reason to compare them"; ``depends_on`` is absent
#: because A depending on B makes them *related*, not *peers*, and a cohort
#: built from it compares a project with its own database.
ANCHOR_RELATIONSHIPS: Mapping[RelationshipType, CohortInclusionBasis] = {
    RelationshipType.OWNED_BY_TEAM: CohortInclusionBasis.SHARED_TEAM_OWNERSHIP,
    RelationshipType.BELONGS_TO_PORTFOLIO: CohortInclusionBasis.SAME_PORTFOLIO,
    RelationshipType.CONTRIBUTES_TO: CohortInclusionBasis.SAME_INITIATIVE,
}

#: Edges that assert peerhood *directly*: the edge already means "these two
#: are comparable", so there is no anchor to name.
#:
#: Splitting these out is not cosmetic. Treating a direct peer edge as an
#: anchor edge makes the peer an anchor, and an anchor is excluded from its
#: own cohort — which silently dropped the single most obviously comparable
#: subject there is, the one the graph explicitly says shares a dependency.
#: The first version of this module did exactly that and the corpus caught
#: it: ``proj_beacon`` vanished from ``proj_identity_rewrite``'s cohort.
PEER_RELATIONSHIPS: Mapping[RelationshipType, CohortInclusionBasis] = {
    RelationshipType.SHARES_DEPENDENCY_WITH: CohortInclusionBasis.SHARED_DEPENDENCY,
}

#: Every relationship that can put a subject in a cohort, and its basis.
#: Union of the two above; the two maps must stay disjoint, because a
#: relationship read both ways would make membership depend on loop order.
COHORT_BEARING_RELATIONSHIPS: Mapping[RelationshipType, CohortInclusionBasis] = {
    **ANCHOR_RELATIONSHIPS,
    **PEER_RELATIONSHIPS,
}

#: Which comparison dimension each shared basis can actually support.
#:
#: Every dimension here is one the *graph* can speak to — counts of edges,
#: presence of dependencies, breadth of ownership. Metric-valued dimensions
#: (cycle time, throughput) are deliberately absent: the graph does not
#: measure them, canonical services do, and a cohort claiming to support a
#: dimension it cannot compute would be exactly the unsupported-comparison
#: fault the trial is looking for.
_BASIS_DIMENSIONS: Mapping[CohortInclusionBasis, tuple[ComparisonDimension, ...]] = {
    CohortInclusionBasis.SHARED_TEAM_OWNERSHIP: (
        ComparisonDimension.DEPENDENCY_EXPOSURE,
        ComparisonDimension.DATA_COVERAGE,
    ),
    CohortInclusionBasis.SAME_PORTFOLIO: (
        ComparisonDimension.DEPENDENCY_EXPOSURE,
        ComparisonDimension.DATA_COVERAGE,
    ),
    CohortInclusionBasis.SAME_INITIATIVE: (ComparisonDimension.DATA_COVERAGE,),
    CohortInclusionBasis.SHARED_DEPENDENCY: (ComparisonDimension.DEPENDENCY_EXPOSURE,),
}


@dataclass(frozen=True, slots=True)
class CohortCandidate:
    """One subject considered for the cohort, included or not."""

    canonical_id: str
    kind: GraphEntityKind
    display_label: str
    #: Each basis paired with the anchors that justify *it*, in order.
    #:
    #: Per-basis rather than one flat anchor list, because a peer commonly
    #: holds two bases at once and a flat list makes the rationale claim the
    #: portfolio anchors the shared dependency too. An empty anchor tuple
    #: means the basis came from a :data:`PEER_RELATIONSHIPS` edge, which
    #: asserts peerhood directly and has nothing in between to name.
    basis_anchors: tuple[tuple[CohortInclusionBasis, tuple[str, ...]], ...]

    @property
    def bases(self) -> tuple[CohortInclusionBasis, ...]:
        return tuple(basis for basis, _ in self.basis_anchors)

    @property
    def via(self) -> tuple[str, ...]:
        """Every anchor, flattened. For assertions that do not care which."""

        return tuple(
            sorted({anchor for _, anchors in self.basis_anchors for anchor in anchors})
        )


@dataclass(frozen=True, slots=True)
class CohortExclusionRecord:
    """A subject considered for the cohort and deliberately left out."""

    canonical_id: str
    kind: GraphEntityKind
    reason: CohortExclusionReason
    rationale: str


@dataclass(frozen=True, slots=True)
class CohortProposal:
    """A proposed comparison cohort, its exclusions and what it can compare."""

    subject_id: str
    members: tuple[CohortCandidate, ...]
    exclusions: tuple[CohortExclusionRecord, ...]
    dimensions: tuple[ComparisonDimension, ...]
    truncated: bool
    #: How many peers the size bound dropped. Carried as a count rather than
    #: as one exclusion each: the frozen contract bounds the exclusion list
    #: too, so a large drop would overflow the very channel meant to disclose
    #: it. The truncation flag plus this count is the disclosure.
    truncated_count: int
    authorization_filtered_count: int
    #: Which entry mode produced this proposal. Defaults to the original
    #: subject-anchored one so every existing caller keeps its meaning and
    #: the relaxation below has to be asked for explicitly.
    entry_mode: CohortEntryMode = CohortEntryMode.SUBJECT_ANCHORED

    @property
    def is_comparable(self) -> bool:
        """Whether this actually supports a comparison.

        One peer and one dimension. The frozen contract's floor is *two
        cohort members*, and the subject is the other one — a proposal holds
        peers only, so requiring two peers here would refuse a legitimate
        one-to-one comparison the contract accepts. Without a dimension the
        cohort is a list the reader has to compare themselves, which is the
        dashboard-redirect fault in cohort clothing.
        """

        return bool(self.members) and bool(self.dimensions)


def build_cohort(
    subject_id: str,
    edges: Sequence[GraphEdge],
    entity_labels: Mapping[str, tuple[GraphEntityKind, str]],
    authorized_entity_ids: Sequence[str] | frozenset[str],
    *,
    max_members: int = 49,
    max_exclusions: int = 50,
) -> CohortProposal:
    """Propose peers of ``subject_id`` that share a comparable relationship.

    Peers are found two hops out: subject -> shared anchor -> peer, over the
    :data:`COHORT_BEARING_RELATIONSHIPS` only. Two hops is not a tuning
    choice — it is what "shares an owning team" *means*, and a deeper walk
    stops describing similarity and starts describing reachability.

    Authorization is applied to the anchor as well as the peer. A peer
    reached only through a team the caller cannot see is a peer whose
    membership discloses that team, which is the same leak the traversal
    guards against.
    """

    authorized = frozenset(authorized_entity_ids)
    if subject_id not in authorized:
        return CohortProposal(
            subject_id=subject_id,
            members=(),
            exclusions=(),
            dimensions=(),
            truncated=False,
            truncated_count=0,
            authorization_filtered_count=0,
        )

    subject_kind = entity_labels.get(subject_id, (None, ""))[0]

    # peer -> basis -> the anchors that justify it. An empty anchor set means
    # the edge asserted peerhood directly and there is nothing in between.
    candidates: dict[str, dict[CohortInclusionBasis, set[str]]] = {}
    withheld: set[str] = set()

    # anchor -> basis, for anchors the subject is attached to.
    anchors: dict[str, CohortInclusionBasis] = {}
    for edge in edges:
        for near, far in (
            (edge.source_canonical_id, edge.target_canonical_id),
            (edge.target_canonical_id, edge.source_canonical_id),
        ):
            if near != subject_id or far == subject_id:
                continue
            anchor_basis = ANCHOR_RELATIONSHIPS.get(edge.relationship)
            peer_basis = PEER_RELATIONSHIPS.get(edge.relationship)
            if anchor_basis is None and peer_basis is None:
                continue
            if far not in authorized:
                withheld.add(far)
                continue
            if anchor_basis is not None:
                anchors[far] = anchor_basis
            if peer_basis is not None:
                candidates.setdefault(far, {}).setdefault(peer_basis, set())

    for edge in edges:
        basis = ANCHOR_RELATIONSHIPS.get(edge.relationship)
        if basis is None:
            continue
        for anchor_side, peer in (
            (edge.target_canonical_id, edge.source_canonical_id),
            (edge.source_canonical_id, edge.target_canonical_id),
        ):
            if anchors.get(anchor_side) is not basis:
                continue
            if peer == subject_id or peer in anchors:
                continue
            if peer not in authorized:
                withheld.add(peer)
                continue
            candidates.setdefault(peer, {}).setdefault(basis, set()).add(anchor_side)

    considered: list[CohortCandidate] = []
    exclusions: list[CohortExclusionRecord] = []
    for peer in sorted(candidates):
        kind, label = entity_labels.get(peer, (None, ""))
        if kind is None:
            continue
        if subject_kind is not None and kind is not subject_kind:
            # A project is not comparable with the repository that implements
            # it. Recorded rather than dropped, because "why is this not in
            # the comparison" is a question the packet exists to answer.
            exclusions.append(
                CohortExclusionRecord(
                    canonical_id=peer,
                    kind=kind,
                    reason=CohortExclusionReason.NOT_COMPARABLE_SLICE,
                    rationale=(
                        f"a {kind.value} is not comparable with the subject's "
                        f"{subject_kind.value}, so including it would compare "
                        "two different kinds of thing"
                    ),
                )
            )
            continue
        considered.append(
            CohortCandidate(
                canonical_id=peer,
                kind=kind,
                display_label=label,
                basis_anchors=tuple(
                    (basis, tuple(sorted(candidates[peer][basis])))
                    for basis in sorted(candidates[peer], key=lambda item: item.value)
                ),
            )
        )

    truncated_count = max(0, len(considered) - max_members) + max(
        0, len(exclusions) - max_exclusions
    )
    included = considered[:max_members]
    exclusions = exclusions[:max_exclusions]

    # Dimensions are derived from the members that SURVIVED the bound, not
    # from every peer ``considered``. A dimension whose only support was a
    # dropped member is a comparison the packet cannot actually make -- the
    # two lists are kept separate so that reading the wrong one is a visible
    # one-word difference rather than an ordering accident.
    dimensions: set[ComparisonDimension] = set()
    for member in included:
        for basis in member.bases:
            dimensions.update(_BASIS_DIMENSIONS.get(basis, ()))

    return CohortProposal(
        subject_id=subject_id,
        members=tuple(included),
        exclusions=tuple(exclusions),
        dimensions=tuple(sorted(dimensions, key=lambda item: item.value)),
        truncated=bool(truncated_count),
        truncated_count=truncated_count,
        authorization_filtered_count=len(withheld),
    )
