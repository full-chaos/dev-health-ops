"""What the CURRENT Ask Dev investigation path can and cannot say.

CHAOS-3618's baseline is only worth measuring if it is honest, and the
dishonest version of this arm is easy to write by accident: take a native
fact that *suggests* a relationship, give it the contract's nearest
relationship type, and the packet suddenly claims lineage the run never
established. This module is the single place where that judgement is made,
written down, and made testable — every projection decision reads from a
table here rather than deciding inline, so "what the baseline claims" is
reviewable in one file instead of scattered across a projection.

Three tables, each total over its contract vocabulary and asserted at
import time (a contract vocabulary that grows without a native ruling
breaks the import rather than silently defaulting to "available"):

:data:`NATIVE_SUBJECT_KIND` / :data:`UNREACHABLE_SUBJECT_KINDS`
    ``contracts_v2.base.EntityKind`` has six members; the contract's
    :class:`InvestigationSubjectKind` has ten. The four the native path
    has no representation for at all are named, not silently absent.

:data:`NATIVE_RELATIONSHIP_CAPABILITY`
    Per contract :class:`RelationshipType`: whether the native path can
    honestly emit it, from which typed ``DevSourceContent`` slot, and — when
    it cannot — the exact mechanism that stops it.

:data:`NATIVE_QUESTION_FAMILY`
    ``(QuestionIntentID, ComparisonShape)`` to the contract's
    :class:`QuestionFamilyID`. :func:`classify_question_family` deliberately
    **takes no question text**: a signature that cannot see the question
    cannot be tuned to the trial corpus, which is what CHAOS-3618's "do not
    add bespoke case-specific logic merely to make the baseline score
    better" requires. The mapping is validated at import time against each
    family's own ``permitted_comparison_shapes``, so a hand-written entry
    that would produce an invalid packet is an import failure, not a
    validation failure at trial time.

Nothing here reads the graph arm, and nothing here is a runtime behavior
change: this module is pure data plus totality checks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
)
from dev_health_ops.api.dev.investigation_contract import (
    ALL_RELATIONSHIP_TYPES,
    QUESTION_FAMILY_REGISTRY,
    RELATIONSHIP_ALLOWLIST,
    TRIAL_SOURCE_ALLOWLIST,
    ComparisonShape,
    InvestigationSubjectKind,
    QuestionFamilyID,
    RelationshipType,
)
from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
    APPROVED_CONTENT_SLOTS,
    RELATIONSHIP_MATRIX,
)

__all__ = [
    "NATIVE_QUESTION_FAMILY",
    "NATIVE_RELATIONSHIP_CAPABILITY",
    "NATIVE_SUBJECT_KIND",
    "NATIVE_UNOBSERVED_SOURCE_CLASSES",
    "OBSERVABLE_SOURCE_CLASSES",
    "STATUS_FACT_ENTITY_KIND",
    "UNREACHABLE_SUBJECT_KINDS",
    "NativeGapMechanism",
    "NativeRelationshipCapability",
    "NativeRelationshipState",
    "classify_question_family",
    "comparison_shape_for",
]


# --------------------------------------------------------------------------
# Subject kinds
# --------------------------------------------------------------------------

#: ``EntityKind`` (the live subject vocabulary, ``contracts_v2/base.py:84``)
#: to the contract's investigation-subject vocabulary. Total over
#: ``EntityKind``; every member has an exact counterpart, so nothing here is
#: a lossy approximation.
NATIVE_SUBJECT_KIND: Mapping[EntityKind, InvestigationSubjectKind] = {
    EntityKind.REPOSITORY: InvestigationSubjectKind.REPOSITORY,
    EntityKind.PROJECT: InvestigationSubjectKind.PROJECT,
    EntityKind.WORK_UNIT: InvestigationSubjectKind.WORK_UNIT,
    EntityKind.ISSUE: InvestigationSubjectKind.ISSUE,
    EntityKind.PULL_REQUEST: InvestigationSubjectKind.PULL_REQUEST,
    EntityKind.TEAM: InvestigationSubjectKind.TEAM,
}

#: The contract subject kinds the native path can never produce, because no
#: native vocabulary carries them. Named explicitly so a reader of a native
#: packet knows an absent ``portfolio`` node is a capability gap and not an
#: empty result.
UNREACHABLE_SUBJECT_KINDS: frozenset[InvestigationSubjectKind] = frozenset(
    set(InvestigationSubjectKind) - set(NATIVE_SUBJECT_KIND.values())
)

#: The entity-type prefix a status/required-child ``fact_id`` carries
#: (``builtin_steps.py:417,429`` mint ``f"{entity_type}:{entity_id}"``;
#: the vocabulary is ``builtin_steps._STATUS_ENTITY_SOURCE_SYSTEM``, keys
#: ``issue``/``pull_request``/``work_unit``/``project``). This is the ONLY
#: native carrier of a typed *target* entity that survives into
#: ``DevInvestigationResult`` — see
#: :data:`NATIVE_RELATIONSHIP_CAPABILITY`'s work-graph rulings for what
#: happens where it does not.
STATUS_FACT_ENTITY_KIND: Mapping[str, InvestigationSubjectKind] = {
    "issue": InvestigationSubjectKind.ISSUE,
    "pull_request": InvestigationSubjectKind.PULL_REQUEST,
    "work_unit": InvestigationSubjectKind.WORK_UNIT,
    "project": InvestigationSubjectKind.PROJECT,
}


# --------------------------------------------------------------------------
# Relationship capability
# --------------------------------------------------------------------------


class NativeRelationshipState(StrEnum):
    """Whether the native path can emit a given contract relationship."""

    #: Both endpoint kinds are pinned by a typed native slot, and the fact
    #: carries real evidence handles.
    AVAILABLE = "available"
    #: Not emittable. ``gap_mechanism`` says why.
    UNREACHABLE = "unreachable"


class NativeGapMechanism(StrEnum):
    """The exact reason a contract relationship is out of native reach.

    Deliberately mechanistic. "Not implemented yet" is not a member,
    because several of these are structural properties of landed contracts
    rather than unfinished work, and a trial report that cannot tell those
    apart cannot tell a scheduling gap from a design gap.
    """

    #: An endpoint kind has no native representation at all (``portfolio``,
    #: ``initiative``, ``service``, ``dependency``).
    SUBJECT_KIND_ABSENT = "subject_kind_absent"
    #: The endpoint kinds exist, but the wire type that reaches
    #: ``DevInvestigationResult`` discards them, so the target's kind is
    #: unknown at projection time.
    ENDPOINT_KIND_DISCARDED = "endpoint_kind_discarded"
    #: Several source sub-kinds are flattened into one non-recoverable
    #: label before they reach the investigation result.
    SUB_KIND_FLATTENED = "sub_kind_flattened"
    #: No registered plan step mints content under the source class this
    #: relationship would have to come from.
    NO_REGISTERED_ADAPTER = "no_registered_adapter"
    #: The relationship needs more than one hop, and native traversal depth
    #: is fixed at one.
    TRAVERSAL_DEPTH_FIXED_AT_ONE = "traversal_depth_fixed_at_one"
    #: Both endpoint kinds are pinned and a canonical service does assert
    #: the relationship, but its carrier holds no evidence handle. An
    #: earlier revision of this table gave that case its own "available
    #: without evidence" state; the ruling (team lead, CHAOS-3618) is that
    #: available means evidence-backed, so an assertion nothing can be
    #: dereferenced against is a gap with a precise mechanism rather than a
    #: third kind of availability.
    NO_EVIDENCE_BACKING = "no_evidence_backing"


@dataclass(frozen=True, slots=True)
class NativeRelationshipCapability:
    """One contract relationship type, and the native path's honest ruling."""

    relationship: RelationshipType
    state: NativeRelationshipState
    #: The ``DevSourceContent`` slot the projection reads; ``None`` for
    #: ``UNREACHABLE``.
    content_slot: str | None
    #: The source class that mints that slot, and the native relationship
    #: token the fact carries. Both are checked at import against the LANDED
    #: relationship matrix, which is the vocabulary that actually describes
    #: what adapters produce. Without them a row could claim ``reviews`` is
    #: available from the ``pull_requests`` slot and pass every test, because
    #: ``team -> pull_request`` is an expressible endpoint pair -- the
    #: adversarial review demonstrated exactly that.
    source_class: SourceClass | None
    native_token: str | None
    #: The mechanism blocking it, for ``UNREACHABLE``; ``None`` otherwise.
    gap_mechanism: NativeGapMechanism | None
    #: Prose for the packet's own limitation/impact fields and for the
    #: architecture doc. Bounded so it can travel on a ``ShortText``.
    detail: str

    def __post_init__(self) -> None:
        reachable = self.state is not NativeRelationshipState.UNREACHABLE
        if reachable and (self.source_class is None or self.native_token is None):
            raise ValueError(
                f"{self.relationship} is {self.state} but does not say which "
                "source class and native token it reads; an unattributed "
                "availability claim cannot be checked against the matrix"
            )
        if not reachable and (
            self.source_class is not None or self.native_token is not None
        ):
            raise ValueError(f"{self.relationship} is unreachable but names a source")
        if reachable and self.content_slot is None:
            raise ValueError(
                f"{self.relationship} is {self.state} but names no content slot; "
                "an available relationship must say what it reads"
            )
        if reachable and self.gap_mechanism is not None:
            raise ValueError(
                f"{self.relationship} is {self.state} and also declares a gap "
                "mechanism; a reachable relationship has no gap"
            )
        if not reachable and self.gap_mechanism is None:
            raise ValueError(
                f"{self.relationship} is unreachable without naming the mechanism; "
                "an unexplained gap is indistinguishable from an empty result"
            )
        if not reachable and self.content_slot is not None:
            raise ValueError(
                f"{self.relationship} is unreachable but names a content slot"
            )
        if len(self.detail) > 240:
            raise ValueError(f"{self.relationship} detail exceeds the ShortText bound")


def _capability(
    relationship: RelationshipType,
    state: NativeRelationshipState,
    *,
    content_slot: str | None = None,
    source_class: SourceClass | None = None,
    native_token: str | None = None,
    gap_mechanism: NativeGapMechanism | None = None,
    detail: str,
) -> NativeRelationshipCapability:
    return NativeRelationshipCapability(
        relationship=relationship,
        state=state,
        content_slot=content_slot,
        source_class=source_class,
        native_token=native_token,
        gap_mechanism=gap_mechanism,
        detail=detail,
    )


_AVAILABLE = NativeRelationshipState.AVAILABLE
_UNREACHABLE = NativeRelationshipState.UNREACHABLE
_M = NativeGapMechanism

#: The whole honest relationship story of the native arm, one row per
#: contract relationship type. Three of twelve are emittable; the other
#: nine are out of reach, each for a named, mechanical reason.
NATIVE_RELATIONSHIP_CAPABILITY: Mapping[
    RelationshipType, NativeRelationshipCapability
] = {
    entry.relationship: entry
    for entry in (
        _capability(
            RelationshipType.IMPLEMENTED_BY,
            _AVAILABLE,
            content_slot="pull_requests",
            source_class=SourceClass.STATUS_CHANGE,
            native_token="linked_pull_request",
            detail=(
                "status_snapshot mints DevPullRequestFactV2 rows whose type pins the "
                "target kind as pull_request; the subject kind comes from the "
                "committed entity ref."
            ),
        ),
        _capability(
            RelationshipType.PARENT_OF,
            _AVAILABLE,
            content_slot="required_children",
            source_class=SourceClass.STATUS_CHANGE,
            native_token="required_child",
            detail=(
                "required_children keeps its own typed slot end to end, and each "
                "fact_id carries an entity-type prefix, so both endpoint kinds are "
                "known when subject and child are both work_unit or both issue."
            ),
        ),
        _capability(
            RelationshipType.CONTRIBUTES_TO,
            _AVAILABLE,
            content_slot="required_children",
            source_class=SourceClass.STATUS_CHANGE,
            native_token="required_child",
            detail=(
                "the same typed required_children slot read in reverse when the "
                "subject is a project: a required child work_unit/issue/pull_request "
                "contributes to it."
            ),
        ),
        _capability(
            RelationshipType.OWNED_BY_TEAM,
            _UNREACHABLE,
            gap_mechanism=_M.NO_EVIDENCE_BACKING,
            detail=(
                "DevResolutionEntry.team_attribution is a real canonical attribution "
                "and both endpoint kinds are expressible, but the ledger entry holds "
                "no evidence handle, so nothing in the packet could be dereferenced "
                "to check it."
            ),
        ),
        _capability(
            RelationshipType.BLOCKED_BY,
            _UNREACHABLE,
            gap_mechanism=_M.SUB_KIND_FLATTENED,
            detail=(
                "status_snapshot flattens declared/child/blocker facts into one "
                "status_facts list, and the executor cannot recover which sub-kind a "
                "fact came from, so a blocker is not distinguishable from a status "
                "assessment."
            ),
        ),
        _capability(
            RelationshipType.REFERENCES,
            _UNREACHABLE,
            gap_mechanism=_M.ENDPOINT_KIND_DISCARDED,
            detail=(
                "the only native carrier is a work-graph edge, and DevGraphEdge drops "
                "the source_type/target_type the neighbours service knew, so no "
                "endpoint kind survives into the investigation result."
            ),
        ),
        _capability(
            RelationshipType.DEPENDS_ON,
            _UNREACHABLE,
            gap_mechanism=_M.NO_REGISTERED_ADAPTER,
            detail=(
                "project->project is representable in kind terms, so this is not a "
                "missing subject kind: no native service emits a project-level "
                "dependency edge at all, and the work graph is work-item scoped."
            ),
        ),
        _capability(
            RelationshipType.SHARES_DEPENDENCY_WITH,
            _UNREACHABLE,
            gap_mechanism=_M.TRAVERSAL_DEPTH_FIXED_AT_ONE,
            detail=(
                "two subjects sharing a dependency is a two-hop fact, and "
                "work_graph_neighbors_service rejects any depth other than one."
            ),
        ),
        _capability(
            RelationshipType.REVIEWS,
            _UNREACHABLE,
            gap_mechanism=_M.NO_REGISTERED_ADAPTER,
            detail=(
                "SourceClass.REVIEW carries requirement not_applicable and an empty "
                "relationship vocabulary in the relationship matrix; no registered "
                "plan step mints review content."
            ),
        ),
        _capability(
            RelationshipType.DEPLOYS,
            _UNREACHABLE,
            gap_mechanism=_M.SUBJECT_KIND_ABSENT,
            detail=(
                "every allowlisted pair terminates on a service, which has no native "
                "subject kind; linked_deployment facts name a deployment record, "
                "which the contract has no node kind for either."
            ),
        ),
        _capability(
            RelationshipType.OPERATES,
            _UNREACHABLE,
            gap_mechanism=_M.NO_REGISTERED_ADAPTER,
            detail=(
                "team->service needs a service kind; team->repository is "
                "representable in kind terms but no native service emits an "
                "operates edge."
            ),
        ),
        _capability(
            RelationshipType.BELONGS_TO_PORTFOLIO,
            _UNREACHABLE,
            gap_mechanism=_M.SUBJECT_KIND_ABSENT,
            detail=(
                "portfolio has no native subject kind; the portfolio status service "
                "evaluates a list of projects and never names a portfolio entity."
            ),
        ),
    )
}


# --------------------------------------------------------------------------
# Source classes
# --------------------------------------------------------------------------

#: Trial-allowlisted source classes some registered plan step actually
#: observes. Derived from the landed relationship matrix rather than
#: hand-listed, so a newly wired adapter widens this set automatically and a
#: removed one narrows it — the drift this table could otherwise carry is
#: not expressible.
OBSERVABLE_SOURCE_CLASSES: frozenset[SourceClass] = frozenset(
    source_class
    for source_class in TRIAL_SOURCE_ALLOWLIST
    if RELATIONSHIP_MATRIX[source_class].requirement != "not_applicable"
)

#: The allowlisted source classes no registered step observes. These are
#: reported as ``MissingSource`` on every packet whose question family
#: requires them — never quietly omitted, which is what would make an
#: unobserved source read as an observed-empty one.
NATIVE_UNOBSERVED_SOURCE_CLASSES: frozenset[SourceClass] = (
    frozenset(TRIAL_SOURCE_ALLOWLIST) - OBSERVABLE_SOURCE_CLASSES
)


# --------------------------------------------------------------------------
# Question family classification
# --------------------------------------------------------------------------


def comparison_shape_for(
    *, cardinality: Cardinality, has_unresolved_mentions: bool
) -> ComparisonShape:
    """The contract comparison shape a native run actually has.

    ``ORGANIZATION_WIDE`` is the load-bearing case. The native interpreter
    reaches it two ways that mean opposite things: a genuinely org-scoped
    question (no mention was ever made) and a question whose named subject
    failed to resolve. Calling the second one ``portfolio_wide`` is exactly
    the unsafe widening ``AskDevInvestigationPacket.
    validate_no_unsafe_organization_widening`` exists to catch, so the two
    are separated here rather than downstream.
    """

    if cardinality is Cardinality.SINGULAR:
        return ComparisonShape.SINGULAR_SUBJECT
    if cardinality is Cardinality.PLURAL_COHORT:
        return ComparisonShape.EXPLICIT_COHORT
    if has_unresolved_mentions:
        return ComparisonShape.ORGANIZATION_WIDE
    return ComparisonShape.PORTFOLIO_WIDE


_F = QuestionFamilyID
_I = QuestionIntentID
_S = ComparisonShape

#: ``(intent, shape)`` to question family, for every combination the native
#: path can honestly claim. A missing key is not an oversight: it means no
#: frozen family both covers that analytical job and permits that shape, and
#: the projection reports the run as unprojectable with that exact reason
#: rather than forcing it into the nearest family.
#:
#: Five native intents appear for no shape at all — ``REMAINING_WORK``,
#: ``OBSERVED_CHANGE``, ``REGISTERED_STATISTICS``, ``METRIC_COMPARISON`` and
#: ``DATA_TRUST``. That is not an omission either. Each of those reduces to
#: reporting a metric or a source state, which every frozen family lists
#: under ``MANDATORY_PROHIBITED_REDUCTIONS`` (``single_dashboard_metric``)
#: as something an answer may not be. ``BOUNDED_INVESTIGATION`` is absent
#: because it is the interpreter's catch-all and is never plan-governed.
NATIVE_QUESTION_FAMILY: Mapping[tuple[QuestionIntentID, ComparisonShape], _F] = {
    (_I.ENTITY_STATUS, _S.SINGULAR_SUBJECT): _F.PROJECT_STATUS_DRIVERS,
    (_I.PROJECT_HEALTH, _S.SINGULAR_SUBJECT): _F.PROJECT_STATUS_DRIVERS,
    (_I.PROJECT_HEALTH, _S.EXPLICIT_COHORT): _F.PRESSURE_SIGNALS,
    (_I.PROJECT_HEALTH, _S.PORTFOLIO_WIDE): _F.PRESSURE_SIGNALS,
    (_I.TEAM_HEALTH, _S.PORTFOLIO_WIDE): _F.STRUGGLING_TEAMS,
    (_I.TEAM_WORKLOAD_BALANCE, _S.EXPLICIT_COHORT): _F.PRESSURE_SIGNALS,
    (_I.TEAM_WORKLOAD_BALANCE, _S.PORTFOLIO_WIDE): _F.PRESSURE_SIGNALS,
    (_I.PORTFOLIO_STATUS, _S.SINGULAR_SUBJECT): _F.DECLARED_VERSUS_ACTUAL,
    (_I.PORTFOLIO_STATUS, _S.PORTFOLIO_WIDE): _F.DECLARED_VERSUS_ACTUAL,
    (_I.OPERATIONAL_DEFICIENCY_INVENTORY, _S.SINGULAR_SUBJECT): (
        _F.DECLARED_VERSUS_ACTUAL
    ),
    (_I.OPERATIONAL_DEFICIENCY_INVENTORY, _S.PORTFOLIO_WIDE): (
        _F.DECLARED_VERSUS_ACTUAL
    ),
    # An unresolved named reference never widens into a substantive family:
    # the only family permitting ORGANIZATION_WIDE is the clarification one,
    # and reaching it is how the packet stays honest about having widened.
    (_I.ENTITY_STATUS, _S.ORGANIZATION_WIDE): _F.CLARIFICATION_AND_NO_MATCH,
    (_I.PROJECT_HEALTH, _S.ORGANIZATION_WIDE): _F.CLARIFICATION_AND_NO_MATCH,
    (_I.TEAM_HEALTH, _S.ORGANIZATION_WIDE): _F.CLARIFICATION_AND_NO_MATCH,
    (_I.TEAM_WORKLOAD_BALANCE, _S.ORGANIZATION_WIDE): _F.CLARIFICATION_AND_NO_MATCH,
    (_I.PORTFOLIO_STATUS, _S.ORGANIZATION_WIDE): _F.CLARIFICATION_AND_NO_MATCH,
    (_I.OPERATIONAL_DEFICIENCY_INVENTORY, _S.ORGANIZATION_WIDE): (
        _F.CLARIFICATION_AND_NO_MATCH
    ),
}


def classify_question_family(
    *, intent_id: QuestionIntentID, shape: ComparisonShape
) -> QuestionFamilyID | None:
    """The frozen family a native run belongs to, or ``None``.

    Takes the interpreter's structural output and nothing else. There is no
    ``question`` parameter, and adding one would be the first step of tuning
    the baseline to the trial corpus — the omission is the guarantee, and
    ``test_chaos_3618_capabilities`` asserts the signature keeps it.
    """

    return NATIVE_QUESTION_FAMILY.get((intent_id, shape))


# --------------------------------------------------------------------------
# Import-time totality
# --------------------------------------------------------------------------

_missing_relationships = sorted(
    relationship.value
    for relationship in ALL_RELATIONSHIP_TYPES
    if relationship not in NATIVE_RELATIONSHIP_CAPABILITY
)
if _missing_relationships:
    raise RuntimeError(
        "native relationship capability is missing a ruling for: "
        f"{_missing_relationships}"
    )

_unknown_relationships = sorted(
    relationship.value
    for relationship in NATIVE_RELATIONSHIP_CAPABILITY
    if relationship not in RELATIONSHIP_ALLOWLIST
)
if _unknown_relationships:
    raise RuntimeError(
        f"native capability rules a relationship the contract does not declare: "
        f"{_unknown_relationships}"
    )

_missing_subject_kinds = sorted(
    kind.value for kind in EntityKind if kind not in NATIVE_SUBJECT_KIND
)
if _missing_subject_kinds:
    raise RuntimeError(
        f"entity kinds without a subject mapping: {_missing_subject_kinds}"
    )

# A family entry that names a shape the family itself forbids would produce
# a packet `validate_question_family_obligations` rejects. Catch it at
# module load, where it is a one-line fix, rather than mid-trial.
_shape_violations = sorted(
    f"{intent.value}/{shape.value}->{family.value}"
    for (intent, shape), family in NATIVE_QUESTION_FAMILY.items()
    if shape not in QUESTION_FAMILY_REGISTRY[family].permitted_comparison_shapes
)
if _shape_violations:
    raise RuntimeError(
        f"native family mapping claims shapes its families forbid: {_shape_violations}"
    )

_unknown_families = sorted(
    family.value
    for family in set(NATIVE_QUESTION_FAMILY.values())
    if family not in QUESTION_FAMILY_REGISTRY
)
if _unknown_families:
    raise RuntimeError(
        f"native family mapping names unknown families: {_unknown_families}"
    )


# Every AVAILABLE row must be backed by the LANDED relationship matrix, not
# only by the contract's endpoint allowlist. The adversarial review showed
# the gap: `reviews` could be marked available with a `pull_requests` slot
# and pass all 29 capability tests, because `team -> pull_request` is an
# expressible pair. The matrix is the vocabulary that says what adapters
# actually mint, so it is the one that has to agree.
for _relationship, _entry in NATIVE_RELATIONSHIP_CAPABILITY.items():
    if _entry.state is not NativeRelationshipState.AVAILABLE:
        continue
    _source = _entry.source_class
    if _source is None:  # unreachable: __post_init__ forbids it
        raise RuntimeError(f"{_relationship.value} is available with no source")
    _matrix = RELATIONSHIP_MATRIX[_source]
    if _matrix.requirement == "not_applicable":
        raise RuntimeError(
            f"{_relationship.value} is available from "
            f"{_source}, which no registered plan step mints"
        )
    if _entry.native_token not in _matrix.approved_relationship_types:
        raise RuntimeError(
            f"{_relationship.value} claims native token {_entry.native_token!r}, "
            f"which {_source} does not approve"
        )
    if _entry.content_slot not in APPROVED_CONTENT_SLOTS[_source]:
        raise RuntimeError(
            f"{_relationship.value} reads slot {_entry.content_slot!r}, which "
            f"{_source} may not populate"
        )
