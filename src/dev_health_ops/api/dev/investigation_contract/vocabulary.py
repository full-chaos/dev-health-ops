"""Closed vocabularies for ``ask_dev_investigation_packet.v1`` (CHAOS-3615).

Every enum here is **closed** and every one is paired with an explicit
``ALL_*`` tuple literal. The tuple is not a convenience: totality tests
iterate it (rather than the enum object) because CodeQL's
``py/non-iterable-in-for-loop`` false-positives on ``(str, Enum)`` mixins —
the same reason ``contracts_v2`` publishes ``DEFICIENCY_CATEGORIES`` and
``investigation_plans`` publishes its literal matrices. ``ALL_*`` is proved
exhaustive against ``__members__`` in
``tests/api/dev/test_chaos_3615_vocabulary_totality.py``, so a member added
without updating the tuple is a red test, not a silent gap.

Three absences are deliberate and load-bearing:

* :class:`InvestigationSubjectKind` has **no person member**. Person-level
  productivity, health, workload and staffing ranking is prohibited by the
  correction addendum, and the cleanest enforcement is to make a person
  subject *unrepresentable* rather than to police it with a validator that a
  future producer could route around.
* :class:`ComparisonDimension` carries no per-person dimension, for the same
  reason: a cohort can be compared on delivery, review, dependency,
  operational, investment and coverage axes, never on the individuals
  inside it.
* No enum anywhere in this package names a graph backend, a graph query
  language, or a graph-store concept. The packet is backend-neutral by
  construction; ``tests/api/dev/test_chaos_3615_backend_neutrality.py``
  scans the *generated schemas* (not just the source) to keep it that way.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "ALL_ANALYTICAL_SLICES",
    "ALL_ASSERTION_BASES",
    "ALL_CLARIFICATION_NEED_KINDS",
    "ALL_COHORT_COMPLETENESS_STATES",
    "ALL_COHORT_EVIDENCE_CLASSIFICATIONS",
    "ALL_COHORT_EXCLUSION_REASONS",
    "ALL_COHORT_INCLUSION_BASES",
    "ALL_COMPARISON_DIMENSIONS",
    "ALL_COMPARISON_SHAPES",
    "ALL_CONFIDENCE_QUALIFIERS",
    "ALL_CONFLICT_RESOLUTIONS",
    "ALL_DRIVER_CATEGORIES",
    "ALL_DRIVER_EXCLUSION_REASONS",
    "ALL_DRIVER_ROLES",
    "ALL_DRIVER_STANDINGS",
    "ALL_EDGE_VALIDITY_BASES",
    "ALL_HISTORICAL_COMPARABILITY_STATES",
    "ALL_INVESTIGATION_OUTCOMES",
    "ALL_INVESTIGATION_SUBJECT_KINDS",
    "ALL_JOB_UNCERTAINTIES",
    "ALL_PACKET_LIMITATION_KINDS",
    "ALL_PACKET_SECTIONS",
    "ALL_RELATIONSHIP_DIRECTIONS",
    "ALL_RELEVANCE_STATES",
    "ALL_STAFFING_DENOMINATOR_STATES",
    "ALL_SUBJECT_COMMITMENT_STATES",
    "ALL_SUBJECT_MATCH_SIGNALS",
    "ALL_SURFACE_KINDS",
    "ALL_TRUNCATION_REASONS",
    "ALL_UNRESOLVED_MENTION_REASONS",
    "AnalyticalSlice",
    "AssertionBasis",
    "ClarificationNeedKind",
    "CohortCompleteness",
    "CohortEvidenceClassification",
    "CohortExclusionReason",
    "CohortInclusionBasis",
    "ComparisonDimension",
    "ComparisonShape",
    "ConfidenceQualifier",
    "ConflictResolution",
    "DriverCategory",
    "DriverExclusionReason",
    "DriverRole",
    "DriverStanding",
    "EdgeValidityBasis",
    "HistoricalComparability",
    "InvestigationOutcome",
    "InvestigationSubjectKind",
    "JobUncertainty",
    "PacketLimitationKind",
    "PacketSection",
    "RelationshipDirection",
    "RelevanceState",
    "StaffingDenominatorState",
    "SubjectCommitmentState",
    "SubjectMatchSignal",
    "SurfaceKind",
    "TruncationReason",
    "UnresolvedMentionReason",
]


class InvestigationSubjectKind(StrEnum):
    """What an investigation may take as a subject, cohort member or node.

    A superset of the *organizational* kinds an Ask Dev question can name,
    and a strict subset of "anything in the work graph": there is no person
    kind, by design (see the module docstring). ``EntityKind``
    (``contracts_v2.base``) is the answer-frame subject vocabulary and is
    deliberately not reused here — it carries ``pull_request``/``issue`` as
    *subjects* of a status answer, whereas this enum has to describe
    portfolio, initiative and service nodes that a status answer never takes
    as its subject but a lineage path routinely crosses.
    """

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


ALL_INVESTIGATION_SUBJECT_KINDS: tuple[InvestigationSubjectKind, ...] = (
    InvestigationSubjectKind.TEAM,
    InvestigationSubjectKind.PROJECT,
    InvestigationSubjectKind.PORTFOLIO,
    InvestigationSubjectKind.INITIATIVE,
    InvestigationSubjectKind.REPOSITORY,
    InvestigationSubjectKind.SERVICE,
    InvestigationSubjectKind.WORK_UNIT,
    InvestigationSubjectKind.ISSUE,
    InvestigationSubjectKind.PULL_REQUEST,
    InvestigationSubjectKind.DEPENDENCY,
)


class SubjectCommitmentState(StrEnum):
    """Proposed-versus-committed subject state.

    ``PROPOSED`` is the normal state during discovery, and the correction
    addendum is explicit that *exact subject commitment must not be a
    prerequisite for authorized candidate and context discovery* — so a
    packet with zero committed subjects and a populated related-context
    section is legal, and
    ``test_chaos_3615_packet_contract.py::
    test_discovery_without_any_commitment_is_legal`` pins that.
    """

    PROPOSED = "proposed"
    COMMITTED = "committed"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"


ALL_SUBJECT_COMMITMENT_STATES: tuple[SubjectCommitmentState, ...] = (
    SubjectCommitmentState.PROPOSED,
    SubjectCommitmentState.COMMITTED,
    SubjectCommitmentState.REJECTED,
    SubjectCommitmentState.AMBIGUOUS,
)


class SubjectMatchSignal(StrEnum):
    """What actually caused a candidate to match the question's reference.

    This is the anti-drift vocabulary for fault mode
    ``wrong_but_similar_subject_ranked_first``: a candidate cannot be
    committed on ``FUZZY_LABEL`` alone, because a similarly-named project is
    exactly what fuzzy label matching returns.
    """

    EXACT_CANONICAL_ID = "exact_canonical_id"
    EXACT_DISPLAY_NAME = "exact_display_name"
    ALIAS = "alias"
    ACRONYM = "acronym"
    PREVIOUS_NAME = "previous_name"
    PROVIDER_IDENTIFIER = "provider_identifier"
    CONVERSATIONAL_REFERENCE = "conversational_reference"
    SURFACE_CONTEXT_REFERENCE = "surface_context_reference"
    FUZZY_LABEL = "fuzzy_label"


ALL_SUBJECT_MATCH_SIGNALS: tuple[SubjectMatchSignal, ...] = (
    SubjectMatchSignal.EXACT_CANONICAL_ID,
    SubjectMatchSignal.EXACT_DISPLAY_NAME,
    SubjectMatchSignal.ALIAS,
    SubjectMatchSignal.ACRONYM,
    SubjectMatchSignal.PREVIOUS_NAME,
    SubjectMatchSignal.PROVIDER_IDENTIFIER,
    SubjectMatchSignal.CONVERSATIONAL_REFERENCE,
    SubjectMatchSignal.SURFACE_CONTEXT_REFERENCE,
    SubjectMatchSignal.FUZZY_LABEL,
)

#: Signals too weak to justify committing a subject on their own. A
#: ``COMMITTED`` candidate whose every signal is in this set is rejected by
#: ``SubjectDiscovery.validate_commitment_requires_a_strong_signal``.
WEAK_SUBJECT_MATCH_SIGNALS: frozenset[SubjectMatchSignal] = frozenset(
    {SubjectMatchSignal.FUZZY_LABEL}
)


class UnresolvedMentionReason(StrEnum):
    NO_CANDIDATE = "no_candidate"
    MULTIPLE_CANDIDATES = "multiple_candidates"
    AUTHORIZATION_FILTERED = "authorization_filtered"
    OUT_OF_SCOPE = "out_of_scope"


ALL_UNRESOLVED_MENTION_REASONS: tuple[UnresolvedMentionReason, ...] = (
    UnresolvedMentionReason.NO_CANDIDATE,
    UnresolvedMentionReason.MULTIPLE_CANDIDATES,
    UnresolvedMentionReason.AUTHORIZATION_FILTERED,
    UnresolvedMentionReason.OUT_OF_SCOPE,
)


class AnalyticalSlice(StrEnum):
    """Current-versus-historical slice of the investigation.

    Slice boundaries, and what each slice requires of edge validity, are
    declared once in :mod:`.allowlists` (``SLICE_BOUNDARIES``).
    """

    CURRENT = "current"
    HISTORICAL = "historical"
    CURRENT_VS_HISTORICAL = "current_vs_historical"


ALL_ANALYTICAL_SLICES: tuple[AnalyticalSlice, ...] = (
    AnalyticalSlice.CURRENT,
    AnalyticalSlice.HISTORICAL,
    AnalyticalSlice.CURRENT_VS_HISTORICAL,
)


class HistoricalComparability(StrEnum):
    """Whether a historical slice can be fairly compared at all.

    ``NOT_COMPARABLE_MISSING_EDGE_VALIDITY`` is the CHAOS-3569 state: native
    historical edge validity is not implemented, so as-of traversal cannot
    be reconstructed for those rows. The correction plan is explicit that
    such rows are **NOT COMPARABLE, not blockers** — a packet that declares
    this state is still a *valid* packet and may still be ``SUPPORTED``
    (pinned by ``test_not_comparable_historical_slice_is_valid_and_
    supported``). What it may not do is stay silent: the packet must also
    carry a ``HISTORICAL_SLICE_NOT_COMPARABLE`` limitation.
    """

    NOT_APPLICABLE = "not_applicable"
    COMPARABLE = "comparable"
    NOT_COMPARABLE_MISSING_EDGE_VALIDITY = "not_comparable_missing_edge_validity"
    NOT_COMPARABLE_MISSING_BASELINE = "not_comparable_missing_baseline"


ALL_HISTORICAL_COMPARABILITY_STATES: tuple[HistoricalComparability, ...] = (
    HistoricalComparability.NOT_APPLICABLE,
    HistoricalComparability.COMPARABLE,
    HistoricalComparability.NOT_COMPARABLE_MISSING_EDGE_VALIDITY,
    HistoricalComparability.NOT_COMPARABLE_MISSING_BASELINE,
)

#: The comparability states that oblige the packet to disclose a
#: ``HISTORICAL_SLICE_NOT_COMPARABLE`` limitation.
NOT_COMPARABLE_STATES: frozenset[HistoricalComparability] = frozenset(
    {
        HistoricalComparability.NOT_COMPARABLE_MISSING_EDGE_VALIDITY,
        HistoricalComparability.NOT_COMPARABLE_MISSING_BASELINE,
    }
)


class EdgeValidityBasis(StrEnum):
    """What backs a slice's claim that its relationships were valid *then*.

    Added after adversarial review round 1 (finding M7). ``SLICE_BOUNDARIES``
    declared that historical slices require edge validity, but nothing on the
    wire recorded whether an arm actually had it — so a packet could label a
    historical slice ``COMPARABLE`` while having read the live projection,
    producing a confident and entirely false delta.

    ``NOT_REQUIRED`` is the current slice. ``OBSERVED_INTERVALS`` means every
    traversed edge carried a validity interval covering the as-of instant, and
    is the only basis on which a historical slice may be ``COMPARABLE``.
    ``UNAVAILABLE`` is the CHAOS-3569 state and forces
    ``NOT_COMPARABLE_MISSING_EDGE_VALIDITY``.
    """

    NOT_REQUIRED = "not_required"
    OBSERVED_INTERVALS = "observed_intervals"
    UNAVAILABLE = "unavailable"


ALL_EDGE_VALIDITY_BASES: tuple[EdgeValidityBasis, ...] = (
    EdgeValidityBasis.NOT_REQUIRED,
    EdgeValidityBasis.OBSERVED_INTERVALS,
    EdgeValidityBasis.UNAVAILABLE,
)


class ComparisonShape(StrEnum):
    """The requested comparison or singular-subject shape."""

    SINGULAR_SUBJECT = "singular_subject"
    EXPLICIT_COHORT = "explicit_cohort"
    DISCOVERED_COHORT = "discovered_cohort"
    PORTFOLIO_WIDE = "portfolio_wide"
    ORGANIZATION_WIDE = "organization_wide"


ALL_COMPARISON_SHAPES: tuple[ComparisonShape, ...] = (
    ComparisonShape.SINGULAR_SUBJECT,
    ComparisonShape.EXPLICIT_COHORT,
    ComparisonShape.DISCOVERED_COHORT,
    ComparisonShape.PORTFOLIO_WIDE,
    ComparisonShape.ORGANIZATION_WIDE,
)

#: Shapes that carry a real cohort (two or more members and at least one
#: declared comparison dimension). ``SINGULAR_SUBJECT`` is the exception.
COHORT_BEARING_SHAPES: frozenset[ComparisonShape] = frozenset(
    {
        ComparisonShape.EXPLICIT_COHORT,
        ComparisonShape.DISCOVERED_COHORT,
        ComparisonShape.PORTFOLIO_WIDE,
        ComparisonShape.ORGANIZATION_WIDE,
    }
)


class JobUncertainty(StrEnum):
    """How precisely the analytical job could be interpreted.

    ``BROAD_WITH_UNCERTAINTY`` exists because the correction addendum bans
    requiring a fully pre-enumerated intent before investigation may begin:
    "what is going sideways?" is a legitimate job whose subject set is
    discovered, not declared. A broad or ambiguous job must carry at least
    one interpretation limitation, which is what stops the state from being
    a free pass.
    """

    PRECISE = "precise"
    BROAD_WITH_UNCERTAINTY = "broad_with_uncertainty"
    AMBIGUOUS = "ambiguous"


ALL_JOB_UNCERTAINTIES: tuple[JobUncertainty, ...] = (
    JobUncertainty.PRECISE,
    JobUncertainty.BROAD_WITH_UNCERTAINTY,
    JobUncertainty.AMBIGUOUS,
)

#: Uncertainty states that oblige the job to declare its limitations.
UNCERTAIN_JOB_STATES: frozenset[JobUncertainty] = frozenset(
    {JobUncertainty.BROAD_WITH_UNCERTAINTY, JobUncertainty.AMBIGUOUS}
)


class SurfaceKind(StrEnum):
    """Safe surface/conversation context references.

    Closed rather than free text on purpose: a surface reference is the one
    field on an investigation packet whose natural implementation is "paste
    the URL the user was looking at", and a URL is both an unbounded
    disclosure channel and the exact shape of a dashboard redirect.
    """

    DASHBOARD = "dashboard"
    PROJECT_PAGE = "project_page"
    TEAM_PAGE = "team_page"
    PORTFOLIO_PAGE = "portfolio_page"
    REPORT = "report"
    CONVERSATION = "conversation"


ALL_SURFACE_KINDS: tuple[SurfaceKind, ...] = (
    SurfaceKind.DASHBOARD,
    SurfaceKind.PROJECT_PAGE,
    SurfaceKind.TEAM_PAGE,
    SurfaceKind.PORTFOLIO_PAGE,
    SurfaceKind.REPORT,
    SurfaceKind.CONVERSATION,
)


class CohortInclusionBasis(StrEnum):
    """Why a subject is in the comparison cohort.

    There is deliberately no ``unspecified`` member and the field is
    ``min_length=1``: "an unrelated project appears in the cohort" is a named
    fault mode, and the cheapest way for an arm to commit it is to include a
    member with no stated basis at all.
    """

    EXPLICITLY_NAMED = "explicitly_named"
    SHARED_DEPENDENCY = "shared_dependency"
    SHARED_TEAM_OWNERSHIP = "shared_team_ownership"
    SAME_PORTFOLIO = "same_portfolio"
    SAME_INITIATIVE = "same_initiative"
    COMPARABLE_DELIVERY_PROFILE = "comparable_delivery_profile"
    PEER_OF_NAMED_SUBJECT = "peer_of_named_subject"


ALL_COHORT_INCLUSION_BASES: tuple[CohortInclusionBasis, ...] = (
    CohortInclusionBasis.EXPLICITLY_NAMED,
    CohortInclusionBasis.SHARED_DEPENDENCY,
    CohortInclusionBasis.SHARED_TEAM_OWNERSHIP,
    CohortInclusionBasis.SAME_PORTFOLIO,
    CohortInclusionBasis.SAME_INITIATIVE,
    CohortInclusionBasis.COMPARABLE_DELIVERY_PROFILE,
    CohortInclusionBasis.PEER_OF_NAMED_SUBJECT,
)


class CohortEvidenceClassification(StrEnum):
    """Closed reasons a cohort member may carry no evidence handle.

    The same "evidence refs XOR an explicit no-evidence classification —
    never both, never neither" pattern as
    ``contracts_v2.embedded.MetricEvidenceClassification`` and
    ``contracts_v2.deficiency.DeficiencyEvidenceClassification``, applied to
    cohort membership here.
    """

    EXPLICITLY_NAMED_BY_QUESTION = "explicitly_named_by_question"
    CANONICAL_REGISTRY_MEMBERSHIP = "canonical_registry_membership"


ALL_COHORT_EVIDENCE_CLASSIFICATIONS: tuple[CohortEvidenceClassification, ...] = (
    CohortEvidenceClassification.EXPLICITLY_NAMED_BY_QUESTION,
    CohortEvidenceClassification.CANONICAL_REGISTRY_MEMBERSHIP,
)


class CohortExclusionReason(StrEnum):
    OUT_OF_AUTHORIZED_SCOPE = "out_of_authorized_scope"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    NOT_COMPARABLE_SLICE = "not_comparable_slice"
    ARCHIVED_OR_INACTIVE = "archived_or_inactive"
    TRUNCATION_BUDGET = "truncation_budget"
    AMBIGUOUS_IDENTITY = "ambiguous_identity"
    EXCLUDED_BY_QUESTION = "excluded_by_question"


ALL_COHORT_EXCLUSION_REASONS: tuple[CohortExclusionReason, ...] = (
    CohortExclusionReason.OUT_OF_AUTHORIZED_SCOPE,
    CohortExclusionReason.INSUFFICIENT_EVIDENCE,
    CohortExclusionReason.NOT_COMPARABLE_SLICE,
    CohortExclusionReason.ARCHIVED_OR_INACTIVE,
    CohortExclusionReason.TRUNCATION_BUDGET,
    CohortExclusionReason.AMBIGUOUS_IDENTITY,
    CohortExclusionReason.EXCLUDED_BY_QUESTION,
)


class CohortCompleteness(StrEnum):
    COMPLETE = "complete"
    TRUNCATED = "truncated"
    BEST_EFFORT_UNCERTAIN = "best_effort_uncertain"


ALL_COHORT_COMPLETENESS_STATES: tuple[CohortCompleteness, ...] = (
    CohortCompleteness.COMPLETE,
    CohortCompleteness.TRUNCATED,
    CohortCompleteness.BEST_EFFORT_UNCERTAIN,
)


class ComparisonDimension(StrEnum):
    """Axes a cohort may be compared on.

    Every member is a *unit-of-work or system* axis. None is per-person, and
    none may be derived per-person: person-level productivity, workload and
    staffing ranking is prohibited outright, and this enum is one of the two
    structural places that ban is enforced (the other being the absence of a
    person member on :class:`InvestigationSubjectKind`).
    """

    DELIVERY_THROUGHPUT = "delivery_throughput"
    CYCLE_TIME = "cycle_time"
    REVIEW_LOAD = "review_load"
    WORK_IN_PROGRESS = "work_in_progress"
    DEPENDENCY_EXPOSURE = "dependency_exposure"
    INCIDENT_LOAD = "incident_load"
    DEPLOYMENT_FREQUENCY = "deployment_frequency"
    INVESTMENT_MIX = "investment_mix"
    OPEN_DEFICIENCY_COUNT = "open_deficiency_count"
    STATUS_DECLARATION_GAP = "status_declaration_gap"
    CAPACITY_LOAD_RATIO = "capacity_load_ratio"
    DATA_COVERAGE = "data_coverage"


ALL_COMPARISON_DIMENSIONS: tuple[ComparisonDimension, ...] = (
    ComparisonDimension.DELIVERY_THROUGHPUT,
    ComparisonDimension.CYCLE_TIME,
    ComparisonDimension.REVIEW_LOAD,
    ComparisonDimension.WORK_IN_PROGRESS,
    ComparisonDimension.DEPENDENCY_EXPOSURE,
    ComparisonDimension.INCIDENT_LOAD,
    ComparisonDimension.DEPLOYMENT_FREQUENCY,
    ComparisonDimension.INVESTMENT_MIX,
    ComparisonDimension.OPEN_DEFICIENCY_COUNT,
    ComparisonDimension.STATUS_DECLARATION_GAP,
    ComparisonDimension.CAPACITY_LOAD_RATIO,
    ComparisonDimension.DATA_COVERAGE,
)


class RelationshipDirection(StrEnum):
    """Which way a hop was traversed relative to the relationship's own
    canonical orientation (declared in :mod:`.relationships`).

    A hop that claims ``FORWARD`` while its endpoint kinds match only the
    reverse orientation is exactly the named "a relationship is reversed"
    fault, and is rejected by ``LineageHop.validate_direction_matches_
    allowlist``.
    """

    FORWARD = "forward"
    REVERSE = "reverse"


ALL_RELATIONSHIP_DIRECTIONS: tuple[RelationshipDirection, ...] = (
    RelationshipDirection.FORWARD,
    RelationshipDirection.REVERSE,
)


class RelevanceState(StrEnum):
    """Whether a node, path, driver or evidence item is *currently* relevant."""

    CURRENT = "current"
    RECENTLY_CURRENT = "recently_current"
    HISTORICAL_ONLY = "historical_only"
    UNKNOWN = "unknown"


ALL_RELEVANCE_STATES: tuple[RelevanceState, ...] = (
    RelevanceState.CURRENT,
    RelevanceState.RECENTLY_CURRENT,
    RelevanceState.HISTORICAL_ONLY,
    RelevanceState.UNKNOWN,
)

#: The relevance states a *principal* driver may hold. A driver whose only
#: support is historical cannot be the principal current driver.
CURRENTLY_RELEVANT_STATES: frozenset[RelevanceState] = frozenset(
    {RelevanceState.CURRENT, RelevanceState.RECENTLY_CURRENT}
)


class AssertionBasis(StrEnum):
    """Measured, source-asserted and inferred distinctions.

    The governing rule is "the graph determines what is relevant; canonical
    services determine what is measurable". ``MEASURED`` therefore means a
    canonical Ops service produced the number and minted evidence for it;
    ``SOURCE_ASSERTED`` means a provider (or a human in a provider field)
    said so; ``INFERRED`` means the investigation concluded it. Only
    ``MEASURED`` may be presented as certain.
    """

    MEASURED = "measured"
    SOURCE_ASSERTED = "source_asserted"
    INFERRED = "inferred"


ALL_ASSERTION_BASES: tuple[AssertionBasis, ...] = (
    AssertionBasis.MEASURED,
    AssertionBasis.SOURCE_ASSERTED,
    AssertionBasis.INFERRED,
)


class ConfidenceQualifier(StrEnum):
    MEASURED_CERTAIN = "measured_certain"
    QUALIFIED = "qualified"
    UNCERTAIN = "uncertain"
    UNSUPPORTED = "unsupported"


ALL_CONFIDENCE_QUALIFIERS: tuple[ConfidenceQualifier, ...] = (
    ConfidenceQualifier.MEASURED_CERTAIN,
    ConfidenceQualifier.QUALIFIED,
    ConfidenceQualifier.UNCERTAIN,
    ConfidenceQualifier.UNSUPPORTED,
)


class DriverRole(StrEnum):
    """Symptom-versus-driver classification.

    ``CONTEXTUAL_CORRELATE`` is the honest third option: something that moved
    at the same time and is worth reporting but is neither the cause nor a
    consequence. Collapsing it into ``DRIVER`` is precisely how unsupported
    attribution enters an answer.
    """

    DRIVER = "driver"
    SYMPTOM = "symptom"
    CONTEXTUAL_CORRELATE = "contextual_correlate"


ALL_DRIVER_ROLES: tuple[DriverRole, ...] = (
    DriverRole.DRIVER,
    DriverRole.SYMPTOM,
    DriverRole.CONTEXTUAL_CORRELATE,
)


class DriverStanding(StrEnum):
    PRINCIPAL_DRIVER = "principal_driver"
    CONTRIBUTING_DRIVER = "contributing_driver"
    CANDIDATE_ONLY = "candidate_only"
    EXCLUDED = "excluded"


ALL_DRIVER_STANDINGS: tuple[DriverStanding, ...] = (
    DriverStanding.PRINCIPAL_DRIVER,
    DriverStanding.CONTRIBUTING_DRIVER,
    DriverStanding.CANDIDATE_ONLY,
    DriverStanding.EXCLUDED,
)

#: Standings that constitute a *judgment* the packet is asserting, as
#: opposed to a candidate it merely surfaced. A ``SUPPORTED`` packet must
#: carry at least one of these — that is the structural difference between
#: an investigation result and a dashboard redirect.
ASSERTED_DRIVER_STANDINGS: frozenset[DriverStanding] = frozenset(
    {DriverStanding.PRINCIPAL_DRIVER, DriverStanding.CONTRIBUTING_DRIVER}
)


class DriverCategory(StrEnum):
    DELIVERY_PRESSURE = "delivery_pressure"
    REVIEW_PRESSURE = "review_pressure"
    OPERATIONAL_PRESSURE = "operational_pressure"
    DEPENDENCY_PRESSURE = "dependency_pressure"
    INVESTMENT_MIX = "investment_mix"
    CAPACITY_OR_STAFFING = "capacity_or_staffing"
    SCOPE_CHANGE = "scope_change"
    QUALITY_OR_DEFECT = "quality_or_defect"
    EXTERNAL_BLOCKER = "external_blocker"
    DATA_COVERAGE = "data_coverage"


ALL_DRIVER_CATEGORIES: tuple[DriverCategory, ...] = (
    DriverCategory.DELIVERY_PRESSURE,
    DriverCategory.REVIEW_PRESSURE,
    DriverCategory.OPERATIONAL_PRESSURE,
    DriverCategory.DEPENDENCY_PRESSURE,
    DriverCategory.INVESTMENT_MIX,
    DriverCategory.CAPACITY_OR_STAFFING,
    DriverCategory.SCOPE_CHANGE,
    DriverCategory.QUALITY_OR_DEFECT,
    DriverCategory.EXTERNAL_BLOCKER,
    DriverCategory.DATA_COVERAGE,
)


class DriverExclusionReason(StrEnum):
    """Why a candidate did not reach principal-driver standing."""

    NO_SUPPORTING_PATH = "no_supporting_path"
    EVIDENCE_CONFLICT_UNRESOLVED = "evidence_conflict_unresolved"
    NOT_CURRENTLY_RELEVANT = "not_currently_relevant"
    SYMPTOM_OF_ANOTHER_CANDIDATE = "symptom_of_another_candidate"
    UNAUTHORIZED_EVIDENCE = "unauthorized_evidence"
    INSUFFICIENT_MEASUREMENT = "insufficient_measurement"


ALL_DRIVER_EXCLUSION_REASONS: tuple[DriverExclusionReason, ...] = (
    DriverExclusionReason.NO_SUPPORTING_PATH,
    DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED,
    DriverExclusionReason.NOT_CURRENTLY_RELEVANT,
    DriverExclusionReason.SYMPTOM_OF_ANOTHER_CANDIDATE,
    DriverExclusionReason.UNAUTHORIZED_EVIDENCE,
    DriverExclusionReason.INSUFFICIENT_MEASUREMENT,
)


class StaffingDenominatorState(StrEnum):
    """How much allocation evidence backs a capacity/staffing claim.

    The correction addendum is explicit on both halves of this: a missing
    denominator must **reduce confidence and require qualification**, and it
    must **not automatically make capacity questions unsupported**. Both
    halves are pinned by tests — ``DENOMINATOR_ABSENT`` with a
    ``MEASURED_CERTAIN`` qualifier is rejected, and ``DENOMINATOR_ABSENT``
    with a ``QUALIFIED`` qualifier is accepted.
    """

    ALLOCATION_EVIDENCE_AVAILABLE = "allocation_evidence_available"
    PARTIAL_ALLOCATION_EVIDENCE = "partial_allocation_evidence"
    DENOMINATOR_ABSENT = "denominator_absent"


ALL_STAFFING_DENOMINATOR_STATES: tuple[StaffingDenominatorState, ...] = (
    StaffingDenominatorState.ALLOCATION_EVIDENCE_AVAILABLE,
    StaffingDenominatorState.PARTIAL_ALLOCATION_EVIDENCE,
    StaffingDenominatorState.DENOMINATOR_ABSENT,
)

#: Denominator states that forbid presenting a staffing claim as certain.
UNQUALIFIED_DENOMINATOR_STATES: frozenset[StaffingDenominatorState] = frozenset(
    {
        StaffingDenominatorState.PARTIAL_ALLOCATION_EVIDENCE,
        StaffingDenominatorState.DENOMINATOR_ABSENT,
    }
)


class TruncationReason(StrEnum):
    PATH_BUDGET = "path_budget"
    NODE_BUDGET = "node_budget"
    COHORT_BUDGET = "cohort_budget"
    EVIDENCE_BUDGET = "evidence_budget"
    TIME_BUDGET = "time_budget"
    AUTHORIZATION_FILTER = "authorization_filter"
    SOURCE_UNAVAILABLE = "source_unavailable"


ALL_TRUNCATION_REASONS: tuple[TruncationReason, ...] = (
    TruncationReason.PATH_BUDGET,
    TruncationReason.NODE_BUDGET,
    TruncationReason.COHORT_BUDGET,
    TruncationReason.EVIDENCE_BUDGET,
    TruncationReason.TIME_BUDGET,
    TruncationReason.AUTHORIZATION_FILTER,
    TruncationReason.SOURCE_UNAVAILABLE,
)


class ConflictResolution(StrEnum):
    UNRESOLVED = "unresolved"
    RESOLVED_BY_PRECEDENCE = "resolved_by_precedence"
    RESOLVED_BY_RECENCY = "resolved_by_recency"
    DEFERRED_TO_CANONICAL_SERVICE = "deferred_to_canonical_service"


ALL_CONFLICT_RESOLUTIONS: tuple[ConflictResolution, ...] = (
    ConflictResolution.UNRESOLVED,
    ConflictResolution.RESOLVED_BY_PRECEDENCE,
    ConflictResolution.RESOLVED_BY_RECENCY,
    ConflictResolution.DEFERRED_TO_CANONICAL_SERVICE,
)


class ClarificationNeedKind(StrEnum):
    AMBIGUOUS_SUBJECT = "ambiguous_subject"
    MISSING_TIME_CONTEXT = "missing_time_context"
    MISSING_COMPARISON_BASIS = "missing_comparison_basis"
    UNRESOLVED_CONVERSATIONAL_REFERENCE = "unresolved_conversational_reference"
    UNSUPPORTED_BY_AVAILABLE_SOURCES = "unsupported_by_available_sources"


ALL_CLARIFICATION_NEED_KINDS: tuple[ClarificationNeedKind, ...] = (
    ClarificationNeedKind.AMBIGUOUS_SUBJECT,
    ClarificationNeedKind.MISSING_TIME_CONTEXT,
    ClarificationNeedKind.MISSING_COMPARISON_BASIS,
    ClarificationNeedKind.UNRESOLVED_CONVERSATIONAL_REFERENCE,
    ClarificationNeedKind.UNSUPPORTED_BY_AVAILABLE_SOURCES,
)

#: Clarification kinds that answer an *unresolved subject reference*. One of
#: these must be present before a packet may widen to organization scope.
SUBJECT_CLARIFICATION_KINDS: frozenset[ClarificationNeedKind] = frozenset(
    {
        ClarificationNeedKind.AMBIGUOUS_SUBJECT,
        ClarificationNeedKind.UNRESOLVED_CONVERSATIONAL_REFERENCE,
    }
)


class PacketLimitationKind(StrEnum):
    MISSING_SOURCE = "missing_source"
    STALE_SOURCE = "stale_source"
    CONFLICTING_EVIDENCE = "conflicting_evidence"
    AUTHORIZATION_FILTERED = "authorization_filtered"
    TRUNCATED_TRAVERSAL = "truncated_traversal"
    ABSENT_STAFFING_DENOMINATOR = "absent_staffing_denominator"
    HISTORICAL_SLICE_NOT_COMPARABLE = "historical_slice_not_comparable"
    INTERPRETATION_UNCERTAINTY = "interpretation_uncertainty"


ALL_PACKET_LIMITATION_KINDS: tuple[PacketLimitationKind, ...] = (
    PacketLimitationKind.MISSING_SOURCE,
    PacketLimitationKind.STALE_SOURCE,
    PacketLimitationKind.CONFLICTING_EVIDENCE,
    PacketLimitationKind.AUTHORIZATION_FILTERED,
    PacketLimitationKind.TRUNCATED_TRAVERSAL,
    PacketLimitationKind.ABSENT_STAFFING_DENOMINATOR,
    PacketLimitationKind.HISTORICAL_SLICE_NOT_COMPARABLE,
    PacketLimitationKind.INTERPRETATION_UNCERTAINTY,
)


class InvestigationOutcome(StrEnum):
    """What the investigation concluded — *not* what the user is told.

    Deliberately distinct from ``contracts_v2.base.PublicOutcome``: that
    enum is the public answer-envelope vocabulary of an Ask Dev answer, and
    reusing it here would make the packet look like an answer. A packet is
    an input to the Ask Dev frame, never a substitute for it.
    """

    SUPPORTED = "supported"
    SUPPORTED_WITH_GAPS = "supported_with_gaps"
    NEEDS_CLARIFICATION = "needs_clarification"
    NO_MATCH = "no_match"
    UNSUPPORTED = "unsupported"


ALL_INVESTIGATION_OUTCOMES: tuple[InvestigationOutcome, ...] = (
    InvestigationOutcome.SUPPORTED,
    InvestigationOutcome.SUPPORTED_WITH_GAPS,
    InvestigationOutcome.NEEDS_CLARIFICATION,
    InvestigationOutcome.NO_MATCH,
    InvestigationOutcome.UNSUPPORTED,
)

#: Outcomes that assert the investigation reached a usable judgment.
SUPPORTED_OUTCOMES: frozenset[InvestigationOutcome] = frozenset(
    {InvestigationOutcome.SUPPORTED, InvestigationOutcome.SUPPORTED_WITH_GAPS}
)


class PacketSection(StrEnum):
    """Named sections of the packet, for the question-family registry.

    A family declares which sections it *requires* an arm to populate, which
    is how "this family is not reducible to one dashboard metric" becomes a
    machine-checkable statement rather than a comment.
    """

    ANALYTICAL_JOB = "analytical_job"
    SUBJECT_DISCOVERY = "subject_discovery"
    COMPARISON_COHORT = "comparison_cohort"
    RELATED_CONTEXT = "related_context"
    DRIVER_ANALYSIS = "driver_analysis"
    EVIDENCE_COVERAGE = "evidence_coverage"
    VERSIONS = "versions"


ALL_PACKET_SECTIONS: tuple[PacketSection, ...] = (
    PacketSection.ANALYTICAL_JOB,
    PacketSection.SUBJECT_DISCOVERY,
    PacketSection.COMPARISON_COHORT,
    PacketSection.RELATED_CONTEXT,
    PacketSection.DRIVER_ANALYSIS,
    PacketSection.EVIDENCE_COVERAGE,
    PacketSection.VERSIONS,
)
