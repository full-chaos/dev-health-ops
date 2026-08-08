"""Scoring, invariant and fault-mode registries for the corrected trial.

CHAOS-3615 deliverables 8 and 12.

Two registries, cross-referenced in both directions:

* :data:`SCORING_DIMENSION_REGISTRY` — the machine-readable evaluation
  dimensions CHAOS-3616 will score *before* either arm's output is observed.
  Every dimension names the packet fields it reads and the fault modes it
  exists to catch.
* :data:`FAULT_MODE_REGISTRY` — the named bad behaviours. Each says whether
  the packet contract itself rejects it (``CONTRACT_VALIDATOR``, with the
  exact model and validator that does the rejecting) or whether it is an
  oracle-only judgment CHAOS-3616 must score.

Three rules are enforced at import time by :func:`validate_scoring_registry`
rather than left to review:

1. **No aggregate score.** There is no total, no weighted composite, and no
   dimension whose value is a function of the others. The trial reports per
   question family × per dimension. A dimension that tried to be an
   aggregate would have to name every other dimension's fields, which
   ``aggregate_prohibited`` forbids outright.
2. **Totality both ways.** Every dimension names at least one fault mode and
   every fault mode is named by at least one dimension. A dimension nobody
   can fail, and a fault nothing scores, are the two shapes of a vacuous
   evaluation framework.
3. **Contract-rejected faults must name a real validator.** A fault mode
   claiming ``CONTRACT_VALIDATOR`` carries a :class:`ValidatorReference`,
   and ``tests/api/dev/test_chaos_3615_scoring_registry.py`` resolves every
   one of them against :mod:`.packet` — a reference to a validator that does
   not exist is a red test, not a stale docstring.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Literal

from dev_health_ops.api.dev.contracts import Label, LongText, ShortText
from dev_health_ops.api.dev.contracts_v2.base import ContractModelV2

from .vocabulary import PacketSection

__all__ = [
    "ALL_FAULT_MODE_IDS",
    "ALL_SCORING_DIMENSION_IDS",
    "FAULT_MODE_REGISTRY",
    "SCORING_DIMENSION_REGISTRY",
    "DimensionPolarity",
    "FaultMode",
    "FaultModeID",
    "MeasurementKind",
    "RejectingMechanism",
    "ScoringDimension",
    "ScoringDimensionID",
    "ValidatorReference",
    "validate_scoring_registry",
]


class ScoringDimensionID(StrEnum):
    """The evaluation dimensions of the corrected trial."""

    SUBJECT_TOP_1 = "subject_top_1"
    SUBJECT_TOP_3 = "subject_top_3"
    CLARIFICATION_CANDIDATE_PRECISION = "clarification_candidate_precision"
    ALIAS_ACRONYM_RENAME_RESOLUTION = "alias_acronym_rename_resolution"
    CONVERSATIONAL_REFERENCE_RESOLUTION = "conversational_reference_resolution"
    NO_UNSAFE_ORGANIZATION_WIDENING = "no_unsafe_organization_widening"
    COHORT_PRECISION = "cohort_precision"
    COHORT_RECALL = "cohort_recall"
    COHORT_INCLUSION_EXPLAINABILITY = "cohort_inclusion_explainability"
    COHORT_EXCLUSION_EXPLAINABILITY = "cohort_exclusion_explainability"
    RELEVANT_ENTITY_RECALL = "relevant_entity_recall"
    RELEVANT_RELATIONSHIP_RECALL = "relevant_relationship_recall"
    LINEAGE_PATH_PRECISION = "lineage_path_precision"
    LINEAGE_DIRECTION_CORRECTNESS = "lineage_direction_correctness"
    CROSS_SOURCE_ASSOCIATION = "cross_source_association"
    EVIDENCE_CLOSURE = "evidence_closure"
    CURRENT_RELEVANCE = "current_relevance"
    PRINCIPAL_DRIVER_PRECISION = "principal_driver_precision"
    PRINCIPAL_DRIVER_RECALL = "principal_driver_recall"
    SYMPTOM_VERSUS_DRIVER_DISTINCTION = "symptom_versus_driver_distinction"
    UNSUPPORTED_ATTRIBUTION_RATE = "unsupported_attribution_rate"
    COMPARATIVE_JUDGMENT_SUPPORT = "comparative_judgment_support"
    ANSWER_USEFULNESS_BEYOND_DASHBOARD = "answer_usefulness_beyond_dashboard"
    USEFUL_UNCERTAINTY_BEHAVIOUR = "useful_uncertainty_behaviour"
    ZERO_UNAUTHORIZED_RESULTS = "zero_unauthorized_results"
    ZERO_PERSON_LEVEL_RANKING = "zero_person_level_ranking"
    ZERO_UNSUPPORTED_STAFFING_CERTAINTY = "zero_unsupported_staffing_certainty"
    ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE = "zero_graph_native_surface_leakage"


ALL_SCORING_DIMENSION_IDS: tuple[ScoringDimensionID, ...] = (
    ScoringDimensionID.SUBJECT_TOP_1,
    ScoringDimensionID.SUBJECT_TOP_3,
    ScoringDimensionID.CLARIFICATION_CANDIDATE_PRECISION,
    ScoringDimensionID.ALIAS_ACRONYM_RENAME_RESOLUTION,
    ScoringDimensionID.CONVERSATIONAL_REFERENCE_RESOLUTION,
    ScoringDimensionID.NO_UNSAFE_ORGANIZATION_WIDENING,
    ScoringDimensionID.COHORT_PRECISION,
    ScoringDimensionID.COHORT_RECALL,
    ScoringDimensionID.COHORT_INCLUSION_EXPLAINABILITY,
    ScoringDimensionID.COHORT_EXCLUSION_EXPLAINABILITY,
    ScoringDimensionID.RELEVANT_ENTITY_RECALL,
    ScoringDimensionID.RELEVANT_RELATIONSHIP_RECALL,
    ScoringDimensionID.LINEAGE_PATH_PRECISION,
    ScoringDimensionID.LINEAGE_DIRECTION_CORRECTNESS,
    ScoringDimensionID.CROSS_SOURCE_ASSOCIATION,
    ScoringDimensionID.EVIDENCE_CLOSURE,
    ScoringDimensionID.CURRENT_RELEVANCE,
    ScoringDimensionID.PRINCIPAL_DRIVER_PRECISION,
    ScoringDimensionID.PRINCIPAL_DRIVER_RECALL,
    ScoringDimensionID.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
    ScoringDimensionID.UNSUPPORTED_ATTRIBUTION_RATE,
    ScoringDimensionID.COMPARATIVE_JUDGMENT_SUPPORT,
    ScoringDimensionID.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
    ScoringDimensionID.USEFUL_UNCERTAINTY_BEHAVIOUR,
    ScoringDimensionID.ZERO_UNAUTHORIZED_RESULTS,
    ScoringDimensionID.ZERO_PERSON_LEVEL_RANKING,
    ScoringDimensionID.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
    ScoringDimensionID.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE,
)


class FaultModeID(StrEnum):
    """The named bad behaviours every invariant in CHAOS-3615 must reject.

    These are the corrective plan's own eleven fault shapes, verbatim in
    intent and renamed only to be identifiers.
    """

    WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST = "wrong_but_similar_subject_ranked_first"
    ORGANIZATION_WIDENING_AFTER_UNRESOLVED_REFERENCE = (
        "organization_widening_after_unresolved_reference"
    )
    IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE = "irrelevant_evidence_displaces_lineage"
    SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER = "symptom_labelled_as_principal_driver"
    STAFFING_CERTAINTY_WITHOUT_ALLOCATION_EVIDENCE = (
        "staffing_certainty_without_allocation_evidence"
    )
    UNRELATED_COHORT_MEMBER = "unrelated_cohort_member"
    REVERSED_RELATIONSHIP_DIRECTION = "reversed_relationship_direction"
    PATH_CROSSES_UNAUTHORIZED_ENTITY = "path_crosses_unauthorized_entity"
    DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT = (
        "dashboard_redirect_without_direct_judgment"
    )
    ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS = "absent_required_field_silently_defaults"
    WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS = (
        "wildcard_or_optional_field_makes_check_vacuous"
    )


ALL_FAULT_MODE_IDS: tuple[FaultModeID, ...] = (
    FaultModeID.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,
    FaultModeID.ORGANIZATION_WIDENING_AFTER_UNRESOLVED_REFERENCE,
    FaultModeID.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,
    FaultModeID.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER,
    FaultModeID.STAFFING_CERTAINTY_WITHOUT_ALLOCATION_EVIDENCE,
    FaultModeID.UNRELATED_COHORT_MEMBER,
    FaultModeID.REVERSED_RELATIONSHIP_DIRECTION,
    FaultModeID.PATH_CROSSES_UNAUTHORIZED_ENTITY,
    FaultModeID.DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT,
    FaultModeID.ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS,
    FaultModeID.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS,
)


class MeasurementKind(StrEnum):
    PRECISION = "precision"
    RECALL = "recall"
    RATE = "rate"
    COUNT = "count"
    BOOLEAN_ZERO = "boolean_zero"
    ORDINAL_TOP_K = "ordinal_top_k"


class DimensionPolarity(StrEnum):
    HIGHER_IS_BETTER = "higher_is_better"
    MUST_BE_ZERO = "must_be_zero"


class RejectingMechanism(StrEnum):
    """Who catches a fault mode.

    ``CONTRACT_VALIDATOR`` means an arm literally cannot emit the bad shape:
    a named validator rejects it. ``REQUIRED_FIELD`` means the rejection
    comes from the field grammar itself — the field is required with no
    default, so omitting it raises before any validator runs; there is no
    function to point at, and pretending otherwise would be a fake citation.
    ``ORACLE_JUDGMENT`` means the shape is well-formed but wrong on the
    merits (a cohort that is structurally explained but factually
    irrelevant), which only CHAOS-3616's oracles can score.

    Being honest about which is which matters: claiming contract coverage
    for an oracle-only judgment is exactly the inaccurate-coverage claim
    that makes a reader stop checking.
    """

    CONTRACT_VALIDATOR = "contract_validator"
    REQUIRED_FIELD = "required_field"
    ORACLE_JUDGMENT = "oracle_judgment"


class ValidatorReference(ContractModelV2):
    """A resolvable pointer at the validator that rejects a fault mode.

    ``model_name`` is a class in :mod:`.packet`; ``validator_name`` is an
    attribute on it. Resolved for real in
    ``tests/api/dev/test_chaos_3615_scoring_registry.py``, which is what
    keeps this from decaying into a comment.
    """

    model_name: Label
    validator_name: Label


class ScoringDimension(ContractModelV2):
    """One evaluation dimension, reported per question family."""

    schema_version: Literal["ask_dev_scoring_dimension.v1"]
    dimension_id: ScoringDimensionID
    title: Label
    definition: LongText
    measurement_kind: MeasurementKind
    polarity: DimensionPolarity
    packet_sections: tuple[PacketSection, ...]
    packet_fields: tuple[Label, ...]
    fault_mode_ids: tuple[FaultModeID, ...]
    #: Both are ``Literal[True]`` rather than plain ``bool``: they are not
    #: configuration, they are the two anti-drift rules of the correction
    #: addendum ("report per question family and per evaluation dimension";
    #: "do not collapse these into one aggregate score") expressed in a form
    #: a future edit cannot flip without changing the type.
    reported_per_question_family: Literal[True]
    aggregate_prohibited: Literal[True]


class FaultMode(ContractModelV2):
    """One named bad behaviour, and what rejects it."""

    schema_version: Literal["ask_dev_fault_mode.v1"]
    fault_mode_id: FaultModeID
    title: Label
    bad_behaviour: LongText
    rejecting_mechanism: RejectingMechanism
    validator_reference: ValidatorReference | None
    dimension_ids: tuple[ScoringDimensionID, ...]
    #: The test that proves the rejection actually happens. Required for
    #: every fault mode, oracle-judgment ones included -- an oracle fault
    #: with no test is an assertion of coverage with nothing behind it.
    proving_test: ShortText


def _dimension(
    dimension_id: ScoringDimensionID,
    title: str,
    definition: str,
    measurement_kind: MeasurementKind,
    polarity: DimensionPolarity,
    packet_sections: tuple[PacketSection, ...],
    packet_fields: tuple[str, ...],
    fault_mode_ids: tuple[FaultModeID, ...],
) -> ScoringDimension:
    return ScoringDimension(
        schema_version="ask_dev_scoring_dimension.v1",
        dimension_id=dimension_id,
        title=title,
        definition=definition,
        measurement_kind=measurement_kind,
        polarity=polarity,
        packet_sections=packet_sections,
        packet_fields=packet_fields,
        fault_mode_ids=fault_mode_ids,
        reported_per_question_family=True,
        aggregate_prohibited=True,
    )


_D = ScoringDimensionID
_F = FaultModeID
_M = MeasurementKind
_P = DimensionPolarity
_S = PacketSection


SCORING_DIMENSION_REGISTRY: Mapping[ScoringDimensionID, ScoringDimension] = {
    _D.SUBJECT_TOP_1: _dimension(
        _D.SUBJECT_TOP_1,
        "Correct canonical subject at rank 1",
        "The expected canonical subject is the rank-1 candidate. Scored per "
        "question family; a family whose questions name no subject is scored "
        "NOT APPLICABLE rather than 0, so an unnameable family cannot drag a "
        "score down or prop one up.",
        _M.ORDINAL_TOP_K,
        _P.HIGHER_IS_BETTER,
        (_S.SUBJECT_DISCOVERY,),
        (
            "subject_discovery.candidates[].rank",
            "subject_discovery.candidates[].canonical_id",
        ),
        (_F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,),
    ),
    _D.SUBJECT_TOP_3: _dimension(
        _D.SUBJECT_TOP_3,
        "Correct canonical subject within rank 3",
        "The expected canonical subject appears in the top three candidates. "
        "Reported alongside rank-1 rather than instead of it: an arm that is "
        "reliably top-3 but rarely top-1 is a clarification-first product, "
        "not a resolution product, and the two must stay distinguishable.",
        _M.ORDINAL_TOP_K,
        _P.HIGHER_IS_BETTER,
        (_S.SUBJECT_DISCOVERY,),
        (
            "subject_discovery.candidates[].rank",
            "subject_discovery.candidates[].canonical_id",
        ),
        (_F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,),
    ),
    _D.CLARIFICATION_CANDIDATE_PRECISION: _dimension(
        _D.CLARIFICATION_CANDIDATE_PRECISION,
        "Useful clarification candidate precision",
        "When the arm asks for clarification, the candidates it offers "
        "contain the expected subject and exclude obvious non-candidates. A "
        "clarification listing every project in the organization is a "
        "failure of this dimension even though it technically contains the "
        "right answer.",
        _M.PRECISION,
        _P.HIGHER_IS_BETTER,
        (_S.SUBJECT_DISCOVERY, _S.EVIDENCE_COVERAGE),
        (
            "subject_discovery.unresolved_mentions[].candidate_ids",
            "evidence_coverage.clarification_needs[].candidate_ids",
        ),
        (
            _F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,
            _F.ORGANIZATION_WIDENING_AFTER_UNRESOLVED_REFERENCE,
        ),
    ),
    _D.ALIAS_ACRONYM_RENAME_RESOLUTION: _dimension(
        _D.ALIAS_ACRONYM_RENAME_RESOLUTION,
        "Alias, acronym and renamed-entity resolution",
        "A reference by alias, acronym, previous name or provider identifier "
        "resolves to the correct canonical subject, and the packet says "
        "which signal did it. Scoring the signal as well as the outcome is "
        "deliberate: an arm that gets the right answer by fuzzy label is not "
        "doing alias resolution and will not generalize.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.SUBJECT_DISCOVERY,),
        (
            "subject_discovery.candidates[].match_signals[].signal",
            "subject_discovery.candidates[].match_signals[].matched_text",
        ),
        (_F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,),
    ),
    _D.CONVERSATIONAL_REFERENCE_RESOLUTION: _dimension(
        _D.CONVERSATIONAL_REFERENCE_RESOLUTION,
        "Conversational and surface reference resolution",
        "'What about the other project we discussed?' resolves against "
        "conversation or surface context, or is escalated to clarification. "
        "Silently guessing is scored as a failure, not a partial success.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.ANALYTICAL_JOB, _S.SUBJECT_DISCOVERY),
        (
            "analytical_job.surface_context_refs",
            "analytical_job.conversation_reference_ids",
            "subject_discovery.candidates[].match_signals[].signal",
        ),
        (_F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,),
    ),
    _D.NO_UNSAFE_ORGANIZATION_WIDENING: _dimension(
        _D.NO_UNSAFE_ORGANIZATION_WIDENING,
        "No unsafe organization widening",
        "An unresolved named reference never silently becomes an "
        "organization-wide sweep. Must be zero: widening after a failed "
        "resolution is how a question about one team becomes a report on "
        "everybody, and it is both a privacy and a usefulness failure.",
        _M.BOOLEAN_ZERO,
        _P.MUST_BE_ZERO,
        (_S.ANALYTICAL_JOB, _S.SUBJECT_DISCOVERY, _S.COMPARISON_COHORT),
        (
            "analytical_job.comparison_shape",
            "subject_discovery.unresolved_mentions",
            "evidence_coverage.clarification_needs[].kind",
        ),
        (_F.ORGANIZATION_WIDENING_AFTER_UNRESOLVED_REFERENCE,),
    ),
    _D.COHORT_PRECISION: _dimension(
        _D.COHORT_PRECISION,
        "Comparison cohort precision",
        "Members of the cohort belong in it. An unrelated project in the "
        "cohort is the failure this scores.",
        _M.PRECISION,
        _P.HIGHER_IS_BETTER,
        (_S.COMPARISON_COHORT,),
        ("comparison_cohort.members[].canonical_id",),
        (_F.UNRELATED_COHORT_MEMBER,),
    ),
    _D.COHORT_RECALL: _dimension(
        _D.COHORT_RECALL,
        "Comparison cohort recall",
        "Subjects that belong in the cohort are in it. Reported alongside "
        "precision because a one-member cohort is trivially precise.",
        _M.RECALL,
        _P.HIGHER_IS_BETTER,
        (_S.COMPARISON_COHORT,),
        ("comparison_cohort.members[].canonical_id", "comparison_cohort.completeness"),
        (_F.UNRELATED_COHORT_MEMBER, _F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS),
    ),
    _D.COHORT_INCLUSION_EXPLAINABILITY: _dimension(
        _D.COHORT_INCLUSION_EXPLAINABILITY,
        "Explainable cohort inclusion",
        "Every member states a closed-vocabulary basis and a rationale, and "
        "carries either evidence handles or an explicit no-evidence "
        "classification.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.COMPARISON_COHORT,),
        (
            "comparison_cohort.members[].inclusion_basis",
            "comparison_cohort.members[].inclusion_rationale",
            "comparison_cohort.members[].inclusion_evidence_ids",
        ),
        (_F.UNRELATED_COHORT_MEMBER,),
    ),
    _D.COHORT_EXCLUSION_EXPLAINABILITY: _dimension(
        _D.COHORT_EXCLUSION_EXPLAINABILITY,
        "Explainable cohort exclusion",
        "Subjects the arm considered and rejected are listed with reasons. "
        "Scored separately from inclusion because an arm can be perfectly "
        "explainable about what it kept while being silent about what it "
        "dropped, which is where quiet authorization filtering hides.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.COMPARISON_COHORT,),
        (
            "comparison_cohort.exclusions[].reason",
            "comparison_cohort.exclusions[].rationale",
            "comparison_cohort.authorization_filtered_count",
        ),
        (_F.UNRELATED_COHORT_MEMBER, _F.ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS),
    ),
    _D.RELEVANT_ENTITY_RECALL: _dimension(
        _D.RELEVANT_ENTITY_RECALL,
        "Relevant entity recall",
        "The entities a human investigator would consider relevant are "
        "present in the related-context section.",
        _M.RECALL,
        _P.HIGHER_IS_BETTER,
        (_S.RELATED_CONTEXT,),
        ("related_context.entities[].entity_id",),
        (_F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,),
    ),
    _D.RELEVANT_RELATIONSHIP_RECALL: _dimension(
        _D.RELEVANT_RELATIONSHIP_RECALL,
        "Relevant relationship and path recall",
        "The relationships that explain the situation are present, not just "
        "the entities they connect.",
        _M.RECALL,
        _P.HIGHER_IS_BETTER,
        (_S.RELATED_CONTEXT,),
        ("related_context.paths[].hops[].relationship",),
        (_F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,),
    ),
    _D.LINEAGE_PATH_PRECISION: _dimension(
        _D.LINEAGE_PATH_PRECISION,
        "Lineage path precision",
        "Included paths actually connect what they claim to connect, and "
        "each states why it was included.",
        _M.PRECISION,
        _P.HIGHER_IS_BETTER,
        (_S.RELATED_CONTEXT,),
        (
            "related_context.paths[].hops",
            "related_context.paths[].inclusion_reason",
        ),
        (
            _F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,
            _F.PATH_CROSSES_UNAUTHORIZED_ENTITY,
        ),
    ),
    _D.LINEAGE_DIRECTION_CORRECTNESS: _dimension(
        _D.LINEAGE_DIRECTION_CORRECTNESS,
        "Lineage direction correctness",
        "Every hop's direction matches the relationship's canonical "
        "orientation. 'A blocks B' and 'B blocks A' are different claims "
        "about the world and only one of them is actionable.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.RELATED_CONTEXT,),
        (
            "related_context.paths[].hops[].direction",
            "related_context.paths[].hops[].relationship",
        ),
        (_F.REVERSED_RELATIONSHIP_DIRECTION,),
    ),
    _D.CROSS_SOURCE_ASSOCIATION: _dimension(
        _D.CROSS_SOURCE_ASSOCIATION,
        "Cross-source association",
        "The investigation connects facts across source classes rather than "
        "staying inside one. A packet whose entire evidence index is one "
        "source class has not done cross-source association, whatever its "
        "other scores.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.EVIDENCE_COVERAGE,),
        ("evidence_coverage.evidence_index[].source_class",),
        (_F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,),
    ),
    _D.EVIDENCE_CLOSURE: _dimension(
        _D.EVIDENCE_CLOSURE,
        "Evidence closure",
        "Every evidence handle referenced anywhere in the packet is declared "
        "in the evidence index, and every indexed item supports something "
        "the packet includes. Both halves matter: the first stops dangling "
        "citations, the second stops a high-volume evidence dump from "
        "displacing the lineage that was actually asked for.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.EVIDENCE_COVERAGE,),
        (
            "evidence_coverage.evidence_index[].evidence.evidence_ref_id",
            "evidence_coverage.evidence_index[].supports_path_ids",
            "evidence_coverage.evidence_index[].supports_driver_ids",
        ),
        (_F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,),
    ),
    _D.CURRENT_RELEVANCE: _dimension(
        _D.CURRENT_RELEVANCE,
        "Current relevance",
        "What the packet presents as current is current. A dependency that "
        "was removed last quarter is not a current driver, however strong "
        "its historical evidence.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.RELATED_CONTEXT, _S.DRIVER_ANALYSIS),
        (
            "related_context.entities[].relevance",
            "driver_analysis.candidates[].relevance",
        ),
        (_F.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER,),
    ),
    _D.PRINCIPAL_DRIVER_PRECISION: _dimension(
        _D.PRINCIPAL_DRIVER_PRECISION,
        "Principal driver precision",
        "Drivers the packet promotes to principal standing really are the "
        "principal current drivers.",
        _M.PRECISION,
        _P.HIGHER_IS_BETTER,
        (_S.DRIVER_ANALYSIS,),
        ("driver_analysis.principal_driver_ids",),
        (
            _F.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER,
            _F.DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT,
        ),
    ),
    _D.PRINCIPAL_DRIVER_RECALL: _dimension(
        _D.PRINCIPAL_DRIVER_RECALL,
        "Principal driver recall",
        "The principal current drivers a human investigator would name are "
        "present. Reported alongside precision because an arm that promotes "
        "nothing is trivially precise and useless.",
        _M.RECALL,
        _P.HIGHER_IS_BETTER,
        (_S.DRIVER_ANALYSIS,),
        ("driver_analysis.principal_driver_ids", "driver_analysis.candidates"),
        (_F.DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT,),
    ),
    _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION: _dimension(
        _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
        "Symptom versus driver distinction",
        "Rising cycle time is a symptom; the review bottleneck causing it is "
        "a driver. Scored on whether the packet classifies each candidate "
        "correctly, not merely on whether it lists both.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.DRIVER_ANALYSIS,),
        (
            "driver_analysis.candidates[].role",
            "driver_analysis.candidates[].standing",
            "driver_analysis.candidates[].supporting_path_ids",
        ),
        (_F.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER,),
    ),
    _D.UNSUPPORTED_ATTRIBUTION_RATE: _dimension(
        _D.UNSUPPORTED_ATTRIBUTION_RATE,
        "Unsupported attribution rate",
        "How often the packet asserts a causal or measured claim without the "
        "supporting basis it declares. Must trend to zero; unlike the "
        "must-be-zero dimensions it is reported as a rate because partial "
        "credit is meaningful here.",
        _M.RATE,
        _P.MUST_BE_ZERO,
        (_S.DRIVER_ANALYSIS,),
        (
            "driver_analysis.candidates[].assertion_basis",
            "driver_analysis.candidates[].supporting_evidence_ids",
            "driver_analysis.candidates[].confidence_qualifier",
        ),
        (
            _F.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER,
            _F.STAFFING_CERTAINTY_WITHOUT_ALLOCATION_EVIDENCE,
        ),
    ),
    _D.COMPARATIVE_JUDGMENT_SUPPORT: _dimension(
        _D.COMPARATIVE_JUDGMENT_SUPPORT,
        "Comparative judgment support",
        "A comparison question gets a cohort with declared comparison "
        "dimensions, not a list of subjects the reader must compare "
        "themselves.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.COMPARISON_COHORT,),
        (
            "comparison_cohort.supported_comparison_dimensions",
            "comparison_cohort.members",
        ),
        (_F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS,),
    ),
    _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD: _dimension(
        _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
        "Direct answer usefulness beyond dashboard redirection",
        "The packet supports a direct judgment. 'Open the project dashboard' "
        "is not a successful outcome, and a packet whose only substance is "
        "surface references is scored zero here regardless of how well "
        "formed it is.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.DRIVER_ANALYSIS, _S.EVIDENCE_COVERAGE),
        (
            "driver_analysis.candidates[].standing",
            "driver_analysis.candidates[].summary",
            "evidence_coverage.evidence_index",
        ),
        (_F.DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT,),
    ),
    _D.USEFUL_UNCERTAINTY_BEHAVIOUR: _dimension(
        _D.USEFUL_UNCERTAINTY_BEHAVIOUR,
        "Useful uncertainty and limitation behaviour",
        "Limitations named are the ones that actually bit, and are specific "
        "enough to act on. A packet that discloses every possible limitation "
        "on every question scores no better than one that discloses none.",
        _M.RATE,
        _P.HIGHER_IS_BETTER,
        (_S.EVIDENCE_COVERAGE,),
        (
            "evidence_coverage.limitations[].kind",
            "evidence_coverage.missing_sources",
            "evidence_coverage.conflicts",
        ),
        (
            _F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS,
            _F.ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS,
        ),
    ),
    _D.ZERO_UNAUTHORIZED_RESULTS: _dimension(
        _D.ZERO_UNAUTHORIZED_RESULTS,
        "Zero unauthorized or cross-tenant results",
        "No entity, path or evidence item outside the caller's authorized "
        "set appears anywhere in the packet. Must be zero.",
        _M.BOOLEAN_ZERO,
        _P.MUST_BE_ZERO,
        (_S.RELATED_CONTEXT, _S.EVIDENCE_COVERAGE),
        (
            "related_context.authorized_entity_ids",
            "related_context.paths[].hops",
            "related_context.authorization_filtered_count",
        ),
        (
            _F.PATH_CROSSES_UNAUTHORIZED_ENTITY,
            _F.ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS,
        ),
    ),
    _D.ZERO_PERSON_LEVEL_RANKING: _dimension(
        _D.ZERO_PERSON_LEVEL_RANKING,
        "Zero person-level ranking",
        "No person is ranked, scored or compared. Must be zero, and is "
        "structurally impossible in this contract: there is no person "
        "subject kind and no per-person comparison dimension, so the "
        "dimension scores the *arm's whole surface*, not just its packet.",
        _M.BOOLEAN_ZERO,
        _P.MUST_BE_ZERO,
        (_S.SUBJECT_DISCOVERY, _S.COMPARISON_COHORT, _S.DRIVER_ANALYSIS),
        (
            "subject_discovery.candidates[].subject_kind",
            "comparison_cohort.supported_comparison_dimensions",
        ),
        (_F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS,),
    ),
    _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY: _dimension(
        _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
        "Zero unsupported staffing certainty",
        "No staffing or capacity claim is presented as certain without "
        "allocation evidence. Must be zero. The converse is explicitly not "
        "scored here: a missing denominator must reduce confidence, never "
        "make the question unsupported.",
        _M.BOOLEAN_ZERO,
        _P.MUST_BE_ZERO,
        (_S.DRIVER_ANALYSIS,),
        (
            "driver_analysis.candidates[].staffing_qualification",
            "driver_analysis.candidates[].confidence_qualifier",
        ),
        (_F.STAFFING_CERTAINTY_WITHOUT_ALLOCATION_EVIDENCE,),
    ),
    _D.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE: _dimension(
        _D.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE,
        "Zero graph-native surface leakage",
        "No backend identity, query language, node identifier or traversal "
        "syntax reaches the packet. Must be zero. Enforced structurally by "
        "the contract's own vocabulary and checked against the generated "
        "schemas, not merely reviewed.",
        _M.BOOLEAN_ZERO,
        _P.MUST_BE_ZERO,
        (_S.VERSIONS, _S.RELATED_CONTEXT),
        ("versions.trial", "related_context.paths[].hops[].relationship"),
        (_F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS,),
    ),
}


def _fault(
    fault_mode_id: FaultModeID,
    title: str,
    bad_behaviour: str,
    mechanism: RejectingMechanism,
    validator: tuple[str, str] | None,
    dimension_ids: tuple[ScoringDimensionID, ...],
    proving_test: str,
) -> FaultMode:
    return FaultMode(
        schema_version="ask_dev_fault_mode.v1",
        fault_mode_id=fault_mode_id,
        title=title,
        bad_behaviour=bad_behaviour,
        rejecting_mechanism=mechanism,
        validator_reference=(
            None
            if validator is None
            else ValidatorReference(
                model_name=validator[0], validator_name=validator[1]
            )
        ),
        dimension_ids=dimension_ids,
        proving_test=proving_test,
    )


_TESTS = "tests/api/dev/test_chaos_3615_fault_modes.py"

FAULT_MODE_REGISTRY: Mapping[FaultModeID, FaultMode] = {
    _F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST: _fault(
        _F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,
        "A wrong but similarly named subject ranks above the correct target",
        "An arm commits to 'Nightfall' when the question meant 'Nightfall "
        "Migration', on the strength of a fuzzy label match alone. The "
        "contract cannot know which is correct -- that is the oracle's job -- "
        "but it can refuse the shape that makes the mistake invisible: a "
        "committed subject whose every match signal is fuzzy, or a ranked "
        "candidate with no stated signal at all.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("SubjectDiscovery", "validate_commitment_is_evidenced"),
        (
            _D.SUBJECT_TOP_1,
            _D.SUBJECT_TOP_3,
            _D.ALIAS_ACRONYM_RENAME_RESOLUTION,
            _D.CLARIFICATION_CANDIDATE_PRECISION,
            _D.CONVERSATIONAL_REFERENCE_RESOLUTION,
        ),
        f"{_TESTS}::test_commitment_on_fuzzy_label_alone_is_rejected",
    ),
    _F.ORGANIZATION_WIDENING_AFTER_UNRESOLVED_REFERENCE: _fault(
        _F.ORGANIZATION_WIDENING_AFTER_UNRESOLVED_REFERENCE,
        "Organization-wide widening after an unresolved named reference",
        "The question named a subject, resolution failed, and the arm "
        "answered about the whole organization instead of asking. Rejected "
        "unless the packet's outcome is NEEDS_CLARIFICATION and it carries a "
        "subject clarification need.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("AskDevInvestigationPacket", "validate_no_unsafe_organization_widening"),
        (
            _D.NO_UNSAFE_ORGANIZATION_WIDENING,
            _D.CLARIFICATION_CANDIDATE_PRECISION,
        ),
        f"{_TESTS}::test_organization_widening_after_unresolved_mention_is_rejected",
    ),
    _F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE: _fault(
        _F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,
        "Irrelevant high-volume evidence displaces the expected lineage",
        "An arm floods the evidence index with whatever it had most of -- "
        "commits, comments -- none of it attached to any included entity, "
        "path or driver, burying the lineage that was actually asked for. "
        "Rejected in both directions: an indexed item that supports nothing, "
        "and a referenced handle that is not indexed.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("InvestigationEvidenceEntry", "validate_supports_something"),
        (
            _D.EVIDENCE_CLOSURE,
            _D.RELEVANT_ENTITY_RECALL,
            _D.RELEVANT_RELATIONSHIP_RECALL,
            _D.LINEAGE_PATH_PRECISION,
            _D.CROSS_SOURCE_ASSOCIATION,
        ),
        f"{_TESTS}::test_evidence_that_supports_nothing_is_rejected",
    ),
    _F.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER: _fault(
        _F.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER,
        "A symptom is labelled the principal driver without supporting paths",
        "'Cycle time is up' promoted to principal driver with no lineage "
        "explaining why. Rejected: principal standing requires the DRIVER "
        "role, at least one supporting path, at least one evidence handle, "
        "and current relevance.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("DriverCandidate", "validate_principal_standing_is_earned"),
        (
            _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
            _D.PRINCIPAL_DRIVER_PRECISION,
            _D.UNSUPPORTED_ATTRIBUTION_RATE,
            _D.CURRENT_RELEVANCE,
        ),
        f"{_TESTS}::test_symptom_promoted_to_principal_driver_is_rejected",
    ),
    _F.STAFFING_CERTAINTY_WITHOUT_ALLOCATION_EVIDENCE: _fault(
        _F.STAFFING_CERTAINTY_WITHOUT_ALLOCATION_EVIDENCE,
        "A staffing claim is presented as certain without allocation evidence",
        "'This project is understaffed' asserted as measured fact with no "
        "allocation denominator. Rejected. The mirror-image drift is "
        "rejected too, by a positive test: a missing denominator with a "
        "QUALIFIED confidence qualifier is a legal, useful packet -- missing "
        "staffing data reduces confidence, it does not make the question "
        "unsupported.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("DriverCandidate", "validate_staffing_claims_are_qualified"),
        (
            _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
            _D.UNSUPPORTED_ATTRIBUTION_RATE,
        ),
        f"{_TESTS}::test_certain_staffing_claim_without_denominator_is_rejected",
    ),
    _F.UNRELATED_COHORT_MEMBER: _fault(
        _F.UNRELATED_COHORT_MEMBER,
        "The cohort includes an unrelated project",
        "A cohort member with no stated inclusion basis, no rationale, or "
        "neither evidence nor an explicit no-evidence classification. "
        "Whether a *well-explained* member is factually relevant remains an "
        "oracle judgment; what the contract removes is the ability to add "
        "one silently.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("CohortMember", "validate_inclusion_is_evidenced"),
        (
            _D.COHORT_PRECISION,
            _D.COHORT_RECALL,
            _D.COHORT_INCLUSION_EXPLAINABILITY,
            _D.COHORT_EXCLUSION_EXPLAINABILITY,
        ),
        f"{_TESTS}::test_cohort_member_without_inclusion_evidence_is_rejected",
    ),
    _F.REVERSED_RELATIONSHIP_DIRECTION: _fault(
        _F.REVERSED_RELATIONSHIP_DIRECTION,
        "A relationship is reversed",
        "'Team owns project' emitted as 'project owns team', or a blocked-by "
        "edge pointing the wrong way. Rejected against the relationship "
        "allowlist's declared canonical orientation.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("LineageHop", "validate_direction_matches_allowlist"),
        (_D.LINEAGE_DIRECTION_CORRECTNESS, _D.LINEAGE_PATH_PRECISION),
        f"{_TESTS}::test_reversed_relationship_direction_is_rejected",
    ),
    _F.PATH_CROSSES_UNAUTHORIZED_ENTITY: _fault(
        _F.PATH_CROSSES_UNAUTHORIZED_ENTITY,
        "A path crosses an unauthorized entity",
        "A lineage path routes through an entity the caller may not see, "
        "leaking its existence through the path even if its own record is "
        "never returned. Rejected: every endpoint of every hop, and every "
        "related entity, must be in the packet's authorized entity set.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("RelatedContext", "validate_paths_stay_inside_authorized_set"),
        (_D.ZERO_UNAUTHORIZED_RESULTS, _D.LINEAGE_PATH_PRECISION),
        f"{_TESTS}::test_path_through_unauthorized_entity_is_rejected",
    ),
    _F.DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT: _fault(
        _F.DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT,
        "Dashboard redirection appears without a direct judgment",
        "A packet claims SUPPORTED while carrying no asserted driver at all "
        "-- the structural form of 'here are some links, you work it out'. "
        "Rejected: a supported outcome requires at least one principal or "
        "contributing driver, each of which must itself be path- and "
        "evidence-backed.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("AskDevInvestigationPacket", "validate_supported_outcome_asserts_a_judgment"),
        (
            _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
            _D.PRINCIPAL_DRIVER_PRECISION,
            _D.PRINCIPAL_DRIVER_RECALL,
        ),
        f"{_TESTS}::test_supported_outcome_without_any_asserted_driver_is_rejected",
    ),
    _F.ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS: _fault(
        _F.ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS,
        "An absent required field silently defaults to a privileged value",
        "The dangerous defaults are all the reassuring ones: "
        "authorization_filtered_count = 0, truncated = False. Each would let "
        "an arm that filtered or truncated look like one that did not. Every "
        "such field on this contract is required with no default, so "
        "omitting it is a validation error rather than a comforting zero.",
        RejectingMechanism.REQUIRED_FIELD,
        None,
        (
            _D.COHORT_EXCLUSION_EXPLAINABILITY,
            _D.ZERO_UNAUTHORIZED_RESULTS,
            _D.USEFUL_UNCERTAINTY_BEHAVIOUR,
        ),
        f"{_TESTS}::test_disclosure_fields_have_no_reassuring_default",
    ),
    _F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS: _fault(
        _F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS,
        "A wildcard or optional field makes a check vacuous",
        "A cohort that claims to compare while declaring zero comparison "
        "dimensions; a question family reduced to one prompt and one metric; "
        "a scoring dimension no fault mode can fail. Each is well-formed and "
        "proves nothing. Rejected at the contract layer for cohorts and at "
        "the registry layer for families and dimensions.",
        RejectingMechanism.CONTRACT_VALIDATOR,
        ("ComparisonCohort", "validate_comparison_is_not_vacuous"),
        (
            _D.COMPARATIVE_JUDGMENT_SUPPORT,
            _D.COHORT_RECALL,
            _D.USEFUL_UNCERTAINTY_BEHAVIOUR,
            _D.ZERO_PERSON_LEVEL_RANKING,
            _D.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE,
        ),
        f"{_TESTS}::test_cohort_claiming_comparison_without_dimensions_is_rejected",
    ),
}


def validate_scoring_registry() -> None:
    """Raise unless both registries are total, cross-referenced and non-vacuous."""

    if set(SCORING_DIMENSION_REGISTRY) != set(ALL_SCORING_DIMENSION_IDS):
        missing = sorted(
            str(item)
            for item in set(ALL_SCORING_DIMENSION_IDS) - set(SCORING_DIMENSION_REGISTRY)
        )
        extra = sorted(
            str(item)
            for item in set(SCORING_DIMENSION_REGISTRY) - set(ALL_SCORING_DIMENSION_IDS)
        )
        raise RuntimeError(
            f"scoring dimension registry is not total; missing={missing}, extra={extra}"
        )
    if set(FAULT_MODE_REGISTRY) != set(ALL_FAULT_MODE_IDS):
        missing = sorted(
            str(item) for item in set(ALL_FAULT_MODE_IDS) - set(FAULT_MODE_REGISTRY)
        )
        extra = sorted(
            str(item) for item in set(FAULT_MODE_REGISTRY) - set(ALL_FAULT_MODE_IDS)
        )
        raise RuntimeError(
            f"fault mode registry is not total; missing={missing}, extra={extra}"
        )

    cited_faults: set[FaultModeID] = set()
    for dimension_id, dimension in SCORING_DIMENSION_REGISTRY.items():
        if dimension.dimension_id is not dimension_id:
            raise RuntimeError(
                f"scoring dimension key {dimension_id} is filed under "
                f"{dimension.dimension_id}"
            )
        if not dimension.fault_mode_ids:
            raise RuntimeError(
                f"scoring dimension {dimension_id} names no fault mode; a "
                "dimension nothing can fail is a vacuous dimension"
            )
        if not dimension.packet_fields or not dimension.packet_sections:
            raise RuntimeError(
                f"scoring dimension {dimension_id} names no packet field or "
                "section, so nothing in a packet could ever be read to score it"
            )
        unknown = sorted(
            str(item)
            for item in set(dimension.fault_mode_ids) - set(ALL_FAULT_MODE_IDS)
        )
        if unknown:
            raise RuntimeError(
                f"scoring dimension {dimension_id} names unknown fault modes: {unknown}"
            )
        cited_faults.update(dimension.fault_mode_ids)

    uncited = sorted(str(item) for item in set(ALL_FAULT_MODE_IDS) - cited_faults)
    if uncited:
        raise RuntimeError(
            f"fault modes no scoring dimension measures: {uncited}; a fault "
            "nothing scores is an unmeasured claim of coverage"
        )

    cited_dimensions: set[ScoringDimensionID] = set()
    for fault_mode_id, fault in FAULT_MODE_REGISTRY.items():
        if fault.fault_mode_id is not fault_mode_id:
            raise RuntimeError(
                f"fault mode key {fault_mode_id} is filed under {fault.fault_mode_id}"
            )
        if not fault.dimension_ids:
            raise RuntimeError(f"fault mode {fault_mode_id} names no scoring dimension")
        unknown_dimensions = sorted(
            str(item)
            for item in set(fault.dimension_ids) - set(ALL_SCORING_DIMENSION_IDS)
        )
        if unknown_dimensions:
            raise RuntimeError(
                f"fault mode {fault_mode_id} names unknown dimensions: "
                f"{unknown_dimensions}"
            )
        contract_rejected = (
            fault.rejecting_mechanism is RejectingMechanism.CONTRACT_VALIDATOR
        )
        if contract_rejected and fault.validator_reference is None:
            raise RuntimeError(
                f"fault mode {fault_mode_id} claims contract rejection but "
                "names no validator; an unnamed validator is an unverifiable "
                "coverage claim"
            )
        if not contract_rejected and fault.validator_reference is not None:
            raise RuntimeError(
                f"fault mode {fault_mode_id} is rejected by "
                f"{fault.rejecting_mechanism}, which has no validator "
                "function to name -- a reference here would be a fake citation"
            )
        cited_dimensions.update(fault.dimension_ids)

    unmeasured = sorted(
        str(item) for item in set(ALL_SCORING_DIMENSION_IDS) - cited_dimensions
    )
    if unmeasured:
        raise RuntimeError(f"scoring dimensions no fault mode exercises: {unmeasured}")


validate_scoring_registry()
