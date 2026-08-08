"""CHAOS-3615: totality and anti-vacuity of the four frozen registries.

The question families, the scoring dimensions, the fault modes and the trial
allowlists are the part of this issue CHAOS-3616 will actually consume, and
they are the part that fails silently if it is wrong: a family nobody scores,
a dimension no fault can fail, a fault mode whose "validator" does not exist.
Each of those is well formed and proves nothing, so each is asserted here.

Closed vocabularies are iterated through their ``ALL_*`` literal tuples
rather than through the enum objects — CodeQL's
``py/non-iterable-in-for-loop`` false-positives on ``(str, Enum)`` mixins,
which is why the repository publishes literal registries in the first place.
The tuples are proved exhaustive against ``__members__`` below, so the
indirection cannot hide a member.
"""

from __future__ import annotations

import re
from enum import StrEnum
from pathlib import Path

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    ALL_ANALYTICAL_SLICES,
    ALL_ASSERTION_BASES,
    ALL_COMPARISON_DIMENSIONS,
    ALL_COMPARISON_SHAPES,
    ALL_DRIVER_CATEGORIES,
    ALL_DRIVER_ROLES,
    ALL_DRIVER_STANDINGS,
    ALL_FAULT_MODE_IDS,
    ALL_INVESTIGATION_OUTCOMES,
    ALL_INVESTIGATION_SUBJECT_KINDS,
    ALL_PACKET_SECTIONS,
    ALL_PROHIBITED_REDUCTIONS,
    ALL_QUESTION_FAMILY_IDS,
    ALL_RELATIONSHIP_TYPES,
    ALL_SCORING_DIMENSION_IDS,
    ALL_STAFFING_DENOMINATOR_STATES,
    ALL_TRUNCATION_REASONS,
    FAULT_MODE_REGISTRY,
    MANDATORY_PROHIBITED_REDUCTIONS,
    QUESTION_FAMILY_REGISTRY,
    RELATIONSHIP_ALLOWLIST,
    SCORING_DIMENSION_REGISTRY,
    SLICE_BOUNDARIES,
    TRIAL_SOURCE_ALLOWLIST,
    AnalyticalSlice,
    AssertionBasis,
    ComparisonDimension,
    ComparisonShape,
    DriverCategory,
    DriverRole,
    DriverStanding,
    FaultModeID,
    InvestigationOutcome,
    InvestigationSubjectKind,
    PacketSection,
    ProhibitedReduction,
    QuestionFamilyID,
    RejectingMechanism,
    RelationshipType,
    ScoringDimensionID,
    StaffingDenominatorPolicy,
    StaffingDenominatorState,
    TruncationReason,
    validate_question_family_registry,
    validate_relationship_allowlist,
    validate_scoring_registry,
    validate_slice_boundaries,
    validate_trial_source_allowlist,
)
from dev_health_ops.api.dev.investigation_contract import packet as packet_module

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
FAULT_MODE_TEST_FILE = "tests/api/dev/test_chaos_3615_fault_modes.py"

#: ``(literal tuple, enum)`` for every closed vocabulary this package owns.
_TOTALITY_TABLE: tuple[tuple[tuple[StrEnum, ...], type[StrEnum]], ...] = (
    (ALL_INVESTIGATION_SUBJECT_KINDS, InvestigationSubjectKind),
    (ALL_COMPARISON_SHAPES, ComparisonShape),
    (ALL_COMPARISON_DIMENSIONS, ComparisonDimension),
    (ALL_ANALYTICAL_SLICES, AnalyticalSlice),
    (ALL_ASSERTION_BASES, AssertionBasis),
    (ALL_DRIVER_ROLES, DriverRole),
    (ALL_DRIVER_STANDINGS, DriverStanding),
    (ALL_DRIVER_CATEGORIES, DriverCategory),
    (ALL_STAFFING_DENOMINATOR_STATES, StaffingDenominatorState),
    (ALL_TRUNCATION_REASONS, TruncationReason),
    (ALL_INVESTIGATION_OUTCOMES, InvestigationOutcome),
    (ALL_PACKET_SECTIONS, PacketSection),
    (ALL_RELATIONSHIP_TYPES, RelationshipType),
    (ALL_QUESTION_FAMILY_IDS, QuestionFamilyID),
    (ALL_SCORING_DIMENSION_IDS, ScoringDimensionID),
    (ALL_FAULT_MODE_IDS, FaultModeID),
    (ALL_PROHIBITED_REDUCTIONS, ProhibitedReduction),
)


# --------------------------------------------------------------------------
# Closed vocabularies
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("literal", "enum"),
    _TOTALITY_TABLE,
    ids=[enum.__name__ for _, enum in _TOTALITY_TABLE],
)
def test_literal_tuple_is_exhaustive(
    literal: tuple[StrEnum, ...], enum: type[StrEnum]
) -> None:
    """A member added without updating its ``ALL_*`` tuple is a red test."""

    members = set(enum.__members__.values())
    assert set(literal) == members, (
        f"{enum.__name__} literal tuple is out of sync with its members; "
        f"missing={sorted(str(item) for item in members - set(literal))}, "
        f"extra={sorted(str(item) for item in set(literal) - members)}"
    )
    assert len(literal) == len(set(literal)), f"{enum.__name__} tuple repeats a member"


def test_no_person_subject_kind_exists() -> None:
    """Person-level ranking is banned by making it unrepresentable.

    A validator forbidding person subjects could be routed around by a
    future producer; an enum with no person member cannot be. This is the
    structural half of the ban — the wire-level half is asserted in
    ``test_chaos_3615_investigation_contract.py``.
    """

    banned = ("person", "user", "individual", "developer", "employee")
    for kind in ALL_INVESTIGATION_SUBJECT_KINDS:
        assert not any(token in str(kind) for token in banned), (
            f"InvestigationSubjectKind.{kind} makes a person a subject"
        )
    for dimension in ALL_COMPARISON_DIMENSIONS:
        assert not any(token in str(dimension) for token in banned), (
            f"ComparisonDimension.{dimension} compares people"
        )


# --------------------------------------------------------------------------
# Question families
# --------------------------------------------------------------------------


def test_question_family_registry_validates() -> None:
    validate_question_family_registry()


def test_every_family_is_registered_once() -> None:
    assert set(QUESTION_FAMILY_REGISTRY) == set(ALL_QUESTION_FAMILY_IDS)
    assert len(ALL_QUESTION_FAMILY_IDS) == 10


@pytest.mark.parametrize("family_id", ALL_QUESTION_FAMILY_IDS, ids=str)
def test_family_is_not_one_metric_and_not_one_prompt(
    family_id: QuestionFamilyID,
) -> None:
    """The registry's whole anti-reduction rule, per family.

    One exact phrasing is enough to pin what the family *is*; two natural
    variants are the minimum that shows it survives rephrasing. Two source
    classes is the line between an investigation and a dashboard metric, and
    three scoring dimensions the line between an evaluation and a pass/fail.
    """

    family = QUESTION_FAMILY_REGISTRY[family_id]
    assert len(family.exact_variants) >= 1
    assert len(family.natural_variants) >= 2
    assert len(set(family.required_source_classes)) >= 2
    assert len(set(family.required_packet_sections)) >= 2
    assert len(set(family.scoring_dimension_ids)) >= 3
    assert set(MANDATORY_PROHIBITED_REDUCTIONS) <= set(family.prohibited_reductions)
    assert family.permitted_comparison_shapes


@pytest.mark.parametrize("family_id", ALL_QUESTION_FAMILY_IDS, ids=str)
def test_family_sources_are_allowlisted(family_id: QuestionFamilyID) -> None:
    family = QUESTION_FAMILY_REGISTRY[family_id]
    assert set(family.required_source_classes) <= set(TRIAL_SOURCE_ALLOWLIST)


def test_question_variants_are_globally_unique() -> None:
    """The same prompt in two families would make both scores unattributable."""

    ids: list[str] = []
    texts: list[str] = []
    for family in QUESTION_FAMILY_REGISTRY.values():
        for variant in family.exact_variants + family.natural_variants:
            ids.append(variant.variant_id)
            texts.append(variant.text.strip().casefold())
    assert len(set(ids)) == len(ids)
    assert len(set(texts)) == len(texts)


def test_capacity_families_qualify_rather_than_refuse() -> None:
    """Missing staffing data reduces confidence; it never disqualifies.

    Both halves are asserted, because a registry that declared only the
    first would be honoured by an arm that answered "unsupported" to every
    capacity question.
    """

    qualifying = [
        family
        for family in QUESTION_FAMILY_REGISTRY.values()
        if family.staffing_denominator_policy
        is StaffingDenominatorPolicy.QUALIFY_WHEN_ABSENT
    ]
    assert qualifying, "no family qualifies on a missing denominator"
    for family in qualifying:
        assert (
            ProhibitedReduction.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR
            in family.prohibited_reductions
        )
        assert (
            ProhibitedReduction.UNQUALIFIED_STAFFING_CERTAINTY
            in family.prohibited_reductions
        )


def test_every_family_prohibits_person_level_ranking() -> None:
    for family in QUESTION_FAMILY_REGISTRY.values():
        assert ProhibitedReduction.PERSON_LEVEL_RANKING in family.prohibited_reductions


def test_a_family_exists_for_clarification_and_no_match() -> None:
    """Safe no-match behaviour is a scored family, not an error path."""

    family = QUESTION_FAMILY_REGISTRY[QuestionFamilyID.CLARIFICATION_AND_NO_MATCH]
    assert ScoringDimensionID.NO_UNSAFE_ORGANIZATION_WIDENING in (
        family.scoring_dimension_ids
    )
    assert ScoringDimensionID.CLARIFICATION_CANDIDATE_PRECISION in (
        family.scoring_dimension_ids
    )


# --------------------------------------------------------------------------
# Scoring dimensions and fault modes
# --------------------------------------------------------------------------


def test_scoring_registry_validates() -> None:
    validate_scoring_registry()


def test_no_aggregate_score_is_expressible() -> None:
    """The correction addendum's "do not collapse into one score", asserted.

    ``reported_per_question_family`` and ``aggregate_prohibited`` are
    ``Literal[True]`` on the model, so this cannot be flipped by a value
    change — only by a type change, which is a visible contract edit.
    """

    for dimension in SCORING_DIMENSION_REGISTRY.values():
        assert dimension.reported_per_question_family is True
        assert dimension.aggregate_prohibited is True
    names = {str(dimension_id) for dimension_id in ALL_SCORING_DIMENSION_IDS}
    assert not any(
        token in name
        for name in names
        for token in ("overall", "aggregate", "total_score", "composite")
    )


def test_every_dimension_names_a_fault_it_can_fail() -> None:
    for dimension_id, dimension in SCORING_DIMENSION_REGISTRY.items():
        assert dimension.fault_mode_ids, f"{dimension_id} can never fail"
        assert set(dimension.fault_mode_ids) <= set(ALL_FAULT_MODE_IDS)
        assert dimension.packet_fields, f"{dimension_id} reads no packet field"
        assert dimension.packet_sections, f"{dimension_id} reads no packet section"


def test_every_fault_mode_is_measured_by_a_dimension() -> None:
    measured: set[FaultModeID] = set()
    for dimension in SCORING_DIMENSION_REGISTRY.values():
        measured.update(dimension.fault_mode_ids)
    unmeasured = sorted(str(item) for item in set(ALL_FAULT_MODE_IDS) - measured)
    assert not unmeasured, f"fault modes nothing scores: {unmeasured}"


def test_the_eleven_named_fault_shapes_are_all_registered() -> None:
    """The corrective plan's own list, pinned as an exact count and set."""

    assert len(ALL_FAULT_MODE_IDS) == 11
    assert set(FAULT_MODE_REGISTRY) == set(ALL_FAULT_MODE_IDS)


@pytest.mark.parametrize("fault_id", ALL_FAULT_MODE_IDS, ids=str)
def test_contract_rejected_fault_names_a_validator_that_exists(
    fault_id: FaultModeID,
) -> None:
    """A validator reference that does not resolve is a fake citation.

    Resolved against the real module, so a validator renamed during a
    refactor turns the registry red rather than leaving behind a plausible
    docstring pointing at nothing.
    """

    fault = FAULT_MODE_REGISTRY[fault_id]
    if fault.rejecting_mechanism is not RejectingMechanism.CONTRACT_VALIDATOR:
        assert fault.validator_reference is None
        return
    reference = fault.validator_reference
    assert reference is not None
    model = getattr(packet_module, reference.model_name, None)
    assert model is not None, (
        f"{fault_id} names model {reference.model_name}, which does not exist"
    )
    validators = model.__pydantic_decorators__.model_validators
    assert reference.validator_name in validators, (
        f"{fault_id} names validator {reference.model_name}."
        f"{reference.validator_name}, which is not a model validator on it; "
        f"available: {sorted(validators)}"
    )


@pytest.mark.parametrize("fault_id", ALL_FAULT_MODE_IDS, ids=str)
def test_every_fault_mode_names_a_test_that_exists(fault_id: FaultModeID) -> None:
    """The proving test must be real, in the file the registry names.

    Without this, ``proving_test`` would be a coverage claim with nothing
    behind it — precisely the failure the corrective plan calls out.
    """

    fault = FAULT_MODE_REGISTRY[fault_id]
    path_part, _, test_name = fault.proving_test.partition("::")
    assert path_part == FAULT_MODE_TEST_FILE, (
        f"{fault_id} points at {path_part}, not the fault-mode test module"
    )
    assert test_name, f"{fault_id} names no test function"
    source = (REPOSITORY_ROOT / path_part).read_text(encoding="utf-8")
    assert re.search(rf"^def {re.escape(test_name)}\(", source, re.MULTILINE), (
        f"{fault_id} names {test_name}, which is not defined in {path_part}"
    )


# --------------------------------------------------------------------------
# Trial allowlists and slice boundaries
# --------------------------------------------------------------------------


def test_trial_source_allowlist_validates() -> None:
    validate_trial_source_allowlist()


def test_trial_source_allowlist_is_a_strict_subset_of_the_platform() -> None:
    """A trial allowlist equal to the whole enum would bound nothing."""

    allowlist = set(TRIAL_SOURCE_ALLOWLIST)
    platform = set(SourceClass.__members__.values())
    assert allowlist < platform
    assert SourceClass.TEMPORAL_CONTEXT not in allowlist, (
        "CHAOS-3567's TEMPORAL_CONTEXT stub is deliberately inert and must "
        "not be reachable through this allowlist"
    )


def test_relationship_allowlist_validates() -> None:
    validate_relationship_allowlist()


def test_relationship_allowlist_is_bounded_and_oriented() -> None:
    assert set(RELATIONSHIP_ALLOWLIST) == set(ALL_RELATIONSHIP_TYPES)
    for relationship, orientation in RELATIONSHIP_ALLOWLIST.items():
        assert orientation.forward_pairs, f"{relationship} permits no ordering"
        assert orientation.canonical_reading


def test_asymmetric_relationships_reject_the_reverse_ordering() -> None:
    """The orientation registry is what makes 'reversed' detectable.

    An allowlist whose every entry permitted both orderings would satisfy
    every direction check trivially. This asserts at least one asymmetric
    type genuinely refuses its reverse — the property the reversed-hop guard
    depends on.
    """

    asymmetric = [
        orientation
        for orientation in RELATIONSHIP_ALLOWLIST.values()
        if not orientation.symmetric
    ]
    assert asymmetric
    proved = 0
    for orientation in asymmetric:
        for pair in orientation.forward_pairs:
            if pair.source_kind == pair.target_kind:
                continue
            assert orientation.permits(pair.source_kind, pair.target_kind)
            if not orientation.permits(pair.target_kind, pair.source_kind):
                proved += 1
    assert proved, "no asymmetric relationship actually refuses its reverse"


def test_slice_boundaries_validate() -> None:
    validate_slice_boundaries()


def test_current_slice_is_not_blocked_by_the_historical_gap() -> None:
    """CHAOS-3569 constrains history only; current intelligence proceeds.

    The corrective plan forbids blocking current-intelligence discovery on
    historical-edge modelling, and this is that rule as an assertion about
    the declared boundaries.
    """

    current = SLICE_BOUNDARIES[AnalyticalSlice.CURRENT]
    assert current.requires_as_of is False
    assert current.requires_edge_validity is False
    assert current.known_gap is None


def test_historical_slices_declare_the_open_gap() -> None:
    for slice_id in (AnalyticalSlice.HISTORICAL, AnalyticalSlice.CURRENT_VS_HISTORICAL):
        boundary = SLICE_BOUNDARIES[slice_id]
        assert boundary.requires_edge_validity is True
        assert boundary.known_gap is not None
        assert "CHAOS-3569" in boundary.known_gap
