"""CHAOS-3618: the native capability table must be honest, not convenient.

These tests exist because the failure mode of a baseline arm is not a
crash — it is a table that quietly says "available" for a relationship the
run never establishes, which inflates the baseline's lineage recall and
corrupts the comparison the whole of CHAOS-3614 exists to run.

So the load-bearing tests here do not restate the table. They cross-check
every row against the *contract's* own relationship allowlist and the
*landed* relationship matrix, both of which are owned elsewhere:

* an ``AVAILABLE`` relationship must have at least one declared endpoint
  pair the native subject vocabulary can actually express;
* a relationship blamed on ``SUBJECT_KIND_ABSENT`` must have **no** such
  pair — otherwise the real blocker is something else and the trial report
  would name the wrong mechanism.

That pairing is what makes the table falsifiable. It already caught one
wrong ruling while being written: ``depends_on`` was blamed on an absent
subject kind when ``project -> project`` is perfectly expressible, and the
real blocker is that no native service emits the edge.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable

import pytest

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
    RelationshipType,
)
from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
    RELATIONSHIP_MATRIX,
)
from dev_health_ops.context_fabric.native_arm import capabilities as caps

_NATIVE_KINDS = frozenset(caps.NATIVE_SUBJECT_KIND.values())
_REACHABLE_STATES = frozenset({caps.NativeRelationshipState.AVAILABLE})


def _sorted_relationships(
    predicate: Callable[[caps.NativeRelationshipCapability], bool],
) -> list[RelationshipType]:
    """Parametrisation ids stay stable, and mypy keeps the element type."""

    selected = [
        relationship
        for relationship, entry in caps.NATIVE_RELATIONSHIP_CAPABILITY.items()
        if predicate(entry)
    ]
    return sorted(selected, key=lambda relationship: relationship.value)


def _has_fully_native_pair(relationship: RelationshipType) -> bool:
    """Whether any declared endpoint pair uses only natively expressible kinds."""

    orientation = RELATIONSHIP_ALLOWLIST[relationship]
    return any(
        pair.source_kind in _NATIVE_KINDS and pair.target_kind in _NATIVE_KINDS
        for pair in orientation.forward_pairs
    )


# --------------------------------------------------------------------------
# Totality
# --------------------------------------------------------------------------


def test_every_contract_relationship_has_a_native_ruling() -> None:
    assert set(caps.NATIVE_RELATIONSHIP_CAPABILITY) == set(ALL_RELATIONSHIP_TYPES)


def test_every_native_entity_kind_maps_to_a_subject_kind() -> None:
    assert set(caps.NATIVE_SUBJECT_KIND) == set(EntityKind)


def test_unreachable_subject_kinds_are_the_contract_kinds_with_no_native_carrier() -> (
    None
):
    assert caps.UNREACHABLE_SUBJECT_KINDS == frozenset(
        {
            InvestigationSubjectKind.PORTFOLIO,
            InvestigationSubjectKind.INITIATIVE,
            InvestigationSubjectKind.SERVICE,
            InvestigationSubjectKind.DEPENDENCY,
        }
    )
    assert not (caps.UNREACHABLE_SUBJECT_KINDS & _NATIVE_KINDS)


# --------------------------------------------------------------------------
# The falsifiable cross-checks
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "relationship",
    _sorted_relationships(lambda entry: entry.state in _REACHABLE_STATES),
)
def test_available_relationship_has_an_expressible_endpoint_pair(
    relationship: RelationshipType,
) -> None:
    """A relationship we claim to emit must be expressible in native kinds.

    Without this, a row could claim ``deploys`` is available while every
    declared pair terminates on a ``service`` the native path cannot name —
    a packet that could never validate, discovered at trial time.
    """

    assert _has_fully_native_pair(relationship), (
        f"{relationship.value} is marked available, but no declared endpoint pair "
        f"uses only native kinds ({sorted(kind.value for kind in _NATIVE_KINDS)})"
    )


@pytest.mark.parametrize(
    "relationship",
    _sorted_relationships(
        lambda entry: entry.gap_mechanism is caps.NativeGapMechanism.SUBJECT_KIND_ABSENT
    ),
)
def test_subject_kind_absent_is_only_blamed_when_it_is_actually_the_blocker(
    relationship: RelationshipType,
) -> None:
    """The named mechanism must be the real one.

    A gap blamed on the wrong mechanism is worse than an unexplained gap:
    the trial report reads as "the contract needs a new node kind" when the
    truth is "nobody wired the adapter", and the two have completely
    different dispositions.
    """

    assert not _has_fully_native_pair(relationship), (
        f"{relationship.value} is blamed on an absent subject kind, but "
        "at least one declared endpoint pair is fully expressible natively; "
        "the real blocker is something else"
    )


def test_the_table_is_not_vacuous_in_either_direction() -> None:
    """A table that said everything, or nothing, is available measures nothing."""

    states = {entry.state for entry in caps.NATIVE_RELATIONSHIP_CAPABILITY.values()}
    assert caps.NativeRelationshipState.AVAILABLE in states
    assert caps.NativeRelationshipState.UNREACHABLE in states


def test_reachable_rows_name_a_slot_and_unreachable_rows_name_a_mechanism() -> None:
    for relationship, entry in caps.NATIVE_RELATIONSHIP_CAPABILITY.items():
        if entry.state in _REACHABLE_STATES:
            assert entry.content_slot, relationship
            assert entry.gap_mechanism is None, relationship
        else:
            assert entry.gap_mechanism is not None, relationship
            assert entry.content_slot is None, relationship


def test_the_dataclass_rejects_an_unexplained_gap() -> None:
    """The invariant above is enforced, not merely satisfied today."""

    with pytest.raises(ValueError, match="without naming the mechanism"):
        caps.NativeRelationshipCapability(
            relationship=RelationshipType.REVIEWS,
            state=caps.NativeRelationshipState.UNREACHABLE,
            content_slot=None,
            gap_mechanism=None,
            detail="silently unavailable",
        )


def test_the_dataclass_rejects_an_available_row_with_no_slot() -> None:
    with pytest.raises(ValueError, match="names no content slot"):
        caps.NativeRelationshipCapability(
            relationship=RelationshipType.REVIEWS,
            state=caps.NativeRelationshipState.AVAILABLE,
            content_slot=None,
            gap_mechanism=None,
            detail="available from nowhere in particular",
        )


# --------------------------------------------------------------------------
# Source classes
# --------------------------------------------------------------------------


def test_observable_and_unobserved_partition_the_trial_allowlist() -> None:
    allowlisted = frozenset(TRIAL_SOURCE_ALLOWLIST)
    assert caps.OBSERVABLE_SOURCE_CLASSES <= allowlisted
    assert caps.NATIVE_UNOBSERVED_SOURCE_CLASSES <= allowlisted
    assert not (caps.OBSERVABLE_SOURCE_CLASSES & caps.NATIVE_UNOBSERVED_SOURCE_CLASSES)
    assert (
        caps.OBSERVABLE_SOURCE_CLASSES | caps.NATIVE_UNOBSERVED_SOURCE_CLASSES
    ) == allowlisted


@pytest.mark.parametrize(
    "source_class",
    [
        SourceClass.REVIEW,
        SourceClass.CODE_CHANGE,
        SourceClass.TEST_REPORT,
        SourceClass.OPERATIONAL_CONTROL,
        SourceClass.COGNITIVE_LOAD,
        SourceClass.INVESTMENT_ALLOCATION,
    ],
)
def test_a_source_class_no_step_mints_is_reported_unobserved(
    source_class: SourceClass,
) -> None:
    """Named individually so wiring an adapter fails here, loudly.

    These six are exactly the classes the landed relationship matrix marks
    ``not_applicable``. If one of them acquires a real step, this test is
    the thing that says so — and it should then be deleted rather than
    weakened.
    """

    assert RELATIONSHIP_MATRIX[source_class].requirement == "not_applicable"
    assert source_class in caps.NATIVE_UNOBSERVED_SOURCE_CLASSES


def test_the_observable_set_is_derived_from_the_matrix_not_hand_listed() -> None:
    """Every observable class must have a non-``not_applicable`` requirement."""

    for source_class in caps.OBSERVABLE_SOURCE_CLASSES:
        assert RELATIONSHIP_MATRIX[source_class].requirement != "not_applicable"


# --------------------------------------------------------------------------
# Question-family classification
# --------------------------------------------------------------------------


def test_classify_question_family_cannot_see_the_question() -> None:
    """The anti-corpus-tuning guarantee is structural, so assert the structure.

    CHAOS-3618: "Do not add bespoke case-specific logic merely to make the
    baseline score better." The cheapest way to violate that is to branch on
    the question text. A signature that never receives it cannot.
    """

    parameters = set(inspect.signature(caps.classify_question_family).parameters)
    assert parameters == {"intent_id", "shape"}
    forbidden = {"question", "text", "prompt", "utterance", "query", "mentions"}
    assert not (parameters & forbidden)

    shape_parameters = set(inspect.signature(caps.comparison_shape_for).parameters)
    assert not (shape_parameters & forbidden)


def test_every_mapped_family_permits_the_shape_it_is_mapped_for() -> None:
    """Mirrors the import-time check, so a regression names itself here too."""

    for (intent, shape), family in caps.NATIVE_QUESTION_FAMILY.items():
        permitted = QUESTION_FAMILY_REGISTRY[family].permitted_comparison_shapes
        assert shape in permitted, f"{intent.value}/{shape.value} -> {family.value}"


def test_an_unmapped_combination_returns_none_rather_than_a_nearest_guess() -> None:
    """The five metric-shaped intents have no family, and must not acquire one.

    Each reduces to reporting a metric or a source state, which every frozen
    family lists under its prohibited reductions as something an answer may
    not be. Forcing them into the nearest family is precisely how a baseline
    gets flattered.
    """

    for intent in (
        QuestionIntentID.REMAINING_WORK,
        QuestionIntentID.OBSERVED_CHANGE,
        QuestionIntentID.REGISTERED_STATISTICS,
        QuestionIntentID.METRIC_COMPARISON,
        QuestionIntentID.DATA_TRUST,
        QuestionIntentID.BOUNDED_INVESTIGATION,
    ):
        for shape in ComparisonShape:
            assert (
                caps.classify_question_family(intent_id=intent, shape=shape) is None
            ), f"{intent.value}/{shape.value} acquired a family"


def test_organization_wide_only_ever_reaches_the_clarification_family() -> None:
    """Widening must never buy a substantive family.

    ``AskDevInvestigationPacket.validate_no_unsafe_organization_widening``
    is the contract-side guard; this is the producer-side one, so the arm
    cannot construct the offending packet in the first place.
    """

    for (_intent, shape), family in caps.NATIVE_QUESTION_FAMILY.items():
        if shape is ComparisonShape.ORGANIZATION_WIDE:
            assert family.value == "clarification_and_no_match"


def test_an_unresolved_reference_is_what_separates_widening_from_portfolio_scope() -> (
    None
):
    """The two ways native reaches ORGANIZATION_WIDE mean opposite things."""

    assert (
        caps.comparison_shape_for(
            cardinality=Cardinality.ORGANIZATION_WIDE, has_unresolved_mentions=False
        )
        is ComparisonShape.PORTFOLIO_WIDE
    )
    assert (
        caps.comparison_shape_for(
            cardinality=Cardinality.ORGANIZATION_WIDE, has_unresolved_mentions=True
        )
        is ComparisonShape.ORGANIZATION_WIDE
    )


@pytest.mark.parametrize(
    ("cardinality", "expected"),
    [
        (Cardinality.SINGULAR, ComparisonShape.SINGULAR_SUBJECT),
        (Cardinality.PLURAL_COHORT, ComparisonShape.EXPLICIT_COHORT),
    ],
)
def test_named_cardinalities_keep_their_shape_regardless_of_resolution_state(
    cardinality: Cardinality, expected: ComparisonShape
) -> None:
    for unresolved in (False, True):
        assert (
            caps.comparison_shape_for(
                cardinality=cardinality, has_unresolved_mentions=unresolved
            )
            is expected
        )


def test_a_plural_cohort_is_explicit_never_discovered() -> None:
    """Native cohorts are the subjects the user named, and nothing else.

    ``DevSubjectSet`` records committed mentions; it has no inclusion basis
    and no exclusions. Calling that a *discovered* cohort would claim
    cohort construction the native path does not do.
    """

    assert (
        caps.comparison_shape_for(
            cardinality=Cardinality.PLURAL_COHORT, has_unresolved_mentions=False
        )
        is not ComparisonShape.DISCOVERED_COHORT
    )
    assert ComparisonShape.DISCOVERED_COHORT not in {
        shape for _intent, shape in caps.NATIVE_QUESTION_FAMILY
    }
