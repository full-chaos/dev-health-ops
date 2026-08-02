"""Tests for CHAOS-3305's ``deficiency.operational.v1`` wire contracts.

Covers the type-level guardrails ``contracts_v2/deficiency.py`` enforces:
``data_semantics`` must agree with ``observed_state`` (queried vs.
unmeasured, mirroring ``DimensionObservation``), every finding carries
evidence or an explicit no-evidence classification (F10, re-checked at the
inventory containment boundary too), category coverage is exactly the
closed eight-category taxonomy, category-declared finding counts and
finding subjects must reconcile against the findings actually present, and
findings are both duplicate-free and deterministically ordered.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    DEFICIENCY_CATEGORIES,
    DeficiencyCategory,
    DeficiencyCategoryStatus,
    DeficiencyEvidenceClassification,
    DeficiencyFinding,
    DeficiencyRemediation,
    DeficiencySeverity,
    OperationalDeficiencyInventory,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import RuleApplicability

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_ORG_ID = "org-1"


def _remediation() -> DeficiencyRemediation:
    return DeficiencyRemediation(
        schema_version="deficiency_remediation.v1",
        remediation_template="Investigate.",
        verification_condition="Resolves once re-evaluated healthy.",
    )


def _finding(
    *,
    finding_id: str = "11111111-1111-1111-1111-111111111111",
    category: DeficiencyCategory = DeficiencyCategory.DATA_INTEGRATION,
    severity: DeficiencySeverity = DeficiencySeverity.CRITICAL,
    subject_id: str = "proj-1",
    observed_state: SourceRequirementState = SourceRequirementState.UNCONFIGURED,
    data_semantics: str = "not_measured",
    evidence_ref_ids: tuple[str, ...] = (),
    evidence_classification: DeficiencyEvidenceClassification | None = (
        DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE
    ),
) -> DeficiencyFinding:
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=finding_id,
        category=category,
        rule_id="deficiency_rule.unconfigured_required_source.v1",
        rule_version="deficiency_rule.unconfigured_required_source.v1",
        subject_kind=RuleApplicability.PROJECT,
        subject_id=subject_id,
        severity=severity,
        fact_kind="observed",
        observed_state=observed_state,
        data_semantics=data_semantics,
        sample_count=None,
        coverage=0.0,
        current_window_days=1,
        comparison_window_days=None,
        evidence_ref_ids=evidence_ref_ids,
        evidence_classification=evidence_classification,
        blast_radius="Required source is unconfigured.",
        remediation=_remediation(),
        limitations=(),
        evaluated_at=_NOW,
    )


def _all_category_statuses(
    *, finding_counts: dict[DeficiencyCategory, int] | None = None
) -> tuple[DeficiencyCategoryStatus, ...]:
    finding_counts = finding_counts or {}
    return tuple(
        DeficiencyCategoryStatus(
            schema_version="deficiency_category_status.v1",
            category=category,
            evaluated=True,
            finding_count=finding_counts.get(category, 0),
            applicability_states_observed=(),
            limitation=None,
        )
        for category in DEFICIENCY_CATEGORIES
    )


def test_deficiency_finding_unmeasured_state_rejects_measured_zero() -> None:
    """An unconfigured/unavailable/unauthorized source is itself the
    deficiency -- it cannot also claim a measured, checked zero.
    """

    with pytest.raises(
        ValidationError, match="must report data_semantics='not_measured'"
    ):
        _finding(
            observed_state=SourceRequirementState.UNCONFIGURED,
            data_semantics="measured_zero",
        )


def test_deficiency_finding_unmeasured_state_accepts_not_measured() -> None:
    finding = _finding(
        observed_state=SourceRequirementState.UNAVAILABLE,
        data_semantics="not_measured",
    )
    assert finding.data_semantics == "not_measured"


def test_deficiency_finding_queried_state_rejects_not_measured() -> None:
    """A stale-but-queried source was genuinely checked -- it cannot claim
    it was never measured.
    """

    with pytest.raises(ValidationError, match="queried observed_state"):
        _finding(
            observed_state=SourceRequirementState.AVAILABLE_STALE,
            data_semantics="not_measured",
        )


def test_deficiency_finding_queried_state_accepts_measured_zero() -> None:
    finding = _finding(
        observed_state=SourceRequirementState.AVAILABLE_STALE,
        data_semantics="measured_zero",
    )
    assert finding.data_semantics == "measured_zero"


def test_deficiency_finding_requires_evidence_or_classification() -> None:
    with pytest.raises(ValidationError, match="evidence_ref_ids or an explicit"):
        _finding(evidence_ref_ids=(), evidence_classification=None)


def test_deficiency_finding_rejects_evidence_and_classification_together() -> None:
    with pytest.raises(ValidationError, match="must not also carry"):
        _finding(
            evidence_ref_ids=("evidence-1",),
            evidence_classification=DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE,
        )


def test_deficiency_finding_accepts_evidence_alone() -> None:
    finding = _finding(evidence_ref_ids=("evidence-1",), evidence_classification=None)
    assert finding.evidence_ref_ids == ("evidence-1",)


def test_deficiency_finding_accepts_classification_alone() -> None:
    finding = _finding()
    assert (
        finding.evidence_classification
        is DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE
    )


def test_category_status_unevaluated_requires_limitation() -> None:
    with pytest.raises(ValidationError, match="requires a bounded limitation"):
        DeficiencyCategoryStatus(
            schema_version="deficiency_category_status.v1",
            category=DeficiencyCategory.INVESTMENT_BALANCE,
            evaluated=False,
            finding_count=0,
            applicability_states_observed=(),
            limitation=None,
        )


def test_category_status_unevaluated_cannot_report_findings() -> None:
    with pytest.raises(ValidationError, match="cannot report findings"):
        DeficiencyCategoryStatus(
            schema_version="deficiency_category_status.v1",
            category=DeficiencyCategory.INVESTMENT_BALANCE,
            evaluated=False,
            finding_count=1,
            applicability_states_observed=(),
            limitation="not evaluated",
        )


def test_inventory_requires_exactly_eight_categories() -> None:
    incomplete_statuses = _all_category_statuses()[:-1]
    with pytest.raises(ValidationError):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(),
            category_statuses=incomplete_statuses,
            evaluated_at=_NOW,
        )


def test_inventory_rejects_duplicate_category() -> None:
    statuses = _all_category_statuses()
    duplicated = (*statuses[:-1], statuses[0])
    with pytest.raises(ValidationError, match="must not repeat a category"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(),
            category_statuses=duplicated,
            evaluated_at=_NOW,
        )


def test_inventory_rejects_duplicate_finding_ids() -> None:
    duplicate_id = "33333333-3333-3333-3333-333333333333"
    findings = (
        _finding(finding_id=duplicate_id, severity=DeficiencySeverity.CRITICAL),
        _finding(finding_id=duplicate_id, severity=DeficiencySeverity.WATCH),
    )
    with pytest.raises(ValidationError, match="must not repeat a finding_id"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=findings,
            category_statuses=_all_category_statuses(
                finding_counts={DeficiencyCategory.DATA_INTEGRATION: 2}
            ),
            evaluated_at=_NOW,
        )


def test_inventory_rejects_unordered_findings() -> None:
    watch = _finding(
        finding_id="44444444-4444-4444-4444-444444444444",
        severity=DeficiencySeverity.WATCH,
    )
    critical = _finding(
        finding_id="55555555-5555-5555-5555-555555555555",
        severity=DeficiencySeverity.CRITICAL,
    )
    with pytest.raises(ValidationError, match="worst-severity-first"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(watch, critical),
            category_statuses=_all_category_statuses(
                finding_counts={DeficiencyCategory.DATA_INTEGRATION: 2}
            ),
            evaluated_at=_NOW,
        )


def test_inventory_accepts_correctly_ordered_findings() -> None:
    critical = _finding(
        finding_id="55555555-5555-5555-5555-555555555555",
        severity=DeficiencySeverity.CRITICAL,
    )
    watch = _finding(
        finding_id="44444444-4444-4444-4444-444444444444",
        severity=DeficiencySeverity.WATCH,
    )
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(critical, watch),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 2}
        ),
        evaluated_at=_NOW,
    )
    assert [f.finding_id for f in inventory.findings] == [
        critical.finding_id,
        watch.finding_id,
    ]


# ---------------------------------------------------------------------------
# Count/subject reconciliation and F10 containment (Codex findings 5 & 6,
# 2026-08-02). Both codex repros reproduced as permanent RED-then-GREEN
# regressions.
# ---------------------------------------------------------------------------


def test_inventory_rejects_category_count_understated_against_real_findings() -> None:
    """Codex repro: finding_count=0 declared for a category that a real
    finding is actually present for.
    """

    finding = _finding(finding_id="77777777-7777-7777-7777-777777777777")
    with pytest.raises(ValidationError, match="finding_count"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(finding,),
            category_statuses=_all_category_statuses(),  # every count 0
            evaluated_at=_NOW,
        )


def test_inventory_rejects_category_count_overstated_against_real_findings() -> None:
    """The converse repro: a declared count with no matching finding present."""

    with pytest.raises(ValidationError, match="finding_count"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(),
            category_statuses=_all_category_statuses(
                finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
            ),
            evaluated_at=_NOW,
        )


def test_inventory_rejects_finding_from_a_different_subject() -> None:
    """Codex repro: an `other-project` finding embedded in `proj-1`'s inventory."""

    foreign_finding = _finding(
        finding_id="88888888-8888-8888-8888-888888888888",
        subject_id="other-project",
    )
    with pytest.raises(ValidationError, match="different subject"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(foreign_finding,),
            category_statuses=_all_category_statuses(
                finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
            ),
            evaluated_at=_NOW,
        )


def test_inventory_rejects_model_copy_forged_finding_missing_evidence() -> None:
    """Codex repro: ``model_copy(update={"evidence_classification": None})``
    on a finding whose ``evidence_ref_ids`` was already empty produces a
    finding satisfying neither side of F10, entirely bypassing
    ``DeficiencyFinding``'s own validator (``model_copy`` never runs
    validators, by pydantic's own design).

    Empirically (this pydantic version/config), placing that forged
    instance into ``findings=`` on a *normally constructed*
    ``OperationalDeficiencyInventory`` still gets caught -- because
    pydantic revalidates the nested model instance on outer construction,
    so ``DeficiencyFinding.validate_evidence_or_classification`` fires
    again and raises first, before this module's own
    ``validate_findings_satisfy_evidence_discipline`` gets a turn. That
    nested-revalidation behavior is a pydantic *default*, not a contract
    guarantee this repo owns or has pinned anywhere -- which is exactly
    why the inventory-level check exists as an explicit, second,
    config-independent guarantee. See
    ``test_inventory_level_f10_check_is_independently_correct`` below for
    a direct proof of that second check in isolation, not merely "some
    validator fired first".
    """

    valid_finding = _finding(finding_id="99999999-9999-9999-9999-999999999999")
    forged = valid_finding.model_copy(update={"evidence_classification": None})
    assert forged.evidence_ref_ids == ()
    assert forged.evidence_classification is None  # bypassed validation entirely

    with pytest.raises(ValidationError, match="requires either evidence_ref_ids"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(forged,),
            category_statuses=_all_category_statuses(
                finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
            ),
            evaluated_at=_NOW,
        )


def test_inventory_rejects_model_copy_forged_finding_with_both_evidence_fields() -> (
    None
):
    """The other F10 half: a finding carrying both evidence_ref_ids and an
    evidence_classification, also only reachable by bypassing
    ``DeficiencyFinding``'s own validator via ``model_copy``. Same
    nested-revalidation caveat as the test above.
    """

    valid_finding = _finding(
        finding_id="10101010-1010-1010-1010-101010101010",
        evidence_ref_ids=("evidence-1",),
        evidence_classification=None,
    )
    forged = valid_finding.model_copy(
        update={
            "evidence_classification": DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE
        }
    )
    assert forged.evidence_ref_ids == ("evidence-1",)
    assert forged.evidence_classification is not None

    with pytest.raises(ValidationError, match="must not also carry"):
        OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id="22222222-2222-2222-2222-222222222222",
            subject_kind=RuleApplicability.PROJECT,
            subject_id="proj-1",
            findings=(forged,),
            category_statuses=_all_category_statuses(
                finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
            ),
            evaluated_at=_NOW,
        )


def test_inventory_level_f10_check_is_independently_correct() -> None:
    """Direct proof that ``validate_findings_satisfy_evidence_discipline``
    itself is correct and reachable, independent of whether pydantic's
    nested-model revalidation happens to also catch a forged finding
    first (see the two tests above). Builds the inventory via
    ``model_construct`` (skips every validator, including this one, the
    same way a forged *inventory* would) and then invokes the method
    directly -- proving the check's own logic, not an accident of
    validator ordering.
    """

    valid_finding = _finding(finding_id="12121212-1212-1212-1212-121212121212")
    forged = valid_finding.model_copy(update={"evidence_classification": None})
    inventory = OperationalDeficiencyInventory.model_construct(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(forged,),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
        ),
        evaluated_at=_NOW,
    )
    with pytest.raises(ValueError, match="violates F10"):
        inventory.validate_findings_satisfy_evidence_discipline()  # type: ignore[operator]

    # Mutation control: a genuinely valid finding must NOT trip this check.
    valid_inventory = OperationalDeficiencyInventory.model_construct(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(valid_finding,),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
        ),
        evaluated_at=_NOW,
    )
    valid_inventory.validate_findings_satisfy_evidence_discipline()  # type: ignore[operator]  # does not raise


# ---------------------------------------------------------------------------
# Codex round 2 (2026-08-02), 3 CONFIRMED findings.
# ---------------------------------------------------------------------------


def test_zero_semantics_totality_covers_every_source_requirement_state() -> None:
    """The import-time totality assertion itself: every
    ``SourceRequirementState`` member must fall into exactly one of
    ``_QUERIED_OBSERVED_STATES``/``_UNMEASURED_OBSERVED_STATES``. This
    re-derives the same check the module performs at import time, so a
    future member silently dropped from both sets fails here too, not
    only via the (harder to attribute) RuntimeError at import.
    """

    from dev_health_ops.api.dev.contracts_v2 import deficiency as deficiency_module

    queried = deficiency_module._QUERIED_OBSERVED_STATES
    unmeasured = deficiency_module._UNMEASURED_OBSERVED_STATES
    assert not (queried & unmeasured)
    assert (queried | unmeasured) == set(SourceRequirementState)


def test_deficiency_finding_unmeasured_state_rejects_no_data() -> None:
    """Codex finding, round 2: the round-1 fix only rejected measured_zero
    in the unmeasured branch, leaving no_data unrejected -- an
    unconfigured source is not the same fact as a queried source that
    came back empty.
    """

    with pytest.raises(
        ValidationError, match="must report data_semantics='not_measured'"
    ):
        _finding(
            observed_state=SourceRequirementState.UNCONFIGURED,
            data_semantics="no_data",
        )


@pytest.mark.parametrize(
    "observed_state",
    [
        SourceRequirementState.UNCONFIGURED,
        SourceRequirementState.UNAVAILABLE,
        SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
        SourceRequirementState.NOT_APPLICABLE,
        SourceRequirementState.TRUNCATED,
    ],
)
def test_deficiency_finding_every_unmeasured_state_rejects_both_queried_semantics(
    observed_state: SourceRequirementState,
) -> None:
    """Exhaustive over every unmeasured state, not just UNCONFIGURED --
    catches a partial fix that only handles the one state a hand-written
    test happens to exercise.
    """

    for bad_semantics in ("measured_zero", "no_data"):
        with pytest.raises(ValidationError):
            _finding(observed_state=observed_state, data_semantics=bad_semantics)
    # The one correct spelling still validates.
    finding = _finding(observed_state=observed_state, data_semantics="not_measured")
    assert finding.data_semantics == "not_measured"


def test_deficiency_finding_available_unknown_is_a_queried_state() -> None:
    """AVAILABLE_UNKNOWN specifically named in the round-2 ask -- a future
    enum addition or a totality-check regression must not silently
    exclude it from the queried partition.
    """

    finding = _finding(
        observed_state=SourceRequirementState.AVAILABLE_UNKNOWN,
        data_semantics="measured_zero",
    )
    assert finding.data_semantics == "measured_zero"
    with pytest.raises(ValidationError, match="never not_measured"):
        _finding(
            observed_state=SourceRequirementState.AVAILABLE_UNKNOWN,
            data_semantics="not_measured",
        )


def test_inventory_model_copy_revalidates_and_rejects_count_desync() -> None:
    """Codex repro: ``model_copy(update={"findings": ()})`` on a valid
    inventory left ``category_statuses`` claiming the old finding count --
    pydantic's base ``model_copy`` never reruns validators. Mirrors
    ``DevScope.model_copy``'s own fix.
    """

    finding = _finding(finding_id="20202020-2020-2020-2020-202020202020")
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(finding,),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
        ),
        evaluated_at=_NOW,
    )
    with pytest.raises(ValidationError, match="finding_count"):
        inventory.model_copy(update={"findings": ()})


def test_inventory_model_copy_rejects_foreign_subject_injection() -> None:
    foreign_finding = _finding(
        finding_id="30303030-3030-3030-3030-303030303030",
        subject_id="other-project",
    )
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(),
        category_statuses=_all_category_statuses(),
        evaluated_at=_NOW,
    )
    with pytest.raises(ValidationError, match="different subject"):
        inventory.model_copy(
            update={
                "findings": (foreign_finding,),
                "category_statuses": _all_category_statuses(
                    finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
                ),
            }
        )


def test_inventory_model_copy_rejects_unordered_findings_injection() -> None:
    watch = _finding(
        finding_id="40404040-4040-4040-4040-404040404040",
        severity=DeficiencySeverity.WATCH,
    )
    critical = _finding(
        finding_id="50505050-5050-5050-5050-505050505050",
        severity=DeficiencySeverity.CRITICAL,
    )
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(critical, watch),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 2}
        ),
        evaluated_at=_NOW,
    )
    with pytest.raises(ValidationError, match="worst-severity-first"):
        inventory.model_copy(update={"findings": (watch, critical)})


def test_inventory_model_copy_rejects_duplicate_finding_ids_injection() -> None:
    duplicate_id = "60606060-6060-6060-6060-606060606060"
    finding_a = _finding(finding_id=duplicate_id, severity=DeficiencySeverity.CRITICAL)
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(finding_a,),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
        ),
        evaluated_at=_NOW,
    )
    finding_b = _finding(finding_id=duplicate_id, severity=DeficiencySeverity.WATCH)
    with pytest.raises(ValidationError, match="must not repeat a finding_id"):
        inventory.model_copy(
            update={
                "findings": (finding_a, finding_b),
                "category_statuses": _all_category_statuses(
                    finding_counts={DeficiencyCategory.DATA_INTEGRATION: 2}
                ),
            }
        )


def test_inventory_model_copy_rejects_f10_forged_finding_injection() -> None:
    valid_finding = _finding(finding_id="70707070-7070-7070-7070-707070707070")
    forged = valid_finding.model_copy(update={"evidence_classification": None})
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(),
        category_statuses=_all_category_statuses(),
        evaluated_at=_NOW,
    )
    with pytest.raises(ValidationError):
        inventory.model_copy(
            update={
                "findings": (forged,),
                "category_statuses": _all_category_statuses(
                    finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
                ),
            }
        )


def test_inventory_model_copy_accepts_a_genuinely_valid_update() -> None:
    """Mutation control / positive path: a legitimate metadata-only update
    must still succeed -- the revalidating override must not become an
    unconditional rejection.
    """

    finding = _finding(finding_id="80808080-8080-8080-8080-808080808080")
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(finding,),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
        ),
        evaluated_at=_NOW,
    )
    later = _NOW + timedelta(hours=1)
    updated = inventory.model_copy(update={"evaluated_at": later})
    assert updated.evaluated_at == later
    assert updated.findings == inventory.findings


# ---------------------------------------------------------------------------
# Codex round 3 (2026-08-02): closed-family subclassing prohibition.
# ---------------------------------------------------------------------------


def test_deficiency_finding_cannot_be_subclassed() -> None:
    """Ratified decision: DeficiencyFinding is a closed wire-contract
    family. Codex's own repro class (a runtime subtype losing its
    identity through model_copy's revalidating round-trip, a PrivateAttr
    reset, a computed-field subclass raising) is dead by construction --
    this test proves the construction itself is impossible, not merely
    that the round-trip happens to handle it.
    """

    with pytest.raises(TypeError, match="may not subclass DeficiencyFinding"):

        class EvilFinding(DeficiencyFinding):  # type: ignore[misc]
            pass


def test_operational_deficiency_inventory_cannot_be_subclassed() -> None:
    with pytest.raises(
        TypeError, match="may not subclass OperationalDeficiencyInventory"
    ):

        class EvilInventory(OperationalDeficiencyInventory):  # type: ignore[misc]
            pass


def test_inventory_model_copy_deep_false_still_revalidates_by_value() -> None:
    """Documented consequence of the round-trip-through-model_validate
    design (see the class docstring): ``deep=False`` does not skip
    rebuilding nested models, because the round-trip always reconstructs
    the whole tree from ``model_dump()``. Callers must compare by value,
    never by nested-object identity -- this test asserts exactly that
    boundary rather than leaving it as an unverified docstring claim.
    """

    finding = _finding(finding_id="90909090-9090-9090-9090-909090909090")
    inventory = OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="22222222-2222-2222-2222-222222222222",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(finding,),
        category_statuses=_all_category_statuses(
            finding_counts={DeficiencyCategory.DATA_INTEGRATION: 1}
        ),
        evaluated_at=_NOW,
    )
    copied = inventory.model_copy(deep=False)
    assert copied == inventory  # value-equal
    assert copied.findings[0] is not inventory.findings[0]  # never identity-preserving
