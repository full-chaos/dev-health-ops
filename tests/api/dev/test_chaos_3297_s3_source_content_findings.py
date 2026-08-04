"""Tests for CHAOS-3297 stack #3's ``DevSourceContent`` extension:

``health_findings``/``deficiency_findings`` -- the channel from the new
health.project.v1/health.team.v1/status.portfolio.v1/balance.
team_workload.v1/deficiency.operational.v1 plan steps to frame builders.

Covers: both fields default empty, worst-severity-first-then-id ordering is
enforced structurally (never merely a wiring convention), the truncation
flags are independent booleans a capped tuple alone cannot express, and the
relationship_matrix.py totality tables (RELATIONSHIP_MATRIX,
APPROVED_CONTENT_SLOTS, EVIDENCE_IDENTITY_TABLE) agree with the new
SourceClass members.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts_v2.base import SourceClass, SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    DeficiencyCategory,
    DeficiencyEvidenceClassification,
    DeficiencyFinding,
    DeficiencyRemediation,
    DeficiencySeverity,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationState,
    DimensionState,
    HealthDimension,
    HealthRuleFinding,
    RuleApplicability,
)
from dev_health_ops.api.dev.contracts_v2.result import DevSourceContent
from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
    APPROVED_CONTENT_SLOTS,
    CONTENT_SLOT_FIELDS,
    EVIDENCE_IDENTITY_TABLE,
    RELATIONSHIP_MATRIX,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def _handle(suffix: str) -> str:
    """A valid ``ServerHandle``-shaped UUID ending in ``suffix`` (hex only)."""

    return f"00000000-0000-0000-0000-{suffix:0>12}"


def _health_finding(
    *, finding_id: str, state: DimensionState = DimensionState.AT_RISK
) -> HealthRuleFinding:
    return HealthRuleFinding(
        schema_version="health_rule_finding.v1",
        finding_id=_handle(finding_id),
        rule_id="health_rule.test_rule.v1",
        rule_version="health_rule.test_rule.v1.1",
        dimension=HealthDimension.EXECUTION_COMPLETION,
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        state=state,
        fact_kind="observed",
        shadow_only=False,
        evidence_source_classes=(SourceClass.STATUS_CHANGE,),
        remediation_template="Investigate.",
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        evaluated_at=_NOW,
        suppressed_reason=None,
    )


def _deficiency_finding(
    *, finding_id: str, severity: DeficiencySeverity = DeficiencySeverity.AT_RISK
) -> DeficiencyFinding:
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=_handle(finding_id),
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.test_rule.v1",
        rule_version="deficiency_rule.test_rule.v1",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        severity=severity,
        fact_kind="observed",
        observed_state=SourceRequirementState.UNCONFIGURED,
        data_semantics="not_measured",
        sample_count=None,
        coverage=0.0,
        current_window_days=1,
        comparison_window_days=None,
        evidence_ref_ids=(),
        evidence_classification=DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE,
        blast_radius="Required source is unconfigured.",
        remediation=DeficiencyRemediation(
            schema_version="deficiency_remediation.v1",
            remediation_template="Investigate.",
            verification_condition="Resolves once re-evaluated healthy.",
        ),
        limitations=(),
        evaluated_at=_NOW,
    )


def test_health_and_deficiency_findings_default_empty() -> None:
    content = DevSourceContent(schema_version="dev_source_content.v1")
    assert content.health_findings == ()
    assert content.health_findings_truncated is False
    assert content.deficiency_findings == ()
    assert content.deficiency_findings_truncated is False


def test_health_findings_accepts_worst_severity_first_order() -> None:
    content = DevSourceContent(
        schema_version="dev_source_content.v1",
        health_findings=(
            _health_finding(finding_id="a", state=DimensionState.CRITICAL),
            _health_finding(finding_id="b", state=DimensionState.AT_RISK),
            _health_finding(finding_id="c", state=DimensionState.WATCH),
        ),
    )
    assert [f.finding_id for f in content.health_findings] == [
        _handle("a"),
        _handle("b"),
        _handle("c"),
    ]


def test_health_findings_rejects_out_of_order() -> None:
    """Kill site: a wiring bug that appends findings in discovery order
    (not severity order) must fail construction, not silently ship an
    unreproducible 50-of-N cap.
    """

    with pytest.raises(ValidationError, match="health_findings must be ordered"):
        DevSourceContent(
            schema_version="dev_source_content.v1",
            health_findings=(
                _health_finding(finding_id="b", state=DimensionState.AT_RISK),
                _health_finding(finding_id="a", state=DimensionState.CRITICAL),
            ),
        )


def test_health_findings_same_severity_orders_by_finding_id() -> None:
    with pytest.raises(ValidationError, match="health_findings must be ordered"):
        DevSourceContent(
            schema_version="dev_source_content.v1",
            health_findings=(
                _health_finding(finding_id="f", state=DimensionState.AT_RISK),
                _health_finding(finding_id="a", state=DimensionState.AT_RISK),
            ),
        )


def test_deficiency_findings_accepts_worst_severity_first_order() -> None:
    content = DevSourceContent(
        schema_version="dev_source_content.v1",
        deficiency_findings=(
            _deficiency_finding(finding_id="a", severity=DeficiencySeverity.CRITICAL),
            _deficiency_finding(finding_id="b", severity=DeficiencySeverity.AT_RISK),
            _deficiency_finding(finding_id="c", severity=DeficiencySeverity.WATCH),
        ),
    )
    assert [f.finding_id for f in content.deficiency_findings] == [
        _handle("a"),
        _handle("b"),
        _handle("c"),
    ]


def test_deficiency_findings_rejects_out_of_order() -> None:
    with pytest.raises(ValidationError, match="deficiency_findings must be ordered"):
        DevSourceContent(
            schema_version="dev_source_content.v1",
            deficiency_findings=(
                _deficiency_finding(finding_id="b", severity=DeficiencySeverity.WATCH),
                _deficiency_finding(
                    finding_id="a", severity=DeficiencySeverity.CRITICAL
                ),
            ),
        )


def test_truncation_flags_are_independent_of_the_tuple_itself() -> None:
    """The whole point of the flag: a 1-item tuple can still be truncated
    (50 kept out of 51+), and a 50-item tuple need not be.
    """

    content = DevSourceContent(
        schema_version="dev_source_content.v1",
        health_findings=(_health_finding(finding_id="a"),),
        health_findings_truncated=True,
    )
    assert len(content.health_findings) == 1
    assert content.health_findings_truncated is True


def test_new_source_classes_are_present_in_every_totality_table() -> None:
    for source_class in (SourceClass.HEALTH_PROFILE, SourceClass.DEFICIENCY_INVENTORY):
        assert source_class in RELATIONSHIP_MATRIX
        assert source_class in APPROVED_CONTENT_SLOTS


def test_health_profile_slot_is_only_health_findings() -> None:
    assert APPROVED_CONTENT_SLOTS[SourceClass.HEALTH_PROFILE] == frozenset(
        {"health_findings"}
    )


def test_deficiency_inventory_slot_is_only_deficiency_findings() -> None:
    assert APPROVED_CONTENT_SLOTS[SourceClass.DEFICIENCY_INVENTORY] == frozenset(
        {"deficiency_findings"}
    )


def test_new_content_fields_are_registered_in_content_slot_fields() -> None:
    assert "health_findings" in CONTENT_SLOT_FIELDS
    assert "deficiency_findings" in CONTENT_SLOT_FIELDS


def test_new_evidence_identity_cells_are_accepted_risk_with_rationale() -> None:
    for field in ("health_findings", "deficiency_findings"):
        cell = EVIDENCE_IDENTITY_TABLE[field]
        assert cell.mode == "accepted_risk"
        assert cell.derive is None
        assert cell.rationale
