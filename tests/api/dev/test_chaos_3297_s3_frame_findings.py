"""Tests for CHAOS-3297 stack #3's ``DevAnswerFrame.health_findings``/
``deficiency_findings`` embedded fields -- the frame-level counterpart to
``DevSourceContent.health_findings``/``deficiency_findings``
(test_chaos_3297_s3_source_content_findings.py), added because there is no
resolution endpoint that dereferences a ``finding_refs``/``deficiency_refs``
opaque id today: a ref-only frame would point at nothing, so the real
finding content is embedded directly.
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
from dev_health_ops.api.dev.contracts_v2.embedded import DevCoverageV2
from dev_health_ops.api.dev.contracts_v2.frame import (
    DevAnswerFact,
    DevAnswerFrame,
    DevAnswerSection,
    DevFrameVersions,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationState,
    DimensionState,
    HealthDimension,
    HealthRuleFinding,
    RuleApplicability,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def _handle(suffix: str) -> str:
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


def _base_fact() -> DevAnswerFact:
    # F10: a fact needs evidence_ref_ids OR a disclosure. This fixture is
    # about health_findings/deficiency_findings, not fact grounding, so an
    # evidence-free legacy-style claim needs a disclosure to satisfy F10
    # without asserting a real minted handle it does not need for this test.
    return DevAnswerFact(
        fact_id="fact_01",
        text="One dimension is at risk.",
        kind="observed",
        evidence_ref_ids=(),
        relationship_path_ids=(),
        confidence=1.0,
        disclosures=("uncertain",),
    )


def _frame(**overrides: object) -> DevAnswerFrame:
    fact = _base_fact()
    base: dict[str, object] = dict(
        schema_version="dev_answer_frame.v1",
        frame_id="00000000-0000-0000-0000-0000000000f1",
        run_id="00000000-0000-0000-0000-0000000000f2",
        generated_at=_NOW,
        public_outcome="answered_with_gaps",
        direct_answer="Repo dev-health has one at-risk dimension.",
        completion=None,
        sections=(
            DevAnswerSection(
                section_id="summary", title="Summary", fact_ids=("fact_01",)
            ),
        ),
        facts=(fact,),
        coverage=DevCoverageV2(
            required_source_count=1,
            available_source_count=1,
            unavailable_required_sources=(),
            stale_required_sources=(),
            as_of=_NOW,
        ),
        limitations=("Health rules are still provisional.",),
        versions=DevFrameVersions(
            interpreter_version="intent_interpreter.v1",
            plan_id="health.project.v1",
            plan_version="health.project.v1.0",
            tool_contract_version="ask_dev_tools.v1",
            metric_definition_version="ask_dev_metrics.v1",
            query_version="ask_dev_queries.v1",
        ),
    )
    base.update(overrides)
    return DevAnswerFrame(**base)


def test_frame_defaults_have_no_findings() -> None:
    frame = _frame()
    assert frame.health_findings == ()
    assert frame.health_findings_truncated is False
    assert frame.deficiency_findings == ()
    assert frame.deficiency_findings_truncated is False


def test_frame_accepts_worst_severity_first_health_findings() -> None:
    frame = _frame(
        health_findings=(
            _health_finding(finding_id="a", state=DimensionState.CRITICAL),
            _health_finding(finding_id="b", state=DimensionState.WATCH),
        )
    )
    assert [f.finding_id for f in frame.health_findings] == [
        _handle("a"),
        _handle("b"),
    ]


def test_frame_rejects_out_of_order_health_findings() -> None:
    with pytest.raises(ValidationError, match="health_findings must be ordered"):
        _frame(
            health_findings=(
                _health_finding(finding_id="b", state=DimensionState.WATCH),
                _health_finding(finding_id="a", state=DimensionState.CRITICAL),
            )
        )


def test_frame_accepts_worst_severity_first_deficiency_findings() -> None:
    frame = _frame(
        deficiency_findings=(
            _deficiency_finding(finding_id="a", severity=DeficiencySeverity.CRITICAL),
            _deficiency_finding(finding_id="b", severity=DeficiencySeverity.WATCH),
        )
    )
    assert [f.finding_id for f in frame.deficiency_findings] == [
        _handle("a"),
        _handle("b"),
    ]


def test_frame_rejects_out_of_order_deficiency_findings() -> None:
    with pytest.raises(ValidationError, match="deficiency_findings must be ordered"):
        _frame(
            deficiency_findings=(
                _deficiency_finding(finding_id="b", severity=DeficiencySeverity.WATCH),
                _deficiency_finding(
                    finding_id="a", severity=DeficiencySeverity.CRITICAL
                ),
            )
        )


def test_frame_truncation_flags_are_independent_of_the_tuple() -> None:
    frame = _frame(
        health_findings=(_health_finding(finding_id="a"),),
        health_findings_truncated=True,
    )
    assert len(frame.health_findings) == 1
    assert frame.health_findings_truncated is True


def _denied_frame(**overrides: object) -> DevAnswerFrame:
    """A minimal, otherwise-valid ``denied`` frame -- every other
    content-bearing field cleared, so ``health_findings``/
    ``deficiency_findings`` is the ONLY field under test (a fuller,
    fixture-driven version of this same rejection is
    test_contracts_v2.py::test_round2_every_absent_frame_field_is_individually_rejected,
    via the "denied_with_health_findings"/"denied_with_deficiency_findings"
    negative fixtures this module registers).
    """

    base: dict[str, object] = dict(
        schema_version="dev_answer_frame.v1",
        frame_id="00000000-0000-0000-0000-0000000000f1",
        run_id="00000000-0000-0000-0000-0000000000f2",
        generated_at=_NOW,
        public_outcome="denied",
        direct_answer="You do not have access to ask about this.",
        sections=(),
        facts=(),
        limitations=(),
        coverage=DevCoverageV2(
            required_source_count=0,
            available_source_count=0,
            unavailable_required_sources=(),
            stale_required_sources=(),
            as_of=_NOW,
        ),
        versions=None,
    )
    base.update(overrides)
    return DevAnswerFrame(**base)


def test_denied_outcome_rejects_health_findings() -> None:
    with pytest.raises(ValidationError, match="health_findings"):
        _denied_frame(health_findings=(_health_finding(finding_id="a"),))


def test_denied_outcome_rejects_deficiency_findings() -> None:
    with pytest.raises(ValidationError, match="deficiency_findings"):
        _denied_frame(deficiency_findings=(_deficiency_finding(finding_id="a"),))
