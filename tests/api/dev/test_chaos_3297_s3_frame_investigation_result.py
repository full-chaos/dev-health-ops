"""Tests for CHAOS-3297 stack #3's ``terminal_frames.wrap_legacy_answer_as_frame``
extension: an optional ``investigation_result`` parameter that embeds a
plan's health/deficiency findings into the SAME frame the legacy model
loop's answer produces (team-lead boundary ruling, 2026-08-02) --
``direct_answer``/``public_outcome``/``completion`` stay driven purely by
the legacy ``DevAnswer``; only ``health_findings``/``deficiency_findings``
are additive.
"""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import UTC, datetime

from dev_health_ops.api.dev import terminal_frames as tf
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevAnswer
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
from dev_health_ops.api.dev.contracts_v2.result import (
    DevInvestigationResult,
    DevSourceContent,
    DevSourceObservation,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_REAL_EVIDENCE_HANDLE = "ev1_" + ("a1b2c3d4e5" * 4)


def _legacy_answer() -> DevAnswer:
    """Mirrors test_terminal_frames.py's own helper: a fully-validated v1
    answer with real evidence handles."""

    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    text = __import__("json").dumps(payload, default=str)
    payload = __import__("json").loads(re.sub(r"ev_\d+", _REAL_EVIDENCE_HANDLE, text))
    # F10 (CHAOS-3297 stack #3): a real v1-sourced metric never carries
    # evidence_ref_ids -- see test_terminal_frames.py's own _legacy_answer
    # for the full rationale.
    for metric in payload.get("metrics", []):
        metric["evidence_ref_ids"] = []
    return DevAnswer.model_validate(payload)


def _handle(suffix: str) -> str:
    return f"00000000-0000-0000-0000-{suffix:0>12}"


def _health_finding(
    *, finding_id: str = "a", state: DimensionState = DimensionState.AT_RISK
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


def _deficiency_finding(*, finding_id: str = "a") -> DeficiencyFinding:
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=_handle(finding_id),
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.test_rule.v1",
        rule_version="deficiency_rule.test_rule.v1",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        severity=DeficiencySeverity.AT_RISK,
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


def _health_observation(
    *,
    health_findings: tuple[HealthRuleFinding, ...] = (),
    truncated: bool = False,
    observation_id: str = "b",
) -> DevSourceObservation:
    return DevSourceObservation(
        schema_version="dev_source_observation.v1",
        observation_id=_handle(observation_id),
        source_class=SourceClass.HEALTH_PROFILE,
        adapter_id="project_health_service.evaluate_project.v1",
        requirement_level="mandatory",
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero" if health_findings else "no_data",
        subject_coverage=1.0,
        usable_fact_count=len(health_findings),
        observed_at=_NOW,
        query_version="health-profile-synthesis.v1",
        content=DevSourceContent(
            schema_version="dev_source_content.v1",
            health_findings=health_findings,
            health_findings_truncated=truncated,
        ),
    )


def _deficiency_observation(
    *,
    deficiency_findings: tuple[DeficiencyFinding, ...] = (),
    truncated: bool = False,
) -> DevSourceObservation:
    return DevSourceObservation(
        schema_version="dev_source_observation.v1",
        observation_id=_handle("c"),
        source_class=SourceClass.DEFICIENCY_INVENTORY,
        adapter_id="operational_deficiency_service.evaluate.v1",
        requirement_level="mandatory",
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero" if deficiency_findings else "no_data",
        subject_coverage=1.0,
        usable_fact_count=len(deficiency_findings),
        observed_at=_NOW,
        query_version="deficiency-operational-inventory.v1",
        content=DevSourceContent(
            schema_version="dev_source_content.v1",
            deficiency_findings=deficiency_findings,
            deficiency_findings_truncated=truncated,
        ),
    )


def _investigation_result(
    *,
    observations: tuple[DevSourceObservation, ...],
    plan_id: str = "health.project.v1",
) -> DevInvestigationResult:
    return DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id=_handle("d"),
        plan_id=plan_id,
        plan_version=f"{plan_id}.0",
        run_id=_handle("e"),
        subject_entity_id="proj-1",
        observations=observations,
        completed_steps=("health_evaluation",),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=True,
        completed_at=_NOW,
    )


def test_no_investigation_result_leaves_findings_empty() -> None:
    answer = _legacy_answer()
    frame = tf.wrap_legacy_answer_as_frame(answer, run_id="run_01")
    assert frame.health_findings == ()
    assert frame.health_findings_truncated is False
    assert frame.deficiency_findings == ()
    assert frame.deficiency_findings_truncated is False


def test_investigation_result_embeds_health_findings_alongside_the_legacy_answer() -> (
    None
):
    answer = _legacy_answer()
    finding = _health_finding()
    result = _investigation_result(
        observations=(_health_observation(health_findings=(finding,)),)
    )

    frame = tf.wrap_legacy_answer_as_frame(
        answer, run_id="run_01", investigation_result=result
    )

    # The legacy answer stays authoritative for direct_answer/public_outcome.
    assert frame.direct_answer == answer.direct_summary
    assert frame.public_outcome.value == "answered_with_gaps"
    assert frame.completion is not None and frame.completion.calculable is False
    # The plan's finding rides alongside.
    assert [f.finding_id for f in frame.health_findings] == [finding.finding_id]
    assert frame.health_findings_truncated is False
    assert frame.deficiency_findings == ()


def test_investigation_result_embeds_deficiency_findings() -> None:
    answer = _legacy_answer()
    finding = _deficiency_finding()
    result = _investigation_result(
        observations=(_deficiency_observation(deficiency_findings=(finding,)),),
        plan_id="deficiency.operational.v1",
    )

    frame = tf.wrap_legacy_answer_as_frame(
        answer, run_id="run_01", investigation_result=result
    )

    assert [f.finding_id for f in frame.deficiency_findings] == [finding.finding_id]
    assert frame.health_findings == ()


def test_investigation_result_flattens_findings_across_observations_in_canonical_order() -> (
    None
):
    """Two observations (health.team.v1 mirrored for the test), each
    individually sorted -- their CONCATENATION is not sorted, and the
    frame's own validator requires it to be: proves capped_health_findings
    re-sorts rather than trusting per-observation pre-sortedness.
    """

    answer = _legacy_answer()
    watch = _health_finding(finding_id="b", state=DimensionState.WATCH)
    critical = _health_finding(finding_id="a", state=DimensionState.CRITICAL)
    result = _investigation_result(
        observations=(
            _health_observation(health_findings=(watch,), observation_id="b1"),
            _health_observation(health_findings=(critical,), observation_id="b2"),
        )
    )

    frame = tf.wrap_legacy_answer_as_frame(
        answer, run_id="run_01", investigation_result=result
    )

    assert [f.finding_id for f in frame.health_findings] == [
        critical.finding_id,
        watch.finding_id,
    ]


def test_investigation_result_preserves_a_per_observation_truncation_signal() -> None:
    """Kill site: an observation whose OWN content already discloses
    truncation (its underlying finding set exceeded 50 before ITS OWN cap)
    must keep disclosing it even though the flattened-and-recapped total
    this function computes lands back under 50 -- re-deriving truncation
    only from len(flattened) > 50 would silently lose that signal.
    """

    answer = _legacy_answer()
    finding = _health_finding()
    result = _investigation_result(
        observations=(_health_observation(health_findings=(finding,), truncated=True),)
    )

    frame = tf.wrap_legacy_answer_as_frame(
        answer, run_id="run_01", investigation_result=result
    )

    assert len(frame.health_findings) == 1
    assert frame.health_findings_truncated is True


def test_frame_construction_with_investigation_result_is_deterministic() -> None:
    answer = _legacy_answer()
    result = _investigation_result(
        observations=(_health_observation(health_findings=(_health_finding(),)),)
    )
    first = tf.wrap_legacy_answer_as_frame(
        answer, run_id="run_determinism", investigation_result=result
    )
    second = tf.wrap_legacy_answer_as_frame(
        answer, run_id="run_determinism", investigation_result=result
    )
    assert first.model_dump(mode="json") == second.model_dump(mode="json")
