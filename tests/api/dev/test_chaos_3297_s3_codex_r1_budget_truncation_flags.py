"""CHAOS-3297 stack #3, codex full-branch review round 1 (2026-08-02),
FINDING 1 (CONFIRMED HIGH, ``executor.py:152-156``): once ``health_findings``/
``deficiency_findings`` joined ``CONTENT_SLOT_FIELDS``, the generic
byte-budget ``_drop_lowest_priority_item`` could drop an item from either
slot WITHOUT setting the corresponding ``health_findings_truncated``/
``deficiency_findings_truncated`` flag -- codex reproduced 2 findings
becoming 1 with both the content-level and (downstream) frame-level flags
still ``False``, so the frame builder's own "fewer than 50 survived" check
also silently reads as untruncated. A capped set with no truncation signal
is exactly the false-complete failure mode CHAOS-3297 s2's completion-
denominator fix, and this same field's own docstring
(``DevSourceContent.health_findings_truncated``), both exist to prevent.

This suite plants the defect end to end through the real
``PlanExecutor``: force a byte budget that can only be satisfied by
dropping one of two real findings from a slot with nothing else populated
(so ``_drop_lowest_priority_item`` is guaranteed to target that exact
slot -- ``health_findings``/``deficiency_findings`` sit at the tail of
``CONTENT_SLOT_FIELDS``, the lowest-priority, first-dropped positions),
then asserts the flag survives on the observation's own content AND (via
``terminal_frames.wrap_legacy_answer_as_frame``) on the final frame.
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev import terminal_frames as tf
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevScope,
    DevTimeRange,
    DirectScope,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
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
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.contracts_v2.result import (
    DevInvestigationResult,
    DevSourceContent,
)
from dev_health_ops.api.dev.investigation_plans import (
    PlanExecutor,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)

_ORG_ID = "org_fullchaos"
_ROOT_ENTITY_ID = "proj-1"
_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def _handle(suffix: str) -> str:
    return f"00000000-0000-0000-0000-{suffix:0>12}"


def _health_finding(finding_id: str, state: DimensionState) -> HealthRuleFinding:
    return HealthRuleFinding(
        schema_version="health_rule_finding.v1",
        finding_id=_handle(finding_id),
        rule_id="health_rule.test_rule.v1",
        rule_version="health_rule.test_rule.v1.1",
        dimension=HealthDimension.EXECUTION_COMPLETION,
        subject_kind=RuleApplicability.PROJECT,
        subject_id=_ROOT_ENTITY_ID,
        state=state,
        fact_kind="observed",
        shadow_only=False,
        evidence_source_classes=(SourceClass.STATUS_CHANGE,),
        remediation_template="Investigate the flagged dimension and file a remediation task.",
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        evaluated_at=_NOW,
        suppressed_reason=None,
    )


def _deficiency_finding(finding_id: str) -> DeficiencyFinding:
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=_handle(finding_id),
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.test_rule.v1",
        rule_version="deficiency_rule.test_rule.v1",
        subject_kind=RuleApplicability.PROJECT,
        subject_id=_ROOT_ENTITY_ID,
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
        blast_radius="Required source is unconfigured for this repository.",
        remediation=DeficiencyRemediation(
            schema_version="deficiency_remediation.v1",
            remediation_template="Investigate.",
            verification_condition="Resolves once re-evaluated healthy.",
        ),
        limitations=(),
        evaluated_at=_NOW,
    )


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": _ROOT_ENTITY_ID,
                "display_label": "Project One",
                "repository_id": None,
            }
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 31, tzinfo=UTC),
            timezone="UTC",
        ),
    )


def _context() -> StepContext:
    return StepContext(
        org_id=_ORG_ID,
        permission_fingerprint="fingerprint",
        scope=_scope(),
        run_id="run-1",
        now=_NOW,
    )


def _plan(source_class: SourceClass) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="test.budget.v1",
        plan_version="test.budget.v1.1",
        intent_id=QuestionIntentID.ENTITY_STATUS,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("one",),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=source_class,
                adapter_id="test.one.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )


async def _run_single_step(
    *, source_class: SourceClass, run, content_byte_budget: int
) -> tuple:
    plan = _plan(source_class)
    registry = StepRegistry()
    registry.register(
        PlanStepDefinition(
            step_id="one",
            plan_id=plan.plan_id,
            source_class=source_class,
            adapter_id="test.one.v1",
            requirement_level="mandatory",
            run=run,
        )
    )
    executor = PlanExecutor(
        registry=registry, now=lambda: _NOW, content_byte_budget=content_byte_budget
    )
    result = await executor.run(
        plan=plan, context=_context(), run_id="run-1", subject_entity_id=_ROOT_ENTITY_ID
    )
    assert len(result.observations) == 1
    return result, result.observations[0]


def _observation_json_bytes(observation) -> int:
    encoded = json.dumps(
        observation.model_dump(mode="json"), separators=(",", ":"), sort_keys=True
    )
    return len(encoded.encode("utf-8"))


def _legacy_answer() -> DevAnswer:
    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    text = json.dumps(payload, default=str)
    real_handle = "ev1_" + ("a1b2c3d4e5" * 4)
    payload = json.loads(re.sub(r"ev_\d+", real_handle, text))
    for metric in payload.get("metrics", []):
        metric["evidence_ref_ids"] = []
    return DevAnswer.model_validate(payload)


@pytest.mark.asyncio
async def test_health_findings_drop_sets_the_truncated_flag() -> None:
    findings = (
        _health_finding("a", DimensionState.CRITICAL),
        _health_finding("b", DimensionState.AT_RISK),
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        content = DevSourceContent(
            schema_version="dev_source_content.v1", health_findings=findings
        )
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics="measured_zero",
            usable_fact_count=2,
            content=content,
        )

    _r, natural = await _run_single_step(
        source_class=SourceClass.HEALTH_PROFILE, run=run, content_byte_budget=10**9
    )
    budget = _observation_json_bytes(natural) - 1

    result, observation = await _run_single_step(
        source_class=SourceClass.HEALTH_PROFILE, run=run, content_byte_budget=budget
    )

    assert observation.content is not None
    survivors = observation.content.health_findings
    assert 0 < len(survivors) < 2
    # The bug: this flag was silently left False after a real drop.
    assert observation.content.health_findings_truncated is True

    # And the signal must survive all the way to the final frame.
    investigation_result = DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id=_handle("f0"),
        plan_id="health.project.v1",
        plan_version="health.project.v1.0",
        run_id=_handle("f1"),
        subject_entity_id=_ROOT_ENTITY_ID,
        observations=(observation,),
        completed_steps=("one",),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=result.relationship_closure_verified,
        completed_at=_NOW,
    )
    frame = tf.wrap_legacy_answer_as_frame(
        _legacy_answer(), run_id="run_01", investigation_result=investigation_result
    )
    assert frame.health_findings_truncated is True


@pytest.mark.asyncio
async def test_deficiency_findings_drop_sets_the_truncated_flag() -> None:
    findings = (_deficiency_finding("a"), _deficiency_finding("b"))

    async def run(_ctx: StepContext) -> StepOutcome:
        content = DevSourceContent(
            schema_version="dev_source_content.v1", deficiency_findings=findings
        )
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics="measured_zero",
            usable_fact_count=2,
            content=content,
        )

    _r, natural = await _run_single_step(
        source_class=SourceClass.DEFICIENCY_INVENTORY,
        run=run,
        content_byte_budget=10**9,
    )
    budget = _observation_json_bytes(natural) - 1

    result, observation = await _run_single_step(
        source_class=SourceClass.DEFICIENCY_INVENTORY,
        run=run,
        content_byte_budget=budget,
    )

    assert observation.content is not None
    survivors = observation.content.deficiency_findings
    assert 0 < len(survivors) < 2
    assert observation.content.deficiency_findings_truncated is True

    investigation_result = DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id=_handle("f2"),
        plan_id="deficiency.operational.v1",
        plan_version="deficiency.operational.v1.0",
        run_id=_handle("f3"),
        subject_entity_id=_ROOT_ENTITY_ID,
        observations=(observation,),
        completed_steps=("one",),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=result.relationship_closure_verified,
        completed_at=_NOW,
    )
    frame = tf.wrap_legacy_answer_as_frame(
        _legacy_answer(), run_id="run_01", investigation_result=investigation_result
    )
    assert frame.deficiency_findings_truncated is True
