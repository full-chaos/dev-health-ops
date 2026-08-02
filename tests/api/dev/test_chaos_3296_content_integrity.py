"""Content-integrity controls at observation construction (CHAOS-3296 Codex
finding, MEDIUM, 2026-08-01).

``PlanExecutor._to_observation`` previously accepted a step's
``StepOutcome.content`` unconditionally: (1) nothing checked that the
populated ``DevSourceContent`` slot(s) actually matched the observation's own
``source_class`` (a step registered for one source class could return content
shaped for a different one), and (2) nothing checked that a fact's
``evidence_ref_ids`` were handles this exact step run actually minted through
``PlanExecutorRuntime.mint_evidence`` -- pydantic's ``EvidenceHandle`` pattern
only proves a string is *shaped* like ``ev1_...``, never that it is genuine.
Both gaps only flipped ``relationship_closure_verified`` False at best;
mismatched or forged content was still minted, persisted, and presented like
legitimate evidence.

Fix (``investigation_plans/executor.py`` + ``relationship_matrix.py``):

* ``relationship_matrix.APPROVED_CONTENT_SLOTS`` is a closed, import-time-
  total mapping of which ``DevSourceContent`` field(s) each ``SourceClass``
  may ever populate; ``_to_observation`` rejects a mismatch structurally
  (demotes to an unmeasured observation, drops the content, flips closure
  False) rather than passing it through.
* ``PlanExecutor(evidence_signer=...)`` (opt-in -- ``None`` by default, so
  every pre-3296 test/harness that hand-builds content is unaffected) lets
  the executor verify every evidence handle in a step's content by
  recomputing the real signature (CHAOS-3296 round 5 -- see
  ``executor._evidence_signature_failures``), never merely trusting that a
  string is shaped like ``ev1_...``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import (
    DevScope,
    DevTimeRange,
    DirectScope,
    FreshnessState,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import DevGraphEdgeV2, DevStatusFactV2
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.contracts_v2.result import DevSourceContent
from dev_health_ops.api.dev.investigation_plans import (
    PlanExecutor,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)
from dev_health_ops.api.dev.investigation_plans.builtin_steps import (
    _STATUS_EVIDENCE_SOURCE_VERSION,
    _bind_content,
    _claim_projection,
)
from tests._chaos_3295_plan_executor import (
    TEST_EVIDENCE_SIGNER,
    sign_evidence_for_scope,
)

ORG_ID = "org_fullchaos"
ROOT_ENTITY_ID = "project-1"
OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)


def _now() -> datetime:
    return OBSERVED_AT


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": ROOT_ENTITY_ID,
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
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=_scope(),
        run_id="run-1",
        now=_now(),
    )


def _plan(source_class: SourceClass) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="status.entity.v2",
        plan_version="status.entity.v2.1",
        intent_id=QuestionIntentID.ENTITY_STATUS,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
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
    *,
    source_class: SourceClass,
    run,
    verify_mint_receipts: bool = False,
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
        registry=registry,
        now=_now,
        evidence_signer=TEST_EVIDENCE_SIGNER if verify_mint_receipts else None,
    )
    result = await executor.run(
        plan=plan, context=_context(), run_id="run-1", subject_entity_id=ROOT_ENTITY_ID
    )
    assert len(result.observations) == 1
    return result, result.observations[0]


def _queried_outcome(content: DevSourceContent) -> StepOutcome:
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        usable_fact_count=1,
        content=content,
    )


# -- SourceClass -> content-slot matrix ------------------------------------


@pytest.mark.asyncio
async def test_content_shaped_for_a_different_source_class_is_rejected_structurally():
    """A step registered under STATUS_CHANGE returning WORK_GRAPH-shaped
    content (graph_edges) must never reach relationship-path minting or
    persistence -- demoted to unmeasured, closure disclosed False."""

    edge = DevGraphEdgeV2(
        edge_id="edge-1",
        source_entity_id=ROOT_ENTITY_ID,
        relationship="references",
        target_entity_id="pr-9",
        provenance="work_graph",
        confidence=1.0,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=("ev1_" + "a" * 40,),
    )
    content = DevSourceContent(
        schema_version="dev_source_content.v1", graph_edges=(edge,)
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("content_source_class_mismatch:")
    assert "graph_edges" in observation.limitation
    assert observation.relationship_paths == ()
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_content_matching_its_own_source_class_is_accepted():
    """Positive control: the matrix must not over-reject legitimate content."""

    fact = DevStatusFactV2(
        fact_id="issue:1",
        text="Issue one is in_progress",
        evidence_ref_ids=("ev1_" + "b" * 40,),
    )
    content = DevSourceContent(
        schema_version="dev_source_content.v1", status_facts=(fact,)
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert observation.content is not None
    assert len(observation.content.status_facts) == 1
    assert len(observation.relationship_paths) == 1
    assert result.relationship_closure_verified is True


# -- executor-verified mint receipts ---------------------------------------


class _MintOnlyRuntime:
    """A minimal ``PlanExecutorRuntime`` double: only ``mint_evidence`` is
    ever exercised by the tests below, the rest exist purely for structural
    protocol conformance."""

    def __init__(self) -> None:
        self.mint_calls = 0

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    def list_metrics(self, scope):
        raise AssertionError("not exercised by this suite")

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        raise AssertionError("not exercised by this suite")

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        raise AssertionError("not exercised by this suite")

    def mint_evidence(
        self,
        *,
        org_id,
        source_system,
        source_version,
        entity_type,
        entity_id,
        display_label,
        observed_at,
        freshness,
        confidence=1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ):
        self.mint_calls += 1
        # CHAOS-3296 round 6: route through the scope-bound helper (using
        # THIS file's fixed ``_scope()``) so a handle minted here carries
        # the same authorization-scope digest suffix a genuine
        # ``_scope_bound_mint``-wrapped mint call would.
        return sign_evidence_for_scope(
            scope=_scope(),
            org_id=org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness,
            confidence=confidence,
            repository_ids=repository_ids,
        )


def _status_fact_source_version(*, fact_id: str, text: str) -> str:
    """The exact ``source_version`` a genuine ``wire_status_fact`` mint
    computes for a ``DevStatusFactV2`` with this fact_id/text (round 6:
    ``_identity_status_fact`` projects the WHOLE wire model programmatically,
    not just ``text``)."""

    provisional = DevStatusFactV2(
        fact_id=fact_id, text=text, evidence_ref_ids=("ev1_" + "0" * 40,)
    )
    return _bind_content(
        _STATUS_EVIDENCE_SOURCE_VERSION, _claim_projection(provisional)
    )


@pytest.mark.asyncio
async def test_verify_mint_receipts_off_by_default_accepts_a_forged_handle_unverified():
    """Backward compatibility: with ``verify_mint_receipts`` off (the
    default -- every pre-3296 test/harness), a hand-fabricated handle that
    was never minted at all still passes through exactly as before."""

    fact = DevStatusFactV2(
        fact_id="issue:1",
        text="Issue one is in_progress",
        evidence_ref_ids=("ev1_" + "f" * 40,),  # never minted anywhere
    )
    content = DevSourceContent(
        schema_version="dev_source_content.v1", status_facts=(fact,)
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=False
    )

    assert observation.content is not None
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_verify_mint_receipts_accepts_a_genuinely_minted_handle():
    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        text = "Issue one is in_progress"
        handle = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=_status_fact_source_version(
                fact_id="issue:issue-1", text=text
            ),
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        # fact_id follows the real convention builtin_steps.py wires
        # (``f"{entity_type}:{entity_id}"``) so the identity a genuine
        # verification check derives from the fact matches exactly what
        # was minted -- see ``executor._evidence_signature_failures``.
        # ``text`` must match the ``claim`` bound above verbatim (round 5):
        # ``_identity_status_fact`` recomputes the content digest straight
        # from this same field.
        fact = DevStatusFactV2(
            fact_id="issue:issue-1",
            text=text,
            evidence_ref_ids=(handle,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1", status_facts=(fact,)
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 1
    assert observation.content is not None
    assert len(observation.content.status_facts) == 1
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_verify_mint_receipts_rejects_a_handle_this_step_never_minted():
    """The forgery this finding names: a step embeds a syntactically valid
    ``ev1_...`` handle it fabricated inline rather than obtaining through
    ``mint_evidence`` -- with verification on, this must never reach
    persistence, and the run must still terminate cleanly (never raise).
    CHAOS-3296 round 5: a handle that was never genuinely signed for ANY
    identity fails ``signer.verify`` the same way a handle signed for the
    WRONG identity does -- both report ``evidence_signature_invalid``."""

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        genuine_text = "Issue one is in_progress"
        genuine = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=_status_fact_source_version(
                fact_id="issue:issue-1", text=genuine_text
            ),
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_fact = DevStatusFactV2(
            fact_id="issue:issue-1",
            text=genuine_text,
            evidence_ref_ids=(genuine,),
        )
        forged_fact = DevStatusFactV2(
            fact_id="issue:2",
            text="Issue two is in_progress",
            evidence_ref_ids=("ev1_" + "c" * 40,),  # fabricated, never minted
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                status_facts=(genuine_fact, forged_fact),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 1
    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation == "evidence_signature_invalid:2"
    assert result.relationship_closure_verified is False
