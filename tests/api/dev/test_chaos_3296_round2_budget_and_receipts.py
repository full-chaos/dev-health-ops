"""Round-2 hardening (CHAOS-3296 Codex round 2, 2026-08-02): disclosed
byte-budget truncation at observation construction, and identity-bound
mint receipts.

Round 1 fixed three findings (relationship-path budget, a raised
persistence envelope, content-slot/mint-receipt structural checks). A
scoped re-review confirmed two of round 1's own fixes were incomplete:

1. [HIGH] Raising the persistence envelope to 64KiB only moved the
   "valid run becomes internal_error" bug to a higher threshold -- a
   genuinely contract-valid observation (every category at its own
   ``max_length``, every string at its own bound) still serializes past
   64KiB by an order of magnitude. The fix here is a deterministic,
   DISCLOSED byte budget applied at construction (``PlanExecutor.
   _budgeted_observation``): drop items in a documented priority order,
   mark the contract's own truncation semantics, and keep the persistence
   envelope only as an unreachable hard backstop.
2. [MEDIUM] Round 1's mint-receipt check proved only that a handle was
   minted *somewhere* during a step, not that it was minted *for the fact
   citing it* -- a step could mint once for a real entity and reuse that
   exact handle to "prove" an arbitrary number of fabricated facts.
   ``_MintedReceipt`` now binds each handle to the identity it was
   actually minted against; verification checks every fact's own claimed
   identity against its cited handle's receipt.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.contracts import (
    DevScope,
    DevTimeRange,
    DirectScope,
    FreshnessState,
    MetricID,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import (
    DevCIFactV2,
    DevDeploymentFactV2,
    DevIncidentFactV2,
    DevMetricPoint,
    DevMetricRefV2,
    DevPullRequestFactV2,
    DevRequiredChildFactV2,
    DevScopeV2,
    DevStatusFactV2,
)
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.contracts_v2.result import (
    DevInvestigationResult,
    DevSourceContent,
    DevSourceObservation,
)
from dev_health_ops.api.dev.investigation_plans import (
    PlanExecutor,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)
from dev_health_ops.api.dev.investigation_plans.builtin_steps import (
    _GRAPH_EVIDENCE_SOURCE_VERSION,
    _STATUS_EVIDENCE_SOURCE_VERSION,
    _bind_content,
)
from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
    CONTENT_SLOT_FIELDS,
)
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.persistence import DevPersistenceService
from dev_health_ops.models.dev_persistence import (
    DevAnswerFrame,
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevRunIntent,
    DevRunNarrative,
    DevRunResolution,
    DevRunSourceObservation,
    DevRunStageDiagnostic,
    DevRunSubjectSet,
    DevToolCall,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._chaos_3295_plan_executor import TEST_EVIDENCE_SIGNER, sign_evidence
from tests._helpers import tables_of

ORG_ID = "org_fullchaos"
ROOT_ENTITY_ID = "project-1"
OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)

# Real production default -- the fixture-actually-exceeds-it sanity checks
# below prove against this value, not an artificially small stand-in.
_PERSISTENCE_ENVELOPE_BYTES = 64 * 1024

_PERSISTENCE_TABLES = tables_of(
    User,
    Organization,
    DevConversation,
    DevMessage,
    DevRun,
    DevToolCall,
    DevFeedback,
    DevConversationTombstone,
    DevRunIntent,
    DevRunResolution,
    DevRunSubjectSet,
    DevRunSourceObservation,
    DevAnswerFrame,
    DevRunNarrative,
    DevRunStageDiagnostic,
)


@pytest_asyncio.fixture
async def persistence(tmp_path: Path):
    database = tmp_path / "ask-dev-3296-round2.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_PERSISTENCE_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(
                    id=org_id, slug="ask-dev-3296-round2", name="Ask Dev 3296 R2"
                ),
                User(id=user_id, email="ask-dev-3296-round2@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


async def _persist_observation(
    maker, org_id: uuid.UUID, user_id: uuid.UUID, observation: DevSourceObservation
) -> None:
    """Drive ``observation`` through the exact real persistence path a
    production run takes (``PersistenceRunRecorder.record_investigation_
    result``) -- never a direct/synthetic call -- so success here proves
    the persistence backstop is genuinely unreachable end to end, not just
    that the executor's own byte estimate matches its own budget check."""

    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="What is the status of this project?",
            scope_snapshot={},
        )
        recorder = PersistenceRunRecorder(
            service,
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            run_id=accepted.run.id,
            provider_source="platform",
        )
        result = DevInvestigationResult(
            schema_version="dev_investigation_result.v1",
            result_id=str(uuid.uuid4()),
            plan_id="status.entity.v2",
            plan_version="status.entity.v2.1",
            run_id=str(uuid.uuid4()),
            subject_entity_id=ROOT_ENTITY_ID,
            observations=(observation,),
            completed_steps=("one",),
            skipped_steps=(),
            failed_steps=(),
            relationship_closure_verified=False,
            completed_at=OBSERVED_AT,
        )
        await recorder.record_investigation_result(result)


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
    content_byte_budget: int | None = None,
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
    evidence_signer = TEST_EVIDENCE_SIGNER if verify_mint_receipts else None
    executor = (
        PlanExecutor(
            registry=registry,
            now=_now,
            evidence_signer=evidence_signer,
            content_byte_budget=content_byte_budget,
        )
        if content_byte_budget is not None
        else PlanExecutor(registry=registry, now=_now, evidence_signer=evidence_signer)
    )
    result = await executor.run(
        plan=plan, context=_context(), run_id="run-1", subject_entity_id=ROOT_ENTITY_ID
    )
    assert len(result.observations) == 1
    return result, result.observations[0]


def _queried_outcome(
    content: DevSourceContent,
    *,
    state: SourceRequirementState = SourceRequirementState.AVAILABLE_CURRENT,
) -> StepOutcome:
    return StepOutcome(
        observed_state=state,
        data_semantics="measured_zero",
        usable_fact_count=1,
        content=content,
    )


def _status_fact(index: int) -> DevStatusFactV2:
    return DevStatusFactV2(
        fact_id=f"issue:issue-{index}",
        text=f"Issue {index} is in_progress, blocked on review from teammate",
        evidence_ref_ids=("ev1_" + f"{index:040x}",),
    )


def _observation_json_bytes(observation) -> int:
    """Byte-for-byte the same encoding ``executor._observation_json_bytes``
    (and ``persistence.service._bounded_json``) measure with -- a test-side
    oracle independent of, but consistent with, the production code path.
    """

    encoded = json.dumps(
        observation.model_dump(mode="json"), separators=(",", ":"), sort_keys=True
    )
    return len(encoded.encode("utf-8"))


# -- byte-budget truncation: boundary tests --------------------------------


@pytest.mark.asyncio
async def test_small_content_is_never_truncated():
    content = DevSourceContent(
        schema_version="dev_source_content.v1", status_facts=(_status_fact(0),)
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert observation.content is not None
    assert len(observation.content.status_facts) == 1
    assert observation.observed_state is SourceRequirementState.AVAILABLE_CURRENT
    assert observation.limitation is None
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_status_observation_exactly_at_budget_is_untouched():
    """Drive the same 10-fact content through two executors: one with an
    effectively unlimited budget (to measure its real byte cost), one
    pinned to EXACTLY that measured cost. At the boundary, ``<=`` must
    still accept -- no truncation."""

    content = DevSourceContent(
        schema_version="dev_source_content.v1",
        status_facts=tuple(_status_fact(i) for i in range(10)),
    )

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    _unbudgeted_result, unbudgeted_observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, content_byte_budget=10**9
    )
    exact_budget = _observation_json_bytes(unbudgeted_observation)

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE,
        run=run,
        content_byte_budget=exact_budget,
    )

    assert observation.content is not None
    assert len(observation.content.status_facts) == 10
    assert observation.observed_state is SourceRequirementState.AVAILABLE_CURRENT
    assert observation.limitation is None
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_status_observation_over_budget_truncates_deterministically_and_discloses():
    """One byte less than the natural (unbudgeted) size of an 11-fact
    observation must force at least one drop. The exact number dropped is
    not asserted as a fixed constant -- the disclosure fields the executor
    adds once truncation triggers (a non-null ``limitation``, a downgraded
    ``observed_state``) cost real bytes too, so how many items fit is a
    function of that overhead, not a simple per-item division. What must
    hold, and what this test proves instead: the result is deterministic
    across repeated runs, survivors are always the lowest-priority-index
    PREFIX of the original facts (never dict/set order), the final payload
    actually fits the budget, and the truncation is disclosed, never
    silent."""

    eleven_facts = tuple(_status_fact(i) for i in range(11))

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1", status_facts=eleven_facts
            )
        )

    _r, natural = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, content_byte_budget=10**9
    )
    budget = _observation_json_bytes(natural) - 1

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, content_byte_budget=budget
    )
    _result_again, observation_again = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, content_byte_budget=budget
    )

    assert observation.content is not None
    survivors = observation.content.status_facts
    assert 0 < len(survivors) < 11
    assert [f.fact_id for f in survivors] == [
        f.fact_id for f in eleven_facts[: len(survivors)]
    ]
    assert _observation_json_bytes(observation) <= budget
    assert observation.observed_state is SourceRequirementState.AVAILABLE_STALE
    assert observation.limitation is not None
    dropped_count = 11 - len(survivors)
    assert observation.limitation == f"budget_truncated:status_facts:{dropped_count}"
    assert observation.usable_fact_count == len(survivors)
    assert result.relationship_closure_verified is False
    # Same input, same budget -> byte-identical outcome every time.
    assert [f.fact_id for f in observation_again.content.status_facts] == [
        f.fact_id for f in survivors
    ]
    assert observation_again.limitation == observation.limitation


def _metric_ref(index: int, *, series_points: int = 366) -> DevMetricRefV2:
    scope_v2 = DevScopeV2.model_validate(_scope().model_dump(mode="json"))
    return DevMetricRefV2(
        schema_version="dev_metric_ref.v1",
        metric_ref_id=f"metric:ref-{index}",
        metric_id=MetricID.CYCLE_TIME_P50_HOURS,
        label=f"Cycle Time {index}",
        definition_version="v1",
        unit="hours",
        aggregation="avg",
        display_precision=1,
        resolved_scope=scope_v2,
        dimensions=(f"team=team-{index}",),
        current_window=_scope().time_range,
        comparison_window=None,
        value=12.5,
        comparison_value=None,
        series=tuple(
            DevMetricPoint(timestamp=datetime(2026, 1, 1, tzinfo=UTC), value=float(p))
            for p in range(series_points)
        ),
        query_version="v1",
        source_version="v1",
        freshness=FreshnessState.FRESH,
        coverage=1.0,
        evidence_ref_ids=("ev1_" + f"{index:040x}",),
    )


@pytest.mark.asyncio
async def test_metric_observation_exactly_at_budget_is_untouched():
    refs = tuple(_metric_ref(i) for i in range(3))

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(
            DevSourceContent(schema_version="dev_source_content.v1", metric_refs=refs)
        )

    _r, unbudgeted = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run, content_byte_budget=10**9
    )
    exact_budget = _observation_json_bytes(unbudgeted)

    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run, content_byte_budget=exact_budget
    )

    assert observation.content is not None
    assert len(observation.content.metric_refs) == 3
    assert observation.limitation is None
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_metric_observation_over_budget_truncates_deterministically_and_discloses():
    """Same reasoning as the status-observation equivalent: the disclosure
    fields cost bytes once truncation triggers, so the exact survivor count
    is not asserted as a hardcoded constant -- determinism, priority-order
    survival, budget compliance, and disclosure are."""

    four_refs = tuple(_metric_ref(i) for i in range(4))

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1", metric_refs=four_refs
            )
        )

    _r, natural = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run, content_byte_budget=10**9
    )
    budget = _observation_json_bytes(natural) - 1

    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run, content_byte_budget=budget
    )
    _result_again, observation_again = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run, content_byte_budget=budget
    )

    assert observation.content is not None
    survivors = observation.content.metric_refs
    assert 0 < len(survivors) < 4
    assert [r.metric_ref_id for r in survivors] == [
        r.metric_ref_id for r in four_refs[: len(survivors)]
    ]
    assert _observation_json_bytes(observation) <= budget
    assert observation.limitation is not None
    dropped_count = 4 - len(survivors)
    assert observation.limitation == f"budget_truncated:metric_refs:{dropped_count}"
    assert result.relationship_closure_verified is False
    assert [r.metric_ref_id for r in observation_again.content.metric_refs] == [
        r.metric_ref_id for r in survivors
    ]


# -- the persistence backstop must become unreachable ----------------------


def _contract_max_status_content() -> DevSourceContent:
    """Every bound the STATUS_CHANGE contract slots actually allow, maxed
    out simultaneously -- the independently-verified ~615KiB true worst
    case from the round-2 Codex finding."""

    def evidence(prefix: str, index: int) -> tuple[str, ...]:
        return tuple(
            f"ev1_{prefix}{n:0{40 - len(prefix)}x}" for n in range(index, index + 25)
        )

    status_facts = tuple(
        DevStatusFactV2(
            fact_id="i" * 128, text="t" * 2048, evidence_ref_ids=evidence("a", i)
        )
        for i in range(25)
    )
    required_children = tuple(
        DevRequiredChildFactV2(
            fact_id="i" * 128,
            text="t" * 2048,
            status="s" * 128,
            evidence_ref_ids=evidence("b", i),
        )
        for i in range(100)
    )
    pull_requests = tuple(
        DevPullRequestFactV2(
            entity_id="i" * 128,
            display_label="d" * 256,
            state="s" * 128,
            review_state="r" * 128,
            changes_requested=1000,
            merged=False,
            required=True,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=evidence("c", i),
        )
        for i in range(25)
    )
    ci_checks = tuple(
        DevCIFactV2(
            entity_id="i" * 128,
            display_label="d" * 256,
            conclusion="c" * 128,
            required=True,
            skipped_required_work=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=evidence("d", i),
        )
        for i in range(25)
    )
    deployments = tuple(
        DevDeploymentFactV2(
            entity_id="i" * 128,
            display_label="d" * 256,
            status="s" * 128,
            environment="e" * 128,
            required=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=evidence("e", i),
        )
        for i in range(25)
    )
    incidents = tuple(
        DevIncidentFactV2(
            entity_id="i" * 128,
            display_label="d" * 256,
            status="s" * 128,
            active=False,
            blocking=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=evidence("f", i),
        )
        for i in range(25)
    )
    return DevSourceContent(
        schema_version="dev_source_content.v1",
        status_facts=status_facts,
        required_children=required_children,
        pull_requests=pull_requests,
        ci_checks=ci_checks,
        deployments=deployments,
        incidents=incidents,
    )


@pytest.mark.asyncio
async def test_contract_max_status_shape_never_exceeds_the_persistence_envelope():
    """The independently-verified ~615KiB true contract maximum, driven
    through the real (default-budget) executor, must fit the real 64KiB
    persistence envelope -- disclosed as TRUNCATED, never raising."""

    content = _contract_max_status_content()

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    _r, unbudgeted = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, content_byte_budget=10**9
    )
    unbudgeted_bytes = _observation_json_bytes(unbudgeted)
    assert unbudgeted_bytes > _PERSISTENCE_ENVELOPE_BYTES, (
        "fixture must actually exceed the real envelope to prove anything"
    )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert _observation_json_bytes(observation) <= _PERSISTENCE_ENVELOPE_BYTES
    assert observation.observed_state in (
        SourceRequirementState.AVAILABLE_STALE,
        SourceRequirementState.TRUNCATED,
    )
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_codex_dense_metric_shape_never_exceeds_the_persistence_envelope():
    """Codex's own round-2 counterexample: eight metric refs each with a
    full 366-point series serialized to 150,740 bytes in isolation."""

    refs = tuple(_metric_ref(i, series_points=366) for i in range(8))
    content = DevSourceContent(schema_version="dev_source_content.v1", metric_refs=refs)

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    _r, unbudgeted = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run, content_byte_budget=10**9
    )
    unbudgeted_bytes = _observation_json_bytes(unbudgeted)
    assert unbudgeted_bytes > _PERSISTENCE_ENVELOPE_BYTES, (
        "fixture must actually exceed the real envelope to prove anything"
    )

    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run
    )

    assert _observation_json_bytes(observation) <= _PERSISTENCE_ENVELOPE_BYTES
    del result


@pytest.mark.asyncio
async def test_truncation_never_leaves_a_dangling_relationship_path():
    """Fabrication-by-omission control: every relationship path in a
    truncated observation must cite a fact that is still actually present
    in the (shrunk) content -- never a path left over from a dropped fact."""

    content = _contract_max_status_content()

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    _result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )
    assert observation.content is not None

    surviving_targets = {
        field: {
            getattr(item, "fact_id", getattr(item, "entity_id", None))
            for item in getattr(observation.content, field)
        }
        for field in CONTENT_SLOT_FIELDS
    }
    all_surviving_ids = {
        i for ids in surviving_targets.values() for i in ids if i is not None
    }
    for path in observation.relationship_paths:
        assert path.target_entity_id in all_surviving_ids, (
            f"relationship path {path.path_id} cites {path.target_entity_id!r}, "
            "which is not present in the truncated content"
        )


# -- identity-bound mint receipts -------------------------------------------


class _MintOnlyRuntime:
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
        return sign_evidence(
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


@pytest.mark.asyncio
async def test_reused_handle_across_unrelated_facts_is_rejected():
    """RED: the exact Codex round-2 repro. A step mints one real handle for
    issue-1, then reuses that same handle to "prove" a wholly fabricated
    fact about issue-999. Must be rejected -- never raise, closure False."""

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        real_text = "Issue one really is in_progress"
        real_handle = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=_bind_content(
                _STATUS_EVIDENCE_SOURCE_VERSION, claim=real_text
            ),
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        real_fact = DevStatusFactV2(
            fact_id="issue:issue-1",
            text=real_text,
            evidence_ref_ids=(real_handle,),
        )
        forged_fact = DevStatusFactV2(
            fact_id="issue:issue-999-fabricated",
            text="Issue 999 is DEFINITELY DONE (fabricated)",
            evidence_ref_ids=(real_handle,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                status_facts=(real_fact, forged_fact),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 1
    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert "issue-999-fabricated" in observation.limitation
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_correct_handle_on_correct_fact_is_accepted():
    """Positive control: a handle cited by the exact fact it was minted for
    passes identity verification."""

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        text = "Issue one is in_progress"
        handle = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=_bind_content(_STATUS_EVIDENCE_SOURCE_VERSION, claim=text),
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
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

    assert observation.content is not None
    assert len(observation.content.status_facts) == 1
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_two_facts_each_citing_their_own_handle_are_both_accepted():
    """Cross-fact control: two DIFFERENT facts, each citing a handle
    genuinely minted for that fact's own entity -- neither is a reuse of
    the other's handle, so both must be accepted."""

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        text_a = "Issue one is in_progress"
        text_b = "Issue two is done"
        handle_a = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=_bind_content(_STATUS_EVIDENCE_SOURCE_VERSION, claim=text_a),
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        handle_b = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=_bind_content(_STATUS_EVIDENCE_SOURCE_VERSION, claim=text_b),
            entity_type="issue",
            entity_id="issue-2",
            display_label="Issue Two",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        fact_a = DevStatusFactV2(
            fact_id="issue:issue-1",
            text=text_a,
            evidence_ref_ids=(handle_a,),
        )
        fact_b = DevStatusFactV2(
            fact_id="issue:issue-2",
            text=text_b,
            evidence_ref_ids=(handle_b,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1", status_facts=(fact_a, fact_b)
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 2
    assert observation.content is not None
    assert len(observation.content.status_facts) == 2
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_graph_edge_handle_reuse_is_rejected():
    """RED (CHAOS-3296 Codex round 3, [HIGH]): round 2 excluded graph_edges
    from identity comparison because ``DevGraphEdgeV2`` never preserved the
    ``edge_id`` minting bound identity to -- a handle genuinely minted for
    edge-1, reused verbatim on a fabricated second edge, passed both the
    existence check (the handle really was minted) and (vacuously) identity
    comparison (which never ran for this category at all). Round 4 adds
    ``DevGraphEdgeV2.edge_id`` and a real identity cell for graph_edges --
    this must now be rejected exactly like the round-2 status-fact reuse
    repro."""

    from dev_health_ops.api.dev.contracts_v2.embedded import DevGraphEdgeV2

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        handle = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_graph",
            source_version=_bind_content(
                _GRAPH_EVIDENCE_SOURCE_VERSION,
                relationship="references",
                source_entity_id=ROOT_ENTITY_ID,
                target_entity_id="pr-1",
            ),
            entity_type="work_graph_edge",
            entity_id="edge-1",
            display_label="issue-1 references pr-1",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        edge_a = DevGraphEdgeV2(
            edge_id="edge-1",
            source_entity_id=ROOT_ENTITY_ID,
            relationship="references",
            target_entity_id="pr-1",
            provenance="work_graph",
            confidence=1.0,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle,),
        )
        edge_b_reusing_handle = DevGraphEdgeV2(
            edge_id="edge-2-fabricated",
            source_entity_id=ROOT_ENTITY_ID,
            relationship="references",
            target_entity_id="pr-2-unrelated",
            provenance="work_graph",
            confidence=1.0,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                graph_edges=(edge_a, edge_b_reusing_handle),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 1
    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert "edge-2-fabricated" in observation.limitation
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_two_graph_edges_each_citing_their_own_handle_are_both_accepted():
    """Cross-edge positive control, mirroring the status-fact equivalent."""

    from dev_health_ops.api.dev.contracts_v2.embedded import DevGraphEdgeV2

    runtime = _MintOnlyRuntime()

    async def run(_ctx: StepContext) -> StepOutcome:
        handle_a = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_graph",
            source_version=_bind_content(
                _GRAPH_EVIDENCE_SOURCE_VERSION,
                relationship="references",
                source_entity_id=ROOT_ENTITY_ID,
                target_entity_id="pr-1",
            ),
            entity_type="work_graph_edge",
            entity_id="edge-1",
            display_label="issue-1 references pr-1",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        handle_b = runtime.mint_evidence(
            org_id=ORG_ID,
            source_system="work_graph",
            source_version=_bind_content(
                _GRAPH_EVIDENCE_SOURCE_VERSION,
                relationship="references",
                source_entity_id=ROOT_ENTITY_ID,
                target_entity_id="pr-2",
            ),
            entity_type="work_graph_edge",
            entity_id="edge-2",
            display_label="issue-1 references pr-2",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        edge_a = DevGraphEdgeV2(
            edge_id="edge-1",
            source_entity_id=ROOT_ENTITY_ID,
            relationship="references",
            target_entity_id="pr-1",
            provenance="work_graph",
            confidence=1.0,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle_a,),
        )
        edge_b = DevGraphEdgeV2(
            edge_id="edge-2",
            source_entity_id=ROOT_ENTITY_ID,
            relationship="references",
            target_entity_id="pr-2",
            provenance="work_graph",
            confidence=1.0,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle_b,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                graph_edges=(edge_a, edge_b),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, run=run, verify_mint_receipts=True
    )

    assert runtime.mint_calls == 2
    assert observation.content is not None
    assert len(observation.content.graph_edges) == 2
    assert result.relationship_closure_verified is True


# -- end to end through the real persistence path ---------------------------


@pytest.mark.asyncio
async def test_contract_max_status_shape_persists_through_the_real_service(persistence):
    """The full loop: contract-max content -> real (default-budget)
    executor -> real PersistenceRunRecorder.record_investigation_result ->
    real aiosqlite DevPersistenceService. Must complete without raising."""

    maker, org_id, user_id = persistence
    content = _contract_max_status_content()

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    _result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    await _persist_observation(maker, org_id, user_id, observation)


@pytest.mark.asyncio
async def test_codex_dense_metric_shape_persists_through_the_real_service(persistence):
    """Codex's own round-2 counterexample, driven through the same real
    persistence path."""

    maker, org_id, user_id = persistence
    refs = tuple(_metric_ref(i, series_points=366) for i in range(8))
    content = DevSourceContent(schema_version="dev_source_content.v1", metric_refs=refs)

    async def run(_ctx: StepContext) -> StepOutcome:
        return _queried_outcome(content)

    _result, observation = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run
    )

    await _persist_observation(maker, org_id, user_id, observation)
