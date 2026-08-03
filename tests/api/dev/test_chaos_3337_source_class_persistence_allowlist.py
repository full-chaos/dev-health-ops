"""CHAOS-3337: ``wave_3_1_plans.py`` uses ``SourceClass.HEALTH_PROFILE``/
``SourceClass.DEFICIENCY_INVENTORY`` (CHAOS-3297 stack #3, merged in #1387),
but ``persistence/service.py``'s own hand-maintained ``_SOURCE_CLASSES``
frozenset allowlist was never updated -- every one of the four newly-wired
intents (health.project.v1/health.team.v1/balance.team_workload.v1/
deficiency.operational.v1) crashed live to a terminal error the instant the
plan executor's REAL result reached ``DevPersistenceService.
append_source_observation``: ``DevPersistenceValidationError('invalid
source_class')``.

This is the third total-table to miss a SourceClass reconciliation (the
CHAOS-3296/3297 relationship-matrix tables at #1374's merge were the first
two -- see ``investigation_plans/relationship_matrix.py``). Every unit test
in the CHAOS-3297 s3 stack drove these two source classes through FAKE
service doubles and asserted on ``StepOutcome``/``DevSourceContent``
directly -- none of them ever reached the real
``DevPersistenceService``/``PersistenceRunRecorder`` write path, which is
the ONLY place ``_SOURCE_CLASSES`` is checked. That is the gap this suite
closes: every test below drives a REAL ``PlanExecutor`` run through the
REAL persistence layer (an in-memory SQLite-backed
``DevPersistenceService``), for each of the four affected intents.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
from dev_health_ops.api.dev.contracts_v2.base import QuestionIntentID
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    DEFICIENCY_CATEGORIES,
    DeficiencyCategoryStatus,
    OperationalDeficiencyInventory,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import RuleApplicability
from dev_health_ops.api.dev.health_profile_synthesis import HealthProfileResult
from dev_health_ops.api.dev.investigation_plans.executor import PlanExecutor
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.investigation_plans.steps import StepContext
from dev_health_ops.api.dev.investigation_plans.wave_3_1_plans import (
    WAVE_3_1_PLANS_BY_INTENT,
    _source_classes_missing_from_persistence_allowlist,
    build_registry_with_wave_3_1,
)
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.persistence import DevPersistenceService
from dev_health_ops.api.dev.persistence.service import (
    _SOURCE_CLASSES as _PERSISTENCE_SOURCE_CLASSES,
)
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
from tests._helpers import tables_of

_ORG_ID = "org_fullchaos"
_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)

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
    database = tmp_path / "ask-dev-3337.db"
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
                Organization(id=org_id, slug="ask-dev-3337", name="Ask Dev 3337"),
                User(id=user_id, email="ask-dev-3337@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


def _time_range() -> DevTimeRange:
    return DevTimeRange(
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 31, tzinfo=UTC),
        timezone="UTC",
    )


def _project_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": "proj-1",
                "display_label": "Project",
                "repository_id": None,
            }
        ],
        time_range=_time_range(),
    )


def _team_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.TEAM,
        entity_refs=[
            {
                "entity_type": "team",
                "entity_id": "team-1",
                "display_label": "Team",
                "repository_id": None,
            }
        ],
        team_ids=["team-1"],
        time_range=_time_range(),
    )


def _step_context(scope: DevScope) -> StepContext:
    return StepContext(
        org_id=_ORG_ID,
        permission_fingerprint="fingerprint",
        scope=scope,
        run_id="run-1",
        now=_NOW,
    )


def _health_profile_result() -> HealthProfileResult:
    return HealthProfileResult(
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        observations=(),
        launch_findings=(),
        shadow_findings=(),
        suppressed_findings=(),
        observations_by_rule={},
    )


def _deficiency_category_statuses() -> tuple[DeficiencyCategoryStatus, ...]:
    return tuple(
        DeficiencyCategoryStatus(
            schema_version="deficiency_category_status.v1",
            category=category,
            evaluated=True,
            finding_count=0,
            applicability_states_observed=(),
            limitation=None,
        )
        for category in DEFICIENCY_CATEGORIES
    )


def _deficiency_inventory() -> OperationalDeficiencyInventory:
    return OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id="00000000-0000-0000-0000-00000000000f",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        findings=(),
        category_statuses=_deficiency_category_statuses(),
        evaluated_at=_NOW,
    )


class _FakeHealth:
    async def evaluate_project(self, *, org_id, permission_fingerprint, scope, now):
        return _health_profile_result()

    async def evaluate_team(
        self, *, org_id, permission_fingerprint, scope, team_id, now
    ):
        return _health_profile_result()

    async def evaluate_workload(
        self, *, org_id, permission_fingerprint, scope, team_id, now
    ):
        return _health_profile_result()


class _FakeOperationalDeficiency:
    async def evaluate_project(self, *, org_id, permission_fingerprint, scope, now):
        return _deficiency_inventory()

    async def evaluate_team(
        self, *, org_id, permission_fingerprint, scope, team_id, now
    ):
        return _deficiency_inventory()


def _registry():
    from tests._chaos_3295_plan_executor import FakePlanExecutorRuntime

    fake = _FakeHealth()
    return build_registry_with_wave_3_1(
        FakePlanExecutorRuntime(),
        project_health=fake,
        team_health=fake,
        team_workload=fake,
        operational_deficiency=_FakeOperationalDeficiency(),
    )


async def _run_real_plan(intent_id: QuestionIntentID, scope: DevScope):
    """Run a REAL PlanExecutor over a REAL registered plan (production
    wiring end to end, fake CHAOS-3303/3304/3305 services only), producing
    a genuine ``DevInvestigationResult`` -- never a hand-authored fixture.
    """

    plan = WAVE_3_1_PLANS_BY_INTENT[intent_id]
    registry = _registry()
    executor = PlanExecutor(registry=registry, now=lambda: _NOW)
    return await executor.run(
        plan=plan,
        context=_step_context(scope),
        run_id="run-1",
        subject_entity_id="proj-1",
    )


async def _persist(maker, org_id, user_id, result) -> None:
    """The exact real production write path
    (``orchestrator.run()`` -> ``PersistenceRunRecorder.
    record_investigation_result`` -> ``DevPersistenceService.
    append_source_observation``) -- never a synthetic call, so success here
    proves the fix genuinely closes the live crash, not just that the
    executor's own in-memory objects look right.
    """

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
            question="What is the health of this project?",
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
        await recorder.record_investigation_result(result)


_INTENT_SCOPES = {
    QuestionIntentID.PROJECT_HEALTH: _project_scope,
    QuestionIntentID.TEAM_HEALTH: _team_scope,
    QuestionIntentID.TEAM_WORKLOAD_BALANCE: _team_scope,
    QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY: _project_scope,
}


_WAVE_3_1_INTENT_IDS: list[QuestionIntentID] = sorted(
    WAVE_3_1_PLANS_BY_INTENT.keys(), key=lambda intent: intent.value
)


@pytest.mark.asyncio
@pytest.mark.parametrize("intent_id", _WAVE_3_1_INTENT_IDS)
async def test_every_wave_3_1_intent_persists_through_the_real_write_path(
    persistence, intent_id: QuestionIntentID
) -> None:
    """The live crash, reproduced and closed: a REAL plan-governed run for
    each of the four newly-wired intents must persist cleanly through the
    REAL DevPersistenceService, not raise DevPersistenceValidationError.
    """

    maker, org_id, user_id = persistence
    scope_factory = _INTENT_SCOPES[intent_id]
    result = await _run_real_plan(intent_id, scope_factory())

    assert len(result.observations) >= 1
    await _persist(maker, org_id, user_id, result)  # must not raise


# ---------------------------------------------------------------------------
# The totality gap itself: a pure, directly-testable function computing
# "every SourceClass a registered plan's steps can emit that persistence's
# allowlist does not contain" -- also run at wave_3_1_plans.py's own import
# time (module-level RuntimeError), so the NEXT SourceClass addition fails
# at import, not live.
# ---------------------------------------------------------------------------


def test_current_registry_has_no_gap_against_the_real_allowlist() -> None:
    gap = _source_classes_missing_from_persistence_allowlist(
        {**CORE_PLANS_BY_INTENT, **WAVE_3_1_PLANS_BY_INTENT},
        allowlist=_PERSISTENCE_SOURCE_CLASSES,
    )
    assert gap == frozenset()


def test_totality_function_detects_a_planted_gap() -> None:
    """Plant defect: shrink the allowlist by one entry a registered plan
    genuinely emits (health_profile) and confirm the totality function
    reports exactly that gap -- proving it is a real, sensitive check, not
    a vacuous one that would pass regardless of the allowlist's contents.
    """

    shrunk_allowlist = _PERSISTENCE_SOURCE_CLASSES - {"health_profile"}
    gap = _source_classes_missing_from_persistence_allowlist(
        {**CORE_PLANS_BY_INTENT, **WAVE_3_1_PLANS_BY_INTENT},
        allowlist=shrunk_allowlist,
    )
    assert gap == frozenset({"health_profile"})


@pytest.mark.asyncio
async def test_production_write_path_fails_closed_under_the_same_planted_gap(
    persistence, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same planted defect as above, proven against the REAL write path
    this whole suite exists to protect: with health_profile removed from
    the allowlist, the exact live crash (DevPersistenceValidationError)
    must reproduce -- confirming the totality function and the real
    persistence check agree on what "missing" means, not just in theory.
    """

    from dev_health_ops.api.dev.persistence import service as persistence_service_module

    monkeypatch.setattr(
        persistence_service_module,
        "_SOURCE_CLASSES",
        _PERSISTENCE_SOURCE_CLASSES - {"health_profile"},
    )

    maker, org_id, user_id = persistence
    result = await _run_real_plan(QuestionIntentID.PROJECT_HEALTH, _project_scope())

    from dev_health_ops.api.dev.persistence import DevPersistenceValidationError

    with pytest.raises(DevPersistenceValidationError, match="invalid source_class"):
        await _persist(maker, org_id, user_id, result)
