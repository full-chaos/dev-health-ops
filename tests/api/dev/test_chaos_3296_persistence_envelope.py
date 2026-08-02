"""The observation persistence envelope must fit a real dense result
(CHAOS-3296 Codex finding, HIGH, 2026-08-01).

``status.entity.v2``'s ``status_snapshot`` step can legitimately mint content
across six categories at once (status_facts, required_children,
pull_requests, ci_checks, deployments, incidents -- see
``relationship_matrix.APPROVED_CONTENT_SLOTS[SourceClass.STATUS_CHANGE]``),
each already bounded by the contract layer
(``DevSourceContent``'s own ``max_length`` per field, ``contracts_v2/
result.py``). A dense-but-otherwise-valid observation of this shape
serializes to comfortably more than the persistence layer's prior 16KB
defense-in-depth bound, which turned an already-contract-valid result into
``DevPersistenceValidationError`` at persist time -- an ``internal_error`` on
a real answer, never disclosed to the caller as anything but a platform
failure.

This is an end-to-end proof through ``PersistenceRunRecorder.
record_investigation_result`` -- the exact orchestrator-owned persistence
path a real run takes (``orchestrator.py`` calls it once investigation
completes) -- against a real (aiosqlite) ``DevPersistenceService``, not a
unit assertion on the content builder alone.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import dev_health_ops.api.dev.persistence.service as persistence_service_module
from dev_health_ops.api.dev.contracts_v2.base import SourceClass, SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.embedded import (
    DevCIFactV2,
    DevDeploymentFactV2,
    DevIncidentFactV2,
    DevPullRequestFactV2,
    DevRequiredChildFactV2,
    DevStatusFactV2,
)
from dev_health_ops.api.dev.contracts_v2.result import (
    DevInvestigationResult,
    DevSourceContent,
    DevSourceObservation,
)
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.persistence import (
    DevPersistenceService,
    DevPersistenceValidationError,
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

OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)

_TABLES = tables_of(
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

# The prior bound this fixture must exceed to actually prove the finding
# (rather than merely proving *some* oversized payload fails).
_PRE_FIX_ENVELOPE_BYTES = 16 * 1024


@pytest_asyncio.fixture
async def persistence(tmp_path: Path):
    database = tmp_path / "ask-dev-3296-persistence-envelope.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ask-dev-3296", name="Ask Dev 3296"),
                User(id=user_id, email="ask-dev-3296@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


async def _accepted_run(
    service: DevPersistenceService, *, org_id: uuid.UUID, user_id: uuid.UUID
) -> tuple[uuid.UUID, uuid.UUID]:
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
    return conversation.id, accepted.run.id


def _dense_status_snapshot_content() -> DevSourceContent:
    """Every category ``status_snapshot`` can populate at once, each near
    its own contract-layer cap -- the realistic shape of the ~19.6KB dense
    case named in the finding, not an artificially padded blob."""

    def evidence(prefix: str, index: int) -> str:
        return f"ev1_{prefix}{index:0{40 - len(prefix)}x}"

    status_facts = tuple(
        DevStatusFactV2(
            fact_id=f"issue:status-{i}",
            text=f"Issue status-{i} is in_progress, blocked on review",
            evidence_ref_ids=(evidence("a", i),),
        )
        for i in range(25)
    )
    required_children = tuple(
        DevRequiredChildFactV2(
            fact_id=f"issue:child-{i}",
            text=f"Required child issue-{i}: implement the remaining subtask",
            status="open",
            evidence_ref_ids=(evidence("b", i),),
        )
        for i in range(100)
    )
    pull_requests = tuple(
        DevPullRequestFactV2(
            entity_id=f"pr-{i}",
            display_label=f"Fix subtask {i} in the affected service",
            state="open",
            review_state="approved",
            changes_requested=0,
            merged=False,
            required=True,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(evidence("c", i),),
        )
        for i in range(25)
    )
    ci_checks = tuple(
        DevCIFactV2(
            entity_id=f"ci-{i}",
            display_label=f"build-and-test-{i}",
            conclusion="success",
            required=True,
            skipped_required_work=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(evidence("d", i),),
        )
        for i in range(25)
    )
    deployments = tuple(
        DevDeploymentFactV2(
            entity_id=f"deploy-{i}",
            display_label=f"production-release-{i}",
            status="succeeded",
            environment="production",
            required=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(evidence("e", i),),
        )
        for i in range(25)
    )
    incidents = tuple(
        DevIncidentFactV2(
            entity_id=f"incident-{i}",
            display_label=f"Elevated error rate incident-{i}",
            status="resolved",
            active=False,
            blocking=False,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(evidence("f", i),),
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


def _dense_observation() -> DevSourceObservation:
    return DevSourceObservation(
        schema_version="dev_source_observation.v1",
        observation_id=str(uuid.uuid4()),
        source_class=SourceClass.STATUS_CHANGE,
        adapter_id="status_change_service.status_snapshot.v1",
        requirement_level="mandatory",
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        subject_coverage=1.0,
        usable_fact_count=201,
        relationship_paths=(),
        evidence_ref_ids=(),
        observed_at=OBSERVED_AT,
        query_version="status-snapshot.v1",
        content=_dense_status_snapshot_content(),
    )


def _dense_result(observation: DevSourceObservation) -> DevInvestigationResult:
    return DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id=str(uuid.uuid4()),
        plan_id="status.entity.v2",
        plan_version="status.entity.v2.1",
        run_id=str(uuid.uuid4()),
        subject_entity_id="project-1",
        observations=(observation,),
        completed_steps=("status_snapshot",),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=True,
        completed_at=OBSERVED_AT,
    )


def test_the_dense_fixture_actually_exceeds_the_pre_fix_envelope() -> None:
    """Sanity check on the fixture itself: if this ever stops being true the
    test below would pass for the wrong reason (nothing dense enough to
    exercise the bound at all)."""

    import json

    payload = _dense_observation().model_dump(mode="json")
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    assert len(encoded.encode("utf-8")) > _PRE_FIX_ENVELOPE_BYTES


@pytest.mark.asyncio
async def test_pre_fix_envelope_would_have_rejected_this_exact_dense_payload(
    persistence, monkeypatch
) -> None:
    """RED: with the prior 16KB bound restored, the exact dense-but-valid
    payload below is rejected -- proving the finding was real, not that some
    unrelated oversized blob fails."""

    maker, org_id, user_id = persistence
    monkeypatch.setattr(
        persistence_service_module,
        "_SOURCE_OBSERVATION_PAYLOAD_MAX_BYTES",
        _PRE_FIX_ENVELOPE_BYTES,
    )
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        observation = _dense_observation()

        with pytest.raises(DevPersistenceValidationError):
            await service.append_source_observation(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                ordinal=0,
                observation_id=uuid.UUID(observation.observation_id),
                source_class=observation.source_class.value,
                requirement_level=observation.requirement_level,
                observed_state=observation.observed_state.value,
                data_semantics=observation.data_semantics,
                usable_fact_count=observation.usable_fact_count,
                sample_count=observation.sample_count,
                subject_coverage=observation.subject_coverage,
                observed_at=observation.observed_at,
                payload=observation.model_dump(mode="json"),
            )


@pytest.mark.asyncio
async def test_dense_status_snapshot_result_persists_through_record_investigation_result(
    persistence,
) -> None:
    """GREEN: the real, unpatched persistence layer -- driven through the
    orchestrator's own ``record_investigation_result`` path, not a direct
    unit call -- accepts the exact same dense-but-valid result end to end."""

    maker, org_id, user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        recorder = PersistenceRunRecorder(
            service,
            org_id=org_id,
            user_id=user_id,
            conversation_id=conv_id,
            run_id=run_id,
            provider_source="platform",
        )
        observation = _dense_observation()
        result = _dense_result(observation)

        await recorder.record_investigation_result(result)

        rows = (
            (
                await session.execute(
                    select(DevRunSourceObservation).where(
                        DevRunSourceObservation.run_id == run_id
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1
        stored = rows[0]
        assert stored.source_class == "status_change"
        assert stored.usable_fact_count == 201
        stored_content = stored.payload["content"]
        assert len(stored_content["status_facts"]) == 25
        assert len(stored_content["required_children"]) == 100
        assert len(stored_content["pull_requests"]) == 25
        assert len(stored_content["ci_checks"]) == 25
        assert len(stored_content["deployments"]) == 25
        assert len(stored_content["incidents"]) == 25

        run = (
            await session.execute(select(DevRun).where(DevRun.id == run_id))
        ).scalar_one()
        assert run.relationship_closure_verified is True
        assert run.plan_step_partition == {
            "completed": ["status_snapshot"],
            "skipped": [],
            "failed": [],
        }
