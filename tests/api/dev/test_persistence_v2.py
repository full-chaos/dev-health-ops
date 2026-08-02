"""Unit proofs for the Wave 3.1 v2 persistence recorder methods (CHAOS-3299).

Mirrors ``test_persistence.py``'s SQLite in-memory + FK-cascade-enabled
fixture pattern. Each new artifact table gets: a happy path, closed-
vocabulary rejection, and (where the table has a genuine bound) an
off-by-one pair proving the bound itself, not just that a large value
fails.
"""

from __future__ import annotations

import json
import re
import uuid
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import terminal_frames
from dev_health_ops.api.dev.contract_fixtures import (
    positive_fixtures as positive_fixtures_v1,
)
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    positive_fixtures as positive_fixtures_v2,
)
from dev_health_ops.api.dev.contracts import DevAnswer
from dev_health_ops.api.dev.persistence import (
    DevPersistenceNotFound,
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

#: The v1 evidence-handle grammar (`evidence_service.EvidenceHandleService.issue`)
#: -- the fixture's `ev_01` fails `EvidenceHandle`'s strict v2 grammar, which
#: only the v2 embedded mirrors enforce (mirrors test_terminal_frames.py's
#: identical helper).
_REAL_EVIDENCE_HANDLE = "ev1_" + ("a1b2c3d4e5" * 4)

#: One real orchestrator error code per no-answer/needs_clarification public
#: outcome, chosen only to drive terminal_frames.build_error_frame -- any
#: code that buckets to the target outcome works identically.
_ERROR_CODE_BY_OUTCOME: dict[str, str] = {
    "needs_clarification": "scope_ambiguous",
    "not_found": "scope_not_found",
    "temporarily_unavailable": "tool_unavailable",
    "unsupported": "feature_not_enabled",
    "denied": "scope_forbidden",
    "failed": "internal_error",
}


def _legacy_answer_with_real_evidence() -> DevAnswer:
    payload = deepcopy(positive_fixtures_v1()["dev_answer.v1"])
    text = json.dumps(payload, default=str)
    payload = json.loads(re.sub(r"ev_\d+", _REAL_EVIDENCE_HANDLE, text))
    return DevAnswer.model_validate(payload)


def _frame_payload(*, run_id: uuid.UUID, outcome: str) -> dict[str, Any]:
    """A valid ``dev_answer_frame.v1`` payload for one public outcome, with
    its own ``run_id``/``public_outcome`` set to match what
    ``record_frame`` now cross-checks against its own arguments (CHAOS-3297
    Codex review round 2 MEDIUM #3) -- an opaque ``{}``/partial dict no
    longer passes validation, let alone the cross-check.
    """

    if outcome in _ERROR_CODE_BY_OUTCOME:
        frame = terminal_frames.build_error_frame(
            code=_ERROR_CODE_BY_OUTCOME[outcome],
            run_id=str(run_id),
            generated_at=datetime.now(UTC),
        )
        return frame.model_dump(mode="json")
    if outcome == "answered_with_gaps":
        frame = terminal_frames.wrap_legacy_answer_as_frame(
            _legacy_answer_with_real_evidence(), run_id=str(run_id)
        )
        return frame.model_dump(mode="json")
    if outcome == "answered":
        payload = deepcopy(positive_fixtures_v2()["dev_answer_frame.v1"])
        payload["run_id"] = str(run_id)
        payload["frame_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"frame:{run_id}"))
        return payload
    raise AssertionError(f"no frame builder registered for outcome {outcome!r}")


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
_ALL_V2_MODELS = (
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
    database = tmp_path / "ask-dev-v2-persistence.db"
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
    org_id, other_org_id, user_id, other_user_id = (
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
    )
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ask-dev-v2", name="Ask Dev v2"),
                Organization(id=other_org_id, slug="other-v2", name="Other v2"),
                User(id=user_id, email="ask-dev-v2@example.com"),
                User(id=other_user_id, email="other-v2@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, other_org_id, user_id, other_user_id
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


# -- dev_run_intents ----------------------------------------------------


@pytest.mark.asyncio
async def test_record_intent_happy_path_and_closed_vocabulary(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        record = await service.record_intent(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            intent_id="entity_status",
            cardinality="singular",
            requires_clarification=False,
            interpreter_version="intent_interpreter.v1",
            payload={"schema_version": "dev_question_intent.v1", "confidence": 0.9},
        )
        assert record.intent_id == "entity_status"
        assert record.payload["confidence"] == 0.9

        with pytest.raises(DevPersistenceValidationError):
            await service.record_intent(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                intent_id="not_a_real_intent",
                cardinality="singular",
                requires_clarification=False,
                interpreter_version="intent_interpreter.v1",
                payload={},
            )
        with pytest.raises(DevPersistenceValidationError):
            await service.record_intent(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                intent_id="entity_status",
                cardinality="plural",  # not 'plural_cohort'
                requires_clarification=False,
                interpreter_version="intent_interpreter.v1",
                payload={},
            )


@pytest.mark.asyncio
async def test_record_intent_payload_bound_off_by_one(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        # Just at the bound: passes.
        padding = "x" * 16_100
        await service.record_intent(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            intent_id="entity_status",
            cardinality="singular",
            requires_clarification=False,
            interpreter_version="intent_interpreter.v1",
            payload={"note": padding},
        )

    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id2, run_id2 = await _accepted_run(
            service, org_id=org_id, user_id=user_id
        )
        # One unit past the bound: rejected.
        with pytest.raises(DevPersistenceValidationError):
            await service.record_intent(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id2,
                intent_id="entity_status",
                cardinality="singular",
                requires_clarification=False,
                interpreter_version="intent_interpreter.v1",
                payload={"note": "x" * 20_000},
            )


@pytest.mark.asyncio
async def test_record_intent_rejects_sensitive_metadata_keys(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        with pytest.raises(DevPersistenceValidationError):
            await service.record_intent(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                intent_id="entity_status",
                cardinality="singular",
                requires_clarification=False,
                interpreter_version="intent_interpreter.v1",
                payload={"nested": {"deeper": {"raw_prompt": "leaked"}}},
            )


# -- dev_run_resolutions (append-only) -----------------------------------


@pytest.mark.asyncio
async def test_append_resolution_is_append_only_and_insert_only(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        mention_id = uuid.uuid4()

        first = await service.append_resolution(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            entry_ordinal=0,
            mention_id=mention_id,
            outcome="ambiguous_candidates",
            resolved_at=datetime.now(UTC),
            payload={"candidates": []},
        )
        assert first.entry_ordinal == 0

        # A later resolution appends at the next ordinal -- never overwrites.
        second = await service.append_resolution(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            entry_ordinal=1,
            mention_id=mention_id,
            outcome="exact_match",
            resolved_at=datetime.now(UTC),
            payload={"committed_entity_ref": {"entity_id": "repo/x"}},
        )
        assert second.entry_ordinal == 1

        # No update path is exposed; re-inserting an already-used ordinal
        # fails via the unique constraint rather than upserting.
        with pytest.raises(Exception):  # noqa: B017 - IntegrityError, dialect-specific
            async with session.begin_nested():
                await service.append_resolution(
                    org_id=org_id,
                    user_id=user_id,
                    run_id=run_id,
                    entry_ordinal=0,
                    mention_id=mention_id,
                    outcome="no_authorized_match",
                    resolved_at=datetime.now(UTC),
                    payload={},
                )

        count = await session.scalar(select(func.count()).select_from(DevRunResolution))
        assert count == 2

        with pytest.raises(DevPersistenceValidationError):
            await service.append_resolution(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                entry_ordinal=2,
                mention_id=mention_id,
                outcome="not_a_real_outcome",
                resolved_at=datetime.now(UTC),
                payload={},
            )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_resolution(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                entry_ordinal=100,  # bound is 0..99
                mention_id=mention_id,
                outcome="exact_match",
                resolved_at=datetime.now(UTC),
                payload={},
            )


# -- dev_run_subject_sets --------------------------------------------------


@pytest.mark.asyncio
async def test_record_subject_set_happy_path_and_invalid_entity_kind(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        record = await service.record_subject_set(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            set_id=uuid.uuid4(),
            entity_kind="team",
            cohort_complete=True,
            fingerprint="sha256:" + "a" * 64,
            payload={"committed_entity_refs": []},
        )
        assert record.entity_kind == "team"

        with pytest.raises(DevPersistenceValidationError):
            await service.record_subject_set(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                set_id=uuid.uuid4(),
                entity_kind="not_a_real_kind",
                cohort_complete=True,
                fingerprint="fp",
                payload={},
            )


# -- dev_run_source_observations ------------------------------------------


@pytest.mark.asyncio
async def test_append_source_observation_bounds_and_closed_vocabulary(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        record = await service.append_source_observation(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            ordinal=24,  # top of the 0..24 bound
            observation_id=uuid.uuid4(),
            source_class="status_change",
            requirement_level="mandatory",
            observed_state="available_current",
            data_semantics="measured_zero",
            usable_fact_count=0,
            sample_count=0,
            subject_coverage=1.0,
            observed_at=datetime.now(UTC),
            payload={"relationship_paths": []},
        )
        assert record.ordinal == 24

        with pytest.raises(DevPersistenceValidationError):
            await service.append_source_observation(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                ordinal=25,  # one past the bound
                observation_id=uuid.uuid4(),
                source_class="status_change",
                requirement_level="mandatory",
                observed_state="available_current",
                data_semantics="measured_zero",
                usable_fact_count=0,
                sample_count=0,
                subject_coverage=1.0,
                observed_at=datetime.now(UTC),
                payload={},
            )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_source_observation(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                ordinal=1,
                observation_id=uuid.uuid4(),
                source_class="not_a_real_source",
                requirement_level="mandatory",
                observed_state="available_current",
                data_semantics="measured_zero",
                usable_fact_count=0,
                sample_count=0,
                subject_coverage=1.0,
                observed_at=datetime.now(UTC),
                payload={},
            )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_source_observation(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                ordinal=1,
                observation_id=uuid.uuid4(),
                source_class="status_change",
                requirement_level="mandatory",
                observed_state="available_current",
                data_semantics="measured_zero",
                usable_fact_count=0,
                sample_count=0,
                subject_coverage=1.5,  # out of [0, 1]
                observed_at=datetime.now(UTC),
                payload={},
            )


# -- dev_runs.plan_step_partition / relationship_closure_verified ---------
# Folded from a dedicated dev_run_investigation_results table (orchestrator
# decision, CHAOS-3299): these two facts are set directly on the owned
# dev_runs row instead.


@pytest.mark.asyncio
async def test_record_investigation_result_sets_partition_and_closure_bit(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        run = await service.record_investigation_result(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            completed_steps=["collect_status_changes", "collect_pull_requests"],
            skipped_steps=["collect_ci_runs"],
            failed_steps=[],
            relationship_closure_verified=False,
        )
        assert run.plan_step_partition == {
            "completed": ["collect_status_changes", "collect_pull_requests"],
            "skipped": ["collect_ci_runs"],
            "failed": [],
        }
        assert run.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_record_investigation_result_rejects_a_step_in_more_than_one_list(
    persistence,
):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        with pytest.raises(DevPersistenceValidationError):
            await service.record_investigation_result(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                completed_steps=["collect_status_changes"],
                skipped_steps=["collect_status_changes"],  # also completed
                failed_steps=[],
                relationship_closure_verified=True,
            )


@pytest.mark.asyncio
async def test_record_investigation_result_step_list_bound_off_by_one(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        # Exactly at the bound: passes.
        run = await service.record_investigation_result(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            completed_steps=[f"step_{i}" for i in range(25)],
            skipped_steps=[],
            failed_steps=[],
            relationship_closure_verified=True,
        )
        partition = run.plan_step_partition
        assert partition is not None
        assert len(partition["completed"]) == 25

    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id2, run_id2 = await _accepted_run(
            service, org_id=org_id, user_id=user_id
        )
        # One entry past the bound: rejected.
        with pytest.raises(DevPersistenceValidationError):
            await service.record_investigation_result(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id2,
                completed_steps=[f"step_{i}" for i in range(26)],
                skipped_steps=[],
                failed_steps=[],
                relationship_closure_verified=True,
            )


# -- dev_answer_frames / dev_run_narratives -------------------------------


@pytest.mark.asyncio
async def test_record_frame_every_public_outcome_and_invalid_outcome(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    outcomes = (
        "answered",
        "answered_with_gaps",
        "needs_clarification",
        "not_found",
        "temporarily_unavailable",
        "unsupported",
        "denied",
        "failed",
    )
    async with maker() as session:
        service = DevPersistenceService(session)
        for outcome in outcomes:
            _conv_id, run_id = await _accepted_run(
                service, org_id=org_id, user_id=user_id
            )
            payload = _frame_payload(run_id=run_id, outcome=outcome)
            record = await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(payload["frame_id"]),
                public_outcome=outcome,
                payload=payload,
            )
            assert record.public_outcome == outcome

            # Codex-review finding (CHAOS-3299): record_frame must
            # atomically tag the owned run v2 -- writing a frame without
            # tagging left contract_generation stuck at 'v1' forever, which
            # made router._replayed_result's "== 'v2'" replay gate
            # permanently unreachable for every real v2 run.
            run = await session.get(DevRun, run_id)
            assert run is not None
            assert run.contract_generation == "v2"
            assert run.public_outcome == outcome

        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        with pytest.raises(DevPersistenceValidationError):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.uuid4(),
                public_outcome="bogus_outcome",
                payload={},
            )


@pytest.mark.asyncio
async def test_record_frame_rejects_a_schema_only_junk_payload(persistence):
    """CHAOS-3297 Codex review round 2 MEDIUM #3, the exact repro.

    Before this fix, ``record_frame`` accepted any ``Mapping`` bounded only
    by byte size and a bare ``public_outcome`` vocabulary check --
    ``{"schema_version": "dev_answer_frame.v1"}`` alone passed both,
    silently bypassing every frame-level invariant the contract enforces
    (structural closure, no-answer projection, plan-registry membership,
    ...) and tagging the run ``contract_generation = 'v2'`` regardless.
    Asserts no row is written and the run is not tagged v2.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        with pytest.raises(DevPersistenceValidationError, match="not a valid"):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.uuid4(),
                public_outcome="answered",
                payload={"schema_version": "dev_answer_frame.v1"},
            )

        run = await session.get(DevRun, run_id)
        assert run is not None
        assert run.contract_generation == "v1", (
            "a rejected junk payload must never tag the run v2"
        )
        assert run.public_outcome is None
        frame_count = await session.scalar(
            select(func.count())
            .select_from(DevAnswerFrame)
            .where(DevAnswerFrame.run_id == run_id)
        )
        assert frame_count == 0, "a rejected junk payload must never be written"


@pytest.mark.asyncio
async def test_record_frame_rejects_a_frame_id_mismatch(persistence):
    """CHAOS-3297 Codex review round 2 MEDIUM #3: a structurally valid frame
    for the *wrong* run/frame identity must be rejected, not silently
    persisted under the caller's own (different) identifiers."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        # A fully valid frame -- just built for a different run_id than the
        # one this call claims to be recording it for.
        mismatched_payload = _frame_payload(run_id=uuid.uuid4(), outcome="answered")

        with pytest.raises(DevPersistenceValidationError, match="run_id"):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(mismatched_payload["frame_id"]),
                public_outcome="answered",
                payload=mismatched_payload,
            )

        run = await session.get(DevRun, run_id)
        assert run is not None
        assert run.contract_generation == "v1"


@pytest.mark.asyncio
async def test_record_narrative_requires_matching_recorded_frame(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        real_frame_id = uuid.UUID(frame_payload["frame_id"])
        await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=real_frame_id,
            public_outcome="answered",
            payload=frame_payload,
        )

        # A mutated frame_id -- not matching the run's actual recorded frame
        # -- is rejected (same posture as append_assistant_answer's
        # payload_conversation_id != conversation_id check).
        with pytest.raises(DevPersistenceValidationError):
            await service.record_narrative(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                narrative_id=uuid.uuid4(),
                frame_id=uuid.uuid4(),  # mismatched
                mode="deterministic_fallback",
                provider_fingerprint=None,
                narrative_text="Here is a safe presentation summary.",
                payload={},
            )

        record = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=uuid.uuid4(),
            frame_id=real_frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text="Here is a safe presentation summary.",
            payload={},
        )
        assert record.frame_id == real_frame_id


@pytest.mark.asyncio
async def test_record_narrative_before_any_frame_is_rejected(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        with pytest.raises(DevPersistenceValidationError):
            await service.record_narrative(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                narrative_id=uuid.uuid4(),
                frame_id=uuid.uuid4(),
                mode="provider",
                provider_fingerprint=None,
                narrative_text="text",
                payload={},
            )


# -- dev_run_stage_diagnostics ---------------------------------------------


@pytest.mark.asyncio
async def test_append_stage_diagnostic_bounds_and_closed_vocabulary(persistence):
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        record = await service.append_stage_diagnostic(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            ordinal=0,
            stage_id="interpreting",
            status="completed",
            latency_ms=12,
            counts={"mention_count": 2},
        )
        assert record.stage_id == "interpreting"

        with pytest.raises(DevPersistenceValidationError):
            await service.append_stage_diagnostic(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                ordinal=1,
                stage_id="not_a_real_stage",
                status="completed",
                latency_ms=None,
                counts={},
            )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_stage_diagnostic(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                ordinal=1,
                stage_id="planning",
                status="not_a_real_status",
                latency_ms=None,
                counts={},
            )


# -- cross-org / cross-run association rejection --------------------------


@pytest.mark.asyncio
async def test_cross_org_run_association_is_rejected_for_every_recorder(persistence):
    """A run_id that exists, but under a different org, must fail via the
    composite ownership lookup -- not via an application check that could be
    bypassed for one artifact but not another."""

    maker, org_id, other_org_id, user_id, other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        with pytest.raises(DevPersistenceNotFound):
            await service.record_intent(
                org_id=other_org_id,
                user_id=other_user_id,
                run_id=run_id,
                intent_id="entity_status",
                cardinality="singular",
                requires_clarification=False,
                interpreter_version="intent_interpreter.v1",
                payload={},
            )
        with pytest.raises(DevPersistenceNotFound):
            await service.record_investigation_result(
                org_id=other_org_id,
                user_id=other_user_id,
                run_id=run_id,
                completed_steps=[],
                skipped_steps=[],
                failed_steps=[],
                relationship_closure_verified=True,
            )
        with pytest.raises(DevPersistenceNotFound):
            await service.record_frame(
                org_id=other_org_id,
                user_id=other_user_id,
                run_id=run_id,
                frame_id=uuid.uuid4(),
                public_outcome="answered",
                payload={},
            )
        with pytest.raises(DevPersistenceNotFound):
            await service.append_stage_diagnostic(
                org_id=other_org_id,
                user_id=other_user_id,
                run_id=run_id,
                ordinal=0,
                stage_id="interpreting",
                status="started",
                latency_ms=None,
                counts={},
            )


# -- purge/retention parity across all 7 new artifact tables ---------------


async def _record_every_v2_artifact(
    service: DevPersistenceService,
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    run_id: uuid.UUID,
) -> None:
    await service.record_intent(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        intent_id="entity_status",
        cardinality="singular",
        requires_clarification=False,
        interpreter_version="intent_interpreter.v1",
        payload={},
    )
    await service.append_resolution(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        entry_ordinal=0,
        mention_id=uuid.uuid4(),
        outcome="exact_match",
        resolved_at=datetime.now(UTC),
        payload={},
    )
    await service.record_subject_set(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        set_id=uuid.uuid4(),
        entity_kind="repository",
        cohort_complete=True,
        fingerprint="fp",
        payload={},
    )
    await service.append_source_observation(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        ordinal=0,
        observation_id=uuid.uuid4(),
        source_class="status_change",
        requirement_level="mandatory",
        observed_state="available_current",
        data_semantics="measured_zero",
        usable_fact_count=0,
        sample_count=0,
        subject_coverage=1.0,
        observed_at=datetime.now(UTC),
        payload={},
    )
    await service.record_investigation_result(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        completed_steps=["collect_status_changes"],
        skipped_steps=[],
        failed_steps=[],
        relationship_closure_verified=True,
    )
    frame_payload = _frame_payload(run_id=run_id, outcome="answered")
    frame_id = uuid.UUID(frame_payload["frame_id"])
    await service.record_frame(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        frame_id=frame_id,
        public_outcome="answered",
        payload=frame_payload,
    )
    await service.record_narrative(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        narrative_id=uuid.uuid4(),
        frame_id=frame_id,
        mode="deterministic_fallback",
        provider_fingerprint=None,
        narrative_text="Safe presentation text.",
        payload={},
    )
    await service.append_stage_diagnostic(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        ordinal=0,
        stage_id="interpreting",
        status="completed",
        latency_ms=5,
        counts={},
    )


@pytest.mark.asyncio
async def test_delete_conversation_purges_every_new_artifact_table(persistence):
    """Fail-before/pass-after for the cascade itself: this is the exact test
    that would fail loudly (table not found / row not purged) if any of the
    7 new FKs were wired wrong -- one INSERT per table, one DELETE, and a
    direct count(*) per table, not just 'no exception was raised'.

    ``plan_step_partition``/``relationship_closure_verified`` are not
    separate FK'd tables -- they're columns folded onto ``dev_runs`` -- so
    their purge parity is proven by the ``dev_runs`` row itself vanishing,
    asserted explicitly rather than assumed."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await _record_every_v2_artifact(
            service, org_id=org_id, user_id=user_id, run_id=run_id
        )
        for model in _ALL_V2_MODELS:
            count = await session.scalar(select(func.count()).select_from(model))
            assert count == 1, f"{model.__tablename__} was not seeded"
        seeded_run = await session.get(DevRun, run_id)
        assert seeded_run is not None
        assert seeded_run.plan_step_partition == {
            "completed": ["collect_status_changes"],
            "skipped": [],
            "failed": [],
        }
        assert seeded_run.relationship_closure_verified is True

        deleted = await service.delete_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conv_id
        )
        assert deleted is True

        for model in _ALL_V2_MODELS:
            count = await session.scalar(select(func.count()).select_from(model))
            assert count == 0, f"{model.__tablename__} was not purged"
        # session.get() would return the stale identity-mapped instance
        # from the seed check above without re-querying; a fresh select
        # is required to observe the cascade-deleted row.
        assert await session.scalar(select(DevRun).where(DevRun.id == run_id)) is None


@pytest.mark.asyncio
async def test_zero_day_immediate_purge_covers_every_new_artifact_and_orphan_write_fails(
    persistence,
):
    """The 0-day path: record every artifact *before* the terminal
    update_run transition, then confirm the immediate purge removes all of
    them, and that writing to an already-purged run fails loudly (a
    DevPersistenceNotFound from the owned-run lookup, not a silent no-op or
    an orphaned row surviving the purge)."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Is this project healthy?",
            scope_snapshot={},
        )
        run_id = accepted.run.id
        await _record_every_v2_artifact(
            service, org_id=org_id, user_id=user_id, run_id=run_id
        )
        seeded_run = await session.get(DevRun, run_id)
        assert seeded_run is not None
        assert seeded_run.plan_step_partition is not None
        assert seeded_run.relationship_closure_verified is True

        result = await service.update_run(
            org_id=org_id, user_id=user_id, run_id=run_id, state="completed"
        )
        assert result is None  # signals the ephemeral purge fired
        assert await session.get(DevConversation, conversation.id) is None

        for model in _ALL_V2_MODELS:
            count = await session.scalar(select(func.count()).select_from(model))
            assert count == 0, f"{model.__tablename__} was left orphaned"
        # The columns are folded onto dev_runs, not a separate table --
        # purge parity means the row (and therefore the columns) is gone.
        # (select(), not get(): get() would return the stale
        # identity-mapped instance from the seed check above.)
        assert await session.scalar(select(DevRun).where(DevRun.id == run_id)) is None

        # Writing to the now-purged run fails loudly, not silently.
        with pytest.raises(DevPersistenceNotFound):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.uuid4(),
                public_outcome="answered",
                payload={},
            )
