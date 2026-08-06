"""Unit proofs for the Wave 3.1 v2 persistence recorder methods (CHAOS-3299).

Mirrors ``test_persistence.py``'s SQLite in-memory + FK-cascade-enabled
fixture pattern. Each new artifact table gets: a happy path, closed-
vocabulary rejection, and (where the table has a genuine bound) an
off-by-one pair proving the bound itself, not just that a large value
fails.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import re
import textwrap
import types
import uuid
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, cast, get_args, get_origin

import pytest
import pytest_asyncio
from pydantic import AwareDatetime
from sqlalchemy import Table, event, func, insert, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError, PendingRollbackError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from dev_health_ops.api.dev import terminal_frames
from dev_health_ops.api.dev.contract_fixtures import (
    positive_fixtures as positive_fixtures_v1,
)
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    _clarification_candidate,
    _needs_clarification_frame_base,
)
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    needs_clarification_frame_with_candidates as _needs_clarification_frame_with_candidates,
)
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    positive_fixtures as positive_fixtures_v2,
)
from dev_health_ops.api.dev.contracts import DevAnswer, DevError
from dev_health_ops.api.dev.contracts_v2 import base as _base
from dev_health_ops.api.dev.contracts_v2.narrative import (
    DevNarrative as DevNarrativeContract,
)
from dev_health_ops.api.dev.persistence import (
    DevPersistenceNotFound,
    DevPersistenceService,
    DevPersistenceValidationError,
)
from dev_health_ops.api.dev.persistence import service as dev_persistence_service
from dev_health_ops.api.dev.persistence.service import (
    _KNOWN_UNVALIDATED_PAYLOAD_GAP as _ORM_BOUNDARY_KNOWN_GAP,
)
from dev_health_ops.api.dev.persistence.service import (
    _PAYLOAD_MODEL_VALIDATORS,
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
    # F10 (CHAOS-3297 stack #3): a real v1-sourced metric never carries
    # evidence_ref_ids -- see test_terminal_frames.py's _legacy_answer for
    # the full rationale. Clear it so wrap_legacy_answer_as_frame's
    # unconditional evidence_classification is exercised realistically.
    for metric in payload.get("metrics", []):
        metric["evidence_ref_ids"] = []
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


def _narrative_payload(
    *, run_id: uuid.UUID, frame_id: uuid.UUID, narrative_id: uuid.UUID, mode: str
) -> tuple[dict[str, Any], str, str | None]:
    """A valid ``dev_narrative.v1``, split into ``(payload_without_body,
    narrative_text, provider_fingerprint)`` exactly the way
    ``PersistenceRunRecorder.record_narrative`` splits a real
    ``DevNarrative`` -- so ``record_narrative``'s reconstruction (``body``
    reinserted from ``narrative_text``) round-trips to something valid
    (CHAOS-3297 Codex review round 3 CLASS B).
    """

    provider_metadata: dict[str, Any] | None = None
    provider_fingerprint: str | None = None
    if mode == "provider":
        model_fingerprint = "model-fingerprint-01"
        provider_metadata = {
            "provider_source": "platform",
            "provider_family": "scripted",
            "model_fingerprint": model_fingerprint,
        }
        provider_fingerprint = (
            "sha256:" + hashlib.sha256(model_fingerprint.encode()).hexdigest()
        )
    narrative_text = "Here is a safe presentation summary."
    payload: dict[str, Any] = {
        "schema_version": "dev_narrative.v1",
        "narrative_id": str(narrative_id),
        "run_id": str(run_id),
        "frame_id": str(frame_id),
        "mode": mode,
        "referenced_fact_ids": [],
        "referenced_section_ids": [],
        "provider_metadata": provider_metadata,
        "generated_at": datetime.now(UTC).isoformat(),
        "validation_warnings": [],
    }
    return payload, narrative_text, provider_fingerprint


def _needs_clarification_frame_payload(
    *, run_id: uuid.UUID, with_candidates: bool
) -> dict[str, Any]:
    """CHAOS-3325: a valid ``needs_clarification`` frame, with or without a
    real ``clarification_candidates`` block, retargeted at ``run_id`` the
    same way ``_frame_payload`` retargets the ``answered`` fixture."""

    payload = deepcopy(
        _needs_clarification_frame_with_candidates()
        if with_candidates
        else _needs_clarification_frame_base()
    )
    payload["run_id"] = str(run_id)
    payload["frame_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"frame:{run_id}"))
    return payload


def _ambiguous_ledger_entry_payload(
    *, mention_id: uuid.UUID, candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    """A valid ``dev_resolution_ledger.v1`` entry payload -- the shape
    ``append_resolution`` stores and ``_authorize_clarification_candidates``
    reads back and validates as ``DevResolutionEntry``."""

    return {
        "entry_ordinal": 0,
        "mention_id": str(mention_id),
        "outcome": "ambiguous_candidates",
        "committed_entity_ref": None,
        "candidates": candidates,
        "repository_attribution": None,
        "team_attribution": None,
        "resolver_version": "resolver.v1",
        "query_version": "resolve_scope.v1",
        "resolved_at": datetime.now(UTC).isoformat(),
    }


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


# -- CHAOS-3325: clarification_candidates must be authorized by the run's
# own persisted resolution ledger, not merely schema-valid ------------------


@pytest.mark.asyncio
async def test_record_frame_accepts_clarification_candidates_matching_the_ledger(
    persistence,
):
    """Positive control: real candidates that exactly match the run's own
    persisted ``ambiguous_candidates`` ledger entry persist cleanly."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        mention_id = uuid.uuid4()

        frame_payload = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=True
        )
        candidates = frame_payload["clarification_candidates"]
        assert candidates, "fixture must actually carry candidates"

        await service.append_resolution(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            entry_ordinal=0,
            mention_id=mention_id,
            outcome="ambiguous_candidates",
            resolved_at=datetime.now(UTC),
            payload=_ambiguous_ledger_entry_payload(
                mention_id=mention_id, candidates=candidates
            ),
        )

        record = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="needs_clarification",
            payload=frame_payload,
        )
        assert record.public_outcome == "needs_clarification"
        assert record.payload["clarification_candidates"] == candidates


@pytest.mark.asyncio
async def test_record_frame_rejects_clarification_candidates_absent_from_the_ledger(
    persistence,
):
    """CHAOS-3325 Codex review (NO-SHIP, confirmed medium), the exact repro:
    a schema-valid ``clarification_candidates`` entry that the resolution
    ledger never authorized (e.g. another org's repository) must never
    persist as canonical v2 state -- no matching ledger row exists for this
    run at all."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        # No append_resolution call at all for this run -- the exact repro:
        # a needs_clarification frame carrying a schema-valid candidate the
        # ledger never recorded.
        frame_payload = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=True
        )
        frame_payload["clarification_candidates"][0]["entity_ref"]["entity_id"] = (
            "other-org-secret-repo"
        )

        with pytest.raises(
            DevPersistenceValidationError, match="clarification_candidates"
        ):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(frame_payload["frame_id"]),
                public_outcome="needs_clarification",
                payload=frame_payload,
            )

        frame_count = await session.scalar(
            select(func.count())
            .select_from(DevAnswerFrame)
            .where(DevAnswerFrame.run_id == run_id)
        )
        assert frame_count == 0, "an unauthorized candidate must never be written"
        run = await session.get(DevRun, run_id)
        assert run is not None
        assert run.contract_generation == "v1"


@pytest.mark.asyncio
async def test_record_frame_persists_the_authorized_snapshot_not_the_caller_mapping(
    persistence, monkeypatch
):
    """CHAOS-3325 confirmation-codex MEDIUM (in-model half), exact repro.

    Every check in ``record_frame`` -- contract validation, the
    frame_id/run_id/outcome cross-checks, and the ledger authorization --
    runs against ``validated``, the immutable contract snapshot. The row
    used to be constructed from ``payload``, the caller's *mutable*
    mapping, so anything that changed that mapping after authorization
    returned was persisted unchecked. Codex mutated it in exactly that
    window and ``other-org-secret-repo`` reached the row.

    The window is reproduced deterministically by wrapping the
    authorization hook: it delegates to the real implementation (so the
    frame genuinely is authorized) and then mutates the caller's mapping
    before returning -- precisely the interleaving the fix must survive.
    The fix is to build the row from ``validated.model_dump(mode="json")``;
    with ``payload_dict=payload`` restored, this test fails.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        mention_id = uuid.uuid4()

        frame_payload = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=True
        )
        candidates = deepcopy(frame_payload["clarification_candidates"])
        authorized_entity_id = candidates[0]["entity_ref"]["entity_id"]

        await service.append_resolution(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            entry_ordinal=0,
            mention_id=mention_id,
            outcome="ambiguous_candidates",
            resolved_at=datetime.now(UTC),
            payload=_ambiguous_ledger_entry_payload(
                mention_id=mention_id, candidates=candidates
            ),
        )

        real_authorize = dev_persistence_service._authorize_clarification_candidates

        async def _mutate_after_authorizing(session_arg, **kwargs):
            await real_authorize(session_arg, **kwargs)
            # Authorization has returned; the row is not constructed yet.
            frame_payload["clarification_candidates"][0]["entity_ref"]["entity_id"] = (
                "other-org-secret-repo"
            )

        monkeypatch.setattr(
            dev_persistence_service,
            "_authorize_clarification_candidates",
            _mutate_after_authorizing,
        )

        record = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="needs_clarification",
            payload=frame_payload,
        )

        # The caller's mapping really was mutated -- otherwise the test is
        # vacuous and would pass against the unfixed code too.
        assert (
            frame_payload["clarification_candidates"][0]["entity_ref"]["entity_id"]
            == "other-org-secret-repo"
        )
        stored = record.payload["clarification_candidates"][0]["entity_ref"][
            "entity_id"
        ]
        assert stored == authorized_entity_id
        assert "other-org-secret-repo" not in json.dumps(record.payload)


@pytest.mark.asyncio
async def test_direct_orm_frame_write_is_not_ledger_checked_documented_residual(
    persistence,
):
    """DOCUMENTED RESIDUAL -- asserts today's behavior, not a desired one.

    CHAOS-3325 confirmation-codex MEDIUM (out-of-model half): a direct
    ORM/Core write carrying a candidate-bearing frame with no authorizing
    ledger row is NOT rejected. That is deliberate and matches where s1 put
    the boundary: the ORM listener and the 0080 trigger validate contract
    shape and payload/row identity, never cross-table provenance. Enforcing
    ledger equality inside a trigger would be cross-table DB logic out of
    proportion to the risk, so ledger provenance stays a service-layer
    guarantee and non-``record_frame`` frame writes remain unsupported
    paths.

    This test exists so that stays a decision rather than an assumption: if
    ledger provenance is ever moved to the boundary, this test fails and is
    flipped deliberately, instead of the residual being rediscovered.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        # No append_resolution for this run: nothing authorizes these
        # candidates. record_frame would reject this exact payload -- see
        # test_record_frame_rejects_clarification_candidates_absent_from_the_ledger.
        frame_payload = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=True
        )
        frame_payload["clarification_candidates"][0]["entity_ref"]["entity_id"] = (
            "other-org-secret-repo"
        )
        row = DevAnswerFrame(
            run_id=run_id,
            org_id=org_id,
            user_id=user_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="needs_clarification",
            payload=frame_payload,
        )
        session.add(row)
        await session.commit()

    async with maker() as session:
        stored = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
        )
        assert stored is not None, (
            "documented residual: the direct-ORM path is shape-checked and "
            "identity-checked, but not ledger-checked -- if this now fails, "
            "ledger provenance reached the boundary and the residual is closed"
        )
        assert (
            stored.payload["clarification_candidates"][0]["entity_ref"]["entity_id"]
            == "other-org-secret-repo"
        )


@pytest.mark.asyncio
async def test_record_frame_rejects_clarification_candidates_mismatching_the_ledger(
    persistence,
):
    """The other half of the same guard: a ledger entry does exist for this
    run, but the frame's candidates diverge from it (a different entity, or
    a different order) -- rejected exactly like the missing-entry case."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        mention_id = uuid.uuid4()

        frame_payload = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=True
        )
        ledger_candidates = deepcopy(frame_payload["clarification_candidates"])
        # The ledger authorized a *different* entity than the frame claims.
        ledger_candidates[0]["entity_ref"]["entity_id"] = "repo_authorized_only"

        await service.append_resolution(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            entry_ordinal=0,
            mention_id=mention_id,
            outcome="ambiguous_candidates",
            resolved_at=datetime.now(UTC),
            payload=_ambiguous_ledger_entry_payload(
                mention_id=mention_id, candidates=ledger_candidates
            ),
        )

        with pytest.raises(
            DevPersistenceValidationError, match="clarification_candidates"
        ):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(frame_payload["frame_id"]),
                public_outcome="needs_clarification",
                payload=frame_payload,
            )

        frame_count = await session.scalar(
            select(func.count())
            .select_from(DevAnswerFrame)
            .where(DevAnswerFrame.run_id == run_id)
        )
        assert frame_count == 0, "a mismatched candidate must never be written"


@pytest.mark.asyncio
async def test_record_frame_allows_empty_clarification_candidates_only_when_the_ledger_is_also_empty(
    persistence,
):
    """CHAOS-3325 Codex review round 2 (confirmed medium): the round-1
    "empty is always allowed" rule let an internal caller downgrade
    canonical state -- persist ``needs_clarification`` with zero candidates
    for a run whose ledger genuinely recorded several, hiding the choices
    the resolver actually offered. The ledger is now always fetched:

    * no ledger entry at all (the uninterpretable-question case) -> an
      empty frame is legitimate, nothing to authorize.
    * a ledger entry exists -> the frame must match it exactly, including
      non-emptiness; an empty frame against a non-empty ledger is now the
      rejected counterexample, not an allowed one.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)

        # Case 1 (unchanged): no ledger entry at all -> empty is legitimate.
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        empty_payload = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=False
        )
        assert empty_payload["clarification_candidates"] == []
        record = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(empty_payload["frame_id"]),
            public_outcome="needs_clarification",
            payload=empty_payload,
        )
        assert record.public_outcome == "needs_clarification"

        # Case 2, the round-2 counterexample: a real ambiguous ledger entry
        # exists for this run, but the frame being persisted carries none --
        # a canonical-state downgrade, now rejected rather than allowed.
        _conv_id2, run_id2 = await _accepted_run(
            service, org_id=org_id, user_id=user_id
        )
        mention_id = uuid.uuid4()
        real_candidates = _needs_clarification_frame_payload(
            run_id=run_id2, with_candidates=True
        )["clarification_candidates"]
        await service.append_resolution(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id2,
            entry_ordinal=0,
            mention_id=mention_id,
            outcome="ambiguous_candidates",
            resolved_at=datetime.now(UTC),
            payload=_ambiguous_ledger_entry_payload(
                mention_id=mention_id, candidates=real_candidates
            ),
        )
        empty_payload_2 = _needs_clarification_frame_payload(
            run_id=run_id2, with_candidates=False
        )
        with pytest.raises(
            DevPersistenceValidationError, match="clarification_candidates"
        ):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id2,
                frame_id=uuid.UUID(empty_payload_2["frame_id"]),
                public_outcome="needs_clarification",
                payload=empty_payload_2,
            )
        frame_count = await session.scalar(
            select(func.count())
            .select_from(DevAnswerFrame)
            .where(DevAnswerFrame.run_id == run_id2)
        )
        assert frame_count == 0, (
            "a canonical-state downgrade (empty frame vs. non-empty ledger) "
            "must never be written"
        )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason="CHAOS-3330: append_resolution validation closes the double-forge seam",
)
async def test_record_frame_double_forged_ledger_and_frame_defeats_the_equality_check(
    persistence,
):
    """CHAOS-3325 Codex review round 2 finding 1 (deferred by ruling, NOT
    fixed in this branch): an internal caller that forges BOTH a
    schema-valid resolution-ledger row and a frame whose
    ``clarification_candidates`` exactly match it defeats
    ``_authorize_clarification_candidates`` -- the equality check only
    proves the two objects agree with each other, not that either was
    honestly produced by ``subject_preflight``'s real resolution against
    the authorized catalog. ``append_resolution`` accepts any schema-valid
    payload today with no check against the catalog, so a forged, mutually
    consistent pair persists cleanly.

    Ruling: the natural closure is CHAOS-3330's ``append_resolution``
    payload validation (already escalated as authorization-load-bearing),
    not a persistence-layer-to-resolver-catalog coupling in this branch.
    This is the repro, held ``xfail(strict=True)`` so it flips loudly --
    an unexpected pass fails the suite -- the moment CHAOS-3330 closes the
    seam; until then it documents the residual risk rather than silently
    passing over it.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        mention_id = uuid.uuid4()

        # Neither the ledger entry nor the frame candidate was ever offered
        # by the real authorized catalog -- an internal caller fabricated
        # both, in agreement with each other, so the equality check alone
        # cannot tell this apart from a genuine resolution.
        forged_candidates = [
            _clarification_candidate(
                entity_id="forged-entity-never-in-catalog",
                display_label="Forged Entity",
            )
        ]
        await service.append_resolution(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            entry_ordinal=0,
            mention_id=mention_id,
            outcome="ambiguous_candidates",
            resolved_at=datetime.now(UTC),
            payload=_ambiguous_ledger_entry_payload(
                mention_id=mention_id, candidates=forged_candidates
            ),
        )
        forged_frame = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=False
        )
        forged_frame["clarification_candidates"] = forged_candidates

        # This SHOULD raise once CHAOS-3330 validates append_resolution's
        # payload against the authorized catalog. It does not today, which
        # is exactly the residual seam this test documents.
        with pytest.raises(DevPersistenceValidationError):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(forged_frame["frame_id"]),
                public_outcome="needs_clarification",
                payload=forged_frame,
            )


@pytest.mark.asyncio
async def test_record_frame_clarification_candidates_check_is_new_not_preexisting(
    persistence, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RED/GREEN pair: before this fix, ``record_frame`` had no notion of
    ``_authorize_clarification_candidates`` at all and would have persisted
    an unauthorized candidate verbatim as canonical v2 state."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _needs_clarification_frame_payload(
            run_id=run_id, with_candidates=True
        )
        frame_payload["clarification_candidates"][0]["entity_ref"]["entity_id"] = (
            "other-org-secret-repo"
        )

        async def _pre_3325_noop_authorization(*_args: Any, **_kwargs: Any) -> None:
            return None

        # RED: the pre-3325 record_frame had no cross-check for this field
        # at all -- reproduced here as a no-op, and shown to accept the
        # exact same unauthorized payload the negative test above rejects.
        monkeypatch.setattr(
            dev_persistence_service,
            "_authorize_clarification_candidates",
            _pre_3325_noop_authorization,
        )
        record = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="needs_clarification",
            payload=frame_payload,
        )
        assert (
            record.payload["clarification_candidates"][0]["entity_ref"]["entity_id"]
            == "other-org-secret-repo"
        ), "RED: the unauthorized candidate persisted verbatim, unguarded"

        # GREEN: restore the real check -- the identical payload on a fresh
        # run is now rejected (proven by the dedicated negative test above;
        # re-asserted here on monkeypatch teardown for the direct contrast).
        monkeypatch.undo()
        _conv_id2, run_id2 = await _accepted_run(
            service, org_id=org_id, user_id=user_id
        )
        frame_payload_2 = _needs_clarification_frame_payload(
            run_id=run_id2, with_candidates=True
        )
        frame_payload_2["clarification_candidates"][0]["entity_ref"]["entity_id"] = (
            "other-org-secret-repo"
        )
        with pytest.raises(
            DevPersistenceValidationError, match="clarification_candidates"
        ):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id2,
                frame_id=uuid.UUID(frame_payload_2["frame_id"]),
                public_outcome="needs_clarification",
                payload=frame_payload_2,
            )


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

        narrative_id = uuid.uuid4()
        narrative_payload, narrative_text, provider_fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=real_frame_id,
            narrative_id=narrative_id,
            mode="deterministic_fallback",
        )
        record = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=real_frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=provider_fingerprint,
            narrative_text=narrative_text,
            payload=narrative_payload,
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


@pytest.mark.asyncio
async def test_record_narrative_rejects_a_schema_only_junk_payload(persistence):
    """CHAOS-3297 Codex review round 3 CLASS B, the exact record_frame
    repro applied to record_narrative: a schema-only stub must not be
    silently accepted just because a real frame already exists."""

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

        with pytest.raises(DevPersistenceValidationError, match="not a valid"):
            await service.record_narrative(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                narrative_id=uuid.uuid4(),
                frame_id=real_frame_id,
                mode="deterministic_fallback",
                provider_fingerprint=None,
                narrative_text="Here is a safe presentation summary.",
                payload={"schema_version": "dev_narrative.v1"},
            )

        narratives = await session.scalar(
            select(func.count()).select_from(DevRunNarrative)
        )
        assert narratives == 0, "a rejected junk payload must never be written"


# -- codex NO-SHIP finding round 1 (HIGH #2b): record_narrative's SAVEPOINT
# isolation. All three exercise the REAL PersistenceRunRecorder-facing
# service layer against a real SQLite session -- the clean-RuntimeError
# fake in test_orchestrator.py's Recorder cannot reproduce either failure
# class, since neither a pre-insert Python-level rejection nor a genuine
# flush-time IntegrityError behaves like a bare raise on a real session.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_narrative_pre_insert_rejection_never_poisons_the_session(
    persistence,
) -> None:
    """The exact mismatch codex found: DevNarrative.body's contract bound
    (LongText, 16,384 chars) is looser than persistence's own byte bound
    (_NARRATIVE_TEXT_MAX_BYTES, 8 KiB) -- a contract-valid narrative can
    still be rejected here. The rejection must be a clean, pre-insert
    DevPersistenceValidationError (no DB write attempted at all), and the
    session must stay fully usable for a subsequent real write on it --
    proving this failure class was never the one needing a SAVEPOINT (the
    Python-level bound check runs entirely before any flush)."""

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

        # 9,063 chars: comfortably inside DevNarrative.body's 16,384-char
        # LongText bound, comfortably past persistence's 8,192-byte bound
        # (pure ASCII, so char count == byte count here) -- codex's exact
        # repro shape.
        oversized_text = "n" * 9_063
        narrative_id = uuid.uuid4()
        narrative_payload, _text, provider_fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=real_frame_id,
            narrative_id=narrative_id,
            mode="deterministic_fallback",
        )
        with pytest.raises(DevPersistenceValidationError):
            await service.record_narrative(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                narrative_id=narrative_id,
                frame_id=real_frame_id,
                mode="deterministic_fallback",
                provider_fingerprint=provider_fingerprint,
                narrative_text=oversized_text,
                payload=narrative_payload,
            )

        narratives = await session.scalar(
            select(func.count()).select_from(DevRunNarrative)
        )
        assert narratives == 0, "an oversized narrative must never be written"

        # The session must still be usable: a genuine follow-up write
        # (mirrors terminal()'s own update_run call in production) must
        # succeed, not raise PendingRollbackError.
        run = await service.update_run(
            org_id=org_id, user_id=user_id, run_id=run_id, state="insufficient_evidence"
        )
        assert run is not None
        assert run.narrative_mode is None


@pytest.mark.asyncio
async def test_record_narrative_flush_failure_is_isolated_by_a_savepoint(
    persistence,
) -> None:
    """A genuine DB-level flush failure (not a Python-level pre-insert
    rejection): dev_run_narratives' own uq_dev_run_narratives_run
    constraint rejects a second narrative row for the same run at flush
    time. Before the SAVEPOINT fix, this would mark the whole session
    rollback-only; after, only the savepoint rolls back and the session
    stays usable for a real follow-up write on it -- the same coherence
    property the pre-insert test above proves for the other failure
    class, proven here for a failure that actually reaches the database."""

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

        first_narrative_id = uuid.uuid4()
        first_payload, first_text, first_fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=real_frame_id,
            narrative_id=first_narrative_id,
            mode="deterministic_fallback",
        )
        await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=first_narrative_id,
            frame_id=real_frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=first_fingerprint,
            narrative_text=first_text,
            payload=first_payload,
        )

        second_narrative_id = uuid.uuid4()
        second_payload, second_text, second_fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=real_frame_id,
            narrative_id=second_narrative_id,
            mode="deterministic_fallback",
        )
        with pytest.raises(IntegrityError):
            await service.record_narrative(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                narrative_id=second_narrative_id,
                frame_id=real_frame_id,
                mode="deterministic_fallback",
                provider_fingerprint=second_fingerprint,
                narrative_text=second_text,
                payload=second_payload,
            )

        # The savepoint rolled back the second (rejected) row -- exactly
        # one narrative row remains, the first.
        narratives = await session.scalars(select(DevRunNarrative))
        remaining = narratives.all()
        assert len(remaining) == 1
        assert remaining[0].narrative_id == first_narrative_id

        # And the session is not poisoned: a genuine follow-up write
        # succeeds (this is the exact terminal()-after-narrative-flush
        # coherence property codex's finding named).
        run = await service.update_run(
            org_id=org_id, user_id=user_id, run_id=run_id, state="completed"
        )
        assert run is not None
        assert run.state == "completed"


@pytest.mark.asyncio
async def test_old_unprotected_narrative_write_would_have_poisoned_the_session(
    persistence,
) -> None:
    """Planted-defect proof, reconstructing record_narrative's OLD shape
    verbatim: before the fix, the narrative row was written with a bare
    ``session.add(record); await self.session.flush()`` -- no
    ``begin_nested()`` SAVEPOINT around it. This test performs exactly
    that (a real, validly-shaped second ``DevRunNarrative`` row for a run
    that already has one, added and flushed directly on the session, with
    no SAVEPOINT), and confirms what the fixed test above (``..._is_
    isolated_by_a_savepoint``) confirms does NOT happen anymore: the
    session goes rollback-only, and a subsequent genuine write -- exactly
    the terminal() call that follows record_narrative in production --
    fails with PendingRollbackError, not because that write is itself
    invalid, but because an earlier, unrelated, already-rolled-back flush
    poisoned the whole transaction."""

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

        first_narrative_id = uuid.uuid4()
        first_payload, first_text, first_fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=real_frame_id,
            narrative_id=first_narrative_id,
            mode="deterministic_fallback",
        )
        await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=first_narrative_id,
            frame_id=real_frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=first_fingerprint,
            narrative_text=first_text,
            payload=first_payload,
        )

        # record_narrative's OLD internals, reproduced directly: a real,
        # validly-shaped second row (same run_id -- violates
        # uq_dev_run_narratives_run), added and flushed with no SAVEPOINT.
        second_narrative_id = uuid.uuid4()
        second_payload, second_text, second_fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=real_frame_id,
            narrative_id=second_narrative_id,
            mode="deterministic_fallback",
        )
        second_row = DevRunNarrative(
            run_id=run_id,
            org_id=org_id,
            user_id=user_id,
            narrative_id=second_narrative_id,
            frame_id=real_frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=second_fingerprint,
            narrative_text=second_text,
            payload=second_payload,
            created_at=datetime.now(UTC),
        )
        session.add(second_row)
        with pytest.raises(IntegrityError):
            await session.flush()

        # The bug codex found: with no SAVEPOINT protecting the write
        # above, the whole session is now rollback-only. A genuine
        # follow-up write -- itself perfectly valid -- cannot proceed.
        with pytest.raises(PendingRollbackError):
            await service.update_run(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                state="completed",
            )


# -- dev_runs.terminal_error_payload (CHAOS-3297 Codex review round 3
# CLASS A closure argument) -------------------------------------------------


def _dev_error_worst_case_bytes() -> int:
    """Total worst-case JSON-encoded byte size of a legal ``dev_error.v1``.

    Derived by walking ``DevError.model_fields`` -- never a single
    hand-assembled "worst" instance, which is exactly what missed the prior
    four attempts on this branch (16 KiB, 64 KiB, 4 KiB, and the fourth
    "computed from the contract" 144 KiB constant that was itself short by
    13 bytes: ``retryable=False`` serializes one byte longer than
    ``True``, and some legal ``AwareDatetime`` renderings are longer than
    a no-fraction UTC instant). Each field's own worst case is computed
    from that field's own declared type/constraints *in isolation* and
    summed; a field shape this walker does not recognize raises rather
    than silently under-counting, so adding a new ``DevError`` field is
    caught here, not discovered by a future review round.
    """

    def _string_worst_case(max_length: int) -> int:
        # Verified empirically (not assumed): under ensure_ascii=True --
        # what json.dumps uses by default, and what every payload in this
        # module is encoded with -- an astral character needs a UTF-16
        # surrogate pair: two \uXXXX escapes, 12 ASCII bytes. That is the
        # worst per-character expansion across the full Unicode range
        # (checked by direct measurement across every Unicode plane, not
        # asserted from ISO/Unicode spec knowledge).
        return max_length * 12 + 2

    def _literal_worst_case(members: tuple[str, ...]) -> int:
        return max(len(json.dumps(m).encode("utf-8")) for m in members)

    def _datetime_worst_case() -> int:
        # Measured through the real pydantic serializer across every
        # combination of the *type's own* hard limits (MAXYEAR/MINYEAR,
        # max microsecond, and the maximal timezone offset Python's
        # datetime.timezone allows in both directions) -- not a guessed
        # ISO 8601 format string. Which combination renders longest is an
        # empirical fact about pydantic-core's renderer, not something to
        # assume by reading the ISO 8601 spec.
        from datetime import MAXYEAR, MINYEAR
        from datetime import timezone as _timezone

        offsets = [
            timedelta(hours=23, minutes=59, seconds=59, microseconds=999999),
            -timedelta(hours=23, minutes=59, seconds=59, microseconds=999999),
            timedelta(0),
        ]
        worst = 0
        for year in (MAXYEAR, MINYEAR + 1):
            for microsecond in (999999, 0):
                for offset in offsets:
                    try:
                        candidate = datetime(
                            year,
                            12,
                            31,
                            23,
                            59,
                            59,
                            microsecond,
                            tzinfo=_timezone(offset),
                        )
                    except ValueError:
                        continue
                    probe = DevError(
                        schema_version="dev_error.v1",
                        request_id="x",
                        code="internal_error",
                        safe_message="m",
                        retryable=True,
                        limit_reset_at=candidate,
                    )
                    dumped = probe.model_dump(mode="json")["limit_reset_at"]
                    worst = max(worst, len(json.dumps(dumped).encode("utf-8")))
        return worst

    def _field_worst_case(annotation: Any, metadata: list[Any]) -> int:
        origin = get_origin(annotation)
        if origin is Literal:
            return _literal_worst_case(get_args(annotation))
        if origin is types.UnionType:
            non_none = [a for a in get_args(annotation) if a is not type(None)]
            assert len(non_none) == 1, (
                f"only Optional[X] unions are handled, got {annotation!r}"
            )
            return max(len(b"null"), _field_worst_case(non_none[0], []))
        if origin is list:
            (inner_annotation,) = get_args(annotation)
            inner_metadata = list(getattr(inner_annotation, "__metadata__", ()))
            inner_type = (
                get_args(inner_annotation)[0] if inner_metadata else inner_annotation
            )
            max_items = next(
                (m.max_length for m in metadata if hasattr(m, "max_length")), None
            )
            assert max_items is not None, (
                f"list field {annotation!r} has no MaxLen constraint"
            )
            inner_worst = _field_worst_case(inner_type, inner_metadata)
            return 2 + max_items * (inner_worst + 1)
        if annotation is bool:
            return len(b"false")
        if annotation is AwareDatetime:
            return _datetime_worst_case()
        if annotation is str:
            max_length = next(
                (m.max_length for m in metadata if hasattr(m, "max_length")), None
            )
            assert max_length is not None, (
                f"str field has no max_length constraint: {metadata!r}"
            )
            return _string_worst_case(max_length)
        raise NotImplementedError(
            f"no worst-case handler for annotation={annotation!r} "
            f"metadata={metadata!r} -- teach this walker the new DevError "
            "field shape, do not let it silently under-count"
        )

    total = 2  # outer {}
    for name, info in DevError.model_fields.items():
        key_overhead = len(json.dumps(name).encode("utf-8")) + 2  # "key": + comma
        total += key_overhead + _field_worst_case(info.annotation, info.metadata)
    return total


def test_dev_error_worst_case_is_within_the_terminal_error_backstop() -> None:
    """CHAOS-3297 Codex review round 3 CLASS A closure argument.

    ``_TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES`` (``persistence/service.py``)
    is only a loose anti-runaway guard -- the real acceptance predicate for
    ``terminal_error_payload`` is ``DevError.model_validate()`` succeeding,
    not a byte cap. This proves the guard can never legitimately reject a
    valid ``dev_error.v1``: the worst case is derived by walking every
    field ``DevError`` actually declares (``_dev_error_worst_case_bytes``,
    not a hand-picked example), so a field added or extended past the
    margin fails *this* test, not a future codex round.
    """

    from dev_health_ops.api.dev.persistence.service import (
        _TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES,
    )

    computed = _dev_error_worst_case_bytes()
    assert computed < _TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES, (
        f"DevError's own worst case ({computed} bytes) no longer fits "
        f"inside the {_TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES}-byte "
        "anti-runaway backstop -- a field was added or extended past the "
        "margin"
    )


@pytest.mark.asyncio
async def test_update_run_persists_codexs_exact_previously_rejected_terminal_error(
    persistence,
) -> None:
    """The exact payload CHAOS-3297 Codex review round 3 showed the prior
    hand-computed 144 KiB cap rejected, by 13 bytes: ``retryable=False``
    serializes one byte longer than ``True``, and a maxed
    ``AwareDatetime`` with microseconds and a near-24h offset renders
    longer than the no-fraction UTC instant the old computation used. Must
    now persist cleanly, since acceptance is contract validity, not a
    hand-tuned byte cap.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        worst_char = "\U0001f600"
        payload = DevError(
            schema_version="dev_error.v1",
            request_id="x" * 128,
            code="provider_contract_violation",
            safe_message=worst_char * 2048,
            retryable=False,
            limit_reset_at=datetime(
                9999,
                12,
                31,
                23,
                59,
                59,
                999999,
                tzinfo=timezone(-timedelta(hours=23, minutes=59)),
            ),
            remediation=[worst_char * 2048] * 5,
        ).model_dump(mode="json")

        run = await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="failed",
            terminal_error_payload=payload,
        )
        assert run is not None
        assert run.terminal_error_payload is not None
        assert run.terminal_error_payload["safe_message"] == worst_char * 2048
        assert run.terminal_error_payload["retryable"] is False


@pytest.mark.asyncio
async def test_update_run_rejects_an_invalid_terminal_error_payload(
    persistence,
) -> None:
    """The acceptance predicate is contract validity, not a byte cap: a
    payload that fails ``DevError.model_validate`` must be rejected
    regardless of size."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        with pytest.raises(DevPersistenceValidationError, match="not a valid"):
            await service.update_run(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                state="failed",
                terminal_error_payload={"schema_version": "dev_error.v1"},
            )


# -- dev_runs.narrative_mode / narrative_failure_code (CHAOS-3297 stack #4) --


@pytest.mark.asyncio
async def test_update_run_persists_narrative_mode_and_failure_code(persistence) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        run = await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="insufficient_evidence",
            narrative_mode="deterministic_fallback",
            narrative_failure_code="narrative_grounding_failed",
        )
        assert run is not None
        assert run.narrative_mode == "deterministic_fallback"
        assert run.narrative_failure_code == "narrative_grounding_failed"


@pytest.mark.asyncio
async def test_update_run_narrative_mode_and_failure_code_default_to_none(
    persistence,
) -> None:
    """A run with no narrative synthesized (e.g. a no-answer outcome) must
    leave both columns NULL, not some sentinel string -- migration 0078's
    docstring: 'Both stay NULL for every run [without narrative
    synthesis]'."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        run = await service.update_run(
            org_id=org_id, user_id=user_id, run_id=run_id, state="insufficient_evidence"
        )
        assert run is not None
        assert run.narrative_mode is None
        assert run.narrative_failure_code is None


@pytest.mark.asyncio
async def test_update_run_rejects_a_narrative_mode_outside_the_closed_vocabulary(
    persistence,
) -> None:
    """Mirrors record_narrative's own ``mode not in _NARRATIVE_MODES``
    check -- the run-level column cannot legally diverge from the
    DevRunNarrative row's own closed vocabulary. Planted-defect proof: a
    value one edit distance from a real mode (a truncation) must still be
    rejected, not silently coerced or accepted."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        with pytest.raises(DevPersistenceValidationError, match="narrative mode"):
            await service.update_run(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                state="failed",
                narrative_mode="provider_",  # not a real mode
            )


@pytest.mark.asyncio
async def test_update_run_rejects_an_oversized_narrative_failure_code(
    persistence,
) -> None:
    """Bounded to the dev_runs.narrative_failure_code column width
    (String(64)) -- oversized input must reject at the service layer, not
    truncate silently at the database."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        with pytest.raises(DevPersistenceValidationError):
            await service.update_run(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                state="failed",
                narrative_failure_code="x" * 65,
            )


@pytest.mark.asyncio
async def test_update_run_rejects_a_narrative_failure_code_outside_the_closed_vocabulary(
    persistence,
) -> None:
    """CHAOS-3297 codex NO-SHIP finding round 1, MEDIUM #3: the producer's
    own check, mirroring narrative_mode's -- a shape/size-legal but
    invented code (never a real ``NarrativeFailureCode`` member) must
    reject here, not merely pass through to the DB CHECK constraint."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        with pytest.raises(
            DevPersistenceValidationError, match="narrative failure code"
        ):
            await service.update_run(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                state="failed",
                # one edit distance from a real member (narrative_grounding_failed)
                narrative_failure_code="narrative_grounding_faile",
            )


# -- CHAOS-3297 codex NO-SHIP finding round 1, MEDIUM #3: dev_runs
# .narrative_failure_code's closed vocabulary at the DB boundary (migration
# 0083's ck_dev_runs_narrative_failure_code CHECK constraint). Unlike
# dev_answer_frames/dev_run_narratives.payload (migration 0080), this
# column has no Python-side ORM listener at all -- a scalar CHECK needs
# none -- so every write shape below is rejected by the database itself,
# with no session-level guard to disable first.


@pytest.mark.asyncio
async def test_db_check_rejects_a_direct_orm_attribute_assignment_bypass(
    persistence,
) -> None:
    """Write shape 1/3: ``row.narrative_failure_code = x`` on an
    already-persisted ORM instance, entirely outside ``update_run`` --
    caught only by the DB CHECK constraint at flush time."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await session.commit()

        run = await session.get(DevRun, run_id)
        assert run is not None
        run.narrative_failure_code = "an_invented_code"
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevRun, run_id)
        assert reloaded is not None
        assert reloaded.narrative_failure_code is None, (
            "the invalid attribute-assignment write must not persist"
        )


@pytest.mark.asyncio
async def test_db_check_rejects_a_bulk_update_values_bypass(persistence) -> None:
    """Write shape 2/3: ``update(DevRun).values(narrative_failure_code=x)``
    -- a Core-style bulk statement issued through the Session, never going
    through the ORM unit-of-work at all."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await session.commit()

        with pytest.raises(IntegrityError):
            await session.execute(
                update(DevRun)
                .where(DevRun.id == run_id)
                .values(narrative_failure_code="an_invented_code")
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevRun, run_id)
        assert reloaded is not None
        assert reloaded.narrative_failure_code is None, (
            "the invalid bulk-update write must not persist"
        )


@pytest.mark.asyncio
async def test_db_check_rejects_a_raw_connection_write(persistence) -> None:
    """Write shape 3/3: a ``Connection`` obtained via ``session.connection()``,
    entirely outside the ORM/Session write surface -- confirms the
    constraint is enforced by the database itself, not by anything this
    application's own code executes."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await session.commit()

        connection = await session.connection()
        with pytest.raises(IntegrityError):
            await connection.execute(
                update(DevRun)
                .where(DevRun.id == run_id)
                .values(narrative_failure_code="an_invented_code")
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevRun, run_id)
        assert reloaded is not None
        assert reloaded.narrative_failure_code is None, (
            "the raw-connection bypass attempt must not persist"
        )


@pytest.mark.asyncio
async def test_db_check_permits_every_real_narrative_failure_code(
    persistence,
) -> None:
    """Positive control for the CHECK constraint itself: every real
    ``NarrativeFailureCode`` member -- not just the one fixture value the
    happy-path test above uses -- must be writable at the DB boundary."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        for code in _base.NarrativeFailureCode:
            _conv_id, run_id = await _accepted_run(
                service, org_id=org_id, user_id=user_id
            )
            run = await service.update_run(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                state="failed",
                narrative_failure_code=code.value,
            )
            assert run is not None
            assert run.narrative_failure_code == code.value


# -- force_terminal_fallback (CHAOS-3297 Codex review round 3 Finding 2) --


@pytest.mark.asyncio
async def test_force_terminal_fallback_forces_a_stuck_run_terminal(persistence) -> None:
    """CHAOS-3297 Codex review round 3 Finding 2.

    A run stuck in a non-terminal state (simulating finish()'s own
    terminal write having failed after other artifacts already flushed on
    a now-poisoned session) must be forced into a safe terminal state by
    this last-resort call, from an independent session, unconditionally.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="resolving_scope",
        )

        run_before = await session.get(DevRun, run_id)
        assert run_before is not None
        assert run_before.state == "resolving_scope"
        assert run_before.ended_at is None

        await service.force_terminal_fallback(
            org_id=org_id, user_id=user_id, run_id=run_id
        )

        run_after = await session.get(DevRun, run_id)
        assert run_after is not None
        assert run_after.state == "failed"
        assert run_after.safe_error_code == "internal_error"
        assert run_after.terminal_error_payload is None
        assert run_after.ended_at is not None


@pytest.mark.asyncio
async def test_force_terminal_fallback_is_idempotent_for_an_already_terminal_run(
    persistence,
) -> None:
    """A run that finish() actually completed successfully (already
    terminal) must be left untouched -- the fallback only exists for a run
    that never reached a terminal state at all."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="insufficient_evidence",
            safe_error_code="scope_not_found",
        )
        completed_at = (await session.get(DevRun, run_id)).ended_at
        assert completed_at is not None

        await service.force_terminal_fallback(
            org_id=org_id, user_id=user_id, run_id=run_id
        )

        run_after = await session.get(DevRun, run_id)
        assert run_after is not None
        assert run_after.state == "insufficient_evidence", (
            "an already-terminal run's real outcome must never be "
            "overwritten by the fallback"
        )
        assert run_after.safe_error_code == "scope_not_found"
        assert run_after.ended_at == completed_at


@pytest.mark.asyncio
async def test_force_terminal_fallback_is_a_noop_for_a_missing_run(persistence) -> None:
    """A run_id that does not exist (or belongs to a different org) must
    not raise -- this is a best-effort last resort, not a hard
    precondition check."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        await service.force_terminal_fallback(
            org_id=org_id, user_id=user_id, run_id=uuid.uuid4()
        )


# -- recover_stale_non_terminal_run (CHAOS-3297 Codex review round 5 HIGH) --
#
# force_terminal_fallback is the request's own last-resort write. This is
# the SECOND chance: if that ALSO fails on the same DB incident, a run
# stays non-terminal forever and every future replay of that
# client_message_id would 409 indefinitely with no fallback attempt left
# to run. recover_stale_non_terminal_run is invoked from the replay path
# itself, at the moment a caller actually asks for the run again.


@pytest.mark.asyncio
async def test_recover_stale_non_terminal_run_forces_a_stuck_run_terminal_when_old_enough(
    persistence,
) -> None:
    """The double-failure scenario this closes: force_terminal_fallback
    itself failed (simulated here by never being called at all -- the run
    is left exactly as a crashed run_with_events would leave it), and the
    run is old enough that it cannot possibly still be genuinely in
    flight. Recovery must force it terminal and persist that to the real
    database, not merely return an in-memory value."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="resolving_scope",
        )
        run = await session.get(DevRun, run_id)
        assert run is not None
        run.started_at = datetime.now(UTC) - timedelta(minutes=10)
        await session.commit()

    async with maker() as session:
        service = DevPersistenceService(session)
        recovered = await service.recover_stale_non_terminal_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            stale_after=timedelta(minutes=5),
        )
        assert recovered is not None
        assert recovered.state == "failed"
        assert recovered.safe_error_code == "internal_error"
        assert recovered.terminal_error_payload is None
        assert recovered.ended_at is not None

    async with maker() as session:
        run_after = await session.get(DevRun, run_id)
        assert run_after is not None
        assert run_after.state == "failed", (
            "recovery must be durably committed to the database, not just "
            "returned in-memory"
        )
        assert run_after.safe_error_code == "internal_error"
        assert run_after.ended_at is not None


@pytest.mark.asyncio
async def test_recover_stale_non_terminal_run_leaves_a_fresh_run_untouched_and_returns_none(
    persistence,
) -> None:
    """A non-terminal run younger than the threshold is genuine in-flight
    concurrency, not a stuck run -- it must be left exactly as-is, and the
    caller (the 409 branch) must still be told to reject the duplicate."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="resolving_scope",
        )
        await session.commit()

    async with maker() as session:
        service = DevPersistenceService(session)
        recovered = await service.recover_stale_non_terminal_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            stale_after=timedelta(minutes=5),
        )
        assert recovered is None, (
            "a fresh non-terminal run must not be recovered -- it may "
            "still be a genuinely in-flight request"
        )

    async with maker() as session:
        run_after = await session.get(DevRun, run_id)
        assert run_after is not None
        assert run_after.state == "resolving_scope", (
            "a fresh non-terminal run must be left completely untouched"
        )
        assert run_after.ended_at is None


@pytest.mark.asyncio
async def test_recover_stale_non_terminal_run_returns_an_already_terminal_run_as_is(
    persistence,
) -> None:
    """If the run actually completed -- by the original request after
    all, or a previous recovery -- between the caller's own read and this
    call's row lock, its real outcome must never be overwritten."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="insufficient_evidence",
            safe_error_code="scope_not_found",
        )
        completed_at = (await session.get(DevRun, run_id)).ended_at
        assert completed_at is not None

        recovered = await service.recover_stale_non_terminal_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            stale_after=timedelta(minutes=5),
        )
        assert recovered is not None
        assert recovered.state == "insufficient_evidence"
        assert recovered.safe_error_code == "scope_not_found"
        assert recovered.ended_at == completed_at


@pytest.mark.asyncio
async def test_recover_stale_non_terminal_run_is_a_noop_for_a_missing_run(
    persistence,
) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        recovered = await service.recover_stale_non_terminal_run(
            org_id=org_id,
            user_id=user_id,
            run_id=uuid.uuid4(),
            stale_after=timedelta(minutes=5),
        )
        assert recovered is None


@pytest.mark.asyncio
async def test_recover_stale_non_terminal_run_refreshes_a_stale_identity_map_entry(
    persistence,
) -> None:
    """CHAOS-3297 Codex review round 7 HIGH.

    The row lock (``SELECT ... FOR UPDATE``) forces the SQL to wait for
    and then see the latest committed row -- but that alone is not
    enough. If the calling session already has this run's identity
    mapped from an EARLIER load (exactly like router.py's
    ``service.session`` does, via ``append_user_message_and_run``,
    earlier in the very same request), SQLAlchemy's default loader
    behavior returns that SAME cached Python object without refreshing
    its attributes from the row this query just locked. A run that
    genuinely completed for real -- via a second, independent session,
    standing in for the live orchestrator's own successful terminal
    write racing this recovery call -- in the gap between the caller's
    stale read and this call must be preserved exactly, never
    overwritten with ``failed``/``internal_error``.

    Two real sessions against the same aiosqlite database, asserting
    final state from a THIRD, fresh session -- no spies on the mechanism
    under test.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session_a:
        service_a = DevPersistenceService(session_a)
        _conv_id, run_id = await _accepted_run(
            service_a, org_id=org_id, user_id=user_id
        )
        await service_a.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="resolving_scope",
        )
        run = await session_a.get(DevRun, run_id)
        assert run is not None
        run.started_at = datetime.now(UTC) - timedelta(minutes=10)
        await session_a.commit()

        # session_a's identity map now holds this run object -- read as
        # non-terminal ('resolving_scope') and old enough to recover.
        # Nothing on session_a touches this row again until the call
        # under test, below.

        # A SEPARATE session -- standing in for the request's own path
        # actually completing the run for real, in the gap between the
        # caller's stale read and the recovery call below -- commits the
        # real, successful outcome.
        async with maker() as session_b:
            service_b = DevPersistenceService(session_b)
            completed = await service_b.update_run(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                state="completed",
            )
            assert completed is not None
            await session_b.commit()
            completed_at = completed.ended_at
            assert completed_at is not None

        # Sanity: session_a's cached object must still read stale right
        # up until the call under test -- otherwise this test would not
        # be exercising the identity-map gap at all, it would just be
        # proving the row lock waits (already covered elsewhere).
        assert run.state == "resolving_scope", (
            "sanity: session_a's identity-mapped instance must still be "
            "stale here for this test to exercise the gap it targets"
        )

        recovered = await service_a.recover_stale_non_terminal_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            stale_after=timedelta(minutes=5),
        )
        assert recovered is not None
        assert recovered.state == "completed", (
            "a run that genuinely completed between the caller's stale "
            "read and this call must be returned as-is, never "
            "overwritten by a stale-identity-map recovery"
        )
        assert recovered.safe_error_code != "internal_error"
        assert recovered.ended_at is not None
        assert recovered.ended_at.replace(tzinfo=UTC) == completed_at

    async with maker() as session_c:
        run_after = await session_c.get(DevRun, run_id)
        assert run_after is not None
        assert run_after.state == "completed", (
            "the real completion must be durably preserved in the "
            "database, not overwritten by the recovery call"
        )
        assert run_after.safe_error_code != "internal_error"
        assert run_after.ended_at is not None
        assert run_after.ended_at.replace(tzinfo=UTC) == completed_at


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
    narrative_id = uuid.uuid4()
    narrative_payload, narrative_text, provider_fingerprint = _narrative_payload(
        run_id=run_id,
        frame_id=frame_id,
        narrative_id=narrative_id,
        mode="deterministic_fallback",
    )
    await service.record_narrative(
        org_id=org_id,
        user_id=user_id,
        run_id=run_id,
        narrative_id=narrative_id,
        frame_id=frame_id,
        mode="deterministic_fallback",
        provider_fingerprint=provider_fingerprint,
        narrative_text=narrative_text,
        payload=narrative_payload,
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


# -- totality: every *_payload JSON-contract sink is registered (CHAOS-3297
# Codex review round 3 CLASS B closure argument) ---------------------------

#: Known, filed gap -- NOT closed this round. Each of these also persists a
#: full wire-contract dump as an opaque ``payload`` without validating it
#: against that contract (``record_intent`` -> ``dev_question_intent.v1``,
#: ``append_resolution`` -> a ``dev_resolution_ledger.v1`` entry,
#: ``record_subject_set`` -> ``dev_subject_set.v1``,
#: ``append_source_observation`` -> ``dev_source_observation.v1``,
#: ``record_qua_shadow`` (CHAOS-3389) -> a ``qua_shadow.QUAShadowRecord``
#: projection, which is not a frontend-facing wire contract at all -- see
#: ``qua_shadow.py``'s own module docstring) -- the same open-boundary
#: shape ``record_frame``/``record_narrative`` had. Enumerated here
#: deliberately, not silently ignored: closing these is out of this
#: round's scope, but touching any of these five names is now a conscious
#: edit to this set, not a silent pass through the totality assertion
#: below.
_KNOWN_UNVALIDATED_PAYLOAD_SINKS = frozenset(
    {
        "record_intent",
        "append_resolution",
        "record_subject_set",
        "append_source_observation",
        "record_qua_shadow",
    }
)


def _payload_bearing_orm_model_names() -> frozenset[str]:
    """Every ``models/dev_persistence`` ORM model with a ``payload``
    column, discovered by walking the module's own classes and SQLAlchemy
    column metadata -- never a hand-typed list."""

    from dev_health_ops.models import dev_persistence as models_module

    names: set[str] = set()
    for name in dir(models_module):
        candidate = getattr(models_module, name)
        if not isinstance(candidate, type) or not hasattr(candidate, "__table__"):
            continue
        if "payload" in {column.name for column in candidate.__table__.columns}:
            names.add(name)
    return frozenset(names)


def _method_references_payload_field(node: ast.AsyncFunctionDef) -> bool:
    """True if ``node``'s own body references the ``payload`` column
    identifier in ANY of the three AST shapes Python has for naming it:

    * a call keyword argument -- covers both ``Model(payload=x)`` and
      ``update(Model).values(payload=x)`` alike, because ``sub.func`` is
      never inspected here; it does not matter whether the call's callee
      is a bare model name, an ``update(...)`` chain, or anything else.
    * a dict-literal string key -- covers ``Model(**{"payload": x})``
      kwargs-splat construction, where the identifier never appears as an
      ``ast.keyword`` at all.
    * an attribute-assignment target -- covers ``row.payload = x``, which
      matches no ``Call`` pattern whatsoever.

    Deny-by-default (CHAOS-3297 Codex review round 5 MEDIUM closure):
    this does not try to recognize specific bypass *forms*, it simply
    refuses to let the ``payload`` identifier appear anywhere in a sink
    method's body in any of the three shapes above.
    """

    for sub in ast.walk(node):
        if isinstance(sub, ast.keyword) and sub.arg == "payload":
            return True
        if isinstance(sub, ast.Constant) and sub.value == "payload":
            return True
        if isinstance(sub, ast.Assign) and any(
            isinstance(target, ast.Attribute) and target.attr == "payload"
            for target in sub.targets
        ):
            return True
        if (
            isinstance(sub, ast.AnnAssign)
            and isinstance(sub.target, ast.Attribute)
            and sub.target.attr == "payload"
        ):
            return True
    return False


def _discover_payload_field_references() -> frozenset[str]:
    """Every ``DevPersistenceService`` method other than the one audited
    construction helper (``_construct_validated_payload_row``) whose body
    references the ``payload`` identifier in any of the three shapes
    ``_method_references_payload_field`` checks -- discovered by walking
    the service's own source (AST), never a hand-typed method list. A
    method renamed, added, or changed to touch ``payload`` in any of
    those shapes is picked up automatically; nothing here has to be
    updated by hand for that.
    """

    source = inspect.getsource(DevPersistenceService)
    tree = ast.parse(source)
    (class_node,) = (node for node in tree.body if isinstance(node, ast.ClassDef))
    discovered: set[str] = set()
    for node in class_node.body:
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        if node.name == "_construct_validated_payload_row":
            continue
        if _method_references_payload_field(node):
            discovered.add(node.name)
    return frozenset(discovered)


def test_payload_bearing_orm_model_names_matches_the_live_schema() -> None:
    """Sanity for the sink-totality test below: the target model set must
    itself track ``models/dev_persistence.py``'s live schema, not a value
    frozen at write time. If this fails, a model gained or lost a
    ``payload`` column and the sink-totality test's coverage silently
    changed underneath it."""

    assert _payload_bearing_orm_model_names() == {
        "DevRunIntent",
        "DevRunResolution",
        "DevRunSubjectSet",
        "DevRunSourceObservation",
        "DevAnswerFrame",
        "DevRunNarrative",
        "DevRunQuaShadow",
    }


def test_every_payload_field_reference_is_confined_to_the_audited_helper() -> None:
    """CHAOS-3297 Codex review round 5 MEDIUM closure argument.

    Deny-by-default, not pattern-completeness: the round-3 scanner this
    replaces pattern-matched a single write *form* (a direct
    ``Model(payload=...)`` call whose callee is a bare ``ast.Name`` on a
    payload-bearing model class), and Codex found three shapes it
    structurally could not see -- ``row.payload = x`` (no ``Call`` node at
    all), ``Model(**{"payload": x})`` (the identifier is a dict key, not
    an ``ast.keyword``), and ``update(Model).values(payload=x)`` (the
    callee is a method-attribute chain, not a bare model name).

    This scanner does not enumerate write forms. It enumerates -- via AST
    introspection of ``DevPersistenceService``'s own source, never a
    hand-typed method list -- every method OTHER than the one audited
    construction helper (``_construct_validated_payload_row``) that
    references the ``payload`` identifier in ANY of its three possible
    AST shapes, and asserts the result is exactly
    ``_KNOWN_UNVALIDATED_PAYLOAD_SINKS`` -- the filed, deliberate gap.

    ``record_frame`` and ``record_narrative`` -- the two sinks CHAOS-3297
    has brought into full contract-validated compliance (payload validated
    against its real wire contract and cross-checked against the call's
    own arguments before any write: ``record_frame`` in round 2 MEDIUM #3,
    ``record_narrative`` in round 3 CLASS B) -- route their construction
    entirely through the helper, so neither appears in the discovered set
    at all -- not merely present in an accounted-for bucket the way the
    round-3 scanner required. A validated sink that regressed to touching
    ``payload`` directly again would show up as an unaccounted-for name
    here, exactly like a brand-new bypass would; there is nothing else it
    could hide behind.
    """

    discovered = _discover_payload_field_references()
    assert discovered == _KNOWN_UNVALIDATED_PAYLOAD_SINKS, (
        f"unaudited payload-field references outside the helper: "
        f"{sorted(discovered - _KNOWN_UNVALIDATED_PAYLOAD_SINKS)} -- "
        f"stale ledger entries (no longer touch payload directly): "
        f"{sorted(_KNOWN_UNVALIDATED_PAYLOAD_SINKS - discovered)}"
    )


def test_a_new_unvalidated_sink_is_caught_by_the_totality_assertion() -> None:
    """Rule 2: observe the totality guard actually fail, not just exist.

    Simulates a new sink method being introduced (the discovered set gains
    a name present in neither accounted-for bucket) and shows the
    assertion the test above makes would now fail -- the guard is load
    bearing, not decorative.
    """

    discovered = _discover_payload_field_references() | {"record_something_new"}
    assert discovered != _KNOWN_UNVALIDATED_PAYLOAD_SINKS
    assert "record_something_new" in (discovered - _KNOWN_UNVALIDATED_PAYLOAD_SINKS)


def _parse_single_async_method(source: str) -> ast.AsyncFunctionDef:
    """Parse a standalone, dedented async-method snippet for the mutation
    tests below -- these exercise ``_method_references_payload_field``
    directly against synthetic bypass-shaped bodies, not the live service
    source, to prove the scanner's own detection logic rather than merely
    that today's service.py happens to be clean."""

    (node,) = (
        n
        for n in ast.parse(textwrap.dedent(source)).body
        if isinstance(n, ast.AsyncFunctionDef)
    )
    return node


def test_scanner_catches_attribute_assignment_bypass() -> None:
    """Codex round 5 bypass form 1/3: ``row.payload = x``. This matches no
    ``Call`` node at all, which is exactly why the round-3 Name-call
    scanner (RED: it only ever inspected ``ast.Call`` nodes) missed it."""

    node = _parse_single_async_method(
        """
        async def sneaky_direct_assignment(self, *, payload):
            row = DevAnswerFrame(run_id=self.run_id)
            row.payload = payload
            self.session.add(row)
        """
    )
    assert _method_references_payload_field(node) is True


def test_scanner_catches_kwargs_splat_bypass() -> None:
    """Codex round 5 bypass form 2/3: ``Model(**{"payload": x})``. The
    round-3 scanner only matched a literal ``payload=`` ``ast.keyword`` in
    the call; a dict-literal key spread into the call via ``**`` is an
    ``ast.Constant``, never an ``ast.keyword``, so it never matched
    (RED)."""

    node = _parse_single_async_method(
        """
        async def sneaky_kwargs_splat(self, *, payload):
            row = DevAnswerFrame(**{"payload": payload, "run_id": self.run_id})
            self.session.add(row)
        """
    )
    assert _method_references_payload_field(node) is True


def test_scanner_catches_update_values_bypass() -> None:
    """Codex round 5 bypass form 3/3: ``update(Model).values(payload=x)``.
    The round-3 scanner required ``sub.func`` to be a bare ``ast.Name``
    equal to a payload-bearing model class name; ``update(...).values(...)``
    is an attribute-chain call, so ``sub.func`` is an ``ast.Attribute``,
    never a bare ``ast.Name`` -- it never matched regardless of the
    keyword the call carried (RED)."""

    node = _parse_single_async_method(
        """
        async def sneaky_update_values(self, *, payload):
            stmt = update(DevAnswerFrame).values(payload=payload)
            await self.session.execute(stmt)
        """
    )
    assert _method_references_payload_field(node) is True


def test_scanner_does_not_false_positive_on_a_clean_method() -> None:
    """Control: a method that never touches ``payload`` in any shape --
    not as a keyword, a dict key, nor an attribute target -- is not
    flagged. Without this, the three mutation tests above would not prove
    the scanner discriminates; they would only prove it always returns
    ``True``."""

    node = _parse_single_async_method(
        """
        async def clean_method(self, *, frame_id):
            row = DevAnswerFrame(run_id=self.run_id, frame_id=frame_id)
            self.session.add(row)
        """
    )
    assert _method_references_payload_field(node) is False


# -- ORM-boundary payload validation (CHAOS-3297 Codex review round 7
# MEDIUM): the AST scanner above is a syntax-level tripwire -- it only
# ever recognizes the write *shapes* it was taught to look for, in this
# module's own source. SQLAlchemy mapper/session events registered in
# persistence/service.py are the load-bearing guard instead: they fire
# for a write against a payload-bearing table however it was
# constructed, in ANY module, and reject an invalid payload before it
# ever reaches the database. -----------------------------------------


def test_orm_boundary_payload_validator_registry_matches_the_live_schema() -> None:
    """The event-listener registry (``_PAYLOAD_MODEL_VALIDATORS``) must
    cover every payload-bearing model discovered from live schema -- the
    same totality property the AST scanner's model-name set already
    protects, now for the ORM-boundary guard too. A ninth payload-bearing
    model with no entry either way (a real validator, or the explicit
    ``_KNOWN_UNVALIDATED_PAYLOAD_GAP`` sentinel) fails this at collection
    time, before any write against it could silently go unvalidated."""

    registered_names = frozenset(
        model_cls.__name__ for model_cls in _PAYLOAD_MODEL_VALIDATORS
    )
    assert registered_names == _payload_bearing_orm_model_names()


@pytest.mark.asyncio
async def test_orm_boundary_rejects_a_direct_attribute_assignment_bypass(
    persistence,
) -> None:
    """Codex bypass form 1/4: ``row.payload = x``, set on a freshly
    constructed instance before it is ever added to the session -- no
    call to ``record_frame`` involved at all. Caught at flush time by the
    ``before_insert`` mapper event, not the AST scanner."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        row = DevAnswerFrame(
            run_id=run_id,
            org_id=org_id,
            user_id=user_id,
            frame_id=uuid.uuid4(),
            public_outcome="answered",
        )
        row.payload = {"schema_version": "dev_answer_frame.v1"}
        session.add(row)
        with pytest.raises(DevPersistenceValidationError):
            await session.commit()
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.run_id == run_id)
        )
        assert count == 0, "the invalid attribute-assignment write must not persist"


@pytest.mark.asyncio
async def test_orm_boundary_rejects_a_kwargs_splat_construction_bypass(
    persistence,
) -> None:
    """Codex bypass form 2/4: ``Model(**{"payload": x})`` -- still an
    ORM-instance construction (``before_insert`` fires), but syntactically
    distinct from a keyword literal, exactly the shape the round-3 AST
    scanner could not see at all."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        kwargs = {
            "run_id": run_id,
            "org_id": org_id,
            "user_id": user_id,
            "frame_id": uuid.uuid4(),
            "public_outcome": "answered",
            "payload": {"schema_version": "dev_answer_frame.v1"},
        }
        row = DevAnswerFrame(**kwargs)
        session.add(row)
        with pytest.raises(DevPersistenceValidationError):
            await session.commit()
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.run_id == run_id)
        )
        assert count == 0, "the invalid kwargs-splat write must not persist"


@pytest.mark.asyncio
async def test_orm_boundary_rejects_a_bulk_update_values_bypass(persistence) -> None:
    """Codex bypass form 3/4: ``update(Model).values(payload=x)``. This
    is a Core-style bulk statement -- it never goes through the ORM
    unit-of-work flush ``before_insert``/``before_update`` fire from at
    all (confirmed empirically: a probe ``before_update`` listener never
    fires for this call against a real engine), so only the
    ``do_orm_execute`` session-level listener can see it."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        with pytest.raises(DevPersistenceValidationError):
            await session.execute(
                update(DevAnswerFrame)
                .where(DevAnswerFrame.id == frame_id)
                .values(payload={"schema_version": "dev_answer_frame.v1"})
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "the bulk-update bypass attempt must leave the original, "
            "valid payload completely untouched"
        )


@pytest.mark.asyncio
async def test_orm_boundary_rejects_a_bulk_insert_values_bypass(persistence) -> None:
    """Codex bypass form 4/4: ``insert(Model).values(payload=x)`` -- a
    bulk helper in a different module never has to go through
    ``record_frame``/``_construct_validated_payload_row`` at all; only
    the ``do_orm_execute`` listener can see this shape."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        new_id = uuid.uuid4()
        with pytest.raises(DevPersistenceValidationError):
            await session.execute(
                insert(DevAnswerFrame).values(
                    id=new_id,
                    run_id=run_id,
                    org_id=org_id,
                    user_id=user_id,
                    frame_id=uuid.uuid4(),
                    public_outcome="answered",
                    payload={"schema_version": "dev_answer_frame.v1"},
                    created_at=datetime.now(UTC),
                )
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, new_id)
        assert reloaded is None, "the bulk-insert bypass attempt must not create a row"


@pytest.mark.asyncio
async def test_orm_boundary_narrative_bulk_update_requires_narrative_text_too(
    persistence,
) -> None:
    """A bulk statement has no live ORM instance to reconstruct
    ``DevRunNarrative``'s ``body`` from (``narrative_text`` is a separate
    column) -- setting ``payload`` alone, without ``narrative_text`` in
    the SAME statement, must be rejected rather than silently validated
    against a stale or absent ``body``."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="answered",
            payload=frame_payload,
        )
        narrative_id = uuid.uuid4()
        payload, narrative_text, _fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=frame.frame_id,
            narrative_id=narrative_id,
            mode="deterministic_fallback",
        )
        narrative = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=frame.frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text=narrative_text,
            payload=payload,
        )
        await session.commit()
        narrative_pk = narrative.id

        # payload alone, no narrative_text in the same statement -- must
        # be rejected even though `payload` on its own looks structurally
        # plausible (it is the same valid `payload` the row already has).
        with pytest.raises(DevPersistenceValidationError):
            await session.execute(
                update(DevRunNarrative)
                .where(DevRunNarrative.id == narrative_pk)
                .values(payload=payload)
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevRunNarrative, narrative_pk)
        assert reloaded is not None
        assert reloaded.narrative_text == narrative_text


@pytest.mark.asyncio
async def test_orm_boundary_permits_a_valid_bulk_update_when_every_needed_column_is_set(
    persistence,
) -> None:
    """Control: the ORM-boundary guard rejects an invalid,
    under-specified, or row-mismatched bulk write -- not bulk writes to
    these tables in general. A bulk update setting every column its
    cross-checks need (``payload``, ``narrative_text``, ``narrative_id``,
    ``run_id``, ``frame_id``, ``mode``, ``provider_fingerprint``), all
    genuinely agreeing with each other and with the payload, must
    succeed."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="answered",
            payload=frame_payload,
        )
        narrative_id = uuid.uuid4()
        payload, narrative_text, _fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=frame.frame_id,
            narrative_id=narrative_id,
            mode="deterministic_fallback",
        )
        narrative = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=frame.frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text=narrative_text,
            payload=payload,
        )
        await session.commit()
        narrative_pk = narrative.id

        new_text = "An updated, still-safe presentation summary."
        await session.execute(
            update(DevRunNarrative)
            .where(DevRunNarrative.id == narrative_pk)
            .values(
                payload=payload,
                narrative_text=new_text,
                narrative_id=narrative_id,
                run_id=run_id,
                frame_id=frame.frame_id,
                mode="deterministic_fallback",
                provider_fingerprint=None,
            )
        )
        await session.commit()

    async with maker() as session:
        reloaded = await session.get(DevRunNarrative, narrative_pk)
        assert reloaded is not None
        assert reloaded.narrative_text == new_text


@pytest.mark.asyncio
async def test_orm_boundary_known_unvalidated_gap_sinks_are_not_blocked(
    persistence,
) -> None:
    """The explicit exemption ledger must actually exempt: a payload that
    would fail every real contract still writes cleanly for a
    ``_KNOWN_UNVALIDATED_PAYLOAD_GAP`` model (``DevRunIntent``, filed
    under CHAOS-3330) -- this is the honest, filed gap, not a silent
    over-broad guard that happens to block everything."""

    assert _PAYLOAD_MODEL_VALIDATORS[DevRunIntent] is _ORM_BOUNDARY_KNOWN_GAP

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
            payload={"not_a_real_contract_field": "anything at all"},
        )
        assert record.id is not None


# -- CHAOS-3297 Codex review round 8: closure argument over the complete
# partition of session-mediated write paths (see the partition comment
# in persistence/service.py directly above `_enforce_payload_contract_at_flush`
# for the full a-f enumeration, verified empirically against SQLAlchemy
# 2.0.49). Round 5's AST scanner caught (a)'s Name-call form only; round 7
# closed (a) fully and (b); round 8 closes (c), (d), and (e), and proves
# (f) is a documented, deliberate non-goal rather than an unnoticed gap.


@pytest.mark.asyncio
async def test_orm_boundary_rejects_a_multi_values_insert_bypass(persistence) -> None:
    """Partition cell (c): ``insert(Model).values([row1, row2])`` -- a
    single multi-row INSERT statement. Confirmed empirically that this
    stores its row data in the statement's own ``_multi_values``, never
    ``_values`` (which is ``None`` here) -- the round-7 boundary only
    read ``_values`` and would have silently allowed this whole shape
    through."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        good_id, bad_id = uuid.uuid4(), uuid.uuid4()
        with pytest.raises(DevPersistenceValidationError):
            await session.execute(
                insert(DevAnswerFrame).values(
                    [
                        dict(
                            id=good_id,
                            run_id=run_id,
                            org_id=org_id,
                            user_id=user_id,
                            frame_id=uuid.UUID(valid_payload["frame_id"]),
                            public_outcome="not_found",
                            payload=valid_payload,
                            created_at=datetime.now(UTC),
                        ),
                        dict(
                            id=bad_id,
                            run_id=run_id,
                            org_id=org_id,
                            user_id=user_id,
                            frame_id=uuid.uuid4(),
                            public_outcome="answered",
                            payload={"schema_version": "dev_answer_frame.v1"},
                            created_at=datetime.now(UTC),
                        ),
                    ]
                )
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.run_id == run_id)
        )
        assert count == 0, (
            "a multi-values insert with even one invalid row must reject "
            "the whole statement -- neither row may persist"
        )


@pytest.mark.asyncio
async def test_orm_boundary_rejects_an_executemany_insert_bypass(persistence) -> None:
    """Partition cell (d): ``session.execute(insert(Model), [params, ...])``
    -- SQLAlchemy 2.0's executemany-style bulk insert. Confirmed
    empirically that both ``_values`` and ``_multi_values`` are empty
    here; the row dicts arrive as ``orm_execute_state.parameters``
    instead, keyed by column NAME rather than ``Column`` object."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        new_id = uuid.uuid4()
        with pytest.raises(DevPersistenceValidationError):
            await session.execute(
                insert(DevAnswerFrame),
                [
                    dict(
                        id=new_id,
                        run_id=run_id,
                        org_id=org_id,
                        user_id=user_id,
                        frame_id=uuid.uuid4(),
                        public_outcome="answered",
                        payload={"schema_version": "dev_answer_frame.v1"},
                        created_at=datetime.now(UTC),
                    )
                ],
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, new_id)
        assert reloaded is None, "the executemany-insert bypass must not create a row"


@pytest.mark.asyncio
async def test_orm_boundary_rejects_an_orm_bulk_update_by_primary_key_bypass(
    persistence,
) -> None:
    """Partition cell (d), the UPDATE half: ``session.execute(update(Model),
    [{"id": pk, ...}, ...])`` -- SQLAlchemy 2.0's ORM bulk
    update-by-primary-key. Also arrives via ``orm_execute_state.parameters``,
    and (same as the single-row bulk update) has no live ORM instance to
    hang the frame_id/run_id/public_outcome cross-check on -- omitting
    those columns is rejected on its own, same as omitting them entirely."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        with pytest.raises(DevPersistenceValidationError):
            await session.execute(
                update(DevAnswerFrame),
                [
                    {
                        "id": frame_id,
                        "payload": {"schema_version": "dev_answer_frame.v1"},
                    }
                ],
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "the ORM bulk update-by-primary-key bypass must leave the "
            "original, valid payload completely untouched"
        )


@pytest.mark.asyncio
async def test_orm_boundary_prohibits_legacy_bulk_save_objects(persistence) -> None:
    """Partition cell (e): ``Session.bulk_save_objects`` fires NEITHER
    the mapper events NOR ``do_orm_execute`` at all (confirmed
    empirically -- probe listeners on both never fire for it), so unlike
    (a)-(d) this cannot be validated at the ORM boundary, only refused
    outright. ``AsyncSession`` does not expose it directly, but it
    remains reachable via ``session.run_sync(...)`` against the
    underlying sync ``Session`` -- exactly how this is reproduced here,
    and exactly how Codex's round-8 repro reached it."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        row = DevAnswerFrame(
            run_id=run_id,
            org_id=org_id,
            user_id=user_id,
            frame_id=uuid.uuid4(),
            public_outcome="answered",
            payload={"schema_version": "dev_answer_frame.v1"},
        )

        def _do_bulk_save(sync_session: Any) -> None:
            sync_session.bulk_save_objects([row])

        with pytest.raises(DevPersistenceValidationError, match="bulk_save_objects"):
            await session.run_sync(_do_bulk_save)
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.run_id == run_id)
        )
        assert count == 0, "bulk_save_objects must not have created a row"


@pytest.mark.asyncio
async def test_orm_boundary_prohibits_legacy_bulk_insert_and_update_mappings(
    persistence,
) -> None:
    """Partition cell (e), the other two legacy bulk APIs:
    ``bulk_insert_mappings``/``bulk_update_mappings`` -- same
    unreachable-by-event shape as ``bulk_save_objects``, prohibited the
    same way."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        await session.commit()

        def _do_bulk_insert_mappings(sync_session: Any) -> None:
            sync_session.bulk_insert_mappings(
                DevAnswerFrame,
                [
                    dict(
                        id=uuid.uuid4(),
                        run_id=run_id,
                        org_id=org_id,
                        user_id=user_id,
                        frame_id=uuid.uuid4(),
                        public_outcome="answered",
                        payload={"schema_version": "dev_answer_frame.v1"},
                    )
                ],
            )

        with pytest.raises(DevPersistenceValidationError, match="bulk_insert_mappings"):
            await session.run_sync(_do_bulk_insert_mappings)
        await session.rollback()

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        def _do_bulk_update_mappings(sync_session: Any) -> None:
            sync_session.bulk_update_mappings(
                DevAnswerFrame,
                [
                    {
                        "id": frame_id,
                        "payload": {"schema_version": "dev_answer_frame.v1"},
                    }
                ],
            )

        with pytest.raises(DevPersistenceValidationError, match="bulk_update_mappings"):
            await session.run_sync(_do_bulk_update_mappings)
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "bulk_update_mappings must not have touched the original payload"
        )
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(
                DevAnswerFrame.run_id == run_id, DevAnswerFrame.id != frame_id
            )
        )
        assert count == 0, "bulk_insert_mappings must not have created a row"


@pytest.mark.asyncio
async def test_orm_boundary_permits_legacy_bulk_apis_for_a_non_payload_bearing_model(
    persistence,
) -> None:
    """Control: the legacy-bulk-API prohibition is scoped to
    payload-bearing models specifically, not bulk APIs on this Session in
    general."""

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)

        def _do_bulk_save(sync_session: Any) -> None:
            sync_session.bulk_save_objects(
                [
                    DevRunStageDiagnostic(
                        run_id=run_id,
                        org_id=org_id,
                        user_id=user_id,
                        ordinal=0,
                        stage_id="interpreting",
                        status="completed",
                        latency_ms=None,
                        counts={},
                    )
                ]
            )

        await session.run_sync(_do_bulk_save)
        await session.commit()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevRunStageDiagnostic.id)).where(
                DevRunStageDiagnostic.run_id == run_id
            )
        )
        assert count == 1


@pytest.mark.asyncio
async def test_orm_boundary_rejects_an_alien_payload_bound_to_the_wrong_row(
    persistence,
) -> None:
    """CHAOS-3297 Codex review round 8 HIGH: the boundary validated the
    payload in isolation but never bound it to the ROW. Codex's repro --
    ``update(DevRunNarrative).values(payload=<a fully valid
    dev_narrative.v1 payload for a DIFFERENT run/frame/narrative>,
    narrative_text=...)``, with narrative_id/run_id/frame_id all
    disagreeing with the target row -- must be rejected even though the
    payload alone passes ``DevNarrativeContract.model_validate`` on its
    own merits. It is caught here because those identity columns are
    absent from the write entirely (deny on an absent cross-check
    column); the round-7 boundary had no cross-check at all and would
    have accepted this verbatim.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="answered",
            payload=frame_payload,
        )
        narrative_id = uuid.uuid4()
        payload, narrative_text, _fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=frame.frame_id,
            narrative_id=narrative_id,
            mode="deterministic_fallback",
        )
        narrative = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=frame.frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text=narrative_text,
            payload=payload,
        )
        await session.commit()
        narrative_pk = narrative.id

        # A fully valid dev_narrative.v1 payload -- just for an entirely
        # different run/frame/narrative_id than the row being written to.
        alien_run_id, alien_frame_id, alien_narrative_id = (
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
        )
        alien_payload, alien_text, _alien_fp = _narrative_payload(
            run_id=alien_run_id,
            frame_id=alien_frame_id,
            narrative_id=alien_narrative_id,
            mode="deterministic_fallback",
        )
        DevNarrativeContract.model_validate({**alien_payload, "body": alien_text})

        with pytest.raises(DevPersistenceValidationError):
            await session.execute(
                update(DevRunNarrative)
                .where(DevRunNarrative.id == narrative_pk)
                .values(payload=alien_payload, narrative_text=alien_text)
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevRunNarrative, narrative_pk)
        assert reloaded is not None
        # Compare identity, not exact dict equality: the persisted payload
        # is the pydantic canonical dump (record_narrative round-trips
        # through DevNarrativeContract), which can format a field like
        # `generated_at` differently than this test's raw input dict did
        # (e.g. "Z" vs "+00:00") without that being a bug.
        assert reloaded.payload["narrative_id"] == str(narrative_id), (
            "the alien-payload bypass must leave the row's real payload "
            "completely untouched"
        )
        assert reloaded.payload["run_id"] == str(run_id)
        assert reloaded.payload["frame_id"] == str(frame.frame_id)
        assert reloaded.narrative_text == narrative_text


def test_orm_boundary_bulk_dml_denies_an_unrecognized_write_shape() -> None:
    """Partition-totality guard: if an ORM DML statement against a
    payload-bearing model carries its row data in NONE of ``_values``,
    ``_multi_values``, or ``parameters`` -- a shape this boundary was not
    written against, whether from a future SQLAlchemy version or
    anything else -- the write must be denied by default. The round-7
    boundary's equivalent function returned an empty dict for this case
    and the caller silently allowed the write through because no
    ``"payload"`` key was found in it; this is the same failure mode
    ``_KNOWN_UNVALIDATED_PAYLOAD_GAP`` exists to make explicit rather
    than silent, applied to "an unrecognized write shape" instead of "a
    known filed gap".
    """

    class _FakeStatement:
        _values = None
        _multi_values = ()

    class _FakeMapper:
        class_ = DevAnswerFrame

    class _FakeOrmExecuteState:
        is_update = True
        is_insert = False
        bind_mapper = _FakeMapper()
        statement = _FakeStatement()
        parameters = None

    with pytest.raises(DevPersistenceValidationError, match="unrecognized"):
        dev_persistence_service._enforce_payload_contract_on_bulk_dml(
            _FakeOrmExecuteState()
        )


@pytest.mark.asyncio
async def test_raw_connection_execution_bypasses_the_listener_but_not_the_trigger(
    persistence,
) -> None:
    """Partition cell (f), CHAOS-3297 Codex review round 9: Core
    execution against a ``Connection`` obtained via
    ``session.connection()``, entirely outside the Session, is confirmed
    empirically to fire neither the mapper events nor ``do_orm_execute``
    -- there is no Session-level event surface to hook for it at all,
    since it is not a session-mediated write in the first place. Round 8
    documented this as an out-of-scope non-goal for the (session-level)
    listener; the round-9 DB trigger closes it anyway, because it is not
    a session-level hook -- it validates every row on its way into the
    table, whatever wrote it. This is exactly what makes the DB trigger
    total where the listener could only ever be partial: a bypass this
    specific (raw connection, no Session at all) still cannot produce an
    invalid row.

    (``test_chaos_3297_frame_reachability.py``'s corrupted-frame tests
    still need to write genuinely out-of-band-invalid data through this
    same raw-connection path, to prove replay degrades safely -- they now
    drop the trigger first, which is the honest way to simulate a DBA-
    level bypass, not a reason to weaken the trigger itself.)
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        connection = await session.connection()
        with pytest.raises(IntegrityError):
            await connection.execute(
                update(DevAnswerFrame)
                .where(DevAnswerFrame.id == frame_id)
                .values(payload={"schema_version": "dev_answer_frame.v1"})
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "the raw-connection bypass attempt must leave the original, "
            "valid payload completely untouched"
        )


# -- CHAOS-3297 Codex review round 9: the DB trigger closure. Ship-gate on
# round 8 confirmed two more real bypasses of every session-level guard
# above -- a Core-table UPDATE issued through the Session (no `bind_mapper`
# to look up) and an `INSERT ... ON CONFLICT DO UPDATE` whose conflict SET
# clause is never inspected. Both are reproduced here verbatim, first with
# the session-level listeners genuinely disabled (via `event.remove`, not
# a monkeypatched module attribute -- the listeners were registered with
# direct references to the original functions at import time, so patching
# the module attribute would not affect them) to prove the DB trigger
# alone rejects both, then again with the listeners restored to prove
# normal operation is unaffected.


@contextmanager
def _session_level_payload_guards_disabled():
    """Temporarily unregisters every session-level payload guard
    (mapper events + do_orm_execute) so a test can prove the DB trigger
    -- not this layer -- is what rejects a given write. Always restores
    on exit, including when the body raises (e.g. inside
    ``pytest.raises``, which itself absorbs the write's own exception
    before this context manager's ``finally`` ever runs)."""

    at_flush = dev_persistence_service._enforce_payload_contract_at_flush
    on_bulk_dml = dev_persistence_service._enforce_payload_contract_on_bulk_dml
    payload_models = list(_PAYLOAD_MODEL_VALIDATORS)
    for model in payload_models:
        event.remove(model, "before_insert", at_flush)
        event.remove(model, "before_update", at_flush)
    event.remove(Session, "do_orm_execute", on_bulk_dml)
    try:
        yield
    finally:
        for model in payload_models:
            event.listen(model, "before_insert", at_flush)
            event.listen(model, "before_update", at_flush)
        event.listen(Session, "do_orm_execute", on_bulk_dml)


@pytest.mark.asyncio
async def test_db_trigger_rejects_a_core_table_update_with_no_bind_mapper(
    persistence,
) -> None:
    """Codex round 9 repro 1/2: ``session.execute(update(DevAnswerFrame
    .__table__)...)`` -- a Core-table UPDATE against the bare ``Table``,
    not the mapped class. ``orm_execute_state.bind_mapper`` is ``None``
    for this statement (there is no ORM mapper to resolve a bare
    ``Table`` against), so ``_enforce_payload_contract_on_bulk_dml``
    returns at its very first ``if mapper is None: return`` -- this is
    session-mediated, and it still defeats every session-level guard.

    Run with the session-level guards disabled first (proving the DB
    trigger alone rejects it), then with them restored (proving normal
    operation still works and this write is still rejected -- the
    listener's own early return means it was never going to help here
    either way).
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        with _session_level_payload_guards_disabled():
            with pytest.raises(IntegrityError, match="dev_answer_frames"):
                await session.execute(
                    update(cast(Table, DevAnswerFrame.__table__))
                    .where(DevAnswerFrame.__table__.c.id == frame_id)
                    .values(payload={"schema_version": "dev_answer_frame.v1"})
                )
            await session.rollback()

        async with maker() as check_session:
            reloaded = await check_session.get(DevAnswerFrame, frame_id)
            assert reloaded is not None
            assert reloaded.payload == valid_payload, (
                "trigger-alone: the bare-Table update must not have "
                "touched the original payload"
            )

        # Re-run with the guards restored -- normal operation, still
        # rejected (the trigger is unconditional either way).
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await session.execute(
                update(cast(Table, DevAnswerFrame.__table__))
                .where(DevAnswerFrame.__table__.c.id == frame_id)
                .values(payload={"schema_version": "dev_answer_frame.v1"})
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload


@pytest.mark.asyncio
async def test_db_trigger_rejects_an_on_conflict_do_update_set_clause(
    persistence,
) -> None:
    """Codex round 9 repro 2/2: ``insert(...).on_conflict_do_update(...,
    set_={"payload": ...})`` -- the INSERT's own values are whatever the
    session-level guard validates, but the conflict resolution's SET
    clause is a separate part of the compiled statement nothing in that
    layer inspects. Two variants, both against the SAME conflicting row:
    a malformed payload (fails contract validity) and a fully valid
    payload for a completely different run/frame (fails the row-binding
    cross-check) -- the trigger enforces both regardless of which
    session-level guard would or would not have caught either one.

    Run with the session-level guards disabled first (proving the DB
    trigger alone rejects both), then with them restored.
    """

    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        # The INSERT's own values target the SAME run_id as the existing
        # frame -- the unique constraint on run_id is exactly what makes
        # this an UPSERT against the row above, not a fresh insert. The
        # INSERT's own payload is fully valid (it would legitimately pass
        # every check on its own, including the row-binding cross-check,
        # since it matches run_id); only the ON CONFLICT SET clause,
        # which fires INSTEAD once the conflict is detected, is malicious.
        insert_payload = _frame_payload(run_id=run_id, outcome="not_found")

        alien_run_id = uuid.uuid4()
        alien_payload = _frame_payload(run_id=alien_run_id, outcome="answered")

        def _upsert_statement(conflict_payload: dict[str, Any]) -> Any:
            return (
                sqlite_insert(DevAnswerFrame)
                .values(
                    id=uuid.uuid4(),
                    run_id=run_id,
                    org_id=org_id,
                    user_id=user_id,
                    frame_id=uuid.UUID(insert_payload["frame_id"]),
                    public_outcome="not_found",
                    payload=insert_payload,
                    created_at=datetime.now(UTC),
                )
                .on_conflict_do_update(
                    index_elements=["run_id"],
                    set_={"payload": conflict_payload},
                )
            )

        with _session_level_payload_guards_disabled():
            for conflict_payload, label in (
                ({"schema_version": "dev_answer_frame.v1"}, "malformed"),
                (alien_payload, "alien-but-internally-valid"),
            ):
                with pytest.raises(IntegrityError, match="dev_answer_frames"):
                    await session.execute(_upsert_statement(conflict_payload))
                await session.rollback()
                async with maker() as check_session:
                    reloaded = await check_session.get(DevAnswerFrame, frame_id)
                    assert reloaded is not None
                    assert reloaded.payload == valid_payload, (
                        f"trigger-alone ({label}): the conflicting row's "
                        f"original payload must be untouched"
                    )

        # Re-run (guards restored) -- normal operation, still rejected.
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await session.execute(_upsert_statement(alien_payload))
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload
        # And the upsert's own INSERT branch did not sneak a second row
        # in either -- every attempted write in this test rolled back in
        # full, and run_id's unique constraint allows exactly one frame.
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.run_id == run_id)
        )
        assert count == 1


# -- CHAOS-3297 Codex review round 10 MEDIUM: SQLite's `json_extract`
# reads the FIRST occurrence of a duplicate JSON object key; Python's
# `json` decoder (what the application validates against) and Postgres's
# `->>` operator both keep the LAST -- confirmed empirically. A raw
# payload with a genuinely matching protected key first and a duplicate,
# mismatched copy of the SAME key after reads as row-matching to the
# (pre-round-10) SQLite trigger, which only ever inspected the first
# copy, while every other reader (the application, Postgres) sees the
# mismatched last copy and would reject it -- a real dialect divergence.
# These payloads can only be constructed via raw SQL (a Python dict has
# no duplicate keys, so nothing that goes through the ORM's own JSON
# encoder could ever produce one).


def _raw_duplicate_key_frame_payload(*, matching_frame_id: str, run_id: str) -> str:
    return (
        '{"schema_version": "dev_answer_frame.v1", '
        f'"frame_id": "{matching_frame_id}", '
        f'"run_id": "{run_id}", '
        '"public_outcome": "not_found", '
        '"frame_id": "11111111-1111-1111-1111-111111111111"}'
    )


def _raw_duplicate_key_narrative_payload(
    *, matching_narrative_id: str, run_id: str, frame_id: str, mode: str
) -> str:
    return (
        '{"schema_version": "dev_narrative.v1", '
        f'"narrative_id": "{matching_narrative_id}", '
        f'"run_id": "{run_id}", '
        f'"frame_id": "{frame_id}", '
        f'"mode": "{mode}", '
        '"referenced_fact_ids": [], "referenced_section_ids": [], '
        '"provider_metadata": null, '
        f'"generated_at": "{datetime.now(UTC).isoformat()}", '
        '"validation_warnings": [], '
        '"narrative_id": "11111111-1111-1111-1111-111111111111"}'
    )


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_duplicate_key_frame_insert(persistence) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")

        connection = await session.connection()
        raw_payload = _raw_duplicate_key_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        new_id = uuid.uuid4()
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await connection.execute(
                text(
                    "INSERT INTO dev_answer_frames "
                    "(id, run_id, org_id, user_id, frame_id, public_outcome, "
                    "payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :frame_id, :public_outcome, "
                    ":payload, :created_at)"
                ),
                {
                    "id": new_id.hex,
                    "run_id": run_id.hex,
                    "org_id": org_id.hex,
                    "user_id": user_id.hex,
                    "frame_id": uuid.UUID(valid_payload["frame_id"]).hex,
                    "public_outcome": "not_found",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC).isoformat(),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.id == new_id)
        )
        assert count == 0, "the duplicate-key insert must not have created a row"


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_duplicate_key_frame_update(persistence) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id_hex = frame.id.hex

        connection = await session.connection()
        raw_payload = _raw_duplicate_key_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await connection.execute(
                text("UPDATE dev_answer_frames SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": frame_id_hex},
            )
        await session.rollback()

    async with maker() as session:
        run = await session.scalar(select(DevRun).where(DevRun.id == run_id))
        assert run is not None
        reloaded = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
        )
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "the duplicate-key update must leave the original payload untouched"
        )


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_duplicate_key_narrative_insert(
    persistence,
) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="answered",
            payload=frame_payload,
        )
        await session.commit()

        narrative_id = uuid.uuid4()
        connection = await session.connection()
        raw_payload = _raw_duplicate_key_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        new_id = uuid.uuid4()
        with pytest.raises(IntegrityError, match="dev_run_narratives"):
            await connection.execute(
                text(
                    "INSERT INTO dev_run_narratives "
                    "(id, run_id, org_id, user_id, narrative_id, frame_id, mode, "
                    "provider_fingerprint, narrative_text, payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :narrative_id, :frame_id, :mode, "
                    ":provider_fingerprint, :narrative_text, :payload, :created_at)"
                ),
                {
                    "id": new_id.hex,
                    "run_id": run_id.hex,
                    "org_id": org_id.hex,
                    "user_id": user_id.hex,
                    "narrative_id": narrative_id.hex,
                    "frame_id": frame.frame_id.hex,
                    "mode": "deterministic_fallback",
                    "provider_fingerprint": None,
                    "narrative_text": "A safe presentation summary.",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC).isoformat(),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevRunNarrative.id)).where(DevRunNarrative.id == new_id)
        )
        assert count == 0, "the duplicate-key insert must not have created a row"


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_duplicate_key_narrative_update(
    persistence,
) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="answered",
            payload=frame_payload,
        )
        narrative_id = uuid.uuid4()
        payload, narrative_text, _fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=frame.frame_id,
            narrative_id=narrative_id,
            mode="deterministic_fallback",
        )
        narrative = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=frame.frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text=narrative_text,
            payload=payload,
        )
        await session.commit()
        narrative_pk_hex = narrative.id.hex

        connection = await session.connection()
        raw_payload = _raw_duplicate_key_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        with pytest.raises(IntegrityError, match="dev_run_narratives"):
            await connection.execute(
                text("UPDATE dev_run_narratives SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": narrative_pk_hex},
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.scalar(
            select(DevRunNarrative).where(DevRunNarrative.run_id == run_id)
        )
        assert reloaded is not None
        assert reloaded.narrative_text == narrative_text


# -- CHAOS-3297 Codex review round 11 HIGH: round 10's duplicate-key fix
# was itself incomplete. `json_extract`'s PATH matching (`'$.frame_id'`)
# truncates an object label at an embedded NUL (U+0000) the same way a C
# string does -- confirmed empirically: `json_extract('{"frame_id\x00XXXX":
# "a", "frame_id": "b"}', '$.frame_id')` returns `'a'`, reading the
# NUL-suffixed alias as if its label were exactly `frame_id`. Neither
# Python's `json` decoder nor Postgres's `->>` operator truncates there,
# so both read `'b'` -- the REAL exact key's value. A payload with a
# NUL-aliased protected key first, carrying a value that matches the row,
# and the real exact key after, carrying a mismatched value, passed both
# round-10 checks: the (then still json_extract-based) value cross-check
# read the aliased match, and the duplicate-key count correctly did NOT
# see two occurrences of the SAME key (`'frame_id\x00XXXX' != 'frame_id'`
# under exact equality -- these are genuinely different labels).
#
# These payloads can only be constructed via raw SQL (embedding a literal
# NUL byte inside a JSON object key is not something any contract encoder
# -- or Python's own json.dumps of a real dict -- would ever produce).


def _raw_nul_alias_frame_payload(*, matching_frame_id: str, run_id: str) -> str:
    return (
        '{"frame_id\\u0000XXXX": "' + matching_frame_id + '", '
        '"schema_version": "dev_answer_frame.v1", '
        f'"run_id": "{run_id}", '
        '"public_outcome": "not_found", '
        '"frame_id": "11111111-1111-1111-1111-111111111111"}'
    )


def _raw_nul_alias_narrative_payload(
    *, matching_narrative_id: str, run_id: str, frame_id: str, mode: str
) -> str:
    return (
        '{"narrative_id\\u0000XXXX": "' + matching_narrative_id + '", '
        '"schema_version": "dev_narrative.v1", '
        f'"run_id": "{run_id}", '
        f'"frame_id": "{frame_id}", '
        f'"mode": "{mode}", '
        '"referenced_fact_ids": [], "referenced_section_ids": [], '
        '"provider_metadata": null, '
        f'"generated_at": "{datetime.now(UTC).isoformat()}", '
        '"validation_warnings": [], '
        '"narrative_id": "11111111-1111-1111-1111-111111111111"}'
    )


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_nul_alias_frame_insert(persistence) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")

        connection = await session.connection()
        raw_payload = _raw_nul_alias_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        new_id = uuid.uuid4()
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await connection.execute(
                text(
                    "INSERT INTO dev_answer_frames "
                    "(id, run_id, org_id, user_id, frame_id, public_outcome, "
                    "payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :frame_id, :public_outcome, "
                    ":payload, :created_at)"
                ),
                {
                    "id": new_id.hex,
                    "run_id": run_id.hex,
                    "org_id": org_id.hex,
                    "user_id": user_id.hex,
                    "frame_id": uuid.UUID(valid_payload["frame_id"]).hex,
                    "public_outcome": "not_found",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC).isoformat(),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.id == new_id)
        )
        assert count == 0, "the NUL-alias insert must not have created a row"


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_nul_alias_frame_update(persistence) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id_hex = frame.id.hex

        connection = await session.connection()
        raw_payload = _raw_nul_alias_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await connection.execute(
                text("UPDATE dev_answer_frames SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": frame_id_hex},
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
        )
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "the NUL-alias update must leave the original payload untouched"
        )


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_nul_alias_narrative_insert(persistence) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="answered",
            payload=frame_payload,
        )
        await session.commit()

        narrative_id = uuid.uuid4()
        connection = await session.connection()
        raw_payload = _raw_nul_alias_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        new_id = uuid.uuid4()
        with pytest.raises(IntegrityError, match="dev_run_narratives"):
            await connection.execute(
                text(
                    "INSERT INTO dev_run_narratives "
                    "(id, run_id, org_id, user_id, narrative_id, frame_id, mode, "
                    "provider_fingerprint, narrative_text, payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :narrative_id, :frame_id, :mode, "
                    ":provider_fingerprint, :narrative_text, :payload, :created_at)"
                ),
                {
                    "id": new_id.hex,
                    "run_id": run_id.hex,
                    "org_id": org_id.hex,
                    "user_id": user_id.hex,
                    "narrative_id": narrative_id.hex,
                    "frame_id": frame.frame_id.hex,
                    "mode": "deterministic_fallback",
                    "provider_fingerprint": None,
                    "narrative_text": "A safe presentation summary.",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC).isoformat(),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevRunNarrative.id)).where(DevRunNarrative.id == new_id)
        )
        assert count == 0, "the NUL-alias insert must not have created a row"


@pytest.mark.asyncio
async def test_sqlite_trigger_rejects_nul_alias_narrative_update(persistence) -> None:
    maker, org_id, _other_org, user_id, _other_user = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        _conv_id, run_id = await _accepted_run(service, org_id=org_id, user_id=user_id)
        frame_payload = _frame_payload(run_id=run_id, outcome="answered")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="answered",
            payload=frame_payload,
        )
        narrative_id = uuid.uuid4()
        payload, narrative_text, _fingerprint = _narrative_payload(
            run_id=run_id,
            frame_id=frame.frame_id,
            narrative_id=narrative_id,
            mode="deterministic_fallback",
        )
        narrative = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=frame.frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text=narrative_text,
            payload=payload,
        )
        await session.commit()
        narrative_pk_hex = narrative.id.hex

        connection = await session.connection()
        raw_payload = _raw_nul_alias_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        with pytest.raises(IntegrityError, match="dev_run_narratives"):
            await connection.execute(
                text("UPDATE dev_run_narratives SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": narrative_pk_hex},
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.scalar(
            select(DevRunNarrative).where(DevRunNarrative.run_id == run_id)
        )
        assert reloaded is not None
        assert reloaded.narrative_text == narrative_text
