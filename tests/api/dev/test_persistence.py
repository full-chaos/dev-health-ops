from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, event, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.contracts import DevError, StreamEventType
from dev_health_ops.api.dev.orchestrator import OrchestratorResult, RunState
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.persistence import (
    DevAdmissionLimits,
    DevConcurrencyLimitExceeded,
    DevPersistenceNotFound,
    DevPersistenceService,
    DevPersistenceValidationError,
    DevRateLimitExceeded,
)
from dev_health_ops.api.dev.streaming import stream_orchestrator
from dev_health_ops.llm.agent.contracts import AgentUsage
from dev_health_ops.models.dev_persistence import (
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevToolCall,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

_TABLES = tables_of(
    User,
    Organization,
    DevConversation,
    DevMessage,
    DevRun,
    DevToolCall,
    DevFeedback,
    DevConversationTombstone,
)
_DIGEST = "sha256:" + ("a" * 64)


@dataclass
class Clock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


@pytest_asyncio.fixture
async def persistence(tmp_path: Path):
    database = tmp_path / "ask-dev-persistence.db"
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
                Organization(id=org_id, slug="ask-dev", name="Ask Dev"),
                Organization(id=other_org_id, slug="other", name="Other"),
                User(id=user_id, email="ask-dev@example.com"),
                User(id=other_user_id, email="other@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, other_org_id, user_id, other_user_id
    finally:
        await engine.dispose()


def _validated_answer(
    conversation_id: uuid.UUID, answer_id: uuid.UUID
) -> dict[str, Any]:
    return {
        "schema_version": "dev_answer.v1",
        "answer_id": str(answer_id),
        "conversation_id": str(conversation_id),
        "summary": "The evidence-backed answer can be rendered from storage.",
        "claims": [],
        "metrics": [],
        "evidence": [],
    }


def _identity_validator(payload: Any) -> Any:
    return payload


@pytest.mark.asyncio
async def test_retention_contract_is_exact_and_ephemeral_content_is_removed(
    persistence,
):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    clock = Clock(datetime(2026, 7, 28, 12, 0, tzinfo=UTC))
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        for unsupported in (7, 90):
            with pytest.raises(DevPersistenceValidationError):
                await service.create_conversation(
                    org_id=org_id,
                    user_id=user_id,
                    current_scope={},
                    retention_days=unsupported,
                )

        retained = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={"repositories": ["full-chaos/dev-health-ops"]},
            retention_days=30,
        )
        assert retained.expires_at == clock.value + timedelta(days=30)

        ephemeral = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
            retention_days=0,
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=ephemeral.id,
            client_message_id=uuid.uuid4(),
            question="Is this actually complete?",
            scope_snapshot={},
        )
        answer_id = uuid.uuid4()
        await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=ephemeral.id,
            answer_payload=_validated_answer(ephemeral.id, answer_id),
            validator=_identity_validator,
            scope_snapshot={},
        )
        result = await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted.run.id,
            state="completed",
            answer_id=answer_id,
            provider_source="platform",
            provider_fingerprint=_DIGEST,
            model_fingerprint=_DIGEST,
            prompt_version="dev-system.v1",
            tool_contract_version="dev-tools.v1",
            metric_version="metric-registry.v1",
            query_version="dev-query.v1",
        )
        assert result is None
        assert await session.get(DevConversation, ephemeral.id) is None
        tombstone = await session.scalar(
            select(DevConversationTombstone).where(
                DevConversationTombstone.conversation_id == ephemeral.id
            )
        )
        assert tombstone is not None
        assert tombstone.reason == "ephemeral_completed"
        assert tombstone.retention_days == 0
        assert await session.scalar(select(func.count()).select_from(DevMessage)) == 0
        assert await session.scalar(select(func.count()).select_from(DevRun)) == 0


@pytest.mark.asyncio
async def test_client_message_id_is_idempotent_for_message_and_run(persistence):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        client_message_id = uuid.uuid4()
        first = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=client_message_id,
            question="What remains?",
            scope_snapshot={},
        )
        replay = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=client_message_id,
            question="A retry must not replace the accepted question.",
            scope_snapshot={},
        )
        assert replay.created is False
        assert replay.message.id == first.message.id
        assert replay.run.id == first.run.id
        assert replay.message.content == "What remains?"
        assert await session.scalar(select(func.count()).select_from(DevMessage)) == 1
        assert await session.scalar(select(func.count()).select_from(DevRun)) == 1


@pytest.mark.asyncio
async def test_retry_creates_a_linked_run_without_mutating_history(persistence):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        original = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Original question",
            scope_snapshot={},
        )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                client_message_id=uuid.uuid4(),
                question="Too early",
                scope_snapshot={},
                retry_of_run_id=original.run.id,
            )
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=original.run.id,
            state="cancelled",
        )

        retry_client_id = uuid.uuid4()
        retry = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=retry_client_id,
            question="Original question",
            scope_snapshot={},
            retry_of_run_id=original.run.id,
        )
        assert retry.run.id != original.run.id
        assert retry.message.id != original.message.id
        assert retry.run.retry_of_run_id == original.run.id
        assert original.run.retry_of_run_id is None
        assert original.message.content == "Original question"

        replay = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=retry_client_id,
            question="Must not replace history",
            scope_snapshot={},
            retry_of_run_id=uuid.uuid4(),
        )
        assert replay.created is False
        assert replay.run.id == retry.run.id
        assert replay.message.content == "Original question"

        other = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        with pytest.raises(DevPersistenceNotFound):
            await service.append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=other.id,
                client_message_id=uuid.uuid4(),
                question="Cross-conversation retry",
                scope_snapshot={},
                retry_of_run_id=original.run.id,
            )


@pytest.mark.asyncio
async def test_transcript_is_safe_owned_retained_and_cursor_paginated(persistence):
    maker, org_id, other_org_id, user_id, other_user_id = persistence
    clock = Clock(datetime(2026, 7, 28, 12, 0, tzinfo=UTC))
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=30
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="What changed?",
            scope_snapshot={"direct_scope": "organization"},
        )
        clock.value += timedelta(seconds=1)
        answer_id = uuid.uuid4()
        answer = await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            answer_payload=_validated_answer(conversation.id, answer_id),
            validator=_identity_validator,
            scope_snapshot={},
            rendered_content="Rendered content must not be returned separately.",
        )
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted.run.id,
            state="completed",
            answer_id=answer_id,
        )

        first = await service.list_transcript_records(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            limit=1,
        )
        assert first.has_more is True
        assert [record.message.role for record in first.records] == ["user"]
        second = await service.list_transcript_records(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            limit=1,
            after=first.records[0].message.created_at,
            after_id=first.records[0].message.id,
        )
        assert second.has_more is False
        assert [record.message.id for record in second.records] == [answer.id]
        assert second.records[0].message.answer_payload is not None
        assert second.records[0].message.content == (
            "Rendered content must not be returned separately."
        )

        for wrong_org, wrong_user in (
            (other_org_id, user_id),
            (org_id, other_user_id),
        ):
            with pytest.raises(DevPersistenceNotFound):
                await service.list_transcript_records(
                    org_id=wrong_org,
                    user_id=wrong_user,
                    conversation_id=conversation.id,
                )

        ephemeral = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        with pytest.raises(DevPersistenceNotFound):
            await service.list_transcript_records(
                org_id=org_id,
                user_id=user_id,
                conversation_id=ephemeral.id,
            )


@pytest.mark.asyncio
async def test_stream_disconnect_persists_cancelled_terminal_state(persistence):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
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
            question="Will disconnect cancel this run?",
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
        cancellation = asyncio.Event()
        error = DevError(
            schema_version="dev_error.v1",
            request_id=str(accepted.run.request_id),
            code="cancelled",
            safe_message="The request was cancelled.",
            retryable=True,
        )

        async def run(_sink):
            await cancellation.wait()
            await recorder.terminal(
                state=RunState.CANCELLED,
                answer=None,
                error=error,
                usage=AgentUsage(),
                tool_call_count=0,
                provider_fingerprint=None,
                model_fingerprint=None,
                prompt_checksum=None,
            )
            return OrchestratorResult(
                run_id=str(accepted.run.id),
                state=RunState.CANCELLED,
                answer=None,
                error=error,
                events=(),
                usage=AgentUsage(),
                tool_call_count=0,
                provider_fingerprint=None,
                model_fingerprint=None,
            )

        stream = stream_orchestrator(
            run_id=str(accepted.run.id),
            run_with_events=run,
            cancellation=cancellation,
        )
        assert (await anext(stream)).event is StreamEventType.RUN_STARTED
        await stream.aclose()
        await session.refresh(accepted.run)
        assert cancellation.is_set()
        assert accepted.run.state == "cancelled"
        assert accepted.run.safe_error_code == "cancelled"


@pytest.mark.asyncio
async def test_submission_admission_enforces_concurrency_rate_and_replay(
    persistence,
):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    clock = Clock(datetime(2026, 7, 28, 12, 0, tzinfo=UTC))
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
        )
        client_message_id = uuid.uuid4()
        limits = DevAdmissionLimits(
            requests_per_user_per_15_minutes=1,
            requests_per_org_per_hour=1,
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=client_message_id,
            question="What changed?",
            scope_snapshot={},
            admission_limits=limits,
        )

        replay = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=client_message_id,
            question="What changed?",
            scope_snapshot={},
            admission_limits=limits,
        )
        assert replay.created is False
        assert replay.run.id == accepted.run.id

        with pytest.raises(DevConcurrencyLimitExceeded):
            await service.append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                client_message_id=uuid.uuid4(),
                question="What remains?",
                scope_snapshot={},
                admission_limits=limits,
            )

        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted.run.id,
            state="cancelled",
        )
        with pytest.raises(DevRateLimitExceeded):
            await service.append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                client_message_id=uuid.uuid4(),
                question="What remains?",
                scope_snapshot={},
                admission_limits=limits,
            )


@pytest.mark.asyncio
async def test_conversation_listing_uses_the_id_tie_breaker_for_cursor_pages(
    persistence,
):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    clock = Clock(datetime(2026, 7, 28, 12, 0, tzinfo=UTC))
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        first = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
            title="first",
        )
        second = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
            title="second",
        )
        first_page = await service.list_conversation_records(
            org_id=org_id,
            user_id=user_id,
            limit=1,
        )
        assert len(first_page) == 1

        second_page = await service.list_conversation_records(
            org_id=org_id,
            user_id=user_id,
            limit=1,
            before=first_page[0].conversation.updated_at,
            before_id=first_page[0].conversation.id,
        )
        assert len(second_page) == 1

        seen = {first_page[0].conversation.id, second_page[0].conversation.id}
        assert seen == {first.id, second.id}
        assert (
            first_page[0].conversation.updated_at
            == second_page[0].conversation.updated_at
        )


@pytest.mark.asyncio
async def test_every_service_read_and_write_is_tenant_and_user_scoped(persistence):
    maker, org_id, other_org_id, user_id, other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        for wrong_org, wrong_user in (
            (other_org_id, user_id),
            (org_id, other_user_id),
            (other_org_id, other_user_id),
        ):
            with pytest.raises(DevPersistenceNotFound):
                await service.get_conversation(
                    org_id=wrong_org,
                    user_id=wrong_user,
                    conversation_id=conversation.id,
                )
            with pytest.raises(DevPersistenceNotFound):
                await service.append_user_message_and_run(
                    org_id=wrong_org,
                    user_id=wrong_user,
                    conversation_id=conversation.id,
                    client_message_id=uuid.uuid4(),
                    question="Cross-tenant attempt",
                    scope_snapshot={},
                )
        assert (
            await service.admin_purge_conversation(
                org_id=other_org_id,
                target_user_id=user_id,
                actor_user_id=other_user_id,
                conversation_id=conversation.id,
            )
            is False
        )
        assert await service.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation.id
        )


@pytest.mark.asyncio
async def test_validated_answer_feedback_and_safe_tool_metadata_round_trip(persistence):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
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
            question="What changed?",
            scope_snapshot={},
        )
        answer_id = uuid.uuid4()
        payload = _validated_answer(conversation.id, answer_id)
        answer = await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            answer_payload=payload,
            validator=_identity_validator,
            scope_snapshot={},
        )
        assert answer.answer_payload == payload
        replayed_answer = await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            answer_payload={**payload, "summary": "retry must not replace the answer"},
            validator=_identity_validator,
            scope_snapshot={},
        )
        assert replayed_answer.id == answer.id
        assert replayed_answer.answer_payload == payload
        tool = await service.append_tool_call(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted.run.id,
            ordinal=0,
            tool_id="query_metric.v1",
            tool_version="v1",
            canonical_input_hash=_DIGEST,
            safe_scope_summary={"repository_count": 1},
            status="completed",
            result_digest=_DIGEST,
            evidence_ref_ids=["evidence:opaque-1"],
            item_count=1,
            byte_count=128,
        )
        assert tool.result_digest == _DIGEST
        first = await service.record_feedback(
            org_id=org_id,
            user_id=user_id,
            answer_id=answer_id,
            rating="helpful",
            reasons=["useful"],
        )
        updated = await service.record_feedback(
            org_id=org_id,
            user_id=user_id,
            answer_id=answer_id,
            rating="not_helpful",
            reasons=["missing_evidence", "missing_evidence"],
            comment="The relevant CI evidence is missing.",
        )
        assert updated.id == first.id
        assert updated.reasons == ["missing_evidence"]
        assert await session.scalar(select(func.count()).select_from(DevFeedback)) == 1


@pytest.mark.asyncio
async def test_deletion_expiry_and_cleanup_are_bounded_idempotent_and_content_free(
    persistence,
):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    clock = Clock(datetime(2026, 7, 28, 12, 0, tzinfo=UTC))
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        deleted = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
            title="Delete me",
        )
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=deleted.id,
            client_message_id=uuid.uuid4(),
            question="Sensitive question content",
            scope_snapshot={},
        )
        assert await service.delete_conversation(
            org_id=org_id, user_id=user_id, conversation_id=deleted.id
        )
        assert not await service.delete_conversation(
            org_id=org_id, user_id=user_id, conversation_id=deleted.id
        )

        expired = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=30
        )
        clock.value += timedelta(days=30)
        result = await service.cleanup_expired(limit=1)
        assert result.selected == result.purged == 1
        assert await session.get(DevConversation, expired.id) is None
        assert (await service.cleanup_expired(limit=1)).purged == 0

        tombstones = (
            (
                await session.execute(
                    select(DevConversationTombstone).order_by(
                        DevConversationTombstone.deleted_at
                    )
                )
            )
            .scalars()
            .all()
        )
        assert {row.reason for row in tombstones} == {
            "user_deleted",
            "retention_expired",
        }
        serialized = json.dumps(
            [
                {
                    "conversation_id": str(row.conversation_id),
                    "reason": row.reason,
                    "retention_days": row.retention_days,
                }
                for row in tombstones
            ]
        )
        assert "Sensitive question content" not in serialized


@pytest.mark.asyncio
async def test_data_minimization_rejects_prohibited_metadata_and_unvalidated_answers(
    persistence,
):
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        with pytest.raises(DevPersistenceValidationError):
            await service.create_conversation(
                org_id=org_id,
                user_id=user_id,
                current_scope={"api_key": "must-not-persist"},
            )
        with pytest.raises(DevPersistenceValidationError):
            await service.create_conversation(
                org_id=org_id,
                user_id=user_id,
                current_scope={
                    "nested": [
                        {
                            "authorization": "Bearer must-not-persist",
                        }
                    ]
                },
            )
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_assistant_answer(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                answer_payload=_validated_answer(conversation.id, uuid.uuid4()),
                validator=lambda _payload: (_ for _ in ()).throw(ValueError("bad")),
                scope_snapshot={},
            )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Safe metadata only?",
            scope_snapshot={},
        )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_tool_call(
                org_id=org_id,
                user_id=user_id,
                run_id=accepted.run.id,
                ordinal=0,
                tool_id="query_metric.v1",
                tool_version="v1",
                canonical_input_hash=_DIGEST,
                safe_scope_summary={"innocent_key": "raw source content"},
                status="completed",
            )
        with pytest.raises(DevPersistenceValidationError):
            await service.append_assistant_answer(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                answer_payload={
                    **_validated_answer(conversation.id, uuid.uuid4()),
                    "provider_response": {"raw": "must-not-persist"},
                },
                validator=_identity_validator,
                scope_snapshot={},
            )


@pytest.mark.asyncio
async def test_user_and_organization_deletion_cascade_all_conversation_content(
    persistence,
):
    maker, org_id, other_org_id, user_id, other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        user_conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=user_conversation.id,
            client_message_id=uuid.uuid4(),
            question="Purge with the user",
            scope_snapshot={},
        )
        await session.execute(delete(User).where(User.id == user_id))
        await session.flush()
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DevConversation)
                .where(DevConversation.id == user_conversation.id)
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(DevMessage)) == 0
        assert await session.scalar(select(func.count()).select_from(DevRun)) == 0

        org_conversation = await service.create_conversation(
            org_id=other_org_id, user_id=other_user_id, current_scope={}
        )
        await service.append_user_message_and_run(
            org_id=other_org_id,
            user_id=other_user_id,
            conversation_id=org_conversation.id,
            client_message_id=uuid.uuid4(),
            question="Purge with the organization",
            scope_snapshot={},
        )
        await session.execute(
            delete(Organization).where(Organization.id == other_org_id)
        )
        await session.flush()
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DevConversation)
                .where(DevConversation.id == org_conversation.id)
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(DevMessage)) == 0
        assert await session.scalar(select(func.count()).select_from(DevRun)) == 0
