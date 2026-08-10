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
async def test_update_run_survives_a_failing_immediate_purge(
    persistence, monkeypatch: pytest.MonkeyPatch
):
    """Team-lead binding constraint, fault-injected directly against the
    ONE place CHAOS-3404 attempts an immediate synchronous purge at all
    (update_run -- the live/success path, safe because nothing reads this
    run's content back from the DB afterward; the orchestrator already has
    it in memory for the live SSE stream). Forces ``_purge_conversation``
    to raise and proves BOTH halves of the durability contract:

    1. The terminal-state write is NOT gated on the purge succeeding --
       ``update_run`` returns normally (the caller's own commit persists
       state + the expires_at stamp), never raising the purge's exception.
       This is what ``_try_purge_ephemeral_conversation``'s SAVEPOINT
       (``begin_nested``) buys: a failed purge rolls back only itself, not
       update_run's shared, caller-owned outer transaction.
    2. Nothing "retries the purge" itself -- the stamped expires_at makes
       the row visible to cleanup_expired, and a real, unmocked sweep call
       collects it later, exactly like a 30-day row: "purged within one
       sweep interval," never "leaked forever."
    """
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        ephemeral = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=ephemeral.id,
            client_message_id=uuid.uuid4(),
            question="Does the purge itself fail on an otherwise-successful run?",
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

        async def _boom(self, *, conversation, reason, actor_user_id):
            raise RuntimeError("simulated purge failure: connection dropped again")

        monkeypatch.setattr(DevPersistenceService, "_purge_conversation", _boom)

        # Must not raise: the terminal write is the durability guarantee,
        # independent of the purge outcome.
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
        # Purge failed -- update_run returns the run (not None, unlike the
        # successful-purge case), since nothing was actually deleted.
        assert result is not None
        assert result.state == "completed"
        await session.commit()

    async with maker() as session2:
        conversation = await session2.get(DevConversation, ephemeral.id)
        assert conversation is not None  # purge failed -- row still exists...
        assert conversation.expires_at is not None
        # SQLite (test harness only) round-trips a naive datetime even
        # though it was written aware; normalize before comparing.
        expires_at = conversation.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        assert expires_at <= datetime.now(UTC)  # ...but is now due

    # Undo the fault injection explicitly -- monkeypatch only auto-reverts
    # at test teardown, and this step must prove a REAL, unmocked sweep
    # (not one still silently patched to fail) collects the row.
    monkeypatch.undo()
    async with maker() as session3:
        sweep_service = DevPersistenceService(session3)
        sweep_result = await sweep_service.cleanup_expired(limit=10)
        await session3.commit()
    assert sweep_result.purged == 1
    async with maker() as session4:
        assert await session4.get(DevConversation, ephemeral.id) is None
        tombstone = await session4.scalar(
            select(DevConversationTombstone).where(
                DevConversationTombstone.conversation_id == ephemeral.id
            )
        )
        assert tombstone is not None
        assert tombstone.reason == "ephemeral_completed"


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
async def test_feedback_accepts_the_additive_reasons_and_rejects_mixed_unspecified(
    persistence,
):
    """CHAOS-3660 §8(f)/(j). ``_FEEDBACK_REASONS`` is one of three
    independent copies of this vocabulary (contracts.DevFeedback, router's
    request model, this persistence-layer allowlist) -- this proves the
    NEW reasons actually made it through to the layer that would silently
    reject them if only the wire contract had been widened.
    """
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="What changed?",
            scope_snapshot={},
        )
        answer_id = uuid.uuid4()
        payload = _validated_answer(conversation.id, answer_id)
        await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            answer_payload=payload,
            validator=_identity_validator,
            scope_snapshot={},
        )

        additive = await service.record_feedback(
            org_id=org_id,
            user_id=user_id,
            answer_id=answer_id,
            rating="not_helpful",
            reasons=["wrong_subject", "wrong_cohort", "wrong_driver"],
        )
        assert sorted(additive.reasons) == [
            "wrong_cohort",
            "wrong_driver",
            "wrong_subject",
        ]

        alone = await service.record_feedback(
            org_id=org_id,
            user_id=user_id,
            answer_id=answer_id,
            rating="not_helpful",
            reasons=["unspecified"],
        )
        assert alone.reasons == ["unspecified"]

        with pytest.raises(DevPersistenceValidationError, match="stand alone"):
            await service.record_feedback(
                org_id=org_id,
                user_id=user_id,
                answer_id=answer_id,
                rating="not_helpful",
                reasons=["unclear", "unspecified"],
            )


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
async def test_force_terminal_fallback_stamps_expiry_for_the_sweep_to_collect(
    persistence,
):
    """CHAOS-3404: force_terminal_fallback is the documented last-resort
    recovery path for a dropped-connection/DB failure mid-finish() (CHAOS-3297
    round 3) -- it used to set a run terminal without ever checking
    retention_days == 0, leaving that conversation permanently unpurgeable
    (expires_at is never set for 0-day rows, so cleanup_expired's async
    sweep couldn't catch it either -- nothing could).

    Fixed: stamps ``expires_at = now()`` on the conversation as part of the
    SAME commit as the terminal-state write, and deliberately does NOT
    attempt an immediate synchronous purge. Two independent races rule that
    out (Codex adversarial-review, both confirmed):

    * round 2 -- a same-request reader: recover_stale_non_terminal_run's
      RETURN VALUE is read back immediately by its own caller to build a
      replay response (see that method's own test).
    * round 3 -- a CONCURRENT reader: a duplicate in-flight request can
      already be blocked in recover_stale_non_terminal_run's
      ``SELECT ... FOR UPDATE`` on this exact run, waiting on force_
      terminal_fallback's commit to release the lock, then reading this
      run's answer/frame content back the instant it does. An immediate
      purge here would delete that content out from under that concurrent
      reader too -- so this method never attempts one at all, matching
      recover_stale_non_terminal_run exactly, even though (unlike that
      method) IT has no same-request reader of its own.

    This proves both halves: the conversation is intact immediately after
    (so any reader, same-request or concurrent, still sees real content)
    but is now immediately due, and a real cleanup_expired sweep call
    collects it.
    """
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        ephemeral = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=ephemeral.id,
            client_message_id=uuid.uuid4(),
            question="Did the connection drop mid-answer?",
            scope_snapshot={},
        )
        await service.force_terminal_fallback(
            org_id=org_id, user_id=user_id, run_id=accepted.run.id
        )
        # Intact immediately after -- never raced by an inline purge.
        conversation = await session.get(DevConversation, ephemeral.id)
        assert conversation is not None
        assert conversation.expires_at is not None
        assert conversation.expires_at <= datetime.now(UTC)
        run = await session.get(DevRun, accepted.run.id)
        assert run is not None
        assert run.state == "failed"

    # The real safety-net sweep collects it.
    async with maker() as session2:
        sweep_service = DevPersistenceService(session2)
        result = await sweep_service.cleanup_expired(limit=10)
        await session2.commit()
    assert result.purged == 1
    async with maker() as session3:
        assert await session3.get(DevConversation, ephemeral.id) is None
        tombstone = await session3.scalar(
            select(DevConversationTombstone).where(
                DevConversationTombstone.conversation_id == ephemeral.id
            )
        )
        assert tombstone is not None
        assert tombstone.reason == "ephemeral_completed"


@pytest.mark.asyncio
async def test_force_terminal_fallback_leaves_a_30_day_conversation_alone(persistence):
    """Regression guard: the new purge check must not touch non-ephemeral
    conversations -- force_terminal_fallback's forced-failed run stays
    exactly as it was before CHAOS-3404 for the 30-day case."""
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=30
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Regression guard for the 30-day path.",
            scope_snapshot={},
        )
        await service.force_terminal_fallback(
            org_id=org_id, user_id=user_id, run_id=accepted.run.id
        )
        assert await session.get(DevConversation, conversation.id) is not None
        run = await session.get(DevRun, accepted.run.id)
        assert run is not None
        assert run.state == "failed"


@pytest.mark.asyncio
async def test_recover_stale_non_terminal_run_stamps_expiry_for_the_sweep_to_collect(
    persistence,
):
    """Same gap, the other documented recovery path (CHAOS-3297 rounds 5/7):
    a stuck non-terminal run recovered on replay must also honor 0-day
    retention when it forces the run terminal -- and, same as
    force_terminal_fallback, via a stamp-then-sweep contract rather than an
    inline purge, because THIS method's return value is immediately read
    back by its caller (router.py's replay path) to build a response from
    the run's answer/frame -- an inline purge would delete that content out
    from under the very read that needs it (Codex adversarial-review
    round 2, MEDIUM, confirmed).
    """
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    clock = Clock(datetime(2026, 7, 28, 12, 0, tzinfo=UTC))
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        ephemeral = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=ephemeral.id,
            client_message_id=uuid.uuid4(),
            question="Stuck non-terminal, recovered on replay.",
            scope_snapshot={},
        )
        clock.value += timedelta(hours=1)
        recovered = await service.recover_stale_non_terminal_run(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted.run.id,
            stale_after=timedelta(minutes=1),
        )
        assert recovered is not None
        assert recovered.state == "failed"
        # Intact immediately after -- the router's own subsequent
        # get_answer_message/get_answer_frame read (keyed off `recovered`)
        # still sees a real, undeleted conversation/run.
        conversation = await session.get(DevConversation, ephemeral.id)
        assert conversation is not None
        assert conversation.expires_at is not None
        assert conversation.expires_at <= clock.value

    # The real safety-net sweep collects it.
    async with maker() as session2:
        sweep_service = DevPersistenceService(session2, now=clock)
        result = await sweep_service.cleanup_expired(limit=10)
        await session2.commit()
    assert result.purged == 1
    async with maker() as session3:
        assert await session3.get(DevConversation, ephemeral.id) is None
        tombstone = await session3.scalar(
            select(DevConversationTombstone).where(
                DevConversationTombstone.conversation_id == ephemeral.id
            )
        )
        assert tombstone is not None
        assert tombstone.reason == "ephemeral_completed"


@pytest.mark.asyncio
async def test_backfill_stranded_ephemeral_expiry_repairs_pre_fix_rows(persistence):
    """Codex adversarial-review round 3 (CHAOS-3404, HIGH, confirmed): the
    synchronous-stamp fix only stops NEW 0-day rows from being stranded --
    it does nothing for rows already left with every run terminal and
    expires_at=NULL by the pre-fix force_terminal_fallback/
    recover_stale_non_terminal_run in production before this deploys.
    Simulates exactly that pre-fix state directly (a raw mutation, bypassing
    update_run/force_terminal_fallback entirely -- those all stamp now, so
    this is the only way to construct the stranded shape a backfill needs to
    repair), then proves backfill_stranded_ephemeral_expiry stamps it and the
    ordinary sweep collects it.

    CHAOS-3544 REWROTE TWO ASSERTIONS HERE, deliberately and under an
    explicit ruling. This test used to assert that a run-less 0-day
    conversation and one with a non-terminal run were left untouched, and
    that the run-less one still EXISTED after a sweep. Read today that looks
    like a guarantee being deleted; it is not. Those assertions encoded
    "nothing to retire YET" under the old model, where the only stamp was
    run-terminal and a creation stamp did not exist. They were never a
    decision that a 0-day conversation should be kept forever -- which is
    exactly what they had come to certify, since neither row could ever be
    stamped by anything.

    Both are now stamped at creation (graced), so the shapes this test
    constructs must set expires_at=None explicitly to simulate a pre-fix row
    at all, and the backfill's own selection is by age rather than run state.
    """
    maker, org_id, _other_org_id, user_id, _other_user_id = persistence
    async with maker() as session:
        service = DevPersistenceService(session)

        stranded = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        stranded_accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=stranded.id,
            client_message_id=uuid.uuid4(),
            question="Stranded by the pre-fix fallback path.",
            scope_snapshot={},
        )
        # Raw mutation -- simulates the pre-fix force_terminal_fallback
        # shape directly: terminal run, expires_at never stamped. Since
        # CHAOS-3544 stamps at creation, the stamp has to be cleared here too
        # or this is no longer a pre-fix row.
        stranded_accepted.run.state = "failed"
        stranded.expires_at = None
        # ...and aged past the grace. A row genuinely stranded in production
        # by the pre-fix code has been sitting there since before the deploy;
        # a row created moments ago is indistinguishable from one whose turn
        # is still starting, which is exactly what the age predicate refuses
        # to touch.
        stranded.created_at = datetime.now(UTC) - timedelta(days=2)
        # ...and untouched since. The repair keys on updated_at, because
        # "idle for a full grace" is the real condition -- a row someone
        # resumed minutes ago is not stranded no matter how old it is.
        stranded.updated_at = datetime.now(UTC) - timedelta(days=2)
        await session.flush()

        still_in_flight = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=still_in_flight.id,
            client_message_id=uuid.uuid4(),
            question="Genuinely still running -- must not be touched.",
            scope_snapshot={},
        )

        run_less = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await session.commit()

    async with maker() as session2:
        service2 = DevPersistenceService(session2)
        stamped = await service2.backfill_stranded_ephemeral_expiry(limit=10)
        # CHAOS-3441 Codex adversarial review round 3 (MEDIUM, confirmed):
        # the stamp must be flushed before returning, not left dirty for
        # whatever this session does next. Unflushed state is emitted by the
        # NEXT operation's savepoint entry -- before the SAVEPOINT is
        # emitted, so outside it -- where a failure poisons the whole
        # transaction and takes unrelated already-flushed rows with it.
        assert not session2.dirty, session2.dirty
        await session2.commit()
    assert stamped == 1

    async with maker() as session3:
        stranded_row = await session3.get(DevConversation, stranded.id)
        assert stranded_row is not None
        assert stranded_row.expires_at is not None
        expires_at = stranded_row.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        assert expires_at <= datetime.now(UTC)

        # CHAOS-3544: these two used to be asserted as expires_at IS NULL --
        # i.e. as permanently unpurgeable, which is the defect. Both are now
        # stamped at CREATION (graced), so they carry a real expiry and will
        # be collected once it elapses. What must still hold is that the
        # backfill did not touch them: their stamp is the creation one, still
        # in the future, not a backfill stamp of `now`.
        in_flight_expiry = (
            await session3.get(DevConversation, still_in_flight.id)
        ).expires_at
        run_less_expiry = (await session3.get(DevConversation, run_less.id)).expires_at
        for expiry in (in_flight_expiry, run_less_expiry):
            assert expiry is not None, (
                "a 0-day conversation must carry an expiry from creation -- "
                "a NULL here is the CHAOS-3544 retained-forever shape"
            )
            # sqlite hands back naive datetimes; normalise before comparing.
            aware = expiry if expiry.tzinfo is not None else expiry.replace(tzinfo=UTC)
            assert aware > datetime.now(UTC), (
                "and it must still be the graced creation stamp, not a "
                "backfill stamp of now: neither row is old enough to repair"
            )

    # Idempotent: a second call finds nothing left to stamp.
    async with maker() as session4:
        service4 = DevPersistenceService(session4)
        assert await service4.backfill_stranded_ephemeral_expiry(limit=10) == 0

    # The ordinary sweep now collects the repaired row.
    async with maker() as session5:
        sweep_service = DevPersistenceService(session5)
        result = await sweep_service.cleanup_expired(limit=10)
        await session5.commit()
    assert result.purged == 1
    async with maker() as session6:
        assert await session6.get(DevConversation, stranded.id) is None
        # Still present: its graced creation stamp has not elapsed, so the
        # sweep has nothing to collect yet. Previously this row was retained
        # forever; now it is retained until its expiry, which is the point.
        assert await session6.get(DevConversation, still_in_flight.id) is not None
        assert await session6.get(DevConversation, run_less.id) is not None


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
