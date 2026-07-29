from __future__ import annotations

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

from dev_health_ops.api.dev.persistence import (
    DevPersistenceNotFound,
    DevPersistenceService,
    DevPersistenceValidationError,
)
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
