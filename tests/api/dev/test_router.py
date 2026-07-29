from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import router as dev_router_module
from dev_health_ops.api.dev.persistence import DevPersistenceService
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing import FeatureDecisionReason
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


@dataclass
class DevApiContext:
    app: FastAPI
    client: AsyncClient
    maker: async_sessionmaker[AsyncSession]
    org_id: uuid.UUID
    user_id: uuid.UUID


def _scope_payload(org_id: uuid.UUID) -> dict[str, object]:
    return {
        "schema_version": "dev_scope.v1",
        "organization_id": str(org_id),
        "direct_scope": "organization",
        "repositories": [],
        "entity_refs": [],
        "team_ids": [],
        "time_range": {
            "start": "2026-07-28T12:00:00+00:00",
            "end": "2026-07-28T13:00:00+00:00",
            "timezone": "UTC",
        },
    }


@pytest_asyncio.fixture
async def dev_api_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    database = tmp_path / "ask-dev-router.db"
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
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ask-dev", name="Ask Dev"),
                User(id=user_id, email="ask-dev@example.com"),
            ]
        )
        await session.commit()

    async def _allow_entitlement(self, _org_id: str) -> None:
        return None

    monkeypatch.setattr(
        dev_router_module.CanonicalAskDevEntitlementAuthorizer,
        "require",
        _allow_entitlement,
    )

    async def _feature_allowed(_session, _org_id, key: str) -> bool:
        return key == "ask_dev"

    monkeypatch.setattr(dev_router_module, "_feature_allowed", _feature_allowed)

    app = FastAPI()
    app.include_router(dev_router_module.router)
    app.add_exception_handler(
        dev_router_module.AskDevApiError,
        dev_router_module.ask_dev_error_handler,
    )
    app.add_exception_handler(
        RequestValidationError,
        dev_router_module.ask_dev_validation_error_handler,
    )

    async def _session_override():
        async with maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[dev_router_module._authenticated_user] = lambda: (
        AuthenticatedUser(
            user_id=str(user_id),
            email="ask-dev@example.com",
            org_id=str(org_id),
            role="member",
        )
    )
    app.dependency_overrides[dev_router_module.get_postgres_session_dep] = (
        _session_override
    )
    app.dependency_overrides[dev_router_module.get_dev_capability_runtime] = lambda: (
        dev_router_module.DevCapabilityRuntime(
            effective_provider_label="OpenAI compatible",
            effective_model_label="Certified model",
            provider_source="platform",
            readiness="ready",
            contextual_entrypoints=True,
            evidence_resolver=True,
            safe_failure_reason=None,
        )
    )

    transport = ASGITransport(app=app, raise_app_exceptions=True)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        try:
            yield DevApiContext(
                app=app,
                client=client,
                maker=maker,
                org_id=org_id,
                user_id=user_id,
            )
        finally:
            app.dependency_overrides.clear()
            await engine.dispose()


@pytest.mark.asyncio
async def test_dev_capabilities_and_conversation_lifecycle(dev_api_context):
    client = dev_api_context.client
    org_id = dev_api_context.org_id

    capabilities = await client.get("/api/v1/dev/capabilities")
    assert capabilities.status_code == 200
    capability_payload = capabilities.json()
    assert capability_payload["schema_version"] == "dev_capabilities.v1"
    assert capability_payload["ask_dev"] is True
    assert capability_payload["can_read"] is True
    assert capability_payload["readiness"] == "ready"
    assert capability_payload["provider_source"] == "platform"
    assert capability_payload["retention_options"] == [0, 30]
    assert capability_payload["request_limits"]["model_decision_rounds"] == 4
    assert "registered_statistics" in capability_payload["supported_question_classes"]

    create_response = await client.post(
        "/api/v1/dev/conversations",
        json={
            "current_scope": _scope_payload(org_id),
            "retention_days": 30,
            "title": "Wave 2 integration",
        },
    )
    assert create_response.status_code == 201
    conversation = create_response.json()
    assert conversation["schema_version"] == "dev_conversation.v1"
    assert conversation["title"] == "Wave 2 integration"
    assert conversation["current_scope"]["organization_id"] == str(org_id)
    assert conversation["message_count"] == 0
    assert conversation["latest_answer_id"] is None
    conversation_id = conversation["conversation_id"]

    list_response = await client.get("/api/v1/dev/conversations")
    assert list_response.status_code == 200
    list_payload = list_response.json()
    summaries = list_payload["items"]
    assert len(summaries) == 1
    assert summaries[0]["schema_version"] == "dev_conversation_summary.v1"
    assert summaries[0]["conversation_id"] == conversation_id
    assert summaries[0]["direct_scope"] == "organization"
    assert summaries[0]["message_count"] == 0

    get_response = await client.get(f"/api/v1/dev/conversations/{conversation_id}")
    assert get_response.status_code == 200
    assert get_response.json()["conversation_id"] == conversation_id

    delete_response = await client.delete(
        f"/api/v1/dev/conversations/{conversation_id}"
    )
    assert delete_response.status_code == 204

    after_delete = await client.get(f"/api/v1/dev/conversations/{conversation_id}")
    assert after_delete.status_code == 404

    empty_list = await client.get("/api/v1/dev/conversations")
    assert empty_list.status_code == 200
    assert empty_list.json() == {"items": [], "next_cursor": None}


@pytest.mark.asyncio
async def test_dev_feedback_is_scoped_to_the_target_conversation(dev_api_context):
    client = dev_api_context.client
    maker = dev_api_context.maker
    org_id = dev_api_context.org_id
    user_id = dev_api_context.user_id

    create_one = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(org_id)},
    )
    assert create_one.status_code == 201
    primary_conversation_id = create_one.json()["conversation_id"]

    answer_id = uuid.uuid4()
    async with maker() as session:
        service = DevPersistenceService(session)
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=uuid.UUID(primary_conversation_id),
            client_message_id=uuid.uuid4(),
            question="What changed?",
            scope_snapshot={},
        )
        await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=uuid.UUID(primary_conversation_id),
            answer_payload={
                "schema_version": "dev_answer.v1",
                "answer_id": str(answer_id),
                "conversation_id": primary_conversation_id,
                "summary": "The answer is stored for feedback testing.",
                "claims": [],
                "metrics": [],
                "evidence": [],
            },
            validator=lambda payload: payload,
            scope_snapshot={},
        )
        await session.commit()

    success = await client.post(
        f"/api/v1/dev/answers/{answer_id}/feedback",
        json={
            "rating": "helpful",
            "reasons": ["useful"],
        },
    )
    assert success.status_code == 200
    assert success.json()["answer_id"] == str(answer_id)

    missing_answer_id = uuid.uuid4()
    mismatch = await client.post(
        f"/api/v1/dev/answers/{missing_answer_id}/feedback",
        json={
            "rating": "helpful",
            "reasons": ["useful"],
        },
    )
    assert mismatch.status_code == 404
    assert mismatch.json()["code"] == "conversation_not_found"


@pytest.mark.asyncio
async def test_dev_requests_reject_payload_ownership_and_invalid_cursors(
    dev_api_context,
):
    client = dev_api_context.client
    response = await client.post(
        "/api/v1/dev/conversations",
        json={
            "current_scope": _scope_payload(dev_api_context.org_id),
            "org_id": str(dev_api_context.org_id),
        },
        headers={"x-request-id": "request_owned_fields"},
    )
    assert response.status_code == 422
    assert response.json() == {
        "schema_version": "dev_error.v1",
        "request_id": "request_owned_fields",
        "code": "invalid_request",
        "safe_message": "The Ask Dev request is invalid.",
        "retryable": False,
        "remediation": [],
    }

    cursor = await client.get("/api/v1/dev/conversations?cursor=not-a-cursor")
    assert cursor.status_code == 422
    assert cursor.json()["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_dev_conversation_lookup_does_not_disclose_cross_tenant_records(
    dev_api_context,
):
    created = await dev_api_context.client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(dev_api_context.org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    dev_api_context.app.dependency_overrides[dev_router_module._authenticated_user] = (
        lambda: AuthenticatedUser(
            user_id=str(uuid.uuid4()),
            email="other@example.com",
            org_id=str(uuid.uuid4()),
            role="member",
        )
    )
    response = await dev_api_context.client.get(
        f"/api/v1/dev/conversations/{conversation_id}"
    )
    assert response.status_code == 404
    assert response.json()["code"] == "conversation_not_found"


@pytest.mark.asyncio
async def test_dev_mutations_fail_closed_when_entitlement_is_denied(
    dev_api_context, monkeypatch: pytest.MonkeyPatch
):
    async def deny(self, _org_id: str) -> None:
        raise dev_router_module.AskDevEntitlementDeniedError(
            FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED
        )

    monkeypatch.setattr(
        dev_router_module.CanonicalAskDevEntitlementAuthorizer,
        "require",
        deny,
    )
    response = await dev_api_context.client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(dev_api_context.org_id)},
        headers={"x-request-id": "request_entitlement"},
    )
    assert response.status_code == 403
    assert response.json()["request_id"] == "request_entitlement"
    assert response.json()["code"] == "feature_not_enabled"
