from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import router as dev_router_module
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevScopeResolution,
    ScopeResolutionOutcome,
)
from dev_health_ops.api.dev.evidence_service import (
    EvidenceAvailability,
    EvidenceExpansion,
    EvidenceExpansionResult,
)
from dev_health_ops.api.dev.orchestrator import (
    OrchestratorEvent,
    OrchestratorResult,
    RunState,
)
from dev_health_ops.api.dev.persistence import DevPersistenceService
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing import FeatureDecisionReason
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


@dataclass
class DevApiContext:
    app: FastAPI
    client: AsyncClient
    maker: async_sessionmaker[AsyncSession]
    org_id: uuid.UUID
    user_id: uuid.UUID


class FakeBoundedRuntime:
    provider_source = "platform"

    async def run(
        self,
        *,
        request,
        run_id: str,
        conversation_id: str,
        answer_id: str,
        recorder,
        event_sink,
        **_kwargs,
    ) -> OrchestratorResult:
        resolution = DevScopeResolution(
            schema_version="dev_scope_resolution.v1",
            outcome=ScopeResolutionOutcome.EXACT,
            requested_scope=request.scope,
            resolved_scope=request.scope,
            authorized_repository_ids=request.scope.repositories,
            authorized_entity_ids=[
                item.entity_id for item in request.scope.entity_refs
            ],
            candidates=[],
            fallbacks=[],
            warnings=[],
            resolved_at=datetime.now(UTC),
        )
        payload = deepcopy(positive_fixtures()["dev_answer.v1"])
        payload.update(
            {
                "answer_id": answer_id,
                "conversation_id": conversation_id,
                "generated_at": datetime.now(UTC).isoformat(),
                "as_of": datetime.now(UTC).isoformat(),
                "resolved_scope": resolution.model_dump(mode="json"),
                "claims": [],
                "metrics": [],
                "evidence": [],
                "conflicts": [],
                "coverage": {
                    "required_source_count": 0,
                    "available_source_count": 0,
                    "unavailable_required_sources": [],
                    "stale_required_sources": [],
                    "as_of": datetime.now(UTC).isoformat(),
                },
                "warnings": [],
            }
        )
        answer = DevAnswer.model_validate(payload)
        for state in (
            RunState.RESOLVING_SCOPE,
            RunState.MODEL_DECISION,
            RunState.ANSWER_VALIDATION,
        ):
            await recorder.transition(state)
            await event_sink(OrchestratorEvent(state))
        await recorder.record_answer(answer)
        await recorder.terminal(
            state=RunState.COMPLETED,
            answer=answer,
            error=None,
            usage=AgentUsage(input_tokens=10, output_tokens=20),
            tool_call_count=0,
            provider_fingerprint="provider-test",
            model_fingerprint="model-test",
            prompt_checksum="checksum-test",
        )
        await event_sink(OrchestratorEvent(RunState.COMPLETED))
        return OrchestratorResult(
            run_id=run_id,
            state=RunState.COMPLETED,
            answer=answer,
            error=None,
            events=(OrchestratorEvent(RunState.COMPLETED),),
            usage=AgentUsage(input_tokens=10, output_tokens=20),
            tool_call_count=0,
            provider_fingerprint="provider-test",
            model_fingerprint="model-test",
        )

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_capability_runtime_degrades_safely_when_resolution_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_resolution(_session, *, org_id: str):
        assert org_id == "org_01"
        raise RuntimeError("database details must not escape")

    monkeypatch.setattr(
        dev_router_module, "resolve_production_provider", fail_resolution
    )
    result = await dev_router_module.get_dev_capability_runtime(
        AuthenticatedUser(
            user_id="user_01",
            email="member@example.com",
            org_id="org_01",
            role="member",
        ),
        cast(AsyncSession, object()),
    )
    assert result.readiness == "degraded"
    assert result.safe_failure_reason == (
        "Ask Dev model readiness is temporarily unavailable."
    )


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
    app.dependency_overrides[dev_router_module.get_dev_execution_runtime] = lambda: (
        dev_router_module.DevExecutionRuntimeResolution(
            runtime=cast(Any, FakeBoundedRuntime())
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
async def test_dev_message_stream_is_bounded_persisted_and_idempotent(
    dev_api_context,
):
    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(dev_api_context.org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    payload = {
        "schema_version": "dev_message_request.v1",
        "request_id": "request_message_01",
        "client_message_id": "client_message_01",
        "conversation_id": conversation_id,
        "question": "What changed?",
        "question_class": "observed_change",
        "scope": _scope_payload(dev_api_context.org_id),
        "requested_metric_ids": [],
    }

    first = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        json=payload,
    )
    assert first.status_code == 200
    assert first.headers["content-type"].startswith("text/event-stream")
    assert first.text.count("event: run.started") == 1
    assert first.text.count("event: answer.completed") == 1
    assert first.text.count("event: done") == 1
    assert "raw_prompt" not in first.text
    assert "provider_response" not in first.text

    replay = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        json=payload,
    )
    assert replay.status_code == 200
    assert replay.text.count("event: run.started") == 1
    assert replay.text.count("event: answer.completed") == 1
    assert replay.text.count("event: done") == 1

    async with dev_api_context.maker() as session:
        assert await session.scalar(select(func.count()).select_from(DevRun)) == 1
        assert await session.scalar(select(func.count()).select_from(DevMessage)) == 2


@pytest.mark.asyncio
async def test_dev_message_fails_closed_when_no_certified_runtime_is_ready(
    dev_api_context,
):
    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(
        runtime=None,
        error_code="provider_not_configured",
        safe_message="No certified Ask Dev model is ready.",
    )
    created = await dev_api_context.client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(dev_api_context.org_id)},
    )
    conversation_id = created.json()["conversation_id"]

    response = await dev_api_context.client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages",
        json={
            "schema_version": "dev_message_request.v1",
            "request_id": "request_runtime_missing",
            "client_message_id": "client_runtime_missing",
            "conversation_id": conversation_id,
            "question": "What changed?",
            "question_class": "observed_change",
            "scope": _scope_payload(dev_api_context.org_id),
        },
    )

    assert response.status_code == 503
    assert response.json()["code"] == "provider_not_configured"
    async with dev_api_context.maker() as session:
        state = await session.scalar(select(DevRun.state))
        assert state == "failed"


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
async def test_dev_evidence_expansion_requires_owned_answer_relationship(
    dev_api_context, monkeypatch: pytest.MonkeyPatch
):
    client = dev_api_context.client
    maker = dev_api_context.maker
    org_id = dev_api_context.org_id
    user_id = dev_api_context.user_id
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    answer_id = uuid.uuid4()
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    answer_payload.update(
        {
            "answer_id": str(answer_id),
            "conversation_id": conversation_id,
            "resolved_scope": {
                "schema_version": "dev_scope_resolution.v1",
                "requested_scope": _scope_payload(org_id),
                "resolved_scope": _scope_payload(org_id),
                "outcome": "exact",
                "authorized_repository_ids": [],
                "authorized_entity_ids": [],
                "candidates": [],
                "fallbacks": [],
                "warnings": [],
                "resolved_at": "2026-07-28T13:00:00+00:00",
            },
        }
    )
    async with maker() as session:
        service = DevPersistenceService(session)
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=uuid.UUID(conversation_id),
            client_message_id=uuid.uuid4(),
            question="Show the evidence",
            scope_snapshot=_scope_payload(org_id),
        )
        await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=uuid.UUID(conversation_id),
            answer_payload=answer_payload,
            validator=lambda payload: payload,
            scope_snapshot=_scope_payload(org_id),
        )
        await session.commit()

    expected = DevAnswer.model_validate(answer_payload).evidence[0]

    captured = {}

    async def expand(_session, **values):
        captured.update(values)
        excerpt = "UNTRUSTED_DATA\nEvidence excerpt\nEND_UNTRUSTED_DATA"
        return EvidenceExpansionResult(
            expansions=(
                EvidenceExpansion(
                    evidence=expected,
                    state=EvidenceAvailability.AVAILABLE,
                    safe_excerpt=excerpt,
                    serialized_bytes=len(excerpt.encode()),
                ),
            ),
            serialized_bytes=len(excerpt.encode()),
        )

    monkeypatch.setattr(dev_router_module, "expand_production_evidence", expand)

    async def analytics_client(_dsn):
        return object()

    monkeypatch.setattr(dev_router_module, "get_global_client", analytics_client)
    monkeypatch.setattr(
        dev_router_module, "_analytics_db_url", lambda: "clickhouse://test"
    )
    response = await client.get(
        f"/api/v1/dev/evidence/{expected.evidence_ref_id}",
        params={"answer_id": str(answer_id)},
    )
    assert response.status_code == 200
    assert captured["org_id"] == str(org_id)
    assert captured["evidence"] == [expected]
    assert response.json()["evidence"]["evidence_ref_id"] == expected.evidence_ref_id
    assert response.json()["safe_excerpt"].startswith("UNTRUSTED_DATA")

    other_org_id = uuid.uuid4()
    other_user_id = uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=other_org_id, slug="other-org", name="Other Org"),
                User(id=other_user_id, email="other-user@example.com"),
            ]
        )
        await session.commit()

    dev_api_context.app.dependency_overrides[dev_router_module._authenticated_user] = (
        lambda: AuthenticatedUser(
            user_id=str(other_user_id),
            email="other-user@example.com",
            org_id=str(other_org_id),
            role="member",
        )
    )
    cross_tenant = await client.get(
        f"/api/v1/dev/evidence/{expected.evidence_ref_id}",
        params={"answer_id": str(answer_id)},
    )
    assert cross_tenant.status_code == 404

    dev_api_context.app.dependency_overrides[dev_router_module._authenticated_user] = (
        lambda: AuthenticatedUser(
            user_id=str(user_id),
            email="ask-dev@example.com",
            org_id=str(org_id),
            role="member",
        )
    )
    unrelated = await client.get(
        "/api/v1/dev/evidence/not_in_the_answer",
        params={"answer_id": str(answer_id)},
    )
    assert unrelated.status_code == 404
    assert unrelated.json()["code"] == cross_tenant.json()["code"]
    assert unrelated.json()["safe_message"] == cross_tenant.json()["safe_message"]


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
