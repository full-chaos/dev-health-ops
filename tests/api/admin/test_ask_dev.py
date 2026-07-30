from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.admin.routers import ask_dev as ask_dev_admin
from dev_health_ops.api.admin.routers.ask_dev import router
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.licensing import FeatureDecision, FeatureDecisionReason
from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
    AgentFinalAnswer,
    AgentProviderCapabilities,
    AgentToolRequest,
    AgentUsage,
    StreamingMode,
    StructuredOutputMode,
    ToolDecisionMode,
)
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.policy import AgentProviderSource
from dev_health_ops.models.dev_persistence import DevConversation, DevRun
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import Setting
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

_TABLES = tables_of(User, Organization, Setting, DevConversation, DevRun)


class FakeReadinessProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = False

    @property
    def capabilities(self) -> AgentProviderCapabilities:
        return AgentProviderCapabilities(
            structured_output=StructuredOutputMode.JSON_SCHEMA,
            tool_decisions=ToolDecisionMode.NATIVE,
            streaming=StreamingMode.BUFFERED,
            supports_cancellation=True,
            context_window_tokens=16_384,
            max_output_tokens=1_024,
            readiness_version=READINESS_VERSION,
            disclosure_key="openai-compatible",
        )

    @property
    def provider_fingerprint(self) -> str:
        return "provider"

    @property
    def model_fingerprint(self) -> str:
        return "model"

    async def decide(self, *_args: Any, **_kwargs: Any) -> AgentDecisionResult:
        self.calls += 1
        if self.calls == 1:
            return AgentDecisionResult(
                decision=AgentToolRequest(
                    "readiness_echo", {"nonce": "ready-v1"}, "call-1"
                ),
                usage=AgentUsage(input_tokens=3, output_tokens=2),
                latency_ms=1,
                provider_fingerprint="provider",
                model_fingerprint="model",
            )
        return AgentDecisionResult(
            decision=AgentFinalAnswer(value={}),
            usage=AgentUsage(input_tokens=4, output_tokens=1),
            latency_ms=1,
            provider_fingerprint="provider",
            model_fingerprint="model",
        )

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class AdminContext:
    app: FastAPI
    client: AsyncClient
    maker: async_sessionmaker[AsyncSession]
    org_id: uuid.UUID
    user_id: uuid.UUID
    user: AuthenticatedUser


@pytest_asyncio.fixture
async def admin_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ask-dev-admin.db'}")
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
                Organization(id=org_id, slug="ask-dev-admin", name="Ask Dev Admin"),
                User(
                    id=user_id,
                    email="admin@example.com",
                    username="ask-dev-admin",
                    password_hash="test",
                ),
            ]
        )
        await session.commit()

    user = AuthenticatedUser(
        user_id=str(user_id),
        email="admin@example.com",
        org_id=str(org_id),
        role="admin",
    )

    async def session_override():
        async with maker() as session:
            yield session
            await session.commit()

    async def current_user_override() -> AuthenticatedUser:
        return user

    async def enabled_feature(_session, _org_id: str):
        return "enabled", True, None

    async def provider_resolution(_session, *, org_id: str):
        assert org_id == str(admin_context_org_id)
        return ProductionProviderResolution(
            provider=FakeReadinessProvider(),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="safe-model",
            provider_label="OpenAI compatible",
            model_label="safe-model",
            readiness_fingerprint="readiness-fingerprint",
        )

    admin_context_org_id = org_id
    monkeypatch.setattr(ask_dev_admin, "_feature_state", enabled_feature)
    monkeypatch.setattr(
        ask_dev_admin, "resolve_certification_provider", provider_resolution
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret")

    app = FastAPI()
    app.include_router(
        router,
        prefix="/api/v1/admin",
        dependencies=[Depends(require_admin)],
    )
    app.dependency_overrides[ask_dev_admin.get_session] = session_override
    app.dependency_overrides[get_current_user] = current_user_override
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    context = AdminContext(app, client, maker, org_id, user_id, user)
    yield context
    await client.aclose()
    await engine.dispose()


@pytest.mark.asyncio
async def test_admin_projection_and_readiness_are_safe_and_shared(admin_context):
    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] == "stale_readiness"
    assert body["chat_window_available"] is False
    assert body["full_page_available"] is False
    assert body["retention_options"] == [0, 30]
    assert body["fallback_options"] == ["fail_closed", "platform"]
    assert body["platform_allowance_bounds"] == {
        "request_minimum": 100,
        "request_maximum": 1000,
        "cost_minimum_microusd": 10_000_000,
        "cost_maximum_microusd": 100_000_000,
    }
    assert body["no_training_by_default"] is True

    certified = await admin_context.client.post("/api/v1/admin/ask-dev/readiness")
    assert certified.status_code == 200
    certified_body = certified.json()
    assert certified_body["readiness"] == "ready"
    assert certified_body["chat_window_available"] is True
    assert certified_body["full_page_available"] is True
    serialized = certified.text.lower()
    for forbidden in (
        "api_key",
        "base_url",
        "prompt",
        "evidence",
        "packet",
        "conversation",
    ):
        assert forbidden not in serialized


@pytest.mark.asyncio
async def test_operator_maxima_do_not_raise_the_unconfigured_org_defaults(
    admin_context, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX", "5000")
    monkeypatch.setenv("ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD", "500000000")

    response = await admin_context.client.get("/api/v1/admin/ask-dev")

    assert response.status_code == 200
    body = response.json()
    assert body["settings"]["platform_monthly_request_limit"] == 1000
    assert body["settings"]["platform_monthly_cost_limit_microusd"] == 100_000_000
    assert body["settings"]["fallback_policy"] == "platform"
    assert body["platform_allowance_bounds"]["request_maximum"] == 5000
    assert body["platform_allowance_bounds"]["cost_maximum_microusd"] == 500_000_000


@pytest.mark.asyncio
async def test_settings_are_bounded_preserve_byo_and_disable_both_surfaces(
    admin_context, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX", "600")
    monkeypatch.setenv("ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD", "50000000")
    async with admin_context.maker() as session:
        await SettingsService(session, str(admin_context.org_id)).set(
            "api_key", "tenant-secret", "llm", encrypt=False
        )
        await session.commit()

    rejected = await admin_context.client.patch(
        "/api/v1/admin/ask-dev/settings", json={"retention_days": 7}
    )
    assert rejected.status_code == 422
    over_provisioned = await admin_context.client.patch(
        "/api/v1/admin/ask-dev/settings",
        json={"platform_monthly_request_limit": 601},
    )
    assert over_provisioned.status_code == 422

    changed = await admin_context.client.patch(
        "/api/v1/admin/ask-dev/settings",
        json={
            "retention_days": 0,
            "fallback_policy": "platform",
            "emergency_disabled": True,
            "platform_monthly_request_limit": 500,
            "platform_monthly_cost_limit_microusd": 20_000_000,
        },
    )
    assert changed.status_code == 200
    body = changed.json()
    assert body["settings"] == {
        "retention_days": 0,
        "fallback_policy": "platform",
        "emergency_disabled": True,
        "platform_monthly_request_limit": 500,
        "platform_monthly_cost_limit_microusd": 20_000_000,
    }
    assert body["entitlement_state"] == "org_disabled"
    assert body["ask_dev_enabled"] is False
    assert body["chat_window_available"] is False
    assert body["full_page_available"] is False

    readiness = await admin_context.client.post("/api/v1/admin/ask-dev/readiness")
    assert readiness.status_code == 403
    assert readiness.json()["detail"] == (
        "Ask Dev readiness cannot run while the organization emergency disable is active"
    )

    async with admin_context.maker() as session:
        stored = await session.scalar(
            select(Setting).where(
                Setting.org_id == str(admin_context.org_id),
                Setting.category == "llm",
                Setting.key == "api_key",
            )
        )
        assert stored is not None
        assert stored.value == "tenant-secret"


@pytest.mark.asyncio
async def test_impersonation_blocks_settings_and_readiness(admin_context):
    admin_context.user.impersonated_by = str(uuid.uuid4())
    settings = await admin_context.client.patch(
        "/api/v1/admin/ask-dev/settings", json={"retention_days": 0}
    )
    readiness = await admin_context.client.post("/api/v1/admin/ask-dev/readiness")
    assert settings.status_code == 403
    assert readiness.status_code == 403


@pytest.mark.asyncio
async def test_non_admin_is_rejected_but_superuser_is_allowed(admin_context):
    admin_context.user.role = "member"
    denied = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert denied.status_code == 403

    admin_context.user.is_superuser = True
    allowed = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert allowed.status_code == 200


@pytest.mark.asyncio
async def test_usage_is_ask_dev_only_content_free_aggregation(admin_context):
    now = datetime.now(UTC)
    async with admin_context.maker() as session:
        conversation = DevConversation(
            org_id=admin_context.org_id,
            user_id=admin_context.user_id,
            current_scope={},
            retention_days=30,
        )
        session.add(conversation)
        await session.flush()
        session.add_all(
            [
                DevRun(
                    request_id=uuid.uuid4(),
                    conversation_id=conversation.id,
                    org_id=admin_context.org_id,
                    user_id=admin_context.user_id,
                    state="completed",
                    input_tokens=10,
                    output_tokens=4,
                    estimated_cost_microusd=25,
                    provider_source="platform",
                    started_at=now,
                ),
                DevRun(
                    request_id=uuid.uuid4(),
                    conversation_id=conversation.id,
                    org_id=admin_context.org_id,
                    user_id=admin_context.user_id,
                    state="failed",
                    input_tokens=3,
                    output_tokens=0,
                    provider_source="platform",
                    started_at=now,
                ),
                DevRun(
                    request_id=uuid.uuid4(),
                    conversation_id=conversation.id,
                    org_id=admin_context.org_id,
                    user_id=admin_context.user_id,
                    state="insufficient_evidence",
                    input_tokens=5,
                    output_tokens=2,
                    estimated_cost_microusd=10,
                    provider_source="byo",
                    started_at=now,
                ),
            ]
        )
        await session.commit()

    response = await admin_context.client.get("/api/v1/admin/ask-dev/usage")
    assert response.status_code == 200
    body = response.json()
    assert body["use_case"] == "ask_dev"
    assert body["request_count"] == body["run_count"] == 3
    assert body["completed_runs"] == 1
    assert body["failed_runs"] == 1
    assert body["degraded_runs"] == 1
    assert body["input_tokens"] == 18
    assert body["output_tokens"] == 6
    assert body["estimated_cost_microusd"] == 35
    assert body["failure_rate"] == pytest.approx(1 / 3)
    assert body["degraded_rate"] == pytest.approx(1 / 3)
    allowance = body["platform_allowance"]
    assert allowance["request_limit"] == 1000
    assert allowance["request_used"] == 2
    assert allowance["request_remaining"] == 998
    assert allowance["cost_limit_microusd"] == 100_000_000
    assert allowance["cost_used_microusd"] == 5_000_025
    assert allowance["cost_remaining_microusd"] == 94_999_975
    assert allowance["warning"] == "none"
    assert allowance["window_start"].endswith("T00:00:00Z")
    assert allowance["reset_at"].endswith("T00:00:00Z")
    assert "tenant-secret" not in response.text


@pytest.mark.asyncio
async def test_global_disable_has_precedence(monkeypatch: pytest.MonkeyPatch):
    async def decision(_session, _org_id, _feature):
        return FeatureDecision(
            feature_key="ask_dev",
            allowed=False,
            reason=FeatureDecisionReason.GLOBAL_DISABLED,
        )

    monkeypatch.setattr(ask_dev_admin, "evaluate_org_feature_async", decision)
    state, allowed, reason = await ask_dev_admin._feature_state(
        cast(AsyncSession, object()),
        str(uuid.uuid4()),
    )
    assert (state, allowed) == ("globally_disabled", False)
    assert reason == "Ask Dev is globally disabled."
