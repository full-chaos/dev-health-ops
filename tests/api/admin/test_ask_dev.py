from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timezone
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
from dev_health_ops.llm.agent.readiness import (
    PLATFORM_READINESS_SETTING_KEY,
    PLATFORM_SETTINGS_ORG_ID,
    AgentReadinessOutcome,
    AgentReadinessRecord,
    SettingsAgentReadinessStore,
    readiness_failure_state,
)
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
    RoleCertificationRecord,
    RoleCertificationState,
    SettingsRoleCertificationStore,
)
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
                    "readiness_echo.v1", {"nonce": "ready-v1"}, "call-1"
                ),
                usage=AgentUsage(input_tokens=3, output_tokens=2),
                latency_ms=1,
                provider_fingerprint="provider",
                model_fingerprint="model",
            )
        return AgentDecisionResult(
            decision=AgentFinalAnswer(value={"nonce": "ready-v1"}),
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
async def test_admin_projection_nulls_platform_identity_until_platform_admin_certifies(
    admin_context,
):
    """CHAOS-3265: the org-admin surface never learns the platform provider's
    identity, and Platform Admin -- not this org -- is what makes it ready."""

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] == "stale_readiness"
    assert body["chat_window_available"] is False
    assert body["full_page_available"] is False
    assert body["effective_provider_label"] is None
    assert body["effective_model_label"] is None
    assert body["provider_source"] is None
    assert body["administrator_safe_failure_reason"] == (
        "Ask Dev is temporarily unavailable. Contact your platform operator."
    )
    assert body["retention_options"] == [0, 30]
    assert body["fallback_options"] == ["fail_closed", "platform"]
    assert body["platform_allowance_bounds"] == {
        "request_minimum": 100,
        "request_maximum": 1000,
        "cost_minimum_microusd": 10_000_000,
        "cost_maximum_microusd": 100_000_000,
    }
    assert body["no_training_by_default"] is True

    # This org has no route left that can certify the platform provider.
    # Simulate Platform Admin's route certifying it in the platform-global
    # sentinel scope (org_id="", distinct key) -- the only place that
    # certification can be written now.
    async with admin_context.maker() as session:
        await SettingsAgentReadinessStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_READINESS_SETTING_KEY,
        ).save(
            AgentReadinessRecord(
                fingerprint="readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.READY,
            )
        )
        # CHAOS-3285 round 2: effective readiness also requires a current,
        # COMPATIBLE legacy_agent role certification -- mirror what a real
        # POST /platform/ask-dev/readiness run now also writes.
        await SettingsRoleCertificationStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
        ).save_record(
            RoleCertificationRecord(
                role=AgentRole.LEGACY_AGENT,
                certification_key="readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                state=RoleCertificationState.COMPATIBLE,
            )
        )
        await session.commit()

    certified = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert certified.status_code == 200
    certified_body = certified.json()
    assert certified_body["readiness"] == "ready"
    assert certified_body["chat_window_available"] is True
    assert certified_body["full_page_available"] is True
    assert certified_body["effective_provider_label"] is None
    assert certified_body["effective_model_label"] is None
    assert certified_body["provider_source"] is None
    assert certified_body["administrator_safe_failure_reason"] is None
    serialized = certified.text.lower()
    for forbidden in (
        "api_key",
        "base_url",
        "prompt",
        "evidence",
        "packet",
        "conversation",
        "openai compatible",
        "safe-model",
    ):
        assert forbidden not in serialized


@pytest.mark.asyncio
async def test_byo_source_keeps_its_own_identity_and_failure_reason(
    admin_context, monkeypatch: pytest.MonkeyPatch
):
    """The null-out is source-scoped: a BYO-sourced org still sees its own
    identity and failure reason (it's the org's own data, not platform's)."""

    async def byo_resolution(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=FakeReadinessProvider(),
            source=AgentProviderSource.BYO,
            family="openai",
            model="org-model",
            provider_label="OpenAI compatible",
            model_label="org-model",
            readiness_fingerprint="byo-readiness-fingerprint",
        )

    monkeypatch.setattr(ask_dev_admin, "resolve_certification_provider", byo_resolution)

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] == "stale_readiness"
    assert body["effective_provider_label"] == "OpenAI compatible"
    assert body["effective_model_label"] == "org-model"
    assert body["provider_source"] == "byo"
    assert body["administrator_safe_failure_reason"] == (
        "The configured Ask Dev model has not been certified."
    )

    async with admin_context.maker() as session:
        await SettingsAgentReadinessStore(
            SettingsService(session, str(admin_context.org_id))
        ).save(
            AgentReadinessRecord(
                fingerprint="byo-readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.READY,
            )
        )
        # CHAOS-3285 round 2: effective readiness also requires a current,
        # COMPATIBLE legacy_agent role certification.
        await SettingsRoleCertificationStore(
            SettingsService(session, str(admin_context.org_id))
        ).save_record(
            RoleCertificationRecord(
                role=AgentRole.LEGACY_AGENT,
                certification_key="byo-readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                state=RoleCertificationState.COMPATIBLE,
            )
        )
        await session.commit()

    certified = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert certified.status_code == 200
    certified_body = certified.json()
    assert certified_body["readiness"] == "ready"
    assert certified_body["effective_provider_label"] == "OpenAI compatible"
    assert certified_body["effective_model_label"] == "org-model"
    assert certified_body["provider_source"] == "byo"
    assert certified_body["administrator_safe_failure_reason"] is None


@pytest.mark.asyncio
async def test_deprecated_post_readiness_route_is_410_and_touches_nothing(
    admin_context, monkeypatch: pytest.MonkeyPatch
):
    """CHAOS-3265: the deprecated org-scoped POST route runs NO certification
    logic at all (kept only for rolling-deploy safety against an
    already-deployed older web frontend)."""

    def _must_not_resolve(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(
            "the deprecated /ask-dev/readiness route must never resolve a "
            "certification provider"
        )

    monkeypatch.setattr(
        ask_dev_admin, "resolve_certification_provider", _must_not_resolve
    )

    async def _setting_rows() -> list[tuple[str, str, str, str | None]]:
        async with admin_context.maker() as session:
            rows = (await session.execute(select(Setting))).scalars().all()
            return [(s.org_id, s.category, s.key, s.value) for s in rows]

    before = await _setting_rows()

    # Even an impersonated, otherwise-emergency-disabled org admin still just
    # gets the static 410 -- there is no code path left that runs anything.
    admin_context.user.impersonated_by = str(uuid.uuid4())
    response = await admin_context.client.post("/api/v1/admin/ask-dev/readiness")
    assert response.status_code == 410
    assert response.json() == {
        "detail": (
            "Platform preflight has moved to Platform Admin. "
            "Use BYO LLM settings for BYO preflight."
        )
    }
    serialized = response.text.lower()
    for forbidden in (
        "openai",
        "provider_source",
        "effective_provider_label",
        "readiness",
    ):
        assert forbidden not in serialized

    after = await _setting_rows()
    assert before == after


@pytest.mark.parametrize(
    ("safe_error_code", "state", "message"),
    [
        (
            "provider_not_configured",
            "missing_credentials",
            "could not authenticate",
        ),
        ("timeout", "degraded", "timed out"),
        ("rate_limited", "degraded", "rate limit"),
        ("model_not_supported", "unsupported_model", "unavailable to this provider"),
        ("invalid_request", "unsupported_model", "required agent request capability"),
        ("invalid_response", "unsupported_model", "capability contract"),
        ("provider_unavailable", "degraded", "endpoint is unavailable"),
        (
            "provider_contract_violation",
            "unsupported_model",
            "sequential tool-call contract",
        ),
        (
            "output_exhausted",
            "unsupported_model",
            "output/reasoning token budget",
        ),
    ],
)
def test_failed_readiness_exposes_only_specific_safe_remediation(
    safe_error_code: str, state: str, message: str
) -> None:
    readiness, reason = readiness_failure_state(safe_error_code)

    assert readiness == state
    assert message in reason
    assert "api_key" not in reason
    assert "base_url" not in reason


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

    # The deprecated route no longer inspects entitlement/policy at all -- it
    # is a static 410 regardless of the emergency-disable state.
    readiness = await admin_context.client.post("/api/v1/admin/ask-dev/readiness")
    assert readiness.status_code == 410

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
async def test_impersonation_blocks_settings(admin_context):
    admin_context.user.impersonated_by = str(uuid.uuid4())
    settings = await admin_context.client.patch(
        "/api/v1/admin/ask-dev/settings", json={"retention_days": 0}
    )
    assert settings.status_code == 403
    # The deprecated readiness route has no logic left to block -- see
    # test_deprecated_post_readiness_route_is_410_and_touches_nothing.


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


@pytest.mark.asyncio
async def test_role_readiness_shows_not_yet_certified_with_no_stored_record(
    admin_context, monkeypatch: pytest.MonkeyPatch
):
    """CHAOS-3285: before anything certifies a role, every role reads as
    the honest not_yet_certified state -- distinct from stale_readiness (was
    certified, now invalidated) and from any FAILED-derived state."""

    async def byo_resolution(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=FakeReadinessProvider(),
            source=AgentProviderSource.BYO,
            family="openai",
            model="org-model",
            provider_label="OpenAI compatible",
            model_label="org-model",
            readiness_fingerprint="byo-readiness-fingerprint",
        )

    monkeypatch.setattr(ask_dev_admin, "resolve_certification_provider", byo_resolution)

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    body = response.json()
    roles = {entry["role"]: entry for entry in body["role_readiness"]}
    assert set(roles) == {
        "legacy_agent",
        "intent_classification",
        "answer_frame_narrative",
    }
    for entry in roles.values():
        assert entry["state"] == "not_yet_certified"
        assert entry["checked_at"] is None
        assert entry["safe_remediation"]


@pytest.mark.asyncio
async def test_role_readiness_reflects_a_current_legacy_agent_certification(
    admin_context, monkeypatch: pytest.MonkeyPatch
):
    async def byo_resolution(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=FakeReadinessProvider(),
            source=AgentProviderSource.BYO,
            family="openai",
            model="org-model",
            provider_label="OpenAI compatible",
            model_label="org-model",
            readiness_fingerprint="byo-readiness-fingerprint",
        )

    monkeypatch.setattr(ask_dev_admin, "resolve_certification_provider", byo_resolution)

    async with admin_context.maker() as session:
        store = SettingsRoleCertificationStore(
            SettingsService(session, str(admin_context.org_id))
        )
        profile = (await store.load()).with_record(
            RoleCertificationRecord(
                role=AgentRole.LEGACY_AGENT,
                certification_key="byo-readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                state=RoleCertificationState.COMPATIBLE,
            )
        )
        await store.save(profile)
        await session.commit()

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    roles = {entry["role"]: entry for entry in response.json()["role_readiness"]}
    assert roles["legacy_agent"]["state"] == "ready"
    assert roles["legacy_agent"]["safe_remediation"] is None
    assert roles["legacy_agent"]["checked_at"] is not None
    assert roles["intent_classification"]["state"] == "not_yet_certified"
    assert roles["answer_frame_narrative"]["state"] == "not_yet_certified"


@pytest.mark.asyncio
async def test_role_readiness_reports_stale_when_certification_key_changed(
    admin_context, monkeypatch: pytest.MonkeyPatch
):
    """A record certified under a DIFFERENT capability-input key (e.g. the
    prompt/tool/budget contract changed since) must never read as ready."""

    async def byo_resolution(_session, *, org_id: str):
        return ProductionProviderResolution(
            provider=FakeReadinessProvider(),
            source=AgentProviderSource.BYO,
            family="openai",
            model="org-model",
            provider_label="OpenAI compatible",
            model_label="org-model",
            readiness_fingerprint="byo-readiness-fingerprint-current",
        )

    monkeypatch.setattr(ask_dev_admin, "resolve_certification_provider", byo_resolution)

    async with admin_context.maker() as session:
        store = SettingsRoleCertificationStore(
            SettingsService(session, str(admin_context.org_id))
        )
        profile = (await store.load()).with_record(
            RoleCertificationRecord(
                role=AgentRole.LEGACY_AGENT,
                certification_key="byo-readiness-fingerprint-stale",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                state=RoleCertificationState.COMPATIBLE,
            )
        )
        await store.save(profile)
        await session.commit()

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    roles = {entry["role"]: entry for entry in response.json()["role_readiness"]}
    assert roles["legacy_agent"]["state"] == "stale_readiness"


@pytest.mark.asyncio
async def test_role_readiness_platform_boundary_redacts_remediation_not_state(
    admin_context,
):
    """CHAOS-3265 boundary applied to the new per-role surface: an org
    relying on platform fallback must see the platform role's state (it
    carries no identity) but never platform's own specific remediation
    text -- same discipline as the existing single-readiness field."""

    async with admin_context.maker() as session:
        store = SettingsRoleCertificationStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
        )
        profile = (await store.load()).with_record(
            RoleCertificationRecord(
                role=AgentRole.LEGACY_AGENT,
                certification_key="readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                state=RoleCertificationState.INCOMPATIBLE,
                safe_error_code="output_exhausted",
            )
        )
        await store.save(profile)
        await session.commit()

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    roles = {entry["role"]: entry for entry in response.json()["role_readiness"]}
    assert roles["legacy_agent"]["state"] == "unsupported_model"
    assert roles["legacy_agent"]["safe_remediation"] == (
        "Ask Dev is temporarily unavailable. Contact your platform operator."
    )


@pytest.mark.asyncio
async def test_binary_ready_role_absent_reports_unavailable_not_ready(
    admin_context,
):
    """CHAOS-3285 round 2 (Codex HIGH): the exact contradiction codex found --
    the binary transport-echo check passing must never, by itself, report
    "ready"/available when the legacy_agent role has no certification at
    all. Live selection (production_runtime.py) already rejects this
    candidate; the admin surface must agree, not contradict it."""

    async with admin_context.maker() as session:
        await SettingsAgentReadinessStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_READINESS_SETTING_KEY,
        ).save(
            AgentReadinessRecord(
                fingerprint="readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.READY,
            )
        )
        # Deliberately NO role-certification row at all.
        await session.commit()

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] != "ready"
    assert body["binary_transport_readiness"] == "ready"
    assert body["chat_window_available"] is False
    assert body["full_page_available"] is False
    roles = {entry["role"]: entry for entry in body["role_readiness"]}
    assert roles["legacy_agent"]["state"] == "not_yet_certified"


@pytest.mark.asyncio
async def test_binary_ready_role_incompatible_reports_unavailable_not_ready(
    admin_context,
):
    """Same contradiction, with a role record present but INCOMPATIBLE
    rather than absent -- must also fail to report ready/available."""

    async with admin_context.maker() as session:
        await SettingsAgentReadinessStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_READINESS_SETTING_KEY,
        ).save(
            AgentReadinessRecord(
                fingerprint="readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.READY,
            )
        )
        await SettingsRoleCertificationStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
        ).save_record(
            RoleCertificationRecord(
                role=AgentRole.LEGACY_AGENT,
                certification_key="readiness-fingerprint",
                readiness_version=READINESS_VERSION,
                checked_at=datetime.now(timezone.utc).isoformat(),
                state=RoleCertificationState.INCOMPATIBLE,
                safe_error_code="output_exhausted",
            )
        )
        await session.commit()

    response = await admin_context.client.get("/api/v1/admin/ask-dev")
    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] != "ready"
    assert body["binary_transport_readiness"] == "ready"
    assert body["chat_window_available"] is False
    assert body["full_page_available"] is False
