from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.admin.routers import platform_ask_dev
from dev_health_ops.api.admin.routers.platform_ask_dev import router
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    _impersonation_ctx,
    set_impersonation_context,
)
from dev_health_ops.api.services.configuration import SettingsService
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
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.openai_compatible import READINESS_VERSION
from dev_health_ops.llm.agent.policy import AgentProviderSource
from dev_health_ops.llm.agent.readiness import (
    PLATFORM_READINESS_SETTING_KEY,
    PLATFORM_SETTINGS_ORG_ID,
    READINESS_ECHO_TOOL_ID,
    READINESS_SETTING_KEY,
    AgentReadinessOutcome,
    AgentReadinessRecord,
    SettingsAgentReadinessStore,
)
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
    SettingsRoleCertificationStore,
)
from dev_health_ops.models.dev_persistence import DevRun
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import Setting
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

_TABLES = tables_of(User, Organization, Setting, DevRun)
_FINGERPRINT = "platform-readiness-fingerprint"


class FakeReadinessProvider:
    """Echoes the readiness nonce successfully -- for the READY path."""

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

    async def decide(self, *_args: Any, **_kwargs: Any) -> AgentDecisionResult:
        # CHAOS-3285: the platform POST route now runs the OLD binary
        # transport-echo probe (calls 1-2) AND the NEW production-sized
        # legacy_agent role probe (calls 3-6: two independent 2-call chains,
        # committed-subject then uncommitted-subject -- CHAOS-3285 round 4,
        # Codex HIGH) against the same resolved provider, 6 calls total. Odd
        # calls request a tool; even calls answer. Call 1 must echo
        # READINESS_ECHO_TOOL_ID exactly (readiness.py's own strict match)
        # -- every later tool_request round names a real registered tool
        # instead, since certify_legacy_agent validates its synthetic tool
        # result against the real DevToolResult/ToolID contract. This fake
        # is round-aware by parity (odd/even), not call-count-specific, so
        # it transparently supports however many role-probe rounds the real
        # probe currently makes -- but see the pinned provider.calls == 6
        # assertion in the success test below, which fails loudly if a
        # chain silently vanishes.
        self.calls += 1
        if self.calls % 2 == 1:
            tool_id = READINESS_ECHO_TOOL_ID if self.calls == 1 else "query_metric.v1"
            arguments = (
                {"nonce": "ready-v1"} if tool_id == READINESS_ECHO_TOOL_ID else {}
            )
            return AgentDecisionResult(
                decision=AgentToolRequest(tool_id, arguments, f"call-{self.calls}"),
                usage=AgentUsage(input_tokens=3, output_tokens=2),
                latency_ms=1,
                provider_fingerprint="provider",
                model_fingerprint="model",
            )
        value = (
            {"nonce": "ready-v1"}
            if self.calls == 2
            else {"status": "complete", "direct_summary": "Stub role probe answer."}
        )
        return AgentDecisionResult(
            decision=AgentFinalAnswer(value=value),
            usage=AgentUsage(input_tokens=4, output_tokens=1),
            latency_ms=1,
            provider_fingerprint="provider",
            model_fingerprint="model",
        )

    async def aclose(self) -> None:
        self.closed = True


class FailingReadinessProvider(FakeReadinessProvider):
    """Always fails the readiness exchange -- for the FAILED path."""

    async def decide(self, *_args: Any, **_kwargs: Any) -> AgentDecisionResult:
        raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)


def _resolution(provider: Any) -> ProductionProviderResolution:
    return ProductionProviderResolution(
        provider=provider,
        source=AgentProviderSource.PLATFORM,
        family="openai",
        model="platform-model",
        provider_label="OpenAI compatible",
        model_label="platform-model",
        readiness_fingerprint=_FINGERPRINT,
    )


@dataclass
class PlatformContext:
    app: FastAPI
    client: AsyncClient
    maker: async_sessionmaker[AsyncSession]
    superuser: AuthenticatedUser
    org_admin: AuthenticatedUser
    current_user: AuthenticatedUser


@pytest_asyncio.fixture
async def platform_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'platform-ask-dev-admin.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    superuser = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="superuser@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
        is_superuser=True,
    )
    org_admin = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="org-admin@example.com",
        org_id=str(uuid.uuid4()),
        role="admin",
        is_superuser=False,
    )

    holder = {"user": superuser}

    async def session_override():
        async with maker() as session:
            yield session
            await session.commit()

    async def current_user_override() -> AuthenticatedUser:
        return holder["user"]

    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret")

    app = FastAPI()
    app.include_router(
        router,
        prefix="/api/v1/admin",
        dependencies=[Depends(require_admin)],
    )
    app.dependency_overrides[platform_ask_dev.get_session] = session_override
    app.dependency_overrides[get_current_user] = current_user_override
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    context = PlatformContext(app, client, maker, superuser, org_admin, superuser)
    context._holder = holder  # type: ignore[attr-defined]
    yield context
    await client.aclose()
    await engine.dispose()


def _as(context: PlatformContext, user: AuthenticatedUser) -> None:
    context._holder["user"] = user  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_org_admin_token_cannot_reach_platform_readiness(
    platform_context: PlatformContext,
):
    """The key regression test: an org-admin token must never reach platform
    diagnostics -- require_superuser must reject it (CHAOS-3265)."""

    _as(platform_context, platform_context.org_admin)

    get_response = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    post_response = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )

    assert get_response.status_code == 403
    assert post_response.status_code == 403


@pytest.mark.asyncio
async def test_superuser_certifies_and_readiness_persists_across_a_second_get(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    provider = FakeReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    before = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert before.status_code == 200
    before_body = before.json()
    assert before_body["configured"] is True
    assert before_body["readiness"] == "stale_readiness"
    assert before_body["provider_label"] == "OpenAI compatible"
    assert before_body["model_label"] == "platform-model"

    posted = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert posted.status_code == 200
    posted_body = posted.json()
    assert posted_body["readiness"] == "ready"
    assert posted_body["safe_remediation"] is None
    assert provider.closed is True

    after = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert after.status_code == 200
    after_body = after.json()
    assert after_body["readiness"] == "ready"
    assert after_body["provider_label"] == "OpenAI compatible"
    assert after_body["model_label"] == "platform-model"


@pytest.mark.asyncio
async def test_post_succeeds_and_logs_when_provider_close_fails(
    platform_context: PlatformContext,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """A transport-close failure after a successful certification must never
    mask the readiness result the caller already committed (CHAOS-3265 /
    CodeQL empty-except finding) -- and must be logged, not silently
    swallowed. Deleting the try/except's log call (or reverting it to a bare
    `pass`) makes this test fail on the caplog assertion below.

    CHAOS-3358 moved the certify sequence (and therefore this log call) into
    production_runtime.certify_platform_resolution, shared with automatic
    re-certification; the logger name below follows it."""

    class CloseFailsProvider(FakeReadinessProvider):
        async def aclose(self) -> None:
            self.closed = True
            raise RuntimeError("transport already shut down")

    provider = CloseFailsProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    with caplog.at_level("WARNING", logger="dev_health_ops.api.dev.production_runtime"):
        posted = await platform_context.client.post(
            "/api/v1/admin/platform/ask-dev/readiness"
        )

    assert posted.status_code == 200
    assert posted.json()["readiness"] == "ready"
    assert provider.closed is True
    assert any(
        "Failed to close platform Ask Dev provider connection" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_superuser_post_transitions_to_failed_and_persists(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    provider = FailingReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    posted = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert posted.status_code == 200
    posted_body = posted.json()
    assert posted_body["readiness"] == "unsupported_model"
    assert posted_body["safe_remediation"] is not None
    assert "capability contract" in posted_body["safe_remediation"]

    after = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    after_body = after.json()
    assert after_body["readiness"] == "unsupported_model"


@pytest.mark.asyncio
async def test_platform_not_configured_returns_safe_unconfigured_state(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    async def resolve() -> ProductionProviderResolution:
        raise DevRuntimeUnavailable(
            "provider_not_configured", "No certified Ask Dev model is ready."
        )

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    get_response = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert get_response.status_code == 200
    body = get_response.json()
    assert body["configured"] is False
    assert body["readiness"] == "missing_credentials"
    assert body["provider_label"] is None
    assert body["model_label"] is None
    assert body["safe_remediation"] == "No certified Ask Dev model is ready."

    post_response = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert post_response.status_code == 200
    post_body = post_response.json()
    assert post_body["configured"] is False
    assert post_body["readiness"] == "missing_credentials"


@pytest.mark.asyncio
async def test_certification_writes_the_sentinel_row_and_never_a_real_orgs_row(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    """The security-fix test: platform certification must land under the
    org_id="" sentinel scope and never under any real organization's row."""

    real_org_id = str(uuid.uuid4())
    async with platform_context.maker() as session:
        session.add(
            Setting(
                org_id=real_org_id,
                category="llm",
                key=READINESS_SETTING_KEY,
                value='{"marker": "a-real-orgs-own-byo-readiness-untouched"}',
            )
        )
        await session.commit()

    provider = FakeReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    response = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert response.status_code == 200
    assert response.json()["readiness"] == "ready"

    async with platform_context.maker() as session:
        real_org_row = await session.scalar(
            select(Setting).where(
                Setting.org_id == real_org_id,
                Setting.category == "llm",
                Setting.key == READINESS_SETTING_KEY,
            )
        )
        assert real_org_row is not None
        assert real_org_row.value == (
            '{"marker": "a-real-orgs-own-byo-readiness-untouched"}'
        )

        sentinel_row = await session.scalar(
            select(Setting).where(
                Setting.org_id == PLATFORM_SETTINGS_ORG_ID,
                Setting.category == "llm",
                Setting.key == PLATFORM_READINESS_SETTING_KEY,
            )
        )
        assert sentinel_row is not None
        assert sentinel_row.value is not None
        assert '"outcome":"ready"' in sentinel_row.value

        no_stray_row_under_ordinary_key = await session.scalar(
            select(Setting).where(
                Setting.org_id == PLATFORM_SETTINGS_ORG_ID,
                Setting.category == "llm",
                Setting.key == READINESS_SETTING_KEY,
            )
        )
        assert no_stray_row_under_ordinary_key is None


@pytest.mark.asyncio
async def test_stray_empty_org_row_under_the_ordinary_key_is_invisible(
    platform_context: PlatformContext,
):
    """Defense in depth: even a hypothetical accidental write that landed
    with an empty org_id under the *ordinary* readiness key must not affect
    the platform reader, because it only ever reads the distinct platform
    key."""

    async with platform_context.maker() as session:
        session.add(
            Setting(
                org_id=PLATFORM_SETTINGS_ORG_ID,
                category="llm",
                key=READINESS_SETTING_KEY,
                value=(
                    '{"fingerprint":"stray","readiness_version":"stray",'
                    '"checked_at":"2026-01-01T00:00:00+00:00","outcome":"ready",'
                    '"safe_error_code":null}'
                ),
            )
        )
        await session.commit()

    response = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert response.status_code == 200
    body = response.json()
    # Nothing has ever certified the *distinct* platform key, so this must
    # still report as never certified -- not "ready" from the stray row.
    assert body["readiness"] != "ready"


@pytest.mark.asyncio
async def test_impersonated_superuser_is_blocked_on_post(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    def _must_not_resolve() -> Any:
        raise AssertionError(
            "an impersonated superuser POST must never resolve a certification provider"
        )

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", _must_not_resolve
    )

    impersonating = AuthenticatedUser(
        user_id=platform_context.superuser.user_id,
        email=platform_context.superuser.email,
        org_id=platform_context.superuser.org_id,
        role="admin",
        is_superuser=True,
        impersonated_by=str(uuid.uuid4()),
    )
    _as(platform_context, impersonating)

    response = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_live_impersonation_context_blocks_post_without_jwt_claim(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    """Codex regression test (CHAOS-3265): the JWT ``impersonated_by`` claim
    is a static, token-issue-time signal. The AUTHORITATIVE, real-time signal
    is ``is_impersonating()``, set per-request by ``ImpersonationMiddleware``
    from the live Valkey-cached session. A guard that only checks the JWT
    claim is bypassable by a still-impersonating superuser whose current
    token happens not to carry that claim. This proves the guard also catches
    the live context, with ``impersonated_by`` left unset."""

    def _must_not_resolve() -> Any:
        raise AssertionError(
            "must never resolve a certification provider while a live "
            "impersonation context is active, even if impersonated_by is unset"
        )

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", _must_not_resolve
    )

    assert platform_context.superuser.impersonated_by is None
    token = set_impersonation_context(
        target_user_id=str(uuid.uuid4()),
        target_org_id=str(uuid.uuid4()),
        target_role="admin",
        real_user_id=platform_context.superuser.user_id,
    )
    try:
        response = await platform_context.client.post(
            "/api/v1/admin/platform/ask-dev/readiness"
        )
    finally:
        _impersonation_ctx.reset(token)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_stale_readiness_version_reports_stale_not_ready(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    """CHAOS-3254 (READINESS_VERSION v2->v3) exposed a gap: a stored record
    whose fingerprint matches but whose readiness_version is OUTDATED must
    never be reported as "ready". It must report stale_readiness with a
    safe, accurate remediation -- not a message implying something broken."""

    provider = FakeReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    async with platform_context.maker() as session:
        await SettingsAgentReadinessStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_READINESS_SETTING_KEY,
        ).save(
            AgentReadinessRecord(
                fingerprint=_FINGERPRINT,  # matches the current candidate
                readiness_version="stale-version-v2",  # does NOT match current
                checked_at="2026-01-01T00:00:00+00:00",
                outcome=AgentReadinessOutcome.READY,
            )
        )
        await session.commit()

    response = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] == "stale_readiness"
    assert body["readiness"] != "ready"
    assert body["safe_remediation"] is not None
    lowered = body["safe_remediation"].lower()
    assert "unavailable" not in lowered
    assert "endpoint" not in lowered
    assert "fail" not in lowered

    # A POST run afterward must also land on stale_readiness pre-certify,
    # and re-certify cleanly to ready (proving it's not permanently wedged).
    posted = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert posted.status_code == 200
    assert posted.json()["readiness"] == "ready"


@pytest.mark.asyncio
async def test_post_certifies_legacy_agent_role_and_projects_it(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    """CHAOS-3285: preflight now also certifies the legacy_agent role in the
    new per-role store, on the same resolved provider -- so the per-role
    projection reflects real results instead of staying not_yet_certified
    forever. intent/narrative have no probe yet (PR4) and stay
    not_yet_certified."""

    provider = FakeReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    before = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    before_roles = {entry["role"]: entry for entry in before.json()["role_readiness"]}
    assert before_roles["legacy_agent"]["state"] == "not_yet_certified"

    posted = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert posted.status_code == 200
    # CHAOS-3285 round 5 (Codex LOW): pin the full preflight call count (2
    # binary transport-echo + 4 legacy_agent role-probe calls, two
    # independent 2-call chains) so a chain silently vanishing in a future
    # refactor fails loudly here rather than passing unnoticed.
    assert provider.calls == 6
    posted_roles = {entry["role"]: entry for entry in posted.json()["role_readiness"]}
    assert posted_roles["legacy_agent"]["state"] == "ready"
    assert posted_roles["legacy_agent"]["safe_remediation"] is None
    assert posted_roles["legacy_agent"]["checked_at"] is not None
    assert posted_roles["intent_classification"]["state"] == "not_yet_certified"
    assert posted_roles["answer_frame_narrative"]["state"] == "not_yet_certified"

    async with platform_context.maker() as session:
        stored = await SettingsRoleCertificationStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
        ).load()
        record = stored.for_role(AgentRole.LEGACY_AGENT)
        assert record is not None
        assert record.certification_key == _FINGERPRINT

    after = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    after_roles = {entry["role"]: entry for entry in after.json()["role_readiness"]}
    assert after_roles["legacy_agent"]["state"] == "ready"


@pytest.mark.asyncio
async def test_role_certification_failure_does_not_regress_binary_readiness(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    """A failing provider must still land the existing binary readiness
    outcome cleanly (no 500), and the new per-role projection must report
    the same failure -- not silently swallow it, and not crash the route."""

    provider = FailingReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    posted = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert posted.status_code == 200
    posted_body = posted.json()
    assert posted_body["readiness"] == "unsupported_model"
    roles = {entry["role"]: entry for entry in posted_body["role_readiness"]}
    assert roles["legacy_agent"]["state"] == "unsupported_model"


@pytest.mark.asyncio
async def test_binary_ready_role_absent_reports_unavailable_not_ready(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    """CHAOS-3285 round 2 (Codex HIGH): the platform surface must agree with
    live selection -- a binary-ready record with no legacy_agent role
    certification at all must never report "ready" here."""

    provider = FakeReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        platform_ask_dev, "resolve_platform_certification_provider", resolve
    )

    async with platform_context.maker() as session:
        await SettingsAgentReadinessStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_READINESS_SETTING_KEY,
        ).save(
            AgentReadinessRecord(
                fingerprint=_FINGERPRINT,
                readiness_version=READINESS_VERSION,
                checked_at="2026-07-29T12:00:00+00:00",
                outcome=AgentReadinessOutcome.READY,
            )
        )
        # Deliberately NO role-certification row at all.
        await session.commit()

    response = await platform_context.client.get(
        "/api/v1/admin/platform/ask-dev/readiness"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["readiness"] != "ready"
    assert body["binary_transport_readiness"] == "ready"
    roles = {entry["role"]: entry for entry in body["role_readiness"]}
    assert roles["legacy_agent"]["state"] == "not_yet_certified"


# -- CHAOS-3522: platform allowance reconcile (operator escape hatch) -------


@pytest.mark.asyncio
async def test_org_admin_token_cannot_reach_allowance_reconcile(
    platform_context: PlatformContext,
):
    """Same regression class as readiness: an org-admin token must never
    reach another org's allowance counter -- reconcile is platform-wide,
    superuser-only, and deliberately absent from the org-scoped
    ``/admin/ask-dev`` surface (an org admin resetting their own spend cap
    enforcement would defeat the cap)."""

    _as(platform_context, platform_context.org_admin)
    org_id = str(uuid.uuid4())

    response = await platform_context.client.post(
        f"/api/v1/admin/platform/ask-dev/organizations/{org_id}/platform-allowance/reconcile"
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_reconcile_rejects_a_non_uuid_org_id(platform_context: PlatformContext):
    response = await platform_context.client.post(
        "/api/v1/admin/platform/ask-dev/organizations/not-a-uuid/platform-allowance/reconcile"
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reconcile_recomputes_from_dev_runs_and_overwrites_the_counter(
    platform_context: PlatformContext, monkeypatch: pytest.MonkeyPatch
):
    """The full round trip: seed dev_runs directly (bypassing any counter),
    seed a DELIBERATELY WRONG Valkey value, call reconcile, and assert the
    response AND the Valkey key both reflect dev_runs -- never the stale
    value that was there before."""

    fakeredis = pytest.importorskip("fakeredis")
    from dev_health_ops.api.dev.org_policy import platform_month_window
    from dev_health_ops.api.services import (
        askdev_allowance_counters as counters,
    )

    client = fakeredis.FakeAsyncValkey(server=fakeredis.FakeServer())
    monkeypatch.setattr(counters, "_client", client)
    monkeypatch.setattr(counters, "_circuit_open_until", 0.0)
    monkeypatch.setattr(counters, "_needs_recovery_recompute", False)

    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with platform_context.maker() as session:
        session.add_all(
            [
                DevRun(
                    request_id=uuid.uuid4(),
                    conversation_id=uuid.uuid4(),
                    org_id=org_id,
                    user_id=user_id,
                    state="completed",
                    estimated_cost_microusd=300_000,
                    provider_source="platform",
                    started_at=now,
                ),
                DevRun(
                    request_id=uuid.uuid4(),
                    conversation_id=uuid.uuid4(),
                    org_id=org_id,
                    user_id=user_id,
                    state="model_decision",
                    provider_source="platform",
                    started_at=now,
                ),
            ]
        )
        await session.commit()

    window_start, _reset_at = platform_month_window(now)
    key = counters._key(str(org_id), window_start)
    # A deliberately wrong stale value the reconcile must overwrite.
    await client.hset(key, mapping={"requests": 999, "cost_microusd": 999_000_000})

    response = await platform_context.client.post(
        f"/api/v1/admin/platform/ask-dev/organizations/{org_id}/platform-allowance/reconcile"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["request_used"] == 2
    # completed run: real cost (300_000). running run: worst-case reservation.
    from dev_health_ops.api.dev.org_policy import ASK_DEV_RUN_COST_HARD_MAX_MICROUSD

    assert body["cost_used_microusd"] == 300_000 + ASK_DEV_RUN_COST_HARD_MAX_MICROUSD

    persisted = await client.hmget(key, ["requests", "cost_microusd"])
    assert persisted == [b"2", str(body["cost_used_microusd"]).encode()]
