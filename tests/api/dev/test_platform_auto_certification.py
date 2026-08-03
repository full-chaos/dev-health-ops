"""CHAOS-3358: the platform provider re-certifies itself.

The advisory change stops a stale platform certification from blocking runs.
On its own that would leave the operator badge parked on "stale, run
preflight" forever, because nothing else ever noticed. These controls cover
the other half: the record heals itself, without a human pressing a button,
and without stampeding the provider while it does.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import platform_auto_certification, production_runtime
from dev_health_ops.api.dev.platform_auto_certification import PlatformAutoCertifier
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
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
    AgentReadinessOutcome,
    SettingsAgentReadinessStore,
)
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
    RoleCertificationState,
    SettingsRoleCertificationStore,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import Setting
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

_TABLES = tables_of(User, Organization, Setting)
_FINGERPRINT = "platform-readiness-fingerprint"


class FakeReadinessProvider:
    """Satisfies both probes the platform certification runs: the binary
    transport echo (calls 1-2) and the production-sized legacy_agent role
    probe (calls 3-6). Mirrors the fake the Platform Admin route's own tests
    use, on purpose -- the two callers must be able to certify the same
    provider to the same verdict."""

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


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest_asyncio.fixture
async def settings_maker(tmp_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'platform-auto-certification.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield maker
    await engine.dispose()


@pytest.fixture(autouse=True)
def _isolated_certifier():
    platform_auto_certification.reset_platform_auto_certifier()
    yield
    platform_auto_certification.reset_platform_auto_certifier()


def _session_factory(maker: async_sessionmaker[AsyncSession]):
    @contextlib.asynccontextmanager
    async def factory() -> AsyncIterator[AsyncSession]:
        async with maker() as session:
            yield session
            await session.commit()

    return factory


async def _write_stale_record(maker: async_sessionmaker[AsyncSession]) -> None:
    async with maker() as session:
        await SettingsService(session, PLATFORM_SETTINGS_ORG_ID).set(
            PLATFORM_READINESS_SETTING_KEY,
            json.dumps(
                {
                    "fingerprint": "fingerprint-from-the-previous-readiness-version",
                    "readiness_version": "ask-dev-readiness.v0",
                    "checked_at": "2026-07-29T12:00:00+00:00",
                    "outcome": "ready",
                    "safe_error_code": None,
                }
            ),
            category="llm",
            description="Safe Ask Dev provider certification result",
        )
        await session.commit()


async def _load_record(maker: async_sessionmaker[AsyncSession]):
    async with maker() as session:
        return await SettingsAgentReadinessStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_READINESS_SETTING_KEY,
        ).load()


async def _load_role_record(maker: async_sessionmaker[AsyncSession]):
    async with maker() as session:
        profile = await SettingsRoleCertificationStore(
            SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
            key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
        ).load()
    return profile.for_role(AgentRole.LEGACY_AGENT)


@pytest.mark.asyncio
async def test_stale_record_self_heals_with_no_admin_action(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The headline control: a stale stored certification plus a working
    provider becomes a CURRENT record after one scheduled attempt, with
    nobody pressing preflight. Observed by reading the store, not by
    trusting a return value."""

    provider = FakeReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )
    await _write_stale_record(settings_maker)

    stale = await _load_record(settings_maker)
    assert stale is not None
    assert (
        stale.is_current(fingerprint=_FINGERPRINT, readiness_version=READINESS_VERSION)
        is False
    )

    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker), clock=FakeClock()
    )
    task = certifier.schedule()
    assert task is not None
    await task

    healed = await _load_record(settings_maker)
    assert healed is not None
    assert healed.outcome is AgentReadinessOutcome.READY
    assert (
        healed.is_current(fingerprint=_FINGERPRINT, readiness_version=READINESS_VERSION)
        is True
    )
    # The per-role certification the runtime and the badge both read must
    # heal too -- a binary-only heal would leave the badge stale forever.
    role_record = await _load_role_record(settings_maker)
    assert role_record is not None
    assert role_record.state is RoleCertificationState.COMPATIBLE
    assert role_record.is_current(certification_key=_FINGERPRINT) is True
    assert provider.closed is True


@pytest.mark.asyncio
async def test_concurrent_resolutions_certify_the_provider_once(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No stampede: eight concurrent schedule() calls -- what a burst of Ask
    Dev questions right after a deploy looks like -- must produce exactly one
    certification, not eight sets of live provider round-trips."""

    resolves = 0
    release = asyncio.Event()

    async def resolve() -> ProductionProviderResolution:
        nonlocal resolves
        resolves += 1
        # Hold the attempt open so every sibling schedule() below runs while
        # this one is genuinely in flight; without that the test could pass
        # on timing rather than on the single-flight guard.
        await release.wait()
        return _resolution(FakeReadinessProvider())

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )

    # min_interval_seconds=0 disables the throttle deliberately. With the
    # throttle active, the throttle alone suppresses every sibling call on a
    # frozen clock and this test passes whether or not single-flight exists --
    # it would be testing the wrong guard. Zero leaves single-flight as the
    # only thing that can suppress these.
    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker),
        min_interval_seconds=0.0,
        clock=FakeClock(),
    )

    first = certifier.schedule()
    assert first is not None
    await asyncio.sleep(0)  # let the task reach the awaited resolve

    suppressed = [certifier.schedule() for _ in range(7)]
    assert suppressed == [None] * 7

    release.set()
    await first
    assert resolves == 1


@pytest.mark.asyncio
async def test_a_failed_attempt_is_throttled_before_it_retries(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single-flight alone does not bound a FAILING provider: once the
    attempt finishes, the record is still not current, so the very next
    request would schedule another one. The throttle is what turns that into
    a bounded retry rate."""

    attempts = 0

    async def resolve() -> ProductionProviderResolution:
        nonlocal attempts
        attempts += 1
        return _resolution(FailingReadinessProvider())

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )

    clock = FakeClock()
    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker),
        min_interval_seconds=300.0,
        clock=clock,
    )

    first = certifier.schedule()
    assert first is not None
    await first
    assert attempts == 1

    # Attempt finished, record still not current, nothing in flight -- only
    # the throttle stands between here and a retry per request.
    clock.advance(299.0)
    assert certifier.schedule() is None
    assert attempts == 1

    clock.advance(2.0)
    later = certifier.schedule()
    assert later is not None
    await later
    assert attempts == 2


@pytest.mark.asyncio
async def test_a_failing_provider_records_the_failure_and_raises_nothing(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto-certification failure is a diagnostic, never an exception the
    caller has to survive: the task completes, the record shows the failure
    with a reason, and the run that scheduled it is unaffected."""

    provider = FailingReadinessProvider()

    async def resolve() -> ProductionProviderResolution:
        return _resolution(provider)

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )

    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker), clock=FakeClock()
    )
    task = certifier.schedule()
    assert task is not None
    await task
    assert task.exception() is None

    record = await _load_record(settings_maker)
    assert record is not None
    assert record.outcome is AgentReadinessOutcome.FAILED
    assert record.safe_error_code == "invalid_response"
    assert (
        record.is_current(fingerprint=_FINGERPRINT, readiness_version=READINESS_VERSION)
        is False
    )


@pytest.mark.asyncio
async def test_infrastructure_failure_never_escapes_the_task(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failure AROUND certification (database down, provider construction
    blowing up) is not persisted by certify() itself, so it has to be caught
    here or it would surface as an unretrieved task exception."""

    async def resolve() -> ProductionProviderResolution:
        raise RuntimeError("postgres is unreachable")

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )

    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker), clock=FakeClock()
    )
    with caplog.at_level(
        "WARNING", logger="dev_health_ops.api.dev.platform_auto_certification"
    ):
        task = certifier.schedule()
        assert task is not None
        await task

    assert task.exception() is None
    assert any(
        "Automatic platform Ask Dev re-certification failed" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_an_unconfigured_platform_leaves_the_stored_record_untouched(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing to certify (no operator provider, or an explicit "none") must
    not overwrite a real prior verdict with a synthetic one -- and must be a
    clean "nothing to do", not an error the caller has to absorb."""

    async def resolve() -> ProductionProviderResolution:
        raise DevRuntimeUnavailable(
            "provider_not_configured", "No certified Ask Dev model is ready."
        )

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )
    await _write_stale_record(settings_maker)
    before = await _load_record(settings_maker)

    # Asserted directly on certify_platform_provider, not only through the
    # scheduler: the scheduler's blanket except would swallow a propagating
    # DevRuntimeUnavailable and this would read as "untouched" for the wrong
    # reason -- an unconfigured platform is an expected state, not a failure.
    async with settings_maker() as session:
        assert await production_runtime.certify_platform_provider(session) is False
        await session.commit()
    assert await _load_record(settings_maker) == before

    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker), clock=FakeClock()
    )
    task = certifier.schedule()
    assert task is not None
    await task

    assert await _load_record(settings_maker) == before


@pytest.mark.asyncio
async def test_automatic_and_manual_certification_write_the_same_record(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Differential control on the two callers of the shared certify path.

    The whole premise of making the button optional is that the record the
    automatic path writes is the record the button writes. If these two ever
    diverge -- a different fingerprint, a missing role row, a different
    outcome -- the badge would report one thing after a button press and
    another after a self-heal. Compared field by field except checked_at,
    which is a timestamp and must differ.
    """

    async def resolve() -> ProductionProviderResolution:
        return _resolution(FakeReadinessProvider())

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )

    # The manual path: exactly what the Platform Admin POST route runs.
    async with settings_maker() as session:
        await production_runtime.certify_platform_resolution(
            session, _resolution(FakeReadinessProvider())
        )
        await session.commit()
    manual = await _load_record(settings_maker)
    manual_role = await _load_role_record(settings_maker)

    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker), clock=FakeClock()
    )
    task = certifier.schedule()
    assert task is not None
    await task
    automatic = await _load_record(settings_maker)
    automatic_role = await _load_role_record(settings_maker)

    assert manual is not None and automatic is not None
    assert manual.fingerprint == automatic.fingerprint == _FINGERPRINT
    assert manual.readiness_version == automatic.readiness_version
    assert manual.outcome is automatic.outcome
    assert manual.safe_error_code == automatic.safe_error_code

    assert manual_role is not None and automatic_role is not None
    assert manual_role.role is automatic_role.role
    assert manual_role.certification_key == automatic_role.certification_key
    assert manual_role.readiness_version == automatic_role.readiness_version
    assert manual_role.state is automatic_role.state
    assert manual_role.safe_error_code == automatic_role.safe_error_code


@pytest.mark.asyncio
async def test_the_record_reflects_the_configuration_actually_probed(
    settings_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """It must be impossible for the automatic path to certify one
    configuration and file the result under another -- a "lying record"
    would read as green for a configuration that was never exercised. The
    fingerprint written comes off the same resolution whose provider was
    probed, so a resolution change between attempts changes the record."""

    resolutions = [
        _resolution(FakeReadinessProvider()),
        ProductionProviderResolution(
            provider=FakeReadinessProvider(),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="rotated-model",
            provider_label="OpenAI compatible",
            model_label="rotated-model",
            readiness_fingerprint="fingerprint-after-the-operator-rotated-the-model",
        ),
    ]

    async def resolve() -> ProductionProviderResolution:
        return resolutions.pop(0)

    monkeypatch.setattr(
        production_runtime, "resolve_platform_certification_provider", resolve
    )

    clock = FakeClock()
    certifier = PlatformAutoCertifier(
        session_factory=_session_factory(settings_maker), clock=clock
    )
    first = certifier.schedule()
    assert first is not None
    await first
    record = await _load_record(settings_maker)
    assert record is not None and record.fingerprint == _FINGERPRINT

    clock.advance(1_000.0)
    second = certifier.schedule()
    assert second is not None
    await second
    rotated = await _load_record(settings_maker)
    assert rotated is not None
    assert rotated.fingerprint == "fingerprint-after-the-operator-rotated-the-model"
    # And the previously-current fingerprint is no longer claimed.
    assert (
        rotated.is_current(
            fingerprint=_FINGERPRINT, readiness_version=READINESS_VERSION
        )
        is False
    )
