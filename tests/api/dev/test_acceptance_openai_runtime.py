from __future__ import annotations

import asyncio
import json
import threading
import uuid
from collections.abc import AsyncIterator, Iterator
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevContractVersions,
    DevMessageRequest,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    ToolID,
)
from dev_health_ops.api.dev.orchestrator import DevOrchestrator, RunState
from dev_health_ops.api.dev.production_runtime import (
    ACCEPTANCE_OPENAI_MODEL,
    resolve_certification_provider,
    resolve_production_provider,
)
from dev_health_ops.api.dev.router import get_dev_capability_runtime
from dev_health_ops.api.dev.runtime import DevRuntimeUnavailable
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentToolDefinition,
    AgentToolRequest,
)
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.agent.policy import AgentProviderSource
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    AgentReadinessService,
    SettingsAgentReadinessStore,
)
from dev_health_ops.llm.agent.scripted_openai_service import ScriptedOpenAIServer
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import Setting

_ORG_ID = "org_fullchaos"
_ACCEPTANCE_KEY = "ask-dev-acceptance-test-key"
_ORACLE = json.loads(
    (
        Path(__file__).resolve().parents[2] / "acceptance" / "ask-dev-oracle.v1.json"
    ).read_text(encoding="utf-8")
)
_ACCEPTANCE_ENV_KEYS = (
    "ASK_DEV_LIVE_ACCEPTANCE",
    "ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL",
    "ASK_DEV_ACCEPTANCE_OPENAI_API_KEY",
    "LLM_PROVIDER",
    "LLM_MODEL",
    "LLM_BASE_URL",
    "LLM_API_KEY",
    "OPENAI_MODEL",
    "OPENAI_BASE_URL",
    "OPENAI_API_KEY",
)


@pytest.fixture
def scripted_openai_server() -> Iterator[ScriptedOpenAIServer]:
    server = ScriptedOpenAIServer(_ACCEPTANCE_KEY)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest_asyncio.fixture
async def settings_session(tmp_path: Path) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'ask-dev-acceptance.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=[cast(Any, Setting.__table__)]
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


@pytest.fixture(autouse=True)
def clean_provider_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ACCEPTANCE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _configure_acceptance(monkeypatch: pytest.MonkeyPatch, *, base_url: str) -> None:
    monkeypatch.setenv("ASK_DEV_LIVE_ACCEPTANCE", "1")
    monkeypatch.setenv("ENVIRONMENT", "acceptance")
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL", base_url)
    monkeypatch.setenv("ASK_DEV_ACCEPTANCE_OPENAI_API_KEY", _ACCEPTANCE_KEY)


@pytest.mark.asyncio
async def test_acceptance_openai_runs_real_readiness_grounding_and_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    scripted_openai_server: ScriptedOpenAIServer,
    settings_session: AsyncSession,
) -> None:
    host, port = cast(tuple[str, int], scripted_openai_server.server_address)
    _configure_acceptance(monkeypatch, base_url=f"http://{host}:{port}/v1")

    certification = await resolve_certification_provider(
        settings_session, org_id=_ORG_ID
    )
    assert isinstance(certification.provider, OpenAICompatibleAgentProvider)
    assert certification.family == "openai"
    assert certification.source is AgentProviderSource.PLATFORM
    assert certification.model == ACCEPTANCE_OPENAI_MODEL
    assert certification.provider_label == "OpenAI compatible"
    assert certification.model_label == ACCEPTANCE_OPENAI_MODEL
    assert (
        certification.provider.capabilities.disclosure_key
        == "ask_dev_scripted_acceptance"
    )
    fingerprint = certification.readiness_fingerprint
    provider_fingerprint = certification.provider.provider_fingerprint

    readiness = await AgentReadinessService(
        SettingsAgentReadinessStore(SettingsService(settings_session, _ORG_ID)),
        org_id=_ORG_ID,
    ).certify(
        certification.provider,
        provider_name=certification.family,
        model=certification.model,
        fingerprint=fingerprint,
    )
    await certification.provider.aclose()
    assert readiness.outcome is AgentReadinessOutcome.READY

    production = await resolve_production_provider(settings_session, org_id=_ORG_ID)
    assert isinstance(production.provider, OpenAICompatibleAgentProvider)
    assert production.family == "openai"
    assert production.source is AgentProviderSource.PLATFORM
    assert production.readiness_fingerprint == fingerprint
    assert production.provider.provider_fingerprint == provider_fingerprint

    tool = AgentToolDefinition(
        tool_id="status_snapshot.v1",
        description="Return a grounded status snapshot.",
        input_schema={"type": "object", "additionalProperties": False},
    )
    response_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "value"],
        "properties": {
            "kind": {"const": "final_answer"},
            "value": {"type": "object"},
        },
    }
    first = await production.provider.decide(
        [AgentMessage(AgentMessageRole.USER, "Ground the seeded project status.")],
        [tool],
        response_schema,
        2,
        256,
    )
    assert isinstance(first.decision, AgentToolRequest)
    assert first.decision.tool_id == "status_snapshot.v1"
    second = await production.provider.decide(
        [
            AgentMessage(AgentMessageRole.USER, "Ground the seeded project status."),
            AgentMessage(
                AgentMessageRole.ASSISTANT,
                "",
                tool_request=first.decision,
            ),
            AgentMessage(
                AgentMessageRole.TOOL,
                json.dumps(
                    {
                        "tool_id": "status_snapshot.v1",
                        "status": "success",
                        "evidence": [],
                        "metrics": [],
                    }
                ),
                tool_call_id=first.decision.call_id,
            ),
        ],
        [tool],
        response_schema,
        2,
        256,
    )
    assert isinstance(second.decision, AgentFinalAnswer)
    assert second.decision.value["status"] == "degraded"
    assert second.decision.value["metrics"] == []
    assert second.decision.value["evidence"] == []
    assert second.decision.value["coverage"]["available_source_count"] == 0
    await production.provider.aclose()

    orchestrated = await resolve_production_provider(settings_session, org_id=_ORG_ID)

    executed_requests: list[DevToolRequest] = []

    async def execute_registered_tool(
        _context: Any, request: DevToolRequest
    ) -> DevToolResult:
        executed_requests.append(request)
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        if request.tool_id is ToolID.QUERY_METRIC:
            payload["evidence"] = []
        elif request.tool_id is ToolID.SEARCH_EVIDENCE:
            payload["metrics"] = []
            payload["metric_definitions"] = []
            payload["evidence"][0]["entity_id"] = "meridian/web-app-100"
        elif request.tool_id is ToolID.DATA_HEALTH:
            payload["metrics"] = []
            payload["metric_definitions"] = []
            payload["evidence"] = []
            payload["data_health"] = [
                {
                    "source_system": "work_items",
                    "freshness": "fresh",
                    "last_successful_at": positive_fixtures()["dev_evidence_ref.v1"][
                        "observed_at"
                    ],
                    "coverage": 1.0,
                    "warning": None,
                }
            ]
        else:
            pytest.fail(f"unexpected scripted tool request: {request.tool_id}")
        return DevToolResult.model_validate(payload)

    async def resolve_scope(**_values: Any) -> DevScopeResolution:
        return DevScopeResolution.model_validate(
            positive_fixtures()["dev_scope_resolution.v1"]
        )

    orchestrator = DevOrchestrator(
        provider=orchestrated.provider,
        provider_source="platform",
        provider_family="openai",
        registry=AskDevToolRegistry(
            {tool_id: execute_registered_tool for tool_id in ToolID}
        ),
        scope_resolver=resolve_scope,
        versions=DevContractVersions.model_validate(
            positive_fixtures()["dev_answer.v1"]["versions"]
        ),
    )
    result = await orchestrator.run(
        request=DevMessageRequest.model_validate(
            positive_fixtures()["dev_message_request.v1"]
        ),
        org_id=_ORG_ID,
        user_id="user_01",
        permission_fingerprint="permissions_01",
        run_id="run_01",
        conversation_id="conversation_01",
        answer_id="answer_01",
        cancellation=asyncio.Event(),
    )
    await orchestrated.provider.aclose()
    assert result.state is RunState.COMPLETED
    assert result.answer is not None
    assert result.answer.metrics
    assert result.answer.evidence
    assert result.answer.model.provider_family == "openai"
    assert result.answer.model.provider_source == "platform"
    metric = next(
        item
        for item in result.answer.metrics
        if item.metric_id.value == _ORACLE["expected_metric_id"]
    )
    assert metric.value is not None
    assert metric.comparison_value is not None
    direction = (
        "increased"
        if metric.value > metric.comparison_value
        else "decreased"
        if metric.value < metric.comparison_value
        else "was unchanged"
    )
    expected_summary = (
        f"Completed work {direction} from {metric.comparison_value:g} to "
        f"{metric.value:g} items in the selected time range."
    )
    assert result.answer.direct_summary == expected_summary
    assert len(result.answer.claims) == 1
    claim = result.answer.claims[0]
    assert claim.kind.value == _ORACLE["expected_claim_kind"]
    assert claim.text == expected_summary
    assert claim.metric_ref_ids == [metric.metric_ref_id]
    assert len(claim.evidence_ref_ids) == 1
    cited_evidence = next(
        item
        for item in result.answer.evidence
        if item.evidence_ref_id == claim.evidence_ref_ids[0]
    )
    assert _ORACLE["expected_evidence_entity_fragment"] in cited_evidence.entity_id
    assert result.answer.coverage.required_source_count == 3
    assert result.answer.coverage.available_source_count == 3
    assert result.answer.coverage.stale_required_sources == []
    assert [request.tool_id for request in executed_requests] == [
        ToolID(tool_id) for tool_id in _ORACLE["required_tool_ids"]
    ]
    assert executed_requests[0].include_comparison is True
    assert executed_requests[1].query == "meridian/web-app"
    changed_summary = result.answer.model_copy(
        update={"direct_summary": f"{result.answer.direct_summary} changed"}
    )
    with pytest.raises(AssertionError):
        assert changed_summary.direct_summary == expected_summary

    capability = await get_dev_capability_runtime(
        AuthenticatedUser(
            user_id=str(uuid.uuid4()),
            email="acceptance@example.com",
            org_id=_ORG_ID,
            role="admin",
        ),
        settings_session,
    )
    assert capability.readiness == "ready"
    assert capability.effective_provider_label == "OpenAI compatible"
    assert capability.effective_model_label == ACCEPTANCE_OPENAI_MODEL
    assert capability.provider_source == "platform"
    assert len(scripted_openai_server.requests) == 8


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_guard",
    [
        "ASK_DEV_LIVE_ACCEPTANCE",
        "ENVIRONMENT",
        "LLM_PROVIDER",
        "ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL",
        "ASK_DEV_ACCEPTANCE_OPENAI_API_KEY",
    ],
)
async def test_acceptance_openai_requires_every_guard(
    monkeypatch: pytest.MonkeyPatch,
    settings_session: AsyncSession,
    missing_guard: str,
) -> None:
    _configure_acceptance(monkeypatch, base_url="http://127.0.0.1:8001/v1")
    monkeypatch.delenv(missing_guard, raising=False)
    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await resolve_certification_provider(settings_session, org_id=_ORG_ID)
    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "base_url",
    [
        "http://127.0.0.2:8001/v1",
        "http://10.0.0.10:8001/v1",
        "http://169.254.169.254/latest/meta-data/v1",
        "http://ask-dev-scripted-openai.example.com:8001/v1",
        "http://user@localhost:8001/v1",
        "http://localhost:8001/not-v1",
    ],
)
async def test_acceptance_openai_rejects_non_allowlisted_or_ambiguous_targets(
    monkeypatch: pytest.MonkeyPatch,
    settings_session: AsyncSession,
    base_url: str,
) -> None:
    _configure_acceptance(monkeypatch, base_url=base_url)
    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await resolve_certification_provider(settings_session, org_id=_ORG_ID)
    assert exc_info.value.code == "provider_not_configured"


@pytest.mark.asyncio
async def test_operator_platform_base_url_accepts_loopback(
    monkeypatch: pytest.MonkeyPatch,
    settings_session: AsyncSession,
) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "customer-model")
    monkeypatch.setenv("LLM_API_KEY", "customer-key")
    monkeypatch.setenv("LLM_BASE_URL", "http://127.0.0.1:8001/v1")
    resolution = await resolve_certification_provider(settings_session, org_id=_ORG_ID)
    provider = cast(OpenAICompatibleAgentProvider, resolution.provider)
    try:
        assert resolution.family == "openai"
        assert resolution.source is AgentProviderSource.PLATFORM
        assert resolution.model == "customer-model"
        assert provider.base_url == "http://127.0.0.1:8001/v1"
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_acceptance_endpoint_variables_are_inert_in_customer_runtime(
    monkeypatch: pytest.MonkeyPatch,
    settings_session: AsyncSession,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "customer-model")
    monkeypatch.setenv("LLM_API_KEY", "customer-key")
    monkeypatch.setenv("ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL", "http://127.0.0.1:8001/v1")
    monkeypatch.setenv("ASK_DEV_ACCEPTANCE_OPENAI_API_KEY", _ACCEPTANCE_KEY)

    resolution = await resolve_certification_provider(settings_session, org_id=_ORG_ID)
    provider = cast(OpenAICompatibleAgentProvider, resolution.provider)
    try:
        assert resolution.family == "openai"
        assert resolution.source is AgentProviderSource.PLATFORM
        assert resolution.model == "customer-model"
        assert provider.base_url == ""
        assert provider.capabilities.disclosure_key == "openai_compatible"
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_partial_acceptance_gate_never_falls_back_to_live_platform(
    monkeypatch: pytest.MonkeyPatch,
    settings_session: AsyncSession,
) -> None:
    _configure_acceptance(monkeypatch, base_url="http://127.0.0.1:8001/v1")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("LLM_MODEL", "customer-model")
    monkeypatch.setenv("LLM_API_KEY", "customer-key")
    with pytest.raises(DevRuntimeUnavailable) as exc_info:
        await resolve_certification_provider(settings_session, org_id=_ORG_ID)
    assert exc_info.value.code == "provider_not_configured"
