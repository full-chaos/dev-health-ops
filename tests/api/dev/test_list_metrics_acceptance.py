"""CHAOS-3262 end-to-end regression: list_metrics.v1 through the real
OpenAI-compatible provider adapter and the real production tool registry.

Reproduces the reported failure: the literal question "Which Ask Dev metrics
are available?" must invoke list_metrics.v1 through the production registry
and complete, instead of failing before any tool call is recorded
(``dev_error.v1`` / ``safe_error_code=tool_unavailable`` / ``tool_call_count=0``).
"""

from __future__ import annotations

import asyncio
import secrets
import threading
from collections.abc import Iterator
from copy import deepcopy
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevContractVersions,
    DevMessageRequest,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
)
from dev_health_ops.api.dev.orchestrator import DevOrchestrator, RunState
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.agent.policy import AgentProviderSource
from dev_health_ops.llm.agent.scripted_openai_service import (
    LIST_METRICS_QUESTION,
    ScriptedOpenAIServer,
)

_ORG_ID = "org_fullchaos"
_ACCEPTANCE_KEY = secrets.token_hex(16)


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


@pytest.mark.asyncio
async def test_list_metrics_question_executes_through_the_real_provider_and_registry(
    monkeypatch: pytest.MonkeyPatch,
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    host, port = cast(tuple[str, int], scripted_openai_server.server_address)
    monkeypatch.setenv("JWT_SECRET_KEY", secrets.token_hex(32))

    provider = OpenAICompatibleAgentProvider(
        api_key=_ACCEPTANCE_KEY,
        model="ask-dev-scripted-v1",
        base_url=f"http://{host}:{port}/v1",
    )

    async def resolve_provider(_session: Any, *, org_id: str) -> Any:
        assert org_id == _ORG_ID
        return ProductionProviderResolution(
            provider=provider,
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="ask-dev-scripted-v1",
            provider_label="OpenAI compatible",
            model_label="ask-dev-scripted-v1",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )

    # The production registry (real MetricQueryService/list_metrics wiring)
    # only needs a real ClickHouse connection for tools this scenario never
    # calls; list_metrics.v1 itself is a pure code-owned catalog read.
    runtime = await production_runtime.build_production_runtime(
        cast(AsyncSession, object()),
        org_id=_ORG_ID,
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )

    executed: list[tuple[DevToolRequest, DevToolResult]] = []
    real_execute = runtime.registry.execute

    async def spying_execute(request: DevToolRequest, context: Any) -> Any:
        execution = await real_execute(request, context)
        executed.append((request, execution.result))
        return execution

    monkeypatch.setattr(runtime.registry, "execute", spying_execute)

    # list_metrics.v1 is filtered by scope: organization scope with no team
    # filter is required to see the full eight-metric V1 catalog (a
    # repository/team-scoped resolution legitimately narrows the catalog).
    org_scope = deepcopy(positive_fixtures()["dev_scope.v1"])
    org_scope.update(
        {
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "team_ids": [],
            "surface_context": None,
        }
    )
    resolution_payload = deepcopy(positive_fixtures()["dev_scope_resolution.v1"])
    resolution_payload.update(
        {"requested_scope": org_scope, "resolved_scope": org_scope}
    )
    resolution = DevScopeResolution.model_validate(resolution_payload)

    async def resolve_scope(**_values: Any) -> DevScopeResolution:
        return resolution

    orchestrator = DevOrchestrator(
        provider=runtime.provider,
        provider_source=runtime.provider_source,
        provider_family=runtime.provider_family,
        registry=runtime.registry,
        scope_resolver=resolve_scope,
        versions=DevContractVersions.model_validate(
            positive_fixtures()["dev_answer.v1"]["versions"]
        ),
    )

    request = DevMessageRequest.model_validate(
        positive_fixtures()["dev_message_request.v1"]
        | {
            "question": LIST_METRICS_QUESTION,
            "question_class": "registered_statistics",
            "requested_metric_ids": [],
        }
    )

    try:
        result = await orchestrator.run(
            request=request,
            org_id=_ORG_ID,
            user_id="user_01",
            permission_fingerprint="permissions_01",
            run_id="run_01",
            conversation_id="conversation_01",
            answer_id="answer_01",
            cancellation=asyncio.Event(),
        )
    finally:
        await runtime.aclose()

    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.answer is not None

    # Exactly one tool call was executed, and it succeeded before the answer.
    assert result.tool_call_count == 1
    assert len(executed) == 1
    executed_request, executed_result = executed[0]
    assert executed_request.tool_id.value == "list_metrics.v1"
    assert executed_result.status == "success"
    assert len(executed_result.metric_definitions) == 8
    metric_ids = {item.metric_id.value for item in executed_result.metric_definitions}
    assert len(metric_ids) == 8  # no duplicate metric IDs in the catalog

    # The catalog was genuinely available: the answer must claim complete
    # coverage, not merely complete without saying so (a scripted answer
    # that silently downgraded to "degraded" while keeping 1-of-1 coverage
    # would be just as wrong as the original tool_unavailable failure).
    assert result.answer.status == "complete"
    assert "8" in result.answer.direct_summary

    assert result.answer.coverage.required_source_count == 1
    assert result.answer.coverage.available_source_count == 1
    assert result.answer.coverage.unavailable_required_sources == []
