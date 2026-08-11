from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    QuestionClass,
    ToolID,
)
from dev_health_ops.api.dev.metrics.clickhouse import ClickHouseMetricSource
from dev_health_ops.api.dev.metrics.service import MetricQueryService
from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.status_change_service import (
    ChangeSummaryRequest,
    StatusChangeService,
    StatusResultState,
)
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.policy import AgentProviderSource
from dev_health_ops.llm.agent.scripted import ScriptedStep
from tests._chaos_3292_preflight import (
    ANSWER_ID,
    CONVERSATION_ID,
    PERMISSION_FINGERPRINT,
    USER_ID,
    Recorder,
    RecordingProvider,
    answer_payload,
    organization_resolution,
    request_for,
)

LIVE_ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"
LIVE_WORK_SCOPE_ID = "13e65c04-40ec-4a95-8216-f7c2ce233214"


class _ToolResultRecorder(Recorder):
    def __init__(self) -> None:
        super().__init__()
        self.tool_results: list[str] = []

    async def record_tool(self, **values: Any) -> None:
        self.tool_results.append(values["execution"].result.model_dump_json())


def _live_work_scope_overrides() -> dict[str, Any]:
    return {
        "direct_scope": "work_unit",
        "repositories": [],
        "entity_refs": [
            {
                "entity_type": "work_unit",
                "entity_id": LIVE_WORK_SCOPE_ID,
                "display_label": "work scope 13e65c04",
            }
        ],
        "team_ids": [],
        "time_range": {
            "start": "2026-07-14T00:00:00Z",
            "end": "2026-07-28T00:00:00Z",
            "timezone": "UTC",
        },
        "comparison_range": {
            "start": "2026-06-30T00:00:00Z",
            "end": "2026-07-14T00:00:00Z",
            "timezone": "UTC",
        },
    }


@pytest.mark.asyncio
async def test_change_summary_work_scope_does_not_require_repository() -> None:
    """A committed work scope stays work-scoped through the metric pass.

    The clients are intentionally opaque here: the real adapters fail closed
    at their source boundary, while the test exercises the production service
    composition and the exact validation path that previously raised before a
    source read. No repository is attached to the request and none is
    synthesized to make a repository-backed metric pass.
    """

    current_start = datetime(2026, 7, 14, tzinfo=UTC)
    current_end = datetime(2026, 7, 28, tzinfo=UTC)
    comparison_start = datetime(2026, 6, 30, tzinfo=UTC)
    scope = DevScope(
        schema_version="dev_scope.v1",
        organization_id="70d529e0-3c06-4597-8480-794fd02328b6",
        direct_scope=DirectScope.WORK_UNIT,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.WORK_UNIT,
                entity_id="13e65c04-40ec-4a95-8216-f7c2ce233214",
                display_label="work scope 13e65c04",
            )
        ],
        time_range=DevTimeRange(
            start=current_start,
            end=current_end,
            timezone="UTC",
        ),
        comparison_range=DevTimeRange(
            start=comparison_start,
            end=current_start,
            timezone="UTC",
        ),
    )
    request = ChangeSummaryRequest(
        scope=scope,
        current_start=current_start,
        current_end=current_end,
        comparison_start=comparison_start,
        comparison_end=current_start,
        max_items=100,
    )
    metric_service = MetricQueryService(ClickHouseMetricSource(object()))
    service = StatusChangeService(
        ClickHouseStatusChangeSource(object()), metric_service=metric_service
    )

    result = await service.change_summary(
        scope.organization_id, "live-shaped-permissions", request
    )

    assert result.state is StatusResultState.DEGRADED
    assert result.changes == ()
    assert result.warnings == (
        "Observed-change scope was not widened.",
        "source_unavailable",
    )
    assert all(
        "Metric scope does not resolve" not in warning for warning in result.warnings
    )


@pytest.mark.asyncio
async def test_public_change_summary_work_scope_is_repository_free_and_token_safe(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Replay both live failures through the real production runtime."""

    script_id = "change-summary-work-scope-public"
    answer = answer_payload(script_id=script_id)
    answer.update(
        {
            "status": "degraded",
            "direct_summary": "Some metric data was temporarily unavailable.",
            "claims": [],
            "metrics": [],
            "evidence": [],
        }
    )
    provider = RecordingProvider(
        [
            ScriptedStep(
                decision=AgentToolRequest(
                    tool_id=ToolID.CHANGE_SUMMARY.value,
                    arguments={"limit": 25, "include_comparison": True},
                    call_id="tool_call_01",
                ),
                usage=AgentUsage(input_tokens=100, output_tokens=10),
            ),
            ScriptedStep(decision=AgentFinalAnswer(answer)),
        ],
        script_id=script_id,
    )

    async def resolve_provider(_session: Any, *, org_id: str):
        assert org_id == LIVE_ORG_ID
        return ProductionProviderResolution(
            provider=cast(Any, provider),
            source=AgentProviderSource.PLATFORM,
            family="scripted",
            model="scripted",
            provider_label="Scripted",
            model_label="scripted",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret-32-bytes")
    runtime = await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id=LIVE_ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        clickhouse=cast(Any, object()),
    )
    request = request_for(
        "What changed in this work scope?",
        question_class=QuestionClass.OBSERVED_CHANGE,
        scope_overrides=_live_work_scope_overrides(),
        organization_id=LIVE_ORG_ID,
    )
    resolution = organization_resolution(request.scope)

    async def resolve_scope(**_values: Any):
        return resolution

    runtime.scope_resolver = resolve_scope
    recorder = _ToolResultRecorder()

    async def discard_event(_event: Any) -> None:
        return None

    with caplog.at_level(logging.ERROR):
        result = await runtime.run(
            request=request,
            org_id=LIVE_ORG_ID,
            user_id=USER_ID,
            permission_fingerprint=PERMISSION_FINGERPRINT,
            run_id="run_change_summary_work_scope",
            conversation_id=CONVERSATION_ID,
            answer_id=ANSWER_ID,
            cancellation=asyncio.Event(),
            recorder=recorder,
            event_sink=discard_event,
        )

    assert result.state is RunState.COMPLETED
    assert result.error is None
    assert result.answer is not None
    assert result.answer.direct_summary == (
        "Some metric data was temporarily unavailable."
    )
    assert "source_unavailable" not in result.answer.model_dump_json()
    assert recorder.tool_results
    assert all("source_unavailable" not in item for item in recorder.tool_results)
    assert any(
        "Some metric data was temporarily unavailable." in text
        for text in provider.user_texts
    )
    assert all("source_unavailable" not in text for text in provider.user_texts)
    assert not any(
        record.getMessage()
        in {
            "ask_dev.orchestrator.tool_executor_fault",
            "ask_dev.orchestrator.internal_token_leak",
        }
        for record in caplog.records
    )

    await runtime.aclose()
