from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevError, DevToolRequest, DevToolResult
from dev_health_ops.api.dev.orchestrator import RunState
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.persistence.service import DevPersistenceService
from dev_health_ops.api.dev.prompts import PROMPT_VERSION
from dev_health_ops.api.dev.tool_registry import ToolExecution
from dev_health_ops.llm.agent.contracts import AgentUsage


def _recorder(service: Any) -> PersistenceRunRecorder:
    return PersistenceRunRecorder(
        cast(DevPersistenceService, service),
        org_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        conversation_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        provider_source="platform",
        started=0.0,
    )


@pytest.mark.asyncio
async def test_recorder_persists_only_safe_versioned_terminal_metadata() -> None:
    service = AsyncMock()
    recorder = _recorder(service)
    error = DevError(
        schema_version="dev_error.v1",
        request_id="request_01",
        code="provider_unavailable",
        safe_message="The provider is temporarily unavailable.",
        retryable=True,
    )
    await recorder.transition(RunState.MODEL_DECISION)
    await recorder.terminal(
        state=RunState.FAILED,
        answer=None,
        error=error,
        usage=AgentUsage(input_tokens=12, output_tokens=3),
        tool_call_count=0,
        provider_fingerprint="provider-safe-id",
        model_fingerprint="model-safe-id",
        prompt_checksum="a" * 64,
    )

    assert service.update_run.await_count == 2
    terminal = service.update_run.await_args_list[-1].kwargs
    assert terminal["state"] == "failed"
    assert terminal["prompt_version"] == f"{PROMPT_VERSION}:sha256:" + "a" * 64
    assert terminal["provider_fingerprint"].startswith("sha256:")
    assert terminal["model_fingerprint"].startswith("sha256:")
    assert terminal["safe_error_code"] == "provider_unavailable"
    # CHAOS-3297 Codex review HIGH #1: the exact validated v1 DevError must
    # flow through to update_run so a later replay can reuse it verbatim.
    assert terminal["terminal_error_payload"] == error.model_dump(mode="json")
    assert "prompt" not in terminal and "provider_response" not in terminal


@pytest.mark.asyncio
async def test_recorder_persists_bounded_tool_counts_and_digests_not_raw_results() -> (
    None
):
    service = AsyncMock()
    recorder = _recorder(service)
    request = DevToolRequest.model_validate(positive_fixtures()["dev_tool_request.v1"])
    result_payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
    result = DevToolResult.model_validate(result_payload)
    await recorder.record_tool(
        ordinal=0,
        request=request,
        canonical_input_hash="sha256:" + "b" * 64,
        execution=ToolExecution(result=result, serialized_bytes=4096, latency_ms=12),
    )

    values = service.append_tool_call.await_args.kwargs
    assert values["status"] == "completed"
    assert values["byte_count"] == 4096
    assert values["item_count"] > 0
    assert values["result_digest"].startswith("sha256:")
    assert "result" not in values
