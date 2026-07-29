from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentToolDefinition,
    AgentToolRequest,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider


class Completions:
    def __init__(self, responses: list[Any]):
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if isinstance(response, float):
            await asyncio.sleep(response)
        return response


class Client:
    def __init__(self, responses: list[Any]):
        self.chat = SimpleNamespace(completions=Completions(responses))
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def response(*, content: str | None = None, tool_call: Any = None) -> Any:
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content=content,
                    tool_calls=[tool_call] if tool_call else [],
                )
            )
        ],
        usage=SimpleNamespace(prompt_tokens=11, completion_tokens=7),
    )


@pytest.mark.asyncio
async def test_normalizes_native_tool_decision_and_usage() -> None:
    call = SimpleNamespace(
        id="call-1",
        function=SimpleNamespace(name="lookup", arguments='{"id":"WU-1"}'),
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )
    result = await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [
            AgentToolDefinition(
                "lookup",
                "Lookup a work unit",
                {"type": "object", "properties": {"id": {"type": "string"}}},
            )
        ],
        {"type": "object"},
        1,
        256,
    )
    assert result.decision == AgentToolRequest("lookup", {"id": "WU-1"}, "call-1")
    assert (result.usage.input_tokens, result.usage.output_tokens) == (11, 7)
    assert result.usage.estimated_cost_microusd is None
    sent = client.chat.completions.calls[0]
    assert sent["tools"][0]["function"]["name"] == "lookup"
    assert sent["response_format"]["json_schema"]["strict"] is True


@pytest.mark.asyncio
async def test_estimates_known_platform_model_cost_in_integer_microusd() -> None:
    client = Client([response(content='{"kind":"refusal","code":"no","message":"no"}')])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-mini-2026-07-29", client=client
    )

    result = await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [],
        {"type": "object"},
        1,
        256,
    )

    # ceil((11 * $0.25/M + 7 * $2/M) * 1M microUSD/USD)
    assert result.usage.estimated_cost_microusd == 17


@pytest.mark.asyncio
async def test_normalizes_structured_final_answer() -> None:
    client = Client(
        [
            response(
                content=json.dumps({"kind": "final_answer", "value": {"answer": 42}})
            )
        ]
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )
    result = await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [],
        {"type": "object"},
        1,
        256,
    )
    assert result.decision == AgentFinalAnswer({"answer": 42})


def test_translates_normalized_tool_continuation_to_openai_wire_shape() -> None:
    request = AgentToolRequest("lookup", {"id": "WU-1"}, "call-1")
    assistant = OpenAICompatibleAgentProvider._message_payload(
        AgentMessage(AgentMessageRole.ASSISTANT, "", tool_request=request)
    )
    tool = OpenAICompatibleAgentProvider._message_payload(
        AgentMessage(AgentMessageRole.TOOL, '{"result":1}', tool_call_id="call-1")
    )
    assert assistant["tool_calls"][0]["function"] == {
        "name": "lookup",
        "arguments": '{"id":"WU-1"}',
    }
    assert tool["tool_call_id"] == "call-1"


@pytest.mark.asyncio
async def test_invalid_wire_response_maps_to_safe_error() -> None:
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client([response(content="not-json")]),
    )
    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            1,
            256,
        )
    assert caught.value.code is AgentProviderErrorCode.INVALID_RESPONSE
    assert "not-json" not in str(caught.value)


@pytest.mark.asyncio
async def test_timeout_is_enforced_by_adapter() -> None:
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=Client([0.1])
    )
    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            0.001,
            256,
        )
    assert caught.value.code is AgentProviderErrorCode.TIMEOUT


@pytest.mark.asyncio
async def test_closes_injected_client() -> None:
    client = Client([])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )
    await provider.aclose()
    assert client.closed is True
