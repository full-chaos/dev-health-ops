from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import DevAnswer
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


def response(
    *,
    content: str | None = None,
    tool_call: Any = None,
    tool_calls: list[Any] | None = None,
) -> Any:
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content=content,
                    tool_calls=(
                        tool_calls
                        if tool_calls is not None
                        else [tool_call]
                        if tool_call
                        else []
                    ),
                )
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=11,
            completion_tokens=7,
            prompt_tokens_details=SimpleNamespace(cached_tokens=3),
        ),
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
    assert result.usage.cached_input_tokens == 3
    sent = client.chat.completions.calls[0]
    assert sent["tools"][0]["function"]["name"] == "lookup"
    assert sent["tool_choice"] == "required"
    assert sent["temperature"] == 0
    assert "reasoning_effort" not in sent
    assert "response_format" not in sent


@pytest.mark.asyncio
async def test_gpt5_request_omits_unsupported_temperature() -> None:
    call = SimpleNamespace(
        id="call-1",
        function=SimpleNamespace(name="lookup", arguments='{"id":"WU-1"}'),
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-nano", client=client
    )

    await provider.decide(
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

    sent = client.chat.completions.calls[0]
    assert sent["model"] == "gpt-5-nano"
    assert "temperature" not in sent
    assert sent["reasoning_effort"] == "minimal"


@pytest.mark.asyncio
async def test_normalizes_json_tool_decision_for_openai_compatible_fallback() -> None:
    client = Client(
        [
            response(
                content=json.dumps(
                    {
                        "kind": "tool_request",
                        "tool_id": "lookup",
                        "arguments": {"id": "WU-1"},
                        "call_id": "json-call-1",
                    }
                )
            )
        ]
    )
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

    assert result.decision == AgentToolRequest("lookup", {"id": "WU-1"}, "json-call-1")


@pytest.mark.asyncio
async def test_normalizes_strict_root_object_json_tool_decision() -> None:
    client = Client(
        [
            response(
                content=json.dumps(
                    {
                        "kind": "tool_request",
                        "tool_id": "lookup",
                        "arguments": {"id": "WU-1"},
                        "call_id": "json-call-1",
                        "value": None,
                        "prompt": None,
                        "candidates": None,
                        "code": None,
                        "message": None,
                    }
                )
            )
        ]
    )
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

    assert result.decision == AgentToolRequest("lookup", {"id": "WU-1"}, "json-call-1")


@pytest.mark.asyncio
async def test_ignores_inactive_strict_fields_populated_by_lmstudio() -> None:
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client(
            [
                response(
                    content=json.dumps(
                        {
                            "kind": "tool_request",
                            "tool_id": "lookup",
                            "arguments": {"id": "WU-1"},
                            "call_id": "json-call-1",
                            "value": None,
                            "prompt": None,
                            "candidates": [],
                            "code": 'call:lookup{id:"WU-1"}',
                            "message": "",
                        }
                    )
                )
            ]
        ),
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

    assert result.decision == AgentToolRequest("lookup", {"id": "WU-1"}, "json-call-1")


@pytest.mark.asyncio
async def test_normalizes_observed_lmstudio_tool_envelope_without_leaking_content() -> (
    None
):
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client(
            [
                response(
                    content=json.dumps(
                        {
                            "kind": "final_answer",
                            "value": {
                                "tool_call": {
                                    "name": "readiness_echo",
                                    "args": {"nonce": "ready-v1"},
                                }
                            },
                        }
                    )
                )
            ]
        ),
    )

    result = await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "Call readiness_echo")],
        [
            AgentToolDefinition(
                "readiness_echo",
                "Return the supplied nonce",
                {"type": "object", "properties": {"nonce": {"type": "string"}}},
            )
        ],
        {"type": "object"},
        1,
        128,
    )

    assert isinstance(result.decision, AgentToolRequest)
    assert result.decision.tool_id == "readiness_echo"
    assert result.decision.arguments == {"nonce": "ready-v1"}
    assert result.decision.call_id.startswith("json-call-")


@pytest.mark.asyncio
async def test_unknown_json_tool_decision_fails_closed() -> None:
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client(
            [
                response(
                    content=json.dumps(
                        {
                            "kind": "tool_request",
                            "tool_id": "not_registered",
                            "arguments": {},
                            "call_id": "json-call-1",
                        }
                    )
                )
            ]
        ),
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [
                AgentToolDefinition(
                    "lookup", "Lookup", {"type": "object", "properties": {}}
                )
            ],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.INVALID_RESPONSE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "kind": "tool_request",
            "tool_id": "lookup",
            "arguments": {},
            "call_id": "json-call-1",
            "unexpected": True,
        },
        {"kind": "tool_request", "tool_id": "lookup", "arguments": {}},
        {"kind": "not-a-decision"},
    ],
)
async def test_malformed_json_decisions_fail_closed(payload: dict[str, object]) -> None:
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client([response(content=json.dumps(payload))]),
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.INVALID_RESPONSE


@pytest.mark.asyncio
async def test_multiple_native_tool_calls_fail_closed() -> None:
    first = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    second = SimpleNamespace(
        id="call-2", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client([response(tool_calls=[first, second])]),
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.INVALID_RESPONSE


def test_decision_schema_hoists_final_and_tool_definitions_for_root_refs() -> None:
    schema = OpenAICompatibleAgentProvider._decision_response_schema(
        {
            "$defs": {"Answer": {"type": "object"}},
            "$ref": "#/$defs/Answer",
        },
        [
            AgentToolDefinition(
                "lookup",
                "Lookup",
                {
                    "$defs": {"Arguments": {"type": "object"}},
                    "$ref": "#/$defs/Arguments",
                },
            )
        ],
    )

    assert schema["$defs"] == {
        "Answer": {"type": "object"},
        "Arguments": {"type": "object"},
    }
    assert "$defs" not in schema["properties"]["value"]["anyOf"][0]
    assert "$defs" not in schema["properties"]["arguments"]["anyOf"][0]


def test_structural_schema_preserves_shape_and_removes_runtime_constraints() -> None:
    schema = OpenAICompatibleAgentProvider._structural_schema(
        {
            "type": "object",
            "title": "Answer",
            "properties": {
                "id": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                    "pattern": "^[a-z]+$",
                },
                "nested": {"$ref": "#/$defs/Nested"},
            },
            "required": ["id"],
            "$defs": {
                "Nested": {
                    "type": "object",
                    "properties": {"at": {"type": "string", "format": "date-time"}},
                    "default": {},
                }
            },
        }
    )

    assert set(schema["properties"]) == {"id", "nested"}
    assert schema["required"] == ["id", "nested"]
    assert schema["additionalProperties"] is False
    assert schema["properties"]["nested"] == {"$ref": "#/$defs/Nested"}
    assert schema["$defs"]["Nested"] == {
        "type": "object",
        "properties": {"at": {"type": "string"}},
        "required": ["at"],
        "additionalProperties": False,
    }
    rendered = json.dumps(schema)
    assert "minLength" not in rendered
    assert "maxLength" not in rendered
    assert "pattern" not in rendered
    assert "format" not in rendered
    assert "default" not in rendered
    assert "title" not in rendered


def test_answer_draft_schema_keeps_grounding_and_drops_server_owned_fields() -> None:
    draft = OpenAICompatibleAgentProvider._answer_draft_schema(
        DevAnswer.model_json_schema(mode="validation")
    )

    assert set(draft["properties"]) == {
        "status",
        "direct_summary",
    }
    assert set(draft["required"]) == set(draft["properties"])
    assert set(draft["$defs"]) == {"AnswerStatus"}
    assert "DevScopeResolution" not in draft["$defs"]
    assert "DevContractVersions" not in draft["$defs"]
    assert "DevModelMetadata" not in draft["$defs"]


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
    sent_schema = client.chat.completions.calls[0]["response_format"]["json_schema"]
    assert "final_answer" in sent_schema["schema"]["properties"]["kind"]["enum"]
    assert "tool_request" not in sent_schema["schema"]["properties"]["kind"]["enum"]


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
async def test_tool_result_continuation_allows_tools_and_strict_final_answer() -> None:
    client = Client(
        [response(content=json.dumps({"kind": "final_answer", "value": {"ok": True}}))]
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )
    request = AgentToolRequest("lookup", {"id": "WU-1"}, "call-1")

    result = await provider.decide(
        [
            AgentMessage(AgentMessageRole.USER, "question"),
            AgentMessage(AgentMessageRole.ASSISTANT, "", tool_request=request),
            AgentMessage(
                AgentMessageRole.TOOL,
                '{"result":1}',
                tool_call_id="call-1",
            ),
        ],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
        },
        1,
        256,
    )

    assert result.decision == AgentFinalAnswer({"ok": True})
    schema = client.chat.completions.calls[0]["response_format"]["json_schema"][
        "schema"
    ]
    assert schema["properties"]["kind"]["enum"] == [
        "tool_request",
        "final_answer",
        "disambiguation",
        "refusal",
    ]
    assert schema["properties"]["value"]["anyOf"][0]["required"] == ["ok"]
    assert client.chat.completions.calls[0]["tool_choice"] == "auto"


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
async def test_pre_dispatch_cancellation_is_explicitly_identified() -> None:
    class CancelledSignal:
        def is_cancelled(self) -> bool:
            return True

        async def wait(self) -> None:
            return None

    client = Client([])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            1,
            256,
            CancelledSignal(),
        )

    assert caught.value.code is AgentProviderErrorCode.CANCELLED
    assert caught.value.provider_dispatched is False
    assert client.chat.completions.calls == []


@pytest.mark.asyncio
async def test_closes_injected_client() -> None:
    client = Client([])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )
    await provider.aclose()
    assert client.closed is True
