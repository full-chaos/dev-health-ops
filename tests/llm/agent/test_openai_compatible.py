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
from dev_health_ops.llm.agent.openai_compatible import (
    OpenAICompatibleAgentProvider,
    build_completion_request,
)


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
    finish_reason: str = "stop",
    reasoning_tokens: int | None = None,
) -> Any:
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason=finish_reason,
                message=SimpleNamespace(
                    content=content,
                    tool_calls=(
                        tool_calls
                        if tool_calls is not None
                        else [tool_call]
                        if tool_call
                        else []
                    ),
                ),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=11,
            completion_tokens=7,
            prompt_tokens_details=SimpleNamespace(cached_tokens=3),
            completion_tokens_details=SimpleNamespace(
                reasoning_tokens=reasoning_tokens
            ),
        ),
    )


@pytest.mark.asyncio
async def test_decide_sends_exactly_what_build_completion_request_produces() -> None:
    """CHAOS-3285 round 5 (Codex HIGH): the readiness fingerprint's
    wire-request digest calls build_completion_request directly and hashes
    its output; that is only a meaningful guarantee if decide() actually
    dispatches EXACTLY that output, not an independently-assembled
    approximation of it (round 4's gap: decide() still built
    max_completion_tokens, the response_format wrapper's literal name/
    strict, and the full schema body itself). Prove it differentially: the
    real kwargs decide() sends to the wire must equal what
    build_completion_request returns for the identical inputs."""

    call = SimpleNamespace(
        id="call-1",
        function=SimpleNamespace(name="lookup", arguments="{}"),
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-mini", client=client
    )
    messages = [
        AgentMessage(AgentMessageRole.SYSTEM, "sys"),
        AgentMessage(AgentMessageRole.USER, "question"),
    ]
    tools = [
        AgentToolDefinition(
            "lookup",
            "Lookup a work unit",
            {"type": "object", "properties": {"id": {"type": "string"}}},
        )
    ]
    response_schema = {"type": "object", "properties": {"status": {"type": "string"}}}

    await provider.decide(messages, tools, response_schema, 1, 256)

    sent = client.chat.completions.calls[0]
    expected = build_completion_request(
        model="gpt-5-mini",
        messages=messages,
        tools=tools,
        response_schema=response_schema,
        max_output_tokens=256,
    )
    assert sent == expected


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
    assert sent["parallel_tool_calls"] is False
    assert sent["temperature"] == 0
    assert "reasoning_effort" not in sent
    assert "response_format" not in sent


@pytest.mark.asyncio
async def test_native_tool_request_sanitizes_dotted_tool_name() -> None:
    """CHAOS-3286: OpenAI rejects dotted tools[].function.name outright.

    Every real OpenAI-backed request using a canonical registry tool_id
    (e.g. "query_metric.v1") 400s with "Invalid 'tools[0].function.name'"
    because OpenAI requires ^[a-zA-Z0-9_-]+$. The adapter must sanitize the
    outbound name and reverse-map a model's wire-legal response back to the
    canonical tool_id -- which is what the rest of Ask Dev (contracts,
    persistence, telemetry) expects to see.
    """
    call = SimpleNamespace(
        id="call-1",
        # A real model can only echo back the wire-legal name it was
        # offered -- never the canonical dotted tool_id.
        function=SimpleNamespace(
            name="query_metric_v1", arguments='{"metric_id":"items_completed"}'
        ),
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )

    result = await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [
            AgentToolDefinition(
                "query_metric.v1",
                "Query a bounded metric.",
                {"type": "object", "properties": {"metric_id": {"type": "string"}}},
            )
        ],
        {"type": "object"},
        1,
        256,
    )

    sent = client.chat.completions.calls[0]
    assert sent["tools"][0]["function"]["name"] == "query_metric_v1"
    # The canonical dotted tool_id survives the round trip -- everything
    # downstream of the adapter (ToolID enum, persistence, telemetry)
    # continues to see the identifier it already expects.
    assert result.decision == AgentToolRequest(
        "query_metric.v1", {"metric_id": "items_completed"}, "call-1"
    )


@pytest.mark.asyncio
async def test_native_tool_call_with_unregistered_wire_name_fails_closed() -> None:
    call = SimpleNamespace(
        id="call-1",
        function=SimpleNamespace(name="not_a_real_tool", arguments="{}"),
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [
                AgentToolDefinition(
                    "query_metric.v1", "Query a metric.", {"type": "object"}
                )
            ],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.INVALID_RESPONSE


@pytest.mark.asyncio
async def test_tool_name_mapping_collision_fails_loudly_not_silently() -> None:
    """A registry bug (two tool_ids sanitizing to the same wire name) must
    surface as a plain, uncaught error -- never silently misroute a tool
    decision to the wrong canonical tool_id, and never be misclassified as
    an ordinary provider failure (CHAOS-3286).
    """
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client([response(content="{}")]),
    )

    with pytest.raises(ValueError, match="tool name mapping collision"):
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [
                AgentToolDefinition("query.metric", "d", {"type": "object"}),
                AgentToolDefinition("query_metric", "d", {"type": "object"}),
            ],
            {"type": "object"},
            1,
            256,
        )


@pytest.mark.asyncio
async def test_native_tool_request_disables_parallel_tool_calls() -> None:
    """CHAOS-3254: the wire request must enforce the sequential contract.

    Ask Dev is a sequential one-decision-per-round state machine. Whenever
    native tools are offered on an OpenAI-compatible model that accepts the
    control, the outbound request must explicitly disable
    ``parallel_tool_calls`` so a standards-compliant model cannot return more
    than one tool call in a single response.
    """
    call = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )

    await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {"type": "object"},
        1,
        256,
    )

    sent = client.chat.completions.calls[0]
    assert sent["parallel_tool_calls"] is False


@pytest.mark.asyncio
async def test_request_without_tools_omits_parallel_tool_calls_control() -> None:
    client = Client(
        [
            response(
                content=json.dumps({"kind": "refusal", "code": "no", "message": "no"})
            )
        ]
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )

    await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [],
        {"type": "object"},
        1,
        256,
    )

    sent = client.chat.completions.calls[0]
    assert sent["tools"] is None
    assert sent["tool_choice"] is None
    assert "parallel_tool_calls" not in sent


@pytest.mark.asyncio
async def test_gpt5_request_disables_parallel_tool_calls() -> None:
    """GPT-5's Chat Completions surface accepts parallel_tool_calls=false.

    Live-verified against the real API (2026-07-30): gpt-5-mini and
    gpt-5-nano both return HTTP 200 for a tools + tool_choice:auto +
    reasoning_effort:minimal + parallel_tool_calls:false request. GPT-5 is
    NOT bucketed with the o-series reasoning models for this control (see
    test_o_series_reasoning_model_omits_parallel_tool_calls_control) --
    only supports_temperature/reasoning_effort bucket it that way, for an
    unrelated constraint (CHAOS-3254/CHAOS-3263).
    """
    call = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-nano", client=client
    )

    await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {"type": "object"},
        1,
        256,
    )

    sent = client.chat.completions.calls[0]
    assert sent["parallel_tool_calls"] is False
    assert sent["reasoning_effort"] == "minimal"


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["o3-mini", "o4-mini"])
async def test_o_series_reasoning_model_omits_parallel_tool_calls_control(
    model: str,
) -> None:
    """o-series reasoning models reject parallel_tool_calls with a 400.

    Live-verified against the real API (2026-07-30): both o3-mini and
    o4-mini, with tools + tool_choice:auto + parallel_tool_calls:false,
    return HTTP 400 invalid_request_error/unsupported_parameter,
    param="parallel_tool_calls" (o1-mini was inaccessible under the probing
    key -- 404 model_not_found -- but is excluded on the same family basis;
    see supports_parallel_tool_calls's docstring). The sequential contract is
    still enforced for this model family, but on the response side only (see
    test_multiple_native_tool_calls_fail_closed), since sending the field at
    all would break the request outright.
    """
    call = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model=model, client=client
    )

    await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {"type": "object"},
        1,
        256,
    )

    sent = client.chat.completions.calls[0]
    assert "parallel_tool_calls" not in sent


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
    """CHAOS-3254: a defensive multi-call response must not become internal_error.

    Even with parallel_tool_calls=false on the outbound request (see
    test_native_tool_request_disables_parallel_tool_calls), a provider may
    still violate the sequential contract. That must fail closed as a
    stable, distinguishable provider/decision contract error -- never the
    opaque application ``internal_error`` bucket that ``INVALID_RESPONSE``
    maps to (see DevOrchestrator._provider_error).
    """
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

    assert caught.value.code is AgentProviderErrorCode.PROVIDER_CONTRACT_VIOLATION
    assert caught.value.code is not AgentProviderErrorCode.INVALID_RESPONSE


@pytest.mark.asyncio
async def test_literal_reproduction_question_completes_through_sequential_rounds() -> (
    None
):
    """CHAOS-3254/CHAOS-3260 reproduction: a naturally multi-metric question.

    ``What is the current delivery metric and project timing across the org
    look like compared the previous period?`` previously failed before any
    tool executed (safe_error_code=internal_error, tool_call_count=0) because
    the LM Studio-style provider correctly returned three native tool calls
    in one response and the request never disabled parallel tool calls. This
    proves the fix end to end at the adapter: one native tool request per
    round -- each with a comparison -- through a final answer, with no
    generic internal error and with parallel_tool_calls explicitly disabled
    on every tool-bearing request.
    """
    question = (
        "What is the current delivery metric and project timing across the "
        "org look like compared the previous period?"
    )
    metric_tool = AgentToolDefinition(
        "query_metric.v1",
        "Query a bounded metric.",
        {
            "type": "object",
            "properties": {
                "metric_id": {"type": "string"},
                "include_comparison": {"type": "boolean"},
            },
        },
    )

    def tool_call(call_id: str, metric_id: str) -> SimpleNamespace:
        return SimpleNamespace(
            id=call_id,
            function=SimpleNamespace(
                # A real model can only echo back the wire-sanitized name it
                # was offered (CHAOS-3286), never the canonical dotted
                # tool_id -- see test_native_tool_request_sanitizes_dotted_name.
                name="query_metric_v1",
                arguments=json.dumps(
                    {"metric_id": metric_id, "include_comparison": True}
                ),
            ),
        )

    client = Client(
        [
            response(tool_call=tool_call("call-1", "items_completed")),
            response(tool_call=tool_call("call-2", "cycle_time_p50_hours")),
            response(tool_call=tool_call("call-3", "avg_wip")),
            response(
                content=json.dumps(
                    {
                        "kind": "final_answer",
                        "value": {"status": "partial"},
                    }
                )
            ),
        ]
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )

    messages: list[AgentMessage] = [AgentMessage(AgentMessageRole.USER, question)]
    tool_requests: list[AgentToolRequest] = []
    for _ in range(3):
        result = await provider.decide(
            messages, [metric_tool], {"type": "object"}, 1, 256
        )
        assert isinstance(result.decision, AgentToolRequest)
        tool_requests.append(result.decision)
        messages = [
            *messages,
            AgentMessage(AgentMessageRole.ASSISTANT, "", tool_request=result.decision),
            AgentMessage(
                AgentMessageRole.TOOL,
                json.dumps({"status": "success"}),
                tool_call_id=result.decision.call_id,
            ),
        ]

    final = await provider.decide(messages, [metric_tool], {"type": "object"}, 1, 256)
    assert isinstance(final.decision, AgentFinalAnswer)

    assert [request.arguments["metric_id"] for request in tool_requests] == [
        "items_completed",
        "cycle_time_p50_hours",
        "avg_wip",
    ]
    assert all(
        request.arguments["include_comparison"] is True for request in tool_requests
    )
    # One native tool call per round -- never more than one in a single
    # response -- and the sequential contract explicitly enforced on the wire.
    for call in client.chat.completions.calls:
        assert call["parallel_tool_calls"] is False


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
async def test_combined_tools_auto_and_strict_grammar_shape_pinned_together() -> None:
    """CHAOS-3285 plan §6.2: the wire shape sent on every production round
    >= 2 -- tools present *and* tool_choice:"auto" *and* a strict
    json_schema response_format *and* parallel_tool_calls:false, all
    simultaneously -- is the exact shape certification never sent
    (readiness's two probe calls each send only a subset). Pin all four in
    one kwargs dict from a single round so a regression in any one clause
    fails this test.
    """
    client = Client(
        [response(content=json.dumps({"kind": "final_answer", "value": {"ok": True}}))]
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )
    request = AgentToolRequest("lookup", {"id": "WU-1"}, "call-1")

    await provider.decide(
        [
            AgentMessage(AgentMessageRole.USER, "question"),
            AgentMessage(AgentMessageRole.ASSISTANT, "", tool_request=request),
            AgentMessage(AgentMessageRole.TOOL, '{"result":1}', tool_call_id="call-1"),
        ],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {"type": "object", "properties": {"ok": {"type": "boolean"}}},
        1,
        256,
    )

    sent = client.chat.completions.calls[0]
    assert sent["tools"] is not None and len(sent["tools"]) == 1
    assert sent["tool_choice"] == "auto"
    assert sent["parallel_tool_calls"] is False
    assert sent["response_format"]["json_schema"]["strict"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["gpt-5-nano", "o3-mini", "agent-model"])
async def test_max_completion_tokens_unchanged_for_every_family_at_zero_headroom(
    model: str,
) -> None:
    """Behavior-unchanged proof for CHAOS-3285 commit 2's budget_policy
    wiring: at reasoning_headroom_tokens == 0 (the only value any family has
    today), the wire max_completion_tokens must equal the caller's visible
    cap verbatim -- byte-identical to the pre-commit-2 behavior -- for a
    reasoning-counted family (gpt-5, o-series) and the default family alike.
    """
    call = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    client = Client([response(tool_call=call)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model=model, client=client
    )

    await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {"type": "object"},
        1,
        4_096,
    )

    assert client.chat.completions.calls[0]["max_completion_tokens"] == 4_096


@pytest.mark.asyncio
async def test_finish_reason_length_with_empty_content_raises_output_exhausted() -> (
    None
):
    """CHAOS-3285: before this fix, a reasoning model that ran out of its
    output budget mid-decision returned finish_reason="length" with empty
    content, which json.loads() turned into a JSONDecodeError, classified as
    INVALID_RESPONSE -> internal_error (an opaque application failure for
    what is actually a structural model-capability mismatch). finish_reason
    is now read and dispositive before any JSON parsing is attempted.
    """
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="gpt-5-nano",
        client=Client([response(content="", finish_reason="length")]),
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED
    assert caught.value.code is not AgentProviderErrorCode.INVALID_RESPONSE
    assert caught.value.retryable is False


@pytest.mark.asyncio
async def test_finish_reason_length_with_truncated_partial_content_still_exhausted() -> (
    None
):
    """Belt-and-braces (plan §3.3): a truncated-but-technically-parseable
    partial JSON payload is the *worse* case, because it can validate and
    silently pass through as a malformed-but-plausible decision. finish_reason
    alone must be dispositive regardless of whether content happens to parse.
    """
    truncated = '{"kind": "final_answer", "value": {"ok"'
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="gpt-5-nano",
        client=Client([response(content=truncated, finish_reason="length")]),
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED


@pytest.mark.asyncio
async def test_finish_reason_length_attaches_billed_usage_to_the_error() -> None:
    """CHAOS-3285: before this fix, OUTPUT_EXHAUSTED was raised before
    response.usage was ever read, so the error carried no usage data at
    all -- downstream, guard_byo_call's exception handler
    (dev_health_ops.llm.budget) treats a usage-less failure as
    "usage_unavailable", which poisons the whole BYO budget window and
    rejects every later call without dispatching it. Reviewer's exact
    reproduction: a parseable finish_reason="length" response reporting 40
    input / 256 output / 240 reasoning tokens must still surface those
    numbers on the raised error.
    """
    exhausted = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="length",
                message=SimpleNamespace(content="", tool_calls=[]),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=40,
            completion_tokens=256,
            prompt_tokens_details=SimpleNamespace(cached_tokens=0),
            completion_tokens_details=SimpleNamespace(reasoning_tokens=240),
        ),
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-nano", client=Client([exhausted])
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED
    usage = caught.value.usage
    assert usage is not None
    assert usage.input_tokens == 40
    assert usage.output_tokens == 256
    assert usage.reasoning_tokens == 240
    assert usage.cached_input_tokens == 0


@pytest.mark.asyncio
async def test_finish_reason_length_with_zero_reported_usage_withholds_usage() -> None:
    """CHAOS-3285 follow-up: a valid completion can never consume zero input
    AND zero output tokens (the same invariant budget.py's _agent_usage()
    already relies on for the success path). A provider that reports
    exactly that on an exhausted response must not have that zero usage
    attached to the error -- otherwise guard_byo_call reconciles the
    reservation as a real $0 charge and silently drops the actual
    (unreported) cost from BYO budget accounting instead of holding the
    reservation conservatively.
    """
    exhausted = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="length",
                message=SimpleNamespace(content="", tool_calls=[]),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=0,
            completion_tokens=0,
            prompt_tokens_details=SimpleNamespace(cached_tokens=0),
            completion_tokens_details=SimpleNamespace(reasoning_tokens=0),
        ),
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-nano", client=Client([exhausted])
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED
    assert caught.value.usage is None


@pytest.mark.asyncio
async def test_finish_reason_length_with_no_usage_object_withholds_usage() -> None:
    """A response that omits ``usage`` entirely normalizes to the same 0/0
    shape as one that reports literal zeros -- both must be treated as
    unreported, not as a free call.
    """
    exhausted = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="length",
                message=SimpleNamespace(content="", tool_calls=[]),
            )
        ],
        usage=None,
    )
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-nano", client=Client([exhausted])
    )

    with pytest.raises(AgentProviderError) as caught:
        await provider.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            [],
            {"type": "object"},
            1,
            256,
        )

    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED
    assert caught.value.usage is None


@pytest.mark.asyncio
async def test_contract_violation_and_invalid_response_also_attach_usage() -> None:
    """CHAOS-3285 audit: OUTPUT_EXHAUSTED was the reported finding, but every
    raise path inside the same try block discards response.usage the same
    way once a response has been received -- a sequential-tool-contract
    violation and a malformed-but-received decision both still billed real
    tokens and must not be reconciled as usage_unavailable either.
    """
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
    assert caught.value.code is AgentProviderErrorCode.PROVIDER_CONTRACT_VIOLATION
    assert caught.value.usage is not None
    assert caught.value.usage.input_tokens == 11
    assert caught.value.usage.output_tokens == 7

    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client([response(content="not-json", finish_reason="stop")]),
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
    assert caught.value.usage is not None
    assert caught.value.usage.input_tokens == 11
    assert caught.value.usage.output_tokens == 7


@pytest.mark.asyncio
async def test_finish_reason_stop_does_not_raise_output_exhausted() -> None:
    """The fail-before/pass-after control: an ordinary, non-exhausted
    malformed response (finish_reason="stop") must still classify as
    INVALID_RESPONSE, exactly as before this change -- proving the new
    finish_reason check is additive, not a wholesale reclassification of
    every malformed response as exhaustion.
    """
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="agent-model",
        client=Client([response(content="not-json", finish_reason="stop")]),
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


@pytest.mark.asyncio
async def test_reasoning_tokens_surfaced_on_agent_usage() -> None:
    call = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    client = Client([response(tool_call=call, reasoning_tokens=192)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="gpt-5-nano", client=client
    )

    result = await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {"type": "object"},
        1,
        256,
    )

    assert result.usage.reasoning_tokens == 192


@pytest.mark.asyncio
async def test_missing_reasoning_tokens_detail_surfaces_as_none_not_zero() -> None:
    """None (field absent/unreported) must stay distinguishable from an
    explicit 0 -- collapsing them would make a provider that never reports
    reasoning usage indistinguishable from one that reported zero reasoning
    tokens, corrupting the accounting CHAOS-3285's later commits build on.
    """
    call = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    client = Client([response(tool_call=call, reasoning_tokens=None)])
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model="agent-model", client=client
    )

    result = await provider.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        [AgentToolDefinition("lookup", "Lookup", {"type": "object"})],
        {"type": "object"},
        1,
        256,
    )

    assert result.usage.reasoning_tokens is None


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
