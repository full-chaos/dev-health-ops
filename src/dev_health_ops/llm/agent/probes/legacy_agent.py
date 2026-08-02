"""Production-sized ``legacy_agent`` role readiness probe (CHAOS-3285).

Unlike ``readiness.py``'s ``AgentReadinessService.certify`` (a 512-token echo
against one 240-byte tool), this probe is built from the exact real
producers the production Ask Dev orchestrator uses: ``PromptComposer`` (the
full fixed-policy sections + the committed-subject section + the complete
tool registry), ``AskDevToolRegistry`` (all nine registered tools, real
per-tool input schemas via ``DevOrchestrator._provider_tool_input_schema``),
``DevRunLimits`` (the real per-call output-token cap), and ``DevAnswer``'s
own JSON schema. It never hand-authors a miniature payload.

Two rounds mirror the worst-case production shape identified by the
CHAOS-3285 root-cause analysis: round 1 (tools offered, no tool result yet)
sends ``tool_choice="required"`` with no structured-output grammar; round 2
(tools offered *and* a synthetic tool result already in the conversation)
sends ``tool_choice="auto"`` **and** the full ``DevAnswer`` grammar
simultaneously -- the combined shape every real round >= 2 sends and which
the synthetic readiness echo probe never exercises at all.

The synthetic round-2 tool result is schema-valid, fabricated content from
the checked-in production contract fixtures (``contract_fixtures.py``) --
never a live tool call, never tenant data (ticket guardrail: "Readiness
cannot make a real source/tool call with tenant data").
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevScopeResolution,
    DevToolResult,
    ToolID,
)
from dev_health_ops.api.dev.orchestrator import DevOrchestrator, DevRunLimits
from dev_health_ops.api.dev.prompts.composer import (
    ComposedPrompt,
    PromptComposer,
    PromptConversationTurn,
)
from dev_health_ops.api.dev.tool_registry import TOOL_DEFINITIONS, AskDevToolRegistry

from ..contracts import (
    AgentDecisionResult,
    AgentFinalAnswer,
    AgentLLMProvider,
    AgentMessage,
    AgentMessageRole,
    AgentToolDefinition,
    AgentToolRequest,
    AgentUsage,
    CancellationSignal,
)
from ..errors import (
    AgentProviderError,
    AgentProviderErrorCode,
    safe_agent_provider_error,
)
from ..readiness import role_state_for_safe_error_code
from ..roles import RoleCertificationState

_PROBE_QUESTION = (
    "What changed in this repository during the selected period, and is the "
    "in-flight work complete?"
)
_PROBE_RUN_ID = "role-probe-legacy-agent-01"

# A production round-1 system prompt (full fixed policy sections + the
# committed-subject section + the complete nine-tool registry) measures
# ~5.8 KB per the CHAOS-3285 root-cause analysis; the synthetic readiness
# echo probe's entire request is well under 1 KB. This floor sits
# comfortably below the real measurement -- so ordinary prompt-text edits
# don't flake the probe -- but well above anything a shrunk/miniature probe
# could ever produce (Rule 4: a measurement that did not happen must fail
# loudly, not silently read as coverage).
_PRODUCTION_FLOOR_BYTES = 4_000


@dataclass(frozen=True, slots=True)
class LegacyAgentProbeResult:
    state: RoleCertificationState
    safe_error_code: str | None
    usage: AgentUsage
    observed_request_bytes: int


async def _unused_executor(context: object, request: object) -> DevToolResult:
    # The probe never actually executes a tool -- it only needs a real,
    # fully-validated AskDevToolRegistry to produce the exact production
    # tool manifest/schemas. A real invocation here would be a probe defect.
    raise AssertionError("legacy_agent probe must never execute a real tool")


def _probe_registry() -> AskDevToolRegistry:
    return AskDevToolRegistry({tool_id: _unused_executor for tool_id in ToolID})


def _probe_tools(registry: AskDevToolRegistry) -> tuple[AgentToolDefinition, ...]:
    """Mirror ``DevOrchestrator._provider_tools`` verbatim: the real producer
    of the wire tool definitions, never a hand-rolled schema."""

    manifest = registry.manifest(allowed_tool_ids=None)
    tools = manifest["tools"]
    assert isinstance(tools, list)
    return tuple(
        AgentToolDefinition(
            tool_id=str(item["tool_id"]),
            description=str(item["description"]),
            input_schema=DevOrchestrator._provider_tool_input_schema(
                ToolID(str(item["tool_id"])), int(item["max_items"])
            ),
        )
        for item in tools
        if isinstance(item, Mapping)
    )


def _probe_scope_resolution() -> DevScopeResolution:
    return DevScopeResolution.model_validate(
        positive_fixtures()["dev_scope_resolution.v1"]
    )


def _probe_history() -> tuple[PromptConversationTurn, ...]:
    """A representative (not maximal) prior conversation.

    The dominant cost driver in the CHAOS-3285 measurement is the fixed
    system prompt + full tool registry + full grammar (~12.6 KB round-1
    floor), not history -- so a representative multi-turn history proves the
    mechanism without needing to hit ``MAX_PRIOR_CONTENT_BYTES``.
    """

    return tuple(
        PromptConversationTurn(
            role="user" if index % 2 == 0 else "assistant",
            content=(
                f"Prior conversation turn {index}: representative "
                "production-sized context describing previously observed "
                "status, delivered work, and open follow-up questions from "
                "an earlier round of this investigation. "
            )
            * 4,
        )
        for index in range(6)
    )


def _probe_tool_result(tool_request: AgentToolRequest) -> DevToolResult:
    """A schema-valid, fabricated tool result -- never a live call, never
    tenant data (CHAOS-3285 guardrail)."""

    payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
    payload["run_id"] = _PROBE_RUN_ID
    payload["tool_call_id"] = tool_request.call_id
    payload["tool_id"] = tool_request.tool_id
    return DevToolResult.model_validate(payload)


def _assert_production_shape(
    system_text: str, tools: tuple[AgentToolDefinition, ...]
) -> None:
    if len(tools) != len(TOOL_DEFINITIONS):
        raise AssertionError(
            f"legacy_agent probe must advertise the full {len(TOOL_DEFINITIONS)}-tool "
            f"registry, got {len(tools)}"
        )
    observed_bytes = len(system_text.encode("utf-8"))
    if observed_bytes < _PRODUCTION_FLOOR_BYTES:
        raise AssertionError(
            f"legacy_agent probe's composed system prompt ({observed_bytes} bytes) "
            f"is below the production-representative floor ({_PRODUCTION_FLOOR_BYTES} "
            "bytes) -- the probe shrank and no longer reproduces the real request shape"
        )


def _merge_usage(left: AgentUsage, right: AgentUsage) -> AgentUsage:
    cost = None
    if (
        left.estimated_cost_microusd is not None
        or right.estimated_cost_microusd is not None
    ):
        cost = (left.estimated_cost_microusd or 0) + (
            right.estimated_cost_microusd or 0
        )
    reasoning_tokens = None
    if left.reasoning_tokens is not None or right.reasoning_tokens is not None:
        reasoning_tokens = (left.reasoning_tokens or 0) + (right.reasoning_tokens or 0)
    return AgentUsage(
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        estimated_cost_microusd=cost,
        reasoning_tokens=reasoning_tokens,
    )


async def _decide_round_2(
    *,
    provider: AgentLLMProvider,
    composer: PromptComposer,
    scope_resolution: DevScopeResolution,
    history: tuple[PromptConversationTurn, ...],
    tools: tuple[AgentToolDefinition, ...],
    response_schema: Mapping[str, object],
    tool_request: AgentToolRequest,
    tool_result: DevToolResult,
    timeout_seconds: float,
    max_output_tokens: int,
    subject_committed: bool,
    signal: CancellationSignal | None,
) -> tuple[AgentDecisionResult, ComposedPrompt]:
    """Round 2 for one prompt shape (committed or uncommitted subject):
    tools offered AND a synthetic tool result already in the conversation
    -- ``tool_choice="auto"`` and the full ``DevAnswer`` grammar
    simultaneously, the combined worst-case shape. Shares the SAME
    synthetic tool request/result across both shapes: the tool-result
    content does not depend on ``subject_committed`` at all, only the
    system/user prompt sections do, so reusing it isolates exactly the
    prompt-shape dimension CHAOS-3285 round 2 (Codex HIGH) flagged as
    never invalidated.
    """

    composed = composer.compose(
        question=_PROBE_QUESTION,
        scope=scope_resolution,
        prior_turns=history,
        tool_results=(tool_result,),
        allowed_tools=None,
        subject_committed=subject_committed,
    )
    messages = (
        AgentMessage(AgentMessageRole.SYSTEM, composed.system_text),
        AgentMessage(AgentMessageRole.USER, composed.user_text),
        AgentMessage(AgentMessageRole.ASSISTANT, "", tool_request=tool_request),
        AgentMessage(
            AgentMessageRole.TOOL,
            tool_result.model_dump_json(),
            tool_call_id=tool_request.call_id,
        ),
    )
    result = await provider.decide(
        messages, tools, response_schema, timeout_seconds, max_output_tokens, signal
    )
    if not isinstance(result.decision, (AgentFinalAnswer, AgentToolRequest)):
        raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)
    return result, composed


async def certify_legacy_agent(
    provider: AgentLLMProvider,
    *,
    timeout_seconds: float,
    limits: DevRunLimits | None = None,
    signal: CancellationSignal | None = None,
) -> LegacyAgentProbeResult:
    """Certify the ``legacy_agent`` role against the real production request
    shape. Returns a verdict derived from whatever the provider actually
    does with that shape -- including reproducing ``OUTPUT_EXHAUSTED``
    against a provider/budget combination that cannot handle it.

    Three calls, not two (CHAOS-3285 round 2, Codex HIGH): round 1 (tools
    offered, no tool result yet, always composed with
    ``subject_committed=True``, the lower-risk shape regardless of which
    prompt variant a real run ends up using -- the fixed subject-section
    text is a small delta against the tool registry + grammar + history
    that actually dominate exhaustion risk); round 2 under the
    committed-subject shape (``PROMPT_VERSION``); and round 2 AGAIN under
    the uncommitted-subject shape (``LEGACY_PROMPT_VERSION`` -- an
    organization-wide question, or the Wave 3.1 flag off). Both round-2
    calls exercise the actual combined worst-case shape (tool_choice="auto"
    + the strict grammar), the one that determines exhaustion, so BOTH
    prompt shapes a real run can select must independently certify
    COMPATIBLE for the role to certify COMPATIBLE overall. This is what
    makes ``_canonical_contract_digest``'s folding of BOTH prompt version
    constants meaningful: without probing both shapes, invalidating on a
    LEGACY_PROMPT_VERSION change would correctly force re-certification,
    but the re-certification itself would still never have tested the
    shape that changed.
    """

    limits = limits or DevRunLimits()
    registry = _probe_registry()
    composer = PromptComposer(registry)
    tools = _probe_tools(registry)
    scope_resolution = _probe_scope_resolution()
    history = _probe_history()
    response_schema = DevAnswer.model_json_schema(mode="validation")
    usage = AgentUsage()

    try:
        composed_round_1 = composer.compose(
            question=_PROBE_QUESTION,
            scope=scope_resolution,
            prior_turns=history,
            tool_results=(),
            allowed_tools=None,
            subject_committed=True,
        )
        _assert_production_shape(composed_round_1.system_text, tools)
        messages_round_1 = (
            AgentMessage(AgentMessageRole.SYSTEM, composed_round_1.system_text),
            AgentMessage(AgentMessageRole.USER, composed_round_1.user_text),
        )
        result_1 = await provider.decide(
            messages_round_1,
            tools,
            response_schema,
            timeout_seconds,
            limits.max_output_tokens_per_call,
            signal,
        )
        usage = _merge_usage(usage, result_1.usage)
        if not isinstance(result_1.decision, AgentToolRequest):
            raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)

        tool_result = _probe_tool_result(result_1.decision)

        result_2_committed, composed_round_2_committed = await _decide_round_2(
            provider=provider,
            composer=composer,
            scope_resolution=scope_resolution,
            history=history,
            tools=tools,
            response_schema=response_schema,
            tool_request=result_1.decision,
            tool_result=tool_result,
            timeout_seconds=timeout_seconds,
            max_output_tokens=limits.max_output_tokens_per_call,
            subject_committed=True,
            signal=signal,
        )
        usage = _merge_usage(usage, result_2_committed.usage)

        result_2_uncommitted, composed_round_2_uncommitted = await _decide_round_2(
            provider=provider,
            composer=composer,
            scope_resolution=scope_resolution,
            history=history,
            tools=tools,
            response_schema=response_schema,
            tool_request=result_1.decision,
            tool_result=tool_result,
            timeout_seconds=timeout_seconds,
            max_output_tokens=limits.max_output_tokens_per_call,
            subject_committed=False,
            signal=signal,
        )
        usage = _merge_usage(usage, result_2_uncommitted.usage)

        observed_bytes = max(
            len(composed_round_2_committed.system_text.encode("utf-8"))
            + len(composed_round_2_committed.user_text.encode("utf-8")),
            len(composed_round_2_uncommitted.system_text.encode("utf-8"))
            + len(composed_round_2_uncommitted.user_text.encode("utf-8")),
        )
        return LegacyAgentProbeResult(
            state=RoleCertificationState.COMPATIBLE,
            safe_error_code=None,
            usage=usage,
            observed_request_bytes=observed_bytes,
        )
    except AgentProviderError as exc:
        safe = safe_agent_provider_error(exc)
        # A failure raised by provider.decide() itself (e.g. OUTPUT_EXHAUSTED)
        # carries the usage of the call that failed, which was never merged
        # into ``usage`` above because that call's ``result`` was never
        # returned; a failure this function raised itself over an
        # already-returned decision has no separate usage to add (it is
        # already folded into ``usage``).
        if safe.usage is not None:
            usage = _merge_usage(usage, safe.usage)
        return LegacyAgentProbeResult(
            state=role_state_for_safe_error_code(safe.code.value),
            safe_error_code=safe.code.value,
            usage=usage,
            observed_request_bytes=0,
        )


__all__ = ["LegacyAgentProbeResult", "certify_legacy_agent"]
