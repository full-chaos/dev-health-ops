"""CHAOS-3285: the production-sized legacy_agent probe reproduces
OUTPUT_EXHAUSTED where the synthetic transport-echo probe cannot, and clears
a compliant provider under the identical request-shape mechanism.

Per the "four verification rules" (Rule 2 -- observe the guard failing):
exhaustion here is a deterministic function of the REAL serialized request
the production ``OpenAICompatibleAgentProvider.decide()`` builds -- driven by
``_RequestSizeDrivenClient.tokens_per_byte`` -- never a hard-coded "return
exhausted". The RED/GREEN pair below proves both directions: the same
provider/budget combination that reproduces exhaustion for a
production-sized request clears it for a smaller (compliant) one, and the
old 512-token echo probe cannot see either case because its own request is
never production-sized.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.api.dev.prompts.composer import (
    LEGACY_PROMPT_VERSION,
    PROMPT_VERSION,
)
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.agent.probes.legacy_agent import (
    _PRODUCTION_FLOOR_BYTES,
    _assert_production_shape,
    certify_legacy_agent,
)
from dev_health_ops.llm.agent.readiness import (
    AgentReadinessOutcome,
    AgentReadinessService,
)
from dev_health_ops.llm.agent.roles import RoleCertificationState

# A model whose per-byte cost is low enough that even the full
# production-sized request (~65 KB) stays well inside the 4,096-token
# envelope (~1.3K reasoning tokens at this rate).
_COMPLIANT_TOKENS_PER_BYTE = 0.02


def _stub_response(
    *,
    finish_reason: str,
    content: str | None,
    tool_calls: list[Any] | None,
    prompt_tokens: int,
    completion_tokens: int,
    reasoning_tokens: int,
) -> Any:
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    usage = SimpleNamespace(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        prompt_tokens_details=SimpleNamespace(cached_tokens=0),
        completion_tokens_details=SimpleNamespace(reasoning_tokens=reasoning_tokens),
    )
    choice = SimpleNamespace(message=message, finish_reason=finish_reason)
    return SimpleNamespace(choices=[choice], usage=usage)


def _stub_tool_call(*, name: str, arguments: dict[str, Any]) -> Any:
    return SimpleNamespace(
        id="stub-call-1",
        function=SimpleNamespace(name=name, arguments=json.dumps(arguments)),
    )


class _RequestSizeDrivenClient:
    """Fake OpenAI client whose exhaustion behavior is a deterministic
    function of the REAL serialized request the adapter builds -- never a
    hard-coded "return exhausted" (verification Rule 2).

    Serves both the readiness-echo (single tool, tiny request) and the
    legacy_agent (full registry, production-sized request) request shapes:
    round 1 always names whichever tool is offered first (schema-compliant
    args are not validated client-side by the adapter); round 2 answers
    ``{"nonce": "ready-v1"}`` when the flow was echo (detected from round 1's
    tool name) or a DevAnswer-shaped draft otherwise.
    """

    def __init__(self, *, tokens_per_byte: float) -> None:
        self._tokens_per_byte = tokens_per_byte
        self._echo_mode: bool | None = None
        self.requests: list[dict[str, Any]] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def reasoning_tokens_for(self, kwargs: dict[str, Any]) -> int:
        """The deterministic function under test: reasoning cost as a
        function of the REAL serialized request. Exposed so counterfactual
        tests can evaluate it against a modified copy of a captured request
        without needing a second live round-trip."""

        serialized = json.dumps(kwargs, sort_keys=True, default=str).encode("utf-8")
        return round(len(serialized) * self._tokens_per_byte)

    async def _create(self, **kwargs: Any) -> Any:
        self.requests.append(kwargs)
        tools = kwargs.get("tools") or []
        if self._echo_mode is None:
            tool_names = {str(item["function"]["name"]) for item in tools}
            self._echo_mode = "readiness_echo_v1" in tool_names

        serialized = json.dumps(kwargs, sort_keys=True, default=str).encode("utf-8")
        reasoning_tokens = self.reasoning_tokens_for(kwargs)
        max_completion = int(kwargs["max_completion_tokens"])
        if reasoning_tokens >= max_completion:
            return _stub_response(
                finish_reason="length",
                content="",
                tool_calls=None,
                prompt_tokens=len(serialized) // 4,
                completion_tokens=max_completion,
                reasoning_tokens=reasoning_tokens,
            )

        if kwargs.get("tool_choice") == "required":
            name = str(tools[0]["function"]["name"])
            arguments = {"nonce": "ready-v1"} if self._echo_mode else {}
            return _stub_response(
                finish_reason="tool_calls",
                content=None,
                tool_calls=[_stub_tool_call(name=name, arguments=arguments)],
                prompt_tokens=len(serialized) // 4,
                completion_tokens=8,
                reasoning_tokens=reasoning_tokens,
            )

        value = (
            {"nonce": "ready-v1"}
            if self._echo_mode
            else {"status": "complete", "direct_summary": "Stub legacy_agent answer."}
        )
        return _stub_response(
            finish_reason="stop",
            content=json.dumps({"kind": "final_answer", "value": value}),
            tool_calls=None,
            prompt_tokens=len(serialized) // 4,
            completion_tokens=12,
            reasoning_tokens=reasoning_tokens,
        )


def _provider(
    *, tokens_per_byte: float, model: str = "gpt-5-nano"
) -> tuple[OpenAICompatibleAgentProvider, _RequestSizeDrivenClient]:
    client = _RequestSizeDrivenClient(tokens_per_byte=tokens_per_byte)
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used", model=model, base_url="http://127.0.0.1:1/v1", client=client
    )
    return provider, client


class _Store:
    async def load(self):  # pragma: no cover - readiness.py's own store protocol
        return None

    async def save(self, record) -> None:
        self.record = record


async def _uncalibrated_round_requests() -> tuple[dict[str, Any], dict[str, Any]]:
    """Run the real probe against a client that never exhausts (rate 0), to
    observe the real round-1 and round-2 (committed-subject) request shapes
    it sends without inducing exhaustion. Calibration is derived from
    these -- never a hand-guessed rate.

    CHAOS-3285 round 2 (Codex HIGH): the probe now sends a THIRD request --
    round 2 again under the uncommitted-subject prompt shape
    (LEGACY_PROMPT_VERSION), proving that shape too rather than only ever
    exercising the committed one. This calibration helper deliberately
    keeps using only the first two (round 1 and the committed round 2):
    the combined-shape claim it exists to prove (tool_choice="auto" + the
    strict grammar + accumulated history, together, is what exhausts) is
    the same claim for either prompt shape, and testing it twice here would
    duplicate coverage rather than add any."""

    provider, client = _provider(tokens_per_byte=0.0)
    result = await certify_legacy_agent(provider, timeout_seconds=30)
    assert result.state is RoleCertificationState.COMPATIBLE, (
        "calibration precondition failed: a rate of 0 must never exhaust"
    )
    assert len(client.requests) == 3
    return client.requests[0], client.requests[1]


@pytest.mark.asyncio
async def test_certify_legacy_agent_probes_both_prompt_shapes() -> None:
    """CHAOS-3285 round 2 (Codex HIGH): before this fix, the probe forced
    ``subject_committed=True`` unconditionally, so PromptComposer's OTHER
    shape -- ``LEGACY_PROMPT_VERSION`` (uncommitted subject / the Wave 3.1
    flag off) -- was never exercised at all, and the fingerprint never
    folded that constant either, so a text-only change to only that shape
    invalidated nothing. Prove both shapes are now actually sent, not just
    that a fingerprint constant changed."""

    provider, client = _provider(tokens_per_byte=0.0)
    result = await certify_legacy_agent(provider, timeout_seconds=30)

    assert result.state is RoleCertificationState.COMPATIBLE
    assert len(client.requests) == 3

    round_2_committed, round_2_uncommitted = client.requests[1], client.requests[2]
    # Both are the same worst-case combined shape (tool_choice="auto" + the
    # strict grammar)...
    for request in (round_2_committed, round_2_uncommitted):
        assert request.get("tool_choice") == "auto"
        assert request.get("response_format") is not None

    # ...but the composed system prompt genuinely differs between them --
    # proof this is really two distinct prompt shapes being sent, not the
    # same request twice.
    committed_system_text = round_2_committed["messages"][0]["content"]
    uncommitted_system_text = round_2_uncommitted["messages"][0]["content"]
    assert committed_system_text != uncommitted_system_text
    assert PROMPT_VERSION in committed_system_text
    assert PROMPT_VERSION not in uncommitted_system_text
    assert LEGACY_PROMPT_VERSION in uncommitted_system_text
    assert LEGACY_PROMPT_VERSION not in committed_system_text


def _without_grammar(round_2: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in round_2.items() if key != "response_format"}


def _without_history(
    round_1: dict[str, Any], round_2: dict[str, Any]
) -> dict[str, Any]:
    """Round 2's shape (tool_choice=auto, the strict grammar) with round 1's
    own tool-result-free messages -- isolates the grammar dimension from the
    accumulated tool-result history dimension."""

    without_history = dict(round_2)
    without_history["messages"] = round_1["messages"]
    return without_history


def _calibrate_noncompliant_rate(
    round_1: dict[str, Any], round_2: dict[str, Any], *, cap: int
) -> float:
    """Find a tokens-per-byte rate where round 1 alone stays under the cap,
    EITHER of round 2's two structural additions (the strict grammar,
    the tool-result history) alone also stays under the cap, and only their
    COMBINATION -- round 2 as actually sent -- reaches it. This is what
    "round 2 alone exhausts, and both dimensions are required" means as a
    falsifiable, calibrated claim rather than "the request is big."
    """

    client = _RequestSizeDrivenClient(tokens_per_byte=1.0)
    insufficient_alone = max(
        client.reasoning_tokens_for(round_1),
        client.reasoning_tokens_for(_without_grammar(round_2)),
        client.reasoning_tokens_for(_without_history(round_1, round_2)),
    )
    full_combination = client.reasoning_tokens_for(round_2)

    lower = cap / full_combination
    upper = cap / insufficient_alone
    assert lower < upper, (
        "no rate exists where round 1 and each single dimension of round 2 "
        "stay under the cap while their combination exceeds it -- the "
        "production request shape changed enough that this calibration "
        "needs to be revisited, not silently forced"
    )
    return (lower + upper) / 2


@pytest.mark.asyncio
async def test_production_sized_legacy_probe_reproduces_exhaustion() -> None:
    """The new probe (RED): round 1 (tools offered, no grammar, no tool
    result yet) succeeds; round 2 alone -- the combined tool_choice="auto"
    AND strict DevAnswer grammar AND accumulated tool-result history shape
    every real round >= 2 sends -- is what exhausts the 4,096-token envelope
    (TRD Option B; this PR does not change that cap)."""

    round_1_shape, round_2_shape = await _uncalibrated_round_requests()
    rate = _calibrate_noncompliant_rate(round_1_shape, round_2_shape, cap=4096)

    provider, client = _provider(tokens_per_byte=rate)
    result = await certify_legacy_agent(provider, timeout_seconds=30)

    assert result.state is RoleCertificationState.INCOMPATIBLE
    assert result.safe_error_code == "output_exhausted"
    assert len(client.requests) == 2

    round_1, round_2 = client.requests
    # Round 1 genuinely succeeded (did not itself exhaust) -- the defect is
    # specifically in round 2, not "any request this size fails."
    assert round_1.get("tool_choice") == "required"
    assert "response_format" not in round_1
    assert client.reasoning_tokens_for(round_1) < round_1["max_completion_tokens"]

    # Round 2 is the one that exhausted, and carries exactly the claimed
    # combined shape.
    assert round_2.get("tool_choice") == "auto"
    response_format = round_2.get("response_format")
    assert response_format is not None
    assert response_format["json_schema"]["strict"] is True
    max_completion = round_2["max_completion_tokens"]
    assert client.reasoning_tokens_for(round_2) >= max_completion

    # Counterfactuals: removing EITHER dimension alone -- the grammar, or
    # the accumulated tool-result history -- avoids exhaustion at this same
    # rate. Only their combination does.
    assert client.reasoning_tokens_for(_without_grammar(round_2)) < max_completion
    assert (
        client.reasoning_tokens_for(_without_history(round_1, round_2)) < max_completion
    )

    # Sanity: the request that triggered exhaustion really was
    # production-sized, not a shrunk stand-in (Rule 4).
    assert (
        len(json.dumps(round_2, sort_keys=True, default=str)) >= _PRODUCTION_FLOOR_BYTES
    )


@pytest.mark.asyncio
async def test_transport_echo_probe_does_not_reproduce_exhaustion() -> None:
    """The OLD probe (fail-before pair): the exact same non-compliant
    provider (calibrated against the production request shape) certifies
    READY under the pre-existing 512-token echo probe, because that probe's
    request is never production-sized. This is what makes the new probe an
    actual gap-closer, not a redundant check."""

    round_1_shape, round_2_shape = await _uncalibrated_round_requests()
    rate = _calibrate_noncompliant_rate(round_1_shape, round_2_shape, cap=4096)

    provider, client = _provider(tokens_per_byte=rate)
    store = _Store()
    record = await AgentReadinessService(store).certify(
        provider,
        provider_name="openai",
        model=provider.model,
        fingerprint="probe-fingerprint",
    )

    assert record.outcome is AgentReadinessOutcome.READY
    # Sanity: the transport-echo probe's own requests really were tiny
    # relative to the production floor -- confirming it genuinely could not
    # have observed the defect, not that our stub is miscalibrated.
    for request in client.requests:
        assert (
            len(json.dumps(request, sort_keys=True, default=str))
            < _PRODUCTION_FLOOR_BYTES
        )


@pytest.mark.asyncio
async def test_production_sized_legacy_probe_certifies_a_compliant_model() -> None:
    """The new probe (GREEN): the identical production-sized request shape
    certifies COMPATIBLE against a provider whose reasoning cost fits the
    same 4,096-token envelope."""

    provider, _client = _provider(tokens_per_byte=_COMPLIANT_TOKENS_PER_BYTE)
    result = await certify_legacy_agent(provider, timeout_seconds=30)

    assert result.state is RoleCertificationState.COMPATIBLE
    assert result.safe_error_code is None
    assert result.observed_request_bytes >= _PRODUCTION_FLOOR_BYTES


def test_production_shape_guard_fails_loudly_on_a_shrunk_probe() -> None:
    """Rule 4: a probe whose composed request silently shrank below
    production scale must fail loudly, not silently pass as coverage."""

    from dev_health_ops.api.dev.tool_registry import TOOL_DEFINITIONS
    from dev_health_ops.llm.agent.contracts import AgentToolDefinition

    full_tool_set = tuple(
        AgentToolDefinition(tool_id=item.tool_id.value, description="", input_schema={})
        for item in TOOL_DEFINITIONS
    )

    with pytest.raises(
        AssertionError, match="below the production-representative floor"
    ):
        _assert_production_shape("tiny prompt", tools=full_tool_set)

    with pytest.raises(AssertionError, match="full 9-tool registry"):
        _assert_production_shape("x" * (_PRODUCTION_FLOOR_BYTES + 1), tools=())


class _InMemoryRoleStore:
    def __init__(self) -> None:
        from dev_health_ops.llm.agent.roles import RoleCertificationProfile

        self.profile = RoleCertificationProfile()

    async def load(self):
        return self.profile

    async def save(self, profile) -> None:
        self.profile = profile

    async def save_record(self, record) -> None:
        self.profile = self.profile.with_record(record)


@pytest.mark.asyncio
async def test_role_readiness_service_persists_legacy_agent_certification() -> None:
    from dev_health_ops.llm.agent.role_readiness import RoleReadinessService
    from dev_health_ops.llm.agent.roles import AgentRole

    provider, _client = _provider(tokens_per_byte=_COMPLIANT_TOKENS_PER_BYTE)
    store = _InMemoryRoleStore()
    service = RoleReadinessService(store)

    record = await service.certify_role(
        AgentRole.LEGACY_AGENT, provider, certification_key="key-01"
    )

    assert record.state is RoleCertificationState.COMPATIBLE
    assert store.profile.for_role(AgentRole.LEGACY_AGENT) == record


@pytest.mark.asyncio
async def test_role_readiness_service_rejects_unimplemented_roles() -> None:
    from dev_health_ops.llm.agent.role_readiness import RoleReadinessService
    from dev_health_ops.llm.agent.roles import AgentRole

    provider, _client = _provider(tokens_per_byte=_COMPLIANT_TOKENS_PER_BYTE)
    service = RoleReadinessService(_InMemoryRoleStore())

    with pytest.raises(NotImplementedError):
        await service.certify_role(
            AgentRole.INTENT_CLASSIFICATION, provider, certification_key="key-01"
        )
