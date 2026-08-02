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
from dev_health_ops.llm.agent.openai_compatible import (
    _DECISION_FIELDS,
    OpenAICompatibleAgentProvider,
)
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

    CHAOS-3285 round 4 (Codex HIGH): the probe now runs two fully
    independent chains -- committed-subject (requests[0], requests[1]) and
    uncommitted-subject (requests[2], requests[3]) -- four requests total.
    This calibration helper deliberately keeps using only the committed
    chain's two requests: the combined-shape claim it exists to prove
    (tool_choice="auto" + the strict grammar + accumulated history,
    together, is what exhausts) is the same claim for either prompt shape,
    and testing it twice here would duplicate coverage rather than add
    any."""

    provider, client = _provider(tokens_per_byte=0.0)
    result = await certify_legacy_agent(provider, timeout_seconds=30)
    assert result.state is RoleCertificationState.COMPATIBLE, (
        "calibration precondition failed: a rate of 0 must never exhaust"
    )
    assert len(client.requests) == 4
    return client.requests[0], client.requests[1]


@pytest.mark.asyncio
async def test_certify_legacy_agent_probes_both_prompt_shapes() -> None:
    """CHAOS-3285 round 2 (Codex HIGH): before that fix, the probe forced
    ``subject_committed=True`` unconditionally, so PromptComposer's OTHER
    shape -- ``LEGACY_PROMPT_VERSION`` (uncommitted subject / the Wave 3.1
    flag off) -- was never exercised at all. CHAOS-3285 round 4 (Codex
    HIGH): round 1 itself is now also composed under each chain's own
    shape (it was previously ALWAYS committed-subject, even for the
    "uncommitted" chain's round 2, which then reused round 1's committed
    tool request/result). Prove all four requests -- both rounds, both
    shapes -- are genuinely distinct and shape-correct."""

    provider, client = _provider(tokens_per_byte=0.0)
    result = await certify_legacy_agent(provider, timeout_seconds=30)

    assert result.state is RoleCertificationState.COMPATIBLE
    assert len(client.requests) == 4

    round_1_committed, round_2_committed, round_1_uncommitted, round_2_uncommitted = (
        client.requests
    )

    # Round 1: tool_choice="required", no grammar yet, under EACH chain's
    # own shape -- the exact combination round 4 found was never sent for
    # the uncommitted chain before this fix.
    for round_1 in (round_1_committed, round_1_uncommitted):
        assert round_1.get("tool_choice") == "required"
        assert "response_format" not in round_1
    round_1_committed_text = round_1_committed["messages"][0]["content"]
    round_1_uncommitted_text = round_1_uncommitted["messages"][0]["content"]
    assert round_1_committed_text != round_1_uncommitted_text
    assert PROMPT_VERSION in round_1_committed_text
    assert PROMPT_VERSION not in round_1_uncommitted_text
    assert LEGACY_PROMPT_VERSION in round_1_uncommitted_text
    assert LEGACY_PROMPT_VERSION not in round_1_committed_text

    # Round 2: both are the same worst-case combined shape (tool_choice=
    # "auto" + the strict grammar)...
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


class _UncommittedRound1DefectClient:
    """A provider that fails ONLY on round 1's own ``tool_choice="required"``
    shape under the uncommitted-subject prompt -- the committed chain
    entirely, and the uncommitted chain's round 2, all succeed normally.
    Isolates the exact combination CHAOS-3285 round 4 (Codex HIGH) found
    was never sent to the provider at all before this fix (round 1 was
    always composed committed-subject, and round 2's uncommitted call
    reused round 1's committed tool request/result)."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    async def _create(self, **kwargs: Any) -> Any:
        self.requests.append(kwargs)
        tools = kwargs.get("tools") or []
        system_text = str(kwargs["messages"][0]["content"])
        serialized = json.dumps(kwargs, sort_keys=True, default=str).encode("utf-8")

        if (
            kwargs.get("tool_choice") == "required"
            and LEGACY_PROMPT_VERSION in system_text
        ):
            # The defect: this exact shape exhausts every time.
            max_completion = int(kwargs["max_completion_tokens"])
            return _stub_response(
                finish_reason="length",
                content="",
                tool_calls=None,
                prompt_tokens=len(serialized) // 4,
                completion_tokens=max_completion,
                reasoning_tokens=max_completion,
            )

        if kwargs.get("tool_choice") == "required":
            name = str(tools[0]["function"]["name"])
            return _stub_response(
                finish_reason="tool_calls",
                content=None,
                tool_calls=[_stub_tool_call(name=name, arguments={})],
                prompt_tokens=len(serialized) // 4,
                completion_tokens=8,
                reasoning_tokens=8,
            )

        return _stub_response(
            finish_reason="stop",
            content=json.dumps(
                {
                    "kind": "final_answer",
                    "value": {
                        "status": "complete",
                        "direct_summary": "Stub legacy_agent answer.",
                    },
                }
            ),
            tool_calls=None,
            prompt_tokens=len(serialized) // 4,
            completion_tokens=12,
            reasoning_tokens=12,
        )


@pytest.mark.asyncio
async def test_certify_legacy_agent_fails_on_a_defect_isolated_to_uncommitted_round_1() -> (
    None
):
    """CHAOS-3285 round 4 (Codex HIGH): a provider that fails ONLY on the
    combination of the uncommitted-subject prompt (LEGACY_PROMPT_VERSION)
    AND round 1's own tool_choice="required" shape must certify as failing
    overall -- not COMPATIBLE. Before this fix, round 1 was ALWAYS composed
    under subject_committed=True regardless of which chain it fed, so this
    exact combination was never sent to the provider at all, and a provider
    genuinely broken on it would still have certified COMPATIBLE."""

    client = _UncommittedRound1DefectClient()
    provider = OpenAICompatibleAgentProvider(
        api_key="not-used",
        model="gpt-5-nano",
        base_url="http://127.0.0.1:1/v1",
        client=client,
    )

    result = await certify_legacy_agent(provider, timeout_seconds=30)

    assert result.state is not RoleCertificationState.COMPATIBLE
    # The defect really was reached -- round 1 under the uncommitted shape
    # was actually sent, not skipped or silently reusing the committed
    # chain's round 1.
    uncommitted_round_1_sent = any(
        request.get("tool_choice") == "required"
        and LEGACY_PROMPT_VERSION in str(request["messages"][0]["content"])
        for request in client.requests
    )
    assert uncommitted_round_1_sent


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


# CHAOS-3285 round 2 (Codex MEDIUM): the calibration window
# (cap/full_combination, cap/insufficient_alone) is non-empty for ANY
# strictly-positive byte difference, even one byte -- that alone proves
# nothing about whether the grammar/history dimensions meaningfully drive
# exhaustion, only that they are non-identical. These margins require the
# combined shape to be substantively larger than the largest single
# dimension, in both absolute and relative terms, before trusting the
# calibration at all. Measured against the real production shapes this
# probe currently sends: absolute margin ~3.7 KB, relative margin ~6% --
# both comfortably clear these floors with room for prompt-text drift.
_MIN_CALIBRATION_ABSOLUTE_MARGIN_BYTES = 1_500
_MIN_CALIBRATION_RELATIVE_MARGIN = 1.03


def _assert_structurally_real_grammar(round_2: dict[str, Any]) -> None:
    """CHAOS-3285 round 4 (Codex MEDIUM): byte-size margins ALONE can be
    satisfied by PADDING -- a large ``description`` string stuffed into an
    otherwise-empty schema, with no real ``properties``/``required`` -- so a
    mutation that gutted the grammar's actual structure but kept its byte
    size large would still clear the margin checks below. Assert the real
    decision-schema shape is present BEFORE trusting any byte margin:
    non-empty ``properties``, and a ``required`` set matching the real
    decision envelope (``_DECISION_FIELDS``, the actual producer -- never a
    hand-guessed field list)."""

    response_format = round_2.get("response_format")
    assert isinstance(response_format, dict), "round 2 must carry a response_format"
    json_schema = response_format.get("json_schema")
    assert isinstance(json_schema, dict), "response_format must wrap a json_schema"
    assert json_schema.get("strict") is True, "grammar must be strict"
    schema = json_schema.get("schema")
    assert isinstance(schema, dict), "json_schema must carry a schema body"
    properties = schema.get("properties")
    assert isinstance(properties, dict) and properties, (
        "grammar has no real schema properties -- padding a description "
        "string does not substitute for actual grammar structure"
    )
    required = schema.get("required")
    assert isinstance(required, list) and set(required) == _DECISION_FIELDS, (
        "grammar's required fields do not match the real decision envelope "
        "-- padding does not substitute for the actual schema shape"
    )


def _assert_structurally_real_history(round_2: dict[str, Any]) -> None:
    """Same guard for the accumulated tool-result history dimension: a
    large padded content string in the assistant/tool messages is not the
    same as a real tool request followed by a schema-shaped tool result."""

    messages = round_2.get("messages")
    assert isinstance(messages, list) and len(messages) >= 4, (
        "round 2 must carry the full history shape: system, user, "
        "assistant tool-request, tool result"
    )
    assistant_message = messages[2]
    assert assistant_message.get("role") == "assistant"
    tool_calls = assistant_message.get("tool_calls")
    assert isinstance(tool_calls, list) and tool_calls, (
        "history's assistant message has no real tool_calls -- padding "
        "content does not substitute for a real tool request"
    )
    tool_message = messages[3]
    assert tool_message.get("role") == "tool"
    content = tool_message.get("content")
    assert isinstance(content, str)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = None
    assert isinstance(parsed, dict) and {"run_id", "tool_id", "tool_call_id"} <= set(
        parsed
    ), (
        "history's tool message content is not a schema-shaped tool "
        "result -- padding does not substitute for real tool-result "
        "structure"
    )


def _calibrate_noncompliant_rate(
    round_1: dict[str, Any], round_2: dict[str, Any], *, cap: int
) -> float:
    """Find a tokens-per-byte rate where round 1 alone stays under the cap,
    EITHER of round 2's two structural additions (the strict grammar,
    the tool-result history) alone also stays under the cap, and only their
    COMBINATION -- round 2 as actually sent -- reaches it. This is what
    "round 2 alone exhausts, and both dimensions are required" means as a
    falsifiable, calibrated claim rather than "the request is big."

    Rule 4 (a measurement that did not happen must fail loudly): a
    calibration window that exists only because of a trivial byte
    difference is not a real proof that the grammar or the history
    dimension matters -- it means the probe's production shape has become
    degenerate (round 1 and round 2 barely differ) and this calibration
    must refuse to silently produce a knife-edge rate. CHAOS-3285 round 4
    (Codex MEDIUM): byte margins are necessary but NOT sufficient -- a
    padded-but-structurally-empty grammar or history can satisfy them
    without the dimension actually contributing any real structure, so
    semantic-structure checks run FIRST, before any byte-margin math.
    """

    _assert_structurally_real_grammar(round_2)
    _assert_structurally_real_history(round_2)

    client = _RequestSizeDrivenClient(tokens_per_byte=1.0)
    insufficient_alone = max(
        client.reasoning_tokens_for(round_1),
        client.reasoning_tokens_for(_without_grammar(round_2)),
        client.reasoning_tokens_for(_without_history(round_1, round_2)),
    )
    full_combination = client.reasoning_tokens_for(round_2)

    absolute_margin = full_combination - insufficient_alone
    relative_margin = (
        full_combination / insufficient_alone if insufficient_alone else float("inf")
    )
    assert absolute_margin >= _MIN_CALIBRATION_ABSOLUTE_MARGIN_BYTES, (
        f"calibration margin too small ({absolute_margin} bytes < "
        f"{_MIN_CALIBRATION_ABSOLUTE_MARGIN_BYTES}) -- the probe's production "
        "shape is degenerate: neither the grammar nor the accumulated "
        "tool-result history contributes enough size to isolate the "
        "combined-shape claim from noise, not just calibrate a valid rate"
    )
    assert relative_margin >= _MIN_CALIBRATION_RELATIVE_MARGIN, (
        f"calibration margin too small ({relative_margin:.3f}x < "
        f"{_MIN_CALIBRATION_RELATIVE_MARGIN}x) -- same degenerate-shape failure "
        "as the absolute margin check, expressed relatively"
    )

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
async def test_calibration_fails_loudly_when_the_grammar_degenerates() -> None:
    """Mutation test (Rule 3/4): if the DevAnswer grammar's contribution to
    round 2's size were to collapse to near-nothing -- simulating a future
    regression that degenerates the combined-shape claim -- calibration
    must refuse to proceed with a knife-edge rate, not silently pick one
    that no longer proves the grammar dimension matters. CHAOS-3285 round 4:
    an empty schema is caught by the structural check now, before it would
    even reach the byte-margin math -- a strictly earlier, more precise
    failure than before."""

    round_1, round_2 = await _uncalibrated_round_requests()
    degenerate_grammar = dict(round_2)
    degenerate_grammar["response_format"] = {
        "json_schema": {"strict": True, "schema": {}}
    }

    with pytest.raises(AssertionError, match="no real schema properties"):
        _calibrate_noncompliant_rate(round_1, degenerate_grammar, cap=4096)


@pytest.mark.asyncio
async def test_calibration_fails_loudly_when_the_history_degenerates() -> None:
    """Same guard, for the accumulated tool-result history dimension: if
    round 2's messages collapsed to be identical to round 1's (the
    synthetic tool result contributing nothing), calibration must refuse
    to proceed. CHAOS-3285 round 4: round 1 has only 2 messages (no
    assistant tool-request, no tool result), so the structural shape check
    now catches this before the byte-margin math."""

    round_1, round_2 = await _uncalibrated_round_requests()
    degenerate_history = dict(round_2)
    degenerate_history["messages"] = round_1["messages"]

    with pytest.raises(AssertionError, match="full history shape"):
        _calibrate_noncompliant_rate(round_1, degenerate_history, cap=4096)


@pytest.mark.asyncio
async def test_calibration_fails_loudly_on_padded_but_structurally_empty_grammar() -> (
    None
):
    """CHAOS-3285 round 4 (Codex MEDIUM): byte-size margins ALONE can be
    satisfied by PADDING -- a large ``description`` string stuffed into an
    otherwise-empty schema, with no real ``properties``/``required`` -- so a
    mutant that gutted the grammar's actual structure while keeping its
    byte size large would previously have cleared the (byte-only) margin
    checks. Structural checks must catch this even though the byte margin
    would pass."""

    round_1, round_2 = await _uncalibrated_round_requests()
    padded_empty_grammar = dict(round_2)
    padded_empty_grammar["response_format"] = {
        "json_schema": {
            "strict": True,
            # Padded well past _MIN_CALIBRATION_ABSOLUTE_MARGIN_BYTES --
            # the byte-margin check alone would pass this.
            "schema": {"description": "x" * 5_000},
        }
    }

    with pytest.raises(AssertionError, match="no real schema properties"):
        _calibrate_noncompliant_rate(round_1, padded_empty_grammar, cap=4096)


@pytest.mark.asyncio
async def test_calibration_fails_loudly_on_padded_but_structurally_empty_history() -> (
    None
):
    """Same guard for history: a large padded content string in the
    assistant/tool messages is not the same as a real tool request followed
    by a schema-shaped tool result, even when it is byte-large enough to
    clear the margin checks on its own."""

    round_1, round_2 = await _uncalibrated_round_requests()
    padded_empty_history = dict(round_2)
    padded_empty_history["messages"] = [
        round_2["messages"][0],
        round_2["messages"][1],
        # Padded well past _MIN_CALIBRATION_ABSOLUTE_MARGIN_BYTES, but no
        # real tool_calls -- not a real tool request.
        {"role": "assistant", "content": "x" * 5_000},
        {"role": "tool", "content": "x" * 5_000, "tool_call_id": "stub"},
    ]

    with pytest.raises(AssertionError, match="no real tool_calls"):
        _calibrate_noncompliant_rate(round_1, padded_empty_history, cap=4096)


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
