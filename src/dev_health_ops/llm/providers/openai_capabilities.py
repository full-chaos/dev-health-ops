"""Shared model capability policy for OpenAI wire adapters."""

from __future__ import annotations

import re
from collections.abc import Iterable

# OpenAI's Chat Completions API requires tools[].function.name (and the
# native tool_calls[].function.name a model echoes back) to match this
# pattern. Ask Dev's canonical registry tool identifiers are dotted
# (e.g. "query_metric.v1"), which is illegal on the wire -- every real
# OpenAI-backed request using one 400s (CHAOS-3286).
_WIRE_LEGAL_TOOL_NAME = re.compile(r"^[a-zA-Z0-9_-]+$")
_WIRE_ILLEGAL_CHARACTER = re.compile(r"[^a-zA-Z0-9_-]")


def sanitize_tool_name(tool_id: str) -> str:
    """Map a canonical tool_id to an OpenAI wire-legal ``function.name``.

    Deterministic and applied only at the adapter's outbound wire boundary:
    every character outside ``[a-zA-Z0-9_-]`` (in practice just ``.``) is
    replaced with ``_``. The canonical dotted ``tool_id`` remains the
    identifier used everywhere else -- contracts, persistence, telemetry,
    and Ask Dev's own JSON decision-envelope fallback schema (which is not
    subject to this wire constraint at all, since it's a JSON Schema string
    enum inside ``response_format``, not a native ``tools[]`` definition).
    """

    return _WIRE_ILLEGAL_CHARACTER.sub("_", tool_id)


def is_wire_legal_tool_name(name: str) -> bool:
    """Return whether ``name`` already satisfies OpenAI's function-name pattern."""

    return bool(_WIRE_LEGAL_TOOL_NAME.match(name))


def build_wire_tool_name_map(tool_ids: Iterable[str]) -> dict[str, str]:
    """Build the wire-name -> canonical-tool_id reverse map for one request.

    Raises ``ValueError`` if two distinct tool_ids would sanitize to the same
    wire name -- a mapping collision must never silently misroute a tool
    decision. Callers that own a fixed, closed tool registry (e.g.
    ``AskDevToolRegistry``) should additionally assert collision-freedom at
    registry build time so this can never happen at request time in
    production; this function is the shared, generic enforcement any caller
    (including a synthetic readiness probe) gets for free.
    """

    reverse: dict[str, str] = {}
    for tool_id in tool_ids:
        wire_name = sanitize_tool_name(tool_id)
        existing = reverse.get(wire_name)
        if existing is not None and existing != tool_id:
            raise ValueError(
                f"tool name mapping collision: {existing!r} and {tool_id!r} "
                f"both sanitize to {wire_name!r}"
            )
        reverse[wire_name] = tool_id
    return reverse


def supports_temperature(model: str) -> bool:
    """Return whether the model accepts a caller-selected temperature.

    This policy is shared by the batch/completion provider and Ask Dev's
    OpenAI-compatible agent adapter so an OpenAI platform model receives one
    consistent request shape regardless of its Dev Health use case.
    """

    return not model.strip().lower().startswith(("gpt-5", "o1", "o3"))


def chat_completion_reasoning_effort(model: str) -> str | None:
    """Return the Chat Completions reasoning control required by GPT-5.

    GPT-5 counts reasoning tokens against ``max_completion_tokens``. Keeping
    the default reasoning effort can exhaust the bounded Ask Dev response
    budget before the model emits its required structured decision.
    """

    if model.strip().lower().startswith("gpt-5"):
        return "minimal"
    return None


def supports_parallel_tool_calls(model: str) -> bool:
    """Return whether the wire API accepts the ``parallel_tool_calls`` control.

    Unlike ``supports_temperature``, GPT-5 is NOT bucketed with the o-series
    models here. Live-verified against the real OpenAI Chat Completions API
    (2026-07-30, org key, request/response bodies not retained -- only HTTP
    status and the safe error-classification fields):

    * ``gpt-5-mini`` and ``gpt-5-nano`` with
      ``tools`` + ``tool_choice: "auto"`` + ``reasoning_effort: "minimal"`` +
      ``parallel_tool_calls: false`` -> HTTP 200 (accepted).
    * ``o3-mini`` and ``o4-mini`` with the same request shape (no
      ``reasoning_effort``, which this adapter never sends for o-series) ->
      HTTP 400 ``invalid_request_error`` / ``unsupported_parameter`` /
      ``param: "parallel_tool_calls"`` for both.
    * ``o1-mini`` was not accessible under the probing key (404
      model_not_found) -- inconclusive on its own, but excluded anyway since
      it shares the o1/o3/o4 Chat Completions reasoning-model family and
      community reports (e.g. langchain#29704, semantic-kernel#10365,
      litellm#8980) independently confirm o1 rejects this parameter too.

    So only the true o-series reasoning models reject this parameter outright;
    GPT-5's Chat Completions surface accepts it and needs it disabled just
    like any other tool-calling model to enforce Ask Dev's sequential
    one-decision contract on the wire (CHAOS-3254). For o-series models the
    contract is instead enforced purely on the response side (see
    ``OpenAICompatibleAgentProvider._normalize_response``), so omitting the
    field there is a verified necessity, not an assumption.
    """

    return not model.strip().lower().startswith(("o1", "o3", "o4"))
