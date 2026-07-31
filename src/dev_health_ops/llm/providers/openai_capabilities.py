"""Shared model capability policy for OpenAI wire adapters."""

from __future__ import annotations


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
