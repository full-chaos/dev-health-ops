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
