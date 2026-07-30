"""Shared model capability policy for OpenAI wire adapters."""

from __future__ import annotations


def supports_temperature(model: str) -> bool:
    """Return whether the model accepts a caller-selected temperature.

    This policy is shared by the batch/completion provider and Ask Dev's
    OpenAI-compatible agent adapter so an OpenAI platform model receives one
    consistent request shape regardless of its Dev Health use case.
    """

    return not model.strip().lower().startswith(("gpt-5", "o1", "o3"))
