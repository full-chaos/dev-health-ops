from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class CompletionResult:
    text: str
    input_tokens: int | None
    output_tokens: int | None
    model: str


@runtime_checkable
class LLMProvider(Protocol):
    async def complete(self, prompt: str) -> CompletionResult:
        raise NotImplementedError()

    async def complete_text(self, prompt: str) -> str:
        raise NotImplementedError()

    async def aclose(self) -> None:
        raise NotImplementedError()


class LLMProviderBase:
    async def complete(self, prompt: str) -> CompletionResult:
        raise NotImplementedError()

    async def complete_text(self, prompt: str) -> str:
        return (await self.complete(prompt)).text


DEFAULT_MODEL_BY_PROVIDER: dict[str, str] = {
    "openai": "gpt-5-mini",
    "anthropic": "claude-3-haiku-20240307",
    "gemini": "gemini-3",
    "local": "llama3.2",
    "ollama": "llama3.2",
    "lmstudio": "local-model",
    "qwen": "qwen-plus",
    "qwen-local": "qwen2.5:7b",
    "qwen-lmstudio": "local-model",
}


def usage_token_count(usage: object | None, *field_names: str) -> int | None:
    if usage is None:
        return None

    for field_name in field_names:
        if isinstance(usage, dict):
            raw_value = usage.get(field_name)
        else:
            raw_value = getattr(usage, field_name, None)

        if isinstance(raw_value, bool) or raw_value is None:
            continue
        if isinstance(raw_value, int):
            return raw_value
        if isinstance(raw_value, float):
            return int(raw_value)
        if isinstance(raw_value, str):
            try:
                return int(raw_value)
            except ValueError:
                continue

    return None
