from __future__ import annotations

from .base import CompletionResult, LLMProviderBase


class NoneProvider(LLMProviderBase):
    async def complete(self, prompt: str) -> CompletionResult:
        return CompletionResult(
            text="", input_tokens=None, output_tokens=None, model="none"
        )

    async def aclose(self) -> None:
        pass
