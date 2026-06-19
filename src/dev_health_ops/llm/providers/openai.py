"""
OpenAI LLM provider implementation.

Goals:
- Hard-separate GPT-5+ (Responses API) from legacy GPT (Chat Completions).
- Avoid "empty reasoning" outputs.
- Make JSON validity a first-class concern (validate + retry on truncation).

Notes:
- GPT-5 / GPT-5-mini MUST use the Responses API.
- Chat Completions is legacy and should only be used for older models.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from dev_health_ops.llm.errors import (
    LLMRateLimitError,
    classify_provider_error,
    is_retryable,
    retry_delay,
)
from dev_health_ops.llm.json_utils import validate_json_or_empty

from .base import (
    DEFAULT_MODEL_BY_PROVIDER,
    CompletionResult,
    LLMProviderBase,
    usage_token_count,
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Shared helpers
# -----------------------------------------------------------------------------


def is_json_schema_prompt(prompt: str) -> bool:
    """Heuristic: categorization prompts include the fixed schema keys."""
    text = prompt or ""
    return (
        "Output schema" in text
        and '"subcategories"' in text
        and '"evidence_quotes"' in text
        and '"uncertainty"' in text
    )


def system_message(prompt: str) -> str:
    if is_json_schema_prompt(prompt):
        return (
            "You are a specialized JSON generator.\n"
            "Return ONLY valid JSON.\n"
            "No markdown. No commentary.\n"
            "Output must start with { and end with }."
        )
    return (
        "You are an assistant that explains PRECOMPUTED work analytics.\n"
        "Use probabilistic language (appears, suggests, leans).\n"
        "Do NOT introduce new conclusions or recommendations.\n"
        "Return ONLY valid JSON."
    )


def categorization_json_schema() -> dict[str, Any]:
    """Strict schema for categorization outputs."""
    keys = [
        "feature_delivery.customer",
        "feature_delivery.enablement",
        "feature_delivery.roadmap",
        "maintenance.debt",
        "maintenance.refactor",
        "maintenance.upgrade",
        "operational.incident_response",
        "operational.on_call",
        "operational.support",
        "quality.bugfix",
        "quality.reliability",
        "quality.testing",
        "risk.compliance",
        "risk.security",
        "risk.vulnerability",
    ]

    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["subcategories", "evidence_quotes", "uncertainty"],
        "properties": {
            "subcategories": {
                "type": "object",
                "additionalProperties": False,
                "required": keys,
                "properties": {
                    k: {"type": "number", "minimum": 0, "maximum": 1} for k in keys
                },
            },
            "evidence_quotes": {
                "type": "array",
                "minItems": 1,
                "maxItems": 10,
                "items": {
                    "type": "object",
                    "required": ["quote", "source", "id"],
                    "additionalProperties": False,
                    "properties": {
                        "quote": {"type": "string"},
                        "source": {"type": "string", "enum": ["issue", "pr", "commit"]},
                        "id": {"type": "string"},
                    },
                },
            },
            "uncertainty": {"type": "string", "minLength": 1, "maxLength": 280},
        },
    }


# -----------------------------------------------------------------------------
# Provider selection (public facade)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class OpenAIProviderConfig:
    api_key: str
    base_url: str | None
    model: str
    max_output_tokens: int
    temperature: float


class OpenAIProvider(LLMProviderBase):
    """Public provider facade.

    Delegates to a model-specific implementation:
    - OpenAIGPT5Provider: GPT-5+ via Responses API
    - OpenAIGPTLegacyProvider: <= GPT-4 via Chat Completions
    """

    def __init__(
        self,
        api_key: str,
        base_url: str | None = None,
        model: str | None = None,
        max_completion_tokens: int = 4096,
        temperature: float = 0.3,
    ) -> None:
        cfg = OpenAIProviderConfig(
            api_key=api_key,
            base_url=base_url,
            model=model or DEFAULT_MODEL_BY_PROVIDER["openai"],
            # keep param name stable at the facade; impl maps to correct API param
            max_output_tokens=max(4096, int(max_completion_tokens)),
            temperature=float(temperature),
        )

        self._impl = openai_provider_class_for(cfg.model)(cfg)

    async def complete(self, prompt: str) -> CompletionResult:
        return await self._impl.complete(prompt)

    async def aclose(self) -> None:
        await self._impl.aclose()


def _is_gpt5_family(model: str) -> bool:
    m = (model or "").strip()
    return m.startswith(("gpt-5", "gpt-6", "openai/gpt-oss"))


def openai_provider_class_for(model: str) -> type[_OpenAIProviderBase]:
    return OpenAIGPT5Provider if _is_gpt5_family(model) else OpenAIGPTLegacyProvider


# -----------------------------------------------------------------------------
# Base implementation
# -----------------------------------------------------------------------------


class _OpenAIProviderBase(LLMProviderBase):
    def __init__(self, cfg: OpenAIProviderConfig) -> None:
        self.cfg = cfg
        self._client: Any | None = None

    def _get_client(self) -> Any:
        if self._client is None:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI(
                api_key=self.cfg.api_key,
                base_url=self.cfg.base_url,
                max_retries=0,
            )
        return self._client

    def _supports_temperature(self) -> bool:
        # GPT-5 ignores temperature; keep for legacy and future overrides.
        m = (self.cfg.model or "").strip()
        return not m.startswith(("gpt-5", "o1", "o3"))

    async def complete(self, prompt: str) -> CompletionResult:
        raise NotImplementedError

    async def _retry_transient_error(
        self,
        exc: Exception,
        *,
        retry_count: int,
        max_retries: int,
        api_name: str,
    ) -> bool:
        llm_exc = classify_provider_error(exc, provider="openai", model=self.cfg.model)
        if is_retryable(llm_exc) and retry_count < max_retries:
            delay = retry_delay(retry_count)
            if isinstance(llm_exc, LLMRateLimitError) and llm_exc.retry_after:
                delay = llm_exc.retry_after
            logger.warning(
                "%s transient error on attempt %d/%d; retrying in %.1fs: %s",
                api_name,
                retry_count + 1,
                max_retries + 1,
                delay,
                llm_exc,
            )
            await asyncio.sleep(delay)
            return True
        logger.error("%s error: %s", api_name, llm_exc)
        raise llm_exc from exc

    def _completion_result(
        self, text: str, usage: object | None = None
    ) -> CompletionResult:
        return CompletionResult(
            text=text,
            input_tokens=usage_token_count(usage, "input_tokens", "prompt_tokens"),
            output_tokens=usage_token_count(
                usage, "output_tokens", "completion_tokens"
            ),
            model=self.cfg.model,
        )

    async def aclose(self) -> None:
        if self._client:
            await self._client.close()

    # -----------------------------------------------------------------------------
    # GPT-5+ (Responses API)
    # -----------------------------------------------------------------------------


class OpenAIGPT5Provider(_OpenAIProviderBase):
    """GPT-5+ via Responses API."""

    async def complete(self, prompt: str) -> CompletionResult:
        client = self._get_client()

        # Retry once on truncation / invalid JSON.
        retry_count = 0
        max_retries = 1

        # Explanation payloads are large; start higher than 4096.
        is_schema_prompt = is_json_schema_prompt(prompt)
        max_tokens = max(
            self.cfg.max_output_tokens, 4096 if not is_schema_prompt else 2048
        )

        while retry_count <= max_retries:
            token_budget = max_tokens

            try:
                sys_msg = system_message(prompt)

                # Response formatting
                if is_schema_prompt:
                    text_format: dict[str, Any] = {
                        "format": {
                            "type": "json_schema",
                            "name": "categorization",
                            "strict": True,
                            "schema": categorization_json_schema(),
                        }
                    }
                else:
                    # For explanation, enforce valid JSON but not a strict schema.
                    text_format = {"format": {"type": "json_object"}}

                kwargs: dict[str, Any] = {
                    "model": self.cfg.model,
                    "instructions": sys_msg,
                    "input": prompt,
                    "text": text_format,
                    "reasoning": {"effort": "low"},
                    "max_output_tokens": token_budget,
                }

                if self._supports_temperature():
                    kwargs["temperature"] = self.cfg.temperature

                response = await client.responses.create(**kwargs)
                usage = getattr(response, "usage", None)

                # Best-effort extraction
                content = getattr(response, "output_text", "") or ""
                if not content.strip():
                    parts: list[str] = []
                    for item in getattr(response, "output", []) or []:
                        for c in getattr(item, "content", []) or []:
                            if getattr(c, "type", None) in ("output_text", "text"):
                                parts.append(getattr(c, "text", "") or "")
                    content = "".join(parts)

                incomplete_reason = getattr(
                    getattr(response, "incomplete_details", None), "reason", None
                )
                finish_reason = incomplete_reason or "completed"

                cleaned = validate_json_or_empty(content)

                logger.info(
                    "OpenAI completion (responses): model=%s, finish_reason=%s, content_length=%d, tokens=%d",
                    self.cfg.model,
                    finish_reason,
                    len(content.strip()),
                    token_budget,
                )

                if cleaned:
                    return self._completion_result(cleaned, usage)

                should_retry = finish_reason == "max_output_tokens"

                if retry_count < max_retries and should_retry:
                    retry_count += 1
                    max_tokens = min(8192, max_tokens * 2)
                    logger.warning(
                        "Invalid/empty JSON from responses API (reason=%s). Retrying with max_output_tokens=%d",
                        finish_reason,
                        max_tokens,
                    )
                    await asyncio.sleep(0.5)
                    continue

                # Final failure: return empty string (caller handles)
                logger.error(
                    "Invalid JSON returned from responses API (reason=%s, is_schema=%s, p_len=%d, budget=%d). Sample=%s",
                    finish_reason,
                    is_schema_prompt,
                    len(prompt),
                    token_budget,
                    (content.strip()[:200] + "...") if content else "<empty>",
                )
                return self._completion_result("", usage)

            except Exception as e:
                if await self._retry_transient_error(
                    e,
                    retry_count=retry_count,
                    max_retries=max_retries,
                    api_name="OpenAI Responses API",
                ):
                    retry_count += 1
                    continue

        return self._completion_result("")


# -----------------------------------------------------------------------------
# Legacy GPT (Chat Completions)
# -----------------------------------------------------------------------------


class OpenAIGPTLegacyProvider(_OpenAIProviderBase):
    """<= GPT-4 via Chat Completions."""

    async def complete(self, prompt: str) -> CompletionResult:
        client = self._get_client()

        retry_count = 0
        max_retries = 1
        max_tokens = max(self.cfg.max_output_tokens, 2048)

        while retry_count <= max_retries:
            try:
                sys_msg = system_message(prompt)

                kwargs: dict[str, Any] = {
                    "model": self.cfg.model,
                    "messages": [
                        {"role": "system", "content": sys_msg},
                        {"role": "user", "content": prompt},
                    ],
                    "response_format": {"type": "json_object"},
                    "max_completion_tokens": max_tokens,
                }

                if self._supports_temperature():
                    kwargs["temperature"] = self.cfg.temperature

                response = await client.chat.completions.create(**kwargs)
                usage = getattr(response, "usage", None)

                choice = response.choices[0]
                content = choice.message.content or ""
                finish_reason = getattr(choice, "finish_reason", "unknown")

                cleaned = validate_json_or_empty(content)

                logger.info(
                    "OpenAI completion (chat): model=%s, finish_reason=%s, content_length=%d, tokens=%d",
                    self.cfg.model,
                    finish_reason,
                    len(content.strip()),
                    max_tokens,
                )

                if cleaned:
                    return self._completion_result(cleaned, usage)

                should_retry = finish_reason in (
                    "length",
                    "max_tokens",
                    "max_output_tokens",
                )

                if retry_count < max_retries and should_retry:
                    retry_count += 1
                    max_tokens = min(8192, max_tokens * 2)
                    logger.warning(
                        "Invalid/empty JSON from chat completions (reason=%s). Retrying with max_completion_tokens=%d",
                        finish_reason,
                        max_tokens,
                    )
                    await asyncio.sleep(0.5)
                    continue

                is_schema_prompt = is_json_schema_prompt(prompt)
                logger.error(
                    "Invalid JSON returned from chat completions (reason=%s, is_schema=%s, p_len=%d, budget=%d). Sample=%s",
                    finish_reason,
                    is_schema_prompt,
                    len(prompt),
                    max_tokens,
                    (content.strip()[:200] + "...") if content else "<empty>",
                )
                return self._completion_result("", usage)

            except Exception as e:
                if await self._retry_transient_error(
                    e,
                    retry_count=retry_count,
                    max_retries=max_retries,
                    api_name="OpenAI Chat Completions",
                ):
                    retry_count += 1
                    continue

        return self._completion_result("")
