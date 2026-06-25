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
import io
import json
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

from ._http import make_hardened_async_httpx_client, make_hardened_httpx_client
from .base import (
    DEFAULT_MODEL_BY_PROVIDER,
    CompletionResult,
    LLMProviderBase,
    usage_token_count,
)
from .batch import (
    BatchCapability,
    BatchItemRequest,
    BatchItemResult,
    BatchJobState,
    BatchJobStatus,
    BatchJobSubmission,
    BatchProviderFeature,
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


def _map_openai_batch_status(status: str) -> BatchJobStatus:
    normalized = (status or "").strip().lower()
    if normalized in {"validating", "in_progress", "finalizing"}:
        return BatchJobStatus.RUNNING
    if normalized == "completed":
        return BatchJobStatus.SUCCEEDED
    if normalized in {"failed", "expired"}:
        return (
            BatchJobStatus.EXPIRED if normalized == "expired" else BatchJobStatus.FAILED
        )
    if normalized in {"cancelling", "cancelled"}:
        return BatchJobStatus.CANCELLED
    return BatchJobStatus.SUBMITTED


async def _openai_file_text(client: Any, file_id: str) -> str:
    content = await client.files.content(file_id)
    text_attr = getattr(content, "text", None)
    if callable(text_attr):
        maybe_text = text_attr()
        if isinstance(maybe_text, str):
            return maybe_text
    read_attr = getattr(content, "read", None)
    if callable(read_attr):
        raw = read_attr()
        if isinstance(raw, bytes):
            return raw.decode("utf-8")
        if isinstance(raw, str):
            return raw
    if isinstance(content, bytes):
        return content.decode("utf-8")
    return str(content)


def _extract_batch_response_text(body: object) -> str | None:
    if not isinstance(body, dict):
        return None
    output_text = body.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return validate_json_or_empty(output_text) or output_text
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return validate_json_or_empty(content) or content
    output = body.get("output")
    if isinstance(output, list):
        parts: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for chunk in content:
                if isinstance(chunk, dict):
                    text = chunk.get("text")
                    if isinstance(text, str):
                        parts.append(text)
        joined = "".join(parts)
        if joined:
            return validate_json_or_empty(joined) or joined
    return None


def _parse_openai_batch_lines(text: str) -> list[BatchItemResult]:
    results: list[BatchItemResult] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        custom_id = str(payload.get("custom_id") or "")
        error = payload.get("error")
        if isinstance(error, dict):
            results.append(
                BatchItemResult(
                    custom_id=custom_id,
                    error_code=str(error.get("code") or error.get("type") or "error"),
                    error_message=str(error.get("message") or error),
                    provider_metadata=_batch_payload_metadata(payload),
                )
            )
            continue
        response = payload.get("response")
        if isinstance(response, dict):
            body = response.get("body")
            raw_response = _extract_batch_response_text(body)
            status_code = response.get("status_code")
            if raw_response is not None:
                results.append(
                    BatchItemResult(
                        custom_id=custom_id,
                        raw_response=raw_response,
                        provider_metadata=_batch_payload_metadata(payload),
                    )
                )
            else:
                results.append(
                    BatchItemResult(
                        custom_id=custom_id,
                        error_code=f"http_{status_code or 'unknown'}",
                        error_message="Batch response did not contain completion text",
                        provider_metadata=_batch_payload_metadata(payload),
                    )
                )
    return results


def _batch_payload_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    if payload.get("id") is not None:
        metadata["id"] = str(payload["id"])
    if payload.get("custom_id") is not None:
        metadata["custom_id"] = str(payload["custom_id"])
    response = payload.get("response")
    if isinstance(response, dict):
        if response.get("request_id") is not None:
            metadata["request_id"] = str(response["request_id"])
        if response.get("status_code") is not None:
            metadata["status_code"] = response["status_code"]
        body = response.get("body")
        if isinstance(body, dict):
            usage = body.get("usage")
            if isinstance(usage, dict):
                input_tokens = usage.get("input_tokens", usage.get("prompt_tokens"))
                output_tokens = usage.get(
                    "output_tokens", usage.get("completion_tokens")
                )
                if input_tokens is not None:
                    metadata["input_tokens"] = int(input_tokens or 0)
                if output_tokens is not None:
                    metadata["output_tokens"] = int(output_tokens or 0)
    error = payload.get("error")
    if isinstance(error, dict):
        if error.get("code") is not None:
            metadata["error_code"] = str(error["code"])
        if error.get("type") is not None:
            metadata["error_type"] = str(error["type"])
    return metadata


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
    validate_model_on_startup: bool = False
    validation_provider_name: str = "openai"


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

    def batch_capability(self, model: str | None = None) -> BatchCapability:
        return self._impl.batch_capability(model)

    async def submit_batch(self, items: list[BatchItemRequest]) -> BatchJobSubmission:
        return await self._impl.submit_batch(items)

    async def poll_batch(self, provider_job_id: str) -> BatchJobState:
        return await self._impl.poll_batch(provider_job_id)

    async def fetch_batch_results(self, provider_job_id: str) -> list[BatchItemResult]:
        return await self._impl.fetch_batch_results(provider_job_id)

    async def cancel_batch(self, provider_job_id: str) -> None:
        await self._impl.cancel_batch(provider_job_id)

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
        if cfg.validate_model_on_startup:
            self._validate_model_on_startup()

    def _get_client(self) -> Any:
        if self._client is None:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI(
                api_key=self.cfg.api_key,
                base_url=self.cfg.base_url,
                http_client=make_hardened_async_httpx_client(),
                max_retries=0,
            )
        return self._client

    def _validate_model_on_startup(self) -> None:
        from openai import OpenAI

        client = OpenAI(
            api_key=self.cfg.api_key,
            base_url=self.cfg.base_url,
            http_client=make_hardened_httpx_client(),
            max_retries=0,
        )
        try:
            client.models.list()
        except Exception as exc:
            llm_exc = classify_provider_error(
                exc,
                provider=self.cfg.validation_provider_name,
                model=self.cfg.model,
            )
            logger.error(
                "LLM startup model validation failed for provider '%s' model '%s'. "
                "Check provider reachability, base URL, credentials, and loaded model. "
                "Disable with LLM_VALIDATE_MODEL_ON_STARTUP=false or "
                "LMSTUDIO_VALIDATE_MODEL_ON_STARTUP=false: %s",
                self.cfg.validation_provider_name,
                self.cfg.model,
                llm_exc,
            )
            raise llm_exc from exc
        finally:
            client.close()

    def _supports_temperature(self) -> bool:
        # GPT-5 ignores temperature; keep for legacy and future overrides.
        m = (self.cfg.model or "").strip()
        return not m.startswith(("gpt-5", "o1", "o3"))

    async def complete(self, prompt: str) -> CompletionResult:
        raise NotImplementedError

    def batch_capability(self, model: str | None = None) -> BatchCapability:
        resolved_model = model or self.cfg.model
        return BatchCapability(
            provider=self.cfg.validation_provider_name,
            model=resolved_model,
            supported=True,
            features=frozenset(
                {
                    BatchProviderFeature.SUBMIT,
                    BatchProviderFeature.POLL,
                    BatchProviderFeature.FETCH_RESULTS,
                    BatchProviderFeature.CANCEL,
                }
            ),
        )

    def _batch_endpoint(self) -> str:
        return (
            "/v1/responses"
            if _is_gpt5_family(self.cfg.model)
            else "/v1/chat/completions"
        )

    def _batch_body(self, prompt: str) -> dict[str, Any]:
        sys_msg = system_message(prompt)
        if _is_gpt5_family(self.cfg.model):
            text_format: dict[str, Any]
            if is_json_schema_prompt(prompt):
                text_format = {
                    "format": {
                        "type": "json_schema",
                        "name": "categorization",
                        "strict": True,
                        "schema": categorization_json_schema(),
                    }
                }
            else:
                text_format = {"format": {"type": "json_object"}}
            body: dict[str, Any] = {
                "model": self.cfg.model,
                "instructions": sys_msg,
                "input": prompt,
                "text": text_format,
                "reasoning": {"effort": "low"},
                "max_output_tokens": max(self.cfg.max_output_tokens, 2048),
            }
            if self._supports_temperature():
                body["temperature"] = self.cfg.temperature
            return body
        body = {
            "model": self.cfg.model,
            "messages": [
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
            "max_completion_tokens": max(self.cfg.max_output_tokens, 2048),
        }
        if self._supports_temperature():
            body["temperature"] = self.cfg.temperature
        return body

    def _batch_line(self, item: BatchItemRequest) -> str:
        return json.dumps(
            {
                "custom_id": item.custom_id,
                "method": "POST",
                "url": self._batch_endpoint(),
                "body": self._batch_body(item.prompt),
            },
            separators=(",", ":"),
        )

    async def submit_batch(self, items: list[BatchItemRequest]) -> BatchJobSubmission:
        if not items:
            raise ValueError("Cannot submit an empty provider batch")
        client = self._get_client()
        payload = "\n".join(self._batch_line(item) for item in items).encode("utf-8")
        file_obj = io.BytesIO(payload)
        file_obj.name = "investment-categorization-batch.jsonl"
        uploaded = await client.files.create(file=file_obj, purpose="batch")
        batch = await client.batches.create(
            input_file_id=uploaded.id,
            endpoint=self._batch_endpoint(),
            completion_window="24h",
            metadata={"source": "investment_materialize"},
        )
        return BatchJobSubmission(
            provider_job_id=batch.id,
            provider=self.cfg.validation_provider_name,
            model=self.cfg.model,
            item_count=len(items),
            metadata={"input_file_id": uploaded.id},
        )

    async def poll_batch(self, provider_job_id: str) -> BatchJobState:
        client = self._get_client()
        batch = await client.batches.retrieve(provider_job_id)
        status = _map_openai_batch_status(str(getattr(batch, "status", "")))
        counts = getattr(batch, "request_counts", None)
        total = int(getattr(counts, "total", 0) or 0)
        completed = int(getattr(counts, "completed", 0) or 0)
        failed = int(getattr(counts, "failed", 0) or 0)
        return BatchJobState(
            provider_job_id=provider_job_id,
            status=status,
            total_count=total,
            completed_count=completed,
            failed_count=failed,
            metadata={
                "output_file_id": getattr(batch, "output_file_id", None),
                "error_file_id": getattr(batch, "error_file_id", None),
            },
        )

    async def fetch_batch_results(self, provider_job_id: str) -> list[BatchItemResult]:
        client = self._get_client()
        batch = await client.batches.retrieve(provider_job_id)
        results: list[BatchItemResult] = []
        output_file_id = getattr(batch, "output_file_id", None)
        error_file_id = getattr(batch, "error_file_id", None)
        if output_file_id:
            output_text = await _openai_file_text(client, output_file_id)
            results.extend(_parse_openai_batch_lines(output_text))
        if error_file_id:
            error_text = await _openai_file_text(client, error_file_id)
            results.extend(_parse_openai_batch_lines(error_text))
        return results

    async def cancel_batch(self, provider_job_id: str) -> None:
        client = self._get_client()
        await client.batches.cancel(provider_job_id)

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
                    "Invalid JSON returned from responses API (reason=%s, is_schema=%s, p_len=%d, budget=%d, content_length=%d).",
                    finish_reason,
                    is_schema_prompt,
                    len(prompt),
                    token_budget,
                    len(content.strip()),
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
                    "Invalid JSON returned from chat completions (reason=%s, is_schema=%s, p_len=%d, budget=%d, content_length=%d).",
                    finish_reason,
                    is_schema_prompt,
                    len(prompt),
                    max_tokens,
                    len(content.strip()),
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
