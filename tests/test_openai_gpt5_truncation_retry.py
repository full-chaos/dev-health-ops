from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.llm.errors import LLMAuthError
from dev_health_ops.llm.providers.openai import OpenAIProvider


class FakeRateLimitError(Exception):
    status_code = 429
    headers = {"Retry-After": "2"}
    code = "rate_limit_exceeded"


@pytest.mark.asyncio
async def test_gpt5_truncation_retry_logic():
    # Setup facade with GPT-5 model
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=1024
    )

    mock_client = AsyncMock()

    # First response: truncated
    resp1 = MagicMock()
    resp1.output_text = '{"summary": "This is truncated'
    resp1.incomplete_details = MagicMock(reason="max_output_tokens")

    # Second response: valid and complete
    resp2 = MagicMock()
    resp2.output_text = '{"summary": "This is complete now"}'
    resp2.incomplete_details = None

    mock_client.responses.create.side_effect = [resp1, resp2]

    # Inject mock client into implementation
    provider._impl._client = mock_client

    result = await provider.complete("explain something")
    result_json = result.text

    # Verify parsing
    result = json.loads(result_json)
    assert result["summary"] == "This is complete now"

    # Verify two calls
    assert mock_client.responses.create.call_count == 2

    # Verify token budget increase (First call default for explanation is 2048)
    first_call_kwargs = mock_client.responses.create.call_args_list[0].kwargs
    second_call_kwargs = mock_client.responses.create.call_args_list[1].kwargs

    assert first_call_kwargs["max_output_tokens"] == 4096
    assert second_call_kwargs["max_output_tokens"] == 8192

    # Verify validate_json_or_empty output is compact (json.dumps)
    assert result_json == json.dumps(result)


@pytest.mark.asyncio
async def test_gpt5_transient_error_retries_without_doubling_tokens(monkeypatch):
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=1024
    )
    mock_client = AsyncMock()
    resp = MagicMock()
    resp.output_text = '{"summary": "ok"}'
    resp.incomplete_details = None
    mock_client.responses.create.side_effect = [
        FakeRateLimitError("rate_limit_exceeded"),
        resp,
    ]
    provider._impl._client = mock_client
    sleeps: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(
        "dev_health_ops.llm.providers.openai.asyncio.sleep", _fake_sleep
    )

    result = await provider.complete("explain something")

    assert json.loads(result.text)["summary"] == "ok"
    assert sleeps == [2.0]
    first_call_kwargs = mock_client.responses.create.call_args_list[0].kwargs
    second_call_kwargs = mock_client.responses.create.call_args_list[1].kwargs
    assert first_call_kwargs["max_output_tokens"] == 4096
    assert second_call_kwargs["max_output_tokens"] == 4096


@pytest.mark.asyncio
async def test_gpt5_insufficient_quota_fails_without_retry_or_token_doubling():
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=1024
    )
    mock_client = AsyncMock()
    mock_client.responses.create.side_effect = RuntimeError(
        "Error code: insufficient_quota; api_key=sk-secret-value"
    )
    provider._impl._client = mock_client

    with pytest.raises(LLMAuthError) as exc_info:
        await provider.complete("explain something")

    assert "sk-secret-value" not in str(exc_info.value)
    assert mock_client.responses.create.call_count == 1
    first_call_kwargs = mock_client.responses.create.call_args_list[0].kwargs
    assert first_call_kwargs["max_output_tokens"] == 4096
