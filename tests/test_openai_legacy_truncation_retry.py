from __future__ import annotations

import pytest
import json
from unittest.mock import AsyncMock, MagicMock
from dev_health_ops.llm.providers.openai import OpenAIProvider


@pytest.mark.asyncio
async def test_openai_legacy_truncation_retry_success():
    # Setup facade with legacy model
    provider = OpenAIProvider(api_key="test", model="gpt-4o", max_completion_tokens=512)

    mock_client = AsyncMock()

    # First response: truncated
    mock_choice1 = MagicMock()
    mock_choice1.message.content = '{"summary": "This is trunca'
    mock_choice1.finish_reason = "length"

    resp1 = MagicMock()
    resp1.choices = [mock_choice1]

    # Second response: valid and complete
    mock_choice2 = MagicMock()
    mock_choice2.message.content = '{"summary": "This is complete now"}'
    mock_choice2.finish_reason = "stop"

    resp2 = MagicMock()
    resp2.choices = [mock_choice2]

    mock_client.chat.completions.create.side_effect = [resp1, resp2]

    # Inject mock client
    provider._impl._client = mock_client

    result_json = await provider.complete("explain legacy")

    # Verify parsing
    result = json.loads(result_json)
    assert result["summary"] == "This is complete now"

    # Verify two calls
    assert mock_client.chat.completions.create.call_count == 2

    # Verify token budget increase
    # Baseline for gpt-4o (non-gpt5) is max(cfg, 1024) = 1024 (since cfg 512)
    first_call_kwargs = mock_client.chat.completions.create.call_args_list[0].kwargs
    second_call_kwargs = mock_client.chat.completions.create.call_args_list[1].kwargs

    assert first_call_kwargs["max_completion_tokens"] == 4096
    assert second_call_kwargs["max_completion_tokens"] == 8192
