from __future__ import annotations

import pytest
import json
from unittest.mock import AsyncMock, MagicMock
from dev_health_ops.llm.providers.openai import OpenAIProvider


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

    result_json = await provider.complete("explain something")

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
