from __future__ import annotations

import pytest
import logging
from unittest.mock import AsyncMock, MagicMock
from dev_health_ops.llm.providers.openai import OpenAIProvider


@pytest.mark.asyncio
async def test_gpt5_reasoning_only_empty_result(caplog):
    # Setup facade with GPT-5 model
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini")

    mock_client = AsyncMock()

    # Response with reasoning but NO output_text and NO text parts in output
    resp = MagicMock()
    resp.output_text = ""
    # Simulate a reasoning item that we don't extract as text
    reasoning_item = MagicMock()
    reasoning_item.content = [MagicMock(type="reasoning", text="I am thinking...")]
    resp.output = [reasoning_item]
    resp.incomplete_details = None

    mock_client.responses.create.return_value = resp

    # Inject mock client
    provider._impl._client = mock_client

    with caplog.at_level(logging.ERROR):
        result = await provider.complete("empty please")

        # Should return empty string after retries (if it retries on empty)
        # Actually it retries once on empty content.
        assert result == ""

        # Should have log entry for invalid JSON/empty
        assert "Invalid JSON returned from responses API" in caplog.text
        assert "reason=completed" in caplog.text
        assert "is_schema=False" in caplog.text
