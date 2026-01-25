import pytest
from unittest.mock import AsyncMock
from dev_health_ops.llm.providers.openai import OpenAIProvider


@pytest.mark.asyncio
async def test_openai_retry_on_empty_content():
    # Legacy models use chat completions
    provider = OpenAIProvider(api_key="test", model="gpt-4o")

    # Mock client and response
    mock_client = AsyncMock()

    # First response is empty, second is valid JSON
    mock_response_empty = AsyncMock()
    mock_response_empty.choices = [
        AsyncMock(message=AsyncMock(content=""), finish_reason="stop")
    ]

    mock_response_valid = AsyncMock()
    mock_response_valid.choices = [
        AsyncMock(
            message=AsyncMock(content='{"summary": "test"}'), finish_reason="stop"
        )
    ]

    mock_client.chat.completions.create.side_effect = [
        mock_response_empty,
        mock_response_valid,
    ]

    # Patch the internal implementation's client
    provider._impl._client = mock_client

    result = await provider.complete("test prompt")

    assert result == '{"summary": "test"}'
    assert mock_client.chat.completions.create.call_count == 2

    # Check that tokens were doubled on retry
    second_call_kwargs = mock_client.chat.completions.create.call_args_list[1].kwargs
    assert second_call_kwargs["max_completion_tokens"] == 8192


@pytest.mark.asyncio
async def test_gpt5_retry_on_finish_reason_truncation():
    # GPT-5 uses responses API and 'max_output_tokens'
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=512
    )

    mock_client = AsyncMock()

    # First response truncated
    mock_response_trunc = AsyncMock()
    mock_response_trunc.output_text = '{"summary": "truncated'
    mock_response_trunc.incomplete_details = AsyncMock(reason="max_output_tokens")

    # Second response valid
    mock_response_valid = AsyncMock()
    mock_response_valid.output_text = '{"summary": "complete"}'
    mock_response_valid.incomplete_details = None

    mock_client.responses.create.side_effect = [
        mock_response_trunc,
        mock_response_valid,
    ]

    provider._impl._client = mock_client

    result = await provider.complete("test prompt")

    assert result == '{"summary": "complete"}'
    assert mock_client.responses.create.call_count == 2

    # Verify token doubling in second call
    second_call_kwargs = mock_client.responses.create.call_args_list[1].kwargs
    assert second_call_kwargs["max_output_tokens"] == 8192


@pytest.mark.asyncio
async def test_openai_token_clamping():
    # Facade clamps tokens to min 1024
    provider = OpenAIProvider(api_key="test", model="gpt-4o", max_completion_tokens=128)

    mock_client = AsyncMock()
    mock_response = AsyncMock()
    mock_response.choices = [
        AsyncMock(message=AsyncMock(content="{}"), finish_reason="stop")
    ]
    mock_client.chat.completions.create.return_value = mock_response

    provider._impl._client = mock_client
    await provider.complete("test")

    # Check that tokens were clamped to min 1024
    call_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert call_kwargs["max_completion_tokens"] == 4096
