import pytest
from unittest.mock import AsyncMock, patch
from dev_health_ops.api.services.llm_providers.openai import OpenAIProvider


@pytest.mark.asyncio
async def test_openai_retry_on_empty_content():
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

    with patch.object(provider, "_get_client", return_value=mock_client):
        result = await provider.complete("test prompt")

        assert result == '{"summary": "test"}'
        assert mock_client.chat.completions.create.call_count == 2

        # Check that max_completion_tokens was doubled on retry
        second_call_kwargs = mock_client.chat.completions.create.call_args_list[
            1
        ].kwargs
        assert second_call_kwargs["max_completion_tokens"] == 2048


@pytest.mark.asyncio
async def test_openai_retry_on_finish_reason_length():
    provider = OpenAIProvider(api_key="test", model="gpt-4o", max_completion_tokens=512)

    mock_client = AsyncMock()

    # First response truncated, second is valid JSON
    mock_response_trunc = AsyncMock()
    mock_response_trunc.choices = [
        AsyncMock(
            message=AsyncMock(content='{"summary": "truncated'), finish_reason="length"
        )
    ]

    mock_response_valid = AsyncMock()
    mock_response_valid.choices = [
        AsyncMock(
            message=AsyncMock(content='{"summary": "complete"}'), finish_reason="stop"
        )
    ]

    mock_client.chat.completions.create.side_effect = [
        mock_response_trunc,
        mock_response_valid,
    ]

    with patch.object(provider, "_get_client", return_value=mock_client):
        result = await provider.complete("test prompt")

        assert result == '{"summary": "complete"}'
        assert mock_client.chat.completions.create.call_count == 2

        # Check that prompt was updated with explicit JSON instruction
        second_call_messages = mock_client.chat.completions.create.call_args_list[
            1
        ].kwargs["messages"]
        assert "Output ONLY valid JSON" in second_call_messages[1]["content"]


@pytest.mark.asyncio
async def test_openai_token_param_selection():
    # gpt-4o should use max_completion_tokens
    provider_new = OpenAIProvider(api_key="test", model="gpt-4o")
    assert provider_new._token_param_name() == "max_completion_tokens"

    # older models should use max_tokens
    provider_old = OpenAIProvider(api_key="test", model="gpt-3.5-turbo")
    assert provider_old._token_param_name() == "max_tokens"


@pytest.mark.asyncio
async def test_openai_token_clamping():
    provider = OpenAIProvider(api_key="test", model="gpt-4o", max_completion_tokens=128)

    mock_client = AsyncMock()
    mock_response = AsyncMock()
    mock_response.choices = [
        AsyncMock(message=AsyncMock(content="{}"), finish_reason="stop")
    ]
    mock_client.chat.completions.create.return_value = mock_response

    with patch.object(provider, "_get_client", return_value=mock_client):
        await provider.complete("test")

        # Check that tokens were clamped to min 512
        call_kwargs = mock_client.chat.completions.create.call_args.kwargs
        assert call_kwargs["max_completion_tokens"] == 512
