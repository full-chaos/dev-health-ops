from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.llm.providers.batch import BatchItemRequest, BatchJobStatus
from dev_health_ops.llm.providers.openai import OpenAIProvider


class _Files:
    def __init__(self) -> None:
        self.uploaded = b""

    async def create(self, *, file, purpose):
        self.uploaded = file.read()
        assert purpose == "batch"
        return SimpleNamespace(id="file-1")

    async def content(self, file_id):
        body = {
            "choices": [{"message": {"content": json.dumps({"ok": True})}}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 7},
        }
        if file_id == "output-1":
            line = json.dumps(
                {
                    "custom_id": "item-1",
                    "response": {"status_code": 200, "body": body},
                }
            )
            return SimpleNamespace(text=lambda: line)
        if file_id == "error-1":
            line = json.dumps(
                {
                    "custom_id": "item-2",
                    "error": {
                        "code": "rate_limit_exceeded",
                        "message": "provider rejected item",
                        "type": "rate_limit_error",
                    },
                }
            )
            return SimpleNamespace(text=lambda: line)
        raise AssertionError(f"unexpected file id {file_id}")


class _Batches:
    def __init__(self) -> None:
        self.created: dict[str, Any] | None = None

    async def create(self, **kwargs):
        self.created = kwargs
        return SimpleNamespace(id="batch-1")

    async def retrieve(self, provider_job_id):
        assert provider_job_id == "batch-1"
        return SimpleNamespace(
            status="completed",
            request_counts=SimpleNamespace(total=2, completed=1, failed=1),
            output_file_id="output-1",
            error_file_id="error-1",
        )

    async def cancel(self, provider_job_id):
        assert provider_job_id == "batch-1"


class _Client:
    def __init__(self) -> None:
        self.files = _Files()
        self.batches = _Batches()


@pytest.mark.asyncio
async def test_openai_batch_submit_poll_and_fetch():
    provider = OpenAIProvider(api_key="sk-test", model="gpt-4o-mini")
    fake_client = _Client()
    provider._impl._client = fake_client

    submission = await provider.submit_batch(
        [BatchItemRequest(custom_id="item-1", prompt="Return JSON")]
    )
    state = await provider.poll_batch(submission.provider_job_id)
    results = await provider.fetch_batch_results(submission.provider_job_id)

    assert submission.provider_job_id == "batch-1"
    assert fake_client.batches.created is not None
    assert fake_client.batches.created["input_file_id"] == "file-1"
    assert fake_client.batches.created["endpoint"] == "/v1/chat/completions"
    assert b'"custom_id":"item-1"' in fake_client.files.uploaded
    assert state.status == BatchJobStatus.SUCCEEDED
    assert state.total_count == 2
    assert state.completed_count == 1
    assert state.failed_count == 1
    assert results[0].custom_id == "item-1"
    assert results[0].raw_response == json.dumps({"ok": True})
    assert results[0].provider_metadata == {
        "custom_id": "item-1",
        "status_code": 200,
        "input_tokens": 11,
        "output_tokens": 7,
    }
    assert "body" not in json.dumps(results[0].provider_metadata)
    assert results[1].custom_id == "item-2"
    assert results[1].error_code == "rate_limit_exceeded"
    assert results[1].error_message == "provider rejected item"
    assert "body" not in json.dumps(results[1].provider_metadata)
