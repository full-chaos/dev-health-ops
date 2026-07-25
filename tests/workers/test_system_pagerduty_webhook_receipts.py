from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from dev_health_ops.workers import system_webhooks


class _RetrySentinel(Exception):
    pass


def _retry_sentinel(*, exc: BaseException, countdown: int) -> None:
    raise _RetrySentinel from exc


def _run(coroutine: Any) -> Any:
    async def await_coroutine() -> Any:
        return await coroutine

    return anyio.run(await_coroutine)


def test_pagerduty_redelivery_after_ack_failure_does_not_persist_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")

    class Redis:
        def __init__(self) -> None:
            self.ack_attempts = 0
            self.receipts: dict[str, str] = {}

        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
            return [
                (
                    "1-0",
                    {
                        "payload": json.dumps(
                            {
                                "event": {
                                    "id": "event-1",
                                    "event_type": "incident.acknowledged",
                                    "occurred_at": "2026-07-17T12:00:00Z",
                                    "data": {
                                        "id": "incident-1",
                                        "title": "Payments unavailable",
                                        "status": "acknowledged",
                                        "created_at": "2026-07-17T12:00:00Z",
                                    },
                                }
                            }
                        ),
                        "received_at": "2026-07-17T12:00:00+00:00",
                    },
                )
            ]

        def get(self, key: str) -> str | None:
            return self.receipts.get(key)

        def set(self, key: str, value: str, **_: object) -> None:
            self.receipts[key] = value

        def xdel(self, _: str, __: str) -> None:
            self.ack_attempts += 1
            if self.ack_attempts == 1:
                raise RuntimeError("ack lost")

    redis = Redis()
    reconcile = AsyncMock(return_value=True)
    context = SimpleNamespace(org_id="org-1")
    with (
        patch("dev_health_ops.api.ingest.streams.get_redis_client", return_value=redis),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=context),
        ),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.reconcile_pagerduty_webhook_with_locked_graph",
            new=reconcile,
        ),
        patch("dev_health_ops.workers.system_webhooks.run_async", side_effect=_run),
        patch(
            "dev_health_ops.workers.system_webhooks._canonical_incident_ingestion_allowed",
            return_value=True,
        ),
        patch.object(
            system_webhooks.process_pagerduty_webhook_event, "retry", _retry_sentinel
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(binding_id="binding-1", stream_entry_id="1-0")
        result = task.run(binding_id="binding-1", stream_entry_id="1-0")

    assert reconcile.await_count == 1
    assert result["processed"] is False
    assert result["receipt"] == "completed"


def test_pagerduty_whitespace_event_ids_use_raw_body_hashes_for_distinct_receipts() -> (
    None
):
    # Given
    stream_name = "pagerduty-webhooks:binding-1"
    first_body = b'{"event":{"id":"","data":{"id":"incident-1"}}}'
    second_body = b'{"event":{"id":"","data":{"id":"incident-2"}}}'

    # When
    first_key = system_webhooks._pagerduty_webhook_receipt_key(
        stream_name, "   ", hashlib.sha256(first_body).hexdigest()
    )
    second_key = system_webhooks._pagerduty_webhook_receipt_key(
        stream_name, "   ", hashlib.sha256(second_body).hexdigest()
    )

    # Then
    assert first_key != second_key
    assert first_key.endswith(hashlib.sha256(first_body).hexdigest())
    assert second_key.endswith(hashlib.sha256(second_body).hexdigest())


def test_pagerduty_whitespace_event_ids_use_the_same_raw_body_receipt() -> None:
    # Given
    stream_name = "pagerduty-webhooks:binding-1"
    body_hash = hashlib.sha256(b'{"event":{"id":""}}').hexdigest()

    # When
    first_key = system_webhooks._pagerduty_webhook_receipt_key(
        stream_name, "   ", body_hash
    )
    second_key = system_webhooks._pagerduty_webhook_receipt_key(
        stream_name, "   ", body_hash
    )

    # Then
    assert first_key == second_key
