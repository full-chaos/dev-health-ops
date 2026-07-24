"""The Celery task must stand down when Go owns the PagerDuty stream entry.

The producer gate alone is not enough: tasks queued before the cutover, and
tasks dispatched by an API pod that had not yet picked up the new env, still
reach the worker after the flip. Those two windows are what these tests pin.
"""

from __future__ import annotations

import json
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest

from dev_health_ops.providers.pagerduty.webhook_transport import (
    WEBHOOK_TRANSPORT_ENV,
)
from dev_health_ops.workers import system_webhooks

_STREAM = "pagerduty-webhooks:binding-1"


def _run(coroutine: Any) -> Any:
    async def await_coroutine() -> Any:
        return await coroutine

    return anyio.run(await_coroutine)


def _worker_context() -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(
        org_id="org-1",
        binding_id="binding-1",
        provider_instance_id="source-1",
        credential_id="credential-1",
    )


class _Redis:
    def __init__(self) -> None:
        self.deleted: list[tuple[str, str]] = []
        self.receipts: dict[str, str] = {}

    def xrange(
        self, stream: str, *_: object, **__: object
    ) -> list[tuple[str, dict[str, str]]]:
        return [
            (
                "1-0",
                {
                    "event_id": "event-1",
                    "payload": json.dumps(
                        {
                            "event": {
                                "id": "event-1",
                                "event_type": "incident.triggered",
                                "occurred_at": "2026-07-17T12:00:00Z",
                                "data": {
                                    "id": "incident-1",
                                    "title": "Payments unavailable",
                                    "status": "triggered",
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

    def set(self, key: str, value: str, **_: object) -> bool:
        self.receipts[key] = value
        return True

    def xdel(self, stream: str, entry_id: str) -> None:
        self.deleted.append((stream, entry_id))


@pytest.mark.parametrize(
    "transport", [None, "celery", "CELERY", " celery ", "", "quantum"]
)
def test_pagerduty_task_reconciles_unless_the_transport_names_river(
    transport: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: anything unset, empty, or unrecognised must fail safe to Celery.
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")
    if transport is None:
        monkeypatch.delenv(WEBHOOK_TRANSPORT_ENV, raising=False)
    else:
        monkeypatch.setenv(WEBHOOK_TRANSPORT_ENV, transport)
    redis = _Redis()

    with (
        patch("dev_health_ops.api.ingest.streams.get_redis_client", return_value=redis),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.reconcile_pagerduty_webhook_with_locked_graph",
            new=AsyncMock(return_value=True),
        ) as reconcile,
        patch("dev_health_ops.workers.system_webhooks.run_async", side_effect=_run),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=_worker_context()),
        ),
        patch(
            "dev_health_ops.workers.system_webhooks._canonical_incident_ingestion_allowed",
            return_value=True,
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        result = task.run(binding_id="binding-1", stream_entry_id="1-0")

    # Then: today's behaviour is untouched.
    assert result["processed"] is True
    reconcile.assert_awaited_once()
    assert redis.deleted == [(_STREAM, "1-0")]
    assert list(redis.receipts) == [f"{_STREAM}:receipts:event-1"]


@pytest.mark.parametrize("transport", ["river", "RIVER", " river "])
def test_pagerduty_task_stands_down_when_river_owns_the_entry(
    transport: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: a task queued before the flip lands on a worker that reads river.
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")
    monkeypatch.setenv(WEBHOOK_TRANSPORT_ENV, transport)
    redis_client = MagicMock()

    with (
        patch(
            "dev_health_ops.api.ingest.streams.get_redis_client", redis_client
        ) as get_redis_client,
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.reconcile_pagerduty_webhook_with_locked_graph",
            new=AsyncMock(return_value=True),
        ) as reconcile,
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=_worker_context()),
        ) as resolve,
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        result = task.run(binding_id="binding-1", stream_entry_id="1-0")

    # Then: the entry, its receipt, and its ACK are left wholly to Go.
    assert result == {
        "processed": False,
        "reason": "transport_delegated",
        "transport": "river",
        "binding_id": "binding-1",
        "stream_entry_id": "1-0",
    }
    reconcile.assert_not_awaited()
    resolve.assert_not_awaited()
    get_redis_client.assert_not_called()


def test_pagerduty_task_reads_the_transport_at_execution_not_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: the same worker process runs one task either side of the cutover.
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")
    monkeypatch.delenv(WEBHOOK_TRANSPORT_ENV, raising=False)
    redis = _Redis()

    with (
        patch("dev_health_ops.api.ingest.streams.get_redis_client", return_value=redis),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.reconcile_pagerduty_webhook_with_locked_graph",
            new=AsyncMock(return_value=True),
        ),
        patch("dev_health_ops.workers.system_webhooks.run_async", side_effect=_run),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=_worker_context()),
        ),
        patch(
            "dev_health_ops.workers.system_webhooks._canonical_incident_ingestion_allowed",
            return_value=True,
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)

        # When: the flip happens between the two executions.
        before = task.run(binding_id="binding-1", stream_entry_id="1-0")
        monkeypatch.setenv(WEBHOOK_TRANSPORT_ENV, "river")
        after = task.run(binding_id="binding-1", stream_entry_id="2-0")

    # Then: the second execution defers without a fresh import or restart.
    assert before["processed"] is True
    assert after["reason"] == "transport_delegated"
    assert redis.deleted == [(_STREAM, "1-0")]
