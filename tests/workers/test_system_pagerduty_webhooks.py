from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.workers import system_webhooks


class _RetrySentinel(Exception):
    pass


def _retry_sentinel(*, exc: BaseException, countdown: int) -> None:
    raise _RetrySentinel from exc


def test_pagerduty_worker_retries_when_configuration_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PAGERDUTY_API_TOKEN", raising=False)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)

    with patch.object(
        system_webhooks.process_pagerduty_webhook_event, "retry", _retry_sentinel
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(
                org_id="org-1",
                provider_instance_id="acme",
                stream_entry_id="1-0",
            )


def test_pagerduty_worker_deletes_stream_entry_only_after_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PAGERDUTY_API_TOKEN", "token")
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")

    class Redis:
        deleted: list[tuple[str, str]] = []

        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
            return [
                (
                    "1-0",
                    {
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

        def xdel(self, stream: str, entry_id: str) -> None:
            self.deleted.append((stream, entry_id))

    redis = Redis()
    with (
        patch("dev_health_ops.api.ingest.streams.get_redis_client", return_value=redis),
        patch("dev_health_ops.storage.run_with_store", return_value=MagicMock()),
        patch("dev_health_ops.workers.system_webhooks.run_async", return_value=True),
        patch(
            "dev_health_ops.workers.system_webhooks._canonical_incident_ingestion_allowed",
            return_value=True,
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        result = task.run(
            org_id="org-1",
            provider_instance_id="acme",
            stream_entry_id="1-0",
        )

    assert result["processed"] is True
    assert redis.deleted == [("pagerduty-webhooks:org-1:acme", "1-0")]


def test_pagerduty_worker_moves_exhausted_entry_to_dlq_before_deleting_source() -> None:
    writes: list[tuple[str, dict[str, str]]] = []
    deleted: list[tuple[str, str]] = []

    class Redis:
        def xadd(self, stream: str, fields: dict[str, str], **_: object) -> str:
            writes.append((stream, fields))
            return "2-0"

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    task = SimpleNamespace(
        max_retries=3,
        request=SimpleNamespace(id="task-1", retries=3),
    )

    with pytest.raises(RuntimeError, match="persistence exhausted"):
        system_webhooks._retry_or_dead_letter_pagerduty_webhook(
            task=task,
            redis_client=Redis(),
            stream_name="pagerduty-webhooks:org-1:acme",
            stream_entry_id="1-0",
            fields={"event_id": "event-1", "payload": "{}"},
            error=RuntimeError("ClickHouse unavailable"),
        )

    assert writes[0][0] == "pagerduty-webhooks:org-1:acme:dlq"
    assert writes[0][1]["event_id"] == "event-1"
    assert deleted == [("pagerduty-webhooks:org-1:acme", "1-0")]


def test_pagerduty_worker_retains_source_when_dlq_write_fails() -> None:
    deleted: list[tuple[str, str]] = []

    class Redis:
        def xadd(self, *_: object, **__: object) -> str:
            raise RuntimeError("Redis unavailable")

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    task = SimpleNamespace(
        max_retries=3,
        request=SimpleNamespace(id="task-1", retries=3),
    )

    with pytest.raises(RuntimeError, match="Redis unavailable"):
        system_webhooks._retry_or_dead_letter_pagerduty_webhook(
            task=task,
            redis_client=Redis(),
            stream_name="pagerduty-webhooks:org-1:acme",
            stream_entry_id="1-0",
            fields={"event_id": "event-1", "payload": "{}"},
            error=RuntimeError("ClickHouse unavailable"),
        )

    assert deleted == []


def test_pagerduty_worker_reroutes_unparseable_entry_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PAGERDUTY_API_TOKEN", "token")
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")

    class Redis:
        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
            return [
                (
                    "1-0",
                    {
                        "payload": "not-json",
                        "received_at": "2026-07-17T12:00:00+00:00",
                    },
                )
            ]

        def xadd(self, *_: object, **__: object) -> str:
            return "2-0"

        def xdel(self, *_: object, **__: object) -> None:
            return None

    with (
        patch(
            "dev_health_ops.api.ingest.streams.get_redis_client",
            return_value=Redis(),
        ),
        patch.object(
            system_webhooks.process_pagerduty_webhook_event, "retry", _retry_sentinel
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(
                org_id="org-1",
                provider_instance_id="acme",
                stream_entry_id="1-0",
            )
