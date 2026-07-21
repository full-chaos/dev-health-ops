from __future__ import annotations

import json
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.workers import system_webhooks


def test_queued_webhook_rechecks_disabled_feature_and_dead_letters_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    monkeypatch.setenv("PAGERDUTY_API_TOKEN", "token")
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")
    monkeypatch.setattr(
        system_webhooks,
        "_canonical_incident_ingestion_allowed",
        MagicMock(return_value=False),
        raising=False,
    )
    dlq_writes: list[tuple[str, dict[str, str]]] = []
    deleted: list[tuple[str, str]] = []

    class Redis:
        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
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

        def xadd(self, stream: str, fields: dict[str, str], **_: object) -> str:
            dlq_writes.append((stream, fields))
            return "2-0"

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    redis = Redis()
    run_async = MagicMock(return_value=True)

    # When
    with (
        patch("dev_health_ops.api.ingest.streams.get_redis_client", return_value=redis),
        patch("dev_health_ops.storage.run_with_store", return_value=MagicMock()),
        patch("dev_health_ops.workers.system_webhooks.run_async", run_async),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        with pytest.raises(RuntimeError, match="persistence exhausted"):
            task.run(
                org_id="00000000-0000-0000-0000-000000003024",
                provider_instance_id="acme",
                stream_entry_id="1-0",
            )

    # Then
    run_async.assert_not_called()
    assert dlq_writes[0][0].endswith(":dlq")
    assert dlq_writes[0][1]["failure_type"] == (
        "CanonicalIncidentIngestionDisabledError"
    )
    assert deleted == [
        (
            "pagerduty-webhooks:00000000-0000-0000-0000-000000003024:acme",
            "1-0",
        )
    ]
