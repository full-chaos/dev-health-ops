from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.webhooks import pagerduty
from tests.api.webhooks.test_pagerduty import _payload, _signature

pytest_plugins = ("tests.api.webhooks.test_pagerduty",)


def test_feature_off_rejects_webhook_without_enqueue_or_dispatch(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    writes: list[tuple[str, dict[str, str]]] = []
    dispatches: list[dict[str, str]] = []

    class Redis:
        def xadd(self, stream: str, fields: dict[str, str]) -> str:
            writes.append((stream, fields))
            return "1-0"

    class Task:
        @staticmethod
        def delay(**kwargs: str) -> None:
            dispatches.append(kwargs)

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())
    monkeypatch.setattr(pagerduty, "process_pagerduty_webhook_event", Task())
    monkeypatch.setattr(
        pagerduty,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=False),
        raising=False,
    )
    body = _payload()

    # When
    response = client.post(
        "/api/v1/webhooks/pagerduty",
        content=body,
        headers={"x-pagerduty-signature": _signature(body)},
    )

    # Then
    assert response.status_code == 403
    assert writes == []
    assert dispatches == []


def test_feature_off_keeps_webhook_test_event_validation_available(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    monkeypatch.setattr(
        pagerduty,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=False),
        raising=False,
    )
    body = _payload(event_id="inspect-event")

    # When
    response = client.post(
        "/api/v1/webhooks/pagerduty/test-event",
        content=body,
        headers={"x-pagerduty-signature": _signature(body)},
    )

    # Then
    assert response.status_code == 200
    assert response.json()["event_id"] == "inspect-event"
