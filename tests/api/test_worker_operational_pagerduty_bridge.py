from __future__ import annotations

from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from dev_health_ops.api.main import app
from dev_health_ops.providers.pagerduty import webhook_worker
from dev_health_ops.providers.pagerduty.webhook_worker_shared import (
    PagerDutyWebhookWorkerContext,
)
from dev_health_ops.workers import system_webhooks

_BRIDGE_PATH = "/api/internal/worker-operational/pagerduty"
_TOKEN = "test-token"
_BINDING_ID = "625d6f61-9507-4690-8e90-3e6c2fa73b6c"
_ORG_ID = "5a23f94c-dac3-4542-9b95-149b2e5192e0"
_CONTEXT = PagerDutyWebhookWorkerContext(
    org_id=_ORG_ID,
    binding_id=_BINDING_ID,
    provider_instance_id="account-1",
    credential_id="ea999a45-91b1-4e6c-ae2b-5a6cf8aec2fb",
)


def _payload(
    *, event_type: str = "incident.triggered", event_id: str = "pagey-event-1"
) -> dict[str, Any]:
    return {
        "event": {
            "id": event_id,
            "event_type": event_type,
            "occurred_at": "2026-07-21T12:00:00Z",
            "data": {"id": "incident-1"},
        }
    }


def _delivery(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "binding_id": _BINDING_ID,
        "event_id": "pagey-event-1",
        "receipt_id": "pagerduty-webhooks:receipt-1",
        "received_at": "2026-07-21T12:00:05Z",
        "payload": _payload(),
    }
    body.update(overrides)
    return body


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_TOKEN}"}


@pytest.fixture
def reconcile(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    # The bridge resolves these symbols lazily inside the handler, so the
    # defining module is the only seam a patch can take effect through.
    reconciler = AsyncMock(return_value=True)
    monkeypatch.setattr(
        webhook_worker,
        "reconcile_pagerduty_webhook_with_locked_graph",
        reconciler,
    )
    return reconciler


@pytest.fixture
def client(
    monkeypatch: pytest.MonkeyPatch, reconcile: AsyncMock
) -> Generator[TestClient]:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", _TOKEN)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost:9000/dev_health")
    monkeypatch.setattr(
        webhook_worker,
        "resolve_pagerduty_webhook_binding",
        AsyncMock(return_value=_CONTEXT),
    )
    monkeypatch.setattr(
        system_webhooks,
        "_canonical_incident_ingestion_allowed",
        MagicMock(return_value=True),
    )
    yield TestClient(app)


def test_pagerduty_bridge_requires_the_operational_token(
    client: TestClient, reconcile: AsyncMock
) -> None:
    # When
    response = client.post(_BRIDGE_PATH, json=_delivery())

    # Then
    assert response.status_code == 401
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_rejects_a_non_utc_receipt_before_reconciling(
    client: TestClient, reconcile: AsyncMock
) -> None:
    # When
    response = client.post(
        _BRIDGE_PATH,
        headers=_headers(),
        json=_delivery(received_at="2026-07-21T12:00:05+05:00"),
    )

    # Then
    assert response.status_code == 422
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_rejects_a_timezone_free_receipt(
    client: TestClient, reconcile: AsyncMock
) -> None:
    # When
    response = client.post(
        _BRIDGE_PATH,
        headers=_headers(),
        json=_delivery(received_at="2026-07-21T12:00:05"),
    )

    # Then
    assert response.status_code == 422
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_reports_processed_without_touching_the_stream(
    client: TestClient, reconcile: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: Go owns the stream entry, the receipt, and the ACK.
    from dev_health_ops.api.ingest import streams

    redis_client = MagicMock()
    monkeypatch.setattr(streams, "get_redis_client", redis_client)

    # When
    response = client.post(_BRIDGE_PATH, headers=_headers(), json=_delivery())

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "processed"}
    redis_client.assert_not_called()
    awaited = reconcile.await_args
    assert awaited is not None, "the bridge must have reconciled the delivery"
    assert awaited.kwargs["binding_id"] == _BINDING_ID
    assert awaited.kwargs["expected_context"] == _CONTEXT
    assert awaited.kwargs["webhook"].event.id == "pagey-event-1"


def test_pagerduty_bridge_reports_skipped_for_a_reconciled_replay(
    client: TestClient, reconcile: AsyncMock
) -> None:
    # Given
    reconcile.return_value = False

    # When
    response = client.post(_BRIDGE_PATH, headers=_headers(), json=_delivery())

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "skipped"}


def test_pagerduty_bridge_reports_feature_disabled_for_a_gated_org(
    client: TestClient, reconcile: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    monkeypatch.setattr(
        system_webhooks,
        "_canonical_incident_ingestion_allowed",
        MagicMock(return_value=False),
    )

    # When
    response = client.post(_BRIDGE_PATH, headers=_headers(), json=_delivery())

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "feature_disabled"}
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_reports_revoked_binding_for_an_unusable_binding(
    client: TestClient, reconcile: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    monkeypatch.setattr(
        webhook_worker,
        "resolve_pagerduty_webhook_binding",
        AsyncMock(side_effect=RuntimeError("pagerduty webhook binding is unavailable")),
    )

    # When
    response = client.post(_BRIDGE_PATH, headers=_headers(), json=_delivery())

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "revoked_binding"}
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_reports_malformed_for_an_invalid_payload(
    client: TestClient, reconcile: AsyncMock
) -> None:
    # When
    response = client.post(
        _BRIDGE_PATH,
        headers=_headers(),
        json=_delivery(payload={"event": {"id": "pagey-event-1"}}),
    )

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "malformed"}
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_reports_rejected_for_a_verification_ping(
    client: TestClient, reconcile: AsyncMock
) -> None:
    # When
    response = client.post(
        _BRIDGE_PATH,
        headers=_headers(),
        json=_delivery(payload=_payload(event_type="pagey.ping")),
    )

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "rejected"}
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_reports_rejected_when_the_payload_event_id_disagrees(
    client: TestClient, reconcile: AsyncMock
) -> None:
    # When: the receipt identity Go deduplicates on would name another event.
    response = client.post(
        _BRIDGE_PATH,
        headers=_headers(),
        json=_delivery(payload=_payload(event_id="a-different-event")),
    )

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "rejected"}
    reconcile.assert_not_awaited()


def test_pagerduty_bridge_surfaces_a_dependency_failure_as_retryable(
    monkeypatch: pytest.MonkeyPatch, reconcile: AsyncMock
) -> None:
    # Given
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", _TOKEN)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://localhost:9000/dev_health")
    monkeypatch.setattr(
        webhook_worker,
        "resolve_pagerduty_webhook_binding",
        AsyncMock(return_value=_CONTEXT),
    )
    monkeypatch.setattr(
        system_webhooks,
        "_canonical_incident_ingestion_allowed",
        MagicMock(return_value=True),
    )
    reconcile.side_effect = SQLAlchemyError("postgres is unreachable")

    # When
    response = TestClient(app, raise_server_exceptions=False).post(
        _BRIDGE_PATH, headers=_headers(), json=_delivery()
    )

    # Then: 5xx keeps the delivery in the Go pending list instead of
    # quarantining it as a permanent rejection.
    assert response.status_code >= 500


def test_pagerduty_bridge_treats_unconfigured_persistence_as_retryable(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)

    # When
    response = client.post(_BRIDGE_PATH, headers=_headers(), json=_delivery())

    # Then
    assert response.status_code == 503
