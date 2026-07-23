from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from dev_health_ops.api.main import app


def test_internal_bridge_requires_token(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    response = TestClient(app).post(
        "/api/internal/worker-operational/webhook",
        json={
            "delivery_id": "00000000-0000-4000-8000-000000000012",
            "provider": "github",
            "event_type": "push",
        },
    )
    assert response.status_code == 401


def test_internal_bridge_passes_only_durable_webhook_reference(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with patch(
        "dev_health_ops.api.internal.worker_operational.process_webhook_event.run",
        return_value={"status": "success"},
    ) as run:
        response = TestClient(app).post(
            "/api/internal/worker-operational/webhook",
            headers={"Authorization": "Bearer test-token"},
            json={
                "delivery_id": "00000000-0000-4000-8000-000000000012",
                "provider": "github",
                "event_type": "push",
            },
        )
    assert response.status_code == 200
    run.assert_called_once_with(
        durable_delivery_id="00000000-0000-4000-8000-000000000012"
    )


def test_internal_bridge_passes_only_durable_billing_reference(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with patch(
        "dev_health_ops.api.internal.worker_operational.send_billing_notification.run",
        return_value={"status": "sent"},
    ) as run:
        response = TestClient(app).post(
            "/api/internal/worker-operational/billing",
            headers={"Authorization": "Bearer test-token"},
            json={
                "notification_id": "00000000-0000-4000-8000-000000000011",
                "organization_id": "00000000-0000-4000-8000-000000000010",
                "notification_type": "invoice_receipt",
            },
        )
    assert response.status_code == 200
    run.assert_called_once_with(
        durable_notification_id="00000000-0000-4000-8000-000000000011"
    )


def test_internal_bridge_classifies_dropped_billing_as_permanent(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with patch(
        "dev_health_ops.api.internal.worker_operational.send_billing_notification.run",
        return_value={"status": "dropped", "reason": "missing_durable_notification"},
    ):
        response = TestClient(app).post(
            "/api/internal/worker-operational/billing",
            headers={"Authorization": "Bearer test-token"},
            json={
                "notification_id": "00000000-0000-4000-8000-000000000011",
                "organization_id": "00000000-0000-4000-8000-000000000010",
                "notification_type": "invoice_receipt",
            },
        )
    assert response.status_code == 422
    assert response.json() == {"detail": "Operational delivery rejected"}


def test_internal_bridge_classifies_unknown_result_shape_as_retryable(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with patch(
        "dev_health_ops.api.internal.worker_operational.process_webhook_event.run",
        return_value=None,
    ):
        response = TestClient(app).post(
            "/api/internal/worker-operational/webhook",
            headers={"Authorization": "Bearer test-token"},
            json={
                "delivery_id": "00000000-0000-4000-8000-000000000012",
                "provider": "github",
                "event_type": "push",
            },
        )
    assert response.status_code == 502


def test_internal_bridge_dispatches_heartbeat_occurrence(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with patch(
        "dev_health_ops.api.internal.worker_operational.phone_home_heartbeat.run",
        return_value={"status": "ok"},
    ) as run:
        response = TestClient(app).post(
            "/api/internal/worker-operational/heartbeat",
            headers={"Authorization": "Bearer test-token"},
            json={"scheduled_for": "2026-07-21T12:00:00Z"},
        )
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    run.assert_called_once_with()


def test_internal_bridge_rejects_timezone_free_heartbeat(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    response = TestClient(app).post(
        "/api/internal/worker-operational/heartbeat",
        headers={"Authorization": "Bearer test-token"},
        json={"scheduled_for": "2026-07-21T12:00:00"},
    )
    assert response.status_code == 422
