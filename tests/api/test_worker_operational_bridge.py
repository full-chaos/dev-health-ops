from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from dev_health_ops.api.main import app


def test_internal_bridge_requires_token(monkeypatch) -> None:
    # This test has now outlived two routes: CHAOS-5320 deleted /webhook and
    # CHAOS-5353 deleted /billing. Retargeted to /heartbeat, the surviving
    # durable-row route with the same auth requirement --
    # authorize_worker_bridge gates every route on this router identically,
    # independent of which one carries the check.
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    response = TestClient(app).post(
        "/api/internal/worker-operational/heartbeat",
        json={"scheduled_for": "2026-07-21T12:00:00Z"},
    )
    assert response.status_code == 401


def test_internal_bridge_classifies_unknown_result_shape_as_retryable(
    monkeypatch,
) -> None:
    # Retargeted twice now (CHAOS-5320 deleted /webhook, CHAOS-5353 deleted
    # /billing): _bridge_result's "non-dict result -> 502" path is generic
    # across every route on this router, never specific to one of them.
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with patch(
        "dev_health_ops.api.internal.worker_operational.phone_home_heartbeat.run",
        return_value=None,
    ):
        response = TestClient(app).post(
            "/api/internal/worker-operational/heartbeat",
            headers={"Authorization": "Bearer test-token"},
            json={"scheduled_for": "2026-07-21T12:00:00Z"},
        )
    assert response.status_code == 502


def test_internal_bridge_no_longer_exposes_a_billing_route(monkeypatch) -> None:
    """CHAOS-5353: the billing bridge is gone, not merely unused.

    A route left mounted would still accept and act on a POST from an old Go
    binary, sending an email the native handler has already sent -- so the
    deletion, not just the disuse, is the property worth pinning.
    """
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    response = TestClient(app).post(
        "/api/internal/worker-operational/billing",
        headers={"Authorization": "Bearer test-token"},
        json={
            "notification_id": "00000000-0000-4000-8000-000000000011",
            "organization_id": "00000000-0000-4000-8000-000000000010",
            "notification_type": "invoice_receipt",
        },
    )
    assert response.status_code == 404


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


def test_internal_bridge_rejects_non_utc_heartbeat_offset(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with patch(
        "dev_health_ops.api.internal.worker_operational.phone_home_heartbeat.run"
    ) as run:
        response = TestClient(app).post(
            "/api/internal/worker-operational/heartbeat",
            headers={"Authorization": "Bearer test-token"},
            json={"scheduled_for": "2026-07-21T12:00:00+05:00"},
        )
    assert response.status_code == 422
    run.assert_not_called()
