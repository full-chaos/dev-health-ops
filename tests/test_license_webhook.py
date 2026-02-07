import pytest
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient
from dev_health_ops.api.main import app
from dev_health_ops.db import get_postgres_session


@pytest.fixture
def client():
    mock_session = AsyncMock()

    async def override_get_session():
        yield mock_session

    app.dependency_overrides[get_postgres_session] = override_get_session
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


class TestLicenseWebhook:
    """Tests for POST /api/v1/webhooks/license endpoint."""

    def test_accepts_valid_payload(self, client, monkeypatch):
        """Valid tier change payload is accepted."""
        monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret")

        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": "test-org-123",
                "tier": "team",
                "action": "license_generated",
                "license_id": "lic-abc",
                "expires_at": "2027-01-01T00:00:00Z",
            },
            headers={"x-webhook-secret": "test-secret"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert "test-org-123" in data["message"]

    def test_accepts_minimal_payload(self, client, monkeypatch):
        """Minimal required fields are sufficient."""
        monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret")

        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": "org-minimal",
                "tier": "enterprise",
                "action": "license_upgraded",
            },
            headers={"x-webhook-secret": "test-secret"},
        )
        assert response.status_code == 200

    def test_rejects_invalid_payload(self, client, monkeypatch):
        """Missing required fields return 400."""
        monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret")

        response = client.post(
            "/api/v1/webhooks/license",
            json={"tier": "team"},  # missing org_id and action
            headers={"x-webhook-secret": "test-secret"},
        )
        assert response.status_code == 400

    def test_rejects_invalid_json(self, client, monkeypatch):
        """Non-JSON body returns 400."""
        monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret")

        response = client.post(
            "/api/v1/webhooks/license",
            content="not json",
            headers={"content-type": "application/json", "x-webhook-secret": "test-secret"},
        )
        assert response.status_code == 400

    def test_validates_webhook_secret(self, client, monkeypatch):
        """When LICENSE_WEBHOOK_SECRET is set, requests must include it."""
        monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret-123")

        # Without secret — should fail
        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": "org-1",
                "tier": "team",
                "action": "license_generated",
            },
        )
        assert response.status_code == 401

    def test_accepts_with_correct_secret(self, client, monkeypatch):
        """Correct webhook secret allows the request through."""
        monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret-123")

        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": "org-1",
                "tier": "team",
                "action": "license_generated",
            },
            headers={"x-webhook-secret": "test-secret-123"},
        )
        assert response.status_code == 200

    def test_rejects_wrong_secret(self, client, monkeypatch):
        """Wrong webhook secret is rejected."""
        monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "correct-secret")

        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": "org-1",
                "tier": "team",
                "action": "license_generated",
            },
            headers={"x-webhook-secret": "wrong-secret"},
        )
        assert response.status_code == 401
