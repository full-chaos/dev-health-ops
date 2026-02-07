import uuid
from datetime import datetime
import importlib
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.users import Organization


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _SessionContext:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_session(*values):
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[_ScalarResult(value) for value in values])
    session.flush = AsyncMock()
    session.add = Mock()
    session.rollback = AsyncMock()
    return session


def _make_org(org_uuid: uuid.UUID, tier: str = "free") -> Organization:
    org = Organization(slug=f"org-{org_uuid.hex[:8]}", name="Test Org")
    org.id = org_uuid
    org.tier = tier
    return org


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_license_webhook_creates_org_license(client, monkeypatch):
    org_uuid = uuid.uuid4()
    org = _make_org(org_uuid)
    session = _make_session(org, None)

    monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret")

    async def override_get_session():
        yield session

    app.dependency_overrides[get_postgres_session] = override_get_session
    try:
        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": str(org_uuid),
                "tier": "pro",
                "action": "license_generated",
                "licensed_users": 25,
                "licensed_repos": 15,
                "customer_id": "cus_123",
                "features_override": {"sso_saml": True},
                "limits_override": {"max_users": 40, "api_rate_limit_per_min": 2500},
                "expires_at": "2027-01-01T00:00:00Z",
            },
            headers={"x-webhook-secret": "test-secret"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    added = session.add.call_args[0][0]
    assert isinstance(added, OrgLicense)
    assert added.org_id == org_uuid
    assert added.tier == "pro"
    assert added.licensed_users == 25
    assert added.licensed_repos == 15
    assert added.customer_id == "cus_123"
    assert added.features_override == {"sso_saml": True}
    assert added.limits_override == {"max_users": 40, "api_rate_limit_per_min": 2500}
    assert added.expires_at is not None
    assert added.is_valid is True
    assert added.last_validated_at is not None


def test_license_webhook_updates_existing_org_license(client, monkeypatch):
    org_uuid = uuid.uuid4()
    org = _make_org(org_uuid)
    existing = OrgLicense(org_id=org_uuid, tier="starter", license_type="saas")

    session = _make_session(org, existing)

    monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret")

    async def override_get_session():
        yield session

    app.dependency_overrides[get_postgres_session] = override_get_session
    try:
        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": str(org_uuid),
                "tier": "enterprise",
                "action": "license_upgraded",
                "licensed_users": 200,
                "licensed_repos": 500,
                "customer_id": "cus_updated",
                "features_override": {"audit_log": True},
                "limits_override": {"api_rate_limit_per_min": None},
            },
            headers={"x-webhook-secret": "test-secret"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    session.add.assert_not_called()
    assert existing.tier == "enterprise"
    assert existing.licensed_users == 200
    assert existing.licensed_repos == 500
    assert existing.customer_id == "cus_updated"
    assert existing.features_override == {"audit_log": True}
    assert existing.limits_override == {"api_rate_limit_per_min": None}
    assert existing.is_valid is True
    assert existing.last_validated_at is not None


def test_license_webhook_partial_payload(client, monkeypatch):
    org_uuid = uuid.uuid4()
    org = _make_org(org_uuid)
    session = _make_session(org, None)

    monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "test-secret")

    async def override_get_session():
        yield session

    app.dependency_overrides[get_postgres_session] = override_get_session
    try:
        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": str(org_uuid),
                "tier": "team",
                "action": "license_generated",
            },
            headers={"x-webhook-secret": "test-secret"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    added = session.add.call_args[0][0]
    assert isinstance(added, OrgLicense)
    assert added.tier == "starter"
    assert added.licensed_users is None
    assert added.licensed_repos is None
    assert added.customer_id is None
    assert added.features_override == {}
    assert added.limits_override == {}
    assert added.expires_at is None


def test_entitlements_endpoint_returns_org_state(client, monkeypatch):
    org_uuid = uuid.uuid4()
    org = _make_org(org_uuid, tier="starter")
    org_license = OrgLicense(org_id=org_uuid, tier="starter", license_type="saas")
    org_license.licensed_users = 12
    org_license.licensed_repos = 9
    org_license.features_override = {"audit_log": False}
    org_license.limits_override = {"max_users": 99}
    org_license.expires_at = datetime(2027, 1, 1)
    org_license.is_valid = True

    session = _make_session(org, org_license)
    licensing_router_module = importlib.import_module(
        "dev_health_ops.api.licensing.router"
    )
    monkeypatch.setattr(
        licensing_router_module,
        "get_postgres_session",
        lambda: _SessionContext(session),
    )

    response = client.get(f"/api/v1/licensing/entitlements/{org_uuid}")

    assert response.status_code == 200
    data = response.json()
    assert data["org_id"] == str(org_uuid)
    assert data["tier"] == "starter"
    assert data["licensed_users"] == 12
    assert data["licensed_repos"] == 9
    assert data["features_override"] == {"audit_log": False}
    assert data["limits_override"] == {"max_users": 99}
    assert data["is_valid"] is True
    assert data["limits"]["max_users"] == 99
    assert data["limits"]["max_repos"] == 10


def test_entitlements_endpoint_org_not_found(client, monkeypatch):
    session = _make_session(None)
    licensing_router_module = importlib.import_module(
        "dev_health_ops.api.licensing.router"
    )
    monkeypatch.setattr(
        licensing_router_module,
        "get_postgres_session",
        lambda: _SessionContext(session),
    )

    response = client.get(f"/api/v1/licensing/entitlements/{uuid.uuid4()}")

    assert response.status_code == 404


def test_license_webhook_requires_secret_configured(client, monkeypatch):
    """Test that webhook fails with 500 if LICENSE_WEBHOOK_SECRET is not set."""
    monkeypatch.delenv("LICENSE_WEBHOOK_SECRET", raising=False)
    
    response = client.post(
        "/api/v1/webhooks/license",
        json={
            "org_id": str(uuid.uuid4()),
            "tier": "pro",
            "action": "license_generated",
        },
    )
    
    assert response.status_code == 500
    assert "not configured" in response.json()["detail"]


def test_license_webhook_rejects_invalid_secret(client, monkeypatch):
    """Test that webhook rejects requests with wrong secret."""
    monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "correct-secret")
    
    response = client.post(
        "/api/v1/webhooks/license",
        json={
            "org_id": str(uuid.uuid4()),
            "tier": "pro",
            "action": "license_generated",
        },
        headers={"x-webhook-secret": "wrong-secret"},
    )
    
    assert response.status_code == 401
    assert "Invalid webhook secret" in response.json()["detail"]


def test_license_webhook_accepts_valid_secret(client, monkeypatch):
    """Test that webhook accepts requests with correct secret."""
    org_uuid = uuid.uuid4()
    org = _make_org(org_uuid)
    session = _make_session(org, None)
    
    monkeypatch.setenv("LICENSE_WEBHOOK_SECRET", "correct-secret")
    
    async def override_get_session():
        yield session
    
    app.dependency_overrides[get_postgres_session] = override_get_session
    try:
        response = client.post(
            "/api/v1/webhooks/license",
            json={
                "org_id": str(org_uuid),
                "tier": "pro",
                "action": "license_generated",
            },
            headers={"x-webhook-secret": "correct-secret"},
        )
    finally:
        app.dependency_overrides.clear()
    
    assert response.status_code == 200
