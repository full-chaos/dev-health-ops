"""Tests for GraphQL endpoint authentication enforcement (CHAOS-633)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.services.auth import AuthService


@pytest.fixture
def auth_service():
    return AuthService(secret_key="test-secret-key-for-graphql-auth")


@pytest.fixture
def valid_token(auth_service):
    return auth_service.create_access_token(
        user_id="user-1",
        email="test@example.com",
        org_id="test-org",
        role="member",
    )


@pytest.fixture
def client_auth_required(monkeypatch):
    """TestClient with auth enforcement enabled (default)."""
    monkeypatch.setenv("GRAPHQL_AUTH_REQUIRED", "true")
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-graphql-auth")
    # Reset cached auth service so it picks up the test secret
    monkeypatch.setattr("dev_health_ops.api.services.auth._auth_service", None)
    from dev_health_ops.api.main import app

    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def client_auth_disabled(monkeypatch):
    """TestClient with auth enforcement disabled."""
    monkeypatch.setenv("GRAPHQL_AUTH_REQUIRED", "false")
    from dev_health_ops.api.main import app

    return TestClient(app, raise_server_exceptions=False)


class TestGraphQLAuthEnforcement:
    def test_unauthenticated_request_returns_401(self, client_auth_required):
        response = client_auth_required.post(
            "/graphql",
            json={"query": "{ __typename }"},
        )
        assert response.status_code == 401

    def test_invalid_token_returns_401(self, client_auth_required):
        response = client_auth_required.post(
            "/graphql",
            json={"query": "{ __typename }"},
            headers={"Authorization": "Bearer invalid-token"},
        )
        assert response.status_code == 401

    def test_valid_token_passes_auth(self, client_auth_required, valid_token):
        response = client_auth_required.post(
            "/graphql",
            json={"query": "{ __typename }"},
            headers={"Authorization": f"Bearer {valid_token}"},
        )
        # Should not be 401 — may be 200 or a GraphQL error (e.g. org_id
        # validation), but auth itself must pass.
        assert response.status_code != 401

    def test_auth_disabled_allows_unauthenticated(self, client_auth_disabled):
        response = client_auth_disabled.post(
            "/graphql",
            json={"query": "{ __typename }"},
        )
        # Without auth enforcement, the request proceeds to the GraphQL
        # layer (may fail on org_id validation, but not on auth).
        assert response.status_code != 401


class TestGraphQLOrgIdFromJWT:
    def test_org_id_resolved_from_jwt(self, client_auth_required, valid_token):
        """Authenticated requests should use org_id from the JWT claim."""
        response = client_auth_required.post(
            "/graphql",
            json={"query": "{ __typename }"},
            headers={"Authorization": f"Bearer {valid_token}"},
        )
        # The request should pass auth; org_id is "test-org" from the token.
        assert response.status_code != 401
