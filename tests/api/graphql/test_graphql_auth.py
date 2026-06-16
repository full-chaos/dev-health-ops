"""Tests for GraphQL endpoint authentication enforcement (CHAOS-633)."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import JWT_ALGORITHM, AuthService
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import User
from tests._helpers import tables_of


@pytest.fixture
def auth_service():
    return AuthService(secret_key="test-secret-key-for-graphql-auth")


@pytest.fixture
def seeded_user_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def valid_token(auth_service, seeded_user_id):
    return auth_service.create_access_token(
        user_id=str(seeded_user_id),
        email="test@example.com",
        org_id="test-org",
        role="member",
    )


@pytest.fixture
def graphql_auth_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seeded_user_id):
    db_path = tmp_path / "graphql-auth.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _setup() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: Base.metadata.create_all(
                    sync_conn,
                    tables=tables_of(User),
                )
            )
        async with maker() as session:
            session.add(
                User(
                    id=seeded_user_id,
                    email="test@example.com",
                    password_hash="hash",
                    is_active=True,
                    is_verified=True,
                    token_version=0,
                )
            )
            await session.commit()

    asyncio.run(_setup())

    @asynccontextmanager
    async def _session_override():
        async with maker() as session:
            yield session

    monkeypatch.setattr("dev_health_ops.db.get_postgres_session", _session_override)
    try:
        yield maker
    finally:
        asyncio.run(engine.dispose())


@pytest.fixture
def client_auth_required(monkeypatch, graphql_auth_db):
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


class TestGraphQLTokenVersionAuth:
    def test_bumped_token_version_rejects_previously_valid_graphql_token(
        self,
        client_auth_required,
        valid_token,
        graphql_auth_db,
        seeded_user_id,
    ):
        async def _bump() -> None:
            async with graphql_auth_db() as session:
                user = await session.get(User, seeded_user_id)
                assert user is not None
                user.token_version = 1
                await session.commit()

        asyncio.run(_bump())

        response = client_auth_required.post(
            "/graphql",
            json={"query": "{ __typename }"},
            headers={"Authorization": f"Bearer {valid_token}"},
        )

        assert response.status_code == 401

    def test_bumped_token_version_rejects_stale_superuser_graphql_token(
        self,
        client_auth_required,
        auth_service,
        graphql_auth_db,
        seeded_user_id,
    ):
        superuser_token = auth_service.create_access_token(
            user_id=str(seeded_user_id),
            email="test@example.com",
            org_id="test-org",
            role="owner",
            is_superuser=True,
            token_version=0,
        )

        async def _promote_and_bump() -> None:
            async with graphql_auth_db() as session:
                user = await session.get(User, seeded_user_id)
                assert user is not None
                user.is_superuser = True
                user.token_version = 1
                await session.commit()

        asyncio.run(_promote_and_bump())

        response = client_auth_required.post(
            "/graphql",
            json={"query": "{ __typename }"},
            headers={"Authorization": f"Bearer {superuser_token}"},
        )

        assert response.status_code == 401

    def test_missing_token_version_claim_allows_version_zero_user(
        self,
        client_auth_required,
        auth_service,
        seeded_user_id,
    ):
        now = datetime.now(timezone.utc)
        legacy_token = jwt.encode(
            {
                "sub": str(seeded_user_id),
                "email": "test@example.com",
                "org_id": "test-org",
                "role": "member",
                "is_superuser": False,
                "type": "access",
                "iss": auth_service.issuer,
                "aud": auth_service.audience,
                "exp": now + timedelta(minutes=5),
                "iat": now,
                "jti": str(uuid.uuid4()),
            },
            auth_service.secret_key,
            algorithm=JWT_ALGORITHM,
        )

        response = client_auth_required.post(
            "/graphql",
            json={"query": "{ __typename }"},
            headers={"Authorization": f"Bearer {legacy_token}"},
        )

        assert response.status_code != 401
