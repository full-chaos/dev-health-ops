"""Tests for credential resolution fallback in discovery endpoints.

Connections created through the admin UI are stored under a user-chosen
name (e.g. "chaos"), but the discovery endpoints used to hardcode
``name="default"`` — making team/member discovery 404 for every provider
unless a credential happened to be named "default".

Covers ``IntegrationCredentialsService.resolve_with_fallback`` and the
``GET /teams/discover`` endpoint behaviour built on it.
"""

from __future__ import annotations

import importlib
import os
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import (
    AmbiguousCredentialError,
    IntegrationCredentialsService,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(User, Organization, Membership, IntegrationCredential)

ORG_ID = str(uuid.uuid4())


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "cred-fallback.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_credential(
    session_maker,
    provider: str,
    name: str,
    is_active: bool = True,
    credentials: dict | None = None,
) -> None:
    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        await svc.set(
            provider=provider,
            credentials=credentials or {"apiKey": "lin_api_test"},
            name=name,
            is_active=is_active,
        )
        await session.commit()


# ---------------------------------------------------------------------------
# Service-level: resolve_with_fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_falls_back_to_single_active_credential(session_maker):
    await _seed_credential(session_maker, "linear", "chaos")

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        cred, decrypted = await svc.resolve_with_fallback("linear")

    assert cred is not None
    assert str(cred.name) == "chaos"
    assert decrypted == {"api_key": "lin_api_test"}


@pytest.mark.asyncio
async def test_prefers_default_name_when_present(session_maker):
    await _seed_credential(
        session_maker, "linear", "default", credentials={"apiKey": "default-key"}
    )
    await _seed_credential(
        session_maker, "linear", "chaos", credentials={"apiKey": "chaos-key"}
    )

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        cred, decrypted = await svc.resolve_with_fallback("linear")

    assert cred is not None
    assert str(cred.name) == "default"
    assert decrypted == {"api_key": "default-key"}


@pytest.mark.asyncio
async def test_multiple_active_without_default_is_ambiguous(session_maker):
    await _seed_credential(session_maker, "linear", "chaos")
    await _seed_credential(session_maker, "linear", "other")

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        with pytest.raises(AmbiguousCredentialError) as exc_info:
            await svc.resolve_with_fallback("linear")

    assert exc_info.value.names == ["chaos", "other"]


@pytest.mark.asyncio
async def test_inactive_credentials_are_not_fallback_candidates(session_maker):
    await _seed_credential(session_maker, "linear", "chaos", is_active=False)

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        cred, decrypted = await svc.resolve_with_fallback("linear")

    assert cred is None
    assert decrypted is None


@pytest.mark.asyncio
async def test_explicit_name_lookup(session_maker):
    await _seed_credential(session_maker, "linear", "chaos")
    await _seed_credential(session_maker, "linear", "other")

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        cred, decrypted = await svc.resolve_with_fallback("linear", name="other")

    assert cred is not None
    assert str(cred.name) == "other"


@pytest.mark.asyncio
async def test_credential_id_must_match_provider(session_maker):
    await _seed_credential(session_maker, "github", "chaos", credentials={"token": "x"})

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        github_cred = await svc.get("github", "chaos")
        assert github_cred is not None
        cred, decrypted = await svc.resolve_with_fallback(
            "linear", credential_id=str(github_cred.id)
        )

    assert cred is None
    assert decrypted is None


@pytest.mark.asyncio
async def test_credential_id_lookup(session_maker):
    await _seed_credential(session_maker, "linear", "chaos")

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        stored = await svc.get("linear", "chaos")
        assert stored is not None
        cred, decrypted = await svc.resolve_with_fallback(
            "linear", credential_id=str(stored.id)
        )

    assert cred is not None
    assert decrypted == {"api_key": "lin_api_test"}


# ---------------------------------------------------------------------------
# Endpoint-level: GET /teams/discover
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def client(session_maker):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=ORG_ID,
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_discover_uses_non_default_credential(client, session_maker):
    await _seed_credential(session_maker, "linear", "chaos")

    with patch(
        "dev_health_ops.api.services.configuration.team_discovery."
        "TeamDiscoveryService.discover_linear",
        new=AsyncMock(return_value=[]),
    ) as mock_discover:
        response = await client.get("/api/v1/admin/teams/discover?provider=linear")

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["provider"] == "linear"
    assert data["teams"] == []
    mock_discover.assert_awaited_once_with(api_key="lin_api_test")


@pytest.mark.asyncio
async def test_discover_404_when_no_credentials(client):
    response = await client.get("/api/v1/admin/teams/discover?provider=linear")
    assert response.status_code == 404
    assert "No credentials found for provider 'linear'" in response.text


@pytest.mark.asyncio
async def test_discover_409_when_ambiguous(client, session_maker):
    await _seed_credential(session_maker, "linear", "chaos")
    await _seed_credential(session_maker, "linear", "other")

    response = await client.get("/api/v1/admin/teams/discover?provider=linear")
    assert response.status_code == 409
    assert "Multiple active credentials" in response.text


@pytest.mark.asyncio
async def test_discover_with_explicit_credential_name(client, session_maker):
    await _seed_credential(
        session_maker, "linear", "chaos", credentials={"apiKey": "chaos-key"}
    )
    await _seed_credential(
        session_maker, "linear", "other", credentials={"apiKey": "other-key"}
    )

    with patch(
        "dev_health_ops.api.services.configuration.team_discovery."
        "TeamDiscoveryService.discover_linear",
        new=AsyncMock(return_value=[]),
    ) as mock_discover:
        response = await client.get(
            "/api/v1/admin/teams/discover?provider=linear&credential_name=other"
        )

    assert response.status_code == 200, response.text
    mock_discover.assert_awaited_once_with(api_key="other-key")
