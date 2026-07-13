from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.internal.acr import router
from dev_health_ops.models.git import Base
from dev_health_ops.models.internal_service_credential import (
    InternalServiceCredential,
    InternalServiceCredentialAudit,
    generate_internal_service_token,
)
from tests._helpers import tables_of

_TABLES = tables_of(InternalServiceCredential, InternalServiceCredentialAudit)
_HEALTH_PATH = "/api/v1/internal/acr/health"


@pytest_asyncio.fixture
async def session_maker(tmp_path):
    db_path = tmp_path / "acr-health.db"
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


async def _create_credential(
    session: AsyncSession,
    *,
    scopes: list[str] | None = None,
    expires_at: datetime | None = None,
    revoked_at: datetime | None = None,
) -> str:
    token = generate_internal_service_token()
    credential = InternalServiceCredential.from_plaintext_token(
        token=token,
        service_name="acr",
        scopes=scopes or ["entitlements:read"],
        expires_at=expires_at,
    )
    credential.revoked_at = revoked_at
    session.add(credential)
    await session.commit()
    return token


def _patch_session(monkeypatch, session_maker) -> None:
    from dev_health_ops.api.internal import acr

    @asynccontextmanager
    async def _session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(acr, "get_postgres_session", _session)


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.mark.asyncio
async def test_acr_health_returns_exact_contract_for_valid_service_credential(
    session_maker, monkeypatch
):
    async with session_maker() as session:
        token = await _create_credential(session)
    _patch_session(monkeypatch, session_maker)
    transport = ASGITransport(app=_app())
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            _HEALTH_PATH, headers={"Authorization": f"Bearer {token}"}
        )
    assert response.status_code == 200
    assert response.json() == {
        "schema_version": "acr_service_health.v1",
        "service": "dev-health-ops",
        "status": "ok",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization",
    [None, "Basic abc", "Bearer invalid", "Bearer svc_acr_unknown"],
)
async def test_acr_health_rejects_missing_malformed_or_unknown_tokens(
    session_maker, monkeypatch, authorization
):
    _patch_session(monkeypatch, session_maker)
    headers = {} if authorization is None else {"Authorization": authorization}
    transport = ASGITransport(app=_app())
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(_HEALTH_PATH, headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("credential_kwargs", "expected_status"),
    [
        ({"scopes": ["other:read"]}, 403),
        ({"expires_at": datetime.now(timezone.utc) - timedelta(seconds=1)}, 401),
        ({"revoked_at": datetime.now(timezone.utc)}, 401),
    ],
)
async def test_acr_health_fails_closed_for_insufficient_or_inactive_token(
    session_maker, monkeypatch, credential_kwargs, expected_status
):
    async with session_maker() as session:
        token = await _create_credential(session, **credential_kwargs)
    _patch_session(monkeypatch, session_maker)
    transport = ASGITransport(app=_app())
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            _HEALTH_PATH, headers={"Authorization": f"Bearer {token}"}
        )
    assert response.status_code == expected_status
    assert token not in response.text


@pytest.mark.asyncio
async def test_acr_health_rate_limit_stops_authentication_burst(
    session_maker, monkeypatch
):
    class Limiter:
        calls = 0

        def hit(self, _limit, _key):
            self.calls += 1
            return self.calls <= 2

    class RateLimiter:
        limiter = Limiter()

    from dev_health_ops.api.internal import acr

    monkeypatch.setattr(acr, "limiter", RateLimiter())
    _patch_session(monkeypatch, session_maker)
    transport = ASGITransport(app=_app())
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.get(_HEALTH_PATH)
        second = await client.get(_HEALTH_PATH)
        third = await client.get(_HEALTH_PATH)
    assert first.status_code == 401
    assert second.status_code == 401
    assert third.status_code == 429


@pytest.mark.asyncio
async def test_acr_health_sanitizes_unavailable_credential_store(monkeypatch):
    from dev_health_ops.api.internal import acr

    @asynccontextmanager
    async def _unavailable_session():
        raise RuntimeError("postgres configuration is unavailable")
        yield

    monkeypatch.setattr(acr, "get_postgres_session", _unavailable_session)
    transport = ASGITransport(app=_app(), raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            _HEALTH_PATH, headers={"Authorization": "Bearer svc_acr_valid"}
        )
    assert response.status_code == 503
    assert response.json() == {"detail": "Service unavailable"}
    assert "postgres configuration is unavailable" not in response.text
