from __future__ import annotations

import base64
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.internal.acr import router
from dev_health_ops.api.services.auth import AuthService
from dev_health_ops.licensing.generator import generate_test_license
from dev_health_ops.models.git import Base
from dev_health_ops.models.internal_service_credential import (
    InternalServiceCredential,
    InternalServiceCredentialAudit,
    generate_internal_service_token,
)
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.subscriptions import Subscription
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

_TABLES = tables_of(
    Organization,
    OrgLicense,
    FeatureFlag,
    OrgFeatureOverride,
    Subscription,
    InternalServiceCredential,
    InternalServiceCredentialAudit,
)


@pytest_asyncio.fixture
async def session_maker(tmp_path):
    db_path = tmp_path / "acr-entitlements.db"
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


@pytest_asyncio.fixture
async def entitled_org(session_maker):
    org_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="acr-entitled", name="ACR Entitled"),
                FeatureFlag(
                    key="agent_context_runtime",
                    name="Agent Context Runtime",
                    category="integrations",
                    min_tier="community",
                ),
                OrgLicense(
                    org_id=org_id,
                    tier="team",
                    features_override={"agent_context_runtime": True},
                ),
            ]
        )
        await session.commit()
    return str(org_id)


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


def _user_jwt() -> str:
    return "Bearer " + AuthService(
        secret_key="test-secret-key-for-internal-acr"
    ).create_access_token(
        user_id="11111111-1111-1111-1111-111111111111",
        email="user@example.com",
        org_id="22222222-2222-2222-2222-222222222222",
    )


def _unknown_internal_service_token() -> str:
    return "Bearer " + generate_internal_service_token()


def _license_key_token() -> str:
    return "Bearer " + generate_test_license(org_id=str(uuid.uuid4()))


def _acr_client_credential_token() -> str:
    token = "fcacr_" + base64.urlsafe_b64encode(b"a" * 32).decode().rstrip("=")
    return "Bearer " + token


@pytest.mark.asyncio
async def test_acr_entitlement_returns_exact_minimal_contract_for_valid_service_token(
    session_maker, entitled_org, monkeypatch
):
    async with session_maker() as session:
        token = await _create_credential(session)
    _patch_session(monkeypatch, session_maker)
    app = FastAPI()
    app.include_router(router)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/api/v1/internal/acr/entitlements/{entitled_org}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200
    assert response.json() == {
        "schema_version": "acr_entitlement.v1",
        "org_id": entitled_org,
        "agent_context_runtime": True,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "Basic abc",
        "Bearer invalid",
        _unknown_internal_service_token,
        _acr_client_credential_token,
        _license_key_token,
        _user_jwt,
    ],
)
async def test_acr_entitlement_rejects_missing_malformed_or_unknown_tokens(
    session_maker, entitled_org, monkeypatch, authorization
):
    _patch_session(monkeypatch, session_maker)
    app = FastAPI()
    app.include_router(router)
    resolved_authorization = (
        authorization() if callable(authorization) else authorization
    )
    headers = (
        {}
        if resolved_authorization is None
        else {"Authorization": resolved_authorization}
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/api/v1/internal/acr/entitlements/{entitled_org}", headers=headers
        )
    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}
    async with session_maker() as session:
        audits = (
            (await session.execute(select(InternalServiceCredentialAudit)))
            .scalars()
            .all()
        )
    assert len(audits) == 1
    assert audits[0].credential_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "credential_kwargs",
    [
        {"scopes": ["other:read"]},
        {"expires_at": datetime.now(timezone.utc) - timedelta(seconds=1)},
        {"revoked_at": datetime.now(timezone.utc)},
    ],
)
async def test_acr_entitlement_fails_closed_for_insufficient_or_inactive_token(
    session_maker, entitled_org, monkeypatch, credential_kwargs
):
    async with session_maker() as session:
        token = await _create_credential(session, **credential_kwargs)
    _patch_session(monkeypatch, session_maker)
    app = FastAPI()
    app.include_router(router)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/api/v1/internal/acr/entitlements/{entitled_org}",
            headers={"Authorization": f"Bearer {token}"},
        )
    expected_status = 403 if credential_kwargs.get("scopes") else 401
    assert response.status_code == expected_status
    assert token not in response.text


@pytest.mark.asyncio
async def test_acr_entitlement_returns_404_for_missing_org_after_service_auth(
    session_maker, monkeypatch
):
    async with session_maker() as session:
        token = await _create_credential(session)
    _patch_session(monkeypatch, session_maker)
    app = FastAPI()
    app.include_router(router)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/api/v1/internal/acr/entitlements/{uuid.uuid4()}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_acr_entitlement_audits_and_safely_fails_when_entitlement_lookup_errors(
    session_maker, entitled_org, monkeypatch
):
    async with session_maker() as session:
        token = await _create_credential(session)
    _patch_session(monkeypatch, session_maker)

    async def _raise_entitlement_error(_org_id, _session):
        raise RuntimeError("sensitive upstream failure")

    from dev_health_ops.api.internal import acr

    monkeypatch.setattr(acr, "get_org_entitlements_from_db", _raise_entitlement_error)
    app = FastAPI()
    app.include_router(router)
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/api/v1/internal/acr/entitlements/{entitled_org}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 503
    assert response.json() == {"detail": "Service unavailable"}
    assert "sensitive upstream failure" not in response.text
    async with session_maker() as session:
        audit = (
            await session.execute(
                select(InternalServiceCredentialAudit).order_by(
                    InternalServiceCredentialAudit.created_at.desc()
                )
            )
        ).scalar_one()
    assert audit.credential_id is not None
    assert audit.requested_org_id == entitled_org
    assert audit.outcome == "entitlement_lookup_failed"


@pytest.mark.asyncio
async def test_acr_entitlement_rate_limit_stops_burst_before_auth_audit_writes(
    session_maker, entitled_org, monkeypatch
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
    app = FastAPI()
    app.include_router(router)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.get(f"/api/v1/internal/acr/entitlements/{entitled_org}")
        second = await client.get(f"/api/v1/internal/acr/entitlements/{entitled_org}")
        third = await client.get(f"/api/v1/internal/acr/entitlements/{entitled_org}")
    assert first.status_code == 401
    assert second.status_code == 401
    assert third.status_code == 429
    async with session_maker() as session:
        audits = (
            (await session.execute(select(InternalServiceCredentialAudit)))
            .scalars()
            .all()
        )
    assert len(audits) == 2
