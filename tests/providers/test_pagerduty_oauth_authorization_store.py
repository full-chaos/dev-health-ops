from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import PagerDutyOAuthAuthorizationRequest
from dev_health_ops.providers.pagerduty.oauth_authorization_store import (
    PagerDutyAuthorizationRequestStore,
)
from tests._helpers import tables_of

_TABLES = tables_of(PagerDutyOAuthAuthorizationRequest)
_NOW = datetime(2026, 7, 18, tzinfo=UTC)


@pytest.fixture(autouse=True)
def encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-pagerduty-oauth-key")


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with session_factory() as database_session:
        yield database_session
    await engine.dispose()


@pytest.mark.anyio
async def test_consume_returns_encrypted_pkce_context(session: AsyncSession) -> None:
    store = PagerDutyAuthorizationRequestStore(session)

    await store.create(
        org_id="org-1",
        state="opaque-state",
        credential_name="primary",
        code_verifier="pkce-verifier",
        enabled_datasets=["incidents", "services"],
        region="us",
        subdomain="acme",
        now=_NOW,
    )

    consumed = await store.consume(org_id="org-1", state="opaque-state", now=_NOW)

    assert consumed is not None
    assert consumed.credential_name == "primary"
    assert consumed.code_verifier == "pkce-verifier"
    assert consumed.enabled_datasets == ["incidents", "services"]
    assert consumed.region == "us"
    assert consumed.subdomain == "acme"


@pytest.mark.anyio
async def test_create_persists_only_state_hash_and_ciphertext(
    session: AsyncSession,
) -> None:
    store = PagerDutyAuthorizationRequestStore(session)
    state = "opaque-state"
    code_verifier = "pkce-verifier"

    await store.create(
        org_id="org-1",
        state=state,
        credential_name="primary",
        code_verifier=code_verifier,
        enabled_datasets=["incidents"],
        region="us",
        now=_NOW,
    )

    persisted_request = (
        await session.execute(select(PagerDutyOAuthAuthorizationRequest))
    ).scalar_one()

    assert persisted_request.state_hash == hashlib.sha256(state.encode()).hexdigest()
    assert persisted_request.code_verifier_encrypted != code_verifier


@pytest.mark.anyio
async def test_consume_returns_none_after_request_is_used(
    session: AsyncSession,
) -> None:
    store = PagerDutyAuthorizationRequestStore(session)

    await store.create(
        org_id="org-1",
        state="single-use-state",
        credential_name="primary",
        code_verifier="pkce-verifier",
        enabled_datasets=["incidents"],
        region="us",
        now=_NOW,
    )
    first_consume = await store.consume(
        org_id="org-1", state="single-use-state", now=_NOW
    )

    second_consume = await store.consume(
        org_id="org-1", state="single-use-state", now=_NOW
    )

    assert first_consume is not None
    assert second_consume is None


@pytest.mark.anyio
async def test_consume_removes_expired_request(session: AsyncSession) -> None:
    store = PagerDutyAuthorizationRequestStore(session)

    await store.create(
        org_id="org-1",
        state="expired-state",
        credential_name="primary",
        code_verifier="pkce-verifier",
        enabled_datasets=["incidents"],
        region="us",
        ttl=timedelta(seconds=0),
        now=_NOW,
    )

    consumed = await store.consume(org_id="org-1", state="expired-state", now=_NOW)

    assert consumed is None
    assert await store.purge_expired(now=_NOW) == 0


@pytest.mark.anyio
async def test_consume_preserves_request_for_other_organizations(
    session: AsyncSession,
) -> None:
    store = PagerDutyAuthorizationRequestStore(session)

    await store.create(
        org_id="org-1",
        state="organization-scoped-state",
        credential_name="primary",
        code_verifier="pkce-verifier",
        enabled_datasets=["incidents"],
        region="us",
        now=_NOW,
    )

    wrong_organization = await store.consume(
        org_id="org-2", state="organization-scoped-state", now=_NOW
    )
    matching_organization = await store.consume(
        org_id="org-1", state="organization-scoped-state", now=_NOW
    )

    assert wrong_organization is None
    assert matching_organization is not None
    assert matching_organization.code_verifier == "pkce-verifier"


@pytest.mark.anyio
async def test_consume_returns_none_for_unknown_state(session: AsyncSession) -> None:
    store = PagerDutyAuthorizationRequestStore(session)

    consumed = await store.consume(org_id="org-1", state="unknown-state", now=_NOW)

    assert consumed is None


@pytest.mark.anyio
async def test_purge_expired_keeps_active_requests(session: AsyncSession) -> None:
    store = PagerDutyAuthorizationRequestStore(session)

    await store.create(
        org_id="org-1",
        state="expired-state",
        credential_name="primary",
        code_verifier="expired-verifier",
        enabled_datasets=["incidents"],
        region="us",
        ttl=timedelta(seconds=0),
        now=_NOW,
    )
    await store.create(
        org_id="org-1",
        state="active-state",
        credential_name="primary",
        code_verifier="active-verifier",
        enabled_datasets=["services"],
        region="eu",
        ttl=timedelta(minutes=1),
        now=_NOW,
    )

    purged = await store.purge_expired(now=_NOW)

    active_request = await store.consume(org_id="org-1", state="active-state", now=_NOW)
    assert purged == 0
    assert active_request is not None
    assert active_request.code_verifier == "active-verifier"


@pytest.mark.anyio
async def test_create_purges_expired_requests_for_same_organization(
    session: AsyncSession,
) -> None:
    store = PagerDutyAuthorizationRequestStore(session)
    await store.create(
        org_id="org-1",
        state="expired",
        credential_name="primary",
        code_verifier="old",
        enabled_datasets=[],
        region="us",
        ttl=timedelta(seconds=0),
        now=_NOW,
    )
    await store.create(
        org_id="org-1",
        state="active",
        credential_name="primary",
        code_verifier="new",
        enabled_datasets=[],
        region="us",
        now=_NOW + timedelta(seconds=1),
    )

    rows = list(
        (await session.execute(select(PagerDutyOAuthAuthorizationRequest))).scalars()
    )

    assert len(rows) == 1
    assert rows[0].state_hash == hashlib.sha256(b"active").hexdigest()
