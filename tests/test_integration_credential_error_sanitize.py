"""Write-path + scrub coverage for ``integration_credentials.last_test_error``
(CHAOS-2780, codex HIGH).

``last_test_error`` stores errors from testing the credential itself -- the
most likely text to embed the secret -- and is republished verbatim via
admin API responses (``api/admin/routers/credentials.py``) and sync-preflight
HTTP details (``api/admin/routers/sync.py``). CHAOS-2766 sanitized every
other legacy error-text sink; this column was moved in scope for CHAOS-2780
with both a write-path fix (the service setter) and inclusion in the
``scrub-error-text`` column registry for rows persisted before the fix.

Covers:
  * ``IntegrationCredentialsService.update_test_result`` sanitizes a
    secret-bearing error before it reaches the DB.
  * The admin GET serializer (``_integration_credential_response``) serves
    whatever is stored verbatim -- once storage is sanitized, the response
    is sanitized by construction; no separate response-layer redaction is
    needed.
  * ``scrub-error-text`` redacts a pre-existing (legacy, unsanitized) row on
    this column, seeded here to simulate data written before the write-path
    fix shipped.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from dev_health_ops.api.admin.routers.credentials import (
    _integration_credential_response,
)
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.maintenance.scrub_error_text import run_scrub_error_text
from dev_health_ops.models import Base, IntegrationCredential
from dev_health_ops.sync.error_sanitize import REDACTION_MARKER
from tests._helpers import tables_of


def _fake_secret(*parts: str) -> str:
    return "".join(parts)


_LEAK = _fake_secret("ghp_", "FAKEintegcred1234567890AB")


def _fetch(session: Session, model: Any, row_id: uuid.UUID) -> Any:
    """``session.get`` typed loosely on purpose: this test file only needs a
    non-None row back, and the mapped attributes read off it downstream are
    all nullable columns compared/redaction-checked in ways mypy can't
    narrow through a ``Model | None`` return without per-call assertions."""
    row = session.get(model, row_id)
    assert row is not None
    return row


@pytest_asyncio.fixture
async def async_session_maker(tmp_path: Path):
    db_path = tmp_path / "cred-sanitize.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn, tables=tables_of(IntegrationCredential)
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_update_test_result_sanitizes_secret_bearing_error(async_session_maker):
    # Credentials are seeded directly at the ORM layer -- no encryption
    # round-trip -- since this test exercises only ``last_test_error``
    # sanitization, not the encrypted-blob path.
    org_id = f"org-{uuid.uuid4().hex[:8]}"
    async with async_session_maker() as session:
        session.add(
            IntegrationCredential(provider="github", name="default", org_id=org_id)
        )
        await session.commit()

        svc = IntegrationCredentialsService(session, org_id)
        raw_error = f"403 rate limited -- Authorization: Bearer {_LEAK}"
        await svc.update_test_result(
            "github", success=False, error=raw_error, name="default"
        )
        await session.commit()

        cred = await svc.get("github", "default")
        assert cred is not None
        assert cred.last_test_error is not None
        assert REDACTION_MARKER in cred.last_test_error
        assert _LEAK not in cred.last_test_error
        assert "Bearer" not in cred.last_test_error


@pytest.mark.asyncio
async def test_update_test_result_clears_error_on_success(async_session_maker):
    org_id = f"org-{uuid.uuid4().hex[:8]}"
    async with async_session_maker() as session:
        session.add(
            IntegrationCredential(provider="github", name="default", org_id=org_id)
        )
        await session.commit()

        svc = IntegrationCredentialsService(session, org_id)
        await svc.update_test_result("github", success=True, error=None, name="default")
        await session.commit()

        cred = await svc.get("github", "default")
        assert cred is not None
        assert cred.last_test_error is None


def test_admin_serializer_returns_stored_value_verbatim():
    """The admin GET response is a pass-through of whatever is persisted --
    demonstrating that sanitizing storage (the write-path fix above, or the
    scrub below) is sufficient; the serializer needs no separate redaction."""
    already_sanitized = "RateLimitException: 403 rate limited -- [REDACTED]"
    now = datetime.now(timezone.utc)

    class _FakeCredential:
        id = uuid.uuid4()
        provider = "github"
        name = "default"
        is_active = True
        config: dict = {}
        last_test_at = now
        last_test_success = False
        last_test_error = already_sanitized
        created_at = now
        updated_at = now

    response = _integration_credential_response(_FakeCredential())
    assert response.last_test_error == already_sanitized
    assert REDACTION_MARKER in response.last_test_error


def test_scrub_error_text_redacts_preexisting_last_test_error_row(tmp_path):
    """Simulates a row written before the CHAOS-2780 write-path fix shipped:
    seeded directly at the ORM layer (bypassing the now-sanitizing service
    setter), it must still be picked up and redacted by the scrub."""
    db_path = tmp_path / "cred-scrub.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    org_id = f"org-{uuid.uuid4().hex[:8]}"
    raw_error = f"push rejected using {_LEAK}"
    with Session(engine, expire_on_commit=False) as session:
        cred = IntegrationCredential(provider="github", name="default", org_id=org_id)
        cred.last_test_error = raw_error
        session.add(cred)
        session.commit()
        cred_id = cred.id
    engine.dispose()

    db_uri = f"sqlite:///{db_path}"
    rc = run_scrub_error_text(
        argparse.Namespace(db=db_uri, apply=True, org=None, batch_size=1000)
    )
    assert rc == 0

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        fetched = _fetch(session, IntegrationCredential, cred_id)
        assert REDACTION_MARKER in fetched.last_test_error
        assert _LEAK not in fetched.last_test_error
    engine.dispose()
