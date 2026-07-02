"""Tests for external_ingest.payload_store (CHAOS-2693 D2/CC22).

Follows the confirmed sqlite-in-memory convention (no live-Postgres pytest
marker exists in this codebase -- see tests/test_rate_limit_observations.py)
adapted for async: aiosqlite + AsyncSession, matching
tests/api/auth/test_invite_flow.py's fixture pattern (AGENTS.md explicitly
allows aiosqlite for test fixtures).
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.external_ingest import payload_store
from dev_health_ops.models.external_ingest import ExternalIngestBatchPayload
from dev_health_ops.models.git import Base
from tests._helpers import tables_of

_TABLES = tables_of(ExternalIngestBatchPayload)


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


def _uuid() -> uuid.UUID:
    return uuid.uuid4()


@pytest.mark.asyncio
async def test_insert_fetch_delete_round_trip(session: AsyncSession):
    ingestion_id = _uuid()
    payload = b'{"records": [1, 2, 3]}'

    await payload_store.upsert_payload(
        session,
        ingestion_id=ingestion_id,
        org_id="org-1",
        schema_version="external-ingest.v1",
        payload_bytes=payload,
    )
    await session.commit()

    fetched = await payload_store.fetch_payload(
        session, ingestion_id=ingestion_id, org_id="org-1"
    )
    assert fetched == payload

    await payload_store.delete_payload(session, ingestion_id=ingestion_id)
    await session.commit()

    assert (
        await payload_store.fetch_payload(
            session, ingestion_id=ingestion_id, org_id="org-1"
        )
        is None
    )


@pytest.mark.asyncio
async def test_fetch_with_mismatched_org_returns_none(session: AsyncSession):
    """Tenant isolation: org_id is in the predicate even though ingestion_id
    alone is already unique (house rule: org_id in every lookup predicate)."""
    ingestion_id = _uuid()
    await payload_store.upsert_payload(
        session,
        ingestion_id=ingestion_id,
        org_id="org-1",
        schema_version="external-ingest.v1",
        payload_bytes=b"payload",
    )
    await session.commit()

    assert (
        await payload_store.fetch_payload(
            session, ingestion_id=ingestion_id, org_id="org-2"
        )
        is None
    )


@pytest.mark.asyncio
async def test_upsert_updates_existing_row_in_place(session: AsyncSession):
    """CC22: a RETRY accept reuses the same ingestion_id -- upsert_payload
    must UPDATE the existing row (stream_unavailable case: row exists,
    worker never ran), not attempt a colliding INSERT."""
    ingestion_id = _uuid()
    await payload_store.upsert_payload(
        session,
        ingestion_id=ingestion_id,
        org_id="org-1",
        schema_version="external-ingest.v1",
        payload_bytes=b"first-attempt",
    )
    await session.commit()

    await payload_store.upsert_payload(
        session,
        ingestion_id=ingestion_id,
        org_id="org-1",
        schema_version="external-ingest.v1",
        payload_bytes=b"second-attempt-after-retry",
    )
    await session.commit()

    fetched = await payload_store.fetch_payload(
        session, ingestion_id=ingestion_id, org_id="org-1"
    )
    assert fetched == b"second-attempt-after-retry"


@pytest.mark.asyncio
async def test_upsert_recreates_row_after_delete(session: AsyncSession):
    """CC22: the other RETRY case (worker already deleted the row on
    ``failed``) -- upsert_payload must INSERT again for the same
    ingestion_id, not error."""
    ingestion_id = _uuid()
    await payload_store.upsert_payload(
        session,
        ingestion_id=ingestion_id,
        org_id="org-1",
        schema_version="external-ingest.v1",
        payload_bytes=b"attempt-1",
    )
    await session.commit()
    await payload_store.delete_payload(session, ingestion_id=ingestion_id)
    await session.commit()

    await payload_store.upsert_payload(
        session,
        ingestion_id=ingestion_id,
        org_id="org-1",
        schema_version="external-ingest.v1",
        payload_bytes=b"attempt-2-after-failed-retry",
    )
    await session.commit()

    fetched = await payload_store.fetch_payload(
        session, ingestion_id=ingestion_id, org_id="org-1"
    )
    assert fetched == b"attempt-2-after-failed-retry"


@pytest.mark.asyncio
async def test_delete_missing_row_is_a_noop(session: AsyncSession):
    await payload_store.delete_payload(session, ingestion_id=_uuid())
    await session.commit()  # must not raise
