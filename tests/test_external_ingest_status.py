"""Unit tests for the external-ingest status store (CHAOS-2694).

sqlite-in-memory (aiosqlite), no live Postgres -- mirrors the
tests/test_rate_limit_observations.py / tests/api/admin/test_customer_push.py
convention. Exercises status.py's CRUD/state-machine functions directly
(never via HTTP -- see tests/test_external_ingest_status_api.py for the GET
endpoint layer).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest import status
from dev_health_ops.models.external_ingest import (
    MAX_STORED_REJECTIONS_PER_BATCH,
    BatchStatus,
    ExternalIngestBatch,
    ExternalIngestRejection,
    terminal_status_for,
)
from dev_health_ops.models.git import Base
from tests._helpers import tables_of

_TABLES = tables_of(ExternalIngestBatch, ExternalIngestRejection)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "external-ingest-status.db"
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
async def session(session_maker):
    async with session_maker() as s:
        yield s


ORG_A = "org-a"
ORG_B = "org-b"
SYSTEM = "github"
INSTANCE = "acme/api"


async def _create(
    session: AsyncSession,
    *,
    org_id: str = ORG_A,
    idempotency_key: str = "key-1",
    payload_hash: str = "hash-1",
    source_system: str = SYSTEM,
    source_instance: str = INSTANCE,
    items_received: int = 3,
    ingestion_id: uuid.UUID | None = None,
) -> status.BatchRow:
    return await status.create_batch(
        session,
        ingestion_id=ingestion_id or uuid.uuid4(),
        org_id=org_id,
        idempotency_key=idempotency_key,
        payload_hash=payload_hash,
        source_system=source_system,
        source_instance=source_instance,
        producer="dev-hops-cli",
        producer_version="0.1.0",
        schema_version="external-ingest.v1",
        window_started_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        window_ended_at=datetime(2026, 6, 2, tzinfo=timezone.utc),
        items_received=items_received,
    )


# ---------------------------------------------------------------------------
# create_batch / find_existing_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_batch_inserts_accepted_row(session):
    batch = await _create(session, payload_hash="deadbeef")
    await session.commit()

    assert batch.status == BatchStatus.ACCEPTED.value
    assert batch.items_received == 3
    assert batch.items_accepted == 0
    assert batch.items_rejected == 0
    assert batch.payload_hash == "deadbeef"
    assert batch.attempts == 1


@pytest.mark.asyncio
async def test_find_existing_batch_none_for_fresh_key(session):
    result = await status.find_existing_batch(
        session,
        org_id=ORG_A,
        source_system=SYSTEM,
        source_instance=INSTANCE,
        idempotency_key="never-seen",
    )
    assert result is None


@pytest.mark.asyncio
async def test_find_existing_batch_returns_known_key(session):
    created = await _create(session, idempotency_key="key-known")
    await session.commit()

    found = await status.find_existing_batch(
        session,
        org_id=ORG_A,
        source_system=SYSTEM,
        source_instance=INSTANCE,
        idempotency_key="key-known",
    )
    assert found is not None
    assert found.ingestion_id == created.ingestion_id


@pytest.mark.asyncio
async def test_create_batch_duplicate_idempotency_key_raises(session):
    await _create(session, idempotency_key="dupe-key")
    await session.commit()

    with pytest.raises(status.DuplicateIdempotencyKeyError):
        await _create(session, idempotency_key="dupe-key")


# ---------------------------------------------------------------------------
# mark_processing / mark_stream_unavailable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_processing_transitions_accepted_to_processing(session):
    batch = await _create(session)
    await session.commit()

    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)
    await session.commit()

    updated = await status.get_batch(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id
    )
    assert updated is not None
    assert updated.status == BatchStatus.PROCESSING.value


@pytest.mark.asyncio
async def test_mark_processing_is_noop_when_already_processing(session):
    batch = await _create(session)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)
    await session.commit()

    # Second call: WHERE status='accepted' matches nothing -- no exception,
    # no regression of an already-processing (or terminal) status.
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)
    await session.commit()

    updated = await status.get_batch(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id
    )
    assert updated is not None
    assert updated.status == BatchStatus.PROCESSING.value


@pytest.mark.asyncio
async def test_mark_stream_unavailable_transitions_accepted(session):
    batch = await _create(session)
    await session.commit()

    await status.mark_stream_unavailable(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id
    )
    await session.commit()

    updated = await status.get_batch(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id
    )
    assert updated is not None
    assert updated.status == BatchStatus.STREAM_UNAVAILABLE.value


# ---------------------------------------------------------------------------
# complete_batch / terminal-status derivation
# ---------------------------------------------------------------------------


def test_terminal_status_for_boundary_table():
    assert terminal_status_for(3, 3, 0) == BatchStatus.COMPLETED
    assert terminal_status_for(3, 0, 3) == BatchStatus.FAILED
    assert terminal_status_for(3, 1, 2) == BatchStatus.PARTIAL
    with pytest.raises(AssertionError):
        terminal_status_for(0, 0, 0)


@pytest.mark.asyncio
async def test_complete_batch_all_accepted_is_completed(session):
    batch = await _create(session, items_received=2)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    result = await status.complete_batch(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        items_accepted=2,
        items_rejected=0,
        rejections=[],
    )
    await session.commit()

    assert result.status == BatchStatus.COMPLETED.value
    assert result.completed_at is not None
    assert result.error_summary is None


@pytest.mark.asyncio
async def test_complete_batch_zero_accepted_is_failed(session):
    batch = await _create(session, items_received=2)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    rejections = [
        status.RejectedRecord(
            0, "commit.v1", "c1", "missing_required_field", "boom", "hash"
        ),
        status.RejectedRecord(
            1, "commit.v1", "c2", "missing_required_field", "boom", "hash"
        ),
    ]
    result = await status.complete_batch(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        items_accepted=0,
        items_rejected=2,
        rejections=rejections,
    )
    await session.commit()

    assert result.status == BatchStatus.FAILED.value


@pytest.mark.asyncio
async def test_complete_batch_mixed_is_partial(session):
    batch = await _create(session, items_received=3)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    rejections = [
        status.RejectedRecord(
            1, "commit.v1", "c2", "missing_required_field", "boom", "hash"
        ),
    ]
    result = await status.complete_batch(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        items_accepted=2,
        items_rejected=1,
        rejections=rejections,
    )
    await session.commit()

    assert result.status == BatchStatus.PARTIAL.value
    assert result.error_summary is not None
    assert result.error_summary["total_rejected"] == 1
    assert result.error_summary["stored_rejections"] == 1
    assert result.error_summary["truncated"] is False
    assert result.error_summary["top_codes"] == [
        {"code": "missing_required_field", "count": 1}
    ]


@pytest.mark.asyncio
async def test_complete_batch_truncates_stored_rejections_at_cap(session):
    batch = await _create(session, items_received=MAX_STORED_REJECTIONS_PER_BATCH + 5)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    total_rejected = MAX_STORED_REJECTIONS_PER_BATCH + 5
    rejections = [
        status.RejectedRecord(
            i, "commit.v1", f"c{i}", "missing_required_field", "boom", None
        )
        for i in range(total_rejected)
    ]
    result = await status.complete_batch(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        items_accepted=0,
        items_rejected=total_rejected,
        rejections=rejections,
    )
    await session.commit()

    assert result.error_summary is not None
    assert result.error_summary["total_rejected"] == total_rejected
    assert result.error_summary["stored_rejections"] == MAX_STORED_REJECTIONS_PER_BATCH
    assert result.error_summary["truncated"] is True

    stored_rows, stored_total = await status.list_rejections(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        limit=MAX_STORED_REJECTIONS_PER_BATCH,
        offset=0,
    )
    assert stored_total == MAX_STORED_REJECTIONS_PER_BATCH
    assert len(stored_rows) == MAX_STORED_REJECTIONS_PER_BATCH
    # items_rejected on the batch row always reflects the TRUE total.
    assert result.items_rejected == total_rejected


@pytest.mark.asyncio
async def test_complete_batch_idempotent_under_redelivery(session):
    batch = await _create(session, items_received=3)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    rejections = [
        status.RejectedRecord(
            1, "commit.v1", "c2", "missing_required_field", "boom", "hash"
        ),
    ]
    first = await status.complete_batch(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        items_accepted=2,
        items_rejected=1,
        rejections=rejections,
    )
    await session.commit()

    second = await status.complete_batch(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        items_accepted=2,
        items_rejected=1,
        rejections=rejections,
    )
    await session.commit()

    assert second.status == first.status == BatchStatus.PARTIAL.value
    assert second.items_accepted == 2
    assert second.items_rejected == 1

    _, total_stored = await status.list_rejections(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id, limit=10, offset=0
    )
    # No duplicate rows from the redelivered call.
    assert total_stored == 1


@pytest.mark.asyncio
async def test_complete_batch_missing_batch_raises(session):
    with pytest.raises(ValueError):
        await status.complete_batch(
            session,
            org_id=ORG_A,
            ingestion_id=uuid.uuid4(),
            items_accepted=1,
            items_rejected=0,
            rejections=[],
        )


# ---------------------------------------------------------------------------
# complete_batch state/counter invariants (adversarial-review findings)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_complete_batch_rejects_accepted_batch(session):
    # Never processed (no mark_processing call) -- must not be completable
    # directly from 'accepted'.
    batch = await _create(session, items_received=2)
    await session.commit()

    with pytest.raises(ValueError):
        await status.complete_batch(
            session,
            org_id=ORG_A,
            ingestion_id=batch.ingestion_id,
            items_accepted=2,
            items_rejected=0,
            rejections=[],
        )


@pytest.mark.asyncio
async def test_complete_batch_rejects_stream_unavailable_batch(session):
    batch = await _create(session, items_received=2)
    await session.commit()
    await status.mark_stream_unavailable(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id
    )
    await session.commit()

    with pytest.raises(ValueError):
        await status.complete_batch(
            session,
            org_id=ORG_A,
            ingestion_id=batch.ingestion_id,
            items_accepted=2,
            items_rejected=0,
            rejections=[],
        )


@pytest.mark.asyncio
async def test_complete_batch_rejects_negative_counters(session):
    batch = await _create(session, items_received=2)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    with pytest.raises(ValueError):
        await status.complete_batch(
            session,
            org_id=ORG_A,
            ingestion_id=batch.ingestion_id,
            items_accepted=-1,
            items_rejected=3,
            rejections=[],
        )


@pytest.mark.asyncio
async def test_complete_batch_rejects_counter_sum_mismatch(session):
    batch = await _create(session, items_received=2)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    with pytest.raises(ValueError):
        await status.complete_batch(
            session,
            org_id=ORG_A,
            ingestion_id=batch.ingestion_id,
            items_accepted=2,
            items_rejected=2,  # sums to 4, but items_received is 2
            rejections=[],
        )


@pytest.mark.asyncio
async def test_complete_batch_rejects_zero_and_zero_when_received_nonzero(session):
    batch = await _create(session, items_received=2)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    with pytest.raises(ValueError):
        await status.complete_batch(
            session,
            org_id=ORG_A,
            ingestion_id=batch.ingestion_id,
            items_accepted=0,
            items_rejected=0,
            rejections=[],
        )


@pytest.mark.asyncio
async def test_complete_batch_cas_loses_race_to_concurrent_completion(session):
    """Adversarial-review regression: simulates a second worker completing
    the same batch between this call's initial read and its CAS UPDATE (the
    exact window a check-then-write implementation would race on). The
    losing call must not persist its own outcome or insert its own rejection
    rows -- it must return whatever the concurrent winner actually wrote."""
    batch = await _create(session, items_received=2)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)
    await session.commit()

    real_execute = session.execute
    raced = {"done": False}

    async def racing_execute(clause, params=None, *args, **kwargs):
        sql_text = getattr(clause, "text", "")
        if not raced["done"] and "SET status = :status" in sql_text:
            raced["done"] = True
            # A concurrent worker wins the race first, with a DIFFERENT
            # outcome than the call under test is about to attempt.
            await real_execute(
                text(
                    "UPDATE external_ingest_batches SET status = 'completed', "
                    "items_accepted = 2, items_rejected = 0, updated_at = updated_at "
                    "WHERE ingestion_id = :id"
                ),
                {"id": str(batch.ingestion_id)},
            )
        if params is None:
            return await real_execute(clause, *args, **kwargs)
        return await real_execute(clause, params, *args, **kwargs)

    session.execute = racing_execute
    try:
        result = await status.complete_batch(
            session,
            org_id=ORG_A,
            ingestion_id=batch.ingestion_id,
            items_accepted=1,
            items_rejected=1,
            rejections=[
                status.RejectedRecord(1, "commit.v1", "c1", "code", "msg", None)
            ],
        )
    finally:
        session.execute = real_execute

    # The losing call's own outcome must NOT win -- the concurrently
    # committed state (from the "other worker") is what's returned.
    assert result.status == "completed"
    assert result.items_accepted == 2
    assert result.items_rejected == 0

    rows, total = await status.list_rejections(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id, limit=10, offset=0
    )
    assert total == 0


# ---------------------------------------------------------------------------
# get_batch tenant isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_batch_cross_org_returns_none(session):
    batch = await _create(session, org_id=ORG_A)
    await session.commit()

    result = await status.get_batch(
        session, org_id=ORG_B, ingestion_id=batch.ingestion_id
    )
    assert result is None

    nonexistent = await status.get_batch(
        session, org_id=ORG_A, ingestion_id=uuid.uuid4()
    )
    assert nonexistent is None


# ---------------------------------------------------------------------------
# list_batches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_batches_filters_and_orders(session):
    b1 = await _create(session, idempotency_key="k1", source_system="github")
    await session.commit()
    b2 = await _create(session, idempotency_key="k2", source_system="gitlab")
    await session.commit()
    b3 = await _create(session, idempotency_key="k3", source_system="github")
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=b3.ingestion_id)
    await session.commit()

    github_rows, github_total = await status.list_batches(
        session, org_id=ORG_A, source_system="github", limit=50, offset=0
    )
    assert github_total == 2
    assert {r.ingestion_id for r in github_rows} == {b1.ingestion_id, b3.ingestion_id}
    # created_at DESC -- b3 created after b1.
    assert github_rows[0].ingestion_id == b3.ingestion_id

    processing_rows, processing_total = await status.list_batches(
        session, org_id=ORG_A, status="processing", limit=50, offset=0
    )
    assert processing_total == 1
    assert processing_rows[0].ingestion_id == b3.ingestion_id

    gitlab_rows, gitlab_total = await status.list_batches(
        session,
        org_id=ORG_A,
        source_instance=INSTANCE,
        source_system="gitlab",
        limit=50,
        offset=0,
    )
    assert gitlab_total == 1
    assert gitlab_rows[0].ingestion_id == b2.ingestion_id


@pytest.mark.asyncio
async def test_list_batches_respects_limit_offset(session):
    for i in range(5):
        await _create(session, idempotency_key=f"page-key-{i}")
        await session.commit()

    page1, total = await status.list_batches(session, org_id=ORG_A, limit=2, offset=0)
    page2, _ = await status.list_batches(session, org_id=ORG_A, limit=2, offset=2)

    assert total == 5
    assert len(page1) == 2
    assert len(page2) == 2
    assert {r.ingestion_id for r in page1}.isdisjoint({r.ingestion_id for r in page2})


@pytest.mark.asyncio
async def test_list_batches_pagination_stable_across_tied_created_at(session):
    # Adversarial-review regression: created_at alone is not unique (e.g.
    # concurrent inserts within the same tick), so pagination must break
    # ties on a second, unique column (ingestion_id) to avoid duplicating or
    # skipping rows across page boundaries.
    tied_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
    ids = sorted(uuid.uuid4() for _ in range(4))
    for i, ingestion_id in enumerate(ids):
        session.add(
            ExternalIngestBatch(
                ingestion_id=ingestion_id,
                org_id=ORG_A,
                idempotency_key=f"tied-{i}",
                payload_hash="hash",
                source_system=SYSTEM,
                source_instance=INSTANCE,
                schema_version="external-ingest.v1",
                status=BatchStatus.ACCEPTED.value,
                items_received=1,
                created_at=tied_at,
                updated_at=tied_at,
            )
        )
    await session.commit()

    seen: list[uuid.UUID] = []
    for offset in range(0, 4, 2):
        page, total = await status.list_batches(
            session, org_id=ORG_A, limit=2, offset=offset
        )
        assert total == 4
        seen.extend(r.ingestion_id for r in page)

    # Every seeded id appears exactly once across all pages (no duplicates,
    # no gaps) and in descending-ingestion_id tiebreak order.
    assert seen == list(reversed(ids))


@pytest.mark.asyncio
async def test_list_batches_created_after_before(session):
    old = await _create(session, idempotency_key="old-key")
    await session.commit()
    new = await _create(session, idempotency_key="new-key")
    await session.commit()

    cutoff = old.created_at + timedelta(microseconds=1)
    rows, total = await status.list_batches(
        session, org_id=ORG_A, created_after=cutoff, limit=50, offset=0
    )
    assert total == 1
    assert rows[0].ingestion_id == new.ingestion_id

    rows, total = await status.list_batches(
        session, org_id=ORG_A, created_before=cutoff, limit=50, offset=0
    )
    assert total == 1
    assert rows[0].ingestion_id == old.ingestion_id


# ---------------------------------------------------------------------------
# list_rejections
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_rejections_orders_by_record_index_and_paginates(session):
    batch = await _create(session, items_received=3)
    await session.commit()
    await status.mark_processing(session, org_id=ORG_A, ingestion_id=batch.ingestion_id)

    rejections = [
        status.RejectedRecord(2, "commit.v1", "c2", "code-c", "msg", None),
        status.RejectedRecord(0, "commit.v1", "c0", "code-a", "msg", None),
        status.RejectedRecord(1, "commit.v1", "c1", "code-b", "msg", None),
    ]
    await status.complete_batch(
        session,
        org_id=ORG_A,
        ingestion_id=batch.ingestion_id,
        items_accepted=0,
        items_rejected=3,
        rejections=rejections,
    )
    await session.commit()

    all_rows, total = await status.list_rejections(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id, limit=50, offset=0
    )
    assert total == 3
    assert [r.record_index for r in all_rows] == [0, 1, 2]

    page, _ = await status.list_rejections(
        session, org_id=ORG_A, ingestion_id=batch.ingestion_id, limit=1, offset=1
    )
    assert len(page) == 1
    assert page[0].record_index == 1
