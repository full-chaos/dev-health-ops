"""Unit tests for the CHAOS-2695 idempotency policy (brief §9).

Canonicalization invariance is pure (no DB); the 4-way outcome matrix runs
against the REAL direct-SQL store (aiosqlite) so the unique-constraint /
SAVEPOINT race handling is exercised for real, per house convention
(tests/test_external_ingest_status_api.py). Live-Postgres divergence is
covered by the changeset's §11 live verification, not here.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest import idempotency as idem
from dev_health_ops.api.external_ingest import status as status_mod
from dev_health_ops.api.external_ingest.idempotency import (
    IdempotencyOutcomeKind,
    IngestTemporarilyUnavailableError,
    compute_payload_hash,
    resolve_batch_idempotency,
)
from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION, BatchEnvelope
from dev_health_ops.models.external_ingest import (
    ExternalIngestBatch,
    ExternalIngestRejection,
)
from dev_health_ops.models.git import Base
from tests._helpers import tables_of

_TABLES = tables_of(ExternalIngestBatch, ExternalIngestRejection)

ORG = "org-1"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'external-ingest-idempotency.db'}"
    )
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Canonicalization (brief decisions 1-3)
# ---------------------------------------------------------------------------


def _envelope_dict(**overrides) -> dict:
    base = {
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": "key-1",
        "source": {
            "type": "customer_push",
            "system": "github",
            "instance": "acme/api",
        },
        "window": {
            "startedAt": "2026-06-25T00:00:00Z",
            "endedAt": "2026-06-26T00:00:00Z",
        },
        "records": [
            {
                "kind": "commit.v1",
                "externalId": "abc1234567",
                "payload": {
                    "repositoryExternalId": "acme/api",
                    "hash": "abc1234567",
                    "authorWhen": "2026-06-25T12:00:00Z",
                },
            }
        ],
    }
    base.update(overrides)
    return base


def test_canonicalization_field_order_invariant():
    ordered = BatchEnvelope.model_validate(_envelope_dict())
    reordered_raw = dict(reversed(list(_envelope_dict().items())))
    reordered_raw["records"] = [
        dict(reversed(list(record.items()))) for record in reordered_raw["records"]
    ]
    reordered = BatchEnvelope.model_validate(reordered_raw)

    assert compute_payload_hash(ordered) == compute_payload_hash(reordered)


def test_canonicalization_whitespace_invariant():
    import json as json_mod

    minified = json_mod.dumps(_envelope_dict(), separators=(",", ":"))
    pretty = json_mod.dumps(_envelope_dict(), indent=4)

    assert compute_payload_hash(
        BatchEnvelope.model_validate_json(minified)
    ) == compute_payload_hash(BatchEnvelope.model_validate_json(pretty))


def test_canonicalization_timestamp_format_invariant():
    zulu = BatchEnvelope.model_validate(_envelope_dict())
    offset_dict = _envelope_dict()
    offset_dict["window"] = {
        "startedAt": "2026-06-25T00:00:00+00:00",
        "endedAt": "2026-06-26T00:00:00+00:00",
    }
    offset = BatchEnvelope.model_validate(offset_dict)

    assert compute_payload_hash(zulu) == compute_payload_hash(offset)


def test_legacy_payload_hash_matches_the_pre_entity_family_wire_shape():
    legacy_payload = _envelope_dict()
    envelope = BatchEnvelope.model_validate(legacy_payload)
    import hashlib
    import json as json_mod

    pre_deploy_payload = envelope.model_dump(mode="json")
    pre_deploy_payload["source"].pop("entity_family")
    expected = hashlib.sha256(
        json_mod.dumps(
            pre_deploy_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()

    assert compute_payload_hash(envelope) == expected


def test_record_order_is_position_significant():
    """Brief decision 3: records are NOT sorted before hashing -- swapping
    two records is a different payload (CONFLICT), by design."""
    two = _envelope_dict()
    second = {
        "kind": "commit.v1",
        "externalId": "def8901234",
        "payload": {
            "repositoryExternalId": "acme/api",
            "hash": "def8901234",
            "authorWhen": "2026-06-25T13:00:00Z",
        },
    }
    two["records"] = [two["records"][0], second]
    swapped = _envelope_dict()
    swapped["records"] = [second, dict(_envelope_dict()["records"][0])]

    assert compute_payload_hash(
        BatchEnvelope.model_validate(two)
    ) != compute_payload_hash(BatchEnvelope.model_validate(swapped))


# ---------------------------------------------------------------------------
# Outcome matrix (brief decision 7 + post-critique CC13 stale-accepted)
# ---------------------------------------------------------------------------


async def _resolve(
    session,
    *,
    idempotency_key: str = "key-1",
    payload_hash: str = "h" * 64,
    source_instance: str = "acme/api",
    items_received: int = 3,
):
    return await resolve_batch_idempotency(
        session,
        org_id=ORG,
        source_system="github",
        source_instance=source_instance,
        idempotency_key=idempotency_key,
        payload_hash=payload_hash,
        schema_version=SCHEMA_VERSION,
        producer="pytest",
        producer_version="1.0",
        window_started_at=None,
        window_ended_at=None,
        items_received=items_received,
    )


async def _set_status(
    session_maker, ingestion_id: uuid.UUID, status: str, *, updated_at=None
) -> None:
    async with session_maker() as session:
        params = {"status": status, "ingestion_id": str(ingestion_id)}
        set_clause = "status = :status"
        if updated_at is not None:
            set_clause += ", updated_at = :updated_at"
            params["updated_at"] = updated_at
        await session.execute(
            text(
                "UPDATE external_ingest_batches SET "
                + set_clause
                + " WHERE ingestion_id = :ingestion_id"
            ),
            params,
        )
        await session.commit()


@pytest.mark.asyncio
async def test_new_batch_returns_new_outcome(session_maker):
    async with session_maker() as session:
        outcome = await _resolve(session)
        await session.commit()

    assert outcome.kind is IdempotencyOutcomeKind.NEW
    assert outcome.batch.status == "accepted"
    assert outcome.batch.attempts == 1


@pytest.mark.asyncio
async def test_replay_same_key_same_hash_returns_existing_row(session_maker):
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    async with session_maker() as session:
        second = await _resolve(session)

    assert second.kind is IdempotencyOutcomeKind.REPLAY
    assert second.batch.ingestion_id == first.batch.ingestion_id


@pytest.mark.asyncio
async def test_conflict_same_key_different_hash(session_maker):
    async with session_maker() as session:
        await _resolve(session)
        await session.commit()
    async with session_maker() as session:
        outcome = await _resolve(session, payload_hash="x" * 64)

    assert outcome.kind is IdempotencyOutcomeKind.CONFLICT


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable_status", ["stream_unavailable", "failed"])
async def test_retry_outcome_for_retryable_statuses(session_maker, retryable_status):
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    await _set_status(session_maker, first.batch.ingestion_id, retryable_status)

    async with session_maker() as session:
        outcome = await _resolve(session)

    assert outcome.kind is IdempotencyOutcomeKind.RETRY
    assert outcome.batch.ingestion_id == first.batch.ingestion_id


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["completed", "partial"])
async def test_replay_not_retry_for_terminal_statuses(session_maker, terminal_status):
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    await _set_status(session_maker, first.batch.ingestion_id, terminal_status)

    async with session_maker() as session:
        outcome = await _resolve(session)

    assert outcome.kind is IdempotencyOutcomeKind.REPLAY


@pytest.mark.asyncio
async def test_stale_accepted_becomes_retry(session_maker):
    """Post-critique CC13: an accepted row older than the stale window is
    presumed lost (crash-before-XADD / stream trim -- no worker can ever see
    it) and RETRY -- otherwise `accepted` would be permanently
    unrecoverable."""
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    stale = datetime.now(timezone.utc) - timedelta(minutes=16)
    await _set_status(
        session_maker, first.batch.ingestion_id, "accepted", updated_at=stale
    )

    async with session_maker() as session:
        outcome = await _resolve(session)

    assert outcome.kind is IdempotencyOutcomeKind.RETRY


@pytest.mark.asyncio
async def test_stale_processing_stays_replay(session_maker):
    """Adversarial-review finding: a stale `processing` row must NOT be
    client-retryable -- a worker provably holds the pointer, and an unfenced
    retry would race a slow-but-alive worker's terminal CAS (double-apply).
    Recovery for dead workers is the stream reclaim path / `failed` status,
    both outside this policy."""
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    stale = datetime.now(timezone.utc) - timedelta(minutes=120)
    await _set_status(
        session_maker, first.batch.ingestion_id, "processing", updated_at=stale
    )

    async with session_maker() as session:
        outcome = await _resolve(session)

    assert outcome.kind is IdempotencyOutcomeKind.REPLAY


@pytest.mark.asyncio
async def test_young_accepted_stays_replay(session_maker):
    async with session_maker() as session:
        await _resolve(session)
        await session.commit()

    async with session_maker() as session:
        outcome = await _resolve(session)

    assert outcome.kind is IdempotencyOutcomeKind.REPLAY


@pytest.mark.asyncio
async def test_stale_window_env_override(session_maker, monkeypatch):
    monkeypatch.setenv("EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES", "1")
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    barely_old = datetime.now(timezone.utc) - timedelta(minutes=2)
    await _set_status(
        session_maker, first.batch.ingestion_id, "accepted", updated_at=barely_old
    )

    async with session_maker() as session:
        outcome = await _resolve(session)

    assert outcome.kind is IdempotencyOutcomeKind.RETRY


@pytest.mark.asyncio
async def test_conflict_wins_over_retryable_status(session_maker):
    """Hash mismatch dominates: a different payload against a failed row is
    still a 409 CONFLICT, never a silent overwrite via RETRY."""
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    await _set_status(session_maker, first.batch.ingestion_id, "failed")

    async with session_maker() as session:
        outcome = await _resolve(session, payload_hash="x" * 64)

    assert outcome.kind is IdempotencyOutcomeKind.CONFLICT


@pytest.mark.asyncio
async def test_unique_constraint_scoped_to_org_system_instance(session_maker):
    """Same key string across two source instances = two independent NEW
    rows (brief §2: no cross-source namespacing)."""
    async with session_maker() as session:
        first = await _resolve(session, source_instance="acme/api")
        await session.commit()
    async with session_maker() as session:
        second = await _resolve(session, source_instance="acme/other")
        await session.commit()

    assert first.kind is IdempotencyOutcomeKind.NEW
    assert second.kind is IdempotencyOutcomeKind.NEW
    assert first.batch.ingestion_id != second.batch.ingestion_id


@pytest.mark.asyncio
async def test_lost_insert_race_reclassifies_winner_row(session_maker, monkeypatch):
    """DuplicateIdempotencyKeyError -> re-read the winner's row and apply the
    same hash policy (here: same hash, accepted -> REPLAY)."""
    async with session_maker() as session:
        winner = await _resolve(session)
        await session.commit()

    finds = {"n": 0}
    real_find = idem.find_existing_batch

    async def _find_miss_then_hit(session, **kwargs):
        finds["n"] += 1
        if finds["n"] == 1:
            return None  # pre-check misses; the INSERT then collides
        return await real_find(session, **kwargs)

    monkeypatch.setattr(idem, "find_existing_batch", _find_miss_then_hit)

    async with session_maker() as session:
        outcome = await _resolve(session)

    assert outcome.kind is IdempotencyOutcomeKind.REPLAY
    assert outcome.batch.ingestion_id == winner.batch.ingestion_id


@pytest.mark.asyncio
async def test_true_race_raises_temporarily_unavailable(session_maker, monkeypatch):
    """The winner's row is not visible on the post-conflict re-read (still
    uncommitted) -> IngestTemporarilyUnavailableError (503)."""

    async def _find_none(session, **kwargs):
        return None

    async def _create_collides(session, **kwargs):
        raise status_mod.DuplicateIdempotencyKeyError(
            org_id=ORG,
            source_system="github",
            source_instance="acme/api",
            idempotency_key="key-1",
        )

    monkeypatch.setattr(idem, "find_existing_batch", _find_none)
    monkeypatch.setattr(idem, "create_batch", _create_collides)

    async with session_maker() as session:
        with pytest.raises(IngestTemporarilyUnavailableError):
            await _resolve(session)


# ---------------------------------------------------------------------------
# reset_for_retry (store-level half of the RETRY contract)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reset_for_retry_clears_failed_attempt_and_rejections(session_maker):
    async with session_maker() as session:
        first = await _resolve(session, items_received=2)
        await session.commit()
    ingestion_id = first.batch.ingestion_id

    rejections = [
        status_mod.RejectedRecord(
            record_index=i,
            record_kind="commit.v1",
            external_id=f"c{i}",
            code="invalid_field",
            message="bad",
            path=None,
        )
        for i in range(2)
    ]
    async with session_maker() as session:
        await status_mod.mark_processing(session, org_id=ORG, ingestion_id=ingestion_id)
        failed = await status_mod.complete_batch(
            session,
            org_id=ORG,
            ingestion_id=ingestion_id,
            items_accepted=0,
            items_rejected=2,
            rejections=rejections,
        )
        await session.commit()
    assert failed.status == "failed"

    async with session_maker() as session:
        won = await status_mod.reset_for_retry(
            session, org_id=ORG, ingestion_id=ingestion_id, from_status="failed"
        )
        await session.commit()
    assert won is True

    async with session_maker() as session:
        row = await status_mod.get_batch(session, org_id=ORG, ingestion_id=ingestion_id)
        assert row is not None
        assert row.status == "accepted"
        assert row.attempts == 2
        assert row.items_accepted == 0
        assert row.items_rejected == 0
        assert row.error_summary is None
        assert row.completed_at is None
        stored, total = await status_mod.list_rejections(
            session, org_id=ORG, ingestion_id=ingestion_id
        )
        assert stored == []
        assert total == 0

        # The retry attempt can complete again without tripping the
        # (ingestion_id, record_index) unique index on rejection rows.
        await status_mod.mark_processing(session, org_id=ORG, ingestion_id=ingestion_id)
        done = await status_mod.complete_batch(
            session,
            org_id=ORG,
            ingestion_id=ingestion_id,
            items_accepted=1,
            items_rejected=1,
            rejections=rejections[:1],
        )
        await session.commit()
    assert done.status == "partial"
    assert done.attempts == 2


@pytest.mark.asyncio
async def test_reset_for_retry_cas_loses_on_status_move(session_maker):
    async with session_maker() as session:
        first = await _resolve(session)
        await session.commit()
    await _set_status(session_maker, first.batch.ingestion_id, "completed")

    async with session_maker() as session:
        won = await status_mod.reset_for_retry(
            session,
            org_id=ORG,
            ingestion_id=first.batch.ingestion_id,
            from_status="stream_unavailable",
        )
    assert won is False
