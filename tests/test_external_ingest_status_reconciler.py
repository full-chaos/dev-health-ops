"""Tests for the external-ingest status store retention prune task
(CHAOS-2694). Sync sqlite-in-memory, mirroring
tests/test_rate_limit_observations.py's prune-task harness (reuses its
``_patch_db_session``/``_fake_session_ctx`` helpers)."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from dev_health_ops.models.external_ingest import (
    BatchStatus,
    ExternalIngestBatch,
    ExternalIngestRejection,
)
from dev_health_ops.models.git import Base
from dev_health_ops.workers.external_ingest_reconciler import (
    _DELETE_CHUNK_SIZE,
    prune_external_ingest_batches,
)
from tests._helpers import tables_of
from tests.test_sync_units import _patch_db_session

_TABLES = tables_of(ExternalIngestBatch)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=_TABLES)
    with Session(engine) as session:
        yield session
    engine.dispose()


@pytest.fixture
def db_session_with_fk_enforcement():
    """Separate engine with ``PRAGMA foreign_keys=ON`` (off by default on
    sqlite) so cascade-delete behavior is actually exercised, not just
    declared in the schema."""
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_connection, _record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(
        engine, tables=tables_of(ExternalIngestBatch, ExternalIngestRejection)
    )
    with Session(engine) as session:
        yield session
    engine.dispose()


def _make_batch(**overrides):
    defaults = dict(
        ingestion_id=uuid.uuid4(),
        org_id="org-a",
        idempotency_key=str(uuid.uuid4()),
        payload_hash="hash",
        source_system="github",
        source_instance="acme/api",
        schema_version="external-ingest.v1",
        status=BatchStatus.COMPLETED.value,
        items_received=1,
        items_accepted=1,
        items_rejected=0,
    )
    defaults.update(overrides)
    return ExternalIngestBatch(**defaults)


def test_prune_deletes_only_expired_terminal_batches(db_session, monkeypatch):
    now = datetime.now(timezone.utc)
    expired_completed = _make_batch(
        status=BatchStatus.COMPLETED.value, created_at=now - timedelta(days=100)
    )
    expired_partial = _make_batch(
        status=BatchStatus.PARTIAL.value, created_at=now - timedelta(days=100)
    )
    expired_failed = _make_batch(
        status=BatchStatus.FAILED.value, created_at=now - timedelta(days=100)
    )
    fresh_completed = _make_batch(
        status=BatchStatus.COMPLETED.value, created_at=now - timedelta(days=1)
    )
    db_session.add_all(
        [expired_completed, expired_partial, expired_failed, fresh_completed]
    )
    db_session.commit()

    _patch_db_session(monkeypatch, db_session)

    result = getattr(prune_external_ingest_batches, "run")(retention_days=90)

    assert result["status"] == "completed"
    assert result["deleted"] == 3
    assert result["retention_days"] == 90
    remaining_ids = {
        row.ingestion_id for row in db_session.query(ExternalIngestBatch).all()
    }
    assert remaining_ids == {fresh_completed.ingestion_id}


def test_prune_never_deletes_non_terminal_batches_regardless_of_age(
    db_session, monkeypatch
):
    now = datetime.now(timezone.utc)
    stuck_accepted = _make_batch(
        status=BatchStatus.ACCEPTED.value, created_at=now - timedelta(days=365)
    )
    stuck_processing = _make_batch(
        status=BatchStatus.PROCESSING.value, created_at=now - timedelta(days=365)
    )
    stuck_stream_unavailable = _make_batch(
        status=BatchStatus.STREAM_UNAVAILABLE.value,
        created_at=now - timedelta(days=365),
    )
    db_session.add_all([stuck_accepted, stuck_processing, stuck_stream_unavailable])
    db_session.commit()

    _patch_db_session(monkeypatch, db_session)

    result = getattr(prune_external_ingest_batches, "run")(retention_days=1)

    assert result["deleted"] == 0
    assert db_session.query(ExternalIngestBatch).count() == 3


def test_prune_honors_env_var_when_no_explicit_override(db_session, monkeypatch):
    now = datetime.now(timezone.utc)
    three_days_old = _make_batch(
        status=BatchStatus.COMPLETED.value, created_at=now - timedelta(days=3)
    )
    one_hour_old = _make_batch(
        status=BatchStatus.COMPLETED.value, created_at=now - timedelta(hours=1)
    )
    db_session.add_all([three_days_old, one_hour_old])
    db_session.commit()

    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("EXTERNAL_INGEST_STATUS_RETENTION_DAYS", "1")

    result = getattr(prune_external_ingest_batches, "run")()

    assert result["deleted"] == 1
    assert result["retention_days"] == 1
    remaining = {
        row.ingestion_id for row in db_session.query(ExternalIngestBatch).all()
    }
    assert remaining == {one_hour_old.ingestion_id}


def test_prune_deletes_in_bounded_chunks_across_multiple_iterations(
    db_session, monkeypatch
):
    # Seed more rows than one chunk to exercise the loop's multi-iteration
    # path (adversarial-review finding: a single unbounded DELETE risks one
    # huge long-running transaction against a large backlog).
    now = datetime.now(timezone.utc)
    total_rows = _DELETE_CHUNK_SIZE + 3
    for _ in range(total_rows):
        db_session.add(
            _make_batch(
                status=BatchStatus.COMPLETED.value, created_at=now - timedelta(days=100)
            )
        )
    db_session.commit()

    commit_calls = {"count": 0}
    real_commit = db_session.commit

    def counting_commit():
        commit_calls["count"] += 1
        return real_commit()

    monkeypatch.setattr(db_session, "commit", counting_commit)
    _patch_db_session(monkeypatch, db_session)

    result = getattr(prune_external_ingest_batches, "run")(retention_days=1)

    assert result["deleted"] == total_rows
    assert db_session.query(ExternalIngestBatch).count() == 0
    # More than one chunk was needed, so more than one commit happened
    # (each chunk commits independently rather than one giant transaction).
    assert commit_calls["count"] >= 2


def test_prune_cascades_to_rejections_with_fk_enforcement(
    db_session_with_fk_enforcement, monkeypatch
):
    session = db_session_with_fk_enforcement
    now = datetime.now(timezone.utc)
    batch = _make_batch(
        status=BatchStatus.PARTIAL.value, created_at=now - timedelta(days=100)
    )
    session.add(batch)
    session.commit()
    session.add(
        ExternalIngestRejection(
            id=uuid.uuid4(),
            org_id=batch.org_id,
            ingestion_id=batch.ingestion_id,
            record_index=0,
            record_kind="commit.v1",
            code="code",
            message="msg",
        )
    )
    session.commit()

    _patch_db_session(monkeypatch, session)

    result = getattr(prune_external_ingest_batches, "run")(retention_days=1)

    assert result["deleted"] == 1
    assert session.query(ExternalIngestBatch).count() == 0
    assert session.query(ExternalIngestRejection).count() == 0
