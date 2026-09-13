"""Tests for CHAOS-2519: structured log context on finalize_sync_run.

error_category classification and run_sync_unit's own success/failure log
fields were covered here too, but run_sync_unit (the Celery task body) is
deleted -- it had no Celery producer or HTTP bridge, so it was already
unreachable in production. The native Go unit worker
(internal/jobs/providerunit) owns per-unit error classification and logging
now.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    yield session
    session.commit()


def _patch_db_session(monkeypatch, session):
    import dev_health_ops.db as db

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _seed_run(session, *, mode=SyncRunMode.INCREMENTAL.value):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="github",
        source_type="repo",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key="commits",
        is_enabled=True,
        options={},
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, dataset, run])
    session.flush()
    unit = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=mode,
        since_at=None,
        before_at=datetime.now(timezone.utc),
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True},
    )
    session.add(unit)
    session.flush()
    return run, unit


def test_finalize_sync_run_emits_structured_log(db_session, monkeypatch, caplog):
    """finalize_sync_run.finalized log must carry sync_run_id and counts."""
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    with caplog.at_level(logging.INFO, logger="dev_health_ops.workers.sync_units"):
        result = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "finalized"
    finalized_records = [r for r in caplog.records if "finalized" in r.getMessage()]
    assert finalized_records, "Expected a finalize_sync_run.finalized log record"
    rec = finalized_records[0]
    assert hasattr(rec, "sync_run_id")
    assert hasattr(rec, "completed_units")
    assert hasattr(rec, "failed_units")
    assert hasattr(rec, "run_status")
    assert rec.completed_units == 1
    assert rec.failed_units == 0
    assert rec.run_status == SyncRunStatus.SUCCESS.value
