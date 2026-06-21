"""Tests for CHAOS-2519: error category persistence and structured log context."""

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
from dev_health_ops.workers.sync_units import _classify_error

# ---------------------------------------------------------------------------
# Helpers (shared with test_sync_units.py pattern)
# ---------------------------------------------------------------------------


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


def _mark_dispatching(session, unit):
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    session.flush()


def _patch_runtime(monkeypatch):
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_bootstrap import ProviderRuntime

    class RuntimeCache:
        def get(self, context):
            return ProviderRuntime(extra={"unit_id": context.unit_id})

    monkeypatch.setattr(sync_units, "_runtime_cache", RuntimeCache())


def _patch_finalize_apply(monkeypatch):
    from dev_health_ops.workers import sync_units

    calls = []
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: calls.append((args, queue)),
    )
    return calls


# ---------------------------------------------------------------------------
# _classify_error unit tests
# ---------------------------------------------------------------------------


def test_classify_error_rate_limit():
    assert _classify_error(Exception("HTTP 429 rate limit exceeded")) == "rate_limit"


def test_classify_error_timeout():
    assert _classify_error(Exception("Request timed out after 30s")) == "timeout"


def test_classify_error_network():
    assert _classify_error(Exception("Connection refused")) == "network"


def test_classify_error_auth():
    assert _classify_error(Exception("HTTP 401 Unauthorized")) == "auth"


def test_classify_error_not_found():
    assert _classify_error(Exception("Resource not found (404)")) == "not_found"


def test_classify_error_provider_error():
    assert _classify_error(Exception("Server error 500")) == "provider_error"


def test_classify_error_adapter_error():
    assert _classify_error(Exception("unexpected NoneType")) == "adapter_error"


# ---------------------------------------------------------------------------
# Error category persisted in unit.result on failure
# ---------------------------------------------------------------------------


def test_run_sync_unit_failure_persists_error_category(db_session, monkeypatch):
    """On failure, unit.result must contain error_category."""
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def fail(ctx, runtime):
        raise RuntimeError("HTTP 429 rate limit exceeded")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", fail)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "failed"
    assert result["error_category"] == "rate_limit"

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result is not None
    assert unit.result["error_category"] == "rate_limit"


def test_run_sync_unit_failure_adapter_error_category(db_session, monkeypatch):
    """Generic failures get error_category='adapter_error'."""
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def fail(ctx, runtime):
        raise ValueError("unexpected None in response")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", fail)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["error_category"] == "adapter_error"
    db_session.refresh(unit)
    assert unit.result is not None
    assert unit.result["error_category"] == "adapter_error"


def test_run_sync_unit_success_result_has_no_error_category(db_session, monkeypatch):
    """On success, unit.result must NOT contain error_category."""
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"rows": 42}
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.result is not None
    assert "error_category" not in unit.result


# ---------------------------------------------------------------------------
# Structured log context fields (caplog)
# ---------------------------------------------------------------------------


def test_run_sync_unit_success_emits_structured_log(db_session, monkeypatch, caplog):
    """run_sync_unit.success log must carry all required context fields."""
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {})

    with caplog.at_level(logging.INFO, logger="dev_health_ops.workers.sync_units"):
        getattr(run_sync_unit, "run")(str(unit.id))

    success_records = [r for r in caplog.records if "success" in r.getMessage()]
    assert success_records, "Expected a run_sync_unit.success log record"
    rec = success_records[0]
    assert hasattr(rec, "sync_run_id")
    assert hasattr(rec, "unit_id")
    assert hasattr(rec, "source_id")
    assert hasattr(rec, "dataset_key")
    assert hasattr(rec, "provider")
    assert hasattr(rec, "cost_class")
    assert rec.unit_id == str(unit.id)
    assert rec.dataset_key == "commits"
    assert rec.provider == "github"
    assert rec.cost_class == "medium"


def test_run_sync_unit_failure_emits_structured_log(db_session, monkeypatch, caplog):
    """run_sync_unit.failed log must carry all required context fields + error_category."""
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def fail(ctx, runtime):
        raise ConnectionError("Connection refused")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", fail)

    with caplog.at_level(logging.ERROR, logger="dev_health_ops.workers.sync_units"):
        getattr(run_sync_unit, "run")(str(unit.id))

    failed_records = [r for r in caplog.records if "failed" in r.getMessage()]
    assert failed_records, "Expected a run_sync_unit.failed log record"
    rec = failed_records[0]
    assert hasattr(rec, "sync_run_id")
    assert hasattr(rec, "unit_id")
    assert hasattr(rec, "source_id")
    assert hasattr(rec, "dataset_key")
    assert hasattr(rec, "provider")
    assert hasattr(rec, "cost_class")
    assert hasattr(rec, "error_category")
    assert rec.error_category == "network"


def test_finalize_sync_run_emits_structured_log(db_session, monkeypatch, caplog):
    """finalize_sync_run.finalized log must carry sync_run_id and counts."""
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(sync_units, "_dispatch_post_sync_tasks", lambda **kwargs: None)

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
