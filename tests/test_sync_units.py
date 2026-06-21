from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncConfiguration,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_FINALIZE,
    OUTBOX_KIND_POST_SYNC,
    OUTBOX_STATUS_DISPATCHED,
    OUTBOX_STATUS_PENDING,
    build_post_sync_dispatch_payload,
)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _patch_db_session(monkeypatch, session):
    import dev_health_ops.db as db

    session.commit()
    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


@contextmanager
def _new_session_ctx(engine):
    with Session(engine) as session:
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        else:
            session.commit()


def _patch_db_session_factory(monkeypatch, engine):
    import dev_health_ops.db as db

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _new_session_ctx(engine)
    )


def _file_backed_engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'sync-unit-race.db'}")
    Base.metadata.create_all(engine)
    return engine


def _commit_reconciler_failure(engine, unit_id):
    with Session(engine) as session:
        unit = session.query(SyncRunUnit).filter(SyncRunUnit.id == unit_id).one()
        unit.status = SyncRunUnitStatus.FAILED.value
        unit.error = "sync unit lease expired"
        unit.result = {"error_category": "worker_lost"}
        unit.lease_owner = None
        unit.lease_expires_at = None
        session.commit()


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


def _seed_zero_unit_run(session):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="linear",
        name="linear-demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.PLANNED.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
    )
    session.add(run)
    session.flush()
    return run


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


def _patch_worker_enqueues(monkeypatch):
    from dev_health_ops.workers import sync_units

    dispatch_calls = []
    finalize_calls = []
    chord_calls = []

    class FakeChord:
        def apply_async(self):
            chord_calls.append("apply_async")

    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None, **kwargs: dispatch_calls.append((args, queue)),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalize_calls.append((args, queue)),
    )
    monkeypatch.setattr(sync_units, "chord", lambda *args, **kwargs: FakeChord())
    return dispatch_calls, finalize_calls, chord_calls


def test_run_sync_unit_success_persists_status_and_incremental_watermark(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.attempts == 1
    assert unit.result == {"ok": True}
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert unit.last_heartbeat_at is not None
    watermark = db_session.query(SyncWatermark).one()
    assert watermark.org_id == run.org_id
    assert watermark.source_id == "full-chaos/dev-health"
    assert watermark.dataset_key == "commits"
    finalize_outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_FINALIZE)
        .one()
    )
    assert finalize_outbox.status == OUTBOX_STATUS_PENDING
    assert finalize_calls == [((str(run.id),), "sync")]


def test_run_sync_unit_success_survives_finalize_enqueue_failure(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )

    def fail_finalize_enqueue(*_args, **_kwargs):
        raise RuntimeError("broker down")

    monkeypatch.setattr(
        sync_units.finalize_sync_run, "apply_async", fail_finalize_enqueue
    )

    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "success"
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.result == {"ok": True}


def test_run_sync_unit_success_skips_watermark_for_backfill(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session, mode=SyncRunMode.BACKFILL.value)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert db_session.query(SyncWatermark).count() == 0
    assert finalize_calls == [((str(run.id),), "sync")]


def test_run_sync_unit_failure_persists_failed_and_error(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def fail(ctx, runtime):
        raise RuntimeError("adapter failed")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", fail)

    result = getattr(run_sync_unit, "run")(str(unit.id))
    assert result["status"] == "failed"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.error == "adapter failed"
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert unit.last_heartbeat_at is not None
    finalize_outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_FINALIZE)
        .one()
    )
    assert finalize_outbox.status == OUTBOX_STATUS_PENDING
    assert finalize_calls == [((str(run.id),), "sync")]


def test_run_sync_unit_sets_and_clears_lease_around_provider_call(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.setenv("SYNC_UNIT_RUNNING_STALE_SECONDS", "120")

    def run_dataset(ctx, runtime):
        db_session.refresh(unit)
        assert unit.status == SyncRunUnitStatus.RUNNING.value
        assert unit.lease_owner is not None
        assert unit.lease_expires_at is not None
        assert _aware(unit.lease_expires_at) > datetime.now(timezone.utc)
        assert unit.last_heartbeat_at is not None
        return {"ok": True}

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", run_dataset)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "success"
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None


def test_heartbeat_extends_live_matching_lease(db_session, monkeypatch):
    import threading

    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    now = datetime.now(timezone.utc)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.lease_owner = "worker-1"
    unit.lease_expires_at = now + timedelta(seconds=30)
    unit.last_heartbeat_at = now
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(sync_units, "_heartbeat_interval_seconds", lambda: 1)
    monkeypatch.setattr(sync_units, "_running_lease_seconds", lambda: 120)

    class OneHeartbeatStop(threading.Event):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def wait(self, timeout=None):
            self.calls += 1
            return self.calls > 1

    sync_units._heartbeat_unit_lease(str(unit.id), "worker-1", OneHeartbeatStop())

    db_session.refresh(unit)
    lease_expires_at = unit.lease_expires_at
    last_heartbeat_at = unit.last_heartbeat_at
    assert lease_expires_at is not None
    assert last_heartbeat_at is not None
    assert _aware(lease_expires_at) > now + timedelta(seconds=30)
    assert _aware(last_heartbeat_at) > now


def test_heartbeat_loses_after_reconciler_terminalizes(tmp_path, monkeypatch):
    import threading

    from dev_health_ops.workers import sync_units

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            _, unit = _seed_run(seed_session)
            now = datetime.now(timezone.utc)
            unit.status = SyncRunUnitStatus.RUNNING.value
            unit.lease_owner = "worker-1"
            unit.lease_expires_at = now + timedelta(seconds=30)
            unit.last_heartbeat_at = now
            unit_id = unit.id
            seed_session.commit()
        _commit_reconciler_failure(engine, unit_id)
        _patch_db_session_factory(monkeypatch, engine)
        monkeypatch.setattr(sync_units, "_heartbeat_interval_seconds", lambda: 1)
        monkeypatch.setattr(sync_units, "_running_lease_seconds", lambda: 120)

        class OneHeartbeatStop(threading.Event):
            def __init__(self):
                super().__init__()
                self.calls = 0

            def wait(self, timeout=None):
                self.calls += 1
                return self.calls > 1

        sync_units._heartbeat_unit_lease(str(unit_id), "worker-1", OneHeartbeatStop())

        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.error == "sync unit lease expired"
            assert unit.result == {"error_category": "worker_lost"}
            assert unit.lease_owner is None
            assert unit.lease_expires_at is None
    finally:
        engine.dispose()


def test_worker_success_after_reconciler_failed_does_not_overwrite_terminal(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)

    def run_dataset(ctx, runtime):
        db_session.refresh(unit)
        unit.status = SyncRunUnitStatus.FAILED.value
        unit.error = "sync unit lease expired"
        unit.result = {"error_category": "worker_lost"}
        unit.lease_owner = None
        unit.lease_expires_at = None
        db_session.flush()
        return {"ok": True}

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", run_dataset)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "lease_lost",
    }
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.error == "sync unit lease expired"
    assert unit.result == {"error_category": "worker_lost"}
    assert finalize_calls == []


def test_worker_success_cas_loses_to_reconciler_does_not_overwrite_failed(
    tmp_path, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            _mark_dispatching(seed_session, unit)
            run_id = run.id
            unit_id = unit.id
            seed_session.commit()
        _patch_db_session_factory(monkeypatch, engine)
        _patch_runtime(monkeypatch)
        finalize_calls = _patch_finalize_apply(monkeypatch)
        monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        def run_dataset(ctx, runtime):
            _commit_reconciler_failure(engine, unit_id)
            return {"ok": True}

        monkeypatch.setattr(dataset_adapters, "run_dataset_unit", run_dataset)

        result = getattr(run_sync_unit, "run")(str(unit_id))

        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert result == {
                "status": "skipped",
                "unit_id": str(unit_id),
                "reason": "lease_lost",
            }
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.error == "sync unit lease expired"
            assert unit.result == {"error_category": "worker_lost"}
            assert unit.lease_owner is None
            assert unit.lease_expires_at is None
            assert assert_session.query(SyncWatermark).count() == 0
            assert assert_session.get(SyncRun, run_id) is not None
        assert finalize_calls == []
    finally:
        engine.dispose()


def test_worker_failure_cas_loses_to_reconciler_does_not_overwrite_failed(
    tmp_path, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            _, unit = _seed_run(seed_session)
            _mark_dispatching(seed_session, unit)
            unit_id = unit.id
            seed_session.commit()
        _patch_db_session_factory(monkeypatch, engine)
        _patch_runtime(monkeypatch)
        finalize_calls = _patch_finalize_apply(monkeypatch)

        def run_dataset(ctx, runtime):
            _commit_reconciler_failure(engine, unit_id)
            raise RuntimeError("adapter failed after lease loss")

        monkeypatch.setattr(dataset_adapters, "run_dataset_unit", run_dataset)

        result = getattr(run_sync_unit, "run")(str(unit_id))

        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert result == {
                "status": "skipped",
                "unit_id": str(unit_id),
                "reason": "lease_lost",
            }
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.error == "sync unit lease expired"
            assert unit.result == {"error_category": "worker_lost"}
        assert finalize_calls == []
    finally:
        engine.dispose()


def test_run_sync_unit_bootstrap_failure_enqueues_finalize(db_session, monkeypatch):
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)

    def fail_bootstrap(session, unit_id):
        raise ValueError("missing source")

    monkeypatch.setattr(SyncTaskBootstrap, "load", fail_bootstrap)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "failed"
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.attempts == 1
    assert unit.error == "missing source"
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert unit.result == {"error_category": "adapter_error"}
    assert dispatch_calls == []
    assert finalize_calls == [((str(run.id),), "sync")]
    assert chord_calls == []


def test_run_sync_unit_bootstrap_failure_survives_session_rollback(
    db_session, monkeypatch
):
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)

    def fail_bootstrap(session, unit_id):
        db_session.refresh(unit)
        assert unit.status == SyncRunUnitStatus.RUNNING.value
        assert unit.lease_owner is not None
        assert unit.lease_expires_at is not None
        raise ValueError("missing source")

    monkeypatch.setattr(SyncTaskBootstrap, "load", fail_bootstrap)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "failed"
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.attempts == 1
    assert unit.error == "missing source"
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert unit.result == {"error_category": "adapter_error"}
    assert dispatch_calls == []
    assert finalize_calls == [((str(run.id),), "sync")]
    assert chord_calls == []


def test_bootstrap_failure_cas_loses_to_reconciler(tmp_path, monkeypatch):
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap
    from dev_health_ops.workers.sync_units import run_sync_unit

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            _, unit = _seed_run(seed_session)
            _mark_dispatching(seed_session, unit)
            unit_id = unit.id
            seed_session.commit()
        _patch_db_session_factory(monkeypatch, engine)
        dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(
            monkeypatch
        )

        def fail_bootstrap(session, unit_id_arg):
            assert unit_id_arg == str(unit_id)
            _commit_reconciler_failure(engine, unit_id)
            raise ValueError("missing source")

        monkeypatch.setattr(SyncTaskBootstrap, "load", fail_bootstrap)

        result = getattr(run_sync_unit, "run")(str(unit_id))

        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert result == {
                "status": "skipped",
                "unit_id": str(unit_id),
                "reason": "lease_lost",
            }
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.error == "sync unit lease expired"
            assert unit.result == {"error_category": "worker_lost"}
        assert dispatch_calls == []
        assert finalize_calls == []
        assert chord_calls == []
    finally:
        engine.dispose()


def test_slow_bootstrap_loses_lease_before_provider_does_not_execute(
    tmp_path, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            _, unit = _seed_run(seed_session)
            _mark_dispatching(seed_session, unit)
            unit_id = unit.id
            seed_session.commit()
        _patch_db_session_factory(monkeypatch, engine)
        dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(
            monkeypatch
        )
        heartbeat_started = []
        original_load = SyncTaskBootstrap.load

        def start_heartbeat(unit_id_arg, lease_owner):
            heartbeat_started.append((unit_id_arg, lease_owner))
            return None, None

        monkeypatch.setattr(
            sync_units,
            "_start_unit_heartbeat",
            start_heartbeat,
        )

        def load_then_lose_lease(session, unit_id_arg):
            ctx = original_load(session, unit_id_arg)
            session.commit()
            _commit_reconciler_failure(engine, unit_id)
            return ctx

        def fail_if_provider_called(ctx, runtime):
            raise AssertionError("provider work must not run after lease loss")

        monkeypatch.setattr(SyncTaskBootstrap, "load", load_then_lose_lease)
        monkeypatch.setattr(
            dataset_adapters, "run_dataset_unit", fail_if_provider_called
        )

        result = getattr(sync_units.run_sync_unit, "run")(str(unit_id))

        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert result == {
                "status": "skipped",
                "unit_id": str(unit_id),
                "reason": "lease_lost",
            }
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.error == "sync unit lease expired"
            assert unit.result == {"error_category": "worker_lost"}
            assert assert_session.query(SyncWatermark).count() == 0
        assert heartbeat_started and heartbeat_started[0][0] == str(unit_id)
        assert dispatch_calls == []
        assert finalize_calls == []
        assert chord_calls == []
    finally:
        engine.dispose()


def test_success_cas_and_watermark_are_one_transaction(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )

    def fail_watermark(*_args, **_kwargs):
        raise RuntimeError("watermark store down")

    monkeypatch.setattr(sync_units, "set_watermark", fail_watermark)

    with pytest.raises(RuntimeError, match="watermark store down"):
        getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RUNNING.value
    assert unit.lease_owner is not None
    assert unit.lease_expires_at is not None
    assert unit.result is None
    assert unit.error is None
    assert db_session.query(SyncWatermark).count() == 0
    assert finalize_calls == []
    assert run.id == unit.sync_run_id


def test_run_sync_unit_bootstrap_failure_skips_duplicate_live_running_lease(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    lease_expires_at = now + timedelta(minutes=10)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.lease_owner = "other-worker"
    unit.lease_expires_at = lease_expires_at
    unit.last_heartbeat_at = now
    unit.error = "existing error"
    unit.result = {"existing": True}
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)

    def fail_bootstrap(session, unit_id):
        raise ValueError("missing source")

    monkeypatch.setattr(SyncTaskBootstrap, "load", fail_bootstrap)

    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "not_dispatchable",
    }
    assert unit.status == SyncRunUnitStatus.RUNNING.value
    assert unit.lease_owner == "other-worker"
    persisted_lease_expires_at = unit.lease_expires_at
    assert persisted_lease_expires_at is not None
    assert _aware(persisted_lease_expires_at) == lease_expires_at
    assert unit.error == "existing error"
    assert unit.result == {"existing": True}
    assert dispatch_calls == []
    assert finalize_calls == []
    assert chord_calls == []


def test_run_sync_unit_skips_terminal_run_without_overwriting_unit(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    run.status = SyncRunStatus.FAILED.value
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    unit.error = "broker down"
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)

    def fail_if_called(ctx, runtime):
        raise AssertionError("terminal unit should not execute")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", fail_if_called)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "terminal",
    }
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.error == "broker down"
    assert finalize_calls == []


def test_run_sync_unit_skips_duplicate_delivery_with_live_running_lease(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.lease_owner = "worker-live"
    unit.lease_expires_at = now + timedelta(minutes=10)
    unit.last_heartbeat_at = now
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)

    def fail_if_called(ctx, runtime):
        raise AssertionError("duplicate delivery must not execute provider work")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", fail_if_called)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "not_dispatchable",
    }
    assert unit.status == SyncRunUnitStatus.RUNNING.value
    assert unit.lease_owner == "worker-live"
    assert finalize_calls == []


def test_finalize_once_only_dispatches_metrics_once(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical",
        provider="github",
        sync_targets=["git"],
        migrated_integration_id=run.integration_id,
    )
    db_session.add(config)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    monkeypatch.setattr(
        sync_units,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    first = sync_units.finalize_sync_run(str(run.id))
    second = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(config)
    assert first["status"] == "finalized"
    assert second["status"] == "already_dispatched"
    assert run.status == SyncRunStatus.SUCCESS.value
    assert db_session.query(SyncRunPostDispatch).count() == 1
    post_sync_outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_POST_SYNC)
        .one()
    )
    assert post_sync_outbox.status == OUTBOX_STATUS_DISPATCHED
    assert len(dispatches) == 1
    assert dispatches[0]["sync_targets"] == ["git"]
    assert config.last_sync_at is not None
    assert config.last_sync_success is True
    assert config.last_sync_error is None
    assert config.last_sync_stats == {"completed_units": 1, "failed_units": 0}


def test_finalize_aggregates_partial_failed(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical-partial",
        provider="github",
        sync_targets=["git", "prs"],
        migrated_integration_id=run.integration_id,
    )
    db_session.add(config)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    failed = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.FAILED.value,
        attempts=1,
    )
    run.total_units = 2
    db_session.add(failed)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(sync_units, "_dispatch_post_sync_tasks", lambda **kwargs: None)

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(config)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.completed_units == 1
    assert run.failed_units == 1
    assert config.last_sync_success is False
    assert config.last_sync_error == "Sync run completed with failed units"


def test_build_post_sync_dispatch_payload_matches_finalize_window_fields(db_session):
    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    unit.since_at = datetime(2026, 6, 1, 10, 30, tzinfo=timezone.utc)
    unit.before_at = datetime(2026, 6, 3, 22, 15, tzinfo=timezone.utc)
    db_session.flush()

    payload = build_post_sync_dispatch_payload(db_session, run.id)

    assert payload is not None
    assert payload.provider == "github"
    assert payload.sync_targets == ["git"]
    assert payload.org_id == run.org_id
    assert payload.from_date == "2026-06-01"
    assert payload.to_date == "2026-06-03"
    assert payload.work_graph_from_date == "2026-06-01T00:00:00+00:00"
    assert payload.work_graph_to_date == "2026-06-04T00:00:00+00:00"


def test_build_post_sync_dispatch_payload_returns_none_without_success(db_session):
    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.FAILED.value
    db_session.flush()

    assert build_post_sync_dispatch_payload(db_session, run.id) is None


def test_finalize_zero_unit_run_does_not_report_success(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    monkeypatch.setattr(
        sync_units,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.FAILED.value
    assert run.completed_units == 0
    assert run.failed_units == 0
    assert run.error == "No sync units planned"
    assert run.result == {
        "completed_units": 0,
        "failed_units": 0,
        "reason": "no_sync_units_planned",
    }
    assert db_session.query(SyncRunPostDispatch).count() == 1
    post_sync_outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_POST_SYNC)
        .one()
    )
    assert post_sync_outbox.status == OUTBOX_STATUS_DISPATCHED
    assert dispatches == []


def test_dispatch_sync_run_redispatches_only_planned_units(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, planned = _seed_run(db_session)
    recent_dispatching = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
    )
    db_session.add(recent_dispatching)
    db_session.flush()
    recent_dispatching.updated_at = datetime.now(timezone.utc)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    queued = []

    class FakeSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id
            self.queue = None

        def set(self, *, queue):
            self.queue = queue
            queued.append(self)
            return self

    class FakeChord:
        def __init__(self, header, callback):
            self.header = header
            self.callback = callback

        def apply_async(self):
            return None

    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda unit_id: FakeSig(unit_id))
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "si", lambda run_id: FakeSig(run_id)
    )
    monkeypatch.setattr(sync_units, "group", lambda signatures: list(signatures))
    monkeypatch.setattr(
        sync_units, "chord", lambda header, callback: FakeChord(header, callback)
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(planned)
    db_session.refresh(recent_dispatching)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert [sig.unit_id for sig in queued if sig.unit_id != str(run.id)] == [
        str(planned.id)
    ]
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert recent_dispatching.status == SyncRunUnitStatus.DISPATCHING.value


def test_dispatch_sync_run_denies_inactive_planner_config(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="paused-planner",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        migrated_integration_id=run.integration_id,
        is_active=False,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    def fail_queue(*_args, **_kwargs):
        raise AssertionError("inactive planner config must not queue units")

    monkeypatch.setattr(sync_units.run_sync_unit, "s", fail_queue)
    monkeypatch.setattr(sync_units, "chord", fail_queue)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    db_session.refresh(config)
    assert result == {"status": "denied", "reason": "sync configuration is paused"}
    assert run.status == SyncRunStatus.FAILED.value
    assert run.error == "sync configuration is paused"
    assert run.result == {"reason": "inactive_sync_configuration"}
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.error == "sync configuration is paused"
    assert config.last_sync_success is False
    assert config.last_sync_error == "sync configuration is paused"


def test_paused_config_with_running_and_planned_units_does_not_strand(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    running.status = SyncRunUnitStatus.RUNNING.value
    running.attempts = 1
    running.lease_owner = "worker-live"
    running.lease_expires_at = now + timedelta(minutes=5)
    running.last_heartbeat_at = now
    planned = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=running.integration_id,
        source_id=running.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_prs": True},
    )
    config = SyncConfiguration(
        org_id=run.org_id,
        name="paused-with-running",
        provider="github",
        sync_targets=["git", "prs"],
        sync_options={},
        migrated_integration_id=run.integration_id,
        is_active=False,
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 2
    db_session.add_all([planned, config])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)
    monkeypatch.setattr(sync_units, "_dispatch_post_sync_tasks", lambda **kwargs: None)

    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(running)
    db_session.refresh(planned)
    db_session.refresh(config)
    assert dispatch_result == {
        "status": "denied_active",
        "reason": "sync configuration is paused",
        "failed_planned_units": 1,
        "failed_stale_dispatching_units": 0,
    }
    assert run.status not in {
        SyncRunStatus.SUCCESS.value,
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }
    assert run.completed_at is None
    assert config.last_sync_at is None
    assert planned.status == SyncRunUnitStatus.FAILED.value
    assert planned.error == "sync configuration is paused"
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert running.lease_owner == "worker-live"
    assert dispatch_calls == []
    assert finalize_calls == [((str(run.id),), "sync")]
    assert chord_calls == []

    running.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db_session.flush()

    reconcile_result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(running)
    assert reconcile_result["expired_units"] == 1
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert finalize_calls == [((str(run.id),), "sync"), ((str(run.id),), "sync")]

    finalize_result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    units = (
        db_session.query(SyncRunUnit).filter(SyncRunUnit.sync_run_id == run.id).all()
    )
    assert finalize_result["status"] == "finalized"
    assert all(
        unit.status in {SyncRunUnitStatus.SUCCESS.value, SyncRunUnitStatus.FAILED.value}
        for unit in units
    )
    assert run.status == SyncRunStatus.FAILED.value
    assert run.completed_at is not None
    assert run.failed_units == 2


def test_paused_config_with_stale_dispatching_does_not_redispatch(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, stale_dispatching = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    stale_dispatching.status = SyncRunUnitStatus.DISPATCHING.value
    stale_dispatching.updated_at = now - timedelta(minutes=30)
    running = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-live",
        lease_expires_at=now + timedelta(minutes=5),
        last_heartbeat_at=now,
        processor_flags={"sync_prs": True},
    )
    planned = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        dataset_key="issues",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_issues": True},
    )
    config = SyncConfiguration(
        org_id=run.org_id,
        name="paused-with-stale-dispatching",
        provider="github",
        sync_targets=["git", "prs", "issues"],
        sync_options={},
        migrated_integration_id=run.integration_id,
        is_active=False,
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 3
    db_session.add_all([running, planned, config])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)

    def fail_queue(*_args, **_kwargs):
        raise AssertionError("paused config must not queue run_sync_unit")

    monkeypatch.setattr(sync_units.run_sync_unit, "s", fail_queue)

    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(stale_dispatching)
    db_session.refresh(running)
    db_session.refresh(planned)
    assert dispatch_result == {
        "status": "denied_active",
        "reason": "sync configuration is paused",
        "failed_planned_units": 1,
        "failed_stale_dispatching_units": 1,
    }
    assert stale_dispatching.status == SyncRunUnitStatus.FAILED.value
    assert stale_dispatching.error == "sync configuration is paused"
    assert stale_dispatching.result == {"error_category": "dispatch_denied"}
    assert planned.status == SyncRunUnitStatus.FAILED.value
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert running.lease_owner == "worker-live"
    assert run.status not in {
        SyncRunStatus.SUCCESS.value,
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }
    assert run.completed_at is None
    assert dispatch_calls == []
    assert finalize_calls == [((str(run.id),), "sync")]
    assert chord_calls == []

    running.status = SyncRunUnitStatus.FAILED.value
    running.error = "sync unit lease expired"
    running.result = {"error_category": "worker_lost"}
    running.lease_owner = None
    running.lease_expires_at = None
    running.updated_at = datetime.now(timezone.utc)
    db_session.flush()

    finalize_result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert finalize_result["status"] == "finalized"
    assert run.status == SyncRunStatus.FAILED.value
    assert run.failed_units == 3


def test_total_cap_hard_deny_with_stale_dispatching_does_not_redispatch(
    db_session, monkeypatch
):
    from dev_health_ops.sync.guard import GuardDecision
    from dev_health_ops.workers import sync_units

    run, stale_dispatching = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    stale_dispatching.status = SyncRunUnitStatus.DISPATCHING.value
    stale_dispatching.updated_at = now - timedelta(minutes=30)
    running = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-live",
        lease_expires_at=now + timedelta(minutes=5),
        last_heartbeat_at=now,
        processor_flags={"sync_prs": True},
    )
    planned = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=stale_dispatching.integration_id,
        source_id=stale_dispatching.source_id,
        provider="github",
        dataset_key="issues",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_issues": True},
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 3
    db_session.add_all([running, planned])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)
    reason = "sync run unit cap exceeded: 3/1"
    monkeypatch.setattr(
        sync_units.DispatchGuard,
        "authorize_run",
        lambda session, sync_run_id: GuardDecision(
            False,
            reason,
            (str(stale_dispatching.id), str(running.id), str(planned.id)),
        ),
    )

    def fail_queue(*_args, **_kwargs):
        raise AssertionError("total-cap hard-deny must not queue run_sync_unit")

    monkeypatch.setattr(sync_units.run_sync_unit, "s", fail_queue)

    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(stale_dispatching)
    db_session.refresh(running)
    db_session.refresh(planned)
    assert dispatch_result == {
        "status": "denied_active",
        "reason": reason,
        "failed_planned_units": 1,
        "failed_stale_dispatching_units": 1,
    }
    assert stale_dispatching.status == SyncRunUnitStatus.FAILED.value
    assert stale_dispatching.error == reason
    assert stale_dispatching.result == {"error_category": "dispatch_denied"}
    assert planned.status == SyncRunUnitStatus.FAILED.value
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert run.status not in {
        SyncRunStatus.SUCCESS.value,
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }
    assert run.completed_at is None
    assert dispatch_calls == []
    assert finalize_calls == [((str(run.id),), "sync")]
    assert chord_calls == []


def test_dispatch_sync_run_denies_inactive_migrated_child_config(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    parent_config = SyncConfiguration(
        org_id=run.org_id,
        name="active-parent",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        migrated_integration_id=run.integration_id,
        is_active=True,
    )
    db_session.add(parent_config)
    db_session.flush()
    child_config = SyncConfiguration(
        org_id=run.org_id,
        parent_id=parent_config.id,
        name="paused-child",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        migrated_integration_id=run.integration_id,
        migrated_source_id=unit.source_id,
        is_active=False,
    )
    db_session.add(child_config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    def fail_queue(*_args, **_kwargs):
        raise AssertionError("inactive child config must not queue units")

    monkeypatch.setattr(sync_units.run_sync_unit, "s", fail_queue)
    monkeypatch.setattr(sync_units, "chord", fail_queue)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    db_session.refresh(parent_config)
    assert result == {"status": "denied", "reason": "sync configuration is paused"}
    assert run.status == SyncRunStatus.FAILED.value
    assert run.error == "sync configuration is paused"
    assert run.result == {"reason": "inactive_sync_configuration"}
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.error == "sync configuration is paused"
    assert parent_config.last_sync_success is False
    assert parent_config.last_sync_error == "sync configuration is paused"


def test_dispatch_sync_run_does_not_terminalize_when_chord_enqueue_fails(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)

    class FakeSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id

        def set(self, *, queue):
            return self

    class FailingChord:
        def __init__(self, header, callback):
            self.header = header
            self.callback = callback

        def apply_async(self):
            raise RuntimeError("broker down")

    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda unit_id: FakeSig(unit_id))
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "si", lambda run_id: FakeSig(run_id)
    )
    monkeypatch.setattr(sync_units, "group", list)
    monkeypatch.setattr(
        sync_units, "chord", lambda header, callback: FailingChord(header, callback)
    )

    with pytest.raises(RuntimeError, match="broker down"):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert run.completed_at is None
    assert run.error is None
    assert run.result is None
    assert run.failed_units == 0
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.error is None


def test_dispatch_sync_run_redispatches_stale_dispatching_units(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    unit.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    queued = []

    class FakeSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id

        def set(self, *, queue):
            queued.append((self.unit_id, queue))
            return self

    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda unit_id: FakeSig(unit_id))
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "si", lambda run_id: FakeSig(run_id)
    )
    monkeypatch.setattr(sync_units, "group", lambda signatures: list(signatures))
    monkeypatch.setattr(
        sync_units,
        "chord",
        lambda header, callback: type("C", (), {"apply_async": lambda self: None})(),
    )

    result = sync_units.dispatch_sync_run(str(run.id))
    assert result["queued_units"] == 1
    assert queued[0][0] == str(unit.id)


def test_dispatch_sync_run_does_not_reclaim_stale_running_units(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.updated_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda unit_id: None)
    monkeypatch.setattr(sync_units.finalize_sync_run, "si", lambda run_id: None)
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "apply_async", lambda *a, **k: None
    )
    monkeypatch.setattr(sync_units, "group", lambda signatures: list(signatures))
    monkeypatch.setattr(
        sync_units,
        "chord",
        lambda header, callback: type("C", (), {"apply_async": lambda self: None})(),
    )
    monkeypatch.setattr(
        sync_units.dispatch_sync_run, "apply_async", lambda *a, **k: None
    )

    result = sync_units.dispatch_sync_run(str(run.id))
    assert result["queued_units"] == 0
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RUNNING.value


def test_dispatch_sync_run_does_not_reclaim_fresh_running_units(
    db_session, monkeypatch
):
    # A unit that is legitimately still running (fresh updated_at) must NOT be
    # reclaimed, or we would double-execute it.
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.updated_at = datetime.now(timezone.utc)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda unit_id: None)
    monkeypatch.setattr(sync_units.finalize_sync_run, "si", lambda run_id: None)
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "apply_async", lambda *a, **k: None
    )
    monkeypatch.setattr(sync_units, "group", lambda signatures: list(signatures))
    monkeypatch.setattr(
        sync_units,
        "chord",
        lambda header, callback: type("C", (), {"apply_async": lambda self: None})(),
    )
    monkeypatch.setattr(
        sync_units.dispatch_sync_run, "apply_async", lambda *a, **k: None
    )

    result = sync_units.dispatch_sync_run(str(run.id))
    assert result["queued_units"] == 0
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RUNNING.value


# ---------------------------------------------------------------------------
# Finding #2 regression: full_resync stamps watermark on success (CHAOS-2569)
# ---------------------------------------------------------------------------


def test_run_sync_unit_success_stamps_watermark_for_full_resync(
    db_session, monkeypatch
):
    """Successful full_resync unit must advance the watermark (end-to-end)."""
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session, mode=SyncRunMode.FULL_RESYNC.value)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    # full_resync must stamp the watermark so the next incremental doesn't cold-start
    watermark = db_session.query(SyncWatermark).one()
    assert watermark.dataset_key == "commits"


def test_post_sync_dispatch_includes_window(db_session, monkeypatch):
    """finalize_sync_run threads min(since_at)/max(before_at) of successful units
    into _dispatch_post_sync_tasks (CHAOS-2577).
    """
    from datetime import date

    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical-window",
        provider="github",
        sync_targets=["git"],
        migrated_integration_id=run.integration_id,
    )
    db_session.add(config)
    # Give the unit explicit window bounds.
    since = datetime(2026, 6, 1, tzinfo=timezone.utc)
    before = datetime(2026, 6, 7, 23, 59, tzinfo=timezone.utc)
    unit.since_at = since
    unit.before_at = before
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    monkeypatch.setattr(
        sync_units,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    result = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "finalized"
    assert len(dispatches) == 1
    kwargs = dispatches[0]
    # The covered window must be threaded through.
    assert kwargs.get("from_date") == date(2026, 6, 1).isoformat()
    assert kwargs.get("to_date") == date(2026, 6, 7).isoformat()


def test_post_sync_dispatch_none_window_unit_unbounds_lower(db_session, monkeypatch):
    """Mixed run: one NONE-window unit (since_at=None) + one bounded unit.

    The aggregate lower bound must be unbounded (from_date=None and
    work_graph_from_date=None), not the bounded unit's date (CHAOS-2577 fix).
    """
    from dev_health_ops.workers import sync_units

    # Seed the run with the first unit (bounded).
    run, unit_bounded = _seed_run(db_session)
    since_bounded = datetime(2026, 6, 1, tzinfo=timezone.utc)
    before_bounded = datetime(2026, 6, 7, 23, 59, tzinfo=timezone.utc)
    unit_bounded.since_at = since_bounded
    unit_bounded.before_at = before_bounded
    unit_bounded.status = SyncRunUnitStatus.SUCCESS.value

    # Add a second unit with since_at=None (NONE-window / unbounded lower).
    unit_none = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=run.integration_id,
        source_id=unit_bounded.source_id,
        provider="github",
        dataset_key="work-item-labels",
        cost_class="low",
        mode=run.mode,
        since_at=None,  # NONE-window: unbounded lower
        before_at=before_bounded,
        status=SyncRunUnitStatus.SUCCESS.value,
        attempts=1,
        processor_flags={},
    )
    db_session.add(unit_none)

    config = SyncConfiguration(
        org_id=run.org_id,
        name="mixed-window",
        provider="github",
        sync_targets=["git"],
        migrated_integration_id=run.integration_id,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    dispatches: list[dict] = []
    monkeypatch.setattr(
        sync_units,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    result = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "finalized"
    assert len(dispatches) == 1
    kwargs = dispatches[0]
    # The NONE-window unit makes the lower bound unbounded.
    assert kwargs.get("from_date") is None, (
        f"expected from_date=None (unbounded), got {kwargs.get('from_date')!r}"
    )
    assert kwargs.get("work_graph_from_date") is None, (
        f"expected work_graph_from_date=None (unbounded), got {kwargs.get('work_graph_from_date')!r}"
    )
    # Upper bound: both units have before_at set, so to_date must be non-None.
    assert kwargs.get("to_date") is not None
    assert kwargs.get("work_graph_to_date") is not None


def test_fail_stale_dispatching_does_not_overwrite_concurrent_claim(tmp_path):
    """Write-time CAS: a stale DISPATCHING row a delayed run_sync_unit has
    concurrently claimed to RUNNING (with a live lease) must NOT be clobbered to
    FAILED by the dispatch-denial stale-fail path.
    """
    from sqlalchemy import update as sa_update

    from dev_health_ops.workers.sync_units import (
        _fail_stale_dispatching_units,
        _stale_dispatch_seconds,
    )

    engine = _file_backed_engine(tmp_path)
    try:
        stale_age = timedelta(seconds=_stale_dispatch_seconds() + 600)
        seeded_at = datetime.now(timezone.utc) - stale_age
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            run_id = run.id
            unit_id = unit.id
            failed_unit = SyncRunUnit(
                org_id=unit.org_id,
                sync_run_id=run.id,
                integration_id=unit.integration_id,
                source_id=unit.source_id,
                provider=unit.provider,
                dataset_key="pull_requests",
                cost_class=unit.cost_class,
                mode=unit.mode,
                since_at=unit.since_at,
                before_at=unit.before_at,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags=unit.processor_flags,
                updated_at=seeded_at,
            )
            run.total_units = 2
            seed_session.add(failed_unit)
            # Explicit updated_at in the SET clause suppresses the column onupdate,
            # so the row is durably STALE DISPATCHING.
            seed_session.execute(
                sa_update(SyncRunUnit)
                .where(SyncRunUnit.id == unit_id)
                .values(
                    status=SyncRunUnitStatus.DISPATCHING.value,
                    updated_at=seeded_at,
                )
                .execution_options(synchronize_session=False)
            )
            seed_session.commit()
            failed_unit_id = failed_unit.id

        # A delayed run_sync_unit atomically claims the SAME stale row
        # DISPATCHING -> RUNNING with a live lease, in an independent session.
        lease_owner = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc)
        lease_expires_at = claimed_at + timedelta(seconds=3600)
        with Session(engine) as claim_session:
            claimed_count = (
                claim_session.query(SyncRunUnit)
                .filter(
                    SyncRunUnit.id == unit_id,
                    SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
                )
                .update(
                    {
                        SyncRunUnit.status: SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnit.error: None,
                        SyncRunUnit.lease_owner: lease_owner,
                        SyncRunUnit.lease_expires_at: lease_expires_at,
                        SyncRunUnit.last_heartbeat_at: claimed_at,
                        SyncRunUnit.updated_at: claimed_at,
                    },
                    synchronize_session=False,
                )
            )
            assert claimed_count == 1
            claim_session.commit()

        # The dispatch-denial path now runs the stale-fail helper.  The write-time
        # status='dispatching' predicate excludes the now-RUNNING row.
        with Session(engine) as fail_session:
            failed = _fail_stale_dispatching_units(
                fail_session, run_id, "sync dispatch denied"
            )
            fail_session.commit()

        assert failed == 1
        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            stale_unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == failed_unit_id)
                .one()
            )
            assert unit.status == SyncRunUnitStatus.RUNNING.value
            assert unit.lease_owner == lease_owner
            assert unit.lease_expires_at is not None
            assert _aware(unit.lease_expires_at) > datetime.now(timezone.utc)
            assert unit.error is None
            assert unit.result is None
            assert stale_unit.status == SyncRunUnitStatus.FAILED.value
            assert stale_unit.error == "sync dispatch denied"
            assert stale_unit.result == {"error_category": "dispatch_denied"}
    finally:
        engine.dispose()


def test_fail_stale_dispatching_fails_genuinely_stale_unit(tmp_path):
    """Control: a genuinely-stale DISPATCHING unit with no concurrent claim IS
    failed by the write-time CAS.
    """
    from sqlalchemy import update as sa_update

    from dev_health_ops.workers.sync_units import (
        _fail_stale_dispatching_units,
        _stale_dispatch_seconds,
    )

    engine = _file_backed_engine(tmp_path)
    try:
        stale_age = timedelta(seconds=_stale_dispatch_seconds() + 600)
        seeded_at = datetime.now(timezone.utc) - stale_age
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            run_id = run.id
            unit_id = unit.id
            seed_session.execute(
                sa_update(SyncRunUnit)
                .where(SyncRunUnit.id == unit_id)
                .values(
                    status=SyncRunUnitStatus.DISPATCHING.value,
                    updated_at=seeded_at,
                )
                .execution_options(synchronize_session=False)
            )
            seed_session.commit()

        with Session(engine) as fail_session:
            failed = _fail_stale_dispatching_units(
                fail_session, run_id, "sync dispatch denied"
            )
            fail_session.commit()

        assert failed == 1
        with Session(engine) as assert_session:
            unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert unit.status == SyncRunUnitStatus.FAILED.value
            assert unit.error == "sync dispatch denied"
            assert unit.result == {"error_category": "dispatch_denied"}
    finally:
        engine.dispose()


def test_claim_units_does_not_reclaim_concurrently_claimed_running(tmp_path):
    """Write-time CAS in _claim_units stale-reclaim: a stale DISPATCHING row a
    delayed run_sync_unit has concurrently claimed to RUNNING (with a live
    lease) must NOT be reclaimed/requeued. A genuinely-stale DISPATCHING unit
    with no concurrent claim IS reclaimed (returned, updated_at refreshed).
    """
    from sqlalchemy import update as sa_update

    from dev_health_ops.workers.sync_units import (
        _claim_units,
        _stale_dispatch_seconds,
    )

    engine = _file_backed_engine(tmp_path)
    try:
        stale_age = timedelta(seconds=_stale_dispatch_seconds() + 600)
        seeded_at = datetime.now(timezone.utc) - stale_age
        with Session(engine) as seed_session:
            run, reclaimable = _seed_run(seed_session)
            run_id = run.id
            reclaimable_id = reclaimable.id
            # Second unit: stale DISPATCHING that a delayed run_sync_unit will
            # concurrently claim to RUNNING below.
            concurrent = SyncRunUnit(
                org_id=reclaimable.org_id,
                sync_run_id=run.id,
                integration_id=reclaimable.integration_id,
                source_id=reclaimable.source_id,
                provider=reclaimable.provider,
                dataset_key="pull_requests",
                cost_class=reclaimable.cost_class,
                mode=reclaimable.mode,
                since_at=reclaimable.since_at,
                before_at=reclaimable.before_at,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags=reclaimable.processor_flags,
                updated_at=seeded_at,
            )
            run.total_units = 2
            seed_session.add(concurrent)
            # Explicit updated_at in the SET clause suppresses the column
            # onupdate, so both rows are durably STALE DISPATCHING.
            seed_session.execute(
                sa_update(SyncRunUnit)
                .where(SyncRunUnit.id == reclaimable_id)
                .values(
                    status=SyncRunUnitStatus.DISPATCHING.value,
                    updated_at=seeded_at,
                )
                .execution_options(synchronize_session=False)
            )
            seed_session.commit()
            concurrent_id = concurrent.id

        # A delayed run_sync_unit atomically claims the SAME stale row
        # DISPATCHING -> RUNNING with a live lease, in an independent session.
        lease_owner = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc)
        lease_expires_at = claimed_at + timedelta(seconds=3600)
        with Session(engine) as claim_session:
            claimed_count = (
                claim_session.query(SyncRunUnit)
                .filter(
                    SyncRunUnit.id == concurrent_id,
                    SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value,
                )
                .update(
                    {
                        SyncRunUnit.status: SyncRunUnitStatus.RUNNING.value,
                        SyncRunUnit.error: None,
                        SyncRunUnit.lease_owner: lease_owner,
                        SyncRunUnit.lease_expires_at: lease_expires_at,
                        SyncRunUnit.last_heartbeat_at: claimed_at,
                        SyncRunUnit.updated_at: claimed_at,
                    },
                    synchronize_session=False,
                )
            )
            assert claimed_count == 1
            claim_session.commit()

        # Now run the stale-reclaim path. The write-time status='dispatching'
        # AND updated_at<=stale_dispatch predicates exclude the now-RUNNING row.
        with Session(engine) as claim_units_session:
            claimed = _claim_units(claim_units_session, run_id)
            claimed_ids = {unit.id for unit in claimed}
            claim_units_session.commit()

        # The concurrently-claimed RUNNING unit was NOT reclaimed/requeued.
        assert concurrent_id not in claimed_ids
        # The genuinely-stale DISPATCHING unit WAS reclaimed.
        assert reclaimable_id in claimed_ids

        with Session(engine) as assert_session:
            running_unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == concurrent_id)
                .one()
            )
            reclaimed_unit = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == reclaimable_id)
                .one()
            )
            # RUNNING row untouched: lease intact, never reclaimed.
            assert running_unit.status == SyncRunUnitStatus.RUNNING.value
            assert running_unit.lease_owner == lease_owner
            assert running_unit.lease_expires_at is not None
            assert _aware(running_unit.lease_expires_at) > datetime.now(timezone.utc)
            # Reclaimed row stays DISPATCHING with a freshly-refreshed updated_at.
            assert reclaimed_unit.status == SyncRunUnitStatus.DISPATCHING.value
            assert _aware(reclaimed_unit.updated_at) > seeded_at
    finally:
        engine.dispose()
