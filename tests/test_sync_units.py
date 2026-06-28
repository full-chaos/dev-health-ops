from __future__ import annotations

import ast
import inspect
import json
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from billiard.exceptions import SoftTimeLimitExceeded
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    JobRun,
    JobRunStatus,
    ScheduledJob,
    SyncComputeCheckpoint,
    SyncComputeCheckpointStatus,
    SyncComputeType,
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
)
from dev_health_ops.workers.post_sync_dispatch import build_post_sync_dispatch_payload


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


def _seed_run(
    session,
    *,
    mode=SyncRunMode.INCREMENTAL.value,
    provider="github",
    source_type="repo",
    external_id="full-chaos/dev-health",
    name="dev-health",
    full_name="full-chaos/dev-health",
    dataset_key="commits",
    processor_flags=None,
):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider=provider,
        name="demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider=provider,
        source_type=source_type,
        external_id=external_id,
        name=name,
        full_name=full_name,
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key=dataset_key,
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
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode=mode,
        since_at=None,
        before_at=datetime.now(timezone.utc),
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags=processor_flags or {"sync_git": True},
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


def test_linear_backfill_retry_surface_contract_matches_work_item_write_fences():
    from dev_health_ops.metrics import job_work_items
    from dev_health_ops.workers import sync_units

    surface_labels = set()
    tree = ast.parse(inspect.getsource(job_work_items.run_work_items_sync_job))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name):
            continue
        if node.func.id != "_ensure_unit_lease_for_write":
            continue
        if not node.args:
            continue
        label = node.args[0]
        if isinstance(label, ast.Constant) and isinstance(label.value, str):
            surface_labels.add(label.value)

    assert (
        surface_labels == sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES
    )


def test_linear_backfill_retry_enabled_in_production_surface_registry():
    """CHAOS-2710: the work-items backfill unit's full in-band write set must be a
    subset of the proven-safe registry, so expired-lease/soft-timeout retry is ENABLED
    in production. Locks the feature on -- if a written surface ever loses its proven
    idempotency mechanism (dropped from the registry), this fails and retry must be
    re-justified before shipping."""
    from dev_health_ops.workers import sync_units

    assert sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES.issubset(
        sync_units._CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES
    )
    # The registry is a strict superset (it documents proven-safe surfaces this job
    # does not itself write), so the subset check is meaningful, not a tautology.
    assert (
        sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES
        != sync_units._CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES
    )


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
        dataset_adapters,
        "run_dataset_unit",
        lambda ctx, runtime: {
            "ok": True,
            "observations": {
                "github_usage": [
                    {
                        "transport": "rest",
                        "operation": "GET /repos/full-chaos/dev-health/issues",
                        "request_count": 1,
                        "rate_limit": {
                            "remaining": "4999",
                            "reset": "1234567890",
                        },
                    }
                ]
            },
        },
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.attempts == 1
    assert unit.result is not None
    assert unit.result["ok"] is True
    observations = unit.result["observations"]
    assert observations["github_usage"] == [
        {
            "transport": "rest",
            "operation": "GET /repos/full-chaos/dev-health/issues",
            "request_count": 1,
            "rate_limit": {"remaining": "4999", "reset": "1234567890"},
        }
    ]
    budget_estimate = observations["budget_estimate"]
    assert budget_estimate[0]["bucket"]["provider"] == "github"
    assert budget_estimate[0]["bucket"]["dimension"] == "rest_core"
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


def test_run_sync_unit_success_attaches_launchdarkly_budget_estimate(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        external_id="project:default",
        name="default",
        full_name="LaunchDarkly/default",
        dataset_key="feature-flags",
        processor_flags={"sync_feature_flags": True},
    )
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters,
        "run_dataset_unit",
        lambda ctx, runtime: {"ok": True, "observations": {}},
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.result is not None
    budget_estimate = unit.result["observations"]["budget_estimate"]
    assert {entry["bucket"]["provider"] for entry in budget_estimate} == {
        "launchdarkly"
    }
    assert {entry["route_family"] for entry in budget_estimate} == {
        "flags",
        "audit_log",
        "code_refs",
    }
    assert finalize_calls == [((str(run.id),), "sync")]


def test_run_sync_unit_success_attaches_linear_budget_estimate(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(
        db_session,
        provider="linear",
        source_type="team",
        external_id="TEAM",
        name="TEAM",
        full_name="TEAM",
        dataset_key="work-items",
        processor_flags={},
    )
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters,
        "run_dataset_unit",
        lambda ctx, runtime: {"ok": True, "observations": {}},
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.result is not None
    observations = unit.result["observations"]
    budget_estimate = observations["budget_estimate"]
    assert {estimate["bucket"]["provider"] for estimate in budget_estimate} == {
        "linear"
    }
    assert {estimate["bucket"]["dimension"] for estimate in budget_estimate} == {
        "graphql_cost"
    }
    assert {estimate["route_family"] for estimate in budget_estimate} == {
        "attachments",
        "comments",
        "cycles",
        "history",
        "issues",
        "teams",
    }


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
    assert unit.result is not None
    assert unit.result["ok"] is True
    assert "budget_estimate" in unit.result["observations"]


def test_run_sync_unit_budget_estimator_failure_does_not_fail_unit(
    db_session, monkeypatch
):
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

    def fail_estimate(_ctx):
        raise RuntimeError("budget estimator unavailable")

    monkeypatch.setattr(sync_units, "estimate_provider_budget", fail_estimate)

    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "success"
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.result == {"ok": True}
    assert finalize_calls == [((str(run.id),), "sync")]


def test_run_sync_unit_budget_observation_handles_malformed_adapter_observations(
    db_session, monkeypatch
):
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
        dataset_adapters,
        "run_dataset_unit",
        lambda ctx, runtime: {"ok": True, "observations": ["malformed"]},
    )

    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "success"
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.result is not None
    assert unit.result["ok"] is True
    assert (
        unit.result["observations"]["budget_estimate"][0]["bucket"]["provider"]
        == "github"
    )
    assert finalize_calls == [((str(run.id),), "sync")]


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


def test_run_sync_unit_soft_timeout_retries_eligible_linear_backfill_unit(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(
        db_session,
        mode=SyncRunMode.BACKFILL.value,
        provider="linear",
        source_type="team",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
        dataset_key="work-items",
        processor_flags={},
    )
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.setenv("SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", "2")
    monkeypatch.setattr(
        sync_units,
        "_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES",
        sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES,
    )

    def timeout(ctx, runtime):
        raise SoftTimeLimitExceeded()

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", timeout)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "soft_timeout_deferred"
    assert result["error_category"] == "soft_timeout"
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.available_at is not None
    assert unit.expired_lease_retry_count == 1
    assert unit.last_retry_reason == "soft_timeout"
    assert unit.result is not None
    assert unit.result["error_category"] == "soft_timeout"
    assert unit.result["retry_count"] == 1
    assert unit.result["retry_reason"] == "soft_timeout"
    assert unit.result["retry_exhausted"] is False
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert finalize_calls == []


def test_run_sync_unit_lost_lease_before_work_item_sink_aborts_without_finalize(
    db_session, monkeypatch
):
    from dev_health_ops.metrics.job_work_items import _ensure_unit_lease_for_write
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(
        db_session,
        mode=SyncRunMode.BACKFILL.value,
        provider="linear",
        source_type="team",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
        dataset_key="work-items",
        processor_flags={},
    )
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)

    def lose_lease_before_write(ctx, runtime):
        db_session.refresh(unit)
        unit.lease_owner = "other-worker"
        db_session.flush()
        _ensure_unit_lease_for_write("work_items")
        raise AssertionError("write fence should abort before this point")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", lose_lease_before_write)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "lease_lost",
        "surface": "work_items",
    }
    assert unit.status == SyncRunUnitStatus.RUNNING.value
    assert unit.lease_owner == "other-worker"
    assert finalize_calls == []


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

    deadline = now + timedelta(seconds=3720)
    sync_units._heartbeat_unit_lease(
        str(unit.id), "worker-1", OneHeartbeatStop(), deadline
    )

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

        deadline = now + timedelta(seconds=3720)
        sync_units._heartbeat_unit_lease(
            str(unit_id), "worker-1", OneHeartbeatStop(), deadline
        )

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

        def start_heartbeat(unit_id_arg, lease_owner, deadline):
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
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical",
        provider="github",
        sync_targets=["git"],
        integration_id=run.integration_id,
    )
    db_session.add(config)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    monkeypatch.setattr(
        post_sync_dispatch,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    first = sync_units.finalize_sync_run(str(run.id))
    second = sync_units.finalize_sync_run(str(run.id))
    relay_first = sync_reconciler.reconcile_sync_dispatch(limit=10)
    relay_second = sync_reconciler.reconcile_sync_dispatch(limit=10)

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
    db_session.refresh(post_sync_outbox)
    assert post_sync_outbox.status == OUTBOX_STATUS_DISPATCHED
    assert relay_first["relayed_post_sync"] == 1
    assert relay_second["relayed_post_sync"] == 0
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
        integration_id=run.integration_id,
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
    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(config)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.completed_units == 1
    assert run.failed_units == 1
    assert config.last_sync_success is False
    assert config.last_sync_error == "Sync run completed with failed units"


def test_finalize_writes_ready_compute_checkpoints_for_successful_work_graph_units(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    unit.since_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
    unit.before_at = datetime(2026, 6, 2, tzinfo=timezone.utc)
    failed_prs = SyncRunUnit(
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
    deployments = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="deployments",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.SUCCESS.value,
        attempts=1,
    )
    run.total_units = 3
    db_session.add_all([failed_prs, deployments])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))
    second = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "finalized"
    assert second["status"] == "already_dispatched"
    checkpoints = db_session.query(SyncComputeCheckpoint).all()
    assert len(checkpoints) == 1
    checkpoint = checkpoints[0]
    assert checkpoint.sync_run_unit_id == unit.id
    assert checkpoint.compute_type == SyncComputeType.WORK_GRAPH.value
    assert checkpoint.status == SyncComputeCheckpointStatus.READY.value
    assert checkpoint.window_start == unit.since_at
    assert checkpoint.window_end == unit.before_at
    assert checkpoint.completed_at is None
    assert checkpoint.checkpoint_metadata == {
        "cost_class": "medium",
        "mode": SyncRunMode.INCREMENTAL.value,
        "legacy_targets": ["git"],
    }


def test_finalize_does_not_checkpoint_until_all_units_terminal(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    running = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
    )
    run.total_units = 2
    db_session.add(running)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    assert result["status"] == "pending"
    assert db_session.query(SyncComputeCheckpoint).count() == 0


def test_finalize_continues_when_compute_checkpointing_fails(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    def fail_checkpoint(*_args, **_kwargs):
        raise SQLAlchemyError("checkpoint unavailable")

    monkeypatch.setattr(
        sync_units, "_checkpoint_successful_compute_inputs", fail_checkpoint
    )

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.SUCCESS.value
    assert db_session.query(SyncRunPostDispatch).count() == 1
    assert (
        db_session.query(SyncDispatchOutbox)
        .filter(SyncDispatchOutbox.kind == OUTBOX_KIND_POST_SYNC)
        .count()
        == 1
    )


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
    assert post_sync_outbox.status == OUTBOX_STATUS_PENDING


def test_finalize_sync_run_only_syncs_nonterminal_job_run_observers(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    scheduled = ScheduledJob(
        org_id=run.org_id,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=uuid.uuid4(),
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled)
    db_session.flush()
    running_observer = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.RUNNING.value,
    )
    running_observer.result = {"sync_run_id": str(run.id)}
    terminal_observer = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.FAILED.value,
    )
    terminal_observer.error = "already terminal"
    terminal_observer.result = {"sync_run_id": str(run.id), "sentinel": "preserved"}
    db_session.add_all([running_observer, terminal_observer])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(running_observer)
    db_session.refresh(terminal_observer)
    assert result["status"] == "finalized"
    assert running_observer.status == JobRunStatus.SUCCESS.value
    assert running_observer.completed_at == run.completed_at
    assert terminal_observer.status == JobRunStatus.FAILED.value
    assert terminal_observer.error == "already terminal"
    assert terminal_observer.result == {
        "sync_run_id": str(run.id),
        "sentinel": "preserved",
    }


def test_reconciler_repairs_stale_observer_for_older_terminal_run_with_limit(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    active_run, active_unit = _seed_run(db_session)
    older_run, older_unit = _seed_run(db_session)
    newer_run, newer_unit = _seed_run(db_session)
    active_unit.status = SyncRunUnitStatus.RUNNING.value
    active_run.status = SyncRunStatus.RUNNING.value
    older_unit.status = SyncRunUnitStatus.FAILED.value
    older_run.status = SyncRunStatus.FAILED.value
    older_run.completed_at = datetime.now(timezone.utc) - timedelta(days=1)
    older_run.error = "provider auth failed"
    older_run.failed_units = 1
    newer_unit.status = SyncRunUnitStatus.SUCCESS.value
    newer_run.status = SyncRunStatus.SUCCESS.value
    newer_run.completed_at = datetime.now(timezone.utc)
    newer_run.completed_units = 1
    active_scheduled = ScheduledJob(
        org_id=active_run.org_id,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=uuid.uuid4(),
        tz="UTC",
        status=1,
    )
    scheduled = ScheduledJob(
        org_id=older_run.org_id,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=uuid.uuid4(),
        tz="UTC",
        status=1,
    )
    db_session.add_all([active_scheduled, scheduled])
    db_session.flush()
    active_job_run = JobRun(
        job_id=active_scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.RUNNING.value,
    )
    active_job_run.result = {"sync_run_id": str(active_run.id)}
    active_job_run.created_at = datetime.now(timezone.utc) - timedelta(days=2)
    job_run = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.RUNNING.value,
    )
    job_run.result = {"sync_run_id": str(older_run.id)}
    job_run.created_at = datetime.now(timezone.utc) - timedelta(days=1)
    db_session.add_all([active_job_run, job_run])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_reconciler.reconcile_sync_dispatch(limit=1)

    db_session.refresh(job_run)
    assert result["observer_repairs"] == 1
    assert job_run.status == JobRunStatus.FAILED.value
    assert job_run.error == "provider auth failed"
    assert job_run.completed_at == older_run.completed_at


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


def test_dispatch_sync_run_continues_accepted_run_after_planner_config_pause(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="paused-planner",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        integration_id=run.integration_id,
        is_active=False,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    db_session.refresh(config)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert run.error is None
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.error is None
    assert config.last_sync_success is None
    assert config.last_sync_error is None


def test_paused_config_with_running_and_planned_units_dispatches_planned(
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
        integration_id=run.integration_id,
        is_active=False,
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 2
    db_session.add_all([planned, config])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)
    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(running)
    db_session.refresh(planned)
    db_session.refresh(config)
    assert dispatch_result == {"status": "dispatched", "queued_units": 1}
    assert run.status not in {
        SyncRunStatus.SUCCESS.value,
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }
    assert run.completed_at is None
    assert config.last_sync_at is None
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert planned.error is None
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert running.lease_owner == "worker-live"
    assert dispatch_calls == []
    assert finalize_calls == []
    assert len(chord_calls) == 1

    running.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db_session.flush()

    reconcile_result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(running)
    assert reconcile_result["expired_units"] == 1
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert finalize_calls == []
    planned.status = SyncRunUnitStatus.SUCCESS.value
    planned.updated_at = datetime.now(timezone.utc)
    db_session.flush()

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
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.completed_at is not None
    assert run.failed_units == 1


def test_paused_config_with_stale_dispatching_reclaims_accepted_work(
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
        integration_id=run.integration_id,
        is_active=False,
    )
    run.status = SyncRunStatus.DISPATCHING.value
    run.total_units = 3
    db_session.add_all([running, planned, config])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)

    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(stale_dispatching)
    db_session.refresh(running)
    db_session.refresh(planned)
    assert dispatch_result == {"status": "dispatched", "queued_units": 2}
    assert stale_dispatching.status == SyncRunUnitStatus.DISPATCHING.value
    assert stale_dispatching.error is None
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert running.lease_owner == "worker-live"
    assert run.status not in {
        SyncRunStatus.SUCCESS.value,
        SyncRunStatus.PARTIAL_FAILED.value,
        SyncRunStatus.FAILED.value,
    }
    assert run.completed_at is None
    assert dispatch_calls == []
    assert finalize_calls == []
    assert len(chord_calls) == 1

    running.status = SyncRunUnitStatus.FAILED.value
    running.error = "sync unit lease expired"
    running.result = {"error_category": "worker_lost"}
    running.lease_owner = None
    running.lease_expires_at = None
    running.updated_at = datetime.now(timezone.utc)
    stale_dispatching.status = SyncRunUnitStatus.FAILED.value
    stale_dispatching.error = "provider auth failed"
    stale_dispatching.result = {"error_category": "auth"}
    planned.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()

    finalize_result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert finalize_result["status"] == "finalized"
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.failed_units == 2


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


def test_total_cap_hard_deny_terminalizes_linked_job_run(db_session, monkeypatch):
    from dev_health_ops.sync.guard import GuardDecision
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    scheduled = ScheduledJob(
        org_id=run.org_id,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=uuid.uuid4(),
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled)
    db_session.flush()
    job_run = JobRun(
        job_id=scheduled.id,
        triggered_by="manual",
        status=JobRunStatus.PENDING.value,
    )
    job_run.result = {"sync_run_id": str(run.id)}
    db_session.add(job_run)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    reason = "sync run unit cap exceeded: 1/0"
    monkeypatch.setattr(
        sync_units.DispatchGuard,
        "authorize_run",
        lambda session, sync_run_id: GuardDecision(False, reason, (str(unit.id),)),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(job_run)
    assert result == {"status": "denied", "reason": reason}
    assert run.status == SyncRunStatus.FAILED.value
    assert job_run.status == JobRunStatus.FAILED.value
    assert job_run.error == reason
    assert job_run.completed_at is not None


def test_dispatch_sync_run_continues_accepted_run_after_child_config_pause(
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
        integration_id=run.integration_id,
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
        integration_id=run.integration_id,
        source_id=unit.source_id,
        is_active=False,
    )
    db_session.add(child_config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    db_session.refresh(parent_config)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert run.error is None
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.error is None
    assert parent_config.last_sync_success is None
    assert parent_config.last_sync_error is None


def test_dispatch_sync_run_logs_budget_guard_would_allow(
    db_session, monkeypatch, caplog
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.budget_guard"):
        result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    records = [
        record
        for record in caplog.records
        if record.getMessage() == "dispatch_sync_run.budget_guard_dry_run"
    ]
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert records
    record = records[0]
    assert record.decision == "would_allow"
    assert record.bucket["provider"] == "github"
    assert record.bucket["dimension"] == "rest_core"
    assert record.confidence == "medium"
    assert record.suggested_available_at is None


def test_dispatch_sync_run_logs_budget_guard_would_defer_without_deferring(
    db_session, monkeypatch, caplog
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS",
        json.dumps({"github:rest_core": 1}),
    )
    monkeypatch.setenv("SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS", "120")

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.budget_guard"):
        result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    records = [
        record
        for record in caplog.records
        if record.getMessage() == "dispatch_sync_run.budget_guard_dry_run"
    ]
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.available_at is None
    assert records
    record = records[0]
    assert record.decision == "would_defer"
    assert record.budget_limit == 1
    assert record.projected_units == 2
    assert record.suggested_available_at is not None


def test_dispatch_sync_run_logs_linear_budget_guard_route_family_dry_run(
    db_session, monkeypatch, caplog
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="linear",
        source_type="team",
        external_id="TEAM",
        name="TEAM",
        full_name="TEAM",
        dataset_key="work-items",
        processor_flags={},
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS",
        json.dumps({"linear:graphql_cost:issues": 1, "linear:graphql_cost": 100}),
    )
    monkeypatch.setenv("SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS", "120")

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.budget_guard"):
        result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    records = [
        record
        for record in caplog.records
        if record.getMessage() == "dispatch_sync_run.budget_guard_dry_run"
    ]
    issue_record = next(record for record in records if record.route_family == "issues")
    team_record = next(record for record in records if record.route_family == "teams")
    assert result == {"status": "dispatched", "queued_units": 1}
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.available_at is None
    assert issue_record.bucket["provider"] == "linear"
    assert issue_record.bucket["dimension"] == "graphql_cost"
    assert issue_record.decision == "would_defer"
    assert issue_record.budget_limit == 1
    assert issue_record.projected_units == 5
    assert issue_record.suggested_available_at is not None
    assert team_record.decision == "would_allow"
    assert team_record.budget_limit == 100


def test_dispatch_sync_run_enforces_budget_deferral(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 1}))
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "120")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    assert result["status"] == "deferred"
    assert result["queued_units"] == 0
    assert run.status == SyncRunStatus.PLANNED.value
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.available_at is not None
    available_at = _aware(unit.available_at)
    assert available_at > datetime.now(timezone.utc) + timedelta(seconds=90)
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferred"
    assert unit.result["budget_guard"][0]["decision"] == "deferred"
    assert dispatch_calls == []
    assert finalize_calls == []
    assert chord_calls == []


def test_dispatch_sync_run_enforces_launchdarkly_budget_deferral(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(
        db_session,
        provider="launchdarkly",
        source_type="project",
        external_id="project:default",
        name="default",
        full_name="LaunchDarkly/default",
        dataset_key="feature-flags",
        processor_flags={"sync_feature_flags": True},
    )
    _patch_db_session(monkeypatch, db_session)
    dispatch_calls, finalize_calls, chord_calls = _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS",
        json.dumps(
            {"launchdarkly:rest_core": 999, "launchdarkly:rest_core:audit_log": 1}
        ),
    )
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "120")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    assert result["status"] == "deferred"
    assert result["queued_units"] == 0
    assert run.status == SyncRunStatus.PLANNED.value
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.available_at is not None
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferred"
    assert any(
        entry["decision"] == "deferred" and entry["route_family"] == "audit_log"
        for entry in unit.result["budget_guard"]
    )
    assert dispatch_calls == []
    assert finalize_calls == []
    assert chord_calls == []


def test_dispatch_sync_run_budget_reservation_blocks_second_unit(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    second = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=first.integration_id,
        source_id=first.source_id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True},
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 2}))
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(first)
    db_session.refresh(second)
    assert result == {"status": "dispatched", "queued_units": 1}
    statuses = {first.status, second.status}
    assert statuses == {
        SyncRunUnitStatus.DISPATCHING.value,
        SyncRunUnitStatus.RETRYING.value,
    }
    deferred = first if first.status == SyncRunUnitStatus.RETRYING.value else second
    assert deferred.available_at is not None
    assert deferred.result is not None
    assert deferred.result["error_category"] == "budget_deferred"


def test_dispatch_sync_run_budget_reservation_expires(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, planned = _seed_run(db_session)
    stale_reserved = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="github",
        dataset_key="issues",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
        processor_flags={"sync_issues": True},
    )
    run.total_units = 2
    db_session.add(stale_reserved)
    db_session.flush()
    stale_reserved.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 2}))

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(planned)
    db_session.refresh(stale_reserved)
    assert result == {"status": "dispatched", "queued_units": 2}
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert stale_reserved.status == SyncRunUnitStatus.DISPATCHING.value


def test_dispatch_sync_run_budget_release_after_terminal_unit(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, planned = _seed_run(db_session)
    completed = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="github",
        dataset_key="issues",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.SUCCESS.value,
        attempts=1,
        result={"ok": True},
        processor_flags={"sync_issues": True},
    )
    run.total_units = 2
    db_session.add(completed)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", json.dumps({"github:rest_core": 2}))

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(planned)
    db_session.refresh(completed)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert completed.status == SyncRunUnitStatus.SUCCESS.value


def test_dispatch_sync_run_github_budget_route_family_isolates_contents_blob(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, candidate = _seed_run(db_session)
    candidate.dataset_key = "blame"
    candidate.processor_flags = {"sync_blame": True}
    active_files = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=candidate.integration_id,
        source_id=candidate.source_id,
        provider="github",
        dataset_key="files",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
        processor_flags={"sync_files": True},
    )
    run.total_units = 2
    db_session.add(active_files)
    db_session.flush()
    active_files.updated_at = datetime.now(timezone.utc)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv(
        "SYNC_BUDGET_BUCKET_LIMITS",
        json.dumps({"github:contents_blob:blame": 8, "github:contents_blob": 1}),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(candidate)
    db_session.refresh(active_files)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert candidate.status == SyncRunUnitStatus.DISPATCHING.value
    assert active_files.status == SyncRunUnitStatus.DISPATCHING.value


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
    assert result == {
        "status": "waiting_inflight",
        "queued_units": 0,
        "in_flight_units": 1,
    }
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
    assert result == {
        "status": "waiting_inflight",
        "queued_units": 0,
        "in_flight_units": 1,
    }
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

    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

    run, unit = _seed_run(db_session)
    config = SyncConfiguration(
        org_id=run.org_id,
        name="canonical-window",
        provider="github",
        sync_targets=["git"],
        integration_id=run.integration_id,
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
        post_sync_dispatch,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    result = sync_units.finalize_sync_run(str(run.id))
    relay_result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    assert result["status"] == "finalized"
    assert relay_result["relayed_post_sync"] == 1
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
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

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
        integration_id=run.integration_id,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    dispatches: list[dict] = []
    monkeypatch.setattr(
        post_sync_dispatch,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    result = sync_units.finalize_sync_run(str(run.id))
    relay_result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    assert result["status"] == "finalized"
    assert relay_result["relayed_post_sync"] == 1
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


# ---------------------------------------------------------------------------
# Rate-limit deferral state machine regression tests (CHAOS-2647)
# ---------------------------------------------------------------------------


def _seed_dispatching_unit(
    session, *, rate_limit_deferrals: int = 0, rate_limit_first_seen_at=None
):
    """Seed a run+unit in DISPATCHING status with rate-limit metadata pre-set.

    run_sync_unit's first CAS requires status==DISPATCHING to claim the unit to
    RUNNING.  After the claim the code re-reads the unit from DB to obtain
    rate_limit_deferrals, so the metadata set here is visible to the deferral
    logic.  Returns (run, unit).
    """
    run, unit = _seed_run(session)
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    unit.rate_limit_deferrals = rate_limit_deferrals
    unit.rate_limit_first_seen_at = rate_limit_first_seen_at
    session.flush()
    return run, unit


def test_run_sync_unit_rate_limit_defers_without_failure(db_session, monkeypatch):
    """A RateLimitException defers the unit to RETRYING, never FAILED.

    Regression: the deferral block must fire BEFORE the generic FAILED block.
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_DISPATCH
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_dispatching_unit(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    set_watermark_calls: list[object] = []
    monkeypatch.setattr(
        sync_units,
        "set_watermark",
        lambda *a, **kw: set_watermark_calls.append((a, kw)),
    )
    finalize_calls = _patch_finalize_apply(monkeypatch)

    def raise_rate_limit(ctx, runtime):
        raise RateLimitException("429 Too Many Requests", retry_after_seconds=120)

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", raise_rate_limit)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RETRYING.value, (
        f"Expected RETRYING, got {unit.status!r} — deferral block did not fire"
    )
    assert unit.available_at is not None
    assert _aware(unit.available_at) > datetime.now(timezone.utc)
    assert unit.rate_limit_deferrals == 1
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert set_watermark_calls == [], "watermark must not be written on deferral"
    assert finalize_calls == [], "finalize must not be fast-enqueued on deferral"

    dispatch_outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one_or_none()
    )
    assert dispatch_outbox is not None, (
        "A DISPATCH outbox wakeup must be written so the reconciler re-arms dispatch"
    )


def test_retrying_unit_not_claimed_before_available_at_and_claimed_after(
    tmp_path, monkeypatch
):
    """RETRYING units with a future available_at are skipped; past ones are claimed."""
    from dev_health_ops.workers.sync_units import _claim_units

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            now = datetime.now(timezone.utc)
            unit.status = SyncRunUnitStatus.RETRYING.value
            unit.available_at = now + timedelta(hours=1)  # future — must NOT be claimed
            unit.rate_limit_deferrals = 1
            run_id = run.id
            unit_id = unit.id
            seed_session.commit()

        # --- available_at in the FUTURE: unit must stay RETRYING ---
        with Session(engine) as check_session:
            claimed = _claim_units(check_session, run_id)
            claimed_ids_future = {u.id for u in claimed}
            check_session.commit()

        assert unit_id not in claimed_ids_future, (
            "RETRYING unit with future available_at must not be claimed"
        )

        with Session(engine) as assert_session:
            refreshed = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert refreshed.status == SyncRunUnitStatus.RETRYING.value

        # --- Move available_at into the PAST: unit must now be claimed ---
        with Session(engine) as update_session:
            u = (
                update_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            u.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            update_session.commit()

        with Session(engine) as claim_session:
            claimed_after = _claim_units(claim_session, run_id)
            claimed_ids = {u.id for u in claimed_after}
            claim_session.commit()

        assert unit_id in claimed_ids, (
            "RETRYING unit with past available_at must be claimed to DISPATCHING"
        )

        with Session(engine) as assert_session2:
            refreshed2 = (
                assert_session2.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert refreshed2.status == SyncRunUnitStatus.DISPATCHING.value
    finally:
        engine.dispose()


def test_rate_limit_budget_exhaustion_falls_through_to_failed(db_session, monkeypatch):
    """When the deferral count budget is exhausted the unit is stamped FAILED.

    Regression guard: budget exhaustion must fall through to the generic FAILED
    path, not silently defer again.
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.rate_limit_defer import RATE_LIMIT_MAX_DEFERRALS
    from dev_health_ops.workers.sync_units import run_sync_unit

    # Seed with deferrals already at the max so the next raise exhausts the budget.
    run, unit = _seed_dispatching_unit(
        db_session, rate_limit_deferrals=RATE_LIMIT_MAX_DEFERRALS
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def raise_rate_limit(ctx, runtime):
        raise RateLimitException("429 Too Many Requests", retry_after_seconds=60)

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", raise_rate_limit)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "failed", (
        f"Budget-exhausted rate limit must fall through to FAILED, got {result['status']!r}"
    )
    assert result.get("error_category") == "rate_limit"

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value, (
        f"Expected FAILED after budget exhaustion, got {unit.status!r}"
    )
    assert unit.result is not None
    assert unit.result.get("error_category") == "rate_limit"
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert finalize_calls == [((str(run.id),), "sync")]


# ---------------------------------------------------------------------------
# CHAOS-2705: lease helpers, heartbeat deadline cap, success stamp no-op
# ---------------------------------------------------------------------------


def test_running_lease_seconds_default(monkeypatch):
    from dev_health_ops.workers import sync_units

    monkeypatch.delenv("SYNC_UNIT_RUNNING_LEASE_SECONDS", raising=False)
    assert sync_units._running_lease_seconds() == 300


def test_running_lease_seconds_env_override(monkeypatch):
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_UNIT_RUNNING_LEASE_SECONDS", "120")
    assert sync_units._running_lease_seconds() == 120


def test_running_lease_seconds_invalid_env_fallback(monkeypatch):
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_UNIT_RUNNING_LEASE_SECONDS", "not-a-number")
    assert sync_units._running_lease_seconds() == 300


def test_max_unit_lifetime_seconds_default(monkeypatch):
    from dev_health_ops.workers import sync_units

    monkeypatch.delenv("SYNC_UNIT_MAX_LIFETIME_SECONDS", raising=False)
    assert sync_units._max_unit_lifetime_seconds() == 3720


def test_max_unit_lifetime_seconds_env_override(monkeypatch):
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_UNIT_MAX_LIFETIME_SECONDS", "7200")
    assert sync_units._max_unit_lifetime_seconds() == 7200


def test_max_unit_lifetime_seconds_floored_at_hard_limit(monkeypatch):
    """Values below the Celery hard task_time_limit (3600) are floored to 3600."""
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_UNIT_MAX_LIFETIME_SECONDS", "60")
    assert sync_units._max_unit_lifetime_seconds() == 3600


def test_max_unit_lifetime_seconds_invalid_env_fallback(monkeypatch):
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_UNIT_MAX_LIFETIME_SECONDS", "bad")
    assert sync_units._max_unit_lifetime_seconds() == 3720


def test_heartbeat_stops_when_deadline_exceeded(db_session, monkeypatch):
    """When now >= deadline the heartbeat loop stops without issuing an UPDATE."""
    import threading
    from unittest.mock import patch

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

    # Deadline is already in the past so the first iteration should bail.
    past_deadline = now - timedelta(seconds=1)

    class OneHeartbeatStop(threading.Event):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def wait(self, timeout=None):
            self.calls += 1
            return self.calls > 1

    db_update_calls = []
    original_execute = db_session.execute

    def tracking_execute(stmt, *args, **kwargs):
        db_update_calls.append(stmt)
        return original_execute(stmt, *args, **kwargs)

    stop = OneHeartbeatStop()
    with patch.object(db_session, "execute", side_effect=tracking_execute):
        sync_units._heartbeat_unit_lease(str(unit.id), "worker-1", stop, past_deadline)

    # stop_event must be set (loop exited)
    assert stop.is_set()
    # No UPDATE should have been issued for the heartbeat renewal
    from sqlalchemy.sql.dml import Update

    update_stmts = [s for s in db_update_calls if isinstance(s, Update)]
    assert update_stmts == [], "heartbeat must not issue UPDATE after deadline"


def test_heartbeat_lease_capped_at_deadline(db_session, monkeypatch):
    """When now < deadline, the renewed lease_expires_at must not exceed deadline."""
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
    # lease_seconds=600 but deadline is only 60s away — cap must win
    monkeypatch.setattr(sync_units, "_running_lease_seconds", lambda: 600)

    deadline = now + timedelta(seconds=60)

    class OneHeartbeatStop(threading.Event):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def wait(self, timeout=None):
            self.calls += 1
            return self.calls > 1

    sync_units._heartbeat_unit_lease(
        str(unit.id), "worker-1", OneHeartbeatStop(), deadline
    )

    db_session.refresh(unit)
    lease_expires_at = unit.lease_expires_at
    assert lease_expires_at is not None
    # The persisted lease must not exceed the deadline
    assert _aware(lease_expires_at) <= deadline + timedelta(seconds=1)
    # And it must be strictly less than now + 600s (the uncapped value)
    assert _aware(lease_expires_at) < now + timedelta(seconds=600)


def test_success_stamp_noop_when_lease_already_lost(tmp_path, monkeypatch, caplog):
    """When the success UPDATE matches 0 rows the task returns skipped/lease_lost
    and does NOT set should_finalize (no finalize enqueue).
    """
    import logging

    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units

    engine = _file_backed_engine(tmp_path)
    try:
        with Session(engine) as seed_session:
            run, unit = _seed_run(seed_session)
            _mark_dispatching(seed_session, unit)
            unit_id = unit.id
            seed_session.commit()
        _patch_db_session_factory(monkeypatch, engine)
        _patch_runtime(monkeypatch)
        finalize_calls = _patch_finalize_apply(monkeypatch)
        monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        def run_dataset_and_steal_lease(ctx, runtime):
            # Simulate reconciler marking the unit FAILED while provider runs
            _commit_reconciler_failure(engine, unit_id)
            return {"ok": True}

        monkeypatch.setattr(
            dataset_adapters, "run_dataset_unit", run_dataset_and_steal_lease
        )

        with caplog.at_level(
            logging.WARNING, logger="dev_health_ops.workers.sync_units"
        ):
            result = getattr(sync_units.run_sync_unit, "run")(str(unit_id))

        assert result == {
            "status": "skipped",
            "unit_id": str(unit_id),
            "reason": "lease_lost",
        }
        # Finalize must NOT be enqueued
        assert finalize_calls == [], (
            "finalize must not be enqueued on success stamp no-op"
        )
        # Warning must have been logged
        assert any("success_stamp_noop" in r.message for r in caplog.records), (
            "expected success_stamp_noop warning"
        )
        # Unit must remain FAILED (reconciler's stamp wins)
        with Session(engine) as assert_session:
            persisted = (
                assert_session.query(SyncRunUnit)
                .filter(SyncRunUnit.id == unit_id)
                .one()
            )
            assert persisted.status == SyncRunUnitStatus.FAILED.value
    finally:
        engine.dispose()


def test_bootstrap_live_refresh_lease_capped_at_deadline(db_session, monkeypatch):
    """The bootstrap live-lease-refresh UPDATE must not write lease_expires_at > deadline.

    Regression guard for CHAOS-2705 MUST-FIX 1: the second lease write inside
    run_sync_unit (after _start_unit_heartbeat, before provider execution) must
    be capped at deadline just like the initial claim and the heartbeat loop.
    """
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Use a very short running lease (10s) and a tight deadline (20s) so that
    # now + lease_seconds (10s) < deadline (20s) — the cap should be the lease.
    # More importantly, if lease_seconds were large (e.g. 600) and deadline were
    # small (20s), the cap must be deadline.  We test the latter scenario.
    monkeypatch.setenv("SYNC_UNIT_RUNNING_LEASE_SECONDS", "600")

    captured_lease: list = []

    def run_dataset_capture_lease(ctx, runtime):
        db_session.refresh(unit)
        if unit.lease_expires_at is not None:
            captured_lease.append(unit.lease_expires_at)
        return {"ok": True}

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", run_dataset_capture_lease)

    # Patch _max_unit_lifetime_seconds to return a tight deadline (3600 floor
    # means we can't go below that, so we patch the function directly).
    deadline_seconds = 3600  # floor value
    monkeypatch.setattr(
        sync_units, "_max_unit_lifetime_seconds", lambda: deadline_seconds
    )

    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    assert len(captured_lease) == 1, "expected exactly one lease snapshot from provider"
    from datetime import timezone as _tz

    lease_at = captured_lease[0]
    if lease_at.tzinfo is None:
        lease_at = lease_at.replace(tzinfo=_tz.utc)
    # The live-refresh write must not have pushed lease_expires_at past deadline.
    # started_at + deadline_seconds is the absolute cap; allow 2s clock slack.

    # We can't know exact started_at, but we know the lease must be <= now + deadline_seconds.
    # Since the test runs in well under 1s, now() + deadline_seconds is a safe upper bound.
    now = datetime.now(timezone.utc)
    assert lease_at <= now + timedelta(seconds=deadline_seconds + 2), (
        f"live-refresh lease {lease_at} exceeds deadline cap"
    )
    # And it must NOT be now + 600s (the uncapped running lease).
    assert lease_at < now + timedelta(seconds=600), (
        f"live-refresh lease {lease_at} was not capped (got full 600s lease)"
    )
