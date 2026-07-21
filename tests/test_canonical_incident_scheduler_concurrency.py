from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event, Lock
from time import monotonic_ns
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import Base, JobStatus, ScheduledJob, SyncConfiguration
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride
from dev_health_ops.models.users import Organization
from tests.canonical_incident_orchestration_support import (
    FEATURE_KEY,
    CanonicalState,
    create_canonical_graph,
)

NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def _seed_due_schedule(session: Session) -> tuple[uuid.UUID, uuid.UUID]:
    org_id = uuid.uuid4()
    feature = session.query(FeatureFlag).filter_by(key=FEATURE_KEY).one_or_none()
    if feature is None:
        feature = FeatureFlag(
            key=FEATURE_KEY,
            name="Canonical Incident Ingestion",
            category="integrations",
            min_tier="community",
            is_enabled=True,
        )
        session.add(feature)
    session.add(
        Organization(
            id=org_id,
            slug=f"scheduler-race-{org_id}",
            name="Scheduler Race",
            tier="enterprise",
        )
    )
    session.flush()
    override = OrgFeatureOverride(
        org_id=org_id,
        feature_id=feature.id,
        is_enabled=True,
    )
    session.add(override)
    session.commit()
    state = CanonicalState(
        session=session,
        enabled_org_id=org_id,
        disabled_org_id=uuid.uuid4(),
        feature_id=feature.id,
    )
    graph = create_canonical_graph(state, org_id, with_config=True)
    assert graph.config is not None
    session.add(
        ScheduledJob(
            name=f"sync-config-{graph.config.id}",
            job_type="sync",
            schedule_cron="* * * * *",
            org_id=str(org_id),
            provider="pagerduty",
            sync_config_id=graph.config.id,
            tz="UTC",
            status=JobStatus.ACTIVE.value,
        )
    )
    session.commit()
    return graph.config.id, override.id


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_no_apply_async_after_concurrent_disable_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("psycopg2")
    from dev_health_ops.sync import canonical_incident_gate
    from dev_health_ops.workers import sync_scheduler, sync_units

    engine = create_engine(os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"])
    Base.metadata.create_all(engine)
    with Session(engine) as setup:
        config_id, override_id = _seed_due_schedule(setup)

    final_gate_passed = Event()
    disable_committed = Event()
    timestamp_lock = Lock()
    timestamps: dict[str, int] = {}
    real_require = canonical_incident_gate.require_canonical_incident_feature_sync
    dispatch = MagicMock()

    def pause_after_final_gate(session: Session, org_id: str | uuid.UUID) -> None:
        real_require(session, org_id)
        final_gate_passed.set()
        disable_committed.wait(timeout=2)

    def record_enqueue(*_args, **_kwargs) -> None:
        with timestamp_lock:
            timestamps["enqueue"] = monotonic_ns()

    def dispatch_schedule() -> bool:
        with Session(engine) as session:
            config = session.get(SyncConfiguration, config_id)
            assert config is not None
            return sync_scheduler._maybe_dispatch_config(session, config, NOW)

    def remove_override() -> None:
        assert final_gate_passed.wait(timeout=10)
        with Session(engine) as session:
            override = session.get(OrgFeatureOverride, override_id)
            assert override is not None
            session.delete(override)
            session.commit()
        with timestamp_lock:
            timestamps["disable_commit"] = monotonic_ns()
        disable_committed.set()

    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_args: True)
    monkeypatch.setattr(
        sync_scheduler,
        "require_canonical_incident_feature_sync",
        pause_after_final_gate,
    )
    monkeypatch.setattr(sync_units, "dispatch_sync_run", dispatch)
    dispatch.apply_async.side_effect = record_enqueue

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            schedule_future = executor.submit(dispatch_schedule)
            disable_future = executor.submit(remove_override)
            dispatched = schedule_future.result(timeout=30)
            disable_future.result(timeout=30)

        enqueue_at = timestamps.get("enqueue")
        disable_commit_at = timestamps["disable_commit"]
        assert dispatched is True
        assert enqueue_at is None or enqueue_at < disable_commit_at
    finally:
        engine.dispose()


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_disable_commit_before_scheduler_prevents_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("psycopg2")
    from dev_health_ops.workers import sync_scheduler, sync_units

    engine = create_engine(os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"])
    Base.metadata.create_all(engine)
    with Session(engine) as setup:
        config_id, override_id = _seed_due_schedule(setup)
    disable_committed = Event()
    dispatch = MagicMock()

    def remove_override() -> None:
        with Session(engine) as session:
            override = session.get(OrgFeatureOverride, override_id)
            assert override is not None
            session.delete(override)
            session.commit()
        disable_committed.set()

    def dispatch_schedule() -> bool:
        assert disable_committed.wait(timeout=10)
        with Session(engine) as session:
            config = session.get(SyncConfiguration, config_id)
            assert config is not None
            return sync_scheduler._maybe_dispatch_config(session, config, NOW)

    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_args: True)
    monkeypatch.setattr(sync_units, "dispatch_sync_run", dispatch)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            disable_future = executor.submit(remove_override)
            schedule_future = executor.submit(dispatch_schedule)
            disable_future.result(timeout=30)
            dispatched = schedule_future.result(timeout=30)

        assert dispatched is False
        dispatch.apply_async.assert_not_called()
    finally:
        engine.dispose()
