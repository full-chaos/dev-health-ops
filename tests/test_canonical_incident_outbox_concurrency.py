from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    SyncConfiguration,
    SyncDispatchOutbox,
    SyncDispatchTransportRoute,
    SyncRun,
    SyncRunStatus,
)
from dev_health_ops.models.licensing import OrgFeatureOverride
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    ClaimedOutboxRow,
    claim_due_outbox_rows,
    lock_outbox_claim_for_publish,
    mark_outbox_dispatched,
)
from tests.test_canonical_incident_scheduler_concurrency import _seed_due_schedule


@dataclass(frozen=True, slots=True)
class ClaimedDiscovery:
    row: ClaimedOutboxRow
    override_id: uuid.UUID


def _seed_claimed_discovery(
    engine,
    monkeypatch: pytest.MonkeyPatch,
) -> ClaimedDiscovery:
    from dev_health_ops.workers import sync_scheduler

    with Session(engine) as session:
        for kind in (
            "dispatch_sync_run",
            "finalize_sync_run",
            "post_sync",
            "reference_discovery",
        ):
            if session.get(SyncDispatchTransportRoute, kind) is None:
                session.add(
                    SyncDispatchTransportRoute(
                        kind=kind,
                        transport="celery",
                        generation=1,
                        paused=False,
                        paused_at=None,
                        rollback_transport="celery",
                    )
                )
        config_id, override_id = _seed_due_schedule(session)
        config = session.get(SyncConfiguration, config_id)
        assert config is not None
        monkeypatch.setattr(
            sync_scheduler,
            "organization_exists_sync",
            lambda *_args: True,
        )
        assert sync_scheduler._maybe_dispatch_config(
            session,
            config,
            datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
        )
        run = session.query(SyncRun).filter_by(org_id=str(config.org_id)).one()
        outbox = (
            session.query(SyncDispatchOutbox)
            .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISCOVERY)
            .one()
        )
        claimed = claim_due_outbox_rows(
            session,
            now=datetime.now(timezone.utc),
            limit=100,
        )
        row = next(candidate for candidate in claimed if candidate.id == outbox.id)
        session.commit()
    return ClaimedDiscovery(row=row, override_id=override_id)


def _publish_discovery(
    session: Session, row: ClaimedOutboxRow, discovery
) -> str | None:
    from dev_health_ops.workers import sync_reconciler

    assert lock_outbox_claim_for_publish(session, row.id, row.claim_token)
    relayed = sync_reconciler._publish_claimed_outbox_row(
        session,
        row=row,
        stale_dispatch_cutoff=datetime.now(timezone.utc),
        dispatch_sync_run=MagicMock(),
        finalize_sync_run=MagicMock(),
        run_sync_reference_discovery=discovery,
        upsert_outbox_wakeup=MagicMock(),
        build_post_sync_dispatch_payload=MagicMock(),
        dispatch_post_sync_tasks=MagicMock(),
    )
    marked = mark_outbox_dispatched(
        session,
        row_id=row.id,
        claim_token=row.claim_token,
    )
    session.commit()
    if relayed is not None:
        assert marked is True
    return relayed


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_disable_commit_waits_for_outbox_publish_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("psycopg2")
    from dev_health_ops.sync.canonical_incident_gate import (
        CanonicalIncidentFeatureDisabledError,
        require_canonical_incident_feature_for_update_sync,
    )

    engine = create_engine(os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"])
    Base.metadata.create_all(engine)
    claimed = _seed_claimed_discovery(engine, monkeypatch)
    publish_entered = Event()
    disable_flushing = Event()
    worker_started = Event()
    disable_committed = Event()
    disable_visible_during_publish: list[bool] = []
    provider_calls: list[str] = []
    discovery = MagicMock()

    def pause_publish(*_args, **_kwargs) -> None:
        publish_entered.set()
        assert disable_flushing.wait(timeout=10)
        assert worker_started.wait(timeout=10)
        disable_visible_during_publish.append(disable_committed.wait(timeout=2))

    def publish() -> str | None:
        with Session(engine) as session:
            return _publish_discovery(session, claimed.row, discovery)

    def disable() -> None:
        assert publish_entered.wait(timeout=10)
        with Session(engine) as session:
            override = session.get(OrgFeatureOverride, claimed.override_id)
            assert override is not None
            # Canonical incident ingestion is tier-default-on. Deleting the
            # override restores that default, so denial must be an explicit
            # false override.
            override.is_enabled = False
            disable_flushing.set()
            session.flush()
            session.commit()
        disable_committed.set()

    def worker_recheck() -> bool:
        assert disable_flushing.wait(timeout=10)
        worker_started.set()
        with Session(engine) as session:
            try:
                require_canonical_incident_feature_for_update_sync(
                    session,
                    claimed.row.org_id,
                )
            except CanonicalIncidentFeatureDisabledError:
                return False
        provider_calls.append("called")
        return True

    discovery.apply_async.side_effect = pause_publish
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            publish_future = executor.submit(publish)
            disable_future = executor.submit(disable)
            worker_future = executor.submit(worker_recheck)
            assert publish_future.result(timeout=30) == OUTBOX_KIND_DISCOVERY
            disable_future.result(timeout=30)
            assert worker_future.result(timeout=30) is False

        assert disable_visible_during_publish == [False]
        assert disable_committed.is_set()
        assert provider_calls == []
        discovery.apply_async.assert_called_once()
    finally:
        engine.dispose()


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_disable_commit_before_outbox_lock_terminalizes_without_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("psycopg2")
    engine = create_engine(os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"])
    Base.metadata.create_all(engine)
    claimed = _seed_claimed_discovery(engine, monkeypatch)
    discovery = MagicMock()
    try:
        with Session(engine) as session:
            override = session.get(OrgFeatureOverride, claimed.override_id)
            assert override is not None
            override.is_enabled = False
            session.commit()
        with Session(engine) as session:
            assert _publish_discovery(session, claimed.row, discovery) is None

        with Session(engine) as session:
            run = session.get(SyncRun, claimed.row.sync_run_id)
            outbox = session.get(SyncDispatchOutbox, claimed.row.id)
            assert run is not None
            assert run.status == SyncRunStatus.FAILED.value
            assert outbox is not None
            assert outbox.status == "dispatched"
        discovery.apply_async.assert_not_called()
    finally:
        engine.dispose()
