from __future__ import annotations

import inspect
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest
from sqlalchemy import create_engine, event, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    SyncDispatchOutbox,
    SyncDispatchTransportRoute,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    OUTBOX_STATUS_DISPATCHED,
    OUTBOX_STATUS_PENDING,
    SYNC_DISPATCH_PARITY_DIGEST_VERSION,
    SYNC_DISPATCH_PARITY_EVENT,
    SYNC_DISPATCH_PARITY_PREDICATE_VERSION,
    ClaimedOutboxRow,
    SyncDispatchParityObservationUnavailable,
    backoff_seconds,
    claim_due_outbox_rows,
    lock_outbox_claim_for_publish,
    mark_outbox_dispatched,
    mark_outbox_publish_failed,
    observe_due_outbox_rows,
    upsert_outbox_wakeup,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        _seed_transport_routes(session)
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


def _seed_transport_routes(session):
    for kind in (
        "dispatch_sync_run",
        "finalize_sync_run",
        "post_sync",
        "reference_discovery",
    ):
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
    session.flush()


def _seed_run(session):
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
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.DISPATCHING.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
    )
    session.add(run)
    session.flush()
    return run


def _seed_outbox(session, *, available_at, kind=OUTBOX_KIND_DISPATCH):
    run = _seed_run(session)
    upsert_outbox_wakeup(
        session,
        sync_run_id=run.id,
        kind=kind,
        available_at=available_at,
    )
    return session.query(SyncDispatchOutbox).filter_by(sync_run_id=run.id).one()


def _aware(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def test_observe_due_outbox_rows_matches_claim_window_and_go_digest(db_session):
    now = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)
    dispatch = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=3),
        kind=OUTBOX_KIND_DISPATCH,
    )
    post_sync = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=2),
        kind="post_sync",
    )
    finalize = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=1),
        kind=OUTBOX_KIND_FINALIZE,
    )
    live_claim = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=4),
        kind="reference_discovery",
    )
    dispatch.id = uuid.UUID("00000000-0000-4000-8000-000000000001")
    post_sync.id = uuid.UUID("00000000-0000-4000-8000-000000000002")
    finalize.id = uuid.UUID("00000000-0000-4000-8000-000000000003")
    live_claim.id = uuid.UUID("00000000-0000-4000-8000-000000000004")
    dispatch.claim_token = "expired-claim"
    dispatch.claim_expires_at = now - timedelta(seconds=1)
    dispatch.claim_transport = "celery"
    dispatch.claim_route_generation = 1
    live_claim.claim_token = "live-claim"
    live_claim.claim_expires_at = now + timedelta(seconds=1)
    live_claim.claim_transport = "celery"
    live_claim.claim_route_generation = 1
    db_session.flush()

    statements: list[str] = []

    def record_statement(_conn, _cursor, statement, _parameters, _context, _many):
        statements.append(statement)

    engine = db_session.get_bind()
    event.listen(engine, "before_cursor_execute", record_statement)
    try:
        record = observe_due_outbox_rows(db_session, now=now, limit=2)
    finally:
        event.remove(engine, "before_cursor_execute", record_statement)

    assert record == {
        "event": SYNC_DISPATCH_PARITY_EVENT,
        "runtime": "celery",
        "observed_at": "2026-07-22T12:00:00.000000000Z",
        "limit": 2,
        "predicate_version": SYNC_DISPATCH_PARITY_PREDICATE_VERSION,
        "digest_version": SYNC_DISPATCH_PARITY_DIGEST_VERSION,
        "candidate_digest": (
            "sha256:6bc27cbf7ac850d910ad225ac42fdafd287b1fb5254333621d6ee32294771545"
        ),
        "sampled_candidates": 2,
        "truncated": True,
        "unknown_kind_count": 0,
        "celery_due_pending": 2,
        "river_due_pending": 0,
        "kinds": [
            {
                "kind": "dispatch_sync_run",
                "route": "celery",
                "due_pending": 1,
                "expired_claims": 1,
            },
            {
                "kind": "finalize_sync_run",
                "route": "celery",
                "due_pending": 0,
                "expired_claims": 0,
            },
            {
                "kind": "post_sync",
                "route": "celery",
                "due_pending": 1,
                "expired_claims": 0,
            },
            {
                "kind": "reference_discovery",
                "route": "celery",
                "due_pending": 0,
                "expired_claims": 0,
            },
        ],
    }
    observed_sql = "\n".join(statements).upper()
    for fragment in (
        "SELECT",
        "JOIN SYNC_DISPATCH_TRANSPORT_ROUTES",
        "SYNC_DISPATCH_TRANSPORT_ROUTES.KIND = SYNC_DISPATCH_OUTBOX.KIND",
        "STATUS =",
        "AVAILABLE_AT <=",
        "TRANSPORT =",
        "PAUSED IS 0",
        "CLAIM_EXPIRES_AT IS NULL",
        "ORDER BY",
        "AVAILABLE_AT, SYNC_DISPATCH_OUTBOX.ID",
        "LIMIT",
    ):
        assert fragment in observed_sql
    assert not any(
        statement.lstrip().upper().startswith("UPDATE") for statement in statements
    )
    assert dispatch.claim_token == "expired-claim"
    assert post_sync.claim_token is None
    assert finalize.claim_token is None
    assert live_claim.claim_token == "live-claim"


def test_observe_due_outbox_rows_mirrors_active_celery_route_fence_and_redacts(
    db_session,
):
    now = datetime(2026, 7, 22, 12, 0, 0, 123456, tzinfo=timezone.utc)
    unknown = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=4),
        kind="future_contract_kind",
    )
    river = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=3),
        kind=OUTBOX_KIND_FINALIZE,
    )
    paused = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=2),
        kind="post_sync",
    )
    known = _seed_outbox(
        db_session,
        available_at=now - timedelta(seconds=1),
        kind=OUTBOX_KIND_DISPATCH,
    )
    db_session.query(SyncDispatchTransportRoute).filter_by(
        kind=OUTBOX_KIND_FINALIZE
    ).update({"transport": "river", "generation": 2})
    db_session.query(SyncDispatchTransportRoute).filter_by(kind="post_sync").update(
        {"paused": True, "paused_at": now}
    )
    db_session.flush()
    record = observe_due_outbox_rows(db_session, now=now, limit=1)

    assert record["observed_at"] == "2026-07-22T12:00:00.123456000Z"
    assert record["unknown_kind_count"] == 0
    assert record["celery_due_pending"] == 1
    assert record["sampled_candidates"] == 1
    assert record["truncated"] is False
    rendered = repr(record)
    for forbidden in (
        str(unknown.id),
        str(river.id),
        str(paused.id),
        str(known.id),
        unknown.org_id,
        str(unknown.sync_run_id),
    ):
        assert forbidden not in rendered
    for limit in (0, 101, True):
        with pytest.raises(
            SyncDispatchParityObservationUnavailable, match="invalid_limit"
        ):
            observe_due_outbox_rows(db_session, now=now, limit=limit)


def test_upsert_insert_creates_pending_row(db_session):
    run = _seed_run(db_session)
    available_at = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)

    with _fake_session_ctx(db_session):
        upsert_outbox_wakeup(
            db_session,
            sync_run_id=run.id,
            kind=OUTBOX_KIND_DISPATCH,
            available_at=available_at,
        )

    row = db_session.query(SyncDispatchOutbox).one()
    assert row.org_id == run.org_id
    assert row.sync_run_id == run.id
    assert row.kind == OUTBOX_KIND_DISPATCH
    assert row.status == OUTBOX_STATUS_PENDING
    assert _aware(row.available_at) == available_at
    assert row.attempts == 0
    assert row.claim_token is None


def test_upsert_derives_org_id_from_sync_run_for_claimed_row(db_session):
    assert "org_id" not in inspect.signature(upsert_outbox_wakeup).parameters
    run = _seed_run(db_session)
    available_at = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)

    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=available_at,
    )

    claimed = claim_due_outbox_rows(db_session, now=available_at, limit=1)
    assert len(claimed) == 1
    assert claimed[0].org_id == run.org_id
    row = db_session.query(SyncDispatchOutbox).filter_by(sync_run_id=run.id).one()
    assert row.org_id == run.org_id


def test_upsert_raises_for_missing_sync_run(db_session):
    missing_run_id = uuid.uuid4()

    with pytest.raises(ValueError, match="sync_run not found for outbox wakeup"):
        upsert_outbox_wakeup(
            db_session,
            sync_run_id=missing_run_id,
            kind=OUTBOX_KIND_DISPATCH,
            available_at=datetime(2026, 6, 21, 12, tzinfo=timezone.utc),
        )


def test_upsert_idempotent_for_same_run_and_kind(db_session):
    run = _seed_run(db_session)
    available_at = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)

    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=available_at,
    )
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=available_at + timedelta(minutes=1),
    )

    rows = db_session.query(SyncDispatchOutbox).all()
    assert len(rows) == 1
    assert rows[0].sync_run_id == run.id
    assert rows[0].kind == OUTBOX_KIND_DISPATCH


def test_upsert_conflict_rearms_dispatched_and_keeps_earliest_available_at(
    db_session,
):
    run = _seed_run(db_session)
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    existing_available_at = now + timedelta(minutes=20)
    earlier_available_at = now + timedelta(minutes=5)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_FINALIZE,
        available_at=existing_available_at,
    )
    row = db_session.query(SyncDispatchOutbox).one()
    row.status = OUTBOX_STATUS_DISPATCHED
    row.dispatched_at = now
    row.dispatched_transport = "celery"
    row.dispatched_route_generation = 1
    row.claim_token = "old-token"
    row.claim_expires_at = now - timedelta(minutes=1)
    row.claim_transport = "celery"
    row.claim_route_generation = 1
    row.attempts = 3
    db_session.flush()

    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_FINALIZE,
        available_at=earlier_available_at,
        now=now,
    )

    db_session.refresh(row)
    assert row.status == OUTBOX_STATUS_PENDING
    assert _aware(row.available_at) == earlier_available_at
    assert row.claim_token is None
    assert row.claim_expires_at is None
    assert row.dispatched_at is None
    assert row.attempts == 3


def test_upsert_preserves_dispatched_feature_denial(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    row.status = OUTBOX_STATUS_DISPATCHED
    row.dispatched_at = now
    row.last_error = "feature_disabled"
    db_session.flush()

    upsert_outbox_wakeup(
        db_session,
        sync_run_id=row.sync_run_id,
        kind=row.kind,
        available_at=now - timedelta(minutes=5),
        now=now + timedelta(minutes=1),
    )

    db_session.refresh(row)
    assert row.status == OUTBOX_STATUS_DISPATCHED
    assert _aware(row.dispatched_at) == now
    assert row.last_error == "feature_disabled"
    assert row.claim_token is None
    assert row.dispatched_transport is None
    assert row.dispatched_route_generation is None
    assert row.transport_job_id is None


def test_dispatched_feature_denial_rejects_route_audit_metadata(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    row.status = OUTBOX_STATUS_DISPATCHED
    row.dispatched_at = now
    row.last_error = "feature_disabled"
    row.dispatched_transport = "celery"
    row.dispatched_route_generation = 1
    row.transport_job_id = "celery-task-123"

    with pytest.raises(IntegrityError):
        db_session.flush()


def test_upsert_rearm_preserves_live_claim(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    db_session.refresh(row)
    token = claimed[0].claim_token
    claim_expires_at = _aware(row.claim_expires_at)

    upsert_outbox_wakeup(
        db_session,
        sync_run_id=row.sync_run_id,
        kind=row.kind,
        available_at=now - timedelta(minutes=5),
        now=now + timedelta(seconds=10),
    )

    db_session.refresh(row)
    assert row.status == OUTBOX_STATUS_PENDING
    assert _aware(row.available_at) == now - timedelta(minutes=5)
    assert row.claim_token == token
    assert _aware(row.claim_expires_at) == claim_expires_at


def test_upsert_rearm_clears_expired_claim(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now + timedelta(minutes=20))
    row.claim_token = "expired-token"
    row.claim_expires_at = now - timedelta(seconds=1)
    row.claim_transport = "celery"
    row.claim_route_generation = 1
    db_session.flush()

    upsert_outbox_wakeup(
        db_session,
        sync_run_id=row.sync_run_id,
        kind=row.kind,
        available_at=now + timedelta(minutes=5),
        now=now,
    )

    db_session.refresh(row)
    assert row.status == OUTBOX_STATUS_PENDING
    assert _aware(row.available_at) == now + timedelta(minutes=5)
    assert row.claim_token is None
    assert row.claim_expires_at is None


def test_claim_sets_token_expiry_attempts_and_waits_for_expiry(db_session, monkeypatch):
    monkeypatch.delenv("SYNC_OUTBOX_CLAIM_TIMEOUT_SECONDS", raising=False)
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now - timedelta(seconds=1))

    claimed = claim_due_outbox_rows(db_session, now=now, limit=10)

    assert len(claimed) == 1
    assert claimed[0].id == row.id
    first_token = claimed[0].claim_token
    db_session.refresh(row)
    assert first_token is not None
    assert row.claim_token == first_token
    assert row.claim_transport == "celery"
    assert row.claim_route_generation == 1
    assert claimed[0].transport == "celery"
    assert claimed[0].route_generation == 1
    assert _aware(row.claim_expires_at) == now + timedelta(seconds=300)
    assert row.attempts == 1

    assert (
        claim_due_outbox_rows(db_session, now=now + timedelta(seconds=299), limit=10)
        == []
    )

    claimed_after_expiry = claim_due_outbox_rows(
        db_session, now=now + timedelta(seconds=300), limit=10
    )

    assert len(claimed_after_expiry) == 1
    assert claimed_after_expiry[0].id == row.id
    db_session.refresh(row)
    assert claimed_after_expiry[0].claim_token != first_token
    assert row.claim_token == claimed_after_expiry[0].claim_token
    assert row.attempts == 2


def test_sequential_claims_do_not_double_claim_unexpired_row(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)

    first_claim = claim_due_outbox_rows(db_session, now=now, limit=10)
    second_claim = claim_due_outbox_rows(db_session, now=now, limit=10)

    assert len(first_claim) == 1
    assert first_claim[0].id == row.id
    assert second_claim == []


def test_claim_requires_active_celery_route_and_binds_generation(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    route = db_session.get(SyncDispatchTransportRoute, OUTBOX_KIND_DISPATCH)
    assert route is not None

    route.transport = "river"
    route.generation = 2
    db_session.flush()
    assert claim_due_outbox_rows(db_session, now=now, limit=1) == []

    route.transport = "celery"
    route.generation = 3
    route.paused = True
    route.paused_at = now
    db_session.flush()
    assert claim_due_outbox_rows(db_session, now=now, limit=1) == []

    route.paused = False
    route.paused_at = None
    route.generation = 4
    db_session.flush()
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)

    assert len(claimed) == 1
    assert claimed[0].id == row.id
    assert claimed[0].transport == "celery"
    assert claimed[0].route_generation == 4
    db_session.refresh(row)
    assert row.claim_transport == "celery"
    assert row.claim_route_generation == 4


def test_claim_fails_closed_when_route_row_is_missing(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    route = db_session.get(SyncDispatchTransportRoute, OUTBOX_KIND_DISPATCH)
    assert route is not None
    db_session.delete(route)
    db_session.flush()

    assert claim_due_outbox_rows(db_session, now=now, limit=1) == []

    db_session.refresh(row)
    assert row.claim_token is None
    assert row.claim_transport is None
    assert row.claim_route_generation is None
    assert row.attempts == 0


def test_route_change_fences_publish_lock_success_and_failure_marks(db_session):
    now = datetime.now(timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claim = claim_due_outbox_rows(db_session, now=now, limit=1)[0]
    route = db_session.get(SyncDispatchTransportRoute, OUTBOX_KIND_DISPATCH)
    assert route is not None
    route.paused = True
    route.paused_at = now
    route.generation += 1
    db_session.flush()

    assert lock_outbox_claim_for_publish(db_session, row.id, claim.claim_token) is False
    assert (
        mark_outbox_dispatched(
            db_session,
            row_id=row.id,
            claim_token=claim.claim_token,
            now=now + timedelta(seconds=1),
        )
        is False
    )
    assert (
        mark_outbox_publish_failed(
            db_session,
            row_id=row.id,
            claim_token=claim.claim_token,
            error="broker unavailable",
            attempts=claim.attempts,
            now=now + timedelta(seconds=1),
        )
        is False
    )
    db_session.refresh(row)
    assert row.status == OUTBOX_STATUS_PENDING
    assert row.claim_token == claim.claim_token
    assert row.claim_transport == "celery"
    assert row.claim_route_generation == 1


def test_publish_lock_rejects_wrong_and_null_lease_owner(db_session):
    now = datetime.now(timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token

    assert lock_outbox_claim_for_publish(db_session, row.id, "wrong-token") is False

    db_session.refresh(row)
    row.claim_token = None
    row.claim_expires_at = None
    row.claim_transport = None
    row.claim_route_generation = None
    db_session.flush()
    assert lock_outbox_claim_for_publish(db_session, row.id, token) is False


def test_publish_lock_rejects_repeated_dispatch_after_mark(db_session):
    now = datetime.now(timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token

    assert lock_outbox_claim_for_publish(db_session, row.id, token) is True
    assert (
        mark_outbox_dispatched(
            db_session,
            row_id=row.id,
            claim_token=token,
        )
        is True
    )
    assert lock_outbox_claim_for_publish(db_session, row.id, token) is False


def test_publish_lock_remains_retryable_after_transaction_rollback(db_session):
    now = datetime.now(timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token
    db_session.commit()

    assert lock_outbox_claim_for_publish(db_session, row.id, token) is True
    db_session.rollback()

    assert lock_outbox_claim_for_publish(db_session, row.id, token) is True


def test_claim_returns_authoritative_post_increment_attempts(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    row.attempts = 2
    db_session.flush()

    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)

    assert len(claimed) == 1
    assert isinstance(claimed[0], ClaimedOutboxRow)
    assert claimed[0].id == row.id
    assert claimed[0].attempts == 3
    db_session.refresh(row)
    assert row.attempts == 3


def test_expired_lease_reclaim_is_at_least_once_and_db_consistent(
    db_session, monkeypatch
):
    monkeypatch.delenv("SYNC_OUTBOX_CLAIM_TIMEOUT_SECONDS", raising=False)
    t0 = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=t0)

    worker_a_claims = claim_due_outbox_rows(db_session, now=t0, limit=10)
    assert len(worker_a_claims) == 1
    worker_a_claim = worker_a_claims[0]
    db_session.refresh(row)
    worker_a_expires_at = _aware(row.claim_expires_at)

    t1 = worker_a_expires_at + timedelta(seconds=1)
    worker_b_claims = claim_due_outbox_rows(db_session, now=t1, limit=10)
    assert len(worker_b_claims) == 1
    worker_b_claim = worker_b_claims[0]
    assert worker_b_claim.id == worker_a_claim.id
    assert worker_b_claim.claim_token != worker_a_claim.claim_token
    assert worker_b_claim.attempts == worker_a_claim.attempts + 1
    db_session.refresh(row)
    worker_b_expires_at = _aware(row.claim_expires_at)
    assert row.claim_token == worker_b_claim.claim_token

    stale_mark = mark_outbox_dispatched(
        db_session,
        row_id=worker_a_claim.id,
        claim_token=worker_a_claim.claim_token,
        now=t1,
    )

    db_session.refresh(row)
    assert stale_mark is False
    assert row.status == OUTBOX_STATUS_PENDING
    assert row.claim_token == worker_b_claim.claim_token
    assert _aware(row.claim_expires_at) == worker_b_expires_at
    assert row.dispatched_at is None

    live_mark = mark_outbox_dispatched(
        db_session,
        row_id=worker_b_claim.id,
        claim_token=worker_b_claim.claim_token,
        now=t1,
    )

    db_session.refresh(row)
    assert live_mark is True
    assert row.status == OUTBOX_STATUS_DISPATCHED
    assert row.claim_token is None
    assert row.claim_expires_at is None


def test_mark_outbox_dispatched_is_token_guarded(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token
    assert token is not None

    wrong_token_result = mark_outbox_dispatched(
        db_session,
        row_id=row.id,
        claim_token="wrong-token",
        now=now + timedelta(seconds=1),
    )

    db_session.refresh(row)
    assert wrong_token_result is False
    assert row.status == OUTBOX_STATUS_PENDING
    assert row.claim_token == token
    assert row.dispatched_at is None

    correct_token_result = mark_outbox_dispatched(
        db_session,
        row_id=row.id,
        claim_token=token,
        now=now + timedelta(seconds=2),
    )

    db_session.refresh(row)
    assert correct_token_result is True
    assert row.status == OUTBOX_STATUS_DISPATCHED
    assert _aware(row.dispatched_at) == now + timedelta(seconds=2)
    assert row.claim_token is None
    assert row.claim_expires_at is None
    assert row.claim_transport is None
    assert row.claim_route_generation is None
    assert row.dispatched_transport == "celery"
    assert row.dispatched_route_generation == 1
    assert row.last_error is None


def test_mark_outbox_dispatched_persists_optional_transport_job_id(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claim = claim_due_outbox_rows(db_session, now=now, limit=1)[0]

    assert mark_outbox_dispatched(
        db_session,
        row_id=row.id,
        claim_token=claim.claim_token,
        now=now + timedelta(seconds=1),
        transport_job_id="celery-task-123",
    )

    db_session.refresh(row)
    assert row.dispatched_transport == "celery"
    assert row.dispatched_route_generation == 1
    assert row.transport_job_id == "celery-task-123"


def test_mark_outbox_dispatched_rejects_expired_lease(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token

    result = mark_outbox_dispatched(
        db_session,
        row_id=row.id,
        claim_token=token,
        now=now + timedelta(seconds=301),
    )

    db_session.refresh(row)
    assert result is False
    assert row.status == OUTBOX_STATUS_PENDING
    assert row.claim_token == token
    assert row.dispatched_at is None


def test_mark_outbox_publish_failed_rearms_with_backoff_and_error(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token
    assert token is not None

    result = mark_outbox_publish_failed(
        db_session,
        row_id=row.id,
        claim_token=token,
        error=RuntimeError("broker down"),
        attempts=claimed[0].attempts,
        now=now + timedelta(seconds=5),
    )

    db_session.refresh(row)
    assert result is True
    assert row.status == OUTBOX_STATUS_PENDING
    assert row.claim_token is None
    assert row.claim_expires_at is None
    assert row.claim_transport is None
    assert row.claim_route_generation is None
    assert _aware(row.available_at) == now + timedelta(seconds=65)
    assert row.last_error == "RuntimeError: broker down"


def test_mark_outbox_publish_failed_rejects_expired_lease(db_session):
    now = datetime(2026, 6, 21, 12, tzinfo=timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token

    result = mark_outbox_publish_failed(
        db_session,
        row_id=row.id,
        claim_token=token,
        error=RuntimeError("broker down"),
        attempts=claimed[0].attempts,
        now=now + timedelta(seconds=301),
    )

    db_session.refresh(row)
    assert result is False
    assert row.status == OUTBOX_STATUS_PENDING
    assert row.claim_token == token
    assert row.last_error is None


def test_backoff_seconds_sequence_is_capped():
    assert [backoff_seconds(attempts) for attempts in range(1, 7)] == [
        60,
        120,
        240,
        480,
        900,
        900,
    ]


@pytest.mark.skipif(
    not os.getenv("DEV_HEALTH_POSTGRES_TEST_URI"),
    reason="requires DEV_HEALTH_POSTGRES_TEST_URI",
)
def test_real_postgres_migration_trigger_keeps_legacy_celery_worker_compatible():
    engine = create_engine(os.environ["DEV_HEALTH_POSTGRES_TEST_URI"])
    Base.metadata.create_all(engine)
    run_id = None
    integration_id = None
    try:
        with Session(engine) as session:
            for kind in (
                OUTBOX_KIND_DISPATCH,
                OUTBOX_KIND_FINALIZE,
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
            route = session.get(
                SyncDispatchTransportRoute,
                OUTBOX_KIND_DISPATCH,
            )
            assert route is not None
            expected_generation = route.generation
            now = datetime.now(timezone.utc)
            row = _seed_outbox(session, available_at=now)
            run_id = row.sync_run_id
            run = session.get(SyncRun, run_id)
            assert run is not None
            integration_id = run.integration_id
            session.flush()

            # Simulate a pre-0049 worker: it writes only the original lease
            # columns. The migration trigger binds the current Celery route.
            session.execute(
                update(SyncDispatchOutbox)
                .where(SyncDispatchOutbox.id == row.id)
                .values(
                    claim_token="legacy-celery-token",
                    claim_expires_at=now + timedelta(minutes=5),
                )
            )
            session.expire_all()
            claimed = session.get(SyncDispatchOutbox, row.id)
            assert claimed is not None
            assert claimed.claim_transport == "celery"
            assert claimed.claim_route_generation == expected_generation

            # The same old worker clears only its original lease columns when
            # marking success. The trigger copies the old binding into the
            # dispatched audit columns before clearing the claim tuple.
            session.execute(
                update(SyncDispatchOutbox)
                .where(SyncDispatchOutbox.id == row.id)
                .values(
                    status=OUTBOX_STATUS_DISPATCHED,
                    dispatched_at=now,
                    claim_token=None,
                    claim_expires_at=None,
                )
            )
            session.expire_all()
            dispatched = session.get(SyncDispatchOutbox, row.id)
            assert dispatched is not None
            assert dispatched.claim_transport is None
            assert dispatched.claim_route_generation is None
            assert dispatched.dispatched_transport == "celery"
            assert dispatched.dispatched_route_generation == expected_generation
            session.commit()
    finally:
        if run_id is not None:
            with Session(engine) as cleanup_session:
                cleanup_session.query(SyncDispatchOutbox).filter_by(
                    sync_run_id=run_id
                ).delete()
                cleanup_session.query(SyncRun).filter_by(id=run_id).delete()
                if integration_id is not None:
                    cleanup_session.query(Integration).filter_by(
                        id=integration_id
                    ).delete()
                cleanup_session.commit()
        engine.dispose()


@pytest.mark.skipif(
    not os.getenv("DEV_HEALTH_POSTGRES_TEST_URI"),
    reason="requires DEV_HEALTH_POSTGRES_TEST_URI",
)
def test_real_postgres_route_change_fences_claim_from_another_session():
    engine = create_engine(os.environ["DEV_HEALTH_POSTGRES_TEST_URI"])
    Base.metadata.create_all(engine)
    run_id = None
    integration_id = None
    route_generation = None
    try:
        with Session(engine) as seed_session:
            for kind in (
                OUTBOX_KIND_DISPATCH,
                OUTBOX_KIND_FINALIZE,
                "post_sync",
                "reference_discovery",
            ):
                if seed_session.get(SyncDispatchTransportRoute, kind) is None:
                    seed_session.add(
                        SyncDispatchTransportRoute(
                            kind=kind,
                            transport="celery",
                            generation=1,
                            paused=False,
                            paused_at=None,
                            rollback_transport="celery",
                        )
                    )
            run = _seed_run(seed_session)
            run_id = run.id
            integration_id = run.integration_id
            now = datetime.now(timezone.utc)
            upsert_outbox_wakeup(
                seed_session,
                sync_run_id=run.id,
                kind=OUTBOX_KIND_DISPATCH,
                available_at=now,
            )
            claim = claim_due_outbox_rows(seed_session, now=now, limit=1)[0]
            seed_session.commit()

        with Session(engine) as route_session:
            route = route_session.get(SyncDispatchTransportRoute, OUTBOX_KIND_DISPATCH)
            assert route is not None
            route_generation = route.generation
            route.paused = True
            route.paused_at = datetime.now(timezone.utc)
            route.generation += 1
            route_session.commit()

        with Session(engine) as publisher_session:
            assert (
                lock_outbox_claim_for_publish(
                    publisher_session, claim.id, claim.claim_token
                )
                is False
            )
            assert (
                mark_outbox_dispatched(
                    publisher_session,
                    row_id=claim.id,
                    claim_token=claim.claim_token,
                )
                is False
            )
    finally:
        if run_id is not None:
            with Session(engine) as cleanup_session:
                route = cleanup_session.get(
                    SyncDispatchTransportRoute, OUTBOX_KIND_DISPATCH
                )
                if route is not None and route.paused:
                    route.paused = False
                    route.paused_at = None
                    route.generation = max(
                        route.generation + 1, (route_generation or 0) + 2
                    )
                cleanup_session.query(SyncDispatchOutbox).filter_by(
                    sync_run_id=run_id
                ).delete()
                cleanup_session.query(SyncRun).filter_by(id=run_id).delete()
                if integration_id is not None:
                    cleanup_session.query(Integration).filter_by(
                        id=integration_id
                    ).delete()
                cleanup_session.commit()
        engine.dispose()


@pytest.mark.skipif(
    not os.getenv("DEV_HEALTH_POSTGRES_TEST_URI"),
    reason="requires DEV_HEALTH_POSTGRES_TEST_URI",
)
def test_real_postgres_publish_lock_blocks_route_change_until_commit():
    engine = create_engine(os.environ["DEV_HEALTH_POSTGRES_TEST_URI"])
    Base.metadata.create_all(engine)
    run_id = None
    integration_id = None
    original_generation = None
    publish_locked = Event()
    route_update_attempted = Event()
    publisher_committed = Event()
    route_update_committed = Event()
    try:
        with Session(engine) as seed_session:
            for kind in (
                OUTBOX_KIND_DISPATCH,
                OUTBOX_KIND_FINALIZE,
                "post_sync",
                "reference_discovery",
            ):
                if seed_session.get(SyncDispatchTransportRoute, kind) is None:
                    seed_session.add(
                        SyncDispatchTransportRoute(
                            kind=kind,
                            transport="celery",
                            generation=1,
                            paused=False,
                            paused_at=None,
                            rollback_transport="celery",
                        )
                    )
            route = seed_session.get(SyncDispatchTransportRoute, OUTBOX_KIND_DISPATCH)
            assert route is not None
            original_generation = route.generation
            run = _seed_run(seed_session)
            run_id = run.id
            integration_id = run.integration_id
            now = datetime.now(timezone.utc)
            upsert_outbox_wakeup(
                seed_session,
                sync_run_id=run.id,
                kind=OUTBOX_KIND_DISPATCH,
                available_at=now,
            )
            claim = claim_due_outbox_rows(seed_session, now=now, limit=1)[0]
            seed_session.commit()

        def publish() -> None:
            with Session(engine) as publisher_session:
                assert lock_outbox_claim_for_publish(
                    publisher_session, claim.id, claim.claim_token
                )
                publish_locked.set()
                assert route_update_attempted.wait(timeout=10)
                time.sleep(0.25)
                assert not route_update_committed.is_set()
                assert mark_outbox_dispatched(
                    publisher_session,
                    row_id=claim.id,
                    claim_token=claim.claim_token,
                )
                publisher_session.commit()
                publisher_committed.set()

        def change_route() -> None:
            assert publish_locked.wait(timeout=10)
            with Session(engine) as route_session:
                route_update_attempted.set()
                route_session.execute(
                    update(SyncDispatchTransportRoute)
                    .where(SyncDispatchTransportRoute.kind == OUTBOX_KIND_DISPATCH)
                    .values(
                        paused=True,
                        paused_at=datetime.now(timezone.utc),
                        generation=SyncDispatchTransportRoute.generation + 1,
                    )
                )
                route_session.commit()
            assert publisher_committed.is_set()
            route_update_committed.set()

        with ThreadPoolExecutor(max_workers=2) as executor:
            publish_future = executor.submit(publish)
            route_future = executor.submit(change_route)
            publish_future.result(timeout=30)
            route_future.result(timeout=30)
        assert route_update_committed.is_set()
    finally:
        if run_id is not None:
            with Session(engine) as cleanup_session:
                route = cleanup_session.get(
                    SyncDispatchTransportRoute, OUTBOX_KIND_DISPATCH
                )
                if route is not None and route.paused:
                    route.paused = False
                    route.paused_at = None
                    route.generation = max(
                        route.generation + 1, (original_generation or 0) + 2
                    )
                cleanup_session.query(SyncDispatchOutbox).filter_by(
                    sync_run_id=run_id
                ).delete()
                cleanup_session.query(SyncRun).filter_by(id=run_id).delete()
                if integration_id is not None:
                    cleanup_session.query(Integration).filter_by(
                        id=integration_id
                    ).delete()
                cleanup_session.commit()
        engine.dispose()
