from __future__ import annotations

import inspect
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    OUTBOX_STATUS_DISPATCHED,
    OUTBOX_STATUS_PENDING,
    ClaimedOutboxRow,
    backoff_seconds,
    claim_due_outbox_rows,
    lock_outbox_claim_for_publish,
    mark_outbox_dispatched,
    mark_outbox_publish_failed,
    upsert_outbox_wakeup,
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
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


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
    row.claim_token = "old-token"
    row.claim_expires_at = now - timedelta(minutes=1)
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


def test_publish_lock_rejects_wrong_and_null_lease_owner(db_session):
    now = datetime.now(timezone.utc)
    row = _seed_outbox(db_session, available_at=now)
    claimed = claim_due_outbox_rows(db_session, now=now, limit=1)
    token = claimed[0].claim_token

    assert lock_outbox_claim_for_publish(db_session, row.id, "wrong-token") is False

    db_session.refresh(row)
    row.claim_token = None
    row.claim_expires_at = None
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
    assert row.last_error is None


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
