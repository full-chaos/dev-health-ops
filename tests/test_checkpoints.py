import uuid
from datetime import date, datetime, time, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.checkpoints import CheckpointStatus, MetricCheckpoint
from dev_health_ops.models.git import Base
from dev_health_ops.metrics.checkpoints import (
    get_checkpoint,
    get_incomplete_repos,
    is_completed,
    mark_completed,
    mark_failed,
    mark_running,
    reset_stale_running,
)

DAY = datetime.combine(date(2025, 1, 15), time.min, tzinfo=timezone.utc)
ORG_ID = "default"
METRIC_TYPE = "daily_batch"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def test_mark_running_creates_new_checkpoint(db_session):
    repo_id = uuid.uuid4()
    worker_id = "celery-task-abc"

    cp = mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, worker_id)

    assert cp.status == CheckpointStatus.RUNNING
    assert cp.org_id == ORG_ID
    assert cp.repo_id == repo_id
    assert cp.metric_type == METRIC_TYPE
    assert cp.day == DAY
    assert cp.worker_id == worker_id
    assert cp.started_at is not None


def test_mark_running_updates_existing(db_session):
    repo_id = uuid.uuid4()

    cp1 = mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "worker-1")
    cp1_id = cp1.id

    cp2 = mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "worker-2")

    assert cp2.id == cp1_id
    assert cp2.worker_id == "worker-2"
    count = db_session.query(MetricCheckpoint).count()
    assert count == 1


def test_get_checkpoint_returns_none_for_missing(db_session):
    result = get_checkpoint(db_session, ORG_ID, uuid.uuid4(), METRIC_TYPE, DAY)
    assert result is None


def test_get_checkpoint_returns_existing(db_session):
    repo_id = uuid.uuid4()
    mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "w1")

    result = get_checkpoint(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY)
    assert result is not None
    assert result.repo_id == repo_id
    assert result.status == CheckpointStatus.RUNNING


def test_mark_completed_sets_status_and_timestamp(db_session):
    repo_id = uuid.uuid4()
    cp = mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "w1")

    mark_completed(db_session, cp.id)

    refreshed = get_checkpoint(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY)
    assert refreshed.status == CheckpointStatus.COMPLETED
    assert refreshed.completed_at is not None


def test_mark_completed_raises_for_missing_id(db_session):
    with pytest.raises(ValueError, match="not found"):
        mark_completed(db_session, uuid.uuid4())


def test_mark_failed_sets_status_and_error(db_session):
    repo_id = uuid.uuid4()
    cp = mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "w1")

    mark_failed(db_session, cp.id, "connection timeout")

    refreshed = get_checkpoint(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY)
    assert refreshed.status == CheckpointStatus.FAILED
    assert refreshed.error == "connection timeout"


def test_mark_failed_raises_for_missing_id(db_session):
    with pytest.raises(ValueError, match="not found"):
        mark_failed(db_session, uuid.uuid4(), "some error")


def test_is_completed_true_when_completed(db_session):
    repo_id = uuid.uuid4()
    cp = mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "w1")
    mark_completed(db_session, cp.id)

    assert is_completed(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY) is True


def test_is_completed_false_when_running(db_session):
    repo_id = uuid.uuid4()
    mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "w1")

    assert is_completed(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY) is False


def test_is_completed_false_when_no_checkpoint(db_session):
    assert is_completed(db_session, ORG_ID, uuid.uuid4(), METRIC_TYPE, DAY) is False


def test_get_incomplete_repos(db_session):
    repo_completed = uuid.uuid4()
    repo_running = uuid.uuid4()
    repo_none = uuid.uuid4()

    cp = mark_running(db_session, ORG_ID, repo_completed, METRIC_TYPE, DAY, "w1")
    mark_completed(db_session, cp.id)

    mark_running(db_session, ORG_ID, repo_running, METRIC_TYPE, DAY, "w2")

    all_repos = [repo_completed, repo_running, repo_none]
    incomplete = get_incomplete_repos(db_session, ORG_ID, METRIC_TYPE, DAY, all_repos)

    assert repo_completed not in incomplete
    assert repo_running in incomplete
    assert repo_none in incomplete
    assert len(incomplete) == 2


def test_reset_stale_running(db_session):
    repo_id = uuid.uuid4()
    cp = mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "w1")
    cp.started_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db_session.flush()

    count = reset_stale_running(db_session, stale_threshold_minutes=60)

    assert count == 1
    refreshed = get_checkpoint(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY)
    assert refreshed.status == CheckpointStatus.PENDING
    assert refreshed.started_at is None
    assert refreshed.worker_id is None


def test_reset_stale_running_ignores_recent(db_session):
    repo_id = uuid.uuid4()
    mark_running(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY, "w1")

    count = reset_stale_running(db_session, stale_threshold_minutes=60)

    assert count == 0
    refreshed = get_checkpoint(db_session, ORG_ID, repo_id, METRIC_TYPE, DAY)
    assert refreshed.status == CheckpointStatus.RUNNING
