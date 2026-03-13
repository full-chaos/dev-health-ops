import sys
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

_fake_croniter_mod = MagicMock()
if "croniter" not in sys.modules:
    sys.modules["croniter"] = _fake_croniter_mod

from dev_health_ops.metrics.checkpoints import get_checkpoint  # noqa: E402
from dev_health_ops.models.checkpoints import CheckpointStatus  # noqa: E402
from dev_health_ops.models.git import Base  # noqa: E402
from dev_health_ops.models.settings import (  # noqa: E402
    JobStatus,
    ScheduledJob,
    SyncConfiguration,
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


def _make_config(
    provider="github",
    sync_options=None,
    sync_targets=None,
    is_active=True,
    name="test-config",
    org_id="default",
):
    return SyncConfiguration(
        name=name,
        provider=provider,
        org_id=org_id,
        sync_targets=sync_targets or ["git", "prs"],
        sync_options=sync_options or {},
        is_active=is_active,
    )


@patch("dev_health_ops.storage.run_with_store")
@patch("dev_health_ops.workers.sync_runtime._dispatch_post_sync_tasks")
@patch(
    "dev_health_ops.workers.sync_runtime._resolve_env_credentials",
    return_value={"token": "test"},
)
@patch("dev_health_ops.db.get_postgres_session_sync")
def test_run_sync_config_with_multiple_job_types_no_collision(
    mock_get_session,
    mock_resolve_creds,
    mock_post_sync,
    mock_run_with_store,
    db_session,
):
    from dev_health_ops.workers.sync_runtime import run_sync_config

    config = _make_config(
        provider="github",
        sync_options={"owner": "my-org", "repo": "my-repo"},
        sync_targets=["git"],
    )
    db_session.add(config)
    db_session.flush()

    sync_job = ScheduledJob(
        name="sync-job",
        job_type="sync",
        schedule_cron="0 * * * *",
        org_id="default",
        job_config={"provider": "github", "sync_config_id": str(config.id)},
        sync_config_id=config.id,
        status=JobStatus.ACTIVE.value,
    )
    backfill_job = ScheduledJob(
        name="backfill-job",
        job_type="backfill",
        schedule_cron="0 0 * * *",
        org_id="default",
        job_config={"provider": "github", "sync_config_id": str(config.id)},
        sync_config_id=config.id,
        status=JobStatus.ACTIVE.value,
    )
    db_session.add_all([sync_job, backfill_job])
    db_session.flush()

    mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
    mock_run_with_store.return_value = None

    task = run_sync_config
    task.push_request(id="bugsink-sync-jobtype")
    try:
        result = task(config_id=str(config.id), org_id="default")
    finally:
        task.pop_request()

    assert result["status"] == "success"


@patch("dev_health_ops.workers.metrics_partitioned.chord")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
def test_dispatch_daily_metrics_partitioned_defaults_none_org_id(
    mock_sink_cls, mock_chord
):
    from dev_health_ops.workers.metrics_partitioned import dispatch_daily_metrics_partitioned

    mock_sink_instance = MagicMock()
    mock_sink_instance.client.query.return_value.result_rows = [(str(uuid.uuid4()),)]
    mock_sink_cls.return_value = mock_sink_instance

    mock_chord_instance = MagicMock()
    mock_chord.return_value = mock_chord_instance

    task = dispatch_daily_metrics_partitioned
    task.push_request(id="bugsink-dispatch-none-org")
    try:
        result = task(
            org_id=None,
            db_url="clickhouse://fake",
            day="2025-01-15",
            backfill_days=1,
            batch_size=5,
        )
    finally:
        task.pop_request()

    assert result["status"] == "dispatched"


@patch("asyncio.run")
@patch("dev_health_ops.db.get_postgres_session_sync")
def test_run_daily_metrics_batch_defaults_none_org_id(
    mock_get_session, mock_asyncio_run, db_session
):
    from dev_health_ops.workers.metrics_partitioned import run_daily_metrics_batch

    mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
    mock_asyncio_run.return_value = None

    repo_id = uuid.uuid4()
    task = run_daily_metrics_batch
    task.push_request(id="bugsink-batch-none-org")
    try:
        result = task(
            repo_ids=[str(repo_id)],
            day="2025-01-15",
            org_id=None,
            db_url="clickhouse://fake",
        )
    finally:
        task.pop_request()

    assert result["results"][str(repo_id)]["status"] == "success"

    checkpoint_day = datetime.combine(date(2025, 1, 15), time.min, tzinfo=timezone.utc)
    cp = get_checkpoint(db_session, "default", repo_id, "daily_batch", checkpoint_day)
    assert cp is not None
    assert cp.status == CheckpointStatus.COMPLETED


@patch("dev_health_ops.workers.metrics_partitioned._invalidate_metrics_cache")
@patch("asyncio.run")
@patch("dev_health_ops.db.get_postgres_session_sync")
def test_run_daily_metrics_finalize_defaults_none_org_id(
    mock_get_session, mock_asyncio_run, mock_invalidate, db_session
):
    from dev_health_ops.workers.metrics_partitioned import run_daily_metrics_finalize_task

    mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
    mock_asyncio_run.return_value = None

    task = run_daily_metrics_finalize_task
    task.push_request(id="bugsink-finalize-none-org", retries=0)
    try:
        result = task(
            batch_results=[{"day": "2025-01-15", "results": {}}],
            day="2025-01-15",
            org_id=None,
            db_url="clickhouse://fake",
        )
    finally:
        task.pop_request()

    assert result["status"] == "success"

    checkpoint_day = datetime.combine(date(2025, 1, 15), time.min, tzinfo=timezone.utc)
    cp = get_checkpoint(db_session, "default", None, "daily_finalize", checkpoint_day)
    assert cp is not None
    assert cp.status == CheckpointStatus.COMPLETED
