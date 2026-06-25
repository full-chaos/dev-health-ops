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
from tests._helpers import closing_coroutine_runner  # noqa: E402


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


@patch("dev_health_ops.workers.metrics_partitioned.chord")
@patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
def test_dispatch_daily_metrics_partitioned_defaults_none_org_id(
    mock_sink_cls, mock_chord
):
    from dev_health_ops.workers.metrics_partitioned import (
        dispatch_daily_metrics_partitioned,
    )

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
    mock_asyncio_run.side_effect = closing_coroutine_runner()

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
    from dev_health_ops.workers.metrics_partitioned import (
        run_daily_metrics_finalize_task,
    )

    mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
    mock_asyncio_run.side_effect = closing_coroutine_runner()

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
