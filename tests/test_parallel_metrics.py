import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.checkpoints import CheckpointStatus
from dev_health_ops.models.git import Base
from dev_health_ops.metrics.checkpoints import (
    get_checkpoint,
    mark_completed,
    mark_running,
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


class TestDispatchDailyMetricsPartitioned:
    @patch("dev_health_ops.workers.tasks.chord")
    @patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
    def test_discovers_repos_and_creates_batches(self, mock_sink_cls, mock_chord):
        from dev_health_ops.workers.tasks import dispatch_daily_metrics_partitioned

        fake_rows = [(str(uuid.uuid4()),) for _ in range(7)]
        mock_sink_instance = MagicMock()
        mock_sink_instance.client.query.return_value.result_rows = fake_rows
        mock_sink_cls.return_value = mock_sink_instance

        mock_chord_instance = MagicMock()
        mock_chord.return_value = mock_chord_instance

        task = dispatch_daily_metrics_partitioned
        task.push_request(id="dispatch-1")
        try:
            result = task(
                db_url="clickhouse://fake",
                day="2025-01-15",
                backfill_days=1,
                batch_size=3,
            )
        finally:
            task.pop_request()

        assert result["status"] == "dispatched"
        assert result["repo_count"] == 7
        assert result["batch_count"] == 3
        mock_chord.assert_called_once()
        mock_chord_instance.assert_called_once()

    @patch("dev_health_ops.workers.tasks.chord")
    @patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
    def test_returns_no_repos_when_empty(self, mock_sink_cls, mock_chord):
        from dev_health_ops.workers.tasks import dispatch_daily_metrics_partitioned

        mock_sink_instance = MagicMock()
        mock_sink_instance.client.query.return_value.result_rows = []
        mock_sink_cls.return_value = mock_sink_instance

        task = dispatch_daily_metrics_partitioned
        task.push_request(id="dispatch-2")
        try:
            result = task(db_url="clickhouse://fake", day="2025-01-15")
        finally:
            task.pop_request()

        assert result["status"] == "no_repos"
        assert result["dispatched"] == 0
        mock_chord.assert_not_called()

    @patch("dev_health_ops.workers.tasks.chord")
    @patch("dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink")
    def test_chord_callback_is_finalize_task(self, mock_sink_cls, mock_chord):
        from dev_health_ops.workers.tasks import (
            dispatch_daily_metrics_partitioned,
            run_daily_metrics_finalize_task,
        )

        fake_rows = [(str(uuid.uuid4()),)]
        mock_sink_instance = MagicMock()
        mock_sink_instance.client.query.return_value.result_rows = fake_rows
        mock_sink_cls.return_value = mock_sink_instance

        mock_chord_instance = MagicMock()
        mock_chord.return_value = mock_chord_instance

        task = dispatch_daily_metrics_partitioned
        task.push_request(id="dispatch-3")
        try:
            task(db_url="clickhouse://fake", day="2025-01-15")
        finally:
            task.pop_request()

        args, kwargs = mock_chord.call_args
        callback = kwargs.get("callback") if kwargs else args[1]
        assert callback.task == run_daily_metrics_finalize_task.name


class TestRunDailyMetricsBatch:
    @patch("asyncio.run")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_marks_running_then_completed_on_success(
        self, mock_get_session, mock_asyncio_run, db_session
    ):
        from dev_health_ops.workers.tasks import run_daily_metrics_batch

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_asyncio_run.return_value = None

        repo_id = uuid.uuid4()
        task = run_daily_metrics_batch
        task.push_request(id="celery-task-123")
        try:
            result = task(
                repo_ids=[str(repo_id)],
                day="2025-01-15",
                db_url="clickhouse://fake",
            )
        finally:
            task.pop_request()

        assert result["results"][str(repo_id)]["status"] == "success"
        mock_asyncio_run.assert_called_once()

        checkpoint_day = datetime.combine(
            date(2025, 1, 15), time.min, tzinfo=timezone.utc
        )
        cp = get_checkpoint(
            db_session, "default", repo_id, "daily_batch", checkpoint_day
        )
        assert cp is not None
        assert cp.status == CheckpointStatus.COMPLETED

    @patch("asyncio.run")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_marks_failed_on_exception(
        self, mock_get_session, mock_asyncio_run, db_session
    ):
        from dev_health_ops.workers.tasks import run_daily_metrics_batch

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_asyncio_run.side_effect = RuntimeError("boom")

        repo_id = uuid.uuid4()
        task = run_daily_metrics_batch
        task.push_request(id="celery-task-456")
        try:
            result = task(
                repo_ids=[str(repo_id)],
                day="2025-01-15",
                db_url="clickhouse://fake",
            )
        finally:
            task.pop_request()

        assert result["results"][str(repo_id)]["status"] == "failed"
        assert "boom" in result["results"][str(repo_id)]["error"]

        checkpoint_day = datetime.combine(
            date(2025, 1, 15), time.min, tzinfo=timezone.utc
        )
        cp = get_checkpoint(
            db_session, "default", repo_id, "daily_batch", checkpoint_day
        )
        assert cp is not None
        assert cp.status == CheckpointStatus.FAILED
        assert cp.error == "boom"

    @patch("asyncio.run")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_skips_already_completed_repos(
        self, mock_get_session, mock_asyncio_run, db_session
    ):
        from dev_health_ops.workers.tasks import run_daily_metrics_batch

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)

        repo_id = uuid.uuid4()
        checkpoint_day = datetime.combine(
            date(2025, 1, 15), time.min, tzinfo=timezone.utc
        )
        cp = mark_running(
            db_session, "default", repo_id, "daily_batch", checkpoint_day, "old-worker"
        )
        mark_completed(db_session, cp.id)

        task = run_daily_metrics_batch
        task.push_request(id="celery-task-789")
        try:
            result = task(
                repo_ids=[str(repo_id)],
                day="2025-01-15",
                db_url="clickhouse://fake",
            )
        finally:
            task.pop_request()

        assert result["results"][str(repo_id)]["status"] == "skipped"
        mock_asyncio_run.assert_not_called()


class TestRunDailyMetricsFinalizeTask:
    @patch("dev_health_ops.workers.tasks._invalidate_metrics_cache")
    @patch("asyncio.run")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_calls_finalize_and_invalidates_cache(
        self, mock_get_session, mock_asyncio_run, mock_invalidate, db_session
    ):
        from dev_health_ops.workers.tasks import run_daily_metrics_finalize_task

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_asyncio_run.return_value = None

        task = run_daily_metrics_finalize_task
        task.push_request(id="finalize-task-1", retries=0)
        try:
            result = task(
                batch_results=[{"day": "2025-01-15", "results": {}}],
                day="2025-01-15",
                db_url="clickhouse://fake",
            )
        finally:
            task.pop_request()

        assert result["status"] == "success"
        mock_asyncio_run.assert_called_once()
        mock_invalidate.assert_called_once_with("2025-01-15", "default")

        checkpoint_day = datetime.combine(
            date(2025, 1, 15), time.min, tzinfo=timezone.utc
        )
        cp = get_checkpoint(
            db_session, "default", None, "daily_finalize", checkpoint_day
        )
        assert cp is not None
        assert cp.status == CheckpointStatus.COMPLETED

    @patch("dev_health_ops.workers.tasks._invalidate_metrics_cache")
    @patch("asyncio.run")
    @patch("dev_health_ops.db.get_postgres_session_sync")
    def test_marks_failed_on_finalize_error(
        self, mock_get_session, mock_asyncio_run, mock_invalidate, db_session
    ):
        from dev_health_ops.workers.tasks import run_daily_metrics_finalize_task

        mock_get_session.side_effect = lambda: _fake_session_ctx(db_session)
        mock_asyncio_run.side_effect = RuntimeError("finalize exploded")

        task = run_daily_metrics_finalize_task
        task.push_request(id="finalize-task-2", retries=0)
        original_retry = task.retry
        task.retry = MagicMock(side_effect=RuntimeError("retry"))
        try:
            with pytest.raises(RuntimeError, match="retry"):
                task(
                    batch_results=[],
                    day="2025-01-15",
                    db_url="clickhouse://fake",
                )
        finally:
            task.retry = original_retry
            task.pop_request()

        checkpoint_day = datetime.combine(
            date(2025, 1, 15), time.min, tzinfo=timezone.utc
        )
        cp = get_checkpoint(
            db_session, "default", None, "daily_finalize", checkpoint_day
        )
        assert cp is not None
        assert cp.status == CheckpointStatus.FAILED


class TestTaskRegistration:
    def test_tasks_have_celery_attributes(self):
        from dev_health_ops.workers.tasks import (
            dispatch_daily_metrics_partitioned,
            run_daily_metrics_batch,
            run_daily_metrics_finalize_task,
        )

        for task in [
            dispatch_daily_metrics_partitioned,
            run_daily_metrics_batch,
            run_daily_metrics_finalize_task,
        ]:
            assert hasattr(task, "apply_async")
            assert hasattr(task, "delay")

    def test_task_queue_assignments(self):
        from dev_health_ops.workers.tasks import (
            dispatch_daily_metrics_partitioned,
            run_daily_metrics_batch,
            run_daily_metrics_finalize_task,
        )

        assert dispatch_daily_metrics_partitioned.queue == "default"
        assert run_daily_metrics_batch.queue == "metrics"
        assert run_daily_metrics_finalize_task.queue == "metrics"
