"""CHAOS-2850: complexity must refresh on a reliable, independent daily cadence.

``run_complexity_job`` previously only ran chained after a git sync
(post_sync_dispatch.py). An org with infrequent syncs left
``repo_complexity_daily`` stale for days, and ``complexity_delta``
(compounding_risk.py) reads a trailing 30-day window from that table, so a
stale table reads as a flat trend. These tests prove:

* the wiring seam -- a registered Celery task + a daily beat entry that
  points at the per-org dispatcher, scheduled ahead of run-daily-metrics,
* the dispatcher fans out one ``run_complexity_job`` per active org with the
  real org_id (never blank), and
* an enumeration failure surfaces as a retry/failure rather than a silent
  empty success.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

from celery.schedules import crontab

# Import connectors first to defeat the providers._base <-> connectors circular
# import that otherwise breaks collection when this module (transitively)
# imports compute/sink modules in isolation (CHAOS-2370 precedent).
import dev_health_ops.connectors  # noqa: F401


class TestComplexityDispatcherRegistered:
    def test_task_importable_and_callable(self) -> None:
        from dev_health_ops.workers.metrics_extra import dispatch_complexity_job

        assert callable(dispatch_complexity_job)

    def test_task_exported_from_tasks_module(self) -> None:
        from dev_health_ops.workers import tasks

        assert "dispatch_complexity_job" in tasks.__all__
        assert hasattr(tasks, "dispatch_complexity_job")

    def test_task_is_celery_task(self) -> None:
        from dev_health_ops.workers.metrics_extra import dispatch_complexity_job

        assert hasattr(dispatch_complexity_job, "apply_async")
        assert hasattr(dispatch_complexity_job, "delay")
        assert (
            dispatch_complexity_job.name
            == "dev_health_ops.workers.tasks.dispatch_complexity_job"
        )
        # Lightweight dispatcher (DB enumeration only) -> default queue,
        # mirroring dispatch_release_impact / dispatch_daily_metrics_for_all_orgs.
        assert dispatch_complexity_job.queue == "default"


class TestComplexityBeatSchedule:
    def test_beat_schedule_dispatches_complexity_daily(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        assert "run-complexity-daily" in beat_schedule
        entry = beat_schedule["run-complexity-daily"]
        assert entry["task"] == "dev_health_ops.workers.tasks.dispatch_complexity_job"
        assert entry["options"]["queue"] == "default"

    def test_beat_schedule_uses_crontab(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        schedule = beat_schedule["run-complexity-daily"]["schedule"]
        assert isinstance(schedule, crontab)

    def test_complexity_runs_before_daily_metrics(self) -> None:
        """Complexity must refresh before run-daily-metrics (01:00 UTC) so the
        daily hotspot/risk compute reads a freshly-refreshed snapshot."""
        from dev_health_ops.workers.config import beat_schedule

        complexity_schedule = beat_schedule["run-complexity-daily"]["schedule"]
        daily_metrics_schedule = beat_schedule["run-daily-metrics"]["schedule"]

        def _minute_of_day(schedule: crontab) -> int:
            hour = next(iter(schedule.hour))
            minute = next(iter(schedule.minute))
            return hour * 60 + minute

        assert _minute_of_day(complexity_schedule) < _minute_of_day(
            daily_metrics_schedule
        )

    def test_late_ack_excluded(self) -> None:
        """The dispatcher is DB-enumeration-only and should not hold a late ack."""
        from dev_health_ops.workers.config import late_ack_excluded_tasks

        assert (
            "dev_health_ops.workers.tasks.dispatch_complexity_job"
            in late_ack_excluded_tasks
        )


class TestComplexityDispatcherFansOutPerOrg:
    def test_dispatcher_enqueues_one_task_per_active_org(self) -> None:
        """The dispatcher enumerates active orgs and enqueues per real org_id."""
        from dev_health_ops.workers import metrics_extra

        org_a = str(uuid4())
        org_b = str(uuid4())

        session = MagicMock()
        # _discover_active_org_ids uses the legacy session.query(...).filter(...)
        # style (not session.execute(select(...))), unlike dispatch_release_impact.
        session.query.return_value.filter.return_value.all.return_value = [
            (org_a,),
            (org_b,),
        ]
        cm = MagicMock()
        cm.__enter__.return_value = session
        cm.__exit__.return_value = False

        with (
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=cm,
            ),
            patch.object(metrics_extra.run_complexity_job, "apply_async") as mock_apply,
        ):
            result = metrics_extra.dispatch_complexity_job.run(day="2026-06-13")

        assert mock_apply.call_count == 2
        dispatched_orgs = {
            call.kwargs["kwargs"]["org_id"] for call in mock_apply.call_args_list
        }
        assert dispatched_orgs == {org_a, org_b}
        assert "" not in dispatched_orgs
        for call in mock_apply.call_args_list:
            assert call.kwargs["queue"] == "metrics"
            assert call.kwargs["kwargs"]["day"] == "2026-06-13"
        assert set(result["dispatched"]) == {org_a, org_b}

    def test_dispatcher_raises_on_enumeration_failure(self) -> None:
        """A transient Postgres failure must NOT report an empty success."""
        import celery.exceptions

        from dev_health_ops.workers import metrics_extra

        cm = MagicMock()
        cm.__enter__.side_effect = RuntimeError("postgres unavailable")

        with (
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=cm,
            ),
            patch.object(metrics_extra.run_complexity_job, "apply_async") as mock_apply,
        ):
            raised: Exception | None = None
            try:
                metrics_extra.dispatch_complexity_job.run()
            except (celery.exceptions.Retry, RuntimeError) as exc:
                raised = exc

        assert raised is not None, (
            "dispatcher must raise (retry/failure) on enumeration failure, "
            "not silently return an empty success"
        )
        mock_apply.assert_not_called()
