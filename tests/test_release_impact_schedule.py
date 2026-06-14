"""CHAOS-2381: release_impact_daily compute must be scheduled.

These tests prove the wiring seam — that the existing
``compute_release_impact_daily`` compute is now reachable via a registered
Celery task with a daily beat entry — WITHOUT touching a live ClickHouse.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from celery.schedules import crontab


class TestReleaseImpactTaskRegistered:
    def test_task_importable_and_callable(self) -> None:
        from dev_health_ops.workers.metrics_extra import run_release_impact_job

        assert callable(run_release_impact_job)

    def test_task_exported_from_tasks_module(self) -> None:
        from dev_health_ops.workers import tasks

        assert "run_release_impact_job" in tasks.__all__
        assert hasattr(tasks, "run_release_impact_job")

    def test_task_is_celery_task(self) -> None:
        from dev_health_ops.workers.metrics_extra import run_release_impact_job

        assert hasattr(run_release_impact_job, "apply_async")
        assert hasattr(run_release_impact_job, "delay")
        assert (
            run_release_impact_job.name
            == "dev_health_ops.workers.tasks.run_release_impact_job"
        )
        # Must land on the metrics queue, alongside the other compute jobs.
        assert run_release_impact_job.queue == "metrics"


class TestReleaseImpactBeatSchedule:
    def test_beat_schedule_contains_release_impact(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        assert "run-release-impact-daily" in beat_schedule
        entry = beat_schedule["run-release-impact-daily"]
        assert entry["task"] == "dev_health_ops.workers.tasks.run_release_impact_job"
        assert entry["options"]["queue"] == "metrics"

    def test_beat_schedule_uses_daily_crontab(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        schedule = beat_schedule["run-release-impact-daily"]["schedule"]
        assert isinstance(schedule, crontab)


class TestReleaseImpactTaskInvokesCompute:
    def test_task_invokes_release_impact_job(self) -> None:
        """The Celery task body must invoke the existing compute job."""
        from dev_health_ops.workers import metrics_extra

        captured: dict[str, object] = {}

        async def fake_job(**kwargs: object) -> int:
            captured.update(kwargs)
            return 7

        with (
            patch(
                "dev_health_ops.metrics.job_release_impact.run_release_impact_job",
                side_effect=fake_job,
            ),
            patch.object(
                metrics_extra, "_get_db_url", return_value="clickhouse://unit-test"
            ),
        ):
            result = metrics_extra.run_release_impact_job.run(
                day="2026-03-15",
                backfill_days=2,
                recomputation_window_days=3,
                org_id="org-abc",
            )

        # Compute was reached with the wired-through arguments.
        assert captured["db_url"] == "clickhouse://unit-test"
        assert str(captured["day"]) == "2026-03-15"
        assert captured["backfill_days"] == 2
        assert captured["recomputation_window_days"] == 3
        assert captured["org_id"] == "org-abc"

        assert result["status"] == "success"
        assert result["day"] == "2026-03-15"
        assert result["records_written"] == 7

    def test_task_defaults_db_url_and_org(self) -> None:
        """db_url falls back to _get_db_url and org_id defaults to ''."""
        from dev_health_ops.workers import metrics_extra

        captured: dict[str, object] = {}

        async def fake_job(**kwargs: object) -> int:
            captured.update(kwargs)
            return 0

        with (
            patch(
                "dev_health_ops.metrics.job_release_impact.run_release_impact_job",
                side_effect=fake_job,
            ),
            patch.object(
                metrics_extra, "_get_db_url", return_value="clickhouse://default"
            ),
            patch.object(metrics_extra, "utc_today") as mock_today,
        ):
            from datetime import date

            mock_today.return_value = date(2026, 6, 13)
            metrics_extra.run_release_impact_job.run()

        assert captured["db_url"] == "clickhouse://default"
        assert captured["org_id"] == ""
        assert str(captured["day"]) == "2026-06-13"

    def test_run_async_used_for_async_job(self) -> None:
        """The async compute is driven via the shared run_async helper."""
        from dev_health_ops.workers import metrics_extra

        async def fake_job(**kwargs: object) -> int:
            return 3

        def fake_run_async(coro: object) -> int:
            # Close the unawaited coroutine to avoid a RuntimeWarning while
            # still proving run_async is the seam that drives the compute.
            getattr(coro, "close", lambda: None)()
            return 3

        sentinel = MagicMock(side_effect=fake_run_async)

        with (
            patch(
                "dev_health_ops.metrics.job_release_impact.run_release_impact_job",
                side_effect=fake_job,
            ),
            patch.object(metrics_extra, "_get_db_url", return_value="clickhouse://x"),
            patch.object(metrics_extra, "run_async", sentinel),
        ):
            result = metrics_extra.run_release_impact_job.run(day="2026-03-15")

        sentinel.assert_called_once()
        assert result["records_written"] == 3
