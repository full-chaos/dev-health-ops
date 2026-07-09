"""CHAOS-2849: repo_metrics_daily must be scheduled per active org.

``discover_repos`` (job_daily.py) scopes the ``repos`` ClickHouse query by
``org_id``. The beat-scheduled 01:00 UTC ``run-daily-metrics`` entry used to
call ``dispatch_daily_metrics_partitioned`` directly with NO ``org_id``,
which defaults to the literal string ``"default"`` -- never a real
(UUID-scoped) tenant's rows. ``repo_metrics_daily`` was therefore never
populated for any real organization. These tests prove:

* the wiring seam -- a registered Celery task + a daily beat entry that
  points at the per-org dispatcher,
* the dispatcher fans out one ``dispatch_daily_metrics_partitioned`` per
  active org with the real org_id (never ``"default"`` / blank), and
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


class TestDailyMetricsDispatcherRegistered:
    def test_task_importable_and_callable(self) -> None:
        from dev_health_ops.workers.metrics_partitioned import (
            dispatch_daily_metrics_for_all_orgs,
        )

        assert callable(dispatch_daily_metrics_for_all_orgs)

    def test_task_exported_from_tasks_module(self) -> None:
        from dev_health_ops.workers import tasks

        assert "dispatch_daily_metrics_for_all_orgs" in tasks.__all__
        assert hasattr(tasks, "dispatch_daily_metrics_for_all_orgs")

    def test_task_is_celery_task(self) -> None:
        from dev_health_ops.workers.metrics_partitioned import (
            dispatch_daily_metrics_for_all_orgs,
        )

        assert hasattr(dispatch_daily_metrics_for_all_orgs, "apply_async")
        assert hasattr(dispatch_daily_metrics_for_all_orgs, "delay")
        assert (
            dispatch_daily_metrics_for_all_orgs.name
            == "dev_health_ops.workers.tasks.dispatch_daily_metrics_for_all_orgs"
        )
        # Lightweight dispatcher (DB enumeration only) -> default queue,
        # mirroring dispatch_release_impact / dispatch_membership_backfill.
        assert dispatch_daily_metrics_for_all_orgs.queue == "default"


class TestDailyMetricsBeatSchedule:
    def test_beat_schedule_points_at_the_per_org_dispatcher(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        assert "run-daily-metrics" in beat_schedule
        entry = beat_schedule["run-daily-metrics"]
        # MUST point at the per-org dispatcher, not the blank-org-defaulting
        # partitioned task directly (the CHAOS-2849 defect).
        assert (
            entry["task"]
            == "dev_health_ops.workers.tasks.dispatch_daily_metrics_for_all_orgs"
        )
        assert entry["options"]["queue"] == "default"

    def test_beat_schedule_uses_daily_crontab(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        schedule = beat_schedule["run-daily-metrics"]["schedule"]
        assert isinstance(schedule, crontab)

    def test_late_ack_excluded(self) -> None:
        """The dispatcher is DB-enumeration-only and should not hold a late ack."""
        from dev_health_ops.workers.config import late_ack_excluded_tasks

        assert (
            "dev_health_ops.workers.tasks.dispatch_daily_metrics_for_all_orgs"
            in late_ack_excluded_tasks
        )


class TestDailyMetricsDispatcherFansOutPerOrg:
    def test_dispatcher_enqueues_one_task_per_active_org(self) -> None:
        """The dispatcher enumerates active orgs and enqueues per real org_id."""
        from dev_health_ops.workers import metrics_partitioned

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
            patch.object(
                metrics_partitioned.dispatch_daily_metrics_partitioned, "apply_async"
            ) as mock_apply,
        ):
            result = metrics_partitioned.dispatch_daily_metrics_for_all_orgs.run(
                day="2026-06-13", backfill_days=1
            )

        # One enqueue per active org, each carrying the REAL org_id (never
        # "default" / blank).
        assert mock_apply.call_count == 2
        dispatched_orgs = {
            call.kwargs["kwargs"]["org_id"] for call in mock_apply.call_args_list
        }
        assert dispatched_orgs == {org_a, org_b}
        assert "default" not in dispatched_orgs
        assert "" not in dispatched_orgs
        for call in mock_apply.call_args_list:
            assert call.kwargs["queue"] == "default"
            assert call.kwargs["kwargs"]["day"] == "2026-06-13"
            assert call.kwargs["kwargs"]["backfill_days"] == 1
        assert set(result["dispatched"]) == {org_a, org_b}

    def test_dispatcher_raises_on_enumeration_failure(self) -> None:
        """A transient Postgres failure must NOT report an empty success.

        Swallowing the enumeration error and returning
        ``{"dispatched": [], "skipped": 0}`` would make Celery beat treat the
        run as successful while computing zero orgs -- every tenant would go
        another day without repo_metrics_daily rows and nothing would page on
        it. The dispatcher must surface the failure via Celery's retry
        machinery (which re-raises here) so transient errors are retried.
        """
        import celery.exceptions

        from dev_health_ops.workers import metrics_partitioned

        cm = MagicMock()
        cm.__enter__.side_effect = RuntimeError("postgres unavailable")

        with (
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=cm,
            ),
            patch.object(
                metrics_partitioned.dispatch_daily_metrics_partitioned, "apply_async"
            ) as mock_apply,
        ):
            raised: Exception | None = None
            try:
                metrics_partitioned.dispatch_daily_metrics_for_all_orgs.run()
            except (celery.exceptions.Retry, RuntimeError) as exc:
                raised = exc

        assert raised is not None, (
            "dispatcher must raise (retry/failure) on enumeration failure, "
            "not silently return an empty success"
        )
        mock_apply.assert_not_called()

    def test_dispatcher_falls_back_to_default_org_when_no_active_orgs(self) -> None:
        """Single-tenant / community installs: no Organization rows -> default org.

        Mirrors ``_discover_active_org_ids``'s designed fallback (used
        identically by ``dispatch_membership_backfill``) so a self-hosted
        install with no Postgres Organization rows still gets its daily
        metrics computed under the ``"default"`` org_id instead of silently
        dispatching nothing.
        """
        from dev_health_ops.workers import metrics_partitioned

        session = MagicMock()
        session.execute.return_value.all.return_value = []
        cm = MagicMock()
        cm.__enter__.return_value = session
        cm.__exit__.return_value = False

        with (
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=cm,
            ),
            patch.object(
                metrics_partitioned.dispatch_daily_metrics_partitioned, "apply_async"
            ) as mock_apply,
        ):
            result = metrics_partitioned.dispatch_daily_metrics_for_all_orgs.run()

        mock_apply.assert_called_once()
        assert mock_apply.call_args.kwargs["kwargs"]["org_id"] == "default"
        assert result["dispatched"] == ["default"]
