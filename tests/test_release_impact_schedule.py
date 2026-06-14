"""CHAOS-2381: release_impact_daily compute must be scheduled per active org.

The existing ``compute_release_impact_daily`` compute is org-scoped
(``WHERE org_id = {org_id:String}``). A single blank-org scheduled run would
match zero telemetry rows for every real (UUID-scoped) tenant, so the beat
entry dispatches one per-org compute. These tests prove:

* the wiring seam — a registered Celery task + a daily beat entry that points
  at the per-org dispatcher,
* the dispatcher fans out one ``run_release_impact_job`` per active org with
  the real org_id (never a blank scope), and
* the per-org task drives the real compute end to end and writes rows carrying
  that concrete org_id.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from celery.schedules import crontab

# Import connectors first to defeat the providers._base <-> connectors circular
# import that otherwise breaks collection when this module (transitively)
# imports compute/sink modules in isolation (CHAOS-2370 precedent).
import dev_health_ops.connectors  # noqa: F401


class FakeQueryResult:
    def __init__(self, column_names: list[str], result_rows: list[list]):
        self.column_names = column_names
        self.result_rows = result_rows


class TestReleaseImpactTaskRegistered:
    def test_task_importable_and_callable(self) -> None:
        from dev_health_ops.workers.metrics_extra import run_release_impact_job

        assert callable(run_release_impact_job)

    def test_tasks_exported_from_tasks_module(self) -> None:
        from dev_health_ops.workers import tasks

        assert "run_release_impact_job" in tasks.__all__
        assert hasattr(tasks, "run_release_impact_job")
        assert "dispatch_release_impact" in tasks.__all__
        assert hasattr(tasks, "dispatch_release_impact")

    def test_task_is_celery_task(self) -> None:
        from dev_health_ops.workers.metrics_extra import run_release_impact_job

        assert hasattr(run_release_impact_job, "apply_async")
        assert hasattr(run_release_impact_job, "delay")
        assert (
            run_release_impact_job.name
            == "dev_health_ops.workers.tasks.run_release_impact_job"
        )
        # Per-org compute lands on the metrics queue, alongside the other jobs.
        assert run_release_impact_job.queue == "metrics"

    def test_dispatcher_is_celery_task(self) -> None:
        from dev_health_ops.workers.metrics_extra import dispatch_release_impact

        assert (
            dispatch_release_impact.name
            == "dev_health_ops.workers.tasks.dispatch_release_impact"
        )
        # Dispatcher is lightweight (DB enumeration only) -> default queue,
        # mirroring dispatch_daily_metrics_partitioned.
        assert dispatch_release_impact.queue == "default"


class TestReleaseImpactBeatSchedule:
    def test_beat_schedule_dispatches_per_org(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        assert "run-release-impact-daily" in beat_schedule
        entry = beat_schedule["run-release-impact-daily"]
        # MUST point at the per-org dispatcher, not the blank-org compute task.
        assert entry["task"] == "dev_health_ops.workers.tasks.dispatch_release_impact"
        assert entry["options"]["queue"] == "default"

    def test_beat_schedule_uses_daily_crontab(self) -> None:
        from dev_health_ops.workers.config import beat_schedule

        schedule = beat_schedule["run-release-impact-daily"]["schedule"]
        assert isinstance(schedule, crontab)


class TestReleaseImpactDispatcherFansOutPerOrg:
    def test_dispatcher_enqueues_one_task_per_active_org(self) -> None:
        """The dispatcher enumerates active orgs and enqueues per real org_id."""
        from dev_health_ops.workers import metrics_extra

        org_a = str(uuid4())
        org_b = str(uuid4())

        # Fake postgres session returning two active org ids.
        session = MagicMock()
        session.execute.return_value.all.return_value = [(org_a,), (org_b,)]
        cm = MagicMock()
        cm.__enter__.return_value = session
        cm.__exit__.return_value = False

        with (
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=cm,
            ),
            patch.object(
                metrics_extra.run_release_impact_job, "apply_async"
            ) as mock_apply,
        ):
            result = metrics_extra.dispatch_release_impact.run(
                day="2026-06-13", backfill_days=1
            )

        # One enqueue per active org, each carrying the REAL org_id (never "").
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
        """A transient Postgres failure must NOT report an empty success.

        Codex no-ship (round 2): swallowing the enumeration error and returning
        ``{"dispatched": [], "skipped": 0}`` makes Celery beat treat the run as
        successful while computing zero orgs — every tenant gets stale,
        flat-zero release-reliability cards for ~24h with no failure to page on.
        The dispatcher must surface the failure via Celery's retry machinery
        (which re-raises here) so transient errors are retried and a hard
        failure is signalled once retries are exhausted.
        """
        import celery.exceptions

        from dev_health_ops.workers import metrics_extra

        cm = MagicMock()
        cm.__enter__.side_effect = RuntimeError("postgres unavailable")

        with (
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=cm,
            ),
            patch.object(
                metrics_extra.run_release_impact_job, "apply_async"
            ) as mock_apply,
        ):
            # bound .retry() re-raises (Retry for transient, or the original
            # exc once max_retries is exhausted) — never a normal return.
            raised: Exception | None = None
            try:
                metrics_extra.dispatch_release_impact.run()
            except (celery.exceptions.Retry, RuntimeError) as exc:
                raised = exc

        assert raised is not None, (
            "dispatcher must raise (retry/failure) on enumeration failure, "
            "not silently return an empty success"
        )
        # Nothing was dispatched, and the task did NOT return an empty success.
        mock_apply.assert_not_called()

    def test_dispatcher_no_active_orgs_dispatches_nothing(self) -> None:
        from dev_health_ops.workers import metrics_extra

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
                metrics_extra.run_release_impact_job, "apply_async"
            ) as mock_apply,
        ):
            result = metrics_extra.dispatch_release_impact.run()

        mock_apply.assert_not_called()
        assert result["dispatched"] == []


class TestReleaseImpactTaskWritesRowsForRealOrg:
    def test_task_computes_and_writes_rows_for_concrete_org(self) -> None:
        """End-to-end: a real UUID org's telemetry yields rows tagged that org.

        Drives the per-org Celery task against a seeded fake ClickHouse client
        (no mocking-away of the compute), proving release_impact_daily rows are
        written and carry the concrete org_id — the live-path defect the prior
        attempt left unproven.
        """
        from dev_health_ops.workers import metrics_extra

        org_id = str(uuid4())
        repo_id = uuid4()
        deploy_ts = datetime(2026, 6, 13, 10, 0, tzinfo=timezone.utc)

        # Seeded responses mirror the single-release compute path
        # (test_compute_release_impact.py::test_compute_day_single_release).
        # The recomputation window is pinned to 1 day so exactly one
        # _compute_day pass runs.
        responses = [
            FakeQueryResult(["release_ref", "environment"], [["v1.0.0", "production"]]),
            FakeQueryResult(["cnt"], [[2]]),
            FakeQueryResult(["deploy_ts"], [[deploy_ts]]),
            FakeQueryResult(["repo_id"], [[str(repo_id)]]),
            FakeQueryResult(["total_signals", "total_sessions"], [[10, 500]]),
            FakeQueryResult(["total_signals", "total_sessions"], [[15, 600]]),
            FakeQueryResult(["total_signals", "total_sessions"], [[5, 400]]),
            FakeQueryResult(["total_signals", "total_sessions"], [[8, 500]]),
            FakeQueryResult(["total_signals", "total_sessions"], [[15, 600]]),
            FakeQueryResult(["total_signals", "total_sessions"], [[8, 500]]),
            FakeQueryResult(["first_friction_ts"], [[deploy_ts + timedelta(hours=2)]]),
            FakeQueryResult(["cnt"], [[1]]),
            FakeQueryResult(["bucket_hours"], [[20]]),
        ]
        captured_org_filters: list[str] = []

        def fake_query(query: str, parameters: dict | None = None):
            # Prove the compute is genuinely org-scoped to THIS org.
            if parameters and "org_id" in parameters:
                captured_org_filters.append(parameters["org_id"])
            return responses.pop(0)

        fake_client = MagicMock()
        fake_client.query = MagicMock(side_effect=fake_query)

        written_records: list = []
        fake_sink = MagicMock()
        fake_sink.client = fake_client
        fake_sink.write_release_impact_daily = MagicMock(
            side_effect=lambda rows: written_records.extend(rows)
        )

        with (
            patch.object(
                metrics_extra, "_get_db_url", return_value="clickhouse://unit"
            ),
            patch(
                "dev_health_ops.workers.metrics_extra.organization_exists_sync",
                return_value=True,
            ),
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=MagicMock(),
            ),
            patch(
                "dev_health_ops.metrics.job_release_impact.ClickHouseMetricsSink",
                return_value=fake_sink,
            ),
        ):
            result = metrics_extra.run_release_impact_job.run(
                day="2026-06-13",
                backfill_days=1,
                recomputation_window_days=1,
                org_id=org_id,
            )

        assert result["status"] == "success"
        assert result["records_written"] == 1
        # Rows were actually written, and carry the concrete org_id.
        assert len(written_records) == 1
        assert written_records[0].org_id == org_id
        assert written_records[0].release_ref == "v1.0.0"
        # And the compute filtered telemetry by exactly this org (never blank).
        assert captured_org_filters
        assert set(captured_org_filters) == {org_id}

    def test_deleted_org_is_skipped_without_compute(self) -> None:
        """A per-org task for a deleted org short-circuits before compute."""
        from dev_health_ops.workers import metrics_extra

        org_id = str(uuid4())
        sink_factory = MagicMock()

        with (
            patch.object(
                metrics_extra, "_get_db_url", return_value="clickhouse://unit"
            ),
            patch(
                "dev_health_ops.workers.metrics_extra.organization_exists_sync",
                return_value=False,
            ),
            patch(
                "dev_health_ops.db.get_postgres_session_sync",
                return_value=MagicMock(),
            ),
            patch(
                "dev_health_ops.metrics.job_release_impact.ClickHouseMetricsSink",
                sink_factory,
            ),
        ):
            result = metrics_extra.run_release_impact_job.run(
                day="2026-06-13", org_id=org_id
            )

        assert result["status"] == "skipped"
        assert result["reason"] == "organization_not_found"
        # Compute never constructed a sink / touched ClickHouse.
        sink_factory.assert_not_called()


class TestReleaseImpactTaskWiringDefaults:
    def test_task_defaults_db_url_and_today(self) -> None:
        """db_url falls back to _get_db_url and day defaults to today.

        Unlike the prior attempt, this does NOT assert org_id == "" is correct;
        org scope is owned by the dispatcher and exercised above.
        """
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
            mock_today.return_value = date(2026, 6, 13)
            metrics_extra.run_release_impact_job.run()

        assert captured["db_url"] == "clickhouse://default"
        assert str(captured["day"]) == "2026-06-13"

    def test_run_async_used_for_async_job(self) -> None:
        """The async compute is driven via the shared run_async helper."""
        from dev_health_ops.workers import metrics_extra

        async def fake_job(**kwargs: object) -> int:
            return 3

        def fake_run_async(coro: object) -> int:
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
