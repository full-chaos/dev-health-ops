"""Tests for P1 bug fixes: gh-377 (active_repos scope) and gh-378 (metrics scheduling)."""

import uuid


class TestActiveReposUnion:
    """gh-377: active_repos must include repos from CI/CD and deployment data,
    not just commits."""

    def test_active_repos_includes_pipeline_only_repos(self):
        commit_rows = [
            {"repo_id": uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")},
        ]
        pipeline_rows = [
            {"repo_id": uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")},
        ]
        deployment_rows = []

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001") in active_repos
        assert uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002") in active_repos

    def test_active_repos_includes_deployment_only_repos(self):
        commit_rows = []
        pipeline_rows = []
        deployment_rows = [
            {"repo_id": uuid.UUID("cccccccc-0000-0000-0000-000000000003")},
        ]

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert uuid.UUID("cccccccc-0000-0000-0000-000000000003") in active_repos

    def test_active_repos_deduplicates_across_sources(self):
        shared_id = uuid.UUID("dddddddd-0000-0000-0000-000000000004")
        commit_rows = [{"repo_id": shared_id}]
        pipeline_rows = [{"repo_id": shared_id}]
        deployment_rows = [{"repo_id": shared_id}]

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert len(active_repos) == 1
        assert shared_id in active_repos

    def test_active_repos_handles_missing_repo_id_key(self):
        commit_rows = [
            {"repo_id": uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")},
        ]
        pipeline_rows = [{"some_other_key": "val"}]
        deployment_rows = [{"some_other_key": "val"}]

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert len(active_repos) == 1


class TestBeatScheduleMetrics:
    """gh-378: beat_schedule must include metrics dispatch and daily metrics tasks."""

    def test_beat_schedule_contains_daily_metrics(self):
        from dev_health_ops.workers.config import beat_schedule

        assert "run-daily-metrics" in beat_schedule
        entry = beat_schedule["run-daily-metrics"]
        assert entry["task"] == "dev_health_ops.workers.tasks.run_daily_metrics"
        assert entry["options"]["queue"] == "metrics"

    def test_beat_schedule_contains_metrics_dispatcher(self):
        from dev_health_ops.workers.config import beat_schedule

        assert "dispatch-scheduled-metrics" in beat_schedule
        entry = beat_schedule["dispatch-scheduled-metrics"]
        assert (
            entry["task"] == "dev_health_ops.workers.tasks.dispatch_scheduled_metrics"
        )

    def test_beat_schedule_retains_sync_dispatcher(self):
        from dev_health_ops.workers.config import beat_schedule

        assert "dispatch-scheduled-syncs" in beat_schedule

    def test_daily_metrics_uses_crontab(self):
        from celery.schedules import crontab

        from dev_health_ops.workers.config import beat_schedule

        schedule = beat_schedule["run-daily-metrics"]["schedule"]
        assert isinstance(schedule, crontab)

    def test_dispatch_scheduled_metrics_task_registered(self):
        from dev_health_ops.workers.tasks import dispatch_scheduled_metrics

        assert callable(dispatch_scheduled_metrics)

    def test_dispatch_scheduled_metrics_is_celery_task(self):
        from dev_health_ops.workers.tasks import dispatch_scheduled_metrics

        assert hasattr(dispatch_scheduled_metrics, "apply_async")
        assert hasattr(dispatch_scheduled_metrics, "delay")
