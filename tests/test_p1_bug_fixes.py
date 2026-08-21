"""Tests for P1 bug fixes: gh-377 (active_repos scope) and gh-378 (metrics scheduling)."""

import uuid
from typing import TypedDict
from uuid import UUID


class _RepoRow(TypedDict):
    repo_id: UUID


class _MaybeRepoRow(TypedDict, total=False):
    repo_id: UUID
    some_other_key: str


class TestActiveReposUnion:
    """gh-377: active_repos must include repos from CI/CD and deployment data,
    not just commits."""

    def test_active_repos_includes_pipeline_only_repos(self):
        commit_rows: list[_RepoRow] = [
            {"repo_id": uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")},
        ]
        pipeline_rows: list[_MaybeRepoRow] = [
            {"repo_id": uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")},
        ]
        deployment_rows: list[_MaybeRepoRow] = []

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001") in active_repos
        assert uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002") in active_repos

    def test_active_repos_includes_deployment_only_repos(self):
        commit_rows: list[_RepoRow] = []
        pipeline_rows: list[_MaybeRepoRow] = []
        deployment_rows: list[_MaybeRepoRow] = [
            {"repo_id": uuid.UUID("cccccccc-0000-0000-0000-000000000003")},
        ]

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert uuid.UUID("cccccccc-0000-0000-0000-000000000003") in active_repos

    def test_active_repos_deduplicates_across_sources(self):
        shared_id = uuid.UUID("dddddddd-0000-0000-0000-000000000004")
        commit_rows: list[_RepoRow] = [{"repo_id": shared_id}]
        pipeline_rows: list[_MaybeRepoRow] = [{"repo_id": shared_id}]
        deployment_rows: list[_MaybeRepoRow] = [{"repo_id": shared_id}]

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert len(active_repos) == 1
        assert shared_id in active_repos

    def test_active_repos_handles_missing_repo_id_key(self):
        commit_rows: list[_RepoRow] = [
            {"repo_id": uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")},
        ]
        pipeline_rows: list[_MaybeRepoRow] = [{"some_other_key": "val"}]
        deployment_rows: list[_MaybeRepoRow] = [{"some_other_key": "val"}]

        active_repos = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}

        assert len(active_repos) == 1


class TestBeatScheduleMetrics:
    """Daily metrics stays explicitly scheduled after the obsolete sweep retires.

    CHAOS-4026 (2026-08-21): test_beat_schedule_contains_daily_metrics,
    test_obsolete_metrics_dispatcher_is_retired, and
    test_daily_metrics_uses_crontab tested the run-daily-metrics beat entry
    and dispatch_daily_metrics_for_all_orgs/metrics_tasks, all deleted with
    this cleanup (Go's daily_metrics_fanout fixed schedule now owns the
    periodic cadence). See tests/workers/test_celery_dead_code_contract.py.
    """

    def test_beat_schedule_retains_sync_dispatcher(self):
        from dev_health_ops.workers.config import beat_schedule

        assert "dispatch-scheduled-syncs" in beat_schedule
