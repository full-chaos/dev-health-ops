import pytest

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.models.git import CiPipelineRun


@pytest.fixture
def generator() -> SyntheticDataGenerator:
    return SyntheticDataGenerator(repo_name="acme/demo-app", seed=42)


@pytest.fixture
def pipeline_runs(generator: SyntheticDataGenerator) -> list[CiPipelineRun]:
    return generator.generate_ci_pipeline_runs(days=7, runs_per_day=3)


class TestGenerateCiJobRuns:
    def test_returns_nonempty(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        assert len(job_runs) > 0

    def test_each_job_references_valid_pipeline(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        run_ids = {pr.run_id for pr in pipeline_runs}
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        for job in job_runs:
            assert job["run_id"] in run_ids

    def test_job_has_required_fields(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        required = {
            "repo_id",
            "run_id",
            "job_id",
            "job_name",
            "status",
            "started_at",
            "finished_at",
            "duration_seconds",
        }
        for job in job_runs[:10]:
            assert required.issubset(job.keys()), (
                f"Missing keys: {required - job.keys()}"
            )

    def test_status_values(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        valid_statuses = {"success", "failed", "skipped"}
        for job in job_runs:
            assert job["status"] in valid_statuses

    def test_job_names_are_valid(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        valid_names = {"build", "test", "lint", "deploy", "integration-test"}
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        for job in job_runs:
            assert job["job_name"] in valid_names

    def test_two_to_five_jobs_per_pipeline(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        jobs_per_pipeline: dict[str, int] = {}
        for job in job_runs:
            jobs_per_pipeline[job["run_id"]] = (
                jobs_per_pipeline.get(job["run_id"], 0) + 1
            )
        for count in jobs_per_pipeline.values():
            assert 2 <= count <= 5

    def test_empty_pipeline_list(self, generator: SyntheticDataGenerator) -> None:
        assert generator.generate_ci_job_runs([]) == []


class TestGenerateTestExecutions:
    def test_returns_suite_and_case_results(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        result = generator.generate_test_executions(job_runs)
        assert "suite_results" in result
        assert "case_results" in result

    def test_only_test_jobs_produce_results(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        test_job_count = sum(
            1 for j in job_runs if j["job_name"] in ("test", "integration-test")
        )
        result = generator.generate_test_executions(job_runs)
        assert len(result["suite_results"]) == test_job_count

    def test_suite_counts_are_consistent(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        result = generator.generate_test_executions(job_runs)
        for suite in result["suite_results"]:
            total = suite["total_count"]
            assert total >= 50
            assert total <= 500
            assert suite["passed_count"] >= 0
            assert suite["failed_count"] >= 0
            assert suite["skipped_count"] >= 0

    def test_case_results_reference_valid_suites(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        result = generator.generate_test_executions(job_runs)
        suite_ids = {s["suite_id"] for s in result["suite_results"]}
        for case in result["case_results"][:50]:
            assert case["suite_id"] in suite_ids

    def test_case_required_fields(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        result = generator.generate_test_executions(job_runs)
        required = {
            "repo_id",
            "run_id",
            "suite_id",
            "case_id",
            "case_name",
            "status",
            "duration_seconds",
        }
        for case in result["case_results"][:10]:
            assert required.issubset(case.keys())

    def test_case_statuses(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        job_runs = generator.generate_ci_job_runs(pipeline_runs)
        result = generator.generate_test_executions(job_runs)
        valid = {"passed", "failed", "skipped"}
        for case in result["case_results"]:
            assert case["status"] in valid

    def test_empty_job_runs(self, generator: SyntheticDataGenerator) -> None:
        result = generator.generate_test_executions([])
        assert result["suite_results"] == []
        assert result["case_results"] == []


class TestGenerateCoverageSnapshots:
    def test_returns_snapshots(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        snapshots = generator.generate_coverage_snapshots(pipeline_runs, days=7)
        assert len(snapshots) > 0

    def test_line_coverage_in_range(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        snapshots = generator.generate_coverage_snapshots(pipeline_runs, days=7)
        for snap in snapshots:
            assert 0.0 <= snap["line_coverage_pct"] <= 100.0

    def test_branch_coverage_less_than_line(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        snapshots = generator.generate_coverage_snapshots(pipeline_runs, days=7)
        for snap in snapshots:
            assert snap["branch_coverage_pct"] <= snap["line_coverage_pct"]

    def test_required_fields(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        snapshots = generator.generate_coverage_snapshots(pipeline_runs, days=7)
        required = {
            "repo_id",
            "run_id",
            "snapshot_id",
            "lines_total",
            "lines_covered",
            "line_coverage_pct",
        }
        for snap in snapshots[:5]:
            assert required.issubset(snap.keys())

    def test_lines_covered_consistent(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        snapshots = generator.generate_coverage_snapshots(pipeline_runs, days=7)
        for snap in snapshots:
            assert snap["lines_covered"] <= snap["lines_total"]

    def test_references_valid_pipeline_runs(
        self, generator: SyntheticDataGenerator, pipeline_runs: list[CiPipelineRun]
    ) -> None:
        run_ids = {pr.run_id for pr in pipeline_runs}
        snapshots = generator.generate_coverage_snapshots(pipeline_runs, days=7)
        for snap in snapshots:
            assert snap["run_id"] in run_ids

    def test_empty_pipeline_list(self, generator: SyntheticDataGenerator) -> None:
        assert generator.generate_coverage_snapshots([], days=7) == []


class TestGeneratePipelineRunExtendedRows:
    """CHAOS-2173: pipeline extended rows must carry retry_count and team_id."""

    def test_returns_nonempty(self, generator: SyntheticDataGenerator) -> None:
        rows = generator.generate_pipeline_run_extended_rows(days=7, runs_per_day=3)
        assert len(rows) > 0

    def test_required_fields_present(self, generator: SyntheticDataGenerator) -> None:
        rows = generator.generate_pipeline_run_extended_rows(days=7, runs_per_day=3)
        required = {
            "repo_id",
            "run_id",
            "provider",
            "status",
            "started_at",
            "finished_at",
        }
        for row in rows[:10]:
            assert required.issubset(row.keys()), f"Missing: {required - row.keys()}"

    def test_retry_count_varies(self, generator: SyntheticDataGenerator) -> None:
        """At least one run should have retry_count > 0 across 30 days (~10% rate)."""
        rows = generator.generate_pipeline_run_extended_rows(days=30, runs_per_day=5)
        retry_counts = [row.get("retry_count", 0) for row in rows]
        assert any(c > 0 for c in retry_counts), (
            "Expected some runs with retry_count > 0 (approx 10% rate)"
        )

    def test_team_id_populated_when_teams_assigned(self) -> None:
        """team_id must be set on rows when the generator has assigned_teams."""
        from dev_health_ops.models.teams import Team

        teams = [
            Team(id="t-alpha", name="Alpha", members=["alice@example.com"]),
            Team(id="t-beta", name="Beta", members=["bob@example.com"]),
        ]
        gen = SyntheticDataGenerator(
            repo_name="acme/demo-app", seed=42, assigned_teams=teams
        )
        rows = gen.generate_pipeline_run_extended_rows(days=7, runs_per_day=3)
        team_ids = [row.get("team_id") for row in rows]
        assert any(t is not None for t in team_ids), (
            "Expected at least some runs to have non-NULL team_id"
        )
        # All assigned team IDs must come from the known list.
        valid_ids = {"t-alpha", "t-beta", None}
        for tid in team_ids:
            assert tid in valid_ids, f"Unexpected team_id: {tid}"

    def test_compute_gives_nonzero_rerun_rate(self) -> None:
        """CHAOS-2173: compute_pipeline_metrics_daily must report rerun_rate > 0."""
        from datetime import date, datetime, timezone
        from typing import cast

        from dev_health_ops.metrics.compute_testops import (
            compute_pipeline_metrics_daily,
        )
        from dev_health_ops.metrics.testops_schemas import PipelineRunExtendedRow
        from dev_health_ops.models.teams import Team

        teams = [Team(id="t-alpha", name="Alpha", members=[])]
        gen = SyntheticDataGenerator(
            repo_name="acme/metrics-test", seed=7, assigned_teams=teams
        )
        target_day = date(2025, 6, 15)
        # Build synthetic PipelineRunExtendedRow dicts for a controlled single day.
        rows = [
            {
                "repo_id": gen.repo_id,
                "run_id": "r-001",
                "provider": "github_actions",
                "status": "success",
                "queued_at": datetime(2025, 6, 15, 8, 0, tzinfo=timezone.utc),
                "started_at": datetime(2025, 6, 15, 8, 5, tzinfo=timezone.utc),
                "finished_at": datetime(2025, 6, 15, 8, 20, tzinfo=timezone.utc),
                "retry_count": 0,
                "team_id": "t-alpha",
                "org_id": "org-test",
            },
            {
                "repo_id": gen.repo_id,
                "run_id": "r-002",
                "provider": "github_actions",
                "status": "success",
                "queued_at": datetime(2025, 6, 15, 9, 0, tzinfo=timezone.utc),
                "started_at": datetime(2025, 6, 15, 9, 5, tzinfo=timezone.utc),
                "finished_at": datetime(2025, 6, 15, 9, 25, tzinfo=timezone.utc),
                "retry_count": 2,  # This run was retried.
                "team_id": "t-alpha",
                "org_id": "org-test",
            },
        ]
        records = compute_pipeline_metrics_daily(
            day=target_day,
            pipeline_runs=cast(list[PipelineRunExtendedRow], rows),
            job_runs=[],
            computed_at=datetime(2025, 6, 15, 12, 0, tzinfo=timezone.utc),
        )
        assert len(records) == 1
        rec = records[0]
        assert rec.rerun_rate == pytest.approx(0.5)  # 1 of 2 runs has retry_count > 0
        assert rec.team_id == "t-alpha"  # non-NULL team attribution
