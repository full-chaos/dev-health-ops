"""Unit tests for ops fixture metric-coherence (CHAOS-2040).

Guards the invariants defined in ``dev_health_ops/fixtures/coherence.py``
and the Rule 1 enforcement baked into the generator methods.

Test charter
============
1. ``validate_all`` passes on correctly-formed fixture bundles.
2. ``validate_all`` raises ``CoherenceError`` on every category of known
   violation, with a descriptive message.
3. Generator methods produce rows that pass ``validate_all`` across a
   range of seeds (non-flaky regression surface).
4. Deliberately broken rows trigger the correct per-check function.
"""

from __future__ import annotations

import pytest

from dev_health_ops.fixtures.coherence import (
    CoherenceError,
    FixtureBundle,
    check_commit_stats,
    check_coverage_snapshots,
    check_test_suite_results,
    check_work_item_metrics,
    validate_all,
)
from dev_health_ops.fixtures.generator import SyntheticDataGenerator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gen(seed: int = 42) -> SyntheticDataGenerator:
    return SyntheticDataGenerator(repo_name="acme/test-repo", provider="github", seed=seed)


def _valid_coverage_row(**overrides) -> dict:
    base = {
        "run_id": "run-1",
        "line_coverage_pct": 80.0,
        "branch_coverage_pct": 70.0,
        "lines_total": 10_000,
        "lines_covered": 8_000,
        "branches_total": 3_000,
        "branches_covered": 2_100,
    }
    base.update(overrides)
    return base


def _valid_suite_row(**overrides) -> dict:
    base = {
        "suite_id": "suite-1",
        "total_count": 100,
        "passed_count": 80,
        "failed_count": 10,
        "skipped_count": 5,
        "error_count": 5,
    }
    base.update(overrides)
    return base


def _valid_work_item_row(**overrides) -> dict:
    base = {
        "work_scope_id": "repo",
        "day": "2024-01-01",
        "team_id": "team-1",
        "items_started": 5,
        "items_started_unassigned": 2,
        "items_completed": 4,
        "items_completed_unassigned": 1,
        "wip_count_end_of_day": 10,
        "wip_unassigned_end_of_day": 3,
        "cycle_time_p50_hours": 24.0,
        "cycle_time_p90_hours": 72.0,
        "lead_time_p50_hours": 48.0,
        "lead_time_p90_hours": 96.0,
        "wip_age_p50_hours": 12.0,
        "wip_age_p90_hours": 48.0,
    }
    base.update(overrides)
    return base


def _valid_commit_stat_row(**overrides) -> dict:
    base = {
        "commit_hash": "abc123",
        "file_path": "src/foo.py",
        "additions": 50,
        "deletions": 20,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# validate_all — happy path
# ---------------------------------------------------------------------------


class TestValidateAllHappyPath:
    def test_empty_bundle_passes(self) -> None:
        validate_all(FixtureBundle())  # should not raise

    def test_valid_rows_pass(self) -> None:
        bundle = FixtureBundle(
            coverage_snapshots=[_valid_coverage_row()],
            test_suite_results=[_valid_suite_row()],
            work_item_metrics=[_valid_work_item_row()],
            commit_stats=[_valid_commit_stat_row()],
        )
        validate_all(bundle)  # should not raise

    def test_none_collections_are_skipped(self) -> None:
        bundle = FixtureBundle(
            coverage_snapshots=None,
            test_suite_results=[_valid_suite_row()],
        )
        validate_all(bundle)


# ---------------------------------------------------------------------------
# check_coverage_snapshots
# ---------------------------------------------------------------------------


class TestCheckCoverageSnapshots:
    def test_valid_row_produces_no_violations(self) -> None:
        assert check_coverage_snapshots([_valid_coverage_row()]) == []

    def test_branch_exceeds_line_pct(self) -> None:
        row = _valid_coverage_row(line_coverage_pct=70.0, branch_coverage_pct=75.0)
        violations = check_coverage_snapshots([row])
        assert len(violations) == 1
        assert "branch_coverage_pct" in violations[0]

    def test_lines_covered_exceeds_total(self) -> None:
        row = _valid_coverage_row(lines_covered=10_001, lines_total=10_000)
        violations = check_coverage_snapshots([row])
        assert any("lines_covered" in v for v in violations)

    def test_branches_covered_exceeds_total(self) -> None:
        row = _valid_coverage_row(branches_covered=3_001, branches_total=3_000)
        violations = check_coverage_snapshots([row])
        assert any("branches_covered" in v for v in violations)

    def test_equal_branch_and_line_pct_is_valid(self) -> None:
        row = _valid_coverage_row(line_coverage_pct=80.0, branch_coverage_pct=80.0)
        assert check_coverage_snapshots([row]) == []

    def test_multiple_violations_reported(self) -> None:
        row = _valid_coverage_row(
            line_coverage_pct=60.0,
            branch_coverage_pct=80.0,
            lines_covered=11_000,
            lines_total=10_000,
        )
        violations = check_coverage_snapshots([row])
        assert len(violations) == 2


# ---------------------------------------------------------------------------
# check_test_suite_results
# ---------------------------------------------------------------------------


class TestCheckTestSuiteResults:
    def test_valid_row_produces_no_violations(self) -> None:
        assert check_test_suite_results([_valid_suite_row()]) == []

    def test_sum_equals_total_is_valid(self) -> None:
        row = _valid_suite_row(
            total_count=100, passed_count=80, failed_count=10,
            skipped_count=5, error_count=5,
        )
        assert check_test_suite_results([row]) == []

    def test_sum_less_than_total_is_valid(self) -> None:
        # quarantined tests may not appear in any sub-count bucket
        row = _valid_suite_row(
            total_count=100, passed_count=70, failed_count=10,
            skipped_count=5, error_count=5,
        )
        assert check_test_suite_results([row]) == []

    def test_sum_exceeds_total_is_violation(self) -> None:
        row = _valid_suite_row(
            total_count=100, passed_count=90, failed_count=10,
            skipped_count=5, error_count=5,
        )
        violations = check_test_suite_results([row])
        assert len(violations) == 1
        assert "suite_id" not in violations[0] or "suite-1" in violations[0]

    def test_overflow_from_min_failed_floor(self) -> None:
        """Regression: forcing min failed ≥ N while pass_rate is high used to overflow."""
        row = _valid_suite_row(
            total_count=50, passed_count=47, failed_count=10,
            skipped_count=2, error_count=2,
        )
        violations = check_test_suite_results([row])
        assert violations  # 47+10+2+2=61 > 50


# ---------------------------------------------------------------------------
# check_work_item_metrics
# ---------------------------------------------------------------------------


class TestCheckWorkItemMetrics:
    def test_valid_row_produces_no_violations(self) -> None:
        assert check_work_item_metrics([_valid_work_item_row()]) == []

    def test_unassigned_completed_exceeds_completed(self) -> None:
        row = _valid_work_item_row(items_completed=2, items_completed_unassigned=3)
        violations = check_work_item_metrics([row])
        assert any("items_completed_unassigned" in v for v in violations)

    def test_unassigned_started_exceeds_started(self) -> None:
        row = _valid_work_item_row(items_started=3, items_started_unassigned=5)
        violations = check_work_item_metrics([row])
        assert any("items_started_unassigned" in v for v in violations)

    def test_wip_unassigned_exceeds_wip(self) -> None:
        row = _valid_work_item_row(wip_count_end_of_day=5, wip_unassigned_end_of_day=8)
        violations = check_work_item_metrics([row])
        assert any("wip_unassigned" in v for v in violations)

    def test_cycle_time_p90_less_than_p50(self) -> None:
        row = _valid_work_item_row(cycle_time_p50_hours=80.0, cycle_time_p90_hours=40.0)
        violations = check_work_item_metrics([row])
        assert any("cycle_time_p90" in v for v in violations)

    def test_lead_time_less_than_cycle_time(self) -> None:
        row = _valid_work_item_row(
            cycle_time_p50_hours=60.0, lead_time_p50_hours=48.0
        )
        violations = check_work_item_metrics([row])
        assert any("lead_time_p50" in v for v in violations)

    def test_lead_time_equals_cycle_time_is_valid(self) -> None:
        row = _valid_work_item_row(
            cycle_time_p50_hours=48.0, lead_time_p50_hours=48.0
        )
        assert check_work_item_metrics([row]) == []

    def test_wip_age_p90_less_than_p50(self) -> None:
        row = _valid_work_item_row(wip_age_p50_hours=50.0, wip_age_p90_hours=30.0)
        violations = check_work_item_metrics([row])
        assert any("wip_age_p90" in v for v in violations)


# ---------------------------------------------------------------------------
# check_commit_stats
# ---------------------------------------------------------------------------


class TestCheckCommitStats:
    def test_valid_row_produces_no_violations(self) -> None:
        assert check_commit_stats([_valid_commit_stat_row()]) == []

    def test_deletions_equal_additions_is_valid(self) -> None:
        row = _valid_commit_stat_row(additions=30, deletions=30)
        assert check_commit_stats([row]) == []

    def test_deletions_exceed_additions_is_violation(self) -> None:
        row = _valid_commit_stat_row(additions=20, deletions=30)
        violations = check_commit_stats([row])
        assert len(violations) == 1
        assert "deletions" in violations[0]


# ---------------------------------------------------------------------------
# validate_all — collects all violations before raising
# ---------------------------------------------------------------------------


class TestValidateAllCollectsViolations:
    def test_raises_with_all_violations_in_one_error(self) -> None:
        bundle = FixtureBundle(
            coverage_snapshots=[
                _valid_coverage_row(line_coverage_pct=50.0, branch_coverage_pct=80.0)
            ],
            test_suite_results=[
                _valid_suite_row(total_count=10, passed_count=9, failed_count=5,
                                 skipped_count=0, error_count=0)
            ],
        )
        with pytest.raises(CoherenceError) as exc_info:
            validate_all(bundle)
        error = exc_info.value
        assert len(error.violations) == 2  # one per domain

    def test_error_message_lists_domains(self) -> None:
        bundle = FixtureBundle(
            commit_stats=[_valid_commit_stat_row(additions=5, deletions=10)]
        )
        with pytest.raises(CoherenceError) as exc_info:
            validate_all(bundle)
        assert "commit_stat" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Generator integration — across multiple seeds
# ---------------------------------------------------------------------------


SEEDS = [0, 1, 7, 42, 99, 137, 255, 1024]


class TestGeneratorCoherence:
    """Generated rows must satisfy validate_all for every seed in SEEDS."""

    @pytest.mark.parametrize("seed", SEEDS)
    def test_coverage_snapshots_are_coherent(self, seed: int) -> None:
        gen = _make_gen(seed)
        pipeline_runs = gen.generate_ci_pipeline_runs(days=14)
        snapshots = gen.generate_coverage_snapshots(pipeline_runs, days=14)
        bundle = FixtureBundle(coverage_snapshots=snapshots)
        validate_all(bundle)

    @pytest.mark.parametrize("seed", SEEDS)
    def test_test_suite_results_are_coherent(self, seed: int) -> None:
        gen = _make_gen(seed)
        pipeline_runs = gen.generate_ci_pipeline_runs(days=14)
        job_runs = gen.generate_ci_job_runs(pipeline_runs)
        executions = gen.generate_test_executions(job_runs, days=14)
        bundle = FixtureBundle(
            test_suite_results=executions["suite_results"],
        )
        validate_all(bundle)

    @pytest.mark.parametrize("seed", SEEDS)
    def test_work_item_metrics_are_coherent(self, seed: int) -> None:
        gen = _make_gen(seed)
        records = gen.generate_work_item_metrics(days=14)
        # Convert dataclass records to dicts for the validator
        row_dicts = [r.__dict__ if hasattr(r, "__dict__") else dict(r) for r in records]
        bundle = FixtureBundle(work_item_metrics=row_dicts)
        validate_all(bundle)

    @pytest.mark.parametrize("seed", SEEDS)
    def test_commit_stats_are_coherent(self, seed: int) -> None:
        gen = _make_gen(seed)
        commits = gen.generate_commits(days=14)
        stats = gen.generate_commit_stats(commits)
        stat_dicts = [s.__dict__ if hasattr(s, "__dict__") else dict(s) for s in stats]
        bundle = FixtureBundle(commit_stats=stat_dicts)
        validate_all(bundle)

    @pytest.mark.parametrize("seed", SEEDS)
    def test_full_bundle_is_coherent(self, seed: int) -> None:
        """All domains together still satisfy validate_all."""
        gen = _make_gen(seed)
        pipeline_runs = gen.generate_ci_pipeline_runs(days=14)
        job_runs = gen.generate_ci_job_runs(pipeline_runs)
        executions = gen.generate_test_executions(job_runs, days=14)
        commits = gen.generate_commits(days=14)
        stats = gen.generate_commit_stats(commits)
        snapshots = gen.generate_coverage_snapshots(pipeline_runs, days=14)
        wi_records = gen.generate_work_item_metrics(days=14)

        bundle = FixtureBundle(
            coverage_snapshots=snapshots,
            test_suite_results=executions["suite_results"],
            work_item_metrics=[
                r.__dict__ if hasattr(r, "__dict__") else dict(r)
                for r in wi_records
            ],
            commit_stats=[
                s.__dict__ if hasattr(s, "__dict__") else dict(s) for s in stats
            ],
        )
        validate_all(bundle)
