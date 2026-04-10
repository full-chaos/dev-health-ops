from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute_testops_risk import (
    compute_pipeline_stability,
    compute_quality_drag,
    compute_release_confidence,
)
from dev_health_ops.metrics.testops_schemas import (
    CoverageMetricsDailyRecord,
    PipelineMetricsDailyRecord,
    TestMetricsDailyRecord,
)

REPO_A = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
DAY = date(2025, 4, 10)
NOW = datetime(2025, 4, 10, 12, 0, 0, tzinfo=timezone.utc)


def _pipe(
    repo_id: uuid.UUID = REPO_A,
    day: date = DAY,
    success_rate: float = 0.9,
    failure_count: int = 2,
    pipelines_count: int = 20,
    median_duration_seconds: float = 300.0,
    avg_queue_seconds: float = 30.0,
    rerun_rate: float = 0.1,
) -> PipelineMetricsDailyRecord:
    return PipelineMetricsDailyRecord(
        repo_id=repo_id,
        day=day,
        pipelines_count=pipelines_count,
        success_count=int(pipelines_count * success_rate),
        failure_count=failure_count,
        cancelled_count=0,
        success_rate=success_rate,
        failure_rate=1 - success_rate,
        cancel_rate=0.0,
        rerun_rate=rerun_rate,
        median_duration_seconds=median_duration_seconds,
        p95_duration_seconds=600.0,
        avg_queue_seconds=avg_queue_seconds,
        p95_queue_seconds=60.0,
        computed_at=NOW,
    )


def _test(
    repo_id: uuid.UUID = REPO_A,
    day: date = DAY,
    pass_rate: float = 0.95,
    flake_rate: float = 0.02,
    total_cases: int = 100,
    failure_recurrence_score: float = 0.1,
) -> TestMetricsDailyRecord:
    return TestMetricsDailyRecord(
        repo_id=repo_id,
        day=day,
        total_cases=total_cases,
        passed_count=int(total_cases * pass_rate),
        failed_count=int(total_cases * (1 - pass_rate)),
        skipped_count=0,
        quarantined_count=0,
        pass_rate=pass_rate,
        failure_rate=1 - pass_rate,
        flake_rate=flake_rate,
        retry_dependency_rate=0.0,
        total_suites=5,
        suite_duration_p50_seconds=60.0,
        suite_duration_p95_seconds=120.0,
        failure_recurrence_score=failure_recurrence_score,
        computed_at=NOW,
    )


def _cov(
    repo_id: uuid.UUID = REPO_A,
    day: date = DAY,
    line_coverage_pct: float = 80.0,
    coverage_delta_pct: float = 0.5,
) -> CoverageMetricsDailyRecord:
    return CoverageMetricsDailyRecord(
        repo_id=repo_id,
        day=day,
        line_coverage_pct=line_coverage_pct,
        branch_coverage_pct=70.0,
        lines_total=10000,
        lines_covered=8000,
        coverage_delta_pct=coverage_delta_pct,
        uncovered_files_count=0,
        coverage_regression_count=0,
        computed_at=NOW,
    )


def test_release_confidence_healthy_repo():
    results = compute_release_confidence(
        day=DAY,
        pipeline_metrics=[_pipe(success_rate=0.95)],
        test_metrics=[_test(pass_rate=0.98, flake_rate=0.01)],
        coverage_metrics=[_cov(line_coverage_pct=85.0)],
        computed_at=NOW,
    )
    assert len(results) == 1
    r = results[0]
    assert r.confidence_score > 0.8
    assert r.flake_penalty == 0.0
    assert r.regression_penalty == 0.0
    factors = json.loads(r.factors_json)
    assert "pipeline_success_rate" in factors


def test_release_confidence_with_penalties():
    results = compute_release_confidence(
        day=DAY,
        pipeline_metrics=[_pipe(success_rate=0.7)],
        test_metrics=[
            _test(pass_rate=0.8, flake_rate=0.1, failure_recurrence_score=0.5)
        ],
        coverage_metrics=[_cov(line_coverage_pct=50.0, coverage_delta_pct=-5.0)],
        computed_at=NOW,
    )
    r = results[0]
    assert r.flake_penalty == 0.1
    assert r.regression_penalty == 0.15
    assert r.confidence_score < 0.7


def test_release_confidence_empty_inputs():
    results = compute_release_confidence(
        day=DAY,
        pipeline_metrics=[],
        test_metrics=[],
        coverage_metrics=[],
        computed_at=NOW,
    )
    assert results == []


def test_quality_drag_computation():
    results = compute_quality_drag(
        day=DAY,
        pipeline_metrics=[
            _pipe(
                failure_count=5,
                pipelines_count=20,
                median_duration_seconds=600,
                avg_queue_seconds=60,
                rerun_rate=0.15,
            )
        ],
        test_metrics=[_test(flake_rate=0.04, total_cases=200)],
        computed_at=NOW,
    )
    assert len(results) == 1
    r = results[0]
    assert r.failure_rework_hours > 0
    assert r.flake_investigation_hours > 0
    assert r.queue_wait_hours > 0
    assert r.retry_overhead_hours > 0
    expected_sum = (
        r.failure_rework_hours
        + r.flake_investigation_hours
        + r.queue_wait_hours
        + r.retry_overhead_hours
    )
    assert abs(r.drag_hours - expected_sum) < 0.01


def test_quality_drag_zero_failures():
    results = compute_quality_drag(
        day=DAY,
        pipeline_metrics=[
            _pipe(failure_count=0, rerun_rate=0.0, avg_queue_seconds=0.0)
        ],
        test_metrics=[_test(flake_rate=0.0, total_cases=100)],
        computed_at=NOW,
    )
    r = results[0]
    assert r.drag_hours == 0.0


def test_pipeline_stability_improving_trend():
    metrics_7d = [
        _pipe(day=date(2025, 4, d), success_rate=0.8 + d * 0.02, failure_count=0)
        for d in range(4, 11)
    ]
    results = compute_pipeline_stability(
        day=DAY, pipeline_metrics_7d=metrics_7d, computed_at=NOW
    )
    assert len(results) == 1
    r = results[0]
    assert r.success_rate_trend > 0
    assert r.stability_index > 0.5


def test_pipeline_stability_single_day():
    results = compute_pipeline_stability(
        day=DAY,
        pipeline_metrics_7d=[_pipe(success_rate=1.0, failure_count=0)],
        computed_at=NOW,
    )
    assert len(results) == 1
    r = results[0]
    assert r.success_rate_trend == 0.0
    assert r.stability_index > 0.9


def test_pipeline_stability_all_failures():
    metrics_7d = [
        _pipe(day=date(2025, 4, d), success_rate=0.0, failure_count=20)
        for d in range(4, 11)
    ]
    results = compute_pipeline_stability(
        day=DAY, pipeline_metrics_7d=metrics_7d, computed_at=NOW
    )
    r = results[0]
    assert r.stability_index == 0.0
    assert r.failure_clustering_score > 0
