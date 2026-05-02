"""TestOps risk model computations: release confidence, quality drag, pipeline stability."""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from datetime import date, datetime

from dev_health_ops.metrics.testops_schemas import (
    CoverageMetricsDailyRecord,
    PipelineMetricsDailyRecord,
    PipelineStabilityRecord,
    QualityDragRecord,
    ReleaseConfidenceRecord,
    TestMetricsDailyRecord,
)


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def compute_release_confidence(
    *,
    day: date,
    pipeline_metrics: Sequence[PipelineMetricsDailyRecord],
    test_metrics: Sequence[TestMetricsDailyRecord],
    coverage_metrics: Sequence[CoverageMetricsDailyRecord],
    computed_at: datetime,
) -> list[ReleaseConfidenceRecord]:
    repo_ids: set[uuid.UUID] = set()
    pipe_by_repo: dict[uuid.UUID, PipelineMetricsDailyRecord] = {}
    test_by_repo: dict[uuid.UUID, TestMetricsDailyRecord] = {}
    cov_by_repo: dict[uuid.UUID, CoverageMetricsDailyRecord] = {}

    for pipeline_metric in pipeline_metrics:
        pipe_by_repo[pipeline_metric.repo_id] = pipeline_metric
        repo_ids.add(pipeline_metric.repo_id)
    for test_metric in test_metrics:
        test_by_repo[test_metric.repo_id] = test_metric
        repo_ids.add(test_metric.repo_id)
    for coverage_metric in coverage_metrics:
        cov_by_repo[coverage_metric.repo_id] = coverage_metric
        repo_ids.add(coverage_metric.repo_id)

    results: list[ReleaseConfidenceRecord] = []
    for repo_id in sorted(repo_ids, key=str):
        pipe = pipe_by_repo.get(repo_id)
        test = test_by_repo.get(repo_id)
        cov = cov_by_repo.get(repo_id)

        success_rate = pipe.success_rate if pipe else 0.0
        pass_rate = test.pass_rate if test else 0.0
        coverage_pct = (cov.line_coverage_pct or 0.0) if cov else 0.0
        flake_rate = test.flake_rate if test else 0.0
        failure_recurrence = test.failure_recurrence_score if test else 0.0
        coverage_delta = (cov.coverage_delta_pct or 0.0) if cov else 0.0

        pipeline_factor = 0.4 * success_rate
        test_factor = 0.3 * pass_rate
        cov_factor = 0.2 * _clamp(coverage_pct / 100.0)
        flake_factor = 0.1 * (1.0 - flake_rate)

        base_score = pipeline_factor + test_factor + cov_factor + flake_factor

        flake_penalty = 0.1 if flake_rate > 0.05 else 0.0
        regression_penalty = 0.0
        if coverage_delta < -2.0:
            regression_penalty += 0.05
        if failure_recurrence > 0.3:
            regression_penalty += 0.1

        score = _clamp(base_score - flake_penalty - regression_penalty)

        factors = {
            "pipeline_success_rate": round(success_rate, 4),
            "test_pass_rate": round(pass_rate, 4),
            "coverage_pct": round(coverage_pct, 2),
            "flake_rate": round(flake_rate, 4),
            "failure_recurrence": round(failure_recurrence, 4),
            "coverage_delta_pct": round(coverage_delta, 2),
            "base_score": round(base_score, 4),
            "flake_penalty": round(flake_penalty, 4),
            "regression_penalty": round(regression_penalty, 4),
        }

        results.append(
            ReleaseConfidenceRecord(
                repo_id=repo_id,
                day=day,
                confidence_score=round(score, 4),
                pipeline_success_factor=round(pipeline_factor, 4),
                test_pass_factor=round(test_factor, 4),
                coverage_factor=round(cov_factor, 4),
                flake_penalty=round(flake_penalty, 4),
                regression_penalty=round(regression_penalty, 4),
                factors_json=json.dumps(factors),
                computed_at=computed_at,
                team_id=pipe.team_id if pipe else (test.team_id if test else None),
                service_id=pipe.service_id
                if pipe
                else (test.service_id if test else None),
                org_id=(pipe.org_id if pipe else (test.org_id if test else "")),
            )
        )
    return results


def compute_quality_drag(
    *,
    day: date,
    pipeline_metrics: Sequence[PipelineMetricsDailyRecord],
    test_metrics: Sequence[TestMetricsDailyRecord],
    computed_at: datetime,
) -> list[QualityDragRecord]:
    repo_ids: set[uuid.UUID] = set()
    pipe_by_repo: dict[uuid.UUID, PipelineMetricsDailyRecord] = {}
    test_by_repo: dict[uuid.UUID, TestMetricsDailyRecord] = {}

    for pipeline_metric in pipeline_metrics:
        pipe_by_repo[pipeline_metric.repo_id] = pipeline_metric
        repo_ids.add(pipeline_metric.repo_id)
    for test_metric in test_metrics:
        test_by_repo[test_metric.repo_id] = test_metric
        repo_ids.add(test_metric.repo_id)

    results: list[QualityDragRecord] = []
    for repo_id in sorted(repo_ids, key=str):
        pipe = pipe_by_repo.get(repo_id)
        test = test_by_repo.get(repo_id)

        median_dur = (pipe.median_duration_seconds or 0.0) if pipe else 0.0
        failure_count = pipe.failure_count if pipe else 0
        pipelines_count = pipe.pipelines_count if pipe else 0
        avg_queue = (pipe.avg_queue_seconds or 0.0) if pipe else 0.0
        rerun_rate = pipe.rerun_rate if pipe else 0.0

        flake_rate = test.flake_rate if test else 0.0
        total_cases = test.total_cases if test else 0

        failure_rework_hours = failure_count * median_dur / 3600.0
        flake_investigation_hours = flake_rate * total_cases * 0.25
        queue_wait_hours = pipelines_count * avg_queue / 3600.0
        retry_overhead_hours = rerun_rate * pipelines_count * median_dur / 3600.0

        drag_hours = (
            failure_rework_hours
            + flake_investigation_hours
            + queue_wait_hours
            + retry_overhead_hours
        )

        factors = {
            "failure_count": failure_count,
            "median_duration_seconds": round(median_dur, 2),
            "pipelines_count": pipelines_count,
            "avg_queue_seconds": round(avg_queue, 2),
            "rerun_rate": round(rerun_rate, 4),
            "flake_rate": round(flake_rate, 4),
            "total_cases": total_cases,
        }

        results.append(
            QualityDragRecord(
                repo_id=repo_id,
                day=day,
                drag_hours=round(drag_hours, 4),
                failure_rework_hours=round(failure_rework_hours, 4),
                flake_investigation_hours=round(flake_investigation_hours, 4),
                queue_wait_hours=round(queue_wait_hours, 4),
                retry_overhead_hours=round(retry_overhead_hours, 4),
                factors_json=json.dumps(factors),
                computed_at=computed_at,
                team_id=pipe.team_id if pipe else (test.team_id if test else None),
                service_id=pipe.service_id
                if pipe
                else (test.service_id if test else None),
                org_id=(pipe.org_id if pipe else (test.org_id if test else "")),
            )
        )
    return results


def compute_pipeline_stability(
    *,
    day: date,
    pipeline_metrics_7d: Sequence[PipelineMetricsDailyRecord],
    computed_at: datetime,
) -> list[PipelineStabilityRecord]:
    by_repo: dict[uuid.UUID, list[PipelineMetricsDailyRecord]] = {}
    for m in pipeline_metrics_7d:
        by_repo.setdefault(m.repo_id, []).append(m)

    results: list[PipelineStabilityRecord] = []
    for repo_id in sorted(by_repo, key=str):
        days_data = sorted(by_repo[repo_id], key=lambda m: m.day)

        n = len(days_data)
        if n == 0:
            continue

        weights = [1.0 + i * 0.5 for i in range(n)]
        total_weight = sum(weights)
        success_rate_7d = (
            sum(m.success_rate * w for m, w in zip(days_data, weights)) / total_weight
        )

        if n >= 2:
            x_mean = (n - 1) / 2.0
            y_mean = sum(m.success_rate for m in days_data) / n
            num = sum(
                (i - x_mean) * (m.success_rate - y_mean)
                for i, m in enumerate(days_data)
            )
            den = sum((i - x_mean) ** 2 for i in range(n))
            success_rate_trend = num / den if den > 0 else 0.0
        else:
            success_rate_trend = 0.0

        consecutive_failures = 0
        total_failures = 0
        for i, m in enumerate(days_data):
            if m.failure_count > 0:
                total_failures += 1
                if i > 0 and days_data[i - 1].failure_count > 0:
                    consecutive_failures += 1

        failure_clustering = (
            consecutive_failures / max(total_failures, 1) if total_failures > 0 else 0.0
        )

        durations = [
            m.median_duration_seconds
            for m in days_data
            if m.median_duration_seconds is not None and m.failure_count > 0
        ]
        median_recovery = None
        if durations:
            sorted_d = sorted(durations)
            mid = len(sorted_d) // 2
            median_recovery = (
                sorted_d[mid]
                if len(sorted_d) % 2 == 1
                else (sorted_d[mid - 1] + sorted_d[mid]) / 2.0
            )

        stability = _clamp(
            success_rate_7d
            * (1.0 - failure_clustering)
            * (1.0 + min(success_rate_trend, 0.1))
        )

        latest = days_data[-1]
        results.append(
            PipelineStabilityRecord(
                repo_id=repo_id,
                day=day,
                stability_index=round(stability, 4),
                success_rate_7d=round(success_rate_7d, 4),
                success_rate_trend=round(success_rate_trend, 4),
                failure_clustering_score=round(failure_clustering, 4),
                median_recovery_time_seconds=(
                    round(median_recovery, 2) if median_recovery is not None else None
                ),
                computed_at=computed_at,
                team_id=latest.team_id,
                service_id=latest.service_id,
                org_id=latest.org_id,
            )
        )
    return results
