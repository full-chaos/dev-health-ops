from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime

from dev_health_ops.metrics.compute import _median, _percentile, _utc_day_window
from dev_health_ops.metrics.testops_schemas import (
    CoverageMetricsDailyRecord,
    CoverageSnapshotRow,
    JobRunRow,
    PipelineMetricsDailyRecord,
    PipelineRunExtendedRow,
    TestCaseResultRow,
    TestMetricsDailyRecord,
    TestSuiteResultRow,
)
from dev_health_ops.utils.datetime import to_utc


def _normalize_pipeline_status(status: str | None) -> str:
    normalized = (status or "").strip().lower()
    if normalized in {"success", "succeeded", "passed"}:
        return "success"
    if normalized in {"failure", "failed", "error", "errors", "timeout", "timed_out"}:
        return "failure"
    if normalized in {"cancelled", "canceled", "cancel"}:
        return "cancelled"
    return normalized


def _normalize_test_status(status: str | None) -> str:
    normalized = (status or "").strip().lower()
    if normalized in {"success", "succeeded", "passed"}:
        return "passed"
    if normalized in {"failure", "failed", "error", "errors", "timeout", "timed_out"}:
        return "failed"
    if normalized in {"quarantined", "quarantine"}:
        return "quarantined"
    if normalized in {"skipped", "skip"}:
        return "skipped"
    return normalized


def _safe_duration_seconds(
    started_at: datetime | None,
    finished_at: datetime | None,
    explicit_seconds: float | None,
) -> float | None:
    if explicit_seconds is not None and explicit_seconds >= 0:
        return float(explicit_seconds)
    if started_at is None or finished_at is None:
        return None
    duration_seconds = (to_utc(finished_at) - to_utc(started_at)).total_seconds()
    return float(duration_seconds) if duration_seconds >= 0 else None


def _safe_queue_seconds(
    queued_at: datetime | None,
    started_at: datetime | None,
    explicit_seconds: float | None,
) -> float | None:
    if explicit_seconds is not None and explicit_seconds >= 0:
        return float(explicit_seconds)
    if queued_at is None or started_at is None:
        return None
    queue_seconds = (to_utc(started_at) - to_utc(queued_at)).total_seconds()
    return float(queue_seconds) if queue_seconds >= 0 else None


def _latest_snapshot_key(snapshot: CoverageSnapshotRow) -> tuple[str, str]:
    return (str(snapshot.get("run_id") or ""), str(snapshot.get("snapshot_id") or ""))


@dataclass
class _PipelineBucket:
    pipelines: int = 0
    success: int = 0
    failure: int = 0
    cancelled: int = 0
    reruns: int = 0
    durations: list[float] = field(default_factory=list)
    queues: list[float] = field(default_factory=list)
    org_id: str = ""


def compute_pipeline_metrics_daily(
    *,
    day: date,
    pipeline_runs: Sequence[PipelineRunExtendedRow],
    job_runs: Sequence[JobRunRow],
    computed_at: datetime,
) -> list[PipelineMetricsDailyRecord]:
    del job_runs

    start, end = _utc_day_window(day)
    computed_at_utc = to_utc(computed_at)

    by_group: dict[tuple[uuid.UUID, str | None, str | None], _PipelineBucket] = {}
    for row in pipeline_runs:
        started_at = to_utc(row["started_at"])
        if not (start <= started_at < end):
            continue

        repo_id = row["repo_id"]
        team_id = row.get("team_id")
        service_id = row.get("service_id")
        key = (repo_id, team_id, service_id)
        bucket = by_group.get(key)
        if bucket is None:
            bucket = _PipelineBucket(org_id=str(row.get("org_id") or ""))
            by_group[key] = bucket

        bucket.pipelines += 1
        status = _normalize_pipeline_status(row.get("status"))
        if status == "success":
            bucket.success += 1
        elif status == "failure":
            bucket.failure += 1
        elif status == "cancelled":
            bucket.cancelled += 1

        if int(row.get("retry_count") or 0) > 0:
            bucket.reruns += 1

        duration_seconds = _safe_duration_seconds(
            row.get("started_at"),
            row.get("finished_at"),
            row.get("duration_seconds"),
        )
        if duration_seconds is not None:
            bucket.durations.append(duration_seconds)

        queue_seconds = _safe_queue_seconds(
            row.get("queued_at"),
            row.get("started_at"),
            row.get("queue_seconds"),
        )
        if queue_seconds is not None:
            bucket.queues.append(queue_seconds)

    records: list[PipelineMetricsDailyRecord] = []
    for (repo_id, team_id, service_id), bucket in sorted(
        by_group.items(),
        key=lambda item: (str(item[0][0]), item[0][1] or "", item[0][2] or ""),
    ):
        pipelines = bucket.pipelines
        success = bucket.success
        failure = bucket.failure
        cancelled = bucket.cancelled
        reruns = bucket.reruns
        durations = list(bucket.durations)
        queues = list(bucket.queues)

        records.append(
            PipelineMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                pipelines_count=pipelines,
                success_count=success,
                failure_count=failure,
                cancelled_count=cancelled,
                success_rate=(success / pipelines) if pipelines else 0.0,
                failure_rate=(failure / pipelines) if pipelines else 0.0,
                cancel_rate=(cancelled / pipelines) if pipelines else 0.0,
                rerun_rate=(reruns / pipelines) if pipelines else 0.0,
                median_duration_seconds=float(_median(durations))
                if durations
                else None,
                p95_duration_seconds=float(_percentile(durations, 95.0))
                if durations
                else None,
                avg_queue_seconds=float(sum(queues) / len(queues)) if queues else None,
                p95_queue_seconds=float(_percentile(queues, 95.0)) if queues else None,
                computed_at=computed_at_utc,
                team_id=team_id,
                service_id=service_id,
                org_id=bucket.org_id,
            )
        )

    return records


def compute_test_metrics_daily(
    *,
    day: date,
    suite_results: Sequence[TestSuiteResultRow],
    case_results: Sequence[TestCaseResultRow],
    computed_at: datetime,
) -> list[TestMetricsDailyRecord]:
    start, end = _utc_day_window(day)
    computed_at_utc = to_utc(computed_at)

    current_suites_by_repo: dict[uuid.UUID, list[TestSuiteResultRow]] = defaultdict(
        list
    )
    current_run_ids_by_repo: dict[uuid.UUID, set[str]] = defaultdict(set)
    for row in suite_results:
        suite_time = row.get("started_at") or row.get("finished_at")
        if suite_time is None:
            continue
        suite_time_utc = to_utc(suite_time)
        if not (start <= suite_time_utc < end):
            continue
        repo_id = row["repo_id"]
        current_suites_by_repo[repo_id].append(row)
        current_run_ids_by_repo[repo_id].add(str(row["run_id"]))

    current_cases_by_repo: dict[uuid.UUID, list[TestCaseResultRow]] = defaultdict(list)
    historical_failed_names_by_repo: dict[uuid.UUID, set[str]] = defaultdict(set)
    for row in case_results:
        repo_id = row["repo_id"]
        run_id = str(row["run_id"])
        normalized_status = _normalize_test_status(row.get("status"))
        if run_id in current_run_ids_by_repo.get(repo_id, set()):
            current_cases_by_repo[repo_id].append(row)
        elif normalized_status == "failed":
            historical_failed_names_by_repo[repo_id].add(
                str(row.get("case_name") or "")
            )

    records: list[TestMetricsDailyRecord] = []
    repo_ids = sorted(
        set(current_suites_by_repo.keys()) | set(current_cases_by_repo.keys()), key=str
    )
    for repo_id in repo_ids:
        repo_suites = current_suites_by_repo.get(repo_id, [])
        repo_cases = current_cases_by_repo.get(repo_id, [])
        if not repo_suites and not repo_cases:
            continue

        total_cases = sum(int(row.get("total_count") or 0) for row in repo_suites)
        passed_count = sum(int(row.get("passed_count") or 0) for row in repo_suites)
        failed_count = sum(
            int(row.get("failed_count") or 0) + int(row.get("error_count") or 0)
            for row in repo_suites
        )
        skipped_count = sum(int(row.get("skipped_count") or 0) for row in repo_suites)
        quarantined_count = sum(
            int(row.get("quarantined_count") or 0) for row in repo_suites
        )
        suite_durations = [
            float(duration)
            for duration in (row.get("duration_seconds") for row in repo_suites)
            if duration is not None and duration >= 0
        ]

        case_statuses: dict[str, set[str]] = defaultdict(set)
        retry_attempts_by_case: dict[str, set[int]] = defaultdict(set)
        current_failed_names: set[str] = set()
        for row in repo_cases:
            case_name = str(row.get("case_name") or "")
            if not case_name:
                continue
            normalized_status = _normalize_test_status(row.get("status"))
            case_statuses[case_name].add(normalized_status)
            retry_attempts_by_case[case_name].add(int(row.get("retry_attempt") or 0))
            if normalized_status == "failed":
                current_failed_names.add(case_name)

        distinct_cases = len(case_statuses)
        flake_cases = sum(
            1
            for statuses in case_statuses.values()
            if "passed" in statuses and "failed" in statuses
        )
        retry_dependent_cases = sum(
            1
            for case_name, statuses in case_statuses.items()
            if "passed" in statuses
            and any(
                attempt > 0 for attempt in retry_attempts_by_case.get(case_name, set())
            )
        )
        recurrent_failures = len(
            current_failed_names & historical_failed_names_by_repo.get(repo_id, set())
        )

        first_suite = repo_suites[0] if repo_suites else None
        records.append(
            TestMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                total_cases=total_cases,
                passed_count=passed_count,
                failed_count=failed_count,
                skipped_count=skipped_count,
                quarantined_count=quarantined_count,
                pass_rate=(passed_count / total_cases) if total_cases else 0.0,
                failure_rate=(failed_count / total_cases) if total_cases else 0.0,
                flake_rate=(flake_cases / distinct_cases) if distinct_cases else 0.0,
                retry_dependency_rate=(retry_dependent_cases / distinct_cases)
                if distinct_cases
                else 0.0,
                total_suites=len(repo_suites),
                suite_duration_p50_seconds=float(_median(suite_durations))
                if suite_durations
                else None,
                suite_duration_p95_seconds=float(_percentile(suite_durations, 95.0))
                if suite_durations
                else None,
                failure_recurrence_score=(
                    recurrent_failures / len(current_failed_names)
                )
                if current_failed_names
                else 0.0,
                computed_at=computed_at_utc,
                team_id=first_suite.get("team_id") if first_suite else None,
                service_id=first_suite.get("service_id") if first_suite else None,
                org_id=str(first_suite.get("org_id", "") if first_suite else ""),
            )
        )

    return records


def compute_coverage_metrics_daily(
    *,
    day: date,
    snapshots: Sequence[CoverageSnapshotRow],
    prior_snapshots: Sequence[CoverageSnapshotRow] | None,
    computed_at: datetime,
) -> list[CoverageMetricsDailyRecord]:
    computed_at_utc = to_utc(computed_at)

    latest_current_by_repo: dict[uuid.UUID, CoverageSnapshotRow] = {}
    for snapshot in snapshots:
        repo_id = snapshot["repo_id"]
        existing = latest_current_by_repo.get(repo_id)
        if existing is None or _latest_snapshot_key(snapshot) > _latest_snapshot_key(
            existing
        ):
            latest_current_by_repo[repo_id] = snapshot

    latest_prior_by_repo: dict[uuid.UUID, CoverageSnapshotRow] = {}
    for snapshot in prior_snapshots or []:
        repo_id = snapshot["repo_id"]
        existing = latest_prior_by_repo.get(repo_id)
        if existing is None or _latest_snapshot_key(snapshot) > _latest_snapshot_key(
            existing
        ):
            latest_prior_by_repo[repo_id] = snapshot

    records: list[CoverageMetricsDailyRecord] = []
    for repo_id, snapshot in sorted(
        latest_current_by_repo.items(), key=lambda item: str(item[0])
    ):
        prior_snapshot = latest_prior_by_repo.get(repo_id)
        current_line_coverage = snapshot.get("line_coverage_pct")
        current_branch_coverage = snapshot.get("branch_coverage_pct")
        current_lines_total = snapshot.get("lines_total")
        current_lines_covered = snapshot.get("lines_covered")
        prior_line_coverage = (
            prior_snapshot.get("line_coverage_pct")
            if prior_snapshot is not None
            else None
        )
        coverage_delta_pct = None
        if current_line_coverage is not None and prior_line_coverage is not None:
            coverage_delta_pct = float(current_line_coverage - prior_line_coverage)

        records.append(
            CoverageMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                line_coverage_pct=float(current_line_coverage)
                if current_line_coverage is not None
                else None,
                branch_coverage_pct=float(current_branch_coverage)
                if current_branch_coverage is not None
                else None,
                lines_total=int(current_lines_total)
                if current_lines_total is not None
                else None,
                lines_covered=int(current_lines_covered)
                if current_lines_covered is not None
                else None,
                coverage_delta_pct=coverage_delta_pct,
                uncovered_files_count=0,
                coverage_regression_count=0,
                computed_at=computed_at_utc,
                team_id=snapshot.get("team_id"),
                service_id=snapshot.get("service_id"),
                org_id=str(snapshot.get("org_id", "")),
            )
        )

    return records
