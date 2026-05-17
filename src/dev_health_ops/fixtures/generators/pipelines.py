"""CI / pipeline / test execution / coverage / deployment / DORA fixture generators."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.models.git import CiPipelineRun, Deployment


class PipelinesGeneratorMixin(BaseGeneratorMixin):
    """Generates CI pipeline runs, job runs, test executions, coverage, deployments, and DORA records."""

    def generate_ci_pipeline_runs(
        self, days: int = 30, runs_per_day: int = 3
    ) -> list[CiPipelineRun]:
        runs = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        run_index = 0
        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(1, max(1, runs_per_day * 2))
            for _ in range(daily_count):
                queued_at = current_date + timedelta(minutes=random.randint(0, 60 * 12))
                started_at = queued_at + timedelta(minutes=random.randint(1, 30))
                duration_minutes = random.randint(5, 60)
                finished_at = started_at + timedelta(minutes=duration_minutes)
                status = random.choices(
                    ["success", "failed", "canceled"], weights=[0.7, 0.2, 0.1], k=1
                )[0]

                run_index += 1
                runs.append(
                    CiPipelineRun(
                        repo_id=self.repo_id,
                        run_id=f"synth-run-{run_index}",
                        status=status,
                        queued_at=queued_at,
                        started_at=started_at,
                        finished_at=finished_at,
                    )
                )
            current_date += timedelta(days=1)
        return runs

    def generate_ci_job_runs(
        self, pipeline_runs: list[CiPipelineRun], *, org_id: str = ""
    ) -> list[dict[str, Any]]:
        """Generate CI job runs for each pipeline run.

        Returns dicts matching JobRunRow schema from testops_schemas.
        Each pipeline run gets 2-5 jobs (build, test, lint, deploy, integration-test).
        Status distribution: 75% success, 15% failed, 10% skipped
        (failed pipelines have higher job failure rate).
        """
        job_names = ["build", "test", "lint", "deploy", "integration-test"]
        duration_ranges: dict[str, tuple[int, int]] = {
            "build": (120, 600),
            "test": (180, 900),
            "lint": (60, 180),
            "deploy": (120, 1200),
            "integration-test": (300, 1200),
        }
        job_runs: list[dict[str, Any]] = []
        service_id = self._get_service_id()

        for pipeline in pipeline_runs:
            num_jobs = random.randint(2, 5)
            selected_jobs = random.sample(job_names, k=min(num_jobs, len(job_names)))
            pipeline_failed = getattr(pipeline, "status", None) == "failed"

            for job_idx, job_name in enumerate(selected_jobs):
                queue_offset_seconds = random.randint(10, 300)
                pipeline_queued = getattr(pipeline, "queued_at", None)
                pipeline_started = getattr(pipeline, "started_at", None)
                if pipeline_queued is None:
                    pipeline_queued = pipeline_started
                if pipeline_queued is None:
                    continue

                job_queued_at = pipeline_queued + timedelta(
                    seconds=queue_offset_seconds + job_idx * 60
                )
                job_started_at = job_queued_at + timedelta(
                    seconds=random.randint(10, 300)
                )

                dur_min, dur_max = duration_ranges.get(job_name, (60, 600))
                duration_seconds = random.randint(dur_min, dur_max)
                job_finished_at = job_started_at + timedelta(seconds=duration_seconds)

                if pipeline_failed:
                    status = random.choices(
                        ["success", "failed", "skipped"],
                        weights=[0.4, 0.45, 0.15],
                        k=1,
                    )[0]
                else:
                    status = random.choices(
                        ["success", "failed", "skipped"],
                        weights=[0.75, 0.15, 0.10],
                        k=1,
                    )[0]

                job_id = f"{pipeline.run_id}-job-{job_idx}"
                team_id = self._pick_assigned_team_id(job_id)

                job_runs.append(
                    {
                        "repo_id": pipeline.repo_id,
                        "run_id": pipeline.run_id,
                        "job_id": job_id,
                        "job_name": job_name,
                        "stage": None,
                        "status": status,
                        "started_at": job_started_at,
                        "finished_at": job_finished_at,
                        "duration_seconds": float(duration_seconds),
                        "runner_type": random.choice(
                            ["hosted", "hosted", "hosted", "self-hosted"]
                        ),
                        "retry_attempt": 0,
                        "team_id": team_id,
                        "service_id": service_id,
                        "org_id": org_id,
                    }
                )

        return job_runs

    def generate_test_executions(
        self,
        job_runs: list[dict[str, Any]],
        days: int = 30,
        *,
        org_id: str = "",
    ) -> dict[str, list[dict[str, Any]]]:
        """Generate test suite and case results for test/integration-test jobs.

        Returns dict with 'suite_results' (TestSuiteResultRow dicts) and
        'case_results' (TestCaseResultRow dicts).
        """
        suite_results: list[dict[str, Any]] = []
        case_results: list[dict[str, Any]] = []
        service_id = self._get_service_id()

        flaky_test_names = [
            "test_api_timeout",
            "test_race_condition_handler",
            "test_concurrent_db_writes",
            "test_websocket_reconnect",
            "test_cache_invalidation",
            "test_async_event_ordering",
            "test_retry_backoff_timing",
            "test_session_expiry_edge",
        ]
        persistent_failures = [
            "test_legacy_auth_compat",
            "test_timezone_edge_case",
            "test_unicode_normalization",
            "test_migration_rollback_safety",
        ]
        frameworks = {
            "test": ["pytest", "jest", "junit", "go test"],
            "integration-test": ["playwright", "cypress", "selenium"],
        }

        test_name_pools = {
            "test": [
                "test_user_creation",
                "test_login_flow",
                "test_data_validation",
                "test_error_handling",
                "test_pagination",
                "test_search_query",
                "test_permission_check",
                "test_rate_limiter",
                "test_input_sanitization",
                "test_config_loading",
                "test_db_connection",
                "test_cache_hit",
                "test_serialization",
                "test_middleware_chain",
                "test_health_endpoint",
            ],
            "integration-test": [
                "test_end_to_end_signup",
                "test_payment_flow",
                "test_webhook_delivery",
                "test_third_party_sync",
                "test_bulk_import",
                "test_report_generation",
                "test_notification_pipeline",
                "test_data_export",
            ],
        }

        for job in job_runs:
            job_name = job.get("job_name", "")
            if job_name not in ("test", "integration-test"):
                continue

            repo_id = job["repo_id"]
            run_id = job["run_id"]
            job_id = job["job_id"]
            team_id = job.get("team_id") or self._pick_assigned_team_id(
                f"{run_id}:{job_id}"
            )
            suite_service_id = job.get("service_id") or service_id

            total_tests = random.randint(50, 500)

            is_bad_run = random.random() < 0.15
            if is_bad_run:
                pass_rate = random.uniform(0.10, 0.60)
            else:
                pass_rate = random.uniform(0.85, 0.98)

            passed = int(total_tests * pass_rate)
            flake_rate = random.uniform(0.02, 0.15)
            flaky_count = max(0, int(total_tests * flake_rate))
            skipped = random.randint(0, max(1, total_tests // 20))
            error_count = max(0, int(total_tests * random.uniform(0.02, 0.05)))
            quarantined_count = max(0, int(total_tests * random.uniform(0.01, 0.03)))
            failed = total_tests - passed - skipped - error_count
            failed = max(failed, len(persistent_failures))
            if failed < 0:
                overflow = -failed
                failed = 0
                passed = max(0, passed - overflow)
            if passed + skipped + failed + error_count > total_tests:
                passed = max(0, total_tests - skipped - failed - error_count)

            suite_duration = random.uniform(30.0, 600.0)

            suite_name = f"{job_name}_suite_{job_id}"
            suite_id = f"suite-{run_id}-{job_id}"

            job_started = job.get("started_at")
            job_finished = job.get("finished_at")

            suite_results.append(
                {
                    "repo_id": repo_id,
                    "run_id": run_id,
                    "suite_id": suite_id,
                    "suite_name": suite_name,
                    "framework": random.choice(frameworks[job_name]),
                    "environment": "linux-x64",
                    "total_count": total_tests,
                    "passed_count": passed,
                    "failed_count": failed,
                    "skipped_count": skipped,
                    "error_count": error_count,
                    "quarantined_count": quarantined_count,
                    "retried_count": flaky_count,
                    "duration_seconds": suite_duration,
                    "started_at": job_started,
                    "finished_at": job_finished,
                    "team_id": team_id,
                    "service_id": suite_service_id,
                    "org_id": org_id,
                }
            )

            name_pool = test_name_pools.get(job_name, test_name_pools["test"])
            all_names = (
                list(name_pool) + list(flaky_test_names) + list(persistent_failures)
            )

            case_names: list[str] = []
            for i in range(total_tests):
                base = all_names[i % len(all_names)]
                suffix = f"_{i // len(all_names)}" if i >= len(all_names) else ""
                case_names.append(f"{base}{suffix}")

            flaky_indices = set(
                random.sample(range(total_tests), k=min(flaky_count, total_tests))
            )

            passed_so_far = 0
            failed_so_far = 0
            skipped_so_far = 0
            quarantined_indices = set(
                random.sample(range(total_tests), k=min(quarantined_count, total_tests))
            )

            for case_idx, case_name in enumerate(case_names):
                if case_name in persistent_failures:
                    case_status = "failed"
                    retry_attempt = 0
                    failed_so_far += 1
                elif case_idx in flaky_indices:
                    case_status = "passed"
                    retry_attempt = 1
                    passed_so_far += 1
                elif skipped_so_far < skipped and random.random() < 0.3:
                    case_status = "skipped"
                    retry_attempt = 0
                    skipped_so_far += 1
                elif failed_so_far < failed and random.random() < (
                    failed / max(1, total_tests - case_idx)
                ):
                    case_status = "failed"
                    retry_attempt = 0
                    failed_so_far += 1
                else:
                    case_status = "passed"
                    retry_attempt = 0
                    passed_so_far += 1

                case_duration = random.uniform(
                    0.01, suite_duration / max(1, total_tests) * 3
                )

                case_id = f"case-{suite_id}-{case_idx}"
                failure_message = None
                failure_type = None
                if case_status == "failed":
                    failure_type = random.choice(
                        ["assertion", "timeout", "error", "infrastructure"]
                    )
                    failure_message = f"Expected condition not met in {case_name}"
                is_quarantined = case_idx in quarantined_indices

                case_results.append(
                    {
                        "repo_id": repo_id,
                        "run_id": run_id,
                        "suite_id": suite_id,
                        "case_id": case_id,
                        "case_name": case_name,
                        "class_name": suite_name,
                        "status": case_status,
                        "duration_seconds": case_duration,
                        "retry_attempt": retry_attempt,
                        "failure_message": failure_message,
                        "failure_type": failure_type,
                        "stack_trace": None,
                        "is_quarantined": is_quarantined,
                        "team_id": team_id,
                        "service_id": suite_service_id,
                        "org_id": org_id,
                    }
                )

        return {"suite_results": suite_results, "case_results": case_results}

    def generate_coverage_snapshots(
        self,
        pipeline_runs: list[CiPipelineRun],
        days: int = 30,
        *,
        org_id: str = "",
    ) -> list[dict[str, Any]]:
        """Generate daily coverage snapshots tied to pipeline runs.

        Returns dicts matching CoverageSnapshotRow schema.
        Uses random walk with mean reversion for realistic drift.
        One snapshot per day per repo (picks a pipeline run from that day).
        """
        if not pipeline_runs:
            return []

        snapshots: list[dict[str, Any]] = []
        service_id = self._get_service_id()

        runs_by_day: dict[date, list[CiPipelineRun]] = {}
        for run in pipeline_runs:
            started = getattr(run, "started_at", None)
            if started is None:
                continue
            day = started.date()
            runs_by_day.setdefault(day, []).append(run)

        line_coverage = random.uniform(70.0, 90.0)
        branch_coverage = line_coverage - random.uniform(5.0, 15.0)
        lines_total = random.randint(8000, 50000)

        line_target = line_coverage
        branch_target = branch_coverage

        sorted_days = sorted(runs_by_day.keys())

        for day in sorted_days:
            day_runs = runs_by_day[day]
            chosen_run = random.choice(day_runs)

            line_delta = random.gauss(0, 0.5)
            line_delta += (line_target - line_coverage) * 0.1
            line_coverage = max(40.0, min(99.0, line_coverage + line_delta))

            branch_delta = random.gauss(0, 0.4)
            branch_delta += (branch_target - branch_coverage) * 0.1
            branch_coverage = max(30.0, min(95.0, branch_coverage + branch_delta))

            branch_coverage = min(branch_coverage, line_coverage - 2.0)

            lines_covered = int(lines_total * line_coverage / 100.0)
            branches_total = int(lines_total * 0.3)
            branches_covered = int(branches_total * branch_coverage / 100.0)

            snapshot_id = f"cov-{chosen_run.run_id}-{day.isoformat()}"
            team_id = self._pick_assigned_team_id(snapshot_id)

            snapshots.append(
                {
                    "repo_id": self.repo_id,
                    "run_id": chosen_run.run_id,
                    "snapshot_id": snapshot_id,
                    "report_format": "lcov",
                    "lines_total": lines_total,
                    "lines_covered": lines_covered,
                    "line_coverage_pct": round(line_coverage, 2),
                    "branches_total": branches_total,
                    "branches_covered": branches_covered,
                    "branch_coverage_pct": round(branch_coverage, 2),
                    "functions_total": None,
                    "functions_covered": None,
                    "commit_hash": None,
                    "branch": "main",
                    "pr_number": None,
                    "team_id": team_id,
                    "service_id": service_id,
                    "org_id": org_id,
                }
            )

        return snapshots

    def generate_deployments(
        self,
        days: int = 30,
        deployments_per_day: int = 2,
        pr_numbers: list[int] | None = None,
        release_refs: list[str] | None = None,
    ) -> list[Deployment]:
        deployments = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        if not release_refs:
            release_refs = self._default_release_refs(days)

        deploy_index = 0
        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(0, max(1, deployments_per_day * 2))
            for _ in range(daily_count):
                started_at = current_date + timedelta(
                    minutes=random.randint(0, 60 * 20)
                )
                duration_minutes = random.randint(5, 90)
                finished_at = started_at + timedelta(minutes=duration_minutes)
                deployed_at = finished_at + timedelta(minutes=random.randint(0, 15))
                status = random.choices(["success", "failed"], weights=[0.8, 0.2], k=1)[
                    0
                ]
                environment = random.choice(["production", "staging"])
                merged_at = started_at - timedelta(hours=random.randint(1, 72))
                pr_number = None
                if pr_numbers:
                    pr_number = random.choice(pr_numbers)

                release_ref = random.choice(release_refs)

                deploy_index += 1
                deployments.append(
                    Deployment(
                        repo_id=self.repo_id,
                        deployment_id=f"synth-deploy-{deploy_index}",
                        status=status,
                        environment=environment,
                        started_at=started_at,
                        finished_at=finished_at,
                        deployed_at=deployed_at,
                        merged_at=merged_at,
                        pull_request_number=pr_number,
                        release_ref=release_ref,
                        release_ref_confidence=1.0,
                    )
                )
            current_date += timedelta(days=1)
        return deployments

    def generate_dora_metrics(self, days: int = 30) -> list[Any]:
        """Generate synthetic DORA metrics records."""
        from dev_health_ops.metrics.schemas import DORAMetricsRecord

        records = []
        end_date = datetime.now(timezone.utc).date()
        computed_at = datetime.now(timezone.utc)

        metric_names = [
            "deployment_frequency",
            "lead_time_for_changes",
            "change_failure_rate",
            "time_to_restore_service",
        ]

        for i in range(days):
            day = end_date - timedelta(days=i)
            for metric_name in metric_names:
                if metric_name == "deployment_frequency":
                    value = random.uniform(0.5, 3.0)  # deploys per day
                elif metric_name == "lead_time_for_changes":
                    value = random.uniform(2.0, 72.0)  # hours
                elif metric_name == "change_failure_rate":
                    value = random.uniform(0.05, 0.25)  # ratio
                else:  # time_to_restore_service
                    value = random.uniform(0.5, 8.0)  # hours

                records.append(
                    DORAMetricsRecord(
                        repo_id=self.repo_id,
                        day=day,
                        metric_name=metric_name,
                        value=value,
                        computed_at=computed_at,
                    )
                )
        return records
