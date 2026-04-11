import hashlib
import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dev_health_ops.metrics.schemas import (
    FileMetricsRecord,
    RepoMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemUserMetricsDailyRecord,
)
from dev_health_ops.models.git import (
    CiPipelineRun,
    Deployment,
    GitCommit,
    GitCommitStat,
    GitFile,
    GitPullRequest,
    GitPullRequestReview,
    Incident,
    Repo,
)
from dev_health_ops.models.teams import Team
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
    WorkItemType,
    Worklog,
)
from dev_health_ops.providers.teams import normalize_team_id, normalize_team_name


class SyntheticDataGenerator:
    def __init__(
        self,
        repo_name: str = "acme/demo-app",
        repo_id: uuid.UUID | None = None,
        provider: str = "synthetic",
        seed: int | None = None,
        assigned_teams: list[Team] | None = None,
    ):
        self.repo_name = repo_name
        self.assigned_teams = assigned_teams
        if repo_id:
            self.repo_id = repo_id
        else:
            # Deterministic UUID based on repo name
            namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
            self.repo_id = uuid.uuid5(namespace, repo_name)
        self.provider = provider
        seed_value = int(seed) if seed is not None else int(self.repo_id.int % (2**32))
        random.seed(seed_value)
        self.authors = [
            ("Alice Smith", "alice@example.com"),
            ("Bob Jones", "bob@example.com"),
            ("Charlie Brown", "charlie@example.com"),
            ("David White", "david@example.com"),
            ("Eve Black", "eve@example.com"),
            ("Frank Green", "frank@example.com"),
            ("Grace Hall", "grace@example.com"),
            ("Heidi Blue", "heidi@example.com"),
            ("Ivan Red", "ivan@example.com"),
            ("Judy Orange", "judy@example.com"),
            ("Kevin Purple", "kevin@example.com"),
            ("Liam Cyan", "liam@example.com"),
            ("Mia Magenta", "mia@example.com"),
            ("Noah Yellow", "noah@example.com"),
            ("Olivia Gray", "olivia@example.com"),
            ("Pat Lime", "pat@example.com"),
        ]
        # Randomize authors order to vary team composition
        random.shuffle(self.authors)
        self.unassigned_authors = [
            ("Unaffiliated One", "unassigned1@example.com"),
            ("Unaffiliated Two", "unassigned2@example.com"),
            ("Unaffiliated Three", "unassigned3@example.com"),
        ]
        self.repo_authors = self._resolve_repo_authors()
        self.files = [
            "src/main.py",
            "src/utils.py",
            "src/models.py",
            "src/api/routes.py",
            "src/api/auth.py",
            "src/api/dependencies.py",
            "src/api/health.py",
            "src/api/errors.py",
            "src/services/user_service.py",
            "src/services/metrics_service.py",
            "src/services/review_service.py",
            "src/db/session.py",
            "src/db/models/user.py",
            "src/db/models/repo.py",
            "src/db/models/work_item.py",
            "src/workflows/ingest.py",
            "src/workflows/compute.py",
            "src/workflows/publish.py",
            "src/utils/time.py",
            "src/utils/metrics.py",
            "src/utils/strings.py",
            "src/config/settings.py",
            "src/config/logging.py",
            "src/clients/github.py",
            "src/clients/gitlab.py",
            "src/clients/jira.py",
            "tests/test_main.py",
            "tests/test_api_routes.py",
            "tests/test_metrics_daily.py",
            "tests/test_hotspots.py",
            "tests/test_blame_loader.py",
            "README.md",
            "README_CONTRIBUTING.md",
            "docs/architecture.md",
            "docs/metrics.md",
            "docs/workflows.md",
            "docs/usage.md",
            "docker-compose.yml",
            "Dockerfile",
            ".github/workflows/ci.yml",
            ".github/workflows/release.yml",
        ]

    def _resolve_repo_authors(self) -> list[tuple[str, str]]:
        if self.assigned_teams is None:
            return list(self.authors)
        if self.assigned_teams:
            member_identities = {
                str(member).strip().lower()
                for team in self.assigned_teams
                for member in (team.members or [])
            }
            filtered = [
                (name, email)
                for name, email in self.authors
                if str(email).strip().lower() in member_identities
                or str(name).strip().lower() in member_identities
            ]
            if filtered:
                return filtered
            return list(self.authors)
        return list(self.unassigned_authors)

    def get_team_assignment(self, count: int = 2) -> dict[str, Any]:
        """
        Returns a consistent assignment of authors to teams.
        Output includes 'teams' (List[Team]) and 'member_map' (email -> (id, name)).
        """
        teams = []
        member_map = {}

        # Ensure at least 1 author per team if possible, loop if more teams than authors
        # For simplicity, just chunk authors.
        chunk_size = max(1, len(self.authors) // count)

        for i in range(count):
            start = i * chunk_size
            # Last team gets the rest
            end = (i + 1) * chunk_size if i < count - 1 else len(self.authors)
            team_members = self.authors[start:end]

            # Stable IDs
            if count == 2:
                team_id = "alpha" if i == 0 else "beta"
                team_name = "Alpha Team" if i == 0 else "Beta Team"
            else:
                team_id = f"team-{chr(97 + i)}"
                team_name = f"Team {chr(65 + i)}"

            member_emails = [email for _, email in team_members]

            teams.append(
                Team(
                    id=team_id,
                    name=team_name,
                    description=f"Synthetic {team_name}",
                    members=member_emails,
                )
            )

            for name, email in team_members:
                member_map[str(email).strip().lower()] = (team_id, team_name)
                member_map[str(name).strip().lower()] = (team_id, team_name)

        return {"teams": teams, "member_map": member_map}

    def generate_teams(self, count: int = 2) -> list[Team]:
        """
        Generate synthetic teams with members distributed among them.
        """
        return self.get_team_assignment(count)["teams"]

    def generate_repo(self) -> Repo:
        return Repo(
            id=self.repo_id,
            repo=self.repo_name,
            ref="main",
            provider="synthetic",
            settings={
                "source": "synthetic",
                "repo_id": str(self.repo_id),
            },
            tags=["demo", "synthetic"],
        )

    def generate_commits(
        self, days: int = 30, commits_per_day: int = 5
    ) -> list[GitCommit]:
        commits = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(1, commits_per_day * 2)
            for _ in range(daily_count):
                author_name, author_email = random.choice(self.repo_authors)
                commit_time = current_date + timedelta(seconds=random.randint(0, 86400))
                if commit_time > end_date:
                    continue

                commit_hash = f"{random.getrandbits(128):032x}"
                base_messages = [
                    "fix typo",
                    "add feature",
                    "update docs",
                    "refactor code",
                    "optimize performance",
                    "fix security vulnerability",
                    "bump dependencies",
                    "revert change",
                    "add tests",
                    "improve logging",
                ]
                message = f"Synthetic commit: {random.choice(base_messages)}"
                if random.random() < 0.4:
                    project_key = self.repo_name.split("/")[-1].upper()[:3]
                    issue_num = random.randint(1, 200)
                    prefix = random.choice(["", "Fixes ", "Closes ", "Refs "])
                    message = f"{prefix}{project_key}-{issue_num}: {message}"
                commits.append(
                    GitCommit(
                        repo_id=self.repo_id,
                        hash=commit_hash,
                        message=message,
                        author_name=author_name,
                        author_email=author_email,
                        author_when=commit_time,
                        committer_name=author_name,
                        committer_email=author_email,
                        committer_when=commit_time,
                        parents=1,
                    )
                )
            current_date += timedelta(days=1)

        return commits

    def generate_commit_stats(self, commits: list[GitCommit]) -> list[GitCommitStat]:
        stats = []
        for commit in commits:
            # Each commit touches 1-5 files
            files_to_touch = random.sample(
                self.files, random.randint(1, min(5, len(self.files)))
            )
            for file_path in files_to_touch:
                # 80% small changes, 15% medium, 5% large
                change_type = random.random()
                if change_type < 0.8:
                    additions = random.randint(1, 50)
                elif change_type < 0.95:
                    additions = random.randint(50, 200)
                else:
                    additions = random.randint(200, 1000)

                deletions = random.randint(0, additions)
                stats.append(
                    GitCommitStat(
                        repo_id=self.repo_id,
                        commit_hash=commit.hash,
                        file_path=file_path,
                        additions=additions,
                        deletions=deletions,
                    )
                )
        return stats

    def generate_prs(
        self,
        count: int = 20,
        issue_numbers: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        prs = []
        end_date = datetime.now(timezone.utc)
        issue_numbers = issue_numbers or []
        pr_keywords = [
            "feature",
            "refactor",
            "incident",
            "bug",
            "test",
            "deploy",
            "rollback",
            "cleanup",
            "hotfix",
        ]
        pr_titles = [
            "Implement User Auth",
            "Fix NPE in Service",
            "Refactor DB Layer",
            "Update API Docs",
            "Add Integration Tests",
            "Bump version",
            "Optimize Startup",
            "Remove Legacy Code",
            "Feature X",
            "Fix Bug Y",
            "Cleanup Z",
        ]

        for i in range(1, count + 1):
            author_name, author_email = random.choice(self.repo_authors)
            # PRs created over the last 60 days
            created_at = end_date - timedelta(
                days=random.randint(0, 60), hours=random.randint(0, 23)
            )
            issue_ref = None
            if issue_numbers and random.random() > 0.3:
                issue_ref = random.choice(issue_numbers)

            # Simulated lifecycle
            state = random.choice(["merged", "merged", "merged", "open", "closed"])
            merged_at = None
            closed_at = None

            first_review_at = None
            first_comment_at = None
            reviews_count = 0
            comments_count = random.randint(0, 10)

            if comments_count > 0:
                first_comment_at = created_at + timedelta(
                    minutes=random.randint(5, 120)
                )

            # Review stats
            has_review = random.random() > 0.2
            if has_review:
                first_review_at = created_at + timedelta(hours=random.randint(1, 48))
                reviews_count = random.randint(1, 5)

            if state == "merged":
                merged_at = created_at + timedelta(days=random.randint(1, 7))
                closed_at = merged_at
            elif state == "closed":
                closed_at = created_at + timedelta(days=random.randint(1, 14))

            summary = random.choice(pr_titles)
            keywords = random.sample(pr_keywords, k=2)
            title = f"Synthetic PR #{i}: {summary}"
            if issue_ref is not None:
                title = f"{title} (Fixes #{issue_ref})"
            body = (
                f"{summary}.\n\n"
                f"This change includes {keywords[0]} updates and {keywords[1]} coverage.\n"
            )
            if issue_ref is not None:
                body += f"\nFixes #{issue_ref}\n"

            prs.append(
                {
                    "pr": GitPullRequest(
                        repo_id=self.repo_id,
                        number=i,
                        title=title,
                        body=body,
                        state=state,
                        author_name=author_name,
                        author_email=author_email,
                        created_at=created_at,
                        merged_at=merged_at,
                        closed_at=closed_at,
                        head_branch=f"feature/{i}",
                        base_branch="main",
                        additions=random.randint(10, 500),
                        deletions=random.randint(5, 200),
                        changed_files=random.randint(1, 10),
                        first_review_at=first_review_at,
                        first_comment_at=first_comment_at,
                        reviews_count=reviews_count,
                        comments_count=comments_count,
                        changes_requested_count=random.randint(0, 2),
                    ),
                    "reviews": self._generate_pr_reviews(
                        i, first_review_at, reviews_count
                    )
                    if first_review_at
                    else [],
                }
            )
        return prs

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
        self, pipeline_runs: list[CiPipelineRun]
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
                        "runner_type": "hosted",
                        "retry_attempt": 0,
                        "org_id": "",
                    }
                )

        return job_runs

    def generate_test_executions(
        self,
        job_runs: list[dict[str, Any]],
        days: int = 30,
    ) -> dict[str, list[dict[str, Any]]]:
        """Generate test suite and case results for test/integration-test jobs.

        Returns dict with 'suite_results' (TestSuiteResultRow dicts) and
        'case_results' (TestCaseResultRow dicts).
        """
        suite_results: list[dict[str, Any]] = []
        case_results: list[dict[str, Any]] = []

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

            total_tests = random.randint(50, 500)

            is_bad_run = random.random() < 0.08
            if is_bad_run:
                pass_rate = random.uniform(0.65, 0.75)
            else:
                pass_rate = random.uniform(0.85, 0.98)

            passed = int(total_tests * pass_rate)
            flake_rate = random.uniform(0.02, 0.08)
            flaky_count = max(0, int(total_tests * flake_rate))
            skipped = random.randint(0, max(1, total_tests // 20))
            failed = total_tests - passed - skipped
            if failed < 0:
                failed = 0
                passed = total_tests - skipped

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
                    "framework": "pytest" if job_name == "test" else "playwright",
                    "environment": "linux-x64",
                    "total_count": total_tests,
                    "passed_count": passed,
                    "failed_count": failed,
                    "skipped_count": skipped,
                    "error_count": 0,
                    "quarantined_count": 0,
                    "retried_count": flaky_count,
                    "duration_seconds": suite_duration,
                    "started_at": job_started,
                    "finished_at": job_finished,
                    "team_id": None,
                    "service_id": None,
                    "org_id": "",
                }
            )

            name_pool = test_name_pools.get(job_name, test_name_pools["test"])
            all_names = list(name_pool) + list(flaky_test_names)

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

            for case_idx, case_name in enumerate(case_names):
                if case_idx in flaky_indices:
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
                        "is_quarantined": False,
                        "org_id": "",
                    }
                )

        return {"suite_results": suite_results, "case_results": case_results}

    def generate_coverage_snapshots(
        self,
        pipeline_runs: list[CiPipelineRun],
        days: int = 30,
    ) -> list[dict[str, Any]]:
        """Generate daily coverage snapshots tied to pipeline runs.

        Returns dicts matching CoverageSnapshotRow schema.
        Uses random walk with mean reversion for realistic drift.
        One snapshot per day per repo (picks a pipeline run from that day).
        """
        if not pipeline_runs:
            return []

        snapshots: list[dict[str, Any]] = []

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
                    "team_id": None,
                    "service_id": None,
                    "org_id": "",
                }
            )

        return snapshots

    def generate_deployments(
        self,
        days: int = 30,
        deployments_per_day: int = 2,
        pr_numbers: list[int] | None = None,
    ) -> list[Deployment]:
        deployments = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

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
                    )
                )
            current_date += timedelta(days=1)
        return deployments

    def generate_incidents(
        self, days: int = 30, incidents_per_day: int = 1
    ) -> list[Incident]:
        incidents = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        incident_index = 0
        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(0, max(1, incidents_per_day * 2))
            for _ in range(daily_count):
                started_at = current_date + timedelta(
                    minutes=random.randint(0, 60 * 20)
                )
                resolved_at = started_at + timedelta(hours=random.randint(1, 12))
                status = random.choices(["resolved", "open"], weights=[0.8, 0.2], k=1)[
                    0
                ]
                if status == "open":
                    resolved_at = None

                incident_index += 1
                incidents.append(
                    Incident(
                        repo_id=self.repo_id,
                        incident_id=f"synth-incident-{incident_index}",
                        status=status,
                        started_at=started_at,
                        resolved_at=resolved_at,
                    )
                )
            current_date += timedelta(days=1)
        return incidents

    def _generate_pr_reviews(
        self, pr_number: int, first_review_at: datetime, count: int
    ) -> list[GitPullRequestReview]:
        reviews = []
        for i in range(count):
            reviewer_name, reviewer_email = random.choice(self.repo_authors)
            review_time = first_review_at + timedelta(hours=random.randint(0, 24) * i)
            state = (
                "APPROVED"
                if i == count - 1
                else random.choice(["COMMENTED", "CHANGES_REQUESTED", "APPROVED"])
            )
            reviews.append(
                GitPullRequestReview(
                    repo_id=self.repo_id,
                    number=pr_number,
                    review_id=f"rev_{pr_number}_{i}",
                    reviewer=reviewer_email,
                    state=state,
                    submitted_at=review_time,
                )
            )
        return reviews

    def generate_complexity_metrics(self, days: int = 30) -> dict[str, list[Any]]:
        from dev_health_ops.metrics.schemas import (
            FileComplexitySnapshot,
            RepoComplexityDaily,
        )

        snapshots = []
        dailies = []
        end_date = datetime.now(timezone.utc)

        for i in range(days):
            day = (end_date - timedelta(days=i)).date()
            computed_at = datetime.now(timezone.utc)

            total_loc = 0
            total_cc = 0
            total_high = 0
            total_very_high = 0

            for file_path in self.files:
                # Synthetic complexity values
                loc = random.randint(50, 500)
                funcs = random.randint(5, 50)
                cc_total = random.randint(funcs, funcs * 5)
                cc_avg = cc_total / funcs

                high = 0
                very_high = 0
                if cc_avg > 10:
                    high = random.randint(1, funcs // 3)
                if cc_avg > 20:
                    very_high = random.randint(0, high // 2)

                snapshots.append(
                    FileComplexitySnapshot(
                        repo_id=self.repo_id,
                        as_of_day=day,
                        ref="HEAD",
                        file_path=file_path,
                        language="python",
                        loc=loc,
                        functions_count=funcs,
                        cyclomatic_total=cc_total,
                        cyclomatic_avg=cc_avg,
                        high_complexity_functions=high,
                        very_high_complexity_functions=very_high,
                        computed_at=computed_at,
                    )
                )

                total_loc += loc
                total_cc += cc_total
                total_high += high
                total_very_high += very_high

            cc_per_kloc = (total_cc / (total_loc / 1000.0)) if total_loc > 0 else 0.0

            dailies.append(
                RepoComplexityDaily(
                    repo_id=self.repo_id,
                    day=day,
                    loc_total=total_loc,
                    cyclomatic_total=total_cc,
                    cyclomatic_per_kloc=cc_per_kloc,
                    high_complexity_functions=total_high,
                    very_high_complexity_functions=total_very_high,
                    computed_at=computed_at,
                )
            )

        return {"snapshots": snapshots, "dailies": dailies}

    def generate_files(self) -> list[GitFile]:
        return [
            GitFile(repo_id=self.repo_id, path=f, executable=False) for f in self.files
        ]

    def _generate_synthetic_python_lines(self, file_path: str) -> list[str]:
        target_lines = random.randint(30, 140)
        safe_name = (
            file_path.replace("/", "_")
            .replace("\\", "_")
            .replace(".", "_")
            .replace("-", "_")
        )
        safe_name = "".join(
            ch if (ch.isalnum() or ch == "_") else "_" for ch in safe_name
        )
        safe_name = safe_name.strip("_") or "synthetic_module"

        lines: list[str] = [
            f'"""Synthetic fixture module: {safe_name}."""',
            "",
            "from __future__ import annotations",
            "",
            "from typing import Iterable",
            "",
        ]

        max_functions = 6
        for func_idx in range(max_functions):
            func_name = f"{safe_name}_fn_{func_idx}"
            threshold = random.randint(3, 12)
            multiplier = random.randint(2, 7)

            block = [
                f"def {func_name}(values: Iterable[int]) -> int:",
                "    total = 0",
                "    for idx, value in enumerate(values):",
                f"        if value % {threshold} == 0:",
                f"            total += value * {multiplier}",
                "        elif value % 2 == 0:",
                "            total += value",
                "        elif value < 0:",
                "            total -= value",
                "        else:",
                "            total -= value // 2",
                "        if idx % 5 == 0 and total > 0:",
                "            total //= 2",
                "    return total",
                "",
            ]

            # Ensure we never truncate mid-block (keeps generated code parseable).
            if func_idx >= 2 and (len(lines) + len(block)) > target_lines:
                break
            lines.extend(block)

        while len(lines) < target_lines:
            lines.append(f"# filler {len(lines) + 1} for {file_path}")
        return lines

    def generate_blame(
        self, commits: list[GitCommit]
    ) -> list[
        Any
    ]:  # using Any to avoid circular import issues if GitBlame isn't imported, but it is
        # We need to import GitBlame inside the method or file level
        from dev_health_ops.models.git import GitBlame

        blame_records = []
        if not commits:
            return blame_records

        for file_path in self.files:
            if file_path.endswith(".py"):
                lines = self._generate_synthetic_python_lines(file_path)
            else:
                num_lines = random.randint(10, 200)
                lines = [
                    f"Line {i} content for {file_path}" for i in range(1, num_lines + 1)
                ]

            for i, line in enumerate(lines, start=1):
                # Pick a random commit that "modified" this line
                commit = random.choice(commits)

                blame_records.append(
                    GitBlame(
                        repo_id=self.repo_id,
                        path=file_path,
                        line_no=i,
                        author_email=commit.author_email,
                        author_name=commit.author_name,
                        author_when=commit.author_when,
                        commit_hash=commit.hash,
                        line=line,
                    )
                )
        return blame_records

    def generate_work_item_metrics(
        self, days: int = 30
    ) -> list[WorkItemMetricsDailyRecord]:
        records = []
        end_date = datetime.now(timezone.utc).date()

        teams_to_use = []
        if self.assigned_teams is None:
            teams_to_use = [("alpha", "Alpha Team")]
        elif self.assigned_teams:
            teams_to_use = [(t.id, t.name) for t in self.assigned_teams]
        else:
            teams_to_use = [("unassigned", "Unassigned")]

        for i in range(days):
            day = end_date - timedelta(days=i)
            for team_id, team_name in teams_to_use:
                records.append(
                    WorkItemMetricsDailyRecord(
                        day=day,
                        provider=self.provider,
                        work_scope_id=self.repo_name,
                        team_id=team_id,
                        team_name=team_name,
                        items_started=random.randint(2, 8),
                        items_completed=random.randint(1, 6),
                        items_started_unassigned=random.randint(0, 2),
                        items_completed_unassigned=random.randint(0, 1),
                        wip_count_end_of_day=random.randint(5, 15),
                        wip_unassigned_end_of_day=random.randint(1, 3),
                        cycle_time_p50_hours=float(random.randint(24, 72)),
                        cycle_time_p90_hours=float(random.randint(72, 120)),
                        lead_time_p50_hours=float(random.randint(48, 96)),
                        lead_time_p90_hours=float(random.randint(96, 240)),
                        wip_age_p50_hours=float(random.randint(12, 48)),
                        wip_age_p90_hours=float(random.randint(48, 168)),
                        bug_completed_ratio=random.uniform(0.1, 0.4),
                        story_points_completed=float(random.randint(10, 50)),
                        # Phase 2 metrics
                        new_bugs_count=random.randint(0, 3),
                        new_items_count=random.randint(3, 10),
                        defect_intro_rate=random.uniform(0.0, 0.3),
                        wip_congestion_ratio=random.uniform(0.5, 2.0),
                        predictability_score=random.uniform(0.5, 1.0),
                        computed_at=datetime.now(timezone.utc),
                    )
                )
        return records

    def generate_work_item_cycle_times(
        self, count: int = 50
    ) -> list[WorkItemCycleTimeRecord]:
        records = []
        end_date = datetime.now(timezone.utc)

        teams_to_use = []
        if self.assigned_teams is None:
            teams_to_use = [("alpha", "Alpha Team")]
        elif self.assigned_teams:
            teams_to_use = [(t.id, t.name) for t in self.assigned_teams]
        else:
            teams_to_use = [("unassigned", "Unassigned")]

        for i in range(count):
            created_at = end_date - timedelta(days=random.randint(0, 60))
            started_at = created_at + timedelta(hours=random.randint(4, 48))
            completed_at = started_at + timedelta(hours=random.randint(24, 168))

            cycle_time = (completed_at - started_at).total_seconds() / 3600
            team_id, team_name = random.choice(teams_to_use)

            # Simulate flow efficiency (typically 10-40%)
            efficiency = random.uniform(0.1, 0.6)
            active_hours = cycle_time * efficiency
            wait_hours = cycle_time * (1.0 - efficiency)

            records.append(
                WorkItemCycleTimeRecord(
                    work_item_id=f"synth:{self.repo_name}#{i}",
                    provider=self.provider,
                    day=completed_at.date(),
                    work_scope_id=self.repo_name,
                    team_id=team_id,
                    team_name=team_name,
                    assignee=random.choice(self.repo_authors)[0],
                    type=random.choice(["story", "bug", "task"]),
                    status="done",
                    created_at=created_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    cycle_time_hours=cycle_time,
                    lead_time_hours=(completed_at - created_at).total_seconds() / 3600,
                    active_time_hours=active_hours,
                    wait_time_hours=wait_hours,
                    flow_efficiency=efficiency,
                    computed_at=datetime.now(timezone.utc),
                )
            )
        return records

    def _resolve_team(
        self,
        member_map: dict[str, Any] | None,
        author_name: str,
        author_email: str,
    ) -> tuple[str | None, str | None]:
        if not member_map:
            return None, None
        for key in (author_email, author_name):
            if not key:
                continue
            entry = member_map.get(str(key).strip().lower())
            if entry:
                return entry[0], entry[1]
        return None, None

    def generate_user_metrics_daily(
        self,
        *,
        day: date,
        member_map: dict[str, Any] | None = None,
    ) -> list[UserMetricsDailyRecord]:
        records = []
        computed_at = datetime.now(timezone.utc)
        for author_name, author_email in self.repo_authors:
            team_id, team_name = self._resolve_team(
                member_map, author_name, author_email
            )
            commits = random.randint(0, 6)
            loc_added = random.randint(0, 400)
            loc_deleted = random.randint(0, loc_added)
            files_changed = random.randint(0, 10)
            prs = random.randint(0, 3)
            records.append(
                UserMetricsDailyRecord(
                    repo_id=self.repo_id,
                    day=day,
                    author_email=author_email,
                    identity_id=author_email,
                    commits_count=commits,
                    loc_added=loc_added,
                    loc_deleted=loc_deleted,
                    files_changed=files_changed,
                    large_commits_count=int(commits * 0.1),
                    avg_commit_size_loc=float(loc_added + loc_deleted) / commits
                    if commits
                    else 0.0,
                    prs_authored=prs,
                    prs_merged=prs,
                    avg_pr_cycle_hours=24.0,
                    median_pr_cycle_hours=24.0,
                    pr_cycle_p75_hours=24.0,
                    pr_cycle_p90_hours=24.0,
                    prs_with_first_review=prs,
                    pr_first_review_p50_hours=4.0,
                    pr_first_review_p90_hours=8.0,
                    pr_review_time_p50_hours=20.0,
                    pr_pickup_time_p50_hours=2.0,
                    reviews_given=random.randint(0, 5),
                    changes_requested_given=random.randint(0, 1),
                    reviews_received=random.randint(0, 5),
                    review_reciprocity=0.8,
                    team_id=normalize_team_id(team_id),
                    team_name=normalize_team_name(team_name),
                    active_hours=6.0,
                    weekend_days=0,
                    loc_touched=loc_added + loc_deleted,
                    prs_opened=prs,
                    work_items_completed=random.randint(0, 2),
                    work_items_active=random.randint(0, 3),
                    delivery_units=random.randint(1, 10),
                    cycle_p50_hours=48.0,
                    cycle_p90_hours=72.0,
                    computed_at=computed_at,
                )
            )
        return records

    def generate_work_item_user_metrics_daily(
        self,
        *,
        day: date,
        member_map: dict[str, Any] | None = None,
    ) -> list[WorkItemUserMetricsDailyRecord]:
        records = []
        computed_at = datetime.now(timezone.utc)
        for author_name, author_email in self.repo_authors:
            team_id, team_name = self._resolve_team(
                member_map, author_name, author_email
            )
            user_identity = author_email or "unknown"
            records.append(
                WorkItemUserMetricsDailyRecord(
                    day=day,
                    provider=self.provider,
                    work_scope_id=self.repo_name,
                    user_identity=user_identity,
                    team_id=normalize_team_id(team_id),
                    team_name=normalize_team_name(team_name),
                    items_started=random.randint(0, 1),
                    items_completed=random.randint(0, 1),
                    wip_count_end_of_day=random.randint(0, 3),
                    cycle_time_p50_hours=48.0,
                    cycle_time_p90_hours=72.0,
                    computed_at=computed_at,
                )
            )
        return records

    def generate_work_items(
        self,
        days: int = 30,
        projects: list[str] | None = None,
        investment_weights: dict[str, float] | None = None,
        provider: str | None = None,
    ) -> list[WorkItem]:
        items = []
        end_date = datetime.now(timezone.utc)
        provider_value = provider or self.provider
        description_keywords = {
            "story": ["feature", "implement"],
            "task": ["refactor", "cleanup"],
            "bug": ["bug", "fix"],
            "epic": ["feature", "introduce"],
            "incident": ["incident", "hotfix"],
            "chore": ["cleanup", "upgrade"],
            "issue": ["feature", "fix"],
        }

        # Defaults
        if not projects:
            projects = [self.repo_name]

        if not investment_weights:
            investment_weights = {
                "product": 0.5,
                "security": 0.1,
                "infra": 0.15,
                "quality": 0.1,
                "docs": 0.05,
                "data": 0.1,
            }

        sub_categories_map = {
            "product": [
                "feature",
                "ux",
                "onboarding",
                "mobile",
                "api",
                "growth",
                "monetization",
            ],
            "security": [
                "auth",
                "vulnerability",
                "compliance",
                "audit",
                "encryption",
                "access-control",
            ],
            "infra": [
                "k8s",
                "terraform",
                "ci-cd",
                "monitoring",
                "cost",
                "network",
                "database",
            ],
            "quality": [
                "testing",
                "flake",
                "coverage",
                "perf",
                "reliability",
                "automation",
            ],
            "docs": ["api-docs", "user-guide", "tutorial", "readme", "release-notes"],
            "data": [
                "pipeline",
                "schema",
                "analytics",
                "warehouse",
                "etl",
                "visualization",
            ],
        }

        # Normalize weights
        total_weight = sum(investment_weights.values())
        normalized_weights = {
            k: v / total_weight for k, v in investment_weights.items()
        }
        categories = list(normalized_weights.keys())
        weights = list(normalized_weights.values())

        # Generate Epics per project (Long running)
        project_epics = {}
        for proj in projects:
            project_epics[proj] = []
            # Create 1-3 active epics per project
            for i in range(random.randint(1, 3)):
                epic_created_at = end_date - timedelta(
                    days=random.randint(days, days + 60)
                )
                epic_number = 9000 + i + 1
                project_key = proj.split("/")[-1].upper()[:3]
                if provider_value == "github":
                    epic_id = f"gh:{proj}#{epic_number}"
                elif provider_value == "gitlab":
                    epic_id = f"gitlab:{proj}#{epic_number}"
                elif provider_value == "jira":
                    epic_id = f"jira:{project_key}-{epic_number}"
                else:
                    epic_id = f"{proj}-EPIC-{i + 1}"
                category = random.choices(categories, weights=weights, k=1)[0]

                # Pick a random sub-category for the epic
                sub_cats = sub_categories_map.get(category, [])
                sub_category = random.choice(sub_cats) if sub_cats else category

                epic_keywords = description_keywords.get(
                    "epic", ["feature", "implement"]
                )
                epic_description = (
                    f"{category.title()} epic focused on {sub_category}. "
                    f"{epic_keywords[0].title()} and {epic_keywords[1]} work planned."
                )
                # Create the Epic item
                epic = WorkItem(
                    work_item_id=epic_id,
                    provider=provider_value,
                    title=f"Epic: {category.title()} - {sub_category.title()} Initiative {i + 1}",
                    type="epic",
                    status="in_progress",  # Epics often stay open
                    status_raw="In Progress",
                    description=epic_description,
                    repo_id=self.repo_id,
                    project_id=proj,
                    project_key=project_key if provider_value == "jira" else proj,
                    created_at=epic_created_at,
                    updated_at=epic_created_at,
                    started_at=epic_created_at + timedelta(days=1),
                    completed_at=None,
                    closed_at=None,
                    reporter=random.choice(self.repo_authors)[1],
                    assignees=[random.choice(self.repo_authors)[1]],
                    labels=[category, sub_category, "strategic"],
                    story_points=None,
                )
                items.append(epic)
                project_epics[proj].append(epic)

        # Generate standard work items
        # Roughly 2 items per day per project
        total_items = days * 2 * len(projects)

        for i in range(total_items):
            project = random.choice(projects)
            author_name, author_email = random.choice(self.repo_authors)

            # Random date within range
            created_at = end_date - timedelta(
                days=random.randint(0, days), hours=random.randint(0, 23)
            )

            # Determine Investment Category & Parent
            category = random.choices(categories, weights=weights, k=1)[0]

            # Pick a random sub-category
            sub_cats = sub_categories_map.get(category, [])
            sub_category = random.choice(sub_cats) if sub_cats else category

            labels = [category, sub_category]

            # Link to an Epic if available (50% chance)
            parent_epic_id = None
            if project_epics.get(project) and random.random() > 0.5:
                parent_epic = random.choice(project_epics[project])
                # Inherit category from Epic if linked, or keep random?
                # Usually child items relate to Epic. Let's align them often.
                if random.random() > 0.3:
                    # primary category is the first label
                    category = parent_epic.labels[0]
                    # Try to inherit sub-category or pick a related one
                    if len(parent_epic.labels) > 1:
                        sub_category = parent_epic.labels[1]
                    else:
                        sub_cats = sub_categories_map.get(category, [])
                        sub_category = random.choice(sub_cats) if sub_cats else category

                    labels = [category, sub_category]

                parent_epic_id = parent_epic.work_item_id

            # Determine Type
            is_bug = (
                random.random() > 0.7
                if category == "quality"
                else random.random() > 0.85
            )
            item_type: WorkItemType = (
                "bug" if is_bug else random.choice(["story", "task"])
            )

            # For bugs, add 'bug' label
            if is_bug:
                labels.append("bug")

            # Lifecycle
            is_done = random.random() > 0.3
            started_at = None
            completed_at = None
            status = "done" if is_done else "in_progress"

            if is_done or random.random() > 0.5:
                # Started 1-5 days after creation
                started_at = created_at + timedelta(hours=random.randint(1, 120))
                if started_at > end_date:
                    started_at = end_date - timedelta(hours=1)

                if is_done:
                    # Completed 1-7 days after start
                    completed_at = started_at + timedelta(hours=random.randint(4, 168))
                    if completed_at > end_date:
                        completed_at = end_date
                        status = "in_progress"  # Can't be done if date is future

            issue_number = i + 100
            project_key = project.split("/")[-1].upper()[:3]
            if provider_value == "github":
                work_item_id = f"gh:{project}#{issue_number}"
            elif provider_value == "gitlab":
                work_item_id = f"gitlab:{project}#{issue_number}"
            elif provider_value == "jira":
                work_item_id = f"jira:{project_key}-{issue_number}"
            else:
                work_item_id = f"{project}-{issue_number}"

            item_keywords = description_keywords.get(item_type, ["feature", "fix"])
            description = (
                f"{category.title()} work in {sub_category}. "
                f"{item_keywords[0].title()} focus with {item_keywords[1]} checks."
            )
            updated_at = completed_at or started_at or created_at

            items.append(
                WorkItem(
                    work_item_id=work_item_id,
                    provider=provider_value,
                    title=f"[{project}] {category.title()}/{sub_category.title()} {item_type} {i}",
                    type=item_type,
                    status=status,
                    status_raw=status,
                    description=description,
                    repo_id=self.repo_id,
                    project_id=project,
                    project_key=project_key
                    if provider_value == "jira"
                    else project,  # Jira style
                    created_at=created_at,
                    updated_at=updated_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    closed_at=completed_at,
                    reporter=author_email,
                    assignees=[author_email] if random.random() > 0.3 else [],
                    labels=labels,
                    epic_id=parent_epic_id,
                    parent_id=parent_epic_id,  # Simplified: parent is epic
                    story_points=random.choice([1, 2, 3, 5, 8])
                    if item_type == "story"
                    else None,
                )
            )

        # Sort by created_at for realism
        items.sort(key=lambda x: x.created_at)
        return items

    def generate_teams_config(self) -> dict[str, Any]:
        """
        Generate a team mapping configuration for the synthetic users.
        """
        # Split authors into two teams
        mid = len(self.authors) // 2
        team_alpha = self.authors[:mid]
        team_beta = self.authors[mid:]

        return {
            "teams": [
                {
                    "team_id": "team-alpha",
                    "team_name": "Team Alpha",
                    "members": [email for _, email in team_alpha],
                },
                {
                    "team_id": "team-beta",
                    "team_name": "Team Beta",
                    "members": [email for _, email in team_beta],
                },
            ]
        }

    def generate_work_item_transitions(
        self, items: list[WorkItem]
    ) -> list[WorkItemStatusTransition]:
        transitions = []
        for item in items:
            # Simple transition from todo -> in_progress -> done
            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=item.work_item_id,
                    provider=item.provider,
                    occurred_at=item.created_at,
                    from_status_raw=None,
                    to_status_raw="todo",
                    from_status="backlog",
                    to_status="todo",
                )
            )
            if item.started_at:
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        occurred_at=item.started_at,
                        from_status_raw="todo",
                        to_status_raw="in_progress",
                        from_status="todo",
                        to_status="in_progress",
                    )
                )

                # Randomly inject a wait state (blocked) between start and complete
                if item.completed_at and random.random() > 0.5:
                    duration = (item.completed_at - item.started_at).total_seconds()
                    if duration > 7200:  # If duration > 2 hours
                        blocked_at = item.started_at + timedelta(
                            seconds=random.randint(3600, int(duration * 0.4))
                        )
                        unblocked_at = blocked_at + timedelta(
                            seconds=random.randint(1800, int(duration * 0.4))
                        )

                        transitions.append(
                            WorkItemStatusTransition(
                                work_item_id=item.work_item_id,
                                provider=item.provider,
                                occurred_at=blocked_at,
                                from_status_raw="in_progress",
                                to_status_raw="blocked",
                                from_status="in_progress",
                                to_status="blocked",
                            )
                        )
                        transitions.append(
                            WorkItemStatusTransition(
                                work_item_id=item.work_item_id,
                                provider=item.provider,
                                occurred_at=unblocked_at,
                                from_status_raw="blocked",
                                to_status_raw="in_progress",
                                from_status="blocked",
                                to_status="in_progress",
                            )
                        )

            if item.completed_at:
                # Need to determine the 'from' status
                # Ideally we track current status, but for now assuming we return to 'in_progress' before done
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        occurred_at=item.completed_at,
                        from_status_raw="in_progress",
                        to_status_raw="done",
                        from_status="in_progress",
                        to_status="done",
                    )
                )
        return transitions

    def generate_work_item_dependencies(
        self, items: list[WorkItem]
    ) -> list[WorkItemDependency]:
        dependencies = []
        synced_at = datetime.now(timezone.utc)
        parent_edge_rate = 0.2

        # 1. Parent/Child (Epic -> Story)
        # Note: In generate_work_items, we already set parent_id/epic_id on items.
        # We should reflect these as explicit dependencies.
        for item in items:
            if item.parent_id and random.random() < parent_edge_rate:
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=item.parent_id,
                        target_work_item_id=item.work_item_id,
                        relationship_type="parent",
                        relationship_type_raw="Parent",
                        last_synced=synced_at,
                    )
                )
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=item.work_item_id,
                        target_work_item_id=item.parent_id,
                        relationship_type="child",
                        relationship_type_raw="Child",
                        last_synced=synced_at,
                    )
                )

        candidates = [i for i in items if i.type != "epic"]
        if len(candidates) > 2:
            num_links = min(len(candidates) // 20, 10)
            for idx in range(num_links):
                source_idx = (idx * 7) % len(candidates)
                target_idx = (source_idx + 1) % len(candidates)
                source = candidates[source_idx]
                target = candidates[target_idx]

                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=source.work_item_id,
                        target_work_item_id=target.work_item_id,
                        relationship_type="blocks",
                        relationship_type_raw="Blocks",
                        last_synced=synced_at,
                    )
                )
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=target.work_item_id,
                        target_work_item_id=source.work_item_id,
                        relationship_type="is_blocked_by",
                        relationship_type_raw="Is Blocked By",
                        last_synced=synced_at,
                    )
                )

        return dependencies

    def generate_worklogs(self, work_items: list[WorkItem]) -> list[Worklog]:
        now = datetime.now(timezone.utc)
        worklogs: list[Worklog] = []
        for work_item in work_items:
            if not work_item.started_at:
                continue
            if random.random() > 0.4:
                continue
            count = random.randint(1, 3)
            end_bound = work_item.completed_at or now
            if end_bound <= work_item.started_at:
                end_bound = work_item.started_at + timedelta(hours=1)
            for i in range(count):
                span = (end_bound - work_item.started_at).total_seconds()
                offset = random.uniform(0, max(span, 1))
                started_at = work_item.started_at + timedelta(seconds=offset)
                time_spent = random.randint(900, 28800)
                created_at = started_at + timedelta(seconds=random.randint(1, 300))
                _, author_email = random.choice(self.repo_authors)
                worklogs.append(
                    Worklog(
                        work_item_id=work_item.work_item_id,
                        provider=work_item.provider,
                        worklog_id=f"wl-{work_item.work_item_id}-{i}",
                        author=author_email,
                        started_at=started_at,
                        time_spent_seconds=time_spent,
                        created_at=created_at,
                        updated_at=created_at,
                    )
                )
        return worklogs

    def generate_pr_commits(
        self,
        prs: list[GitPullRequest],
        commits: list[GitCommit],
        *,
        org_id: str = "",
    ) -> list[dict[str, Any]]:
        """
        Link PRs to commits.
        Assumes commits and PRs are already generated.
        Returns a list of dicts suitable for insertion into work_graph_pr_commit.
        """
        links = []
        synced_at = datetime.now(timezone.utc)

        # Sort commits by date
        commits_sorted = sorted(commits, key=lambda c: c.committer_when)

        # For each PR, pick a range of commits that happened before PR merge/close
        # and after PR creation (loosely).

        # Shuffle PRs to distribute commits
        shuffled_prs = list(prs)
        random.shuffle(shuffled_prs)

        # Naive distribution: each PR gets 1-5 commits
        # If we have more commits than PRs * 5, some commits might be orphaned (which is fine, direct pushes)
        # If we have fewer, we reuse commits? No, commits belong to one PR usually.

        available_commits = list(commits_sorted)

        for pr in shuffled_prs:
            if not commits_sorted:
                break

            upper = min(5, len(available_commits)) if available_commits else 0
            if upper >= 2:
                num_commits = random.randint(2, upper)
            else:
                num_commits = 2

            # Pick commits close to PR creation
            # This is O(N^2) effectively if we iterate, but lists are small for fixtures.
            # Let's just pop from available for simplicity in synthetic gen.

            pr_commits = []
            for _ in range(num_commits):
                if not available_commits:
                    break
                # Pop from end? or start? Start is oldest.
                # PRs are somewhat random in time.
                # Let's just pick random commits for now, but valid logic would be better.
                # Given strict requirements, let's just assign.
                c = available_commits.pop(0)
                pr_commits.append(c)

            if len(pr_commits) < num_commits:
                supplement = [c for c in commits_sorted if c not in pr_commits]
                need = min(num_commits - len(pr_commits), len(supplement))
                if need > 0:
                    pr_commits.extend(random.sample(supplement, k=need))

            for c in pr_commits:
                links.append(
                    {
                        "repo_id": str(pr.repo_id),
                        "pr_number": pr.number,
                        "commit_hash": c.hash,
                        "confidence": 1.0,
                        "provenance": "synthetic",
                        "evidence": "generated_fixture",
                        "last_synced": synced_at,
                        "org_id": org_id,
                    }
                )

        return links

    def generate_issue_pr_links(
        self,
        work_items: list[WorkItem],
        prs: list[GitPullRequest],
        *,
        min_coverage: float = 0.7,
        cluster_size: int = 5,
        org_id: str = "",
    ) -> list[dict[str, Any]]:
        """Generate work_graph_issue_pr rows with isolated clusters for multiple components."""
        if not work_items or not prs:
            return []

        candidates = [wi for wi in work_items if getattr(wi, "work_item_id", None)]
        pr_numbers = [
            int(pr.number) for pr in prs if getattr(pr, "number", None) is not None
        ]
        if not candidates or not pr_numbers:
            return []

        target_count = max(1, int(len(candidates) * float(min_coverage)))
        random.shuffle(candidates)
        linked_items = candidates[:target_count]

        synced_at = datetime.now(timezone.utc)
        links: list[dict[str, Any]] = []

        num_clusters = max(1, len(linked_items) // cluster_size)
        pr_idx = 0

        for cluster_idx in range(num_clusters):
            start = cluster_idx * cluster_size
            end = min(start + cluster_size, len(linked_items))
            cluster_items = linked_items[start:end]

            if not cluster_items:
                continue

            cluster_prs = [pr_numbers[pr_idx % len(pr_numbers)]]
            pr_idx += 1

            if len(pr_numbers) > 1 and random.random() < 0.3:
                second_pr = pr_numbers[pr_idx % len(pr_numbers)]
                if second_pr != cluster_prs[0]:
                    cluster_prs.append(second_pr)
                pr_idx += 1

            for wi in cluster_items:
                links.append(
                    {
                        "repo_id": str(self.repo_id),
                        "work_item_id": str(wi.work_item_id),
                        "pr_number": cluster_prs[0],
                        "confidence": 1.0,
                        "provenance": "synthetic",
                        "evidence": "generated_fixture",
                        "last_synced": synced_at,
                        "org_id": org_id,
                    }
                )
                if len(cluster_prs) > 1 and random.random() < 0.2:
                    links.append(
                        {
                            "repo_id": str(self.repo_id),
                            "work_item_id": str(wi.work_item_id),
                            "pr_number": cluster_prs[1],
                            "confidence": 1.0,
                            "provenance": "synthetic",
                            "evidence": "generated_fixture",
                            "last_synced": synced_at,
                            "org_id": org_id,
                        }
                    )

        return links

    def generate_repo_metrics_daily(
        self, days: int = 30
    ) -> list[RepoMetricsDailyRecord]:
        records = []
        end_date = datetime.now(timezone.utc).date()
        for i in range(days):
            day = end_date - timedelta(days=i)
            records.append(
                RepoMetricsDailyRecord(
                    repo_id=self.repo_id,
                    day=day,
                    commits_count=random.randint(1, 20),
                    total_loc_touched=random.randint(150, 3000),
                    avg_commit_size_loc=float(random.randint(10, 100)),
                    large_commit_ratio=random.uniform(0.0, 0.2),
                    prs_merged=random.randint(0, 5),
                    median_pr_cycle_hours=float(random.randint(4, 72)),
                    computed_at=datetime.now(timezone.utc),
                )
            )
        return records

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

    def generate_investment_classifications(
        self, work_items: list[WorkItem], days: int = 30
    ) -> list[Any]:
        """Generate investment classification records from work items."""
        from dev_health_ops.metrics.schemas import InvestmentClassificationRecord

        records = []
        computed_at = datetime.now(timezone.utc)

        for item in work_items:
            if item.type == "epic":
                continue
            # Use the first label as investment_area (items already have category labels)
            investment_area = item.labels[0] if item.labels else "product"
            project_stream = item.labels[1] if len(item.labels) > 1 else ""
            day = item.created_at.date()

            records.append(
                InvestmentClassificationRecord(
                    repo_id=self.repo_id,
                    day=day,
                    artifact_type="work_item",
                    artifact_id=item.work_item_id,
                    provider=item.provider,
                    investment_area=investment_area,
                    project_stream=project_stream,
                    confidence=random.uniform(0.7, 1.0),
                    rule_id="synthetic-label-match",
                    computed_at=computed_at,
                )
            )
        return records

    def generate_work_unit_investments(
        self,
        work_items: list[WorkItem],
        days: int = 30,
        *,
        org_id: str = "",
        categorization_run_id: str | None = None,
    ) -> list[Any]:
        """Generate synthetic work unit investment records from work items."""
        from dev_health_ops.metrics.schemas import WorkUnitInvestmentRecord

        def _normalize_distribution(
            distribution: dict[str, float],
        ) -> dict[str, float]:
            total = sum(distribution.values())
            if total <= 0:
                return distribution
            normalized = {key: value / total for key, value in distribution.items()}
            keys = list(normalized.keys())
            if keys:
                normalized[keys[-1]] += 1.0 - sum(normalized.values())
            return normalized

        def _theme_distribution_for_item(item: WorkItem) -> dict[str, float]:
            item_type = (item.type or "").lower()
            labels = {label.lower() for label in item.labels}

            if item_type == "bug" or "bug" in labels:
                return _normalize_distribution(
                    {
                        "feature_delivery": random.uniform(0.02, 0.08),
                        "operational_support": random.uniform(0.04, 0.12),
                        "maintenance_tech_debt": random.uniform(0.08, 0.18),
                        "quality_reliability": random.uniform(0.68, 0.82),
                        "risk_security": random.uniform(0.02, 0.08),
                    }
                )

            if item_type == "story":
                return _normalize_distribution(
                    {
                        "feature_delivery": random.uniform(0.68, 0.84),
                        "operational_support": random.uniform(0.02, 0.07),
                        "maintenance_tech_debt": random.uniform(0.06, 0.14),
                        "quality_reliability": random.uniform(0.05, 0.14),
                        "risk_security": random.uniform(0.01, 0.05),
                    }
                )

            theme_distribution = {
                "feature_delivery": random.uniform(0.20, 0.38),
                "operational_support": random.uniform(0.06, 0.15),
                "maintenance_tech_debt": random.uniform(0.24, 0.42),
                "quality_reliability": random.uniform(0.12, 0.26),
                "risk_security": random.uniform(0.02, 0.10),
            }
            if "security" in labels:
                theme_distribution["risk_security"] += 0.10
                theme_distribution["feature_delivery"] = max(
                    0.05, theme_distribution["feature_delivery"] - 0.05
                )
            return _normalize_distribution(theme_distribution)

        def _subcategory_distribution_for_item(
            item: WorkItem, theme_distribution: dict[str, float]
        ) -> dict[str, float]:
            item_type = (item.type or "").lower()
            labels = {label.lower() for label in item.labels}

            feature_value = theme_distribution["feature_delivery"]
            if item_type == "story":
                feature_split = (0.72, 0.28)
            elif item_type == "task":
                feature_split = (0.35, 0.65)
            else:
                feature_split = (0.45, 0.55)

            maintenance_value = theme_distribution["maintenance_tech_debt"]
            maintenance_split = (0.65, 0.35)
            if "infra" in labels or "dependencies" in labels:
                maintenance_split = (0.40, 0.60)

            quality_value = theme_distribution["quality_reliability"]
            quality_split = (0.85, 0.15) if item_type == "bug" else (0.35, 0.65)

            return {
                "feature_delivery.new_feature": feature_value * feature_split[0],
                "feature_delivery.enhancement": feature_value * feature_split[1],
                "maintenance_tech_debt.refactoring": maintenance_value
                * maintenance_split[0],
                "maintenance_tech_debt.dependency_update": maintenance_value
                * maintenance_split[1],
                "quality_reliability.bug_fix": quality_value * quality_split[0],
                "quality_reliability.test_improvement": quality_value
                * quality_split[1],
                "operational_support.incident_response": theme_distribution[
                    "operational_support"
                ],
                "risk_security.vulnerability_fix": theme_distribution["risk_security"],
            }

        records = []
        computed_at = datetime.now(timezone.utc)
        max_duration_days = max(1, min(days, 14))
        run_id = categorization_run_id or str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"fixture-work-unit-investments:{self.repo_id}:{days}:{org_id}",
            )
        )

        for item in work_items:
            if item.type == "epic":
                continue

            from_ts = item.created_at
            if item.completed_at is not None:
                to_ts = item.completed_at
            else:
                to_ts = from_ts + timedelta(days=random.randint(1, max_duration_days))

            if to_ts < from_ts:
                to_ts = from_ts

            theme_distribution = _theme_distribution_for_item(item)
            subcategory_distribution = _subcategory_distribution_for_item(
                item, theme_distribution
            )

            quality = round(random.uniform(0.5, 1.0), 3)
            input_hash = hashlib.md5(
                "|".join(
                    [
                        item.work_item_id,
                        item.type or "",
                        item.title or "",
                        item.status or "",
                        item.provider or "",
                        from_ts.isoformat(),
                        to_ts.isoformat(),
                    ]
                ).encode("utf-8")
            ).hexdigest()

            records.append(
                WorkUnitInvestmentRecord(
                    work_unit_id=item.work_item_id,
                    work_unit_type=item.type,
                    work_unit_name=item.title,
                    from_ts=from_ts,
                    to_ts=to_ts,
                    repo_id=self.repo_id,
                    provider=item.provider,
                    effort_metric="loc",
                    effort_value=float(random.randint(80, 3200)),
                    theme_distribution_json=theme_distribution,
                    subcategory_distribution_json=subcategory_distribution,
                    structural_evidence_json=json.dumps({}),
                    evidence_quality=quality,
                    evidence_quality_band="high" if quality >= 0.75 else "medium",
                    categorization_status="completed",
                    categorization_errors_json=json.dumps({}),
                    categorization_model_version="synthetic-v1",
                    categorization_input_hash=input_hash,
                    categorization_run_id=run_id,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )

        return records

    def generate_investment_metrics(self, days: int = 30) -> list[Any]:
        """Generate investment metrics daily rollup records."""
        from dev_health_ops.metrics.schemas import InvestmentMetricsRecord

        records = []
        end_date = datetime.now(timezone.utc).date()
        computed_at = datetime.now(timezone.utc)

        investment_areas = ["product", "security", "infra", "quality", "docs", "data"]

        teams_to_use = []
        if self.assigned_teams is None:
            teams_to_use = [("alpha", "Alpha Team")]
        elif self.assigned_teams:
            teams_to_use = [(t.id, t.name) for t in self.assigned_teams]
        else:
            teams_to_use = [("unassigned", "Unassigned")]

        for i in range(days):
            day = end_date - timedelta(days=i)
            for team_id, _ in teams_to_use:
                for area in investment_areas:
                    records.append(
                        InvestmentMetricsRecord(
                            repo_id=self.repo_id,
                            day=day,
                            team_id=team_id,
                            investment_area=area,
                            project_stream="",
                            delivery_units=random.randint(0, 5),
                            work_items_completed=random.randint(0, 3),
                            prs_merged=random.randint(0, 2),
                            churn_loc=random.randint(0, 500),
                            cycle_p50_hours=random.uniform(12.0, 72.0),
                            computed_at=computed_at,
                        )
                    )
        return records

    def generate_file_hotspot_daily(self, days: int = 30) -> list[Any]:
        """Generate file hotspot daily records using synthetic complexity and churn data."""
        from dev_health_ops.metrics.schemas import FileHotspotDaily

        records = []
        end_date = datetime.now(timezone.utc)
        computed_at = datetime.now(timezone.utc)

        for i in range(days):
            day = (end_date - timedelta(days=i)).date()
            for file_path in self.files:
                churn_loc = random.randint(10, 500)
                churn_commits = random.randint(1, 20)
                cc_total = random.randint(5, 100)
                funcs = random.randint(3, 30)
                cc_avg = cc_total / funcs if funcs else 0.0
                blame_conc = random.uniform(0.3, 1.0)

                # risk = normalized(churn) + normalized(complexity)
                risk_score = random.uniform(-1.0, 3.0)

                records.append(
                    FileHotspotDaily(
                        repo_id=self.repo_id,
                        day=day,
                        file_path=file_path,
                        churn_loc_30d=churn_loc,
                        churn_commits_30d=churn_commits,
                        cyclomatic_total=cc_total,
                        cyclomatic_avg=cc_avg,
                        blame_concentration=blame_conc,
                        risk_score=risk_score,
                        computed_at=computed_at,
                    )
                )
        return records

    def generate_file_metrics(self) -> list[FileMetricsRecord]:
        records = []
        computed_at = datetime.now(timezone.utc)
        today = computed_at.date()
        for file_path in self.files:
            records.append(
                FileMetricsRecord(
                    repo_id=self.repo_id,
                    day=today,
                    path=file_path,
                    churn=random.randint(10, 1000),
                    contributors=random.randint(1, 5),
                    commits_count=random.randint(1, 20),
                    hotspot_score=random.uniform(0.0, 1.0),
                    computed_at=computed_at,
                )
            )
        return records

    def generate_users(
        self,
        *,
        default_password: str = "devhealth123",
        include_admin: bool = True,
    ) -> dict[str, Any]:
        import bcrypt

        from dev_health_ops.licensing.types import LicenseTier
        from dev_health_ops.models.licensing import OrgLicense
        from dev_health_ops.models.users import Membership, Organization, User

        users = []
        orgs = []
        memberships = []
        licenses = []

        password_hash = bcrypt.hashpw(
            default_password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

        if include_admin:
            admin_user = User(
                id=uuid.uuid5(
                    uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                    "admin@devhealth.example",
                ),
                email="admin@devhealth.example",
                username="admin",
                password_hash=password_hash,
                full_name="Admin User",
                auth_provider="local",
                is_active=True,
                is_verified=True,
                is_superuser=True,
            )
            users.append(admin_user)

            admin_org = Organization(
                id=uuid.uuid5(
                    uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), "default-org"
                ),
                slug="default-org",
                name="Default Organization",
                tier="enterprise",
                is_active=True,
            )
            orgs.append(admin_org)

            memberships.append(
                Membership(
                    id=uuid.uuid5(admin_user.id, str(admin_org.id)),
                    user_id=admin_user.id,
                    org_id=admin_org.id,
                    role="owner",
                    joined_at=datetime.now(timezone.utc),
                )
            )

            admin_license = OrgLicense(
                org_id=admin_org.id,
                tier=LicenseTier.ENTERPRISE.value,
                license_type="saas",
                licensed_users=None,
                licensed_repos=None,
                issued_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(days=365),
            )
            admin_license.id = uuid.uuid5(admin_org.id, "org-license")
            licenses.append(admin_license)

        default_org_id = None
        if orgs:
            default_org_id = orgs[0].id

        for name, email in self.authors[:5]:
            user_id = uuid.uuid5(
                uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), email
            )
            user = User(
                id=user_id,
                email=email,
                username=email.split("@")[0],
                password_hash=password_hash,
                full_name=name,
                auth_provider="local",
                is_active=True,
                is_verified=True,
                is_superuser=False,
            )
            users.append(user)

            if default_org_id:
                memberships.append(
                    Membership(
                        id=uuid.uuid5(user_id, str(default_org_id)),
                        user_id=user_id,
                        org_id=default_org_id,
                        role="member",
                        joined_at=datetime.now(timezone.utc),
                    )
                )

        return {
            "users": users,
            "organizations": orgs,
            "memberships": memberships,
            "licenses": licenses,
            "default_password": default_password,
        }

    def generate_work_item_reopen_events(
        self, transitions: list[WorkItemStatusTransition]
    ) -> list[WorkItemReopenEvent]:
        """Extract reopen events from transitions where from_status is 'done' and
        to_status is not 'done' or 'canceled'.

        Also synthetically generates reopen events for ~10% of completed work items
        that do not already have a reopen transition.
        """
        reopen_events = []
        last_synced = datetime.now(timezone.utc)
        reopened_item_ids: set = set()

        for t in transitions:
            if t.from_status == "done" and t.to_status not in ("done", "canceled"):
                reopen_events.append(
                    WorkItemReopenEvent(
                        work_item_id=t.work_item_id,
                        occurred_at=t.occurred_at,
                        from_status=t.from_status,
                        to_status=t.to_status,
                        from_status_raw=t.from_status_raw,
                        to_status_raw=t.to_status_raw,
                        actor=getattr(t, "actor", None),
                        last_synced=last_synced,
                    )
                )
                reopened_item_ids.add(t.work_item_id)

        # Collect completed items not already reopened, then add ~10% more
        done_transitions_by_item: dict = {}
        for t in transitions:
            if t.to_status == "done":
                done_transitions_by_item[t.work_item_id] = t

        candidates = [
            t
            for item_id, t in done_transitions_by_item.items()
            if item_id not in reopened_item_ids
        ]
        num_extra = max(0, int(len(candidates) * 0.1))
        if num_extra > 0 and candidates:
            extra = random.sample(candidates, min(num_extra, len(candidates)))
            for done_t in extra:
                # Reopen occurs 1-7 days after completion
                reopen_at = done_t.occurred_at + timedelta(
                    days=random.randint(1, 7), hours=random.randint(0, 23)
                )
                actor_name, actor_email = random.choice(self.repo_authors)
                reopen_events.append(
                    WorkItemReopenEvent(
                        work_item_id=done_t.work_item_id,
                        occurred_at=reopen_at,
                        from_status="done",
                        to_status="in_progress",
                        from_status_raw="done",
                        to_status_raw="in_progress",
                        actor=actor_email,
                        last_synced=last_synced,
                    )
                )

        return reopen_events

    def generate_work_item_interactions(
        self, work_items: list[WorkItem]
    ) -> list[WorkItemInteractionEvent]:
        """Generate 0-5 comment interaction events per work item."""
        interactions = []
        last_synced = datetime.now(timezone.utc)
        now = datetime.now(timezone.utc)

        for item in work_items:
            num_interactions = random.randint(0, 5)
            if num_interactions == 0:
                continue

            end_time = item.completed_at or now
            if end_time <= item.created_at:
                end_time = item.created_at + timedelta(hours=1)

            duration_seconds = int((end_time - item.created_at).total_seconds())

            for _ in range(num_interactions):
                offset_seconds = (
                    random.randint(0, duration_seconds) if duration_seconds > 0 else 0
                )
                occurred_at = item.created_at + timedelta(seconds=offset_seconds)
                actor_name, actor_email = random.choice(self.repo_authors)

                interactions.append(
                    WorkItemInteractionEvent(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        interaction_type="comment",
                        occurred_at=occurred_at,
                        actor=actor_email,
                        body_length=random.randint(20, 500),
                        last_synced=last_synced,
                    )
                )

        return interactions

    def generate_sprints(self, days: int = 30) -> list[Sprint]:
        """Generate 2-week sprints covering the time window."""
        sprints = []
        last_synced = datetime.now(timezone.utc)
        now = datetime.now(timezone.utc)

        sprint_duration = timedelta(days=14)
        # Start far enough back to cover the full window
        window_start = now - timedelta(days=days)

        # Align sprint start to the earliest 2-week boundary before window_start
        sprint_start = window_start - timedelta(
            days=window_start.weekday()
        )  # align to Monday

        # Generate enough sprints to cover window + a couple future sprints
        sprint_index = 1
        current_start = sprint_start
        while current_start < now + timedelta(days=28):
            sprint_end = current_start + sprint_duration

            if sprint_end < now:
                state = "closed"
                completed_at = sprint_end
            elif current_start <= now < sprint_end:
                state = "active"
                completed_at = None
            else:
                state = "future"
                completed_at = None

            sprints.append(
                Sprint(
                    provider=self.provider,
                    sprint_id=f"sprint-{sprint_index}",
                    name=f"Sprint {sprint_index}",
                    state=state,
                    started_at=current_start,
                    ended_at=sprint_end,
                    completed_at=completed_at,
                    last_synced=last_synced,
                )
            )

            current_start = sprint_end
            sprint_index += 1

        return sprints

    def assign_sprints_to_work_items(
        self, work_items: list[WorkItem], sprints: list[Sprint]
    ) -> list[WorkItem]:
        """Assign sprint_id/sprint_name to ~60% of non-epic work items.

        For each eligible work item, picks the sprint whose time window contains
        the item's created_at, falling back to any closed/active sprint.
        """
        import dataclasses

        if not sprints:
            return work_items

        closed_or_active = [s for s in sprints if s.state in ("closed", "active")]
        if not closed_or_active:
            closed_or_active = list(sprints)

        result = []
        for item in work_items:
            if item.type == "epic" or random.random() > 0.6:
                result.append(item)
                continue

            # Find the sprint that contains the item's created_at
            chosen_sprint = None
            for s in sprints:
                if s.started_at and s.ended_at:
                    if s.started_at <= item.created_at <= s.ended_at:
                        chosen_sprint = s
                        break

            if chosen_sprint is None:
                chosen_sprint = random.choice(closed_or_active)

            result.append(
                dataclasses.replace(
                    item,
                    sprint_id=chosen_sprint.sprint_id,
                    sprint_name=chosen_sprint.name,
                )
            )

        return result
