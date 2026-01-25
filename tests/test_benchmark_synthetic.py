import logging
import time
from datetime import datetime, timedelta, timezone, time as dt_time

import pytest
from sqlalchemy import text

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.metrics.job_daily import _normalize_sqlite_url
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink
from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.compute_wellbeing import (
    compute_team_wellbeing_metrics_daily,
)
from dev_health_ops.metrics.compute_work_items import compute_work_item_metrics_daily
from dev_health_ops.metrics.compute_ic import compute_ic_metrics_daily
from dev_health_ops.metrics.quality import (
    compute_rework_churn_ratio,
    compute_single_owner_file_ratio,
)
from dev_health_ops.providers.teams import TeamResolver
from dev_health_ops.storage import SQLAlchemyStore


# Use a marker to allow skipping this slow benchmark
@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_synthetic_data_performance(tmp_path):
    """
    Benchmark the performance of metrics computation on a synthetically generated dataset.

    This test generates a moderate dataset (90 days, 10 commits/day) and runs the full
    daily metrics computation pipeline, measuring execution time.
    """
    # Configuration
    DAYS = 30
    COMMITS_PER_DAY = 10
    PR_COUNT = 50
    REPO_NAME = "benchmark/synthetic-repo"

    db_path = tmp_path / "benchmark.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"

    logging.info(
        f"Starting benchmark with DAYS={DAYS}, COMMITS_PER_DAY={COMMITS_PER_DAY}, DB={db_path}"
    )

    # 1. Generate Data
    start_time = time.time()
    generator = SyntheticDataGenerator(repo_name=REPO_NAME)

    repo = generator.generate_repo()
    files = generator.generate_files()
    commits = generator.generate_commits(days=DAYS, commits_per_day=COMMITS_PER_DAY)
    stats = generator.generate_commit_stats(commits)
    pr_data = generator.generate_prs(count=PR_COUNT)
    prs = [p["pr"] for p in pr_data]
    reviews = [r for p in pr_data for r in p["reviews"]]
    pipeline_runs = generator.generate_ci_pipeline_runs(days=DAYS)
    deployments = generator.generate_deployments(
        days=DAYS, pr_numbers=[pr.number for pr in prs]
    )
    incidents = generator.generate_incidents(days=DAYS)
    work_items = generator.generate_work_items(days=DAYS)
    transitions = generator.generate_work_item_transitions(work_items)

    gen_duration = time.time() - start_time
    logging.info(f"Data generation took {gen_duration:.2f}s")

    # 2. Insert Data
    start_time = time.time()
    store = SQLAlchemyStore(db_url)
    async with store:
        await store.ensure_tables()
        await store.insert_repo(repo)
        await store.insert_git_file_data(files)
        await store.insert_git_commit_data(commits)
        await store.insert_git_commit_stats(stats)
        await store.insert_git_pull_requests(prs)
        await store.insert_git_pull_request_reviews(reviews)
        await store.insert_ci_pipeline_runs(pipeline_runs)
        await store.insert_deployments(deployments)
        await store.insert_incidents(incidents)

    insert_duration = time.time() - start_time
    logging.info(f"Data insertion took {insert_duration:.2f}s")

    # 3. Compute Metrics
    start_time = time.time()

    sink = SQLiteMetricsSink(_normalize_sqlite_url(db_url))
    sink.ensure_tables()

    # Prepare data structures for computation (simulate loading from DB)
    commit_by_hash = {c.hash: c for c in commits}
    commit_stat_rows = []
    for stat in stats:
        commit = commit_by_hash.get(stat.commit_hash)
        if not commit:
            continue
        commit_stat_rows.append(
            {
                "repo_id": stat.repo_id,
                "commit_hash": stat.commit_hash,
                "author_email": commit.author_email,
                "author_name": commit.author_name,
                "committer_when": commit.committer_when,
                "file_path": stat.file_path,
                "additions": stat.additions,
                "deletions": stat.deletions,
            }
        )

    pull_request_rows = [
        {
            "repo_id": pr.repo_id,
            "number": pr.number,
            "author_email": pr.author_email,
            "author_name": pr.author_name,
            "created_at": pr.created_at,
            "merged_at": pr.merged_at,
            "first_review_at": pr.first_review_at,
            "first_comment_at": pr.first_comment_at,
            "reviews_count": pr.reviews_count,
            "comments_count": pr.comments_count,
            "changes_requested_count": pr.changes_requested_count,
            "additions": pr.additions,
            "deletions": pr.deletions,
            "changed_files": pr.changed_files,
        }
        for pr in prs
    ]

    review_rows = [
        {
            "repo_id": r.repo_id,
            "number": r.number,
            "reviewer": r.reviewer,
            "submitted_at": r.submitted_at,
            "state": r.state,
        }
        for r in reviews
    ]

    _pipeline_rows = [  # noqa: F841
        {
            "repo_id": r.repo_id,
            "run_id": r.run_id,
            "status": r.status,
            "queued_at": r.queued_at,
            "started_at": r.started_at,
            "finished_at": r.finished_at,
        }
        for r in pipeline_runs
    ]

    _deployment_rows = [  # noqa: F841
        {
            "repo_id": d.repo_id,
            "deployment_id": d.deployment_id,
            "status": d.status,
            "environment": d.environment,
            "started_at": d.started_at,
            "finished_at": d.finished_at,
            "deployed_at": d.deployed_at,
            "merged_at": d.merged_at,
            "pull_request_number": d.pull_request_number,
        }
        for d in deployments
    ]

    _incident_rows = [  # noqa: F841
        {
            "repo_id": i.repo_id,
            "incident_id": i.incident_id,
            "status": i.status,
            "started_at": i.started_at,
            "resolved_at": i.resolved_at,
        }
        for i in incidents
    ]

    # Setup Team Resolver
    member_to_team = {}
    for idx, (name, email) in enumerate(generator.authors):
        team_id = "alpha" if idx < len(generator.authors) // 2 else "beta"
        team_name = "Alpha Team" if team_id == "alpha" else "Beta Team"
        member_to_team[str(email).strip().lower()] = (team_id, team_name)
    team_resolver = TeamResolver(member_to_team=member_to_team)
    team_map = {k: v[0] for k, v in member_to_team.items()}

    computed_at = datetime.now(timezone.utc)
    end_day = computed_at.date()
    start_day = end_day - timedelta(days=DAYS - 1)

    processed_days = 0

    for i in range(DAYS):
        day = start_day + timedelta(days=i)
        start_dt = datetime.combine(day, dt_time.min, tzinfo=timezone.utc)
        end_dt = start_dt + timedelta(days=1)

        # Filter data for this day (Simulation of DB query)
        day_commits = [
            c for c in commit_stat_rows if start_dt <= c["committer_when"] < end_dt
        ]
        window_start = datetime.combine(
            day - timedelta(days=29), dt_time.min, tzinfo=timezone.utc
        )
        window_commits = [
            c for c in commit_stat_rows if window_start <= c["committer_when"] < end_dt
        ]

        # Compute Helpers
        rework_ratio = {
            repo.id: compute_rework_churn_ratio(
                repo_id=str(repo.id), window_stats=window_commits
            )
        }
        single_owner = {
            repo.id: compute_single_owner_file_ratio(
                repo_id=str(repo.id), window_stats=window_commits
            )
        }
        mttr_by_repo = {}  # Simplified for benchmark

        # Core Metrics
        result = compute_daily_metrics(
            day=day,
            commit_stat_rows=day_commits,
            pull_request_rows=pull_request_rows,
            pull_request_review_rows=review_rows,
            computed_at=computed_at,
            include_commit_metrics=True,
            team_resolver=team_resolver,
            mttr_by_repo=mttr_by_repo,
            rework_churn_ratio_by_repo=rework_ratio,
            single_owner_file_ratio_by_repo=single_owner,
        )

        # Team Metrics
        team_metrics = compute_team_wellbeing_metrics_daily(
            day=day,
            commit_stat_rows=day_commits,
            team_resolver=team_resolver,
            computed_at=computed_at,
        )

        # Work Items
        wi_metrics, wi_user_metrics, cycle_times = compute_work_item_metrics_daily(
            day=day,
            work_items=work_items,
            transitions=transitions,
            computed_at=computed_at,
            team_resolver=team_resolver,
        )

        # IC Metrics
        ic_metrics = compute_ic_metrics_daily(
            git_metrics=result.user_metrics,
            wi_metrics=wi_user_metrics,
            team_map=team_map,
        )
        result.user_metrics[:] = ic_metrics

        # Write to Sink (Mocked or Real SQLite)
        sink.write_repo_metrics(result.repo_metrics)
        sink.write_user_metrics(result.user_metrics)
        sink.write_commit_metrics(result.commit_metrics)
        sink.write_team_metrics(team_metrics)
        sink.write_work_item_metrics(wi_metrics)

        processed_days += 1

    compute_duration = time.time() - start_time
    logging.info(
        f"Metrics computation took {compute_duration:.2f}s for {processed_days} days"
    )

    # Assertions
    assert processed_days == DAYS
    assert compute_duration < 60.0, (
        f"Computation took too long: {compute_duration:.2f}s"
    )

    # Check that data was written
    with sink.engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(1) FROM repo_metrics_daily")).scalar()
    assert count and count > 0
