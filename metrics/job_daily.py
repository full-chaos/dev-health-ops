from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent

from metrics.compute import compute_daily_metrics
from metrics.compute_cicd import compute_cicd_metrics_daily
from metrics.compute_deployments import compute_deploy_metrics_daily
from metrics.compute_incidents import compute_incident_metrics_daily
from metrics.compute_wellbeing import compute_team_wellbeing_metrics_daily
from metrics.compute_work_items import compute_work_item_metrics_daily
from metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from metrics.compute_ic import (
    compute_ic_metrics_daily,
    compute_ic_landscape_rolling,
)
from metrics.identity import load_team_map
from metrics.hotspots import compute_file_hotspots, compute_file_risk_hotspots
from metrics.knowledge import compute_bus_factor, compute_code_ownership_gini
from metrics.quality import compute_rework_churn_ratio, compute_single_owner_file_ratio
from metrics.reviews import compute_review_edges_daily
from metrics.schemas import (
    CommitStatRow,
    PullRequestRow,
    PullRequestReviewRow,
    PipelineRunRow,
    DeploymentRow,
    IncidentRow,
    FileComplexitySnapshot,
    FileHotspotDaily,
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
)
from analytics.complexity import FileComplexity
from analytics.investment import InvestmentClassifier
from analytics.issue_types import IssueTypeNormalizer
from metrics.work_items import (
    DiscoveredRepo,
    fetch_github_project_v2_items,
    fetch_github_work_items,
    fetch_gitlab_work_items,
    fetch_jira_work_items,
    parse_github_projects_v2_env,
)
from metrics.sinks.clickhouse import ClickHouseMetricsSink
from metrics.sinks.mongo import MongoMetricsSink
from metrics.sinks.postgres import PostgresMetricsSink
from metrics.sinks.sqlite import SQLiteMetricsSink
from providers.identity import load_identity_resolver
from providers.status_mapping import load_status_mapping
from providers.teams import load_team_resolver
from storage import create_store, detect_db_type

logger = logging.getLogger(__name__)


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _date_range(end_day: date, backfill_days: int) -> List[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def run_daily_metrics_job(
    *,
    db_url: Optional[str] = None,
    day: date,
    backfill_days: int,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
    include_commit_metrics: bool = True,
    sink: str = "auto",  # auto|clickhouse|mongo|sqlite|both
    provider: str = "none",  # all|jira|github|gitlab|none
) -> None:
    """
    Compute and persist daily metrics into ClickHouse/MongoDB/Postgres (and SQLite for dev).

    Source data:
    - git facts are read from the backend pointed to by `db_url`:
      - ClickHouse: `git_commits`, `git_commit_stats`, `git_pull_requests`
      - MongoDB: `git_commits`, `git_commit_stats`, `git_pull_requests`
      - SQLite: `git_commits`, `git_commit_stats`, `git_pull_requests`

    Derived metrics are written back into the same backend:
    - `repo_metrics_daily`, `user_metrics_daily`, `commit_metrics`
    - `team_metrics_daily`
    - `work_item_metrics_daily`, `work_item_user_metrics_daily`, `work_item_cycle_times`

    When `sink='both'`, metrics are written to the backend given by `db_url` and
    also to a secondary sink configured via `SECONDARY_DATABASE_URI`.
    """
    db_url = db_url or os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    backend = detect_db_type(db_url)
    if backend not in {"clickhouse", "mongo", "sqlite", "postgres"}:
        raise ValueError(
            f"Unsupported db backend for daily metrics: {backend}. "
            f"Use a ClickHouse, MongoDB, SQLite, or Postgres connection URI."
        )

    sink = (sink or "auto").strip().lower()
    if sink == "auto":
        sink = backend

    if sink not in {"clickhouse", "mongo", "sqlite", "postgres", "both"}:
        raise ValueError(
            "sink must be one of: auto, clickhouse, mongo, sqlite, postgres, both"
        )
    if sink != "both" and sink != backend:
        raise ValueError(
            f"sink='{sink}' requires db backend '{sink}', got '{backend}'. "
            "For cross-backend writes use sink='both'."
        )
    if sink == "both" and backend not in {"clickhouse", "mongo"}:
        raise ValueError(
            "sink='both' is only supported when source backend is clickhouse or mongo"
        )

    days = _date_range(day, backfill_days)
    computed_at = datetime.now(timezone.utc)

    status_mapping = load_status_mapping()
    identity = load_identity_resolver()
    team_resolver = load_team_resolver()

    # Load classifiers
    investment_classifier = InvestmentClassifier(
        REPO_ROOT / "config/investment_areas.yaml"
    )
    issue_type_normalizer = IssueTypeNormalizer(
        REPO_ROOT / "config/issue_type_mapping.yaml"
    )

    logger.info(
        "Daily metrics job: backend=%s sink=%s day=%s backfill=%d repo_id=%s provider=%s",
        backend,
        sink,
        day.isoformat(),
        backfill_days,
        str(repo_id) if repo_id else "",
        provider,
    )

    # Optional work tracking fetch (provider APIs).
    provider = (provider or "all").strip().lower()
    provider_set = set()
    provider_strict = provider in {"jira", "github", "gitlab"}
    if provider in {"all", "*"}:
        provider_set = {"jira", "github", "gitlab", "synthetic"}
    elif provider in {"none", "off", "skip"}:
        provider_set = set()
    else:
        provider_set = {provider}
    unknown_providers = provider_set - {"jira", "github", "gitlab", "synthetic"}
    if unknown_providers:
        raise ValueError(f"Unknown provider(s): {sorted(unknown_providers)}")

    # Primary sink is always the same as `backend` unless sink='both'.
    primary_sink: Any
    secondary_sink: Optional[Any] = None

    if backend == "clickhouse":
        primary_sink = ClickHouseMetricsSink(db_url)
        if sink == "both":
            secondary_sink = MongoMetricsSink(_secondary_uri_from_env())
    elif backend == "mongo":
        primary_sink = MongoMetricsSink(db_url)
        if sink == "both":
            secondary_sink = ClickHouseMetricsSink(_secondary_uri_from_env())
    elif backend == "postgres":
        primary_sink = PostgresMetricsSink(db_url)
    else:
        primary_sink = SQLiteMetricsSink(_normalize_sqlite_url(db_url))

    sinks: List[Any] = [primary_sink] + (
        [secondary_sink] if secondary_sink is not None else []
    )

    try:
        for s in sinks:
            if isinstance(s, ClickHouseMetricsSink):
                logger.info("Ensuring ClickHouse tables/migrations")
                s.ensure_tables()
            elif isinstance(s, MongoMetricsSink):
                logger.info("Ensuring Mongo indexes")
                s.ensure_indexes()
            elif isinstance(s, PostgresMetricsSink):
                logger.info("Ensuring Postgres tables")
                s.ensure_tables()
            elif isinstance(s, SQLiteMetricsSink):
                logger.info("Ensuring SQLite tables")
                s.ensure_tables()

        work_items: List[Any] = []
        work_item_transitions: List[Any] = []
        if provider_set:
            since_dt = datetime.combine(min(days), time.min, tzinfo=timezone.utc)
            logger.info(
                "Fetching work items since %s (providers=%s)",
                since_dt.isoformat(),
                sorted(provider_set),
            )
            discovered_repos = _discover_repos(
                backend=backend,
                primary_sink=primary_sink,
                repo_id=repo_id,
                repo_name=repo_name,
            )
            logger.info(
                "Discovered %d repos from backend for work item ingestion",
                len(discovered_repos),
            )

            # Inject a default synthetic repo if 'synthetic' is requested but no synthetic repo exists
            if "synthetic" in provider_set and not any(
                r.source == "synthetic" for r in discovered_repos
            ):
                logger.info(
                    "Injecting default 'synthetic/demo-repo' for synthetic data generation"
                )
                discovered_repos.append(
                    DiscoveredRepo(
                        repo_id=uuid.uuid4(),
                        full_name="synthetic/demo-repo",
                        source="synthetic",
                        settings={},
                    )
                )

            if "jira" in provider_set:
                try:
                    jira_items, jira_transitions = fetch_jira_work_items(
                        since=since_dt,
                        until=datetime.combine(
                            max(days), time.max, tzinfo=timezone.utc
                        ),
                        status_mapping=status_mapping,
                        identity=identity,
                    )
                    work_items.extend(jira_items)
                    work_item_transitions.extend(jira_transitions)
                except Exception as exc:
                    if provider_strict and provider == "jira":
                        raise
                    logger.warning("Skipping Jira work items fetch: %s", exc)

            if "github" in provider_set:
                try:
                    wi, transitions = fetch_github_work_items(
                        repos=discovered_repos,
                        since=since_dt,
                        status_mapping=status_mapping,
                        identity=identity,
                        include_issue_events=True,
                    )
                    work_items.extend(wi)
                    work_item_transitions.extend(transitions)
                except Exception as exc:
                    if provider_strict and provider == "github":
                        raise
                    logger.warning("Skipping GitHub work items fetch: %s", exc)

                try:
                    projects = parse_github_projects_v2_env()
                    if projects:
                        project_items, _project_transitions = (
                            fetch_github_project_v2_items(
                                projects=projects,
                                status_mapping=status_mapping,
                                identity=identity,
                            )
                        )
                        # Project item status can override plain issue status.
                        by_id = {w.work_item_id: w for w in work_items}
                        for w in project_items:
                            by_id[w.work_item_id] = w
                        work_items = list(by_id.values())
                except Exception as exc:
                    logger.warning("Skipping GitHub Projects v2 fetch: %s", exc)

            if "gitlab" in provider_set:
                try:
                    gitlab_items, gitlab_transitions = fetch_gitlab_work_items(
                        repos=discovered_repos,
                        since=since_dt,
                        status_mapping=status_mapping,
                        identity=identity,
                        include_label_events=True,
                    )
                    work_items.extend(gitlab_items)
                    work_item_transitions.extend(gitlab_transitions)
                except Exception as exc:
                    if provider_strict and provider == "gitlab":
                        raise
                    logger.warning("Skipping GitLab work items fetch: %s", exc)

            if "synthetic" in provider_set:
                try:
                    from metrics.work_items import fetch_synthetic_work_items

                    syn_items, syn_transitions = fetch_synthetic_work_items(
                        repos=discovered_repos,
                        days=backfill_days + 1,
                    )
                    work_items.extend(syn_items)
                    work_item_transitions.extend(syn_transitions)
                except Exception as exc:
                    logger.warning("Skipping synthetic work items fetch: %s", exc)

            logger.info("Work items ready for compute: %d", len(work_items))
            logger.info(
                "Work item transitions ready for compute: %d",
                len(work_item_transitions),
            )

        business_tz = os.getenv("BUSINESS_TIMEZONE", "UTC")
        business_start = int(os.getenv("BUSINESS_HOURS_START", "9"))
        business_end = int(os.getenv("BUSINESS_HOURS_END", "17"))

        for d in days:
            logger.info("Computing metrics for day=%s", d.isoformat())
            start, end = _utc_day_window(d)
            if backend == "clickhouse":
                commit_rows, pr_rows = _load_clickhouse_rows(
                    primary_sink.client,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                review_rows = _load_clickhouse_reviews(
                    primary_sink.client,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                pipeline_rows = _load_clickhouse_pipeline_runs(
                    primary_sink.client,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                deployment_rows = _load_clickhouse_deployments(
                    primary_sink.client,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                incident_rows = _load_clickhouse_incidents(
                    primary_sink.client,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
            elif backend in {"sqlite", "postgres"}:
                commit_rows, pr_rows = _load_sqlite_rows(
                    primary_sink.engine,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                review_rows = _load_sqlite_reviews(
                    primary_sink.engine,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                pipeline_rows = _load_sqlite_pipeline_runs(
                    primary_sink.engine,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                deployment_rows = _load_sqlite_deployments(
                    primary_sink.engine,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                incident_rows = _load_sqlite_incidents(
                    primary_sink.engine,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
            else:
                commit_rows, pr_rows = _load_mongo_rows(
                    primary_sink.db,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                review_rows = _load_mongo_reviews(
                    primary_sink.db,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                pipeline_rows = _load_mongo_pipeline_runs(
                    primary_sink.db,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                deployment_rows = _load_mongo_deployments(
                    primary_sink.db,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
                incident_rows = _load_mongo_incidents(
                    primary_sink.db,
                    start=start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
            logger.info(
                "Loaded source facts: commits=%d pr_rows=%d",
                len(commit_rows),
                len(pr_rows),
            )

            # --- MTTR (proxied by Bug Cycle Time) ---
            mttr_by_repo: Dict[uuid.UUID, float] = {}
            bug_times: Dict[uuid.UUID, List[float]] = {}
            for item in work_items:
                if item.type == "bug" and item.completed_at and item.started_at:
                    start_dt = _to_utc(item.started_at)
                    comp_dt = _to_utc(item.completed_at)
                    if start_dt < end and comp_dt >= start:
                        # Find the repo_id. WorkItem from models/work_items.py has repo_id.
                        r_id = getattr(item, "repo_id", None)
                        if r_id:
                            hours = (comp_dt - start_dt).total_seconds() / 3600.0
                            bug_times.setdefault(r_id, []).append(hours)

            for r_id, times in bug_times.items():
                mttr_by_repo[r_id] = sum(times) / len(times)

            # --- Hotspots (30-day window) ---
            window_days = 30
            h_start = datetime.combine(
                d - timedelta(days=window_days - 1), time.min, tzinfo=timezone.utc
            )
            if backend == "clickhouse":
                h_commit_rows, _ = _load_clickhouse_rows(
                    primary_sink.client,
                    start=h_start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
            elif backend in {"sqlite", "postgres"}:
                h_commit_rows, _ = _load_sqlite_rows(
                    primary_sink.engine,
                    start=h_start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )
            else:
                h_commit_rows, _ = _load_mongo_rows(
                    primary_sink.db,
                    start=h_start,
                    end=end,
                    repo_id=repo_id,
                    repo_name=repo_name,
                )

            # Discover repos for this day if not already done.
            # (In reality, we should iterate over each repo separately or group them).
            active_repos: Set[uuid.UUID] = {r["repo_id"] for r in commit_rows}
            rework_ratio_by_repo: Dict[uuid.UUID, float] = {}
            single_owner_ratio_by_repo: Dict[uuid.UUID, float] = {}
            bus_factor_by_repo: Dict[uuid.UUID, int] = {}
            gini_by_repo: Dict[uuid.UUID, float] = {}
            all_file_metrics = []
            for r_id in sorted(active_repos, key=str):
                all_file_metrics.extend(
                    compute_file_hotspots(
                        repo_id=r_id,
                        day=d,
                        window_stats=h_commit_rows,
                        computed_at=computed_at,
                    )
                )
                rework_ratio_by_repo[r_id] = compute_rework_churn_ratio(
                    repo_id=str(r_id),
                    window_stats=h_commit_rows,
                )
                single_owner_ratio_by_repo[r_id] = compute_single_owner_file_ratio(
                    repo_id=str(r_id),
                    window_stats=h_commit_rows,
                )
                bus_factor_by_repo[r_id] = compute_bus_factor(
                    repo_id=str(r_id),
                    window_stats=h_commit_rows,
                )
                gini_by_repo[r_id] = compute_code_ownership_gini(
                    repo_id=str(r_id),
                    window_stats=h_commit_rows,
                )

            result = compute_daily_metrics(
                day=d,
                commit_stat_rows=commit_rows,
                pull_request_rows=pr_rows,
                pull_request_review_rows=review_rows,
                computed_at=computed_at,
                include_commit_metrics=include_commit_metrics,
                team_resolver=team_resolver,
                identity_resolver=identity,
                mttr_by_repo=mttr_by_repo,
                rework_churn_ratio_by_repo=rework_ratio_by_repo,
                single_owner_file_ratio_by_repo=single_owner_ratio_by_repo,
                bus_factor_by_repo=bus_factor_by_repo,
                code_ownership_gini_by_repo=gini_by_repo,
            )

            team_metrics = compute_team_wellbeing_metrics_daily(
                day=d,
                commit_stat_rows=commit_rows,
                team_resolver=team_resolver,
                computed_at=computed_at,
                business_timezone=business_tz,
                business_hours_start=business_start,
                business_hours_end=business_end,
            )

            wi_metrics = []
            wi_user_metrics = []
            wi_cycle_times = []
            wi_state_durations = []
            if work_items:
                wi_metrics, wi_user_metrics, wi_cycle_times = (
                    compute_work_item_metrics_daily(
                        day=d,
                        work_items=work_items,
                        transitions=work_item_transitions,
                        computed_at=computed_at,
                        team_resolver=team_resolver,
                    )
                )
                wi_state_durations = compute_work_item_state_durations_daily(
                    day=d,
                    work_items=work_items,
                    transitions=work_item_transitions,
                    computed_at=computed_at,
                    team_resolver=team_resolver,
                )
            elif not provider_set:
                # Work items are expected to be synced separately via `sync work-items`.
                # We only need user-level aggregates here to enrich IC metrics.
                try:
                    wi_user_metrics = _load_work_item_user_metrics_daily(
                        db_url=db_url, day=d
                    )
                except Exception as exc:
                    logger.debug(
                        "Failed to load work_item_user_metrics_daily for %s: %s",
                        d.isoformat(),
                        exc,
                    )
            review_edges = compute_review_edges_daily(
                day=d,
                pull_request_rows=pr_rows,
                pull_request_review_rows=review_rows,
                computed_at=computed_at,
            )
            cicd_metrics = compute_cicd_metrics_daily(
                day=d,
                pipeline_runs=pipeline_rows,
                computed_at=computed_at,
            )
            deploy_metrics = compute_deploy_metrics_daily(
                day=d,
                deployments=deployment_rows,
                computed_at=computed_at,
            )
            incident_metrics = compute_incident_metrics_daily(
                day=d,
                incidents=incident_rows,
                computed_at=computed_at,
            )

            # --- Complexity & Risk Hotspots ---
            risk_hotspots: List[FileHotspotDaily] = []
            complexity_by_repo = _load_complexity_snapshots(
                db_url=db_url,
                as_of_day=d,
                repo_id=repo_id,
                repo_name=repo_name,
            )
            hotspot_repos: Set[uuid.UUID] = {r["repo_id"] for r in h_commit_rows}
            hotspot_repos |= set(complexity_by_repo.keys())
            blame_by_repo = _load_blame_concentration(
                backend=backend,
                primary_sink=primary_sink,
                repo_ids=hotspot_repos,
                repo_id=repo_id,
                repo_name=repo_name,
            )

            for r_id in sorted(hotspot_repos, key=str):
                risk_hotspots.extend(
                    compute_file_risk_hotspots(
                        repo_id=r_id,
                        day=d,
                        window_stats=h_commit_rows,
                        complexity_map=complexity_by_repo.get(r_id) or {},
                        blame_map=blame_by_repo.get(r_id) or {},
                        computed_at=computed_at,
                    )
                )

            # --- Issue Type Metrics ---
            issue_type_stats: Dict[Tuple[uuid.UUID, str, str, str], Dict[str, int]] = {}
            # Key: (repo_id, provider, team_id, issue_type_norm)
            # Value: {created, completed, active}

            # Helper to resolve team
            def _get_team(wi) -> str:
                # Use team resolver logic or fallback
                # We can reuse the team_id from computed work_item_metrics if we had it mapped,
                # but let's re-resolve quickly or use 'unassigned'
                if wi.assignees:
                    t_id, _ = team_resolver.resolve(wi.assignees[0])
                    if t_id:
                        return t_id
                return "unassigned"

            def _normalize_investment_team_id(team_id: Optional[str]) -> Optional[str]:
                if not team_id or team_id == "unassigned":
                    return None
                return team_id

            start_dt = _to_utc(start)
            end_dt = _to_utc(end)

            for item in work_items:
                r_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
                prov = item.provider
                team_id = _get_team(item)

                # Normalize type
                norm_type = issue_type_normalizer.normalize(
                    prov, item.type, getattr(item, "labels", [])
                )

                key = (r_id, prov, team_id, norm_type)
                if key not in issue_type_stats:
                    issue_type_stats[key] = {
                        "created": 0,
                        "completed": 0,
                        "active": 0,
                        "cycles": [],
                    }

                stats = issue_type_stats[key]

                created = _to_utc(item.created_at)
                if start_dt <= created < end_dt:
                    stats["created"] += 1

                if item.completed_at:
                    completed = _to_utc(item.completed_at)
                    if start_dt <= completed < end_dt:
                        stats["completed"] += 1
                        # Cycle time
                        if item.started_at:
                            started = _to_utc(item.started_at)
                            hours = (completed - started).total_seconds() / 3600.0
                            if hours >= 0:
                                stats.setdefault("cycles", []).append(hours)

                # Active (WIP) at end of day
                is_created = created < end_dt
                is_not_completed = (
                    not item.completed_at or _to_utc(item.completed_at) >= end_dt
                )
                if is_created and is_not_completed:
                    stats["active"] += 1

            issue_type_metrics_rows = []
            for (r_id, prov, team_id, norm_type), stat in issue_type_stats.items():
                cycles = sorted(stat.get("cycles", []))
                p50 = cycles[len(cycles) // 2] if cycles else 0.0
                p90 = cycles[int(len(cycles) * 0.9)] if cycles else 0.0

                # We skip lead time for brevity or compute similar to cycle

                issue_type_metrics_rows.append(
                    IssueTypeMetricsRecord(
                        repo_id=r_id if r_id.int != 0 else None,
                        day=d,
                        provider=prov,
                        team_id=team_id,
                        issue_type_norm=norm_type,
                        created_count=stat["created"],
                        completed_count=stat["completed"],
                        active_count=stat["active"],
                        cycle_p50_hours=p50,
                        cycle_p90_hours=p90,
                        lead_p50_hours=0.0,  # Placeholder
                        computed_at=computed_at,
                    )
                )

            # --- Investment Classifications & Metrics ---
            investment_classifications = []
            inv_metrics_map: Dict[
                Tuple[uuid.UUID, str, str, str], Dict[str, float]
            ] = {}
            # Key: (repo_id, team_id, area, stream)

            # 1. Classify Work Items
            for item in work_items:
                # Check if item relevant for this day (completed or active)
                # We classify ALL items that are active or acted upon
                # For simplicity, let's classify items completed today for the metrics

                r_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)

                # Check timestamps to see if we should emit a classification record
                # We emit classification for items created or updated today?
                # The requirement says "Investment areas (portfolio classification) ... For each artifact"
                # We probably want to store classification for items ACTIVE today.

                # If item existed today
                created = _to_utc(item.created_at)
                if created < end_dt and (
                    not item.completed_at or _to_utc(item.completed_at) >= start_dt
                ):
                    cls = investment_classifier.classify({
                        "labels": getattr(item, "labels", []),
                        "component": getattr(item, "component", ""),
                        "title": item.title,
                        "provider": item.provider,
                    })

                    investment_classifications.append(
                        InvestmentClassificationRecord(
                            repo_id=r_id if r_id.int != 0 else None,
                            day=d,
                            artifact_type="work_item",
                            artifact_id=item.work_item_id,
                            provider=item.provider,
                            investment_area=cls.investment_area,
                            project_stream=cls.project_stream or "",
                            confidence=cls.confidence,
                            rule_id=cls.rule_id,
                            computed_at=computed_at,
                        )
                    )

                    # Metrics Aggregation (only for completed items)
                    if item.completed_at:
                        completed = _to_utc(item.completed_at)
                        if start_dt <= completed < end_dt:
                            team_id = _normalize_investment_team_id(_get_team(item))
                            key = (
                                r_id,
                                team_id,
                                cls.investment_area,
                                cls.project_stream or "",
                            )
                            if key not in inv_metrics_map:
                                inv_metrics_map[key] = {
                                    "units": 0,
                                    "completed": 0,
                                    "churn": 0,
                                    "cycles": [],
                                }

                            inv_metrics_map[key]["completed"] += 1
                            # Units = story points or 1
                            points = getattr(item, "story_points", 1) or 1
                            inv_metrics_map[key]["units"] += int(points)

                            if item.started_at:
                                s = _to_utc(item.started_at)
                                h = (completed - s).total_seconds() / 3600.0
                                if h >= 0:
                                    inv_metrics_map[key]["cycles"].append(h)

            # 2. Classify PRs (merged today)
            for pr in pr_rows:
                if pr["merged_at"] and start <= _to_utc(pr["merged_at"]) < end:
                    # We need PR labels or files to classify accurately.
                    # We only have schemas.PullRequestRow which is limited.
                    # We assume we might have extended info or fetch it?
                    # For now, default to "product" or use repo-based heuristic if possible.
                    # Or we skip PR classification if we don't have enough data in Row.
                    pass

            # 3. Classify Commits (churn)
            # We have commit_rows with file_path.
            for c in commit_rows:
                r_id = c["repo_id"]
                path = c["file_path"]
                if not path:
                    continue

                cls = investment_classifier.classify({
                    "paths": [path],
                    "labels": [],  # No labels for commits usually
                    "component": "",
                })

                # We don't emit a classification record per commit (too many), but we aggregate metrics
                # We need author team
                author_email = c["author_email"] or ""
                t_id, _ = team_resolver.resolve(author_email)
                team_id = _normalize_investment_team_id(t_id)

                key = (r_id, team_id, cls.investment_area, cls.project_stream or "")
                if key not in inv_metrics_map:
                    inv_metrics_map[key] = {
                        "units": 0,
                        "completed": 0,
                        "churn": 0,
                        "cycles": [],
                    }

                inv_metrics_map[key]["churn"] += c["additions"] + c["deletions"]

            investment_metrics_rows = []
            for (r_id, team_id, area, stream), data in inv_metrics_map.items():
                cycles = sorted(data["cycles"])
                p50 = cycles[len(cycles) // 2] if cycles else 0.0

                investment_metrics_rows.append(
                    InvestmentMetricsRecord(
                        repo_id=r_id if r_id.int != 0 else None,
                        day=d,
                        team_id=team_id,
                        investment_area=area,
                        project_stream=stream,
                        delivery_units=data["units"],
                        work_items_completed=data["completed"],
                        prs_merged=0,  # Need PR classification logic
                        churn_loc=data["churn"],
                        cycle_p50_hours=p50,
                        computed_at=computed_at,
                    )
                )

            # --- IC Metrics & Landscape ---
            team_map = load_team_map()
            ic_metrics = compute_ic_metrics_daily(
                git_metrics=result.user_metrics,
                wi_metrics=wi_user_metrics,
                team_map=team_map,
            )
            # Replace basic user metrics with extended IC metrics
            result.user_metrics[:] = ic_metrics

            logger.info(
                "Computed derived metrics: repo=%d user=%d commit=%d team=%d wi=%d wi_user=%d wi_facts=%d",
                len(result.repo_metrics),
                len(result.user_metrics),
                len(result.commit_metrics),
                len(team_metrics),
                len(wi_metrics),
                len(wi_user_metrics),
                len(wi_cycle_times),
            )
            logger.info("Computed time-in-state rows: %d", len(wi_state_durations))

            for s in sinks:
                logger.debug("Writing derived metrics to sink=%s", type(s).__name__)
                s.write_repo_metrics(result.repo_metrics)
                s.write_user_metrics(result.user_metrics)  # Writes extended metrics
                s.write_commit_metrics(result.commit_metrics)
                s.write_file_metrics(all_file_metrics)
                s.write_team_metrics(team_metrics)
                if wi_metrics:
                    s.write_work_item_metrics(wi_metrics)
                if wi_user_metrics and work_items:
                    # When provider fetch is disabled, we read WI user metrics from DB for IC enrichment,
                    # but we do not rewrite the derived WI tables.
                    s.write_work_item_user_metrics(wi_user_metrics)
                if wi_cycle_times:
                    s.write_work_item_cycle_times(wi_cycle_times)
                if wi_state_durations:
                    s.write_work_item_state_durations(wi_state_durations)
                s.write_review_edges(review_edges)
                s.write_cicd_metrics(cicd_metrics)
                s.write_deploy_metrics(deploy_metrics)
                s.write_incident_metrics(incident_metrics)

                # New metrics writes
                if hasattr(s, "write_file_hotspot_daily") and risk_hotspots:
                    s.write_file_hotspot_daily(risk_hotspots)

                if hasattr(s, "write_issue_type_metrics") and issue_type_metrics_rows:
                    s.write_issue_type_metrics(issue_type_metrics_rows)

                if (
                    hasattr(s, "write_investment_classifications")
                    and investment_classifications
                ):
                    s.write_investment_classifications(investment_classifications)

                if hasattr(s, "write_investment_metrics") and investment_metrics_rows:
                    s.write_investment_metrics(investment_metrics_rows)

                # Landscape rolling metrics
                try:
                    rolling_stats = s.get_rolling_30d_user_stats(
                        as_of_day=d, repo_id=repo_id
                    )
                    landscape_recs = compute_ic_landscape_rolling(
                        as_of_day=d,
                        rolling_stats=rolling_stats,
                        team_map=team_map,
                    )
                    s.write_ic_landscape_rolling(landscape_recs)
                    logger.info(
                        "Computed and wrote %d landscape records", len(landscape_recs)
                    )
                except Exception as e:
                    logger.warning("Failed to compute/write landscape metrics: %s", e)

    finally:
        for s in sinks:
            try:
                s.close()
            except Exception:
                logger.exception("Error closing sink %s", type(s).__name__)


def _secondary_uri_from_env() -> str:
    """Get the secondary database URI for sink='both' mode."""
    uri = os.getenv("SECONDARY_DATABASE_URI") or ""
    if not uri:
        raise ValueError("SECONDARY_DATABASE_URI is required for sink='both'")
    return uri


def _safe_json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return None


def _discover_repos(
    *,
    backend: str,
    primary_sink: Any,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[DiscoveredRepo]:
    """
    Discover repos from the synced backend so work item ingestion can reuse them as sources.

    Returns DiscoveredRepo rows with `.source` set from repo.settings["source"] when present.
    """
    repos: List[DiscoveredRepo] = []

    if backend == "clickhouse":
        client = primary_sink.client  # type: ignore[attr-defined]
        params: Dict[str, Any] = {}
        where = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            where = "WHERE id = {repo_id:UUID}"
        elif repo_name is not None:
            params["repo_name"] = repo_name
            where = "WHERE repo = {repo_name:String}"
        rows = _clickhouse_query_dicts(
            client,
            f"SELECT id, repo, settings, tags FROM repos {where}",
            params,
        )
        for row in rows:
            repo_uuid = _parse_uuid(row.get("id"))
            full_name = row.get("repo")
            if repo_uuid is None or not full_name:
                continue
            settings = _safe_json_loads(row.get("settings")) or {}
            tags = _safe_json_loads(row.get("tags")) or []
            source = str((settings or {}).get("source") or "").strip().lower()
            if not source and isinstance(tags, list):
                for t in tags:
                    if str(t).strip().lower() in {"github", "gitlab"}:
                        source = str(t).strip().lower()
                        break
            repos.append(
                DiscoveredRepo(
                    repo_id=repo_uuid,
                    full_name=str(full_name),
                    source=source or "unknown",
                    settings=settings if isinstance(settings, dict) else {},
                )
            )
        return repos

    if backend == "mongo":
        db = primary_sink.db  # type: ignore[attr-defined]
        query: Dict[str, Any] = {}
        if repo_id is not None:
            query["_id"] = str(repo_id)
        projection = {"_id": 1, "repo": 1, "settings": 1, "tags": 1}
        docs = list(db["repos"].find(query, projection))
        for doc in docs:
            repo_uuid = _parse_uuid(doc.get("_id") or doc.get("id"))
            full_name = doc.get("repo")
            if repo_uuid is None or not full_name:
                continue
            settings = (
                doc.get("settings") if isinstance(doc.get("settings"), dict) else {}
            )
            tags = doc.get("tags") if isinstance(doc.get("tags"), list) else []
            source = str((settings or {}).get("source") or "").strip().lower()
            if not source:
                for t in tags:
                    if str(t).strip().lower() in {"github", "gitlab"}:
                        source = str(t).strip().lower()
                        break
            repos.append(
                DiscoveredRepo(
                    repo_id=repo_uuid,
                    full_name=str(full_name),
                    source=source or "unknown",
                    settings=settings,
                )
            )
        return sorted(repos, key=lambda r: r.full_name)

    # sqlite
    from sqlalchemy.orm import Session

    from models.git import Repo

    engine = primary_sink.engine  # type: ignore[attr-defined]
    with Session(engine) as session:
        try:
            q = session.query(Repo)
            if repo_id is not None:
                q = q.filter(Repo.id == repo_id)
            rows = q.all()
        except Exception as e:
            # If table doesn't exist or other error, return empty list
            # The synthetic injection logic will handle creating a fake repo if needed.
            logger.warning(
                "Failed to discover repos from SQLite (table might be missing): %s", e
            )
            rows = []

        for r in rows:
            r_settings = getattr(r, "settings", None)
            settings = r_settings if isinstance(r_settings, dict) else {}
            r_tags = getattr(r, "tags", None)
            tags = r_tags if isinstance(r_tags, list) else []
            source = str((settings or {}).get("source") or "").strip().lower()
            if not source:
                for t in tags:
                    if str(t).strip().lower() in {"github", "gitlab"}:
                        source = str(t).strip().lower()
                        break
            repos.append(
                DiscoveredRepo(
                    repo_id=getattr(r, "id"),
                    full_name=str(getattr(r, "repo")),
                    source=source or "unknown",
                    settings=settings,
                )
            )
    return repos


def _normalize_sqlite_url(db_url: str) -> str:
    """
    Normalize SQLite URLs to a sync driver URL so callers can pass either:
    - sqlite:///...
    - sqlite+aiosqlite:///...
    """
    if "sqlite+aiosqlite://" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return db_url


def _naive_utc(dt: datetime) -> datetime:
    """Convert a datetime to naive UTC (BSON/ClickHouse friendly)."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _to_utc(dt: datetime) -> datetime:
    """Ensure datetime has UTC tzinfo."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_uuid(value: Any) -> Optional[uuid.UUID]:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def _clickhouse_query_dicts(
    client: Any, query: str, parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _load_complexity_snapshots(
    *,
    db_url: str,
    as_of_day: date,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> Dict[uuid.UUID, Dict[str, FileComplexitySnapshot]]:
    async def _fetch():
        backend = detect_db_type(db_url)
        store = create_store(db_url, backend)
        async with store:
            return await store.get_complexity_snapshots(
                as_of_day=as_of_day,
                repo_id=repo_id,
                repo_name=repo_name,
            )

    try:
        snapshots = asyncio.run(_fetch())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            snapshots = loop.run_until_complete(_fetch())
        finally:
            loop.close()

    by_repo: Dict[uuid.UUID, Dict[str, FileComplexitySnapshot]] = {}
    for snap in snapshots:
        by_repo.setdefault(snap.repo_id, {})[snap.file_path] = snap
    return by_repo


def _load_blame_concentration(
    *,
    backend: str,
    primary_sink: Any,
    repo_ids: Optional[Set[uuid.UUID]] = None,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> Dict[uuid.UUID, Dict[str, float]]:
    if repo_ids is not None and not repo_ids:
        return {}

    if backend == "clickhouse":
        return _load_clickhouse_blame_concentration(
            primary_sink.client,
            repo_ids=repo_ids,
            repo_id=repo_id,
            repo_name=repo_name,
        )
    if backend in {"sqlite", "postgres"}:
        return _load_sqlite_blame_concentration(
            primary_sink.engine,
            repo_ids=repo_ids,
            repo_id=repo_id,
            repo_name=repo_name,
        )
    return _load_mongo_blame_concentration(
        primary_sink.db,
        repo_ids=repo_ids,
        repo_id=repo_id,
        repo_name=repo_name,
    )


def _load_work_item_user_metrics_daily(
    *,
    db_url: str,
    day: date,
) -> List[Any]:
    async def _fetch():
        backend = detect_db_type(db_url)
        store = create_store(db_url, backend)
        async with store:
            return await store.get_work_item_user_metrics_daily(day=day)

    try:
        return asyncio.run(_fetch())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_fetch())
        finally:
            loop.close()


def _load_clickhouse_rows(
    client: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from ClickHouse.

    Uses ClickHouse SQL to:
    - join `git_commits` with `git_commit_stats` by (repo_id, commit_hash)
    - filter commits by `committer_when` inside the day window
    - fetch PRs created/merged within the day window
    """
    params: Dict[str, Any] = {"start": _naive_utc(start), "end": _naive_utc(end)}
    repo_filter = ""
    if repo_id is not None:
        params["repo_id"] = str(repo_id)
        repo_filter = " AND c.repo_id = {repo_id:UUID}"
    elif repo_name is not None:
        params["repo_name"] = repo_name
        repo_filter = (
            " AND c.repo_id IN (SELECT id FROM repos WHERE repo = {repo_name:String})"
        )

    commit_query = f"""
    SELECT
      c.repo_id AS repo_id,
      c.hash AS commit_hash,
      c.author_email AS author_email,
      c.author_name AS author_name,
      c.committer_when AS committer_when,
      s.file_path AS file_path,
      s.additions AS additions,
      s.deletions AS deletions
    FROM git_commits AS c
    LEFT JOIN git_commit_stats AS s
      ON (s.repo_id = c.repo_id) AND (s.commit_hash = c.hash)
    WHERE c.committer_when >= {{start:DateTime}} AND c.committer_when < {{end:DateTime}}
    {repo_filter}
    """

    pr_query = f"""
    SELECT
      repo_id,
      number,
      author_email,
      author_name,
      created_at,
      merged_at,
      first_review_at,
      first_comment_at,
      changes_requested_count,
      reviews_count,
      comments_count,
      additions,
      deletions,
      changed_files
    FROM git_pull_requests
    WHERE
      (created_at >= {{start:DateTime}} AND created_at < {{end:DateTime}})
      OR (merged_at IS NOT NULL AND merged_at >= {{start:DateTime}} AND merged_at < {{end:DateTime}})
      {repo_filter.replace("c.repo_id", "repo_id") if repo_id or repo_name else ""}
    """

    commit_dicts = _clickhouse_query_dicts(client, commit_query, params)
    pr_dicts = _clickhouse_query_dicts(client, pr_query, params)

    commit_rows: List[CommitStatRow] = []
    for row in commit_dicts:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        commit_hash = row.get("commit_hash")
        committer_when = row.get("committer_when")
        if (
            repo_uuid is None
            or not commit_hash
            or not isinstance(committer_when, datetime)
        ):
            continue
        file_path = row.get("file_path") or None
        commit_rows.append({
            "repo_id": repo_uuid,
            "commit_hash": str(commit_hash),
            "author_email": row.get("author_email"),
            "author_name": row.get("author_name"),
            "committer_when": committer_when,
            "file_path": str(file_path) if file_path else None,
            "additions": int(row.get("additions") or 0),
            "deletions": int(row.get("deletions") or 0),
        })

    pr_rows: List[PullRequestRow] = []
    for row in pr_dicts:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        created_at = row.get("created_at")
        if repo_uuid is None or not isinstance(created_at, datetime):
            continue
        pr_rows.append({
            "repo_id": repo_uuid,
            "number": int(row.get("number") or 0),
            "author_email": row.get("author_email"),
            "author_name": row.get("author_name"),
            "created_at": created_at,
            "merged_at": row.get("merged_at")
            if isinstance(row.get("merged_at"), datetime)
            else None,
            "first_review_at": row.get("first_review_at")
            if isinstance(row.get("first_review_at"), datetime)
            else None,
            "first_comment_at": row.get("first_comment_at")
            if isinstance(row.get("first_comment_at"), datetime)
            else None,
            "changes_requested_count": int(row.get("changes_requested_count", 0) or 0),
            "reviews_count": int(row.get("reviews_count", 0) or 0),
            "comments_count": int(row.get("comments_count", 0) or 0),
            "additions": int(row.get("additions", 0) or 0),
            "deletions": int(row.get("deletions", 0) or 0),
            "changed_files": int(row.get("changed_files", 0) or 0),
        })

    return commit_rows, pr_rows


def _load_clickhouse_blame_concentration(
    client: Any,
    *,
    repo_ids: Optional[Set[uuid.UUID]] = None,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> Dict[uuid.UUID, Dict[str, float]]:
    params: Dict[str, Any] = {}
    where = ""
    if repo_ids:
        params["repo_ids"] = [str(r) for r in sorted(repo_ids, key=str)]
        where = "WHERE repo_id IN {repo_ids:Array(UUID)}"
    elif repo_id is not None:
        params["repo_id"] = str(repo_id)
        where = "WHERE repo_id = {repo_id:UUID}"
    elif repo_name is not None:
        params["repo_name"] = repo_name
        where = (
            "WHERE repo_id IN (SELECT id FROM repos WHERE repo = {repo_name:String})"
        )

    query = f"""
    SELECT
      repo_id,
      file_path,
      max(lines_by_author) / sum(lines_by_author) AS ownership_concentration
    FROM (
      SELECT
        repo_id,
        path AS file_path,
        author_email,
        count() AS lines_by_author
      FROM git_blame
      {where}
      GROUP BY repo_id, path, author_email
    )
    GROUP BY repo_id, file_path
    """
    rows = _clickhouse_query_dicts(client, query, params)
    by_repo: Dict[uuid.UUID, Dict[str, float]] = {}
    for row in rows:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        file_path = row.get("file_path")
        value = row.get("ownership_concentration")
        if repo_uuid is None or not file_path or value is None:
            continue
        by_repo.setdefault(repo_uuid, {})[str(file_path)] = float(value)
    return by_repo


def _chunked(values: Sequence[str], chunk_size: int) -> Iterable[List[str]]:
    for i in range(0, len(values), chunk_size):
        yield list(values[i : i + chunk_size])


def _load_mongo_rows(
    db: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from MongoDB.

    Mongo doesn't have a cheap join, so we:
    1) query commits for the day window
    2) add one synthetic "0-stats" row per commit (ensures commits with no stats are counted)
    3) query commit_stats for those commits and append real stat rows
    4) query PRs created/merged in the day window
    """
    start_naive = _naive_utc(start)
    end_naive = _naive_utc(end)

    commit_filter: Dict[str, Any] = {
        "committer_when": {"$gte": start_naive, "$lt": end_naive}
    }
    if repo_id is not None:
        commit_filter["repo_id"] = str(repo_id)
    elif repo_name is not None:
        # Resolve repo_id first
        repo_doc = db["repos"].find_one({"repo": repo_name}, {"id": 1, "_id": 1})
        if repo_doc:
            commit_filter["repo_id"] = str(repo_doc.get("id") or repo_doc.get("_id"))
        else:
            return [], []

    commit_projection = {
        "repo_id": 1,
        "hash": 1,
        "author_email": 1,
        "author_name": 1,
        "committer_when": 1,
    }
    commits = list(db["git_commits"].find(commit_filter, commit_projection))

    commit_meta: Dict[Tuple[str, str], Dict[str, Any]] = {}
    commit_hashes_by_repo: Dict[str, List[str]] = {}
    commit_rows: List[CommitStatRow] = []

    for doc in commits:
        repo_id_raw = doc.get("repo_id")
        commit_hash = doc.get("hash")
        repo_uuid = _parse_uuid(repo_id_raw)
        if repo_uuid is None or not commit_hash:
            continue
        repo_id_str = str(repo_id_raw)
        commit_hash_str = str(commit_hash)
        meta = {
            "repo_uuid": repo_uuid,
            "author_email": doc.get("author_email"),
            "author_name": doc.get("author_name"),
            "committer_when": doc.get("committer_when") or start_naive,
        }
        commit_meta[(repo_id_str, commit_hash_str)] = meta
        commit_hashes_by_repo.setdefault(repo_id_str, []).append(commit_hash_str)

        # Synthetic row ensures commits with no stats are still counted.
        commit_rows.append({
            "repo_id": repo_uuid,
            "commit_hash": commit_hash_str,
            "author_email": meta["author_email"],
            "author_name": meta["author_name"],
            "committer_when": meta["committer_when"],
            "file_path": None,
            "additions": 0,
            "deletions": 0,
        })

    stat_projection = {
        "repo_id": 1,
        "commit_hash": 1,
        "file_path": 1,
        "additions": 1,
        "deletions": 1,
    }

    # Fetch stats per repo to keep $in lists reasonable.
    for repo_id_str, hashes in commit_hashes_by_repo.items():
        for chunk in _chunked(hashes, chunk_size=1000):
            stat_filter = {"repo_id": repo_id_str, "commit_hash": {"$in": chunk}}
            for stat in db["git_commit_stats"].find(stat_filter, stat_projection):
                commit_hash = stat.get("commit_hash")
                key = (repo_id_str, str(commit_hash))
                meta = commit_meta.get(key)
                if meta is None:
                    continue

                file_path = stat.get("file_path") or None
                commit_rows.append({
                    "repo_id": meta["repo_uuid"],
                    "commit_hash": key[1],
                    "author_email": meta["author_email"],
                    "author_name": meta["author_name"],
                    "committer_when": meta["committer_when"],
                    "file_path": str(file_path) if file_path else None,
                    "additions": int(stat.get("additions") or 0),
                    "deletions": int(stat.get("deletions") or 0),
                })

    pr_filter: Dict[str, Any] = {
        "$or": [
            {"created_at": {"$gte": start_naive, "$lt": end_naive}},
            {"merged_at": {"$gte": start_naive, "$lt": end_naive}},
        ]
    }
    if repo_id is not None:
        pr_filter["repo_id"] = str(repo_id)

    pr_projection = {
        "repo_id": 1,
        "number": 1,
        "author_email": 1,
        "author_name": 1,
        "created_at": 1,
        "merged_at": 1,
    }
    pr_docs = list(db["git_pull_requests"].find(pr_filter, pr_projection))

    pr_rows: List[PullRequestRow] = []
    for doc in pr_docs:
        repo_uuid = _parse_uuid(doc.get("repo_id"))
        created_at = doc.get("created_at")
        if repo_uuid is None or not isinstance(created_at, datetime):
            continue
        merged_at = doc.get("merged_at")
        pr_rows.append({
            "repo_id": repo_uuid,
            "number": int(doc.get("number") or 0),
            "author_email": doc.get("author_email"),
            "author_name": doc.get("author_name"),
            "created_at": created_at,
            "merged_at": merged_at if isinstance(merged_at, datetime) else None,
        })

    return commit_rows, pr_rows


def _load_mongo_blame_concentration(
    db: Any,
    *,
    repo_ids: Optional[Set[uuid.UUID]] = None,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> Dict[uuid.UUID, Dict[str, float]]:
    match: Dict[str, Any] = {}
    if repo_ids:
        match["repo_id"] = {"$in": [str(r) for r in sorted(repo_ids, key=str)]}
    elif repo_id is not None:
        match["repo_id"] = str(repo_id)
    elif repo_name is not None:
        repo_doc = db["repos"].find_one({"repo": repo_name}, {"id": 1, "_id": 1})
        if not repo_doc:
            return {}
        match["repo_id"] = str(repo_doc.get("id") or repo_doc.get("_id"))

    pipeline = []
    if match:
        pipeline.append({"$match": match})
    pipeline.extend([
        {
            "$group": {
                "_id": {
                    "repo_id": "$repo_id",
                    "path": "$path",
                    "author": "$author_email",
                },
                "lines_by_author": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id": {"repo_id": "$_id.repo_id", "path": "$_id.path"},
                "max_lines": {"$max": "$lines_by_author"},
                "total_lines": {"$sum": "$lines_by_author"},
            }
        },
        {
            "$project": {
                "repo_id": "$_id.repo_id",
                "file_path": "$_id.path",
                "ownership_concentration": {
                    "$cond": [
                        {"$gt": ["$total_lines", 0]},
                        {"$divide": ["$max_lines", "$total_lines"]},
                        None,
                    ]
                },
            }
        },
    ])

    by_repo: Dict[uuid.UUID, Dict[str, float]] = {}
    for row in db["git_blame"].aggregate(pipeline):
        repo_uuid = _parse_uuid(row.get("repo_id"))
        file_path = row.get("file_path")
        value = row.get("ownership_concentration")
        if repo_uuid is None or not file_path or value is None:
            continue
        by_repo.setdefault(repo_uuid, {})[str(file_path)] = float(value)
    return by_repo


def _load_sqlite_rows(
    engine: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from SQLite via SQLAlchemy ORM models.

    Uses a left join between `git_commits` and `git_commit_stats` so commits with
    no stats are still counted (via a synthetic 0-stats row).
    """
    from sqlalchemy import and_, or_, select
    from sqlalchemy.orm import Session

    from models.git import GitCommit, GitCommitStat, GitPullRequest

    start_naive = _naive_utc(start)
    end_naive = _naive_utc(end)

    commit_stmt = (
        select(
            GitCommit.repo_id,
            GitCommit.hash.label("commit_hash"),
            GitCommit.author_email,
            GitCommit.author_name,
            GitCommit.committer_when,
            GitCommitStat.file_path,
            GitCommitStat.additions,
            GitCommitStat.deletions,
        )
        .select_from(GitCommit)
        .outerjoin(
            GitCommitStat,
            and_(
                GitCommitStat.repo_id == GitCommit.repo_id,
                GitCommitStat.commit_hash == GitCommit.hash,
            ),
        )
        .where(
            GitCommit.committer_when >= start_naive,
            GitCommit.committer_when < end_naive,
        )
    )
    if repo_id is not None:
        commit_stmt = commit_stmt.where(GitCommit.repo_id == repo_id)
    elif repo_name is not None:
        from models.git import Repo

        commit_stmt = commit_stmt.join(Repo, GitCommit.repo_id == Repo.id).where(
            Repo.repo == repo_name
        )

    pr_stmt = (
        select(
            GitPullRequest.repo_id,
            GitPullRequest.number,
            GitPullRequest.author_email,
            GitPullRequest.author_name,
            GitPullRequest.created_at,
            GitPullRequest.merged_at,
        )
        .select_from(GitPullRequest)
        .where(
            or_(
                and_(
                    GitPullRequest.created_at >= start_naive,
                    GitPullRequest.created_at < end_naive,
                ),
                and_(
                    GitPullRequest.merged_at.is_not(None),
                    GitPullRequest.merged_at >= start_naive,
                    GitPullRequest.merged_at < end_naive,
                ),
            )
        )
    )
    if repo_id is not None:
        pr_stmt = pr_stmt.where(GitPullRequest.repo_id == repo_id)
    elif repo_name is not None:
        from models.git import Repo

        pr_stmt = pr_stmt.join(Repo, GitPullRequest.repo_id == Repo.id).where(
            Repo.repo == repo_name
        )

    commit_rows: List[CommitStatRow] = []
    pr_rows: List[PullRequestRow] = []

    try:
        with Session(engine) as session:
            try:
                for (
                    repo_uuid,
                    commit_hash,
                    author_email,
                    author_name,
                    committer_when,
                    file_path,
                    additions,
                    deletions,
                ) in session.execute(commit_stmt).all():
                    if isinstance(committer_when, str):
                        try:
                            committer_when = datetime.fromisoformat(committer_when)
                        except ValueError:
                            continue

                    commit_rows.append({
                        "repo_id": repo_uuid,
                        "commit_hash": str(commit_hash),
                        "author_email": author_email,
                        "author_name": author_name,
                        "committer_when": committer_when,  # type: ignore
                        "file_path": str(file_path) if file_path else None,
                        "additions": int(additions or 0),
                        "deletions": int(deletions or 0),
                    })
            except Exception as e:
                logger.warning("Failed to load git commits from SQLite: %s", e)

            try:
                for (
                    repo_uuid,
                    number,
                    author_email,
                    author_name,
                    created_at,
                    merged_at,
                ) in session.execute(pr_stmt).all():
                    if isinstance(created_at, str):
                        try:
                            created_at = datetime.fromisoformat(created_at)
                        except ValueError:
                            continue

                    if isinstance(merged_at, str):
                        try:
                            merged_at = datetime.fromisoformat(merged_at)
                        except ValueError:
                            merged_at = None

                    pr_rows.append({
                        "repo_id": repo_uuid,
                        "number": int(number or 0),
                        "author_email": author_email,
                        "author_name": author_name,
                        "created_at": created_at,  # type: ignore
                        "merged_at": merged_at,  # type: ignore
                    })
            except Exception as e:
                logger.warning("Failed to load pull requests from SQLite: %s", e)

    except Exception as e:
        logger.warning("Failed to access SQLite session: %s", e)
        return [], []

    return commit_rows, pr_rows


def _load_sqlite_blame_concentration(
    engine: Any,
    *,
    repo_ids: Optional[Set[uuid.UUID]] = None,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> Dict[uuid.UUID, Dict[str, float]]:
    from sqlalchemy import bindparam, text

    where = ""
    params: Dict[str, Any] = {}
    bind = None
    if repo_ids:
        where = "WHERE repo_id IN :repo_ids"
        params["repo_ids"] = [str(r) for r in sorted(repo_ids, key=str)]
        bind = bindparam("repo_ids", expanding=True)
    elif repo_id is not None:
        where = "WHERE repo_id = :repo_id"
        params["repo_id"] = str(repo_id)
    elif repo_name is not None:
        where = "WHERE repo_id IN (SELECT id FROM repos WHERE repo = :repo_name)"
        params["repo_name"] = repo_name

    query = text(f"""
        SELECT
          repo_id,
          file_path,
          max(lines_by_author) * 1.0 / sum(lines_by_author) AS ownership_concentration
        FROM (
          SELECT
            repo_id,
            path AS file_path,
            author_email,
            COUNT(*) AS lines_by_author
          FROM git_blame
          {where}
          GROUP BY repo_id, path, author_email
        ) AS author_lines
        GROUP BY repo_id, file_path
    """)
    if bind is not None:
        query = query.bindparams(bind)

    by_repo: Dict[uuid.UUID, Dict[str, float]] = {}
    with engine.connect() as conn:
        for row in conn.execute(query, params).fetchall():
            repo_uuid = _parse_uuid(row[0])
            file_path = row[1]
            value = row[2]
            if repo_uuid is None or not file_path or value is None:
                continue
            by_repo.setdefault(repo_uuid, {})[str(file_path)] = float(value)
    return by_repo


def _load_clickhouse_reviews(
    client,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> List[PullRequestReviewRow]:
    # Simple query for clickhouse-connect
    sql = """
        SELECT repo_id, number, reviewer, submitted_at, state
        FROM git_pull_request_reviews
        WHERE submitted_at >= %(start)s AND submitted_at < %(end)s
    """
    params = {"start": start, "end": end}
    if repo_id:
        sql += " AND repo_id = %(repo_id)s"
        params["repo_id"] = str(repo_id)
    elif repo_name:
        sql += " AND repo_id IN (SELECT id FROM repos WHERE repo = %(repo_name)s)"
        params["repo_name"] = repo_name

    result = client.query(sql, parameters=params)
    rows: List[PullRequestReviewRow] = []
    for r in result.named_results():
        submitted_at = r["submitted_at"]
        if isinstance(submitted_at, str):
            submitted_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))

        rows.append({
            "repo_id": uuid.UUID(str(r["repo_id"])),
            "number": int(r["number"]),
            "reviewer": str(r["reviewer"]),
            "submitted_at": submitted_at,
            "state": str(r["state"]),
        })
    return rows


def _load_mongo_reviews(
    db,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> List[PullRequestReviewRow]:
    query = {"submitted_at": {"$gte": start, "$lt": end}}
    if repo_id:
        query["repo_id"] = str(repo_id)
    elif repo_name:
        repo_doc = db["repos"].find_one({"repo": repo_name}, {"id": 1, "_id": 1})
        if repo_doc:
            query["repo_id"] = str(repo_doc.get("id") or repo_doc.get("_id"))
        else:
            return []

    rows: List[PullRequestReviewRow] = []
    for doc in db["git_pull_request_reviews"].find(query):
        submitted_at = doc["submitted_at"]
        if isinstance(submitted_at, str):
            submitted_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))

        rows.append({
            "repo_id": uuid.UUID(str(doc["repo_id"])),
            "number": int(doc["number"]),
            "reviewer": str(doc["reviewer"]),
            "submitted_at": submitted_at,
            "state": str(doc["state"]),
        })
    return rows


def _load_sqlite_reviews(
    engine,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> List[PullRequestReviewRow]:
    from models.git import GitPullRequestReview
    from sqlalchemy import select

    with engine.connect() as conn:
        stmt = select(
            GitPullRequestReview.repo_id,
            GitPullRequestReview.number,
            GitPullRequestReview.reviewer,
            GitPullRequestReview.submitted_at,
            GitPullRequestReview.state,
        ).where(
            GitPullRequestReview.submitted_at >= start,
            GitPullRequestReview.submitted_at < end,
        )
        if repo_id:
            stmt = stmt.where(GitPullRequestReview.repo_id == repo_id)
        elif repo_name:
            from models.git import Repo

            stmt = stmt.join(Repo, GitPullRequestReview.repo_id == Repo.id).where(
                Repo.repo == repo_name
            )

        rows: List[PullRequestReviewRow] = []
        try:
            for r in conn.execute(stmt).all():
                submitted_at = r[3]
                if isinstance(submitted_at, str):
                    submitted_at = datetime.fromisoformat(
                        submitted_at.replace("Z", "+00:00")
                    )

                rows.append({
                    "repo_id": r[0],
                    "number": int(r[1] or 0),
                    "reviewer": str(r[2]),
                    "submitted_at": submitted_at,
                    "state": str(r[4]),
                })
        except Exception as e:
            logger.warning("Failed to load pull request reviews from SQLite: %s", e)
            return []

        return rows


def _load_clickhouse_pipeline_runs(
    client: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[PipelineRunRow]:
    params: Dict[str, Any] = {"start": _naive_utc(start), "end": _naive_utc(end)}
    repo_filter = ""
    if repo_id is not None:
        params["repo_id"] = str(repo_id)
        repo_filter = " AND repo_id = {repo_id:UUID}"
    elif repo_name is not None:
        params["repo_name"] = repo_name
        repo_filter = (
            " AND repo_id IN (SELECT id FROM repos WHERE repo = {repo_name:String})"
        )

    query = f"""
    SELECT
      repo_id,
      run_id,
      status,
      queued_at,
      started_at,
      finished_at
    FROM ci_pipeline_runs
    WHERE started_at >= {{start:DateTime}} AND started_at < {{end:DateTime}}
    {repo_filter}
    """
    try:
        rows = _clickhouse_query_dicts(client, query, params)
    except Exception as exc:
        logger.warning("Skipping ClickHouse pipeline runs: %s", exc)
        return []

    results: List[PipelineRunRow] = []
    for row in rows:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        started_at = row.get("started_at")
        if repo_uuid is None or not isinstance(started_at, datetime):
            continue
        results.append({
            "repo_id": repo_uuid,
            "run_id": str(row.get("run_id") or ""),
            "status": row.get("status"),
            "queued_at": row.get("queued_at")
            if isinstance(row.get("queued_at"), datetime)
            else None,
            "started_at": started_at,
            "finished_at": row.get("finished_at")
            if isinstance(row.get("finished_at"), datetime)
            else None,
        })
    return results


def _load_clickhouse_deployments(
    client: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[DeploymentRow]:
    params: Dict[str, Any] = {"start": _naive_utc(start), "end": _naive_utc(end)}
    repo_filter = ""
    if repo_id is not None:
        params["repo_id"] = str(repo_id)
        repo_filter = " AND repo_id = {repo_id:UUID}"
    elif repo_name is not None:
        params["repo_name"] = repo_name
        repo_filter = (
            " AND repo_id IN (SELECT id FROM repos WHERE repo = {repo_name:String})"
        )

    query = f"""
    SELECT
      repo_id,
      deployment_id,
      status,
      environment,
      started_at,
      finished_at,
      deployed_at,
      merged_at,
      pull_request_number
    FROM deployments
    WHERE deployed_at >= {{start:DateTime}} AND deployed_at < {{end:DateTime}}
    {repo_filter}
    """
    try:
        rows = _clickhouse_query_dicts(client, query, params)
    except Exception as exc:
        logger.warning("Skipping ClickHouse deployments: %s", exc)
        return []

    results: List[DeploymentRow] = []
    for row in rows:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        if repo_uuid is None:
            continue
        results.append({
            "repo_id": repo_uuid,
            "deployment_id": str(row.get("deployment_id") or ""),
            "status": row.get("status"),
            "environment": row.get("environment"),
            "started_at": row.get("started_at")
            if isinstance(row.get("started_at"), datetime)
            else None,
            "finished_at": row.get("finished_at")
            if isinstance(row.get("finished_at"), datetime)
            else None,
            "deployed_at": row.get("deployed_at")
            if isinstance(row.get("deployed_at"), datetime)
            else None,
            "merged_at": row.get("merged_at")
            if isinstance(row.get("merged_at"), datetime)
            else None,
            "pull_request_number": int(row.get("pull_request_number") or 0)
            if row.get("pull_request_number") is not None
            else None,
        })
    return results


def _load_clickhouse_incidents(
    client: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[IncidentRow]:
    params: Dict[str, Any] = {"start": _naive_utc(start), "end": _naive_utc(end)}
    repo_filter = ""
    if repo_id is not None:
        params["repo_id"] = str(repo_id)
        repo_filter = " AND repo_id = {repo_id:UUID}"
    elif repo_name is not None:
        params["repo_name"] = repo_name
        repo_filter = (
            " AND repo_id IN (SELECT id FROM repos WHERE repo = {repo_name:String})"
        )

    query = f"""
    SELECT
      repo_id,
      incident_id,
      status,
      started_at,
      resolved_at
    FROM incidents
    WHERE resolved_at >= {{start:DateTime}} AND resolved_at < {{end:DateTime}}
    {repo_filter}
    """
    try:
        rows = _clickhouse_query_dicts(client, query, params)
    except Exception as exc:
        logger.warning("Skipping ClickHouse incidents: %s", exc)
        return []

    results: List[IncidentRow] = []
    for row in rows:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        started_at = row.get("started_at")
        if repo_uuid is None or not isinstance(started_at, datetime):
            continue
        results.append({
            "repo_id": repo_uuid,
            "incident_id": str(row.get("incident_id") or ""),
            "status": row.get("status"),
            "started_at": started_at,
            "resolved_at": row.get("resolved_at")
            if isinstance(row.get("resolved_at"), datetime)
            else None,
        })
    return results


def _load_mongo_pipeline_runs(
    db: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[PipelineRunRow]:
    query: Dict[str, Any] = {"started_at": {"$gte": start, "$lt": end}}
    if repo_id:
        query["repo_id"] = str(repo_id)
    elif repo_name:
        repo_doc = db["repos"].find_one({"repo": repo_name}, {"id": 1, "_id": 1})
        if repo_doc:
            query["repo_id"] = str(repo_doc.get("id") or repo_doc.get("_id"))
        else:
            return []
    rows: List[PipelineRunRow] = []
    for doc in db["ci_pipeline_runs"].find(query):
        started_at = doc.get("started_at")
        if not isinstance(started_at, datetime):
            continue
        rows.append({
            "repo_id": uuid.UUID(str(doc["repo_id"])),
            "run_id": str(doc.get("run_id") or ""),
            "status": doc.get("status"),
            "queued_at": doc.get("queued_at")
            if isinstance(doc.get("queued_at"), datetime)
            else None,
            "started_at": started_at,
            "finished_at": doc.get("finished_at")
            if isinstance(doc.get("finished_at"), datetime)
            else None,
        })
    return rows


def _load_mongo_deployments(
    db: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[DeploymentRow]:
    query: Dict[str, Any] = {"deployed_at": {"$gte": start, "$lt": end}}
    if repo_id:
        query["repo_id"] = str(repo_id)
    elif repo_name:
        repo_doc = db["repos"].find_one({"repo": repo_name}, {"id": 1, "_id": 1})
        if repo_doc:
            query["repo_id"] = str(repo_doc.get("id") or repo_doc.get("_id"))
        else:
            return []
    rows: List[DeploymentRow] = []
    for doc in db["deployments"].find(query):
        rows.append({
            "repo_id": uuid.UUID(str(doc["repo_id"])),
            "deployment_id": str(doc.get("deployment_id") or ""),
            "status": doc.get("status"),
            "environment": doc.get("environment"),
            "started_at": doc.get("started_at")
            if isinstance(doc.get("started_at"), datetime)
            else None,
            "finished_at": doc.get("finished_at")
            if isinstance(doc.get("finished_at"), datetime)
            else None,
            "deployed_at": doc.get("deployed_at")
            if isinstance(doc.get("deployed_at"), datetime)
            else None,
            "merged_at": doc.get("merged_at")
            if isinstance(doc.get("merged_at"), datetime)
            else None,
            "pull_request_number": int(doc.get("pull_request_number") or 0)
            if doc.get("pull_request_number") is not None
            else None,
        })
    return rows


def _load_mongo_incidents(
    db: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[IncidentRow]:
    query: Dict[str, Any] = {"resolved_at": {"$gte": start, "$lt": end}}
    if repo_id:
        query["repo_id"] = str(repo_id)
    elif repo_name:
        repo_doc = db["repos"].find_one({"repo": repo_name}, {"id": 1, "_id": 1})
        if repo_doc:
            query["repo_id"] = str(repo_doc.get("id") or repo_doc.get("_id"))
        else:
            return []
    rows: List[IncidentRow] = []
    for doc in db["incidents"].find(query):
        started_at = doc.get("started_at")
        if not isinstance(started_at, datetime):
            continue
        rows.append({
            "repo_id": uuid.UUID(str(doc["repo_id"])),
            "incident_id": str(doc.get("incident_id") or ""),
            "status": doc.get("status"),
            "started_at": started_at,
            "resolved_at": doc.get("resolved_at")
            if isinstance(doc.get("resolved_at"), datetime)
            else None,
        })
    return rows


def _load_sqlite_pipeline_runs(
    engine: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[PipelineRunRow]:
    from sqlalchemy import text

    query = """
        SELECT repo_id, run_id, status, queued_at, started_at, finished_at
        FROM ci_pipeline_runs
        WHERE started_at >= :start AND started_at < :end
    """
    params: Dict[str, Any] = {"start": start, "end": end}
    if repo_id:
        query += " AND repo_id = :repo_id"
        params["repo_id"] = str(repo_id)
    elif repo_name:
        query += " AND repo_id IN (SELECT id FROM repos WHERE repo = :repo_name)"
        params["repo_name"] = repo_name
    rows: List[PipelineRunRow] = []
    try:
        with engine.connect() as conn:
            for r in conn.execute(text(query), params).all():
                started_at = r[4]
                if isinstance(started_at, str):
                    started_at = datetime.fromisoformat(
                        started_at.replace("Z", "+00:00")
                    )
                rows.append({
                    "repo_id": uuid.UUID(str(r[0])),
                    "run_id": str(r[1] or ""),
                    "status": r[2],
                    "queued_at": r[3] if isinstance(r[3], datetime) else None,
                    "started_at": started_at,
                    "finished_at": r[5] if isinstance(r[5], datetime) else None,
                })
    except Exception as exc:
        logger.warning("Skipping SQLite pipeline runs: %s", exc)
        return []
    return rows


def _load_sqlite_deployments(
    engine: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[DeploymentRow]:
    from sqlalchemy import text

    query = """
        SELECT repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number
        FROM deployments
        WHERE deployed_at >= :start AND deployed_at < :end
    """
    params: Dict[str, Any] = {"start": start, "end": end}
    if repo_id:
        query += " AND repo_id = :repo_id"
        params["repo_id"] = str(repo_id)
    elif repo_name:
        query += " AND repo_id IN (SELECT id FROM repos WHERE repo = :repo_name)"
        params["repo_name"] = repo_name
    rows: List[DeploymentRow] = []
    try:
        with engine.connect() as conn:
            for r in conn.execute(text(query), params).all():
                rows.append({
                    "repo_id": uuid.UUID(str(r[0])),
                    "deployment_id": str(r[1] or ""),
                    "status": r[2],
                    "environment": r[3],
                    "started_at": r[4] if isinstance(r[4], datetime) else None,
                    "finished_at": r[5] if isinstance(r[5], datetime) else None,
                    "deployed_at": r[6] if isinstance(r[6], datetime) else None,
                    "merged_at": r[7] if isinstance(r[7], datetime) else None,
                    "pull_request_number": int(r[8] or 0) if r[8] is not None else None,
                })
    except Exception as exc:
        logger.warning("Skipping SQLite deployments: %s", exc)
        return []
    return rows


def _load_sqlite_incidents(
    engine: Any,
    *,
    start: datetime,
    end: datetime,
    repo_id: Optional[uuid.UUID],
    repo_name: Optional[str] = None,
) -> List[IncidentRow]:
    from sqlalchemy import text

    query = """
        SELECT repo_id, incident_id, status, started_at, resolved_at
        FROM incidents
        WHERE resolved_at >= :start AND resolved_at < :end
    """
    params: Dict[str, Any] = {"start": start, "end": end}
    if repo_id:
        query += " AND repo_id = :repo_id"
        params["repo_id"] = str(repo_id)
    elif repo_name:
        query += " AND repo_id IN (SELECT id FROM repos WHERE repo = :repo_name)"
        params["repo_name"] = repo_name
    rows: List[IncidentRow] = []
    try:
        with engine.connect() as conn:
            for r in conn.execute(text(query), params).all():
                started_at = r[3]
                if isinstance(started_at, str):
                    started_at = datetime.fromisoformat(
                        started_at.replace("Z", "+00:00")
                    )
                rows.append({
                    "repo_id": uuid.UUID(str(r[0])),
                    "incident_id": str(r[1] or ""),
                    "status": r[2],
                    "started_at": started_at,
                    "resolved_at": r[4] if isinstance(r[4], datetime) else None,
                })
    except Exception as exc:
        logger.warning("Skipping SQLite incidents: %s", exc)
        return []
    return rows


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    daily = subparsers.add_parser(
        "daily", help="Compute daily metrics for a given day."
    )
    daily.add_argument("--db", help="Database connection string.")
    daily.add_argument(
        "--day",
        type=date.fromisoformat,
        default=date.today().isoformat(),
        help="Target day (YYYY-MM-DD).",
    )
    daily.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Compute metrics for N days ending at --day.",
    )
    daily.add_argument("--repo-id", type=uuid.UUID, help="Filter to specific repo ID.")
    daily.add_argument("--repo-name", help="Filter to specific repo name.")
    daily.add_argument(
        "--no-commits",
        dest="commit_metrics",
        action="store_false",
        help="Skip commit-level metrics.",
    )
    daily.set_defaults(commit_metrics=True)
    daily.add_argument(
        "--sink",
        choices=["clickhouse", "mongo", "sqlite", "postgres", "both", "auto"],
        default="auto",
    )
    daily.add_argument(
        "--provider",
        choices=["all", "jira", "github", "gitlab", "none"],
        default="none",
        help="Sync work items from provider before computing (default: none).",
    )

    from metrics.job_daily import _cmd_metrics_daily

    daily.set_defaults(func=_cmd_metrics_daily)


def _cmd_metrics_daily(ns: argparse.Namespace) -> int:
    try:
        run_daily_metrics_job(
            db_url=ns.db,
            day=ns.day,
            backfill_days=ns.backfill,
            repo_id=ns.repo_id,
            repo_name=ns.repo_name,
            include_commit_metrics=ns.commit_metrics,
            sink=ns.sink,
            provider=ns.provider,
        )
        return 0
    except Exception as e:
        logger.error(f"Daily metrics job failed: {e}")
        return 1
