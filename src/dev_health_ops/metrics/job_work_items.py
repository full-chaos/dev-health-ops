from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from dev_health_ops.analytics.investment import InvestmentClassifier
from dev_health_ops.analytics.issue_types import IssueTypeNormalizer
from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.metrics.compute_work_items import compute_work_item_metrics_daily
from dev_health_ops.metrics.schemas import (
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
)
from dev_health_ops.metrics.work_items import (
    fetch_github_project_v2_items,
    fetch_github_work_items,
    fetch_gitlab_work_items,
    fetch_jira_work_items_with_extras,
    parse_github_projects_v2_env,
)
from dev_health_ops.metrics.job_daily import (
    REPO_ROOT,
    _discover_repos,
    _secondary_uri_from_env,
    _to_utc,
    _normalize_sqlite_url,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink
from dev_health_ops.metrics.sinks.postgres import PostgresMetricsSink
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.status_mapping import load_status_mapping
from dev_health_ops.providers.teams import load_team_resolver
from dev_health_ops.storage import detect_db_type

logger = logging.getLogger(__name__)


def _date_range(end_day: date, backfill_days: int) -> List[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def run_work_items_sync_job(
    *,
    db_url: str,
    day: date,
    backfill_days: int,
    provider: str,
    sink: str = "auto",
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
    search_pattern: Optional[str] = None,
) -> None:
    """
    Sync work tracking facts from provider APIs and write derived work item tables.

    This job exists so `metrics daily` does not need to call external APIs.
    """
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    backend = detect_db_type(db_url)
    sink = (sink or "auto").strip().lower()
    if sink == "auto":
        sink = backend

    if backend not in {"clickhouse", "mongo", "sqlite", "postgres"}:
        raise ValueError(f"Unsupported db backend for work item sync: {backend}")

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

    provider = (provider or "none").strip().lower()
    provider_set: Set[str]
    if provider in {"none", "off", "skip"}:
        raise ValueError(
            "work item sync requires --provider (jira|github|gitlab|linear|synthetic|all)"
        )
    if provider in {"all", "*"}:
        provider_set = {"jira", "github", "gitlab", "linear", "synthetic"}
    else:
        provider_set = {provider}
    unknown = provider_set - {"jira", "github", "gitlab", "linear", "synthetic"}
    if unknown:
        raise ValueError(f"Unknown provider(s): {sorted(unknown)}")

    status_mapping = load_status_mapping()
    identity = load_identity_resolver()
    team_resolver = load_team_resolver()

    investment_classifier = InvestmentClassifier(
        REPO_ROOT / "src/dev_health_ops/config/investment_areas.yaml"
    )
    issue_type_normalizer = IssueTypeNormalizer(
        REPO_ROOT / "src/dev_health_ops/config/issue_type_mapping.yaml"
    )

    computed_at = datetime.now(timezone.utc)
    days = _date_range(day, backfill_days)
    since_dt = datetime.combine(min(days), time.min, tzinfo=timezone.utc)
    until_dt = datetime.combine(max(days), time.max, tzinfo=timezone.utc)

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
                s.ensure_tables()
            elif isinstance(s, MongoMetricsSink):
                s.ensure_indexes()
            elif isinstance(s, PostgresMetricsSink):
                s.ensure_tables()
            elif isinstance(s, SQLiteMetricsSink):
                s.ensure_tables()

        discovered_repos = _discover_repos(
            backend=backend,
            primary_sink=primary_sink,
            repo_id=repo_id,
            repo_name=repo_name,
        )
        from dev_health_ops.utils import match_pattern

        before = len(discovered_repos)
        discovered_repos = [
            r for r in discovered_repos if match_pattern(r.full_name, search_pattern)
        ]
        logger.info(
            "Filtered repos by '%s': %d/%d",
            search_pattern,
            len(discovered_repos),
            before,
        )

        if "synthetic" in provider_set and not any(
            r.source == "synthetic" for r in discovered_repos
        ):
            from dev_health_ops.metrics.work_items import DiscoveredRepo

            discovered_repos.append(
                DiscoveredRepo(
                    repo_id=uuid.uuid4(),
                    full_name="synthetic/demo-repo",
                    source="synthetic",
                    settings={},
                )
            )

        work_items: List[Any] = []
        transitions: List[Any] = []
        dependencies: List[Any] = []
        reopen_events: List[Any] = []
        interactions: List[Any] = []
        sprints: List[Any] = []

        if "jira" in provider_set:
            (
                items,
                tr,
                dep,
                reopen,
                interaction,
                sprint_rows,
            ) = fetch_jira_work_items_with_extras(
                since=since_dt,
                until=until_dt,
                status_mapping=status_mapping,
                identity=identity,
            )
            work_items.extend(items)
            transitions.extend(tr)
            dependencies.extend(dep)
            reopen_events.extend(reopen)
            interactions.extend(interaction)
            sprints.extend(sprint_rows)

        if "github" in provider_set:
            items, tr = fetch_github_work_items(
                repos=discovered_repos,
                since=since_dt,
                status_mapping=status_mapping,
                identity=identity,
                include_issue_events=True,
            )
            work_items.extend(items)
            transitions.extend(tr)

            projects = parse_github_projects_v2_env()
            if projects:
                proj_items, proj_tr = fetch_github_project_v2_items(
                    projects=projects,
                    status_mapping=status_mapping,
                    identity=identity,
                )
                by_id = {w.work_item_id: w for w in work_items}
                for w in proj_items:
                    by_id[w.work_item_id] = w
                work_items = list(by_id.values())
                transitions.extend(list(proj_tr or []))

        if "gitlab" in provider_set:
            items, tr = fetch_gitlab_work_items(
                repos=discovered_repos,
                since=since_dt,
                status_mapping=status_mapping,
                identity=identity,
                include_label_events=True,
            )
            work_items.extend(items)
            transitions.extend(tr)

        if "synthetic" in provider_set:
            from dev_health_ops.metrics.work_items import fetch_synthetic_work_items

            items, tr = fetch_synthetic_work_items(
                repos=discovered_repos, days=backfill_days + 1
            )
            work_items.extend(items)
            transitions.extend(tr)

        if "linear" in provider_set:
            from dev_health_ops.providers.base import IngestionContext, IngestionWindow
            from dev_health_ops.providers.linear.provider import LinearProvider

            linear_provider = LinearProvider(
                status_mapping=status_mapping,
                identity=identity,
            )
            ctx = IngestionContext(
                window=IngestionWindow(updated_since=since_dt, active_until=until_dt),
                repo=None,
            )
            batch = linear_provider.ingest(ctx)
            work_items.extend(batch.work_items)
            transitions.extend(batch.status_transitions)
            reopen_events.extend(batch.reopen_events)
            interactions.extend(batch.interactions)
            sprints.extend(batch.sprints)
            logger.info(
                "Linear: fetched %d work items, %d transitions, %d sprints",
                len(batch.work_items),
                len(batch.status_transitions),
                len(batch.sprints),
            )

        logger.info(
            "Work item sync: fetched %d items and %d transitions (providers=%s)",
            len(work_items),
            len(transitions),
            sorted(provider_set),
        )
        if dependencies:
            logger.info("Jira: extracted %d dependency edges", len(dependencies))
        if reopen_events:
            logger.info("Jira: extracted %d reopen events", len(reopen_events))
        if interactions:
            logger.info("Jira: extracted %d interaction events", len(interactions))
        if sprints:
            logger.info("Jira: extracted %d sprint records", len(sprints))

        # Write raw work items and transitions to sinks
        for s in sinks:
            if hasattr(s, "write_work_items") and work_items:
                logger.info(
                    "Writing %d work items to %s", len(work_items), type(s).__name__
                )
                s.write_work_items(work_items)
            if hasattr(s, "write_work_item_transitions") and transitions:
                logger.info(
                    "Writing %d transitions to %s", len(transitions), type(s).__name__
                )
                s.write_work_item_transitions(transitions)

        for s in sinks:
            if dependencies and hasattr(s, "write_work_item_dependencies"):
                s.write_work_item_dependencies(dependencies)
            if reopen_events and hasattr(s, "write_work_item_reopen_events"):
                s.write_work_item_reopen_events(reopen_events)
            if interactions and hasattr(s, "write_work_item_interactions"):
                s.write_work_item_interactions(interactions)
            if sprints and hasattr(s, "write_sprints"):
                s.write_sprints(sprints)

        for d in days:
            wi_metrics, wi_user_metrics, wi_cycle_times = (
                compute_work_item_metrics_daily(
                    day=d,
                    work_items=work_items,
                    transitions=transitions,
                    computed_at=computed_at,
                    team_resolver=team_resolver,
                )
            )
            wi_state_durations = compute_work_item_state_durations_daily(
                day=d,
                work_items=work_items,
                transitions=transitions,
                computed_at=computed_at,
                team_resolver=team_resolver,
            )

            # --- Issue Type Metrics ---
            issue_type_stats: Dict[Tuple[uuid.UUID, str, str, str], Dict[str, Any]] = {}

            def _get_team(wi: Any) -> str:
                if getattr(wi, "assignees", None):
                    t_id, _ = team_resolver.resolve(wi.assignees[0])
                    if t_id:
                        return t_id
                return "unassigned"

            def _normalize_investment_team_id(team_id: Optional[str]) -> Optional[str]:
                if not team_id or team_id == "unassigned":
                    return None
                return team_id

            start_dt = _to_utc(datetime.combine(d, time.min, tzinfo=timezone.utc))
            end_dt = start_dt + timedelta(days=1)
            for item in work_items:
                r_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
                prov = item.provider
                team_id = _get_team(item)
                norm_type = issue_type_normalizer.normalize(
                    prov, item.type, getattr(item, "labels", [])
                )

                key = (r_id, prov, team_id, norm_type)
                if key not in issue_type_stats:
                    issue_type_stats[key] = {
                        "created": 0,
                        "completed": 0,
                        "active": 0,
                        "cycle_hours": [],
                    }

                stats = issue_type_stats[key]
                created = _to_utc(item.created_at)
                if start_dt <= created < end_dt:
                    stats["created"] += 1

                if item.completed_at:
                    completed = _to_utc(item.completed_at)
                    if start_dt <= completed < end_dt:
                        stats["completed"] += 1
                        if item.started_at:
                            started = _to_utc(item.started_at)
                            h = (completed - started).total_seconds() / 3600.0
                            if h >= 0:
                                stats["cycle_hours"].append(h)

                if created < end_dt and (
                    not item.completed_at or _to_utc(item.completed_at) >= start_dt
                ):
                    stats["active"] += 1

            issue_type_metrics_rows: List[IssueTypeMetricsRecord] = []
            for (r_id, prov, team_id, norm_type), stat in issue_type_stats.items():
                cycles = sorted(stat["cycle_hours"])
                p50 = cycles[len(cycles) // 2] if cycles else 0.0
                p90 = cycles[int(len(cycles) * 0.9)] if cycles else 0.0
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
                        lead_p50_hours=0.0,
                        computed_at=computed_at,
                    )
                )

            # --- Investment areas ---
            investment_classifications: List[InvestmentClassificationRecord] = []
            inv_metrics_map: Dict[Tuple[uuid.UUID, str, str, str], Dict[str, Any]] = {}

            for item in work_items:
                r_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
                created = _to_utc(item.created_at)
                if not (
                    created < end_dt
                    and (
                        not item.completed_at or _to_utc(item.completed_at) >= start_dt
                    )
                ):
                    continue

                cls = investment_classifier.classify(
                    {
                        "labels": getattr(item, "labels", []),
                        "component": getattr(item, "component", ""),
                        "title": item.title,
                        "provider": item.provider,
                    }
                )

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

                if item.completed_at:
                    completed = _to_utc(item.completed_at)
                    if not (start_dt <= completed < end_dt):
                        continue
                    team_id = _normalize_investment_team_id(_get_team(item))
                    key = (r_id, team_id, cls.investment_area, cls.project_stream or "")
                    if key not in inv_metrics_map:
                        inv_metrics_map[key] = {
                            "units": 0,
                            "completed": 0,
                            "churn": 0,
                            "cycles": [],
                        }
                    inv_metrics_map[key]["completed"] += 1
                    points = getattr(item, "story_points", 1) or 1
                    inv_metrics_map[key]["units"] += int(points)
                    if item.started_at:
                        started = _to_utc(item.started_at)
                        h = (completed - started).total_seconds() / 3600.0
                        if h >= 0:
                            inv_metrics_map[key]["cycles"].append(h)

            investment_metrics_rows: List[InvestmentMetricsRecord] = []
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
                        prs_merged=0,
                        churn_loc=data["churn"],
                        cycle_p50_hours=p50,
                        computed_at=computed_at,
                    )
                )

            for s in sinks:
                if wi_metrics:
                    s.write_work_item_metrics(wi_metrics)
                if wi_user_metrics:
                    s.write_work_item_user_metrics(wi_user_metrics)
                if wi_cycle_times:
                    s.write_work_item_cycle_times(wi_cycle_times)
                if wi_state_durations:
                    s.write_work_item_state_durations(wi_state_durations)

                if hasattr(s, "write_issue_type_metrics") and issue_type_metrics_rows:
                    s.write_issue_type_metrics(issue_type_metrics_rows)
                if (
                    hasattr(s, "write_investment_classifications")
                    and investment_classifications
                ):
                    s.write_investment_classifications(investment_classifications)
                if hasattr(s, "write_investment_metrics") and investment_metrics_rows:
                    s.write_investment_metrics(investment_metrics_rows)
    finally:
        for s in sinks:
            try:
                s.close()
            except Exception:
                logger.exception("Error closing sink %s", type(s).__name__)


def register_commands(sync_subparsers: argparse._SubParsersAction) -> None:
    wi = sync_subparsers.add_parser(
        "work-items",
        help="Sync work tracking facts and compute derived work item tables.",
    )
    wi.add_argument("--db", help="Database connection string.")
    wi.add_argument(
        "--day",
        type=date.fromisoformat,
        default=date.today().isoformat(),
        help="Target day (YYYY-MM-DD).",
    )
    wi.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Sync/compute for N days ending at --day.",
    )
    wi.add_argument(
        "--provider",
        choices=["all", "jira", "github", "gitlab", "linear", "synthetic", "none"],
        default="all",
        help="Provider to sync from (default: all).",
    )
    wi.add_argument(
        "--sink",
        choices=["clickhouse", "mongo", "sqlite", "postgres", "both", "auto"],
        default="auto",
    )
    wi.add_argument("--repo-id", type=uuid.UUID, help="Filter to specific repo ID.")
    wi.add_argument("--repo-name", help="Filter to specific repo name.")
    wi.add_argument("-s", "--search", help="Repo name search pattern (glob).")
    wi.set_defaults(func=_cmd_sync_work_items)


def _cmd_sync_work_items(ns: argparse.Namespace) -> int:
    try:
        run_work_items_sync_job(
            db_url=ns.db,
            day=ns.day,
            backfill_days=ns.backfill,
            provider=ns.provider,
            sink=ns.sink,
            repo_id=ns.repo_id,
            repo_name=ns.repo_name,
            search_pattern=ns.search,
        )
        return 0
    except Exception as e:
        logger.error(f"Work item sync job failed: {e}")
        return 1
