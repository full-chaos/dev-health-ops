"""Daily metrics processing job."""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.compute_cicd import compute_cicd_metrics_daily
from dev_health_ops.metrics.compute_deployments import compute_deploy_metrics_daily
from dev_health_ops.metrics.compute_ic import (
    compute_ic_metrics_daily,
    compute_ic_landscape_rolling,
)
from dev_health_ops.metrics.compute_incidents import compute_incident_metrics_daily
from dev_health_ops.metrics.compute_wellbeing import (
    compute_team_wellbeing_metrics_daily,
)
from dev_health_ops.metrics.compute_work_items import compute_work_item_metrics_daily
from dev_health_ops.metrics.hotspots import compute_file_hotspots
from dev_health_ops.metrics.identity import (
    get_team_resolver,
    init_team_resolver,
    load_team_map,
)
from dev_health_ops.metrics.knowledge import (
    compute_bus_factor,
    compute_code_ownership_gini,
)
from dev_health_ops.metrics.loaders import DataLoader, to_utc
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.loaders.mongo import MongoDataLoader
from dev_health_ops.metrics.loaders.sqlalchemy import SqlAlchemyDataLoader
from dev_health_ops.metrics.quality import (
    compute_rework_churn_ratio,
    compute_single_owner_file_ratio,
)
from dev_health_ops.metrics.reviews import compute_review_edges_daily
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink
from dev_health_ops.metrics.sinks.postgres import PostgresMetricsSink
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.teams import build_repo_pattern_resolver
from dev_health_ops.storage import detect_db_type
from dev_health_ops.metrics.db_utils import (
    normalize_sqlite_url as _normalize_sqlite_url,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent

# Public aliases for backward compatibility
_to_utc = to_utc


def discover_repos(
    backend: str,
    primary_sink: Any,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
) -> List[Any]:
    """Discover repositories from the database."""
    from dev_health_ops.metrics.work_items import DiscoveredRepo

    # This is a simplified version of the original discovery logic
    # In a real implementation, this would query the 'repos' table via the sink/backend
    # For now, we'll delegate to the primary_sink's store if possible,
    # or implement enough logic to satisfy typical usage.

    # If a specific repo is requested, return just that one
    if repo_id:
        return [
            DiscoveredRepo(
                repo_id=repo_id,
                full_name=repo_name or str(repo_id),
                source="auto",
                settings={},
            )
        ]

    # Fallback: attempt to get all repos from sink
    try:
        # Most sinks have access to the underlying storage/client
        if backend == "clickhouse":
            rows = primary_sink.client.query(
                "SELECT id, repo, settings FROM repos"
            ).result_rows
            return [
                DiscoveredRepo(
                    repo_id=uuid.UUID(str(r[0])),
                    full_name=r[1],
                    source="auto",
                    settings=r[2] or {},
                )
                for r in rows
            ]
        elif backend == "mongo":
            cursor = primary_sink.db["repos"].find(
                {}, {"id": 1, "repo": 1, "settings": 1}
            )
            return [
                DiscoveredRepo(
                    repo_id=uuid.UUID(str(d.get("id") or d["_id"])),
                    full_name=d["repo"],
                    source="auto",
                    settings=d.get("settings") or {},
                )
                for d in cursor
            ]
        else:
            # SQLAlchemy/SQLite
            from sqlalchemy import text

            with primary_sink.engine.connect() as conn:
                rows = conn.execute(text("SELECT id, repo, settings FROM repos")).all()
                import json

                return [
                    DiscoveredRepo(
                        repo_id=uuid.UUID(str(r[0])),
                        full_name=r[1],
                        source="auto",
                        settings=json.loads(r[2])
                        if isinstance(r[2], str)
                        else (r[2] or {}),
                    )
                    for r in rows
                ]
    except Exception as exc:
        logger.warning("Repo discovery failed: %s", exc)
        return []


# Alias for backward compatibility
_discover_repos = discover_repos


async def _get_loader(db_url: str, backend: str) -> DataLoader:
    """Factory to create the appropriate DataLoader for the backend."""
    if backend == "clickhouse":
        from dev_health_ops.api.queries.client import get_global_client

        client = await get_global_client(db_url)
        return ClickHouseDataLoader(client)
    elif backend == "mongo":
        import pymongo

        client = pymongo.MongoClient(db_url)
        try:
            db = client.get_default_database()
        except Exception:
            db = client["mergestat"]
        return MongoDataLoader(db)
    elif backend in {"sqlite", "postgres"}:
        from sqlalchemy import create_engine

        if "sqlite+aiosqlite://" in db_url:
            db_url = db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
        elif "postgresql+asyncpg://" in db_url:
            db_url = db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        engine = create_engine(db_url)
        return SqlAlchemyDataLoader(engine)
    else:
        raise ValueError(f"No DataLoader implementation for backend: {backend}")


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _date_range(end_day: date, backfill_days: int) -> List[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def _secondary_uri_from_env() -> str:
    uri = os.getenv("SECONDARY_DATABASE_URI")
    if not uri:
        raise ValueError("SECONDARY_DATABASE_URI is not set")
    return uri


async def run_daily_metrics_job(
    *,
    db_url: Optional[str] = None,
    day: date,
    backfill_days: int,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
    include_commit_metrics: bool = True,
    sink: str = "auto",
    provider: str = "auto",
    org_id: str = "default",
    skip_finalize: bool = False,
) -> None:
    db_url = db_url or os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    logger.info("Running daily metrics for org_id=%s", org_id)
    backend = detect_db_type(db_url)
    sink = (sink or "auto").strip().lower()
    if sink == "auto":
        sink = backend

    days = _date_range(day, backfill_days)
    computed_at = datetime.now(timezone.utc)

    identity = load_identity_resolver()

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

    sinks = [primary_sink] + ([secondary_sink] if secondary_sink else [])

    for s in sinks:
        if hasattr(s, "ensure_tables"):
            s.ensure_tables()
        elif hasattr(s, "ensure_indexes"):
            s.ensure_indexes()

    await init_team_resolver(primary_sink)
    team_resolver = get_team_resolver()
    teams_data = await primary_sink.get_all_teams()
    repo_team_resolver = build_repo_pattern_resolver(teams_data)
    repo_names_by_id = {
        r.repo_id: r.full_name
        for r in discover_repos(
            backend=backend,
            primary_sink=primary_sink,
            repo_id=repo_id,
            repo_name=repo_name,
        )
    }

    loader = await _get_loader(db_url, backend)

    load_work_items_from_db = provider == "auto"
    load_work_items_enabled = provider != "none"

    business_tz = os.getenv("BUSINESS_TIMEZONE", "UTC")
    business_start = int(os.getenv("BUSINESS_HOURS_START", "9"))
    business_end = int(os.getenv("BUSINESS_HOURS_END", "17"))

    daily_commit_cache: Dict[date, List[Any]] = {}

    async def _get_cached_commits_for_window(
        window_start: date, window_end: date
    ) -> List[Any]:
        """Load commits for date range using per-day cache to avoid redundant fetches."""
        result = []
        current = window_start
        while current <= window_end:
            if current not in daily_commit_cache:
                d_start = datetime.combine(current, time.min, tzinfo=timezone.utc)
                d_end = d_start + timedelta(days=1)
                rows, _, _ = await loader.load_git_rows(
                    d_start, d_end, repo_id=repo_id, repo_name=repo_name
                )
                daily_commit_cache[current] = rows
            result.extend(daily_commit_cache[current])
            current += timedelta(days=1)
        return result

    for d in days:
        logger.info("Computing metrics for day=%s", d.isoformat())
        start, end = _utc_day_window(d)

        commit_rows, pr_rows, review_rows = await loader.load_git_rows(
            start, end, repo_id=repo_id, repo_name=repo_name
        )
        daily_commit_cache[d] = commit_rows

        pipeline_rows, deployment_rows = await loader.load_cicd_data(
            start, end, repo_id=repo_id, repo_name=repo_name
        )
        incident_rows = await loader.load_incidents(
            start, end, repo_id=repo_id, repo_name=repo_name
        )

        h_start_date = d - timedelta(days=29)
        h_commit_rows = await _get_cached_commits_for_window(h_start_date, d)

        work_items: List[Any] = []
        work_item_transitions: List[Any] = []
        if load_work_items_enabled and load_work_items_from_db:
            work_items, work_item_transitions = await loader.load_work_items(
                start, end, repo_id, repo_name
            )

        mttr_by_repo: Dict[uuid.UUID, float] = {}
        bug_times: Dict[uuid.UUID, List[float]] = {}
        for item in work_items:
            if item.type == "bug" and item.completed_at and item.started_at:
                comp_dt = _to_utc(item.completed_at)
                if start <= comp_dt < end:
                    rid = getattr(item, "repo_id", None)
                    if rid:
                        bug_times.setdefault(rid, []).append(
                            (comp_dt - _to_utc(item.started_at)).total_seconds()
                            / 3600.0
                        )
        for rid, times in bug_times.items():
            mttr_by_repo[rid] = sum(times) / len(times)

        # Build active_repos from ALL data sources, not just commits.
        # Repos with CI/CD or deployment data but no commits in the window
        # were previously excluded, causing missing metrics (gh-377).
        active_repos: Set[uuid.UUID] = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}
        rework_ratio_by_repo: Dict[uuid.UUID, float] = {}
        single_owner_ratio_by_repo: Dict[uuid.UUID, float] = {}
        bus_factor_by_repo: Dict[uuid.UUID, int] = {}
        gini_by_repo: Dict[uuid.UUID, float] = {}

        all_file_metrics = []
        for r_id in active_repos:
            rework_ratio_by_repo[r_id] = compute_rework_churn_ratio(
                repo_id=str(r_id), window_stats=h_commit_rows
            )
            single_owner_ratio_by_repo[r_id] = compute_single_owner_file_ratio(
                repo_id=str(r_id), window_stats=h_commit_rows
            )
            bus_factor_by_repo[r_id] = compute_bus_factor(
                repo_id=str(r_id), window_stats=h_commit_rows
            )
            gini_by_repo[r_id] = compute_code_ownership_gini(
                repo_id=str(r_id), window_stats=h_commit_rows
            )
            file_metrics = compute_file_hotspots(
                repo_id=r_id,
                day=d,
                window_stats=h_commit_rows,
                computed_at=computed_at,
            )
            all_file_metrics.extend(file_metrics)

        result = compute_daily_metrics(
            day=d,
            commit_stat_rows=commit_rows,
            pull_request_rows=pr_rows,
            pull_request_review_rows=review_rows,
            computed_at=computed_at,
            include_commit_metrics=include_commit_metrics,
            team_resolver=team_resolver,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
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
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
            computed_at=computed_at,
            business_timezone=business_tz,
            business_hours_start=business_start,
            business_hours_end=business_end,
        )

        wi_metrics = []
        wi_user_metrics = []
        wi_cycle_times = []
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

        review_edges = compute_review_edges_daily(
            day=d,
            pull_request_rows=pr_rows,
            pull_request_review_rows=review_rows,
            computed_at=computed_at,
        )
        cicd_metrics = compute_cicd_metrics_daily(
            day=d, pipeline_runs=pipeline_rows, computed_at=computed_at
        )
        deploy_metrics = compute_deploy_metrics_daily(
            day=d, deployments=deployment_rows, computed_at=computed_at
        )
        incident_metrics = compute_incident_metrics_daily(
            day=d, incidents=incident_rows, computed_at=computed_at
        )

        for s in sinks:
            s.write_repo_metrics(result.repo_metrics)
            s.write_user_metrics(result.user_metrics)
            if include_commit_metrics:
                s.write_commit_metrics(result.commit_metrics)
            s.write_team_metrics(team_metrics)
            if wi_metrics:
                s.write_work_item_metrics(wi_metrics)
            if wi_user_metrics:
                s.write_work_item_user_metrics(wi_user_metrics)
            if wi_cycle_times:
                s.write_work_item_cycle_times(wi_cycle_times)
            s.write_review_edges(review_edges)
            s.write_cicd_metrics(cicd_metrics)
            s.write_deploy_metrics(deploy_metrics)
            s.write_incident_metrics(incident_metrics)
            if all_file_metrics:
                s.write_file_metrics(all_file_metrics)

        if not skip_finalize:
            ic_metrics = compute_ic_metrics_daily(
                git_metrics=result.user_metrics,
                wi_metrics=wi_user_metrics,
                team_map=load_team_map(),
            )
            for s in sinks:
                s.write_user_metrics(ic_metrics)

            rolling_stats = await loader.load_user_metrics_rolling_30d(as_of=d)
            ic_landscape = compute_ic_landscape_rolling(
                as_of_day=d,
                rolling_stats=rolling_stats,
                team_map=load_team_map(),
            )
            for s in sinks:
                s.write_ic_landscape_rolling(ic_landscape)


async def run_daily_metrics_finalize(
    *,
    db_url: str,
    day: date,
    org_id: str = "default",
    sink: str = "auto",
) -> None:
    """Run only the IC finalize logic (IC metrics + landscape rolling).

    This is designed to run AFTER all per-repo batch tasks have persisted
    their user_metrics for the given *day*.  It loads the already-persisted
    user_metrics and work-item user metrics from the analytics store, then
    computes the cross-repo IC aggregates.

    The function sets up its own identity/team resolver since it may execute
    in a separate Celery worker.
    """
    if not db_url:
        db_url = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL") or ""
    if not db_url:
        raise ValueError("Database URI is required.")

    logger.info("Running IC finalize for day=%s org_id=%s", day.isoformat(), org_id)
    backend = detect_db_type(db_url)
    sink = (sink or "auto").strip().lower()
    if sink == "auto":
        sink = backend

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

    sinks_list = [primary_sink] + ([secondary_sink] if secondary_sink else [])

    for s in sinks_list:
        if hasattr(s, "ensure_tables"):
            s.ensure_tables()
        elif hasattr(s, "ensure_indexes"):
            s.ensure_indexes()

    await init_team_resolver(primary_sink)

    loader = await _get_loader(db_url, backend)

    import dataclasses as _dc

    from dev_health_ops.metrics.loaders.base import clickhouse_query_dicts
    from dev_health_ops.metrics.schemas import (
        UserMetricsDailyRecord,
        WorkItemUserMetricsDailyRecord,
    )

    git_metrics: List[Any] = []
    wi_user_metrics: List[Any] = []

    if backend == "clickhouse":
        from dev_health_ops.api.queries.client import get_global_client

        ch_client = await get_global_client(db_url)
        um_field_names = {f.name for f in _dc.fields(UserMetricsDailyRecord)}
        wi_field_names = {f.name for f in _dc.fields(WorkItemUserMetricsDailyRecord)}

        um_rows = clickhouse_query_dicts(
            ch_client,
            "SELECT * FROM user_metrics_daily WHERE day = {day:Date}",
            {"day": day},
        )
        for row in um_rows:
            try:
                git_metrics.append(
                    UserMetricsDailyRecord(
                        **{k: v for k, v in row.items() if k in um_field_names}
                    )
                )
            except Exception:
                logger.debug("Skipping malformed user_metrics row: %s", row)

        wi_rows = clickhouse_query_dicts(
            ch_client,
            "SELECT * FROM work_item_user_metrics_daily WHERE day = {day:Date}",
            {"day": day},
        )
        for row in wi_rows:
            try:
                wi_user_metrics.append(
                    WorkItemUserMetricsDailyRecord(
                        **{k: v for k, v in row.items() if k in wi_field_names}
                    )
                )
            except Exception:
                logger.debug("Skipping malformed wi_user_metrics row: %s", row)
    else:
        logger.warning(
            "Finalize currently optimised for ClickHouse; "
            "backend=%s may produce empty IC metrics.",
            backend,
        )

    ic_metrics = compute_ic_metrics_daily(
        git_metrics=git_metrics,
        wi_metrics=wi_user_metrics,
        team_map=load_team_map(),
    )
    for s in sinks_list:
        s.write_user_metrics(ic_metrics)

    rolling_stats = await loader.load_user_metrics_rolling_30d(as_of=day)
    ic_landscape = compute_ic_landscape_rolling(
        as_of_day=day,
        rolling_stats=rolling_stats,
        team_map=load_team_map(),
    )
    for s in sinks_list:
        s.write_ic_landscape_rolling(ic_landscape)

    logger.info("IC finalize complete for day=%s", day.isoformat())


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    daily = subparsers.add_parser("daily", help="Compute daily metrics.")
    daily.add_argument(
        "--day", type=date.fromisoformat, default=date.today().isoformat()
    )
    daily.add_argument("--backfill", type=int, default=1)
    daily.add_argument("--repo-id", type=uuid.UUID)
    daily.add_argument("--repo-name")
    daily.add_argument("--no-commits", dest="commit_metrics", action="store_false")
    daily.set_defaults(commit_metrics=True)
    daily.add_argument(
        "--sink",
        choices=["clickhouse", "mongo", "sqlite", "postgres", "both", "auto"],
        default="auto",
        help="Sink backend (mongo, sqlite, postgres deprecated for analytics; use clickhouse)",
    )
    daily.add_argument("--provider", default="auto")
    daily.set_defaults(func=_cmd_metrics_daily)

    rebuild = subparsers.add_parser(
        "rebuild",
        help="Rebuild metrics for specific repos with partitioned finalize.",
    )
    rebuild.add_argument(
        "--day", type=date.fromisoformat, required=True, help="Target date"
    )
    rebuild.add_argument("--backfill", type=int, default=1)
    rebuild.add_argument(
        "--repo-id",
        type=uuid.UUID,
        action="append",
        dest="repo_ids",
        default=[],
        help="Repo UUID (repeatable)",
    )
    rebuild.add_argument(
        "--sink",
        choices=["clickhouse", "mongo", "sqlite", "postgres", "both", "auto"],
        default="auto",
    )
    rebuild.add_argument("--provider", default="auto")
    rebuild.set_defaults(func=_cmd_metrics_rebuild)


async def _cmd_metrics_daily(ns: argparse.Namespace) -> int:
    try:
        await run_daily_metrics_job(
            db_url=resolve_sink_uri(ns),
            day=ns.day,
            backfill_days=ns.backfill,
            repo_id=ns.repo_id,
            repo_name=ns.repo_name,
            include_commit_metrics=ns.commit_metrics,
            sink=ns.sink,
            provider=ns.provider,
            org_id=getattr(ns, "org", "default") or "default",
        )
        return 0
    except Exception as e:
        logger.error(f"Daily metrics job failed: {e}")
        return 1


async def _cmd_metrics_rebuild(ns: argparse.Namespace) -> int:
    try:
        db_url = resolve_sink_uri(ns)
        org_id = getattr(ns, "org", "default") or "default"
        repo_ids: List[uuid.UUID] = ns.repo_ids or []
        days = _date_range(ns.day, ns.backfill)

        for d in days:
            if repo_ids:
                for rid in repo_ids:
                    logger.info("Rebuild batch: day=%s repo_id=%s", d, rid)
                    await run_daily_metrics_job(
                        db_url=db_url,
                        day=d,
                        backfill_days=1,
                        repo_id=rid,
                        sink=ns.sink,
                        provider=ns.provider,
                        org_id=org_id,
                        skip_finalize=True,
                    )
            else:
                logger.info("Rebuild batch: day=%s (all repos)", d)
                await run_daily_metrics_job(
                    db_url=db_url,
                    day=d,
                    backfill_days=1,
                    sink=ns.sink,
                    provider=ns.provider,
                    org_id=org_id,
                    skip_finalize=True,
                )

            logger.info("Rebuild finalize: day=%s", d)
            await run_daily_metrics_finalize(
                db_url=db_url,
                day=d,
                org_id=org_id,
                sink=ns.sink,
            )

        return 0
    except Exception as e:
        logger.error("Metrics rebuild failed: %s", e)
        return 1
