from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, List, Optional

from dev_health_ops.connectors import GitLabConnector
from dev_health_ops.connectors.exceptions import ConnectorException
from dev_health_ops.metrics.job_daily import (
    _discover_repos,
    _normalize_sqlite_url,
    _secondary_uri_from_env,
)
from dev_health_ops.metrics.schemas import DORAMetricsRecord
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink
from dev_health_ops.metrics.sinks.postgres import PostgresMetricsSink
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink
from dev_health_ops.metrics.work_items import DiscoveredRepo
from dev_health_ops.storage import detect_db_type

logger = logging.getLogger(__name__)

DEFAULT_DORA_METRICS = [
    "deployment_frequency",
    "lead_time_for_changes",
    "time_to_restore_service",
    "change_failure_rate",
]


def _date_range(end_day: date, backfill_days: int) -> List[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def _parse_metrics(raw_metrics: Optional[str]) -> List[str]:
    if not raw_metrics:
        return list(DEFAULT_DORA_METRICS)
    metrics = [m.strip() for m in raw_metrics.split(",") if m.strip()]
    return metrics or list(DEFAULT_DORA_METRICS)


def _is_gitlab_repo(repo: DiscoveredRepo, allow_unknown: bool) -> bool:
    source = ""
    if isinstance(repo.settings, dict):
        source = str(repo.settings.get("source") or "")
    if not source:
        source = str(repo.source or "")
    source = source.strip().lower()
    if source == "gitlab":
        return True
    return allow_unknown


def run_dora_metrics_job(
    *,
    db_url: str,
    day: date,
    backfill_days: int,
    repo_id: Optional[uuid.UUID] = None,
    repo_name: Optional[str] = None,
    sink: str = "auto",
    metrics: Optional[str] = None,
    interval: str = "daily",
    gitlab_url: Optional[str] = None,
    auth: Optional[str] = None,
) -> None:
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    backend = detect_db_type(db_url)
    sink = (sink or "auto").strip().lower()
    if sink == "auto":
        sink = backend

    if backend not in {"clickhouse", "mongo", "sqlite", "postgres"}:
        raise ValueError(f"Unsupported db backend for DORA metrics: {backend}")

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

    token = (auth or os.getenv("GITLAB_TOKEN") or "").strip()
    if not token:
        raise ValueError("GitLab token required (set GITLAB_TOKEN or pass --auth).")

    gitlab_url = gitlab_url or os.getenv("GITLAB_URL", "https://gitlab.com")

    days = _date_range(day, backfill_days)
    start_date = min(days).isoformat()
    end_date = max(days).isoformat()
    metrics_list = _parse_metrics(metrics)
    computed_at = datetime.now(timezone.utc)

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

    connector = GitLabConnector(url=gitlab_url, private_token=token)

    try:
        for s in sinks:
            if hasattr(s, "ensure_tables"):
                s.ensure_tables()
            elif hasattr(s, "ensure_indexes"):
                s.ensure_indexes()

        discovered_repos = _discover_repos(
            backend=backend,
            primary_sink=primary_sink,
            repo_id=repo_id,
            repo_name=repo_name,
        )

        allow_unknown = repo_id is not None or repo_name is not None

        for repo in discovered_repos:
            if not _is_gitlab_repo(repo, allow_unknown):
                continue

            rows: List[DORAMetricsRecord] = []
            for metric in metrics_list:
                try:
                    dora_metrics = connector.get_dora_metrics(
                        repo.full_name,
                        metric,
                        start_date=start_date,
                        end_date=end_date,
                        interval=interval,
                    )
                except ConnectorException as exc:
                    logger.warning(
                        "GitLab DORA metric fetch failed for %s (%s): %s",
                        repo.full_name,
                        metric,
                        exc,
                    )
                    continue

                for dp in dora_metrics.data_points:
                    rows.append(
                        DORAMetricsRecord(
                            repo_id=repo.repo_id,
                            day=dp.date.date(),
                            metric_name=metric,
                            value=float(dp.value),
                            computed_at=computed_at,
                        )
                    )

            for s in sinks:
                if rows:
                    s.write_dora_metrics(rows)
    finally:
        try:
            connector.close()
        except Exception:
            logger.exception("Error closing GitLab connector")
        for s in sinks:
            try:
                s.close()
            except Exception:
                logger.exception("Error closing sink %s", type(s).__name__)


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    dora = subparsers.add_parser(
        "dora",
        help="Fetch and persist DORA metrics from GitLab (supplemental).",
    )
    dora.add_argument("--db", help="Database connection string.")
    dora.add_argument(
        "--day",
        type=date.fromisoformat,
        default=date.today().isoformat(),
        help="Target day (YYYY-MM-DD).",
    )
    dora.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Fetch metrics for N days ending at --day.",
    )
    dora.add_argument("--repo-id", type=uuid.UUID)
    dora.add_argument("--repo-name")
    dora.add_argument(
        "--sink",
        choices=["clickhouse", "mongo", "sqlite", "postgres", "both", "auto"],
        default="auto",
    )
    dora.add_argument(
        "--metrics",
        help="Comma-separated metric names to fetch (default: GitLab DORA set).",
    )
    dora.add_argument(
        "--interval",
        default="daily",
        help="DORA interval (default: daily).",
    )
    dora.add_argument(
        "--gitlab-url",
        default=os.getenv("GITLAB_URL", "https://gitlab.com"),
        help="GitLab instance URL.",
    )
    dora.add_argument("--auth", help="GitLab token override.")
    dora.set_defaults(func=_cmd_metrics_dora)


def _cmd_metrics_dora(ns: argparse.Namespace) -> int:
    try:
        run_dora_metrics_job(
            db_url=ns.db,
            day=ns.day,
            backfill_days=ns.backfill,
            repo_id=ns.repo_id,
            repo_name=ns.repo_name,
            sink=ns.sink,
            metrics=ns.metrics,
            interval=ns.interval,
            gitlab_url=ns.gitlab_url,
            auth=ns.auth,
        )
        return 0
    except Exception as e:
        logger.error("DORA metrics job failed: %s", e)
        return 1
