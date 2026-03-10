from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dev_health_ops.connectors import GitLabConnector
from dev_health_ops.connectors.exceptions import ConnectorException
from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.job_daily import (
    _discover_repos,
)
from dev_health_ops.metrics.schemas import DORAMetricsRecord
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.work_items import DiscoveredRepo
from dev_health_ops.storage import detect_db_type
from dev_health_ops.utils.datetime import utc_today

logger = logging.getLogger(__name__)

DEFAULT_DORA_METRICS = [
    "deployment_frequency",
    "lead_time_for_changes",
    "time_to_restore_service",
    "change_failure_rate",
]


def _date_range(end_day: date, backfill_days: int) -> list[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def _parse_metrics(raw_metrics: str | None) -> list[str]:
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
    repo_id: uuid.UUID | None = None,
    repo_name: str | None = None,
    sink: str = "auto",
    metrics: str | None = None,
    interval: str = "daily",
    gitlab_url: str | None = None,
    auth: str | None = None,
    org_id: str,
) -> None:
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    logger.info("Running DORA metrics for org_id=%s", org_id)
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

    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. Only ClickHouse is supported (CHAOS-641). "
            "Set CLICKHOUSE_URI and use a clickhouse:// connection string."
        )
    primary_sink = ClickHouseMetricsSink(db_url)

    sinks: list[Any] = [primary_sink]
    for s in sinks:
        s.org_id = org_id  # type: ignore[attr-defined]

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
            org_id=org_id,
        )

        allow_unknown = repo_id is not None or repo_name is not None

        for repo in discovered_repos:
            if not _is_gitlab_repo(repo, allow_unknown):
                continue

            rows: list[DORAMetricsRecord] = []
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
    dora.add_argument(
        "--day",
        type=date.fromisoformat,
        default=utc_today().isoformat(),
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
        help="Sink backend (mongo, sqlite, postgres deprecated for analytics; use clickhouse)",
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
            db_url=resolve_sink_uri(ns),
            day=ns.day,
            backfill_days=ns.backfill,
            repo_id=ns.repo_id,
            repo_name=ns.repo_name,
            sink=ns.sink,
            metrics=ns.metrics,
            interval=ns.interval,
            gitlab_url=ns.gitlab_url,
            auth=ns.auth,
            org_id=getattr(ns, "org", None),
        )
        return 0
    except Exception as e:
        logger.error("DORA metrics job failed: %s", e)
        return 1
