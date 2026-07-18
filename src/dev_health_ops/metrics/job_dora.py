from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.active_incidents import (
    IncidentWindow,
    active_incidents_query,
    deduplicate_active_incidents,
)
from dev_health_ops.metrics.compute_dora import compute_dora_metrics_daily
from dev_health_ops.metrics.schemas import (
    DeploymentRow,
    IncidentRow,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.storage import detect_db_type
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    validate_sink,
)

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


def _utc_day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _repo_filter(
    params: dict[str, Any],
    *,
    org_id: str,
    repo_id: uuid.UUID | None,
    repo_name: str | None,
) -> str:
    """Build the repo-scoping clause and mutate *params* in place.

    Mirrors the ClickHouseDataLoader pattern: a ``repo_id`` filters directly,
    while a ``repo_name`` resolves to repo UUIDs via an org-scoped ``repos``
    subquery. The subquery is org-scoped so a name collision across tenants
    cannot pull in another org's repo. ``repo_id`` takes precedence when both
    are supplied.
    """
    if repo_id is not None:
        params["repo_id"] = str(repo_id)
        return " AND repo_id = {repo_id:UUID}"
    if repo_name is not None:
        params["repo_name"] = repo_name
        return (
            " AND repo_id IN ("
            "SELECT id FROM repos"
            " WHERE repo = {repo_name:String}"
            " AND org_id = {org_id:String})"
        )
    return ""


def _load_deployments(
    primary_sink: Any,
    *,
    org_id: str,
    start: datetime,
    end: datetime,
    repo_id: uuid.UUID | None,
    repo_name: str | None = None,
) -> list[DeploymentRow]:
    """Read deployments active in the day window from ClickHouse.

    ``deployments`` is a ReplacingMergeTree, so FINAL dedups pre-merge
    duplicates. The window matches the daily job (event time falls back to
    last_synced for in-flight rows).
    """
    params: dict[str, Any] = {"org_id": org_id, "start": start, "end": end}
    repo_filter = _repo_filter(
        params, org_id=org_id, repo_id=repo_id, repo_name=repo_name
    )

    rows = primary_sink.query_dicts(
        "SELECT repo_id, deployment_id, status, environment,"
        " started_at, finished_at, deployed_at, merged_at,"
        " pull_request_number"
        " FROM deployments FINAL"
        " WHERE org_id = {org_id:String}"
        "   AND coalesce(deployed_at, finished_at, started_at, last_synced)"
        "       >= {start:DateTime64(3, 'UTC')}"
        "   AND coalesce(deployed_at, finished_at, started_at, last_synced)"
        "       < {end:DateTime64(3, 'UTC')}"
        f"{repo_filter}",
        params,
    )
    return [r for r in rows if _has_valid_repo(r)]


def _load_incidents(
    primary_sink: Any,
    *,
    org_id: str,
    start: datetime,
    end: datetime,
    repo_id: uuid.UUID | None,
    repo_name: str | None = None,
) -> list[IncidentRow]:
    """Read incidents resolved in the day window from ClickHouse."""
    params: dict[str, Any] = {
        "org_id": org_id,
        "start": start,
        "end": end,
        "as_of": datetime.now(timezone.utc),
    }
    repo_filter = _repo_filter(
        params, org_id=org_id, repo_id=repo_id, repo_name=repo_name
    )

    rows = primary_sink.query_dicts(
        active_incidents_query(
            window=IncidentWindow.RESOLVED,
            org_id=org_id,
            repo_filter=repo_filter,
        ),
        params,
    )
    return deduplicate_active_incidents([r for r in rows if _has_valid_repo(r)])


def _has_valid_repo(row: dict[str, Any]) -> bool:
    try:
        uuid.UUID(str(row.get("repo_id")))
    except (ValueError, TypeError, AttributeError):
        return False
    return True


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
    org_id: str | None,
) -> None:
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    logger.info("Running DORA metrics for org_id=%s", org_id)
    backend = detect_db_type(db_url)
    sink = (sink or "auto").strip().lower()
    if sink == "auto":
        sink = backend

    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. Only ClickHouse is supported (CHAOS-641). "
            "Set CLICKHOUSE_URI and use a clickhouse:// connection string."
        )
    if sink not in {"clickhouse"}:
        raise ValueError("sink must be one of: auto, clickhouse")
    if sink != backend:
        raise ValueError(
            f"sink='{sink}' requires db backend '{sink}', got '{backend}'."
        )

    # CHAOS-2382: DORA is now provider-agnostic — the four metrics are derived
    # from already-synced ClickHouse rows (deployments / incidents) that both
    # the GitHub and GitLab processors write. No live provider fetch and no
    # GITLAB_TOKEN is required, so GitHub-only orgs no longer crash.
    del interval, gitlab_url, auth  # legacy GitLab-API knobs, retained for API compat

    resolved_org_id = (org_id or "").strip()

    days = _date_range(day, backfill_days)
    metrics_list = set(_parse_metrics(metrics))
    computed_at = datetime.now(timezone.utc)

    primary_sink: Any = ClickHouseMetricsSink(db_url)
    sinks: list[Any] = [primary_sink]
    for s in sinks:
        setattr(s, "org_id", resolved_org_id)

    try:
        for s in sinks:
            if hasattr(s, "ensure_tables"):
                s.ensure_tables()
            elif hasattr(s, "ensure_indexes"):
                s.ensure_indexes()

        for d in days:
            start, end = _utc_day_window(d)
            deployments = _load_deployments(
                primary_sink,
                org_id=resolved_org_id,
                start=start,
                end=end,
                repo_id=repo_id,
                repo_name=repo_name,
            )
            incidents = _load_incidents(
                primary_sink,
                org_id=resolved_org_id,
                start=start,
                end=end,
                repo_id=repo_id,
                repo_name=repo_name,
            )

            # org_id is auto-injected by the sink from its bound context
            # (ClickHouseCore._insert_rows), matching the daily-job pattern.
            rows = [
                row
                for row in compute_dora_metrics_daily(
                    day=d,
                    deployments=deployments,
                    incidents=incidents,
                    computed_at=computed_at,
                )
                if row.metric_name in metrics_list
            ]

            if rows:
                for s in sinks:
                    s.write_dora_metrics(rows)
                logger.info(
                    "DORA: wrote %d metric rows for org_id=%s day=%s",
                    len(rows),
                    resolved_org_id,
                    d.isoformat(),
                )
    finally:
        for s in sinks:
            try:
                s.close()
            except Exception:
                logger.exception("Error closing sink %s", type(s).__name__)


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    dora = subparsers.add_parser(
        "dora",
        help="Compute and persist DORA metrics from synced ClickHouse data.",
    )
    add_date_range_args(dora)
    dora.add_argument(
        "--repo-id", type=uuid.UUID, help="Filter to a specific repository UUID."
    )
    dora.add_argument("--repo-name", help="Filter to a specific repository by name.")
    add_sink_arg(dora)
    dora.add_argument(
        "--metrics",
        help="Comma-separated metric names to compute (default: full DORA set).",
    )
    # Legacy GitLab-API flags retained for backward compatibility; DORA is now
    # provider-agnostic and sourced from ClickHouse, so these are ignored.
    dora.add_argument(
        "--interval",
        default="daily",
        help=argparse.SUPPRESS,
    )
    dora.add_argument(
        "--gitlab-url",
        default=os.getenv("GITLAB_URL", "https://gitlab.com"),
        help=argparse.SUPPRESS,
    )
    dora.add_argument("--auth", help=argparse.SUPPRESS)
    dora.set_defaults(func=_cmd_metrics_dora)


def _cmd_metrics_dora(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        end_day, backfill_days = resolve_date_range(ns)
        run_dora_metrics_job(
            db_url=resolve_sink_uri(ns),
            day=end_day,
            backfill_days=backfill_days,
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
