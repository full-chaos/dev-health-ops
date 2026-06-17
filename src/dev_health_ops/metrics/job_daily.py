"""Daily metrics processing job."""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.audit.ai_governance.loaders import build_governance_rows_for_day
from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.ai_impact import compute_ai_impact_metrics_daily
from dev_health_ops.metrics.benchmarking.runner import run_benchmarking_for_day
from dev_health_ops.metrics.compounding_risk import build_compounding_risk_rows_for_day
from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.compute_cicd import compute_cicd_metrics_daily
from dev_health_ops.metrics.compute_deployments import compute_deploy_metrics_daily
from dev_health_ops.metrics.compute_ic import (
    compute_ic_landscape_rolling,
    compute_ic_metrics_daily,
)
from dev_health_ops.metrics.compute_incidents import compute_incident_metrics_daily
from dev_health_ops.metrics.compute_testops import (
    compute_coverage_metrics_daily,
    compute_pipeline_metrics_daily,
    compute_test_metrics_daily,
)
from dev_health_ops.metrics.compute_testops_risk import (
    compute_pipeline_stability,
    compute_quality_drag,
    compute_release_confidence,
)
from dev_health_ops.metrics.compute_wellbeing import (
    compute_team_wellbeing_metrics_daily,
)
from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.metrics.compute_work_items import (
    build_linked_issue_team_resolver,
    compute_work_item_metrics_daily,
    compute_work_item_team_attributions,
)
from dev_health_ops.metrics.dependencies import get_metrics_dependencies
from dev_health_ops.metrics.hotspots import (
    compute_file_hotspots,
    compute_file_risk_hotspots,
)
from dev_health_ops.metrics.identity import (
    get_team_resolver,
    init_team_resolver,
    load_team_map,
)
from dev_health_ops.metrics.job_compounding_risk import _fetch_repo_metrics_for_day
from dev_health_ops.metrics.knowledge import (
    compute_bus_factor,
    compute_code_ownership_gini,
)
from dev_health_ops.metrics.loaders import DataLoader, to_utc
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.quality import (
    compute_rework_churn_ratio,
    compute_single_owner_file_ratio,
)
from dev_health_ops.metrics.reviews import compute_review_edges_daily
from dev_health_ops.metrics.schemas import FileComplexitySnapshot
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.work_items import DiscoveredRepo
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.teams import (
    build_project_key_resolver,
    build_repo_pattern_resolver,
)
from dev_health_ops.storage import detect_db_type
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    validate_sink,
)
from dev_health_ops.work_graph.extractors.ai_workflow import (
    extract_ai_workflow_from_pull_requests,
    extract_review_deployment_incident_edges,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent

# Public aliases for backward compatibility
_to_utc = to_utc


def discover_repos(
    backend: str,
    primary_sink: Any,
    repo_id: uuid.UUID | None = None,
    repo_name: str | None = None,
    org_id: str = "",
    provider: str = "auto",
) -> list[Any]:
    """Discover repositories from the database."""
    # If a specific repo is requested, return just that one
    if repo_id:
        return [
            DiscoveredRepo(
                repo_id=repo_id,
                full_name=repo_name or str(repo_id),
                source=provider,
                settings={},
            )
        ]

    # Query repos from ClickHouse, scoped by org_id
    try:
        query = "SELECT id, repo, settings, provider FROM repos"
        params: dict[str, str] = {}
        if org_id:
            query += " WHERE org_id = {org_id:String}"
            params["org_id"] = org_id
        rows = primary_sink.client.query(query, parameters=params).result_rows
        return [
            DiscoveredRepo(
                repo_id=uuid.UUID(str(r[0])),
                full_name=r[1],
                source=r[3] if len(r) > 3 and r[3] != "unknown" else provider,
                settings=r[2] or {},
            )
            for r in rows
        ]
    except Exception as exc:
        logger.warning("Repo discovery failed: %s", exc)
        return []


# Backward-compat alias used by job_dora and job_work_items
_discover_repos = discover_repos


async def _get_loader(db_url: str, backend: str, org_id: str = "") -> DataLoader:
    """Factory to create the ClickHouse DataLoader."""
    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. Only ClickHouse is supported (CHAOS-641). "
            "Set CLICKHOUSE_URI and use a clickhouse:// connection string."
        )
    deps = get_metrics_dependencies()
    client = await deps.get_global_client(db_url)
    return ClickHouseDataLoader(client, org_id=org_id)


def _utc_day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _date_range(end_day: date, backfill_days: int) -> list[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def _extract_ai_workflow_for_day(
    *,
    primary_sink: Any,
    org_id: str,
    start: datetime,
    end: datetime,
    repo_id: uuid.UUID | None,
    repo_provider_by_id: dict[str, str],
) -> tuple[list[Any], list[Any], list[Any], list[Any], list[Any], list[Any]]:
    """Extract AI workflow runs and Work Graph edges for one UTC day window.

    Returns ``(runs, artifact_edges, issue_edges, review_outcome_edges,
    pr_deployment_edges, deployment_incident_edges)``.
    Returns six empty lists when ``org_id`` is not a UUID — AIWorkflowRun
    requires a tenant UUID by contract, so extraction without one would
    fabricate attribution (CHAOS-2187).

    Deployment↔incident correlation is day-scoped (CHAOS-2367): an incident
    links natively when the deployment row carries its PR number, and
    heuristically (confidence 0.3) to same-repo deployments within the same
    UTC day otherwise.
    """
    org_uuid: uuid.UUID | None = None
    if org_id:
        try:
            org_uuid = uuid.UUID(org_id)
        except ValueError:
            org_uuid = None
    if org_uuid is None:
        logger.debug("AI workflow extraction skipped: org_id %r is not a UUID", org_id)
        return [], [], [], [], [], []

    wf_params: dict[str, Any] = {"org_id": org_id, "start": start, "end": end}
    wf_repo_filter = ""
    if repo_id is not None:
        wf_params["repo_id"] = str(repo_id)
        wf_repo_filter = " AND repo_id = {repo_id:UUID}"

    wf_pr_rows = primary_sink.query_dicts(
        "SELECT repo_id, number, title, body, head_branch,"
        " author_name, author_email, created_at, merged_at,"
        " closed_at, last_synced"
        " FROM git_pull_requests"
        " WHERE org_id = {org_id:String}"
        "   AND ((created_at >= {start:DateTime64(3, 'UTC')}"
        "         AND created_at < {end:DateTime64(3, 'UTC')})"
        "    OR (merged_at IS NOT NULL"
        "        AND merged_at >= {start:DateTime64(3, 'UTC')}"
        "        AND merged_at < {end:DateTime64(3, 'UTC')}))"
        f"{wf_repo_filter}",
        wf_params,
    )

    # Row-local hygiene: drop rows whose repo_id/number cannot parse instead
    # of letting one malformed row abort the whole day (the extractor calls
    # UUID() on every row). Mirrors the pr_commit_stats per-row handling.
    def _valid_rows(rows: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
        valid: list[dict[str, Any]] = []
        dropped = 0
        for row in rows:
            try:
                uuid.UUID(str(row.get("repo_id")))
                int(row.get("number"))  # type: ignore[arg-type]
            except (ValueError, TypeError, AttributeError):
                dropped += 1
                continue
            valid.append(row)
        if dropped:
            logger.warning(
                "AI workflow extraction dropped %d malformed %s row(s)",
                dropped,
                source,
            )
        return valid

    wf_pr_rows = _valid_rows(wf_pr_rows, "git_pull_requests")

    issue_ids_by_pr: dict[str, list[str]] = {}
    wf_pr_numbers = sorted({int(row["number"]) for row in wf_pr_rows})
    if wf_pr_numbers:
        link_params: dict[str, Any] = {
            "org_id": org_id,
            "pr_numbers": wf_pr_numbers,
        }
        link_repo_filter = ""
        if repo_id is not None:
            link_params["repo_id"] = str(repo_id)
            link_repo_filter = " AND repo_id = {repo_id:UUID}"
        link_rows = primary_sink.query_dicts(
            "SELECT repo_id, pr_number, work_item_id"
            " FROM work_graph_issue_pr"
            " WHERE org_id = {org_id:String}"
            "   AND pr_number IN {pr_numbers:Array(UInt32)}"
            f"{link_repo_filter}",
            link_params,
        )
        for link in link_rows:
            wi_id = str(link.get("work_item_id") or "")
            link_repo = str(link.get("repo_id") or "")
            link_number = link.get("pr_number")
            if not wi_id or not link_repo or link_number is None:
                continue
            issue_ids_by_pr.setdefault(f"{link_repo}:{int(link_number)}", []).append(
                wi_id
            )

    wf_review_rows = primary_sink.query_dicts(
        "SELECT repo_id, number, review_id, state, submitted_at, last_synced"
        " FROM git_pull_request_reviews"
        " WHERE org_id = {org_id:String}"
        "   AND submitted_at >= {start:DateTime64(3, 'UTC')}"
        "   AND submitted_at < {end:DateTime64(3, 'UTC')}"
        f"{wf_repo_filter}",
        wf_params,
    )
    wf_review_rows = _valid_rows(wf_review_rows, "git_pull_request_reviews")

    # Deployments/incidents feed the PR→deployment and deployment→incident
    # Work Graph edges (CHAOS-2367). Their identity is repo_id + an opaque
    # string id, so they get their own row hygiene instead of _valid_rows
    # (which requires a PR number).
    def _valid_id_rows(
        rows: list[dict[str, Any]], id_key: str, source: str
    ) -> list[dict[str, Any]]:
        valid: list[dict[str, Any]] = []
        dropped = 0
        for row in rows:
            try:
                uuid.UUID(str(row.get("repo_id")))
            except (ValueError, TypeError, AttributeError):
                dropped += 1
                continue
            if not str(row.get(id_key) or ""):
                dropped += 1
                continue
            valid.append(row)
        if dropped:
            logger.warning(
                "AI workflow extraction dropped %d malformed %s row(s)",
                dropped,
                source,
            )
        return valid

    # Event time falls back to last_synced (non-nullable) so in-flight
    # deployments with no timestamps yet still land in a day bucket instead
    # of silently never matching any window. FINAL: deployments/incidents
    # are ReplacingMergeTree and may hold pre-merge duplicate rows during
    # active sync.
    wf_deployment_rows = primary_sink.query_dicts(
        "SELECT repo_id, deployment_id, pull_request_number,"
        " started_at, finished_at, deployed_at, last_synced"
        " FROM deployments FINAL"
        " WHERE org_id = {org_id:String}"
        "   AND coalesce(deployed_at, finished_at, started_at, last_synced)"
        "       >= {start:DateTime64(3, 'UTC')}"
        "   AND coalesce(deployed_at, finished_at, started_at, last_synced)"
        "       < {end:DateTime64(3, 'UTC')}"
        f"{wf_repo_filter}",
        wf_params,
    )
    wf_deployment_rows = _valid_id_rows(
        wf_deployment_rows, "deployment_id", "deployments"
    )

    wf_incident_rows = primary_sink.query_dicts(
        "SELECT repo_id, incident_id, started_at, last_synced"
        " FROM incidents FINAL"
        " WHERE org_id = {org_id:String}"
        "   AND started_at >= {start:DateTime64(3, 'UTC')}"
        "   AND started_at < {end:DateTime64(3, 'UTC')}"
        f"{wf_repo_filter}",
        wf_params,
    )
    wf_incident_rows = _valid_id_rows(wf_incident_rows, "incident_id", "incidents")

    def _by_provider(
        rows: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            row_provider = repo_provider_by_id.get(
                str(row.get("repo_id") or ""), "unknown"
            )
            grouped.setdefault(row_provider, []).append(row)
        return grouped

    prs_by_provider = _by_provider(wf_pr_rows)
    reviews_by_provider = _by_provider(wf_review_rows)
    deployments_by_provider = _by_provider(wf_deployment_rows)
    incidents_by_provider = _by_provider(wf_incident_rows)

    runs: list[Any] = []
    artifact_edges: list[Any] = []
    issue_edges: list[Any] = []
    review_outcome_edges: list[Any] = []
    pr_deployment_edges: list[Any] = []
    deployment_incident_edges: list[Any] = []
    for wf_provider, provider_prs in prs_by_provider.items():
        extraction = extract_ai_workflow_from_pull_requests(
            provider_prs,
            org_id=org_uuid,
            provider=wf_provider,
            issue_ids_by_pr=issue_ids_by_pr,
        )
        runs.extend(extraction.runs)
        artifact_edges.extend(extraction.artifact_edges)
        issue_edges.extend(extraction.issue_edges)
    edge_providers = (
        set(reviews_by_provider)
        | set(deployments_by_provider)
        | set(incidents_by_provider)
    )
    for wf_provider in sorted(edge_providers):
        review_extraction = extract_review_deployment_incident_edges(
            org_id=org_uuid,
            provider=wf_provider,
            reviews=reviews_by_provider.get(wf_provider),
            deployments=deployments_by_provider.get(wf_provider),
            incidents=incidents_by_provider.get(wf_provider),
        )
        review_outcome_edges.extend(review_extraction.review_outcome_edges)
        pr_deployment_edges.extend(review_extraction.pr_deployment_edges)
        deployment_incident_edges.extend(review_extraction.deployment_incident_edges)
    return (
        runs,
        artifact_edges,
        issue_edges,
        review_outcome_edges,
        pr_deployment_edges,
        deployment_incident_edges,
    )


def _repo_to_team_map_for_compounding_risk(
    *,
    repo_metrics_rows: list[Any],
    repo_names_by_id: dict[uuid.UUID, str],
    repo_team_resolver: Any,
) -> dict[str, str]:
    repo_to_team_map: dict[str, str] = {}
    for row in repo_metrics_rows:
        row_repo_id = getattr(row, "repo_id", None)
        if row_repo_id is None:
            continue
        full_name = repo_names_by_id.get(row_repo_id)
        if not full_name:
            continue
        team_id, _ = repo_team_resolver.resolve(full_name)
        if team_id:
            repo_to_team_map[str(row_repo_id)] = team_id
    return repo_to_team_map


def _write_compounding_risk_for_day(
    *,
    sinks: list[Any],
    primary_sink: Any,
    day: date,
    org_id: str,
    repo_metrics_rows: list[Any],
    computed_at: datetime,
    repo_names_by_id: dict[uuid.UUID, str],
    repo_team_resolver: Any,
) -> int:
    rows_for_compounding = list(repo_metrics_rows)
    if not rows_for_compounding:
        rows_for_compounding = _fetch_repo_metrics_for_day(primary_sink, org_id, day)
    if not rows_for_compounding:
        return 0

    try:
        repo_to_team_map = _repo_to_team_map_for_compounding_risk(
            repo_metrics_rows=rows_for_compounding,
            repo_names_by_id=repo_names_by_id,
            repo_team_resolver=repo_team_resolver,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("repo_team_resolver failed for compounding risk: %s", exc)
        repo_to_team_map = {}

    compounding_rows = build_compounding_risk_rows_for_day(
        sink=primary_sink,
        day=day,
        org_id=org_id,
        repo_metrics_rows=rows_for_compounding,
        computed_at=computed_at,
        repo_to_team=repo_to_team_map or None,
    )
    if not compounding_rows:
        return 0
    for s in sinks:
        s.write_compounding_risk_daily(compounding_rows)
    return len(compounding_rows)


def _secondary_uri_from_env() -> str:
    uri = os.getenv("SECONDARY_DATABASE_URI")
    if not uri:
        raise ValueError("SECONDARY_DATABASE_URI is not set")
    return uri


def _hotspot_repo_ids(
    active_repos: set[uuid.UUID],
    discovered_repo_ids: Iterable[uuid.UUID],
) -> set[uuid.UUID]:
    """Repos eligible for the live ``file_hotspot_daily`` risk pass.

    The risk-hotspot computation must NOT be gated on same-day activity:
    ``compute_file_risk_hotspots`` unions complexity-only files with churned
    files, so a discovered repo whose risk comes from static complexity (no
    commits/pipelines/deployments that day) must still produce rows. Returning
    ``active_repos`` UNION every discovered repo ensures idle complexity-only
    repos are covered; the compute returns no rows for repos with neither churn
    nor complexity, so empty repos are never fabricated (CHAOS-2376 round-4).
    """
    return set(active_repos) | set(discovered_repo_ids)


def _load_complexity_map_for_repo(
    *,
    primary_sink: Any,
    org_id: str,
    repo_id: uuid.UUID,
    day: date,
) -> dict[str, FileComplexitySnapshot]:
    """Load the latest complexity snapshot per file for ``repo_id`` on or before
    ``day`` from ``file_complexity_snapshots``.

    ``file_complexity_snapshots`` is written by the separate complexity job
    (``metrics complexity``); this read joins that compute into the daily
    hotspot/risk computation (CHAOS-2376). Selects, per file, the snapshot with
    the latest ``as_of_day`` on or before ``day`` (breaking ties by
    ``computed_at``) via ``argMax(*, (as_of_day, computed_at))``. The temporal
    key MUST lead with ``as_of_day`` -- keying on ``computed_at`` alone would
    let an older ``as_of_day`` that was *backfilled/recomputed later* clobber a
    newer snapshot and persist stale risk_score/cyclomatic into
    ``file_hotspot_daily`` (CHAOS-2376 round-2). This mirrors the
    ``max(as_of_day)``-first invariant in ``get_file_complexity_snapshots``.
    Returns an empty map (callers treat complexity as 0) on any query failure so
    a missing or unmigrated table never aborts the daily job.
    """
    query = """
        SELECT
            file_path,
            argMax(language,                       (as_of_day, computed_at)) AS language,
            argMax(loc,                            (as_of_day, computed_at)) AS loc,
            argMax(functions_count,                (as_of_day, computed_at)) AS functions_count,
            argMax(cyclomatic_total,               (as_of_day, computed_at)) AS cyclomatic_total,
            argMax(cyclomatic_avg,                 (as_of_day, computed_at)) AS cyclomatic_avg,
            argMax(high_complexity_functions,      (as_of_day, computed_at)) AS high_complexity_functions,
            argMax(very_high_complexity_functions, (as_of_day, computed_at)) AS very_high_complexity_functions
        FROM file_complexity_snapshots
        WHERE repo_id = {repo_id:UUID}
          AND as_of_day <= {day:Date}
    """
    params: dict[str, Any] = {"repo_id": str(repo_id), "day": day}
    if org_id:
        query += "\n          AND org_id = {org_id:String}"
        params["org_id"] = org_id
    query += "\n        GROUP BY file_path"

    try:
        rows = primary_sink.query_dicts(query, params)
    except Exception as exc:
        logger.warning(
            "Complexity snapshot load failed for repo_id=%s day=%s: %s",
            repo_id,
            day,
            exc,
        )
        return {}

    complexity_map: dict[str, FileComplexitySnapshot] = {}
    for row in rows:
        path = row.get("file_path")
        if not path:
            continue
        complexity_map[path] = FileComplexitySnapshot(
            repo_id=repo_id,
            as_of_day=day,
            ref="",
            file_path=path,
            language=row.get("language") or "",
            loc=int(row.get("loc") or 0),
            functions_count=int(row.get("functions_count") or 0),
            cyclomatic_total=int(row.get("cyclomatic_total") or 0),
            cyclomatic_avg=float(row.get("cyclomatic_avg") or 0.0),
            high_complexity_functions=int(row.get("high_complexity_functions") or 0),
            very_high_complexity_functions=int(
                row.get("very_high_complexity_functions") or 0
            ),
            computed_at=datetime.now(timezone.utc),
            org_id=org_id,
        )
    return complexity_map


def _load_blame_map_for_repo(
    *,
    primary_sink: Any,
    org_id: str,
    repo_id: uuid.UUID,
) -> dict[str, float]:
    """Load per-file ownership concentration for ``repo_id`` from ``git_blame``.

    Concentration is the share of currently-blamed lines attributed to the
    single largest contributor for each file (a max-share / dominant-owner
    metric in ``[0, 1]``). This surfaces the Ownership/blame dimension of the
    risk hotspot (CHAOS-2376): a value near ``1.0`` means one author owns
    almost all lines (bus-factor risk), near ``0`` means broad ownership.

    The aggregation is pushed server-side. ``git_blame`` is
    ``ReplacingMergeTree(last_synced)`` keyed by ``(org_id, repo_id, path,
    line_no)`` (migration 027 prepends ``org_id`` to the sorting key and adds
    the ``org_id`` column), so a per-line ``argMax(author, last_synced)``
    collapses re-synced lines to their latest author before the per-file share
    is computed. The read is scoped by BOTH ``org_id`` and ``repo_id``: blame
    rows are tenant-partitioned, and a stale/default-org row for a reused
    ``repo_id`` must not contaminate another tenant's ownership data
    (CHAOS-2376 round-2: cross-org leak). Returns an empty map (callers treat
    concentration as ``NULL``) on any query failure so a missing/unmigrated
    table never aborts the daily job.
    """
    query = """
        SELECT
            path,
            max(author_lines) / sum(author_lines) AS concentration
        FROM
        (
            SELECT
                path,
                author,
                count() AS author_lines
            FROM
            (
                SELECT
                    path,
                    line_no,
                    argMax(
                        coalesce(author_email, author_name, ''),
                        last_synced
                    ) AS author
                FROM git_blame
                WHERE repo_id = {repo_id:UUID}
    """
    params: dict[str, Any] = {"repo_id": str(repo_id)}
    if org_id:
        query += "                  AND org_id = {org_id:String}\n"
        params["org_id"] = org_id
    query += """                GROUP BY path, line_no
            )
            WHERE author != ''
            GROUP BY path, author
        )
        GROUP BY path
    """

    try:
        rows = primary_sink.query_dicts(query, params)
    except Exception as exc:
        logger.warning(
            "Blame map load failed for repo_id=%s: %s",
            repo_id,
            exc,
        )
        return {}

    blame_map: dict[str, float] = {}
    for row in rows:
        path = row.get("path")
        if not path:
            continue
        concentration = row.get("concentration")
        if concentration is None:
            continue
        blame_map[path] = float(concentration)
    return blame_map


async def run_daily_metrics_job(
    *,
    db_url: str | None = None,
    day: date,
    backfill_days: int,
    repo_id: uuid.UUID | None = None,
    repo_name: str | None = None,
    include_commit_metrics: bool = True,
    sink: str = "auto",
    provider: str = "auto",
    org_id: str,
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

    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. Only ClickHouse is supported (CHAOS-641). "
            "Set CLICKHOUSE_URI and use a clickhouse:// connection string."
        )
    primary_sink = ClickHouseMetricsSink(db_url)

    sinks = [primary_sink]

    # Propagate org_id to sinks for auto-injection into metric records.
    for s in sinks:
        setattr(s, "org_id", org_id)

    for s in sinks:
        if hasattr(s, "ensure_tables"):
            s.ensure_tables()

    await init_team_resolver(primary_sink)
    team_resolver = get_team_resolver()
    teams_data = await primary_sink.get_all_teams()
    repo_team_resolver = build_repo_pattern_resolver(teams_data)
    # CHAOS-2377: project-key team attribution for the work-item state-duration
    # rollup. Mirrors job_work_items: team-owned-by-project-key items that are
    # unassigned (or assigned to unmapped users) must still land under their
    # team, not the normalized "unassigned" bucket.
    project_key_resolver = build_project_key_resolver(teams_data)
    discovered_repos = discover_repos(
        backend=backend,
        primary_sink=primary_sink,
        repo_id=repo_id,
        repo_name=repo_name,
        org_id=org_id,
    )
    repo_names_by_id = {r.repo_id: r.full_name for r in discovered_repos}
    # Provider per repo for AI workflow extraction (CHAOS-2187). Falls back to
    # "unknown" so a missing provider never blocks edge extraction.
    repo_provider_by_id = {
        str(r.repo_id): (r.source or "unknown") for r in discovered_repos
    }

    loader = await _get_loader(db_url, backend, org_id=org_id)

    load_work_items_from_db = provider == "auto"
    load_work_items_enabled = provider != "none"

    business_tz = os.getenv("BUSINESS_TIMEZONE", "UTC")
    business_start = int(os.getenv("BUSINESS_HOURS_START", "9"))
    business_end = int(os.getenv("BUSINESS_HOURS_END", "17"))

    daily_commit_cache: dict[date, list[Any]] = {}

    async def _get_cached_commits_for_window(
        window_start: date, window_end: date
    ) -> list[Any]:
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

    # Rolling buffer for pipeline stability (7-day window)
    pipeline_metrics_buffer: list[Any] = []

    # Work-item dependency edges are org-scoped and time-independent (a PR's
    # link to the issue it closes does not expire), so load them once for the
    # whole run rather than per day. They power linked-issue team inheritance.
    # Defensive getattr: loaders without the method (or deployments missing the
    # table) simply skip inheritance instead of failing the daily job.
    work_item_dependencies: list[Any] = []
    linked_issue_resolver = None
    team_attribution_context = None
    # Linked-issue inheritance reads org-wide (repo_id=None), so it is only
    # safe under an explicit tenant scope: without org_id the loader's filter
    # is empty and the donor/edge queries would span every tenant, letting a
    # PR inherit another org's team. Production workers always pass org_id;
    # an unscoped (dev/CLI) run simply skips inheritance.
    if load_work_items_enabled and load_work_items_from_db and days and org_id:
        # Build the linked-issue inheritance resolver ONCE for the run. The
        # donor set is bounded to the work items actually referenced by a
        # dependency edge (not the tenant's whole history) and the read is
        # best-effort: a failure degrades to no inheritance rather than
        # aborting the daily job. A PR can reference a donor that completed
        # before any metrics day, or a repo-less Linear/Jira issue, so the
        # bounded lookup is org-wide and window-independent.
        _load_attr_context = getattr(loader, "load_team_attribution_context", None)
        if _load_attr_context is not None:
            try:
                team_attribution_context = await _load_attr_context(as_of=computed_at)
            except Exception:
                logger.warning(
                    "Team attribution context load failed; using legacy resolvers only",
                    exc_info=True,
                )
        _load_deps = getattr(loader, "load_work_item_dependencies", None)
        _load_donors = getattr(loader, "load_work_item_dependencies_donors", None)
        if _load_deps is not None and _load_donors is not None:
            try:
                # Bound the dependency read to edges whose SOURCE is a work item
                # evaluated this run — load the run-window items once to collect
                # those source ids — so this is never a full-graph scan on the
                # critical daily path.
                run_start = datetime.combine(min(days), time.min, tzinfo=timezone.utc)
                run_end = _utc_day_window(max(days))[1]
                run_items, _ = await loader.load_work_items(
                    run_start, run_end, repo_id, repo_name
                )
                source_ids = {wi.work_item_id for wi in run_items}
                work_item_dependencies = (
                    await _load_deps(source_ids) if source_ids else []
                )
                _target_ids: set[str] = set()
                _issue_keys: set[str] = set()
                for _dep in work_item_dependencies:
                    _t = _dep.target_work_item_id
                    if _t.startswith("extkey:"):
                        _issue_keys.add(_t.split(":", 1)[1])
                    elif _t:
                        _target_ids.add(_t)
                donor_items = await _load_donors(_target_ids, _issue_keys)
                linked_issue_resolver = build_linked_issue_team_resolver(
                    work_items=donor_items,
                    dependencies=work_item_dependencies,
                    team_resolver=team_resolver,
                    project_key_resolver=project_key_resolver,
                    attribution_context=team_attribution_context,
                )
            except Exception:
                logger.warning(
                    "Linked-issue donor load failed; skipping inheritance for this run",
                    exc_info=True,
                )
                linked_issue_resolver = None

    for d in days:
        logger.info("Computing metrics for day=%s", d.isoformat())
        start, end = _utc_day_window(d)

        commit_rows, pr_rows, review_rows = await loader.load_git_rows(
            start, end, repo_id=repo_id, repo_name=repo_name
        )
        daily_commit_cache[d] = commit_rows

        testops_loader: Any = loader
        pipeline_rows, deployment_rows = await loader.load_cicd_data(
            start, end, repo_id=repo_id, repo_name=repo_name
        )
        h_start_date = d - timedelta(days=29)
        (
            testops_pipeline_rows,
            testops_job_rows,
        ) = await testops_loader.load_testops_pipeline_data(start, end, repo_id=repo_id)
        (
            testops_suite_rows,
            testops_case_rows,
        ) = await testops_loader.load_testops_test_data(
            datetime.combine(h_start_date, time.min, tzinfo=timezone.utc),
            end,
            repo_id=repo_id,
        )
        coverage_rows = await testops_loader.load_testops_coverage_data(
            start, end, repo_id=repo_id
        )
        prior_coverage_rows = await testops_loader.load_testops_coverage_data(
            datetime.combine(d - timedelta(days=30), time.min, tzinfo=timezone.utc),
            start,
            repo_id=repo_id,
        )
        incident_rows = await loader.load_incidents(
            start, end, repo_id=repo_id, repo_name=repo_name
        )

        h_commit_rows = await _get_cached_commits_for_window(h_start_date, d)

        work_items: list[Any] = []
        work_item_transitions: list[Any] = []
        if load_work_items_enabled and load_work_items_from_db:
            work_items, work_item_transitions = await loader.load_work_items(
                start, end, repo_id, repo_name
            )

        mttr_by_repo: dict[uuid.UUID, float] = {}
        bug_times: dict[uuid.UUID, list[float]] = {}
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
        active_repos: set[uuid.UUID] = {r["repo_id"] for r in commit_rows}
        active_repos |= {r["repo_id"] for r in pipeline_rows if "repo_id" in r}
        active_repos |= {r["repo_id"] for r in deployment_rows if "repo_id" in r}
        rework_ratio_by_repo: dict[uuid.UUID, float] = {}
        single_owner_ratio_by_repo: dict[uuid.UUID, float] = {}
        bus_factor_by_repo: dict[uuid.UUID, int] = {}
        gini_by_repo: dict[uuid.UUID, float] = {}

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

        # file_hotspot_daily (risk treemap + hotspot drilldown on /complexity)
        # is computed live here by merging the 30d churn window with the latest
        # complexity snapshot per file, so real OAuth orgs get data instead of
        # only fixtures (CHAOS-2376).
        #
        # The risk-hotspot pass is NOT gated on active_repos: a repo's risk can
        # come purely from static complexity (compute_file_risk_hotspots unions
        # complexity-only files with churned files), and discovered repos can
        # have complexity snapshots with zero same-day commits/pipelines/
        # deployments -- common right after onboarding or on quiet-but-risky
        # repos. Gating on active_repos there left /complexity empty/stale for
        # those repos. Iterate over active_repos UNION all discovered repos so
        # idle complexity-only repos still produce rows; compute_file_risk_
        # hotspots returns [] when a repo has neither churn nor complexity, so
        # this never fabricates rows for genuinely empty repos (CHAOS-2376
        # round-4).
        all_file_hotspots = []
        hotspot_repos = _hotspot_repo_ids(active_repos, repo_names_by_id)
        for r_id in hotspot_repos:
            complexity_map = _load_complexity_map_for_repo(
                primary_sink=primary_sink,
                org_id=org_id,
                repo_id=r_id,
                day=d,
            )
            # Ownership concentration per file from git_blame (backfilled on
            # onboarding) feeds blame_concentration so the /complexity
            # Ownership-risk dimension is non-NULL for real orgs (CHAOS-2376).
            blame_map = _load_blame_map_for_repo(
                primary_sink=primary_sink,
                org_id=org_id,
                repo_id=r_id,
            )
            file_hotspots = compute_file_risk_hotspots(
                repo_id=r_id,
                day=d,
                window_stats=h_commit_rows,
                complexity_map=complexity_map,
                blame_map=blame_map,
                computed_at=computed_at,
            )
            all_file_hotspots.extend(file_hotspots)

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

        wi_metrics: list[Any] = []
        wi_user_metrics: list[Any] = []
        wi_cycle_times: list[Any] = []
        wi_team_attributions: list[Any] = []
        wi_state_durations: list[Any] = []
        if work_items:
            wi_metrics, wi_user_metrics, wi_cycle_times = (
                compute_work_item_metrics_daily(
                    day=d,
                    work_items=work_items,
                    transitions=work_item_transitions,
                    computed_at=computed_at,
                    team_resolver=team_resolver,
                    project_key_resolver=project_key_resolver,
                    linked_issue_resolver=linked_issue_resolver,
                    attribution_context=team_attribution_context,
                )
            )
            wi_team_attributions = compute_work_item_team_attributions(
                work_items=work_items,
                computed_at=computed_at,
                team_resolver=team_resolver,
                project_key_resolver=project_key_resolver,
                linked_issue_resolver=linked_issue_resolver,
                attribution_context=team_attribution_context,
            )
            # CHAOS-2377: the state-duration rollup powers /metrics Flow Sankey +
            # Flame and the Operating Review state-duration panel. The compute
            # already exists (and is used by the fixtures runner + job_work_items)
            # but was never invoked in the live scheduled daily job, so the table
            # stayed empty for real orgs. Reuse the work_items / transitions
            # already loaded for this day.
            wi_state_durations = compute_work_item_state_durations_daily(
                day=d,
                work_items=work_items,
                transitions=work_item_transitions,
                computed_at=computed_at,
                team_resolver=team_resolver,
                project_key_resolver=project_key_resolver,
                linked_issue_resolver=linked_issue_resolver,
                attribution_context=team_attribution_context,
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
        testops_pipeline_metrics = compute_pipeline_metrics_daily(
            day=d,
            pipeline_runs=testops_pipeline_rows,
            job_runs=testops_job_rows,
            computed_at=computed_at,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
        testops_test_metrics = compute_test_metrics_daily(
            day=d,
            suite_results=testops_suite_rows,
            case_results=testops_case_rows,
            computed_at=computed_at,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
        testops_coverage_metrics = compute_coverage_metrics_daily(
            day=d,
            snapshots=coverage_rows,
            prior_snapshots=prior_coverage_rows,
            computed_at=computed_at,
            repo_team_resolver=repo_team_resolver,
            repo_names_by_id=repo_names_by_id,
        )
        deploy_metrics = compute_deploy_metrics_daily(
            day=d, deployments=deployment_rows, computed_at=computed_at
        )
        incident_metrics = compute_incident_metrics_daily(
            day=d, incidents=incident_rows, computed_at=computed_at
        )
        ai_policy_events, ai_governance_coverage = build_governance_rows_for_day(
            primary_sink, org_id=org_id, day=d
        )
        ai_attribution_rows = []
        ai_loader: Any = loader
        if hasattr(ai_loader, "load_ai_pr_attributions"):
            ai_attribution_rows = await ai_loader.load_ai_pr_attributions(
                start=start,
                end=end,
                repo_id=repo_id,
            )

        # CHAOS-2187: extract AI workflow runs + Work Graph edges from today's
        # PRs/reviews so ai_workflow_issue_edges, ai_workflow_artifact_edges,
        # and work_graph_pr_review_outcome_edges are populated by ingestion.
        # Infrastructure failures (ClickHouse query errors) propagate and fail
        # the job: there is no persisted job-health table to record a partial
        # day, and empty edge tables are indistinguishable from "no AI
        # activity today" — swallowing here would be silent partial data.
        # Row-local issues (malformed repo ids) are skipped inside the helper,
        # mirroring the per-row handling in the pr_commit_stats build below.
        (
            ai_workflow_runs,
            ai_workflow_artifact_edges,
            ai_workflow_issue_edges,
            ai_review_outcome_edges,
            ai_pr_deployment_edges,
            ai_deployment_incident_edges,
        ) = _extract_ai_workflow_for_day(
            primary_sink=primary_sink,
            org_id=org_id,
            start=start,
            end=end,
            repo_id=repo_id,
            repo_provider_by_id=repo_provider_by_id,
        )

        # Build pr_commit_stats: {(repo_id, pr_number) -> [{"file_path": ...}]} so that
        # compute_ai_impact_metrics_daily can determine which PRs touched test files.
        #
        # Design notes (CHAOS-2183):
        #  • We join work_graph_pr_commit with git_commit_stats rather than using the
        #    day-scoped commit_rows — a PR merged today may have test commits from prior
        #    days (window-mismatch false-gap bug).
        #  • Query is bounded to today's in-window PR numbers (not all-time), so the
        #    scan is proportional to the batch size, not the full table.
        #  • LEFT JOIN ensures PRs whose commits have no file-stat rows still appear in
        #    the result (they get file_path=NULL → has_test_change=False, a real gap).
        #  • UUID parsing is per-row so one malformed row is skipped, not fatal.
        #  • On any outer exception, pr_commit_stats stays None and ai_impact treats
        #    test_gap as unavailable (None), preventing the 100%-inflation false alarm.
        pr_commit_stats: dict[tuple[uuid.UUID, int], list[Any]] | None = None
        try:
            # Identify which PRs fall inside today's UTC window (mirrors the logic in
            # compute_ai_impact_metrics_daily so the sets are consistent).
            in_window_prs: set[tuple[str, int]] = set()
            for pr in pr_rows:
                merged_at_raw = pr.get("merged_at")
                event_at = _to_utc(
                    merged_at_raw if merged_at_raw is not None else pr["created_at"]
                )
                if start <= event_at < end:
                    in_window_prs.add((str(pr["repo_id"]), int(pr["number"])))

            if in_window_prs:
                # Scope to just today's PR numbers (+ optional repo filter).
                pr_numbers: list[int] = list({pr_num for _, pr_num in in_window_prs})
                pc_params: dict[str, Any] = {
                    "org_id": org_id,
                    "pr_numbers": pr_numbers,
                }
                pc_repo_filter = ""
                if repo_id is not None:
                    pc_params["repo_id"] = str(repo_id)
                    pc_repo_filter = " AND p.repo_id = {repo_id:UUID}"

                # LEFT JOIN so PRs with commits that have no file stats still appear
                # (file_path=NULL → not a test path → has_test_change=False for that PR).
                # commit_hash + committer_when (from git_commits, org-scoped) feed
                # follow-up-commit derivation (CHAOS-2437); committer_when is
                # de-duplicated per commit downstream so RMT version rows are
                # harmless. git_commit_stats carries no org_id column, so its join
                # stays on (repo_id, commit_hash) -- p is already org-scoped by the
                # WHERE clause.
                raw_link_rows = primary_sink.query_dicts(
                    "SELECT p.repo_id, p.pr_number, p.commit_hash, p.evidence,"
                    " c.committer_when, s.file_path"
                    " FROM work_graph_pr_commit AS p"
                    " LEFT JOIN git_commit_stats AS s"
                    "   ON s.repo_id = p.repo_id AND s.commit_hash = p.commit_hash"
                    " LEFT JOIN git_commits AS c"
                    "   ON c.repo_id = p.repo_id AND c.hash = p.commit_hash"
                    "   AND c.org_id = p.org_id"
                    f" WHERE p.org_id = {{org_id:String}}{pc_repo_filter}"
                    "   AND p.pr_number IN {pr_numbers:Array(UInt32)}",
                    pc_params,
                )

                built: dict[tuple[uuid.UUID, int], list[Any]] = {}
                for link in raw_link_rows:
                    rid_str = str(link.get("repo_id") or "")
                    pr_num_raw = link.get("pr_number")
                    if not rid_str or pr_num_raw is None:
                        continue
                    pr_num = int(pr_num_raw)
                    # Filter cross-repo collisions (pr_number is per-repo, not global).
                    if (rid_str, pr_num) not in in_window_prs:
                        continue
                    try:
                        rid = uuid.UUID(rid_str)
                    except (ValueError, AttributeError):
                        # One malformed row → skip it, don't abort the whole build.
                        logger.debug(
                            "Skipping malformed repo_id in work_graph_pr_commit: %r",
                            rid_str,
                        )
                        continue
                    built.setdefault((rid, pr_num), []).append(
                        {
                            "file_path": link.get("file_path"),
                            "commit_hash": link.get("commit_hash"),
                            "committer_when": link.get("committer_when"),
                            "evidence": link.get("evidence"),
                        }
                    )
                pr_commit_stats = built
            else:
                pr_commit_stats = {}

        except Exception as exc:
            logger.warning(
                "pr_commit_stats build failed, test_gap_rate unavailable for day=%s: %s",
                d,
                exc,
            )
            # pr_commit_stats stays None → _test_changes_by_pr returns {} → every PR
            # gets has_test_change=None → test_gap_rate=None (unavailable, not 100%).

        ai_impact_metrics = compute_ai_impact_metrics_daily(
            day=d,
            org_id=org_id,
            pull_request_rows=pr_rows,
            pull_request_review_rows=review_rows,
            ai_attribution_rows=ai_attribution_rows,
            incident_rows=incident_rows,
            commit_stat_rows=commit_rows,
            computed_at=computed_at,
            team_resolver=lambda _repo_id, repo_name, _identity: (
                repo_team_resolver.resolve(repo_name)
            ),
            repo_names_by_id=repo_names_by_id,
            pr_commit_stats=pr_commit_stats,
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
            if wi_team_attributions and hasattr(s, "write_work_item_team_attributions"):
                s.write_work_item_team_attributions(wi_team_attributions)
            if wi_state_durations:
                s.write_work_item_state_durations(wi_state_durations)
            s.write_review_edges(review_edges)
            s.write_cicd_metrics(cicd_metrics)
            s.write_testops_pipeline_metrics(testops_pipeline_metrics)
            s.write_testops_test_metrics(testops_test_metrics)
            s.write_testops_coverage_metrics(testops_coverage_metrics)
            s.write_deploy_metrics(deploy_metrics)
            s.write_incident_metrics(incident_metrics)
            s.write_ai_policy_events(ai_policy_events)
            s.write_ai_governance_coverage_daily(ai_governance_coverage)
            if ai_impact_metrics:
                s.write_ai_impact_metrics(ai_impact_metrics)
            if ai_workflow_runs and hasattr(s, "write_ai_workflow_runs"):
                s.write_ai_workflow_runs(ai_workflow_runs)
            if ai_workflow_artifact_edges and hasattr(
                s, "write_ai_workflow_artifact_edges"
            ):
                s.write_ai_workflow_artifact_edges(ai_workflow_artifact_edges)
            if ai_workflow_issue_edges and hasattr(s, "write_ai_workflow_issue_edges"):
                s.write_ai_workflow_issue_edges(ai_workflow_issue_edges)
            if ai_review_outcome_edges and hasattr(
                s, "write_work_graph_pr_review_outcome_edges"
            ):
                s.write_work_graph_pr_review_outcome_edges(ai_review_outcome_edges)
            if ai_pr_deployment_edges and hasattr(
                s, "write_work_graph_pr_deployment_edges"
            ):
                s.write_work_graph_pr_deployment_edges(ai_pr_deployment_edges)
            if ai_deployment_incident_edges and hasattr(
                s, "write_work_graph_deployment_incident_edges"
            ):
                s.write_work_graph_deployment_incident_edges(
                    ai_deployment_incident_edges
                )
            if all_file_metrics:
                s.write_file_metrics(all_file_metrics)
            if all_file_hotspots and hasattr(s, "write_file_hotspot_daily"):
                s.write_file_hotspot_daily(all_file_hotspots)

        _write_compounding_risk_for_day(
            sinks=sinks,
            primary_sink=primary_sink,
            day=d,
            org_id=org_id,
            repo_metrics_rows=result.repo_metrics,
            computed_at=computed_at,
            repo_names_by_id=repo_names_by_id,
            repo_team_resolver=repo_team_resolver,
        )

        # TestOps risk metrics (release confidence, quality drag, pipeline stability)
        release_conf = compute_release_confidence(
            day=d,
            pipeline_metrics=testops_pipeline_metrics,
            test_metrics=testops_test_metrics,
            coverage_metrics=testops_coverage_metrics,
            computed_at=computed_at,
        )
        quality_drag = compute_quality_drag(
            day=d,
            pipeline_metrics=testops_pipeline_metrics,
            test_metrics=testops_test_metrics,
            computed_at=computed_at,
        )
        pipeline_metrics_buffer.extend(testops_pipeline_metrics)
        # Keep only the last 7 days of pipeline metrics
        cutoff = d - timedelta(days=6)
        pipeline_metrics_buffer = [
            m for m in pipeline_metrics_buffer if m.day >= cutoff
        ]
        pipeline_stab = compute_pipeline_stability(
            day=d,
            pipeline_metrics_7d=pipeline_metrics_buffer,
            computed_at=computed_at,
        )
        for s in sinks:
            if release_conf:
                s.write_release_confidence(release_conf)
            if quality_drag:
                s.write_quality_drag(quality_drag)
            if pipeline_stab:
                s.write_pipeline_stability(pipeline_stab)

        # Benchmarking (baselines, maturity, anomalies, period comparisons,
        # correlations, insights). Reads from ClickHouse via the sink.
        for s in sinks:
            try:
                run_benchmarking_for_day(
                    s,
                    as_of_day=d,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            except Exception as exc:
                logger.warning("Benchmarking run failed for day=%s: %s", d, exc)

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
    org_id: str,
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

    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. Only ClickHouse is supported (CHAOS-641). "
            "Set CLICKHOUSE_URI and use a clickhouse:// connection string."
        )
    primary_sink = ClickHouseMetricsSink(db_url)

    sinks_list = [primary_sink]

    # Propagate org_id to sinks for auto-injection into metric records.
    for s in sinks_list:
        setattr(s, "org_id", org_id)

    for s in sinks_list:
        if hasattr(s, "ensure_tables"):
            s.ensure_tables()

    await init_team_resolver(primary_sink)

    loader = await _get_loader(db_url, backend, org_id=org_id)

    import dataclasses as _dc

    deps = get_metrics_dependencies()

    git_metrics: list[Any] = []
    wi_user_metrics: list[Any] = []

    if backend == "clickhouse":
        ch_client = await deps.get_global_client(db_url)
        um_field_names = {f.name for f in _dc.fields(deps.user_metrics_daily_record)}
        wi_field_names = {
            f.name for f in _dc.fields(deps.work_item_user_metrics_daily_record)
        }

        um_query = "SELECT * FROM user_metrics_daily WHERE day = {day:Date}"
        um_params: dict[str, Any] = {"day": day}
        if org_id:
            um_query += " AND org_id = {org_id:String}"
            um_params["org_id"] = org_id
        um_rows = deps.clickhouse_query_dicts(
            ch_client,
            um_query,
            um_params,
        )
        for row in um_rows:
            try:
                git_metrics.append(
                    deps.user_metrics_daily_record(
                        **{k: v for k, v in row.items() if k in um_field_names}
                    )
                )
            except Exception:
                logger.debug("Skipping malformed user_metrics row: %s", row)

        wi_query = "SELECT * FROM work_item_user_metrics_daily WHERE day = {day:Date}"
        wi_params: dict[str, Any] = {"day": day}
        if org_id:
            wi_query += " AND org_id = {org_id:String}"
            wi_params["org_id"] = org_id
        wi_rows = deps.clickhouse_query_dicts(
            ch_client,
            wi_query,
            wi_params,
        )
        for row in wi_rows:
            try:
                wi_user_metrics.append(
                    deps.work_item_user_metrics_daily_record(
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
    add_date_range_args(daily)
    daily.add_argument(
        "--repo-id", type=uuid.UUID, help="Filter to a specific repository UUID."
    )
    daily.add_argument("--repo-name", help="Filter to a specific repository by name.")
    daily.add_argument(
        "--no-commits",
        dest="commit_metrics",
        action="store_false",
        help="Skip per-commit metrics; compute work-item and derived metrics only.",
    )
    daily.set_defaults(commit_metrics=True)
    add_sink_arg(daily)
    daily.add_argument(
        "--provider",
        default="auto",
        help="Restrict to a single provider (default: auto = all providers).",
    )
    daily.set_defaults(func=_cmd_metrics_daily)

    rebuild = subparsers.add_parser(
        "rebuild",
        help=(
            "Recompute daily metrics for one or more repos (or all repos) over a "
            "date range, then run a single partitioned finalize per day. Use after "
            "correcting or re-syncing source data for specific repositories."
        ),
    )
    add_date_range_args(rebuild)
    rebuild.add_argument(
        "--repo-id",
        type=uuid.UUID,
        action="append",
        dest="repo_ids",
        default=[],
        help="Repo UUID to rebuild; repeatable. Omit to rebuild all repos.",
    )
    add_sink_arg(rebuild)
    rebuild.add_argument(
        "--provider",
        default="auto",
        help="Restrict to a single provider (default: auto = all providers).",
    )
    rebuild.set_defaults(func=_cmd_metrics_rebuild)


async def _cmd_metrics_daily(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        end_day, backfill_days = resolve_date_range(ns)
        await run_daily_metrics_job(
            db_url=resolve_sink_uri(ns),
            day=end_day,
            backfill_days=backfill_days,
            repo_id=ns.repo_id,
            repo_name=ns.repo_name,
            include_commit_metrics=ns.commit_metrics,
            sink=ns.sink,
            provider=ns.provider,
            org_id=getattr(ns, "org", None) or "",
        )
        return 0
    except Exception as e:
        logger.error(f"Daily metrics job failed: {e}")
        return 1


async def _cmd_metrics_rebuild(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        end_day, backfill_days = resolve_date_range(ns)
        db_url = resolve_sink_uri(ns)
        org_id = getattr(ns, "org", None) or ""
        repo_ids: list[uuid.UUID] = ns.repo_ids or []
        days = _date_range(end_day, backfill_days)

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
