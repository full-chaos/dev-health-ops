"""Daily metrics processing job."""

from __future__ import annotations

import gc
import json
import logging
import os
import uuid
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.clickhouse_dedup import dedup_from
from dev_health_ops.metrics.benchmarking.runner import run_benchmarking_for_day
from dev_health_ops.metrics.compounding_risk import build_compounding_risk_rows_for_day
from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.compute_cicd import compute_cicd_metrics_daily
from dev_health_ops.metrics.compute_deployments import compute_deploy_metrics_daily
from dev_health_ops.metrics.compute_incidents import compute_incident_metrics_daily
from dev_health_ops.metrics.compute_wellbeing import (
    compute_team_wellbeing_metrics_daily,
)
from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.metrics.compute_work_items import (
    build_linked_issue_team_resolver,
    compute_estimate_coverage_metrics_daily,
    compute_work_item_metrics_daily,
)
from dev_health_ops.metrics.dependencies import get_metrics_dependencies
from dev_health_ops.metrics.identity import (
    get_team_resolver,
    init_team_resolver,
)
from dev_health_ops.metrics.job_compounding_risk import _fetch_repo_metrics_for_day
from dev_health_ops.metrics.knowledge import (
    compute_bus_factor,
    compute_code_ownership_gini,
)
from dev_health_ops.metrics.loaders import DataLoader, to_utc
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.prometheus import (
    record_metrics_family_zero_rows,
    record_team_metrics_daily_repo_rows,
)
from dev_health_ops.metrics.quality import (
    compute_rework_churn_ratio,
    compute_single_owner_file_ratio,
)
from dev_health_ops.metrics.reviews import compute_review_edges_daily
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.work_items import DiscoveredRepo
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.teams import (
    build_project_key_resolver,
    build_repo_pattern_resolver,
    load_team_repo_ownership_map,
)
from dev_health_ops.storage import detect_db_type

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

    # Query repos from ClickHouse, scoped by org_id.
    #
    # ``repos`` is a ReplacingMergeTree(last_synced) ordered by (org_id, id)
    # (migration 027). ``insert_repo`` always writes a fresh row per sync
    # rather than short-circuiting on an existing row (CHAOS-1775), so
    # multiple logical versions of the same (org_id, id) routinely coexist
    # until a background merge collapses them -- a plain ``SELECT *`` here
    # returns those pre-merge duplicates as separate DiscoveredRepo entries,
    # causing duplicate per-project fetches downstream (CHAOS-2787). Dedup
    # server-side to the latest row per (org_id, id) via argMax(*, last_synced)
    # rather than relying on background merges or FINAL.
    #
    # All three projected columns (repo, settings, provider) MUST come from
    # the SAME winning physical row. Three independent
    # argMax(col, last_synced) aggregates each pick a tied-row winner
    # *independently* when two versions share the exact same last_synced --
    # realistic here, since ``last_synced`` is only DateTime64(3) and
    # ``insert_repo`` stamps it from ``datetime.now()``, so rapid re-syncs of
    # the same (org_id, id) can land in the same millisecond. That would let
    # discover_repos synthesize a Frankenstein row (e.g. a new repo name with
    # a stale provider). Instead, collapse to a SINGLE
    # ``argMax(tuple(repo, settings, provider), last_synced)`` -- exactly one
    # row is chosen as "latest", and the three values are unwrapped from that
    # one row via ``tupleElement``, guaranteeing internal consistency. Ties
    # resolve to an arbitrary but internally-consistent version (matching
    # ReplacingMergeTree's own tie semantics); a deterministic tie-breaker
    # beyond that is not required here.
    #
    # ``settings`` is Nullable(String): a bare argMax(settings, last_synced)
    # SKIPS NULL values entirely, so an older *non-NULL* settings value would
    # incorrectly mask a genuinely NULL settings value on the latest row.
    # Wrapping the whole projection in tuple(...) sidesteps this too -- the
    # outer tuple is never NULL itself even when its ``settings`` element is,
    # so argMax compares/carries it correctly, and tupleElement(...) then
    # unwraps each value, NULL and all.
    try:
        query = (
            "SELECT id, "
            "tupleElement(latest, 1) AS repo, "
            "tupleElement(latest, 2) AS settings, "
            "tupleElement(latest, 3) AS provider "
            "FROM ("
            "SELECT id, "
            "argMax(tuple(repo, settings, provider), last_synced) AS latest "
            "FROM repos"
        )
        params: dict[str, str] = {}
        if org_id:
            query += " WHERE org_id = {org_id:String}"
            params["org_id"] = org_id
        query += " GROUP BY org_id, id)"
        rows = primary_sink.client.query(query, parameters=params).result_rows
        return [
            DiscoveredRepo(
                repo_id=uuid.UUID(str(r[0])),
                full_name=r[1],
                source=r[3] if len(r) > 3 and r[3] != "unknown" else provider,
                settings=_parse_repo_settings(r[2]),
            )
            for r in rows
        ]
    except Exception as exc:
        logger.warning("Repo discovery failed: %s", exc)
        return []


def _parse_repo_settings(raw: object) -> dict[str, Any]:
    """Parse the ClickHouse ``repos.settings`` column into a dict.

    ``settings`` is stored as a JSON string (Nullable(String)); the
    ``DiscoveredRepo.settings: dict[str, object]`` annotation was previously
    lying — this returned the raw string unparsed, so any per-provider match
    on a settings key (e.g. CHAOS-2763's gitlab ``project_id`` scoping) would
    always miss on production data. ``None``/malformed JSON/a non-dict JSON
    value all yield ``{}`` so downstream numeric-id matching fails closed
    instead of raising.
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


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


# CHAOS-5216/CHAOS-5234/CHAOS-3092/CHAOS-5242: _extract_ai_workflow_for_day
# (work_graph_edges' review/deployment/incident extraction, CHAOS-4286) is
# DELETED -- chris's standing rule (CHAOS-5233): once a family's Go executor
# is on main, its Python compute is deleted, never skip-gated.
# WorkGraphEdgesExecutor (native Go) is now the only computer/writer of
# work_graph_pr_review_outcome_edges/work_graph_pr_deployment_edges/
# work_graph_deployment_incident_edges, closing CHAOS-5216 by construction
# (single native reader; the Go executor reads git_pull_request_reviews
# FINAL, this Python path never had that fix and never will -- port-with-fix,
# standing order). This function's OTHER half (ai_workflow_runs/
# _artifact_edges/_issue_edges, via extract_ai_workflow_from_pull_requests)
# was already deleted by CHAOS-5242 (#2307) -- both halves are now gone, so
# the function itself, extract_review_deployment_incident_edges,
# extract_ai_workflow_from_pull_requests, AIWorkflowExtractionResult and the
# whole work_graph/extractors/ai_workflow.py module are deleted together in
# this same PR -- rg confirmed no remaining caller once this deletion and
# #2307's were combined.


def _repo_to_team_map_for_compounding_risk(
    *,
    repo_metrics_rows: list[Any],
    repo_names_by_id: dict[uuid.UUID, str],
    repo_team_resolver: Any,
    team_repo_ownership_map: dict[str, str] | None = None,
    org_id: str = "",
    day: date | None = None,
) -> dict[str, str]:
    """Resolve one team per repo for compounding-risk team-scope rows.

    CHAOS-4365: ``team_repo_ownership_map`` (explicit ``team_repo_ownership``
    rows, repo_id-keyed) wins where it resolves a repo -- it is populated for
    every native GitHub/GitLab/Jira/Linear auto-import, unlike
    ``teams.repo_patterns`` (glob strings the pattern resolver reads, which
    those imports never set). The pattern resolver remains the fallback for
    repos it doesn't cover (fixtures orgs, manually configured team globs).

    CHAOS-4365 codex round 2 (P1): a repo is trusted from EITHER source only
    when it also appears in ``repo_names_by_id`` -- the current ``repos``
    catalog for this run. ``team_repo_ownership`` rows never expire on their
    own (writers only ever INSERT; CHAOS-2610 tracks writer-side ``valid_to``
    retirement), so a repo removed/renamed after auto-import last ran can
    still carry a stale ownership row; without this guard that row would
    attribute a team-scope compounding-risk row to a repo that no longer
    exists in the org's current inventory -- something the pattern-resolver
    path never did, since it always required ``repo_names_by_id`` first.
    """
    ownership_map = team_repo_ownership_map or {}
    repo_to_team_map: dict[str, str] = {}
    # CHAOS-4365 telemetry: which source actually resolved each repo, so a
    # future "team rows still zero" report is diagnosable from the run's own
    # log line (ownership vs pattern vs neither) instead of a fresh
    # investigation into whether the wiring itself regressed.
    resolved_via_ownership = 0
    resolved_via_pattern = 0
    unresolved = 0
    for row in repo_metrics_rows:
        row_repo_id = getattr(row, "repo_id", None)
        if row_repo_id is None:
            continue
        repo_id_str = str(row_repo_id)
        full_name = repo_names_by_id.get(row_repo_id)
        if not full_name:
            # Not in the current repos catalog -- neither source is trusted
            # (matches the pattern-only path's pre-existing behavior).
            unresolved += 1
            continue
        team_id = ownership_map.get(repo_id_str)
        if team_id:
            resolved_via_ownership += 1
        else:
            team_id, _ = repo_team_resolver.resolve(full_name)
            if team_id:
                resolved_via_pattern += 1
            else:
                unresolved += 1
        if team_id:
            repo_to_team_map[repo_id_str] = team_id
    logger.info(
        "compounding-risk: repo-to-team resolution org_id=%s day=%s via_ownership=%d "
        "via_pattern=%d unresolved=%d",
        org_id,
        day.isoformat() if day else None,
        resolved_via_ownership,
        resolved_via_pattern,
        unresolved,
    )
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
    """Write REPO-scope compounding-risk rows only.

    CHAOS-4365 finalize-step fix: this runs once per repo (CHAOS-4264's
    one-repo-at-a-time partition loop), so it can never see a team's other
    repos. It used to also resolve and emit a team-scope row here anyway --
    for a team owning 2+ repos, each partition wrote its OWN "complete" team
    row from only that one repo's inputs, and argMax(computed_at) dedup then
    kept whichever repo's partition happened to run last, silently dropping
    every other owned repo's contribution (confirmed live: contributing_repo_
    count stuck at 1 for a 2-repo team). Team-scope rows are now written
    exactly once per org/day, from run_daily_metrics_finalize, after every
    repo's partition has landed -- see the team-aggregation block there.
    """
    rows_for_compounding = list(repo_metrics_rows)
    if not rows_for_compounding:
        rows_for_compounding = _fetch_repo_metrics_for_day(primary_sink, org_id, day)
    if not rows_for_compounding:
        return 0

    compounding_rows = build_compounding_risk_rows_for_day(
        sink=primary_sink,
        day=day,
        org_id=org_id,
        repo_metrics_rows=rows_for_compounding,
        computed_at=computed_at,
        repo_to_team=None,
    )
    if not compounding_rows:
        return 0
    for s in sinks:
        s.write_compounding_risk_daily(compounding_rows)
    return len(compounding_rows)


def _write_compounding_risk_team_rows_for_day(
    *,
    sinks: list[Any],
    primary_sink: Any,
    day: date,
    org_id: str,
    repo_names_by_id: dict[uuid.UUID, str],
    repo_team_resolver: Any,
    computed_at: datetime,
) -> int:
    """Write TEAM-scope compounding-risk rows for the WHOLE org/day.

    CHAOS-4365 finalize-step fix (moved out of the per-repo path -- see
    ``_write_compounding_risk_for_day``'s docstring). Reads back every
    repo's ``repo_metrics_daily`` row for this org/day from ClickHouse
    (``_fetch_repo_metrics_for_day``, already argMax-deduped) rather than
    accumulating in-process across partitions, so it is correct regardless
    of how many separate per-repo calls already landed. Must run AFTER all
    of this day's per-repo partitions have written their repo-scope rows.
    """
    org_repo_metrics = _fetch_repo_metrics_for_day(primary_sink, org_id, day)
    if not org_repo_metrics:
        return 0

    try:
        as_of = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
        team_repo_ownership_map = load_team_repo_ownership_map(
            primary_sink, org_id, as_of=as_of
        )
        repo_to_team_map = _repo_to_team_map_for_compounding_risk(
            repo_metrics_rows=org_repo_metrics,
            repo_names_by_id=repo_names_by_id,
            repo_team_resolver=repo_team_resolver,
            team_repo_ownership_map=team_repo_ownership_map,
            org_id=org_id,
            day=day,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(
            "repo_team_resolver failed for compounding risk (finalize): "
            "org_id=%s day=%s %s",
            org_id,
            day.isoformat(),
            exc,
        )
        repo_to_team_map = {}
    if not repo_to_team_map:
        return 0

    all_rows = build_compounding_risk_rows_for_day(
        sink=primary_sink,
        day=day,
        org_id=org_id,
        repo_metrics_rows=org_repo_metrics,
        computed_at=computed_at,
        repo_to_team=repo_to_team_map,
    )
    team_rows = [r for r in all_rows if r.scope == "team"]
    if not team_rows:
        return 0
    for s in sinks:
        s.write_compounding_risk_daily(team_rows)
    return len(team_rows)


def _secondary_uri_from_env() -> str:
    uri = os.getenv("SECONDARY_DATABASE_URI")
    if not uri:
        raise ValueError("SECONDARY_DATABASE_URI is not set")
    return uri


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
    on_write_starting: Callable[[], None] | None = None,
    skip_families: set[str] | None = None,
) -> dict[date, list[str]]:
    """Run the daily metrics compute+write pipeline.

    Returns a ``{day: [family, ...]}`` map of sub-families (currently
    cicd/deploy/incident -- CHAOS-4246) that computed zero rows
    for that day despite the rest of the run succeeding. This is a
    DEGRADE signal, not a failure: zero rows is often legitimate (no CI
    activity, no deploys, no incidents that day), so an empty result for one
    family never raises or aborts the job. Callers that want the partition to
    reflect it should surface this map (e.g. in the HTTP execution result or
    a log line) rather than relying on the job's plain completion.

    ``skip_families`` (CHAOS-4276) names families.json families a native Go
    executor already computed and wrote for this (org, day, repo) scope --
    this job must neither recompute nor rewrite them. ``None`` or an empty
    set is a no-op: every family computes and writes exactly as it did
    before this parameter existed. Only families with a Go native executor
    check this set (``team_wellbeing`` CHAOS-4276, ``repo_user_commit``
    CHAOS-4275, ``incident`` CHAOS-4269/CHAOS-4295, ``deploy`` CHAOS-4293,
    ``work_item_state`` CHAOS-4278, ``cicd`` CHAOS-4292
    (``file_hotspots``/``file_risk_hotspots``, CHAOS-4277, had their Python
    compute+write deleted outright rather than gated --
    CHAOS-5234/CHAOS-3092 -- so neither checks this set at all anymore),
    ``compounding_risk`` CHAOS-4287, ``review_edges`` CHAOS-4279, and
    ``benchmarking`` CHAOS-4288 (``ai_impact`` CHAOS-4280 had the same
    write-only-skip shape as file_hotspots above until CHAOS-5234/CHAOS-3092
    -- its Python compute+write was deleted outright, so it no longer checks
    this set at all); naming any other family here has no effect. CHAOS-5245
    deleted testops_pipeline/testops_test/testops_coverage/testops_risk's
    Python compute entirely (their native Go executors, CHAOS-4284/
    CHAOS-4294, have no Python fallback left) -- those four names no longer
    appear here at all, not even as a no-op.

    ``compounding_risk`` is REPO scope only: the native executor writes the
    per-partition repo rows, so this set gates the ``_write_compounding_risk_
    for_day`` call below. The TEAM-scope rows are emitted once per org/day from
    ``run_daily_metrics_finalize`` and are NOT covered by this set -- the Go
    finalize handler has no per-family registration to skip them with, so they
    remain Python regardless of what this set names.
    """
    skip_families = skip_families or set()
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
    # CHAOS-4365 codex round 3: the team_repo_ownership map for compounding
    # risk is now loaded PER DAY, as-of that day, inside
    # _write_compounding_risk_for_day -- not once per run here -- so a
    # backfilled day's team-scope attribution reflects ownership validity on
    # that day, not "now".
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
    # CHAOS-5216/CHAOS-5234/CHAOS-3092/CHAOS-5242: repo_provider_by_id used to
    # feed _extract_ai_workflow_for_day's by-provider grouping (CHAOS-2187) --
    # deleted alongside that function, both of whose halves (ai_workflow via
    # #2307, work_graph_edges in this PR) are now gone; rg confirmed no other
    # reader of this dict.

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

    # CHAOS-4246: cicd/deploy/incident stayed at zero rows for
    # 16 days while every metrics.daily_partition run reported succeeded --
    # the compute+write path was correct, but nothing recorded that these
    # specific families produced nothing. families_zero_rows makes that
    # visible per day without failing the job (see run_daily_metrics_job
    # docstring for why this degrades rather than fails).
    families_zero_rows: dict[date, list[str]] = {}

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

    def _note_family_zero_rows(family: str, rows: Any, *, day: date) -> None:
        """Record (log + counter) a family that computed zero rows for `day`.

        Degrades, never raises: zero rows is frequently legitimate (a repo
        with no CI activity, no deploys, or no incidents that day), so this
        must never fail the partition (CHAOS-4246). It exists so that case
        is distinguishable from "never ran" in logs/metrics instead of being
        indistinguishable from a genuinely quiet day.
        """
        if rows:
            return
        logger.warning(
            "metrics.daily family produced zero rows",
            extra={
                "family": family,
                "day": day.isoformat(),
                "org_id": org_id,
                "repo_id": str(repo_id) if repo_id else None,
                "cause": "no_rows_computed",
            },
        )
        record_metrics_family_zero_rows(family=family, cause="no_rows_computed")
        families_zero_rows.setdefault(day, []).append(family)

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
        h_start_date = d - timedelta(days=29)
        # CHAOS-5245 deleted the testops_pipeline/testops_test/testops_coverage
        # compute+write block that used to sit here (the loader fetches for
        # testops_pipeline_rows/testops_job_rows/testops_suite_rows/
        # testops_case_rows/historical_failed_names_by_repo/coverage_rows/
        # prior_coverage_rows and the compute_pipeline_metrics_daily/
        # compute_test_metrics_daily/compute_coverage_metrics_daily calls that
        # consumed them) -- their native Go executors (CHAOS-4284) have no
        # Python fallback left to feed. h_start_date stays: h_commit_rows below
        # is unrelated (file/complexity history), not a testops fetch.
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

        # CHAOS-5234/CHAOS-3092: file_hotspots's daily compute (formerly
        # `compute_file_hotspots` -> `all_file_metrics` -> write_file_metrics)
        # is DELETED here, not skip-gated -- chris's ruling: "once go is in
        # main that does the same thing, skip flags are pointless." The
        # native Go executor (FileHotspotsExecutor, CHAOS-4277) is the only
        # writer of file_metrics_daily now; neither all_file_metrics nor
        # file_hotspots fed anything else downstream in this function (see
        # the deleted gate comment's own admission), so there is no shared
        # input to preserve. `compute_file_hotspots` itself IS ALSO deleted
        # (src/dev_health_ops/metrics/hotspots.py, removed whole-file) --
        # its only other callers were golden-fixture generators and unit
        # tests, never a real production caller (correction from an earlier
        # PR pass on this same family, which left the function in place on
        # that flawed premise; see this PR's own body for the writeup).
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

        # CHAOS-5234/CHAOS-3092: file_risk_hotspots's daily compute (formerly
        # `compute_file_risk_hotspots` over `hotspot_repos` -> `all_file_
        # hotspots` -> write_file_hotspot_daily) is DELETED here, not
        # skip-gated -- chris's ruling: "once go is in main that does the
        # same thing, skip flags are pointless." The native Go executor
        # (FileRiskHotspotsExecutor, CHAOS-4277) is the only writer of
        # file_hotspot_daily now; all_file_hotspots fed nothing else
        # downstream in this function (see the deleted gate comment's own
        # admission), so there is no shared input to preserve.
        # `compute_file_risk_hotspots` itself IS ALSO deleted
        # (src/dev_health_ops/metrics/hotspots.py, removed whole-file),
        # along with the private helpers it used
        # (`_hotspot_repo_ids`/`_load_complexity_map_for_repo`/
        # `_load_blame_map_for_repo`, all formerly defined just above
        # `run_daily_metrics_job` in this file) -- none of the four had a
        # real production caller once this call site is gone, only golden-
        # fixture generators and their own unit tests (which are deleted in
        # the same PR). `post_sync_dispatch.py`'s worker-chaining comment
        # naming `_load_complexity_map_for_repo` is updated in the same PR
        # to no longer describe a live dependency.

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

        # CHAOS-4276: team_wellbeing has a native Go executor. When the Go
        # dispatcher reports it already computed and wrote this scope,
        # skip both compute and write here -- an empty list produces the
        # same "nothing to write" shape write_team_metrics already handles
        # for a legitimately quiet day, so no separate branch is needed
        # below.
        team_metrics = (
            []
            if "team_wellbeing" in skip_families
            else compute_team_wellbeing_metrics_daily(
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
        )

        wi_metrics: list[Any] = []
        wi_user_metrics: list[Any] = []
        wi_cycle_times: list[Any] = []
        estimate_coverage_metrics: list[Any] = []
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
            # CHAOS-5233/CHAOS-3092: work_item_attribution's daily compute is
            # DELETED here, not skip-gated -- chris's ruling: "once go is in
            # main that does the same thing, skip flags are pointless." The
            # native Go executor (WorkItemAttributionExecutor, #2246) is the
            # ONLY writer of work_item_team_attributions for this (org, day,
            # repo) partition scope now; there is no Python fallback to keep
            # alive here. `compute_work_item_team_attributions` itself is NOT
            # deleted -- it has a real, unrelated caller
            # (job_work_items.py's run_work_items_sync_job, a full-backfill
            # sync job outside this function's scope) plus dedicated unit
            # tests and the live-Python oracle comparator
            # (internal/providersync/testdata/oracle_pairs/
            # _github_work_item_derived_helpers.py) that exercise the
            # function directly; only THIS call site is gone.
            estimate_coverage_metrics = compute_estimate_coverage_metrics_daily(
                day=d,
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
            #
            # CHAOS-4278: work_item_state has a native Go executor
            # (WorkItemStateExecutor). When the Go dispatcher reports it
            # already computed and wrote this scope, skip compute here too --
            # unlike repo_user_commit, nothing downstream of
            # wi_state_durations in this function reads it (its only other
            # use is the write below), so there is no shared-input reason to
            # keep computing it, matching team_wellbeing's skip shape.
            wi_state_durations = (
                []
                if "work_item_state" in skip_families
                else compute_work_item_state_durations_daily(
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

        # CHAOS-4279: review_edges has a native Go executor
        # (ReviewEdgesExecutor), registered pre_bridge. When the Go dispatcher
        # names it in skip_families it has already computed and written this
        # scope, so skip compute entirely rather than only the write --
        # nothing else in this function reads review_edges before the write
        # block, which makes this the cicd/team_wellbeing shape rather than
        # repo_user_commit's write-only skip.
        skip_review_edges = "review_edges" in skip_families
        review_edges = (
            []
            if skip_review_edges
            else compute_review_edges_daily(
                day=d,
                pull_request_rows=pr_rows,
                pull_request_review_rows=review_rows,
                computed_at=computed_at,
            )
        )
        # CHAOS-4292: cicd has a native Go executor (CICDExecutor). When the
        # Go dispatcher reports it already computed and wrote this scope,
        # skip compute here -- unlike repo_user_commit, cicd_metrics has no
        # downstream in-process consumer in this function (nothing else
        # reads it before the write block), so skipping compute entirely
        # (mirroring team_wellbeing, not repo_user_commit's write-only skip)
        # is safe.
        skip_cicd = "cicd" in skip_families
        cicd_metrics = (
            []
            if skip_cicd
            else compute_cicd_metrics_daily(
                day=d, pipeline_runs=pipeline_rows, computed_at=computed_at
            )
        )
        deploy_metrics = compute_deploy_metrics_daily(
            day=d, deployments=deployment_rows, computed_at=computed_at
        )
        # CHAOS-4269/CHAOS-4295: incident has a native Go executor, WITH the
        # CHAOS-4269 NULL-guard fix (this Python path stays permanently
        # zero-yield for repository-derived incident mappings -- port-with-fix
        # standing order means the fix lands only in Go, never patched here).
        # Same skip_families gate shape as team_wellbeing above: nothing else
        # in this function reads `incident_metrics` besides the write and the
        # zero-rows note just below, so both are skipped together.
        incident_metrics = (
            []
            if "incident" in skip_families
            else compute_incident_metrics_daily(
                day=d, incidents=incident_rows, computed_at=computed_at
            )
        )
        # CHAOS-5234/CHAOS-3092: ai_governance's daily compute is DELETED
        # here, not skip-gated -- chris's standing rule (CHAOS-5233): once a
        # family's Go executor is on main, its Python compute is deleted,
        # never skip-gated. AIGovernanceExecutor (native Go) is now the only
        # writer of ai_policy_events/ai_governance_coverage_daily for a
        # daily partition. Unlike CHAOS-5233's work_item_attribution,
        # build_governance_rows_for_day ITSELF is also deleted here (from
        # audit/ai_governance/loaders.py) -- codegraph_explore + rg both
        # confirm this job was its ONLY real caller (the other rg hits are
        # its own definition/__all__ export, a docs-generator citation
        # string, and test files that monkeypatched it to a no-op, all
        # updated in this same PR). evaluate_artifacts/rollup_coverage_daily/
        # AIGovernanceLoader (the functions it glued together) are NOT
        # touched -- they have real, separate callers (the Go oracle
        # comparator at internal/jobs/metrics/aigovernance/testdata/
        # python_governance_oracle.py, the GraphQL API resolver, and their
        # own dedicated tests).
        # CHAOS-5234/CHAOS-3092: no ai_attribution_rows load here anymore --
        # it existed solely to feed compute_ai_impact_metrics_daily's
        # ai_attribution_rows parameter, deleted below alongside the compute
        # call and the pr_commit_stats build; verified via rg that
        # ai_attribution_rows was never read by anything else in this
        # function.

        # CHAOS-5216/CHAOS-5234/CHAOS-3092/CHAOS-5242: no
        # _extract_ai_workflow_for_day call here anymore -- both of its
        # halves (ai_workflow's runs/artifact_edges/issue_edges, deleted by
        # #2307/CHAOS-5242; work_graph_edges' review/deployment/incident
        # edges, deleted in this PR) are gone, so the function itself is
        # deleted too (see the comment above its old definition).

        # CHAOS-5234/CHAOS-3092: ai_impact's daily compute is DELETED here,
        # not skip-gated -- chris's standing rule (CHAOS-5233): once a
        # family's Go executor is on main, its Python compute is deleted,
        # never skip-gated. AIImpactExecutor (native Go, CHAOS-4280) is now
        # the only writer of ai_impact_metrics_daily for a daily partition.
        # This deletion also removes the pr_commit_stats build this comment
        # replaces (a ~100-line work_graph_pr_commit/git_commit_stats/
        # git_commits join, CHAOS-2183) and the ai_attribution_rows load
        # above -- both existed SOLELY to feed compute_ai_impact_metrics_
        # daily's own parameters (verified via rg: neither name is read
        # anywhere else in this function). Unlike CHAOS-5233's
        # work_item_attribution, compute_ai_impact_metrics_daily ITSELF is
        # ALSO deleted (from metrics/ai_impact.py), along with its Go
        # bit-exact oracle rot guard (TestAIImpactMatchesLivePythonProduction
        # + testdata/python_ai_impact_oracle.py) and its own dedicated tests
        # (tests/metrics/test_ai_impact.py) -- codegraph_explore + rg
        # confirmed the oracle and those tests were its only real callers
        # once job_daily.py's own reference was removed, so deleting the
        # oracle alongside the compute function leaves nothing to keep it
        # alive for. AttributionBucket/AI_BUCKETS (the same module) are NOT
        # touched -- they have real, separate callers (the GraphQL API
        # resolver and the opportunities detector).
        # pr_rows/review_rows/incident_rows/commit_rows themselves are NOT
        # touched -- they are shared inputs other, still-Python
        # computations in this function also read.

        # CHAOS-4264: this is the FIRST write for (repo_id, d) in the whole
        # function -- everything above is loading/compute, no sink writes.
        # on_write_starting is the caller's durable-proof boundary: if the
        # process dies before this fires, nothing was written for (repo_id,
        # d) and a retry is unconditionally safe; if it fires and the
        # process then dies mid-write, the caller must NOT assume safety --
        # a repo-level "finished" signal (fired only after the whole call
        # returns) is too coarse for that, which is exactly what let a
        # kill-after-first-write-block-but-before-return be misclassified
        # as "no progress" before this callback existed.
        if on_write_starting is not None:
            on_write_starting()

        # CHAOS-4275: repo_user_commit has a native Go executor
        # (RepoUserCommitExecutor). When the Go dispatcher reports it
        # already computed and wrote this scope, skip ONLY the write here --
        # unlike team_wellbeing above, `compute_daily_metrics` itself is
        # NOT skipped: `result.repo_metrics` is a live in-process input to
        # `_write_compounding_risk_for_day` a few lines below, and
        # compounding_risk (not yet ported) has no other source for it. A
        # codex adversarial review (round 2) on the Go port caught that this
        # gate was missing entirely -- the native executor and this
        # unconditional write path would otherwise both fire for every
        # partition, doubling every repo/user/commit row's generation.
        skip_repo_user_commit_write = "repo_user_commit" in skip_families
        # CHAOS-4293: deploy has a native Go executor (DeployExecutor). Same
        # shape as repo_user_commit above -- skip ONLY the write here, not
        # the compute: `deploy_metrics` is also a live in-process input to
        # `_note_family_zero_rows("deploy", deploy_metrics, day=d)` a few
        # lines below (the CHAOS-4246/CHAOS-4263 staleness-with-source-data
        # check), which has no other source for it and must keep observing
        # a real "did compute_deploy_metrics_daily produce rows" signal
        # regardless of which side wrote them. Without this guard the native
        # executor and this unconditional write path both fire for every
        # partition, doubling every (org_id, repo_id, day) deploy_metrics_daily
        # row's generation -- the same class of gap CHAOS-4275's own guard
        # above exists to close, caught here by codex round 1 on this port.
        skip_deploy_write = "deploy" in skip_families
        # CHAOS-4277/CHAOS-5234/CHAOS-3092: file_risk_hotspots (like
        # file_hotspots, its sibling CHAOS-4277 family) no longer checks
        # skip_families at all -- its compute+write is deleted entirely, see
        # the comment above the deleted `compute_file_risk_hotspots` call
        # site earlier in this function.
        # CHAOS-4283: work_item and work_item_estimate have native Go
        # executors (WorkItemExecutor/WorkItemEstimateExecutor). This is the
        # repo_user_commit shape, NOT the team_wellbeing shape -- skip ONLY
        # the writes, never the computes:
        #
        #   * `wi_user_metrics` feeds its OWN write a few lines down
        #     (`s.write_work_item_user_metrics`, gated separately by
        #     `skip_work_item_write`), so the compute that populates it must
        #     stay unconditional regardless of that gate. (It used to ALSO
        #     feed ic_finalize's now-deleted Python compute -- CHAOS-4290
        #     PR3 -- but that was never the reason this compute had to stay
        #     unconditional; this write is.)
        #   * `estimate_coverage_metrics` feeds nothing else here, so its
        #     compute COULD be skipped, but is left unconditional to keep this
        #     diff minimal (work_item_estimate is its own separate deletion
        #     target under CHAOS-5234/CHAOS-3092, not yet done as of this
        #     comment -- file_hotspots, which this comment used to cite as
        #     precedent for "unconditional is fine," has since had its own
        #     compute+write deleted outright rather than left unconditional).
        #
        # CHAOS-5216/CHAOS-5234/CHAOS-3092: no skip_work_graph_edges_write
        # here -- work_graph_edges' compute+write (both deleted alongside
        # _extract_ai_workflow_for_day above) is not skip-gated;
        # WorkGraphEdgesExecutor (native Go) is the only writer now.
        #
        # Without this gate the native executor and the unconditional
        # writes below would BOTH fire for every partition, doubling every row
        # in work_item_metrics_daily and work_item_user_metrics_daily (plain
        # MergeTree, no dedup key) on every single run -- exactly the defect
        # repo_user_commit's own comment above warns about.
        skip_work_item_write = "work_item" in skip_families
        skip_work_item_estimate_write = "work_item_estimate" in skip_families
        # CHAOS-5234/CHAOS-3092: no skip_ai_governance_write here -- deleted
        # alongside the compute call above, not skip-gated.
        # CHAOS-5234/CHAOS-3092: no skip_ai_impact_write here either -- same
        # deletion, not skip-gated. AIImpactExecutor (native Go) is the only
        # writer of ai_impact_metrics_daily now.
        for s in sinks:
            if not skip_repo_user_commit_write:
                s.write_repo_metrics(result.repo_metrics)
                s.write_user_metrics(result.user_metrics)
                if include_commit_metrics:
                    s.write_commit_metrics(result.commit_metrics)
            s.write_team_metrics(team_metrics)
            if wi_metrics and not skip_work_item_write:
                s.write_work_item_metrics(wi_metrics)
            if estimate_coverage_metrics and not skip_work_item_estimate_write:
                s.write_estimate_coverage_metrics(estimate_coverage_metrics)
            if wi_user_metrics and not skip_work_item_write:
                s.write_work_item_user_metrics(wi_user_metrics)
            if wi_cycle_times and not skip_work_item_write:
                s.write_work_item_cycle_times(wi_cycle_times)
            # CHAOS-5233/CHAOS-3092: no write_work_item_team_attributions
            # call here -- deleted alongside the compute call above; the
            # native Go executor is the only writer now.
            if wi_state_durations:
                s.write_work_item_state_durations(wi_state_durations)
            if not skip_review_edges:
                s.write_review_edges(review_edges)
            s.write_cicd_metrics(cicd_metrics)
            # CHAOS-5245 deleted testops_pipeline/testops_test/testops_coverage's
            # Python compute+write entirely (their native Go executors,
            # CHAOS-4284, have no fallback left) -- there is nothing left here
            # to gate.
            if not skip_deploy_write:
                s.write_deploy_metrics(deploy_metrics)
            s.write_incident_metrics(incident_metrics)
            # CHAOS-5234/CHAOS-3092: no write_ai_policy_events/
            # write_ai_governance_coverage_daily call here -- deleted
            # alongside the compute call above; AIGovernanceExecutor (native
            # Go) is the only writer now.
            # CHAOS-5234/CHAOS-3092: no write_ai_impact_metrics call here
            # either -- deleted alongside the compute call above;
            # AIImpactExecutor (native Go) is the only writer now.
            # CHAOS-5242: no write_ai_workflow_runs/_artifact_edges/
            # _issue_edges calls here either -- deleted, not skip-gated,
            # alongside extract_ai_workflow_from_pull_requests above;
            # AIWorkflowExecutor (native Go) is the only writer now.
            # CHAOS-5216/CHAOS-5234/CHAOS-3092: no
            # write_work_graph_pr_review_outcome_edges/
            # write_work_graph_pr_deployment_edges/
            # write_work_graph_deployment_incident_edges calls here either --
            # deleted alongside _extract_ai_workflow_for_day above;
            # WorkGraphEdgesExecutor (native Go) is the only writer now.
            # CHAOS-5234/CHAOS-3092: no write_file_metrics or
            # write_file_hotspot_daily call here -- both file_hotspots' and
            # file_risk_hotspots' compute+write are deleted entirely (see
            # the comments above their deleted call sites); the native Go
            # executors are the only writers of file_metrics_daily and
            # file_hotspot_daily now.

        # CHAOS-4246: cicd/deploy/incident are written unconditionally above
        # (write_*_metrics no-ops on an empty list) -- note it here so a run
        # of zero rows is visible instead of indistinguishable from success.
        # CHAOS-4292: when cicd was skipped (native Go already computed and
        # wrote it), cicd_metrics is always [] here regardless of how many
        # rows the Go side actually wrote -- noting it would be a FALSE zero-
        # rows-computed signal, so skip the note entirely for this partition;
        # the native executor's own anomaly detection is
        # ClickHouseSourceDataChecker (Go), not this Python-side note. Same
        # shape as incident's own gate just below (CHAOS-4269/CHAOS-4295).
        if not skip_cicd:
            _note_family_zero_rows("cicd", cicd_metrics, day=d)
        _note_family_zero_rows("deploy", deploy_metrics, day=d)
        if "incident" not in skip_families:
            _note_family_zero_rows("incident", incident_metrics, day=d)

        # CHAOS-4287: compounding_risk has a native Go executor
        # (CompoundingRiskExecutor), registered post_bridge. When the Go
        # dispatcher names it in skip_families it has already computed and
        # written this partition's REPO-scope rows, so skip the whole call
        # rather than only the write -- nothing else in this function consumes
        # its output (it writes straight to the sinks), which makes this the
        # cicd/team_wellbeing shape rather than repo_user_commit's write-only
        # skip. No _note_family_zero_rows here either way: this call site never
        # had one, and adding it under a skip would be a false zero-rows signal
        # exactly as it would have been for cicd.
        #
        # TEAM-scope rows are NOT covered by this gate. They are written once
        # per org/day from run_daily_metrics_finalize
        # (_write_compounding_risk_team_rows_for_day), which the Go side still
        # reaches through the opaque compatibility.Finalize bridge call -- there
        # is no per-family registration or skip-list at finalize to carve them
        # out with. They stay Python until that hook exists; CHAOS-4287 stays
        # open until then.
        if "compounding_risk" not in skip_families:
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

        # CHAOS-4365 finalize-step fix: team_cognitive_load has no repo-scope
        # table at all -- it is emitted ONCE per org/day from
        # run_daily_metrics_finalize (after every repo's partition has
        # landed), not here. This function runs once per repo (CHAOS-4264),
        # so writing a "complete" team row per-repo silently dropped every
        # other owned repo's contribution once argMax(computed_at) dedup
        # collapsed the redundant per-repo writes down to whichever repo's
        # partition ran last -- confirmed live before this fix.

        # CHAOS-4329: observe distinct repo_id fan-out per team_id AFTER the
        # write above durably lands (mirrors ObserveZeroUnitFinalization's
        # post-commit rule) -- once per write, not once per sink (team_metrics
        # is the exact same list every sink in a dual-sink write received, so
        # observing inside that loop would double-count).
        record_team_metrics_daily_repo_rows(team_metrics)

        # CHAOS-5245 deleted testops_risk's Python compute+write entirely
        # (its native Go executor, TestopsRiskExecutor/CHAOS-4294, has no
        # fallback left, and its only input -- testops_pipeline_metrics/
        # testops_test_metrics/testops_coverage_metrics -- no longer exists
        # either now that CHAOS-4284's Python compute is also gone).

        # Benchmarking (baselines, maturity, anomalies, period comparisons,
        # correlations, insights) MOVED to run_daily_metrics_finalize
        # (CHAOS-5194, astra design-review finding F3), skip_families gate and
        # all -- necessary PLUMBING, not a compute fix. It used to run HERE,
        # once per partition (run_benchmarking_for_day takes no repo_id, so an
        # org with N repos appended N identical row sets to six append-only
        # tables), deduplicated on the Go side by an anchor-partition trick
        # (fixed duplication, not the race F3 found: the anchor partition's
        # own post_bridge phase could complete before every OTHER partition
        # for the org/day had written its own inputs).
        #
        # The move is required for the skip_families MECHANISM to keep
        # working, not optional: partition-scope skip_families (built by
        # computeNativeFamilies/computePostBridgeNativeFamilies) and
        # finalize-scope skip_families (built by
        # computeNativeFinalizeFamilies) are TWO INDEPENDENT lists sent to
        # TWO INDEPENDENT bridge calls. Once the Go executor registers
        # "benchmarking" as a FINALIZE family instead of a post_bridge one,
        # the partition-scope skip_families sent here would never contain it
        # again -- leaving the OLD gate at this call site permanently open
        # and duplicating Python's own compute on top of the native
        # executor's correct one, a WORSE bug than before. Moving the call to
        # where the matching skip_families list actually lives is what keeps
        # "the Go executor computed it, so Python must not" true.
        #
        # This move does NOT fix Python's own compute logic (no ordering
        # change, no query change) -- team-lead ruling, CHAOS-5194: Python's
        # race stays exactly as buggy as it always was WHEN THIS CODE PATH
        # ACTUALLY RUNS. In practice it incidentally runs behind the same
        # partition barrier as the Go executor now (both live in
        # run_daily_metrics_finalize, which only runs once every partition
        # for the day has succeeded), and it runs at all only when the
        # native executor is absent (skip_families gate below), which is the
        # existing fail-open-to-Python contract every other native family
        # already has.

        # ic_finalize's Python compute (compute_ic_metrics_daily /
        # compute_ic_landscape_rolling) was deleted here (CHAOS-4290 PR3,
        # CHAOS-3092 no-straddle): the native Go executor has been the SOLE
        # writer for this family since #2241's finalize policy landed --
        # FinalizeHandler.computeNativeFinalizeFamilies never falls open to
        # this bridge on a native error (daily.go, "The bridge is NOT
        # called" -- see PR3's body for the exact citation), so this call
        # was already dead weight, never a live fallback, before its
        # deletion. `skip_finalize` (this function's own parameter) existed
        # only to gate this block and is removed with it.

        if len(days) > 1:
            # CHAOS-4264: a backfill_days > 1 call holds this day's source
            # rows (commit/PR/CI/testops/incident/work-item lists, complexity
            # and blame maps) as plain local variables that Python's own
            # refcounting already drops once the next iteration reassigns
            # them -- except for anything a reference cycle keeps alive
            # (tracebacks captured in a `except ... as exc` that outlives its
            # block, a resolver closure holding a day's rows). gc.collect()
            # forces that cycle collection at the day boundary instead of
            # letting it accumulate across the whole backfill window; it is
            # a no-op cost in the common single-day case, which skips it.
            gc.collect()

    if families_zero_rows:
        logger.warning(
            "metrics.daily run completed with zero-row families",
            extra={
                "org_id": org_id,
                "repo_id": str(repo_id) if repo_id else None,
                "families_zero_rows": {
                    d.isoformat(): fams for d, fams in families_zero_rows.items()
                },
            },
        )
    return families_zero_rows


async def run_daily_metrics_finalize(
    *,
    db_url: str,
    day: date,
    org_id: str,
    sink: str = "auto",
    skip_families: set[str] | None = None,
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

    # _get_loader(db_url, backend, org_id=org_id) used to be called here to
    # feed ic_finalize's now-deleted compute_ic_landscape_rolling call
    # (loader.load_user_metrics_rolling_30d) -- removed with it (CHAOS-4290
    # PR3). Nothing else in this function needs a DataLoader.

    import dataclasses as _dc

    deps = get_metrics_dependencies()

    # wi_user_metrics (work_item_user_metrics_daily) was loaded here ONLY to
    # feed ic_finalize's now-deleted compute_ic_metrics_daily call -- removed
    # with it (CHAOS-4290 PR3). git_metrics stays: _write_team_cognitive_load_for_day
    # further down still reads it (user_metrics_rows=git_metrics).
    git_metrics: list[Any] = []
    # CodeQL (py/uninitialized-local-variable): ch_client is only assigned
    # inside the `backend == "clickhouse"` branch below, but the
    # team_metrics_daily readback further down (CHAOS-4365) references it
    # unconditionally. backend is already guaranteed "clickhouse" by the
    # ValueError raised earlier in this function for any other backend, so
    # this is unreachable in practice -- explicit None satisfies static
    # analysis without relying on that far-away invariant.
    ch_client: Any = None

    if backend == "clickhouse":
        ch_client = await deps.get_global_client(db_url)
        um_field_names = {f.name for f in _dc.fields(deps.user_metrics_daily_record)}

        um_query = (
            f"SELECT * FROM {dedup_from('user_metrics_daily')} WHERE day = {{day:Date}}"
        )
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
    else:
        logger.warning(
            "Finalize currently optimised for ClickHouse; "
            "backend=%s may produce empty finalize-scope metrics.",
            backend,
        )

    # CHAOS-4290: ic_finalize's Python compute (compute_ic_metrics_daily /
    # compute_ic_landscape_rolling) was deleted here (PR3, CHAOS-3092
    # no-straddle) -- the native Go executor has been the SOLE writer for
    # this family since #2241's finalize policy landed, so this call was
    # already dead weight, never a live fallback, before its deletion (see
    # PR3's body for the no-fail-open citation). `skip_families` stays a
    # parameter of this function: it now gates `benchmarking` below
    # (CHAOS-5194), and worker_metrics.py's finalize call site still passes
    # it through from the Go dispatcher's real skip list for whichever
    # finalize-scope family needs it next.

    computed_at = datetime.now(timezone.utc)
    skip_families = skip_families or set()

    # CHAOS-5194 (astra F3): benchmarking, relocated here from
    # run_daily_metrics_job -- see this function's sibling comment at the old
    # call site for why the move is required plumbing, not a compute fix.
    # Same skip_families gate shape as ic_finalize above: when the Go
    # dispatcher names "benchmarking" in skip_families, the native
    # BenchmarkingFinalizeExecutor already computed and wrote this org/day,
    # so skip the whole call -- nothing else in this function consumes its
    # output.
    if "benchmarking" not in skip_families:
        for s in sinks_list:
            try:
                run_benchmarking_for_day(
                    s,
                    as_of_day=day,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            except Exception as exc:
                logger.warning("Benchmarking run failed for day=%s: %s", day, exc)

    # CHAOS-4365 finalize-step fix: team-scope compounding_risk_daily is
    # written exactly ONCE here, per org/day, after every repo's own
    # partition has landed -- never in-process inside a single per-repo
    # run_daily_metrics_job call (see _write_compounding_risk_for_day's
    # docstring for why that was wrong). team_cognitive_load_daily used to
    # be written from this same finalize step too; CHAOS-5141 deleted that
    # Python compute entirely once it was confirmed unreachable in
    # production -- it is now written ONLY by the Go worker's native
    # FinalizeHandler, never from this Python path.
    teams_data = await primary_sink.get_all_teams()
    repo_team_resolver = build_repo_pattern_resolver(teams_data)
    discovered_repos = discover_repos(
        backend=backend,
        primary_sink=primary_sink,
        repo_id=None,
        repo_name=None,
        org_id=org_id,
    )
    repo_names_by_id = {r.repo_id: r.full_name for r in discovered_repos}

    compounding_risk_team_count = _write_compounding_risk_team_rows_for_day(
        sinks=sinks_list,
        primary_sink=primary_sink,
        day=day,
        org_id=org_id,
        repo_names_by_id=repo_names_by_id,
        repo_team_resolver=repo_team_resolver,
        computed_at=computed_at,
    )
    if not compounding_risk_team_count:
        # CHAOS-4365 codex R1: a resolver failure (or an org with no
        # ownership-resolvable repos) degrades to zero rows here, never
        # raises (same CHAOS-4246 contract run_daily_metrics_job's own
        # families follow) -- log it so a transient CH/resolver failure
        # doesn't look identical to "no repos to attribute" in the logs.
        logger.warning(
            "metrics.daily.finalize family produced zero rows",
            extra={
                "family": "compounding_risk_team",
                "day": day.isoformat(),
                "org_id": org_id,
                "cause": "no_rows_computed",
            },
        )

    # CHAOS-5141: team_cognitive_load's Python compute (the team_metrics_daily
    # aggregation query + _write_team_cognitive_load_for_day) was DELETED
    # here, not merely skip-gated. Reachability analysis at deletion time:
    # buildDailyWorker (cmd/dev-health-worker/daily.go) refuses the WHOLE
    # daily worker if the ClickHouse connection fails to open, before
    # dailyNativeFamilyRegistrations is ever called -- so team_cognitive_load
    # (and ic_finalize, its co-registration dependency) are guaranteed to
    # register natively in every real deployment; a construction-time
    # fallback to this Python path was never actually reachable. On a
    # RUNTIME native failure, FinalizeHandler.Work's computeNativeFinalizeFamilies
    # error path explicitly never calls the Python bridge either (daily.go's
    # own comment: "The bridge is NOT called"). No straddle, no live fallback
    # path -- safe to delete outright rather than leave skip_families-gated.

    # CHAOS-5051: team_complexity's Python compute
    # (_fetch_repo_complexity_for_day + _write_team_complexity_for_day) was
    # DELETED here, not merely skip-gated. Same reachability analysis
    # CHAOS-5141 (team_cognitive_load) already established: buildDailyWorker
    # (cmd/dev-health-worker/daily.go) refuses the WHOLE daily worker if the
    # ClickHouse connection fails to open, before
    # dailyNativeFamilyRegistrations is ever called -- so team_complexity is
    # guaranteed to register natively in every real deployment (it has no
    # co-registration dependency of its own, unlike team_cognitive_load/
    # ic_finalize). On a RUNTIME native failure, FinalizeHandler.Work's
    # computeNativeFinalizeFamilies error path explicitly never calls the
    # Python bridge either (daily.go's own comment: "The bridge is NOT
    # called"). No straddle, no live fallback path -- safe to delete outright
    # rather than leave skip_families-gated.

    logger.info("IC finalize complete for day=%s", day.isoformat())


# CHAOS-5307: `register_commands`/`_cmd_metrics_daily`/`_cmd_metrics_rebuild`
# (the direct-Python-compute `dev-hops metrics daily`/`rebuild` CLI verbs)
# were deleted here. They were already 100% orphaned before this change --
# CHAOS-5055/#2232 repointed `cli.py` to register
# `workerctl_dispatch.register_commands` instead (which dispatches through
# `dev-health-workerctl metrics daily-start`, see that module's own
# docstring), and nothing anywhere in the repo still called these functions
# or `job_daily.register_commands` by name (verified by repo-wide search
# before deletion). `run_daily_metrics_job`/`run_daily_metrics_finalize`
# above are NOT dead -- they still have live callers (the worker bridge,
# `scripts/compute_metrics_daily.py`, fixtures, tests) -- only the unwired
# CLI wrapper functions were removed. (Merge-forward note: #2293/CHAOS-5263
# separately removed `run_daily_metrics_job`'s `skip_finalize` parameter
# entirely while this dead code still called it with `skip_finalize=True` --
# further confirming these two functions were unreachable and rotting, not
# a live code path anyone was maintaining.)
