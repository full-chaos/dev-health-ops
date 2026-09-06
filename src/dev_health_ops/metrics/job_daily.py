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
from dev_health_ops.metrics.dependencies import get_metrics_dependencies
from dev_health_ops.metrics.identity import init_team_resolver
from dev_health_ops.metrics.loaders import DataLoader, to_utc
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.work_items import DiscoveredRepo
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


# CHAOS-5308/CHAOS-3092: _repo_to_team_map_for_compounding_risk and
# _write_compounding_risk_for_day (REPO-scope compounding_risk) are deleted
# together. repo_user_commit and compounding_risk (repo scope) both have
# native Go executors (RepoUserCommitExecutor, CompoundingRiskExecutor) that
# are the sole writers now -- chris's ruling: "once go is in main that does
# the same thing, skip flags are pointless." _repo_to_team_map_for_
# compounding_risk had ZERO production callers left even before this PR (an
# orphan from CHAOS-5084's team-scope deletion, which removed the
# run_daily_metrics_finalize call site but not this helper) -- rg confirmed
# only its own dedicated test and a golden-fixture generator referenced it.
# Both are deleted in this same PR (see test_job_daily_compounding_risk.py's
# and tests/fixtures/generate_teamresolve_python_golden.py's deletion, and
# internal/teamresolve/golden_rot_guard_test.go's deletion -- the frozen
# tests/fixtures/teamresolve_python_golden.json and
# internal/teamresolve/golden_test.go stay, they need no live Python).
# TEAM-scope compounding_risk (compounding_risk_team, CHAOS-5084) is
# unaffected -- its own native executor already has no Python compute or
# skip_families gate anywhere in this module.


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
    sink: str = "auto",
    provider: str = "auto",
    org_id: str,
    on_write_starting: Callable[[], None] | None = None,
    skip_families: set[str] | None = None,
) -> dict[date, list[str]]:
    """Run the daily metrics compute+write pipeline.

    Returns a ``{day: [family, ...]}`` map of sub-families (currently
    cicd/deploy/incident -- CHAOS-4246; all three were removed from this set
    by CHAOS-5234/CHAOS-3092/CHAOS-5309, which deleted their Python compute
    outright) that computed zero rows for that day despite the rest of the
    run succeeding. This is a
    DEGRADE signal, not a failure: zero rows is often legitimate (no CI
    activity, no incidents that day), so an empty result for one
    family never raises or aborts the job. Callers that want the partition to
    reflect it should surface this map (e.g. in the HTTP execution result or
    a log line) rather than relying on the job's plain completion. ``deploy``,
    ``cicd``, and ``incident`` used to be members of this map (CHAOS-4246)
    until CHAOS-5234/CHAOS-3092/CHAOS-5309 deleted their Python
    compute+write+zero-rows-note outright -- see their entries below.

    ``skip_families`` (CHAOS-4276) names families.json families a native Go
    executor already computed and wrote for this (org, day, repo) scope --
    this job must neither recompute nor rewrite them. ``None`` or an empty
    set is a no-op: every family computes and writes exactly as it did
    before this parameter existed. Only families with a Go native executor
    AND a live Python fallback still check this set (``work_item`` CHAOS-4283
    and ``work_item_state`` CHAOS-4278 -- both BLOCKED from outright deletion
    because job_work_items.py's run_work_items_sync_job, an unrelated
    full-backfill sync job, still calls their compute functions directly);
    naming any other family here has no effect. ``benchmarking`` CHAOS-4288
    was NEVER checked in this set within THIS function -- it was moved to
    ``run_daily_metrics_finalize``'s own skip_families gate by CHAOS-5194
    before this docstring paragraph was last touched, and that gate is
    itself now gone too (CHAOS-4288 deleted the Python compute entirely;
    see the comment at the old call site below). ``file_hotspots``/
    ``file_risk_hotspots`` (CHAOS-4277) and ``ai_impact`` (CHAOS-4280) had
    their Python compute+write deleted outright rather than gated
    (CHAOS-5234/CHAOS-3092); ``team_wellbeing`` (CHAOS-4276), ``incident``
    (CHAOS-4269/CHAOS-4295), ``cicd`` (CHAOS-4292) followed the same
    outright-deletion path (CHAOS-5234/CHAOS-3092, this batch); CHAOS-4279
    deleted ``review_edges``' Python compute+write outright too (same
    shape as file_hotspots/ai_impact above), so it no longer checks this
    set either. ``deploy`` (CHAOS-4293) had the same write-only-skip shape
    too, plus a zero-rows note, until CHAOS-5309 deleted its Python
    compute+write+note outright, so it no longer checks this set either.
    ``work_item_attribution`` (CHAOS-5233) also no longer checks this set
    -- unlike work_item/work_item_state above, THIS function's own call to
    its compute (compute_work_item_team_attributions) is deleted outright,
    even though the function itself survives elsewhere (job_work_items.py's
    run_work_items_sync_job still calls it directly) -- see the deletion
    ledger in test_job_daily_skip_families_structural_guard.py.
    ``work_item_estimate`` (CHAOS-5323) no longer checks this set either --
    unlike work_item/work_item_state, its job_work_items.py caller was ALSO
    deleted (no live backfill caller left anywhere), so it has no straddle
    at all: compute_estimate_coverage_metrics_daily itself is gone from the
    codebase. ``repo_user_commit`` (CHAOS-4275) and ``compounding_risk``
    REPO scope (CHAOS-4287) no longer check this set either -- CHAOS-5308
    deleted compute_daily_metrics and ``_write_compounding_risk_for_day``
    outright (see their own test_*_compute_and_write_are_deleted_from_
    job_daily tests). CHAOS-5245 deleted testops_pipeline/testops_test/
    testops_coverage/testops_risk's Python compute entirely (their native
    Go executors, CHAOS-4284/CHAOS-4294, have no Python fallback left) --
    those four names no longer appear here at all, not even as a no-op.

    ``compounding_risk``'s TEAM-scope rows used to be emitted once per
    org/day from ``run_daily_metrics_finalize``, uncovered by this set since
    the Go finalize handler has no per-family registration to skip them
    with -- CHAOS-5084 deleted that Python compute too (CompoundingRiskTeam
    Executor, native Go, is the sole writer of TEAM-scope rows now, same as
    REPO scope above).
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
    # CHAOS-5308/CHAOS-3092/CHAOS-5310/CHAOS-5321: no `computed_at` local
    # here anymore -- every compute call that used to take it as an
    # argument (compute_daily_metrics, _write_compounding_risk_for_day,
    # compute_work_item_metrics_daily and siblings) is deleted, across this
    # PR and its sibling PRs merged into this same tree.

    # CHAOS-5308/CHAOS-3092: no load_identity_resolver() call here anymore --
    # its only consumer was compute_daily_metrics' identity_resolver
    # argument, deleted alongside repo_user_commit's compute+write below.

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
    # CHAOS-5234/CHAOS-3092/CHAOS-5308/CHAOS-5310/CHAOS-5321: no local
    # `team_resolver = get_team_resolver()` here anymore -- every consumer
    # of the local (compute_team_wellbeing_metrics_daily,
    # compute_work_item_metrics_daily and siblings) is deleted, across this
    # PR and its sibling PRs merged into this same tree. The
    # `init_team_resolver(primary_sink)` call above STAYS -- it is a real
    # side effect (sets the module-global `_TEAM_RESOLVER` in
    # metrics/identity.py), not merely a value feeding the now-dead local.
    # teams_data/
    # repo_team_resolver/project_key_resolver/discovered_repos/
    # repo_names_by_id used to be built here to feed compute_daily_metrics
    # (repo_user_commit), compute_team_wellbeing_metrics_daily
    # (team_wellbeing), _extract_ai_workflow_for_day's by-provider grouping
    # (CHAOS-2187), and compute_work_item_metrics_daily/compute_work_item_
    # state_durations_daily's project-key team attribution (work_item/
    # work_item_state) -- every one of those consumers is deleted outright
    # now, across this PR and three sibling PRs merged into this same tree;
    # rg confirmed no other reader of any of the five names, so the whole
    # block (including primary_sink.get_all_teams() and the discover_repos
    # call, both pure reads with no side effects) is removed too, rather
    # than left as dead computation -- same shape as the earlier
    # finalize-scope teams_data/repo_team_resolver/discovered_repos/
    # repo_names_by_id deletion documented further below in this file.

    loader = await _get_loader(db_url, backend, org_id=org_id)

    load_work_items_from_db = provider == "auto"
    load_work_items_enabled = provider != "none"

    # CHAOS-5234/CHAOS-3092: BUSINESS_TIMEZONE/BUSINESS_HOURS_START/
    # BUSINESS_HOURS_END used to be read here to feed
    # compute_team_wellbeing_metrics_daily's business_timezone/
    # business_hours_start/business_hours_end params -- that was their ONLY
    # reader (verified via rg). team_wellbeing's compute is deleted outright
    # now (TeamWellbeingExecutor, native Go, is the only writer), so these
    # three env reads are gone too.

    # CHAOS-4246: cicd/deploy/incident stayed at zero rows for
    # 16 days while every metrics.daily_partition run reported succeeded --
    # the compute+write path was correct, but nothing recorded that these
    # specific families produced nothing. families_zero_rows used to make
    # that visible per day without failing the job; CHAOS-5234/CHAOS-3092/
    # CHAOS-5309 have since deleted all three families' Python compute+
    # write+note outright, so nothing populates this map anymore (the
    # `_note_family_zero_rows` helper that used to feed it is deleted too --
    # this dict/its logging below stay only for run_daily_metrics_job's
    # return-type/HTTP-bridge contract stability, see worker_metrics.py's
    # zero_rows_by_day caller).
    families_zero_rows: dict[date, list[str]] = {}

    # CHAOS-5310/CHAOS-5321/CHAOS-3092: the linked-issue team-inheritance
    # resolver (work_item_dependencies + donor walk +
    # load_team_attribution_context) used to live here, built once per run
    # to feed compute_work_item_metrics_daily/compute_work_item_team_
    # attributions/compute_work_item_state_durations_daily's
    # attribution_context/linked_issue_resolver parameters -- deleted
    # alongside those three (R6: native Go executors are the only writers
    # of work_item_metrics_daily/work_item_team_attributions/work_item_
    # state_durations_daily now; the Python computes that consumed this
    # resolver are gone). rg confirmed nothing else in this function reads
    # work_item_dependencies/linked_issue_resolver/team_attribution_context.

    # CHAOS-5308/CHAOS-3092: the _note_family_zero_rows(family, rows, day=...)
    # helper that used to live here (log + counter + families_zero_rows.
    # setdefault, for a family that computed zero rows) is deleted -- its
    # every caller (cicd/deploy/incident) is itself deleted outright now, so
    # it had zero remaining call sites. record_metrics_family_zero_rows
    # (the prometheus counter it called) is no longer imported into this
    # module either.

    for d in days:
        logger.info("Computing metrics for day=%s", d.isoformat())
        start, end = _utc_day_window(d)

        commit_rows, pr_rows, review_rows = await loader.load_git_rows(
            start, end, repo_id=repo_id, repo_name=repo_name
        )

        # CHAOS-5308/CHAOS-3092: the `loader.load_cicd_data` call that used to
        # sit here is deleted entirely, not just half-discarded -- BOTH tuple
        # elements are now dead. `deployment_rows` fed `active_repos`, deleted
        # alongside repo_user_commit's Python compute below; `pipeline_rows`
        # fed cicd's own compute, already deleted by CHAOS-5312/CHAOS-3092
        # (verified via rg: no other reference to `pipeline_rows` anywhere in
        # this function besides this call site). `load_cicd_data` itself
        # (the DataLoader Protocol method plus its sqlalchemy/clickhouse
        # implementations) is deleted too -- this dead call site was its
        # only real caller.
        # CHAOS-5245 deleted the testops_pipeline/testops_test/testops_coverage
        # compute+write block that used to sit here (the loader fetches for
        # testops_pipeline_rows/testops_job_rows/testops_suite_rows/
        # testops_case_rows/historical_failed_names_by_repo/coverage_rows/
        # prior_coverage_rows and the compute_pipeline_metrics_daily/
        # compute_test_metrics_daily/compute_coverage_metrics_daily calls that
        # consumed them) -- their native Go executors (CHAOS-4284) have no
        # Python fallback left to feed. h_start_date stays: h_commit_rows below
        # is unrelated (file/complexity history), not a testops fetch.
        # CHAOS-5234/CHAOS-3092: incident's daily compute is DELETED here,
        # not skip-gated -- chris's standing rule (CHAOS-5233): once a
        # family's Go executor is on main, its Python compute is deleted,
        # never skip-gated. IncidentExecutor (native Go, CHAOS-4269/CHAOS-4295,
        # WITH the NULL-guard fix this Python path never had -- port-with-fix
        # standing order) is now the only writer of incident_metrics_daily
        # for a daily partition. The loader.load_incidents call this
        # replaces fed ONLY compute_incident_metrics_daily -- verified via
        # rg that incident_rows was never read anywhere else in this
        # function (mttr_by_repo/bug_times just below iterate work_items,
        # unrelated).

        work_items: list[Any] = []
        work_item_transitions: list[Any] = []
        if load_work_items_enabled and load_work_items_from_db:
            work_items, work_item_transitions = await loader.load_work_items(
                start, end, repo_id, repo_name
            )

        # CHAOS-5308/CHAOS-3092: repo_user_commit's daily compute+write
        # (formerly `compute_daily_metrics` -> write_repo_metrics/
        # write_user_metrics/write_commit_metrics) is DELETED here, not
        # skip-gated -- chris's ruling: "once go is in main that does the
        # same thing, skip flags are pointless." The native Go executor
        # (RepoUserCommitExecutor, CHAOS-4275) is the only writer of
        # repo_metrics_daily/user_metrics_daily/commit_metrics now.
        # `compute_daily_metrics` itself IS ALSO deleted (compute.py, along
        # with `DailyMetricsResult` in schemas.py and `commit_size_bucket`,
        # its only other caller) -- rg confirmed zero production callers
        # outside this call site; its remaining "callers" were dedicated
        # unit tests (test_metrics_rework/reciprocity/quality/cognitive_
        # load/compute.py, tests/fixtures/test_pr_review_latency_coverage.py,
        # all deleted in this same PR) and the golden generator/rot-guard
        # pair for tests/fixtures/repo_user_commit_python_golden.json
        # (generate_repo_user_commit_python_golden.py and
        # internal/jobs/metrics/daily/repouser/golden_rot_guard_test.go, both
        # deleted -- the frozen golden JSON and Go's own
        # TestComputeMatchesFrozenPythonGolden stay, they need no live
        # Python). The whole feeder block this call needed -- h_commit_rows,
        # active_repos, and the mttr_by_repo/rework_ratio_by_repo/
        # single_owner_ratio_by_repo/bus_factor_by_repo/gini_by_repo per-repo
        # loop -- is deleted with it: none of those five dicts nor
        # h_commit_rows/active_repos had any other consumer in this
        # function. `compute_rework_churn_ratio`/`compute_single_owner_
        # file_ratio` (quality.py) are ALSO deleted -- zero production
        # callers anywhere (quality.py's only production importer was this
        # file; the whole module is deleted). `compute_code_ownership_gini`
        # and `compute_bus_factor` (both knowledge.py) STAY -- both have a
        # real, unrelated caller each: compute_code_ownership_gini is
        # imported directly by tests/fixtures/generate_pysum_golden.py
        # (CHAOS-4824's floating-point-summation golden), compute_bus_factor
        # (api/graphql/resolvers/bus_factor.py) -- only this file's now-dead
        # import of it is removed.
        #
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
        #
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

        # CHAOS-5234/CHAOS-3092: team_wellbeing's daily compute is DELETED
        # here, not skip-gated -- the native Go executor (TeamWellbeing
        # Executor, CHAOS-4276) is the only writer of team_metrics_daily
        # for a daily partition now. No `team_metrics` local either -- it
        # is never read anywhere else in this function (the write call and
        # the zero-rows/repo-fan-out observer below are all deleted too, see
        # their own comments). commit_rows/team_resolver are NOT touched --
        # they are shared inputs also used above/below for other families,
        # verified via rg that this compute call was not their only reader.
        # repo_team_resolver/repo_names_by_id, unlike commit_rows/
        # team_resolver, are NOT shared any more -- CHAOS-5308/CHAOS-5310/
        # CHAOS-5321 later deleted every OTHER reader too, so that whole
        # block is deleted outright above (see this function's own top).
        #
        # CHAOS-5308/CHAOS-3092: the compute_daily_metrics call that used to
        # sit here (repo_user_commit's compute) is also deleted -- its own
        # native Go executor (RepoUserCommitExecutor) is the only writer of
        # repo_metrics_daily/user_metrics_daily/commit_metrics now, and
        # `result` had no other reader in this function once the writes and
        # the compounding_risk feed below are gone too.

        # CHAOS-5310/CHAOS-3092: work_item's daily compute+write is DELETED
        # here, not skip-gated -- chris's standing rule (CHAOS-5233): once a
        # family's Go executor is on main, its Python compute is deleted,
        # never skip-gated. WorkItemExecutor (native Go, CHAOS-4283) is now
        # the only writer of work_item_metrics_daily/work_item_user_metrics_
        # daily/work_item_cycle_times for a daily partition.
        # `compute_work_item_metrics_daily` ITSELF is also deleted (from
        # compute_work_items.py) -- its only other caller,
        # job_work_items.py's run_work_items_sync_job, is reachable but not a
        # production writer: prod Celery has been stopped since 2026-08-19,
        # so nothing in production dispatches it (R6). That call site is
        # deleted in the same PR; run_work_items_sync_job itself stays for
        # its other, unrelated work (compute_work_item_engine_destinations_
        # daily) pending its own follow-up deletion ticket.
        #
        # CHAOS-5321/CHAOS-3092: work_item_attribution's daily compute is
        # ALSO fully deleted (was already not called here, see git history --
        # #2246 removed this call site) -- `compute_work_item_team_
        # attributions` itself is now deleted too, for the same R6 reason as
        # work_item above: WorkItemAttributionExecutor (native Go) is the
        # only writer of work_item_team_attributions, and its remaining
        # Python caller (run_work_items_sync_job) is unreachable in
        # production since the 2026-08-19 Celery stop.
        #
        # CHAOS-5323/CHAOS-3092: work_item_estimate's daily compute+write was
        # already deleted (its own compute function is gone too, see git
        # history) -- WorkItemEstimateExecutor (native Go) is the only
        # writer of estimate_coverage_metrics_daily now.
        #
        # CHAOS-5321/CHAOS-3092: work_item_state's daily compute+write is
        # ALSO fully deleted here -- WorkItemStateExecutor (native Go,
        # CHAOS-4278) is the only writer of work_item_state_durations_daily
        # for a daily partition now. `compute_work_item_state_durations_
        # daily` itself is deleted too, for the same R6 reason: its
        # remaining Python caller (run_work_items_sync_job) is unreachable
        # in production since the 2026-08-19 Celery stop.

        # CHAOS-4279: this job no longer calls compute_review_edges_daily
        # (src/dev_health_ops/metrics/reviews.py) or names "review_edges" in
        # skip_families at all -- ReviewEdgesExecutor is unconditionally
        # registered whenever the daily worker starts (same reachability
        # analysis as team_cognitive_load/team_complexity/benchmarking,
        # CHAOS-5141/CHAOS-5051/CHAOS-4288), so a construction-time fallback
        # to Python was never actually reachable from this call site in
        # production.
        # CHAOS-5234/CHAOS-3092: cicd's daily compute is DELETED here, not
        # skip-gated -- chris's standing rule (CHAOS-5233): once a family's
        # Go executor is on main, its Python compute is deleted, never
        # skip-gated. CICDExecutor (native Go, CHAOS-4292) is now the only
        # writer of cicd_metrics_daily for a daily partition. `pipeline_rows`
        # itself is deleted too (CHAOS-5308, see the `load_cicd_data` removal
        # above) -- this compute's own call site was its only reader.
        #
        # CHAOS-5234/CHAOS-3092/CHAOS-5309: deploy's daily compute is DELETED
        # here too, not skip-gated -- same standing rule. DeployExecutor
        # (native Go, CHAOS-4293) is now the only writer of
        # deploy_metrics_daily for a daily partition. compute_deploy_metrics_daily
        # itself is ALSO deleted (from compute_deployments.py) -- rg confirmed
        # job_daily.py was its only real caller. CHAOS-5336: the sibling
        # constant DEPLOYMENT_FAILURE_STATUSES, whose only OTHER caller was
        # compute_dora.py, is now genuinely orphaned -- compute_deployments.py
        # (its defining module) is deleted outright along with compute_dora.py,
        # not left behind as a dead single-constant file. `deployment_rows`
        # itself (the raw loader data) is ALSO deleted here (CHAOS-5308, see
        # the `load_cicd_data` removal above) -- `active_repos`, its last
        # remaining consumer, is deleted too.
        #
        # CHAOS-5234/CHAOS-3092: incident's daily compute is DELETED here too
        # (see the loader.load_incidents removal above) -- no
        # write_incident_metrics call left either; IncidentExecutor (native
        # Go) is the only writer now.
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

        # CHAOS-5308/CHAOS-3092: no skip_repo_user_commit_write here anymore
        # -- repo_user_commit's compute+write (result.repo_metrics/
        # user_metrics/commit_metrics) is deleted entirely, not skip-gated;
        # RepoUserCommitExecutor (native Go, CHAOS-4275) is the only writer
        # now. compounding_risk (repo scope), the one other consumer of
        # `result.repo_metrics`, is ALSO deleted outright above -- see the
        # comment above the deleted `compute_daily_metrics` call site
        # earlier in this function.
        # CHAOS-5234/CHAOS-3092: no skip_deploy_write here anymore -- deploy
        # used to have the SAME write-only-skip shape as repo_user_commit
        # above (deploy_metrics fed `_note_family_zero_rows("deploy", ...)`,
        # the CHAOS-4246/CHAOS-4263 staleness check, so only the write could
        # be skipped). CHAOS-4293's DeployExecutor being native superseded
        # that: the compute+write+zero-rows-note are all deleted together
        # now, not skip-gated -- see the comment above the deleted
        # `compute_deploy_metrics_daily` call site earlier in this function.
        # CHAOS-4277/CHAOS-5234/CHAOS-3092: file_risk_hotspots (like
        # file_hotspots, its sibling CHAOS-4277 family) no longer checks
        # skip_families at all -- its compute+write is deleted entirely, see
        # the comment above the deleted `compute_file_risk_hotspots` call
        # site earlier in this function.
        # CHAOS-5310/CHAOS-3092: work_item (WorkItemExecutor) no longer has a
        # skip flag here at all -- R6/CHAOS-5233 deleted its compute+write
        # outright, same as work_item_estimate below: there is no Python
        # fallback to keep alive for the daily partition anymore.
        #
        # work_item_estimate (WorkItemEstimateExecutor, also CHAOS-4283) no
        # longer has a skip flag here at all -- CHAOS-5323/CHAOS-3092 deleted
        # its compute+write outright (see that call site's comment above for
        # the full deletion, including its compute function and both former
        # Python callers): there is no Python fallback to keep alive for the
        # daily partition anymore.
        #
        # CHAOS-5216/CHAOS-5234/CHAOS-3092: no skip_work_graph_edges_write
        # here -- work_graph_edges' compute+write (both deleted alongside
        # _extract_ai_workflow_for_day above) is not skip-gated;
        # WorkGraphEdgesExecutor (native Go) is the only writer now.
        #
        # CHAOS-5321/CHAOS-3092: work_item_state (WorkItemStateExecutor) has
        # the same shape -- no skip flag, compute+write both deleted outright.
        #
        # CHAOS-5323/CHAOS-3092: no skip_work_item_estimate_write here --
        # deleted alongside the compute call above, not skip-gated.
        # CHAOS-5234/CHAOS-3092: no skip_ai_governance_write here -- deleted
        # alongside the compute call above, not skip-gated.
        # CHAOS-5234/CHAOS-3092: no skip_ai_impact_write here either -- same
        # deletion, not skip-gated. AIImpactExecutor (native Go) is the only
        # writer of ai_impact_metrics_daily now.
        # CHAOS-5308/CHAOS-3092/CHAOS-5310/CHAOS-5321 (final family in this
        # cascade): the `for s in sinks:` loop that used to sit here is
        # deleted too -- EVERY write call inside it (repo/user/commit
        # metrics, team metrics, work-item metrics/user-metrics/cycle-times/
        # state-durations/estimate-coverage/team-attributions, review edges,
        # cicd, deploy, incident, ai governance/impact/workflow, work-graph
        # edges, file hotspots/risk-hotspots) has been deleted across this
        # PR and its sibling PRs merged into this same tree; a `for` loop
        # with no live statement in its body is dead code (and, taken
        # literally, no longer valid Python), so the loop itself goes with
        # them, not just its individual calls. Every one of those families'
        # native Go executor is the sole writer now.
        #
        # CHAOS-5308/CHAOS-3092: no write_repo_metrics/write_user_metrics/
        # write_commit_metrics call here -- deleted alongside the
        # compute_daily_metrics call above; RepoUserCommitExecutor
        # (native Go) is the only writer of repo_metrics_daily/
        # user_metrics_daily/commit_metrics now.
        # CHAOS-5234/CHAOS-3092: no write_team_metrics call here either --
        # deleted alongside the compute call above; TeamWellbeingExecutor
        # (native Go) is the only writer of team_metrics_daily now.
        # CHAOS-5310/CHAOS-5321/CHAOS-3092: no write_work_item_metrics /
        # write_work_item_user_metrics / write_work_item_cycle_times /
        # write_work_item_state_durations calls here -- deleted alongside
        # the compute calls above; the native Go executors are the only
        # writers now.
        # CHAOS-5323/CHAOS-3092: no write_estimate_coverage_metrics call
        # here -- deleted alongside the compute call above; the native
        # Go executor is the only writer now.
        # CHAOS-5233/CHAOS-3092: no write_work_item_team_attributions
        # call here -- deleted alongside the compute call above; the
        # native Go executor is the only writer now.
        # CHAOS-5310/CHAOS-5321/CHAOS-3092: no write_work_item_state_durations
        # call here either -- deleted alongside the compute call above; the
        # native Go executor is the only writer now.
        # CHAOS-4279: no write_review_edges call here anymore -- see the
        # compute-block comment above.
        # CHAOS-5234/CHAOS-3092: no write_cicd_metrics call here either --
        # deleted alongside the compute call above; CICDExecutor (native
        # Go) is the only writer now.
        # CHAOS-5245 deleted testops_pipeline/testops_test/testops_coverage's
        # Python compute+write entirely (their native Go executors,
        # CHAOS-4284, have no fallback left) -- there is nothing left here
        # to gate.
        # CHAOS-5234/CHAOS-3092/CHAOS-5309: no write_deploy_metrics call
        # here -- deleted alongside the compute call above; DeployExecutor
        # (native Go) is the only writer now.
        # CHAOS-5234/CHAOS-3092: no write_incident_metrics call here
        # either -- deleted alongside the compute call above;
        # IncidentExecutor (native Go) is the only writer now.
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

        # CHAOS-5234/CHAOS-3092/CHAOS-5309: no _note_family_zero_rows for
        # "cicd", "deploy", or "incident" here any more -- all three
        # families' Python compute is DELETED (see the compute-call removals
        # above), so a permanent [] would always be a FALSE zero-rows-
        # computed signal now; the native executors' own anomaly detection
        # is ClickHouseSourceDataChecker (Go), not this Python-side note.

        # CHAOS-5308/CHAOS-3092: no compounding_risk (REPO scope) call here
        # anymore -- deleted entirely, not skip-gated; CompoundingRiskExecutor
        # (native Go, CHAOS-4287) is the only writer of REPO-scope
        # compounding_risk_daily rows now. `_write_compounding_risk_for_day`
        # itself is ALSO deleted (see the comment above the deleted
        # `compute_daily_metrics` call site earlier in this function).
        #
        # TEAM-scope rows are NOT covered by this deletion -- CHAOS-5084/
        # no-straddle (#2275 v2): CompoundingRiskTeamExecutor (Go) is the
        # SOLE writer for team scope now, with no Python compute or
        # skip_families gate for it anywhere in this module at all (the old
        # run_daily_metrics_finalize call site, _write_compounding_risk_team_rows_for_day,
        # is deleted, not merely gated) -- same no-fail-open posture
        # ic_finalize/team_cognitive_load's own finalize-scope families
        # already have.

        # CHAOS-4365 finalize-step fix: team_cognitive_load has no repo-scope
        # table at all -- it is emitted ONCE per org/day from
        # run_daily_metrics_finalize (after every repo's partition has
        # landed), not here. This function runs once per repo (CHAOS-4264),
        # so writing a "complete" team row per-repo silently dropped every
        # other owned repo's contribution once argMax(computed_at) dedup
        # collapsed the redundant per-repo writes down to whichever repo's
        # partition ran last -- confirmed live before this fix.

        # CHAOS-5234/CHAOS-3092: no record_team_metrics_daily_repo_rows call
        # here either -- deleted alongside the compute+write calls above.
        # TeamWellbeingExecutor (native Go) already emits the same
        # dev_health_team_metrics_daily_repo_count series independently
        # (internal/jobruntime/telemetry.go's repoCountBuckets), so the
        # series stays alive; only this now-callerless Python recorder and
        # its instrument (metrics/prometheus.py) are gone.

        # CHAOS-5245 deleted testops_risk's Python compute+write entirely
        # (its native Go executor, TestopsRiskExecutor/CHAOS-4294, has no
        # fallback left, and its only input -- testops_pipeline_metrics/
        # testops_test_metrics/testops_coverage_metrics -- no longer exists
        # either now that CHAOS-4284's Python compute is also gone).

        # Benchmarking (baselines, maturity, anomalies, period comparisons,
        # correlations, insights) used to run HERE, then CHAOS-5194 (astra
        # design-review finding F3) moved it to run_daily_metrics_finalize so
        # its skip_families gate would line up with the native executor's own
        # finalize-scope registration. CHAOS-4288 has now deleted its Python
        # compute (and the moved-to call site) entirely: the native
        # BenchmarkingFinalizeExecutor (internal/jobs/metrics/daily/
        # benchmarking_finalize_native_executor.go) is unconditionally
        # registered whenever the daily worker starts -- same reachability
        # analysis as team_cognitive_load/team_complexity (CHAOS-5141/
        # CHAOS-5051): buildDailyWorker refuses the whole daily worker before
        # dailyNativeFamilyRegistrations is ever called if ClickHouse/Postgres
        # fail to open, so a construction-time fallback to Python was never
        # actually reachable in production. There is no gate line and no
        # fallback left to find here or in run_daily_metrics_finalize.

        # ic_finalize's Python compute (compute_ic_metrics_daily /
        # compute_ic_landscape_rolling) was deleted here (CHAOS-4290 PR3,
        # CHAOS-3092 no-straddle): the native Go executor has been the SOLE
        # writer for this family since #2241's finalize policy landed --
        # FinalizeHandler.computeNativeFinalizeFamilies never falls open to
        # this bridge on a native error (daily.go, "The bridge is NOT
        # called" -- see PR3's body for the exact citation), so this call
        # was already dead weight, never a live fallback, before its
        # deletion. `skip_finalize` (this function's own parameter) existed
        # only to gate this block and is removed with it (CHAOS-5254
        # independently arrived at the same deletion for the same code, one
        # commit earlier in this branch's history -- the two tickets'
        # reasoning is complementary, not conflicting: CHAOS-5254 traced
        # every live caller and found none still needed skip_finalize=False;
        # CHAOS-4290 PR3 additionally confirms the Go executor made this
        # block's OUTPUT itself redundant, not just its opt-out flag).

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
    # PR3's body for the no-fail-open citation).

    # CHAOS-4288 deleted benchmarking's Python compute (baselines, maturity,
    # anomalies, period comparisons, correlations, insights) entirely -- see
    # the sibling comment in run_daily_metrics_job (this function's old call
    # site, CHAOS-5194) for the reachability analysis. The native
    # BenchmarkingFinalizeExecutor has no Python fallback left; this
    # function no longer names "benchmarking" in skip_families at all.
    # `computed_at`/the `skip_families` normalisation that used to feed that
    # call are gone too. Benchmarking was the LAST body-level consumer of
    # `skip_families` here (ic_finalize/team_cognitive_load/
    # compounding_risk_team/team_complexity had each already lost theirs to
    # earlier PRs, per the reachability analysis two paragraphs down) --
    # `skip_families` stays on this function's SIGNATURE regardless, purely
    # for calling-convention parity with worker_metrics.py's generic
    # finalize dispatch (it passes the same Go-sourced skip set to every
    # finalize-scope Python entry point uniformly), but nothing in this
    # function's BODY reads it anymore.

    # CHAOS-4365 finalize-step fix: this used to be where team-scope
    # compounding_risk_daily, team_cognitive_load_daily, AND
    # team_complexity_daily were all written, exactly once here per org/day,
    # after every repo's own partition had landed -- never in-process inside
    # a single per-repo run_daily_metrics_job call (see
    # _write_compounding_risk_for_day's docstring for why that was wrong).
    # All three Python computes are now DELETED entirely, not merely
    # skip-gated: team_cognitive_load's (CHAOS-5141, #2294),
    # compounding_risk_team's (CHAOS-5084/no-straddle, #2275 v2), and
    # team_complexity's (CHAOS-5051, #2299) are each the SOLE writer for
    # their scope now via a native Go FinalizeHandler registration, with no
    # Python compat-bridge fallback path at all (same no-fail-open posture: a
    # native failure retries via River, it never falls open to a Python
    # recompute). The teams_data/repo_team_resolver/discovered_repos/
    # repo_names_by_id block that used to sit here fed only these three
    # deleted functions -- with all three gone, it had no remaining consumer
    # anywhere else in this function, so it is deleted too rather than left
    # as dead computation.
    #
    # Reachability analysis, same for all three: buildDailyWorker
    # (cmd/dev-health-worker/daily.go) refuses the WHOLE daily worker if the
    # ClickHouse connection fails to open, before
    # dailyNativeFamilyRegistrations is ever called -- so each family is
    # guaranteed to register natively in every real deployment (team_cognitive_load
    # and ic_finalize are co-registration-dependent on each other;
    # compounding_risk_team and team_complexity have no co-registration
    # dependency of their own). On a RUNTIME native failure,
    # FinalizeHandler.Work's computeNativeFinalizeFamilies error path
    # explicitly never calls the Python bridge either (daily.go's own
    # comment: "The bridge is NOT called"). No straddle, no live fallback
    # path for any of the three -- safe to delete outright rather than leave
    # skip_families-gated. See finalize_family_gate_agreement_test.go's
    # pythonGatedFinalizeFamilies (which excludes all three now) for the
    # Go-side proof this stays true.

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
# fixtures, tests) -- only the unwired CLI wrapper functions were removed.
# (Merge-forward notes: #2293/CHAOS-5263 separately removed
# `run_daily_metrics_job`'s `skip_finalize` parameter entirely while this
# dead code still called it with `skip_finalize=True`; #2306/CHAOS-5254
# later deleted `scripts/compute_metrics_daily.py` outright, one of this
# dead code's own comment's cited "live callers" -- both further confirm
# these two functions were unreachable and rotting, not a live code path
# anyone was maintaining.)
