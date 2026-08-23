from __future__ import annotations

from datetime import datetime
from typing import Any

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts
from .investment_membership_scope import (
    INVESTMENT_MEMBERSHIP_SCOPE_CTES,
    INVESTMENT_MEMBERSHIP_SCOPE_FILTER,
    record_stale_investment_membership_scope,
)

# NOTE: This CTE MUST stay tenant-scoped. The ReplacingMergeTree dedup key for
# work_unit_investments is (org_id, work_unit_id) (migration 027), so org_id is
# part of the row identity. We filter org_id BEFORE aggregating and group by
# (org_id, work_unit_id); otherwise two tenants sharing a provider-native
# work_unit_id collapse into a single argMax row and the outer
# `WHERE org_id = %(org_id)s` drops the losing tenant's data entirely
# (cross-org leak / undercount — CHAOS-2374). Every consumer of this CTE already
# supplies the `org_id` query param.
LATEST_WORK_UNIT_INVESTMENTS_CTE = f"""
{INVESTMENT_MEMBERSHIP_SCOPE_CTES},
        latest_work_unit_investments AS (
            SELECT
                work_unit_id,
                argMax(work_unit_type, computed_at) AS work_unit_type,
                argMax(work_unit_name, computed_at) AS work_unit_name,
                argMax(from_ts, computed_at) AS from_ts,
                argMax(to_ts, computed_at) AS to_ts,
                argMax(repo_id, computed_at) AS repo_id,
                argMax(provider, computed_at) AS provider,
                argMax(effort_metric, computed_at) AS effort_metric,
                argMax(effort_value, computed_at) AS effort_value,
                argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
                argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution_json,
                argMax(structural_evidence_json, computed_at) AS structural_evidence_json,
                argMax(evidence_quality, computed_at) AS evidence_quality,
                argMax(evidence_quality_band, computed_at) AS evidence_quality_band,
                argMax(categorization_status, computed_at) AS categorization_status,
                argMax(categorization_model_version, computed_at) AS categorization_model_version,
                argMax(categorization_run_id, computed_at) AS categorization_run_id,
                org_id,
                -- Alias must NOT be ``computed_at``: that name is the ordering
                -- column of every ``argMax(col, computed_at)`` above, and on
                -- ClickHouse 26.5.x an identically-named aggregate alias
                -- shadows the raw column, turning argMax into
                -- ``argMax(col, max(computed_at))`` → ILLEGAL_AGGREGATION (184)
                -- which silently empties the Investment treemap and allocation
                -- sankey. Keep the distinct name.
                max(computed_at) AS latest_computed_at
            FROM work_unit_investments
            WHERE org_id = %(org_id)s
{INVESTMENT_MEMBERSHIP_SCOPE_FILTER}
            GROUP BY org_id, work_unit_id
        )
""".rstrip()


# CHAOS-2777: work_unit_repo_effort is a ReplacingMergeTree deduped on
# computed_at with the RMT/GROUP key (org_id, work_unit_id, repo_id). Each
# materialize run stamps ALL of a unit's per-repo rows with a single run
# ``computed_at`` (materialize.py: one clock per run) -- INCLUDING the
# categorization-skipped path, which rewrites repo-effort even when the
# investment row is left untouched. So a unit's allocation is versioned
# independently of its work_unit_investments row, and if a later generation
# emits a SMALLER repo set (a repo's churn share drops to zero), the per-repo
# dedup key keeps the stale (unit, dropped-repo) row alive under its own older
# ``computed_at``. A naive per-repo argMax would then fan BOTH the current repos
# AND the stale repo out, over-counting effort and breaking the sum invariant.
#
# Fix: scope to the unit's LATEST allocation generation. ``d`` dedups each
# (org, work_unit_id, repo_id) to its newest row; ``g`` is the per-unit clock
# = max(computed_at) across ALL of that unit's repo rows (i.e. the newest run
# that touched it). We keep only rows whose own newest ``computed_at`` equals
# that per-unit clock, dropping repos that only exist in older generations.
# The clock is the allocation table's OWN per-unit max -- NOT the investments
# computed_at -- because the skipped path legitimately writes allocation rows
# newer than the investments row. ``has_allocation = 1`` is an explicit
# match flag so consumers never depend on a non-empty work_unit_id sentinel.
#
# KNOWN THEORETICAL EDGE (accepted, not fixed): two DIFFERENT generations of a
# unit's allocation sharing an IDENTICAL millisecond ``computed_at`` cannot be
# told apart by this clock — a stale repo row from the tied older generation
# would survive. Reaching it requires a retried chunk within one
# frozen-``computed_at`` run to compute a DIFFERENT repo set mid-run; there is
# no per-row version column beyond ``computed_at`` to break such a tie, and a
# schema change is not warranted for it (codex round-3 on CHAOS-2777).
LATEST_WORK_UNIT_REPO_EFFORT_CTE = """
        latest_work_unit_repo_effort AS (
            SELECT
                d.work_unit_id AS work_unit_id,
                d.repo_id AS repo_id,
                d.effort_metric AS effort_metric,
                d.repo_effort_value AS repo_effort_value,
                d.allocation_source AS allocation_source,
                d.org_id AS org_id,
                d.latest_repo_effort_computed_at AS latest_repo_effort_computed_at,
                1 AS has_allocation
            FROM (
                SELECT
                    work_unit_id,
                    repo_id,
                    org_id,
                    argMax(effort_metric, computed_at) AS effort_metric,
                    argMax(effort_value, computed_at) AS repo_effort_value,
                    argMax(allocation_source, computed_at) AS allocation_source,
                    max(computed_at) AS latest_repo_effort_computed_at
                FROM work_unit_repo_effort
                WHERE org_id = %(org_id)s
                GROUP BY org_id, work_unit_id, repo_id
            ) AS d
            INNER JOIN (
                SELECT
                    org_id,
                    work_unit_id,
                    max(computed_at) AS unit_generation_at
                FROM work_unit_repo_effort
                WHERE org_id = %(org_id)s
                GROUP BY org_id, work_unit_id
            ) AS g
                ON g.org_id = d.org_id
                AND g.work_unit_id = d.work_unit_id
            WHERE d.latest_repo_effort_computed_at = g.unit_generation_at
        )
""".rstrip()


# CHAOS-2777: multi-repo work units carry a NULL scalar ``repo_id`` in
# work_unit_investments; their real per-repo effort split lives in
# work_unit_repo_effort (migration 064, one row per (work_unit, repo)). This
# derived table LEFT JOINs the latest-generation per-repo allocation onto
# latest_work_unit_investments and, for units WITH allocation rows, fans each
# unit out to one row per allocated repo -- replacing the scalar ``repo_id`` /
# ``effort_value`` with the per-repo ``repo_id`` / ``repo_effort_value``. Units
# WITHOUT any allocation row keep their scalar repo_id + full effort_value. The
# match is gated on the explicit ``wure.has_allocation = 1`` flag (an unmatched
# LEFT JOIN yields 0 under join_use_nulls=0 and NULL otherwise, both of which
# fall through to the scalar branch), so no unit is ever dropped and we never
# rely on a non-empty work_unit_id sentinel. ``has_allocation`` is re-exposed so
# consumers (e.g. the unassigned-repo count) can tell "no allocation row" apart
# from "allocation row with a NULL repo".
#
# Because a unit's per-repo effort sums to its total effort by construction
# (materialize allocates the unit's effort across its repos) AND the CTE is
# scoped to the unit's latest allocation generation, a downstream
# ``sum(subcategory_kv.2 * effort_value)`` is UNCHANGED for single-repo units and
# split -- same total -- across repos for multi-repo units (the allocation sum
# invariant). Keyed on (org_id, work_unit_id) so tenant isolation is preserved.
# Mirrors the GraphQL compiler (api/graphql/sql/compiler.py) and coverage-stats
# (api/graphql/resolvers/analytics.py) scalar-fallback pattern. Callers MUST
# chain LATEST_WORK_UNIT_REPO_EFFORT_CTE into the WITH clause and expose the
# alias ``work_unit_investments`` so existing column references keep resolving.
REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE = """
            (
                SELECT
                    wui.work_unit_id AS work_unit_id,
                    wui.from_ts AS from_ts,
                    wui.to_ts AS to_ts,
                    wui.org_id AS org_id,
                    if(wure.has_allocation = 1, wure.repo_id, wui.repo_id) AS repo_id,
                    if(wure.has_allocation = 1, wure.repo_effort_value, wui.effort_value) AS effort_value,
                    if(wure.has_allocation = 1, 1, 0) AS has_allocation,
                    wui.subcategory_distribution_json AS subcategory_distribution_json,
                    wui.structural_evidence_json AS structural_evidence_json
                FROM latest_work_unit_investments AS wui
                LEFT JOIN latest_work_unit_repo_effort AS wure
                    ON wure.org_id = wui.org_id
                    AND wure.work_unit_id = wui.work_unit_id
            ) AS work_unit_investments
""".rstrip()


async def _query_investment_dicts(
    sink: BaseMetricsSink, query: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    org_id = str(params.get("org_id") or "")
    if org_id:
        await record_stale_investment_membership_scope(sink, org_id=org_id)
    return await query_dicts(sink, query, params)


# CHAOS-2492: work_unit_investments carries NO author/developer column at all
# (see the WorkUnitInvestmentRecord write columns in metrics/sinks/clickhouse/
# investment.py) -- the closest thing to a developer identity is the set of
# commit/PR node ids recorded in structural_evidence_json (written by
# work_graph/investment/materialize.py using the canonical
# "{repo_uuid}@{sha}" / "{repo_uuid}#pr{number}" id formats from
# work_graph/ids.py). This companion CTE resolves those refs to the REAL
# ClickHouse identity column -- author_email (git_commits, git_pull_requests;
# same column chosen in CHAOS-2385) -- and collapses to one deduplicated
# array of contributor emails per work unit.
#
# Must chain AFTER LATEST_WORK_UNIT_INVESTMENTS_CTE in the same WITH clause
# (references `latest_work_unit_investments`); only pulled in by callers that
# actually need developer filtering, to avoid the extra join cost otherwise.
# Tenant isolation: git_commits / git_pull_requests carry an org_id column
# (migration 027), and repo_id+hash / repo_id+number values CAN collide
# across tenants -- this exact collision risk is documented at
# work_graph/builder.py:1643 for the equivalent git_commits join. Both inner
# dedupe subqueries below MUST be scoped to the CURRENT org: the
# `WHERE org_id = %(org_id)s` filter (the org_id param is already supplied by
# every consumer of this CTE, since it is only ever chained onto the WITH
# clause alongside LATEST_WORK_UNIT_INVESTMENTS_CTE, which requires the same
# param) plus `org_id` in the GROUP BY keeps each org's argMax(author_email, ...)
# computed over ONLY that org's rows. The `ca.org_id = wui.org_id` /
# `pa.org_id = wui.org_id` join predicate is a second, redundant layer of the
# same tenant-scoping (mirrors the dual WHERE-filter + join pattern in
# work_graph/builder.py's PR/commit edge builder). Without BOTH layers, a
# repo_id+hash (or repo_id+number) collision across two orgs lets argMax pull
# ANOTHER org's author_email into this org's investment developer filter --
# a tenant-isolation leak.
LATEST_WORK_UNIT_AUTHORS_CTE = """
        work_unit_authors AS (
            SELECT
                work_unit_id,
                groupUniqArray(author_email) AS author_emails
            FROM (
                SELECT
                    wui.work_unit_id AS work_unit_id,
                    ca.author_email AS author_email
                FROM latest_work_unit_investments AS wui
                ARRAY JOIN JSONExtract(wui.structural_evidence_json, 'commits', 'Array(String)') AS commit_ref
                INNER JOIN (
                    SELECT
                        org_id,
                        concat(toString(repo_id), '@', hash) AS commit_ref,
                        argMax(author_email, last_synced) AS author_email
                    FROM git_commits
                    WHERE org_id = %(org_id)s
                    GROUP BY org_id, repo_id, hash
                ) AS ca ON ca.commit_ref = commit_ref AND ca.org_id = wui.org_id
                WHERE ca.author_email IS NOT NULL AND ca.author_email != ''

                UNION ALL

                SELECT
                    wui.work_unit_id AS work_unit_id,
                    pa.author_email AS author_email
                FROM latest_work_unit_investments AS wui
                ARRAY JOIN JSONExtract(wui.structural_evidence_json, 'prs', 'Array(String)') AS pr_ref
                INNER JOIN (
                    SELECT
                        org_id,
                        concat(toString(repo_id), '#pr', toString(number)) AS pr_ref,
                        argMax(author_email, last_synced) AS author_email
                    FROM git_pull_requests
                    WHERE org_id = %(org_id)s
                    GROUP BY org_id, repo_id, number
                ) AS pa ON pa.pr_ref = pr_ref AND pa.org_id = wui.org_id
                WHERE pa.author_email IS NOT NULL AND pa.author_email != ''
            )
            GROUP BY work_unit_id
        )
""".rstrip()


# CHAOS-2833: Investment Sankey/coverage team resolution previously joined
# work_item_cycle_times for team_id/team_name -- a cycle-time metrics rollup,
# not the authoritative attribution source. A work item can carry a primary
# row in work_item_team_attributions (the CHAOS-2600 precedence-resolved
# winner) with no matching work_item_cycle_times row, which surfaced as a
# false TEAM:unassigned Sankey node even though attribution already existed.
# This is the single source every Investment Sankey/coverage team join must
# read from: work_item_team_attributions FINAL, is_primary = 1, latest by
# computed_at per work_item_id (docs/contribute/architecture/team-attribution.md §0).
# Filter to the latest snapshot before reading plain nullable team fields so a
# newer primary row with NULL team fields clears an older assigned team.
# Centralized so no caller drifts back onto work_item_cycle_times.
PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE = """(
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = %(org_id)s
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = %(org_id)s
          GROUP BY work_item_id
      )
)""".rstrip()


# CHAOS-2416: the Investment team bridge. A work unit reaches a team only
# through the work-item refs recorded in structural_evidence_json. Before this
# change the ONLY bridge was the `issues` array, so a unit whose PR work item
# ALREADY carried a resolved team still landed in TEAM:unassigned whenever the
# unit had no issue ref of its own (49.6% of the unassigned effort in the
# 2026-08-22 prod probe -- e.g. work unit 9626aea0... with `issues: []` whose PR
# `ghpr:full-chaos/dev-health-ops#1726` was attributed to Fullchaos via
# linked_issue). The `prs` array holds work_graph/ids.py `generate_pr_id`
# refs -- "{repo_uuid}#pr{number}" -- which live in a DIFFERENT id space from
# work_items.work_item_id ("ghpr:{owner}/{repo}#{number}" for GitHub,
# "gitlab:{group}/{project}!{number}" for GitLab MRs, minted by
# providers/github/normalize.py and external_ingest/ids.py). Resolving that ref
# through the repos table lets the SAME primary-WITA source answer for PRs.
#
# This invents NO new attribution: it reuses the team the CHAOS-2600 resolver
# already computed for the PR work item, with that resolver's own precedence
# and provenance. A PR whose work item has no primary attribution row still
# contributes nothing, so a unit with no teamed evidence stays unassigned.
#
# Tenant scoping, the same dual-layer shape as LATEST_WORK_UNIT_AUTHORS_CTE
# above: `repos` is a ReplacingMergeTree whose dedup key is `(org_id, id)`
# (migration 027 rewrote it from the `(id)` declared in migration 000 -- read
# 027, not the CREATE TABLE, for the live key). So one repo UUID can hold a
# DISTINCT row per org, and it routinely does: `repos.id` is derived from the
# repo slug alone (`external_ingest/ids.py:derive_repo_uuid` ->
# `get_repo_uuid_from_repo`, no org in the seed), so two tenants syncing the
# same repo mint the SAME UUID and each keep their own row. A `GROUP BY id`
# without an org predicate would argMax ACROSS tenants and hand this org the
# other tenant's slug/provider, rewriting the evidence ref into a work-item id
# from the wrong namespace. Both layers below are required: the WHERE filters
# before aggregating, and the join carries org_id so the grouped row cannot
# migrate between tenants. argMax by last_synced then collapses the
# un-merged ReplacingMergeTree versions WITHIN one org so a re-synced repo
# cannot fan a unit's evidence out and skew the argMax(team, cnt) vote.
#
# Provider ambiguity, and why this FAILS CLOSED. The UUID seed is the slug
# without the provider, so a github `owner/repo` and a gitlab `owner/repo`
# inside ONE org collide on the same UUID. Picking a provider by argMax there
# would be arbitrary, and if BOTH the `ghpr:` and the `gitlab:` work item
# carry an attribution row the arbitrary winner can attach the unit to the
# OTHER provider's team -- a wrong team, not merely a missing one. So a UUID
# that resolves to more than one provider within the org yields an empty
# provider, and `RESOLVED_EVIDENCE_WORK_ITEM_ID` then bridges nothing: the
# unit stays `unassigned` rather than being attributed on a coin flip.
# (`provider` is never legitimately empty here -- migration 028 defaults it to
# 'unknown' -- so '' is unambiguous as an ambiguity sentinel.)
#
# Fail-closed only covers the window in which BOTH provider rows are still
# observable. `repos` dedups on (org_id, id), so once they merge the surviving
# provider is simply whichever synced last and the ambiguity is no longer
# detectable here. Repairing that means making the id seed provider-aware,
# which is a data-model change tracked in **CHAOS-4122**, not something this
# query layer can do; the sibling `LEFT JOIN repos AS r` joins share the same
# seed limitation.
WORK_UNIT_EVIDENCE_REPO_SOURCE = """(
    SELECT
        org_id,
        toString(id) AS repo_uuid,
        argMax(repo, last_synced) AS repo,
        -- Fail closed on a provider-ambiguous UUID: '' bridges nothing.
        if(uniqExact(provider) = 1, argMax(provider, last_synced), '') AS provider
    FROM repos
    WHERE org_id = %(org_id)s
    GROUP BY org_id, id
)""".rstrip()


# Every work-item ref a unit's structural evidence can reach a team through.
# CHAOS-2416 removed a third term, `[work_unit_investments.work_unit_id]`: a
# work_unit_id is a content hash (work_graph/investment materialization), never
# a work_item_id, so that arm could not match a single attribution row -- dead
# code that read as a real fallback.
WORK_UNIT_EVIDENCE_WORK_ITEM_REFS = """arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    JSONExtract(structural_evidence_json, 'prs', 'Array(String)')
                ))""".rstrip()


# Maps one evidence ref into the work_items id space.
#
# The three arms are keyed on the SHAPE of the ref, never on whether a join
# happened to succeed. An `issues` ref is already a work_item_id and passes
# through untouched. A `prs` ref matches `generate_pr_id`'s
# "{repo_uuid}#pr{number}" exactly and is rewritten to the provider's
# work-item id via the repos lookup. A `prs` ref whose repo UUID is NOT in
# `repos` resolves to '' -- deliberately matching no attribution row.
#
# That empty third arm matters: passing an unresolvable PR ref THROUGH as a
# literal join key would make the raw work-graph node id a live lookup key in
# a table that is only ever keyed by namespaced work_item_ids. That is the
# same shape as the `[work_unit_id]` term this ticket deleted -- an arm that
# can only ever match something that should not be there -- and a live
# negative control (`test_unknown_repo_uuid_stays_unassigned`) pins it.
#
# splitByString returns the whole string when the separator is absent and
# index [2] yields '' rather than raising, so every arm is safe to evaluate
# on every row; ClickHouse's multiIf is not short-circuiting.
RESOLVED_EVIDENCE_WORK_ITEM_ID = """multiIf(
                        NOT match(evidence_ref, '^[0-9a-fA-F-]{36}#pr[0-9]+$'),
                        evidence_ref,
                        evidence_repo.repo = '' OR evidence_repo.provider = '',
                        '',
                        concat(
                            if(evidence_repo.provider = 'gitlab', 'gitlab:', 'ghpr:'),
                            evidence_repo.repo,
                            if(evidence_repo.provider = 'gitlab', '!', '#'),
                            splitByString('#pr', evidence_ref)[2]
                        )
                    )""".rstrip()


def build_unit_team_subquery(
    *,
    source: str,
    where: str = "",
    inner_team_alias: str = "team",
    outer_team_alias: str | None = None,
    include_team_id: bool = False,
) -> str:
    """Build the shared per-work-unit team-resolution subquery.

    CHAOS-2416: this body used to be copy-pasted into eight call sites (five
    fetchers here, ``fetch_investment_quality_stats``' team-scope join, the
    GraphQL SQL compiler's team join and the analytics coverage resolver).
    A partial edit made those views disagree about which units have a team,
    so the body now has exactly one definition and every site renders it.

    TENANT SCOPING -- read before adding a call site. This subquery groups by
    ``work_unit_id`` alone and two of its callers (the GraphQL compiler and
    the analytics coverage resolver) pass no ``where`` at all, which reads
    like a cross-tenant leak given that ``work_unit_investments``' dedup
    identity is ``(org_id, work_unit_id)``. It is not one, because the
    invariant lives in ``source``: every caller passes either
    ``latest_work_unit_investments`` -- which applies
    ``WHERE org_id = %(org_id)s`` and groups by ``(org_id, work_unit_id)``
    before anything here sees a row -- or
    ``REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE``, which is built from that
    same CTE. The relation handed in is therefore already single-tenant, and
    ``PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE`` is org-filtered
    independently. **A new call site MUST pass an already-org-scoped
    source.** The property is pinned by
    ``test_shared_team_subquery_is_tenant_isolated`` (two orgs sharing one
    work_unit_id with conflicting PR evidence), not left to this comment.

    The ``repos`` lookup carries its own ``org_id`` filter and join column --
    see ``WORK_UNIT_EVIDENCE_REPO_SOURCE``; one repo UUID can hold a distinct
    row per tenant, so it must not be resolved by UUID alone.

    Args:
        source: the FROM clause -- ``latest_work_unit_investments AS
            work_unit_investments`` or the repo-allocated source, which must
            expose that same alias.
        where: an already-indented WHERE block, or "" for callers that filter
            in the enclosing query instead.
        inner_team_alias: deprecated spelling of ``team_alias``, kept so the
            existing call sites read unchanged.
        outer_team_alias: name the resolved team column is PROJECTED as.
            Defaults to inner_team_alias.
        include_team_id: also project the raw ``team_id``, for callers that
            filter on team id as well as label.
    """
    outer_team_alias = outer_team_alias or inner_team_alias
    team_expr = "ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, ''))"
    # The inner column name is FIXED and deliberately unlike any outer alias.
    # ClickHouse resolves an aggregate's arguments against the SELECT aliases
    # of the same scope, so `argMax(team_label, (cnt, team_label)) AS
    # team_label` reads as an aggregate nested inside itself and fails with
    # ILLEGAL_AGGREGATION (184) -- the same shadowing trap documented on
    # LATEST_WORK_UNIT_INVESTMENTS_CTE's `latest_computed_at` alias. Keeping
    # the vote column distinct from the projected name avoids it for every
    # caller, whatever they ask the output to be called.
    vote = "resolved_team"
    # ONE canonical aggregation key, shared with
    # `api/services/work_units.py:_majority_team_for_issues`: votes are
    # counted per TEAM ID, never per rendered label. Counting by label splits
    # one team into two candidates whenever two attribution rows spell the
    # same team_id with different team_names (names drift per work item), and
    # the drilldown -- which counts by id -- would then name a different team
    # for the same unit. The label is derived from the winning id as
    # ``max(label)`` so it is a deterministic function of the id, not of row
    # order, on both paths.
    #
    # Tie-break: highest vote count, then the lexicographically largest
    # team_id. `argMax(x, cnt)` alone is order-dependent when two teams tie,
    # and the PR bridge makes ties ordinary (one attributed issue for team A,
    # one attributed PR for team B). Ordering the id and the label by the SAME
    # key keeps them coming from one winning row rather than splicing two.
    # The vote counts DISTINCT RESOLVED work-item ids, not raw evidence refs.
    # `arrayDistinct` above only collapses identical ref STRINGS, and a PR
    # reachable both as a resolved `issues` entry (`ghpr:owner/repo#N`) and as
    # a `prs` ref (`{repo_uuid}#prN`) is two different strings that translate
    # to ONE work item. Counting refs would let that single item vote twice
    # here while the drilldown -- which de-duplicates after translation --
    # counted it once, and the two surfaces would disagree.
    vote_id = "resolved_team_id"
    tie_break = f"(cnt, {vote_id})"
    outer_id = (
        f"                argMax({vote_id}, {tie_break}) AS team_id,\n"
        if include_team_id
        else ""
    )
    return f"""
            SELECT
                work_unit_id,
{outer_id}                argMax({vote}, {tie_break}) AS {outer_team_alias}
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_id, ''), '') AS {vote_id},
                    max({team_expr}) AS {vote},
                    uniqExactIf({RESOLVED_EVIDENCE_WORK_ITEM_ID}, {team_expr} IS NOT NULL) AS cnt
                FROM {source}
                ARRAY JOIN {WORK_UNIT_EVIDENCE_WORK_ITEM_REFS} AS evidence_ref
                LEFT JOIN {WORK_UNIT_EVIDENCE_REPO_SOURCE} AS evidence_repo
                    ON evidence_repo.org_id = work_unit_investments.org_id
                    AND evidence_repo.repo_uuid = splitByString('#pr', evidence_ref)[1]
                LEFT JOIN {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE} AS t
                    ON t.work_item_id = {RESOLVED_EVIDENCE_WORK_ITEM_ID}
{where}
                GROUP BY work_unit_id, {vote_id}
            )
            GROUP BY work_unit_id
"""


def unit_team_window_filter(scope_filter: str, category_filter: str = "") -> str:
    """The window/tenant/scope WHERE block the investment fetchers push into
    ``build_unit_team_subquery``. Kept beside the builder so the five fetchers
    cannot drift apart on which rows the team vote is taken over."""
    return f"""                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                  AND work_unit_investments.org_id = %(org_id)s
                {scope_filter}
                {category_filter}"""


async def fetch_investment_breakdown(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            subcategory_kv.1 AS subcategory,
            splitByChar('.', subcategory_kv.1)[1] AS theme,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY subcategory, theme
        ORDER BY value DESC
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_mock_fixture_investment_row_count(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> int:
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    filters: list[str] = []
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT count() AS count
        FROM latest_work_unit_investments AS work_unit_investments
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
          AND (
            lower(ifNull(work_unit_investments.provider, '')) IN ('mock', 'fixture', 'fixtures', 'synthetic')
            -- %% is required: clickhouse-connect applies pyformat
            -- substitution to queries using %%(name)s params; literal
            -- percent signs must be doubled to survive query %% params.
            OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%%mock%%'
            OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%%synthetic%%'
            OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%%fixture%%'
          )
        {scope_filter}
        {category_filter}
    """
    rows = await _query_investment_dicts(sink, query, params)
    if not rows:
        return 0
    return int(rows[0].get("count") or 0)


async def fetch_investment_edges(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
) -> list[dict[str, Any]]:
    theme_filter = ""
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        theme_filter = " AND theme_kv.1 IN %(themes)s"
        params["themes"] = themes
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            theme_kv.1 AS source,
            ifNull(r.repo, toString(repo_id)) AS target,
            sum(theme_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(theme_distribution_json AS Array(Tuple(String, Float32))) AS theme_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {theme_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_investment_subcategory_edges(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            subcategory_kv.1 AS source,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_investment_team_edges(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    unit_team_cte = build_unit_team_subquery(
        source="latest_work_unit_investments AS work_unit_investments",
        where=unit_team_window_filter(scope_filter),
    )
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        unit_team AS ({unit_team_cte}        )
        SELECT
            subcategory_kv.1 AS source,
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_investment_repo_team_edges(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    unit_team_cte = build_unit_team_subquery(
        source=REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE,
        where=unit_team_window_filter(scope_filter),
    )
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        {LATEST_WORK_UNIT_REPO_EFFORT_CTE},
        unit_team AS ({unit_team_cte}        )
        SELECT
            subcategory_kv.1 AS subcategory,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM {REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE}
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY subcategory, repo, team
        ORDER BY value DESC
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_investment_team_category_repo_edges(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    unit_team_cte = build_unit_team_subquery(
        source=REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE,
        where=unit_team_window_filter(scope_filter),
    )
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        {LATEST_WORK_UNIT_REPO_EFFORT_CTE},
        unit_team AS ({unit_team_cte}        )
        SELECT
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            splitByChar('.', subcategory_kv.1)[1] AS category,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM {REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE}
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY team, category, repo
        ORDER BY value DESC
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_investment_team_subcategory_repo_edges(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    unit_team_cte = build_unit_team_subquery(
        source=REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE,
        where=unit_team_window_filter(scope_filter),
    )
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        {LATEST_WORK_UNIT_REPO_EFFORT_CTE},
        unit_team AS ({unit_team_cte}        )
        SELECT
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            subcategory_kv.1 AS subcategory,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM {REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE}
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY team, subcategory, repo
        ORDER BY value DESC
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_investment_unassigned_counts(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> dict[str, int]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append(
            "arrayExists(k -> splitByChar('.', k)[1] IN %(themes)s, mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))))"
        )
        params["themes"] = themes
    if subcategories:
        filters.append(
            "hasAny(mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))), %(subcategories)s)"
        )
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    unit_team_cte = build_unit_team_subquery(
        source=REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE,
        where=unit_team_window_filter(scope_filter, category_filter),
    )
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        {LATEST_WORK_UNIT_REPO_EFFORT_CTE},
        unit_team AS ({unit_team_cte}        )
        SELECT
            -- CHAOS-2777: a unit is only "missing repo" when it has a NULL scalar
            -- repo_id AND no per-repo allocation row at all. Multi-repo units carry
            -- a NULL scalar repo_id but DO have work_unit_repo_effort rows mapping
            -- their effort to real repos, so they must NOT be counted as unassigned.
            -- Reading from the same repo-allocated source as the Sankey edges keeps
            -- the repo scope_filter applied to the fanned repo_id: there
            -- ``has_allocation = 0 AND repo_id IS NULL`` is exactly the
            -- no-allocation + NULL-scalar unit (allocated repos are non-null and
            -- flagged), and countDistinct collapses the per-repo fan-out.
            countDistinctIf(
                work_unit_investments.work_unit_id,
                has_allocation = 0 AND repo_id IS NULL
            ) AS missing_repo,
            countDistinctIf(
                work_unit_investments.work_unit_id,
                ifNull(nullIf(unit_team.team, ''), '') = ''
            ) AS missing_team
        FROM {REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE}
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
    """
    rows = await _query_investment_dicts(sink, query, params)
    if not rows:
        return {"missing_team": 0, "missing_repo": 0}
    row = rows[0]
    return {
        "missing_team": int(row.get("missing_team") or 0),
        "missing_repo": int(row.get("missing_repo") or 0),
    }


async def fetch_investment_sunburst(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit": limit,
    }
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            subcategory_kv.1 AS subcategory,
            splitByChar('.', subcategory_kv.1)[1] AS theme,
            ifNull(r.repo, toString(repo_id)) AS scope,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY theme, subcategory, scope
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    return await _query_investment_dicts(sink, query, params)


async def fetch_investment_quality_stats(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
    team_scope_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Fetch aggregated evidence quality stats: mean, stddev, band counts."""
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append(
            "hasAny(mapKeys(CAST(theme_distribution_json AS Map(String, Float32))), %(themes)s)"
        )
        params["themes"] = themes
    if subcategories:
        filters.append(
            "hasAny(mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))), %(subcategories)s)"
        )
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    team_join = ""
    team_filter = ""
    if team_scope_ids:
        params["team_scope_ids"] = team_scope_ids
        unit_team_cte = build_unit_team_subquery(
            source="latest_work_unit_investments AS work_unit_investments",
            where=unit_team_window_filter(""),
            inner_team_alias="team_label",
            include_team_id=True,
        )
        team_join = f"""
        LEFT JOIN ({unit_team_cte}        ) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        """
        team_filter = """
          AND (
              unit_team.team_label IN %(team_scope_ids)s
              OR unit_team.team_id IN %(team_scope_ids)s
          )
        """
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            count() AS total,
            countIf(evidence_quality IS NOT NULL) AS quality_known_count,
            avgIf(evidence_quality, evidence_quality IS NOT NULL) AS quality_mean,
            stddevPopIf(evidence_quality, evidence_quality IS NOT NULL) AS quality_stddev,
            countIf(evidence_quality_band = 'high') AS high_count,
            countIf(evidence_quality_band = 'moderate') AS moderate_count,
            countIf(evidence_quality_band = 'low') AS low_count,
            countIf(evidence_quality_band = 'very_low') AS very_low_count,
            countIf(evidence_quality IS NULL OR evidence_quality_band = '') AS unknown_count
        FROM latest_work_unit_investments AS work_unit_investments
        {team_join}
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {team_filter}
        {category_filter}
    """
    rows = await _query_investment_dicts(sink, query, params)
    if not rows:
        return {}
    return dict(rows[0])
