from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from dev_health_ops.metrics.loaders.base import parse_uuid
from dev_health_ops.metrics.query_builder import OrgScopedQuery
from dev_health_ops.metrics.schemas import (
    AIImpactMetricsDailyRecord,
    AIOperatingLeverageComponents,
    AIPullRequestAttributionRow,
)


class AIImpactClickHouseLoader:
    def __init__(self, client: Any, org_id: str = "") -> None:
        self.client = client
        self.org_id = org_id
        self._scope = OrgScopedQuery(org_id)

    async def load_ai_pr_attributions(
        self,
        *,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None = None,
        repo_ids: list[uuid.UUID] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[AIPullRequestAttributionRow]:
        """Load AI-attributed PR rows.

        ``repo_ids`` narrows the PR universe in SQL *before* LIMIT/OFFSET so
        team-scoped pagination stays dense (CHAOS-2180 Wave 2). Resolvers
        compute it from team repo patterns; ``None`` means no constraint.
        """
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
        }
        repo_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            repo_filter = "AND pr.repo_id = {repo_id:UUID}"
        if repo_ids is not None:
            params["repo_ids"] = [str(r) for r in repo_ids]
            repo_filter += (
                "\n              AND toString(pr.repo_id) IN {repo_ids:Array(String)}"
            )
        params = self._scope.inject(params)
        org_filter_attr = self._scope.filter_uuid(alias="attr")
        org_filter_pr = self._scope.filter(alias="pr")
        limit_clause = ""
        if limit is not None and int(limit) > 0:
            params["limit"] = int(limit)
            params["offset"] = max(0, int(offset))
            limit_clause = "LIMIT {limit:UInt32} OFFSET {offset:UInt32}"

        # team_id is left empty here; resolvers that need team-scoped filtering
        # resolve it in application code via RepoPatternTeamResolver (see
        # resolve_ai_attributed_prs in resolvers/ai.py).  teams.repo_patterns is
        # an Array(String) of fnmatch glob patterns over repo full-names, so a
        # SQL JOIN on repo UUID would never match.
        query = f"""
        SELECT
            repo_id,
            number,
            kind,
            work_type,
            team_id,
            title,
            merged_at
        FROM (
            SELECT
                pr.repo_id AS repo_id,
                pr.number AS number,
                attr.kind AS kind,
                coalesce(nullIf(wi.type, ''), 'pull_request') AS work_type,
                CAST('', 'String') AS team_id,
                pr.title AS title,
                pr.merged_at AS merged_at
            FROM git_pull_requests AS pr
            INNER JOIN work_graph_issue_pr AS link
                ON link.repo_id = pr.repo_id AND link.pr_number = pr.number
            INNER JOIN ai_attribution_resolved AS attr
                ON attr.subject_type = 'pull_request'
                AND attr.subject_id = link.work_item_id
                AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
            LEFT JOIN work_items AS wi FINAL
                ON wi.repo_id = link.repo_id AND wi.work_item_id = link.work_item_id
            WHERE ((pr.created_at >= {{start:DateTime}} AND pr.created_at < {{end:DateTime}})
                OR (pr.merged_at IS NOT NULL AND pr.merged_at >= {{start:DateTime}} AND pr.merged_at < {{end:DateTime}}))
              {repo_filter}
              {org_filter_pr}
              {org_filter_attr}
            UNION ALL
            SELECT
                pr.repo_id AS repo_id,
                pr.number AS number,
                attr.kind AS kind,
                'pull_request' AS work_type,
                CAST('', 'String') AS team_id,
                pr.title AS title,
                pr.merged_at AS merged_at
            FROM git_pull_requests AS pr
            INNER JOIN ai_attribution_resolved AS attr
                ON attr.subject_type = 'pull_request'
                AND attr.repo_id = pr.repo_id
                AND (attr.subject_id = toString(pr.number) OR attr.subject_id = concat(toString(pr.repo_id), '#', toString(pr.number)))
                AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
            WHERE ((pr.created_at >= {{start:DateTime}} AND pr.created_at < {{end:DateTime}})
                OR (pr.merged_at IS NOT NULL AND pr.merged_at >= {{start:DateTime}} AND pr.merged_at < {{end:DateTime}}))
              {repo_filter}
              {org_filter_pr}
              {org_filter_attr}
        )
        ORDER BY merged_at DESC NULLS LAST, repo_id, number DESC
        {limit_clause}
        """
        raw_rows = await query_dicts(self.client, query, params)
        rows: list[AIPullRequestAttributionRow] = []
        seen: set[tuple[str, int]] = set()
        for raw in raw_rows:
            parsed_repo_id = parse_uuid(raw.get("repo_id"))
            if parsed_repo_id is None:
                continue
            number = int(raw.get("number") or 0)
            dedupe_key = (str(parsed_repo_id), number)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(
                {
                    "repo_id": parsed_repo_id,
                    "number": number,
                    "kind": raw.get("kind"),
                    "work_type": raw.get("work_type"),
                    "team_id": raw.get("team_id") or None,
                    "title": raw.get("title"),
                    "merged_at": raw.get("merged_at"),
                }
            )
        return rows

    async def load_ai_impact_metrics(
        self,
        *,
        start_day: date,
        end_day: date,
        repo_id: uuid.UUID | None = None,
        team_id: str | None = None,
        work_type: str | None = None,
    ) -> list[AIImpactMetricsDailyRecord]:
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
        filters = ["day >= {start_day:Date}", "day <= {end_day:Date}"]
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            filters.append("repo_id = {repo_id:UUID}")
        if team_id is not None:
            params["team_id"] = team_id
            filters.append("team_id = {team_id:String}")
        if work_type is not None:
            params["work_type"] = work_type
            filters.append("work_type = {work_type:String}")
        params = self._scope.inject(params)
        org_expr = self._scope.expression()
        if org_expr:
            filters.append(org_expr)
        where_clause = " AND ".join(filters)
        query = f"""
        SELECT
            org_id,
            team_id,
            repo_id,
            work_type,
            day,
            attribution_bucket,
            argMax(prs_total, metrics.computed_at) AS prs_total,
            argMax(prs_merged, metrics.computed_at) AS prs_merged,
            argMax(ai_assisted_prs, metrics.computed_at) AS ai_assisted_prs,
            argMax(agent_created_prs, metrics.computed_at) AS agent_created_prs,
            argMax(human_prs, metrics.computed_at) AS human_prs,
            argMax(unknown_prs, metrics.computed_at) AS unknown_prs,
            argMax(ai_assisted_pr_ratio, metrics.computed_at) AS ai_assisted_pr_ratio,
            argMax(agent_created_pr_count, metrics.computed_at) AS agent_created_pr_count,
            argMax(cycle_time_avg_hours, metrics.computed_at) AS cycle_time_avg_hours,
            argMax(baseline_cycle_time_avg_hours, metrics.computed_at) AS baseline_cycle_time_avg_hours,
            argMax(ai_cycle_time_delta_hours, metrics.computed_at) AS ai_cycle_time_delta_hours,
            argMax(reviews_per_pr, metrics.computed_at) AS reviews_per_pr,
            argMax(baseline_reviews_per_pr, metrics.computed_at) AS baseline_reviews_per_pr,
            argMax(ai_review_amplification, metrics.computed_at) AS ai_review_amplification,
            argMax(changes_requested_per_pr, metrics.computed_at) AS changes_requested_per_pr,
            argMax(rework_prs, metrics.computed_at) AS rework_prs,
            argMax(rework_drag_rate, metrics.computed_at) AS rework_drag_rate,
            argMax(followup_commits_count, metrics.computed_at) AS followup_commits_count,
            argMax(revert_prs, metrics.computed_at) AS revert_prs,
            argMax(revert_rate, metrics.computed_at) AS revert_rate,
            argMax(incidents_count, metrics.computed_at) AS incidents_count,
            argMax(incident_drag_rate, metrics.computed_at) AS incident_drag_rate,
            argMax(test_gap_prs, metrics.computed_at) AS test_gap_prs,
            argMax(test_gap_rate, metrics.computed_at) AS test_gap_rate,
            argMax(leverage_prs_component, metrics.computed_at) AS leverage_prs_component,
            argMax(leverage_cycle_time_component, metrics.computed_at) AS leverage_cycle_time_component,
            argMax(leverage_review_component, metrics.computed_at) AS leverage_review_component,
            argMax(leverage_rework_component, metrics.computed_at) AS leverage_rework_component,
            argMax(leverage_test_component, metrics.computed_at) AS leverage_test_component,
            argMax(leverage_incident_component, metrics.computed_at) AS leverage_incident_component,
            max(metrics.computed_at) AS computed_at
        FROM ai_impact_metrics_daily AS metrics
        WHERE {where_clause}
        GROUP BY org_id, team_id, repo_id, work_type, day, attribution_bucket
        ORDER BY day, repo_id, work_type, attribution_bucket
        """
        raw_rows = await query_dicts(self.client, query, params)
        return [_to_record(raw) for raw in raw_rows]

    async def load_reviewer_concentration(
        self,
        *,
        start_day: date,
        end_day: date,
        repo_id: uuid.UUID | None = None,
        team_id: str | None = None,
    ) -> tuple[float | None, int]:
        """Load aggregate-only reviewer concentration for AI Review Load.

        The query intentionally returns only the distribution summary inputs.
        Reviewer identities are used inside the aggregation boundary and are not
        returned from this loader or exposed through GraphQL.
        """

        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
        # All filters are qualified with `umd.` to avoid "Ambiguous column" errors
        # when the INNER JOIN subquery (ai_repos) also exposes a `repo_id` column.
        filters = ["umd.day >= {start_day:Date}", "umd.day <= {end_day:Date}"]
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            filters.append("umd.repo_id = {repo_id:UUID}")
        if team_id is not None:
            params["team_id"] = team_id
            filters.append("umd.team_id = {team_id:String}")
        params = self._scope.inject(params)
        org_expr = self._scope.expression(alias="umd")
        if org_expr:
            filters.append(org_expr)
        where_clause = " AND ".join(filters)
        # Narrow reviewer universe to repos that have AI-attributed PRs in the
        # window.  This is a partial scoping: reviewers of human PRs in the same
        # repo are included, but reviewers from purely-human repos are excluded.
        # A full per-PR-reviewer join requires a separate PR-review events table
        # and is deferred to a future wave.
        org_filter_ai_attr = self._scope.filter_uuid(alias="attr")
        query = f"""
        SELECT sum(reviews_given) AS reviews_given
        FROM (
            SELECT
                umd.repo_id,
                umd.author_email,
                umd.day,
                argMax(umd.reviews_given, umd.computed_at) AS reviews_given
            FROM user_metrics_daily AS umd
            INNER JOIN (
                SELECT DISTINCT attr.repo_id AS repo_id
                FROM ai_attribution_resolved AS attr
                WHERE attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
                  AND toDate(attr.observed_at) >= {{start_day:Date}}
                  AND toDate(attr.observed_at) <= {{end_day:Date}}
                  {org_filter_ai_attr}
            ) AS ai_repos ON ai_repos.repo_id = umd.repo_id
            WHERE {where_clause}
            GROUP BY umd.repo_id, umd.author_email, umd.day
        )
        GROUP BY author_email
        """
        rows = await query_dicts(self.client, query, params)
        review_loads = [float(row.get("reviews_given") or 0.0) for row in rows]
        if not review_loads:
            return None, 0
        return _gini(review_loads), len(review_loads)

    def _pr_attribution_subquery(self) -> str:
        """Deduped (repo_id, number, kind) attribution map subquery.

        Mirrors the two linkage paths used by ``load_ai_pr_attributions``:
        via ``work_graph_issue_pr`` and via direct ``subject_id`` matching
        (``"<number>"`` or ``"<repo_id>#<number>"``). ``any(kind)`` collapses
        ReplacingMergeTree duplicates and dual-path matches. Both sides are
        pinned to the same requested org via the WHERE parameters.

        Join identity: ``work_item_id`` is NOT repo-global — an id reused by
        another repo in the same org must not cross-link attribution onto
        unrelated PRs — so the linkage additionally requires
        ``attr.repo_id = link.repo_id`` whenever the attribution carries a
        repo. ``attr.repo_id`` is Nullable: a repo-less record is a
        work-item-level attribution whose stated meaning is "this work
        item's PR work is AI-attributed", and the work-graph link is its
        resolution mechanism — it binds to every PR linked to that work item
        (including multi-repo work items). Only repo-pinned records are
        constrained to their own repo's links.
        """
        org_filter_attr = self._scope.filter_uuid(alias="attr")
        org_filter_link = self._scope.filter(alias="link")
        return f"""
            SELECT repo_id, number, any(kind) AS kind
            FROM (
                SELECT
                    link.repo_id AS repo_id,
                    link.pr_number AS number,
                    attr.kind AS kind
                FROM work_graph_issue_pr AS link
                INNER JOIN ai_attribution_resolved AS attr
                    ON attr.subject_type = 'pull_request'
                    AND attr.subject_id = link.work_item_id
                WHERE (attr.repo_id IS NULL OR attr.repo_id = link.repo_id)
                  {org_filter_link} {org_filter_attr}
                UNION ALL
                SELECT
                    attr.repo_id AS repo_id,
                    toUInt32OrZero(
                        arrayElement(splitByChar('#', attr.subject_id), -1)
                    ) AS number,
                    attr.kind AS kind
                FROM ai_attribution_resolved AS attr
                WHERE attr.subject_type = 'pull_request'
                  AND number > 0
                  {org_filter_attr}
            )
            GROUP BY repo_id, number
        """

    @staticmethod
    def _pr_window_filter() -> str:
        return """((pr.created_at >= {start:DateTime} AND pr.created_at < {end:DateTime})
                OR (pr.merged_at IS NOT NULL AND pr.merged_at >= {start:DateTime} AND pr.merged_at < {end:DateTime}))"""

    # Event-time expression shared with compute_ai_impact_metrics_daily
    # (metrics/ai_impact.py: ``event_at = merged_at or created_at`` — a PR
    # belongs to its merge day when merged, else its creation day). Both
    # window membership AND the day key must use this expression so
    # query-time slices line up with ``ai_impact_metrics_daily`` rows.
    _PR_EVENT_EXPR = "if(pr.merged_at IS NOT NULL, pr.merged_at, pr.created_at)"

    @classmethod
    def _pr_event_window_filter(cls) -> str:
        return (
            f"({cls._PR_EVENT_EXPR} >= {{start:DateTime}} "
            f"AND {cls._PR_EVENT_EXPR} < {{end:DateTime}})"
        )

    def _engagement_scope_filters(
        self,
        params: dict[str, Any],
        repo_id: uuid.UUID | None,
        repo_ids: list[uuid.UUID] | None,
    ) -> str:
        scope_filter = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            scope_filter += "\n              AND pr.repo_id = {repo_id:UUID}"
        if repo_ids is not None:
            params["repo_ids"] = [str(r) for r in repo_ids]
            scope_filter += (
                "\n              AND toString(pr.repo_id) IN {repo_ids:Array(String)}"
            )
        return scope_filter

    def _deduped_prs_subquery(
        self,
        params: dict[str, Any],
        repo_id: uuid.UUID | None,
        repo_ids: list[uuid.UUID] | None,
    ) -> str:
        """Latest-version PR rows, collapsed by (repo_id, number).

        ``git_pull_requests`` is a ReplacingMergeTree keyed by
        ``(repo_id, number)`` with version column ``last_synced``; sync
        re-runs leave multiple row versions visible until a background merge.
        Aggregating over the raw table double-counts those versions, so every
        aggregate read must collapse to ``argMax(..., last_synced)`` first
        (app-code readers like ``load_ai_pr_attributions`` dedupe in Python).
        Org and repo scope filters are applied inside, before the GROUP BY.

        The candidate stage prunes to keys with ANY row version inside the
        event window so argMax does not aggregate the org's entire PR
        history. Filtering raw versions directly would be wrong:
        ``merged_at`` changes between versions (NULL while open, set after
        merge), so a key's latest version can be in-window while stale
        versions are not — any-version-qualifies is the safe prefilter, and
        callers re-apply the window on the deduped values (stage 3).
        """
        scope_filter = self._engagement_scope_filters(params, repo_id, repo_ids)
        org_filter_pr = self._scope.filter(alias="pr")
        return f"""
            SELECT
                pr.repo_id AS repo_id,
                pr.number AS number,
                argMax(pr.created_at, pr.last_synced) AS created_at,
                argMax(pr.merged_at, pr.last_synced) AS merged_at,
                argMax(pr.first_review_at, pr.last_synced) AS first_review_at,
                argMax(pr.comments_count, pr.last_synced) AS comments_count,
                argMax(pr.additions, pr.last_synced) AS additions,
                argMax(pr.deletions, pr.last_synced) AS deletions
            FROM git_pull_requests AS pr
            WHERE 1 = 1
              {scope_filter}
              {org_filter_pr}
              AND (pr.repo_id, pr.number) IN (
                  SELECT pr.repo_id, pr.number
                  FROM git_pull_requests AS pr
                  WHERE {self._pr_event_window_filter()}
                    {scope_filter}
                    {org_filter_pr}
              )
            GROUP BY pr.repo_id, pr.number
        """

    async def load_review_engagement(
        self,
        *,
        start: datetime,
        end: datetime,
        repo_id: uuid.UUID | None = None,
        repo_ids: list[uuid.UUID] | None = None,
    ) -> list[dict[str, Any]]:
        """Per-(bucket, day) review engagement computed from raw PR rows.

        Returns dicts with ``bucket``, ``day``, ``prs_with_first_review``,
        ``pickup_latency_hours`` (avg open→first-review, hours),
        ``review_comments_total`` and ``loc_total`` so the resolver can derive
        ``review_comments_per_loc`` per slice and per bucket without losing
        the None-vs-zero distinction (CHAOS-2194).

        Bucket assignment mirrors compute-time ``_safe_bucket``: matched AI
        kinds keep their kind, ``human`` stays human, anything else —
        including unattributed PRs — is ``unknown``.

        Window membership and the ``day`` key both use the event-time
        expression (merge day when merged, else creation day) so rows merge
        with ``ai_impact_metrics_daily`` cells — a PR created before the
        window but merged inside it lands on its merge day, exactly like the
        compute path.
        """
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
        }
        deduped_prs = self._deduped_prs_subquery(params, repo_id, repo_ids)
        params = self._scope.inject(params)
        query = f"""
        SELECT
            multiIf(
                attr_map.kind IN ('ai_assisted', 'agent_created', 'ai_review'),
                attr_map.kind,
                attr_map.kind = 'human', 'human',
                'unknown'
            ) AS bucket,
            toDate({self._PR_EVENT_EXPR}) AS day,
            countIf(
                pr.first_review_at IS NOT NULL
                AND pr.first_review_at >= pr.created_at
            ) AS prs_with_first_review,
            avgIf(
                dateDiff('second', pr.created_at, pr.first_review_at) / 3600.0,
                pr.first_review_at IS NOT NULL
                AND pr.first_review_at >= pr.created_at
            ) AS pickup_latency_hours,
            sum(pr.comments_count) AS review_comments_total,
            sum(
                coalesce(pr.additions, 0) + coalesce(pr.deletions, 0)
            ) AS loc_total
        FROM (
            {deduped_prs}
        ) AS pr
        LEFT JOIN (
            {self._pr_attribution_subquery()}
        ) AS attr_map
            ON attr_map.repo_id = pr.repo_id AND attr_map.number = pr.number
        WHERE {self._pr_event_window_filter()}
        GROUP BY bucket, day
        ORDER BY day, bucket
        """
        return await query_dicts(self.client, query, params)

    def _ai_pr_files_cte(
        self,
        params: dict[str, Any],
        repo_id: uuid.UUID | None,
        repo_ids: list[uuid.UUID] | None,
    ) -> str:
        """CTE body mapping AI-attributed PRs to their changed file paths.

        Only PRs whose commits are linked through ``work_graph_pr_commit``
        appear — that is the assessable universe for overlap rates. The PR
        universe uses event-time window semantics (merge day when merged) to
        match ``ai_impact_metrics_daily``. Every joined table is org-scoped:
        ``work_graph_pr_commit`` and ``git_commit_stats`` both carry
        ``org_id``, and repo_id/commit_hash values can collide across tenants.
        """
        deduped_prs = self._deduped_prs_subquery(params, repo_id, repo_ids)
        org_filter_pc = self._scope.filter(alias="pc")
        org_filter_cs = self._scope.filter(alias="cs")
        return f"""
            SELECT DISTINCT
                ai.repo_id AS repo_id,
                ai.number AS number,
                ai.kind AS bucket,
                cs.file_path AS file_path
            FROM (
                SELECT pr.repo_id AS repo_id, pr.number AS number, attr_map.kind AS kind
                FROM (
                    {deduped_prs}
                ) AS pr
                INNER JOIN (
                    {self._pr_attribution_subquery()}
                ) AS attr_map
                    ON attr_map.repo_id = pr.repo_id AND attr_map.number = pr.number
                WHERE attr_map.kind IN ('ai_assisted', 'agent_created', 'ai_review')
                  AND {self._pr_event_window_filter()}
            ) AS ai
            INNER JOIN work_graph_pr_commit AS pc
                ON pc.repo_id = ai.repo_id AND pc.pr_number = ai.number
                {org_filter_pc}
            INNER JOIN git_commit_stats AS cs
                ON cs.repo_id = pc.repo_id AND cs.commit_hash = pc.commit_hash
                {org_filter_cs}
        """

    async def load_hotspot_overlap(
        self,
        *,
        start: datetime,
        end: datetime,
        start_day: date,
        end_day: date,
        repo_id: uuid.UUID | None = None,
        repo_ids: list[uuid.UUID] | None = None,
    ) -> list[dict[str, Any]]:
        """Per-bucket overlap of AI-attributed PRs with top-decile hotspot files.

        "Hotspot" here means the top decile of latest ``risk_score`` per repo
        within the window (minimum one file per repo when the decile is
        degenerate), restricted to ``risk_score > 0``. A bare ``> 0`` cut is
        NOT discriminating — risk_score is a sum of z-scores, so above-zero
        means merely above-average and saturates the rate at ~1.0. The
        reported ``hotspot_overlap_rate`` therefore reads as "share of
        assessable AI PRs touching top-decile-risk files" (CHAOS-2185).
        """
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
            "start_day": start_day,
            "end_day": end_day,
        }
        files_cte = self._ai_pr_files_cte(params, repo_id, repo_ids)
        params = self._scope.inject(params)
        org_filter_hs = self._scope.filter(alias="hs")
        query = f"""
        WITH pr_files AS (
            {files_cte}
        ),
        hotspots AS (
            SELECT repo_id, file_path, risk_score
            FROM (
                SELECT
                    repo_id,
                    file_path,
                    risk_score,
                    row_number() OVER (
                        PARTITION BY repo_id
                        ORDER BY risk_score DESC, file_path
                    ) AS risk_rank,
                    count() OVER (PARTITION BY repo_id) AS repo_file_count
                FROM (
                    SELECT
                        hs.repo_id AS repo_id,
                        hs.file_path AS file_path,
                        argMax(hs.risk_score, hs.computed_at) AS risk_score
                    FROM file_hotspot_daily AS hs
                    WHERE hs.day >= {{start_day:Date}}
                      AND hs.day <= {{end_day:Date}}
                      {org_filter_hs}
                    GROUP BY hs.repo_id, hs.file_path
                    HAVING risk_score > 0
                )
            )
            WHERE risk_rank <= greatest(1, toUInt64(ceil(repo_file_count * 0.1)))
        )
        SELECT
            bucket,
            uniqExact((pf.repo_id, pf.number)) AS prs_total,
            uniqExactIf(
                (pf.repo_id, pf.number), h.file_path != ''
            ) AS prs_touching_hotspots,
            avgIf(h.risk_score, h.file_path != '') AS avg_hotspot_risk_score
        FROM pr_files AS pf
        LEFT JOIN hotspots AS h
            ON h.repo_id = pf.repo_id AND h.file_path = pf.file_path
        GROUP BY bucket
        ORDER BY bucket
        """
        return await query_dicts(self.client, query, params)

    async def load_complexity_overlap(
        self,
        *,
        start: datetime,
        end: datetime,
        end_day: date,
        repo_id: uuid.UUID | None = None,
        repo_ids: list[uuid.UUID] | None = None,
    ) -> list[dict[str, Any]]:
        """Per-bucket overlap of AI-attributed PRs with high-complexity files.

        High-complexity = latest snapshot (as of ``end_day``) reporting at
        least one high- or very-high-complexity function (CHAOS-2185).
        """
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {
            "start": start.replace(tzinfo=None),
            "end": end.replace(tzinfo=None),
            "end_day": end_day,
        }
        files_cte = self._ai_pr_files_cte(params, repo_id, repo_ids)
        params = self._scope.inject(params)
        org_filter_fc = self._scope.filter(alias="fc")
        query = f"""
        WITH pr_files AS (
            {files_cte}
        ),
        complex_files AS (
            SELECT
                fc.repo_id AS repo_id,
                fc.file_path AS file_path
            FROM file_complexity_snapshots AS fc
            WHERE fc.as_of_day <= {{end_day:Date}}
              {org_filter_fc}
            GROUP BY fc.repo_id, fc.file_path
            HAVING argMax(
                fc.high_complexity_functions + fc.very_high_complexity_functions,
                fc.computed_at
            ) > 0
        )
        SELECT
            bucket,
            uniqExact((pf.repo_id, pf.number)) AS prs_total,
            uniqExactIf(
                (pf.repo_id, pf.number), cf.file_path != ''
            ) AS prs_touching_high_complexity
        FROM pr_files AS pf
        LEFT JOIN complex_files AS cf
            ON cf.repo_id = pf.repo_id AND cf.file_path = pf.file_path
        GROUP BY bucket
        ORDER BY bucket
        """
        return await query_dicts(self.client, query, params)

    async def load_repo_labels(self, repo_ids: list[str]) -> dict[str, str]:
        """Map repo UUID strings to repo full-names (best effort)."""
        from dev_health_ops.api.queries.client import query_dicts

        if not repo_ids:
            return {}
        params: dict[str, Any] = {"repo_ids": repo_ids}
        params = self._scope.inject(params)
        org_expr = self._scope.expression()
        org_filter = f"AND {org_expr}" if org_expr else ""
        query = f"""
        SELECT toString(id) AS repo_id, repo AS full_name
        FROM repos
        WHERE toString(id) IN {{repo_ids:Array(String)}}
          {org_filter}
        """
        rows = await query_dicts(self.client, query, params)
        return {
            str(r["repo_id"]): str(r.get("full_name") or r["repo_id"]) for r in rows
        }

    async def load_team_labels(self, team_ids: list[str]) -> dict[str, str]:
        """Map team ids to team display names (best effort)."""
        from dev_health_ops.api.queries.client import query_dicts

        if not team_ids:
            return {}
        params: dict[str, Any] = {"team_ids": team_ids}
        params = self._scope.inject(params)
        org_expr = self._scope.expression()
        org_filter = f"AND {org_expr}" if org_expr else ""
        query = f"""
        SELECT toString(id) AS team_id, name
        FROM teams
        WHERE toString(id) IN {{team_ids:Array(String)}}
          {org_filter}
        """
        rows = await query_dicts(self.client, query, params)
        return {str(r["team_id"]): str(r.get("name") or r["team_id"]) for r in rows}


def _to_record(raw: dict[str, Any]) -> AIImpactMetricsDailyRecord:
    repo_id = parse_uuid(raw.get("repo_id"))
    if repo_id is None:
        raise ValueError("ai_impact_metrics_daily row has invalid repo_id")
    return AIImpactMetricsDailyRecord(
        org_id=str(raw.get("org_id") or ""),
        team_id=raw.get("team_id") or None,
        repo_id=repo_id,
        work_type=str(raw.get("work_type") or "pull_request"),
        day=raw["day"],
        attribution_bucket=str(raw.get("attribution_bucket") or "unknown"),
        prs_total=int(raw.get("prs_total") or 0),
        prs_merged=int(raw.get("prs_merged") or 0),
        ai_assisted_prs=int(raw.get("ai_assisted_prs") or 0),
        agent_created_prs=int(raw.get("agent_created_prs") or 0),
        human_prs=int(raw.get("human_prs") or 0),
        unknown_prs=int(raw.get("unknown_prs") or 0),
        ai_assisted_pr_ratio=raw.get("ai_assisted_pr_ratio"),
        agent_created_pr_count=int(raw.get("agent_created_pr_count") or 0),
        cycle_time_avg_hours=raw.get("cycle_time_avg_hours"),
        baseline_cycle_time_avg_hours=raw.get("baseline_cycle_time_avg_hours"),
        ai_cycle_time_delta_hours=raw.get("ai_cycle_time_delta_hours"),
        reviews_per_pr=raw.get("reviews_per_pr"),
        baseline_reviews_per_pr=raw.get("baseline_reviews_per_pr"),
        ai_review_amplification=raw.get("ai_review_amplification"),
        changes_requested_per_pr=raw.get("changes_requested_per_pr"),
        rework_prs=int(raw.get("rework_prs") or 0),
        rework_drag_rate=raw.get("rework_drag_rate"),
        followup_commits_count=int(raw.get("followup_commits_count") or 0),
        revert_prs=int(raw.get("revert_prs") or 0),
        revert_rate=raw.get("revert_rate"),
        incidents_count=int(raw.get("incidents_count") or 0),
        incident_drag_rate=raw.get("incident_drag_rate"),
        test_gap_prs=int(raw.get("test_gap_prs") or 0),
        test_gap_rate=raw.get("test_gap_rate"),
        leverage=AIOperatingLeverageComponents(
            prs_component=float(raw.get("leverage_prs_component") or 0.0),
            cycle_time_component=raw.get("leverage_cycle_time_component"),
            review_component=raw.get("leverage_review_component"),
            rework_component=raw.get("leverage_rework_component"),
            test_component=raw.get("leverage_test_component"),
            incident_component=raw.get("leverage_incident_component"),
        ),
        computed_at=raw["computed_at"],
    )


def _gini(values: list[float]) -> float:
    if not values:
        return 0.0
    total = sum(values)
    if total == 0.0:
        return 0.0
    sorted_values = sorted(values)
    count = len(sorted_values)
    weighted_sum = sum((index + 1) * value for index, value in enumerate(sorted_values))
    return (2.0 * weighted_sum) / (count * total) - (count + 1.0) / count
