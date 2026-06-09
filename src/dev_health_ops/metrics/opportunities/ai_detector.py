from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from dev_health_ops.api.graphql.models.ai import (
    AIOpportunity,
    AIOpportunityKind,
    AIWorkGraphDrilldownRef,
)
from dev_health_ops.metrics.ai_impact import AI_BUCKETS, AttributionBucket
from dev_health_ops.metrics.loaders.base import parse_uuid
from dev_health_ops.metrics.opportunities.scoring import (
    clamp,
    score_delta,
    score_ratio,
    stable_opportunity_id,
)
from dev_health_ops.metrics.query_builder import OrgScopedQuery

_MIN_PRS = 10
_REPETITIVE_CLUSTER_MIN_PRS = 5
_MAX_LIMIT = 100

# CHAOS-2189 thresholds for the documented workflow-type rules.
_TEST_GEN_GAP_THRESHOLD = 0.50
_DEP_UPDATE_MIN_PRS = 5
_MIGRATION_MIN_PRS = 5
_DOC_DRIFT_MIN_CODE_COMMITS = 20
_FLAKY_MIN_CASES = 50
_FLAKY_RATE_THRESHOLD = 0.05

_DOC_FILE_EXPR = (
    "(file_path LIKE '%.md' OR file_path LIKE '%.rst' OR file_path LIKE '%.adoc'"
    " OR file_path LIKE 'docs/%' OR file_path LIKE '%/docs/%')"
)


@dataclass(frozen=True)
class _Scope:
    repo_id: uuid.UUID | None
    team_id: str | None


@dataclass
class _BucketAgg:
    prs_total: int = 0
    reviews_weighted: float = 0.0
    reviews_weight: int = 0
    cycle_weighted: float = 0.0
    cycle_weight: int = 0
    rework_prs: int = 0
    test_gap_prs: int = 0

    def add(self, row: dict[str, Any]) -> None:
        prs = int(row.get("prs_total") or 0)
        self.prs_total += prs
        reviews = _float_or_none(row.get("reviews_per_pr"))
        if reviews is not None and prs > 0:
            self.reviews_weighted += reviews * prs
            self.reviews_weight += prs
        cycle = _float_or_none(row.get("cycle_time_avg_hours"))
        if cycle is not None and prs > 0:
            self.cycle_weighted += cycle * prs
            self.cycle_weight += prs
        self.rework_prs += int(row.get("rework_prs") or 0)
        self.test_gap_prs += int(row.get("test_gap_prs") or 0)

    @property
    def reviews_per_pr(self) -> float | None:
        return _ratio(self.reviews_weighted, self.reviews_weight)

    @property
    def cycle_hours(self) -> float | None:
        return _ratio(self.cycle_weighted, self.cycle_weight)

    @property
    def rework_rate(self) -> float | None:
        return _ratio(self.rework_prs, self.prs_total)

    @property
    def test_gap_rate(self) -> float | None:
        return _ratio(self.test_gap_prs, self.prs_total)


@dataclass
class _RepoAgg:
    repo_id: str
    team_id: str | None
    ai: _BucketAgg
    human: _BucketAgg


class AIOpportunityDetector:
    """Rule-based AI automation opportunity detector.

    This class is read-only. It consumes ClickHouse rows already produced by the
    AI impact and attribution jobs and returns GraphQL opportunity objects.
    """

    def __init__(self, client: Any) -> None:
        self.client = client

    async def detect(
        self,
        org_id: str,
        scope: Any | None = None,
        limit: int = 25,
    ) -> list[AIOpportunity]:
        bounded_limit = _clamp_limit(limit)
        normalized_scope = _scope_from_input(scope)
        impact_rows = await self._load_impact_rows(org_id, normalized_scope)
        opportunities = self._detect_metric_opportunities(impact_rows)
        opportunities.extend(
            await self._detect_repetitive_changes(org_id, normalized_scope)
        )
        # CHAOS-2189: documented workflow-type rules (manual toil + quality
        # signals where AI/agents should be applied next).
        opportunities.extend(
            await self._detect_title_pattern_toil(org_id, normalized_scope)
        )
        opportunities.extend(await self._detect_doc_drift(org_id, normalized_scope))
        opportunities.extend(
            await self._detect_flaky_test_triage(org_id, normalized_scope)
        )
        opportunities.sort(key=lambda item: item.score, reverse=True)
        return opportunities[:bounded_limit]

    async def _load_impact_rows(
        self, org_id: str, scope: _Scope
    ) -> list[dict[str, Any]]:
        from dev_health_ops.api.queries.client import query_dicts

        params: dict[str, Any] = {}
        filters = ["day >= today() - 30"]
        if scope.repo_id is not None:
            params["repo_id"] = str(scope.repo_id)
            filters.append("repo_id = {repo_id:UUID}")
        if scope.team_id is not None:
            params["team_id"] = scope.team_id
            filters.append("team_id = {team_id:String}")
        org_scope = OrgScopedQuery(org_id)
        params = org_scope.inject(params)
        org_expr = org_scope.expression()
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
            argMax(reviews_per_pr, metrics.computed_at) AS reviews_per_pr,
            argMax(cycle_time_avg_hours, metrics.computed_at) AS cycle_time_avg_hours,
            argMax(rework_prs, metrics.computed_at) AS rework_prs,
            argMax(test_gap_prs, metrics.computed_at) AS test_gap_prs,
            max(metrics.computed_at) AS computed_at
        FROM ai_impact_metrics_daily AS metrics
        WHERE {where_clause}
        GROUP BY org_id, team_id, repo_id, work_type, day, attribution_bucket
        """
        return await query_dicts(self.client, query, params)

    def _detect_metric_opportunities(
        self, rows: list[dict[str, Any]]
    ) -> list[AIOpportunity]:
        grouped: dict[tuple[str, str | None], _RepoAgg] = {}
        ai_bucket_values = {bucket.value for bucket in AI_BUCKETS}

        for row in rows:
            repo = parse_uuid(row.get("repo_id"))
            if repo is None:
                continue
            repo_id = str(repo)
            team_id = row.get("team_id") or None
            key = (repo_id, team_id)
            if key not in grouped:
                grouped[key] = _RepoAgg(
                    repo_id=repo_id,
                    team_id=team_id,
                    ai=_BucketAgg(),
                    human=_BucketAgg(),
                )
            bucket = str(row.get("attribution_bucket") or "")
            if bucket in ai_bucket_values:
                grouped[key].ai.add(row)
            elif bucket == AttributionBucket.HUMAN.value:
                grouped[key].human.add(row)

        opportunities: list[AIOpportunity] = []
        for agg in grouped.values():
            opportunities.extend(self._metric_rules_for_repo(agg))
        return opportunities

    def _metric_rules_for_repo(self, agg: _RepoAgg) -> list[AIOpportunity]:
        opportunities: list[AIOpportunity] = []

        # TEST_GENERATION (CHAOS-2189) gates on the HUMAN bucket: a high
        # human test-gap rate is the signal that AI test generation is the
        # next workflow to apply, independent of current AI adoption.
        human_test_gap = agg.human.test_gap_rate
        if (
            agg.human.prs_total >= _MIN_PRS
            and human_test_gap is not None
            and human_test_gap >= _TEST_GEN_GAP_THRESHOLD
        ):
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.TEST_GENERATION,
                    repo_id=agg.repo_id,
                    team_id=agg.team_id,
                    title=f"Test generation candidate in {agg.repo_id}",
                    rationale=(
                        "Human-authored PRs had a "
                        f"{human_test_gap:.0%} test gap rate across "
                        f"{agg.human.prs_total} PRs over the last 30 days."
                    ),
                    score=_score_delta(human_test_gap, _TEST_GEN_GAP_THRESHOLD),
                    evidence_refs=[
                        f"ai_impact_metrics_daily:test_gap_rate:{agg.repo_id}"
                    ],
                )
            )

        if agg.ai.prs_total < _MIN_PRS:
            return opportunities

        ai_reviews = agg.ai.reviews_per_pr
        human_reviews = agg.human.reviews_per_pr
        if (
            ai_reviews is not None
            and human_reviews is not None
            and human_reviews > 0
            and ai_reviews >= human_reviews * 1.5
        ):
            ratio = ai_reviews / human_reviews
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.HIGH_REVIEW_LOAD,
                    repo_id=agg.repo_id,
                    team_id=agg.team_id,
                    title=f"High AI review load in {agg.repo_id}",
                    rationale=(
                        "AI-assisted PRs averaged "
                        f"{ai_reviews:.1f} reviews vs {human_reviews:.1f} for "
                        "human PRs over the last 30 days."
                    ),
                    score=_score_ratio(ratio, 1.5),
                    evidence_refs=[
                        f"ai_impact_metrics_daily:reviews_per_pr:{agg.repo_id}"
                    ],
                )
            )

        ai_rework = agg.ai.rework_rate
        human_rework = agg.human.rework_rate
        if (
            ai_rework is not None
            and human_rework is not None
            and ai_rework >= 0.25
            and ai_rework - human_rework >= 0.10
        ):
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.HIGH_REWORK,
                    repo_id=agg.repo_id,
                    team_id=agg.team_id,
                    title=f"High AI rework in {agg.repo_id}",
                    rationale=(
                        "AI-assisted PRs had a "
                        f"{ai_rework:.0%} rework rate vs {human_rework:.0%} for "
                        "human PRs over the last 30 days."
                    ),
                    score=_score_delta(ai_rework - human_rework, 0.10),
                    evidence_refs=[
                        f"ai_impact_metrics_daily:rework_rate:{agg.repo_id}"
                    ],
                )
            )

        ai_cycle = agg.ai.cycle_hours
        human_cycle = agg.human.cycle_hours
        if (
            ai_cycle is not None
            and human_cycle is not None
            and human_cycle > 0
            and ai_cycle >= human_cycle * 1.25
        ):
            ratio = ai_cycle / human_cycle
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.SLOW_CYCLE,
                    repo_id=agg.repo_id,
                    team_id=agg.team_id,
                    title=f"Slow AI cycle time in {agg.repo_id}",
                    rationale=(
                        "AI-assisted PRs averaged "
                        f"{ai_cycle:.1f} cycle hours vs {human_cycle:.1f} for "
                        "human PRs over the last 30 days."
                    ),
                    score=_score_ratio(ratio, 1.25),
                    evidence_refs=[
                        f"ai_impact_metrics_daily:cycle_time_avg_hours:{agg.repo_id}"
                    ],
                )
            )

        ai_test_gap = agg.ai.test_gap_rate
        if ai_test_gap is not None and ai_test_gap >= 0.50:
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.UNCOVERED_TEST_AREA,
                    repo_id=agg.repo_id,
                    team_id=agg.team_id,
                    title=f"Uncovered AI test area in {agg.repo_id}",
                    rationale=(
                        f"AI-assisted PRs had a {ai_test_gap:.0%} test gap rate "
                        "over the last 30 days."
                    ),
                    score=_score_delta(ai_test_gap, 0.50),
                    evidence_refs=[
                        f"ai_impact_metrics_daily:test_gap_rate:{agg.repo_id}"
                    ],
                )
            )

        return opportunities

    async def _detect_repetitive_changes(
        self, org_id: str, scope: _Scope
    ) -> list[AIOpportunity]:
        from dev_health_ops.api.queries.client import query_dicts

        if scope.team_id is not None:
            return []

        params: dict[str, Any] = {"cluster_min": _REPETITIVE_CLUSTER_MIN_PRS}
        filters = ["pr.created_at >= now() - INTERVAL 30 DAY"]
        if scope.repo_id is not None:
            params["repo_id"] = str(scope.repo_id)
            filters.append("pr.repo_id = {repo_id:UUID}")
        org_scope = OrgScopedQuery(org_id)
        params = org_scope.inject(params)
        org_expr = org_scope.expression(alias="attr")
        if org_expr:
            filters.append(org_expr)
        where_clause = " AND ".join(filters)
        query = f"""
        SELECT
            toString(pr.repo_id) AS repo_id,
            CAST('', 'String') AS team_id,
            coalesce(pr.author_email, pr.author_name, '') AS author,
            coalesce(nullIf(wi.type, ''), 'pull_request') AS work_type,
            lower(arrayStringConcat(arraySlice(splitByChar(' ', coalesce(pr.title, '')), 1, 3), ' ')) AS title_prefix,
            count() AS prs_total,
            groupArray(concat('git_pull_requests:', toString(pr.repo_id), ':', toString(pr.number))) AS pr_refs
        FROM git_pull_requests AS pr
        LEFT JOIN work_graph_issue_pr AS link
            ON link.repo_id = pr.repo_id AND link.pr_number = pr.number
        INNER JOIN ai_attribution_resolved AS attr
            ON attr.repo_id = pr.repo_id
            AND attr.subject_type = 'pull_request'
            AND (attr.subject_id = toString(pr.number) OR attr.subject_id = link.work_item_id)
        LEFT JOIN work_items AS wi
            ON wi.repo_id = link.repo_id AND wi.work_item_id = link.work_item_id
        WHERE {where_clause}
          AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
          AND coalesce(pr.title, '') != ''
        GROUP BY repo_id, team_id, author, work_type, title_prefix
        HAVING prs_total >= {{cluster_min:UInt32}}
        ORDER BY prs_total DESC
        LIMIT 100
        """
        rows = await query_dicts(self.client, query, params)
        opportunities: list[AIOpportunity] = []
        for row in rows:
            repo = parse_uuid(row.get("repo_id"))
            if repo is None:
                continue
            repo_id = str(repo)
            team_id = row.get("team_id") or None
            prs_total = int(row.get("prs_total") or 0)
            title_prefix = str(row.get("title_prefix") or "similar")
            evidence_refs = [str(ref) for ref in (row.get("pr_refs") or [])][:5]
            if not evidence_refs:
                continue
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.REPETITIVE_CHANGE,
                    repo_id=repo_id,
                    team_id=team_id,
                    title=f"Repetitive AI change pattern in {repo_id}",
                    rationale=(
                        f"{prs_total} AI-assisted PRs shared the title prefix "
                        f"'{title_prefix}' with the same author/work type over "
                        "the last 30 days."
                    ),
                    score=_score_delta(prs_total / _REPETITIVE_CLUSTER_MIN_PRS, 1.0),
                    evidence_refs=evidence_refs,
                )
            )
        return opportunities

    async def _detect_title_pattern_toil(
        self, org_id: str, scope: _Scope
    ) -> list[AIOpportunity]:
        """DEPENDENCY_UPDATES + MECHANICAL_MIGRATIONS (CHAOS-2189).

        Both rules look for clusters of human-authored PRs whose titles match
        a documented manual-toil pattern. Bot authors are excluded because
        their work is already automated; AI-attributed PRs are excluded via
        anti-join for the same reason.
        """
        from dev_health_ops.api.queries.client import query_dicts

        if scope.team_id is not None:
            # git_pull_requests carries no team scoping here; mirror the
            # repetitive-change rule and stay silent rather than guess.
            return []

        opportunities: list[AIOpportunity] = []
        for rule in _TITLE_PATTERN_RULES:
            params: dict[str, Any] = {"min_prs": rule["min_prs"]}
            filters = [
                "pr.created_at >= now() - INTERVAL 30 DAY",
                str(rule["title_expr"]),
                "lower(coalesce(pr.author_name, '')) NOT LIKE '%bot%'",
                "lower(coalesce(pr.author_name, '')) NOT LIKE '%renovate%'",
            ]
            if scope.repo_id is not None:
                params["repo_id"] = str(scope.repo_id)
                filters.append("pr.repo_id = {repo_id:UUID}")
            org_scope = OrgScopedQuery(org_id)
            params = org_scope.inject(params)
            org_expr = org_scope.expression(alias="pr")
            if org_expr:
                filters.append(org_expr)
            where_clause = " AND ".join(filters)
            query = f"""
            SELECT
                toString(pr.repo_id) AS repo_id,
                count() AS prs_total,
                groupArray(concat('git_pull_requests:', toString(pr.repo_id), ':', toString(pr.number))) AS pr_refs
            FROM git_pull_requests AS pr
            LEFT ANTI JOIN ai_attribution_resolved AS attr
                ON attr.repo_id = pr.repo_id
                AND attr.subject_type = 'pull_request'
                AND attr.subject_id = toString(pr.number)
                AND attr.kind IN ('ai_assisted', 'agent_created')
            WHERE {where_clause}
            GROUP BY repo_id
            HAVING prs_total >= {{min_prs:UInt32}}
            ORDER BY prs_total DESC
            LIMIT 100
            """
            rows = await query_dicts(self.client, query, params)
            for row in rows:
                repo = parse_uuid(row.get("repo_id"))
                if repo is None:
                    continue
                repo_id = str(repo)
                prs_total = int(row.get("prs_total") or 0)
                evidence_refs = [str(ref) for ref in (row.get("pr_refs") or [])][:5]
                if not evidence_refs:
                    continue
                evidence_refs.append(f"{rule['metric_ref']}:{repo_id}")
                opportunities.append(
                    _opportunity(
                        kind=rule["kind"],
                        repo_id=repo_id,
                        team_id=None,
                        title=str(rule["title"]).format(repo_id=repo_id),
                        rationale=str(rule["rationale"]).format(prs_total=prs_total),
                        score=_score_delta(prs_total / float(rule["min_prs"]), 1.0),
                        evidence_refs=evidence_refs,
                    )
                )
        return opportunities

    async def _detect_doc_drift(
        self, org_id: str, scope: _Scope
    ) -> list[AIOpportunity]:
        """DOCUMENTATION_DRIFT (CHAOS-2189).

        Flags repos with sustained code churn and zero documentation-file
        changes in the same 30-day window — docs are drifting relative to the
        code they describe.
        """
        from dev_health_ops.api.queries.client import query_dicts

        if scope.team_id is not None:
            return []

        params: dict[str, Any] = {"min_commits": _DOC_DRIFT_MIN_CODE_COMMITS}
        filters = ["c.committer_when >= now() - INTERVAL 30 DAY"]
        if scope.repo_id is not None:
            params["repo_id"] = str(scope.repo_id)
            filters.append("c.repo_id = {repo_id:UUID}")
        org_scope = OrgScopedQuery(org_id)
        params = org_scope.inject(params)
        org_expr = org_scope.expression(alias="c")
        if org_expr:
            filters.append(org_expr)
        where_clause = " AND ".join(filters)
        query = f"""
        SELECT
            toString(c.repo_id) AS repo_id,
            uniqExactIf(c.hash, NOT {_DOC_FILE_EXPR}) AS code_commits,
            countIf({_DOC_FILE_EXPR}) AS doc_changes
        FROM git_commits AS c
        INNER JOIN git_commit_stats AS s
            ON s.repo_id = c.repo_id AND s.commit_hash = c.hash
        WHERE {where_clause}
        GROUP BY repo_id
        HAVING code_commits >= {{min_commits:UInt32}} AND doc_changes = 0
        ORDER BY code_commits DESC
        LIMIT 100
        """
        rows = await query_dicts(self.client, query, params)
        opportunities: list[AIOpportunity] = []
        for row in rows:
            repo = parse_uuid(row.get("repo_id"))
            if repo is None:
                continue
            repo_id = str(repo)
            code_commits = int(row.get("code_commits") or 0)
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.DOCUMENTATION_DRIFT,
                    repo_id=repo_id,
                    team_id=None,
                    title=f"Documentation drift in {repo_id}",
                    rationale=(
                        f"{code_commits} code commits landed in the last 30 "
                        "days with zero documentation-file changes."
                    ),
                    score=_score_delta(
                        code_commits / float(_DOC_DRIFT_MIN_CODE_COMMITS), 1.0
                    ),
                    evidence_refs=[f"git_commit_stats:doc_changes:{repo_id}"],
                )
            )
        return opportunities

    async def _detect_flaky_test_triage(
        self, org_id: str, scope: _Scope
    ) -> list[AIOpportunity]:
        """FLAKY_TEST_TRIAGE (CHAOS-2189).

        Reads the persisted TestOps daily rollups: a sustained case-weighted
        flake rate marks the repo as a flaky-triage automation candidate.
        """
        from dev_health_ops.api.queries.client import query_dicts

        if scope.team_id is not None:
            return []

        params: dict[str, Any] = {
            "min_cases": _FLAKY_MIN_CASES,
            "flake_threshold": _FLAKY_RATE_THRESHOLD,
        }
        filters = ["day >= today() - 30"]
        if scope.repo_id is not None:
            params["repo_id"] = str(scope.repo_id)
            filters.append("repo_id = {repo_id:UUID}")
        org_scope = OrgScopedQuery(org_id)
        params = org_scope.inject(params)
        org_expr = org_scope.expression()
        if org_expr:
            filters.append(org_expr)
        where_clause = " AND ".join(filters)
        query = f"""
        SELECT
            toString(repo_id) AS repo_id,
            sum(total_cases) AS total_cases,
            sum(flake_rate * total_cases) / sum(total_cases) AS weighted_flake_rate
        FROM testops_test_metrics_daily
        WHERE {where_clause}
        GROUP BY repo_id
        HAVING total_cases >= {{min_cases:UInt64}}
           AND weighted_flake_rate >= {{flake_threshold:Float64}}
        ORDER BY weighted_flake_rate DESC
        LIMIT 100
        """
        rows = await query_dicts(self.client, query, params)
        opportunities: list[AIOpportunity] = []
        for row in rows:
            repo = parse_uuid(row.get("repo_id"))
            if repo is None:
                continue
            repo_id = str(repo)
            flake_rate = float(row.get("weighted_flake_rate") or 0.0)
            total_cases = int(row.get("total_cases") or 0)
            opportunities.append(
                _opportunity(
                    kind=AIOpportunityKind.FLAKY_TEST_TRIAGE,
                    repo_id=repo_id,
                    team_id=None,
                    title=f"Flaky test triage candidate in {repo_id}",
                    rationale=(
                        f"Test cases flaked at {flake_rate:.1%} across "
                        f"{total_cases} executions over the last 30 days."
                    ),
                    score=_score_delta(flake_rate, _FLAKY_RATE_THRESHOLD),
                    evidence_refs=[f"testops_test_metrics_daily:flake_rate:{repo_id}"],
                )
            )
        return opportunities


_TITLE_PATTERN_RULES: tuple[dict[str, Any], ...] = (
    {
        "kind": AIOpportunityKind.DEPENDENCY_UPDATES,
        "min_prs": _DEP_UPDATE_MIN_PRS,
        "title_expr": (
            "match(lower(coalesce(pr.title, '')),"
            " '^(bump|update|upgrade|chore\\\\(deps\\\\)|build\\\\(deps\\\\))')"
            " AND match(lower(coalesce(pr.title, '')),"
            " '(depend|deps|version|package|requirement|lockfile| from .* to )')"
        ),
        "title": "Manual dependency updates in {repo_id}",
        "rationale": (
            "{prs_total} dependency-update PRs were authored by humans in the "
            "last 30 days; dependency bumps are a documented AI automation "
            "target."
        ),
        "metric_ref": "git_pull_requests:dependency_update_prs",
    },
    {
        "kind": AIOpportunityKind.MECHANICAL_MIGRATIONS,
        "min_prs": _MIGRATION_MIN_PRS,
        "title_expr": (
            "match(lower(coalesce(pr.title, '')),"
            " '(migrat|mass rename|codemod|deprecat.* api|bulk (rename|move))')"
        ),
        "title": "Mechanical migration toil in {repo_id}",
        "rationale": (
            "{prs_total} migration-style PRs were authored by humans in the "
            "last 30 days; mechanical migrations are a documented AI "
            "automation target."
        ),
        "metric_ref": "git_pull_requests:migration_prs",
    },
)


def _scope_from_input(scope: Any | None) -> _Scope:
    repo_id = parse_uuid(getattr(scope, "repo_id", None)) if scope is not None else None
    team_id = getattr(scope, "team_id", None) if scope is not None else None
    return _Scope(repo_id=repo_id, team_id=team_id or None)


def _opportunity(
    *,
    kind: AIOpportunityKind,
    repo_id: str,
    team_id: str | None,
    title: str,
    rationale: str,
    score: float,
    evidence_refs: list[str],
) -> AIOpportunity:
    return AIOpportunity(
        opportunity_id=_stable_id(kind, repo_id, team_id),
        kind=kind,
        repo_id=repo_id,
        team_id=team_id,
        title=title,
        rationale=rationale,
        score=_clamp(score),
        evidence_refs=evidence_refs,
        work_graph_drilldowns=_work_graph_refs(evidence_refs),
    )


def _work_graph_refs(evidence_refs: list[str]) -> list[AIWorkGraphDrilldownRef]:
    refs: list[AIWorkGraphDrilldownRef] = []
    for evidence_ref in evidence_refs:
        parts = evidence_ref.split(":")
        if len(parts) != 3 or parts[0] != "git_pull_requests":
            continue
        repo_id, number = parts[1], parts[2]
        refs.append(
            AIWorkGraphDrilldownRef(
                root_type="pr",
                root_id=f"{repo_id}#{number}",
                label=f"PR {number}",
            )
        )
    return refs


def _stable_id(kind: AIOpportunityKind, repo_id: str, team_id: str | None) -> str:
    return stable_opportunity_id(kind, repo_id, team_id)


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _clamp(value: float) -> float:
    return clamp(value)


def _score_ratio(value: float, threshold: float) -> float:
    return score_ratio(value, threshold)


def _score_delta(value: float, threshold: float) -> float:
    return score_delta(value, threshold)


def _clamp_limit(limit: int | None) -> int:
    if limit is None or limit <= 0:
        return 25
    return min(limit, _MAX_LIMIT)
