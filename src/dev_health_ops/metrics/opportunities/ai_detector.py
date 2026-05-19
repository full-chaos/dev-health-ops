from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Any

from dev_health_ops.api.graphql.models.ai import AIOpportunity, AIOpportunityKind
from dev_health_ops.metrics.ai_impact import AI_BUCKETS, AttributionBucket
from dev_health_ops.metrics.loaders.base import parse_uuid
from dev_health_ops.metrics.query_builder import OrgScopedQuery

_MIN_PRS = 10
_REPETITIVE_CLUSTER_MIN_PRS = 5
_MAX_LIMIT = 100


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
            argMax(prs_total, computed_at) AS prs_total,
            argMax(reviews_per_pr, computed_at) AS reviews_per_pr,
            argMax(cycle_time_avg_hours, computed_at) AS cycle_time_avg_hours,
            argMax(rework_prs, computed_at) AS rework_prs,
            argMax(test_gap_prs, computed_at) AS test_gap_prs,
            max(computed_at) AS computed_at
        FROM ai_impact_metrics_daily
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
    )


def _stable_id(kind: AIOpportunityKind, repo_id: str, team_id: str | None) -> str:
    raw = f"{kind.value}:{repo_id}:{team_id or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _score_ratio(value: float, threshold: float) -> float:
    if threshold <= 0:
        return 0.0
    return _clamp((value / threshold - 1.0) / 2.0 + 0.50)


def _score_delta(value: float, threshold: float) -> float:
    if threshold <= 0:
        return 0.0
    return _clamp((value / threshold - 1.0) / 2.0 + 0.50)


def _clamp_limit(limit: int | None) -> int:
    if limit is None or limit <= 0:
        return 25
    return min(limit, _MAX_LIMIT)
