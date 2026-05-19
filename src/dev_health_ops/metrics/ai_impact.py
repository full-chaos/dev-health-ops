from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from dev_health_ops.metrics.schemas import (
    AIImpactMetricsDailyRecord,
    AIOperatingLeverageComponents,
    AIPullRequestAttributionRow,
    CommitStatRow,
    IncidentRow,
    PullRequestReviewRow,
    PullRequestRow,
)

TeamResolver = Callable[[str, str | None, str | None], tuple[str | None, str | None]]

AI_BUCKETS = {"ai_assisted", "agent_created", "ai_review"}
NON_AI_BUCKET = "human"
UNKNOWN_BUCKET = "unknown"


@dataclass(frozen=True)
class _PRFact:
    repo_id: uuid.UUID
    number: int
    bucket: str
    work_type: str
    team_id: str | None
    merged: bool
    cycle_hours: float | None
    reviews: int
    changes_requested: int
    additions: int
    deletions: int
    changed_files: int
    has_test_change: bool
    followup_commits: int


@dataclass(frozen=True)
class _Agg:
    prs_total: int
    prs_merged: int
    cycle_avg: float | None
    reviews_per_pr: float | None
    changes_requested_per_pr: float | None
    rework_prs: int
    rework_rate: float | None
    followup_commits: int
    revert_prs: int
    revert_rate: float | None
    incidents_count: int
    incident_rate: float | None
    test_gap_prs: int
    test_gap_rate: float | None


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _avg(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def _safe_bucket(kind: str | None) -> str:
    if kind is None or kind == "":
        return UNKNOWN_BUCKET
    normalized = kind.strip().lower().replace("-", "_")
    if normalized in AI_BUCKETS or normalized == NON_AI_BUCKET:
        return normalized
    return UNKNOWN_BUCKET


def _is_test_path(path: str | None) -> bool:
    if not path:
        return False
    lower = path.lower()
    return (
        "/test/" in lower
        or "/tests/" in lower
        or lower.startswith("test/")
        or lower.startswith("tests/")
        or lower.endswith("_test.py")
        or lower.endswith(".test.ts")
        or lower.endswith(".test.tsx")
        or lower.endswith(".spec.ts")
        or lower.endswith(".spec.tsx")
    )


def _attribution_index(
    rows: Sequence[AIPullRequestAttributionRow],
) -> dict[tuple[uuid.UUID, int], AIPullRequestAttributionRow]:
    indexed: dict[tuple[uuid.UUID, int], AIPullRequestAttributionRow] = {}
    for row in rows:
        indexed[(row["repo_id"], int(row["number"]))] = row
    return indexed


def _reviews_by_pr(
    rows: Sequence[PullRequestReviewRow],
) -> dict[tuple[uuid.UUID, int], tuple[int, int]]:
    counts: dict[tuple[uuid.UUID, int], list[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        key = (row["repo_id"], int(row["number"]))
        counts[key][0] += 1
        if str(row.get("state") or "").upper() == "CHANGES_REQUESTED":
            counts[key][1] += 1
    return {key: (value[0], value[1]) for key, value in counts.items()}


def _test_changes_by_pr(
    commit_rows: Sequence[CommitStatRow],
    pr_commit_stats: Mapping[tuple[uuid.UUID, int], Sequence[CommitStatRow]] | None,
) -> dict[tuple[uuid.UUID, int], bool]:
    if pr_commit_stats is None:
        # Without PR↔commit linkage, preserve null-as-unavailable semantics by
        # not manufacturing test coverage evidence from repo-level commits.
        return {}
    result: dict[tuple[uuid.UUID, int], bool] = {}
    for key, rows in pr_commit_stats.items():
        result[key] = any(_is_test_path(row.get("file_path")) for row in rows)
    return result


def _aggregate(facts: Sequence[_PRFact], incidents_count: int) -> _Agg:
    cycles = [fact.cycle_hours for fact in facts if fact.cycle_hours is not None]
    prs_total = len(facts)
    prs_merged = sum(1 for fact in facts if fact.merged)
    reviews = sum(fact.reviews for fact in facts)
    changes_requested = sum(fact.changes_requested for fact in facts)
    rework_prs = sum(
        1 for fact in facts if fact.changes_requested > 0 or fact.followup_commits > 0
    )
    followup_commits = sum(fact.followup_commits for fact in facts)
    revert_prs = sum(
        1
        for fact in facts
        if fact.deletions > fact.additions * 2 and fact.deletions >= 50
    )
    test_gap_prs = sum(1 for fact in facts if not fact.has_test_change)
    return _Agg(
        prs_total=prs_total,
        prs_merged=prs_merged,
        cycle_avg=_avg(cycles),
        reviews_per_pr=_ratio(reviews, prs_total),
        changes_requested_per_pr=_ratio(changes_requested, prs_total),
        rework_prs=rework_prs,
        rework_rate=_ratio(rework_prs, prs_total),
        followup_commits=followup_commits,
        revert_prs=revert_prs,
        revert_rate=_ratio(revert_prs, prs_total),
        incidents_count=incidents_count,
        incident_rate=_ratio(incidents_count, prs_merged),
        test_gap_prs=test_gap_prs,
        test_gap_rate=_ratio(test_gap_prs, prs_total),
    )


def _component_delta(
    value: float | None,
    baseline: float | None,
    *,
    lower_is_better: bool,
) -> float | None:
    if value is None or baseline is None or baseline == 0:
        return None
    ratio = value / baseline
    return (1.0 - ratio) if lower_is_better else (ratio - 1.0)


def compute_ai_impact_metrics_daily(
    *,
    day: date,
    org_id: str,
    pull_request_rows: Sequence[PullRequestRow],
    pull_request_review_rows: Sequence[PullRequestReviewRow],
    ai_attribution_rows: Sequence[AIPullRequestAttributionRow],
    incident_rows: Sequence[IncidentRow] = (),
    commit_stat_rows: Sequence[CommitStatRow] = (),
    computed_at: datetime,
    team_resolver: TeamResolver | None = None,
    repo_names_by_id: Mapping[uuid.UUID, str] | None = None,
    pr_commit_stats: Mapping[tuple[uuid.UUID, int], Sequence[CommitStatRow]]
    | None = None,
) -> list[AIImpactMetricsDailyRecord]:
    """Compute decomposable AI workflow impact metrics for one UTC day.

    Missing attribution is represented as ``unknown``.  It is never folded into
    the human baseline, keeping AI vs non-AI comparisons uncontaminated.
    """

    attribution_by_pr = _attribution_index(ai_attribution_rows)
    review_counts = _reviews_by_pr(pull_request_review_rows)
    test_changes = _test_changes_by_pr(commit_stat_rows, pr_commit_stats)
    repo_names = repo_names_by_id or {}
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    facts: list[_PRFact] = []
    for pr in pull_request_rows:
        created_at = _to_utc(pr["created_at"])
        merged_at_raw = pr.get("merged_at")
        merged_at = _to_utc(merged_at_raw) if merged_at_raw is not None else None
        event_at = merged_at or created_at
        if not (start <= event_at < end):
            continue

        repo_id = pr["repo_id"]
        number = int(pr["number"])
        attr = attribution_by_pr.get((repo_id, number))
        bucket = _safe_bucket(attr.get("kind") if attr else None)
        work_type = (
            str(attr.get("work_type") or "pull_request") if attr else "pull_request"
        )
        team_id = attr.get("team_id") if attr else None
        if team_id is None and team_resolver is not None:
            team_id, _ = team_resolver(str(repo_id), repo_names.get(repo_id), None)

        reviews, changes_requested_from_reviews = review_counts.get(
            (repo_id, number), (0, 0)
        )
        reviews = int(pr.get("reviews_count", reviews) or reviews)
        changes_requested = int(
            pr.get("changes_requested_count", changes_requested_from_reviews)
            or changes_requested_from_reviews
        )
        cycle_hours = None
        if merged_at is not None:
            cycle_hours = (merged_at - created_at).total_seconds() / 3600.0

        additions = int(pr.get("additions", 0) or 0)
        deletions = int(pr.get("deletions", 0) or 0)
        changed_files = int(pr.get("changed_files", 0) or 0)
        facts.append(
            _PRFact(
                repo_id=repo_id,
                number=number,
                bucket=bucket,
                work_type=work_type,
                team_id=team_id,
                merged=merged_at is not None,
                cycle_hours=cycle_hours,
                reviews=reviews,
                changes_requested=changes_requested,
                additions=additions,
                deletions=deletions,
                changed_files=changed_files,
                has_test_change=test_changes.get((repo_id, number), False),
                followup_commits=0,
            )
        )

    incidents_by_repo: dict[uuid.UUID, int] = defaultdict(int)
    for incident in incident_rows:
        started_at = _to_utc(incident["started_at"])
        if start <= started_at < end:
            incidents_by_repo[incident["repo_id"]] += 1

    grouped: dict[tuple[str | None, uuid.UUID, str], list[_PRFact]] = defaultdict(list)
    for fact in facts:
        grouped[(fact.team_id, fact.repo_id, fact.work_type)].append(fact)

    rows: list[AIImpactMetricsDailyRecord] = []
    for (team_id, repo_id, work_type), group_facts in grouped.items():
        baseline = _aggregate(
            [fact for fact in group_facts if fact.bucket == NON_AI_BUCKET], 0
        )
        group_total = len(group_facts)
        for bucket in (*sorted(AI_BUCKETS), NON_AI_BUCKET, UNKNOWN_BUCKET):
            bucket_facts = [fact for fact in group_facts if fact.bucket == bucket]
            if not bucket_facts and bucket != UNKNOWN_BUCKET:
                continue
            agg = _aggregate(
                bucket_facts,
                incidents_by_repo.get(repo_id, 0) if bucket in AI_BUCKETS else 0,
            )
            ai_count = sum(1 for fact in group_facts if fact.bucket in AI_BUCKETS)
            ai_assisted_count = sum(
                1 for fact in group_facts if fact.bucket == "ai_assisted"
            )
            agent_created_count = sum(
                1 for fact in group_facts if fact.bucket == "agent_created"
            )
            human_count = sum(1 for fact in group_facts if fact.bucket == NON_AI_BUCKET)
            unknown_count = sum(
                1 for fact in group_facts if fact.bucket == UNKNOWN_BUCKET
            )
            cycle_delta = (
                agg.cycle_avg - baseline.cycle_avg
                if agg.cycle_avg is not None and baseline.cycle_avg is not None
                else None
            )
            review_amplification = _component_delta(
                agg.reviews_per_pr, baseline.reviews_per_pr, lower_is_better=False
            )
            leverage = AIOperatingLeverageComponents(
                prs_component=float(ai_count),
                cycle_time_component=_component_delta(
                    agg.cycle_avg, baseline.cycle_avg, lower_is_better=True
                ),
                review_component=review_amplification,
                rework_component=_component_delta(
                    agg.rework_rate, baseline.rework_rate, lower_is_better=True
                ),
                test_component=_component_delta(
                    agg.test_gap_rate, baseline.test_gap_rate, lower_is_better=True
                ),
                incident_component=_component_delta(
                    agg.incident_rate, baseline.incident_rate, lower_is_better=True
                ),
            )
            rows.append(
                AIImpactMetricsDailyRecord(
                    org_id=org_id,
                    team_id=team_id,
                    repo_id=repo_id,
                    work_type=work_type,
                    day=day,
                    attribution_bucket=bucket,
                    prs_total=agg.prs_total,
                    prs_merged=agg.prs_merged,
                    ai_assisted_prs=ai_assisted_count,
                    agent_created_prs=agent_created_count,
                    human_prs=human_count,
                    unknown_prs=unknown_count,
                    ai_assisted_pr_ratio=_ratio(ai_assisted_count, group_total),
                    agent_created_pr_count=agent_created_count,
                    cycle_time_avg_hours=agg.cycle_avg,
                    baseline_cycle_time_avg_hours=baseline.cycle_avg,
                    ai_cycle_time_delta_hours=cycle_delta,
                    reviews_per_pr=agg.reviews_per_pr,
                    baseline_reviews_per_pr=baseline.reviews_per_pr,
                    ai_review_amplification=review_amplification,
                    changes_requested_per_pr=agg.changes_requested_per_pr,
                    rework_prs=agg.rework_prs,
                    rework_drag_rate=agg.rework_rate,
                    followup_commits_count=agg.followup_commits,
                    revert_prs=agg.revert_prs,
                    revert_rate=agg.revert_rate,
                    incidents_count=agg.incidents_count,
                    incident_drag_rate=agg.incident_rate,
                    test_gap_prs=agg.test_gap_prs,
                    test_gap_rate=agg.test_gap_rate,
                    leverage=leverage,
                    computed_at=computed_at,
                )
            )
    return rows
