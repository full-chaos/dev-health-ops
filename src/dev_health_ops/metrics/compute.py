from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dev_health_ops.metrics.schemas import (
    CommitMetricsRecord,
    CommitStatRow,
    DailyMetricsResult,
    PullRequestReviewRow,
    PullRequestRow,
    RepoMetricsDailyRecord,
    UserMetricsDailyRecord,
)
from dev_health_ops.providers.teams import TeamResolver
from dev_health_ops.providers.identity import IdentityResolver


def commit_size_bucket(total_loc: int) -> str:
    """
    Bucket a commit by total lines of code touched (additions + deletions).

    - small:  total_loc <= 50
    - medium: 51..300
    - large:  total_loc > 300
    """
    if total_loc <= 50:
        return "small"
    if total_loc <= 300:
        return "medium"
    return "large"


def _normalize_identity(
    author_email: Optional[str],
    author_name: Optional[str],
    identity_resolver: Optional[IdentityResolver] = None,
) -> str:
    """
    Prefer email when present; fall back to author_name; otherwise 'unknown'.
    If identity_resolver is provided, it is used to resolve the canonical identity.

    The returned value is used as `author_email` in stored metrics for stability.
    """
    if identity_resolver:
        # Use the shared identity resolver logic for Git authors.
        # We use 'git' as the provider.
        return identity_resolver.resolve(
            provider="git",  # type: ignore
            email=author_email,
            display_name=author_name,
        )

    if author_email:
        normalized = author_email.strip()
        if normalized:
            return normalized
    if author_name:
        normalized = author_name.strip()
        if normalized:
            return normalized
    return "unknown"


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2 == 1:
        return float(sorted_vals[mid])
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _percentile(values: Sequence[float], percentile: float) -> float:
    """
    Compute a percentile using linear interpolation between closest ranks.

    Returns 0.0 when values is empty.
    """
    if not values:
        return 0.0
    if percentile <= 0:
        return float(min(values))
    if percentile >= 100:
        return float(max(values))

    sorted_vals = sorted(float(v) for v in values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])

    rank = (len(sorted_vals) - 1) * (float(percentile) / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


@dataclass
class _CommitAgg:
    repo_id: uuid.UUID
    commit_hash: str
    author_identity: str
    committer_when: datetime
    additions: int = 0
    deletions: int = 0
    files: Set[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.files is None:
            self.files = set()

    @property
    def total_loc(self) -> int:
        return int(self.additions) + int(self.deletions)

    @property
    def files_changed(self) -> int:
        return len(self.files)


@dataclass
class _UserAgg:
    repo_id: uuid.UUID
    day: date
    author_identity: str
    commits_count: int = 0
    loc_added: int = 0
    loc_deleted: int = 0
    files: Set[str] = None  # type: ignore[assignment]
    large_commits_count: int = 0
    prs_authored: int = 0
    prs_merged: int = 0
    pr_cycle_times: List[float] = None  # type: ignore[assignment]
    pr_first_review_times: List[float] = None  # type: ignore[assignment]
    pr_review_times: List[float] = None  # type: ignore[assignment]
    pr_pickup_times: List[float] = None  # type: ignore[assignment]
    prs_with_first_review: int = 0
    reviews_given: int = 0
    changes_requested_given: int = 0
    reviews_received: int = 0
    activity_timestamps: List[datetime] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.files is None:
            self.files = set()
        if self.pr_cycle_times is None:
            self.pr_cycle_times = []
        if self.pr_first_review_times is None:
            self.pr_first_review_times = []
        if self.pr_review_times is None:
            self.pr_review_times = []
        if self.pr_pickup_times is None:
            self.pr_pickup_times = []
        if self.activity_timestamps is None:
            self.activity_timestamps = []


def compute_daily_metrics(
    *,
    day: date,
    commit_stat_rows: List[CommitStatRow],
    pull_request_rows: List[PullRequestRow],
    pull_request_review_rows: Optional[List[PullRequestReviewRow]] = None,
    computed_at: datetime,
    include_commit_metrics: bool = True,
    large_pr_total_loc_threshold: int = 1000,
    team_resolver: Optional[TeamResolver] = None,
    identity_resolver: Optional[IdentityResolver] = None,
    mttr_by_repo: Optional[Dict[uuid.UUID, float]] = None,
    rework_churn_ratio_by_repo: Optional[Dict[uuid.UUID, float]] = None,
    single_owner_file_ratio_by_repo: Optional[Dict[uuid.UUID, float]] = None,
    bus_factor_by_repo: Optional[Dict[uuid.UUID, int]] = None,
    code_ownership_gini_by_repo: Optional[Dict[uuid.UUID, float]] = None,
) -> DailyMetricsResult:
    """
    Compute daily commit/user/repo metrics for a single UTC day.

    Inputs are simplified rows pulled from the synced relational store.
    This function is pure: it does no I/O and depends only on its arguments.

    Notes:
    - `files_changed` for users is computed as distinct file paths touched by the
      user on that day (union across commits).
    - PR cycle time metrics consider PRs with `merged_at` inside the day window.
    - When no PRs are merged, avg/median PR cycle times are 0.0.
    """
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    # 1) Build per-commit aggregates from commit_stat_rows.
    commit_aggs: Dict[Tuple[uuid.UUID, str], _CommitAgg] = {}
    for row in commit_stat_rows:
        key = (row["repo_id"], row["commit_hash"])
        agg = commit_aggs.get(key)
        if agg is None:
            agg = _CommitAgg(
                repo_id=row["repo_id"],
                commit_hash=row["commit_hash"],
                author_identity=_normalize_identity(
                    row.get("author_email"),
                    row.get("author_name"),
                    identity_resolver,
                ),
                committer_when=_to_utc(row["committer_when"]),
            )
            commit_aggs[key] = agg

        additions = max(0, int(row.get("additions", 0) or 0))
        deletions = max(0, int(row.get("deletions", 0) or 0))
        agg.additions += additions
        agg.deletions += deletions

        file_path = row.get("file_path")
        if file_path:
            agg.files.add(str(file_path))

    # 2) Roll up commit aggregates to per-user.
    user_aggs: Dict[Tuple[uuid.UUID, str], _UserAgg] = {}
    for agg in commit_aggs.values():
        user_key = (agg.repo_id, agg.author_identity)
        ua = user_aggs.get(user_key)
        if ua is None:
            ua = _UserAgg(
                repo_id=agg.repo_id, day=day, author_identity=agg.author_identity
            )
            user_aggs[user_key] = ua

        ua.commits_count += 1
        ua.loc_added += agg.additions
        ua.loc_deleted += agg.deletions
        ua.files.update(agg.files)
        if agg.total_loc > 300:
            ua.large_commits_count += 1
        ua.activity_timestamps.append(agg.committer_when)

    # 3) Process PR rows for the day window.
    # 3) Process PR rows for the day window.
    repo_cycle_times: Dict[uuid.UUID, List[float]] = {}
    repo_first_review_times: Dict[uuid.UUID, List[float]] = {}
    repo_review_times: Dict[uuid.UUID, List[float]] = {}
    repo_pickup_times: Dict[uuid.UUID, List[float]] = {}
    repo_large_prs: Dict[uuid.UUID, int] = {}
    repo_rework_prs: Dict[uuid.UUID, int] = {}
    repo_revert_prs: Dict[uuid.UUID, int] = {}
    repo_prs_with_first_review: Dict[uuid.UUID, int] = {}
    repo_pr_sizes: Dict[uuid.UUID, List[int]] = {}
    repo_pr_loc_totals: Dict[uuid.UUID, int] = {}
    repo_pr_comment_totals: Dict[uuid.UUID, int] = {}
    repo_pr_review_totals: Dict[uuid.UUID, int] = {}
    repo_reviewers: Dict[uuid.UUID, Dict[str, int]] = {}

    pr_author_map: Dict[Tuple[uuid.UUID, int], str] = {}
    for pr in pull_request_rows:
        author_identity = _normalize_identity(
            pr.get("author_email"),
            pr.get("author_name"),
            identity_resolver,
        )
        pr_author_map[(pr["repo_id"], pr["number"])] = author_identity
        user_key = (pr["repo_id"], author_identity)
        ua = user_aggs.get(user_key)
        if ua is None:
            ua = _UserAgg(
                repo_id=pr["repo_id"], day=day, author_identity=author_identity
            )
            user_aggs[user_key] = ua

        created_at = _to_utc(pr["created_at"])
        merged_at = pr.get("merged_at")
        if start <= created_at < end:
            ua.prs_authored += 1
            ua.activity_timestamps.append(created_at)

        if merged_at is not None:
            merged_at_utc = _to_utc(merged_at)
            if start <= merged_at_utc < end:
                ua.prs_merged += 1
                cycle_hours = (merged_at_utc - created_at).total_seconds() / 3600.0
                ua.pr_cycle_times.append(float(cycle_hours))
                repo_cycle_times.setdefault(pr["repo_id"], []).append(
                    float(cycle_hours)
                )

                # Optional PR size facts.
                additions = int(pr.get("additions") or 0)
                deletions = int(pr.get("deletions") or 0)
                total_loc = max(0, additions) + max(0, deletions)
                if total_loc >= int(large_pr_total_loc_threshold):
                    repo_large_prs[pr["repo_id"]] = (
                        int(repo_large_prs.get(pr["repo_id"], 0)) + 1
                    )
                if total_loc > 0:
                    repo_pr_sizes.setdefault(pr["repo_id"], []).append(int(total_loc))
                    repo_pr_loc_totals[pr["repo_id"]] = int(
                        repo_pr_loc_totals.get(pr["repo_id"], 0)
                    ) + int(total_loc)
                    repo_pr_comment_totals[pr["repo_id"]] = int(
                        repo_pr_comment_totals.get(pr["repo_id"], 0)
                    ) + int(pr.get("comments_count") or 0)
                    repo_pr_review_totals[pr["repo_id"]] = int(
                        repo_pr_review_totals.get(pr["repo_id"], 0)
                    ) + int(pr.get("reviews_count") or 0)

                changes_requested_count = int(pr.get("changes_requested_count") or 0)
                if changes_requested_count > 0:
                    repo_rework_prs[pr["repo_id"]] = (
                        int(repo_rework_prs.get(pr["repo_id"], 0)) + 1
                    )

                title = str(pr.get("title") or "").strip().lower()
                if title.startswith("revert") or "revert" in title:
                    repo_revert_prs[pr["repo_id"]] = (
                        int(repo_revert_prs.get(pr["repo_id"], 0)) + 1
                    )

                # Optional collaboration facts (first review/comment timestamps).
                first_review_at = pr.get("first_review_at")
                if isinstance(first_review_at, datetime):
                    first_review_at_utc = _to_utc(first_review_at)
                    ua.prs_with_first_review += 1
                    repo_prs_with_first_review[pr["repo_id"]] = (
                        int(repo_prs_with_first_review.get(pr["repo_id"], 0)) + 1
                    )

                    fr_hours = (
                        first_review_at_utc - created_at
                    ).total_seconds() / 3600.0
                    ua.pr_first_review_times.append(float(fr_hours))
                    repo_first_review_times.setdefault(pr["repo_id"], []).append(
                        float(fr_hours)
                    )

                    review_hours = (
                        merged_at_utc - first_review_at_utc
                    ).total_seconds() / 3600.0
                    ua.pr_review_times.append(float(review_hours))
                    repo_review_times.setdefault(pr["repo_id"], []).append(
                        float(review_hours)
                    )

                first_comment_at = pr.get("first_comment_at")
                interaction_dt = None
                if isinstance(first_comment_at, datetime):
                    interaction_dt = _to_utc(first_comment_at)
                if isinstance(first_review_at, datetime):
                    fr_dt = _to_utc(first_review_at)
                    interaction_dt = (
                        fr_dt if interaction_dt is None else min(interaction_dt, fr_dt)
                    )
                if interaction_dt is not None:
                    pickup_hours = (
                        interaction_dt - created_at
                    ).total_seconds() / 3600.0
                    ua.pr_pickup_times.append(float(pickup_hours))
                    repo_pickup_times.setdefault(pr["repo_id"], []).append(
                        float(pickup_hours)
                    )

    # 3b) Review participation (reviews submitted in day window).
    if pull_request_review_rows:
        for review in pull_request_review_rows:
            submitted_at = _to_utc(review["submitted_at"])
            if not (start <= submitted_at < end):
                continue

            reviewer_identity = _normalize_identity(
                None,
                review["reviewer"],
                identity_resolver,
            )
            rua = user_aggs.get((review["repo_id"], reviewer_identity))
            if rua is None:
                rua = _UserAgg(
                    repo_id=review["repo_id"],
                    day=day,
                    author_identity=reviewer_identity,
                )
                user_aggs[(review["repo_id"], reviewer_identity)] = rua

            rua.reviews_given += 1
            rua.activity_timestamps.append(submitted_at)
            if review["state"] == "CHANGES_REQUESTED":
                rua.changes_requested_given += 1

            # Count review as received for the PR author
            pr_author = pr_author_map.get((review["repo_id"], review["number"]))
            if pr_author:
                aua = user_aggs.get((review["repo_id"], pr_author))
                if aua:
                    aua.reviews_received += 1

            reviewers = repo_reviewers.setdefault(review["repo_id"], {})
            reviewers[reviewer_identity] = int(reviewers.get(reviewer_identity, 0)) + 1

    # 4) Finalize user metrics records.
    user_metrics: List[UserMetricsDailyRecord] = []
    for (repo_id, author_identity), ua in sorted(
        user_aggs.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])
    ):
        team_id = "unassigned"
        team_name = "Unassigned"
        if team_resolver is not None:
            t_id, t_name = team_resolver.resolve(author_identity)
            if t_id:
                team_id = t_id
                team_name = t_name or t_id
        commits_count = int(ua.commits_count)
        total_loc_touched = int(ua.loc_added) + int(ua.loc_deleted)
        avg_commit_size_loc = (
            (total_loc_touched / commits_count) if commits_count else 0.0
        )

        avg_pr_cycle = _mean(ua.pr_cycle_times)
        median_pr_cycle = _median(ua.pr_cycle_times)
        pr_cycle_p75 = _percentile(ua.pr_cycle_times, 75.0)
        pr_cycle_p90 = _percentile(ua.pr_cycle_times, 90.0)

        pr_first_review_p50 = (
            _percentile(ua.pr_first_review_times, 50.0)
            if ua.pr_first_review_times
            else None
        )
        pr_first_review_p90 = (
            _percentile(ua.pr_first_review_times, 90.0)
            if ua.pr_first_review_times
            else None
        )
        pr_review_time_p50 = (
            _percentile(ua.pr_review_times, 50.0) if ua.pr_review_times else None
        )
        pr_pickup_p50 = (
            _percentile(ua.pr_pickup_times, 50.0) if ua.pr_pickup_times else None
        )

        # Calculate active hours from timestamps
        active_hours = 0.0
        if ua.activity_timestamps:
            # We must sort and ensure they are all in the current day window?
            # Ideally they are, as we filtered during collection.
            # But let's be safe and verify connection to day (UTC).
            # Start/end are already computed at top of function.
            day_ts = [t for t in ua.activity_timestamps if start <= t <= end]
            if len(day_ts) > 1:
                min_ts = min(day_ts)
                max_ts = max(day_ts)
                active_hours = (max_ts - min_ts).total_seconds() / 3600.0

        weekend_days = 0
        if active_hours > 0 and day.weekday() >= 5:
            weekend_days = 1

        user_metrics.append(
            UserMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                author_email=author_identity,
                commits_count=commits_count,
                loc_added=int(ua.loc_added),
                loc_deleted=int(ua.loc_deleted),
                files_changed=len(ua.files),
                large_commits_count=int(ua.large_commits_count),
                avg_commit_size_loc=float(avg_commit_size_loc),
                prs_authored=int(ua.prs_authored),
                prs_merged=int(ua.prs_merged),
                avg_pr_cycle_hours=float(avg_pr_cycle),
                median_pr_cycle_hours=float(median_pr_cycle),
                pr_cycle_p75_hours=float(pr_cycle_p75),
                pr_cycle_p90_hours=float(pr_cycle_p90),
                prs_with_first_review=int(ua.prs_with_first_review),
                pr_first_review_p50_hours=float(pr_first_review_p50)
                if pr_first_review_p50 is not None
                else None,
                pr_first_review_p90_hours=float(pr_first_review_p90)
                if pr_first_review_p90 is not None
                else None,
                pr_review_time_p50_hours=float(pr_review_time_p50)
                if pr_review_time_p50 is not None
                else None,
                pr_pickup_time_p50_hours=float(pr_pickup_p50)
                if pr_pickup_p50 is not None
                else None,
                reviews_given=int(ua.reviews_given),
                changes_requested_given=int(ua.changes_requested_given),
                reviews_received=int(ua.reviews_received),
                review_reciprocity=(
                    min(ua.reviews_given, ua.reviews_received)
                    / max(1, max(ua.reviews_given, ua.reviews_received))
                ),
                team_id=team_id,
                team_name=team_name,
                active_hours=float(active_hours),
                weekend_days=int(weekend_days),
                identity_id=author_identity,
                computed_at=computed_at_utc,
            )
        )

    # 5) Roll up to per-repo metrics.
    repos: Set[uuid.UUID] = set()
    repos.update(repo_id for (repo_id, _author) in user_aggs.keys())
    repos.update(pr["repo_id"] for pr in pull_request_rows)
    repo_metrics: List[RepoMetricsDailyRecord] = []
    for repo_id in sorted(repos, key=str):
        repo_users = [u for u in user_metrics if u.repo_id == repo_id]
        commits_count = sum(u.commits_count for u in repo_users)
        total_loc_touched = sum(u.loc_added + u.loc_deleted for u in repo_users)
        large_commits_count = sum(u.large_commits_count for u in repo_users)
        prs_merged = sum(u.prs_merged for u in repo_users)

        avg_commit_size_loc = (
            (total_loc_touched / commits_count) if commits_count else 0.0
        )
        large_commit_ratio = (
            (large_commits_count / commits_count) if commits_count else 0.0
        )
        repo_cycles = repo_cycle_times.get(repo_id, [])
        median_repo_cycle = _median(repo_cycles)
        pr_cycle_p75 = _percentile(repo_cycles, 75.0)
        pr_cycle_p90 = _percentile(repo_cycles, 90.0)

        repo_first_reviews = repo_first_review_times.get(repo_id, [])
        repo_review_times_list = repo_review_times.get(repo_id, [])
        repo_pickups = repo_pickup_times.get(repo_id, [])
        repo_sizes = repo_pr_sizes.get(repo_id, [])
        repo_loc_total = int(repo_pr_loc_totals.get(repo_id, 0))
        repo_comment_total = int(repo_pr_comment_totals.get(repo_id, 0))
        repo_review_total = int(repo_pr_review_totals.get(repo_id, 0))

        large_pr_count = int(repo_large_prs.get(repo_id, 0) or 0)
        rework_pr_count = int(repo_rework_prs.get(repo_id, 0) or 0)
        revert_pr_count = int(repo_revert_prs.get(repo_id, 0) or 0)
        total_prs_merged = prs_merged or 1
        change_failure_rate = revert_pr_count / total_prs_merged
        prs_with_first_review = int(repo_prs_with_first_review.get(repo_id, 0) or 0)
        large_pr_ratio = (large_pr_count / prs_merged) if prs_merged else 0.0
        pr_rework_ratio = (rework_pr_count / prs_merged) if prs_merged else 0.0
        pr_size_p50 = _percentile(repo_sizes, 50.0) if repo_sizes else None
        pr_size_p90 = _percentile(repo_sizes, 90.0) if repo_sizes else None
        pr_comments_per_100_loc = (
            (repo_comment_total / repo_loc_total) * 100.0 if repo_loc_total else None
        )
        pr_reviews_per_100_loc = (
            (repo_review_total / repo_loc_total) * 100.0 if repo_loc_total else None
        )

        reviewer_counts = repo_reviewers.get(repo_id, {})
        total_reviews = sum(reviewer_counts.values())
        top_reviewer_ratio = (
            max(reviewer_counts.values()) / total_reviews
            if total_reviews and reviewer_counts
            else 0.0
        )

        repo_metrics.append(
            RepoMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                commits_count=int(commits_count),
                total_loc_touched=int(total_loc_touched),
                avg_commit_size_loc=float(avg_commit_size_loc),
                large_commit_ratio=float(large_commit_ratio),
                prs_merged=int(prs_merged),
                median_pr_cycle_hours=float(median_repo_cycle),
                pr_cycle_p75_hours=float(pr_cycle_p75),
                pr_cycle_p90_hours=float(pr_cycle_p90),
                prs_with_first_review=int(prs_with_first_review),
                pr_first_review_p50_hours=float(_percentile(repo_first_reviews, 50.0))
                if repo_first_reviews
                else None,
                pr_first_review_p90_hours=float(_percentile(repo_first_reviews, 90.0))
                if repo_first_reviews
                else None,
                pr_review_time_p50_hours=float(
                    _percentile(repo_review_times_list, 50.0)
                )
                if repo_review_times_list
                else None,
                pr_pickup_time_p50_hours=float(_percentile(repo_pickups, 50.0))
                if repo_pickups
                else None,
                large_pr_ratio=float(large_pr_ratio),
                pr_rework_ratio=float(pr_rework_ratio),
                pr_size_p50_loc=float(pr_size_p50) if pr_size_p50 is not None else None,
                pr_size_p90_loc=float(pr_size_p90) if pr_size_p90 is not None else None,
                pr_comments_per_100_loc=float(pr_comments_per_100_loc)
                if pr_comments_per_100_loc is not None
                else None,
                pr_reviews_per_100_loc=float(pr_reviews_per_100_loc)
                if pr_reviews_per_100_loc is not None
                else None,
                rework_churn_ratio_30d=float(
                    rework_churn_ratio_by_repo.get(repo_id, 0.0)
                    if rework_churn_ratio_by_repo
                    else 0.0
                ),
                single_owner_file_ratio_30d=float(
                    single_owner_file_ratio_by_repo.get(repo_id, 0.0)
                    if single_owner_file_ratio_by_repo
                    else 0.0
                ),
                review_load_top_reviewer_ratio=float(top_reviewer_ratio),
                bus_factor=int(
                    bus_factor_by_repo.get(repo_id, 0) if bus_factor_by_repo else 0
                ),
                code_ownership_gini=float(
                    code_ownership_gini_by_repo.get(repo_id, 0.0)
                    if code_ownership_gini_by_repo
                    else 0.0
                ),
                mttr_hours=mttr_by_repo.get(repo_id) if mttr_by_repo else None,
                change_failure_rate=float(change_failure_rate),
                computed_at=computed_at_utc,
            )
        )

    # 6) Optional per-commit metrics.
    commit_metrics: List[CommitMetricsRecord] = []
    if include_commit_metrics:
        for agg in sorted(
            commit_aggs.values(), key=lambda a: (str(a.repo_id), a.commit_hash)
        ):
            commit_metrics.append(
                CommitMetricsRecord(
                    repo_id=agg.repo_id,
                    commit_hash=agg.commit_hash,
                    day=day,
                    author_email=agg.author_identity,
                    total_loc=int(agg.total_loc),
                    files_changed=int(agg.files_changed),
                    size_bucket=commit_size_bucket(int(agg.total_loc)),
                    computed_at=computed_at_utc,
                )
            )

    return DailyMetricsResult(
        day=day,
        repo_metrics=repo_metrics,
        user_metrics=user_metrics,
        commit_metrics=commit_metrics,
    )
