from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from dev_health_ops.models.git import GitCommit, GitCommitStat, GitPullRequest


@dataclass
class CommitMetric:
    commit_hash: str
    repo_id: uuid.UUID  # same type as GitCommit.repo_id
    author_name: str
    committed_at: datetime
    total_loc: int
    files_changed: int
    is_large_commit: bool
    size_bucket: str  # "small", "medium", "large"


@dataclass
class PRMetric:
    repo_id: uuid.UUID
    pr_number: int
    author_name: str
    created_at: datetime
    merged_at: Optional[datetime]
    cycle_time_hours: Optional[
        float
    ]  # merged_at - created_at in hours, None if not merged


@dataclass
class UserMetrics:
    user_name: str  # derived from commit.author_name and pr.author_name
    commits_count: int
    total_loc_added: int
    total_loc_deleted: int
    total_files_changed: int
    large_commits_count: int
    avg_commit_size: float
    prs_authored: int
    avg_pr_cycle_time: Optional[float]


@dataclass
class RepoMetrics:
    repo_id: uuid.UUID
    commits_count: int
    total_loc_touched: int
    avg_commit_size: float
    large_commit_ratio: float
    prs_merged: int
    median_pr_cycle_time: Optional[float]


def _normalize_author_name(author_name: Optional[str]) -> str:
    if author_name is None:
        return "unknown"
    normalized = author_name.strip()
    return normalized if normalized else "unknown"


def _size_bucket(total_loc: int) -> str:
    if total_loc <= 50:
        return "small"
    if total_loc <= 300:
        return "medium"
    return "large"


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2 == 1:
        return float(sorted_vals[mid])
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def _aggregate_commit_stats(
    commit_stats: List[GitCommitStat],
    allowed_commit_keys: Optional[Set[Tuple[uuid.UUID, str]]] = None,
) -> Dict[Tuple[uuid.UUID, str], Tuple[int, int, Set[str]]]:
    """
    Aggregate commit stats keyed by (repo_id, commit_hash).

    Returns a mapping of:
      (repo_id, commit_hash) -> (additions, deletions, unique_file_paths)
    """
    aggregated: Dict[Tuple[uuid.UUID, str], Tuple[int, int, Set[str]]] = {}
    for stat in commit_stats:
        key = (stat.repo_id, stat.commit_hash)
        if allowed_commit_keys is not None and key not in allowed_commit_keys:
            continue

        additions = int(getattr(stat, "additions", 0) or 0)
        deletions = int(getattr(stat, "deletions", 0) or 0)
        file_path = getattr(stat, "file_path", None)

        if key not in aggregated:
            aggregated[key] = (0, 0, set())
        cur_add, cur_del, cur_files = aggregated[key]
        cur_add += additions
        cur_del += deletions
        if file_path:
            cur_files.add(file_path)
        aggregated[key] = (cur_add, cur_del, cur_files)

    return aggregated


def compute_commit_metrics(
    commits: List[GitCommit], commit_stats: List[GitCommitStat]
) -> List[CommitMetric]:
    """
    Compute per-commit metrics.

    - For each `GitCommit` in `commits`, sum the additions and deletions from
      `commit_stats` with matching `commit_hash` to get `total_loc`
      (additions + deletions).
    - Count the number of unique `file_path` entries in `commit_stats` for that
      commit to get `files_changed`.
    - Determine a `size_bucket`:
        * "small"  if total_loc <= 50
        * "medium" if 51 <= total_loc <= 300
        * "large"  if total_loc > 300
    - Set `is_large_commit` to True when size_bucket == "large".
    - Return a list of `CommitMetric` instances with the above properties.

    The order of the returned list should match the input commits list.
    """
    commit_keys: Set[Tuple[uuid.UUID, str]] = {
        (c.repo_id, c.hash) for c in commits if getattr(c, "repo_id", None) and c.hash
    }
    stats_index = _aggregate_commit_stats(commit_stats, allowed_commit_keys=commit_keys)

    metrics: List[CommitMetric] = []
    for commit in commits:
        key = (commit.repo_id, commit.hash)
        additions, deletions, files = stats_index.get(key, (0, 0, set()))
        total_loc = additions + deletions
        bucket = _size_bucket(total_loc)

        committed_at = getattr(commit, "committer_when", None) or getattr(
            commit, "author_when", None
        )
        if committed_at is None:
            # Defensive fallback: keep function total and stable even with
            # malformed input objects.
            committed_at = datetime.min

        metrics.append(
            CommitMetric(
                commit_hash=commit.hash,
                repo_id=commit.repo_id,
                author_name=_normalize_author_name(
                    getattr(commit, "author_name", None)
                ),
                committed_at=committed_at,
                total_loc=total_loc,
                files_changed=len(files),
                is_large_commit=bucket == "large",
                size_bucket=bucket,
            )
        )

    return metrics


def compute_pr_metrics(prs: List[GitPullRequest]) -> List[PRMetric]:
    """
    Compute per-PR metrics. For each PR:
    - If `merged_at` is not None, compute `cycle_time_hours` as
      (merged_at - created_at).total_seconds() / 3600.
    - Otherwise set `cycle_time_hours` to None.
    - Return a list of `PRMetric` objects.
    """
    metrics: List[PRMetric] = []
    for pr in prs:
        created_at = getattr(pr, "created_at", None)
        merged_at = getattr(pr, "merged_at", None)
        cycle_time_hours: Optional[float] = None
        if created_at is not None and merged_at is not None:
            cycle_time_hours = (merged_at - created_at).total_seconds() / 3600.0

        metrics.append(
            PRMetric(
                repo_id=pr.repo_id,
                pr_number=pr.number,
                author_name=_normalize_author_name(getattr(pr, "author_name", None)),
                created_at=created_at or datetime.min,
                merged_at=merged_at,
                cycle_time_hours=cycle_time_hours,
            )
        )

    return metrics


def compute_user_metrics(
    commits: List[GitCommit],
    commit_stats: List[GitCommitStat],
    pr_metrics: List[PRMetric],
) -> Dict[str, UserMetrics]:
    """
    Aggregate per-user metrics based on commit and PR data.

    For each unique author_name from `commits` and `prs`:
    - Initialize counters.
    - **Commits**:
        * Count how many commits they authored (`commits_count`).
        * Sum additions, deletions and file changes from `commit_stats` for each commit.
        * Count how many commits are "large" (total_loc > 300).
        * Compute `avg_commit_size` as total additions + deletions divided by commits_count.
    - **PRs**:
        * Count how many PRs they authored (`prs_authored`).
        * Compute the average cycle time across their merged PRs (ignore PRs where cycle_time is None).
    Return a dictionary keyed by author_name with `UserMetrics` values.
    """
    commit_keys: Set[Tuple[uuid.UUID, str]] = {
        (c.repo_id, c.hash) for c in commits if getattr(c, "repo_id", None) and c.hash
    }
    stats_index = _aggregate_commit_stats(commit_stats, allowed_commit_keys=commit_keys)

    users: Set[str] = set()
    for c in commits:
        users.add(_normalize_author_name(getattr(c, "author_name", None)))
    for pr in pr_metrics:
        users.add(_normalize_author_name(pr.author_name))

    # Commit aggregates
    commits_count: Dict[str, int] = {u: 0 for u in users}
    total_added: Dict[str, int] = {u: 0 for u in users}
    total_deleted: Dict[str, int] = {u: 0 for u in users}
    total_files_changed: Dict[str, int] = {u: 0 for u in users}
    large_commits: Dict[str, int] = {u: 0 for u in users}
    total_commit_loc: Dict[str, int] = {u: 0 for u in users}

    for commit in commits:
        user = _normalize_author_name(getattr(commit, "author_name", None))
        commits_count[user] = commits_count.get(user, 0) + 1

        key = (commit.repo_id, commit.hash)
        additions, deletions, files = stats_index.get(key, (0, 0, set()))
        total_added[user] = total_added.get(user, 0) + additions
        total_deleted[user] = total_deleted.get(user, 0) + deletions
        total_files_changed[user] = total_files_changed.get(user, 0) + len(files)

        total_loc = additions + deletions
        total_commit_loc[user] = total_commit_loc.get(user, 0) + total_loc
        if total_loc > 300:
            large_commits[user] = large_commits.get(user, 0) + 1

    # PR aggregates
    prs_authored: Dict[str, int] = {u: 0 for u in users}
    pr_cycle_time_sum: Dict[str, float] = {u: 0.0 for u in users}
    pr_cycle_time_count: Dict[str, int] = {u: 0 for u in users}

    for pr in pr_metrics:
        user = _normalize_author_name(pr.author_name)
        prs_authored[user] = prs_authored.get(user, 0) + 1
        if pr.cycle_time_hours is not None:
            pr_cycle_time_sum[user] = pr_cycle_time_sum.get(user, 0.0) + float(
                pr.cycle_time_hours
            )
            pr_cycle_time_count[user] = pr_cycle_time_count.get(user, 0) + 1

    result: Dict[str, UserMetrics] = {}
    for user in sorted(users):
        c_count = commits_count.get(user, 0)
        avg_commit_size = (total_commit_loc.get(user, 0) / c_count) if c_count else 0.0

        ct_count = pr_cycle_time_count.get(user, 0)
        avg_pr_cycle_time = (
            (pr_cycle_time_sum.get(user, 0.0) / ct_count) if ct_count else None
        )

        result[user] = UserMetrics(
            user_name=user,
            commits_count=c_count,
            total_loc_added=total_added.get(user, 0),
            total_loc_deleted=total_deleted.get(user, 0),
            total_files_changed=total_files_changed.get(user, 0),
            large_commits_count=large_commits.get(user, 0),
            avg_commit_size=float(avg_commit_size),
            prs_authored=prs_authored.get(user, 0),
            avg_pr_cycle_time=avg_pr_cycle_time,
        )

    return result


def compute_repo_metrics(
    commits: List[GitCommit],
    commit_stats: List[GitCommitStat],
    pr_metrics: List[PRMetric],
) -> Dict[uuid.UUID, RepoMetrics]:
    """
    Aggregate per-repo metrics.

    For each unique `repo_id` in commits and PRs:
    - **Commits**:
        * Count the total number of commits for the repo.
        * Sum all additions + deletions from `commit_stats` for commits in this repo to get `total_loc_touched`.
        * Compute average commit size (`avg_commit_size`) = total_loc_touched / commits_count.
        * Compute `large_commit_ratio` = (number of commits with total_loc > 300) / commits_count.
    - **PRs**:
        * Count how many PRs have `merged_at` not None (`prs_merged`).
        * Collect cycle_time_hours from `pr_metrics` for this repo and compute the median
          (use simple median; if even length, average the two middle values). If no merged PRs, set to None.
    Return a dictionary keyed by repo_id with `RepoMetrics` values.
    """
    commit_keys: Set[Tuple[uuid.UUID, str]] = {
        (c.repo_id, c.hash) for c in commits if getattr(c, "repo_id", None) and c.hash
    }
    stats_index = _aggregate_commit_stats(commit_stats, allowed_commit_keys=commit_keys)

    repo_ids: Set[uuid.UUID] = set()
    for c in commits:
        if getattr(c, "repo_id", None) is not None:
            repo_ids.add(c.repo_id)
    for pr in pr_metrics:
        repo_ids.add(pr.repo_id)

    commits_by_repo: Dict[uuid.UUID, List[Tuple[uuid.UUID, str]]] = {}
    for c in commits:
        commits_by_repo.setdefault(c.repo_id, []).append((c.repo_id, c.hash))

    repo_result: Dict[uuid.UUID, RepoMetrics] = {}
    for repo_id in repo_ids:
        commit_keys_for_repo = commits_by_repo.get(repo_id, [])
        commits_count = len(commit_keys_for_repo)

        total_loc_touched = 0
        large_commits = 0
        for key in commit_keys_for_repo:
            additions, deletions, _files = stats_index.get(key, (0, 0, set()))
            total_loc = additions + deletions
            total_loc_touched += total_loc
            if total_loc > 300:
                large_commits += 1

        avg_commit_size = (total_loc_touched / commits_count) if commits_count else 0.0
        large_commit_ratio = (large_commits / commits_count) if commits_count else 0.0

        merged_prs = [p for p in pr_metrics if p.repo_id == repo_id and p.merged_at]
        prs_merged = len(merged_prs)
        cycle_times = [
            float(p.cycle_time_hours)
            for p in merged_prs
            if p.cycle_time_hours is not None
        ]
        median_pr_cycle_time = _median(cycle_times)

        repo_result[repo_id] = RepoMetrics(
            repo_id=repo_id,
            commits_count=commits_count,
            total_loc_touched=total_loc_touched,
            avg_commit_size=float(avg_commit_size),
            large_commit_ratio=float(large_commit_ratio),
            prs_merged=prs_merged,
            median_pr_cycle_time=median_pr_cycle_time,
        )

    return repo_result
