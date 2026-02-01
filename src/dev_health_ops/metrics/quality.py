from __future__ import annotations

from collections import defaultdict
from typing import Dict, Optional, Sequence

from dev_health_ops.metrics.schemas import CommitStatRow
from dev_health_ops.providers.identity import IdentityResolver, normalize_git_identity


def compute_rework_churn_ratio(
    *,
    repo_id: str,
    window_stats: Sequence[CommitStatRow],
) -> float:
    """
    Approximate rework as churn on files touched by multiple commits in the window.

    This is a proxy for "rework within 30 days" using commit-level churn.
    """
    file_stats: Dict[str, Dict[str, object]] = {}
    for row in window_stats:
        if str(row["repo_id"]) != repo_id:
            continue
        path = row.get("file_path")
        if not path:
            continue
        stats = file_stats.get(path)
        if stats is None:
            stats = {"churn": 0, "commits": set()}
            file_stats[path] = stats
        additions = max(0, int(row.get("additions") or 0))
        deletions = max(0, int(row.get("deletions") or 0))
        stats["churn"] = int(stats["churn"]) + additions + deletions
        stats["commits"].add(str(row.get("commit_hash")))

    total_churn = sum(int(stats["churn"]) for stats in file_stats.values())
    if total_churn == 0:
        return 0.0

    rework_churn = sum(
        int(stats["churn"])
        for stats in file_stats.values()
        if len(stats["commits"]) > 1
    )
    return float(rework_churn) / float(total_churn)


def compute_single_owner_file_ratio(
    *,
    repo_id: str,
    window_stats: Sequence[CommitStatRow],
    owner_threshold: float = 0.75,
    identity_resolver: Optional[IdentityResolver] = None,
) -> float:
    """
    Compute ratio of files dominated by a single owner in the window.
    """
    file_authors: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))

    for row in window_stats:
        if str(row["repo_id"]) != repo_id:
            continue
        path = row.get("file_path")
        if not path:
            continue
        author = normalize_git_identity(
            row.get("author_email"), row.get("author_name"), identity_resolver
        )
        file_authors[str(path)][author].add(str(row.get("commit_hash")))

    if not file_authors:
        return 0.0

    single_owner_files = 0
    for _path, author_commits in file_authors.items():
        counts = [len(commits) for commits in author_commits.values()]
        total = sum(counts)
        if total == 0:
            continue
        if max(counts) / total >= float(owner_threshold):
            single_owner_files += 1

    return float(single_owner_files) / float(len(file_authors))
