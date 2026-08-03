from __future__ import annotations

from typing import Any


def build_gitlab_commit_stat_values(
    detailed_stats: Any,
    repo_id: Any,
) -> dict[str, Any]:
    """Build the canonical GitLab aggregate commit-stat model values."""
    return {
        "repo_id": repo_id,
        "commit_hash": detailed_stats.commit_id,
        "file_path": "__AGGREGATE__",
        "additions": detailed_stats.additions,
        "deletions": detailed_stats.deletions,
    }
