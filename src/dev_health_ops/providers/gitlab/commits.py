"""Pure GitLab commit-row construction shared by production and parity."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any


def build_gitlab_commit_values(
    commit: object,
    repo_id: object,
    *,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Build the exact kwargs used to construct the persisted ``GitCommit``."""
    clock = now or (lambda: datetime.now(timezone.utc))
    commit_id = getattr(commit, "commit_id", None)
    if not commit_id:
        raise ValueError("GitLab commit requires commit_id")
    return {
        "repo_id": repo_id,
        "hash": commit_id,
        "message": getattr(commit, "message", None),
        "author_name": getattr(commit, "author_name", None) or "Unknown",
        "author_email": None,
        "author_when": getattr(commit, "authored_date", None) or clock(),
        "committer_name": getattr(commit, "committer_name", None) or "Unknown",
        "committer_email": None,
        "committer_when": getattr(commit, "committed_date", None) or clock(),
        "parents": len(getattr(commit, "parent_ids", ()) or ()),
    }
