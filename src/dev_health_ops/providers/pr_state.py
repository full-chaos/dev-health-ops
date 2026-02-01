from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional


class PRState(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    MERGED = "merged"


def normalize_pr_state(
    raw_state: Optional[str],
    merged_at: Optional[datetime] = None,
) -> str:
    """Normalize PR state to canonical values: open, closed, merged.

    GitHub returns 'closed' for both merged and unmerged PRs - use merged_at to distinguish.
    GitLab returns 'opened', 'closed', 'merged' - normalize 'opened' to 'open'.
    """
    if not raw_state:
        return PRState.OPEN.value

    state_lower = raw_state.strip().lower()

    if state_lower == "merged":
        return PRState.MERGED.value

    if state_lower == "opened":
        return PRState.OPEN.value

    if state_lower == "open":
        return PRState.OPEN.value

    if state_lower == "closed":
        if merged_at is not None:
            return PRState.MERGED.value
        return PRState.CLOSED.value

    return PRState.OPEN.value
