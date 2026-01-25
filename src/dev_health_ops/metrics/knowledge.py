from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Dict, List

from dev_health_ops.metrics.schemas import CommitStatRow


def compute_bus_factor(
    repo_id: str,
    window_stats: List[CommitStatRow],
    threshold_percent: float = 0.5,
) -> int:
    """
    Compute a simplified Bus Factor (Truck Factor).

    Definition: The smallest number of developers that account for >= `threshold_percent`
    of the total code churn (additions + deletions) in the window.

    This is a proxy for knowledge concentration.
    """
    if not window_stats:
        return 0

    repo_uuid = uuid.UUID(repo_id)

    # Aggregate churn by author
    author_churn: Dict[str, int] = defaultdict(int)
    total_churn = 0

    for row in window_stats:
        if row["repo_id"] != repo_uuid:
            continue

        # Use email as identity, fallback to name
        identity = row.get("author_email") or row.get("author_name") or "unknown"
        churn = (row.get("additions") or 0) + (row.get("deletions") or 0)

        author_churn[identity] += churn
        total_churn += churn

    if total_churn == 0:
        return 0

    # Sort authors by churn descending
    sorted_churns = sorted(author_churn.values(), reverse=True)

    cumulative_churn = 0
    bus_factor = 0

    target_churn = total_churn * threshold_percent

    for churn in sorted_churns:
        cumulative_churn += churn
        bus_factor += 1
        if cumulative_churn >= target_churn:
            break

    return bus_factor


def compute_code_ownership_gini(
    repo_id: str,
    window_stats: List[CommitStatRow],
) -> float:
    """
    Compute the Gini coefficient of code contribution (churn) distribution.

    0.0 = Perfect equality (all authors contribute exactly equally)
    1.0 = Perfect inequality (one author contributes everything)
    """
    if not window_stats:
        return 0.0

    repo_uuid = uuid.UUID(repo_id)

    # Aggregate churn by author
    author_churn: Dict[str, int] = defaultdict(int)

    for row in window_stats:
        if row["repo_id"] != repo_uuid:
            continue

        identity = row.get("author_email") or row.get("author_name") or "unknown"
        churn = (row.get("additions") or 0) + (row.get("deletions") or 0)
        author_churn[identity] += churn

    churns = [c for c in author_churn.values() if c > 0]

    if not churns:
        return 0.0

    churns.sort()
    n = len(churns)

    if n == 0:
        return 0.0

    # Gini coefficient formula
    # G = (2 * sum(i * y_i) / (n * sum(y_i))) - (n + 1) / n
    # where y_i are sorted values (ascending), i is 1-based index

    numerator = sum((i + 1) * val for i, val in enumerate(churns))
    denominator = n * sum(churns)

    if denominator == 0:
        return 0.0

    gini = (2 * numerator) / denominator - (n + 1) / n

    # Clamp to [0, 1] just in case of float precision issues
    return max(0.0, min(1.0, gini))
