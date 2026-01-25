from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from dev_health_ops.metrics.schemas import (
    PullRequestReviewRow,
    PullRequestRow,
    ReviewEdgeDailyRecord,
)


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_identity(author_email: Optional[str], author_name: Optional[str]) -> str:
    if author_email and author_email.strip():
        return author_email.strip()
    if author_name and author_name.strip():
        return author_name.strip()
    return "unknown"


def compute_review_edges_daily(
    *,
    day: date,
    pull_request_rows: Sequence[PullRequestRow],
    pull_request_review_rows: Optional[Sequence[PullRequestReviewRow]],
    computed_at: datetime,
) -> List[ReviewEdgeDailyRecord]:
    """
    Build reviewer -> author edge counts for reviews submitted in the day window.
    """
    if not pull_request_review_rows:
        return []

    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    pr_author_map: Dict[Tuple[uuid.UUID, int], str] = {}
    for pr in pull_request_rows:
        author_identity = _normalize_identity(
            pr.get("author_email"), pr.get("author_name")
        )
        pr_author_map[(pr["repo_id"], pr["number"])] = author_identity

    edge_counts: Dict[Tuple[uuid.UUID, str, str], int] = {}
    for review in pull_request_review_rows:
        submitted_at = _to_utc(review["submitted_at"])
        if not (start <= submitted_at < end):
            continue
        reviewer = _normalize_identity(None, review["reviewer"])
        author = pr_author_map.get((review["repo_id"], review["number"]))
        if not author:
            continue
        key = (review["repo_id"], reviewer, author)
        edge_counts[key] = int(edge_counts.get(key, 0)) + 1

    rows: List[ReviewEdgeDailyRecord] = []
    for (repo_id, reviewer, author), count in sorted(
        edge_counts.items(), key=lambda kv: (str(kv[0][0]), kv[0][1], kv[0][2])
    ):
        rows.append(
            ReviewEdgeDailyRecord(
                repo_id=repo_id,
                day=day,
                reviewer=reviewer,
                author=author,
                reviews_count=int(count),
                computed_at=computed_at_utc,
            )
        )

    return rows
