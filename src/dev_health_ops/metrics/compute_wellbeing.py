from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from zoneinfo import ZoneInfo

from dev_health_ops.metrics.schemas import CommitStatRow, TeamMetricsDailyRecord
from dev_health_ops.providers.teams import TeamResolver


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


def _is_weekend(local_dt: datetime) -> bool:
    return local_dt.weekday() >= 5  # 5=Sat, 6=Sun


def _is_after_hours(local_dt: datetime, *, start_hour: int, end_hour: int) -> bool:
    if _is_weekend(local_dt):
        return False
    hour = local_dt.hour
    return hour < int(start_hour) or hour >= int(end_hour)


def compute_team_wellbeing_metrics_daily(
    *,
    day: date,
    commit_stat_rows: Sequence[CommitStatRow],
    team_resolver: Optional[TeamResolver],
    computed_at: datetime,
    business_timezone: str = "UTC",
    business_hours_start: int = 9,
    business_hours_end: int = 17,
    unknown_team_id: str = "unassigned",
    unknown_team_name: str = "Unassigned",
) -> List[TeamMetricsDailyRecord]:
    """
    Compute team-level (non-individual) after-hours + weekend activity ratios.

    The input is commit_stat_rows; commits are deduplicated by (repo_id, commit_hash)
    because commit_stat_rows are per-file.
    """
    tz = ZoneInfo(business_timezone)
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    # Deduplicate commits.
    commits: Dict[Tuple[uuid.UUID, str], Tuple[str, datetime]] = {}
    for row in commit_stat_rows:
        key = (row["repo_id"], row["commit_hash"])
        if key in commits:
            continue
        commits[key] = (
            _normalize_identity(row.get("author_email"), row.get("author_name")),
            _to_utc(row["committer_when"]),
        )

    # Aggregate by team.
    by_team: Dict[str, Dict[str, object]] = {}
    for _key, (identity, committed_at) in commits.items():
        if not (start <= committed_at < end):
            continue
        team_id = None
        team_name = None
        if team_resolver is not None:
            team_id, team_name = team_resolver.resolve(identity)
        if not team_id:
            team_id = unknown_team_id
            team_name = unknown_team_name
        bucket = by_team.get(team_id)
        if bucket is None:
            bucket = {
                "team_name": team_name,
                "commits": 0,
                "after_hours": 0,
                "weekend": 0,
            }
            by_team[team_id] = bucket

        bucket["commits"] = int(bucket["commits"]) + 1

        local_dt = committed_at.astimezone(tz)
        if _is_weekend(local_dt):
            bucket["weekend"] = int(bucket["weekend"]) + 1
        elif _is_after_hours(
            local_dt, start_hour=business_hours_start, end_hour=business_hours_end
        ):
            bucket["after_hours"] = int(bucket["after_hours"]) + 1

    records: List[TeamMetricsDailyRecord] = []
    for team_id, bucket in sorted(by_team.items(), key=lambda kv: kv[0]):
        commits_count = int(bucket["commits"])
        after_hours_count = int(bucket["after_hours"])
        weekend_count = int(bucket["weekend"])
        after_hours_ratio = (
            (after_hours_count / commits_count) if commits_count else 0.0
        )
        weekend_ratio = (weekend_count / commits_count) if commits_count else 0.0

        records.append(
            TeamMetricsDailyRecord(
                day=day,
                team_id=team_id,
                team_name=str(bucket.get("team_name") or team_id),
                commits_count=commits_count,
                after_hours_commits_count=after_hours_count,
                weekend_commits_count=weekend_count,
                after_hours_commit_ratio=float(after_hours_ratio),
                weekend_commit_ratio=float(weekend_ratio),
                computed_at=computed_at_utc,
            )
        )

    return records
