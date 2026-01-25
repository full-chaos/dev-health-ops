from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Sequence, Tuple

from dev_health_ops.metrics.schemas import IncidentMetricsDailyRecord, IncidentRow


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _percentile(values: Sequence[float], percentile: float) -> float:
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


def compute_incident_metrics_daily(
    *,
    day: date,
    incidents: Sequence[IncidentRow],
    computed_at: datetime,
) -> List[IncidentMetricsDailyRecord]:
    """
    Compute MTTR distributions for incidents resolved on the given day.
    """
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    by_repo: Dict[str, Dict[str, object]] = {}
    for row in incidents:
        resolved_at = row.get("resolved_at")
        if not isinstance(resolved_at, datetime):
            continue
        resolved_at = _to_utc(resolved_at)
        if not (start <= resolved_at < end):
            continue

        repo_id = str(row["repo_id"])
        bucket = by_repo.get(repo_id)
        if bucket is None:
            bucket = {"incidents": 0, "mttr_hours": []}
            by_repo[repo_id] = bucket

        bucket["incidents"] = int(bucket["incidents"]) + 1
        started_at = _to_utc(row["started_at"])
        mttr = (resolved_at - started_at).total_seconds() / 3600.0
        if mttr >= 0:
            bucket["mttr_hours"].append(float(mttr))

    records: List[IncidentMetricsDailyRecord] = []
    for repo_id, bucket in sorted(by_repo.items(), key=lambda kv: kv[0]):
        mttr_hours: List[float] = list(bucket["mttr_hours"])
        records.append(
            IncidentMetricsDailyRecord(
                repo_id=uuid.UUID(repo_id),
                day=day,
                incidents_count=int(bucket["incidents"]),
                mttr_p50_hours=float(_percentile(mttr_hours, 50.0))
                if mttr_hours
                else None,
                mttr_p90_hours=float(_percentile(mttr_hours, 90.0))
                if mttr_hours
                else None,
                computed_at=computed_at_utc,
            )
        )

    return records
