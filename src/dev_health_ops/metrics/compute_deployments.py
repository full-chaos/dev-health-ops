from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Sequence, Tuple

from dev_health_ops.metrics.schemas import DeployMetricsDailyRecord, DeploymentRow


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


def compute_deploy_metrics_daily(
    *,
    day: date,
    deployments: Sequence[DeploymentRow],
    computed_at: datetime,
) -> List[DeployMetricsDailyRecord]:
    """
    Compute per-repo deployment metrics for deployments occurring on the given day.
    """
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    by_repo: Dict[str, Dict[str, object]] = {}
    for row in deployments:
        deployed_at = row.get("deployed_at") or row.get("started_at")
        if not isinstance(deployed_at, datetime):
            continue
        deployed_at = _to_utc(deployed_at)
        if not (start <= deployed_at < end):
            continue

        repo_id = str(row["repo_id"])
        bucket = by_repo.get(repo_id)
        if bucket is None:
            bucket = {
                "deployments": 0,
                "failed": 0,
                "durations": [],
                "lead_times": [],
            }
            by_repo[repo_id] = bucket

        bucket["deployments"] = int(bucket["deployments"]) + 1
        status = (row.get("status") or "").strip().lower()
        if status in {"failed", "error", "canceled"}:
            bucket["failed"] = int(bucket["failed"]) + 1

        started_at = row.get("started_at")
        finished_at = row.get("finished_at")
        if isinstance(started_at, datetime) and isinstance(finished_at, datetime):
            duration = (
                _to_utc(finished_at) - _to_utc(started_at)
            ).total_seconds() / 3600.0
            if duration >= 0:
                bucket["durations"].append(float(duration))

        merged_at = row.get("merged_at")
        if isinstance(merged_at, datetime):
            lead_time = (deployed_at - _to_utc(merged_at)).total_seconds() / 3600.0
            if lead_time >= 0:
                bucket["lead_times"].append(float(lead_time))

    records: List[DeployMetricsDailyRecord] = []
    for repo_id, bucket in sorted(by_repo.items(), key=lambda kv: kv[0]):
        deployments_count = int(bucket["deployments"])
        failed_count = int(bucket["failed"])
        durations: List[float] = list(bucket["durations"])
        lead_times: List[float] = list(bucket["lead_times"])

        records.append(
            DeployMetricsDailyRecord(
                repo_id=uuid.UUID(repo_id),
                day=day,
                deployments_count=deployments_count,
                failed_deployments_count=failed_count,
                deploy_time_p50_hours=float(_percentile(durations, 50.0))
                if durations
                else None,
                lead_time_p50_hours=float(_percentile(lead_times, 50.0))
                if lead_times
                else None,
                computed_at=computed_at_utc,
            )
        )

    return records
