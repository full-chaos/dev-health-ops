from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Sequence, Tuple

from dev_health_ops.metrics.schemas import CICDMetricsDailyRecord, PipelineRunRow


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


def compute_cicd_metrics_daily(
    *,
    day: date,
    pipeline_runs: Sequence[PipelineRunRow],
    computed_at: datetime,
) -> List[CICDMetricsDailyRecord]:
    """
    Compute per-repo CI/CD metrics for pipeline runs started on the given day.
    """
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    by_repo: Dict[str, Dict[str, object]] = {}
    for row in pipeline_runs:
        started_at = _to_utc(row["started_at"])
        if not (start <= started_at < end):
            continue
        repo_id = str(row["repo_id"])
        bucket = by_repo.get(repo_id)
        if bucket is None:
            bucket = {
                "pipelines": 0,
                "success": 0,
                "durations": [],
                "queues": [],
            }
            by_repo[repo_id] = bucket

        bucket["pipelines"] = int(bucket["pipelines"]) + 1
        status = (row.get("status") or "").strip().lower()
        if status in {"success", "succeeded", "passed"}:
            bucket["success"] = int(bucket["success"]) + 1

        finished_at = row.get("finished_at")
        if isinstance(finished_at, datetime):
            duration_min = (_to_utc(finished_at) - started_at).total_seconds() / 60.0
            if duration_min >= 0:
                bucket["durations"].append(float(duration_min))

        queued_at = row.get("queued_at")
        if isinstance(queued_at, datetime):
            queue_min = (started_at - _to_utc(queued_at)).total_seconds() / 60.0
            if queue_min >= 0:
                bucket["queues"].append(float(queue_min))

    records: List[CICDMetricsDailyRecord] = []
    for repo_id, bucket in sorted(by_repo.items(), key=lambda kv: kv[0]):
        pipelines = int(bucket["pipelines"])
        success = int(bucket["success"])
        durations: List[float] = list(bucket["durations"])
        queues: List[float] = list(bucket["queues"])
        success_rate = (success / pipelines) if pipelines else 0.0

        records.append(
            CICDMetricsDailyRecord(
                repo_id=uuid.UUID(repo_id),
                day=day,
                pipelines_count=pipelines,
                success_rate=float(success_rate),
                avg_duration_minutes=float(sum(durations) / len(durations))
                if durations
                else None,
                p90_duration_minutes=float(_percentile(durations, 90.0))
                if durations
                else None,
                avg_queue_minutes=float(sum(queues) / len(queues)) if queues else None,
                computed_at=computed_at_utc,
            )
        )

    return records
