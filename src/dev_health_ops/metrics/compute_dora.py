"""Provider-agnostic DORA metric computation from synced ClickHouse rows.

The four DORA metrics are derived from data both the GitHub and GitLab
processors already write to ClickHouse (``deployments`` and ``incidents``) —
no live provider API fetch is required (CHAOS-2382). Output rows match the
``dora_metrics_daily`` contract: one row per ``(repo, metric_name, day)``.

Units mirror the GitLab DORA API so values stay comparable across providers:

* ``deployment_frequency``     — deployment count for the day
* ``lead_time_for_changes``    — median (deployed_at - merged_at) in seconds
* ``time_to_restore_service``  — median MTTR (resolved_at - started_at) in seconds
* ``change_failure_rate``      — failed deployments / total deployments (0..1)
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import TypedDict

from dev_health_ops.metrics.compute_deployments import DEPLOYMENT_FAILURE_STATUSES
from dev_health_ops.metrics.schemas import (
    DeploymentRow,
    DORAMetricsRecord,
    IncidentRow,
)
from dev_health_ops.utils.datetime import to_utc

# Deployment statuses that count as a failed change for change_failure_rate.
# Single source of truth lives in ``compute_deployments`` so the daily deploy
# metrics, DORA change-failure-rate, and the ClickHouse deployment_daily_rollup
# MV all classify failures identically across providers (CHAOS-2382 / 2395).
_FAILED_STATUSES = DEPLOYMENT_FAILURE_STATUSES


class _DeployBucket(TypedDict):
    deployments: int
    failed: int
    lead_times_seconds: list[float]


class _IncidentBucket(TypedDict):
    mttr_seconds: list[float]


def _utc_day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _median(values: Sequence[float]) -> float:
    sorted_vals = sorted(float(v) for v in values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 1:
        return float(sorted_vals[mid])
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def compute_dora_metrics_daily(
    *,
    day: date,
    deployments: Sequence[DeploymentRow],
    incidents: Sequence[IncidentRow],
    computed_at: datetime,
) -> list[DORAMetricsRecord]:
    """Compute the four DORA metrics per repo for ``day``.

    Returns one ``DORAMetricsRecord`` per (repo, metric) that has data, so a
    repo with deployments but no incidents still gets deployment_frequency /
    lead_time / change_failure_rate rows.
    """
    start, end = _utc_day_window(day)
    computed_at_utc = to_utc(computed_at)

    deploy_by_repo: dict[str, _DeployBucket] = {}
    for deploy_row in deployments:
        deployed_at = deploy_row.get("deployed_at") or deploy_row.get("started_at")
        if not isinstance(deployed_at, datetime):
            continue
        deployed_at = to_utc(deployed_at)
        if not (start <= deployed_at < end):
            continue

        repo_id = str(deploy_row["repo_id"])
        bucket = deploy_by_repo.get(repo_id)
        if bucket is None:
            bucket = {"deployments": 0, "failed": 0, "lead_times_seconds": []}
            deploy_by_repo[repo_id] = bucket

        bucket["deployments"] += 1
        status = (deploy_row.get("status") or "").strip().lower()
        if status in _FAILED_STATUSES:
            bucket["failed"] += 1

        merged_at = deploy_row.get("merged_at")
        if isinstance(merged_at, datetime):
            lead_seconds = (deployed_at - to_utc(merged_at)).total_seconds()
            if lead_seconds >= 0:
                bucket["lead_times_seconds"].append(float(lead_seconds))

    incident_by_repo: dict[str, _IncidentBucket] = {}
    for incident_row in incidents:
        resolved_at = incident_row.get("resolved_at")
        if not isinstance(resolved_at, datetime):
            continue
        resolved_at = to_utc(resolved_at)
        if not (start <= resolved_at < end):
            continue

        started_raw = incident_row.get("started_at")
        if not isinstance(started_raw, datetime):
            continue
        mttr_seconds = (resolved_at - to_utc(started_raw)).total_seconds()
        if mttr_seconds < 0:
            continue

        repo_id = str(incident_row["repo_id"])
        ibucket = incident_by_repo.get(repo_id)
        if ibucket is None:
            ibucket = {"mttr_seconds": []}
            incident_by_repo[repo_id] = ibucket
        ibucket["mttr_seconds"].append(float(mttr_seconds))

    records: list[DORAMetricsRecord] = []

    def _emit(repo_id: str, metric_name: str, value: float) -> None:
        records.append(
            DORAMetricsRecord(
                repo_id=uuid.UUID(repo_id),
                day=day,
                metric_name=metric_name,
                value=float(value),
                computed_at=computed_at_utc,
            )
        )

    for repo_id, bucket in sorted(deploy_by_repo.items(), key=lambda kv: kv[0]):
        total = int(bucket["deployments"])
        _emit(repo_id, "deployment_frequency", float(total))
        if total > 0:
            _emit(
                repo_id,
                "change_failure_rate",
                float(bucket["failed"]) / float(total),
            )
        lead_times = bucket["lead_times_seconds"]
        if lead_times:
            _emit(repo_id, "lead_time_for_changes", _median(lead_times))

    for repo_id, ibucket in sorted(incident_by_repo.items(), key=lambda kv: kv[0]):
        mttr = ibucket["mttr_seconds"]
        if mttr:
            _emit(repo_id, "time_to_restore_service", _median(mttr))

    return records
