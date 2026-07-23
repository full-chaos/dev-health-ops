"""Deterministic River fan-out plan for one authoritative terminal SyncRun.

This module deliberately contains no Celery calls.  The post-sync River
consumer first rebuilds :class:`PostSyncDispatchPayload` from PostgreSQL, then
uses this plan to create the downstream domain requests and worker-job outbox
rows in one transaction.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.post_sync_dispatch import (
    _DORA_TARGETS,
    _GIT_TARGETS,
    _WORK_ITEM_TARGETS,
    PostSyncDispatchPayload,
)

_FANOUT_NAMESPACE = uuid.UUID("0713fbcf-ec5c-49dc-b7dc-18ae3de17536")


@dataclass(frozen=True, slots=True)
class RiverPostSyncTarget:
    """One durable downstream request to stage through ``worker_job_outbox``."""

    consumer: str
    job_kind: str
    domain_type: str
    domain_id: uuid.UUID
    idempotency_key: str
    generation: str


def build_river_post_sync_plan(
    sync_run_id: str | uuid.UUID,
    payload: PostSyncDispatchPayload,
    *,
    today: date | None = None,
) -> tuple[RiverPostSyncTarget, ...]:
    """Return the complete, duplicate-stable downstream fan-out.

    Transport generation is intentionally absent from every identity. A stale
    River delivery is fenced before planning, while a retry or a Celery/River
    cutover for the same SyncRun must converge on the same downstream requests.
    """

    run_id = uuid.UUID(str(sync_run_id))
    current_day = today or utc_today()
    target_set = set(payload.sync_targets)
    has_git = bool(target_set & _GIT_TARGETS)
    has_work_items = bool(target_set & _WORK_ITEM_TARGETS)
    has_dora = bool(target_set & _DORA_TARGETS)
    generation = f"post-sync:{run_id}"

    consumers: list[tuple[str, str, str]] = []
    if has_git and _is_current_single_day(payload, today=current_day):
        consumers.append(
            ("complexity", "metrics.remaining.complexity", "remaining_metric_run")
        )
    if has_git or has_work_items:
        consumers.extend(
            (
                ("daily", "metrics.daily_dispatch", "daily_metrics_run"),
                ("workgraph", "workgraph.build", "work_graph_request"),
                ("investment", "investment.dispatch", "investment_request"),
            )
        )
    if has_git or has_dora:
        consumers.append(("dora", "metrics.remaining.dora", "remaining_metric_run"))
    if payload.auto_import_teams:
        consumers.append(("team_autoimport", "sync.team_autoimport", "sync_run"))

    return tuple(
        RiverPostSyncTarget(
            consumer=consumer,
            job_kind=job_kind,
            domain_type=domain_type,
            domain_id=uuid.uuid5(_FANOUT_NAMESPACE, f"{run_id}:{consumer}"),
            idempotency_key=f"{generation}:{job_kind}",
            generation=generation,
        )
        for consumer, job_kind, domain_type in consumers
    )


def _is_current_single_day(
    payload: PostSyncDispatchPayload,
    *,
    today: date,
) -> bool:
    """Mirror the legacy guard against fabricated historical complexity."""

    if payload.from_date is None and payload.to_date is None:
        return True
    if payload.from_date is None or payload.to_date is None:
        return False
    window_start = date.fromisoformat(payload.from_date)
    window_end = date.fromisoformat(payload.to_date)
    return window_start == window_end == today
