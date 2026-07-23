"""Debounced recompute flush tasks (CHAOS-2699 / CHAOS-3043).

Reads+clears the Valkey pending-scope blob that
``external_ingest.recompute.schedule_or_coalesce`` accumulated for
``(org_id, source_system, source_instance)``, plans + dispatches the
bounded recompute, and persists the outcome onto every coalesced
ingestion's ``external_ingest_batches`` row (D11/D12).

The compatibility task drains only the fixed, typed bridge identity emitted
by the dormant Go external-ingest runner, then invokes this same Python
planner and dispatcher. It cannot select arbitrary Celery task names.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

GO_COMPATIBILITY_BRIDGE_KIND = "external_ingest.recompute.compat.v1"
GO_COMPATIBILITY_TASK_NAME = (
    "dev_health_ops.workers.tasks.dispatch_external_ingest_recompute_bridge"
)
_GO_BRIDGE_CLAIM_TTL = timedelta(minutes=5)
_GO_BRIDGE_BATCH_LIMIT = 50


@dataclass(frozen=True)
class _GoBridgeClaim:
    job_id: str
    bridge_id: str
    org_id: str
    source_system: str
    source_instance: str


def _claim_go_compatibility_bridges(*, limit: int) -> list[_GoBridgeClaim]:
    """Claim only the fixed Go-to-Python recompute bridge identity.

    The bridge rows live in the existing recompute job ledger and use a
    deterministic primary key. ``FOR UPDATE SKIP LOCKED`` prevents concurrent
    default-queue workers from dispatching the same bridge. A stale claim is
    eligible again, covering worker death before the Python planner runs.
    """
    from dev_health_ops.db import get_postgres_session_sync

    now = datetime.now(timezone.utc)
    stale_before = now - _GO_BRIDGE_CLAIM_TTL
    with get_postgres_session_sync() as session:
        rows = (
            session.execute(
                text(
                    """
                    SELECT id, celery_task_id, org_id, source_system, source_instance
                    FROM external_ingest_recompute_jobs
                    WHERE celery_task_name = :task_name
                      AND (
                        status = 'bridge_pending'
                        OR (status = 'bridge_claimed' AND dispatched_at < :stale_before)
                      )
                    ORDER BY dispatched_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                    """
                ),
                {
                    "task_name": GO_COMPATIBILITY_TASK_NAME,
                    "stale_before": stale_before,
                    "limit": max(1, min(limit, _GO_BRIDGE_BATCH_LIMIT)),
                },
            )
            .mappings()
            .all()
        )
        claims = [
            _GoBridgeClaim(
                job_id=str(row["id"]),
                bridge_id=str(row["celery_task_id"]),
                org_id=str(row["org_id"]),
                source_system=str(row["source_system"]),
                source_instance=str(row["source_instance"]),
            )
            for row in rows
        ]
        for claim in claims:
            session.execute(
                text(
                    """
                    UPDATE external_ingest_recompute_jobs
                    SET status = 'bridge_claimed', dispatched_at = :claimed_at
                    WHERE id = :job_id AND celery_task_name = :task_name
                    """
                ),
                {
                    "claimed_at": now,
                    "job_id": claim.job_id,
                    "task_name": GO_COMPATIBILITY_TASK_NAME,
                },
            )
        session.commit()
    return claims


def _load_go_bridge_scope(claim: _GoBridgeClaim) -> dict[str, Any] | None:
    """Load and validate the allowlisted bridge payload from batch status rows."""
    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        rows = (
            session.execute(
                text(
                    """
                    SELECT ingestion_id, recompute_scope
                    FROM external_ingest_batches
                    WHERE org_id = :org_id
                      AND source_system = :source_system
                      AND source_instance = :source_instance
                      AND recompute_status = 'pending'
                    ORDER BY ingestion_id
                    """
                ),
                {
                    "org_id": claim.org_id,
                    "source_system": claim.source_system,
                    "source_instance": claim.source_instance,
                },
            )
            .mappings()
            .all()
        )
    ingestion_ids: list[str] = []
    payload: dict[str, Any] | None = None
    for row in rows:
        candidate = row["recompute_scope"]
        if isinstance(candidate, str):
            candidate = json.loads(candidate)
        if (
            not isinstance(candidate, dict)
            or candidate.get("bridgeId") != claim.bridge_id
        ):
            continue
        if (
            candidate.get("bridgeVersion") != 1
            or candidate.get("bridgeKind") != GO_COMPATIBILITY_BRIDGE_KIND
        ):
            raise ValueError("unsupported Go external recompute bridge payload")
        comparable = {
            key: candidate.get(key)
            for key in (
                "bridgeVersion",
                "bridgeKind",
                "bridgeId",
                "repoIds",
                "teamIds",
                "recordKinds",
                "windowStartedAt",
                "windowEndedAt",
            )
        }
        if payload is not None and payload != comparable:
            raise ValueError("inconsistent Go external recompute bridge scope")
        payload = comparable
        ingestion_ids.append(str(row["ingestion_id"]))
    if payload is None:
        return None
    payload["ingestionIds"] = sorted(set(ingestion_ids))
    return payload


def _mark_go_bridge(job_id: str, status: str) -> None:
    from dev_health_ops.db import get_postgres_session_sync

    with get_postgres_session_sync() as session:
        session.execute(
            text(
                """
                UPDATE external_ingest_recompute_jobs
                SET status = :status
                WHERE id = :job_id AND celery_task_name = :task_name
                """
            ),
            {
                "status": status,
                "job_id": job_id,
                "task_name": GO_COMPATIBILITY_TASK_NAME,
            },
        )
        session.commit()


def _dispatch_go_bridge_claim(claim: _GoBridgeClaim) -> str:
    """Invoke the current Python planner/dispatcher for one typed Go bridge."""
    from dev_health_ops.external_ingest.recompute import (
        _parse_iso,
        dispatch_and_persist_scope,
    )

    payload = _load_go_bridge_scope(claim)
    if payload is None:
        # Crash-after-dispatch/before-bridge-mark: batch outcomes are already
        # non-pending, so a stale claim is completed without re-emitting jobs.
        _mark_go_bridge(claim.job_id, "bridge_dispatched")
        return "already_terminal"
    result = dispatch_and_persist_scope(
        org_id=claim.org_id,
        source_system=claim.source_system,
        source_instance=claim.source_instance,
        ingestion_ids=payload["ingestionIds"],
        repo_ids=payload.get("repoIds") or [],
        team_ids=payload.get("teamIds") or [],
        record_kinds=payload.get("recordKinds") or [],
        window_start=_parse_iso(payload.get("windowStartedAt")),
        window_end=_parse_iso(payload.get("windowEndedAt")),
    )
    _mark_go_bridge(
        claim.job_id,
        "bridge_failed" if result.status == "failed" else "bridge_dispatched",
    )
    return result.status


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="default",
    name="dev_health_ops.workers.tasks.flush_external_ingest_recompute",
)
def flush_external_ingest_recompute(
    self,
    org_id: str,
    source_system: str,
    source_instance: str,
) -> dict:
    from dev_health_ops.external_ingest.recompute import (
        _get_redis_client,
        _parse_iso,
        _pending_key,
        dispatch_and_persist_scope,
    )

    pending_key = _pending_key(org_id, source_system, source_instance)

    try:
        client = _get_redis_client()
        # Adversarial-review finding: a separate GET then DELETE left a
        # window where a NEW schedule_or_coalesce() call (guard already
        # expired) could write a fresher blob between this task's GET and
        # DELETE -- this task would then delete that fresher blob without
        # ever having read it, silently dropping the later batch's scope.
        # GETDEL is atomic: whatever this task reads is exactly what it
        # clears, with no gap for a third party to land in between.
        #
        # The guard key is deliberately left untouched here (no explicit
        # DELETE): its own `ex=seconds` TTL governs its lifecycle, so a
        # schedule_or_coalesce() call that lands before it naturally
        # expires correctly coalesces into the *next* debounce window
        # instead of racing this flush's cleanup.
        raw = client.getdel(pending_key) if client is not None else None
    except Exception as exc:
        logger.exception(
            "flush_external_ingest_recompute.valkey_read_failed org_id=%s "
            "source_system=%s source_instance=%s",
            org_id,
            source_system,
            source_instance,
        )
        raise self.retry(exc=exc, countdown=30 * (2**self.request.retries))

    if not raw:
        # Risk 4 (brief): the pending blob can be lost to Valkey eviction
        # between the guard SETNX and this flush firing. Tolerate a
        # missing/empty blob gracefully -- log + no-op -- rather than
        # raising (D13: recompute dispatch problems must never propagate
        # as task failures that could retry-loop or page anyone).
        logger.info(
            "flush_external_ingest_recompute.empty_blob org_id=%s "
            "source_system=%s source_instance=%s",
            org_id,
            source_system,
            source_instance,
        )
        return {"status": "no_pending_scope"}

    blob = json.loads(raw)
    result = dispatch_and_persist_scope(
        org_id=blob.get("org_id", org_id),
        source_system=blob.get("source_system", source_system),
        source_instance=blob.get("source_instance", source_instance),
        ingestion_ids=blob.get("ingestion_ids", []),
        repo_ids=blob.get("repo_ids", []),
        team_ids=blob.get("team_ids", []),
        record_kinds=blob.get("record_kinds", []),
        window_start=_parse_iso(blob.get("window_start")),
        window_end=_parse_iso(blob.get("window_end")),
    )
    return {
        "status": result.status,
        "jobs": len(result.jobs),
        "capped_days": result.capped_days,
        "capped_repos": result.capped_repos,
        "ingestion_ids": blob.get("ingestion_ids", []),
    }


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="default",
    name=GO_COMPATIBILITY_TASK_NAME,
)
def dispatch_external_ingest_recompute_bridge(
    self,
    limit: int = _GO_BRIDGE_BATCH_LIMIT,
) -> dict[str, Any]:
    """Drain deterministic Go bridge rows into the current Python planner."""
    try:
        claims = _claim_go_compatibility_bridges(limit=limit)
    except Exception as exc:
        logger.exception("external_ingest.recompute.go_bridge_claim_failed")
        raise self.retry(exc=exc, countdown=30 * (2**self.request.retries))

    outcomes: dict[str, int] = {}
    for claim in claims:
        try:
            status = _dispatch_go_bridge_claim(claim)
        except Exception:
            # Leave bridge_claimed in place. Its bounded stale lease makes it
            # retryable without failing or replaying the already-completed
            # external-ingest batch itself.
            logger.exception(
                "external_ingest.recompute.go_bridge_dispatch_failed "
                "bridge_id=%s org_id=%s",
                claim.bridge_id,
                claim.org_id,
            )
            status = "retryable_failure"
        outcomes[status] = outcomes.get(status, 0) + 1
    return {"claimed": len(claims), "outcomes": outcomes}


__all__ = [
    "dispatch_external_ingest_recompute_bridge",
    "flush_external_ingest_recompute",
]
