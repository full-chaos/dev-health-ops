"""Debounced recompute flush task (CHAOS-2699).

# INTEGRATOR TODO (CHAOS-2693, master-spec CC20 -- Celery wiring has ONE
# owner this wave): this task is intentionally NOT re-exported from
# workers/tasks.py and NOT added to workers/config.py's
# late_ack_excluded_tasks (both are hot files owned by CHAOS-2693 this
# wave). At merge time, add exactly this one line to
# workers/config.py's `late_ack_excluded_tasks` tuple (queue="default" is
# already declared on the task decorator below -- no task_queues/compose
# change needed, it reuses the existing `default` queue's worker coverage):
#
#     "dev_health_ops.workers.tasks.flush_external_ingest_recompute",
#
# No workers/tasks.py re-export needed -- the task name is pinned via the
# `name=` kwarg on the decorator below (flat-namespace convention achieved
# without the re-export line, per the synthesizer reconciliation on
# brief-2699-recompute.md).

Reads+clears the Valkey pending-scope blob that
``external_ingest.recompute.schedule_or_coalesce`` accumulated for
``(org_id, source_system, source_instance)``, plans + dispatches the
bounded recompute, and persists the outcome onto every coalesced
ingestion's ``external_ingest_batches`` row (D11/D12).
"""

from __future__ import annotations

import json
import logging

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


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


__all__ = ["flush_external_ingest_recompute"]
