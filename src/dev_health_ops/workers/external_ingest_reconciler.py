"""Retention pruning for the external-ingest status store (CHAOS-2694).

Deletes ``external_ingest_batches`` rows (and cascade-deletes their
``external_ingest_rejections`` via ``ON DELETE CASCADE``) older than the
retention window. Beat-scheduled (``workers/config.py``), env-tunable via
``EXTERNAL_INGEST_STATUS_RETENTION_DAYS`` (default 90 -- this is
customer-support/audit-facing operational history, closer in spirit to audit
logs than to the 14-day ``provider_rate_limit_observations`` telemetry
precedent it otherwise mirrors).

Retention-only: this task never re-enqueues or resurrects stuck batches (a
batch left in ``accepted``/``processing``/``stream_unavailable`` past
retention is a bug signal that should stay visible, never silently pruned --
see the ``status IN (...)`` guard below; this is a deliberate, pinned
master-spec decision -- CC13 -- not an oversight, and a defense-in-depth
reconciler for never-resubmitted stale batches is a separate, filed
follow-up issue). Deletes in bounded chunks, committing each one, so a large
backlog (e.g. the first run after months of accumulation) never holds one
huge long-running transaction (adversarial-review finding).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

_DEFAULT_RETENTION_DAYS = 90
_DELETE_CHUNK_SIZE = 500

_DELETE_CHUNK_SQL = text(
    """
    DELETE FROM external_ingest_batches WHERE ingestion_id IN (
        SELECT ingestion_id FROM external_ingest_batches
        WHERE created_at < :cutoff AND status IN ('completed', 'partial', 'failed')
        LIMIT :chunk_size
    )
    """
)


def _retention_days() -> int:
    raw = os.getenv("EXTERNAL_INGEST_STATUS_RETENTION_DAYS")
    if not raw:
        return _DEFAULT_RETENTION_DAYS
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_RETENTION_DAYS


@celery_app.task(
    queue="sync",
    name="dev_health_ops.workers.tasks.prune_external_ingest_batches",
)
def prune_external_ingest_batches(retention_days: int | None = None) -> dict[str, Any]:
    from dev_health_ops.db import get_postgres_session_sync

    days = retention_days if retention_days is not None else _retention_days()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(days)))
    deleted = 0
    with get_postgres_session_sync() as session:
        while True:
            result: Any = session.execute(
                _DELETE_CHUNK_SQL,
                {"cutoff": cutoff, "chunk_size": _DELETE_CHUNK_SIZE},
            )
            chunk_deleted = int(getattr(result, "rowcount", 0) or 0)
            deleted += chunk_deleted
            session.commit()
            if chunk_deleted < _DELETE_CHUNK_SIZE:
                break
    logger.info(
        "prune_external_ingest_batches.completed",
        extra={"deleted": deleted, "retention_days": days},
    )
    return {"status": "completed", "deleted": deleted, "retention_days": days}


__all__ = ["prune_external_ingest_batches"]
