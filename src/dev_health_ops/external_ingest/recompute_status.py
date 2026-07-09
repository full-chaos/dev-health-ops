"""Direct-SQL persistence for bounded-recompute status (CHAOS-2699, D11/D12).

Mirrors ``api/external_ingest/status.py``'s convention exactly: all
# nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
reads/writes go through ``session.execute(text(...), params)``, never
``session.add()``/ORM query paths. SQL is dialect-portable (no
``RETURNING``/``ON CONFLICT``/``ANY(:array)``) -- per-ingestion_id UPDATEs
in a loop instead of a single ``WHERE ingestion_id = ANY(:ids)`` statement,
so unit tests run on sqlite via ``Base.metadata`` (no new ``postgres``
pytest marker epic-wide, per the synthesizer reconciliation).

Two entry points, two session flavors -- deliberate, not an oversight:

- :func:`record_recompute_dispatch` takes a **sync** ``Session``. It is
  called from ``workers/external_ingest_recompute.py``'s Celery flush task,
  which (like every other Celery task in this codebase, e.g.
  ``prune_external_ingest_batches``) runs synchronously via
  ``get_postgres_session_sync()`` -- no ``run_async`` bridging needed.
- :func:`get_recompute_jobs` and :func:`mark_recompute_pending` take an
  **async** ``AsyncSession``, matching ``api/external_ingest/status.py``'s
  FastAPI request-scoped session (``get_postgres_session_dep``).
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dev_health_ops.external_ingest.recompute import (
    RecomputeDispatchResult,
    RecomputeScope,
)

logger = logging.getLogger(__name__)

__all__ = [
    "RecomputeJobRow",
    "get_recompute_jobs",
    "mark_recompute_pending",
    "record_recompute_dispatch",
]

_BATCHES_TABLE = "external_ingest_batches"
_JOBS_TABLE = "external_ingest_recompute_jobs"


def _scope_to_json(
    scope: RecomputeScope, result: RecomputeDispatchResult
) -> dict[str, Any]:
    return {
        "repoIds": sorted(scope.repo_ids),
        "teamIds": sorted(scope.team_ids),
        "recordKinds": sorted(scope.record_kinds),
        "windowStartedAt": (
            scope.window_start.isoformat() if scope.window_start is not None else None
        ),
        "windowEndedAt": (
            scope.window_end.isoformat() if scope.window_end is not None else None
        ),
        "cappedDays": result.capped_days,
        "cappedRepos": result.capped_repos,
    }


def record_recompute_dispatch(
    session: Session,
    *,
    org_id: str,
    ingestion_ids: list[str],
    scope: RecomputeScope,
    result: RecomputeDispatchResult,
) -> None:
    """Persist a flush's outcome onto every coalesced ingestion's batch row
    plus one job-log row per dispatched Celery task (D11).

    ``ingestion_ids`` is the FULL set that fed into the coalesced scope
    (Risk 5 in the brief: debounce coalesces across multiple ingestion_ids,
    so a flush's status write must cover all of them, not just one) --
    every row gets the identical ``scope``/``result`` snapshot, since they
    were all folded into the same bounded plan.

    D12 (emit-then-raise): commits before returning so a caller that goes
    on to re-raise (e.g. Celery retry on an unrelated later step) never
    rolls back an already-decided recompute outcome.
    """
    now = datetime.now(timezone.utc)
    scope_json = json.dumps(_scope_to_json(scope, result))
    dispatched_at = now if result.status == "dispatched" else None

    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    update_sql = text(
        f"""
        UPDATE {_BATCHES_TABLE}
        SET recompute_status = :status,
            recompute_scope = :scope,
            recompute_dispatched_at = :dispatched_at,
            recompute_completed_at = :completed_at,
            recompute_error = :error
        WHERE org_id = :org_id AND ingestion_id = :ingestion_id
        """
    )
    for ingestion_id in ingestion_ids:
        session.execute(
            update_sql,
            {
                "status": result.status,
                "scope": scope_json,
                "dispatched_at": dispatched_at,
                "completed_at": now,
                "error": result.error,
                "org_id": org_id,
                "ingestion_id": ingestion_id,
            },
        )

    if result.jobs:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        insert_sql = text(
            f"""
            INSERT INTO {_JOBS_TABLE} (
                id, org_id, source_system, source_instance, celery_task_name,
                celery_task_id, queue, repo_id, status, dispatched_at
            ) VALUES (
                :id, :org_id, :source_system, :source_instance, :celery_task_name,
                :celery_task_id, :queue, :repo_id, :status, :dispatched_at
            )
            """
        )
        for job in result.jobs:
            session.execute(
                insert_sql,
                {
                    "id": str(uuid.uuid4()),
                    "org_id": org_id,
                    "source_system": scope.source_system,
                    "source_instance": scope.source_instance,
                    "celery_task_name": job.task,
                    "celery_task_id": job.task_id,
                    "queue": job.queue,
                    "repo_id": job.repo_id,
                    "status": "dispatched",
                    "dispatched_at": now,
                },
            )

    session.commit()


async def mark_recompute_pending(
    session: AsyncSession, *, org_id: str, ingestion_id: uuid.UUID | str
) -> None:
    """``not_applicable -> pending``, best-effort.

    Optional seam for whoever finalizes a batch's own status (CHAOS-2697's
    worker, per D12 "in the same worker task as the final ingestion-status
    update") to call right after :func:`~dev_health_ops.external_ingest.
    recompute.schedule_or_coalesce` returns, so the batch shows ``pending``
    during its debounce window instead of the default ``not_applicable``
    until the flush actually fires. Deliberately NOT called from
    ``schedule_or_coalesce`` itself, whose public signature is
    primitives-only (no ``session`` parameter, per the synthesizer
    reconciliation) -- callers that skip this simply see ``not_applicable``
    until the flush completes, which is a display nuance only (the flush's
    own :func:`record_recompute_dispatch` write is unaffected either way).
    A no-op (never raises) if the batch is not currently
    ``not_applicable`` (e.g. a redelivered call after the flush already
    completed) so it never regresses a terminal recompute outcome.
    """
    try:
        await session.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(
                f"""
                UPDATE {_BATCHES_TABLE}
                SET recompute_status = 'pending'
                WHERE org_id = :org_id AND ingestion_id = :ingestion_id
                    AND recompute_status = 'not_applicable'
                """
            ),
            {"org_id": org_id, "ingestion_id": str(ingestion_id)},
        )
    except Exception:
        logger.exception(
            "external_ingest.recompute.mark_pending_failed org_id=%s ingestion_id=%s",
            org_id,
            ingestion_id,
        )


@dataclass(frozen=True)
class RecomputeJobRow:
    task: str
    task_id: str | None
    queue: str
    repo_id: str | None


async def get_recompute_jobs(
    session: AsyncSession,
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    dispatched_at: datetime | None,
) -> list[RecomputeJobRow]:
    """Jobs from the SAME flush that produced ``dispatched_at`` on the
    batch row -- all job rows a single flush inserts share the identical
    ``dispatched_at`` timestamp (see :func:`record_recompute_dispatch`),
    which is what makes this exact-match join possible without a shared
    ingestion_id (a flush coalesces N ingestion_ids, so no per-job FK to
    ``external_ingest_batches`` exists -- see
    ``ExternalIngestRecomputeJob``'s docstring)."""
    if dispatched_at is None:
        return []
    result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"""
            SELECT celery_task_name, celery_task_id, queue, repo_id
            FROM {_JOBS_TABLE}
            WHERE org_id = :org_id AND source_system = :source_system
                AND source_instance = :source_instance AND dispatched_at = :dispatched_at
            ORDER BY celery_task_name, repo_id
            """
        ),
        {
            "org_id": org_id,
            "source_system": source_system,
            "source_instance": source_instance,
            "dispatched_at": dispatched_at,
        },
    )
    return [
        RecomputeJobRow(
            task=row["celery_task_name"],
            task_id=row["celery_task_id"],
            queue=row["queue"],
            repo_id=row["repo_id"],
        )
        for row in result.mappings().all()
    ]
