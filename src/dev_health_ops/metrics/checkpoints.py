"""CRUD operations for MetricCheckpoint model.

This module provides synchronous data-access functions for managing metric computation
checkpoints. Checkpoints track the completion state of metric computations per
(org, repo, type, day) scope, enabling:

- Resume-on-failure: skip repos that already completed for a given day
- Distributed coordination: prevent duplicate computation across workers
- Backfill tracking: know exactly which (repo, day) pairs have been computed
- Crash recovery: reset stale RUNNING checkpoints back to PENDING

All functions use synchronous SQLAlchemy sessions (Session, not AsyncSession)
and are designed to run inside Celery tasks via get_postgres_session_sync().
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.orm import Session

from dev_health_ops.models.checkpoints import CheckpointStatus, MetricCheckpoint


def get_checkpoint(
    session: Session,
    org_id: str,
    repo_id: Optional[uuid.UUID],
    metric_type: str,
    day: datetime,
) -> Optional[MetricCheckpoint]:
    """Retrieve a checkpoint by its unique scope.

    Args:
        session: SQLAlchemy synchronous session
        org_id: Organization identifier
        repo_id: Repository identifier (may be None for finalize checkpoints)
        metric_type: Metric computation type (e.g., 'daily_batch', 'daily_finalize')
        day: Target date (timezone-aware datetime)

    Returns:
        MetricCheckpoint if found, None otherwise
    """
    return (
        session.query(MetricCheckpoint)
        .filter(
            MetricCheckpoint.org_id == org_id,
            MetricCheckpoint.repo_id == repo_id,
            MetricCheckpoint.metric_type == metric_type,
            MetricCheckpoint.day == day,
        )
        .first()
    )


def mark_running(
    session: Session,
    org_id: str,
    repo_id: Optional[uuid.UUID],
    metric_type: str,
    day: datetime,
    worker_id: str,
) -> MetricCheckpoint:
    """Create or update a checkpoint to RUNNING status.

    Upsert pattern: creates a new checkpoint if it doesn't exist, or updates
    an existing one to RUNNING status with the given worker_id and started_at.

    Args:
        session: SQLAlchemy synchronous session
        org_id: Organization identifier
        repo_id: Repository identifier (may be None for finalize checkpoints)
        metric_type: Metric computation type
        day: Target date (timezone-aware datetime)
        worker_id: Celery task ID for distributed locking

    Returns:
        Updated or created MetricCheckpoint with status=RUNNING
    """
    checkpoint = get_checkpoint(session, org_id, repo_id, metric_type, day)

    if checkpoint is None:
        checkpoint = MetricCheckpoint(
            org_id=org_id,
            repo_id=repo_id,
            metric_type=metric_type,
            day=day,
            status=CheckpointStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
            worker_id=worker_id,
        )
        session.add(checkpoint)
    else:
        checkpoint.status = CheckpointStatus.RUNNING
        checkpoint.started_at = datetime.now(timezone.utc)
        checkpoint.worker_id = worker_id

    session.flush()
    return checkpoint


def mark_completed(session: Session, checkpoint_id: uuid.UUID) -> None:
    """Mark a checkpoint as COMPLETED.

    Args:
        session: SQLAlchemy synchronous session
        checkpoint_id: UUID of the checkpoint to update

    Raises:
        ValueError: If checkpoint not found
    """
    checkpoint = (
        session.query(MetricCheckpoint)
        .filter(MetricCheckpoint.id == checkpoint_id)
        .first()
    )

    if checkpoint is None:
        raise ValueError(f"Checkpoint {checkpoint_id} not found")

    checkpoint.status = CheckpointStatus.COMPLETED
    checkpoint.completed_at = datetime.now(timezone.utc)
    session.flush()


def mark_failed(
    session: Session,
    checkpoint_id: uuid.UUID,
    error: str,
) -> None:
    """Mark a checkpoint as FAILED with error message.

    Args:
        session: SQLAlchemy synchronous session
        checkpoint_id: UUID of the checkpoint to update
        error: Error message describing the failure

    Raises:
        ValueError: If checkpoint not found
    """
    checkpoint = (
        session.query(MetricCheckpoint)
        .filter(MetricCheckpoint.id == checkpoint_id)
        .first()
    )

    if checkpoint is None:
        raise ValueError(f"Checkpoint {checkpoint_id} not found")

    checkpoint.status = CheckpointStatus.FAILED
    checkpoint.error = error
    session.flush()


def is_completed(
    session: Session,
    org_id: str,
    repo_id: Optional[uuid.UUID],
    metric_type: str,
    day: datetime,
) -> bool:
    """Check if a checkpoint has completed successfully.

    Args:
        session: SQLAlchemy synchronous session
        org_id: Organization identifier
        repo_id: Repository identifier (may be None for finalize checkpoints)
        metric_type: Metric computation type
        day: Target date (timezone-aware datetime)

    Returns:
        True if checkpoint exists and status is COMPLETED, False otherwise
    """
    checkpoint = get_checkpoint(session, org_id, repo_id, metric_type, day)
    return checkpoint is not None and checkpoint.status == CheckpointStatus.COMPLETED


def get_incomplete_repos(
    session: Session,
    org_id: str,
    metric_type: str,
    day: datetime,
    all_repo_ids: list[uuid.UUID],
) -> list[uuid.UUID]:
    """Get repo IDs that have not yet completed for the given scope.

    Returns the subset of all_repo_ids where either:
    - No checkpoint exists for (org_id, repo_id, metric_type, day)
    - A checkpoint exists but status != COMPLETED

    Args:
        session: SQLAlchemy synchronous session
        org_id: Organization identifier
        metric_type: Metric computation type
        day: Target date (timezone-aware datetime)
        all_repo_ids: List of all repo IDs to check

    Returns:
        List of repo IDs that are incomplete (not yet COMPLETED)
    """
    completed_repos = (
        session.query(MetricCheckpoint.repo_id)
        .filter(
            MetricCheckpoint.org_id == org_id,
            MetricCheckpoint.metric_type == metric_type,
            MetricCheckpoint.day == day,
            MetricCheckpoint.status == CheckpointStatus.COMPLETED,
        )
        .all()
    )

    completed_set = {row[0] for row in completed_repos}
    return [repo_id for repo_id in all_repo_ids if repo_id not in completed_set]


def reset_stale_running(
    session: Session,
    stale_threshold_minutes: int = 60,
) -> int:
    """Reset RUNNING checkpoints older than threshold back to PENDING.

    Used for crash recovery: if a worker dies while processing a checkpoint,
    this function resets it so another worker can retry.

    Args:
        session: SQLAlchemy synchronous session
        stale_threshold_minutes: Age threshold in minutes (default 60)

    Returns:
        Number of checkpoints reset
    """
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=stale_threshold_minutes)

    stale_checkpoints = (
        session.query(MetricCheckpoint)
        .filter(
            MetricCheckpoint.status == CheckpointStatus.RUNNING,
            MetricCheckpoint.started_at < stale_cutoff,
        )
        .all()
    )

    count = len(stale_checkpoints)
    for checkpoint in stale_checkpoints:
        checkpoint.status = CheckpointStatus.PENDING
        checkpoint.started_at = None
        checkpoint.worker_id = None

    session.flush()
    return count
