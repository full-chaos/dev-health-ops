"""Sync watermark read/write helpers (CHAOS-2571, CHAOS-2572, CHAOS-2578).

Canonical watermark identity
-----------------------------
The canonical key is ``(org_id, source.external_id, dataset_key)``.  This
matches the ``uq_sync_watermark_org_source_dataset`` unique constraint on
``SyncWatermark`` and the key used by the planner when it calls
:func:`get_watermark`.

Legacy ``target`` bridge (D4 — reverse-map reads, raw-row writes)
------------------------------------------------------------------
The legacy ``SyncConfiguration.sync_targets`` vocabulary uses coarse target
strings (``"git"``, ``"prs"``, …) that map to one or more ``dataset_key``
values.  A single raw legacy row ``(target='git', dataset_key='git')`` acts
as the D4 compatibility bridge for ALL of git's incremental datasets
(``commits``, ``commit-stats``, ``files``) until each gets its own canonical
row written by the planner.

Read path (:func:`get_watermark`):
  1. Canonical lookup by ``(org_id, source_id, dataset_key)`` — exact rows
     always win.
  2. Legacy-target lookup by ``(org_id, repo_id=source_id, target=dataset_key)``
     — covers rows where ``target == dataset_key`` (old runtime style).
  3. Reverse-legacy fallback — for a canonical ``dataset_key`` that has no
     exact row, reverse-map it to its legacy target(s) and look up the raw
     legacy row ``(target=legacy_target, dataset_key=legacy_target)``.  This
     lets ``get_watermark(..., 'commits')`` warm from a raw ``target='git'``
     row without collapsing git→commits.  Only INCREMENTAL datasets
     participate; ``repo-metadata`` (WatermarkBehavior.NONE) is excluded.

Write path (:func:`set_legacy_repo_watermark`):
  Upserts the RAW legacy row: ``target=target, dataset_key=target``.  Does
  NOT collapse git→commits.  The raw row is the shared bridge for all sibling
  datasets; deleting it would break the reverse-read fallback for siblings.

Lookback overlap (CHAOS-2572)
------------------------------
:func:`get_watermark_with_overlap` applies a configurable lookback margin
(``SYNC_WATERMARK_OVERLAP`` env var, default 0 seconds) to the stored
watermark **only when the planner reads it for an incremental window**.  The
overlap is never applied to persisted watermark writes or to backfill
coverage.  This ensures that brief provider-side indexing delays do not
create gaps in incremental coverage.

Monotonic writes (CHAOS-2578)
------------------------------
:func:`set_watermark` and :func:`set_legacy_repo_watermark` enforce
``last_synced_at = max(existing, new)`` so that a late-arriving or
out-of-order unit result can never roll the watermark backwards.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, update
from sqlalchemy.orm import Session

from dev_health_ops.models.settings import SyncWatermark

# ---------------------------------------------------------------------------
# Reverse map: dataset_key → frozenset[legacy_target] (INCREMENTAL only)
# ---------------------------------------------------------------------------
# Built by iterating _LEGACY_TARGETS_BY_DATASET.items() (dict iteration —
# NOT `for key in DatasetKey` which triggers a CodeQL false-positive).
# Only datasets with WatermarkBehavior.INCREMENTAL are included so that
# repo-metadata (WatermarkBehavior.NONE) never appears watermarked via the
# reverse fallback.


def _build_dataset_key_to_legacy_targets() -> dict[str, frozenset[str]]:
    """Return dataset_key → frozenset[legacy_target] for INCREMENTAL datasets.

    Derived from ``_LEGACY_TARGETS_BY_DATASET`` at import time.  Built by
    iterating the dict directly (not ``for key in DatasetKey``) to avoid the
    CodeQL false-positive on enum iteration.
    """
    from dev_health_ops.sync.datasets import (
        _LEGACY_TARGETS_BY_DATASET,
        WatermarkBehavior,
        _watermark_behavior,
    )

    result: dict[str, frozenset[str]] = {}
    for dataset_key, legacy_targets in _LEGACY_TARGETS_BY_DATASET.items():
        if _watermark_behavior(dataset_key) == WatermarkBehavior.INCREMENTAL:
            result[dataset_key] = legacy_targets
    return result


# Module-level cache — built once on first import.
_DATASET_KEY_TO_LEGACY_TARGETS: dict[str, frozenset[str]] = (
    _build_dataset_key_to_legacy_targets()
)


# ---------------------------------------------------------------------------
# Lookback overlap helper (CHAOS-2572)
# ---------------------------------------------------------------------------


def _watermark_overlap_seconds() -> int:
    """Return the configured lookback overlap in seconds.

    Read from ``SYNC_WATERMARK_OVERLAP`` (seconds, default 0).  Applied only
    on incremental reads — never on writes or backfill coverage.
    """
    try:
        return max(0, int(os.getenv("SYNC_WATERMARK_OVERLAP", "0")))
    except ValueError:
        return 0


def apply_watermark_overlap(ts: datetime) -> datetime:
    """Subtract the configured lookback overlap from a raw watermark timestamp.

    Intended for legacy incremental read paths that call
    :func:`get_legacy_repo_watermark` directly and need to apply the same
    overlap that :func:`get_watermark_with_overlap` applies for the planner.
    Never call this on writes or backfill coverage.
    """
    overlap = _watermark_overlap_seconds()
    if overlap <= 0:
        return ts
    return ts - timedelta(seconds=overlap)


# ---------------------------------------------------------------------------
# Internal monotonic-update helper
# ---------------------------------------------------------------------------


def _monotonic_update(
    session: Session, row: SyncWatermark, timestamp: datetime
) -> None:
    """Apply a monotonic timestamp update to an existing SyncWatermark row.

    On PostgreSQL uses ``GREATEST(COALESCE(last_synced_at, :ts), :ts)`` so the
    DB resolves concurrent-write races atomically.  On SQLite (tests only) falls
    back to a Python-level comparison.
    """
    dialect_name = session.bind.dialect.name if session.bind is not None else ""
    if dialect_name == "postgresql":
        session.execute(
            update(SyncWatermark)
            .where(SyncWatermark.id == row.id)
            .values(
                last_synced_at=func.greatest(
                    func.coalesce(SyncWatermark.last_synced_at, timestamp),
                    timestamp,
                ),
                updated_at=datetime.now(timezone.utc),
            )
        )
    else:
        # SQLite / other dialects: Python-level monotonic check.
        existing = row.last_synced_at
        if existing is not None:
            existing_utc = (
                existing.replace(tzinfo=timezone.utc)
                if existing.tzinfo is None
                else existing.astimezone(timezone.utc)
            )
            new_utc = (
                timestamp.replace(tzinfo=timezone.utc)
                if timestamp.tzinfo is None
                else timestamp.astimezone(timezone.utc)
            )
            if new_utc <= existing_utc:
                return
        row.last_synced_at = timestamp
        row.updated_at = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Core read/write API
# ---------------------------------------------------------------------------


def get_watermark(
    session: Session, org_id: str, source_id: str, dataset_key: str
) -> datetime | None:
    """Return the stored watermark for ``(org_id, source_id, dataset_key)``.

    Lookup order (D4 compat):

    1. Canonical: ``(org_id, source_id, dataset_key)`` via the
       ``uq_sync_watermark_org_source_dataset`` constraint.
    2. Legacy target column: ``(org_id, repo_id=source_id, target=dataset_key)``
       — covers rows written by the old ``sync_runtime`` path where
       ``target == dataset_key`` (e.g. ``target="git"``, ``dataset_key="git"``).
    3. Reverse-legacy fallback: for a canonical ``dataset_key`` that has no
       exact row, reverse-map it to its legacy target(s) and look up the raw
       legacy row ``(target=legacy_target, dataset_key=legacy_target)``.  This
       lets ``get_watermark(..., 'commits')`` warm from a raw ``target='git'``
       row.  Only INCREMENTAL datasets participate (repo-metadata excluded).

    Returns ``None`` when no row exists yet (cold-start).  Does **not** apply
    the lookback overlap — use :func:`get_watermark_with_overlap` for
    incremental planner reads.
    """
    # 1. Canonical lookup by (org_id, source_id, dataset_key).
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.source_id == source_id,
            SyncWatermark.dataset_key == dataset_key,
        )
        .one_or_none()
    )
    if row is not None:
        return row.last_synced_at

    # 2. Legacy fallback: look up by (org_id, repo_id, target) where
    #    repo_id == source_id and target == dataset_key (old path stored
    #    target == dataset_key, e.g. target="git", dataset_key="git").
    legacy_row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.repo_id == source_id,
            SyncWatermark.target == dataset_key,
        )
        .one_or_none()
    )
    if legacy_row is not None:
        return legacy_row.last_synced_at

    # 3. Reverse-legacy fallback: dataset_key is a canonical key (e.g.
    #    'commits') that has no exact row yet.  Reverse-map it to its legacy
    #    target(s) (e.g. 'git') and look up the raw legacy row
    #    (target='git', dataset_key='git').  This warms the planner for ALL
    #    sibling datasets (commits, commit-stats, files) from a single raw
    #    legacy row without collapsing git→commits.
    #    Only INCREMENTAL datasets are in the reverse map; repo-metadata is
    #    excluded so it never appears watermarked via this path.
    legacy_targets = _DATASET_KEY_TO_LEGACY_TARGETS.get(dataset_key, frozenset())
    for legacy_target in legacy_targets:
        raw_row = (
            session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == org_id,
                SyncWatermark.repo_id == source_id,
                SyncWatermark.target == legacy_target,
                SyncWatermark.dataset_key == legacy_target,
            )
            .one_or_none()
        )
        if raw_row is not None:
            return raw_row.last_synced_at

    return None


def get_watermark_with_overlap(
    session: Session, org_id: str, source_id: str, dataset_key: str
) -> datetime | None:
    """Return the watermark adjusted by the configured lookback overlap.

    Intended for use by the planner's incremental READ path only.  Subtracts
    ``SYNC_WATERMARK_OVERLAP`` seconds from the stored timestamp so that
    brief provider-side indexing delays do not create gaps.

    Semantics:
    - If no watermark exists (cold-start), returns ``None`` unchanged.
    - The overlap is applied only here, never to persisted writes.
    - Backfill coverage uses :func:`get_watermark` directly (no overlap).
    """
    raw = get_watermark(session, org_id, source_id, dataset_key)
    if raw is None:
        return None
    overlap = _watermark_overlap_seconds()
    if overlap <= 0:
        return raw
    return raw - timedelta(seconds=overlap)


def set_watermark(
    session: Session,
    org_id: str,
    source_id: str,
    dataset_key: str,
    timestamp: datetime,
) -> None:
    """Upsert the watermark, enforcing monotonic advance (CHAOS-2578).

    ``last_synced_at`` is set to ``max(existing, new)`` so that a
    late-arriving or out-of-order unit result can never roll the watermark
    backwards.  Both incremental and full-resync runs call this on success.

    The ``target`` column is set to ``dataset_key`` (preserving the existing
    convention where ``target == dataset_key``) so that the legacy unique
    constraint ``uq_sync_watermark_org_repo_target`` remains satisfied.
    """
    # Canonical lookup first.
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.source_id == source_id,
            SyncWatermark.dataset_key == dataset_key,
        )
        .one_or_none()
    )
    if row is None:
        # Also check legacy path (target == dataset_key, repo_id == source_id)
        # to avoid creating a duplicate that would violate the legacy constraint.
        row = (
            session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == org_id,
                SyncWatermark.repo_id == source_id,
                SyncWatermark.target == dataset_key,
            )
            .one_or_none()
        )

    if row is None:
        row = SyncWatermark(
            repo_id=source_id,
            target=dataset_key,  # preserve existing convention: target == dataset_key
            org_id=org_id,
            source_id=source_id,
            dataset_key=dataset_key,
            last_synced_at=timestamp,
        )
        session.add(row)
    else:
        _monotonic_update(session, row, timestamp)
        return
    session.flush()


# ---------------------------------------------------------------------------
# Legacy shim functions (D4 — keep read/write compat until fallbacks removed)
# ---------------------------------------------------------------------------


def get_legacy_repo_watermark(
    session: Session, org_id: str, repo_id: str, target: str
) -> datetime | None:
    """Legacy shim: read watermark by (org_id, repo_id, target).

    Reads by the legacy ``target`` column first (D4 compat), then falls back
    to the canonical ``dataset_key`` lookup via :func:`get_watermark` so that
    rows written by the new planner path are also visible to legacy readers.
    """
    # Direct legacy lookup: target column == target string.
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.repo_id == repo_id,
            SyncWatermark.target == target,
        )
        .one_or_none()
    )
    if row is not None:
        return row.last_synced_at

    # Canonical fallback: the planner may have written a per-dataset row.
    # Use get_watermark so the reverse-legacy fallback also applies.
    return get_watermark(session, org_id, repo_id, target)


def set_legacy_repo_watermark(
    session: Session,
    org_id: str,
    repo_id: str,
    target: str,
    timestamp: datetime,
) -> None:
    """Legacy shim: upsert the RAW legacy watermark row (target=target, dataset_key=target).

    Writes the row with ``target`` AND ``dataset_key`` both set to the raw
    legacy target string (e.g. ``'git'``).  This is the D4 compatibility
    bridge: a single raw row serves as the shared watermark for ALL of the
    target's incremental sibling datasets (commits, commit-stats, files) via
    the reverse-legacy fallback in :func:`get_watermark`.

    Do NOT collapse target→canonical here.  The raw row must be preserved so
    that sibling dataset reads (e.g. ``get_watermark(..., 'commit-stats')``)
    can fall back to it.  Deleting or renaming it would cold-start siblings.

    Monotonic advance is enforced at the DB level on PostgreSQL using
    ``GREATEST(COALESCE(last_synced_at, :ts), :ts)`` so that two concurrent
    legacy completions cannot race and roll the watermark backwards
    (CHAOS-2578).  SQLite (tests only) falls back to Python-level comparison.
    """
    # Look up the raw legacy row by (org_id, repo_id, target).
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.repo_id == repo_id,
            SyncWatermark.target == target,
        )
        .one_or_none()
    )

    if row is None:
        # Create the raw legacy row: target=target, dataset_key=target.
        row = SyncWatermark(
            repo_id=repo_id,
            target=target,
            org_id=org_id,
            source_id=repo_id,
            dataset_key=target,  # raw — NOT the canonical key
            last_synced_at=timestamp,
        )
        session.add(row)
    else:
        _monotonic_update(session, row, timestamp)
    session.flush()
