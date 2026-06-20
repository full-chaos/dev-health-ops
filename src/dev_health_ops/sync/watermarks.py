"""Sync watermark read/write helpers (CHAOS-2571, CHAOS-2572, CHAOS-2578).

Canonical watermark identity
-----------------------------
The canonical key is ``(org_id, source.external_id, dataset_key)``.  This
matches the ``uq_sync_watermark_org_source_dataset`` unique constraint on
``SyncWatermark`` and the key used by the planner when it calls
:func:`get_watermark`.

Legacy ``target`` alias (D4 — one-way toward canonical)
---------------------------------------------------------
The legacy ``SyncConfiguration.sync_targets`` vocabulary uses coarse target
strings (``"git"``, ``"prs"``, …) that map to one or more ``dataset_key``
values.  :func:`dataset_key_for_legacy_target` resolves a legacy target to
the *primary* dataset key so that legacy read paths can locate the canonical
row.  The mapping is derived at import time from
:data:`dev_health_ops.sync.datasets._LEGACY_TARGETS_BY_DATASET` (the
registry-owned source of truth) — it is **not** hardcoded here.

Legacy read/write compat (D4):

* :func:`get_legacy_repo_watermark` reads by ``(org_id, repo_id, target)``
  using the ``target`` column (legacy unique constraint).  Falls back to the
  canonical ``dataset_key`` lookup via the alias map so that rows written by
  the new planner path are also visible to legacy readers.
* :func:`set_legacy_repo_watermark` delegates to :func:`set_watermark`.
* The legacy ``sync_runtime`` path continues to call ``set_watermark`` with
  ``dataset_key=target`` (e.g. ``"git"``).  These rows have
  ``target == dataset_key`` and satisfy the legacy constraint.

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
:func:`set_watermark` enforces ``last_synced_at = max(existing, new)`` so
that a late-arriving or out-of-order unit result can never roll the watermark
backwards.  Both incremental and full-resync runs call this on success and
benefit from this guarantee automatically.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, update
from sqlalchemy.orm import Session

from dev_health_ops.models.settings import SyncWatermark

# ---------------------------------------------------------------------------
# Legacy target → dataset_key alias (built from the registry, not hardcoded)
# ---------------------------------------------------------------------------


def _build_legacy_target_to_dataset_key() -> dict[str, str]:
    """Return a mapping of legacy target string → primary dataset_key.

    Derived from ``_LEGACY_TARGETS_BY_DATASET`` at import time.  When
    multiple dataset keys share the same legacy target (e.g. ``"git"`` covers
    ``repo-metadata``, ``commits``, ``commit-stats``, ``files``), the
    *first* dataset key in ``DatasetKey`` enum order that has
    ``WatermarkBehavior.INCREMENTAL`` is chosen as the primary representative.
    Datasets with ``WatermarkBehavior.NONE`` (e.g. ``repo-metadata``) are
    excluded so that legacy ``target='git'`` resolves to a key that the
    planner actually stores watermarks under (CHAOS-2573/D4).
    """
    from dev_health_ops.sync.datasets import (
        _LEGACY_TARGETS_BY_DATASET,
        DatasetKey,
        WatermarkBehavior,
        _watermark_behavior,
    )

    # Build target → [dataset_keys] in DatasetKey enum order for determinism.
    target_to_keys: dict[str, list[str]] = {}
    for key in DatasetKey:
        targets = _LEGACY_TARGETS_BY_DATASET.get(key.value, frozenset())
        for target in targets:
            target_to_keys.setdefault(target, []).append(key.value)

    # Primary representative = first dataset_key in enum order that has
    # WatermarkBehavior.INCREMENTAL.  Skip NONE datasets so that legacy
    # target='git' does not resolve to 'repo-metadata' (which has no
    # watermark row) and leave planner reads for commits/commit-stats/files
    # cold-starting (CHAOS-2573/D4).
    result: dict[str, str] = {}
    for target, keys in target_to_keys.items():
        for k in keys:
            if _watermark_behavior(k) == WatermarkBehavior.INCREMENTAL:
                result[target] = k
                break
    return result


# Module-level cache — built once on first import.
_LEGACY_TARGET_TO_DATASET_KEY: dict[str, str] = _build_legacy_target_to_dataset_key()


def dataset_key_for_legacy_target(target: str) -> str | None:
    """Return the primary dataset_key for a legacy sync target string.

    Returns ``None`` if the target is not recognised.  Callers that need a
    fallback should use ``dataset_key_for_legacy_target(t) or t``.
    """
    return _LEGACY_TARGET_TO_DATASET_KEY.get(target)


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
    3. Canonical alias: if ``dataset_key`` is a legacy target string (e.g.
       ``"git"``), look up the canonical dataset_key via the alias map and
       retry step 1.  This covers rows written by :func:`set_legacy_repo_watermark`
       which resolves the target to the canonical key before writing.

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

    # 3. Canonical alias fallback: if dataset_key is a legacy target string,
    #    resolve it to the canonical dataset_key and retry the canonical lookup.
    #    Covers rows written by set_legacy_repo_watermark (which resolves
    #    target → canonical key before writing).
    canonical_key = dataset_key_for_legacy_target(dataset_key)
    if canonical_key is not None and canonical_key != dataset_key:
        alias_row = (
            session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == org_id,
                SyncWatermark.source_id == source_id,
                SyncWatermark.dataset_key == canonical_key,
            )
            .one_or_none()
        )
        if alias_row is not None:
            return alias_row.last_synced_at

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
        # Monotonic write (CHAOS-2578): never roll the watermark backwards.
        #
        # On PostgreSQL we use a DB-level GREATEST(COALESCE(...), :ts) UPDATE so
        # that two concurrent sessions cannot both read the same value, both decide
        # theirs is newer, and the later commit overwrite a higher timestamp with a
        # lower one — the DB resolves the race atomically.
        #
        # On SQLite (tests only) GREATEST is not available; fall back to the
        # Python-level comparison which is correct for single-session use.
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
                    session.flush()
                    return
            row.last_synced_at = timestamp
            row.updated_at = datetime.now(timezone.utc)
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
    to the canonical ``dataset_key`` lookup via the alias map so that rows
    written by the new planner path are also visible to legacy readers.
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

    # Alias fallback: resolve legacy target to canonical dataset_key.
    canonical_key = dataset_key_for_legacy_target(target)
    if canonical_key is not None and canonical_key != target:
        return get_watermark(session, org_id, repo_id, canonical_key)

    return None


def set_legacy_repo_watermark(
    session: Session,
    org_id: str,
    repo_id: str,
    target: str,
    timestamp: datetime,
) -> None:
    """Legacy shim: write watermark by (org_id, repo_id, target).

    Writes the row with ``target`` preserved as the legacy string (so the
    ``uq_sync_watermark_org_repo_target`` constraint is satisfied and existing
    readers that filter on ``target`` continue to find the row) while also
    populating ``dataset_key`` with the canonical key resolved via the alias
    map (D4 one-way toward canonical).

    Monotonic advance is enforced: ``last_synced_at = max(existing, new)``.
    """
    canonical_key = dataset_key_for_legacy_target(target) or target

    # Look up by the legacy (org_id, repo_id, target) key first so we update
    # the existing row rather than creating a duplicate.
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
        # Also check canonical key in case the new planner already wrote it.
        row = (
            session.query(SyncWatermark)
            .filter(
                SyncWatermark.org_id == org_id,
                SyncWatermark.source_id == repo_id,
                SyncWatermark.dataset_key == canonical_key,
            )
            .one_or_none()
        )

    if row is None:
        row = SyncWatermark(
            repo_id=repo_id,
            target=target,  # preserve legacy target string
            org_id=org_id,
            source_id=repo_id,
            dataset_key=canonical_key,
            last_synced_at=timestamp,
        )
        session.add(row)
    else:
        # Monotonic: never roll back the watermark (CHAOS-2578).
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
                session.flush()
                return
        row.last_synced_at = timestamp
        row.updated_at = datetime.now(timezone.utc)
    session.flush()
