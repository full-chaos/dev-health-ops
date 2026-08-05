"""Watermark-vs-now lag for the admin sync status surface (CHAOS-3430).

Why this exists
---------------
The CHAOS-3412 incremental window ratchet caps a HEAVY dataset's cold-start
window at ``SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS`` and stamps the watermark
at the window *end*.  Each capped tick therefore finalizes as an ordinary
successful run with ``last_sync_success=true`` while the dataset's watermark
may still trail ``now`` by weeks.  Run status alone reads "complete"; the
advancing watermark is the only observable evidence of catch-up progress.

This module turns that watermark into a reportable state so the status
surface can say so out loud.  It is a **read-only** computation over the
existing ``sync_watermarks`` rows — no schema change, no new persisted state,
and no change to run finalization semantics (a capped run IS a successful
run).

Flagging rule
-------------
"Catching up" is deliberately narrow, so it means something when it appears:

* the dataset's cost class must be HEAVY — only HEAVY families ratchet, so a
  trailing LIGHT/MEDIUM watermark is a different problem and is not claimed
  to be catch-up here; and
* the watermark must trail ``now`` by **strictly more** than the configured
  window cap — a watermark exactly one cap-window back is the steady state of
  a healthy ratchet mid-flight, not arrears.

``lag_seconds`` is always reported when a watermark exists, for every cost
class.  Only the catch-up *verdict* is withheld — an honest lag with no
verdict beats a verdict the ratchet cannot justify.

Cap resolution
--------------
``heavy_max_window_days`` **delegates to the planner's own resolver**
(``_effective_heavy_max_window_days``) rather than re-reading the env var.
That resolver is not a plain env read: it widens the cap when
``SYNC_WATERMARK_OVERLAP`` is greater than or equal to it, because a window
ending at or before the watermark it started from would never advance.  A
second env read here would judge lag against a cap the planner does not
actually use, and every dataset inside the widened window would be reported
as behind.  Executing the pinned production resolver makes that drift
impossible; the local default exists only as a fallback for the case where
the planner cannot be imported.
"""

from __future__ import annotations

import math
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

#: Env var naming the HEAVY incremental window cap, in days.
HEAVY_MAX_WINDOW_DAYS_ENV = "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS"

#: Default cap, matching the proven backfill chunk size.
DEFAULT_HEAVY_MAX_WINDOW_DAYS = 7

#: The only cost class the ratchet caps, and so the only one that can be
#: "catching up".  Matches ``sync.datasets.CostClass.HEAVY``.
HEAVY_COST_CLASS = "heavy"

_SECONDS_PER_DAY = 86_400


def heavy_max_window_days() -> int:
    """Return the HEAVY incremental window cap the planner actually uses.

    Delegates to ``planner._effective_heavy_max_window_days`` so the lag
    verdict is taken against the same cap that sizes the windows — including
    its widening when ``SYNC_WATERMARK_OVERLAP`` meets or exceeds the
    configured cap.  Reading the env var here instead would drift from the
    planner precisely in the configurations the planner corrects for.

    Falls back to a direct env read only if the planner cannot be imported,
    and to :data:`DEFAULT_HEAVY_MAX_WINDOW_DAYS` when the value is absent,
    non-numeric, or below one day — a zero or negative cap would divide by
    zero when counting ticks and would flag every watermark, however fresh.
    """
    try:
        from dev_health_ops.sync.planner import _effective_heavy_max_window_days
    except ImportError:  # pragma: no cover - defensive
        pass
    else:
        resolved = _effective_heavy_max_window_days()
        return resolved if resolved >= 1 else DEFAULT_HEAVY_MAX_WINDOW_DAYS

    raw = os.getenv(HEAVY_MAX_WINDOW_DAYS_ENV)
    if raw is None:
        return DEFAULT_HEAVY_MAX_WINDOW_DAYS
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_HEAVY_MAX_WINDOW_DAYS
    if value < 1:
        return DEFAULT_HEAVY_MAX_WINDOW_DAYS
    return value


@dataclass(frozen=True)
class WatermarkLag:
    """How far a (source, dataset) watermark trails the current time."""

    #: The stored watermark, normalized to UTC; ``None`` when no row exists.
    watermark_at: datetime | None
    #: ``now - watermark_at`` in whole seconds, clamped at zero; ``None`` when
    #: there is no watermark to measure from.
    lag_seconds: int | None
    #: True only for a HEAVY dataset trailing by strictly more than the cap.
    catching_up: bool
    #: Scheduled ticks still needed to reach ``now``, at the ratchet's NET
    #: advance per tick (cap minus watermark overlap); ``None`` unless
    #: ``catching_up``.
    ticks_behind: int | None
    #: The cap the verdict was taken against, so the surface can explain it.
    window_cap_days: int


def _as_utc(value: datetime) -> datetime:
    """Read a possibly-naive timestamp as UTC (SQLite round-trips drop tzinfo)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def heavy_net_advance_seconds() -> int:
    """Seconds of watermark advance per successful capped HEAVY tick.

    Delegates to the planner for the same reason :func:`heavy_max_window_days`
    does: the ratchet's arithmetic has one owner, and an estimate that
    recomputed it would drift from the windows it claims to describe.
    """
    try:
        from dev_health_ops.sync.planner import heavy_ratchet_net_advance_seconds
    except ImportError:  # pragma: no cover - defensive
        pass
    else:
        return max(1, heavy_ratchet_net_advance_seconds())

    # Planner unavailable: fall back to cap minus overlap, computed the same way.
    from dev_health_ops.sync.watermarks import _watermark_overlap_seconds

    return max(
        1, heavy_max_window_days() * _SECONDS_PER_DAY - _watermark_overlap_seconds()
    )


def compute_watermark_lag(
    *,
    cost_class: str,
    watermark_at: datetime | None,
    now: datetime,
    window_cap_days: int | None = None,
    net_advance_seconds: int | None = None,
) -> WatermarkLag:
    """Compute watermark lag and the catch-up verdict for one dataset.

    ``window_cap_days`` overrides the env-resolved cap and
    ``net_advance_seconds`` the per-tick advance; callers computing many
    datasets should resolve both once and pass them, so a mid-loop env change
    cannot make one response internally inconsistent.

    The two are deliberately distinct. The VERDICT is taken against the cap — a
    dataset within one window of ``now`` is a healthy ratchet mid-flight. The
    tick ESTIMATE is taken against the net advance, which is ``cap - overlap``:
    the window re-reads the overlap on every tick, so that portion buys no
    forward progress.
    """
    cap_days = (
        heavy_max_window_days() if window_cap_days is None else int(window_cap_days)
    )
    if cap_days < 1:
        cap_days = DEFAULT_HEAVY_MAX_WINDOW_DAYS

    if watermark_at is None:
        return WatermarkLag(
            watermark_at=None,
            lag_seconds=None,
            catching_up=False,
            ticks_behind=None,
            window_cap_days=cap_days,
        )

    watermark_utc = _as_utc(watermark_at)
    # A watermark is never recorded ahead of now, but a clock skew or a
    # hand-edited row must not surface as negative lag.
    lag_seconds = max(0, int((_as_utc(now) - watermark_utc).total_seconds()))

    cap_seconds = cap_days * _SECONDS_PER_DAY
    catching_up = cost_class == HEAVY_COST_CLASS and lag_seconds > cap_seconds
    # Ticks are paced by NET advance (cap - overlap), not the cap: the overlap
    # slice is re-read every tick and buys no forward progress.
    advance = (
        heavy_net_advance_seconds()
        if net_advance_seconds is None
        else max(1, int(net_advance_seconds))
    )
    ticks_behind = math.ceil(lag_seconds / advance) if catching_up else None

    return WatermarkLag(
        watermark_at=watermark_utc,
        lag_seconds=lag_seconds,
        catching_up=catching_up,
        ticks_behind=ticks_behind,
        window_cap_days=cap_days,
    )


class _WatermarkRow(Protocol):
    """The ``SyncWatermark`` columns this module reads."""

    source_id: str
    repo_id: str
    target: str
    dataset_key: str
    last_synced_at: datetime | None


def resolve_watermark(
    rows: Iterable[_WatermarkRow], source_key: str, dataset_key: str
) -> datetime | None:
    """Resolve one watermark from pre-loaded rows, mirroring ``get_watermark``.

    The status surface reads many (source, dataset) pairs at once and cannot
    afford ``get_watermark``'s per-pair queries — and ``get_watermark`` is a
    sync-``Session`` helper the async admin API cannot call.  This applies the
    same three-tier precedence in Python over rows already in memory:

    1. canonical ``(source_id, dataset_key)``;
    2. legacy target column ``(repo_id, target == dataset_key)``;
    3. reverse-legacy fallback to the raw legacy row
       ``(repo_id, target == legacy_target, dataset_key == legacy_target)``.

    Because this is a second implementation of a rule that already exists,
    ``tests/test_watermark_lag_parity.py`` runs it against ``get_watermark``
    over a shared DB fixture; the two must agree on every case.
    """
    return build_watermark_index(rows).resolve(source_key, dataset_key)


class WatermarkIndex:
    """Pre-indexed watermark rows, resolvable in O(1) per (source, dataset).

    CHAOS-3430: the status surface resolves P pairs against R rows. Scanning the
    row list per pair is O(P x R) and re-walks the same rows for every tier of
    the precedence; a run with many sources and datasets does that thousands of
    times for a result that never changes. Indexing once collapses each lookup
    to a few dict hits.

    The precedence is IDENTICAL to the scan it replaces — and to
    ``watermarks.get_watermark``, which the differential oracle in
    ``tests/test_watermark_lag_parity.py`` enforces across exactly this kind of
    refactor. That test is the reason this optimization is safe to make: it
    cannot silently reorder the tiers.

    First row wins within a tier, matching the scan's behaviour (the DB holds a
    unique constraint per tier, so duplicates are not expected in practice).
    """

    __slots__ = ("_canonical", "_legacy_target", "_raw_legacy")

    def __init__(self, rows: Iterable[_WatermarkRow]) -> None:
        # tier 1: (source_id, dataset_key)
        self._canonical: dict[tuple[str, str], datetime | None] = {}
        # tier 2: (repo_id, target)
        self._legacy_target: dict[tuple[str, str], datetime | None] = {}
        # tier 3: (repo_id, target) where target == dataset_key (the raw bridge row)
        self._raw_legacy: dict[tuple[str, str], datetime | None] = {}
        for row in rows:
            self._canonical.setdefault(
                (row.source_id, row.dataset_key), row.last_synced_at
            )
            self._legacy_target.setdefault(
                (row.repo_id, row.target), row.last_synced_at
            )
            if row.target == row.dataset_key:
                self._raw_legacy.setdefault(
                    (row.repo_id, row.target), row.last_synced_at
                )

    def resolve(self, source_key: str, dataset_key: str) -> datetime | None:
        """Resolve one watermark, applying ``get_watermark``'s three tiers."""
        from dev_health_ops.sync.watermarks import _DATASET_KEY_TO_LEGACY_TARGETS

        key = (source_key, dataset_key)
        if key in self._canonical:
            return self._canonical[key]
        if key in self._legacy_target:
            return self._legacy_target[key]
        for legacy_target in sorted(
            _DATASET_KEY_TO_LEGACY_TARGETS.get(dataset_key, frozenset())
        ):
            raw_key = (source_key, legacy_target)
            if raw_key in self._raw_legacy:
                return self._raw_legacy[raw_key]
        return None

    def relevant_lookup_values(self, dataset_keys: Iterable[str]) -> set[str]:
        """Dataset/target values that could satisfy any of ``dataset_keys``.

        Used to narrow the SELECT: a row whose dataset_key AND target are both
        outside this set can never win any tier, so there is no reason to load
        it.
        """
        from dev_health_ops.sync.watermarks import _DATASET_KEY_TO_LEGACY_TARGETS

        values: set[str] = set()
        for dataset_key in dataset_keys:
            values.add(dataset_key)
            values.update(_DATASET_KEY_TO_LEGACY_TARGETS.get(dataset_key, frozenset()))
        return values


def build_watermark_index(rows: Iterable[_WatermarkRow]) -> WatermarkIndex:
    """Index ``rows`` once for repeated (source, dataset) resolution."""
    return WatermarkIndex(rows)
