"""CHAOS-3602: registry of ClickHouse TTL retention windows, parsed
directly from the migration source -- not hand-copied -- so a migration's
TTL changing can never silently drift out of sync with what fixture
generation and the mint guard both treat as "safe to backdate a row to".

Why this exists
================

Migration 034's ``feature_flag_event.event_ts`` carries ``TTL
toDateTime(event_ts) + INTERVAL 90 DAY DELETE``. The fixture generator
independently, and coincidentally, chose ``random.randint(7, 90)`` days as
its own "how old can a synthetic event be" range -- landing exactly on the
TTL's own horizon, with zero margin.

A row minted at precisely ``now - 90 days`` is due for TTL deletion the
moment ``now`` advances even one second past mint time. ClickHouse applies
TTL deletion AT MERGE TIME, asynchronously and silently: no error, no
warning, just fewer rows the next time anyone reads the table. That row's
disappearance is exactly what a mint's own content oracle caught (``world
restore``'s ClickHouse content oracle): the row existed right after
``fixtures world`` generated it, and was gone by the time ``fixtures
world-snapshot`` read the table minutes later, once a background merge had
swept it.

Confirmed directly against ``system.part_log`` (``merge_reason:
TTLDeleteMerge``, the exact part shrinking from 95 to 94 rows) and by
reproducing the SAME missing row on a completely fresh ClickHouse
container with zero session history -- this is not a raw-transfer flake and
not a ClickHouse merge defect. It is a real TTL horizon collision, entirely
on the fixture/mint side.
"""

from __future__ import annotations

import contextvars
import math
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations" / "clickhouse"

# Anchored the same way org_deletion.py's own CREATE TABLE scanner is (PR
# #1602 review D): a real CREATE TABLE always starts its own line, so prose
# referencing "CREATE TABLE" mid-sentence can never falsely match.
_CREATE_TABLE_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?P<table>[A-Za-z_]\w*)`?\s*\(",
    re.IGNORECASE | re.MULTILINE,
)
# Matches both `TTL toDateTime(event_ts) + INTERVAL 90 DAY` (a column
# wrapped in a cast/function) and `TTL day + INTERVAL 365 DAY` (a bare
# column already of a temporal type) -- both forms are used across the
# existing migrations.
_TTL_RE = re.compile(
    r"TTL\s+(?:\w+\()?(?P<column>[\w.]+)\)?\s*\+\s*INTERVAL\s+(?P<days>\d+)\s+DAY",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TtlRetention:
    table: str
    column: str
    retention_days: int


@lru_cache(maxsize=1)
def clickhouse_ttl_retentions() -> dict[str, TtlRetention]:
    """Every ClickHouse table carrying a row-level TTL, keyed by table name.

    Scans ``migrations/clickhouse/*.sql`` and ``*.py`` directly -- the same
    files ClickHouse itself is migrated from -- so this can never describe a
    TTL that isn't actually enforced, or fail to describe one that is.
    """

    retentions: dict[str, TtlRetention] = {}
    if not _MIGRATIONS_DIR.exists():
        return retentions

    for path in sorted(_MIGRATIONS_DIR.glob("*")):
        if path.suffix not in {".sql", ".py"}:
            continue
        text = path.read_text(encoding="utf-8")
        for statement in text.split(";"):
            table_match = _CREATE_TABLE_RE.search(statement)
            ttl_match = _TTL_RE.search(statement)
            if not table_match or not ttl_match:
                continue
            table = table_match.group("table")
            retentions[table] = TtlRetention(
                table=table,
                column=ttl_match.group("column"),
                retention_days=int(ttl_match.group("days")),
            )

    return retentions


#: How much headroom the mint GUARD keeps inside a table's own TTL horizon
#: before refusing to snapshot. Not zero: a boot that restores hours or
#: days after a mint must not itself cross the horizon a row was
#: deliberately backdated close to.
TTL_SAFETY_MARGIN_DAYS = 7

#: Extra headroom fixture GENERATION keeps below the guard's own threshold
#: (``max_safe_backdate_days``) -- deliberately a SEPARATE, smaller number,
#: not the same value reused twice. A mint's own guard check runs seconds
#: to minutes after generation finishes; if generation were allowed to date
#: a row at EXACTLY the guard's threshold, that ordinary elapsed time alone
#: -- `now()` at guard-check time being a few seconds later than `now()` at
#: generation time -- pushes the row's age past the threshold and the
#: guard fires on its own mint's own freshly-generated data (observed live:
#: the first re-mint attempt after this fix was built failed exactly this
#: way, 8 rows "at or past 83 days old" when generation had targeted
#: precisely 83 as its ceiling). Generation must stay strictly, comfortably
#: inside what the guard considers safe, not tied to the same number.
GENERATOR_SLACK_DAYS = 3


def max_safe_backdate_days(table: str) -> int | None:
    """The mint guard's own threshold: rows at or past this many days old
    refuse the snapshot. ``None`` if the table carries no TTL at all."""

    retention = clickhouse_ttl_retentions().get(table)
    if retention is None:
        return None
    return max(0, retention.retention_days - TTL_SAFETY_MARGIN_DAYS)


#: Threaded per-mint drift (see :func:`compute_drift_days`) into every
#: :func:`max_generated_age_days` call, without changing any generator's
#: signature. Each generator already reads ``datetime.now()``, which
#: CHAOS-3392's ``_frozen_clock`` separately freezes to ``pinned_now`` --
#: this is a second, orthogonal piece of context (how stale that pinned
#: reference itself is), not a second clock. A ``ContextVar`` rather than a
#: module-level global so nested/concurrent generation runs (tests, or a
#: future parallel-repo mint) can't cross-contaminate each other's drift.
_drift_days_var: contextvars.ContextVar[int] = contextvars.ContextVar(
    "dev_health_ops_ttl_registry_drift_days", default=0
)


@contextmanager
def drift_days_context(drift_days: int):
    """Makes ``drift_days`` visible to every :func:`max_generated_age_days`
    call for the duration of the ``with`` block."""

    token = _drift_days_var.set(drift_days)
    try:
        yield
    finally:
        _drift_days_var.reset(token)


def max_generated_age_days(table: str) -> int | None:
    """The oldest a fixture generator may date a row for ``table``, given
    the drift currently active via :func:`drift_days_context` (0 if none).

    Strictly less than :func:`max_safe_backdate_days` -- the mint guard's
    own threshold -- so that ordinary elapsed time between generation and
    the guard's check, within the same mint run, can never itself trip the
    guard. Drift is subtracted on top of that: generation is deliberately
    anchored to ``world.json``'s ``pinned_now`` (CHAOS-3392, for
    WORLD_DIGEST determinism), not real wall-clock time, but the mint
    guard and ClickHouse's own TTL enforcement both operate on REAL time --
    every day ``pinned_now`` sits unrefreshed, a row dated at exactly the
    (drift-unaware) ceiling is that many days OLDER in real terms than the
    ceiling accounts for. Observed live: a 3-day-stale ``pinned_now``
    produced rows the guard correctly flagged as "83 days old" when
    generation had targeted precisely 83 as its (undrifted) ceiling.
    Subtracting drift keeps a row's REAL-world age constant regardless of
    how stale the pinned reference has become. ``None`` if the table
    carries no TTL at all.
    """

    limit = max_safe_backdate_days(table)
    if limit is None:
        return None
    drift_days = _drift_days_var.get()
    return max(0, limit - GENERATOR_SLACK_DAYS - drift_days)


def compute_drift_days(pinned_now: datetime, real_now: datetime) -> int:
    """How many whole days real time has moved past ``pinned_now``.

    Never negative -- a ``pinned_now`` at or after ``real_now`` (clock
    skew, or a manifest deliberately pinned slightly ahead for testing)
    needs no compensation. Rounds UP (``ceil``): a partial day of drift is
    still a full day a row could tip over its TTL horizon.
    """

    delta_days = (real_now - pinned_now).total_seconds() / 86400
    return max(0, math.ceil(delta_days))


#: How stale ``world.json``'s ``pinned_now`` may be (in days, relative to
#: real wall-clock time) before a mint refuses to proceed. Past this
#: ceiling, silently squeezing the generatable-history window further to
#: compensate would produce an ever-thinner, ever less realistic world with
#: no visible signal that anything was wrong; refuse instead and name the
#: real fix -- re-pinning ``pinned_now`` is a deliberate, ticketed decision
#: (it revalidates the digest and the acceptance corpus), not something a
#: mint decides on its own.
MAX_PINNED_NOW_STALENESS_DAYS = 14

#: Mirrors GENERATOR_SLACK_DAYS for the restore side: how much of
#: TTL_SAFETY_MARGIN_DAYS a restored snapshot must still have unspent
#: before a restore is allowed, so an ordinary boot -- which itself takes
#: real time -- can't itself walk a shelf-life-expired snapshot across a
#: TTL horizon while it runs.
RESTORE_SIDE_SLACK_DAYS = 2


class PinnedNowStaleError(RuntimeError):
    """``world.json``'s ``pinned_now`` has drifted too far from real time
    to mint safely. Re-pinning is a deliberate, ticketed decision -- never
    something a mint silently compensates for by squeezing the
    generatable-history window."""


class SnapshotExpiredError(RuntimeError):
    """The minted snapshot has exceeded its TTL shelf life: restoring it
    now risks losing rows to a live TTL merge before anything ever reads
    them. Re-mint required."""


def assert_pinned_now_not_too_stale(pinned_now: datetime, real_now: datetime) -> int:
    """Returns the drift in days if it is within :data:`MAX_PINNED_NOW_
    STALENESS_DAYS`; raises :class:`PinnedNowStaleError` naming the real
    fix otherwise."""

    drift_days = compute_drift_days(pinned_now, real_now)
    if drift_days > MAX_PINNED_NOW_STALENESS_DAYS:
        raise PinnedNowStaleError(
            f"world.json pinned_now ({pinned_now.isoformat()}) is "
            f"{drift_days} days stale (ceiling: "
            f"{MAX_PINNED_NOW_STALENESS_DAYS}) -- re-pinning is a "
            "deliberate decision (it revalidates WORLD_DIGEST and the "
            "acceptance corpus against the new pinned_now), not something "
            "a mint does silently by squeezing the generatable-history "
            "window to compensate. See CHAOS-3602."
        )
    return drift_days


def snapshot_shelf_life_days() -> int:
    """How many days past ``pinned_now`` a minted snapshot may still be
    restored before it risks losing rows to a live TTL merge."""

    return max(0, TTL_SAFETY_MARGIN_DAYS - RESTORE_SIDE_SLACK_DAYS)


def snapshot_expiry(pinned_now: datetime) -> datetime:
    """The instant a snapshot minted with this ``pinned_now`` expires --
    recorded in the snapshot manifest so humans can see it without doing
    the arithmetic themselves."""

    return pinned_now + timedelta(days=snapshot_shelf_life_days())


def assert_snapshot_not_expired(pinned_now: datetime, real_now: datetime) -> None:
    """Raises :class:`SnapshotExpiredError` if ``real_now`` is at or past
    this snapshot's shelf life."""

    expiry = snapshot_expiry(pinned_now)
    if real_now >= expiry:
        raise SnapshotExpiredError(
            f"snapshot expired (TTL shelf life): pinned_now="
            f"{pinned_now.isoformat()}, shelf life="
            f"{snapshot_shelf_life_days()} days, expired at "
            f"{expiry.isoformat()}, real now={real_now.isoformat()} -- "
            "re-mint required."
        )
