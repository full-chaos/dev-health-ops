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
# referencing "CREATE TABLE" mid-sentence can never falsely match. This is
# the ONE table-locating regex in this module -- both the precise parser
# and the coarse sweep below use it (see the round-4 review note on
# `_attribute_ttl_matches`): the coarse sweep's independence lives entirely
# in `_COARSE_TTL_RE`, never in a second, unaudited table-name pattern.
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
# Codex round-4 finding (MED, confirmed via `toIntervalDay` in the real
# tree -- the cited file/table was wrong, the mechanism was real): naive
# substring checks (`"TTL" in text`, `"INTERVAL" in text`) match INSIDE
# other identifiers -- `toIntervalDay` contains "Interval", a hypothetical
# `THROTTLED` would contain "TTL". Word-boundaried so it still matches
# "TTL ... INTERVAL" regardless of time unit (DAY/WEEK/MONTH/..., the
# actual looseness this sweep exists for) without matching inside an
# unrelated longer identifier.
_COARSE_TTL_RE = re.compile(r"\bTTL\b.*?\bINTERVAL\b", re.IGNORECASE | re.DOTALL)
# Codex round-4 finding (MED, confirmed dormant -- no real migration uses
# this syntax today): `ALTER TABLE x MODIFY TTL ...` is the standard way to
# add/change retention on an EXISTING table, and none of the three sources
# (precise parser, the old coarse sweep, KNOWN_TTL_TABLES) could ever see
# it -- a CREATE-time-only blind spot shared by all three. The coarse sweep
# below also scans for this form directly; if a future migration ever adds
# one, this makes it visible immediately (`coarse - precise` fires,
# forcing `clickhouse_ttl_retentions`'s CREATE-only extraction to be
# extended too, which is this guard working as designed, not a false
# alarm).
_ALTER_TABLE_MODIFY_TTL_RE = re.compile(
    r"^\s*ALTER\s+TABLE\s+`?(?P<table>[A-Za-z_]\w*)`?\s+MODIFY\s+TTL\b",
    re.IGNORECASE | re.MULTILINE,
)


@dataclass(frozen=True, slots=True)
class TtlRetention:
    table: str
    column: str
    retention_days: int


#: Codex round-2 finding (HIGH, confirmed): an empty registry is not the
#: only way :func:`clickhouse_ttl_retentions` can fail open -- a parser
#: regression or migration-format change can miss ONE table's TTL clause
#: while every other table still parses fine, and ``retentions.get(table)``
#: returning ``None`` is indistinguishable from "genuinely no TTL" at every
#: call site. Reproduced directly: dropping ``telemetry_signal_bucket``
#: alone from an otherwise-full registry let a 999999-row violation for
#: that exact table pass `_assert_no_ttl_horizon_rows` silently.
#:
#: This CLOSED VOCABULARY is the backstop: every table known to carry a
#: production TTL today. `_assert_no_ttl_horizon_rows` and
#: `ttl_horizon.max_generated_age_days_for_table` both require the parsed
#: registry to cover ALL of these before trusting ANY lookup against it --
#: a registry missing even one of them fails closed instead of silently
#: treating the missing table as risk-free. A future migration adding a new
#: TTL'd table is a deliberate, visible change to this set (mirrored by
#: `tests/test_ttl_registry.py::test_every_known_ttl_table_is_discovered`),
#: not something this code can discover on its own -- that residual is the
#: same one `ttl_horizon.tightest_ttl_days` already accepts for the same
#: reason (see its own docstring).
KNOWN_TTL_TABLES: frozenset[str] = frozenset(
    {
        "feature_flag_event",
        "telemetry_signal_bucket",
        "release_impact_daily",
        "product_telemetry_events",
    }
)


def _attribute_ttl_matches(
    statement: str, ttl_re: re.Pattern[str]
) -> list[tuple[str, re.Match[str]]]:
    """Associate every ``ttl_re`` match in ``statement`` with the CLOSEST
    PRECEDING ``_CREATE_TABLE_RE`` match -- not just the first CREATE TABLE
    in the whole chunk.

    Codex round-4 finding (MED, confirmed dormant): the original
    ``.search()``-based version always paired the FIRST CREATE TABLE with
    the FIRST TTL clause in a ``;``-delimited chunk. This repo's ClickHouse
    migrations split into ``.sql`` (semicolon-delimited, usually one
    CREATE TABLE per statement) and ``.py`` (DDL embedded as a Python
    string, frequently ZERO semicolons -- confirmed directly: 027, 028, 067
    each have none). A ``.py`` migration with two CREATE TABLE blocks and a
    TTL clause belonging to the SECOND would misattribute it to the first
    -- currently dormant only because no existing ``.py`` migration happens
    to carry a TTL at all (checked directly), not because the bug cannot
    fire.
    """

    table_matches = list(_CREATE_TABLE_RE.finditer(statement))
    if not table_matches:
        return []

    results: list[tuple[str, re.Match[str]]] = []
    idx = 0
    current_owner: re.Match[str] | None = None
    for ttl_match in ttl_re.finditer(statement):
        while (
            idx < len(table_matches) and table_matches[idx].start() <= ttl_match.start()
        ):
            current_owner = table_matches[idx]
            idx += 1
        if current_owner is not None:
            results.append((current_owner.group("table"), ttl_match))
    return results


@lru_cache(maxsize=1)
def _coarse_ttl_table_sweep() -> frozenset[str]:
    """A second, INDEPENDENT, deliberately LOOSER pass over the same
    migration source used by :func:`clickhouse_ttl_retentions`.

    Codex round-4 finding (HIGH, confirmed live): the original version used
    its OWN, unanchored, un-``MULTILINE`` table-name regex -- reproduced
    directly against 055_work_item_daily_rollups_replacing_merge_tree.py's
    own docstring, which yielded a phantom table ``'to'`` from "SHOW CREATE
    TABLE to get the live DDL" and a second phantom ``'statement'`` from
    "the table name in a CREATE TABLE statement." Both were silenced only
    because that specific chunk happened to lack the substring "INTERVAL"
    -- one unrelated docstring edit anywhere near either sentence would
    have made this guard abort ALL fixture generation over a table that
    does not exist.

    NAMED DESIGN ASSUMPTION (round-4 review): this function now reuses the
    EXACT SAME anchored :data:`_CREATE_TABLE_RE` the precise parser uses --
    it does not have its own table-locating regex at all anymore. This
    function's independence from :func:`clickhouse_ttl_retentions` lives
    ENTIRELY on the TTL-DETECTION axis (:data:`_COARSE_TTL_RE`'s
    word-boundaried, any-time-unit match, plus the
    :data:`_ALTER_TABLE_MODIFY_TTL_RE` scan below), never on DDL location.
    The tradeoff: a CREATE-statement shape ``_CREATE_TABLE_RE`` does not
    match (e.g. some future ``CREATE TABLE ... AS SELECT`` form) now blinds
    BOTH sources identically, rather than one catching what the other
    misses. That is accepted deliberately -- location disagreement between
    two DIFFERENT table-name regexes was pure parsing noise (this exact
    finding, and the round-3 fifth-table gap before it, were both about TTL
    DETECTION, never about which regex finds the table name), and sharing
    one regex is what makes the "coarse is a strict superset of precise"
    invariant a STRUCTURAL guarantee rather than a hope.
    """

    if not _MIGRATIONS_DIR.exists():
        return frozenset()

    tables: set[str] = set()
    for path in sorted(_MIGRATIONS_DIR.glob("*")):
        if path.suffix not in {".sql", ".py"}:
            continue
        text = path.read_text(encoding="utf-8")
        for match in _ALTER_TABLE_MODIFY_TTL_RE.finditer(text):
            tables.add(match.group("table"))
        for statement in text.split(";"):
            for table, _ttl_match in _attribute_ttl_matches(statement, _COARSE_TTL_RE):
                tables.add(table)
    return frozenset(tables)


def assert_ttl_vocabulary_is_consistent() -> None:
    """Cross-checks THREE independent descriptions of "which tables carry a
    TTL" against each other -- the precise parser (:func:`clickhouse_ttl_
    retentions`), an independent coarse sweep (:func:`_coarse_ttl_table_
    sweep`), and the hand-maintained :data:`KNOWN_TTL_TABLES` -- and fails
    loudly the moment any two disagree, in EITHER direction.

    Codex round-3 finding (HIGH, confirmed): every previous check compared
    the registry against KNOWN_TTL_TABLES alone, which only catches a table
    falling OUT of an otherwise-working registry. A table that never enters
    the registry (an unmatched TTL syntax variant) AND was never added to
    KNOWN_TTL_TABLES (nobody knew about it) satisfies "the registry covers
    every known table" trivially -- the vocabulary was checking itself
    against itself. The coarse sweep is the independent third source that
    breaks that circularity: precision (does the parser over-match?) is
    checked by comparing against the coarse sweep's recall -- see
    :func:`_coarse_ttl_table_sweep`'s own docstring for the round-4 note on
    exactly which axis that independence lives on.
    """

    precise = set(clickhouse_ttl_retentions().keys())
    coarse = set(_coarse_ttl_table_sweep())
    known = set(KNOWN_TTL_TABLES)

    problems: list[str] = []
    if coarse - precise:
        problems.append(
            "the coarse sweep found TTL'd table(s) the precise parser "
            f"missed: {sorted(coarse - precise)}"
        )
    if precise - coarse:
        problems.append(
            "the precise parser found TTL'd table(s) the coarse sweep "
            "missed -- should be impossible, since the coarse sweep is a "
            f"strict superset by construction: {sorted(precise - coarse)}"
        )
    if coarse - known:
        problems.append(
            "TTL'd table(s) not yet added to KNOWN_TTL_TABLES: "
            f"{sorted(coarse - known)}"
        )
    if known - coarse:
        problems.append(
            "KNOWN_TTL_TABLES entry no longer found carrying a TTL clause "
            f"(renamed, dropped, or the migration changed shape): "
            f"{sorted(known - coarse)}"
        )
    if problems:
        raise RuntimeError(
            "TTL table vocabulary is inconsistent across the precise "
            "parser, an independent coarse sweep, and the hand-maintained "
            "KNOWN_TTL_TABLES registry (CHAOS-3602) -- " + "; ".join(problems)
        )


@lru_cache(maxsize=1)
def clickhouse_ttl_retentions() -> dict[str, TtlRetention]:
    """Every ClickHouse table carrying a row-level TTL, keyed by table name.

    Scans ``migrations/clickhouse/*.sql`` and ``*.py`` directly -- the same
    files ClickHouse itself is migrated from -- so this can never describe a
    TTL that isn't actually enforced, or fail to describe one that is.

    Codex round-4 finding (MED, confirmed dormant): previously took only the
    FIRST CREATE TABLE and FIRST TTL clause per ``;``-delimited chunk --
    see :func:`_attribute_ttl_matches` for the multi-table misattribution
    this fixes. Note this function does NOT catch a residual note also
    raised in review: ``read_text`` failures (a non-UTF-8 migration file,
    a permissions error) surface as a bare ``OSError``/``UnicodeDecodeError``,
    not the framed ``RuntimeError`` this module's other fail-closed checks
    use -- pre-existing, not fixed here (tracked as a residual, not a live
    gap: every migration file in this repo is valid UTF-8 today).
    """

    retentions: dict[str, TtlRetention] = {}
    if not _MIGRATIONS_DIR.exists():
        return retentions

    for path in sorted(_MIGRATIONS_DIR.glob("*")):
        if path.suffix not in {".sql", ".py"}:
            continue
        text = path.read_text(encoding="utf-8")
        for statement in text.split(";"):
            for table, ttl_match in _attribute_ttl_matches(statement, _TTL_RE):
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
