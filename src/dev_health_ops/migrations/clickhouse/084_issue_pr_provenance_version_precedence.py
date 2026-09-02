"""CHAOS-4769: make provenance decide which issue<->PR link survives a merge.

DRAFT -- see "UNVERIFIED" below. Two items are lane-4752-go's [SLOT] work and
this file is not final until they land.

# The defect, measured rather than argued

``work_graph_issue_pr`` is ``ReplacingMergeTree(last_synced)`` keyed on
``(org_id, repo_id, work_item_id, pr_number)`` (migrations 014 + 024 + 027).
``provenance`` is in NEITHER the sorting key nor the version column, so the
engine's max-version rule decides the winner and provenance has no say.

Three producers write the same key. Writer 1 (native) stamps the DEPENDENCY
row's own ``last_synced`` (builder.py:769-792); writers 2 and 3 (explicit_text,
heuristic) stamp ``self._now``, captured once per build at builder.py:159. So
the native row normally carries the LOWEST version and a merge deletes it --
and a ReplacingMergeTree merge does not hide losing rows, it physically
discards them.

Measured on a real container (CHAOS-4769 repro, ClickHouse 26.7.5.10):

    after OPTIMIZE ... FINAL:  Key A 3 rows -> 1, Key B 2 rows -> 1,
    the fallback surviving both.

That is why reader-side precedence was withdrawn: after a merge there is no
alternative row left to rank. Precedence has to live in the VERSION COLUMN.

# The fix

A server-side expression, so no writer on either plane stamps anything new
(constraint: no Python writer changes):

    version_rank = rank(provenance) * MULTIPLIER + toUnixTimestamp64Milli(last_synced)
    rank: native=3, explicit_text=2, heuristic=1, unknown=0

Higher wins, matching ReplacingMergeTree's max-version rule.

# HAZARD 1: MILLISECOND PRECISION

``toUnixTimestamp64Milli``, never ``toUnixTimestamp``. ``last_synced`` is
``DateTime64(3, 'UTC')`` and the sub-second component IS the current tie-break
between two rows of the SAME provenance. Seconds would silently discard it and
reintroduce exact ties -- which resolve by part recency (measured 10/10), i.e.
by insertion order, which is not a semantic anyone chose.

    toUnixTimestamp64Milli(2026-09-02T12:00:00.500Z) = 1788350400500   keeps .500
    toUnixTimestamp       (2026-09-02T12:00:00.500Z) = 1788350400      drops it

# HAZARD 2: THE MULTIPLIER, AND WHY 2**40 IS THE WRONG ONE

The design note justified ``2**40`` as "≈34.8 years, so the recency term cannot
overflow into the rank term until 2004+34.8". That reasoning is WRONG: the Unix
epoch is 1970, so 2**40 ms is exhausted in 2004.8. Every real ``last_synced``
already exceeds it --

    2**40                = 1,099,511,627,776
    ms(2026-09-02)       = 1,788,307,200,000     already 1.6x larger

The ordering nevertheless comes out CORRECT, for a reason the note does not
give: all rows share the same epoch offset, so the constant cancels and only
the SPAN of timestamps matters. A plausible 2015..2026 span is 3.79e11, which
is smaller than one rank step of 1.10e12, so rank still dominates -- with a
margin of about 2.9x rather than the ~decades the note implies.

That margin is thin enough to break on data rather than on time: a sentinel or
corrupt stamp (epoch 0, or a far-future date) exceeds one rank step and lets a
lower-ranked row outrank a native one. A row stamped year 2100 beats a native
row stamped 2026.

So this migration uses 2**45, which makes the stated justification literally
true instead of accidentally true:

    2**45 = 35,184,372,088,832    ms exhausts it in year 3085
    3 * 2**45 = 105,553,116,266,496   (UInt64 max 18,446,744,073,709,551,615)

# HAZARD 3: THE COPY MUST NOT USE FINAL

``INSERT INTO <shadow> SELECT ... FROM <table>`` WITHOUT ``FINAL``, deliberately.
Selecting with ``FINAL`` would carry over only today's winners -- the fallback
rows -- and permanently destroy the native rows this migration exists to
promote. The copy must read every unmerged version. What happens on the WRITE side is
better than "await a merge", and measured rather than assumed (lane-4752-go):
a single ``INSERT ... SELECT`` is one block, and ReplacingMergeTree collapses
same-key rows AT PART CREATION under the new ranking -- 2 rows in, 1 out. The
shadow therefore lands already-correct instead of depending on a later merge.

An EXPLICIT column list is used rather than ``SELECT *``, so the copy is immune
to whether ``version_rank`` ends up MATERIALIZED (excluded from ``SELECT *``)
or a plain/DEFAULT column (included), and to any later column drift.

# WHAT THIS DOES NOT FIX

Keys whose native row was ALREADY physically merged away before this runs. No
migration can resurrect a row the engine has deleted; those keys are repaired
by the next build that rewrites them, not by this change. The mechanism is
forward-only in effect as well as in migration.

# UNVERIFIED -- lane-4752-go [SLOT], this file is a DRAFT until they land

  1. Whether ClickHouse 26.7 accepts a MATERIALIZED column as the
     ReplacingMergeTree version parameter. Fallback order under test:
     MATERIALIZED -> DEFAULT -> plain UInt64 populated by the copy. The plain
     column certainly works but reintroduces a writer obligation for future
     inserts, which violates the no-Python-writer constraint and changes the
     answer -- so this is not a detail.
  2. Whether a version column may be added to an existing table at all, or only
     at CREATE. The shadow-table rebuild below sidesteps it either way.

# Rebuild shape

Follows 055 (and 027/042) rather than inventing one: SHOW CREATE TABLE ->
shadow with the new engine -> verify the shadow's sorting key is byte-for-byte
the original -> copy without FINAL -> verify distinct sorting-key tuple counts
-> EXCHANGE TABLES -> catch-up copy of writes that raced the swap -> DROP.

Ops note, inherited from 055: run during a quiet period. Writes racing the
EXCHANGE are benign here -- they land under the new version rule on either
side of the swap.
"""

from __future__ import annotations

import logging
import re

# rank(provenance) * MULTIPLIER + toUnixTimestamp64Milli(last_synced).
# See HAZARD 2 for why this is 2**45 and not 2**40.
VERSION_MULTIPLIER = 2**45

VERSION_EXPRESSION = (
    "multiIf("
    "provenance = 'native', 3, "
    "provenance = 'explicit_text', 2, "
    "provenance = 'heuristic', 1, "
    f"0) * {VERSION_MULTIPLIER} + toUnixTimestamp64Milli(last_synced)"
)

# Explicit, so the copy does not depend on whether version_rank is MATERIALIZED
# (absent from SELECT *) or DEFAULT (present). See HAZARD 3.
CARRIED_COLUMNS = (
    "repo_id",
    "work_item_id",
    "pr_number",
    "confidence",
    "provenance",
    "evidence",
    "last_synced",
    "org_id",
)

TABLE = "work_graph_issue_pr"
SHADOW = f"{TABLE}_new"
VERSION_COLUMN = "version_rank"

_ENGINE_RE = re.compile(r"ENGINE\s*=\s*ReplacingMergeTree\s*\([^)]*\)", re.IGNORECASE)
_VERSION_COL_RE = re.compile(rf"`?{VERSION_COLUMN}`?\s+UInt64", re.IGNORECASE)

log = logging.getLogger(__name__)


def _table_exists(client, table: str) -> bool:
    res = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return bool(rows and rows[0] and int(rows[0][0]) > 0)


def _engine_full(client, table: str) -> str:
    res = client.query(
        "SELECT engine_full FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return str(rows[0][0]) if rows and rows[0] else ""


def _normalize_sorting_key(key: str) -> str:
    return re.sub(r"\s*,\s*", ", ", re.sub(r"\s+", " ", key.replace("`", ""))).strip(
        " ()"
    )


def _sorting_key(client, table: str) -> str:
    res = client.query(
        "SELECT sorting_key FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    if not rows or not rows[0]:
        raise RuntimeError(f"{table}: could not read sorting_key from system.tables")
    return str(rows[0][0])


def _distinct_key_count(client, table: str, key_columns: list[str]) -> int:
    key_tuple = ", ".join(f"`{c}`" for c in key_columns)
    res = client.query(f"SELECT uniqExact(({key_tuple})) FROM `{table}`")
    rows = getattr(res, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _show_create(client, table: str) -> str:
    res = client.query(f"SHOW CREATE TABLE `{table}`")
    rows = getattr(res, "result_rows", None) or []
    if not rows or not rows[0]:
        raise RuntimeError(f"{table}: SHOW CREATE TABLE returned nothing")
    return str(rows[0][0])


def _inject_version_column(ddl: str) -> str:
    """Add the MATERIALIZED version column to the DDL's column list.

    It must be present AT CREATE: the engine names it as its version parameter,
    so an ALTER ... ADD COLUMN afterwards is too late. Injection follows 027's
    shape (close-paren first, then the ENGINE fallback for DDL variants that
    put them on one line).
    """
    if _VERSION_COL_RE.search(ddl):
        return ddl
    column = f",\n    `{VERSION_COLUMN}` UInt64 MATERIALIZED {VERSION_EXPRESSION}\n)\n"
    result = re.sub(r"(\n\)\s*\n)", column, ddl, count=1)
    if result == ddl:
        result = re.sub(
            r"\)\s*ENGINE", column + "ENGINE", ddl, count=1, flags=re.IGNORECASE
        )
    if result == ddl:
        raise ValueError(f"could not inject {VERSION_COLUMN} into DDL: {ddl[:300]}...")
    return result


def _replace_engine(ddl: str) -> str:
    result, count = _ENGINE_RE.subn(
        f"ENGINE = ReplacingMergeTree({VERSION_COLUMN})", ddl, count=1
    )
    if count == 0:
        raise ValueError(
            f"no 'ENGINE = ReplacingMergeTree(...)' clause found in DDL: {ddl[:300]}..."
        )
    return result


def _replace_table_name(ddl: str, old: str, new: str) -> str:
    result, count = re.subn(
        rf"CREATE TABLE (?:`?\w+`?\.)?`?{re.escape(old)}`?",
        f"CREATE TABLE `{new}`",
        ddl,
        count=1,
    )
    if count == 0:
        raise ValueError(f"could not rename {old} in DDL: {ddl[:200]}...")
    return result


def _copy(client, source: str, target: str) -> None:
    """Copy every version across, WITHOUT FINAL. See HAZARD 3.

    The column list is explicit rather than `SELECT *` so the copy does not
    depend on whether the version column is MATERIALIZED (absent from
    `SELECT *`) or DEFAULT (present), and survives later column drift.
    """
    columns = ", ".join(f"`{c}`" for c in CARRIED_COLUMNS)
    client.command(
        f"INSERT INTO `{target}` ({columns}) SELECT {columns} FROM `{source}`"
    )


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    """Re-insert the OLD table's rows after the swap, then drop it.

    Post-EXCHANGE *shadow* holds the OLD table, including rows written between
    the snapshot copy and the swap. Re-inserting all of them is idempotent under
    ReplacingMergeTree: rows already carried dedup away on the key with the
    highest version winning -- which is now the provenance-ranked version, so
    the catch-up cannot reintroduce the defect. Only after it succeeds is the
    old table dropped.
    """
    log.info(f"  {table}: catch-up copy of post-snapshot writes from `{shadow}`")
    _copy(client, shadow, table)
    client.command(f"DROP TABLE `{shadow}`")


def _rebuild(client) -> None:
    if not _table_exists(client, TABLE):
        log.warning(f"  {TABLE}: does not exist, skipping")
        return

    # Skip path, which also converges a crash between EXCHANGE and DROP.
    if VERSION_COLUMN in _engine_full(client, TABLE):
        log.info(f"  {TABLE}: already ReplacingMergeTree({VERSION_COLUMN})")
        if _table_exists(client, SHADOW):
            _catch_up_and_drop(client, TABLE, SHADOW)
        return

    if _table_exists(client, SHADOW):
        log.warning(f"  {SHADOW}: dropping a leftover pre-EXCHANGE shadow")
        client.command(f"DROP TABLE `{SHADOW}`")

    original_key = _sorting_key(client, TABLE)
    key_columns = [c.strip() for c in original_key.strip("() ").split(",") if c.strip()]

    ddl = _show_create(client, TABLE)
    shadow_ddl = _replace_table_name(
        _replace_engine(_inject_version_column(ddl)), TABLE, SHADOW
    )
    client.command(shadow_ddl)

    # Fail CLOSED on a regex miss: if the shadow's key is not byte-for-byte the
    # original, the rebuild would silently re-key the table.
    if _normalize_sorting_key(_sorting_key(client, SHADOW)) != _normalize_sorting_key(
        original_key
    ):
        client.command(f"DROP TABLE `{SHADOW}`")
        raise RuntimeError(
            f"{SHADOW}: sorting key {_sorting_key(client, SHADOW)!r} does not match "
            f"the original {original_key!r}; aborting without swapping"
        )
    if VERSION_COLUMN not in _engine_full(client, SHADOW):
        client.command(f"DROP TABLE `{SHADOW}`")
        raise RuntimeError(
            f"{SHADOW}: engine {_engine_full(client, SHADOW)!r} is not versioned on "
            f"{VERSION_COLUMN}; aborting without swapping"
        )

    before = _distinct_key_count(client, TABLE, key_columns)
    _copy(client, TABLE, SHADOW)
    after = _distinct_key_count(client, SHADOW, key_columns)
    # Distinct KEY TUPLES, not raw rows. Raw counts legitimately differ: the copy
    # collapses same-key rows at part creation under the NEW ranking, which is
    # the fix working rather than data loss. Key tuples must be conserved.
    if before != after:
        client.command(f"DROP TABLE `{SHADOW}`")
        raise RuntimeError(
            f"{TABLE}: {before} distinct sorting-key tuples but the shadow has "
            f"{after}; aborting without swapping"
        )

    log.info(f"  {TABLE}: EXCHANGE with `{SHADOW}` ({before} distinct keys carried)")
    client.command(f"EXCHANGE TABLES `{TABLE}` AND `{SHADOW}`")
    _catch_up_and_drop(client, TABLE, SHADOW)


def upgrade(client):
    """Rank provenance above recency in work_graph_issue_pr's version column."""
    log.info("=== Migration 084: issue<->PR provenance precedence (CHAOS-4769) ===")
    try:
        _rebuild(client)
    except Exception as exc:
        # No blanket shadow cleanup. A pre-EXCHANGE failure already dropped its
        # disposable shadow above; a post-EXCHANGE failure leaves `<table>_new`
        # holding the OLD data, and dropping it would lose the catch-up delta.
        # The rerun skip path converges it.
        log.error(f"FAILED on {TABLE}: {exc}")
        raise
    log.info("=== Migration 084: Complete ===")
