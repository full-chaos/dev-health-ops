"""CHAOS-4769: make provenance decide which issue<->PR link survives a merge.

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

An EXPLICIT column list is used rather than ``SELECT *``. The reason is the
MATERIALIZED-vs-``SELECT *`` ambiguity: a MATERIALIZED column is excluded from
``SELECT *`` while a DEFAULT one is included, so ``SELECT *`` would couple the
copy's correctness to which form the version column takes.

It is NOT drift immunity, and an earlier version of this docstring claimed it
was -- backwards. The explicit list is VULNERABLE to column drift where
``SELECT *`` is the mirror image: a column added to ``work_graph_issue_pr``
after this file is written would be present in the shadow (which is built from
``SHOW CREATE TABLE``) but unnamed by the copy, left at its DEFAULT, swapped in
by ``EXCHANGE``, and the old table dropped -- silent, total loss of that column
with no error at any step. The window is wide by design, because the prod apply
is a separate per-op GO deferred in time.

That is why ``_assert_carried_columns_match`` runs BEFORE the copy and fails
closed: the migration refuses to run rather than quietly truncating. Found on
review by lane-4752-go.

# WHAT THIS DOES NOT FIX

Keys whose native row was ALREADY physically merged away before this runs. No
migration can resurrect a row the engine has deleted; those keys are repaired
by the next build that rewrites them, not by this change. The mechanism is
forward-only in effect as well as in migration.

# The version column's FORM, verified on containers rather than assumed

Measured by lane-4752-go on ClickHouse 26.6.1.1193 (2026-09-02), and this
file's acceptance test passes on 26.6.1.1193 and 26.7.6.57 (2026-09-02):

    MATERIALIZED column              ACCEPTED   <- what this migration uses
    DEFAULT column                   ACCEPTED
    ALIAS column                     REFUSED  Code 16 NO_SUCH_COLUMN_IN_TABLE
                                              "Version column version_rank does
                                               not exist in table declaration."
    expression as engine argument    REFUSED  Code 36 "Cannot evaluate engine
                                              argument 0: Unknown expression or
                                              function identifier `provenance`"
    EXCHANGE TABLES                  ACCEPTED

The two refusals are kept because they are WHY there is no fallback ladder:
MATERIALIZED works, so no writer obligation is reintroduced and constraint (1)
holds. An ALIAS column would have been the tidier expression of "derived, never
stored" and the engine rejects it outright.

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
VERSION_MULTIPLIER = 2**50

# `rank + 1`, not `rank`, and 2**50 rather than a smaller constant. Both are
# bounded by what DateTime64(3) can REPRESENT, not by what a stamp is likely to
# hold -- that distinction has now produced two defects on this file.
#
# WHY +1: `toUnixTimestamp64Milli` is SIGNED. An unsupported provenance falls
# through multiIf to 0, so a bare rank gives `0 * M + <negative>` for any
# pre-1970 stamp, which WRAPS to ~1.8e19 and outranks everything. Measured:
#     provenance='unknown', last_synced='1900-01-01' -> 18446741864720751616
# Ranks 1..4 give even the unknown bucket a full multiplier of headroom.
#
# WHY 2**50 -- THE PRINCIPLE FIRST, BECAUSE THE NUMBER IS DOWNSTREAM OF IT:
#
#   The multiplier must exceed the maximum the TYPE CAN REPRESENT, never the
#   maximum date anyone considers plausible.
#
# One rank step must be wider than any value the column can hold; only then is
# it impossible for a timestamp to cross a rank boundary, for any input. Every
# constant chosen against a plausible extreme has been wrong here:
#
#     max DateTime64(3) millis (9999-12-31 23:59:59.999) = 253,402,300,799,999
#
#     power    value                    exceeds max?  margin   orders the tested pair?
#     2**40       1,099,511,627,776     no            x0.004   no   (round 2 P1)
#     2**45      35,184,372,088,832     no            x0.14    no   (round 4 P1)
#     2**47     140,737,488,355,328     no            x0.56    YES  <- ACCIDENT
#     2**48     281,474,976,710,656     YES           x1.11    yes  <- first correct
#     2**50   1,125,899,906,842,624     YES           x4.44    yes  <- chosen
#
# 2**47 is the instructive one: it orders a year-9999 heuristic against a 2026
# native correctly while NOT exceeding the span, so it is the same KIND of
# number as 2**40 and 2**45 -- right for the cases someone thought to check.
# 2**48 is the first correct by construction; 2**50 is chosen because x1.11 is
# the shape of margin that produced this finding in the first place.
# Arithmetic by lane-4752-go, checked independently here.
#
#     worst case 4*M + max = 4,757,001,928,170,495  (UInt64 max 1.8e19)
#     min case   1*M + min(1900-01-01) = 1,123,690,918,042,624, still positive
#
# The corpus carries keys at BOTH representable extremes. The harness image had
# to move to :26.7 (CHAOS-4854 / #2138) for that to mean anything: DateTime64(3)
# SATURATES at 2299 on 26.6.1.1193, so a year-9999 stamp clamped and the 2**45
# defect was unreachable in CI while live in production. On :26.7 the mutant is
# red on the default path.
VERSION_EXPRESSION = (
    "(multiIf("
    "provenance = 'native', 3, "
    "provenance = 'explicit_text', 2, "
    "provenance = 'heuristic', 1, "
    f"0) + 1) * {VERSION_MULTIPLIER} + toUnixTimestamp64Milli(last_synced)"
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

# TEST SEAM. None in production, and there is exactly one call site: between the
# snapshot key-count and the copy.
#
# The window between those two steps is the one a background merge can use to
# delete the older (native) row while the conservation check still passes, and
# it is not reachable deterministically from outside -- a pre-084 table with
# merges enabled may collapse a contested key at any instant, so "native still
# present, merges running, migration mid-snapshot" cannot be held open by
# arranging the fixture. The acceptance test sets this hook to force a merge
# exactly there instead of racing for it.
after_snapshot_hook = None
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


def _source_fingerprint(client, table: str) -> tuple[tuple[str, ...], int]:
    """The source's active part names and exact row count.

    Taken once after STOP MERGES and again before EXCHANGE. If it moved, a
    merge ran under the copy despite the guard, and on a contested key a merge
    deletes the OLDER row -- the native one. The conservation check cannot see
    that: one key remains one key.

    This exists because `SYSTEM STOP MERGES` is not a counter. Measured: two
    STOPs followed by ONE START re-enable merges. So any concurrent START --
    an operator, or another migration's own `finally` -- silently re-arms
    merges underneath this copy, and the guard alone cannot be trusted to have
    held for the whole window. Detection turns a silent fallback promotion into
    a loud refusal.
    """
    parts = client.query(
        "SELECT name FROM system.parts "
        "WHERE database = currentDatabase() AND table = {name:String} AND active "
        "ORDER BY name",
        parameters={"name": table},
    )
    rows = client.query(f"SELECT count() FROM `{table}`")
    part_names = tuple(str(r[0]) for r in (getattr(parts, "result_rows", None) or []))
    count_rows = getattr(rows, "result_rows", None) or []
    return part_names, int(count_rows[0][0]) if count_rows and count_rows[0] else 0


def _version_expression(client, table: str) -> str:
    """The MATERIALIZED expression actually stored for the version column."""
    res = client.query(
        "SELECT default_expression FROM system.columns "
        "WHERE database = currentDatabase() AND table = {name:String} "
        "AND name = {col:String}",
        parameters={"name": table, "col": VERSION_COLUMN},
    )
    rows = getattr(res, "result_rows", None) or []
    return str(rows[0][0]) if rows and rows[0] else ""


def _canonical_expression(client, expression: str) -> str:
    """What ClickHouse STORES for `expression`, obtained by round-tripping it.

    Text comparison cannot work here. ClickHouse canonicalises a MATERIALIZED
    expression when it stores it -- notably by adding grouping parentheses:

        given  (multiIf(...) + 1) * 35184372088832 + toUnixTimestamp64Milli(x)
        stored ((multiIf(...) + 1) * 35184372088832) + toUnixTimestamp64Milli(x)

    so a table THIS MIGRATION CREATED failed its own skip-path check on every
    rerun (codex round 3, P1). Normalising harder is not the fix: stripping
    parentheses would let genuinely different expressions compare equal, and
    the next canonicalisation change would break it again.

    Instead both sides are put through the SAME canonicaliser -- ClickHouse's
    own -- by creating a scratch table with the intended expression and reading
    back what `system.columns` holds for it.
    """
    scratch = f"{TABLE}_expr_probe"
    client.command(f"DROP TABLE IF EXISTS `{scratch}`")
    try:
        client.command(
            f"CREATE TABLE `{scratch}` (provenance String, "
            f"last_synced DateTime64(3, 'UTC'), "
            f"`{VERSION_COLUMN}` UInt64 MATERIALIZED {expression}) "
            f"ENGINE = MergeTree ORDER BY provenance"
        )
        return _version_expression(client, scratch)
    finally:
        client.command(f"DROP TABLE IF EXISTS `{scratch}`")


def _column_shape(client, table: str) -> list[tuple[str, str, str]]:
    """(name, type, default_kind) for a table, in position order."""
    res = client.query(
        "SELECT name, type, default_kind FROM system.columns "
        "WHERE database = currentDatabase() AND table = {name:String} "
        "ORDER BY position",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return [(str(r[0]), str(r[1]), str(r[2] or "")) for r in rows]


def _assert_carried_columns_match(client, table: str) -> None:
    """Refuse to run if the live table is not exactly what the copy names.

    `_copy` names a hardcoded column list. If a column is added to
    `work_graph_issue_pr` after this file is written, the shadow gets it (it is
    built from SHOW CREATE TABLE) but the copy does not name it, so EXCHANGE
    swaps in a table where that column is empty for every row and the original
    is dropped. Silent, total loss, no error at any step.

    The window is wide by design: prod apply is a separate per-op GO, deferred.
    So this fails CLOSED on any difference rather than trusting the constant.
    """
    live = [
        name for name, _, kind in _column_shape(client, table) if kind != "MATERIALIZED"
    ]
    if tuple(live) != tuple(CARRIED_COLUMNS):
        raise RuntimeError(
            f"{table} has stored columns {live!r} but this migration copies "
            f"{list(CARRIED_COLUMNS)!r}. A column was added or removed since this "
            "migration was written; copying anyway would silently drop it during "
            "EXCHANGE. Update CARRIED_COLUMNS and re-verify the acceptance test."
        )


def _assert_shadow_matches(client, table: str, shadow: str) -> None:
    """The shadow must be the source plus exactly the version column."""
    # Drop any EXISTING version column from the source shape before appending
    # the one the shadow must have. A partial run can leave the column present
    # while the engine is still ReplacingMergeTree(last_synced); without this,
    # `expected` would name version_rank TWICE, the check would abort, and the
    # rerun would drop the shadow and fail identically forever -- leaving the
    # precedence defect in place permanently (codex round 4, P2).
    expected = [
        column for column in _column_shape(client, table) if column[0] != VERSION_COLUMN
    ] + [(VERSION_COLUMN, "UInt64", "MATERIALIZED")]
    actual = _column_shape(client, shadow)
    if actual != expected:
        raise RuntimeError(
            f"{shadow} column shape {actual!r} is not {table}'s {expected!r}; "
            "the DDL rewrite changed something it should not have"
        )


def _copy(client, source: str, target: str) -> None:
    """Copy every version across, WITHOUT FINAL. See HAZARD 3.

    The column list is explicit rather than `SELECT *` so the copy does not
    depend on whether the version column is MATERIALIZED (absent from
    `SELECT *`) or DEFAULT (present).

    It does NOT survive column drift -- it is precisely what drift breaks, and
    an earlier version of this docstring claimed the opposite.
    `_assert_carried_columns_match` is what makes drift safe, by refusing to
    run at all.
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
        # Validate the EXPRESSION, not merely that the engine names the column.
        # A partial or manual run leaving `version_rank UInt64 MATERIALIZED 0`
        # would otherwise be skipped forever while keeping part-recency
        # behaviour -- migrated in name only. Codex round 2, P2.
        # The column must be MATERIALIZED, not merely expression-equal. A
        # DEFAULT column canonicalises identically but stays explicitly
        # WRITABLE, so a client can supply its own version -- the reviewer
        # showed UInt64 max letting heuristic beat native after a merge. Kind
        # first, because an accepted DEFAULT would record 084 as applied and
        # leave that door open (codex round 4, P2).
        kind = next(
            (
                k
                for name, _, k in _column_shape(client, TABLE)
                if name == VERSION_COLUMN
            ),
            "",
        )
        if kind != "MATERIALIZED":
            raise RuntimeError(
                f"{TABLE}.{VERSION_COLUMN} is {kind or 'an ordinary column'}, not "
                "MATERIALIZED. A DEFAULT column canonicalises the same but stays "
                "writable, so a caller could supply its own version and outrank "
                "native. Refusing to skip; resolve by hand."
            )
        actual = _version_expression(client, TABLE)
        if actual != _canonical_expression(client, VERSION_EXPRESSION):
            raise RuntimeError(
                f"{TABLE} is already versioned on {VERSION_COLUMN}, but its "
                f"expression is {actual!r}, not this migration's "
                f"{VERSION_EXPRESSION!r}. Refusing to skip: the table would keep "
                "whatever ranking that expression encodes. Resolve by hand."
            )
        log.info(f"  {TABLE}: already ReplacingMergeTree({VERSION_COLUMN})")
        if _table_exists(client, SHADOW):
            _catch_up_and_drop(client, TABLE, SHADOW)
        return

    if _table_exists(client, SHADOW):
        log.warning(f"  {SHADOW}: dropping a leftover pre-EXCHANGE shadow")
        client.command(f"DROP TABLE `{SHADOW}`")

    # Fail closed BEFORE building anything if the table is not what we copy.
    _assert_carried_columns_match(client, TABLE)

    # STOP MERGES ON THE SOURCE, and this is the difference between a migration
    # that promotes the native row and one that destroys it.
    #
    # The source is still `ReplacingMergeTree(last_synced)` while we snapshot
    # and copy it. A background merge landing after the key count but before
    # `_copy` physically removes the OLDER row -- which on a contested key is
    # exactly the native one -- and the conservation check cannot see it,
    # because one key remains one key. The fallback is then swapped in and the
    # original dropped (codex round 3, P1).
    #
    # The window is the migration's own, so the guard belongs here rather than
    # in whatever happens to be running around it. An earlier version of the
    # acceptance test stopped merges around its migration call, which
    # manufactured this protection and hid the defect.
    # The `finally` below does NOT undo this STOP, and the pairing is misleading
    # unless that is written down.
    #
    # Measured by lane-4752-go on a container: SYSTEM STOP MERGES follows the
    # STORAGE, not the name. Across EXCHANGE TABLES:
    #
    #     before EXCHANGE  OPTIMIZE <table>  -> Code 236 ABORTED   (stopped)
    #     after  EXCHANGE  OPTIMIZE <table>  -> collapses          (NOT stopped)
    #     after  EXCHANGE  OPTIMIZE <shadow> -> Code 236 ABORTED   (still stopped)
    #
    # So after the swap this name points at storage that was never stopped, and
    # the `finally` STARTs merges on that -- a no-op, not a restore. The stopped
    # storage is the OLD one, which the catch-up drops moments later, so nothing
    # leaks today.
    #
    # It is benign for a reason unrelated to the guard: the new table is
    # ReplacingMergeTree(version_rank), so any merge on it keeps the native row
    # by construction. WHOEVER REORDERS THIS -- moving the catch-up out of the
    # guarded window, or retaining the shadow -- loses that argument and leaves
    # a global STOP on live storage. Do not rely on the try/finally pairing.
    client.command(f"SYSTEM STOP MERGES `{TABLE}`")
    try:
        _rebuild_guarded(client)
    finally:
        client.command(f"SYSTEM START MERGES `{TABLE}`")


def _rebuild_guarded(client) -> None:
    """The rebuild proper, with the source's merges already stopped."""
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

    _assert_shadow_matches(client, TABLE, SHADOW)

    before = _distinct_key_count(client, TABLE, key_columns)
    fingerprint = _source_fingerprint(client, TABLE)
    if after_snapshot_hook is not None:
        after_snapshot_hook()
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

    # Refuse if the source moved under the copy. Checked BEFORE the swap, so a
    # failure leaves the original table untouched and the shadow disposable.
    moved = _source_fingerprint(client, TABLE)
    if moved != fingerprint:
        client.command(f"DROP TABLE IF EXISTS `{SHADOW}`")
        raise RuntimeError(
            f"{TABLE} changed between the snapshot and the copy: parts/rows went "
            f"{fingerprint!r} -> {moved!r}. A merge ran under the copy despite "
            "SYSTEM STOP MERGES (which is not a counter -- any concurrent START "
            "re-arms merges), so the copy may be missing the native rows this "
            "migration exists to promote. Refusing to EXCHANGE; the source is "
            "untouched. Re-run with writers and operator merges quiesced."
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
