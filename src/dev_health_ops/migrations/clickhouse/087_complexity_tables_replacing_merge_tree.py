"""Migration 087: make the complexity tables idempotent under re-runs (CHAOS-4291).

``file_complexity_snapshots``, ``repo_complexity_daily`` and
``team_complexity_daily`` are written append-only: the sink calls a plain
``_insert_rows`` (``metrics/sinks/clickhouse/work_graph.py:455,480`` and
``sinks/clickhouse/team_complexity.py:33``) and nothing in
``job_complexity_db.py`` issues an ALTER, a DELETE, or a partition drop. Every
re-run of a day therefore ADDS a second full set of rows for the same key
rather than superseding the first, and flat-aggregating readers double-count.

``internal/jobs/metrics/remaining/families.json`` already declares this family
``"replay": "generation_replace"``. That contract is not implemented anywhere:
neither table carries a generation column, so there is nothing for a
"generation replace" to key on. This migration makes the declared contract
true using the engine, which is the only mechanism available -- ClickHouse has
no compare-and-set, and adding a generation column plus a reader-side
discriminator would push correctness onto every consumer of three tables.

MEASURED DUPLICATION on the local stack immediately before this migration was
written (scoped to `default`; these tables also exist in seven ci_*/acr_*
scratch databases, where the counts are meaningless):

    repo_complexity_daily        5,271 raw /   769 distinct (org_id, repo_id, day)  = 6.85x
    file_complexity_snapshots  4,071,107 / 677,856 (org_id, repo_id, as_of_day, file_path) = 6.01x
    team_complexity_daily          560 /    28 distinct (org_id, team_id, day)      = 20.0x

Those ratios are STABLE, not an artefact of pending merges: plain MergeTree
performs no key-collapse at all, and two reads twenty seconds apart returned
identical counts. (This is the opposite of a ReplacingMergeTree table, whose
raw count falls continuously as background merges run and must never be
differenced to derive a rate -- CHAOS-5046.)

VERSION COLUMN AND ITS ONE ACCEPTED TIE. ``computed_at`` is the RMT version on
all three tables. It is ``DateTime`` (one-second resolution) on
``file_complexity_snapshots`` and ``repo_complexity_daily``, and
``DateTime64(6, 'UTC')`` on ``team_complexity_daily``. Two writes of the SAME
key inside one second therefore tie on the two second-resolution tables, and
ClickHouse resolves a tie arbitrarily. This migration deliberately does NOT
widen those columns: migration 055 changed the engine only and accepted the
identical tie ("benign: same logical day, newest computed_at wins"), and a
column-type change is a wire-contract change that would reach every Python
writer and reader of these tables. Two runs of a daily partition landing in
the same second is implausible, and when it happens the tied rows are
near-identical by construction -- same day, same inputs, same scan bound.

Readers must still use ``FINAL`` (or ``argMax(..., computed_at)``) to see one
row per key before background merges run; the engine guarantees eventual
collapse, not immediate collapse. That is handled in the application layer.

Rebuild uses the shadow-table pattern from migrations 027/042/055 (Altinity):

    1. SHOW CREATE TABLE for the live DDL (preserves columns, partitioning,
       the existing ORDER BY, settings, codecs, TTL).
    2. Rewrite: rename to ``<table>_new``, swap the engine to
       ``ReplacingMergeTree(computed_at)``.
    3. Verify via system.tables that the shadow is ReplacingMergeTree AND that
       its sorting key is byte-for-byte the original -- abort (dropping the
       shadow) on any mismatch, so a regex miss fails closed.
    4. INSERT INTO <table>_new SELECT * FROM <table> (snapshot copy).
    5. Verify the DISTINCT sorting-key tuple counts match. Raw counts will NOT
       match here and must not be compared: the copy collapses the 6-20x
       duplication above, which is the entire point.
    6. EXCHANGE TABLES <table> AND <table>_new (atomic swap).
    7. CATCH-UP: INSERT INTO <table> SELECT * FROM <table>_new -- the shadow now
       holds the OLD table, including rows written between snapshot and swap.
       Idempotent under RMT: already-copied rows dedup on the key, late rows
       survive.
    8. DROP TABLE <table>_new -- only after the catch-up succeeded.

The sorting keys are read from system.tables rather than from migration 007's
CREATE TABLE, because they have been amended since (007 predates the org_id
prefix). At the time of writing they are org_id-first on all three tables --
(org_id, repo_id, as_of_day, file_path), (org_id, repo_id, day) and
(org_id, team_id, day) -- so no two tenants' rows can ever collapse into one.
Step 3 re-asserts that per run rather than trusting this note.

Crash convergence: a run that dies after EXCHANGE but before catch-up/DROP
leaves the main table already ReplacingMergeTree, so a rerun takes the skip
path, which finishes the catch-up + DROP of any leftover ``<table>_new`` first.
A crash before EXCHANGE leaves a disposable shadow the next run drops.

Ops note: run during a quiet period, or with the metrics workers stopped --
writes racing the EXCHANGE resolve their version tie arbitrarily.

Idempotent: tables already ReplacingMergeTree are skipped, after converging any
leftover shadow.

NOTE: this file is loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from other
migration modules -- the helpers below are intentionally duplicated from
027/042/055.
"""

import logging
import re

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tables to convert to ReplacingMergeTree(computed_at). The ORDER BY is left
# unchanged; ``computed_at`` is the RMT version column (newest wins).
# ---------------------------------------------------------------------------

TABLES = (
    "file_complexity_snapshots",
    "repo_complexity_daily",
    "team_complexity_daily",
)

RMT_VERSION_COLUMN = "computed_at"

# Match a plain MergeTree engine clause (NOT ReplacingMergeTree / others):
# ``ENGINE = MergeTree`` immediately followed by a non-identifier char.
_ENGINE_RE = re.compile(r"ENGINE\s*=\s*MergeTree\b", re.IGNORECASE)


def _table_name_re(table: str) -> re.Pattern:
    """Regex for the table name in a CREATE TABLE statement."""
    return re.compile(
        rf"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        rf"(?:`?[\w\d_]+`?\.)?`?){re.escape(table)}(`?\s|`?\()",
        re.IGNORECASE,
    )


def _replace_table_name(ddl: str, old_name: str, new_name: str) -> str:
    """Replace the table name in a CREATE TABLE DDL string."""
    pattern = _table_name_re(old_name)
    result, count = pattern.subn(rf"\g<1>{new_name}\g<2>", ddl, count=1)
    if count == 0:
        raise ValueError(
            f"Could not replace table name '{old_name}' in DDL: {ddl[:300]}..."
        )
    return result


def _replace_engine_with_rmt(ddl: str) -> str:
    """Swap a plain ``MergeTree`` engine for ``ReplacingMergeTree(computed_at)``."""
    result, count = _ENGINE_RE.subn(
        f"ENGINE = ReplacingMergeTree({RMT_VERSION_COLUMN})", ddl, count=1
    )
    if count == 0:
        raise ValueError(
            f"Could not find a plain 'ENGINE = MergeTree' clause in DDL: {ddl[:300]}..."
        )
    return result


def _table_exists(client, table: str) -> bool:
    try:
        res = client.query(
            "SELECT count() FROM system.tables "
            "WHERE database = currentDatabase() AND name = {name:String}",
            parameters={"name": table},
        )
        rows = getattr(res, "result_rows", None) or []
        return bool(rows and rows[0] and rows[0][0] > 0)
    except Exception:
        return False


def _engine_name(client, table: str) -> str:
    res = client.query(
        "SELECT engine FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return str(rows[0][0]) if rows and rows[0] else ""


def _has_column(client, table: str, column: str) -> bool:
    res = client.query(
        "SELECT count() FROM system.columns "
        "WHERE database = currentDatabase() AND table = {t:String} AND name = {c:String}",
        parameters={"t": table, "c": column},
    )
    rows = getattr(res, "result_rows", None) or []
    return bool(rows and rows[0] and rows[0][0] > 0)


def _normalize_sorting_key(key: str) -> str:
    """Normalize a sorting key for string comparison (backticks, spacing)."""
    return re.sub(r"\s*,\s*", ", ", re.sub(r"\s+", " ", key.replace("`", ""))).strip(
        " ()"
    )


def _sorting_key(client, table: str) -> str:
    """Read a table's actual sorting key from system.tables."""
    res = client.query(
        "SELECT sorting_key FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    if not rows or not rows[0]:
        raise RuntimeError(f"{table}: could not read sorting_key from system.tables")
    return str(rows[0][0])


def _key_columns(sorting_key: str) -> list[str]:
    """Parse 'org_id, repo_id, day, ...' into its column names."""
    return [c.strip() for c in sorting_key.strip("() ").split(",") if c.strip()]


def _distinct_key_count(client, table: str, key_columns: list[str]) -> int:
    """Count distinct sorting-key tuples (stable under RMT merges)."""
    key_tuple = ", ".join(f"`{c}`" for c in key_columns)
    res = client.query(f"SELECT uniqExact(({key_tuple})) FROM `{table}`")
    rows = getattr(res, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    """Post-EXCHANGE catch-up: re-insert the old table's rows, then drop it.

    After EXCHANGE TABLES, *shadow* holds the OLD table -- including any rows
    written between the snapshot copy and the swap. Re-inserting ALL of its
    rows is idempotent under ReplacingMergeTree: rows already present dedup away
    on the key with the newest version winning, while late-written rows survive.
    Only after the catch-up succeeds is the old table dropped; on failure the
    shadow is left in place so a rerun can converge (see the skip path).
    """
    log.info(f"  {table}: catch-up copy of post-snapshot writes from `{shadow}`")
    client.command(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`")
    client.command(f"DROP TABLE `{shadow}`")


def _rebuild_table(client, table: str) -> None:
    """Convert a single table to ReplacingMergeTree(computed_at) in place."""
    shadow = f"{table}_new"

    if not _table_exists(client, table):
        log.warning(f"  {table}: table does not exist, skipping")
        return

    if _engine_name(client, table) == "ReplacingMergeTree":
        # Convergence: a previous run may have crashed after EXCHANGE but before
        # its catch-up/DROP. The leftover shadow then holds the OLD table --
        # finish the catch-up before skipping so post-snapshot writes survive.
        if _table_exists(client, shadow):
            log.info(
                f"  {table}: already ReplacingMergeTree but leftover `{shadow}` "
                f"found -- converging interrupted run"
            )
            _catch_up_and_drop(client, table, shadow)
        else:
            log.info(f"  {table}: already ReplacingMergeTree, skipping")
        return

    if not _has_column(client, table, RMT_VERSION_COLUMN):
        raise ValueError(
            f"{table}: required RMT version column '{RMT_VERSION_COLUMN}' not "
            f"found; cannot convert to ReplacingMergeTree"
        )

    res = client.query(f"SHOW CREATE TABLE `{table}`")
    ddl = res.result_rows[0][0]

    new_ddl = _replace_table_name(ddl, table, shadow)
    new_ddl = _replace_engine_with_rmt(new_ddl)

    log.info(f"  {table}: creating shadow table")
    client.command(f"DROP TABLE IF EXISTS `{shadow}`")
    client.command(new_ddl)

    # Everything before EXCHANGE is safely retryable: on any failure drop the
    # (disposable, pre-swap) shadow and re-raise. After EXCHANGE the shadow
    # holds real data and must NOT be dropped without a catch-up.
    try:
        # Fail closed if the DDL rewrite did not produce the intended engine,
        # or if it disturbed the sorting key (the dedup key MUST be unchanged:
        # a different key would silently collapse distinct rows -- and on these
        # three tables the key is what keeps two tenants apart).
        if _engine_name(client, shadow) != "ReplacingMergeTree":
            raise RuntimeError(
                f"{table}: shadow engine is not ReplacingMergeTree after rewrite; "
                f"aborting"
            )
        old_key = _normalize_sorting_key(_sorting_key(client, table))
        shadow_key = _normalize_sorting_key(_sorting_key(client, shadow))
        if shadow_key != old_key:
            raise RuntimeError(
                f"{table}: shadow sorting key changed during engine swap "
                f"(expected {old_key!r}, got {shadow_key!r}); aborting"
            )

        log.info(f"  {table}: copying data")
        client.command(f"INSERT INTO `{shadow}` SELECT * FROM `{table}`")

        # Verify no logical rows were lost before swapping. Raw row counts WILL
        # differ here -- the RMT copy collapses the 6-20x append duplication
        # this migration exists to end -- so compare the number of distinct
        # sorting-key tuples, which must be identical.
        key_columns = _key_columns(old_key)
        src_keys = _distinct_key_count(client, table, key_columns)
        dst_keys = _distinct_key_count(client, shadow, key_columns)
        if dst_keys != src_keys:
            raise RuntimeError(
                f"{table}: shadow copy distinct-key mismatch "
                f"(source={src_keys}, shadow={dst_keys}); aborting before swap"
            )
        log.info(
            f"  {table}: distinct sorting-key tuples verified "
            f"(source={src_keys}, shadow={dst_keys})"
        )
    except Exception:
        try:
            client.command(f"DROP TABLE IF EXISTS `{shadow}`")
        except Exception as cleanup_err:  # pragma: no cover - best effort
            log.warning(f"  {table}: shadow table cleanup failed: {cleanup_err}")
        raise

    log.info(f"  {table}: atomic swap via EXCHANGE TABLES")
    client.command(f"EXCHANGE TABLES `{table}` AND `{shadow}`")

    # From here on the shadow is the OLD table; never drop it without the
    # catch-up. If this fails, the rerun skip path converges it.
    _catch_up_and_drop(client, table, shadow)

    log.info(f"  {table}: done")


def upgrade(client):
    """Convert the complexity tables to ReplacingMergeTree(computed_at)."""
    log.info("=== Migration 087: idempotent complexity tables (CHAOS-4291) ===")

    total = len(TABLES)
    for i, table in enumerate(TABLES, 1):
        log.info(f"[{i}/{total}] {table}")
        try:
            _rebuild_table(client, table)
        except Exception as exc:
            # No blanket shadow cleanup: pre-EXCHANGE failures already dropped
            # their disposable shadow inside _rebuild_table; a post-EXCHANGE
            # failure leaves `<table>_new` holding the OLD data -- dropping it
            # would lose the catch-up delta. The rerun skip path converges it.
            log.error(f"FAILED on {table}: {exc}")
            raise

    log.info("=== Migration 087: Complete ===")
