"""Migration 055: Make work-item daily rollups idempotent under re-runs (CHAOS-2645).

``work_item_metrics_daily`` and ``work_item_user_metrics_daily`` are written
append-only by the work-item sync job. A sync that restarts from scratch
(today's Celery retry, or a rate-limit deferral re-enqueue from CHAOS-2644)
re-writes the same ``(grain, day)`` rows, leaving DUPLICATE versions that
flat-aggregating readers then double-count (inflated metrics).

This migration converts both tables in place from ``MergeTree`` to
``ReplacingMergeTree(computed_at)`` so duplicate versions of the same sorting
key collapse to the newest ``computed_at``. The ORDER BY (already org_id-first
after migration 027) is the dedup key and is preserved exactly; only the engine
changes. Reads must still use ``FINAL`` / argMax to see one row per key before
background merges run — that is handled in the application layer.

``work_item_state_durations_daily`` is intentionally NOT converted here: its
readers already deduplicate with ``argMax(metric, computed_at)`` grouped by the
natural key (with dedicated tests), so it does not double-count. A separate
storage-hygiene follow-up may convert it later.

Rebuild uses the same shadow-table pattern as migrations 027/042 (Altinity):

    1. SHOW CREATE TABLE to get the live DDL (preserves columns, partitioning,
       the existing ORDER BY, settings, codecs, TTL).
    2. Rewrite DDL: rename to ``<table>_new`` and swap the engine to
       ``ReplacingMergeTree(computed_at)``.
    3. Verify via system.tables that the shadow is ReplacingMergeTree AND that
       its sorting key is byte-for-byte the original key — abort (dropping the
       shadow) on any mismatch, so a regex miss fails closed.
    4. INSERT INTO <table>_new SELECT * FROM <table> (snapshot copy).
    5. Verify the distinct sorting-key tuple counts match between original and
       shadow (raw row counts may legitimately differ: the copy can collapse
       not-yet-merged same-key duplicate versions — that is exactly the bug we
       are fixing, not data loss).
    6. EXCHANGE TABLES <table> AND <table>_new (atomic swap).
    7. CATCH-UP: INSERT INTO <table> SELECT * FROM <table>_new — the shadow now
       holds the OLD table, including rows written between snapshot (4) and swap
       (6). Re-inserting is idempotent under RMT: already-copied rows dedup away
       on the key with the newest version winning; late rows survive.
    8. DROP TABLE <table>_new — only after the catch-up succeeded.

Crash convergence: if a run crashes after EXCHANGE but before catch-up/DROP, the
main table is already ReplacingMergeTree, so a rerun takes the skip path; that
path finishes the catch-up + DROP of any leftover ``<table>_new`` before
declaring done. A crash before EXCHANGE leaves a disposable shadow the next run
drops and recreates.

Ops note: run during a quiet period / with sync workers stopped — writes that
race the EXCHANGE resolve their version tie arbitrarily (benign: same logical
day, newest ``computed_at`` wins).

Idempotent: tables already ReplacingMergeTree are skipped (after converging any
leftover shadow).

NOTE: this file is loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from other
migration modules — helpers are intentionally duplicated from 027/042.
"""

import logging
import re

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tables to convert to ReplacingMergeTree(computed_at). The ORDER BY is left
# unchanged; ``computed_at`` is the RMT version column (newest wins).
# ---------------------------------------------------------------------------

TABLES = (
    "work_item_metrics_daily",
    "work_item_user_metrics_daily",
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
    """Parse 'org_id, provider, day, ...' into its column names."""
    return [c.strip() for c in sorting_key.strip("() ").split(",") if c.strip()]


def _distinct_key_count(client, table: str, key_columns: list[str]) -> int:
    """Count distinct sorting-key tuples (stable under RMT merges)."""
    key_tuple = ", ".join(f"`{c}`" for c in key_columns)
    res = client.query(f"SELECT uniqExact(({key_tuple})) FROM `{table}`")
    rows = getattr(res, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    """Post-EXCHANGE catch-up: re-insert the old table's rows, then drop it.

    After EXCHANGE TABLES, *shadow* holds the OLD table — including any rows
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
        # its catch-up/DROP. The leftover shadow then holds the OLD table —
        # finish the catch-up before skipping so post-snapshot writes survive.
        if _table_exists(client, shadow):
            log.info(
                f"  {table}: already ReplacingMergeTree but leftover `{shadow}` "
                f"found — converging interrupted run"
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
        # a different key would silently collapse distinct rows).
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

        # Verify no logical rows were lost before swapping. Raw row counts may
        # legitimately differ (the RMT copy collapses not-yet-merged duplicate
        # versions of the SAME key — exactly the dedup we want), so compare the
        # number of distinct sorting-key tuples, which must be identical.
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
    """Convert work-item daily rollups to ReplacingMergeTree(computed_at)."""
    log.info("=== Migration 055: idempotent work-item daily rollups (CHAOS-2645) ===")

    total = len(TABLES)
    for i, table in enumerate(TABLES, 1):
        log.info(f"[{i}/{total}] {table}")
        try:
            _rebuild_table(client, table)
        except Exception as exc:
            # No blanket shadow cleanup: pre-EXCHANGE failures already dropped
            # their disposable shadow inside _rebuild_table; a post-EXCHANGE
            # failure leaves `<table>_new` holding the OLD data — dropping it
            # would lose the catch-up delta. The rerun skip path converges it.
            log.error(f"FAILED on {table}: {exc}")
            raise

    log.info("=== Migration 055: Complete ===")
