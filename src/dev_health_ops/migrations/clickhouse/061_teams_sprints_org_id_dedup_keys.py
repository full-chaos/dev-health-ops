"""Migration 061: Rebuild teams/sprints RMT keys with org_id first (CHAOS-2735).

ReplacingMergeTree deduplicates rows that share the same ORDER BY sorting-key
tuple. The ``teams`` table was originally created with ``ORDER BY (id)`` and
``sprints`` with ``ORDER BY (provider, sprint_id)`` before migration 024 added
``org_id`` to both tables. If two tenants share one of those natural keys,
FINAL/background dedup can collapse their rows together unless ``org_id`` is
part of the sorting key.

This is defense-in-depth: the current readers already use org-filtered argMax
patterns where required, but the storage layer itself must not be able to merge
logical rows across tenants. ClickHouse cannot ALTER an existing
ReplacingMergeTree ORDER BY key, so the only safe repair is a shadow-table
rebuild:

    1. SHOW CREATE TABLE to get full DDL, preserving all columns/settings and
       any schema added by later migrations.
    2. Modify DDL: rename to ``<table>_new`` and prepend org_id to ORDER BY.
    3. Verify via system.tables that the shadow's sorting key is exactly
       ``org_id, <old sorting key>``. Any mismatch aborts and drops the shadow
       before copying data.
    4. INSERT INTO <table>_new SELECT * FROM <table> for the snapshot copy.
    5. Verify distinct new-key tuple counts match between source and shadow.
       Raw row counts can differ under normal RMT semantics, so compare logical
       sorting-key tuples instead.
    6. EXCHANGE TABLES <table> AND <table>_new for an atomic swap.
    7. CATCH-UP: INSERT INTO <table> SELECT * FROM <table>_new so rows written
       between the snapshot and swap are re-inserted into the rebuilt table.
    8. DROP TABLE <table>_new only after catch-up succeeds.

Crash convergence: if a run crashes after EXCHANGE but before catch-up/DROP,
the main table already has the org_id-first key. A rerun takes the skip path,
detects the leftover shadow, performs catch-up, and drops it. A crash before
EXCHANGE leaves a disposable shadow that the next run drops and recreates.

Idempotent: tables already having org_id first in ORDER BY are skipped after
converging any leftover shadow.

NOTE: this file is loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from sibling
migration modules. Helpers are intentionally duplicated from migration 042.
"""

import logging
import re

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table catalog: table_name -> new ORDER BY with org_id prepended
# ---------------------------------------------------------------------------

TABLES = {
    "teams": "(org_id, id)",
    "sprints": "(org_id, provider, sprint_id)",
}

# ---------------------------------------------------------------------------
# Helpers (duplicated from migration 042 — see module docstring)
# ---------------------------------------------------------------------------

# Regex: ORDER BY (col, col, ...) | ORDER BY tuple(col, ...) | ORDER BY col
_ORDER_BY_RE = re.compile(r"ORDER BY\s+(?:tuple\([^)]+\)|\([^)]+\)|\S+)", re.IGNORECASE)

# Regex: org_id column definition (plain String or LowCardinality(String))
_ORG_ID_COL_RE = re.compile(
    r"`?org_id`?\s+(?:LowCardinality\(\s*)?String", re.IGNORECASE
)


def _table_name_re(table: str) -> re.Pattern:
    """Regex for the table name in a CREATE TABLE statement."""
    return re.compile(
        rf"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        rf"(?:`?[\w\d_]+`?\.)?`?){re.escape(table)}(`?\s|`?\()",
        re.IGNORECASE,
    )


def _has_org_id_first_in_order_by(ddl: str) -> bool:
    """Check if org_id is already the first column in the ORDER BY clause."""
    match = _ORDER_BY_RE.search(ddl)
    if not match:
        return False
    order_clause = match.group(0)
    return bool(
        re.search(r"ORDER BY\s+(?:tuple)?\(?\s*`?org_id`?", order_clause, re.IGNORECASE)
    )


def _replace_order_by(ddl: str, new_order_by: str) -> str:
    """Replace the ORDER BY clause in a CREATE TABLE DDL string."""
    result, count = _ORDER_BY_RE.subn(f"ORDER BY {new_order_by}", ddl, count=1)
    if count == 0:
        raise ValueError(f"Could not find ORDER BY in DDL: {ddl[:300]}...")
    return result


def _replace_table_name(ddl: str, old_name: str, new_name: str) -> str:
    """Replace the table name in a CREATE TABLE DDL string."""
    pattern = _table_name_re(old_name)
    result, count = pattern.subn(rf"\g<1>{new_name}\g<2>", ddl, count=1)
    if count == 0:
        raise ValueError(
            f"Could not replace table name '{old_name}' in DDL: {ddl[:300]}..."
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


def _distinct_key_count(client, table: str, key_columns: list[str]) -> int:
    """Count distinct sorting-key tuples (stable under RMT merges)."""
    key_tuple = ", ".join(f"`{c}`" for c in key_columns)
    res = client.query(f"SELECT uniqExact(({key_tuple})) FROM `{table}`")
    rows = getattr(res, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _key_columns(new_order_by: str) -> list[str]:
    """Parse '(org_id, repo_id, ...)' into its column names."""
    return [c.strip() for c in new_order_by.strip("() ").split(",") if c.strip()]


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


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    """Post-EXCHANGE catch-up: re-insert the old table's rows, then drop it.

    After EXCHANGE TABLES, *shadow* holds the OLD table, including any rows
    written between the snapshot copy and the swap. Re-inserting ALL of its
    rows is idempotent under ReplacingMergeTree: rows already present dedup
    away on the (org_id-first) key with the newest version winning, while
    late-written rows survive. Only after the catch-up succeeds is the old
    table dropped. On failure the shadow is left in place so a rerun can
    converge.
    """
    log.info(f"  {table}: catch-up copy of post-snapshot writes from `{shadow}`")
    client.command(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`")
    client.command(f"DROP TABLE `{shadow}`")


def _rebuild_table(client, table: str, new_order_by: str) -> None:
    """Rebuild a single table with org_id prepended to its ORDER BY."""
    shadow = f"{table}_new"

    if not _table_exists(client, table):
        log.warning(f"  {table}: table does not exist, skipping")
        return

    res = client.query(f"SHOW CREATE TABLE `{table}`")
    ddl = res.result_rows[0][0]

    if _has_org_id_first_in_order_by(ddl):
        # Convergence: a previous run may have crashed after EXCHANGE but
        # before its catch-up/DROP. The leftover shadow then holds the OLD
        # table. Finish the catch-up before skipping.
        if _table_exists(client, shadow):
            log.info(
                f"  {table}: org_id already first in ORDER BY but leftover "
                f"`{shadow}` found — converging interrupted run"
            )
            _catch_up_and_drop(client, table, shadow)
        else:
            log.info(f"  {table}: org_id already first in ORDER BY, skipping")
        return

    if not _ORG_ID_COL_RE.search(ddl):
        raise ValueError(
            f"{table}: org_id column not found in DDL; cannot add it to the "
            f"sorting key. DDL: {ddl[:300]}..."
        )

    new_ddl = _replace_table_name(ddl, table, shadow)
    new_ddl = _replace_order_by(new_ddl, new_order_by)

    log.info(f"  {table}: creating shadow table")
    client.command(f"DROP TABLE IF EXISTS `{shadow}`")
    client.command(new_ddl)

    # Everything before EXCHANGE is safely retryable: on any failure drop the
    # disposable pre-swap shadow and re-raise. After EXCHANGE the shadow holds
    # real data and must NOT be dropped without a catch-up.
    try:
        old_key = _normalize_sorting_key(_sorting_key(client, table))
        shadow_key = _normalize_sorting_key(_sorting_key(client, shadow))
        expected_key = f"org_id, {old_key}"
        if shadow_key != expected_key:
            raise RuntimeError(
                f"{table}: shadow sorting key mismatch after DDL rewrite "
                f"(expected {expected_key!r}, got {shadow_key!r}); aborting"
            )

        log.info(f"  {table}: copying data")
        client.command(f"INSERT INTO `{shadow}` SELECT * FROM `{table}`")

        key_columns = _key_columns(new_order_by)
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

    _catch_up_and_drop(client, table, shadow)

    log.info(f"  {table}: done")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def upgrade(client):
    """Rebuild teams/sprints RMT dedup keys with org_id first (CHAOS-2735)."""
    log.info("=== Migration 061: org_id in teams/sprints RMT keys (CHAOS-2735) ===")

    total = len(TABLES)
    for i, (table, new_order_by) in enumerate(TABLES.items(), 1):
        log.info(f"[{i}/{total}] {table}")
        try:
            _rebuild_table(client, table, new_order_by)
        except Exception as exc:
            # No blanket shadow cleanup here: pre-EXCHANGE failures already
            # dropped their disposable shadow inside _rebuild_table, while a
            # post-EXCHANGE failure leaves `<table>_new` holding the OLD
            # table's data. Dropping it would lose the catch-up delta. The
            # rerun skip path converges any such leftover.
            log.error(f"FAILED on {table}: {exc}")
            raise

    log.info("=== Migration 061: Complete ===")
