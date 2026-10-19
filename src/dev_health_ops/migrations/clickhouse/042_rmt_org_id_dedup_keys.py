"""Migration 042: Ensure org_id is in every ReplacingMergeTree dedup key (CHAOS-2290).

ReplacingMergeTree deduplicates rows that share the same ORDER BY (sorting
key) tuple. When org_id is a column but NOT part of the sorting key,
identical natural keys across two tenants (e.g. the same ``(repo_id,
work_item_id)`` under two different org_ids) collapse into ONE row on a
background merge — silent cross-tenant data loss.

Migration 027 fixed this for the tables that existed at the time (including
``work_items`` / ``work_item_transitions`` from 009_raw_work_items.sql, the
tables named by CHAOS-2290). However:

* 029_testops_tables.sql created ``ci_job_runs``, ``test_suite_results``,
  ``test_case_results`` and ``coverage_snapshots`` with org_id as a column
  but ``ORDER BY`` keyed only on ``(repo_id, ...)``.
* 032_security_alerts.sql created ``security_alerts`` with
  ``ORDER BY (repo_id, alert_id)``; 033 added the org_id column but could
  not fix the sorting key (ClickHouse cannot ALTER an existing RMT key).

This migration rebuilds those tables with org_id prepended to the sorting
key using the same shadow-table pattern as migration 027 (Altinity pattern):

    1. SHOW CREATE TABLE to get full DDL (preserves settings, indexes, the
       RMT version column, partitioning, etc.)
    2. Modify DDL: rename to ``<table>_new``, prepend org_id to ORDER BY
    3. Verify via system.tables that the shadow's sorting key is exactly
       ``org_id, <old sorting key>`` — abort (and drop the shadow) on any
       mismatch, so a regex miss on exotic DDL fails closed instead of
       silently shipping the wrong dedup key
    4. INSERT INTO <table>_new SELECT * FROM <table> (snapshot copy)
    5. Verify the distinct new-key tuple counts match between original and
       shadow (raw row counts may legitimately differ: the copy can collapse
       not-yet-merged same-key duplicate versions, which is RMT semantics,
       not data loss)
    6. EXCHANGE TABLES <table> AND <table>_new (atomic swap)
    7. CATCH-UP: INSERT INTO <table> SELECT * FROM <table>_new — the shadow
       now holds the OLD table, including any rows written between the
       snapshot (step 4) and the swap (step 6). Re-inserting ALL old rows is
       idempotent under RMT semantics: rows already copied dedup away on the
       (now org_id-first) key with the newest version winning, while rows
       that landed after the snapshot survive. This shrinks the write-loss
       window from the whole copy duration to the EXCHANGE instant, which is
       atomic.
    8. DROP TABLE <table>_new — only after the catch-up succeeded.

Residual operational risk: writes that race the EXCHANGE itself can target
either side of the swap; both sides are preserved (catch-up re-inserts the
old side), but version ties on the same key resolve arbitrarily. Ops note:
run this migration during a quiet period / with sync workers stopped.

Crash convergence: if a run crashes after EXCHANGE but before the catch-up
or DROP, the main table already has the org_id-first key, so a rerun takes
the idempotency-skip path. That path checks for a leftover ``<table>_new``
shadow and, if present, performs the catch-up INSERT + DROP before
declaring the table done — no rows stranded, no shadow left behind. A crash
*before* EXCHANGE leaves a disposable shadow that the next run drops and
recreates.

``work_items`` and ``work_item_transitions`` are included defensively: on
any database where migration 027 already ran they are skipped by the
idempotency check (org_id already first in ORDER BY), but a database whose
027 run was interrupted gets repaired here.

Idempotent: tables already having org_id first in ORDER BY are skipped
(after converging any leftover shadow, see above).

NOTE: this file is loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from other
migration modules — helpers are intentionally duplicated from 027.
"""

import logging
import re

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table catalog: table_name -> new ORDER BY with org_id prepended
# ---------------------------------------------------------------------------

TABLES = {
    # --- CHAOS-2290 headline tables (009_raw_work_items.sql) ---
    # Already rebuilt by migration 027 on healthy databases; kept here as an
    # idempotent safety net.
    "work_items": "(org_id, repo_id, work_item_id)",
    "work_item_transitions": "(org_id, repo_id, work_item_id, occurred_at)",
    # --- TestOps raw tables (029_testops_tables.sql) ---
    "ci_job_runs": "(org_id, repo_id, run_id, job_id)",
    "test_suite_results": "(org_id, repo_id, run_id, suite_id)",
    "test_case_results": "(org_id, repo_id, run_id, suite_id, case_id)",
    "coverage_snapshots": "(org_id, repo_id, run_id, snapshot_id)",
    # --- Security alerts (032_security_alerts.sql / 033 org_id column) ---
    "security_alerts": "(org_id, repo_id, alert_id)",
}

# ---------------------------------------------------------------------------
# Helpers (duplicated from migration 027 — see module docstring)
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

    After EXCHANGE TABLES, *shadow* holds the OLD table — including any rows
    written between the snapshot copy and the swap. Re-inserting ALL of its
    rows is idempotent under ReplacingMergeTree: rows already present dedup
    away on the (org_id-first) key with the newest version winning, while
    late-written rows survive. Only after the catch-up succeeds is the old
    table dropped; on failure the shadow is left in place so a rerun can
    converge (see the skip path in _rebuild_table).
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
        # table (shadows are only created while the main table still lacks
        # the org_id-first key) — finish the catch-up before skipping so the
        # post-snapshot writes inside it are not lost.
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
        # All targeted tables gained org_id in 024/029/033; if it is missing
        # the database is in an unexpected state — fail loudly rather than
        # guess a column type.
        raise ValueError(
            f"{table}: org_id column not found in DDL; cannot add it to the "
            f"sorting key. DDL: {ddl[:300]}..."
        )

    new_ddl = _replace_table_name(ddl, table, shadow)
    new_ddl = _replace_order_by(new_ddl, new_order_by)

    log.info(f"  {table}: creating shadow table")
    client.command(f"DROP TABLE IF EXISTS `{shadow}`")
    client.command(new_ddl)

    # Everything before EXCHANGE is safely retryable: on any failure drop
    # the (disposable, pre-swap) shadow and re-raise. After EXCHANGE the
    # shadow holds real data and must NOT be dropped without a catch-up.
    try:
        # Fail closed on DDL-rewrite misses: the regex rewrite cannot be
        # trusted on arbitrary DDL (nested expressions like cityHash64(...)
        # in ORDER BY), so verify the *actual* shadow sorting key is exactly
        # `org_id, <old key>` before copying any data.
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

        # Verify no logical rows were lost before swapping. Raw row counts
        # may legitimately differ (the copy can collapse not-yet-merged
        # duplicate versions of the SAME key — normal RMT semantics), so
        # compare the number of distinct new-key tuples instead, which must
        # be identical: the new key is a superset of the old one, so no two
        # source rows that differ on it can ever merge.
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

    # From here on the shadow is the OLD table; never drop it without the
    # catch-up. If this fails, the rerun skip path converges it.
    _catch_up_and_drop(client, table, shadow)

    log.info(f"  {table}: done")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def upgrade(client):
    """Rebuild RMT tables whose dedup key is missing org_id (CHAOS-2290)."""
    log.info("=== Migration 042: org_id in RMT dedup keys (CHAOS-2290) ===")

    total = len(TABLES)
    for i, (table, new_order_by) in enumerate(TABLES.items(), 1):
        log.info(f"[{i}/{total}] {table}")
        try:
            _rebuild_table(client, table, new_order_by)
        except Exception as exc:
            # No blanket shadow cleanup here: pre-EXCHANGE failures already
            # dropped their disposable shadow inside _rebuild_table, while a
            # post-EXCHANGE failure leaves `<table>_new` holding the OLD
            # table's data — dropping it would lose the catch-up delta. The
            # rerun skip path converges any such leftover.
            log.error(f"FAILED on {table}: {exc}")
            raise

    log.info("=== Migration 042: Complete ===")
