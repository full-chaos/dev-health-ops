"""Migration 088: make release_impact_daily idempotent under re-runs (CHAOS-4296).

``release_impact_daily`` is written append-only by both the Python job and the
native Go executor's recomputation window (the last N days are recomputed on
every run, per the module docstring in release_impact.py). Nothing DELETEs the
table, so a re-run within that window leaves DUPLICATE versions of the same
``(org_id, release_ref, environment, day)`` row -- the writer's docstring has
claimed "argMax on computed_at wins" since it was written, but no reader
anywhere (query-api analytics, ff_validation, GraphQL flag metrics, workerctl)
actually implements that; see release_impact_native.go's package doc for the
full audit. Duplication is currently 1.0x only because the family has produced
nothing since 2026-08-27 (CHAOS-4256 is open) -- it becomes real the moment
that recompute window overlaps a live day.

This migration converts the table in place from ``MergeTree`` to
``ReplacingMergeTree(computed_at)`` so duplicate versions of the same sorting
key collapse to the newest ``computed_at`` on merge. The ORDER BY
``(org_id, release_ref, environment, day)`` (migration 034) is the dedup key
and is preserved exactly; only the engine changes. THIS MIGRATION DOES NOT FIX
READS: a SELECT without FINAL can still observe duplicates between merges, and
release_impact_daily is deliberately unregistered in query-api's read-side
dedup registry (CHAOS-4536, tracked separately, not this PR).

Rebuild uses the same shadow-table pattern as migrations 027/042/055
(Altinity):

    1. SHOW CREATE TABLE to get the live DDL.
    2. Rewrite DDL: rename to ``release_impact_daily_new``, swap the engine to
       ``ReplacingMergeTree(computed_at)``.
    3. Verify via system.tables that the shadow is ReplacingMergeTree AND that
       its sorting key is byte-for-byte the original key -- abort (dropping
       the shadow) on any mismatch.
    4. INSERT INTO release_impact_daily_new SELECT * FROM release_impact_daily
       (snapshot copy).
    5. Verify the distinct sorting-key tuple count matches between original
       and shadow. PLUS A CALLER-SIDE count() CROSS-CHECK (new in 088): 055's
       ``_distinct_key_count`` reads ``uniqExact(...)`` and returns 0 whenever
       the result set comes back empty, which is indistinguishable from "the
       table is genuinely empty" -- a source and shadow that both fail their
       uniqExact read the same way would compare 0 == 0 and pass with nothing
       verified. This migration additionally reads ``count()`` on the SAME two
       tables and fails closed if the raw row count says data exists but the
       distinct-key read claimed zero -- so a masked read failure cannot hide
       behind an empty-looking comparison.
    6. EXCHANGE TABLES release_impact_daily AND release_impact_daily_new
       (atomic swap).
    7. CATCH-UP: INSERT INTO release_impact_daily SELECT * FROM
       release_impact_daily_new -- the shadow now holds the OLD table,
       including rows written between snapshot (4) and swap (6). Re-inserting
       is idempotent under RMT.
    8. DROP TABLE release_impact_daily_new -- only after catch-up succeeded.

NO PROJECTION GUARD: team-lead ruled DDL-history evidence sufficient (checked
2026-09-05 -- only migration 034 ever touches this table, no
``ALTER ... ADD PROJECTION`` anywhere in history) and explicitly declined a
``deduplicate_merge_projection_mode`` setting for 088. If a projection is ever
added to this table out-of-band, the FIRST background merge after this
migration would fail with ClickHouse error code 344 -- a loud, immediate
failure, not silent corruption, so this is a deliberate scope cut, not an
oversight.

Crash convergence: identical to 055 -- a crash after EXCHANGE but before
catch-up/DROP leaves the main table already ReplacingMergeTree, so a rerun
takes the skip path, which finishes the catch-up + DROP of any leftover
``release_impact_daily_new`` before declaring done. A crash before EXCHANGE
leaves a disposable shadow the next run drops and recreates.

Ops note: run during a quiet period / with the release_impact job stopped --
writes that race the EXCHANGE resolve their version tie arbitrarily (benign:
same logical day, newest ``computed_at`` wins).

Idempotent: a table already ReplacingMergeTree is skipped (after converging
any leftover shadow).

NOTE: this file is loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from other
migration modules -- helpers are intentionally duplicated from 027/042/055.
"""

import logging
import re

log = logging.getLogger(__name__)

TABLE = "release_impact_daily"
SHADOW = f"{TABLE}_new"
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
    # A table genuinely absent from system.tables returns zero rows -- that is
    # NOT an exception, so there is no legitimate reason for this probe to ever
    # need to swallow one. A bare `except Exception: return False` here would
    # silently treat a real query/connection failure as "table doesn't exist,
    # skip it" (codex r1 finding 1, CHAOS-4296/#2262): the caller's fail-open
    # skip path would fire on infrastructure trouble, not just absence, and the
    # migration runner would still record the migration as applied. Let any
    # real failure propagate.
    res = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return bool(rows and rows[0] and rows[0][0] > 0)


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
    """Parse 'org_id, release_ref, ...' into its column names."""
    return [c.strip() for c in sorting_key.strip("() ").split(",") if c.strip()]


def _distinct_key_count(client, table: str, key_columns: list[str]) -> int:
    """Count distinct sorting-key tuples (stable under RMT merges)."""
    key_tuple = ", ".join(f"`{c}`" for c in key_columns)
    res = client.query(f"SELECT uniqExact(({key_tuple})) FROM `{table}`")
    rows = getattr(res, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _row_count(client, table: str) -> int:
    """Plain row count -- the caller-side cross-check for the 0==0 hole below."""
    res = client.query(f"SELECT count() FROM `{table}`")
    rows = getattr(res, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    """Post-EXCHANGE catch-up: re-insert the old table's rows, then drop it."""
    log.info(f"  {table}: catch-up copy of post-snapshot writes from `{shadow}`")
    client.command(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`")
    client.command(f"DROP TABLE `{shadow}`")


def _rebuild_table(client, table: str, shadow: str) -> None:
    """Convert a single table to ReplacingMergeTree(computed_at) in place."""
    if not _table_exists(client, table):
        log.warning(f"  {table}: table does not exist, skipping")
        return

    if _engine_name(client, table) == "ReplacingMergeTree":
        # Convergence: a previous run may have crashed after EXCHANGE but
        # before its catch-up/DROP.
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

    try:
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

        key_columns = _key_columns(old_key)
        src_keys = _distinct_key_count(client, table, key_columns)
        dst_keys = _distinct_key_count(client, shadow, key_columns)
        if dst_keys != src_keys:
            raise RuntimeError(
                f"{table}: shadow copy distinct-key mismatch "
                f"(source={src_keys}, shadow={dst_keys}); aborting before swap"
            )

        # Caller-side count() cross-check (088 addition, see module docstring
        # step 6): a 0==0 distinct-key match is not proof of anything if BOTH
        # reads silently came back empty. Fail closed if the raw row count on
        # either table disagrees with what its own distinct-key read implied.
        src_rows = _row_count(client, table)
        dst_rows = _row_count(client, shadow)
        if src_keys == 0 and src_rows > 0:
            raise RuntimeError(
                f"{table}: distinct-key read returned 0 but the table has "
                f"{src_rows} row(s) -- the uniqExact read is unreliable here; "
                f"aborting before swap rather than trusting a 0==0 match"
            )
        if dst_keys == 0 and dst_rows > 0:
            raise RuntimeError(
                f"{shadow}: distinct-key read returned 0 but the shadow has "
                f"{dst_rows} row(s) -- the uniqExact read is unreliable here; "
                f"aborting before swap rather than trusting a 0==0 match"
            )
        log.info(
            f"  {table}: distinct sorting-key tuples verified "
            f"(source={src_keys}, shadow={dst_keys}; row counts {src_rows}/{dst_rows})"
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
    """Convert release_impact_daily to ReplacingMergeTree(computed_at)."""
    log.info("=== Migration 088: idempotent release_impact_daily (CHAOS-4296) ===")
    try:
        _rebuild_table(client, TABLE, SHADOW)
    except Exception as exc:
        log.error(f"FAILED on {TABLE}: {exc}")
        raise
    log.info("=== Migration 088: Complete ===")
