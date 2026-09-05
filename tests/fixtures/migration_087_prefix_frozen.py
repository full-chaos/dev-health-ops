"""FROZEN, pre-fix copy of migration 087's relevant functions (CHAOS-4291).

Byte-for-byte from ``src/dev_health_ops/migrations/clickhouse/
087_complexity_tables_replacing_merge_tree.py`` as it existed at commit
``5f3533b8531aa37df21f18fdf9959cb089a9ceb6`` -- codex r1's reviewed, FROZEN
tip (chaos-4291-2250-r1-20260905T052512) -- before this lane's r1 fixes.

WHY A STATIC FILE INSTEAD OF `git show <ref>:<path>`: the first version of
these tests fetched the pre-fix source from git at test time (`git show`,
falling back to `git fetch --depth=1 origin <ref>` on a shallow checkout).
That reached outside the test sandbox and depended on network + git depth
-- a flake by construction (team-lead, 09-05: "a test that depends on git
depth or network is a flake by construction"). This file is the fix: the
exact bytes are committed once, here, and every future test run reads them
from disk like any other fixture.

NEVER update this file to track the current migration. Its entire purpose
is proving CHAOS-4291 r1's findings were REAL bugs in a REAL prior state,
not just described ones -- editing it to "fix" the frozen functions would
make the two prove-by-failure tests that import it vacuous. If a future
round finds another pre-fix bug needing this same before/after treatment,
add a NEW frozen module named for that round, do not modify this one.

Two functions are frozen, matching the two r1 P1 findings this file backs:

  1. ``_table_exists`` -- swallowed every ``system.tables`` query exception
     into ``False`` (fail-OPEN): a transient probe failure on an EXISTING
     table read as "does not exist, skip", and the migration completed
     "successfully" without converting anything.
  2. ``_rebuild_table`` -- the orchestrator that (1) is reached through, and
     the concrete proof that NO mutual-exclusion mechanism existed at this
     commit: this module defines no ``_acquire_lock`` (or any lock) at all,
     so nothing here could have stopped two concurrent runners from both
     passing every check and racing their own shadow-table creation and
     EXCHANGE.

Every OTHER function below (``_engine_name``, ``_has_column``,
``_replace_table_name``, ``_replace_engine_with_rmt``,
``_normalize_sorting_key``, ``_sorting_key``, ``_key_columns``,
``_distinct_key_count``, ``_catch_up_and_drop``, ``_ENGINE_RE``,
``RMT_VERSION_COLUMN``) is included ONLY because ``_rebuild_table`` calls
it and was, at this commit, byte-identical to the current migration's own
copy -- diffed against ``5f3533b8``'s tree at freeze time to confirm that.
They are frozen alongside it (rather than imported from the live module)
so this fixture has ZERO dependency on the live migration file ever again:
a future refactor of those helpers' names or signatures in the live module
must not be able to change what this file proves about the OLD one.
"""

from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

TABLES = (
    "file_complexity_snapshots",
    "repo_complexity_daily",
    "team_complexity_daily",
)

RMT_VERSION_COLUMN = "computed_at"

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
    # FROZEN r1 P1 BUG, verbatim: every exception from the existence probe
    # is swallowed into False. See TestTableExistsFailsClosedNotOpen... in
    # tests/test_migration_087_complexity_rmt.py for what this causes.
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
    """Post-EXCHANGE catch-up: re-insert the old table's rows, then drop it."""
    log.info(f"  {table}: catch-up copy of post-snapshot writes from `{shadow}`")
    client.command(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`")
    client.command(f"DROP TABLE `{shadow}`")


def _rebuild_table(client, table: str) -> None:
    """Convert a single table to ReplacingMergeTree(computed_at) in place.

    FROZEN r1 P1 BUG, verbatim: no lock, no mutual exclusion of any kind --
    this whole file defines no ``_acquire_lock``. Two concurrent calls to
    this exact function for the same table can both pass every check below
    and race their own shadow creation, copy, and EXCHANGE.
    """
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
