"""Migration 096: daily-family output tables -> ReplacingMergeTree(computed_at).

Every daily-family writer appends one full row set per run, stamped with that
run's ``computed_at``, and nothing deletes the rows of an earlier run. On the
plain ``MergeTree`` tables below those copies are never collapsed, so each
re-run, redrive and post-sync recompute adds another full copy that every
reader must scan and then discard with ``argMax(<col>, computed_at)`` or
``ORDER BY computed_at DESC LIMIT 1 BY <key>``. Measured before this migration:
5x-30x copies on the local stack and 45x on ``file_hotspot_daily`` in
production (46.8M raw rows for 1.05M keys).

``ReplacingMergeTree(computed_at)`` keeps the newest version per sorting key
during background merges. That is exactly the row every reader already picks,
so readers keep their dedup unchanged and return the same rows before and after
a merge; they only scan fewer copies. Merges are eventual, which is why readers
must keep deduplicating. One reader shape can change after a merge: a
per-column ``argMax(nullable_col, computed_at)`` skips NULL, so while an older
non-NULL copy still exists it returns that stale value, and once the merge
removes the copy it returns the newest row's NULL. The whole-row readers
(``LIMIT 1 BY``, ``argMax(tuple(...), computed_at)``) never had that stale
value.

SORTING KEY = READER KEY. ReplacingMergeTree collapses on the sorting key, so
the key must be the key readers deduplicate on
(``clickhouse_dedup._APPEND_ONLY_DAILY_KEYS``,
``api/queries/metrics._DEDUP_BY_COMPUTED_AT``), never narrower. Three shapes
differ today and are rebuilt with the reader key:

* ``team_metrics_daily`` is ordered by ``(org_id, team_id, day)`` while a team
  owning several repos writes one row per repo; without ``repo_id`` the merge
  would keep one repo's row and drop the others.
* the six testops tables are ordered by ``(repo_id, day)``; without
  ``org_id`` two organizations syncing the same repository would collapse
  into one row.
* ``compounding_risk_daily`` has ``computed_at`` inside its key, which makes
  every version a distinct key that no merge can ever collapse.

Rebuild uses the shadow-table pattern of migrations 027/055/087/088:

    1. SHOW CREATE TABLE for the live DDL (columns, codecs, TTL, settings and
       projections are preserved).
    2. Rewrite: rename to ``<table>_new``, engine to
       ``ReplacingMergeTree(computed_at)``, ORDER BY to the reader key, and
       ``deduplicate_merge_projection_mode = 'rebuild'`` when the table has a
       projection (ClickHouse refuses a projection on a ReplacingMergeTree
       otherwise; 'rebuild' keeps ``file_hotspot_daily``'s latest-run
       projection exact instead of letting it go stale).
    3. Verify the shadow in system.tables: engine, version column and sorting
       key must be exactly the intended ones, else drop it and abort.
    4. INSERT INTO <table>_new SELECT * FROM <table>.
    5. Verify distinct reader-key tuples match between source and shadow, and
       refuse a 0 == 0 match while the source holds rows. Raw counts are NOT
       compared: collapsing copies is the point.
    6. EXCHANGE TABLES <table> AND <table>_new (atomic swap).
    7. Catch-up: INSERT INTO <table> SELECT * FROM <table>_new re-copies rows
       written between the snapshot and the swap; rows already present
       collapse on the key.
    8. DROP TABLE <table>_new, only after the catch-up succeeded.

A per-table lock (a Memory table created without IF NOT EXISTS) keeps two
concurrent runners from interleaving their EXCHANGEs. A crash after EXCHANGE
leaves the table already converted plus a leftover shadow; a rerun finishes
the catch-up and the drop. A crash before EXCHANGE leaves a disposable shadow
that the rerun recreates.

Ops note: run with the metrics workers quiet. A write that races the EXCHANGE
still lands through the catch-up, but two versions of one key stamped in the
same second (``computed_at`` is second-resolution on most of these tables) are
resolved arbitrarily, exactly as the reader dedup resolves them today.

This file is loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import other
migration modules; the helpers are duplicated from 087/088 on purpose.
"""

import logging
import re
import time

log = logging.getLogger(__name__)

RMT_VERSION_COLUMN = "computed_at"

# Target sorting key per table: the key its readers deduplicate on.
TARGET_SORT_KEYS: dict[str, tuple[str, ...]] = {
    "repo_metrics_daily": ("org_id", "repo_id", "day"),
    "user_metrics_daily": ("org_id", "repo_id", "author_email", "day"),
    "commit_metrics": ("org_id", "repo_id", "day", "author_email", "commit_hash"),
    "team_metrics_daily": ("org_id", "team_id", "repo_id", "day"),
    "file_metrics_daily": ("org_id", "repo_id", "day", "path"),
    "file_hotspot_daily": ("org_id", "repo_id", "day", "file_path"),
    "review_edges_daily": ("org_id", "repo_id", "reviewer", "author", "day"),
    "cicd_metrics_daily": ("org_id", "repo_id", "day"),
    "deploy_metrics_daily": ("org_id", "repo_id", "day"),
    "incident_metrics_daily": ("org_id", "repo_id", "day"),
    "work_item_state_durations_daily": (
        "org_id",
        "provider",
        "work_scope_id",
        "team_id",
        "status",
        "day",
    ),
    "team_cognitive_load_daily": ("org_id", "team_id", "day"),
    "compounding_risk_daily": ("org_id", "scope", "scope_id", "day"),
    "testops_pipeline_metrics_daily": ("org_id", "repo_id", "day"),
    "testops_test_metrics_daily": ("org_id", "repo_id", "day"),
    "testops_coverage_metrics_daily": ("org_id", "repo_id", "day"),
    "testops_release_confidence": ("org_id", "repo_id", "day"),
    "testops_quality_drag": ("org_id", "repo_id", "day"),
    "testops_pipeline_stability": ("org_id", "repo_id", "day"),
}

# The live key a table may carry before this migration when it differs from
# the target. Any other live key is refused: converting a key this migration
# was not written for could collapse rows nobody reviewed.
LEGACY_SORT_KEYS: dict[str, tuple[str, ...]] = {
    "team_metrics_daily": ("org_id", "team_id", "day"),
    "compounding_risk_daily": ("org_id", "scope", "scope_id", "day", "computed_at"),
    "testops_pipeline_metrics_daily": ("repo_id", "day"),
    "testops_test_metrics_daily": ("repo_id", "day"),
    "testops_coverage_metrics_daily": ("repo_id", "day"),
    "testops_release_confidence": ("repo_id", "day"),
    "testops_quality_drag": ("repo_id", "day"),
    "testops_pipeline_stability": ("repo_id", "day"),
}

TABLES = tuple(TARGET_SORT_KEYS)

PROJECTION_DEDUP_SETTING = "deduplicate_merge_projection_mode = 'rebuild'"

_LOCK_SUFFIX = "_096_lock"
_LOCK_POLL_INTERVAL_SECS = 2
_LOCK_WAIT_TIMEOUT_SECS = 300

_ENGINE_RE = re.compile(r"ENGINE\s*=\s*MergeTree\b", re.IGNORECASE)
_ORDER_BY_RE = re.compile(
    r"ORDER BY\s+(?:tuple\([^)]*\)|\([^)]+\)|[^\s(]+)", re.IGNORECASE
)
_PRIMARY_KEY_RE = re.compile(r"\bPRIMARY\s+KEY\b", re.IGNORECASE)
_SETTINGS_RE = re.compile(r"\bSETTINGS\s+", re.IGNORECASE)
_VERSION_RE = re.compile(r"ReplacingMergeTree\(\s*`?([^`\s)]+)`?\s*\)")


def _one_value(client, query: str, parameters: dict | None = None):
    res = client.query(query, parameters=parameters)
    rows = getattr(res, "result_rows", None) or []
    return rows[0][0] if rows and rows[0] else None


def _table_exists(client, table: str) -> bool:
    # A failed probe raises instead of reading as "absent": reading it as
    # absent would skip the table and still record the migration as applied.
    count = _one_value(
        client,
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        {"name": table},
    )
    return bool(count)


def _engine_name(client, table: str) -> str:
    value = _one_value(
        client,
        "SELECT engine FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        {"name": table},
    )
    return str(value) if value is not None else ""


def _version_column(client, table: str) -> str | None:
    value = _one_value(
        client,
        "SELECT engine_full FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        {"name": table},
    )
    match = _VERSION_RE.search(str(value or ""))
    return match.group(1) if match else None


def _sorting_key_columns(client, table: str) -> tuple[str, ...]:
    value = _one_value(
        client,
        "SELECT sorting_key FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        {"name": table},
    )
    if value is None:
        raise RuntimeError(f"{table}: could not read sorting_key from system.tables")
    return tuple(
        column.strip().strip("`")
        for column in str(value).strip("() ").split(",")
        if column.strip()
    )


def _has_column(client, table: str, column: str) -> bool:
    count = _one_value(
        client,
        "SELECT count() FROM system.columns WHERE database = currentDatabase() "
        "AND table = {t:String} AND name = {c:String}",
        {"t": table, "c": column},
    )
    return bool(count)


def _projections(client, table: str) -> list[str]:
    res = client.query(
        "SELECT name FROM system.projections "
        "WHERE database = currentDatabase() AND table = {table:String}",
        parameters={"table": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return [str(row[0]) for row in rows if row]


def _distinct_key_count(client, table: str, key: tuple[str, ...]) -> int:
    columns = ", ".join(f"`{column}`" for column in key)
    return int(_one_value(client, f"SELECT uniqExact(({columns})) FROM `{table}`") or 0)


def _row_count(client, table: str) -> int:
    return int(_one_value(client, f"SELECT count() FROM `{table}`") or 0)


def _replace_table_name(ddl: str, old_name: str, new_name: str) -> str:
    pattern = re.compile(
        rf"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?\w+`?\.)?`?)"
        rf"{re.escape(old_name)}(`?\s|`?\()",
        re.IGNORECASE,
    )
    result, count = pattern.subn(rf"\g<1>{new_name}\g<2>", ddl, count=1)
    if count == 0:
        raise ValueError(f"could not find table name {old_name!r} in DDL: {ddl[:300]}")
    return result


def _rewrite_engine_and_key(ddl: str, key: tuple[str, ...]) -> str:
    """Swap the engine and the table's ORDER BY.

    Only the text AFTER the engine clause is searched for ORDER BY: a
    projection body is rendered inside the column list, before ENGINE, and may
    carry its own ORDER BY that must not be rewritten.
    """
    if _PRIMARY_KEY_RE.search(ddl):
        # An explicit PRIMARY KEY must be a prefix of ORDER BY; rewriting the
        # key without rewriting it could produce an invalid or wrong table.
        raise ValueError("DDL declares an explicit PRIMARY KEY; refusing to rewrite")
    match = _ENGINE_RE.search(ddl)
    if match is None:
        raise ValueError(f"no plain 'ENGINE = MergeTree' clause in DDL: {ddl[:300]}")
    head = ddl[: match.start()]
    tail = _ENGINE_RE.sub(
        f"ENGINE = ReplacingMergeTree({RMT_VERSION_COLUMN})", ddl[match.start() :], 1
    )
    tail, count = _ORDER_BY_RE.subn(f"ORDER BY ({', '.join(key)})", tail, count=1)
    if count == 0:
        raise ValueError(f"no ORDER BY after the engine clause in DDL: {ddl[:300]}")
    return head + tail


def _with_projection_setting(ddl: str, projections: list[str]) -> str:
    if not projections:
        return ddl
    if _SETTINGS_RE.search(ddl):
        return _SETTINGS_RE.sub(
            lambda m: f"{m.group(0)}{PROJECTION_DEDUP_SETTING}, ", ddl, count=1
        )
    return ddl.rstrip("\n; \t") + f"\nSETTINGS {PROJECTION_DEDUP_SETTING}\n"


def _assert_converted(client, table: str) -> None:
    key = _sorting_key_columns(client, table)
    if key != TARGET_SORT_KEYS[table]:
        raise RuntimeError(
            f"{table}: already ReplacingMergeTree but sorted by {key!r}, not the "
            f"reader key {TARGET_SORT_KEYS[table]!r}; refusing to treat it as converted"
        )
    version = _version_column(client, table)
    if version != RMT_VERSION_COLUMN:
        raise RuntimeError(
            f"{table}: already ReplacingMergeTree but its version column is "
            f"{version!r}, not {RMT_VERSION_COLUMN!r}; refusing to treat it as converted"
        )


def _acquire_lock(client, table: str) -> bool:
    try:
        client.command(
            f"CREATE TABLE `{table}{_LOCK_SUFFIX}` (x UInt8) ENGINE = Memory"
        )
        return True
    except Exception as exc:
        text = str(exc)
        if (
            getattr(exc, "code", None) == 57
            or "TABLE_ALREADY_EXISTS" in text
            or "already exists" in text
        ):
            return False
        raise


def _release_lock(client, table: str) -> bool:
    # Never raises: it runs in a finally and must not replace an exception
    # that is already propagating. The caller raises when this was the only
    # failure.
    try:
        client.command(f"DROP TABLE IF EXISTS `{table}{_LOCK_SUFFIX}`")
        return True
    except Exception as exc:
        log.warning(
            f"  {table}: could not release `{table}{_LOCK_SUFFIX}`: {exc}. Once no "
            f"runner is converting this table, drop it by hand."
        )
        return False


def _wait_for_concurrent_conversion(client, table: str) -> None:
    shadow = f"{table}_new"
    deadline = time.monotonic() + _LOCK_WAIT_TIMEOUT_SECS
    while time.monotonic() < deadline:
        converged = _engine_name(client, table) == "ReplacingMergeTree"
        if converged and not _table_exists(client, shadow):
            _assert_converted(client, table)
            return
        if not _table_exists(client, f"{table}{_LOCK_SUFFIX}"):
            raise RuntimeError(
                f"{table}: another runner released its lock without finishing the "
                f"conversion (converted={converged}); aborting instead of skipping"
            )
        time.sleep(_LOCK_POLL_INTERVAL_SECS)
    raise RuntimeError(
        f"{table}: timed out after {_LOCK_WAIT_TIMEOUT_SECS}s waiting for another "
        f"runner. If none is running, DROP TABLE IF EXISTS {table}{_LOCK_SUFFIX} "
        f"and rerun; the rerun converges any leftover {table}_new."
    )


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    log.info(f"  {table}: catch-up copy from `{shadow}`")
    client.command(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`")
    client.command(f"DROP TABLE `{shadow}`")


def _convert(client, table: str, shadow: str) -> None:
    target = TARGET_SORT_KEYS[table]
    live_key = _sorting_key_columns(client, table)
    if live_key not in (target, LEGACY_SORT_KEYS.get(table, target)):
        raise RuntimeError(
            f"{table}: live sorting key {live_key!r} is neither the expected legacy "
            f"key nor the reader key {target!r}; refusing to convert"
        )
    if not _has_column(client, table, RMT_VERSION_COLUMN):
        raise ValueError(f"{table}: no '{RMT_VERSION_COLUMN}' column to version by")

    ddl = _one_value(client, f"SHOW CREATE TABLE `{table}`")
    new_ddl = _replace_table_name(str(ddl), table, shadow)
    new_ddl = _rewrite_engine_and_key(new_ddl, target)
    new_ddl = _with_projection_setting(new_ddl, _projections(client, table))

    log.info(f"  {table}: creating shadow sorted by {target!r}")
    client.command(f"DROP TABLE IF EXISTS `{shadow}`")
    client.command(new_ddl)
    try:
        if _engine_name(client, shadow) != "ReplacingMergeTree":
            raise RuntimeError(f"{table}: shadow engine is not ReplacingMergeTree")
        if _version_column(client, shadow) != RMT_VERSION_COLUMN:
            raise RuntimeError(f"{table}: shadow version column is not computed_at")
        shadow_key = _sorting_key_columns(client, shadow)
        if shadow_key != target:
            raise RuntimeError(
                f"{table}: shadow sorted by {shadow_key!r}, expected {target!r}"
            )

        client.command(f"INSERT INTO `{shadow}` SELECT * FROM `{table}`")

        source_keys = _distinct_key_count(client, table, target)
        shadow_keys = _distinct_key_count(client, shadow, target)
        if source_keys != shadow_keys:
            raise RuntimeError(
                f"{table}: distinct reader keys differ after copy "
                f"(source={source_keys}, shadow={shadow_keys})"
            )
        if source_keys == 0 and _row_count(client, table) > 0:
            raise RuntimeError(
                f"{table}: distinct-key read returned 0 for a table that holds rows"
            )
        log.info(f"  {table}: {source_keys} distinct reader keys verified")
    except Exception:
        try:
            client.command(f"DROP TABLE IF EXISTS `{shadow}`")
        except Exception as cleanup_error:
            log.warning(f"  {table}: shadow cleanup failed: {cleanup_error}")
        raise

    client.command(f"EXCHANGE TABLES `{table}` AND `{shadow}`")
    # From here the shadow holds the old table; it is dropped only by the
    # catch-up, and a failed catch-up is converged by the rerun.
    _catch_up_and_drop(client, table, shadow)


def _rebuild_table(client, table: str) -> None:
    shadow = f"{table}_new"
    if not _table_exists(client, table):
        log.warning(f"  {table}: table does not exist, skipping")
        return
    if _engine_name(client, table) == "ReplacingMergeTree":
        _assert_converted(client, table)
        if not _table_exists(client, shadow):
            log.info(f"  {table}: already converted")
            return

    if not _acquire_lock(client, table):
        log.info(f"  {table}: another runner holds the lock; waiting")
        _wait_for_concurrent_conversion(client, table)
        return

    succeeded = False
    try:
        if _engine_name(client, table) == "ReplacingMergeTree":
            _assert_converted(client, table)
            if _table_exists(client, shadow):
                log.info(f"  {table}: converging leftover `{shadow}`")
                _catch_up_and_drop(client, table, shadow)
        else:
            _convert(client, table, shadow)
        succeeded = True
    finally:
        released = _release_lock(client, table)
        if succeeded and not released:
            raise RuntimeError(
                f"{table}: converted, but `{table}{_LOCK_SUFFIX}` could not be dropped"
            )


def upgrade(client):
    """Convert the daily-family output tables to ReplacingMergeTree(computed_at)."""
    log.info("=== Migration 096: daily-family tables to ReplacingMergeTree ===")
    for index, table in enumerate(TABLES, 1):
        log.info(f"[{index}/{len(TABLES)}] {table}")
        try:
            _rebuild_table(client, table)
        except Exception as exc:
            log.error(f"FAILED on {table}: {exc}")
            raise
    log.info("=== Migration 096: complete ===")
