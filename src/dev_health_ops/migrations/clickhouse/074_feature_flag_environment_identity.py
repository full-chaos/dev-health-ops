"""Migration 074: keep feature-flag environments in the RMT identity.

The feature-flag producer emits one row for every environment scope.  Before
this migration, ``feature_flag`` was a ``ReplacingMergeTree`` keyed by
``(org_id, provider, project_key, flag_key)``.  Two environments for the same
flag therefore represented one physical identity: a merge (or ``FINAL``)
silently discarded one environment and readback reported a conflict or an
incomplete provider result.

The deployed identity is now:

    (org_id, provider, project_key, flag_key, environment)

ClickHouse cannot alter a ReplacingMergeTree sorting key in place.  This
migration uses the same shadow-table/atomic-exchange pattern as migrations
042 and 061:

1. Read the current DDL and sorting key.
2. Create a shadow with the environment-aware key, verifying the *actual*
   ClickHouse sorting key before copying data.
3. Copy the old table and compare distinct environment-aware key tuples.
4. Atomically exchange the tables.
5. Catch up rows written during the snapshot and only then drop the old
   shadow.

The catch-up and idempotency paths deliberately leave a post-exchange shadow
in place when catch-up fails.  A rerun converges it without dropping rows.
Historical rows are copied unchanged; adding ``environment`` to the sorting
key does not rewrite values or collapse rows that differ by environment.

The migration is forward-only for application compatibility: once this schema
is deployed, application binaries must use the registry's logical
environment aggregation. A pre-074 registry reader would expose one row per
environment and is not a safe rollback target, although old writers remain
able to insert rows because the original columns are unchanged.

Each invocation uses a unique shadow name. The migration runner's asyncio lock
serializes migrations within one process, but not across processes or pods.
Unique target-key shadows plus the final source-key preflight make a
concurrent interleaving safe: a runner that observes the target key catches
up its shadow instead of exchanging, and every exchange candidate has the
target key so the table cannot be exchanged back to the legacy key.

This module is loaded standalone by the ClickHouse migration runner, so it
must not import sibling migration modules.
"""

import logging
import re
import uuid

log = logging.getLogger(__name__)


TABLES = {
    "feature_flag": "(org_id, provider, project_key, flag_key, environment)",
}

LEGACY_KEY = "org_id, provider, project_key, flag_key"

# ORDER BY (col, ...) | ORDER BY tuple(col, ...) | ORDER BY col
_ORDER_BY_RE = re.compile(r"ORDER BY\s+(?:tuple\([^)]+\)|\([^)]+\)|\S+)", re.IGNORECASE)
_ORG_ID_COL_RE = re.compile(
    r"`?org_id`?\s+(?:LowCardinality\(\s*)?String", re.IGNORECASE
)
_EXCHANGE_DATABASE_ENGINES = frozenset({"Atomic", "Shared"})


def _table_name_re(table: str) -> re.Pattern:
    return re.compile(
        rf"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        rf"(?:`?[\w\d_]+`?\.)?`?){re.escape(table)}(`?\s|`?\()",
        re.IGNORECASE,
    )


def _replace_order_by(ddl: str, new_order_by: str) -> str:
    result, count = _ORDER_BY_RE.subn(f"ORDER BY {new_order_by}", ddl, count=1)
    if count == 0:
        raise ValueError(f"Could not find ORDER BY in DDL: {ddl[:300]}...")
    return result


def _replace_table_name(ddl: str, old_name: str, new_name: str) -> str:
    pattern = _table_name_re(old_name)
    result, count = pattern.subn(rf"\g<1>{new_name}\g<2>", ddl, count=1)
    if count == 0:
        raise ValueError(
            f"Could not replace table name '{old_name}' in DDL: {ddl[:300]}..."
        )
    return result


def _table_exists(client, table: str) -> bool:
    try:
        result = client.query(
            "SELECT count() FROM system.tables "
            "WHERE database = currentDatabase() AND name = {name:String}",
            parameters={"name": table},
        )
        rows = getattr(result, "result_rows", None) or []
        return bool(rows and rows[0] and rows[0][0] > 0)
    except Exception:
        return False


def _shadow_name(table: str) -> str:
    """Return a per-run shadow name so concurrent runners never share DDL."""
    return f"{table}_074_new_{uuid.uuid4().hex}"


def _shadow_tables(client, table: str) -> list[str]:
    prefix = f"{table}_074_new_"
    result = client.query(
        "SELECT name FROM system.tables "
        "WHERE database = currentDatabase() AND name LIKE {prefix:String}",
        parameters={"prefix": f"{prefix}%"},
    )
    rows = getattr(result, "result_rows", None) or []
    return [str(row[0]) for row in rows if row and str(row[0]).startswith(prefix)]


def _assert_exchange_supported(client) -> None:
    result = client.query(
        "SELECT engine FROM system.databases WHERE name = currentDatabase()"
    )
    rows = getattr(result, "result_rows", None) or []
    engine = str(rows[0][0]) if rows and rows[0] else ""
    if engine not in _EXCHANGE_DATABASE_ENGINES:
        raise RuntimeError(
            "feature_flag migration 074 requires an Atomic or Shared database "
            f"for EXCHANGE TABLES; current engine={engine or '<unknown>'!r}"
        )


def _sorting_key(client, table: str) -> str:
    result = client.query(
        "SELECT sorting_key FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(result, "result_rows", None) or []
    if not rows or not rows[0]:
        raise RuntimeError(f"{table}: could not read sorting_key from system.tables")
    return str(rows[0][0])


def _normalize_sorting_key(key: str) -> str:
    """Normalize ClickHouse sorting-key display forms for exact comparison."""
    normalized = re.sub(
        r"\s*,\s*", ", ", re.sub(r"\s+", " ", key.replace("`", ""))
    ).strip()
    if normalized.lower().startswith("tuple(") and normalized.endswith(")"):
        normalized = normalized[6:-1]
    return normalized.strip(" ()")


def _key_columns(order_by: str) -> list[str]:
    return [
        column.strip() for column in order_by.strip("() ").split(",") if column.strip()
    ]


def _distinct_key_count(client, table: str, key_columns: list[str]) -> int:
    key_tuple = ", ".join(f"`{column}`" for column in key_columns)
    result = client.query(f"SELECT uniqExact(({key_tuple})) FROM `{table}`")
    rows = getattr(result, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    """Reinsert the old side of an exchange before dropping its shadow."""
    log.info("  %s: catch-up copy from `%s`", table, shadow)
    client.command(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`")
    client.command(f"DROP TABLE `{shadow}`")


def _legacy_shadows(client, table: str) -> list[str]:
    """Return migration-owned shadows that still have the legacy identity."""
    shadows = []
    for shadow in _shadow_tables(client, table):
        shadow_key = _normalize_sorting_key(_sorting_key(client, shadow))
        if shadow_key == LEGACY_KEY:
            shadows.append(shadow)
        elif shadow_key != _normalize_sorting_key(TABLES[table]):
            raise RuntimeError(
                f"{table}: migration shadow `{shadow}` has unexpected sorting key "
                f"{shadow_key!r}"
            )
    return shadows


def _drain_legacy_shadows(client, table: str) -> None:
    """Drain every recoverable old-key shadow before reporting success.

    A runner may have exchanged successfully and then failed while catching up
    its old side.  Another runner can observe the target key while its own
    snapshot is still in flight, so cleaning only that runner's shadow would
    incorrectly let the migration ledger advance with the failed runner's
    legacy shadow (and its writes) still stranded.  Re-discover after draining
    to make the success boundary explicit.
    """
    for shadow in _legacy_shadows(client, table):
        _catch_up_and_drop(client, table, shadow)
    remaining = _legacy_shadows(client, table)
    if remaining:
        raise RuntimeError(
            f"{table}: recoverable legacy migration shadows remain after drain: "
            f"{remaining!r}"
        )


def _rebuild_table(
    client, table: str, new_order_by: str, *, shadow: str | None = None
) -> None:
    """Rebuild one feature-flag table with environment-aware identity."""
    shadow = shadow or _shadow_name(table)
    target_key = _normalize_sorting_key(new_order_by)

    if not _table_exists(client, table):
        log.warning("  %s: table does not exist, skipping", table)
        return

    current_key = _normalize_sorting_key(_sorting_key(client, table))
    if current_key == target_key:
        # A prior run may have exchanged successfully and crashed before
        # catch-up/drop.  Finish that operation before declaring success.
        leftovers = _shadow_tables(client, table)
        if leftovers:
            for leftover in leftovers:
                shadow_key = _normalize_sorting_key(_sorting_key(client, leftover))
                if shadow_key not in {LEGACY_KEY, target_key}:
                    raise RuntimeError(
                        f"{table}: migration shadow `{leftover}` has unexpected "
                        f"sorting key {shadow_key!r}"
                    )
                log.info(
                    "  %s: target key already present with leftover `%s`; converging",
                    table,
                    leftover,
                )
                _catch_up_and_drop(client, table, leftover)
        else:
            log.info("  %s: target key already present, skipping", table)
        _drain_legacy_shadows(client, table)
        return

    if current_key != LEGACY_KEY:
        raise RuntimeError(
            f"{table}: unexpected source sorting key {current_key!r}; "
            f"expected legacy {LEGACY_KEY!r} or target {target_key!r}"
        )

    result = client.query(f"SHOW CREATE TABLE `{table}`")
    rows = getattr(result, "result_rows", None) or []
    if not rows or not rows[0]:
        raise RuntimeError(f"{table}: SHOW CREATE TABLE returned no DDL")
    ddl = str(rows[0][0])
    if not _ORG_ID_COL_RE.search(ddl):
        raise ValueError(
            f"{table}: org_id column not found in DDL; cannot preserve tenant identity"
        )

    new_ddl = _replace_table_name(ddl, table, shadow)
    new_ddl = _replace_order_by(new_ddl, new_order_by)

    log.info("  %s: creating shadow table", table)
    client.command(f"DROP TABLE IF EXISTS `{shadow}`")
    client.command(new_ddl)

    # Before EXCHANGE the shadow is disposable.  After EXCHANGE it contains
    # the old live table and must remain available for catch-up/rerun.
    target_already_visible = False
    try:
        shadow_key = _normalize_sorting_key(_sorting_key(client, shadow))
        if shadow_key != target_key:
            raise RuntimeError(
                f"{table}: shadow sorting key mismatch after DDL rewrite "
                f"(expected {target_key!r}, got {shadow_key!r}); aborting"
            )

        client.command(f"INSERT INTO `{shadow}` SELECT * FROM `{table}`")
        key_columns = _key_columns(new_order_by)
        source_keys = _distinct_key_count(client, table, key_columns)
        shadow_keys = _distinct_key_count(client, shadow, key_columns)
        if shadow_keys != source_keys:
            raise RuntimeError(
                f"{table}: shadow copy distinct-key mismatch "
                f"(source={source_keys}, shadow={shadow_keys}); aborting before swap"
            )

        # A second process may have completed the exchange while this runner
        # was copying. Never exchange a legacy main table after the target key
        # is visible; converge our independently named target shadow.
        current_after_copy = _normalize_sorting_key(_sorting_key(client, table))
        if current_after_copy == target_key:
            target_already_visible = True
        elif current_after_copy != LEGACY_KEY:
            raise RuntimeError(
                f"{table}: source sorting key changed during migration "
                f"(got {current_after_copy!r}); refusing EXCHANGE"
            )
    except Exception:
        try:
            client.command(f"DROP TABLE IF EXISTS `{shadow}`")
        except Exception as cleanup_error:  # pragma: no cover - best effort
            log.warning("  %s: shadow cleanup failed: %s", table, cleanup_error)
        raise

    if target_already_visible:
        # This shadow was copied from the legacy side, but another runner won
        # the exchange while we were copying.  Keep catch-up outside the
        # disposable-shadow cleanup block: if it fails, the legacy shadow must
        # remain recoverable for the next runner.
        _catch_up_and_drop(client, table, shadow)
        _drain_legacy_shadows(client, table)
        return

    log.info("  %s: atomic swap via EXCHANGE TABLES", table)
    client.command(f"EXCHANGE TABLES `{table}` AND `{shadow}`")

    # If this fails, the leftover shadow is intentional: rerunning the
    # migration enters the target-key convergence branch above.
    _catch_up_and_drop(client, table, shadow)
    _drain_legacy_shadows(client, table)
    log.info("  %s: done", table)


def upgrade(client):
    """Rebuild feature_flag so environment scopes survive RMT merges."""
    log.info("=== Migration 074: feature_flag environment identity (CHAOS-3737) ===")
    _assert_exchange_supported(client)
    for table, new_order_by in TABLES.items():
        _rebuild_table(client, table, new_order_by)
    log.info("=== Migration 074: Complete ===")
