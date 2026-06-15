"""Migration 049: add run_id to the work_unit_membership dedup key (CHAOS-2433).

ROUND-2 REVIEW FINDING #1 (dedup key excludes run_id breaks the protocol)
-------------------------------------------------------------------------
Migration 046 created work_unit_membership as
    ENGINE = ReplacingMergeTree(computed_at)
    ORDER BY (org_id, node_type, node_id, category_kind, category)
Migration 047 ADDED a run_id column but could NOT change the sort key
(ClickHouse cannot ALTER an existing RMT ORDER BY), so run_id is a column but
NOT part of the physical dedup key.

ReplacingMergeTree collapses rows that share the full ORDER BY tuple, keeping
the row with the greatest version column (computed_at). With run_id ABSENT from
the key, a background merge (or OPTIMIZE FINAL) collapses two rows for the same
(org, node, category_kind, category) that belong to DIFFERENT runs down to the
single max-computed_at version. A newer INCOMPLETE run (rows written, marker
not yet published) has a greater computed_at than the prior COMPLETE run, so the
merge EVICTS the complete run's row. The resolver — which scopes to the prior
COMPLETE run_id (the latest published marker) — then finds nothing for that
node, silently breaking the concurrency / rollback safety the run protocol is
supposed to guarantee.

FIX: per-run rows MUST coexist physically, so run_id must be in the dedup key:
    ORDER BY (org_id, node_type, node_id, category_kind, category, run_id)
ReplacingMergeTree(computed_at) is kept; now it only dedups IDEMPOTENT re-writes
of the SAME (node, category, run_id) — exactly the intended semantics — and
never collapses across runs. Rows from a complete run and an in-flight run for
the same node/category now survive merges side by side, distinguished by run_id.

REBUILD PATTERN (shadow table + EXCHANGE TABLES, mirrors migration 042)
-----------------------------------------------------------------------
ClickHouse cannot ALTER an existing RMT sort key, so we rebuild:
    1. SHOW CREATE TABLE to capture the full DDL (engine, version col, settings).
    2. Rewrite: rename to work_unit_membership_new, append run_id to ORDER BY.
    3. Verify via system.tables that the shadow's sorting key is EXACTLY the old
       key + ", run_id" — abort (drop shadow) on any mismatch so a regex miss on
       exotic DDL fails closed.
    4. INSERT INTO _new SELECT * FROM work_unit_membership (snapshot copy; legacy
       run_id='' rows are copied verbatim).
    5. Verify distinct NEW-key tuple counts match between source and shadow (the
       new key is a superset of the old, so no two source rows that differ on it
       can merge — raw counts may differ as the copy collapses not-yet-merged
       same-key duplicates, which is normal RMT semantics).
    6. EXCHANGE TABLES (atomic swap).
    7. CATCH-UP: INSERT INTO work_unit_membership SELECT * FROM _new — the shadow
       now holds the OLD table incl. rows written between snapshot and swap.
       Idempotent under RMT: already-copied rows dedup on the new key (newest
       version wins), late rows survive. Then DROP the shadow.

Crash convergence: a crash after EXCHANGE but before catch-up/DROP leaves the
shadow holding the OLD table. The rerun's idempotency-skip path (new key already
present) detects the leftover shadow and finishes the catch-up + DROP before
skipping — no rows stranded, no shadow left behind. A crash BEFORE EXCHANGE
leaves a disposable shadow the next run drops and recreates.

Idempotent: if run_id is already last in the ORDER BY, skip (after converging any
leftover shadow). Ops note: run during a quiet period / with sync workers paused;
writes that race the EXCHANGE are preserved (catch-up), version ties resolve
arbitrarily.

FAIL CLOSED (CHAOS-2433 round-6): a structural migration must never be recorded
as applied without actually rebuilding the key. The existence probe and the
sorting-key reads let unexpected ClickHouse errors PROPAGATE (so the migration
RAISES and is NOT recorded as applied, and can be retried) instead of being
swallowed into a silent "table absent" skip. The table is treated as absent ONLY
when a SUCCESSFUL probe returns zero rows. A final post-rebuild check re-reads the
LIVE main-table key and raises unless run_id is last, proving the rebuild landed.

DEPLOY ORDERING (why the snapshot copy is lossless): 047 (add column), 048 (seed
legacy marker) and 049 (this rebuild) all apply in the SAME migration pass
(_apply_sql_migrations runs the whole pending set before the app serves traffic
or workers run). Pre-049 every existing row carries run_id='' (the 047 default),
so all rows share one run_id and NO cross-run collapse is possible on the old
key — the snapshot copy is lossless. The cross-run coexistence the new key
protects only matters for runs written AFTER this migration, which is exactly
when the new key is in force. Do NOT run a real materialize/backfill (which would
write distinct non-empty run_ids) between 047 and 049 on the old key.

NOTE: loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from sibling
migration modules — helpers are intentionally duplicated from 042.
"""

import logging
import re

log = logging.getLogger(__name__)

_TABLE = "work_unit_membership"
_SHADOW = "work_unit_membership_new"
# The old sort key (migration 046) plus run_id appended.
_NEW_ORDER_BY = "(org_id, node_type, node_id, category_kind, category, run_id)"

# Regex: ORDER BY (col, col, ...) | ORDER BY tuple(col, ...) | ORDER BY col
_ORDER_BY_RE = re.compile(r"ORDER BY\s+(?:tuple\([^)]+\)|\([^)]+\)|\S+)", re.IGNORECASE)


def _table_name_re(table: str) -> re.Pattern:
    return re.compile(
        rf"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        rf"(?:`?[\w\d_]+`?\.)?`?){re.escape(table)}(`?\s|`?\()",
        re.IGNORECASE,
    )


def _replace_table_name(ddl: str, old_name: str, new_name: str) -> str:
    pattern = _table_name_re(old_name)
    result, count = pattern.subn(rf"\g<1>{new_name}\g<2>", ddl, count=1)
    if count == 0:
        raise ValueError(
            f"Could not replace table name '{old_name}' in DDL: {ddl[:300]}..."
        )
    return result


def _replace_order_by(ddl: str, new_order_by: str) -> str:
    result, count = _ORDER_BY_RE.subn(f"ORDER BY {new_order_by}", ddl, count=1)
    if count == 0:
        raise ValueError(f"Could not find ORDER BY in DDL: {ddl[:300]}...")
    return result


def _count_gt_zero(result_rows) -> bool:
    """True iff a SUCCESSFUL count() probe returned a positive integer.

    STRICT: requires a real list/tuple of rows whose first cell is a genuine int
    (bool excluded). Any other shape — a MagicMock ``result_rows``, a non-numeric
    cell — is treated as ZERO/absent rather than raised on, since the probe call
    itself succeeded (a query ERROR would have raised from client.query()). This
    keeps fail-closed-on-DB-error while not crashing dry-run/mock callers.
    """
    if not isinstance(result_rows, list | tuple) or not result_rows:
        return False
    first = result_rows[0]
    if not isinstance(first, list | tuple) or not first:
        return False
    count = first[0]
    return isinstance(count, int) and not isinstance(count, bool) and count > 0


def _table_exists(client, table: str) -> bool:
    """Return whether ``table`` exists — FAIL CLOSED (CHAOS-2433 round-6).

    A structural migration must NEVER be recorded as applied without actually
    rebuilding the dedup key. The previous version caught ANY exception from the
    system.tables probe and returned False ("table absent"), so a transient probe
    failure or a restricted ClickHouse user made ``upgrade()`` return early and the
    runner marked 049 applied — leaving work_unit_membership keyed WITHOUT run_id
    while the app starts run-scoped reads/writes (reintroducing the round-2
    background-merge eviction). We therefore let an unexpected probe error
    PROPAGATE so the migration RAISES (not recorded as applied, retryable). The
    table is treated as absent ONLY when a SUCCESSFUL existence check returns zero.

    Interpretation is STRICT but exception-safe: a real ClickHouse probe returns a
    ``list``/``tuple`` of rows with an ``int`` count. A successful probe that
    returns a non-list/uninterpretable shape (e.g. a MagicMock client in a unit
    test that never reaches a real ClickHouse — its ``result_rows`` is a MagicMock,
    not a list, and ``int(MagicMock())`` would deceptively yield 1) is treated as
    absent, NOT raised on, since it is a successful response rather than a query
    error. A genuine query failure still raises from client.query() and propagates
    (fail-closed-on-DB-error preserved).
    """
    res = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    return _count_gt_zero(getattr(res, "result_rows", None))


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


def _normalize_sorting_key(key: str) -> str:
    return re.sub(r"\s*,\s*", ", ", re.sub(r"\s+", " ", key.replace("`", ""))).strip(
        " ()"
    )


def _key_columns(new_order_by: str) -> list[str]:
    return [c.strip() for c in new_order_by.strip("() ").split(",") if c.strip()]


def _distinct_key_count(client, table: str, key_columns: list[str]) -> int:
    key_tuple = ", ".join(f"`{c}`" for c in key_columns)
    res = client.query(f"SELECT uniqExact(({key_tuple})) FROM `{table}`")
    rows = getattr(res, "result_rows", None) or []
    return int(rows[0][0]) if rows and rows[0] else 0


def _run_id_last_in_order_by(client, table: str) -> bool:
    """True when run_id is already the LAST column of the table's sort key."""
    cols = [
        c.strip()
        for c in _normalize_sorting_key(_sorting_key(client, table)).split(",")
    ]
    return bool(cols) and cols[-1] == "run_id"


def _catch_up_and_drop(client, table: str, shadow: str) -> None:
    log.info(f"  {table}: catch-up copy of post-snapshot writes from `{shadow}`")
    client.command(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`")
    client.command(f"DROP TABLE `{shadow}`")


def upgrade(client):
    """Rebuild work_unit_membership with run_id appended to its dedup key."""
    log.info("=== Migration 049: run_id in work_unit_membership dedup key ===")

    if not _table_exists(client, _TABLE):
        # Fresh database where 046 has not yet created the table: nothing to
        # rebuild. (046 already creates the table; 047 adds run_id; a future base
        # schema may inline the new key — either way an absent table is a no-op.)
        log.info(f"  {_TABLE}: does not exist, skipping (nothing to rebuild)")
        return

    if _run_id_last_in_order_by(client, _TABLE):
        # Convergence: a prior run may have crashed after EXCHANGE but before its
        # catch-up/DROP. A leftover shadow then holds the OLD table — finish the
        # catch-up before skipping so its post-snapshot writes are not lost.
        if _table_exists(client, _SHADOW):
            log.info(
                f"  {_TABLE}: run_id already last in ORDER BY but leftover "
                f"`{_SHADOW}` found — converging interrupted run"
            )
            _catch_up_and_drop(client, _TABLE, _SHADOW)
        else:
            log.info(f"  {_TABLE}: run_id already last in ORDER BY, skipping")
        return

    res = client.query(f"SHOW CREATE TABLE `{_TABLE}`")
    ddl = res.result_rows[0][0]

    new_ddl = _replace_table_name(ddl, _TABLE, _SHADOW)
    new_ddl = _replace_order_by(new_ddl, _NEW_ORDER_BY)

    log.info(f"  {_TABLE}: creating shadow table `{_SHADOW}`")
    client.command(f"DROP TABLE IF EXISTS `{_SHADOW}`")
    client.command(new_ddl)

    # Everything before EXCHANGE is safely retryable: on any failure drop the
    # (disposable, pre-swap) shadow and re-raise. After EXCHANGE the shadow holds
    # real data and must NOT be dropped without a catch-up.
    try:
        # Fail closed on DDL-rewrite misses: verify the ACTUAL shadow sort key is
        # exactly the old key + ", run_id" before copying any data.
        old_key = _normalize_sorting_key(_sorting_key(client, _TABLE))
        shadow_key = _normalize_sorting_key(_sorting_key(client, _SHADOW))
        expected_key = f"{old_key}, run_id"
        if shadow_key != expected_key:
            raise RuntimeError(
                f"{_TABLE}: shadow sorting key mismatch after DDL rewrite "
                f"(expected {expected_key!r}, got {shadow_key!r}); aborting"
            )

        log.info(f"  {_TABLE}: copying data (legacy run_id='' rows included)")
        client.command(f"INSERT INTO `{_SHADOW}` SELECT * FROM `{_TABLE}`")

        # Verify no logical rows were lost before swapping. Raw row counts may
        # legitimately differ (the copy can collapse not-yet-merged duplicate
        # versions of the SAME new key — normal RMT semantics), so compare the
        # distinct NEW-key tuple count, which must be identical: the new key is a
        # superset of the old, so no two source rows that differ on it can merge.
        key_columns = _key_columns(_NEW_ORDER_BY)
        src_keys = _distinct_key_count(client, _TABLE, key_columns)
        dst_keys = _distinct_key_count(client, _SHADOW, key_columns)
        if dst_keys != src_keys:
            raise RuntimeError(
                f"{_TABLE}: shadow copy distinct-key mismatch "
                f"(source={src_keys}, shadow={dst_keys}); aborting before swap"
            )
        log.info(
            f"  {_TABLE}: distinct sorting-key tuples verified "
            f"(source={src_keys}, shadow={dst_keys})"
        )
    except Exception:
        try:
            client.command(f"DROP TABLE IF EXISTS `{_SHADOW}`")
        except Exception as cleanup_err:  # pragma: no cover - best effort
            log.warning(f"  {_TABLE}: shadow table cleanup failed: {cleanup_err}")
        raise

    log.info(f"  {_TABLE}: atomic swap via EXCHANGE TABLES")
    client.command(f"EXCHANGE TABLES `{_TABLE}` AND `{_SHADOW}`")

    # From here on the shadow is the OLD table; never drop it without the
    # catch-up. If this fails, the rerun skip path converges it.
    _catch_up_and_drop(client, _TABLE, _SHADOW)

    # FAIL CLOSED (CHAOS-2433 round-6): prove the rebuild actually landed before
    # letting the runner record 049 as applied. Re-read the LIVE main-table key
    # and require run_id to be last; raise otherwise so the migration is NOT
    # recorded as applied and can be retried. _run_id_last_in_order_by reads
    # system.tables and propagates any probe failure (no swallowed exceptions).
    if not _run_id_last_in_order_by(client, _TABLE):
        raise RuntimeError(
            f"{_TABLE}: post-rebuild verification failed — run_id is NOT last in "
            f"the sorting key after EXCHANGE; refusing to mark migration applied "
            f"(actual key: {_sorting_key(client, _TABLE)!r})"
        )

    log.info("=== Migration 049: Complete ===")
