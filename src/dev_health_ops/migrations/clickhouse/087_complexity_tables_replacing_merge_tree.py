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
import time

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

# The org-scoped sorting key each table MUST carry, per the module docstring
# ("At the time of writing they are org_id-first on all three tables").
# r1 P1: the pre-EXCHANGE check only verified the SHADOW's key matched
# whatever the CURRENT table's key happened to be -- it never verified that
# current key was actually this one, and the "already ReplacingMergeTree"
# skip path checked engine name only, never the key. Either gap lets a
# table with the WRONG key (a plain `(repo_id, day)`, say) either get
# "converted" with that same wrong key preserved, or be silently accepted
# as already done -- both defeat the entire point of an org-id-first key,
# which is that no two tenants' rows can ever collapse into one.
EXPECTED_SORT_KEYS = {
    "file_complexity_snapshots": ["org_id", "repo_id", "as_of_day", "file_path"],
    "repo_complexity_daily": ["org_id", "repo_id", "day"],
    "team_complexity_daily": ["org_id", "team_id", "day"],
}

# Best-effort mutual exclusion for the migration runner itself (r1 P1): two
# concurrent runners can otherwise both observe MergeTree, both build a
# shadow, and interleave their EXCHANGEs so the table ends up back on
# MergeTree even though the migration is recorded as applied. See
# _acquire_lock/_release_lock.
_LOCK_SUFFIX = "_087_lock"
_LOCK_POLL_INTERVAL_SECS = 2
_LOCK_WAIT_TIMEOUT_SECS = 300

# Match a plain MergeTree engine clause (NOT ReplacingMergeTree / others):
# ``ENGINE = MergeTree`` immediately followed by a non-identifier char.
_ENGINE_RE = re.compile(r"ENGINE\s*=\s*MergeTree\b", re.IGNORECASE)

# Whether a CREATE TABLE DDL already has its own SETTINGS clause (SHOW
# CREATE TABLE always renders SETTINGS, if present, as the trailing
# clause). Whether the table has any PROJECTIONs at all is read live from
# system.projections (_table_projections), not pattern-matched here.
_SETTINGS_RE = re.compile(r"\bSETTINGS\s+", re.IGNORECASE)


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


def _table_projections(client, table: str) -> list[str]:
    """Read a table's projection NAMES from ``system.projections``, scoped to
    ``currentDatabase()`` (team-lead 09-05: use the live catalog, the same
    way every other introspection in this migration already does, rather
    than pattern-matching the DDL text for the word "PROJECTION" -- a regex
    over `SHOW CREATE TABLE` output is one syntax variant away from a false
    negative, and `system.projections` is authoritative).

    ``system.projections`` spans every database on the instance, exactly
    like the `system.tables` scoping trap already documented on
    ``EXPECTED_SORT_KEYS`` above -- unscoped, a same-named table in a
    ci_*/acr_* scratch database would answer for the wrong one.
    """
    res = client.query(
        "SELECT name FROM system.projections "
        "WHERE database = currentDatabase() AND table = {table:String}",
        parameters={"table": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return [str(row[0]) for row in rows if row]


def _ensure_projection_dedup_setting(ddl: str, projections: list[str]) -> str:
    """Make a projection-carrying DDL legal under ReplacingMergeTree.

    ClickHouse refuses to CREATE a ReplacingMergeTree table that has a
    PROJECTION unless ``deduplicate_merge_projection_mode`` is explicitly
    ``'drop'`` or ``'rebuild'`` -- the default is ``'throw'``, which raises
    ``Code: 344. DB::Exception: ... (SUPPORT_IS_DISABLED)`` at CREATE time.
    Measured live: ``file_complexity_snapshots`` carries
    ``prj_acr_file_complexity_runs`` (added by migration 069, same family
    as ``file_hotspot_daily``'s ``prj_acr_file_hotspot_runs``) and this
    migration's shadow-table CREATE failed with exactly that error before
    this fix existed. ``repo_complexity_daily`` and ``team_complexity_daily``
    carry no projections as of this writing (migration 069 added none to
    either), so ``projections`` is empty for them and this is a no-op.

    MODE CHOICE, PER TEAM-LEAD'S REQUEST FOR A STATED REASON (parity of
    reads): ``'rebuild'``, never ``'drop'``, for every projection found.
    ``069_acr_code_metric_access_paths.sql``'s own sibling projection,
    ``prj_acr_file_hotspot_runs`` on ``file_hotspot_daily``, is PROVEN
    load-bearing for a bounded-read guarantee --
    ``tests/test_acr_code_metric_access_paths_live.py::
    test_hotspot_run_projection_keeps_latest_replacement_read_bounded``
    forces its use with ``SETTINGS force_optimize_projection_name=...``
    and asserts the equivalent full-scan query FAILS closed under
    ``max_bytes_to_read``. ``file_complexity_snapshots``'s
    ``prj_acr_file_complexity_runs`` was added by the SAME migration for
    the SAME "bounded latest-run lookup" purpose (its own comment says so)
    -- its actual reader lives in the separate ACR service, not this repo,
    so its use cannot be grepped here, but nothing in this codebase
    contradicts the sibling's proof. ``'drop'`` would let the projection
    go silently stale relative to the base table until the next full
    merge, turning that reader's bounded read back into an unbounded scan
    with no visible failure -- the same silent-degradation shape this
    migration's other fixes exist to avoid. If a future projection is
    added to one of these three tables for a purpose known NOT to need
    read-time correctness, this default should be revisited for that
    projection specifically, not changed globally.
    """
    if not projections:
        return ddl
    setting = "deduplicate_merge_projection_mode = 'rebuild'"
    if _SETTINGS_RE.search(ddl):
        # Prepend to the existing SETTINGS clause -- inserting right after
        # the keyword works regardless of how many settings already follow.
        return _SETTINGS_RE.sub(lambda m: f"{m.group(0)}{setting}, ", ddl, count=1)
    return ddl.rstrip("\n; \t") + f"\nSETTINGS {setting}\n"


def _assert_expected_sort_key(table: str, actual_key_columns: list[str]) -> None:
    """Fail closed if a table's live sorting key is not the org-scoped key
    this migration requires (r1 P1).

    This is checked both before converting a plain MergeTree table (so a
    table that was never given the org-prefixed key is never "converted"
    with its wrong key intact) and before accepting an already-
    ReplacingMergeTree table as done (so a table that reached RMT some
    other way, with the wrong key, is never silently treated as safe).
    A mismatched key means two tenants' rows CAN collapse into one under
    RMT dedup -- there is no safe way to proceed automatically, so this
    raises rather than logging and continuing.
    """
    expected = EXPECTED_SORT_KEYS.get(table)
    if expected is None:
        raise ValueError(
            f"{table}: no expected sorting key registered for migration 087 "
            f"-- refusing to guess"
        )
    if actual_key_columns != expected:
        raise RuntimeError(
            f"{table}: live sorting key {actual_key_columns!r} does not "
            f"match the required org-scoped key {expected!r}; refusing to "
            f"convert or to treat this table as already converted -- "
            f"proceeding could collapse two tenants' rows into one"
        )


def _table_exists(client, table: str) -> bool:
    """Whether `table` exists, per `system.tables`.

    r1 P1: this used to swallow every exception and return False, which is
    fail-OPEN -- a transient query failure against an EXISTING table would
    read as "does not exist, skip", `upgrade()` would then complete "'
    successfully" without converting anything, and the migration ledger
    would record 087 as applied while the table stayed MergeTree. A probe
    that cannot determine the truth must abort the migration, not silently
    choose the answer that lets it finish; the caller's own try/except (in
    `upgrade()`) is what turns this into a clean migration failure.
    """
    res = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return bool(rows and rows[0] and rows[0][0] > 0)


def _acquire_lock(client, table: str) -> bool:
    """Best-effort mutual exclusion for converting one table (r1 P1).

    ClickHouse has no advisory-lock primitive, but a plain `CREATE TABLE`
    (without IF NOT EXISTS) is atomic and fails if the name already exists
    -- exactly the property a mutex needs. Returns True if this runner won
    the lock (and is responsible for releasing it via `_release_lock`),
    False if another runner already holds it.
    """
    try:
        client.command(
            f"CREATE TABLE `{table}{_LOCK_SUFFIX}` (x UInt8) ENGINE = Memory"
        )
        return True
    except Exception:
        return False


def _release_lock(client, table: str) -> None:
    try:
        client.command(f"DROP TABLE IF EXISTS `{table}{_LOCK_SUFFIX}`")
    except Exception as exc:  # pragma: no cover - best effort
        log.warning(f"  {table}: failed to release migration 087 lock: {exc}")


def _wait_for_concurrent_conversion(client, table: str) -> None:
    """Block until another runner's conversion of `table` finishes.

    Polls rather than merely sleeping once, so this returns as soon as the
    other runner is actually done instead of always paying the full
    timeout. If the lock disappears WITHOUT the table ever reaching
    ReplacingMergeTree, the other runner's attempt failed -- raising here
    (rather than returning as if nothing was wrong) keeps that failure from
    being masked as a silent skip on this runner.
    """
    deadline = time.monotonic() + _LOCK_WAIT_TIMEOUT_SECS
    while time.monotonic() < deadline:
        if _engine_name(client, table) == "ReplacingMergeTree":
            return
        if not _table_exists(client, f"{table}{_LOCK_SUFFIX}"):
            raise RuntimeError(
                f"{table}: another runner's migration 087 lock was released "
                f"without converting the table to ReplacingMergeTree -- its "
                f"attempt likely failed; aborting rather than treating this "
                f"as a skip"
            )
        time.sleep(_LOCK_POLL_INTERVAL_SECS)
    raise RuntimeError(
        f"{table}: timed out after {_LOCK_WAIT_TIMEOUT_SECS}s waiting for a "
        f"concurrent runner to finish converting this table"
    )


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
    """Convert a single table to ReplacingMergeTree(computed_at) in place.

    The mutating portion (leftover-shadow convergence, or a fresh convert)
    runs under `_acquire_lock`/`_release_lock` (r1 P1): without it, two
    concurrent runners can both observe MergeTree, both build a shadow
    named `<table>_new`, and interleave their EXCHANGEs so the live table
    ends up back on MergeTree even though the migration is recorded as
    applied. The pure read checks above the lock (existence, engine name)
    are safe unlocked since they mutate nothing.
    """
    shadow = f"{table}_new"

    if not _table_exists(client, table):
        log.warning(f"  {table}: table does not exist, skipping")
        return

    if _engine_name(client, table) == "ReplacingMergeTree":
        _assert_expected_sort_key(table, _key_columns(_sorting_key(client, table)))
        if not _table_exists(client, shadow):
            log.info(f"  {table}: already ReplacingMergeTree, skipping")
            return
        # Convergence: a previous run may have crashed after EXCHANGE but
        # before its catch-up/DROP. Fall through to the locked section so
        # a concurrent runner can't race this catch-up too.
    elif not _has_column(client, table, RMT_VERSION_COLUMN):
        raise ValueError(
            f"{table}: required RMT version column '{RMT_VERSION_COLUMN}' not "
            f"found; cannot convert to ReplacingMergeTree"
        )

    if not _acquire_lock(client, table):
        log.info(
            f"  {table}: another runner holds the migration 087 lock, "
            f"waiting for it to finish instead of racing it"
        )
        _wait_for_concurrent_conversion(client, table)
        return

    try:
        # Re-check under the lock: the state above may be stale by the time
        # the lock was won (this runner may have lost a race to acquire it
        # while another runner completed a full conversion in between).
        if _engine_name(client, table) == "ReplacingMergeTree":
            if _table_exists(client, shadow):
                log.info(
                    f"  {table}: already ReplacingMergeTree but leftover "
                    f"`{shadow}` found -- converging interrupted run"
                )
                _catch_up_and_drop(client, table, shadow)
            else:
                log.info(f"  {table}: already ReplacingMergeTree, skipping")
            return

        old_key = _normalize_sorting_key(_sorting_key(client, table))
        _assert_expected_sort_key(table, _key_columns(old_key))

        res = client.query(f"SHOW CREATE TABLE `{table}`")
        ddl = res.result_rows[0][0]

        new_ddl = _replace_table_name(ddl, table, shadow)
        new_ddl = _replace_engine_with_rmt(new_ddl)
        new_ddl = _ensure_projection_dedup_setting(
            new_ddl, _table_projections(client, table)
        )

        log.info(f"  {table}: creating shadow table")
        client.command(f"DROP TABLE IF EXISTS `{shadow}`")
        client.command(new_ddl)

        # Everything before EXCHANGE is safely retryable: on any failure
        # drop the (disposable, pre-swap) shadow and re-raise. After
        # EXCHANGE the shadow holds real data and must NOT be dropped
        # without a catch-up.
        try:
            # Fail closed if the DDL rewrite did not produce the intended
            # engine, or if it disturbed the sorting key (the dedup key
            # MUST be unchanged: a different key would silently collapse
            # distinct rows -- and on these three tables the key is what
            # keeps two tenants apart).
            if _engine_name(client, shadow) != "ReplacingMergeTree":
                raise RuntimeError(
                    f"{table}: shadow engine is not ReplacingMergeTree after "
                    f"rewrite; aborting"
                )
            shadow_key = _normalize_sorting_key(_sorting_key(client, shadow))
            if shadow_key != old_key:
                raise RuntimeError(
                    f"{table}: shadow sorting key changed during engine swap "
                    f"(expected {old_key!r}, got {shadow_key!r}); aborting"
                )

            log.info(f"  {table}: copying data")
            client.command(f"INSERT INTO `{shadow}` SELECT * FROM `{table}`")

            # Verify no logical rows were lost before swapping. Raw row
            # counts WILL differ here -- the RMT copy collapses the 6-20x
            # append duplication this migration exists to end -- so
            # compare the number of distinct sorting-key tuples, which
            # must be identical.
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

        # From here on the shadow is the OLD table; never drop it without
        # the catch-up. If this fails, the rerun skip path converges it.
        _catch_up_and_drop(client, table, shadow)

        log.info(f"  {table}: done")
    finally:
        _release_lock(client, table)


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
