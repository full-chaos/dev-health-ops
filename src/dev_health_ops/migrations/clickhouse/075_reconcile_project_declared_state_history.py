"""Migration 075: reconcile ``project_declared_state_history`` to the
current schema (CHAOS-3563 round-3 review A, HIGH, blocking).

WHY THIS EXISTS
----------------
Migration 074 was amended IN PLACE multiple times during this feature's
active development, before ever merging: an ``is_backfill_floor`` column
shape, then a ``write_seq``-only version-column shape, then a
``version_key`` shape bit-packing ``last_synced`` with masked
``write_seq`` low bits, and now a ``version_key`` shape bit-packing
``last_synced`` with a masked ``cityHash64`` of the declared content (see
074's own docstring for the full history of why each earlier shape was
wrong).

Every one of those amendments changed 074's ``CREATE TABLE`` text, but
NONE of them changed 074's MIGRATION VERSION NUMBER ("074") -- and the
migration runner (``storage/clickhouse.py``) records applied migrations
by version number in ``schema_migrations``, then SKIPS any version
already recorded. A database that ran 074 once, at ANY point during this
development window, has "074" marked applied FOREVER, regardless of which
shape of 074's script was checked out at the time. Re-running
``dev-hops migrate clickhouse upgrade`` against that database re-executes
NOTHING for 074 (the runner skips it), and 074's own
``CREATE TABLE IF NOT EXISTS`` would be a no-op even if it did re-run --
the table already exists, just with a stale shape. ``status --check``
reports zero pending migrations (technically true: 074 IS recorded as
applied) while ``project_declared_state_history`` is missing ``version_key``
entirely, and ``_PROJECT_DECLARED_FACTS_SQL`` (which SELECTs it
unconditionally) throws -- a live 500, not a soft degradation, proven
against a real engine by the round-3 verifier. The gate's own scratch-db
provisioning cannot catch this class of bug BY CONSTRUCTION: every gate
run creates a brand-new scratch database and runs every migration fresh
from 001, so it only ever exercises the CURRENT shape of every migration,
never an intermediate one a real, reused development database could be
stuck on.

FIX: THIS MIGRATION IS A RECONCILER, NOT A FORWARD MIGRATION
--------------------------------------------------------------
075 detects whether ``project_declared_state_history`` already has the
CURRENT shape (a ``version_key`` column present) and whether
``project_declared_state_floor`` exists. If BOTH are already correct, this
migration is a NO-OP -- the common case for every fresh database and every
database that has only ever run the current 074. If EITHER is
missing/stale, this migration DROPS and RECREATES both tables from the
CURRENT 074 definitions (duplicated here verbatim -- this file is loaded
standalone by the migration runner and must not import from 074) and
re-runs BOTH backfills from ``projects FINAL``.

Rebuilding is the correct healing move, not a data-loss shortcut, because
this table's OWN design principle (074's "BACKFILL / WHAT IS
UNRECOVERABLE" section) already declares it a floor seeded from current
state, not an append-only source of truth in its own right -- it exists to
recover what can STILL be recovered from ``projects FINAL``, exactly what
074's original backfill already does. Any DB that could ever be in the
broken shape this migration heals is, by construction, a database that
never shipped this feature to real users (074 has not merged to
``origin/main`` as of this migration) -- only in-development scratch/
worktree databases from this same active branch can be stuck on a stale
074 shape, so rebuilding from current `projects` state loses nothing this
feature has ever actually promised to any caller.

KNOWN CONSERVATIVE SIDE EFFECT (documented, not silently accepted): the
re-seeded ``project_declared_state_floor`` marks EVERY project currently
in ``projects`` as having a floor row, including ones that were first
synced AFTER the ORIGINAL 074 application this reconciler is healing.
There is no reliable way to reconstruct, after the fact, "was this
project already being tracked as of that original run" -- this table
records provider mtimes and our own sync timestamps, never "when did
THIS SYSTEM first start tracking this project." The practical
consequence: a project first synced between the original 074 run and this
reconciliation could show the explicit floor-breach warning for an
``as_of`` before its true creation, where the correct, knowable answer is
plain absence. This is the SAME conservative direction 074's own genuine
floor-breach case already accepts: willing to say "unknown" one project
too many, never silently wrong the other way (claiming certainty, or
fabricating a state, that isn't there). A precise fix would require a
reliable "when did this system start tracking this project" signal that
does not exist anywhere in this schema; inventing one is out of scope for
a repair migration.

IDEMPOTENT
----------
Re-running this migration when the shape is ALREADY current is a pure
no-op (fast, single-column existence check, no DROP/backfill). Re-running
it while the shape is stale would DROP and rebuild again, landing on the
identical current shape and re-seeding identical backfill content
(deterministic from ``projects FINAL`` at reconciliation time) -- safe,
if wasteful, to run twice in a row.

NOTE: loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from sibling
migration modules -- every SQL statement below is duplicated verbatim from
074's CURRENT shape, not imported.
"""

import logging

log = logging.getLogger(__name__)

# NOTE: table names spelled out LITERALLY, matching 074's own convention --
# `org_deletion.py`'s `_clickhouse_tables_from_migrations()` regex-scans this
# file's RAW TEXT for `CREATE TABLE ... org_id ...`. Both tables are already
# discovered via 074's own CREATE TABLE statements; these are the SAME two
# tables (074's `IF NOT EXISTS` and this file's rebuild path both target
# them), so no NEW org-deletion registry entry is required.
_CREATE_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS project_declared_state_history (
    org_id String,
    provider LowCardinality(String),
    id String,
    project_key Nullable(String),
    name String DEFAULT '',
    is_active UInt8 DEFAULT 1,
    state LowCardinality(String) DEFAULT '',
    target_date Nullable(Date),
    url String DEFAULT '',
    updated_at DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    version_key UInt64 MATERIALIZED (
        bitShiftLeft(toUInt64(toUnixTimestamp64Milli(last_synced)), 22)
        + bitAnd(
            cityHash64(
                state, ifNull(toString(target_date), ''), url,
                is_active, name, ifNull(project_key, '')
            ),
            4194303
        )
    )
) ENGINE = ReplacingMergeTree(version_key)
ORDER BY (org_id, provider, id, updated_at)
"""

_BACKFILL_SQL = """
INSERT INTO project_declared_state_history
    (org_id, provider, id, project_key, name, is_active, state,
     target_date, url, updated_at, last_synced)
SELECT org_id, provider, id, project_key, name, is_active, state,
       target_date, url, updated_at, last_synced
FROM projects FINAL
"""

_CREATE_FLOOR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS project_declared_state_floor (
    org_id String,
    provider LowCardinality(String),
    id String,
    floor_updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(floor_updated_at)
ORDER BY (org_id, provider, id)
"""

_FLOOR_BACKFILL_SQL = """
INSERT INTO project_declared_state_floor (org_id, provider, id, floor_updated_at)
SELECT org_id, provider, id, updated_at
FROM projects FINAL
"""


def _table_exists(client, table: str) -> bool:
    """Mirrors 074's own fail-open-on-absent, fail-closed-on-error probe."""
    res = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = list(getattr(res, "result_rows", None) or [])
    if not rows or not rows[0]:
        return False
    count = rows[0][0]
    return isinstance(count, int) and not isinstance(count, bool) and count > 0


def _column_exists(client, table: str, column: str) -> bool:
    """Same fail-open/fail-closed shape as `_table_exists`, for a column."""
    res = client.query(
        "SELECT count() FROM system.columns "
        "WHERE database = currentDatabase() AND table = {table:String} "
        "AND name = {column:String}",
        parameters={"table": table, "column": column},
    )
    rows = list(getattr(res, "result_rows", None) or [])
    if not rows or not rows[0]:
        return False
    count = rows[0][0]
    return isinstance(count, int) and not isinstance(count, bool) and count > 0


def upgrade(client):
    log.info("=== Migration 075: reconcile project_declared_state_history ===")

    history_exists = _table_exists(client, "project_declared_state_history")
    floor_exists = _table_exists(client, "project_declared_state_floor")
    has_version_key = history_exists and _column_exists(
        client, "project_declared_state_history", "version_key"
    )

    if history_exists and has_version_key and floor_exists:
        log.info(
            "  project_declared_state_history already has the current "
            "shape (version_key present, floor table exists) -- no-op"
        )
        log.info("=== Migration 075: Complete (no-op, already current) ===")
        return

    log.info(
        "  stale shape detected (version_key present=%s, floor table "
        "exists=%s) -- rebuilding project_declared_state_history and "
        "project_declared_state_floor from projects FINAL",
        has_version_key,
        floor_exists,
    )
    client.command("DROP TABLE IF EXISTS project_declared_state_history")
    client.command("DROP TABLE IF EXISTS project_declared_state_floor")
    client.command(_CREATE_HISTORY_TABLE_SQL)
    client.command(_CREATE_FLOOR_TABLE_SQL)

    if not _table_exists(client, "projects"):
        log.info("  projects does not exist yet -- tables rebuilt, nothing to backfill")
        log.info("=== Migration 075: Complete (tables rebuilt, no backfill) ===")
        return

    client.command(_BACKFILL_SQL)
    client.command(_FLOOR_BACKFILL_SQL)
    log.info("=== Migration 075: Complete (tables rebuilt, backfill re-seeded) ===")
