"""Migration 074: retain declared-state history for projects (CHAOS-3563).

`projects` is a ReplacingMergeTree keyed on (org_id, provider, id) (migration
051; lifecycle columns `state`/`target_date` added in 073). A background merge
physically drops every earlier version of a row sharing that key -- so once a
project's declared state changes, the value it held before that change is
gone. There is no history left to read (see
`api/dev/native_status_change.py`'s own comment on
``_PROJECT_DECLARED_FACTS_SQL``, and the CHAOS-3563 ticket this migration
closes).

FIX
---
``project_declared_state_history`` is additive and keyed one level finer,
by (org_id, provider, id, updated_at): a ReplacingMergeTree merge only ever
collapses rows that share ALL FOUR of those columns -- i.e. only a genuine
re-sync of an UNCHANGED declared state (the same provider mtime,
``updated_at``, the exact version column ``projects`` already uses for its
own dedup). A REAL state change carries a NEW ``updated_at`` and therefore
lands as a NEW row, retained forever rather than merged away.
``metrics/sinks/clickhouse/core.py``'s ``write_projects`` appends every
future sync's row here too (in addition to, never instead of, the existing
``projects`` write -- no change to that table's own behavior).

BACKFILL / WHAT IS UNRECOVERABLE
---------------------------------
This migration seeds history with exactly the CURRENT declared state of
every project, read from ``projects FINAL``. That is the ONLY state
recoverable at all: every transition before this migration ran was already
discarded by ``projects``' own ReplacingMergeTree collapse, and no other
source ever recorded it. This backfill is therefore explicitly a floor, not
a reconstruction -- anything before it is UNRECOVERABLE. From the moment
this migration lands, every subsequent transition IS retained; the gap this
closes is prospective, not retroactive.

IDEMPOTENT
----------
Re-running re-inserts the same (org_id, provider, id, updated_at) keys with
the same values (``projects FINAL`` has not changed between runs unless a
real sync happened, in which case the new row is exactly the row a normal
write would have produced anyway); ReplacingMergeTree collapses the
duplicates. Matches migration 048's own documented idempotency pattern.

NOTE: loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from sibling
migration modules.
"""

import logging

log = logging.getLogger(__name__)

# NOTE: the table name is spelled out LITERALLY in both statements below, not
# built from a shared Python constant -- `org_deletion.py`'s
# `_clickhouse_tables_from_migrations()` discovers deletion targets by
# regex-scanning this file's RAW TEXT for `CREATE TABLE ... org_id ...`, not
# by executing it. An f-string/variable-interpolated table name would leave
# an unresolved placeholder in the source text and the table would silently
# never be discovered as an org deletion target.
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
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
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


def _table_exists(client, table: str) -> bool:
    """Mirrors migration 048's fail-open-on-absent, fail-closed-on-error probe.

    A SUCCESSFUL zero-row probe means the table genuinely does not exist yet
    (fresh DB, or this migration run standalone against a mocked client) --
    a clean no-backfill skip, not an error. An unexpected query exception
    still propagates and fails the migration (retryable), never swallowed
    into a silent skip.
    """
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


def upgrade(client):
    log.info("=== Migration 074: project declared-state history ===")

    client.command(_CREATE_HISTORY_TABLE_SQL)

    if not _table_exists(client, "projects"):
        log.info(
            "  projects does not exist yet -- nothing to backfill, skipping "
            "(no pre-existing declared state to seed)"
        )
        log.info("=== Migration 074: Complete (table created, no backfill) ===")
        return

    client.command(_BACKFILL_SQL)
    log.info("=== Migration 074: Complete (table created, backfill seeded) ===")
