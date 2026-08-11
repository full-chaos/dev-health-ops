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

``version_key`` (codex cross-system review C1, HIGH -- this is the THIRD
and current design; the two prior attempts are recorded below, not
silently corrected, because both were shipped as "fixed" before being
caught): ``updated_at`` and ``last_synced`` are both only
millisecond-precision (``DateTime64(3)``). Two DIFFERENT observed states
sharing the same millisecond for BOTH columns -- plausible under real
concurrent writers, e.g. a provider webhook and a backfill sync racing, or
two workers processing a burst of events from the same batch -- share the
FULL ``ORDER BY`` key AND (before this column existed) the version column
too: ``argMax((updated_at, last_synced))`` had no further tie-break, so a
pre-merge read was implementation-defined and could disagree with
whatever ReplacingMergeTree eventually kept after a background merge,
silently discarding one of the two states from the reader's perspective.

ATTEMPT 1 (superseded, kept only as a record): made ``write_seq`` (a
``generateSnowflakeID()``-backed value) the SOLE ReplacingMergeTree
version column, replacing ``last_synced`` entirely. Team-lead's follow-up
caught the real problem: it changed the dedup semantic from "freshest
``last_synced`` wins" to "whichever was INSERTED last wins" for EVERY
same-``updated_at`` collision, not just the narrow same-millisecond one --
an out-of-order-delivered replay carrying an OLDER ``last_synced`` would
then win over an already-recorded fresher one merely by arriving later,
regressing the freshness watermark for the ordinary, much MORE common
case F6 already covered.

ATTEMPT 2 (superseded, kept only as a record): a ``MATERIALIZED
version_key`` combining ``last_synced`` (high bits, primary ordering) with
``write_seq``'s LOW 22 BITS (tertiary tie-break, only reached when
``last_synced`` also tied exactly) -- this fixed attempt 1's regression
(``last_synced`` differing still governs, unaffected). But the low-bit
tie-break itself was ALSO wrong, measured empirically by the verifier:
ClickHouse's ``generateSnowflakeID()`` packs its low 22 bits as
``machine_id(10 bits) | per-millisecond counter(12 bits)`` -- the counter
RESETS every millisecond, so two snowflake IDs generated ~100ms apart on
the SAME machine can (and in the verifier's trial, did: 10/20 runs)
collide OUTRIGHT in those low 22 bits despite being nowhere near each
other in full value. This is the opposite of the "~1-in-4.19-million"
claim that attempt's docstring made (off by roughly six orders of
magnitude) -- worse, because the low bits are STRUCTURED, not uniformly
distributed, "later write_seq wins" was not even reliably true once
masked: the EARLIER-inserted row won 5 of 10 broken ties in the verifier's
measurement, contradicting that docstring's own claimed tie-break
direction. A test (``test_c1_same_millisecond_collision_is_deterministic``)
shipped alongside it asserted FULL ``write_seq`` distinctness -- which is
essentially always true (snowflake IDs are globally unique) and therefore
could never fail for the actual defect (masked-value collision) it was
meant to catch.

ACTUAL FIX (attempt 3, current): replace the write_seq-derived tie-break
with a CONTENT-DERIVED one -- ``cityHash64`` over the declared-state
payload columns, masked the same way:

    version_key = (unix_millis(last_synced) << 22)
                  | (cityHash64(state, target_date, url, is_active,
                                 name, project_key) & 0x3FFFFF)

``unix_millis(last_synced)`` still occupies the high bits, so ordering by
``version_key`` remains IDENTICAL to ordering by ``last_synced`` whenever
``last_synced`` differs -- F6's semantic is still completely unaffected.
Only when ``last_synced`` ALSO ties does the content hash break the tie,
with three properties a randomized or insertion-order tie-break cannot
offer: (1) IDENTICAL content hashes to the IDENTICAL key, so a tie between
two rows carrying the SAME declared state is harmless by construction --
there is no real ambiguity to resolve; (2) DIFFERING content resolves to
a DETERMINISTIC winner purely as a function of that content, with no
dependency on which process inserted it or when; (3) STABLE across
replays and across a FULL REBUILD of this table (migration 075's
reconciler, CHAOS-3563 round-3 review A, drops and recreates this table
from ``projects FINAL`` when its shape is stale) -- recomputing the hash
of the SAME content always yields the SAME tie-break result, unlike
``write_seq``, which is freshly (and differently) generated on every
insert and could not survive a rebuild with the same answer. This directly
serves this table's own "derived, rebuildable store" design principle.

Residual (honestly documented, and now an actual probability rather than
a mis-stated one): a full ``version_key`` tie requires BOTH ``updated_at``
and ``last_synced`` to coincide exactly (already the rare C1 scenario) AND
a genuine ``cityHash64`` COLLISION in the low 22 bits for two rows with
DIFFERENT content -- approximately 1-in-4.19-million per such pair, this
time relying on a general-purpose hash function's actual distribution
properties rather than an id-generator's internal bit layout that was
never designed to be masked this way.

``_PROJECT_DECLARED_FACTS_SQL``'s own ``argMax`` tie-break is
``(updated_at, version_key)`` -- reading ``version_key`` DIRECTLY (it is a
real, queryable column despite being ``MATERIALIZED``), never recomputing
an independent expression and reasoning that it "should" match the
engine's own value. Verified 20/20 agreement across repeated live runs.

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

``project_declared_state_floor`` (PR #1602 review F4, corrected by round-2
review NEW-1): a SEPARATE table, written ONLY by this migration's own
backfill INSERT below, recording the floor instant -- the ``updated_at`` a
project held at THIS migration's run time -- for every project that
existed pre-migration. This is how a reader distinguishes the two DIFFERENT
reasons a project can have retained history that entirely postdates a
requested ``as_of``: (a) a floor row exists for this project, meaning it
existed pre-migration and real state may have existed even earlier that
this migration's own floor could not recover (genuine floor breach, must
render as an explicit "unknown, not absent" signal); versus (b) no floor
row exists, meaning this project was created AFTER the migration ran and
its retained history already IS the complete history back to its true
creation (the requested `as_of` simply predates the project's existence --
must render as plain absence, same as any other not-yet-created entity,
never as an "unknown past" warning).

NEW-1's correction (why this is a SEPARATE table, not a column on
``project_declared_state_history``): an earlier version of this migration
put the floor marker in an ``is_backfill_floor`` column on the history
table itself. That table's ReplacingMergeTree version column is
``last_synced``, not the floor marker -- an ORDINARY re-sync of a project
whose declared state has NOT changed writes a new row sharing the exact
same ``(org_id, provider, id, updated_at)`` key as the backfilled row (the
provider's own mtime is unchanged), but with a FRESHER ``last_synced`` and
``is_backfill_floor = 0`` (every ordinary writer's default). After the next
background merge, ReplacingMergeTree keeps that fresher 0 row and the
floor marker is gone -- often within one sync cycle of the migration
running. Worse, while both rows coexist pre-merge, `argMin(is_backfill_
floor, updated_at)` ties on `updated_at` (both rows share it) and returns
an ARBITRARY one of the two, observed to flip between runs -- the exact
"answer can flip across a background merge" instability class review
finding F6 already fixed elsewhere in this migration's own query,
reintroduced by the column approach. This table is written ONLY here and
NEVER by any ordinary sync, so nothing can ever merge over or discard a
floor row once recorded -- it survives arbitrary sync + ``OPTIMIZE ...
FINAL`` cycles. Keyed one level coarser than the history table (no
``updated_at`` in the key) because there is exactly one floor instant per
``(org_id, provider, id)``.

KNOWN RESIDUAL AMBIGUITY (codex cross-system review C2, MEDIUM, accepted
as-is): a project whose provider-side genesis happens to fall EXACTLY at
this migration's own floor instant -- i.e. it was created so close to (at
or just before) migration time that its floor-seeded state genuinely WAS
its first-ever declared state, never anything earlier -- is
INDISTINGUISHABLE, by this design, from a project that existed well before
migration with real earlier state the floor could not recover. Both
produce the identical floor-table shape: a floor row exists, dated at that
one instant. A request for ``as_of`` strictly before that instant
therefore renders the "unknown, not absent" floor-breach warning in BOTH
cases, even though the former's true answer is knowable ("absent -- this
project did not exist yet"). This is not a bug to fix: the floor table
records "what was this project's state AT migration time", by
construction the ONLY fact ``projects FINAL`` could ever supply, with no
way to also know whether that captured state was the project's genuine
origin or a later snapshot of something older. Resolving it would require
a source of truth this migration does not have (e.g. the provider's own
creation timestamp, tracked separately, out of scope here). The floor
table's presence-only signal is deliberately conservative: it is willing
to say "unknown" one instant too often (this one, rare, boundary case)
rather than ever silently mislabel a genuine floor breach as absence.

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

# PR #1602 review NEW-1: written ONLY by this migration's backfill below --
# never by an ordinary sync -- so a floor row, once recorded, can never be
# overwritten or merged away (see this module's docstring for why the
# earlier is_backfill_floor COLUMN approach was unsound).
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
    client.command(_CREATE_FLOOR_TABLE_SQL)

    if not _table_exists(client, "projects"):
        log.info(
            "  projects does not exist yet -- nothing to backfill, skipping "
            "(no pre-existing declared state to seed)"
        )
        log.info("=== Migration 074: Complete (table created, no backfill) ===")
        return

    client.command(_BACKFILL_SQL)
    client.command(_FLOOR_BACKFILL_SQL)
    log.info("=== Migration 074: Complete (table created, backfill seeded) ===")
