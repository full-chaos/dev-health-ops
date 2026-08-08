"""CHAOS-3463: ``ask-dev-world.v1`` snapshot / restore.

Why this module exists at all
=============================

``fixtures world`` (``world.py``) can only ever be pointed at an explicit
scratch database -- ``_require_scratch_database`` hard-denies ``default``
(ClickHouse) and ``postgres`` (Postgres), which are exactly the two
databases the ask-dev acceptance stack's API actually serves. That guard is
correct and is NOT relaxed here: ``fixtures world`` still refuses those
names, unchanged.

Two independent problems had to be solved together (CHAOS-3219 Phase 2
exit blockers B2 + B3), and one mechanism solves both:

* **B2** -- nothing seeded the world into the DBs the acceptance API reads.
* **B3** -- per-boot regeneration *cannot* reproduce the pinned
  ``WORLD_DIGEST``. Cross-generation digest reproducibility is
  declared-blocked (``world.json``'s ``cross_generation_digest_status``,
  CHAOS-3432) and is explicitly NOT reopened here. Only SINGLE-generation
  pinning is proven technology in this repo.

So: **generate ONCE into a scratch database, snapshot it, restore that
snapshot at every boot, and re-mint the pin from the restored state.**
Every boot then serves bytes descended from one single generation, which is
precisely the regime single-generation pinning already covers.

Why restore is not a hole in the scratch guard
==============================================

``_require_scratch_database`` exists to fence ``fixtures world``'s
*destructive generation* verbs -- ``ALTER TABLE ... DELETE``, the
delete-and-reinsert in ``source_health.age_source_rows``, ``DROP``/``CREATE
DATABASE``. None of those are on this module's restore path.
:func:`restore_world` issues no DDL and deletes nothing. It writes exactly
two kinds of statement: ``INSERT`` of the snapshot's own rows, and -- see
:func:`_align_reference_ids` -- an ``UPDATE`` of the surrogate primary key
of migration-seeded *reference* rows (today only ``feature_flags``), matched
by their natural key, so that a per-database random id does not make the
world's own rows differ on every boot. That UPDATE is stated plainly rather
than buried: it is a write to a table the snapshot does not carry, and it is
guarded by its own precondition (nothing may yet reference the rows being
renumbered) on top of everything below.

It refuses to run unless all of the following hold, checked before a single
row is written:

1. ``ENVIRONMENT == "acceptance"`` -- set on the acceptance ``api``
   container by ``tests/acceptance/compose.ask-dev.yml``, and on nothing
   else. A dev/prod container never reports ``acceptance``.
2. Every table the snapshot carries is EMPTY in the target. A *populated*
   dev or production database always has organizations, commits and work
   items, so it always fails this. Stated precisely, because the obvious
   stronger claim is false: a freshly-migrated, still-empty dev database
   would satisfy this predicate. It is (1) that stops that case, and (1) is
   a deliberate act to defeat -- someone would have to export
   ``ENVIRONMENT=acceptance`` themselves. That residual is named here rather
   than papered over (Codex adversarial review, HIGH).
3. The target's schema fingerprint -- Postgres alembic head(s), the applied
   ClickHouse migration versions, and the ClickHouse server version -- matches
   the one recorded when the snapshot was minted. Checked BEFORE any write,
   so a migration-head or image change fails as a preflight rather than as a
   raw insert error part way through a restore.

There is deliberately no ``--force``.

What this path is NOT: it is not atomic. ClickHouse has no transaction
spanning the inserts, and Postgres is a separate transaction from them. A
crash mid-restore leaves partial state, and the next attempt then fails the
emptiness precondition rather than silently completing. That is the intended
direction to fail in for a stack whose every boot begins with ``down
--volumes``, but it does mean recovery is "recreate the volumes", not
"re-run".

The two oracles
===============

Neither "the snapshot covers everything the world writes" nor "the restore
reproduced the source" is asserted from reading this code -- both are
measured, every run:

* **Completeness (mint time).** The table set to snapshot is not a
  hardcoded list. It is *derived*: every table whose row count in the
  generated scratch database differs from its row count in a
  freshly-migrated baseline database. A table the world writes that nobody
  remembered to enumerate is therefore included automatically; a table
  whose rows come from migrations (``feature_flags``, ``permissions``,
  ``alembic_version``, ClickHouse ``schema_migrations``, ...) is excluded
  automatically, with no ignore list to rot.
* **Round trip (restore time).** Different measure per store, because the two
  stores have different notions of a stable identity:

  - *Postgres* -- the per-table row-count DELTA the restore produced in the
    target must equal the delta the generation produced in the source, for
    EVERY table in either map, not merely for the snapshotted ones. A table
    the snapshot missed shows up as ``expected +N, got +0``.
  - *ClickHouse* -- a per-table CONTENT hash (through ``FINAL``) must be
    identical in both databases, again for every table. Counts cannot be used
    here: background merges collapse ReplacingMergeTree/AggregatingMergeTree
    duplicates on their own schedule, so a count is a moving target (live:
    ``teams: expected +16, got +10`` for a restore that had inserted all 16).
    The content hash is both merge-invariant and strictly stronger -- it also
    catches the right number of wrong rows.

Finally :func:`restore_world` recomputes the full ``WORLD_DIGEST`` of the
restored database and compares it to the pinned file. A boot whose restored
world has drifted fails at boot, before any acceptance case runs, rather
than minting receipts against a world nobody verified.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any

from dev_health_ops.fixtures.ttl_horizon import TTL_SAFETY_MARGIN
from dev_health_ops.fixtures.ttl_registry import (
    TTL_SAFETY_MARGIN_DAYS,
    clickhouse_ttl_retentions,
    snapshot_expiry,
)
from dev_health_ops.fixtures.world import (
    WorldManifest,
    _require_scratch_database,
    compute_world_digest,
    default_digest_path,
    load_world_manifest,
    read_pinned_digest,
    world_manifest_contract_hash,
    write_digest,
)
from dev_health_ops.storage import run_with_store

logger = logging.getLogger(__name__)

SNAPSHOT_SCHEMA_VERSION = "ask_dev_world_snapshot.v1"

#: The only value of ``ENVIRONMENT`` :func:`restore_world` will write under.
#: ``tests/acceptance/compose.ask-dev.yml`` sets exactly this on the
#: acceptance ``api``/``worker``/``beat`` containers; the dev compose stack
#: does not.
ACCEPTANCE_ENVIRONMENT = "acceptance"

#: ClickHouse system tables that are never part of a world snapshot even if
#: their counts differ -- the migration ledger belongs to whichever database
#: applied the migrations, never to the fixture payload. (This is the ONE
#: name-based exclusion in the module, and it is an exclusion of a table that
#: the baseline diff would already exclude in every ordinary case; it exists
#: so a target migrated at a *different* head fails on the head mismatch it
#: really has, rather than by trying to overwrite its own ledger.)
_CLICKHOUSE_LEDGER_TABLES = frozenset({"schema_migrations"})

#: Same, for Postgres.
_POSTGRES_LEDGER_TABLES = frozenset({"alembic_version"})

__all__ = [
    "SNAPSHOT_SCHEMA_VERSION",
    "SnapshotError",
    "RestoreRefusedError",
    "RestoreDriftError",
    "snapshot_world",
    "restore_world",
]


class SnapshotError(RuntimeError):
    """The snapshot could not be taken, or could not be taken honestly."""


class RestoreRefusedError(RuntimeError):
    """The restore target failed a safety precondition -- nothing written.

    Raised BEFORE any write. Never downgraded to a warning: a restore that
    proceeds against a database it cannot prove is a disposable, freshly
    migrated acceptance target is exactly the failure mode
    ``_require_scratch_database`` protects ``fixtures world`` from, and this
    path must fail just as closed.
    """


class SnapshotExpiredError(RuntimeError):
    """The snapshot is older than the shelf life its own generation bought.

    CHAOS-3432/3544. ClickHouse TTLs delete rows on load, so a restored world
    is not the world that was snapshotted once enough real time has passed --
    the bytes are identical and the TABLE is not. Generated history stops a
    full ``TTL_SAFETY_MARGIN`` inside the tightest TTL, and that margin is
    exactly how long a snapshot stays restorable.

    Raised BEFORE the content oracle, deliberately. The oracle would also
    fail -- with a hash mismatch on whichever table happened to cross its
    horizon first, which is how this defect spent months being attributed to
    generator nondeterminism. "SNAPSHOT EXPIRED, re-mint required" is a
    five-minute fix; "feature_flag_event: source=32c53f52 target=0160527f" at
    2am is a night of archaeology for the same cause.
    """


class RestoreDriftError(RuntimeError):
    """The restored state does not match what the snapshot promised.

    Either the round-trip row-count oracle disagreed, or the recomputed
    ``WORLD_DIGEST`` does not match the pinned one. Both mean the acceptance
    stack must NOT serve this world (ruling D2).
    """


# ---------------------------------------------------------------------------
# Value codec (Postgres rows -> JSON -> Postgres rows)
# ---------------------------------------------------------------------------
#
# Deliberately tag-based and self-describing rather than type-driven: the
# decoder never needs the reflected column type, so a snapshot stays
# readable even if the schema is later reflected differently. Every tag
# round-trips to the SAME Python type SQLAlchemy handed us, which is what
# keeps `compute_world_digest`'s row hashes identical across the trip
# (a str/`datetime` confusion would silently change the hash).

_TAG = "__t__"
_VAL = "v"


def _encode_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, uuid.UUID):
        return {_TAG: "uuid", _VAL: str(value)}
    if isinstance(value, datetime):
        return {_TAG: "datetime", _VAL: value.isoformat()}
    if isinstance(value, date):
        return {_TAG: "date", _VAL: value.isoformat()}
    if isinstance(value, time):
        return {_TAG: "time", _VAL: value.isoformat()}
    if isinstance(value, timedelta):
        return {_TAG: "timedelta", _VAL: value.total_seconds()}
    if isinstance(value, Decimal):
        return {_TAG: "decimal", _VAL: str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {_TAG: "bytes", _VAL: base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return {_TAG: "list", _VAL: [_encode_value(item) for item in value]}
    if isinstance(value, dict):
        # A JSON/JSONB column value. Encoded under its own tag so a plain
        # dict payload can never be confused with one of this codec's own
        # tagged envelopes.
        return {_TAG: "json", _VAL: json.dumps(value, sort_keys=True)}
    raise SnapshotError(
        f"world snapshot: no encoding for value of type {type(value)!r} "
        f"({value!r}). Refusing to write a snapshot that cannot be restored "
        "losslessly -- add an explicit encoding rather than coercing to str."
    )


def _decode_value(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    tag = value.get(_TAG)
    if tag is None:
        return value
    payload = value[_VAL]
    if tag == "uuid":
        return uuid.UUID(payload)
    if tag == "datetime":
        return datetime.fromisoformat(payload)
    if tag == "date":
        return date.fromisoformat(payload)
    if tag == "time":
        return time.fromisoformat(payload)
    if tag == "timedelta":
        return timedelta(seconds=payload)
    if tag == "decimal":
        return Decimal(payload)
    if tag == "bytes":
        return base64.b64decode(payload)
    if tag == "list":
        return [_decode_value(item) for item in payload]
    if tag == "json":
        return json.loads(payload)
    raise SnapshotError(f"world snapshot: unknown value tag {tag!r}")


# ---------------------------------------------------------------------------
# Row-count maps (the raw material both oracles are built from)
# ---------------------------------------------------------------------------


#: MergeTree families whose rows are collapsed by BACKGROUND merges, so a raw
#: ``count()`` is a moving target and only the ``FINAL`` view is a stable
#: identity. Found live: restoring ``teams`` inserted exactly the 16 rows the
#: source held and a moment later ``count()`` read 10, because a merge had
#: collapsed the ReplacingMergeTree duplicates in between.
_COLLAPSING_ENGINE_MARKERS = (
    "Replacing",
    "Summing",
    "Aggregating",
    "Collapsing",
)


def _collapses_on_merge(engine: str) -> bool:
    return any(marker in engine for marker in _COLLAPSING_ENGINE_MARKERS)


async def _clickhouse_table_engines(client: Any) -> dict[str, str]:
    result = await asyncio.to_thread(
        client.query,
        "SELECT name, engine FROM system.tables WHERE database = {db:String} "
        "AND engine NOT LIKE '%View%' ORDER BY name",
        parameters={"db": client.database},
    )
    return {str(name): str(engine) for name, engine in result.result_rows}


async def _clickhouse_row_counts(client: Any) -> dict[str, int]:
    engines = await _clickhouse_table_engines(client)
    counts: dict[str, int] = {}
    for name in engines:
        rows = await asyncio.to_thread(client.query, f"SELECT count() FROM `{name}`")
        counts[str(name)] = int(rows.result_rows[0][0])
    return counts


async def _clickhouse_content_hashes(client: Any) -> dict[str, str]:
    """A merge-invariant content fingerprint of every ClickHouse table.

    This -- not a row count -- is the ClickHouse round-trip oracle.
    ``count()`` on a ReplacingMergeTree/AggregatingMergeTree changes on its
    own as background merges collapse duplicates, so comparing counts across
    two databases is comparing two moving targets (live: ``teams: expected
    +16, got +10`` for a restore that had in fact inserted all 16 rows). The
    ``FINAL`` view is what those engines exist to present and what every
    reader actually sees, so that is what is hashed.

    Covers EVERY table, not just the 14 in ``_CLICKHOUSE_DIGEST_TABLES``:
    ``WORLD_DIGEST`` deliberately scopes itself to the world's own orgs and
    tables, while this has to catch a restore that quietly dropped or doubled
    anything anywhere.
    """

    engines = await _clickhouse_table_engines(client)
    hashes: dict[str, str] = {}
    for name, engine in engines.items():
        if name in _CLICKHOUSE_LEDGER_TABLES:
            # `schema_migrations` records WHEN each migration ran in THIS
            # database. Two databases migrated to the same head hold the same
            # migration names and different timestamps, so comparing its
            # content across them is comparing two clocks. Head equality is
            # what matters and is enforced elsewhere (a target at a different
            # head fails on a missing table or a missing reference row, with a
            # message that names the real problem).
            continue
        final = " FINAL" if _collapses_on_merge(engine) else ""
        result = await asyncio.to_thread(client.query, f"SELECT * FROM `{name}`{final}")
        digest = hashlib.sha256()
        for row in sorted(repr(tuple(row)) for row in result.result_rows):
            digest.update(row.encode())
            digest.update(b"\n")
        hashes[name] = digest.hexdigest()
    return hashes


async def _clickhouse_materialized_view_targets(client: Any) -> set[str]:
    """Tables that a materialized view writes into.

    They must NOT be restored in the same pass as everything else: inserting
    the snapshot's ``git_commits`` makes ``commit_daily_rollup_mv`` fire and
    write its own rows into ``commit_daily_rollup``, so restoring that table's
    snapshot rows too leaves it holding both (live: ``commit_daily_rollup:
    expected +161, got +302``). :func:`_restore_clickhouse` therefore restores
    every other table first, then truncates and refills these.
    """

    result = await asyncio.to_thread(
        client.query,
        "SELECT name, create_table_query FROM system.tables "
        "WHERE database = {db:String} AND engine = 'MaterializedView'",
        parameters={"db": client.database},
    )
    targets: set[str] = set()
    for name, create_query in result.result_rows:
        match = re.search(
            r"\bTO\s+`?[A-Za-z_0-9]+`?\.`?([A-Za-z_0-9]+)`?", str(create_query)
        )
        if match is None:
            raise SnapshotError(
                f"world snapshot/restore: materialized view {name!r} has no "
                "explicit `TO <db>.<table>` target, so which table it writes "
                "into cannot be determined. Refusing rather than guessing -- an "
                "unhandled MV silently doubles its target's rows on restore."
            )
        targets.add(match.group(1))
    return targets


_PG_BASE_TABLES_SQL = (
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_type = 'BASE TABLE' AND table_schema = 'public' "
    "ORDER BY table_name"
)


async def _postgres_row_counts(conn: Any) -> dict[str, int]:
    from sqlalchemy import text

    names = [
        row[0] for row in (await conn.execute(text(_PG_BASE_TABLES_SQL))).fetchall()
    ]
    counts: dict[str, int] = {}
    for name in names:
        value = (
            await conn.execute(text(f'SELECT count(*) FROM public."{name}"'))
        ).scalar_one()
        counts[str(name)] = int(value)
    return counts


async def _clickhouse_schema_fingerprint(client: Any) -> dict[str, Any]:
    """What the snapshot's ClickHouse bytes are only valid against.

    The applied-migration versions (``schema_migrations.version``, NOT its
    ``applied_at`` -- that differs per database and says nothing about
    compatibility) plus the ClickHouse server version, since ``Native`` is a
    server-version-coupled binary format.
    """

    migrations = await asyncio.to_thread(
        client.query, "SELECT version FROM schema_migrations ORDER BY version"
    )
    version = await asyncio.to_thread(client.query, "SELECT version()")
    # The actual catalog, not just the migration ledger. Codex adversarial
    # review round 2 (MEDIUM, confirmed): version names alone do not fingerprint
    # a SCHEMA -- hand-run DDL, an edited migration under an unchanged version,
    # a column type change, or an engine/ORDER BY change all pass a
    # version-only check while making the Native payload no longer match the
    # target. Deliberately excludes the database NAME (the source is a scratch
    # database and the target is not), so only shape is compared.
    tables = await asyncio.to_thread(
        client.query,
        "SELECT name, engine, sorting_key, primary_key FROM system.tables "
        "WHERE database = {db:String} AND engine NOT LIKE '%View%' ORDER BY name",
        parameters={"db": client.database},
    )
    columns = await asyncio.to_thread(
        client.query,
        "SELECT table, name, type, position, default_expression "
        "FROM system.columns WHERE database = {db:String} ORDER BY table, position",
        parameters={"db": client.database},
    )
    catalog = hashlib.sha256()
    for row in tables.result_rows:
        catalog.update(("\t".join(str(value) for value in row) + "\n").encode())
    for row in columns.result_rows:
        catalog.update(("\t".join(str(value) for value in row) + "\n").encode())
    return {
        "migrations": [str(row[0]) for row in migrations.result_rows],
        "server_version": str(version.result_rows[0][0]),
        "catalog_sha256": catalog.hexdigest(),
    }


async def _postgres_schema_fingerprint(conn: Any) -> dict[str, Any]:
    from sqlalchemy import text

    rows = (
        await conn.execute(text("SELECT version_num FROM alembic_version ORDER BY 1"))
    ).fetchall()
    # See the ClickHouse sibling: the alembic head alone is a version label,
    # not a schema. This hashes the real column catalog of every public table,
    # so DDL drift under an unchanged head is caught before any write.
    catalog_rows = (
        await conn.execute(
            text(
                "SELECT table_name, column_name, data_type, is_nullable, "
                "ordinal_position, column_default FROM information_schema.columns "
                "WHERE table_schema = 'public' ORDER BY table_name, ordinal_position"
            )
        )
    ).fetchall()
    catalog = hashlib.sha256()
    for row in catalog_rows:
        catalog.update(("\t".join(str(value) for value in row) + "\n").encode())
    return {
        "alembic_heads": [str(row[0]) for row in rows],
        "catalog_sha256": catalog.hexdigest(),
    }


def _assert_schema_compatible(*, store: str, minted: Any, live: dict[str, Any]) -> None:
    """Refuse BEFORE writing when the target's schema is not the one the
    snapshot was minted against.

    Codex adversarial review (MEDIUM, confirmed): without this the first sign
    of a migration-head or ClickHouse-image change was a raw insert failing
    PART WAY THROUGH the restore, leaving partial state and an error that
    named a column rather than the real cause. Now it is a preflight with a
    message that says what to do (re-mint).

    A snapshot minted before this field existed carries no fingerprint; that
    is reported as unverifiable rather than silently accepted.
    """

    if not minted:
        raise RestoreRefusedError(
            f"world restore: the snapshot records no {store} schema "
            "fingerprint, so its compatibility with this target cannot be "
            "checked. Re-mint it with a build that records one."
        )
    if minted != live:
        raise RestoreRefusedError(
            f"world restore: {store} schema fingerprint mismatch. The snapshot "
            f"was minted against {minted!r}; this target is {live!r}. The "
            "snapshot's bytes are only valid against the schema they were "
            "taken from -- re-mint it (scripts/acceptance/"
            "mint_ask_dev_world_snapshot.sh) rather than restoring this one."
        )


async def _with_clickhouse_client(sink: str, handler: Any) -> Any:
    box: dict[str, Any] = {}

    async def _run(store: Any) -> None:
        box["result"] = await handler(store.client)

    await run_with_store(sink, "clickhouse", _run, org_id=None)
    return box["result"]


async def _with_postgres_conn(postgres_uri: str, handler: Any) -> Any:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(postgres_uri, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            return await handler(conn)
    finally:
        await engine.dispose()


def _changed_tables(
    *,
    source: dict[str, int],
    baseline: dict[str, int],
    store: str,
    ledger: frozenset[str],
) -> list[str]:
    """Tables the world generation actually wrote, derived by execution.

    A table whose baseline count is non-zero AND whose source count differs
    is a case this module's table-granular, insert-only restore cannot
    reproduce (it would double-count the migration-seeded rows). That is
    reported as an outright failure rather than silently snapshotted -- an
    unhandled case must fail loudly, not produce a snapshot that restores
    wrong.
    """

    changed: list[str] = []
    unsupported: list[str] = []
    for table in sorted(set(source) | set(baseline)):
        if table in ledger:
            continue
        src = source.get(table, 0)
        base = baseline.get(table, 0)
        if src == base:
            continue
        if base != 0:
            unsupported.append(f"{table} (baseline={base}, generated={src})")
            continue
        if src < base:  # pragma: no cover -- implied by base != 0 above
            unsupported.append(f"{table} (baseline={base}, generated={src})")
            continue
        changed.append(table)
    if unsupported:
        raise SnapshotError(
            f"world snapshot: {store} table(s) were already non-empty in the "
            f"freshly-migrated baseline and the world changed them: "
            f"{unsupported}. A table-granular, insert-only snapshot cannot "
            "reproduce that without double-counting the pre-existing rows. "
            "Fix the world generation (or teach this module a row-level "
            "strategy for that table) -- do not weaken this check."
        )
    return changed


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def _assert_no_ttl_horizon_rows(client: Any, tables: list[str]) -> None:
    """CHAOS-3602: refuse to snapshot a table holding rows within
    ``TTL_SAFETY_MARGIN_DAYS`` of their own TTL deletion horizon.

    ClickHouse applies TTL deletion AT MERGE TIME -- asynchronously,
    silently, with no error, no warning. A table with horizon-adjacent rows
    is racing the background merge scheduler: whether a row survives to be
    read depends entirely on exactly when a merge happens to run relative
    to when this snapshot reads the table.

    CHAOS-3432/3544 already keeps fixture GENERATION a margin inside the
    single tightest schema-wide TTL horizon (``ttl_horizon.py``), and that
    is the primary defense. This is the belt-and-braces guard on top of it,
    scoped PER TABLE (via :func:`clickhouse_ttl_retentions`, parsed from the
    migration source the same way) and checked against the LIVE data right
    before any dump runs -- catching a violation from any source (clock
    drift, a future generator that forgets to clamp, hand-seeded data)
    before it can race the merge scheduler, rather than discovering it via
    a content-oracle mismatch after data has already been silently lost.
    """

    retentions = clickhouse_ttl_retentions()
    violations: list[str] = []
    for table in tables:
        retention = retentions.get(table)
        if retention is None:
            continue
        safe_days = max(0, retention.retention_days - TTL_SAFETY_MARGIN_DAYS)
        result = await asyncio.to_thread(
            client.query,
            f"SELECT count() FROM `{table}` "
            f"WHERE `{retention.column}` <= now() - INTERVAL {safe_days} DAY",
        )
        count = int(result.result_rows[0][0]) if result.result_rows else 0
        if count:
            violations.append(
                f"{table}: {count} row(s) with {retention.column} at or "
                f"past {safe_days} days old (this table's TTL deletes at "
                f"{retention.retention_days} days)"
            )

    if violations:
        raise SnapshotError(
            "world snapshot: refusing to snapshot table(s) holding rows "
            "within their own TTL deletion margin -- a mint over "
            "horizon-adjacent data races ClickHouse's background merge "
            "scheduler rather than producing a deterministic artifact "
            "(CHAOS-3602). Regenerate the world so every date stays inside "
            "its table's safe margin "
            f"(dev_health_ops.fixtures.ttl_registry.max_safe_backdate_days): "
            f"{violations}"
        )


_DUMP_VERIFY_ATTEMPTS = 3


async def _dump_clickhouse(
    client: Any, table: str, engine: str, path: Path, *, raw_source_row_count: int
) -> int:
    """Dump one ClickHouse table, through ``FINAL`` where the engine collapses.

    Two decisions, both learned live:

    ``Native`` is ClickHouse's own binary column format -- an exact,
    type-preserving round trip. A text format (JSONEachRow/CSV) would silently
    reformat DateTime64 precision and Decimal scale, which
    ``compute_world_digest`` hashes; the digest guard would then fail on every
    boot for a reason that has nothing to do with the world.

    ``FINAL`` matters because a ReplacingMergeTree with no version column
    keeps the LAST row inserted for a key, and "last" is decided by insertion
    order. ``work_item_cycle_times`` held 223 raw rows collapsing to 220; a
    raw dump re-inserted them as one unordered block, and the restored table
    picked a different survivor for the three duplicated keys -- same 220 rows
    under ``FINAL``, different CONTENT (caught by the content oracle, not by
    the count). Dumping the ``FINAL`` view stores exactly the rows every
    reader can actually see and makes the restore deterministic. Nothing
    visible is lost: the discarded rows are ones ``FINAL`` already hides.

    CHAOS-3602: ``raw_query``'s Native payload and a ``count()`` run moments
    later, on a fully idle table with zero concurrent writes, have been
    observed to silently DISAGREE -- both a mint's manifest.json and a
    dedicated stress run recorded ``count() == 1042`` while the payload
    ``raw_query`` had actually captured only 1041 rows of
    ``feature_flag_event``. This is a flake in the raw Native transfer
    itself (clickhouse-connect and/or server), not a write-visibility race
    (reproduced with no writes in flight) and not query pagination (this
    function issues exactly one unpaginated ``SELECT *``, no ORDER BY/LIMIT/
    OFFSET). ~2% per-table-dump in stress testing, which is not negligible
    across the ~90 ClickHouse tables one mint dumps.

    So the payload is never trusted on the strength of a *separate* count()
    query -- it is DECODED (via a throwaway staging table, itself covered by
    ``_require_scratch_database`` since ``client`` only ever points at a
    scratch database here) and the decoded row count is what gets compared,
    retried, and ultimately recorded in the manifest. A short payload is
    retried up to ``_DUMP_VERIFY_ATTEMPTS`` times and never written to disk.
    """

    final = " FINAL" if _collapses_on_merge(engine) else ""
    verify_table = f"__snapshot_verify_{table}"
    last_diagnostics = ""

    for attempt in range(1, _DUMP_VERIFY_ATTEMPTS + 1):
        payload: bytes = await asyncio.to_thread(
            partial(client.raw_query, f"SELECT * FROM `{table}`{final}", fmt="Native")
        )
        pre_count = await asyncio.to_thread(
            client.query, f"SELECT count() FROM `{table}`{final}"
        )
        expected = int(pre_count.result_rows[0][0])

        # Decode what the payload ACTUALLY holds, server-side, via a
        # throwaway staging table -- not a second independent count() on the
        # source table, which is exactly the query that was observed to
        # agree with a short payload above.
        await asyncio.to_thread(
            client.command, f"DROP TABLE IF EXISTS `{verify_table}`"
        )
        await asyncio.to_thread(
            client.command, f"CREATE TABLE `{verify_table}` AS `{table}`"
        )
        try:
            await asyncio.to_thread(
                partial(
                    client.raw_insert,
                    table=verify_table,
                    insert_block=payload,
                    fmt="Native",
                )
            )
            decoded = await asyncio.to_thread(
                client.query, f"SELECT count() FROM `{verify_table}`"
            )
            decoded_count = int(decoded.result_rows[0][0])
        finally:
            await asyncio.to_thread(
                client.command, f"DROP TABLE IF EXISTS `{verify_table}`"
            )

        if decoded_count == expected:
            path.write_bytes(gzip.compress(payload, mtime=0))
            return decoded_count

        parts_result = await asyncio.to_thread(
            client.query,
            "SELECT count() FROM system.parts WHERE table = {t:String} "
            "AND database = {db:String} AND active",
            parameters={"t": table, "db": client.database},
        )
        active_parts = (
            int(parts_result.result_rows[0][0]) if parts_result.result_rows else -1
        )
        last_diagnostics = (
            f"table={table!r} attempt={attempt}/{_DUMP_VERIFY_ATTEMPTS} "
            f"decoded_payload_rows={decoded_count} expected_count={expected} "
            f"raw_source_row_count={raw_source_row_count} active_parts={active_parts} "
            f"engine={engine!r}"
        )
        logger.warning(
            "world snapshot: clickhouse dump payload came up short of its own "
            "count() -- retrying. %s",
            last_diagnostics,
        )

    raise SnapshotError(
        "world snapshot: clickhouse dump produced a SHORT payload on every "
        f"attempt (CHAOS-3602 -- a known intermittent raw Native transfer "
        f"flake, not app logic). {last_diagnostics}. Refusing to write a "
        "short snapshot artifact to disk."
    )


async def _reflect(conn: Any, table: str, metadata: Any) -> Any:
    import sqlalchemy

    return await conn.run_sync(
        lambda sync_conn: sqlalchemy.Table(
            table, metadata, autoload_with=sync_conn, schema="public"
        )
    )


async def _natural_key_columns(conn: Any, table: str) -> list[str]:
    """The stable, cross-database identity of a row in ``table``.

    A UNIQUE constraint other than the primary key. Needed because some
    migration-seeded tables key on a per-database surrogate: ``feature_flags``
    generates a fresh random ``id`` in every database it is migrated into, so
    a snapshot that copied ``org_feature_overrides.feature_id`` verbatim
    points at a UUID that exists in the source database and nowhere else
    (live-reproduced: ``ForeignKeyViolationError ... Key (feature_id)=(6cd6…)
    is not present in table "feature_flags"``). ``feature_flags.key`` is the
    identity that actually travels.
    """

    import sqlalchemy

    constraints = await conn.run_sync(
        lambda sync_conn: sqlalchemy.inspect(sync_conn).get_unique_constraints(
            table, schema="public"
        )
    )
    candidates = sorted(
        (sorted(c["column_names"]) for c in constraints if c.get("column_names")),
        key=lambda cols: (len(cols), cols),
    )
    if not candidates:
        raise SnapshotError(
            f"world snapshot: table {table!r} is referenced by a snapshotted "
            "foreign key but is not itself snapshotted, and it has no UNIQUE "
            "constraint to identify its rows across databases. Its primary key "
            "may be a per-database surrogate, in which case restoring the "
            "referencing rows verbatim would produce a dangling reference. "
            "Refusing to guess."
        )
    return candidates[0]


def foreign_key_is_live(
    rows: list[Any], column_index: dict[str, int], local_columns: list[str]
) -> bool:
    """Does any row actually reference something through this foreign key?

    ``all(...)`` inside ``any(...)``, and the distinction is load-bearing:
    SQL's default MATCH SIMPLE means a composite foreign key is NOT enforced
    for a row where ANY of its columns is NULL. ``dev_runs`` carries a
    composite key ``(answer_id, org_id, user_id)`` into ``dev_messages`` where
    ``answer_id`` is NULL but ``org_id``/``user_id`` are not. Testing
    ``any(column is not NULL)`` read that as a live reference into an
    un-snapshotted table and refused a snapshot that restores perfectly.
    Found live, on the second mint attempt -- not from reading the schema.
    """

    return any(
        all(row[column_index[name]] is not None for name in local_columns)
        for row in rows
    )


async def _referenced_surrogate_tables(
    conn: Any, tables: list[str], metadata: Any
) -> dict[str, list[str]]:
    """Tables the snapshot REFERENCES but does not itself carry, mapped to the
    natural key their rows are identified by across databases.

    Found live, and not a corner case: ``org_feature_overrides.feature_id``
    points at ``feature_flags.id``, and ``feature_flags`` is seeded by a
    migration with a FRESH RANDOM UUID in every database it runs in.
    Restoring the referencing rows verbatim produced
    ``ForeignKeyViolationError: Key (feature_id)=(6cd6...) is not present in
    table "feature_flags"``.

    Translating the referencing VALUE at restore time would fix the foreign
    key and break something worse: ``compute_world_digest`` hashes
    ``feature_id``, so a per-boot-different id means a per-boot-different
    digest and a pin that can never match. The reference table's ids are
    therefore ALIGNED to the snapshot instead (:func:`_align_reference_ids`),
    which keeps every snapshotted row byte-identical to the generation it
    came from -- which is exactly what makes the pin verifiable at all.
    """

    referenced: dict[str, list[str]] = {}
    snapshot_tables = set(tables)
    for table in tables:
        reflected = await _reflect(conn, table, metadata)
        column_index = {
            column.name: position for position, column in enumerate(reflected.columns)
        }
        rows = (await conn.execute(reflected.select())).fetchall()
        for constraint in reflected.foreign_key_constraints:
            elements = list(constraint.elements)
            target_table = elements[0].column.table.name
            if target_table in snapshot_tables or target_table in referenced:
                continue
            local_columns = [element.parent.name for element in elements]
            if not foreign_key_is_live(rows, column_index, local_columns):
                continue
            if len(elements) != 1:
                raise SnapshotError(
                    f"world snapshot: composite foreign key {local_columns} on "
                    f"{table!r} points at un-snapshotted table {target_table!r}. "
                    "Composite cross-snapshot references are not supported -- "
                    "refusing rather than restoring a dangling reference."
                )
            referenced[target_table] = await _natural_key_columns(conn, target_table)
    return referenced


async def _dump_reference_tables(
    conn: Any, referenced: dict[str, list[str]], metadata: Any
) -> dict[str, Any]:
    """Record each reference table's natural-key -> surrogate-id mapping as it
    stood in the database the world was generated in."""

    from sqlalchemy import select

    document: dict[str, Any] = {}
    for table, natural_key in sorted(referenced.items()):
        reflected = await _reflect(conn, table, metadata)
        primary_key = [column.name for column in reflected.primary_key.columns]
        if len(primary_key) != 1:
            raise SnapshotError(
                f"world snapshot: reference table {table!r} has a composite "
                f"primary key {primary_key}; id alignment supports a single "
                "surrogate column only."
            )
        rows = (
            await conn.execute(
                select(
                    reflected.c[primary_key[0]],
                    *[reflected.c[name] for name in natural_key],
                )
            )
        ).fetchall()
        ids = {
            json.dumps([_encode_value(value) for value in row[1:]], sort_keys=True): (
                _encode_value(row[0])
            )
            for row in rows
        }
        if len(ids) != len(rows):
            raise SnapshotError(
                f"world snapshot: reference table {table!r} natural key "
                f"{natural_key} is not unique across its {len(rows)} row(s) -- "
                "it cannot identify a row across databases."
            )
        document[table] = {
            "primary_key": primary_key[0],
            "natural_key": natural_key,
            "ids": ids,
        }
    return document


async def _dump_postgres(conn: Any, table: str, path: Path) -> tuple[list[str], int]:
    from sqlalchemy import MetaData

    reflected = await _reflect(conn, table, MetaData())
    columns = [column.name for column in reflected.columns]
    rows = (await conn.execute(reflected.select())).fetchall()
    encoded = [[_encode_value(value) for value in row] for row in rows]
    # Sorted by the canonical JSON of each row: a snapshot must not change
    # byte-for-byte just because Postgres returned an unordered heap in a
    # different physical order on a re-mint.
    encoded.sort(key=lambda row: json.dumps(row, sort_keys=True))
    document = {"table": table, "columns": columns, "rows": encoded}
    path.write_bytes(
        gzip.compress(
            (json.dumps(document, sort_keys=True, indent=1) + "\n").encode(), mtime=0
        )
    )
    return columns, len(rows)


async def snapshot_world(
    *,
    sink: str,
    postgres_uri: str,
    baseline_sink: str,
    baseline_postgres_uri: str,
    out_dir: Path,
    manifest: WorldManifest,
) -> dict[str, Any]:
    """Snapshot the generated world in ``sink``/``postgres_uri`` into
    ``out_dir``, using ``baseline_*`` (a freshly-migrated, unseeded pair)
    to derive -- by execution -- which tables the generation actually wrote.
    """

    # The SOURCE of a snapshot must still be a scratch database: this keeps
    # the "generate once, into scratch" discipline intact end to end and
    # makes it impossible to mint a world snapshot out of a shared dev
    # database that merely happens to look right.
    _require_scratch_database(sink, kind="clickhouse")
    _require_scratch_database(postgres_uri, kind="postgres")

    source_ch = await _with_clickhouse_client(sink, _clickhouse_row_counts)
    baseline_ch = await _with_clickhouse_client(baseline_sink, _clickhouse_row_counts)
    source_pg = await _with_postgres_conn(postgres_uri, _postgres_row_counts)
    baseline_pg = await _with_postgres_conn(baseline_postgres_uri, _postgres_row_counts)

    ch_tables = _changed_tables(
        source=source_ch,
        baseline=baseline_ch,
        store="clickhouse",
        ledger=_CLICKHOUSE_LEDGER_TABLES,
    )
    pg_tables = _changed_tables(
        source=source_pg,
        baseline=baseline_pg,
        store="postgres",
        ledger=_POSTGRES_LEDGER_TABLES,
    )
    # CHAOS-3602: before dumping anything, refuse a snapshot over data that
    # is currently racing a TTL'd table's own background merge scheduler --
    # see _assert_no_ttl_horizon_rows for why this can't wait until after
    # the dump.
    await _with_clickhouse_client(
        sink, lambda client: _assert_no_ttl_horizon_rows(client, ch_tables)
    )

    if not ch_tables and not pg_tables:
        raise SnapshotError(
            "world snapshot: the generated database is indistinguishable from "
            "a freshly-migrated one -- nothing to snapshot. Did `fixtures "
            "world` actually run against --sink/--postgres-uri?"
        )

    ch_dir = out_dir / "clickhouse"
    pg_dir = out_dir / "postgres"
    for directory in (out_dir, ch_dir, pg_dir):
        directory.mkdir(parents=True, exist_ok=True)
    for stale in (*ch_dir.glob("*.native.gz"), *pg_dir.glob("*.json.gz")):
        stale.unlink()

    ch_entries: dict[str, Any] = {}

    async def _dump_all_clickhouse(client: Any) -> None:
        engines = await _clickhouse_table_engines(client)
        for table in ch_tables:
            path = ch_dir / f"{table}.native.gz"
            engine = engines[table]
            # The verified, DECODED payload count -- see _dump_clickhouse's
            # docstring (CHAOS-3602): a separate count() query has been
            # observed to agree with a payload that was actually short, so
            # this number is never sourced from one.
            row_count = await _dump_clickhouse(
                client, table, engine, path, raw_source_row_count=source_ch[table]
            )
            # Postgres has always asserted raw-vs-dumped agreement here (a
            # table changing under the snapshot mid-dump); ClickHouse never
            # did -- the gap found alongside CHAOS-3602. Collapsing engines
            # (ReplacingMergeTree etc.) legitimately dedupe under FINAL, so
            # raw and dumped counts differ BY DESIGN there -- only a bound
            # applies: FINAL cannot ever produce MORE rows than went in.
            # Non-collapsing engines have no such excuse: an exact mismatch
            # means the same "changing/flaky underneath the snapshot"
            # problem postgres already guards.
            if _collapses_on_merge(engine):
                if row_count > source_ch[table]:
                    raise SnapshotError(
                        f"world snapshot: clickhouse table {table!r} dumped "
                        f"{row_count} FINAL rows, more than the "
                        f"{source_ch[table]} raw rows counted before the dump "
                        "-- FINAL cannot increase a row count. The source "
                        "database is changing underneath the snapshot. Stop "
                        "whatever is writing to it and re-run."
                    )
            elif row_count != source_ch[table]:
                raise SnapshotError(
                    f"world snapshot: clickhouse table {table!r} had "
                    f"{source_ch[table]} rows when counted and {row_count} "
                    "when dumped -- the source database is changing "
                    "underneath the snapshot. Stop whatever is writing to "
                    "it and re-run."
                )
            ch_entries[table] = {
                "file": f"clickhouse/{table}.native.gz",
                # The number of rows the FILE holds -- which for a collapsing
                # engine is the FINAL count, not the raw one. Recording the raw
                # count here would describe the artifact wrongly.
                "row_count": row_count,
                "raw_source_row_count": source_ch[table],
                "sha256": _sha256_file(path),
            }

    await _with_clickhouse_client(sink, _dump_all_clickhouse)

    pg_entries: dict[str, Any] = {}
    reference_tables: dict[str, Any] = {}

    async def _dump_all_postgres(conn: Any) -> None:
        from sqlalchemy import MetaData

        metadata = MetaData()
        for table in pg_tables:
            path = pg_dir / f"{table}.json.gz"
            columns, row_count = await _dump_postgres(conn, table, path)
            if row_count != source_pg[table]:
                raise SnapshotError(
                    f"world snapshot: postgres table {table!r} had "
                    f"{source_pg[table]} rows when counted and {row_count} "
                    "when dumped -- the source database is changing underneath "
                    "the snapshot. Stop whatever is writing to it and re-run."
                )
            pg_entries[table] = {
                "file": f"postgres/{table}.json.gz",
                "row_count": row_count,
                "columns": columns,
                "sha256": _sha256_file(path),
            }
        referenced = await _referenced_surrogate_tables(conn, pg_tables, metadata)
        reference_tables.update(
            await _dump_reference_tables(conn, referenced, metadata)
        )

    await _with_postgres_conn(postgres_uri, _dump_all_postgres)

    # Hashed AFTER the dump, so what is recorded is the state the artifact
    # actually carries -- not a fingerprint of a database that then changed.
    source_ch_hashes = await _with_clickhouse_client(sink, _clickhouse_content_hashes)
    ch_schema = await _with_clickhouse_client(sink, _clickhouse_schema_fingerprint)
    pg_schema = await _with_postgres_conn(postgres_uri, _postgres_schema_fingerprint)

    document = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "world_schema_version": manifest.world["schema_version"],
        "master_seed": manifest.master_seed,
        # CHAOS-3432/3544: the snapshot's own shelf life, recorded so a
        # restore can fail on STALENESS rather than on the cryptic content
        # mismatch staleness eventually causes.
        "minted_at": datetime.now(UTC).isoformat(),
        "shelf_life_days": _shelf_life_days(manifest),
        # CHAOS-3602: a second, PER-TABLE-registry-derived expiry instant,
        # informational only -- recorded so a human (or a future restore
        # guard) can see the shelf life implied by ttl_registry's per-table
        # margins without doing the pinned_now + margin arithmetic
        # themselves. Deliberately NOT enforced at restore here: it uses a
        # much tighter margin (TTL_SAFETY_MARGIN_DAYS) than the
        # CHAOS-3432/3544 `shelf_life_days` guard above, which already
        # enforces staleness at restore (`_assert_snapshot_within_shelf_
        # life`) using the wider, schema-tightest-horizon margin this
        # snapshot was actually minted under. Wiring both as enforcing
        # checks would fail restores of already-valid, CHAOS-3432/3544-
        # compliant snapshots on a stricter, uncalibrated-for-this-mint
        # threshold.
        "ttl_shelf_life_expiry": snapshot_expiry(manifest.pinned_now).isoformat(),
        # CHAOS-3463, Codex adversarial review (MEDIUM, confirmed): the two
        # fields above were stamped and then never read by anything -- a
        # recorded value nobody checks is a claim, not a guard. They are now
        # verified at restore, together with this hash of the world.json
        # identity/credential contract, which is the field that actually
        # catches a manifest edit the digest cannot see (see
        # world.world_manifest_contract_hash).
        "world_manifest_contract": world_manifest_contract_hash(manifest),
        "clickhouse": {
            "tables": ch_entries,
            "schema_fingerprint": ch_schema,
            "source_content_hashes": content_hashes_to_manifest(source_ch_hashes),
            "source_row_counts": source_ch,
            "baseline_row_counts": baseline_ch,
        },
        "postgres": {
            "tables": pg_entries,
            "schema_fingerprint": pg_schema,
            "reference_tables": reference_tables,
            "source_row_counts": source_pg,
            "baseline_row_counts": baseline_pg,
        },
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n"
    )
    logger.info(
        "world snapshot: wrote %d clickhouse table(s) and %d postgres table(s) to %s",
        len(ch_entries),
        len(pg_entries),
        out_dir,
    )
    return document


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


def read_snapshot_manifest(snapshot_dir: Path) -> dict[str, Any]:
    path = snapshot_dir / "manifest.json"
    if not path.exists():
        raise RestoreRefusedError(
            f"world restore: no snapshot manifest at {path} -- the acceptance "
            "stack cannot serve a pinned world it does not have. Re-mint with "
            "scripts/acceptance/mint_ask_dev_world_snapshot.sh."
        )
    document = json.loads(path.read_text())
    version = document.get("schema_version")
    if version != SNAPSHOT_SCHEMA_VERSION:
        raise RestoreRefusedError(
            f"world restore: snapshot at {path} declares schema_version "
            f"{version!r}, this build understands {SNAPSHOT_SCHEMA_VERSION!r}."
        )
    return document


def _require_matching_world_manifest(
    document: dict[str, Any], manifest: WorldManifest
) -> None:
    """The snapshot must have been minted for THIS ``world.json``.

    CHAOS-3463, Codex adversarial review (MEDIUM, confirmed): the mint stamped
    ``world_schema_version`` and ``master_seed`` into the snapshot manifest and
    then nothing ever read them back, so they asserted nothing at all.

    The ``WORLD_DIGEST`` guard cannot stand in for this check, which is the
    part that is easy to get wrong: the digest is computed FROM THE RESTORED
    DATABASE, so a world.json edit that leaves the derived ids alone -- a
    changed email, username, full_name, membership_role or is_superuser flag --
    leaves the restored rows, and therefore the digest, bit-for-bit identical,
    while every consumer that reads world.json now disagrees with the database
    the stack serves. This is the check that sees it. (Edits that DO move a
    derived id were already caught, by a different mechanism: the digest
    queries by derived id, finds no rows, and drifts.)

    Fails CLOSED on a snapshot carrying no contract hash. An artifact minted
    before this guard existed is exactly the artifact whose agreement with the
    manifest was never established, so "absent" must not read as "fine".
    """

    expected_version = manifest.world["schema_version"]
    actual_version = document.get("world_schema_version")
    if actual_version != expected_version:
        raise RestoreRefusedError(
            "world restore: snapshot was minted for world schema_version "
            f"{actual_version!r}, but this world.json declares "
            f"{expected_version!r}. Re-mint the snapshot for this world."
        )

    actual_seed = document.get("master_seed")
    if actual_seed != manifest.master_seed:
        raise RestoreRefusedError(
            "world restore: snapshot was minted for master_seed "
            f"{actual_seed!r}, but this world.json declares "
            f"{manifest.master_seed!r}. Every id in the artifact is derived "
            "from that seed, so the snapshot describes a different world."
        )

    expected_contract = world_manifest_contract_hash(manifest)
    actual_contract = document.get("world_manifest_contract")
    if actual_contract is None:
        raise RestoreRefusedError(
            "world restore: snapshot records no world_manifest_contract hash, "
            "so its agreement with world.json cannot be established. Re-mint "
            "with scripts/acceptance/mint_ask_dev_world_snapshot.sh."
        )
    if actual_contract != expected_contract:
        raise RestoreRefusedError(
            "world restore: snapshot was minted for world manifest contract "
            f"{actual_contract}, but this world.json hashes to "
            f"{expected_contract}. An org/user alias, id_seed, name, slug, "
            "email, username, full_name, membership_role or is_superuser "
            "value changed since the mint -- WORLD_DIGEST cannot see that, "
            "because it hashes the restored rows rather than the manifest. "
            "Re-mint the snapshot and the pin together."
        )


def _verify_snapshot_files(snapshot_dir: Path, document: dict[str, Any]) -> None:
    """Every file the manifest names must exist and hash exactly as
    recorded. (``manifest.json`` itself is not among them -- it is the thing
    doing the recording, and the mint stamps ``world_digest`` into it after the
    payload files are already final.) A truncated or partially-checked-out artifact must fail here,
    not silently restore a short world that then fails the digest guard with
    a confusing message."""

    for store in ("clickhouse", "postgres"):
        for table, entry in document[store]["tables"].items():
            path = snapshot_dir / entry["file"]
            if not path.exists():
                raise RestoreRefusedError(
                    f"world restore: snapshot file {path} for {store} table "
                    f"{table!r} is missing."
                )
            actual = _sha256_file(path)
            if actual != entry["sha256"]:
                raise RestoreRefusedError(
                    f"world restore: snapshot file {path} sha256 {actual} does "
                    f"not match the manifest's {entry['sha256']} -- the "
                    "artifact is corrupt or was edited by hand."
                )


def _require_acceptance_environment(env: dict[str, str] | None = None) -> None:
    source = os.environ if env is None else env
    value = source.get("ENVIRONMENT")
    if value != ACCEPTANCE_ENVIRONMENT:
        raise RestoreRefusedError(
            "world restore: ENVIRONMENT is "
            f"{value!r}, not {ACCEPTANCE_ENVIRONMENT!r}. This command writes "
            "fixture rows into the database its process is pointed at and only "
            "ever runs inside the ask-dev acceptance Compose stack (which sets "
            "ENVIRONMENT=acceptance on its api/worker/beat services). Refusing."
        )


def _require_empty_targets(
    *, store: str, counts: dict[str, int], tables: dict[str, Any]
) -> None:
    non_empty = {t: counts.get(t, 0) for t in tables if counts.get(t, 0)}
    if not non_empty:
        return
    # Show a bounded sample, not all of them: a populated target hits this with
    # ~85 tables and the resulting single-line log entry buries the actual
    # instruction (observed while running the negative controls).
    sample = dict(sorted(non_empty.items())[:8])
    more = len(non_empty) - len(sample)
    raise RestoreRefusedError(
        f"world restore: {store} target is not a freshly-migrated, empty "
        f"acceptance database -- {len(non_empty)} snapshot table(s) already "
        f"hold rows, e.g. {sample}"
        + (f" (+{more} more)" if more else "")
        + ". A POPULATED dev or production database always fails this check, "
        "which is the point. Bring the acceptance stack up with "
        "`down --volumes` first; there is deliberately no --force."
    )


async def _restore_clickhouse(
    client: Any, snapshot_dir: Path, tables: dict[str, Any]
) -> None:
    """Two passes, because materialized views fire on the first one.

    Inserting the snapshot's ``git_commits`` makes ``commit_daily_rollup_mv``
    write its own rows into ``commit_daily_rollup``; inserting that table's
    snapshot rows as well leaves it holding both copies (live: ``expected
    +161, got +302``). So every non-MV-target table is restored first, and
    each MV target is then TRUNCATEd and refilled from the snapshot, which
    leaves it holding exactly what the generated database held.

    The TRUNCATE is the one destructive statement on this path. It is scoped
    to tables a materialized view owns, in a database already proven empty and
    already proven to be an acceptance target, and it is preferred to
    detaching the views: a detach that failed to re-attach (a crash between
    the two) would leave the stack silently not populating rollups for
    everything `fixtures generate` writes afterwards.
    """

    async def _insert(table: str, entry: dict[str, Any]) -> None:
        payload = gzip.decompress((snapshot_dir / entry["file"]).read_bytes())
        if not payload:
            return
        await asyncio.to_thread(
            partial(client.raw_insert, table, insert_block=payload, fmt="Native")
        )

    mv_targets = await _clickhouse_materialized_view_targets(client)
    for table, entry in sorted(tables.items()):
        if table not in mv_targets:
            await _insert(table, entry)
    for table, entry in sorted(tables.items()):
        if table in mv_targets:
            await asyncio.to_thread(client.command, f"TRUNCATE TABLE `{table}`")
            await _insert(table, entry)


_REFERRING_TABLES_SQL = """
SELECT DISTINCT c.conrelid::regclass::text
FROM pg_constraint c
WHERE c.contype = 'f' AND c.confrelid = to_regclass('public.' || :table)
ORDER BY 1
"""


async def _tables_referencing(conn: Any, table: str) -> list[str]:
    """Every table with a foreign key INTO ``table``.

    Asked of the live catalog rather than of SQLAlchemy's reflected metadata:
    metadata only knows about tables that have been reflected, so a referrer
    nobody happened to reflect would silently not be checked -- and this is a
    safety check, so "we did not look" must not read as "there is nothing
    there".
    """

    from sqlalchemy import text

    rows = (
        await conn.execute(text(_REFERRING_TABLES_SQL), {"table": table})
    ).fetchall()
    return [str(row[0]).removeprefix("public.") for row in rows]


def reference_alignment_plan(
    spec: dict[str, Any], target_by_key: dict[str, Any], *, table: str
) -> list[tuple[Any, Any]]:
    """``[(current_id, wanted_id), …]`` for one reference table, or raise.

    Split out from :func:`_align_reference_ids` purely so the decision -- the
    part that can be wrong -- is testable without a live database. Rows whose
    id already matches produce no work; a natural key the target does not have
    is a refusal, never a skip (skipping it would leave a dangling reference
    the FK would then reject with a far less informative message).
    """

    missing = sorted(set(spec["ids"]) - set(target_by_key))
    if missing:
        raise RestoreRefusedError(
            f"world restore: reference table {table!r} in the target is "
            f"missing {len(missing)} row(s) the snapshot expects (natural key "
            f"{spec['natural_key']}): {missing[:5]}. The target is migrated at "
            "a different head than the snapshot was minted against -- re-mint "
            "the snapshot rather than restoring this one."
        )
    plan: list[tuple[Any, Any]] = []
    for key_json, source_id in sorted(spec["ids"].items()):
        wanted = _decode_value(source_id)
        current = target_by_key[key_json]
        if current != wanted:
            plan.append((current, wanted))
    return plan


async def _align_reference_ids(
    conn: Any, reference_tables: dict[str, Any], metadata: Any
) -> None:
    """Give the target's migration-seeded reference rows the SAME surrogate
    ids they had in the database the world was generated in.

    ``feature_flags`` (the only such table today) gets a fresh random ``id``
    per database, so ``org_feature_overrides.feature_id`` -- a column
    ``compute_world_digest`` hashes -- would otherwise differ on every boot
    and no pin could ever match. Matching on the natural key and rewriting the
    id makes every snapshotted row restore byte-identical to the generation it
    came from.

    Safe here and nowhere else: this only ever runs behind
    :func:`_require_acceptance_environment` and the empty-target precondition,
    and it verifies that nothing yet references the rows it is about to
    renumber. Anything unexpected -- a natural key the target does not have,
    or an existing referencing row -- refuses; nothing is silently skipped.
    """

    from sqlalchemy import select, text, update

    for table, spec in sorted(reference_tables.items()):
        reflected = await _reflect(conn, table, metadata)
        primary_key = spec["primary_key"]
        natural_key = spec["natural_key"]

        # Renumbering a primary key is only safe while nothing points at it.
        # Checked, not assumed -- the restore inserts referencing rows AFTER
        # this runs, and a stray pre-existing referrer would silently
        # re-target.
        for referrer in await _tables_referencing(conn, table):
            existing = (
                await conn.execute(text(f'SELECT count(*) FROM public."{referrer}"'))
            ).scalar_one()
            if existing:
                raise RestoreRefusedError(
                    f"world restore: cannot align {table}.{primary_key} because "
                    f"{referrer} already holds {existing} row(s) that reference "
                    "it. The target is not a freshly-migrated acceptance "
                    "database."
                )

        rows = (
            await conn.execute(
                select(
                    reflected.c[primary_key],
                    *[reflected.c[name] for name in natural_key],
                )
            )
        ).fetchall()
        target_by_key = {
            json.dumps(
                [_encode_value(value) for value in row[1:]], sort_keys=True
            ): row[0]
            for row in rows
        }

        for current, wanted in reference_alignment_plan(
            spec, target_by_key, table=table
        ):
            await conn.execute(
                update(reflected)
                .where(reflected.c[primary_key] == current)
                .values({primary_key: wanted})
            )


async def _restore_postgres(
    conn: Any,
    snapshot_dir: Path,
    tables: dict[str, Any],
    reference_tables: dict[str, Any],
) -> None:
    from sqlalchemy import MetaData

    # Reflect every snapshot table into ONE MetaData first, then insert in
    # `sorted_tables` order: SQLAlchemy topologically sorts by foreign-key
    # dependency, so `organizations` lands before `memberships` and
    # `dev_runs` before `dev_run_subject_sets`. Alphabetical order (the
    # obvious-looking choice) violates both and fails on the FK.
    metadata = MetaData()
    documents: dict[str, dict[str, Any]] = {}
    for table, entry in sorted(tables.items()):
        await _reflect(conn, table, metadata)
        documents[f"public.{table}"] = json.loads(
            gzip.decompress((snapshot_dir / entry["file"]).read_bytes()).decode()
        )

    await _align_reference_ids(conn, reference_tables, metadata)

    for reflected in metadata.sorted_tables:
        document = documents.get(reflected.fullname)
        if document is None:  # pragma: no cover -- reflection pulled in a FK target
            continue
        rows = document["rows"]
        if not rows:
            continue
        columns = document["columns"]
        payload = [
            {column: _decode_value(value) for column, value in zip(columns, row)}
            for row in rows
        ]
        await conn.execute(reflected.insert(), payload)


def _assert_round_trip(
    *,
    store: str,
    source: dict[str, int],
    baseline: dict[str, int],
    target_before: dict[str, int],
    target_after: dict[str, int],
) -> None:
    """The restore's per-table row-count delta must equal the generation's.

    Compared over the UNION of every table in any of the four maps, not over
    the snapshotted tables alone -- that is what makes this a completeness
    check and not merely a "did the files load" check. A table the world
    wrote but the snapshot missed reads as ``expected +N, got +0``.

    Postgres only. ClickHouse gets :func:`_assert_content_identity` instead:
    counts there are collapsed by background merges and so are not a stable
    identity (see ``_clickhouse_content_hashes``).
    """

    mismatches: list[str] = []
    for table in sorted(
        set(source) | set(baseline) | set(target_before) | set(target_after)
    ):
        expected = source.get(table, 0) - baseline.get(table, 0)
        actual = target_after.get(table, 0) - target_before.get(table, 0)
        if expected != actual:
            mismatches.append(f"{table}: expected {expected:+d}, got {actual:+d}")
    if mismatches:
        raise RestoreDriftError(
            f"world restore: {store} round-trip row-count oracle FAILED. The "
            "restored database did not reproduce the generated one: "
            f"{mismatches}. The acceptance stack must not serve this world."
        )


def content_hashes_to_manifest(hashes: dict[str, str]) -> list[dict[str, str]]:
    """Serialize the ClickHouse content hashes as a LIST of objects.

    Not a ``{table: hash}`` map, and the reason is mechanical rather than
    stylistic: as a map, ``json.dumps(indent=2)`` renders
    ``"llm_token_usage": "<64 hex chars>"`` on a single line, which gitleaks'
    ``generic-api-key`` rule matches -- a key name containing "token" adjacent
    to a high-entropy value. Verified: it failed a real gitleaks scan of the
    committed artifact. As a list of objects the table name and the digest land
    on separate lines, which that (single-line) rule cannot match -- so no
    ``.gitleaksignore`` entry is needed to paper over a detection that was,
    from the scanner's point of view, entirely reasonable.
    """

    return [{"table": table, "content_hash": hashes[table]} for table in sorted(hashes)]


def content_hashes_from_manifest(entries: Any) -> dict[str, str]:
    return {entry["table"]: entry["content_hash"] for entry in entries}


def _assert_content_identity(*, source: dict[str, str], target: dict[str, str]) -> None:
    """Every ClickHouse table must hash identically in both databases.

    Stronger than the row-count oracle it replaces on this side: it catches a
    restore that loaded the right NUMBER of wrong rows, and it is immune to
    background merges (both sides are hashed through ``FINAL``). Compared over
    the union of both maps, so a table present in one database and not the
    other is a failure rather than an omission.
    """

    mismatches: list[str] = []
    for table in sorted(set(source) | set(target)):
        expected = source.get(table)
        actual = target.get(table)
        if expected != actual:
            mismatches.append(
                f"{table}: source={expected or 'ABSENT'} target={actual or 'ABSENT'}"
            )
    if mismatches:
        raise RestoreDriftError(
            "world restore: clickhouse content oracle FAILED. The restored "
            "database does not hold the same rows as the generated one: "
            f"{mismatches}. The acceptance stack must not serve this world."
        )


@dataclass(frozen=True, slots=True)
class RestoreResult:
    clickhouse_tables: int
    postgres_tables: int
    digest: str
    minted: bool


def _shelf_life_days(manifest: WorldManifest) -> int:
    """How long THIS snapshot actually restores cleanly, in days.

    Not simply ``TTL_SAFETY_MARGIN``. The generators place history relative to
    the world's ``pinned_now``, but ClickHouse evaluates TTLs against the
    WALL CLOCK -- so every day between ``pinned_now`` and the moment of
    minting is a day of margin already spent before the snapshot is even
    written.

    Measured on the first re-mint under CHAOS-3544: history capped at 60 days
    before a ``pinned_now`` of 2026-08-05, minted on 2026-08-07, left the
    oldest row 62 days old against a 90-day TTL -- 28 days of real shelf
    life, not the nominal 30. Recording the nominal figure would have left a
    two-day window in which rows decay while the expiry preflight still says
    the snapshot is fresh, which is exactly the cryptic-content-mismatch
    failure this preflight exists to replace.

    The gap grows as a world's ``pinned_now`` ages, so this cannot be a
    constant.
    """

    pinned_raw = manifest.world.get("pinned_now")
    if not pinned_raw:
        return TTL_SAFETY_MARGIN.days
    pinned = datetime.fromisoformat(str(pinned_raw))
    if pinned.tzinfo is None:
        pinned = pinned.replace(tzinfo=UTC)
    spent = (datetime.now(UTC) - pinned).days
    return max(0, TTL_SAFETY_MARGIN.days - max(0, spent))


def _assert_snapshot_within_shelf_life(document: dict) -> None:
    """Fail on STALENESS before failing on its symptoms (CHAOS-3432/3544).

    A snapshot older than its shelf life restores rows the database then
    deletes on TTL, so the content oracle below would fail anyway -- on
    whichever table crossed its horizon first, with a hash mismatch that
    looks like generator nondeterminism and is not. This turns that into a
    named, actionable failure.

    An older snapshot with no recorded mint time is not rejected: it predates
    this field, and refusing it would break restores that are still perfectly
    valid. It simply cannot be checked, which is disclosed rather than
    silently treated as fresh.
    """

    minted_at_raw = document.get("minted_at")
    shelf_life_days = document.get("shelf_life_days")
    if not minted_at_raw or not shelf_life_days:
        logger.warning(
            "world restore: snapshot records no mint time, so its shelf life "
            "cannot be checked (pre-CHAOS-3544 snapshot); a stale one will "
            "surface as a content-oracle mismatch instead"
        )
        return

    minted_at = datetime.fromisoformat(minted_at_raw)
    if minted_at.tzinfo is None:
        minted_at = minted_at.replace(tzinfo=UTC)
    age_days = (datetime.now(UTC) - minted_at).days
    if age_days > int(shelf_life_days):
        raise SnapshotExpiredError(
            f"SNAPSHOT EXPIRED (age {age_days}d > shelf life "
            f"{shelf_life_days}d): re-mint required. Generated history stops "
            "a fixed margin inside the tightest ClickHouse TTL, and that "
            "margin IS the shelf life -- past it, rows cross the TTL horizon "
            "and are deleted on restore, so the restored world is not the "
            "world that was snapshotted. Re-mint with "
            "`scripts/acceptance/mint_ask_dev_world_snapshot.sh`."
        )


async def restore_world(
    *,
    sink: str,
    postgres_uri: str,
    snapshot_dir: Path,
    manifest: WorldManifest,
    digest_path: Path | None = None,
    mint_digest: bool = False,
    generated_digest: str | None = None,
    env: dict[str, str] | None = None,
) -> RestoreResult:
    """Restore ``snapshot_dir`` into the acceptance stack's serving DBs.

    Fails closed on every precondition, then proves the result with the
    round-trip oracle and the ``WORLD_DIGEST`` comparison. ``mint_digest``
    is used ONLY by the one-off mint flow, which re-pins ``WORLD_DIGEST``
    from the restored state; ordinary boots leave it False and verify.
    """

    document = read_snapshot_manifest(snapshot_dir)
    _verify_snapshot_files(snapshot_dir, document)
    _require_matching_world_manifest(document, manifest)
    _require_acceptance_environment(env)

    ch_tables = document["clickhouse"]["tables"]
    pg_tables = document["postgres"]["tables"]

    _assert_schema_compatible(
        store="clickhouse",
        minted=document["clickhouse"].get("schema_fingerprint"),
        live=await _with_clickhouse_client(sink, _clickhouse_schema_fingerprint),
    )
    _assert_schema_compatible(
        store="postgres",
        minted=document["postgres"].get("schema_fingerprint"),
        live=await _with_postgres_conn(postgres_uri, _postgres_schema_fingerprint),
    )

    target_ch_before = await _with_clickhouse_client(sink, _clickhouse_row_counts)
    _require_empty_targets(
        store="clickhouse", counts=target_ch_before, tables=ch_tables
    )
    target_pg_before = await _with_postgres_conn(postgres_uri, _postgres_row_counts)
    _require_empty_targets(store="postgres", counts=target_pg_before, tables=pg_tables)

    async def _do_clickhouse(client: Any) -> None:
        await _restore_clickhouse(client, snapshot_dir, ch_tables)

    await _with_clickhouse_client(sink, _do_clickhouse)

    async def _do_postgres(conn: Any) -> None:
        await _restore_postgres(
            conn,
            snapshot_dir,
            pg_tables,
            document["postgres"].get("reference_tables", {}),
        )

    await _with_postgres_conn(postgres_uri, _do_postgres)

    target_ch_hashes = await _with_clickhouse_client(sink, _clickhouse_content_hashes)
    target_pg_after = await _with_postgres_conn(postgres_uri, _postgres_row_counts)
    _assert_snapshot_within_shelf_life(document)

    _assert_content_identity(
        source=content_hashes_from_manifest(
            document["clickhouse"]["source_content_hashes"]
        ),
        target=target_ch_hashes,
    )
    _assert_round_trip(
        store="postgres",
        source=document["postgres"]["source_row_counts"],
        baseline=document["postgres"]["baseline_row_counts"],
        target_before=target_pg_before,
        target_after=target_pg_after,
    )

    live = await compute_world_digest(manifest, sink=sink, postgres_uri=postgres_uri)
    path = digest_path if digest_path is not None else default_digest_path(manifest)
    manifest_path = snapshot_dir / "manifest.json"
    if mint_digest:
        # Rider 3 (team-lead, binding): the scratch-generation digest and the
        # restored digest are asserted EQUAL inside the command, not once by an
        # operator script -- the lossless-round-trip guard has to hold on every
        # future re-mint, including ones nobody watches. `generated_digest` is
        # what `fixtures world` computed against the scratch database it
        # generated; if the snapshot round trip lost or altered anything the
        # digest covers, these differ.
        #
        # This runs BEFORE either file is written, and that ordering is the
        # whole point. Codex adversarial review round 3 (HIGH, confirmed): the
        # first version compared AFTER `write_digest`, so a lossy round trip
        # raised only once it had already overwritten WORLD_DIGEST -- leaving
        # the pin and the artifact inconsistent while the surrounding shell
        # stopped before copying the new snapshot. The comment beside it even
        # claimed the abort happened first. Nothing is written until the guard
        # has passed.
        if generated_digest is not None and generated_digest != live["digest"]:
            raise RestoreDriftError(
                "world restore: the snapshot round trip is LOSSY. The generated "
                f"scratch database hashed to {generated_digest}, the database "
                f"restored from its snapshot hashes to {live['digest']}. "
                "Refusing to mint -- neither the pin nor the snapshot manifest "
                "has been touched; fix the codec/format first."
            )
        # Stamp the digest INTO the snapshot manifest as well as the pin file.
        # Codex round 2 (MEDIUM, confirmed): without it, "the pin and the
        # artifact are the same generation" was only a row-count sanity check
        # a swapped artifact could satisfy. With it the two files are bound by
        # an exact value only this restore could produce, and both the boot
        # path (below) and a unit test can assert equality.
        write_digest(live, path)
        document["world_digest"] = live["digest"]
        manifest_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        logger.info("world restore: re-minted WORLD_DIGEST at %s", path)
        return RestoreResult(len(ch_tables), len(pg_tables), live["digest"], True)

    pinned = read_pinned_digest(path)
    stamped = document.get("world_digest")
    if stamped != pinned["digest"]:
        raise RestoreDriftError(
            f"world restore: the snapshot at {snapshot_dir} was minted for "
            f"world digest {stamped!r}, but the pin at {path} is "
            f"{pinned['digest']!r}. The artifact and the pin are from different "
            "generations and are only ever valid as a pair -- re-mint both "
            "together."
        )
    if live["digest"] != pinned["digest"]:
        from dev_health_ops.fixtures.world import _diff_components

        drifted = list(
            _diff_components(pinned.get("components", {}), live.get("components", {}))
        )
        raise RestoreDriftError(
            f"world restore: restored WORLD_DIGEST {live['digest']} does not "
            f"match the pinned {pinned['digest']} in {path}. Drifted "
            f"component(s): {drifted}. The snapshot and the pin disagree -- "
            "re-mint them together, never suppress this check."
        )
    logger.info(
        "world restore: WORLD_DIGEST %s verified against %s", live["digest"], path
    )
    return RestoreResult(len(ch_tables), len(pg_tables), live["digest"], False)


# ---------------------------------------------------------------------------
# CLI entrypoints (wired in fixtures/runner.py's register_commands)
# ---------------------------------------------------------------------------


def _resolve_manifest(ns: Any) -> WorldManifest:
    return load_world_manifest(ns.manifest)


async def run_fixtures_world_snapshot(ns: Any) -> int:
    manifest = _resolve_manifest(ns)
    try:
        await snapshot_world(
            sink=ns.sink,
            postgres_uri=ns.postgres_uri,
            baseline_sink=ns.baseline_sink,
            baseline_postgres_uri=ns.baseline_postgres_uri,
            out_dir=Path(ns.out),
            manifest=manifest,
        )
    except SnapshotError as exc:
        logging.error("%s", exc)
        return 1
    return 0


def _read_generated_digest(path: str | None) -> str | None:
    """The digest `fixtures world` wrote for the generation being snapshotted.

    Optional, and only supplied by the mint flow -- an ordinary boot restores
    an already-proven artifact and has nothing to compare against.
    """

    if not path:
        return None
    document = json.loads(Path(path).read_text())
    digest = document.get("digest")
    if not isinstance(digest, str):
        raise SnapshotError(f"world restore: {path} does not contain a 'digest' string")
    return digest


async def run_fixtures_world_restore(ns: Any) -> int:
    manifest = _resolve_manifest(ns)
    digest_path = Path(ns.digest_path) if getattr(ns, "digest_path", None) else None
    try:
        result = await restore_world(
            sink=ns.sink,
            postgres_uri=ns.postgres_uri,
            snapshot_dir=Path(ns.snapshot),
            manifest=manifest,
            digest_path=digest_path,
            mint_digest=bool(getattr(ns, "mint_digest", False)),
            generated_digest=_read_generated_digest(
                getattr(ns, "generated_digest_path", None)
            ),
        )
    except (RestoreRefusedError, RestoreDriftError, SnapshotError) as exc:
        logging.error("%s", exc)
        return 1
    logger.info(
        "world restore: %d clickhouse + %d postgres table(s), digest %s%s",
        result.clickhouse_tables,
        result.postgres_tables,
        result.digest,
        " (minted)" if result.minted else " (verified)",
    )
    return 0
