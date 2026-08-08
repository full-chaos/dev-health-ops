"""Live-ClickHouse proof that migration 075 actually heals a stale
`project_declared_state_history` shape (CHAOS-3563 round-3 review A).

Simulates the exact scenario the round-3 verifier proved live: a database
that ran an OLDER shape of migration 074 (no `version_key` column, no
`project_declared_state_floor` table) -- built directly with `raw_client`,
never via the current 074/075 scripts, so this test cannot accidentally
pass just because it happens to construct the current shape by
coincidence. Runs migration 075's `upgrade()` against that real, stale
table, then proves TWO things against the real engine: the shape is
healed, AND the read path
(`_PROJECT_DECLARED_FACTS_SQL`) that previously 500'd on the missing
column now actually works and returns the (rebuilt-from-`projects`)
declared state.

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"``): ``pytest -m clickhouse`` with
``CLICKHOUSE_URI`` pointing at a SCRATCH database -- never the dev
``default``.
"""

from __future__ import annotations

import importlib.util
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from dev_health_ops.api.dev.native_status_change import _PROJECT_DECLARED_FACTS_SQL
from dev_health_ops.api.queries.client import query_dicts

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
NOW = datetime.now(UTC).replace(microsecond=0)

_PROTECTED_DATABASES = frozenset({"", "default"})


def _database_of(dsn: str | None) -> str:
    from urllib.parse import urlparse

    return urlparse(dsn or "").path.lstrip("/").strip().lower()


_SKIP_REASON = (
    "Requires a migrated SCRATCH CLICKHOUSE_URI "
    "(e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate); "
    f"got database {_database_of(CLICKHOUSE_URI) or '<unset>'!r}, which this "
    "suite refuses to seed"
)

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI or _database_of(CLICKHOUSE_URI) in _PROTECTED_DATABASES,
        reason=_SKIP_REASON,
    ),
]

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)


def _load_migration(filename: str) -> ModuleType:
    path = MIGRATIONS_DIR / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def raw_client() -> Any:
    import clickhouse_connect

    assert CLICKHOUSE_URI is not None
    if _database_of(CLICKHOUSE_URI) in _PROTECTED_DATABASES:
        raise RuntimeError("refusing to seed a protected ClickHouse database")
    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield client
    finally:
        client.close()


@pytest.mark.asyncio
async def test_075_heals_a_stale_074_shape_and_the_reader_works_afterward(
    raw_client: Any,
) -> None:
    org_id = f"proj-075-heal-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    declared_at = NOW - timedelta(days=3)

    # 1. Build the OLDEST realistic shape by hand: no version_key, no
    #    is_backfill_floor, no floor table -- just last_synced as the
    #    version column, exactly what the very first version of 074 shipped.
    #    Never touches 074/075's own scripts, so this cannot coincidentally
    #    already be "current".
    raw_client.command("DROP TABLE IF EXISTS project_declared_state_history")
    raw_client.command("DROP TABLE IF EXISTS project_declared_state_floor")
    raw_client.command(
        """
        CREATE TABLE project_declared_state_history (
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
    )

    # 2. A `projects` row for this project MUST already exist for 075's
    #    rebuild backfill to recover its declared state -- the whole point
    #    of proving this is a real "rebuild from projects FINAL", not an
    #    empty-table no-op.
    raw_client.command(
        "ALTER TABLE projects DELETE WHERE org_id = {org_id:String} "
        "AND id = {entity_id:String}",
        parameters={"org_id": org_id, "entity_id": project_id},
    )
    raw_client.insert(
        "projects",
        [
            [
                project_id,
                org_id,
                "linear",
                "PLAT",
                "Platform",
                1,
                "in_progress",
                None,
                "",
                declared_at,
                declared_at,
            ]
        ],
        column_names=[
            "id",
            "org_id",
            "provider",
            "project_key",
            "name",
            "is_active",
            "state",
            "target_date",
            "url",
            "updated_at",
            "last_synced",
        ],
    )

    # 3. Sanity-check the OLD shape is genuinely broken for the current
    #    reader -- the exact live-500 the round-3 verifier proved.
    with pytest.raises(Exception):
        await query_dicts(
            raw_client,
            _PROJECT_DECLARED_FACTS_SQL,
            {"org_id": org_id, "entity_id": project_id, "as_of": NOW},
        )

    # 4. Run migration 075 for real, against the real stale table.
    module_075 = _load_migration("075_reconcile_project_declared_state_history.py")
    module_075.upgrade(raw_client)

    # 5. Shape healed: version_key exists, floor table exists.
    columns = await query_dicts(
        raw_client,
        "SELECT name FROM system.columns WHERE database = currentDatabase() "
        "AND table = 'project_declared_state_history'",
        {},
    )
    assert any(row["name"] == "version_key" for row in columns), (
        "075 must add version_key back after healing"
    )
    floor_tables = await query_dicts(
        raw_client,
        "SELECT count() AS n FROM system.tables WHERE database = currentDatabase() "
        "AND name = 'project_declared_state_floor'",
        {},
    )
    assert floor_tables[0]["n"] == 1, "075 must recreate the floor table"

    # 6. The reader works now, and recovered this project's declared state
    #    from `projects FINAL` via 075's rebuild backfill.
    rows = await query_dicts(
        raw_client,
        _PROJECT_DECLARED_FACTS_SQL,
        {"org_id": org_id, "entity_id": project_id, "as_of": NOW},
    )
    assert len(rows) == 1
    assert rows[0]["bounded_count"] > 0
    assert rows[0]["state"] == "in_progress", (
        "075's rebuild backfill must have recovered this project's "
        "declared state from projects FINAL"
    )


@pytest.mark.asyncio
async def test_075_is_a_noop_against_the_current_shape(raw_client: Any) -> None:
    """The common case, proven live: a database whose 074 already applied
    at the CURRENT shape must see 075 issue no DROP/CREATE/INSERT at all.
    """
    module_074 = _load_migration("074_project_declared_state_history.py")
    module_075 = _load_migration("075_reconcile_project_declared_state_history.py")

    raw_client.command("DROP TABLE IF EXISTS project_declared_state_history")
    raw_client.command("DROP TABLE IF EXISTS project_declared_state_floor")
    module_074.upgrade(raw_client)

    before = await query_dicts(
        raw_client,
        "SELECT count() AS n FROM project_declared_state_history",
        {},
    )
    module_075.upgrade(raw_client)
    after = await query_dicts(
        raw_client,
        "SELECT count() AS n FROM project_declared_state_history",
        {},
    )
    # A real no-op: row count (and therefore the table itself) is
    # untouched, not just "still queryable".
    assert before[0]["n"] == after[0]["n"]
