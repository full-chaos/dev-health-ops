"""Tests for migration 088 (release_impact_daily -> ReplacingMergeTree).

Against a fake client (no database needed), covering:

1. the rebuild flow itself: shadow creation, snapshot copy, atomic EXCHANGE,
   post-EXCHANGE catch-up, and crash convergence from a leftover shadow;
2. the sorting-key fail-closed check aborts (and drops the shadow) on a key
   mismatch;
3. the caller-side count() cross-check (new in 088): a distinct-key read that
   comes back 0 while the table actually holds rows aborts the migration
   instead of trusting a misleading 0==0 comparison.

No projection guard: team-lead ruled DDL-history evidence (only migration 034
ever touches this table) sufficient and declined a
deduplicate_merge_projection_mode setting for 088, so there is nothing to test
here -- see the module docstring in 088 itself.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)
MIGRATION_088 = "088_release_impact_daily_replacing_merge_tree.py"

TABLE = "release_impact_daily"
SHADOW = f"{TABLE}_new"
OLD_KEY = "org_id, release_ref, environment, day"

OLD_DDL = (
    "CREATE TABLE release_impact_daily (`org_id` String DEFAULT 'default', "
    "`day` Date, `release_ref` String, `environment` String, "
    "`computed_at` DateTime64(3, 'UTC')) "
    "ENGINE = MergeTree() PARTITION BY toYYYYMM(day) "
    "ORDER BY (org_id, release_ref, environment, day) "
    "TTL day + INTERVAL 365 DAY DELETE"
)


def _load_migration() -> ModuleType:
    path = MIGRATIONS_DIR / MIGRATION_088
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeResult:
    def __init__(self, rows: list[list]) -> None:
        self.result_rows = rows


class FakeClient:
    """Minimal ClickHouse client double tracking executed commands.

    ``row_counts`` and ``distinct_key_counts`` let a test set the two reads
    apart, which is the whole point of the 088 count() cross-check test --
    055's FakeClient never needed the distinction because it never compared
    them to each other.
    """

    def __init__(
        self,
        tables: dict[str, dict[str, str]],
        *,
        created_sorting_key: str = OLD_KEY,
        distinct_key_counts: dict[str, int] | None = None,
        row_counts: dict[str, int] | None = None,
    ) -> None:
        self.tables = {name: dict(spec) for name, spec in tables.items()}
        self.created_sorting_key = created_sorting_key
        self.distinct_key_counts = distinct_key_counts or {}
        self.row_counts = row_counts or {}
        self.commands: list[str] = []

    def query(self, query: str, parameters: dict | None = None) -> _FakeResult:
        if "count() FROM system.tables" in query:
            assert parameters is not None
            return _FakeResult([[1 if parameters["name"] in self.tables else 0]])
        if "engine FROM system.tables" in query:
            assert parameters is not None
            spec = self.tables.get(parameters["name"])
            if not spec:
                return _FakeResult([])
            engine = (
                "ReplacingMergeTree"
                if "ReplacingMergeTree" in spec["ddl"]
                else "MergeTree"
            )
            return _FakeResult([[engine]])
        if "sorting_key FROM system.tables" in query:
            assert parameters is not None
            spec = self.tables.get(parameters["name"])
            return _FakeResult([[spec["sorting_key"]]] if spec else [])
        if "count() FROM system.columns" in query:
            return _FakeResult([[1]])
        if query.startswith("SHOW CREATE TABLE"):
            name = query.split("`")[1]
            return _FakeResult([[self.tables[name]["ddl"]]])
        if "uniqExact" in query:
            name = query.split("FROM `")[1].rstrip("`")
            return _FakeResult([[self.distinct_key_counts.get(name, 5)]])
        if query.startswith("SELECT count() FROM `"):
            name = query.split("`")[1]
            return _FakeResult([[self.row_counts.get(name, 5)]])
        raise AssertionError(f"unexpected query: {query}")

    def command(self, cmd: str) -> None:
        self.commands.append(cmd)
        if cmd.startswith("DROP TABLE"):
            self.tables.pop(cmd.split("`")[1], None)
        elif cmd.startswith("CREATE TABLE"):
            name = re.search(r"CREATE TABLE\s+`?(\w+)`?", cmd).group(1)  # type: ignore[union-attr]
            self.tables[name] = {"ddl": cmd, "sorting_key": self.created_sorting_key}
        elif cmd.startswith("EXCHANGE TABLES"):
            a, b = re.findall(r"`(\w+)`", cmd)
            self.tables[a], self.tables[b] = self.tables[b], self.tables[a]


@pytest.fixture()
def migration() -> ModuleType:
    return _load_migration()


def test_migration_088_exists_and_has_upgrade(migration) -> None:
    assert callable(getattr(migration, "upgrade", None))


def test_rebuild_converts_engine_and_preserves_sorting_key(migration) -> None:
    client = FakeClient({TABLE: {"ddl": OLD_DDL, "sorting_key": OLD_KEY}})

    migration._rebuild_table(client, TABLE, SHADOW)

    assert SHADOW not in client.tables
    assert client.tables[TABLE]["sorting_key"] == OLD_KEY
    assert f"EXCHANGE TABLES `{TABLE}` AND `{SHADOW}`" in client.commands


def test_rebuild_runs_catch_up_after_exchange_then_drops_shadow(migration) -> None:
    client = FakeClient({TABLE: {"ddl": OLD_DDL, "sorting_key": OLD_KEY}})
    migration._rebuild_table(client, TABLE, SHADOW)

    snapshot = client.commands.index(f"INSERT INTO `{SHADOW}` SELECT * FROM `{TABLE}`")
    exchange = client.commands.index(f"EXCHANGE TABLES `{TABLE}` AND `{SHADOW}`")
    catchup = client.commands.index(f"INSERT INTO `{TABLE}` SELECT * FROM `{SHADOW}`")
    drop = client.commands.index(f"DROP TABLE `{SHADOW}`")
    assert snapshot < exchange < catchup < drop


def test_already_rmt_skips(migration) -> None:
    client = FakeClient(
        {
            TABLE: {
                "ddl": OLD_DDL.replace(
                    "MergeTree()", "ReplacingMergeTree(computed_at)"
                ),
                "sorting_key": OLD_KEY,
            }
        }
    )
    migration._rebuild_table(client, TABLE, SHADOW)
    assert client.commands == []


def test_already_rmt_with_leftover_shadow_converges(migration) -> None:
    """A crash after EXCHANGE but before catch-up/DROP must converge on rerun."""
    client = FakeClient(
        {
            TABLE: {
                "ddl": OLD_DDL.replace(
                    "MergeTree()", "ReplacingMergeTree(computed_at)"
                ),
                "sorting_key": OLD_KEY,
            },
            SHADOW: {"ddl": OLD_DDL, "sorting_key": OLD_KEY},
        }
    )
    migration._rebuild_table(client, TABLE, SHADOW)
    assert f"INSERT INTO `{TABLE}` SELECT * FROM `{SHADOW}`" in client.commands
    assert f"DROP TABLE `{SHADOW}`" in client.commands
    assert SHADOW not in client.tables


def test_already_rmt_missing_version_column_refuses_instead_of_skipping(
    migration,
) -> None:
    """codex r3 finding 1 (CHAOS-4296/#2262): the already-ReplacingMergeTree
    convergence path trusted the engine NAME alone -- a table converted by
    hand (or a partial run) could be ReplacingMergeTree with no version
    column at all, and this branch used to treat that as "already converged,
    skip" without checking. It must refuse instead."""

    class MissingVersionColumnClient(FakeClient):
        def query(self, query: str, parameters: dict | None = None):
            if "count() FROM system.columns" in query:
                return _FakeResult([[0]])
            return super().query(query, parameters)

    client = MissingVersionColumnClient(
        {
            TABLE: {
                "ddl": OLD_DDL.replace(
                    "MergeTree()", "ReplacingMergeTree(computed_at)"
                ),
                "sorting_key": OLD_KEY,
            },
        }
    )
    with pytest.raises(ValueError, match="missing the required version column"):
        migration._rebuild_table(client, TABLE, SHADOW)
    assert client.commands == []


def test_table_exists_probe_failure_propagates_instead_of_reading_as_absent(
    migration,
) -> None:
    """codex r1 finding 1 (CHAOS-4296/#2262): a genuine query/connection failure
    on the system.tables probe must NOT be swallowed and read as "table
    doesn't exist" -- that would silently skip the whole rebuild while the
    migration runner still records it as applied."""

    class FailingClient(FakeClient):
        def query(self, query: str, parameters: dict | None = None):
            if "count() FROM system.tables" in query:
                raise RuntimeError("connection reset by peer")
            return super().query(query, parameters)

    client = FailingClient({TABLE: {"ddl": OLD_DDL, "sorting_key": OLD_KEY}})
    with pytest.raises(RuntimeError, match="connection reset by peer"):
        migration._rebuild_table(client, TABLE, SHADOW)
    assert client.commands == []


def test_sorting_key_mismatch_aborts_and_drops_shadow(migration) -> None:
    client = FakeClient(
        {TABLE: {"ddl": OLD_DDL, "sorting_key": OLD_KEY}},
        created_sorting_key="org_id, release_ref",  # wrong -- missing environment/day
    )
    with pytest.raises(RuntimeError, match="sorting key changed"):
        migration._rebuild_table(client, TABLE, SHADOW)
    assert f"EXCHANGE TABLES `{TABLE}` AND `{SHADOW}`" not in client.commands
    assert SHADOW not in client.tables


# ---------------------------------------------------------------------------
# 088-specific: caller-side count() cross-check for the 0==0 hole
# ---------------------------------------------------------------------------


def test_distinct_key_zero_with_real_rows_aborts_before_swap(migration) -> None:
    """A uniqExact read that comes back 0 while the table has rows must not
    be trusted just because both sides happen to agree on 0."""
    client = FakeClient(
        {TABLE: {"ddl": OLD_DDL, "sorting_key": OLD_KEY}},
        distinct_key_counts={TABLE: 0, SHADOW: 0},
        row_counts={TABLE: 240, SHADOW: 240},
    )
    with pytest.raises(RuntimeError, match="distinct-key read returned 0"):
        migration._rebuild_table(client, TABLE, SHADOW)
    assert f"EXCHANGE TABLES `{TABLE}` AND `{SHADOW}`" not in client.commands
    assert SHADOW not in client.tables


def test_distinct_key_and_row_counts_both_zero_is_a_genuinely_empty_table(
    migration,
) -> None:
    """The legitimate empty-table case must still pass -- 0==0 backed by 0
    real rows on both sides is not the failure this guards against."""
    client = FakeClient(
        {TABLE: {"ddl": OLD_DDL, "sorting_key": OLD_KEY}},
        distinct_key_counts={TABLE: 0, SHADOW: 0},
        row_counts={TABLE: 0, SHADOW: 0},
    )
    migration._rebuild_table(client, TABLE, SHADOW)
    assert f"EXCHANGE TABLES `{TABLE}` AND `{SHADOW}`" in client.commands
