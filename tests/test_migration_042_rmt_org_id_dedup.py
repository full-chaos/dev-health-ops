"""Static checks for migration 042 (CHAOS-2290).

ReplacingMergeTree deduplicates on the sorting key. If org_id is a column
but not part of ``ORDER BY``, rows with identical natural keys across two
tenants collapse into one row on a background merge — cross-tenant data
loss. These tests guard, at the string/DDL level (no database needed):

1. every rebuild target in migration 042 puts org_id FIRST in the new key;
2. every ReplacingMergeTree table defined across ALL ClickHouse migrations
   ends up with org_id in its *effective* sorting key (file DDL, overridden
   by the 027 / 042 rebuild maps);
3. the rebuild flow itself (against a fake client): post-EXCHANGE catch-up
   of writes that raced the snapshot copy, crash convergence from a
   leftover shadow, and the fail-closed sorting-key verification.
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

MIGRATION_042 = "042_rmt_org_id_dedup_keys.py"
MIGRATION_027 = "027_add_org_id_to_sorting_keys.py"


def _load_migration(filename: str) -> ModuleType:
    """Load a migration module standalone, exactly like the runner does."""
    path = MIGRATIONS_DIR / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None, f"cannot load {filename}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Migration 042 itself
# ---------------------------------------------------------------------------


def test_migration_042_exists_and_has_upgrade() -> None:
    module = _load_migration(MIGRATION_042)
    assert callable(getattr(module, "upgrade", None))


def test_migration_042_every_new_order_by_starts_with_org_id() -> None:
    module = _load_migration(MIGRATION_042)
    assert module.TABLES, "migration 042 must rebuild at least one table"
    for table, order_by in module.TABLES.items():
        assert re.match(r"\(\s*org_id\s*,", order_by), (
            f"{table}: new ORDER BY must start with org_id, got {order_by!r}"
        )


def test_migration_042_covers_known_affected_tables() -> None:
    """The tables whose live dedup key was missing org_id (CHAOS-2290)."""
    module = _load_migration(MIGRATION_042)
    expected = {
        # CHAOS-2290 headline tables (009_raw_work_items.sql); rebuilt by 027
        # on healthy databases, kept as an idempotent safety net.
        "work_items",
        "work_item_transitions",
        # 029_testops_tables.sql — created with org_id column but keyed on
        # (repo_id, ...) only.
        "ci_job_runs",
        "test_suite_results",
        "test_case_results",
        "coverage_snapshots",
        # 032_security_alerts.sql / 033 added the column but not the key.
        "security_alerts",
    }
    missing = expected - set(module.TABLES)
    assert not missing, f"migration 042 missing rebuilds for: {sorted(missing)}"


def test_migration_042_runs_after_the_migrations_it_repairs() -> None:
    """The runner applies files in sorted-filename order."""
    for prerequisite in (
        "009_raw_work_items.sql",
        MIGRATION_027,
        "029_testops_tables.sql",
        "033_security_alerts_org_id.sql",
    ):
        assert (MIGRATIONS_DIR / prerequisite).exists()
        assert prerequisite < MIGRATION_042


# ---------------------------------------------------------------------------
# Whole-catalog guard: every RMT table's effective key includes org_id
# ---------------------------------------------------------------------------

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?", re.IGNORECASE
)
_ORDER_BY_RE = re.compile(r"ORDER\s+BY\s+(\([^;]*?\)|\S+)\s*(?:;|$|\n)", re.IGNORECASE)

# product_telemetry_events deliberately keys on the privacy-preserving
# org_id_hash instead of org_id — both are valid tenant keys.
_TENANT_KEY_RE = re.compile(r"\borg_id(_hash)?\b")


def _strip_sql_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _rmt_tables_from_sql_migrations() -> dict[str, tuple[str, str]]:
    """Map of table -> (defining file, ORDER BY as written in the file)."""
    tables: dict[str, tuple[str, str]] = {}
    for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
        text = _strip_sql_comments(sql_file.read_text(encoding="utf-8"))
        for stmt in text.split(";"):
            if "ReplacingMergeTree" not in stmt:
                continue
            create = _CREATE_TABLE_RE.search(stmt)
            if create is None:
                continue  # e.g. a view referencing an RMT table
            order_by = _ORDER_BY_RE.search(stmt + ";")
            assert order_by is not None, (
                f"{sql_file.name}: ReplacingMergeTree table "
                f"{create.group(1)} has no ORDER BY"
            )
            tables[create.group(1)] = (sql_file.name, order_by.group(1))
    return tables


def test_every_rmt_table_effective_sorting_key_includes_org_id() -> None:
    """No ReplacingMergeTree table may dedup across tenants.

    The effective sorting key is the table's CREATE TABLE ORDER BY, unless a
    later rebuild migration (027 or 042) replaced it. A failure here means a
    migration introduced (or left behind) an RMT table whose dedup key lacks
    org_id — add the table to a rebuild migration or key it on org_id from
    the start.
    """
    overrides: dict[str, str] = {}
    overrides.update(_load_migration(MIGRATION_027).TABLES)
    overrides.update(_load_migration(MIGRATION_042).TABLES)

    offenders: list[str] = []
    for table, (filename, declared_order_by) in sorted(
        _rmt_tables_from_sql_migrations().items()
    ):
        effective = overrides.get(table, declared_order_by)
        if not _TENANT_KEY_RE.search(effective):
            offenders.append(f"{table} ({filename}): ORDER BY {effective}")

    assert not offenders, (
        "ReplacingMergeTree tables whose effective sorting key is missing "
        "org_id (cross-tenant dedup, see CHAOS-2290):\n  " + "\n  ".join(offenders)
    )


# ---------------------------------------------------------------------------
# Rebuild flow: catch-up, crash convergence, sorting-key verification
# ---------------------------------------------------------------------------

OLD_KEY = "repo_id, work_item_id"
NEW_KEY = "org_id, repo_id, work_item_id"

OLD_DDL = (
    "CREATE TABLE work_items (`repo_id` UUID, `work_item_id` String, "
    "`org_id` String, `last_synced` DateTime) "
    "ENGINE = ReplacingMergeTree(last_synced) "
    "ORDER BY (repo_id, work_item_id) SETTINGS index_granularity = 8192"
)
NEW_DDL = OLD_DDL.replace(
    "ORDER BY (repo_id, work_item_id)",
    "ORDER BY (org_id, repo_id, work_item_id)",
)


class _FakeResult:
    def __init__(self, rows: list[list]) -> None:
        self.result_rows = rows


class FakeClient:
    """Minimal ClickHouse client double tracking executed commands.

    ``tables`` maps table name -> {"ddl": str, "sorting_key": str}. DROP,
    CREATE and EXCHANGE mutate the catalog; ``created_sorting_key`` is the
    sorting key assigned to any table CREATEd during the run (lets a test
    simulate the regex rewrite producing a wrong key). ``fail_on`` injects a
    failure into the first command containing the substring.
    """

    def __init__(
        self,
        tables: dict[str, dict[str, str]],
        *,
        created_sorting_key: str = NEW_KEY,
        fail_on: str | None = None,
    ) -> None:
        self.tables = {name: dict(spec) for name, spec in tables.items()}
        self.created_sorting_key = created_sorting_key
        self.fail_on = fail_on
        self.commands: list[str] = []

    def query(self, query: str, parameters: dict | None = None) -> _FakeResult:
        if "count() FROM system.tables" in query:
            assert parameters is not None
            return _FakeResult([[1 if parameters["name"] in self.tables else 0]])
        if "sorting_key FROM system.tables" in query:
            assert parameters is not None
            spec = self.tables.get(parameters["name"])
            return _FakeResult([[spec["sorting_key"]]] if spec else [])
        if query.startswith("SHOW CREATE TABLE"):
            name = query.split("`")[1]
            return _FakeResult([[self.tables[name]["ddl"]]])
        if "uniqExact" in query:
            return _FakeResult([[5]])
        raise AssertionError(f"unexpected query: {query}")

    def command(self, cmd: str) -> None:
        self.commands.append(cmd)
        if self.fail_on and self.fail_on in cmd:
            self.fail_on = None  # fail once
            raise RuntimeError(f"injected failure on: {cmd}")
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
    return _load_migration(MIGRATION_042)


def test_rebuild_runs_catch_up_after_exchange_then_drops_shadow(migration) -> None:
    """Writes landing between snapshot and EXCHANGE must be re-inserted."""
    client = FakeClient(
        {"work_items": {"ddl": OLD_DDL, "sorting_key": OLD_KEY}},
    )
    migration._rebuild_table(client, "work_items", f"({NEW_KEY})")

    snapshot = client.commands.index(
        "INSERT INTO `work_items_new` SELECT * FROM `work_items`"
    )
    exchange = client.commands.index(
        "EXCHANGE TABLES `work_items` AND `work_items_new`"
    )
    catch_up = client.commands.index(
        "INSERT INTO `work_items` SELECT * FROM `work_items_new`"
    )
    drop = client.commands.index("DROP TABLE `work_items_new`")
    assert snapshot < exchange < catch_up < drop, client.commands
    assert "work_items_new" not in client.tables
    assert client.tables["work_items"]["sorting_key"] == NEW_KEY


def test_rebuild_does_not_drop_shadow_when_catch_up_fails(migration) -> None:
    """Post-EXCHANGE failure must leave the shadow for rerun convergence."""
    client = FakeClient(
        {"work_items": {"ddl": OLD_DDL, "sorting_key": OLD_KEY}},
        fail_on="INSERT INTO `work_items` SELECT",
    )
    with pytest.raises(RuntimeError, match="injected failure"):
        migration._rebuild_table(client, "work_items", f"({NEW_KEY})")

    # The shadow (now holding the OLD table) must survive the failure.
    assert "work_items_new" in client.tables
    assert not any(
        cmd.startswith("DROP TABLE") and "work_items_new" in cmd
        for cmd in client.commands[
            client.commands.index("EXCHANGE TABLES `work_items` AND `work_items_new`") :
        ]
    )


def test_skip_path_converges_leftover_shadow(migration) -> None:
    """Rerun after crash-post-EXCHANGE: catch up from the shadow, then drop."""
    client = FakeClient(
        {
            "work_items": {"ddl": NEW_DDL, "sorting_key": NEW_KEY},
            "work_items_new": {"ddl": OLD_DDL, "sorting_key": OLD_KEY},
        }
    )
    migration._rebuild_table(client, "work_items", f"({NEW_KEY})")

    assert client.commands == [
        "INSERT INTO `work_items` SELECT * FROM `work_items_new`",
        "DROP TABLE `work_items_new`",
    ]
    assert "work_items_new" not in client.tables


def test_skip_path_without_leftover_shadow_is_a_no_op(migration) -> None:
    client = FakeClient({"work_items": {"ddl": NEW_DDL, "sorting_key": NEW_KEY}})
    migration._rebuild_table(client, "work_items", f"({NEW_KEY})")
    assert client.commands == []


def test_rebuild_aborts_and_drops_shadow_on_sorting_key_mismatch(migration) -> None:
    """A regex miss on exotic DDL must fail closed before any data copy."""
    client = FakeClient(
        {"work_items": {"ddl": OLD_DDL, "sorting_key": OLD_KEY}},
        # Simulate the ORDER BY rewrite mangling a nested expression.
        created_sorting_key="org_id, cityHash64(repo_id)",
    )
    with pytest.raises(RuntimeError, match="sorting key mismatch"):
        migration._rebuild_table(client, "work_items", f"({NEW_KEY})")

    # Shadow dropped, original untouched, no data copied, no swap.
    assert "work_items_new" not in client.tables
    assert client.tables["work_items"]["sorting_key"] == OLD_KEY
    assert not any("INSERT INTO" in cmd for cmd in client.commands)
    assert not any("EXCHANGE" in cmd for cmd in client.commands)


def test_rebuild_drops_shadow_when_snapshot_copy_fails(migration) -> None:
    """Pre-EXCHANGE failures leave no shadow behind (disposable phase)."""
    client = FakeClient(
        {"work_items": {"ddl": OLD_DDL, "sorting_key": OLD_KEY}},
        fail_on="INSERT INTO `work_items_new`",
    )
    with pytest.raises(RuntimeError, match="injected failure"):
        migration._rebuild_table(client, "work_items", f"({NEW_KEY})")

    assert "work_items_new" not in client.tables
    assert not any("EXCHANGE" in cmd for cmd in client.commands)
