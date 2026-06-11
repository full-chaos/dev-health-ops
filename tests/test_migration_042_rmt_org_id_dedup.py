"""Static checks for migration 042 (CHAOS-2290).

ReplacingMergeTree deduplicates on the sorting key. If org_id is a column
but not part of ``ORDER BY``, rows with identical natural keys across two
tenants collapse into one row on a background merge — cross-tenant data
loss. These tests guard, at the string/DDL level (no database needed):

1. every rebuild target in migration 042 puts org_id FIRST in the new key;
2. every ReplacingMergeTree table defined across ALL ClickHouse migrations
   ends up with org_id in its *effective* sorting key (file DDL, overridden
   by the 027 / 042 rebuild maps).
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

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
