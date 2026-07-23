from __future__ import annotations

import hashlib
from pathlib import Path

from dev_health_ops.migrations.clickhouse import split_sql_statements
from dev_health_ops.models.operational import OPERATIONAL_ENTITY_TABLES

_MIGRATION_066 = (
    Path(__file__).parents[1]
    / "src/dev_health_ops/migrations/clickhouse/066_operational_canonical.sql"
)
_MIGRATION_066_SHA256 = (
    "89ae3354983cf84aa1663b44fd6a494c067cc05ceb522364e778b3a5c1f4e69a"
)


def test_migration_066_remains_the_immutable_legacy_baseline() -> None:
    # Given: the migration that established the twelve canonical tables.
    migration_bytes = _MIGRATION_066.read_bytes()

    # When: its content digest is computed.
    digest = hashlib.sha256(migration_bytes).hexdigest()

    # Then: later ordering work cannot silently rewrite deployed history.
    assert digest == _MIGRATION_066_SHA256


def test_legacy_baseline_uses_source_time_final_compaction_for_every_family() -> None:
    # Given: the immutable pre-contract table definitions.
    statements = split_sql_statements(_MIGRATION_066.read_text(encoding="utf-8"))

    # When: their engine and sorting-key contracts are characterized.
    legacy_tables = {
        table
        for table in OPERATIONAL_ENTITY_TABLES.values()
        if any(
            f"CREATE TABLE IF NOT EXISTS {table}" in statement
            and "ReplacingMergeTree(source_version_at)" in statement
            and "ORDER BY (org_id, id)" in statement
            for statement in statements
        )
    }

    # Then: all actual families share the collision-prone FINAL baseline being replaced.
    assert legacy_tables == set(OPERATIONAL_ENTITY_TABLES.values())
    assert len(legacy_tables) == 12
