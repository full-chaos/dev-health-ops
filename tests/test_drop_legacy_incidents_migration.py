import os
from pathlib import Path
from urllib.parse import urlparse

import pytest

from dev_health_ops.migrations.clickhouse import split_sql_statements

_ROOT = Path(__file__).resolve().parents[1]
_MIGRATIONS = _ROOT / "src/dev_health_ops/migrations/clickhouse"


def test_current_schema_drops_and_does_not_create_legacy_incidents() -> None:
    # Given: the clean-install schema and the forward migration for existing databases.
    raw_schema = (_MIGRATIONS / "000_raw_tables.sql").read_text(encoding="utf-8")
    drop_migration = (_MIGRATIONS / "068_drop_legacy_incidents.sql").read_text(
        encoding="utf-8"
    )

    # Then: fresh databases never create the dead table and upgrades remove it safely.
    assert "CREATE TABLE IF NOT EXISTS incidents" not in raw_schema
    assert "DROP TABLE IF EXISTS incidents" in drop_migration


def test_intermediate_sorting_key_migration_does_not_depend_on_legacy_incidents() -> (
    None
):
    # Given: a fresh install where migration 000 omits the dead table.
    migration = (_MIGRATIONS / "027_add_org_id_to_sorting_keys.py").read_text(
        encoding="utf-8"
    )

    # Then: the intermediate rebuild catalog has no dead-table dependency.
    assert '"incidents"' not in migration


@pytest.mark.clickhouse
def test_upgrade_migration_drops_existing_legacy_table_idempotently() -> None:
    # Given: an isolated migrated database that still contains the pre-068 table.
    clickhouse_uri = os.environ.get("CLICKHOUSE_URI")
    if not clickhouse_uri:
        pytest.skip("requires CLICKHOUSE_URI pointed at an isolated scratch database")
    database = (urlparse(clickhouse_uri).path or "").lstrip("/")
    if database in ("", "default"):
        pytest.skip("refusing to modify the default ClickHouse database")

    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    sink = ClickHouseMetricsSink(clickhouse_uri)
    migration = (_MIGRATIONS / "068_drop_legacy_incidents.sql").read_text(
        encoding="utf-8"
    )
    statements = split_sql_statements(migration)
    try:
        sink.client.command(
            "CREATE TABLE incidents (repo_id UUID, incident_id String) "
            "ENGINE = MergeTree ORDER BY (repo_id, incident_id)"
        )

        # When: the exact migration is applied twice, as an interrupted upgrade may do.
        for _ in range(2):
            for statement in statements:
                sink.client.command(statement)

        # Then: the legacy table is absent and the repeated drop remains safe.
        result = sink.client.query(
            "SELECT count() FROM system.tables "
            "WHERE database = currentDatabase() AND name = 'incidents'"
        )
        assert result.result_rows == [(0,)]
    finally:
        sink.client.command("DROP TABLE IF EXISTS incidents")
        sink.close()
