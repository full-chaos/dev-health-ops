from __future__ import annotations

from pathlib import Path

from dev_health_ops.migrations.clickhouse import split_sql_statements

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
    / "069_acr_code_metric_access_paths.sql"
)


def test_acr_code_metric_access_paths_are_added_and_materialized_synchronously() -> (
    None
):
    # Given
    migration = _MIGRATION.read_text(encoding="utf-8")

    # When
    statements = split_sql_statements(migration)
    canonical = [" ".join(statement.split()) for statement in statements]

    # Then
    assert len(statements) == 6
    assert (
        "ADD INDEX IF NOT EXISTS idx_file_complexity_ref ref "
        "TYPE bloom_filter(0.01) GRANULARITY 1"
    ) in canonical[0]
    assert "MATERIALIZE INDEX IF EXISTS idx_file_complexity_ref" in canonical[1]
    assert "SETTINGS mutations_sync = 2" in canonical[1]
    assert "ADD PROJECTION IF NOT EXISTS prj_acr_file_hotspot_runs" in canonical[2]
    assert "MATERIALIZE PROJECTION IF EXISTS prj_acr_file_hotspot_runs" in canonical[3]
    assert "ADD PROJECTION IF NOT EXISTS prj_acr_file_complexity_runs" in canonical[4]
    assert (
        "MATERIALIZE PROJECTION IF EXISTS prj_acr_file_complexity_runs" in canonical[5]
    )
    assert all(
        "SETTINGS mutations_sync = 2" in statement for statement in canonical[1::2]
    )
