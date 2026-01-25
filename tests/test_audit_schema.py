from pathlib import Path

from dev_health_ops.audit.schema import (
    _build_clickhouse_expected_schema,
    format_schema_report,
)


def test_clickhouse_schema_includes_added_columns():
    repo_root = Path(__file__).resolve().parents[1]
    expected, migrations = _build_clickhouse_expected_schema(
        repo_root / "migrations" / "clickhouse"
    )

    assert "work_items" in expected
    assert "priority_raw" in expected["work_items"]
    hints = migrations["columns"].get(("work_items", "priority_raw"))
    assert hints and "011_work_item_extras.sql" in hints


def test_format_schema_report_includes_migrations():
    report = {
        "status": "missing",
        "db_backend": "clickhouse",
        "missing_tables": ["work_items"],
        "missing_columns": {"work_items": ["priority_raw"]},
        "type_mismatches": {},
        "migration_hints": {
            "tables": {"work_items": ["009_raw_work_items.sql"]},
            "columns": {"work_items.priority_raw": ["011_work_item_extras.sql"]},
        },
    }

    output = format_schema_report(report)
    assert "work_items" in output
    assert "011_work_item_extras.sql" in output
