import pytest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "migrations" / "clickhouse"


def test_migrations_no_hardcoded_stats_db():
    """
    Ensure no ClickHouse migration contains hardcoded 'stats.' database references.
    """
    if not MIGRATIONS_DIR.exists():
        pytest.skip("No ClickHouse migrations found")

    sql_files = list(MIGRATIONS_DIR.glob("*.sql"))
    errors = []

    for path in sql_files:
        content = path.read_text(encoding="utf-8")
        if "stats." in content:
            # Check if it's not a legitimate use (e.g. comment?)
            # But the requirement is strict: "No ClickHouse migration SQL may reference a hardcoded database name like stats."
            # We can allow comments if we strictly parse, but simple string search is safer for now.
            lines = content.splitlines()
            for i, line in enumerate(lines):
                if "stats." in line:
                    errors.append(f"{path.name}:{i + 1}: {line.strip()}")

    assert not errors, (
        "Found hardcoded 'stats.' DB references in migrations:\n" + "\n".join(errors)
    )
