import pytest
from unittest.mock import MagicMock, patch
from dev_health_ops.storage import ClickHouseStore


@pytest.mark.asyncio
async def test_clickhouse_migrations_dry_run_non_default_db():
    """
    Simulate running migrations against a non-default database (e.g. 'test_db').
    Verifies that:
    1. Migration files are discovered.
    2. SQL commands are executed.
    3. Executed SQL does not contain 'stats.' references.
    """

    # Mock ClickHouse client
    mock_client = MagicMock()
    # Mock query result for schema_migrations (empty, so all migrations run)
    mock_client.query.return_value.result_rows = []

    # Capture commands executed
    executed_commands = []

    def mock_command(cmd, parameters=None):
        executed_commands.append(cmd)

    mock_client.command.side_effect = mock_command

    # Mock clickhouse_connect
    with patch("clickhouse_connect.get_client", return_value=mock_client):
        # Initialize store with a non-default DB
        conn_string = "clickhouse://localhost:8123/test_db"
        store = ClickHouseStore(conn_string)

        # Manually trigger ensure_tables (usually called in __aenter__)
        # We need to mock the migrations path to ensure it finds the real files
        # storage.py uses: Path(__file__).resolve().parent / "migrations" / "clickhouse"
        # Since we are importing ClickHouseStore from dev_health_ops.storage, it uses storage.py's path.
        # This is fine, it should find the real files in the project.

        await store.__aenter__()

        # Verify migrations were attempted
        assert len(executed_commands) > 0, "No migrations were executed"

        # Check for 'stats.' in any executed command
        # We allow 'stats' if it's part of a word like 'statistics' but not 'stats.' as a schema qualifier.
        for cmd in executed_commands:
            # We already have a static check, but this confirms what is actually run.
            assert "stats." not in cmd, f"Found 'stats.' in executed command: {cmd}"

        # Verify we inserted into schema_migrations
        inserts = [
            cmd for cmd in executed_commands if "INSERT INTO schema_migrations" in cmd
        ]
        assert len(inserts) > 0, "No migration records inserted"

        print(
            f"Verified {len(executed_commands)} commands executed against 'test_db' without 'stats.' references."
        )
