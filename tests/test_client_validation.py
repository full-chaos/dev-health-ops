import pytest
from dev_health_ops.api.queries.client import query_dicts


@pytest.mark.asyncio
async def test_query_dicts_validates_client():
    # Test None client
    with pytest.raises(RuntimeError) as excinfo:
        await query_dicts(None, "SELECT 1", {})
    assert "ClickHouse client is None" in str(excinfo.value)

    # Test invalid client object (no query method)
    invalid_client = object()
    with pytest.raises(RuntimeError) as excinfo:
        await query_dicts(invalid_client, "SELECT 1", {})
    assert "Invalid ClickHouse client" in str(excinfo.value)
    assert "object" in str(excinfo.value)  # Type name should be in message

    # Test valid-looking mock
    class MockClient:
        def query(self, query, parameters):
            return []

    # strict validation shouldn't fail for a client with query method,
    # but _rows_to_dicts might fail if return value isn't right.
    # We only care that it passed the client validation check.
    # To pass _rows_to_dicts, we need a result object with column_names and result_rows

    class MockResult:
        column_names = ["a"]
        result_rows = [[1]]

    class ValidMockClient:
        def query(self, query, parameters):
            return MockResult()

    result = await query_dicts(ValidMockClient(), "SELECT 1", {})
    assert result == [{"a": 1}]
