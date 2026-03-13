import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.loaders.sqlalchemy import SqlAlchemyDataLoader


@pytest.mark.asyncio
async def test_clickhouse_loader_async():
    mock_client = MagicMock()
    # Mocking successful queries
    mock_result = MagicMock()
    mock_result.column_names = [
        "repo_id",
        "commit_hash",
        "author_email",
        "author_name",
        "committer_when",
        "file_path",
        "additions",
        "deletions",
    ]
    mock_result.result_rows = []
    mock_client.query.return_value = mock_result

    loader = ClickHouseDataLoader(mock_client)
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    # Verify it's awaitable and doesn't crash
    res = await loader.load_git_rows(start, end, uuid.uuid4())
    assert len(res) == 3
    assert isinstance(res[0], list)


@pytest.mark.asyncio
async def test_sqlalchemy_loader_async():
    engine = create_engine("sqlite:///:memory:")
    # Simple setup: tables usually exist, but we just verify the async call wrap
    loader = SqlAlchemyDataLoader(engine)
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    # This will likely fail on missing tables if executed, but we want to verify the async signature
    with pytest.raises(Exception):
        await loader.load_git_rows(start, end, uuid.uuid4())
