"""Tests for MongoDB DORA metrics sink methods."""

import uuid
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from dev_health_ops.metrics.schemas import DORAMetricsRecord
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink


def test_mongo_sink_writes_dora_metrics():
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    with patch("dev_health_ops.metrics.sinks.mongo.MongoClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.get_default_database.return_value = mock_db
        mock_client_cls.return_value = mock_client

        sink = MongoMetricsSink("mongodb://localhost:27017/test_db", db_name="test_db")

        repo_id = uuid.uuid4()
        row = DORAMetricsRecord(
            repo_id=repo_id,
            day=date(2025, 1, 1),
            metric_name="deployment_frequency",
            value=3.0,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )

        sink.write_dora_metrics([row])

        mock_collection.bulk_write.assert_called_once()
        ops = mock_collection.bulk_write.call_args[0][0]
        assert len(ops) == 1
        assert ops[0]._filter == {
            "_id": f"{repo_id}:2025-01-01:deployment_frequency"
        }
