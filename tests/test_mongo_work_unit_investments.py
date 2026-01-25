"""Tests for MongoDB work unit investment sink methods."""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.metrics.schemas import (
    WorkUnitInvestmentRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
)
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink


@pytest.fixture
def mock_mongo_client():
    """Create a mock MongoDB client and database."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    return mock_client, mock_db


def test_mongo_sink_writes_work_unit_investments():
    """Test that write_work_unit_investments correctly writes to MongoDB."""
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    with patch("dev_health_ops.metrics.sinks.mongo.MongoClient") as mock_client_cls:
        mock_client = MagicMock()
        # MongoMetricsSink uses client[db_name] to get db, then db[collection] to get collection
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.get_default_database.return_value = mock_db
        mock_client_cls.return_value = mock_client

        sink = MongoMetricsSink("mongodb://localhost:27017/test_db", db_name="test_db")

        repo_id = uuid.uuid4()
        row = WorkUnitInvestmentRecord(
            work_unit_id="WU-123",
            work_unit_type="pr",
            work_unit_name="Fix authentication bug",
            from_ts=datetime(2025, 1, 1, tzinfo=timezone.utc),
            to_ts=datetime(2025, 1, 2, tzinfo=timezone.utc),
            repo_id=repo_id,
            provider="github",
            effort_metric="loc",
            effort_value=150.0,
            theme_distribution_json={"maintenance": 0.7, "feature": 0.3},
            subcategory_distribution_json={"bug_fix": 0.7, "refactor": 0.3},
            structural_evidence_json='{"files": ["auth.py"]}',
            evidence_quality=0.85,
            evidence_quality_band="high",
            categorization_status="success",
            categorization_errors_json="[]",
            categorization_model_version="v1.0",
            categorization_input_hash="abc123",
            categorization_run_id="run-001",
            computed_at=datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
        )

        sink.write_work_unit_investments([row])

        # Verify bulk_write was called
        mock_collection.bulk_write.assert_called_once()

        # Verify the operation contains correct data
        call_args = mock_collection.bulk_write.call_args
        ops = call_args[0][0]  # First positional arg is the list of operations
        assert len(ops) == 1

        # Check the document structure - ReplaceOne has _filter attribute
        op = ops[0]
        assert op._filter == {"_id": "WU-123:run-001"}


def test_mongo_sink_writes_work_unit_investment_quotes():
    """Test that write_work_unit_investment_quotes correctly writes to MongoDB."""
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    with patch("dev_health_ops.metrics.sinks.mongo.MongoClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.get_default_database.return_value = mock_db
        mock_client_cls.return_value = mock_client

        sink = MongoMetricsSink("mongodb://localhost:27017/test_db", db_name="test_db")

        row = WorkUnitInvestmentEvidenceQuoteRecord(
            work_unit_id="WU-123",
            quote="Refactored authentication flow to use JWT tokens",
            source_type="pr_description",
            source_id="PR-456",
            computed_at=datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            categorization_run_id="run-001",
        )

        sink.write_work_unit_investment_quotes([row])

        # Verify bulk_write was called
        mock_collection.bulk_write.assert_called_once()

        # Verify the operation contains correct data
        call_args = mock_collection.bulk_write.call_args
        ops = call_args[0][0]
        assert len(ops) == 1


def test_mongo_sink_skips_empty_work_unit_investments():
    """Test that write_work_unit_investments handles empty list gracefully."""
    mock_db = MagicMock()

    with patch("dev_health_ops.metrics.sinks.mongo.MongoClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.get_database.return_value = mock_db
        mock_client_cls.return_value = mock_client

        sink = MongoMetricsSink("mongodb://localhost:27017/test_db")

        # Should not raise and should not call bulk_write
        sink.write_work_unit_investments([])

        # Verify bulk_write was NOT called
        assert not mock_db["work_unit_investments"].bulk_write.called


def test_mongo_sink_skips_empty_work_unit_investment_quotes():
    """Test that write_work_unit_investment_quotes handles empty list gracefully."""
    mock_db = MagicMock()

    with patch("dev_health_ops.metrics.sinks.mongo.MongoClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.get_database.return_value = mock_db
        mock_client_cls.return_value = mock_client

        sink = MongoMetricsSink("mongodb://localhost:27017/test_db")

        # Should not raise and should not call bulk_write
        sink.write_work_unit_investment_quotes([])

        # Verify bulk_write was NOT called
        assert not mock_db["work_unit_investment_quotes"].bulk_write.called


def test_mongo_sink_handles_multiple_work_unit_investments():
    """Test that write_work_unit_investments handles multiple records."""
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    with patch("dev_health_ops.metrics.sinks.mongo.MongoClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.get_default_database.return_value = mock_db
        mock_client_cls.return_value = mock_client

        sink = MongoMetricsSink("mongodb://localhost:27017/test_db", db_name="test_db")

        rows = [
            WorkUnitInvestmentRecord(
                work_unit_id=f"WU-{i}",
                work_unit_type="pr",
                work_unit_name=f"Work unit {i}",
                from_ts=datetime(2025, 1, 1, tzinfo=timezone.utc),
                to_ts=datetime(2025, 1, 2, tzinfo=timezone.utc),
                repo_id=None,
                provider="github",
                effort_metric="loc",
                effort_value=float(i * 10),
                theme_distribution_json={"feature": 1.0},
                subcategory_distribution_json={"new_feature": 1.0},
                structural_evidence_json="{}",
                evidence_quality=0.8,
                evidence_quality_band="high",
                categorization_status="success",
                categorization_errors_json="[]",
                categorization_model_version="v1.0",
                categorization_input_hash=f"hash{i}",
                categorization_run_id=f"run-{i:03d}",
                computed_at=datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            )
            for i in range(3)
        ]

        sink.write_work_unit_investments(rows)

        # Verify bulk_write was called with 3 operations
        call_args = mock_collection.bulk_write.call_args
        ops = call_args[0][0]
        assert len(ops) == 3
