"""
Contract tests for metrics sink abstraction.

These tests validate that all sink implementations provide consistent
interfaces and handle edge cases uniformly.
"""

from __future__ import annotations

import pytest

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.factory import (
    SinkBackend,
    create_sink,
    detect_backend,
)


class TestBackendDetection:
    """Tests for detect_backend function."""

    def test_clickhouse_detection(self):
        assert detect_backend("clickhouse://localhost:8123") == SinkBackend.CLICKHOUSE
        assert detect_backend("clickhouse+http://localhost") == SinkBackend.CLICKHOUSE
        assert (
            detect_backend("clickhouse+https://localhost:8123")
            == SinkBackend.CLICKHOUSE
        )

    def test_postgres_detection(self):
        assert detect_backend("postgresql://localhost/db") == SinkBackend.POSTGRES
        assert detect_backend("postgres://localhost/db") == SinkBackend.POSTGRES
        assert (
            detect_backend("postgresql+asyncpg://localhost/db") == SinkBackend.POSTGRES
        )

    def test_sqlite_detection(self):
        assert detect_backend("sqlite:///./test.db") == SinkBackend.SQLITE
        assert detect_backend("sqlite+aiosqlite:///./test.db") == SinkBackend.SQLITE

    def test_mongo_detection(self):
        assert detect_backend("mongodb://localhost:27017") == SinkBackend.MONGO
        assert detect_backend("mongodb+srv://cluster.example.net") == SinkBackend.MONGO

    def test_unknown_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown sink scheme"):
            detect_backend("unknown://localhost")


class TestSinkFactory:
    """Tests for sink factory creation."""

    def test_creates_clickhouse_sink(self):
        """Verify factory returns ClickHouseSink for clickhouse:// DSN."""
        from unittest.mock import MagicMock, patch

        mock_client = MagicMock()
        with patch("clickhouse_connect.get_client", return_value=mock_client):
            sink = create_sink("clickhouse://localhost:8123/default")
            assert sink.backend_type == "clickhouse"
            sink.close()

    def test_creates_sqlite_sink(self):
        sink = create_sink("sqlite:///./test_sinks.db")
        assert sink.backend_type == "sqlite"
        sink.close()

    def test_requires_dsn_or_env_var(self):
        import os

        # Temporarily clear the env var if set
        old_val = os.environ.pop("DEV_HEALTH_SINK", None)
        try:
            with pytest.raises(ValueError, match="No sink DSN provided"):
                create_sink()
        finally:
            if old_val:
                os.environ["DEV_HEALTH_SINK"] = old_val


class TestSinkInterface:
    """Tests for sink interface contract."""

    def test_base_sink_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            BaseMetricsSink()  # type: ignore

    def test_sqlite_sink_has_required_methods(self):
        sink = create_sink("sqlite:///:memory:")
        try:
            # Check all required methods exist
            assert hasattr(sink, "backend_type")
            assert hasattr(sink, "close")
            assert hasattr(sink, "ensure_schema")
            assert hasattr(sink, "write_repo_metrics")
            assert hasattr(sink, "write_user_metrics")
            assert hasattr(sink, "write_commit_metrics")
            assert hasattr(sink, "write_file_metrics")
            assert hasattr(sink, "write_team_metrics")
            assert hasattr(sink, "write_work_item_metrics")
            assert hasattr(sink, "write_investment_classifications")
            assert hasattr(sink, "write_investment_metrics")
            assert hasattr(sink, "write_issue_type_metrics")
        finally:
            sink.close()

    def test_sqlite_sink_ensure_schema(self):
        sink = create_sink("sqlite:///:memory:")
        try:
            # Should not raise
            sink.ensure_schema()
        finally:
            sink.close()
