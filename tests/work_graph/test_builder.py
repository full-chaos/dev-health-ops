"""Tests for work graph builder."""

import pytest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from dev_health_ops.work_graph.builder import (
    BuildConfig,
    WorkGraphBuilder,
)


@pytest.fixture
def mock_ch_client():
    """Create a mock ClickHouse client."""
    client = MagicMock()
    # Mock query_df to return empty dataframe
    try:
        import pandas as pd

        client.query_df.return_value = pd.DataFrame()
    except ImportError:
        # pandas is an optional test dependency; if it's not available,
        # leave query_df unconfigured and allow tests that need it to handle this.
        pass
    return client


@pytest.fixture
def config():
    """Create a build config."""
    return BuildConfig(
        dsn="clickhouse://localhost:9000/default",
    )


class TestBuildConfig:
    """Tests for BuildConfig."""

    def test_defaults(self):
        """Default values should be set."""
        cfg = BuildConfig(
            dsn="clickhouse://localhost:9000/default",
        )
        assert cfg.from_date is None
        assert cfg.to_date is None
        assert cfg.repo_id is None
        assert cfg.heuristic_days_window == 7
        assert cfg.heuristic_confidence == 0.3

    def test_custom_values(self):
        """Custom values should be set."""
        from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        repo_id = uuid.uuid4()
        cfg = BuildConfig(
            dsn="clickhouse://localhost:9000/default",
            from_date=from_dt,
            repo_id=repo_id,
            heuristic_days_window=14,
            heuristic_confidence=0.5,
        )
        assert cfg.from_date == from_dt
        assert cfg.repo_id == repo_id
        assert cfg.heuristic_days_window == 14
        assert cfg.heuristic_confidence == 0.5


class TestWorkGraphBuilder:
    """Tests for WorkGraphBuilder."""

    def test_init(self, config):
        """Builder should initialize with config using sink pattern."""
        # Create a fake sink that mimics ClickHouseMetricsSink
        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.client = MagicMock()

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            assert builder.config == config
            assert builder.sink == fake_sink
            builder.close()
            fake_sink.close.assert_called_once()


class TestHeuristicMatching:
    """Tests for heuristic issue->PR matching with binary search optimization."""

    def test_heuristic_finds_closest_pr_in_window(self):
        """Heuristic should find the closest PR within time window."""
        repo_id = uuid.uuid4()
        base_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"
        fake_sink.query_dicts = MagicMock()

        wi_rows = [
            {
                "repo_id": repo_id,
                "work_item_id": "jira:TEST-1",
                "updated_at": base_time,
            },
        ]
        pr_rows = [
            {
                "repo_id": repo_id,
                "number": 1,
                "created_at": base_time - timedelta(days=10),
            },
            {
                "repo_id": repo_id,
                "number": 2,
                "created_at": base_time - timedelta(days=2),
            },
            {
                "repo_id": repo_id,
                "number": 3,
                "created_at": base_time + timedelta(days=1),
            },
            {
                "repo_id": repo_id,
                "number": 4,
                "created_at": base_time + timedelta(days=10),
            },
        ]

        def mock_query(query, params):
            if "work_items" in query:
                return wi_rows
            if "git_pull_requests" in query:
                return pr_rows
            return []

        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", heuristic_days_window=7
        )

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_heuristic_issue_pr_edges(set())
            builder.close()

        assert count == 1
        written_edges = fake_sink.write_work_graph_edges.call_args[0][0]
        assert len(written_edges) == 1
        assert written_edges[0].confidence == 0.3

    def test_heuristic_excludes_prs_outside_window(self):
        """PRs outside time window should not be matched."""
        repo_id = uuid.uuid4()
        base_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"

        wi_rows = [
            {
                "repo_id": repo_id,
                "work_item_id": "jira:TEST-1",
                "updated_at": base_time,
            },
        ]
        pr_rows = [
            {
                "repo_id": repo_id,
                "number": 1,
                "created_at": base_time - timedelta(days=30),
            },
            {
                "repo_id": repo_id,
                "number": 2,
                "created_at": base_time + timedelta(days=30),
            },
        ]

        def mock_query(query, params):
            if "work_items" in query:
                return wi_rows
            if "git_pull_requests" in query:
                return pr_rows
            return []

        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", heuristic_days_window=7
        )

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_heuristic_issue_pr_edges(set())
            builder.close()

        assert count == 0

    def test_heuristic_skips_explicit_links(self):
        """Already-linked work items should be skipped."""
        repo_id = uuid.uuid4()
        base_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fake_sink = MagicMock()
        fake_sink.backend_type = "clickhouse"

        wi_rows = [
            {
                "repo_id": repo_id,
                "work_item_id": "jira:TEST-1",
                "updated_at": base_time,
            },
        ]
        pr_rows = [
            {"repo_id": repo_id, "number": 1, "created_at": base_time},
        ]

        def mock_query(query, params):
            if "work_items" in query:
                return wi_rows
            if "git_pull_requests" in query:
                return pr_rows
            return []

        fake_sink.query_dicts.side_effect = mock_query
        fake_sink.write_work_graph_edges = MagicMock()
        fake_sink.write_work_graph_issue_pr = MagicMock()

        config = BuildConfig(
            dsn="clickhouse://localhost:9000/default", heuristic_days_window=7
        )
        explicit_links = {("jira:TEST-1", 1)}

        with patch(
            "dev_health_ops.work_graph.builder.create_sink", return_value=fake_sink
        ):
            builder = WorkGraphBuilder(config)
            count = builder._build_heuristic_issue_pr_edges(explicit_links)
            builder.close()

        assert count == 0


class TestWorkGraphBuilderIntegration:
    """Integration tests for WorkGraphBuilder.

    These tests are skipped by default and require a real ClickHouse instance.
    Run with: pytest -m integration
    """

    @pytest.mark.skip(reason="Requires ClickHouse instance")
    def test_full_build(self):
        """Build complete work graph."""
        pass

    @pytest.mark.skip(reason="Requires ClickHouse instance")
    def test_incremental_build(self):
        """Incremental build with from_date parameter."""
        pass
