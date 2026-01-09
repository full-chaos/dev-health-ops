"""Tests for work graph ClickHouse writer."""

import pytest
import uuid
from unittest.mock import MagicMock, patch

from work_graph.models import (
    NodeType,
    EdgeType,
    Provenance,
    WorkGraphEdge,
    WorkGraphIssuePR,
    WorkGraphPRCommit,
)
from work_graph.writers.clickhouse import ClickHouseWorkGraphWriter


@pytest.fixture
def mock_client():
    """Create a mock ClickHouse client."""
    return MagicMock()


@pytest.fixture
def writer(mock_client):
    """Create a writer with a mock client."""
    with patch(
        "work_graph.writers.clickhouse.clickhouse_connect.get_client",
        return_value=mock_client,
    ):
        return ClickHouseWorkGraphWriter("clickhouse://localhost:9000/default")


class TestClickHouseWorkGraphWriter:
    """Tests for ClickHouseWorkGraphWriter."""

    def test_write_edges_empty(self, writer, mock_client):
        """Writing empty list should not call client."""
        result = writer.write_edges([])
        assert result == 0
        mock_client.insert.assert_not_called()

    def test_write_edges(self, writer, mock_client):
        """Writing edges should call client.insert."""
        repo_id = uuid.uuid4()
        edges = [
            WorkGraphEdge(
                edge_id="edge-1",
                source_type=NodeType.ISSUE,
                source_id="jira:ABC-123",
                target_type=NodeType.PR,
                target_id="pr:repo-uuid#456",
                edge_type=EdgeType.REFERENCES,
                provenance=Provenance.EXPLICIT_TEXT,
                confidence=0.8,
                evidence="found in PR title",
                repo_id=repo_id,
            ),
        ]
        writer.write_edges(edges)
        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        # First positional arg is table name
        assert call_args[0][0] == "work_graph_edges"

    def test_write_issue_pr_links_empty(self, writer, mock_client):
        """Writing empty list should not call client."""
        result = writer.write_issue_pr_links([])
        assert result == 0
        mock_client.insert.assert_not_called()

    def test_write_issue_pr_links(self, writer, mock_client):
        """Writing issue-PR links should call client.insert."""
        repo_id = uuid.uuid4()
        links = [
            WorkGraphIssuePR(
                repo_id=repo_id,
                work_item_id="jira:ABC-123",
                pr_number=456,
                provenance=Provenance.NATIVE,
                confidence=0.95,
                evidence="closing keyword",
            ),
        ]
        writer.write_issue_pr_links(links)
        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        # First positional arg is table name
        assert call_args[0][0] == "work_graph_issue_pr"

    def test_write_pr_commit_links_empty(self, writer, mock_client):
        """Writing empty list should not call client."""
        result = writer.write_pr_commit_links([])
        assert result == 0
        mock_client.insert.assert_not_called()

    def test_write_pr_commit_links(self, writer, mock_client):
        """Writing PR-commit links should call client.insert."""
        repo_id = uuid.uuid4()
        links = [
            WorkGraphPRCommit(
                repo_id=repo_id,
                pr_number=123,
                commit_hash="abc123def456789012345678901234567890abcd",
                confidence=1.0,
                provenance=Provenance.NATIVE,
                evidence="PR API",
            ),
        ]
        writer.write_pr_commit_links(links)
        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        # First positional arg is table name
        assert call_args[0][0] == "work_graph_pr_commit"


class TestClickHouseWorkGraphWriterIntegration:
    """Integration tests for ClickHouseWorkGraphWriter.

    These tests are skipped by default and require a real ClickHouse instance.
    Run with: pytest -m integration
    """

    @pytest.mark.skip(reason="Requires ClickHouse instance")
    def test_write_and_read_edges(self):
        """Write edges and verify they can be read back."""
        # This would require a real ClickHouse instance
        pass

    @pytest.mark.skip(reason="Requires ClickHouse instance")
    def test_deduplication(self):
        """ReplacingMergeTree should deduplicate by edge_id."""
        pass
