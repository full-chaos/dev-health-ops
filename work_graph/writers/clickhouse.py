"""
ClickHouse writer for work graph edges.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Sequence

import clickhouse_connect

from work_graph.models import (
    WorkGraphEdge,
    WorkGraphIssuePR,
    WorkGraphPRCommit,
)

logger = logging.getLogger(__name__)


def _dt_to_clickhouse_datetime(value: datetime) -> datetime:
    """Convert datetime to ClickHouse-compatible format (UTC, no tzinfo)."""
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


class ClickHouseWorkGraphWriter:
    """
    Writer for persisting work graph data to ClickHouse.

    Uses batch inserts for efficiency with ReplacingMergeTree tables.
    """

    def __init__(self, dsn: str) -> None:
        """
        Initialize the ClickHouse writer.

        Args:
            dsn: ClickHouse connection string
        """
        if not dsn:
            raise ValueError("ClickHouse DSN is required")
        self.dsn = dsn
        settings = {
            "max_query_size": 1 * 1024 * 1024,  # 1MB
        }
        self.client = clickhouse_connect.get_client(dsn=dsn, settings=settings)

    def close(self) -> None:
        """Close the ClickHouse connection."""
        try:
            self.client.close()
        except Exception as e:
            logger.warning(
                "Exception occurred when closing ClickHouse client: %s",
                e,
                exc_info=True,
            )

    def write_edges(self, edges: Sequence[WorkGraphEdge]) -> int:
        """
        Write generic work graph edges.

        Args:
            edges: Sequence of WorkGraphEdge objects

        Returns:
            Number of edges written
        """
        if not edges:
            return 0

        columns = [
            "edge_id",
            "source_type",
            "source_id",
            "target_type",
            "target_id",
            "edge_type",
            "repo_id",
            "provider",
            "provenance",
            "confidence",
            "evidence",
            "discovered_at",
            "last_synced",
        ]

        data = []
        for edge in edges:
            row = [
                edge.edge_id,
                edge.source_type.value,
                edge.source_id,
                edge.target_type.value,
                edge.target_id,
                edge.edge_type.value,
                str(edge.repo_id) if edge.repo_id else None,
                edge.provider,
                edge.provenance.value,
                edge.confidence,
                edge.evidence,
                _dt_to_clickhouse_datetime(edge.discovered_at),
                _dt_to_clickhouse_datetime(edge.last_synced),
            ]
            data.append(row)

        self.client.insert("work_graph_edges", data, column_names=columns)
        logger.debug("Wrote %d work graph edges", len(data))
        return len(data)

    def write_issue_pr_links(self, links: Sequence[WorkGraphIssuePR]) -> int:
        """
        Write issue-to-PR relationships to the fast path table.

        Args:
            links: Sequence of WorkGraphIssuePR objects

        Returns:
            Number of links written
        """
        if not links:
            return 0

        columns = [
            "repo_id",
            "work_item_id",
            "pr_number",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
        ]

        data = []
        for link in links:
            row = [
                str(link.repo_id),
                link.work_item_id,
                link.pr_number,
                link.confidence,
                link.provenance.value,
                link.evidence,
                _dt_to_clickhouse_datetime(link.last_synced),
            ]
            data.append(row)

        self.client.insert("work_graph_issue_pr", data, column_names=columns)
        logger.debug("Wrote %d issue-PR links", len(data))
        return len(data)

    def write_pr_commit_links(self, links: Sequence[WorkGraphPRCommit]) -> int:
        """
        Write PR-to-commit relationships to the fast path table.

        Args:
            links: Sequence of WorkGraphPRCommit objects

        Returns:
            Number of links written
        """
        if not links:
            return 0

        columns = [
            "repo_id",
            "pr_number",
            "commit_hash",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
        ]

        data = []
        for link in links:
            row = [
                str(link.repo_id),
                link.pr_number,
                link.commit_hash,
                link.confidence,
                link.provenance.value,
                link.evidence,
                _dt_to_clickhouse_datetime(link.last_synced),
            ]
            data.append(row)

        self.client.insert("work_graph_pr_commit", data, column_names=columns)
        logger.debug("Wrote %d PR-commit links", len(data))
        return len(data)
