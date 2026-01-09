"""
Work Graph Builder - orchestrates work graph construction.

This module provides the main entry point for building the work graph
from raw data sources (work items, PRs, commits).
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from metrics.sinks.factory import create_sink
from metrics.schemas import WorkGraphEdgeRecord, WorkGraphIssuePRRecord

from work_graph.extractors.text_parser import (
    RefType,
    extract_github_issue_refs,
    extract_gitlab_issue_refs,
    extract_jira_keys,
)
from work_graph.ids import (
    generate_edge_id,
    generate_pr_id,
)
from work_graph.models import (
    EdgeType,
    NodeType,
    Provenance,
    WorkGraphEdge,
    WorkGraphIssuePR,
)

logger = logging.getLogger(__name__)


def _format_datetime_for_clickhouse(dt: datetime) -> str:
    """Format datetime for ClickHouse SQL queries."""
    # ClickHouse expects 'YYYY-MM-DD HH:MM:SS' format without timezone suffix
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# Mapping from work_item_dependencies relationship types to EdgeType
DEPENDENCY_TYPE_MAP: Dict[str, EdgeType] = {
    "blocks": EdgeType.BLOCKS,
    "is_blocked_by": EdgeType.IS_BLOCKED_BY,
    "relates": EdgeType.RELATES,
    "is_related_to": EdgeType.IS_RELATED_TO,
    "duplicates": EdgeType.DUPLICATES,
    "is_duplicate_of": EdgeType.IS_DUPLICATE_OF,
    "parent": EdgeType.PARENT_OF,
    "child": EdgeType.CHILD_OF,
    "is_parent_of": EdgeType.PARENT_OF,
    "is_child_of": EdgeType.CHILD_OF,
}


@dataclass
class BuildConfig:
    """Configuration for work graph build."""

    dsn: str
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    repo_id: Optional[uuid.UUID] = None
    heuristic_days_window: int = 7
    heuristic_confidence: float = 0.3


class WorkGraphBuilder:
    """
    Orchestrates work graph construction from raw data.

    The builder:
    1. Reads raw data from ClickHouse (work items, PRs, commits, dependencies)
    2. Extracts links using text parsing and heuristics
    3. Writes derived edges to work graph tables

    All operations are idempotent using deterministic edge IDs and
    ReplacingMergeTree for deduplication.
    """

    def __init__(self, config: BuildConfig) -> None:
        """
        Initialize the builder.

        Args:
            config: Build configuration
        """
        self.config = config
        # Canonical pattern: a single sink owns the backend client + migrations.
        self.sink = create_sink(config.dsn)
        if getattr(self.sink, "backend_type", None) != "clickhouse":
            raise ValueError("WorkGraphBuilder currently requires a ClickHouse sink")
        self.client = getattr(self.sink, "client", None)
        if self.client is None:
            raise ValueError("ClickHouse sink did not expose a client")
        self._now = datetime.now(timezone.utc)
        # NOTE: schema creation is handled by sink.ensure_schema()
        self.sink.ensure_schema()

    def close(self) -> None:
        """Close connections."""
        self.sink.close()

    def _edge_to_record(self, edge: WorkGraphEdge) -> WorkGraphEdgeRecord:
        """Convert WorkGraphEdge to WorkGraphEdgeRecord for sink."""
        return WorkGraphEdgeRecord(
            edge_id=edge.edge_id,
            source_type=edge.source_type.value,
            source_id=edge.source_id,
            target_type=edge.target_type.value,
            target_id=edge.target_id,
            edge_type=edge.edge_type.value,
            repo_id=edge.repo_id,
            provider=edge.provider,
            provenance=edge.provenance.value,
            confidence=edge.confidence,
            evidence=edge.evidence,
            discovered_at=edge.discovered_at or self._now,
            last_synced=edge.last_synced or self._now,
        )

    def _issue_pr_to_record(self, link: WorkGraphIssuePR) -> WorkGraphIssuePRRecord:
        """Convert WorkGraphIssuePR to WorkGraphIssuePRRecord for sink."""
        return WorkGraphIssuePRRecord(
            repo_id=link.repo_id,
            work_item_id=link.work_item_id,
            pr_number=link.pr_number,
            confidence=link.confidence,
            provenance=link.provenance.value,
            evidence=link.evidence,
            last_synced=link.last_synced or self._now,
        )

    def _write_edges(self, edges: List[WorkGraphEdge]) -> int:
        """Write edges via the sink."""
        if not edges:
            return 0
        records = [self._edge_to_record(e) for e in edges]
        self.sink.write_work_graph_edges(records)
        return len(records)

    def _write_issue_pr_links(self, links: List[WorkGraphIssuePR]) -> None:
        """Write issue-PR links via the sink."""
        if not links:
            return
        records = [self._issue_pr_to_record(lnk) for lnk in links]
        self.sink.write_work_graph_issue_pr(records)

    def build(self) -> dict:
        """
        Execute the full work graph build.

        Returns:
            Dictionary with statistics about edges created
        """
        stats = {
            "issue_issue_edges": 0,
            "issue_pr_edges": 0,
            "pr_commit_edges": 0,
            "commit_file_edges": 0,
            "heuristic_edges": 0,
        }

        logger.info("Starting work graph build...")

        # 1. Build issue->issue edges from work_item_dependencies
        stats["issue_issue_edges"] = self._build_issue_issue_edges()

        # 2. Build issue->PR edges from PR title/body text parsing
        issue_pr_explicit, stats["issue_pr_edges"] = self._build_issue_pr_edges()

        # 3. Build heuristic issue->PR edges for items not linked explicitly
        stats["heuristic_edges"] = self._build_heuristic_issue_pr_edges(
            issue_pr_explicit
        )

        # 4. Build PR->commit edges (if available in work_graph_pr_commit)
        stats["pr_commit_edges"] = self._count_pr_commit_edges()

        # 5. Commit->file edges are handled by view over git_commit_stats
        stats["commit_file_edges"] = self._count_commit_file_edges()

        logger.info(
            "Work graph build complete: %s",
            ", ".join(f"{k}={v}" for k, v in stats.items()),
        )

        return stats

    def _build_issue_issue_edges(self) -> int:
        """
        Build edges from work_item_dependencies.

        Returns:
            Number of edges created
        """
        logger.info("Building issue->issue edges from work_item_dependencies...")

        query = """
        SELECT
            source_work_item_id,
            target_work_item_id,
            relationship_type,
            relationship_type_raw
        FROM work_item_dependencies FINAL
        """

        result = self.client.query(query)
        rows = result.result_rows or []
        logger.info("Found %d rows in work_item_dependencies", len(rows))

        if not rows:
            logger.info("No work_item_dependencies found")
            return 0

        edges = []
        for row in rows:
            source_id, target_id, rel_type, rel_type_raw = row

            # Map relationship type to EdgeType
            edge_type = DEPENDENCY_TYPE_MAP.get(
                rel_type.lower() if rel_type else "",
                EdgeType.RELATES,  # Default to relates
            )

            edge_id = generate_edge_id(
                NodeType.ISSUE,
                source_id,
                edge_type,
                NodeType.ISSUE,
                target_id,
            )

            edge = WorkGraphEdge(
                edge_id=edge_id,
                source_type=NodeType.ISSUE,
                source_id=source_id,
                target_type=NodeType.ISSUE,
                target_id=target_id,
                edge_type=edge_type,
                provenance=Provenance.NATIVE,
                confidence=1.0,
                evidence=rel_type_raw or rel_type or "dependency",
                discovered_at=self._now,
                last_synced=self._now,
            )
            edges.append(edge)

        count = self._write_edges(edges)
        logger.info("Created %d issue->issue edges", count)
        return count

    def _build_issue_pr_edges(self) -> Tuple[Set[Tuple[str, int]], int]:
        """
        Build issue->PR edges from PR title text parsing.

        Returns:
            Tuple of (set of (work_item_id, pr_number) pairs, edge count)
        """
        logger.info("Building issue->PR edges from PR title parsing...")

        # Query PRs
        pr_query = """
        SELECT
            repo_id,
            number,
            title,
            created_at
        FROM git_pull_requests FINAL
        """
        if self.config.from_date:
            pr_query += f" WHERE created_at >= '{_format_datetime_for_clickhouse(self.config.from_date)}'"
        if self.config.to_date:
            if "WHERE" in pr_query:
                pr_query += f" AND created_at <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
            else:
                pr_query += f" WHERE created_at <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
        if self.config.repo_id:
            if "WHERE" in pr_query:
                pr_query += f" AND repo_id = '{self.config.repo_id}'"
            else:
                pr_query += f" WHERE repo_id = '{self.config.repo_id}'"

        pr_result = self.client.query(pr_query)
        pr_rows = pr_result.result_rows or []
        logger.info("Found %d PRs to process", len(pr_rows))

        if not pr_rows:
            logger.info("No PRs found")
            return set(), 0

        # Query work items to build lookup
        wi_query = """
        SELECT
            repo_id,
            work_item_id,
            provider,
            project_key,
            project_id
        FROM work_items FINAL
        """
        wi_result = self.client.query(wi_query)
        wi_rows = wi_result.result_rows or []
        logger.info("Found %d work items for lookup", len(wi_rows))

        # Build work item lookups
        # For Jira: key -> work_item_id
        jira_key_lookup: Dict[str, str] = {}
        # For GitHub/GitLab: (repo_id, issue_number) -> work_item_id
        gh_issue_lookup: Dict[Tuple[str, str], str] = {}
        gl_issue_lookup: Dict[Tuple[str, str], str] = {}

        for wi_row in wi_rows:
            repo_id, work_item_id, provider, project_key, project_id = wi_row

            if provider == "jira" and work_item_id:
                # Extract key from work_item_id (format: "jira:ABC-123")
                if work_item_id.startswith("jira:"):
                    jira_key = work_item_id[5:]  # Remove "jira:" prefix
                    jira_key_lookup[jira_key.upper()] = work_item_id
            elif provider == "github" and repo_id and work_item_id:
                # Extract issue number from work_item_id (format: "gh:owner/repo#123")
                if "#" in work_item_id:
                    issue_num = work_item_id.split("#")[-1]
                    gh_issue_lookup[(str(repo_id), issue_num)] = work_item_id
            elif provider == "gitlab" and repo_id and work_item_id:
                if "#" in work_item_id:
                    issue_num = work_item_id.split("#")[-1]
                    gl_issue_lookup[(str(repo_id), issue_num)] = work_item_id

        logger.info(
            "Built lookups: jira=%d, github=%d, gitlab=%d",
            len(jira_key_lookup),
            len(gh_issue_lookup),
            len(gl_issue_lookup),
        )

        # Debug: Log sample lookup keys
        if gh_issue_lookup:
            sample_gh_keys = list(gh_issue_lookup.keys())[:3]
            logger.debug("Sample GitHub lookup keys: %s", sample_gh_keys)

        # Collect unique repo_ids from PRs for comparison
        pr_repo_ids = set(str(row[0]) for row in pr_rows if row[0])
        wi_repo_ids = set(
            str(row[0]) for row in wi_rows if row[0] and row[2] == "github"
        )
        logger.debug("PR repo_ids: %s", pr_repo_ids)
        logger.debug("Work item repo_ids (GitHub): %s", wi_repo_ids)
        logger.debug("Repo ID overlap: %s", pr_repo_ids & wi_repo_ids)

        # Process PRs and extract references
        edges: List[WorkGraphEdge] = []
        fast_path_links: List[WorkGraphIssuePR] = []
        explicit_links: Set[Tuple[str, int]] = set()
        jira_refs_found = 0
        gh_refs_found = 0
        gl_refs_found = 0

        for pr_row in pr_rows:
            repo_id, pr_number, title, created_at = pr_row
            repo_id_str = str(repo_id)

            if not title:
                continue

            # Extract Jira keys
            jira_refs = extract_jira_keys(title)
            jira_refs_found += len(jira_refs)
            for ref in jira_refs:
                work_item_id = jira_key_lookup.get(ref.issue_key.upper())
                if not work_item_id:
                    logger.debug(
                        "No match for Jira ref %s in lookup (PR #%s: %s)",
                        ref.issue_key,
                        pr_number,
                        title[:50],
                    )
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    pr_id = generate_pr_id(uuid.UUID(repo_id_str), pr_number)
                    edge_id = generate_edge_id(
                        NodeType.PR,
                        pr_id,
                        edge_type,
                        NodeType.ISSUE,
                        work_item_id,
                    )

                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.PR,
                            source_id=pr_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=edge_type,
                            repo_id=uuid.UUID(repo_id_str),
                            provider="jira",
                            provenance=Provenance.EXPLICIT_TEXT,
                            confidence=0.9,
                            evidence=ref.raw_match,
                            discovered_at=self._now,
                            last_synced=self._now,
                        )
                    )

                    fast_path_links.append(
                        WorkGraphIssuePR(
                            repo_id=uuid.UUID(repo_id_str),
                            work_item_id=work_item_id,
                            pr_number=pr_number,
                            confidence=0.9,
                            provenance=Provenance.EXPLICIT_TEXT,
                            evidence=ref.raw_match,
                            last_synced=self._now,
                        )
                    )
                    explicit_links.add((work_item_id, pr_number))

            # Extract GitHub issue refs
            gh_refs = extract_github_issue_refs(title)
            gh_refs_found += len(gh_refs)
            for ref in gh_refs:
                work_item_id = gh_issue_lookup.get((repo_id_str, ref.issue_key))
                if not work_item_id:
                    logger.debug(
                        "No match for GitHub ref #%s in repo %s (PR #%s: %s) - lookup key: %s",
                        ref.issue_key,
                        repo_id_str,
                        pr_number,
                        title[:50],
                        (repo_id_str, ref.issue_key),
                    )
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    pr_id = generate_pr_id(uuid.UUID(repo_id_str), pr_number)
                    edge_id = generate_edge_id(
                        NodeType.PR,
                        pr_id,
                        edge_type,
                        NodeType.ISSUE,
                        work_item_id,
                    )

                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.PR,
                            source_id=pr_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=edge_type,
                            repo_id=uuid.UUID(repo_id_str),
                            provider="github",
                            provenance=Provenance.EXPLICIT_TEXT,
                            confidence=0.9,
                            evidence=ref.raw_match,
                            discovered_at=self._now,
                            last_synced=self._now,
                        )
                    )

                    fast_path_links.append(
                        WorkGraphIssuePR(
                            repo_id=uuid.UUID(repo_id_str),
                            work_item_id=work_item_id,
                            pr_number=pr_number,
                            confidence=0.9,
                            provenance=Provenance.EXPLICIT_TEXT,
                            evidence=ref.raw_match,
                            last_synced=self._now,
                        )
                    )
                    explicit_links.add((work_item_id, pr_number))

            # Extract GitLab issue refs
            gl_refs = extract_gitlab_issue_refs(title)
            gl_refs_found += len(gl_refs)
            for ref in gl_refs:
                work_item_id = gl_issue_lookup.get((repo_id_str, ref.issue_key))
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    pr_id = generate_pr_id(uuid.UUID(repo_id_str), pr_number)
                    edge_id = generate_edge_id(
                        NodeType.PR,
                        pr_id,
                        edge_type,
                        NodeType.ISSUE,
                        work_item_id,
                    )

                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.PR,
                            source_id=pr_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=edge_type,
                            repo_id=uuid.UUID(repo_id_str),
                            provider="gitlab",
                            provenance=Provenance.EXPLICIT_TEXT,
                            confidence=0.9,
                            evidence=ref.raw_match,
                            discovered_at=self._now,
                            last_synced=self._now,
                        )
                    )

                    fast_path_links.append(
                        WorkGraphIssuePR(
                            repo_id=uuid.UUID(repo_id_str),
                            work_item_id=work_item_id,
                            pr_number=pr_number,
                            confidence=0.9,
                            provenance=Provenance.EXPLICIT_TEXT,
                            evidence=ref.raw_match,
                            last_synced=self._now,
                        )
                    )
                    explicit_links.add((work_item_id, pr_number))

        # Write edges
        edge_count = self._write_edges(edges)
        self._write_issue_pr_links(fast_path_links)

        logger.info(
            "Extracted refs: jira=%d, github=%d, gitlab=%d",
            jira_refs_found,
            gh_refs_found,
            gl_refs_found,
        )
        logger.info("Created %d issue->PR edges from text parsing", edge_count)
        return explicit_links, edge_count

    def _build_heuristic_issue_pr_edges(
        self, explicit_links: Set[Tuple[str, int]]
    ) -> int:
        """
        Build heuristic issue->PR edges for items not linked explicitly.

        Uses time-window matching: PR created within N days of issue updated_at.

        Args:
            explicit_links: Set of (work_item_id, pr_number) pairs already linked

        Returns:
            Number of heuristic edges created
        """
        if not self.config.heuristic_days_window:
            return 0

        logger.info(
            "Building heuristic issue->PR edges (window=%d days)...",
            self.config.heuristic_days_window,
        )

        # Query work items with timestamps
        wi_query = """
        SELECT
            repo_id,
            work_item_id,
            updated_at
        FROM work_items FINAL
        WHERE repo_id IS NOT NULL
        """
        wi_result = self.client.query(wi_query)
        wi_rows = wi_result.result_rows or []

        if not wi_rows:
            return 0

        # Query PRs with timestamps
        pr_query = """
        SELECT
            repo_id,
            number,
            created_at
        FROM git_pull_requests FINAL
        """
        pr_result = self.client.query(pr_query)
        pr_rows = pr_result.result_rows or []

        if not pr_rows:
            return 0

        # Group PRs by repo
        prs_by_repo: Dict[str, List[Tuple[int, datetime]]] = {}
        for repo_id, pr_number, created_at in pr_rows:
            repo_key = str(repo_id)
            if repo_key not in prs_by_repo:
                prs_by_repo[repo_key] = []
            prs_by_repo[repo_key].append((pr_number, created_at))

        window = timedelta(days=self.config.heuristic_days_window)
        edges: List[WorkGraphEdge] = []
        fast_path_links: List[WorkGraphIssuePR] = []

        for wi_row in wi_rows:
            repo_id, work_item_id, updated_at = wi_row
            repo_key = str(repo_id)

            if repo_key not in prs_by_repo:
                continue

            for pr_number, pr_created_at in prs_by_repo[repo_key]:
                # Skip if already linked explicitly
                if (work_item_id, pr_number) in explicit_links:
                    continue

                # Check time window
                if not updated_at or not pr_created_at:
                    continue

                time_diff = abs((pr_created_at - updated_at).total_seconds())
                if time_diff <= window.total_seconds():
                    pr_id = generate_pr_id(uuid.UUID(repo_key), pr_number)
                    edge_id = generate_edge_id(
                        NodeType.PR,
                        pr_id,
                        EdgeType.RELATES,
                        NodeType.ISSUE,
                        work_item_id,
                    )

                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.PR,
                            source_id=pr_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=EdgeType.RELATES,
                            repo_id=uuid.UUID(repo_key),
                            provenance=Provenance.HEURISTIC,
                            confidence=self.config.heuristic_confidence,
                            evidence=f"time_window_{self.config.heuristic_days_window}d",
                            discovered_at=self._now,
                            last_synced=self._now,
                        )
                    )

                    fast_path_links.append(
                        WorkGraphIssuePR(
                            repo_id=uuid.UUID(repo_key),
                            work_item_id=work_item_id,
                            pr_number=pr_number,
                            confidence=self.config.heuristic_confidence,
                            provenance=Provenance.HEURISTIC,
                            evidence=f"time_window_{self.config.heuristic_days_window}d",
                            last_synced=self._now,
                        )
                    )

        count = self._write_edges(edges)
        self._write_issue_pr_links(fast_path_links)
        logger.info("Created %d heuristic issue->PR edges", count)
        return count

    def _count_pr_commit_edges(self) -> int:
        """Count existing PR->commit edges (from processors)."""
        query = "SELECT count() FROM work_graph_pr_commit"
        try:
            result = self.client.query(query)
            count = result.result_rows[0][0] if result.result_rows else 0
            logger.info("Found %d existing PR->commit edges", count)
            return count
        except Exception as e:
            logger.warning("Could not count PR->commit edges: %s", e)
            return 0

    def _count_commit_file_edges(self) -> int:
        """Count commit->file edges from the view."""
        query = "SELECT count() FROM work_graph_commit_file"
        try:
            result = self.client.query(query)
            count = result.result_rows[0][0] if result.result_rows else 0
            logger.info("Found %d commit->file edges (via view)", count)
            return count
        except Exception as e:
            logger.warning("Could not count commit->file edges: %s", e)
            return 0


def main() -> int:
    """CLI entry point for work graph builder."""
    parser = argparse.ArgumentParser(
        description="Build work graph from raw data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full rebuild
  python -m work_graph.builder --db clickhouse://localhost:8123/default

  # Rebuild for date range
  python -m work_graph.builder --from 2025-01-01 --to 2025-01-31 --db ...

  # Rebuild for specific repo
  python -m work_graph.builder --repo <uuid> --db ...
        """,
    )

    parser.add_argument(
        "--db",
        required=True,
        help="ClickHouse connection string",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--repo",
        dest="repo_id",
        type=str,
        help="Repository UUID to filter by",
    )
    parser.add_argument(
        "--heuristic-window",
        type=int,
        default=7,
        help="Days window for heuristic matching (default: 7)",
    )
    parser.add_argument(
        "--heuristic-confidence",
        type=float,
        default=0.3,
        help="Confidence score for heuristic matches (default: 0.3)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Parse dates
    from_date = None
    to_date = None
    if args.from_date:
        from_date = datetime.fromisoformat(args.from_date).replace(tzinfo=timezone.utc)
    if args.to_date:
        to_date = datetime.fromisoformat(args.to_date).replace(tzinfo=timezone.utc)

    # Parse repo UUID
    repo_uuid = None
    if args.repo_id:
        repo_uuid = uuid.UUID(args.repo_id)

    config = BuildConfig(
        dsn=args.db,
        from_date=from_date,
        to_date=to_date,
        repo_id=repo_uuid,
        heuristic_days_window=args.heuristic_window,
        heuristic_confidence=args.heuristic_confidence,
    )

    builder = WorkGraphBuilder(config)
    try:
        stats = builder.build()
        total = sum(stats.values())
        print(f"Work graph build complete. Total edges: {total}")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        return 0
    except Exception as e:
        logger.exception("Work graph build failed: %s", e)
        return 1
    finally:
        builder.close()


if __name__ == "__main__":
    sys.exit(main())
