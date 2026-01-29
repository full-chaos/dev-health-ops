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

from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.metrics.schemas import WorkGraphEdgeRecord, WorkGraphIssuePRRecord

from dev_health_ops.work_graph.extractors.text_parser import (
    RefType,
    extract_github_issue_refs,
    extract_gitlab_issue_refs,
    extract_jira_keys,
)
from dev_health_ops.work_graph.ids import (
    generate_edge_id,
    generate_commit_id,
    generate_pr_id,
)
from dev_health_ops.work_graph.models import (
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
            event_ts=edge.event_ts or self._now,
            day=edge.day or (edge.event_ts or self._now).date(),
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

    @staticmethod
    def _parse_provenance(value: Optional[str]) -> Provenance:
        raw = str(value or "").strip().lower()
        if raw == Provenance.NATIVE.value:
            return Provenance.NATIVE
        if raw == Provenance.EXPLICIT_TEXT.value:
            return Provenance.EXPLICIT_TEXT
        if raw == Provenance.HEURISTIC.value:
            return Provenance.HEURISTIC
        if raw:
            return Provenance.NATIVE
        return Provenance.NATIVE

    def build(self) -> dict:
        """
        Execute the full work graph build.

        Returns:
            Dictionary with statistics about edges created
        """
        stats = {
            "issue_issue_edges": 0,
            "issue_pr_edges": 0,
            "issue_commit_edges": 0,
            "pr_commit_edges": 0,
            "commit_file_edges": 0,
            "heuristic_edges": 0,
        }

        logger.info("Starting work graph build...")

        # 1. Build issue->issue edges from work_item_dependencies
        stats["issue_issue_edges"] = self._build_issue_issue_edges()

        # 2. Build issue->PR edges from existing fast-path table (prerequisite)
        issue_pr_existing, stats["issue_pr_edges"] = (
            self._build_issue_pr_edges_from_fast_path()
        )

        # 3. Build issue->PR edges from PR title/body text parsing (fills fast path)
        issue_pr_explicit, parsed_count = self._build_issue_pr_edges()
        stats["issue_pr_edges"] += parsed_count
        issue_pr_explicit |= issue_pr_existing

        # 3b. Build issue->commit edges from commit message parsing
        stats["issue_commit_edges"] = self._build_issue_commit_edges_from_text_parsing()

        # 4. Build heuristic issue->PR edges for items not linked explicitly
        stats["heuristic_edges"] = self._build_heuristic_issue_pr_edges(
            issue_pr_explicit
        )

        # 5. Build PR->commit edges from fast-path table (prerequisite)
        stats["pr_commit_edges"] = self._build_pr_commit_edges_from_fast_path()

        # 6. Commit->file edges are handled by view over git_commit_stats
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
            relationship_type_raw,
            last_synced
        FROM work_item_dependencies
        """

        rows = self.sink.query_dicts(query, {})
        logger.info("Found %d rows in work_item_dependencies", len(rows))

        if not rows:
            logger.info("No work_item_dependencies found")
            return 0

        edges = []
        for row in rows:
            source_id = row.get("source_work_item_id")
            target_id = row.get("target_work_item_id")
            rel_type = row.get("relationship_type")
            rel_type_raw = row.get("relationship_type_raw")
            last_synced = row.get("last_synced")

            if not source_id or not target_id:
                continue

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

            # Ensure timezone
            event_ts = last_synced
            if isinstance(event_ts, str):
                try:
                    event_ts = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
                except ValueError:
                    event_ts = self._now

            if event_ts and event_ts.tzinfo is None:
                event_ts = event_ts.replace(tzinfo=timezone.utc)
            if not event_ts:
                event_ts = self._now

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
                event_ts=event_ts,
            )
            edges.append(edge)

        count = self._write_edges(edges)
        logger.info("Created %d issue->issue edges", count)
        return count

    def _build_issue_pr_edges(self) -> Tuple[Set[Tuple[str, int]], int]:
        """
        Build issue->PR edges from PR title and body text parsing.

        Returns:
            Tuple of (set of (work_item_id, pr_number) pairs, edge count)
        """
        logger.info("Building issue->PR edges from PR title/body parsing...")

        pr_query = """
        SELECT
            repo_id,
            number,
            title,
            body,
            head_branch,
            created_at
        FROM git_pull_requests
        """
        where_clauses = []
        if self.config.from_date:
            where_clauses.append(
                f"created_at >= '{_format_datetime_for_clickhouse(self.config.from_date)}'"
            )
        if self.config.to_date:
            where_clauses.append(
                f"created_at <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
            )
        if self.config.repo_id:
            where_clauses.append(f"repo_id = '{self.config.repo_id}'")

        if where_clauses:
            pr_query += " WHERE " + " AND ".join(where_clauses)

        pr_rows = self.sink.query_dicts(pr_query, {})
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
        FROM work_items
        """
        wi_rows = self.sink.query_dicts(wi_query, {})
        logger.info("Found %d work items for lookup", len(wi_rows))

        # Build work item lookups
        # For Jira: key -> work_item_id
        jira_key_lookup: Dict[str, str] = {}
        # For GitHub/GitLab: (repo_id, issue_number) -> work_item_id
        gh_issue_lookup: Dict[Tuple[str, str], str] = {}
        gl_issue_lookup: Dict[Tuple[str, str], str] = {}

        for wi_row in wi_rows:
            repo_id = wi_row.get("repo_id")
            work_item_id = wi_row.get("work_item_id")
            provider = wi_row.get("provider")

            if provider == "jira" and work_item_id:
                # Extract key from work_item_id (format: "jira:ABC-123")
                if str(work_item_id).startswith("jira:"):
                    jira_key = str(work_item_id)[5:]  # Remove "jira:" prefix
                    jira_key_lookup[jira_key.upper()] = str(work_item_id)
            elif provider == "github" and repo_id and work_item_id:
                # Extract issue number from work_item_id (format: "gh:owner/repo#123")
                if "#" in str(work_item_id):
                    issue_num = str(work_item_id).split("#")[-1]
                    gh_issue_lookup[(str(repo_id), issue_num)] = str(work_item_id)
            elif provider == "gitlab" and repo_id and work_item_id:
                if "#" in str(work_item_id):
                    issue_num = str(work_item_id).split("#")[-1]
                    gl_issue_lookup[(str(repo_id), issue_num)] = str(work_item_id)

        logger.info(
            "Built lookups: jira=%d, github=%d, gitlab=%d",
            len(jira_key_lookup),
            len(gh_issue_lookup),
            len(gl_issue_lookup),
        )

        # Collect unique repo_ids from PRs for comparison
        pr_repo_ids = {str(row.get("repo_id")) for row in pr_rows if row.get("repo_id")}
        wi_repo_ids = {
            str(row.get("repo_id"))
            for row in wi_rows
            if row.get("repo_id") and row.get("provider") == "github"
        }
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
            repo_id = pr_row.get("repo_id")
            pr_number = pr_row.get("number")
            title = pr_row.get("title") or ""
            body = pr_row.get("body") or ""
            head_branch = pr_row.get("head_branch") or ""
            created_at = pr_row.get("created_at")
            repo_id_str = str(repo_id)

            if not title and not body and not head_branch:
                continue
            if pr_number is None:
                continue
            pr_number_int = int(pr_number)

            event_ts = created_at
            if isinstance(event_ts, str):
                try:
                    event_ts = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
                except ValueError:
                    event_ts = self._now
            if event_ts and event_ts.tzinfo is None:
                event_ts = event_ts.replace(tzinfo=timezone.utc)
            if not event_ts:
                event_ts = self._now

            text_to_parse = f"{title}\n{body}\n{head_branch}"
            jira_refs = extract_jira_keys(text_to_parse)
            jira_refs_found += len(jira_refs)
            for ref in jira_refs:
                work_item_id = jira_key_lookup.get(ref.issue_key.upper())
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    pr_id = generate_pr_id(uuid.UUID(repo_id_str), pr_number_int)
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
                            event_ts=event_ts,
                        )
                    )

                    fast_path_links.append(
                        WorkGraphIssuePR(
                            repo_id=uuid.UUID(repo_id_str),
                            work_item_id=work_item_id,
                            pr_number=pr_number_int,
                            confidence=0.9,
                            provenance=Provenance.EXPLICIT_TEXT,
                            evidence=ref.raw_match,
                            last_synced=self._now,
                        )
                    )
                    explicit_links.add((work_item_id, pr_number_int))

            gh_refs = extract_github_issue_refs(text_to_parse)
            gh_refs_found += len(gh_refs)
            for ref in gh_refs:
                work_item_id = gh_issue_lookup.get((repo_id_str, ref.issue_key))
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    pr_id = generate_pr_id(uuid.UUID(repo_id_str), pr_number_int)
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
                            event_ts=event_ts,
                        )
                    )

                    fast_path_links.append(
                        WorkGraphIssuePR(
                            repo_id=uuid.UUID(repo_id_str),
                            work_item_id=work_item_id,
                            pr_number=pr_number_int,
                            confidence=0.9,
                            provenance=Provenance.EXPLICIT_TEXT,
                            evidence=ref.raw_match,
                            last_synced=self._now,
                        )
                    )
                    explicit_links.add((work_item_id, pr_number_int))

            gl_refs = extract_gitlab_issue_refs(text_to_parse)
            gl_refs_found += len(gl_refs)
            for ref in gl_refs:
                work_item_id = gl_issue_lookup.get((repo_id_str, ref.issue_key))
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    pr_id = generate_pr_id(uuid.UUID(repo_id_str), pr_number_int)
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
                            event_ts=event_ts,
                        )
                    )

                    fast_path_links.append(
                        WorkGraphIssuePR(
                            repo_id=uuid.UUID(repo_id_str),
                            work_item_id=work_item_id,
                            pr_number=pr_number_int,
                            confidence=0.9,
                            provenance=Provenance.EXPLICIT_TEXT,
                            evidence=ref.raw_match,
                            last_synced=self._now,
                        )
                    )
                    explicit_links.add((work_item_id, pr_number_int))

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

    def _build_issue_commit_edges_from_text_parsing(self) -> int:
        """Build issue->commit edges by parsing commit messages for issue refs."""
        logger.info("Building issue->commit edges from commit message parsing...")

        commit_query = """
        SELECT
            repo_id,
            hash,
            message,
            author_when
        FROM git_commits
        WHERE message IS NOT NULL AND message != ''
        """
        where_clauses = []
        if self.config.from_date:
            where_clauses.append(
                f"author_when >= '{_format_datetime_for_clickhouse(self.config.from_date)}'"
            )
        if self.config.to_date:
            where_clauses.append(
                f"author_when <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
            )
        if self.config.repo_id:
            where_clauses.append(f"repo_id = '{self.config.repo_id}'")

        if where_clauses:
            commit_query += " AND " + " AND ".join(where_clauses)

        commit_rows = self.sink.query_dicts(commit_query, {})
        logger.info("Found %d commits to process for issue refs", len(commit_rows))

        if not commit_rows:
            return 0

        wi_query = """
        SELECT
            repo_id,
            work_item_id,
            provider,
            project_key,
            project_id
        FROM work_items
        """
        wi_rows = self.sink.query_dicts(wi_query, {})

        jira_key_lookup: Dict[str, str] = {}
        gh_issue_lookup: Dict[Tuple[str, str], str] = {}
        gl_issue_lookup: Dict[Tuple[str, str], str] = {}

        for wi_row in wi_rows:
            repo_id = wi_row.get("repo_id")
            work_item_id = wi_row.get("work_item_id")
            provider = wi_row.get("provider")

            if provider == "jira" and work_item_id:
                if str(work_item_id).startswith("jira:"):
                    jira_key = str(work_item_id)[5:]
                    jira_key_lookup[jira_key.upper()] = str(work_item_id)
            elif provider == "github" and repo_id and work_item_id:
                if "#" in str(work_item_id):
                    issue_num = str(work_item_id).split("#")[-1]
                    gh_issue_lookup[(str(repo_id), issue_num)] = str(work_item_id)
            elif provider == "gitlab" and repo_id and work_item_id:
                if "#" in str(work_item_id):
                    issue_num = str(work_item_id).split("#")[-1]
                    gl_issue_lookup[(str(repo_id), issue_num)] = str(work_item_id)

        logger.info(
            "Built lookups for commits: jira=%d, github=%d, gitlab=%d",
            len(jira_key_lookup),
            len(gh_issue_lookup),
            len(gl_issue_lookup),
        )

        edges: List[WorkGraphEdge] = []
        jira_refs_found = 0
        gh_refs_found = 0
        gl_refs_found = 0
        seen_edges: Set[str] = set()

        for commit_row in commit_rows:
            repo_id = commit_row.get("repo_id")
            commit_hash = commit_row.get("hash")
            message = commit_row.get("message") or ""
            author_when = commit_row.get("author_when")

            if not message or not commit_hash:
                continue

            repo_id_str = str(repo_id)
            repo_uuid = uuid.UUID(repo_id_str)
            commit_id = generate_commit_id(repo_uuid, str(commit_hash))

            event_ts = author_when
            if isinstance(event_ts, str):
                try:
                    event_ts = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
                except ValueError:
                    event_ts = self._now
            if event_ts and event_ts.tzinfo is None:
                event_ts = event_ts.replace(tzinfo=timezone.utc)
            if not event_ts:
                event_ts = self._now

            jira_refs = extract_jira_keys(message)
            jira_refs_found += len(jira_refs)
            for ref in jira_refs:
                work_item_id = jira_key_lookup.get(ref.issue_key.upper())
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    edge_id = generate_edge_id(
                        NodeType.COMMIT,
                        commit_id,
                        edge_type,
                        NodeType.ISSUE,
                        work_item_id,
                    )
                    if edge_id in seen_edges:
                        continue
                    seen_edges.add(edge_id)

                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.COMMIT,
                            source_id=commit_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=edge_type,
                            repo_id=repo_uuid,
                            provider="jira",
                            provenance=Provenance.EXPLICIT_TEXT,
                            confidence=0.85,
                            evidence=ref.raw_match,
                            discovered_at=self._now,
                            last_synced=self._now,
                            event_ts=event_ts,
                        )
                    )

            gh_refs = extract_github_issue_refs(message)
            gh_refs_found += len(gh_refs)
            for ref in gh_refs:
                work_item_id = gh_issue_lookup.get((repo_id_str, ref.issue_key))
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    edge_id = generate_edge_id(
                        NodeType.COMMIT,
                        commit_id,
                        edge_type,
                        NodeType.ISSUE,
                        work_item_id,
                    )
                    if edge_id in seen_edges:
                        continue
                    seen_edges.add(edge_id)

                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.COMMIT,
                            source_id=commit_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=edge_type,
                            repo_id=repo_uuid,
                            provider="github",
                            provenance=Provenance.EXPLICIT_TEXT,
                            confidence=0.85,
                            evidence=ref.raw_match,
                            discovered_at=self._now,
                            last_synced=self._now,
                            event_ts=event_ts,
                        )
                    )

            gl_refs = extract_gitlab_issue_refs(message)
            gl_refs_found += len(gl_refs)
            for ref in gl_refs:
                work_item_id = gl_issue_lookup.get((repo_id_str, ref.issue_key))
                if work_item_id:
                    edge_type = (
                        EdgeType.IMPLEMENTS
                        if ref.ref_type == RefType.CLOSES
                        else EdgeType.REFERENCES
                    )
                    edge_id = generate_edge_id(
                        NodeType.COMMIT,
                        commit_id,
                        edge_type,
                        NodeType.ISSUE,
                        work_item_id,
                    )
                    if edge_id in seen_edges:
                        continue
                    seen_edges.add(edge_id)

                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.COMMIT,
                            source_id=commit_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=edge_type,
                            repo_id=repo_uuid,
                            provider="gitlab",
                            provenance=Provenance.EXPLICIT_TEXT,
                            confidence=0.85,
                            evidence=ref.raw_match,
                            discovered_at=self._now,
                            last_synced=self._now,
                            event_ts=event_ts,
                        )
                    )

        edge_count = self._write_edges(edges)
        logger.info(
            "Commit message refs: jira=%d, github=%d, gitlab=%d",
            jira_refs_found,
            gh_refs_found,
            gl_refs_found,
        )
        logger.info("Created %d issue->commit edges from commit messages", edge_count)
        return edge_count

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
        FROM work_items
        WHERE repo_id IS NOT NULL
        """
        if self.config.from_date:
            wi_query += f" AND updated_at >= '{_format_datetime_for_clickhouse(self.config.from_date)}'"
        if self.config.to_date:
            wi_query += f" AND updated_at <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
        if self.config.repo_id:
            wi_query += f" AND repo_id = '{self.config.repo_id}'"

        wi_rows = self.sink.query_dicts(wi_query, {})

        if not wi_rows:
            return 0

        # Query PRs with timestamps
        pr_query = """
        SELECT
            repo_id,
            number,
            created_at
        FROM git_pull_requests
        """
        where_clauses = []
        if self.config.from_date:
            where_clauses.append(
                f"created_at >= '{_format_datetime_for_clickhouse(self.config.from_date)}'"
            )
        if self.config.to_date:
            where_clauses.append(
                f"created_at <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
            )
        if self.config.repo_id:
            where_clauses.append(f"repo_id = '{self.config.repo_id}'")

        if where_clauses:
            pr_query += " WHERE " + " AND ".join(where_clauses)

        pr_rows = self.sink.query_dicts(pr_query, {})

        if not pr_rows:
            return 0

        # Group PRs by repo
        prs_by_repo: Dict[str, List[Tuple[int, datetime]]] = {}
        for row in pr_rows:
            repo_id = row.get("repo_id")
            pr_number = row.get("number")
            created_at = row.get("created_at")

            if repo_id is None or pr_number is None or created_at is None:
                continue

            repo_key = str(repo_id)
            # Ensure created_at is datetime
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

            prs_by_repo.setdefault(repo_key, []).append((int(pr_number), created_at))

        window = timedelta(days=self.config.heuristic_days_window)
        edges: List[WorkGraphEdge] = []
        fast_path_links: List[WorkGraphIssuePR] = []

        linked_work_items = {work_item_id for work_item_id, _ in explicit_links}

        for wi_row in wi_rows:
            repo_id = wi_row.get("repo_id")
            work_item_id = str(wi_row.get("work_item_id"))
            updated_at = wi_row.get("updated_at")

            repo_key = str(repo_id)
            if repo_key not in prs_by_repo:
                continue

            if work_item_id in linked_work_items:
                continue

            if isinstance(updated_at, str):
                updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))

            best: Optional[Tuple[int, datetime, float]] = None
            for pr_number, pr_created_at in prs_by_repo[repo_key]:
                if (work_item_id, pr_number) in explicit_links:
                    continue
                if not updated_at or not pr_created_at:
                    continue
                time_diff = abs((pr_created_at - updated_at).total_seconds())
                if time_diff > window.total_seconds():
                    continue
                if best is None or time_diff < best[2]:
                    best = (pr_number, pr_created_at, time_diff)

            if best is None:
                continue

            pr_number = best[0]
            pr_created_at = best[1]

            # Use max(updated_at, pr_created_at) as event time
            event_ts = pr_created_at
            if updated_at and updated_at > event_ts:
                event_ts = updated_at
            # Ensure timezone
            if event_ts and event_ts.tzinfo is None:
                event_ts = event_ts.replace(tzinfo=timezone.utc)

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
                    event_ts=event_ts,
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

    def _build_issue_pr_edges_from_fast_path(self) -> Tuple[Set[Tuple[str, int]], int]:
        logger.info(
            "Building issue->PR edges from dev_health_ops.work_graph_issue_pr..."
        )

        query = """
        SELECT
            p.repo_id,
            p.work_item_id,
            p.pr_number,
            p.confidence,
            p.provenance,
            p.evidence,
            p.last_synced,
            pr.created_at
        FROM work_graph_issue_pr AS p
        INNER JOIN git_pull_requests AS pr ON (toString(p.repo_id) = toString(pr.repo_id) AND p.pr_number = pr.number)
        """
        where_parts: List[str] = []
        if self.config.repo_id:
            where_parts.append(f"p.repo_id = '{self.config.repo_id}'")
        if self.config.from_date:
            where_parts.append(
                f"pr.created_at >= '{_format_datetime_for_clickhouse(self.config.from_date)}'"
            )
        if self.config.to_date:
            where_parts.append(
                f"pr.created_at <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
            )
        if where_parts:
            query += " WHERE " + " AND ".join(where_parts)

        rows = self.sink.query_dicts(query, {})
        logger.info("Found %d rows in work_graph_issue_pr", len(rows))
        if not rows:
            return set(), 0

        edges: List[WorkGraphEdge] = []
        links: Set[Tuple[str, int]] = set()
        for row in rows:
            repo_id = row.get("repo_id")
            work_item_id = str(row.get("work_item_id"))
            pr_number = int(row.get("pr_number") or 0)
            confidence = float(row.get("confidence") or 1.0)
            provenance = row.get("provenance")
            evidence = row.get("evidence")
            created_at = row.get("created_at")

            repo_uuid = uuid.UUID(str(repo_id))
            pr_id = generate_pr_id(repo_uuid, pr_number)
            edge_id = generate_edge_id(
                NodeType.PR,
                pr_id,
                EdgeType.IMPLEMENTS,
                NodeType.ISSUE,
                work_item_id,
            )

            # Ensure timezone
            event_ts = created_at
            if isinstance(event_ts, str):
                event_ts = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
            if event_ts and event_ts.tzinfo is None:
                event_ts = event_ts.replace(tzinfo=timezone.utc)
            if not event_ts:
                event_ts = self._now

            edges.append(
                WorkGraphEdge(
                    edge_id=edge_id,
                    source_type=NodeType.PR,
                    source_id=pr_id,
                    target_type=NodeType.ISSUE,
                    target_id=work_item_id,
                    edge_type=EdgeType.IMPLEMENTS,
                    repo_id=repo_uuid,
                    provenance=self._parse_provenance(str(provenance)),
                    confidence=confidence,
                    evidence=str(evidence or "issue_pr_fast_path"),
                    discovered_at=self._now,
                    last_synced=self._now,
                    event_ts=event_ts,
                )
            )
            links.add((work_item_id, pr_number))

        count = self._write_edges(edges)
        logger.info("Created %d issue->PR edges from fast-path table", count)
        return links, count

    def _build_pr_commit_edges_from_fast_path(self) -> int:
        logger.info(
            "Building PR->commit edges from dev_health_ops.work_graph_pr_commit..."
        )

        query = """
        SELECT
            p.repo_id,
            p.pr_number,
            p.commit_hash,
            p.confidence,
            p.provenance,
            p.evidence,
            p.last_synced,
            c.author_when
        FROM work_graph_pr_commit AS p
        INNER JOIN git_commits AS c ON (toString(p.repo_id) = toString(c.repo_id) AND p.commit_hash = c.hash)
        """
        where_parts: List[str] = []
        if self.config.repo_id:
            where_parts.append(f"p.repo_id = '{self.config.repo_id}'")
        if self.config.from_date:
            where_parts.append(
                f"c.author_when >= '{_format_datetime_for_clickhouse(self.config.from_date)}'"
            )
        if self.config.to_date:
            where_parts.append(
                f"c.author_when <= '{_format_datetime_for_clickhouse(self.config.to_date)}'"
            )
        if where_parts:
            query += " WHERE " + " AND ".join(where_parts)

        rows = self.sink.query_dicts(query, {})
        logger.info("Found %d rows in work_graph_pr_commit", len(rows))
        if not rows:
            return 0

        edges: List[WorkGraphEdge] = []
        for row in rows:
            repo_id = row.get("repo_id")
            pr_number = int(row.get("pr_number") or 0)
            commit_hash = str(row.get("commit_hash"))
            confidence = float(row.get("confidence") or 1.0)
            provenance = row.get("provenance")
            evidence = row.get("evidence")
            author_when = row.get("author_when")

            repo_uuid = uuid.UUID(str(repo_id))
            pr_id = generate_pr_id(repo_uuid, pr_number)
            commit_id = generate_commit_id(repo_uuid, commit_hash)
            edge_id = generate_edge_id(
                NodeType.PR,
                pr_id,
                EdgeType.CONTAINS,
                NodeType.COMMIT,
                commit_id,
            )

            # Ensure timezone
            event_ts = author_when
            if isinstance(event_ts, str):
                event_ts = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
            if event_ts and event_ts.tzinfo is None:
                event_ts = event_ts.replace(tzinfo=timezone.utc)
            if not event_ts:
                event_ts = self._now

            edges.append(
                WorkGraphEdge(
                    edge_id=edge_id,
                    source_type=NodeType.PR,
                    source_id=pr_id,
                    target_type=NodeType.COMMIT,
                    target_id=commit_id,
                    edge_type=EdgeType.CONTAINS,
                    repo_id=repo_uuid,
                    provenance=self._parse_provenance(str(provenance)),
                    confidence=confidence,
                    evidence=str(evidence or "pr_commit_fast_path"),
                    discovered_at=self._now,
                    last_synced=self._now,
                    event_ts=event_ts,
                )
            )

        count = self._write_edges(edges)
        logger.info("Created %d PR->commit edges from fast-path table", count)
        return count

    def _count_commit_file_edges(self) -> int:
        """Count commit->file edges."""
        # View work_graph_commit_file is specific to ClickHouse.
        # For others, we count git_commit_stats rows.
        query = "SELECT count(*) AS total FROM git_commit_stats"
        try:
            rows = self.sink.query_dicts(query, {})
            count = rows[0].get("total") if rows else 0
            logger.info("Found %d commit->file edges", count)
            return int(count or 0)
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
