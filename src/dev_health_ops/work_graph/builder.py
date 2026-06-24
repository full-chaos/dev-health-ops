"""
Work Graph Builder - orchestrates work graph construction.

This module provides the main entry point for building the work graph
from raw data sources (work items, PRs, commits).
"""

from __future__ import annotations

import argparse
import bisect
import logging
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from dev_health_ops.metrics.schemas import (
    FeatureFlagLinkRecord,
    WorkGraphEdgeRecord,
    WorkGraphIssuePRRecord,
    WorkGraphPRCommitRecord,
)
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.work_graph.extractors.text_parser import (
    RefType,
    extract_flag_key_refs,
    extract_github_issue_refs,
    extract_gitlab_issue_refs,
    extract_jira_keys,
    extract_pr_refs,
    extract_squash_pr_refs,
)
from dev_health_ops.work_graph.ids import (
    generate_commit_id,
    generate_edge_id,
    generate_feature_flag_id,
    generate_pr_id,
    generate_release_id,
)
from dev_health_ops.work_graph.models import (
    EdgeType,
    NodeType,
    Provenance,
    WorkGraphEdge,
    WorkGraphIssuePR,
    WorkGraphPRCommit,
)

logger = logging.getLogger(__name__)


def _format_datetime_for_clickhouse(dt: datetime) -> str:
    """Format datetime for ClickHouse SQL queries."""
    # ClickHouse expects 'YYYY-MM-DD HH:MM:SS' format without timezone suffix
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# Mapping from work_item_dependencies relationship types to EdgeType
DEPENDENCY_TYPE_MAP: dict[str, EdgeType] = {
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

# CHAOS-2630 Phase C1: confidence ceiling for flag associations inferred from a
# flag key literally appearing in PR/issue/commit text. Kept well below the 0.9
# used for structured PROJECT-123 issue refs (flag keys are noisier free-form
# strings) and strictly below NATIVE, per the design sign-off.
FLAG_TEXT_REF_CONFIDENCE = 0.6


@dataclass
class BuildConfig:
    """Configuration for work graph build."""

    dsn: str
    from_date: datetime | None = None
    to_date: datetime | None = None
    repo_id: uuid.UUID | None = None
    heuristic_days_window: int = 7
    heuristic_confidence: float = 0.3
    org_id: str = ""


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

    def _org_id_clause(self, *, alias: str = "") -> str:
        if not self.config.org_id:
            return ""
        qualifier = f"{alias}." if alias else ""
        return f"AND {qualifier}org_id = '{self.config.org_id}'"

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
            org_id=self.config.org_id,
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
            org_id=self.config.org_id,
        )

    def _pr_commit_to_record(self, link: WorkGraphPRCommit) -> WorkGraphPRCommitRecord:
        """Convert WorkGraphPRCommit to WorkGraphPRCommitRecord for sink."""
        return WorkGraphPRCommitRecord(
            repo_id=link.repo_id,
            pr_number=link.pr_number,
            commit_hash=link.commit_hash,
            confidence=link.confidence,
            provenance=link.provenance.value,
            evidence=link.evidence,
            last_synced=link.last_synced or self._now,
            org_id=self.config.org_id,
        )

    def _write_edges(self, edges: list[WorkGraphEdge]) -> int:
        """Write edges via the sink."""
        if not edges:
            return 0
        records = [self._edge_to_record(e) for e in edges]
        self.sink.write_work_graph_edges(records)
        return len(records)

    def _write_issue_pr_links(self, links: list[WorkGraphIssuePR]) -> None:
        """Write issue-PR links via the sink."""
        if not links:
            return
        records = [self._issue_pr_to_record(lnk) for lnk in links]
        self.sink.write_work_graph_issue_pr(records)

    def _write_pr_commit_links(self, links: list[WorkGraphPRCommit]) -> None:
        """Write PR-commit links via the sink."""
        if not links:
            return
        records = [self._pr_commit_to_record(lnk) for lnk in links]
        self.sink.write_work_graph_pr_commit(records)

    @staticmethod
    def _parse_provenance(value: str | None) -> Provenance:
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

    def add_release_node(
        self,
        release_ref: str,
        environment: str,
        *,
        provider: str | None = None,
        repo_id: uuid.UUID | None = None,
        event_ts: datetime | None = None,
    ) -> WorkGraphEdge:
        """Create a RELEASE node placeholder edge (self-referencing identity edge).

        Returns the identity edge so callers can chain ``add_release_edge``.
        """
        release_id = generate_release_id(self.config.org_id, release_ref)
        edge_id = generate_edge_id(
            NodeType.RELEASE,
            release_id,
            EdgeType.RELATES,
            NodeType.RELEASE,
            release_id,
        )
        edge = WorkGraphEdge(
            edge_id=edge_id,
            source_type=NodeType.RELEASE,
            source_id=release_id,
            target_type=NodeType.RELEASE,
            target_id=release_id,
            edge_type=EdgeType.RELATES,
            provenance=Provenance.NATIVE,
            confidence=1.0,
            evidence=f"release:{release_ref}@{environment}",
            repo_id=repo_id or self.config.repo_id,
            provider=provider,
            event_ts=event_ts or self._now,
        )
        self._write_edges([edge])
        return edge

    def add_feature_flag_node(
        self,
        flag_key: str,
        provider: str,
        project_key: str,
        *,
        repo_id: uuid.UUID | None = None,
        event_ts: datetime | None = None,
    ) -> WorkGraphEdge:
        """Create a FEATURE_FLAG node placeholder edge (self-referencing identity edge)."""
        flag_id = generate_feature_flag_id(
            self.config.org_id, provider, project_key, flag_key
        )
        edge_id = generate_edge_id(
            NodeType.FEATURE_FLAG,
            flag_id,
            EdgeType.RELATES,
            NodeType.FEATURE_FLAG,
            flag_id,
        )
        edge = WorkGraphEdge(
            edge_id=edge_id,
            source_type=NodeType.FEATURE_FLAG,
            source_id=flag_id,
            target_type=NodeType.FEATURE_FLAG,
            target_id=flag_id,
            edge_type=EdgeType.RELATES,
            provenance=Provenance.NATIVE,
            confidence=1.0,
            evidence=f"flag:{provider}/{project_key}/{flag_key}",
            repo_id=repo_id or self.config.repo_id,
            provider=provider,
            event_ts=event_ts or self._now,
        )
        self._write_edges([edge])
        return edge

    def add_release_edge(
        self,
        release_id: str,
        target_id: str,
        edge_type: EdgeType,
        confidence: float,
        *,
        target_type: NodeType = NodeType.PR,
        evidence: str = "",
        provenance: Provenance = Provenance.NATIVE,
        repo_id: uuid.UUID | None = None,
        event_ts: datetime | None = None,
    ) -> WorkGraphEdge:
        """Create an edge from a RELEASE node to a PR (or other target)."""
        edge_id = generate_edge_id(
            NodeType.RELEASE,
            release_id,
            edge_type,
            target_type,
            target_id,
        )
        edge = WorkGraphEdge(
            edge_id=edge_id,
            source_type=NodeType.RELEASE,
            source_id=release_id,
            target_type=target_type,
            target_id=target_id,
            edge_type=edge_type,
            provenance=provenance,
            confidence=confidence,
            evidence=evidence,
            repo_id=repo_id or self.config.repo_id,
            event_ts=event_ts or self._now,
        )
        self._write_edges([edge])
        return edge

    def add_feature_flag_edge(
        self,
        flag_id: str,
        target_type: NodeType,
        target_id: str,
        edge_type: EdgeType,
        confidence: float,
        *,
        evidence: str = "",
        provenance: Provenance = Provenance.NATIVE,
        repo_id: uuid.UUID | None = None,
        provider: str | None = None,
        event_ts: datetime | None = None,
    ) -> WorkGraphEdge:
        """Create an edge from a FEATURE_FLAG node to another graph node."""
        edge_id = generate_edge_id(
            NodeType.FEATURE_FLAG,
            flag_id,
            edge_type,
            target_type,
            target_id,
        )
        edge = WorkGraphEdge(
            edge_id=edge_id,
            source_type=NodeType.FEATURE_FLAG,
            source_id=flag_id,
            target_type=target_type,
            target_id=target_id,
            edge_type=edge_type,
            provenance=provenance,
            confidence=confidence,
            evidence=evidence,
            repo_id=repo_id or self.config.repo_id,
            provider=provider,
            event_ts=event_ts or self._now,
        )
        self._write_edges([edge])
        return edge

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
            "flag_guards_edges": 0,
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

        # 4b. Derive PR->commit links from commit messages (fills fast path).
        # Without this, work_graph_pr_commit is only ever written by fixtures, so
        # real orgs see no commits under PRs in the /work GraphView.
        self._derive_pr_commit_links()

        # 5. Build PR->commit edges from fast-path table (prerequisite)
        stats["pr_commit_edges"] = self._build_pr_commit_edges_from_fast_path()

        # 6. Commit->file edges are handled by view over git_commit_stats
        stats["commit_file_edges"] = self._count_commit_file_edges()

        # 7. Build feature-flag GUARDS edges (flag -> issue) from real flag-key
        #    references in issue text. CHAOS-2630 Phase C1: the only non-fixture
        #    source of flag associations; registry-validated + confidence-gated.
        stats["flag_guards_edges"] = self._build_flag_guards_edges()

        logger.info(
            "Work graph build complete: %s",
            ", ".join(f"{k}={v}" for k, v in stats.items()),
        )

        return stats

    def _build_flag_guards_edges(self) -> int:
        """Build GUARDS edges (feature_flag -> issue) from real flag-key text refs.

        CHAOS-2630 Phase C1 -- the only non-fixture source of feature-flag
        associations. A flag key that literally appears in an issue's title or
        description is an evidence-backed signal that the issue is guarded by
        that flag. Matching is **registry-validated** (only keys present in the
        ``feature_flag`` table for the org can match) and emitted as
        ``EXPLICIT_TEXT`` with a confidence ceiling strictly below ``NATIVE``;
        an unknown flag key never produces an edge or a link.
        """
        logger.info("Building feature-flag GUARDS edges from issue text references...")

        # 1. Load the org's real flag registry (env-agnostic identity).
        flag_query = "SELECT flag_key, provider, project_key FROM feature_flag FINAL"
        if self.config.org_id:
            flag_query += f" WHERE org_id = '{self.config.org_id}'"
        flag_rows = self.sink.query_dicts(flag_query, {})
        if not flag_rows:
            logger.info("No feature flags in registry; skipping GUARDS edges")
            return 0

        # flag_key -> list of (provider, project_key, flag_id). A key can in
        # principle exist under more than one provider/project; emit for each.
        flag_identities: dict[str, list[tuple[str, str, str]]] = {}
        for row in flag_rows:
            flag_key = str(row.get("flag_key") or "")
            if not flag_key:
                continue
            provider = str(row.get("provider") or "")
            project_key = str(row.get("project_key") or "")
            flag_id = generate_feature_flag_id(
                self.config.org_id, provider, project_key, flag_key
            )
            flag_identities.setdefault(flag_key, []).append(
                (provider, project_key, flag_id)
            )

        known_keys = list(flag_identities.keys())

        # 2. Load issue text (work_items title + description).
        wi_query = "SELECT work_item_id, title, description FROM work_items"
        if self.config.org_id:
            wi_query += f" WHERE org_id = '{self.config.org_id}'"
        wi_rows = self.sink.query_dicts(wi_query, {})
        if not wi_rows:
            return 0

        edges: list[WorkGraphEdge] = []
        links: list[FeatureFlagLinkRecord] = []
        seen_edges: set[str] = set()
        now = self._now

        for wi_row in wi_rows:
            work_item_id = str(wi_row.get("work_item_id") or "")
            if not work_item_id:
                continue
            text = " ".join(
                str(wi_row.get(col) or "") for col in ("title", "description")
            ).strip()
            if not text:
                continue
            for ref in extract_flag_key_refs(text, known_keys):
                for provider, _project_key, flag_id in flag_identities[ref.flag_key]:
                    edge_id = generate_edge_id(
                        NodeType.FEATURE_FLAG,
                        flag_id,
                        EdgeType.GUARDS,
                        NodeType.ISSUE,
                        work_item_id,
                    )
                    if edge_id in seen_edges:
                        continue
                    seen_edges.add(edge_id)
                    edges.append(
                        WorkGraphEdge(
                            edge_id=edge_id,
                            source_type=NodeType.FEATURE_FLAG,
                            source_id=flag_id,
                            target_type=NodeType.ISSUE,
                            target_id=work_item_id,
                            edge_type=EdgeType.GUARDS,
                            provenance=Provenance.EXPLICIT_TEXT,
                            confidence=FLAG_TEXT_REF_CONFIDENCE,
                            evidence=f"flagref:{ref.raw_match}",
                            provider=provider or None,
                            event_ts=now,
                        )
                    )
                    links.append(
                        FeatureFlagLinkRecord(
                            flag_key=ref.flag_key,
                            target_type="issue",
                            target_id=work_item_id,
                            provider=provider,
                            link_source="explicit_text",
                            link_type="tracks",
                            evidence_type="issue_text",
                            confidence=FLAG_TEXT_REF_CONFIDENCE,
                            valid_from=now,
                            valid_to=None,
                            last_synced=now,
                            org_id=self.config.org_id,
                        )
                    )

        if edges:
            self._write_edges(edges)
        if links:
            self.sink.write_feature_flag_links(links)
        logger.info(
            "Created %d feature-flag GUARDS edges (%d links) from text references",
            len(edges),
            len(links),
        )
        return len(edges)

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
        org_id_clause = self._org_id_clause()
        if org_id_clause:
            query += f" WHERE 1=1 {org_id_clause}"

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

    def _build_issue_pr_edges(self) -> tuple[set[tuple[str, int]], int]:
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
        if self.config.org_id:
            where_clauses.append(f"org_id = '{self.config.org_id}'")

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
        if self.config.org_id:
            wi_query += f" WHERE org_id = '{self.config.org_id}'"
        wi_rows = self.sink.query_dicts(wi_query, {})
        logger.info("Found %d work items for lookup", len(wi_rows))

        # Build work item lookups
        # For Jira: key -> work_item_id
        jira_key_lookup: dict[str, str] = {}
        # For GitHub/GitLab: (repo_id, issue_number) -> work_item_id
        gh_issue_lookup: dict[tuple[str, str], str] = {}
        gl_issue_lookup: dict[tuple[str, str], str] = {}

        # Providers not covered by PR text parsing (notably Linear): their
        # issue<->PR links arrive as native attachments and become edges via
        # the work_item_dependencies pass (_build_issue_issue_edges), not here.
        # Counted so this log does not imply they were silently dropped.
        non_text_path_counts: dict[str, int] = {}

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
            elif provider and provider not in ("jira", "github", "gitlab"):
                non_text_path_counts[str(provider)] = (
                    non_text_path_counts.get(str(provider), 0) + 1
                )

        logger.info(
            "Built text-parse lookups: jira=%d, github=%d, gitlab=%d; "
            "non-text-path providers (edges via dependency pass): %s",
            len(jira_key_lookup),
            len(gh_issue_lookup),
            len(gl_issue_lookup),
            non_text_path_counts or "none",
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
        edges: list[WorkGraphEdge] = []
        fast_path_links: list[WorkGraphIssuePR] = []
        explicit_links: set[tuple[str, int]] = set()
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
        if self.config.org_id:
            where_clauses.append(f"org_id = '{self.config.org_id}'")

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
        if self.config.org_id:
            wi_query += f" WHERE org_id = '{self.config.org_id}'"
        wi_rows = self.sink.query_dicts(wi_query, {})

        jira_key_lookup: dict[str, str] = {}
        gh_issue_lookup: dict[tuple[str, str], str] = {}
        gl_issue_lookup: dict[tuple[str, str], str] = {}

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

        edges: list[WorkGraphEdge] = []
        jira_refs_found = 0
        gh_refs_found = 0
        gl_refs_found = 0
        seen_edges: set[str] = set()

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
        self, explicit_links: set[tuple[str, int]]
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
        org_id_clause = self._org_id_clause()
        if org_id_clause:
            wi_query += f" {org_id_clause}"
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
        if self.config.org_id:
            where_clauses.append(f"org_id = '{self.config.org_id}'")

        if where_clauses:
            pr_query += " WHERE " + " AND ".join(where_clauses)

        pr_rows = self.sink.query_dicts(pr_query, {})

        if not pr_rows:
            return 0

        # Group PRs by repo with sorted timestamps for O(log n) binary search
        # Data structure: {repo_key: (sorted_timestamps, [(pr_number, created_at), ...])}
        prs_by_repo: dict[str, tuple[list[float], list[tuple[int, datetime]]]] = {}
        for row in pr_rows:
            repo_id = row.get("repo_id")
            pr_number = row.get("number")
            created_at = row.get("created_at")

            if repo_id is None or pr_number is None or created_at is None:
                continue

            repo_key = str(repo_id)
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

            if repo_key not in prs_by_repo:
                prs_by_repo[repo_key] = ([], [])
            prs_by_repo[repo_key][1].append((int(pr_number), created_at))

        # Sort PRs by created_at and build timestamp index for binary search
        for repo_key, (timestamps, prs_list) in prs_by_repo.items():
            prs_list.sort(key=lambda x: x[1].timestamp() if x[1] else 0)
            timestamps.clear()
            timestamps.extend(pr[1].timestamp() if pr[1] else 0 for pr in prs_list)

        window_seconds = timedelta(
            days=self.config.heuristic_days_window
        ).total_seconds()
        edges: list[WorkGraphEdge] = []
        fast_path_links: list[WorkGraphIssuePR] = []

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

            if not updated_at:
                continue

            # Binary search to find PRs within time window: O(log n) instead of O(n)
            timestamps, prs_list = prs_by_repo[repo_key]
            updated_ts = updated_at.timestamp()
            left_idx = bisect.bisect_left(timestamps, updated_ts - window_seconds)
            right_idx = bisect.bisect_right(timestamps, updated_ts + window_seconds)

            best: tuple[int, datetime, float] | None = None
            for idx in range(left_idx, right_idx):
                pr_number, pr_created_at = prs_list[idx]
                if (work_item_id, pr_number) in explicit_links:
                    continue
                if not pr_created_at:
                    continue
                time_diff = abs((pr_created_at - updated_at).total_seconds())
                if best is None or time_diff < best[2]:
                    best = (pr_number, pr_created_at, time_diff)

            if best is None:
                continue

            pr_number = best[0]
            pr_created_at = best[1]

            event_ts = max(updated_at, pr_created_at) if updated_at else pr_created_at
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

    def _build_issue_pr_edges_from_fast_path(self) -> tuple[set[tuple[str, int]], int]:
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
        where_parts: list[str] = []
        if self.config.repo_id:
            where_parts.append(f"p.repo_id = '{self.config.repo_id}'")
        if self.config.org_id:
            where_parts.append(f"p.org_id = '{self.config.org_id}'")
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

        edges: list[WorkGraphEdge] = []
        links: set[tuple[str, int]] = set()
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

    def _derive_pr_commit_links(self) -> int:
        """Derive PR->commit fast-path links from already-synced git tables.

        Mirrors the issue<->PR self-fill: the live OAuth-provider sync writes raw
        ``git_pull_requests`` and ``git_commits`` but never populates
        ``work_graph_pr_commit`` (only fixtures did), so the ``CONTAINS`` edges
        built by :meth:`_build_pr_commit_edges_from_fast_path` were empty for real
        orgs. Here we parse commit messages for PR/MR numbers via two tiers:

        1. **Explicit merge keywords** (``Merge pull request #N``,
           ``See merge request grp/proj!N``) -- unambiguous, persisted with
           ``provenance=explicit_text``, ``confidence=0.9`` and
           ``evidence='commit_message_pr_ref'`` (see :func:`extract_pr_refs`).
        2. **Squash-merge subject suffix** (``<subject> (#N)``) -- GitHub's
           *squash and merge* default, which leaves no explicit merge keyword.
           This form is ambiguous with a hand-authored issue reference, so it is
           only promoted when ``N`` matches a *known* PR number in the **same
           (org, repo)**, and is then tagged distinctly:
           ``provenance=heuristic``, ``confidence=0.6`` and
           ``evidence='commit_message_squash_pr_ref'`` so downstream consumers
           can weight or filter it. Without this tier, squash-merge orgs lose
           nearly all PR->commit edges (CHAOS-2435: live org a78c1a6a had only
           22 explicit-merge edges while ~3218 squash commits were discarded).

        Bare ``#N`` references are never accepted (indistinguishable from issue
        mentions). Re-running is idempotent: ``work_graph_pr_commit`` is a
        ReplacingMergeTree keyed on (org_id, repo_id, pr_number, commit_hash).

        Tenant isolation: known PRs are keyed by ``(org_id, repo_id)`` and a
        commit is only ever matched against PRs in its *own* org, so a squash
        ``(#N)`` in org A can never link to org B's PR #N even when ``repo_id``
        collides across tenants (CHAOS-2189 mirror).

        Returns:
            Number of PR->commit links written.
        """
        logger.info("Deriving PR->commit links from commit messages...")

        # Known PR numbers per (org, repo), so we only link to PRs that actually
        # exist *within the same tenant*. ``repo_id`` can collide across orgs, so
        # org_id MUST be part of the key (tenant isolation).
        pr_query = """
        SELECT
            org_id,
            repo_id,
            number
        FROM git_pull_requests
        """
        pr_where: list[str] = []
        if self.config.repo_id:
            pr_where.append(f"repo_id = '{self.config.repo_id}'")
        if self.config.org_id:
            pr_where.append(f"org_id = '{self.config.org_id}'")
        if pr_where:
            pr_query += " WHERE " + " AND ".join(pr_where)

        pr_rows = self.sink.query_dicts(pr_query, {})
        if not pr_rows:
            logger.info("No PRs found; skipping PR->commit derivation")
            return 0

        known_prs: dict[tuple[str, str], set[int]] = {}
        for pr_row in pr_rows:
            repo_id = pr_row.get("repo_id")
            number = pr_row.get("number")
            if repo_id is None or number is None:
                continue
            org_key = str(pr_row.get("org_id") or "")
            known_prs.setdefault((org_key, str(repo_id)), set()).add(int(number))

        commit_query = """
        SELECT
            org_id,
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
        if self.config.org_id:
            where_clauses.append(f"org_id = '{self.config.org_id}'")
        if where_clauses:
            commit_query += " AND " + " AND ".join(where_clauses)

        commit_rows = self.sink.query_dicts(commit_query, {})
        logger.info("Found %d commits to scan for PR refs", len(commit_rows))
        if not commit_rows:
            return 0

        # Two extraction tiers, processed in order so the higher-confidence
        # explicit-merge link wins the (org, repo, pr, hash) dedup over a squash
        # match for the same pair:
        #   1. explicit merge keywords  -> explicit_text, 0.9
        #   2. squash subject "(#N)"    -> heuristic, 0.6 (ambiguous; corroborated
        #      only against known PRs in the same (org, repo), tagged distinctly)
        link_tiers: tuple[
            tuple[Callable[[str], list[int]], float, Provenance, str], ...
        ] = (
            (extract_pr_refs, 0.9, Provenance.EXPLICIT_TEXT, "commit_message_pr_ref"),
            (
                extract_squash_pr_refs,
                0.6,
                Provenance.HEURISTIC,
                "commit_message_squash_pr_ref",
            ),
        )

        links: list[WorkGraphPRCommit] = []
        seen: set[tuple[str, str, int, str]] = set()
        for commit_row in commit_rows:
            repo_id = commit_row.get("repo_id")
            commit_hash = commit_row.get("hash")
            message = commit_row.get("message") or ""

            if not commit_hash or repo_id is None:
                continue

            repo_id_str = str(repo_id)
            commit_hash_str = str(commit_hash)
            org_key = str(commit_row.get("org_id") or "")
            # Only PRs in this commit's *own* (org, repo) are candidates --
            # never another tenant's PRs, even on a repo_id collision.
            repo_prs = known_prs.get((org_key, repo_id_str))
            if not repo_prs:
                continue

            for extractor, confidence, provenance, evidence in link_tiers:
                for pr_number in extractor(message):
                    if pr_number not in repo_prs:
                        continue
                    key = (org_key, repo_id_str, pr_number, commit_hash_str)
                    if key in seen:
                        continue
                    seen.add(key)
                    links.append(
                        WorkGraphPRCommit(
                            repo_id=uuid.UUID(repo_id_str),
                            pr_number=pr_number,
                            commit_hash=commit_hash_str,
                            confidence=confidence,
                            provenance=provenance,
                            evidence=evidence,
                            last_synced=self._now,
                        )
                    )

        self._write_pr_commit_links(links)
        logger.info("Derived %d PR->commit links from commit messages", len(links))
        return len(links)

    def _build_pr_commit_edges_from_fast_path(self) -> int:
        logger.info(
            "Building PR->commit edges from dev_health_ops.work_graph_pr_commit..."
        )

        # Tenant isolation: ``git_commits`` carries an ``org_id`` column
        # (migration 027) and ``repo_id``/``hash`` values can collide across
        # tenants (documented in metrics/loaders/ai_impact.py). The commit side
        # of this join MUST be scoped to the same org as the PR-commit row, or a
        # tenant-scoped build could satisfy the commit from another org and stamp
        # a cross-tenant edge into the current org's work_graph_edges. Matching
        # ``c.org_id = p.org_id`` keeps both sides within one tenant. The
        # ``p.org_id`` WHERE filter below then pins it to the selected org.
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
        INNER JOIN git_commits AS c ON (
            toString(p.repo_id) = toString(c.repo_id)
            AND p.commit_hash = c.hash
            AND toString(p.org_id) = toString(c.org_id)
        )
        """
        where_parts: list[str] = []
        if self.config.repo_id:
            where_parts.append(f"p.repo_id = '{self.config.repo_id}'")
        if self.config.org_id:
            where_parts.append(f"p.org_id = '{self.config.org_id}'")
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

        edges: list[WorkGraphEdge] = []
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
        org_id_clause = self._org_id_clause()
        if org_id_clause:
            query += f" WHERE 1=1 {org_id_clause}"
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
        "--org-id",
        dest="org_id",
        type=str,
        default="",
        help="Organization ID to filter by",
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
        org_id=args.org_id,
    )

    builder = WorkGraphBuilder(config)
    try:
        stats = builder.build()
        total = sum(stats.values())
        logger.info("Work graph build complete. Total edges: %d", total)
        for key, value in stats.items():
            logger.info("  %s: %s", key, value)
        return 0
    except Exception as e:
        logger.exception("Work graph build failed: %s", e)
        return 1
    finally:
        builder.close()


if __name__ == "__main__":
    sys.exit(main())
