"""
Work Graph Builder - orchestrates work graph construction.

This module provides the main entry point for building the work graph
from raw data sources (work items, PRs, commits).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from dev_health_ops.metrics.schemas import (
    WorkGraphEdgeRecord,
)
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.work_graph.extractors.text_parser import (
    RefType,
    extract_github_issue_refs,
    extract_gitlab_issue_refs,
    extract_jira_keys,
)
from dev_health_ops.work_graph.ids import (
    generate_commit_id,
    generate_edge_id,
    generate_feature_flag_id,
    generate_release_id,
)
from dev_health_ops.work_graph.models import (
    EdgeType,
    NodeType,
    Provenance,
    WorkGraphEdge,
)

logger = logging.getLogger(__name__)


def _format_datetime_for_clickhouse(dt: datetime) -> str:
    """Format datetime for ClickHouse SQL queries."""
    # ClickHouse expects 'YYYY-MM-DD HH:MM:SS' format without timezone suffix
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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

    def _write_edges(self, edges: list[WorkGraphEdge]) -> int:
        """Write edges via the sink."""
        if not edges:
            return 0
        records = [self._edge_to_record(e) for e in edges]
        self.sink.write_work_graph_edges(records)
        return len(records)

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
            "operational_incident_edges": 0,
        }

        logger.info("Starting work graph build...")

        self._delete_stale_pr_dependency_issue_edges()

        # 1. Issue->issue edges from work_item_dependencies: CHAOS-4924 ported
        # to Go (internal/jobs/workgraph/edges), wired as a native pre-step
        # ahead of this bridge call, not here -- deleted the Python
        # `_build_issue_issue_edges`/`_delete_dependency_edge_candidates`/
        # `_publish_blocker_projection` trio. stats stays at its 0 default
        # (see the dict literal above) -- the native pre-step reports its own
        # counts through the ledger, same as issue_pr_links (CHAOS-5249) and
        # pr_commit_links/pr_commit_edges (CHAOS-5264).

        # issue->PR native-provenance links (work_item_dependencies ->
        # work_graph_issue_pr) are derived by the Go pre-step
        # (internal/jobs/workgraph/issueprlinks) before this bridge call runs,
        # not here -- CHAOS-5249 deleted _derive_issue_pr_links_from_dependencies,
        # retiring the Python half of that straddle (it ran a second time,
        # every build, after the Go pre-step already wrote the same rows).

        # 2/3/4. Issue->PR edges from the fast-path table, from PR
        # title/body text parsing, and the heuristic time-window matcher:
        # CHAOS-4924 ported all three to Go
        # (internal/jobs/workgraph/issuepredges), wired as native pre-steps
        # ahead of this bridge call, not here. stats stays at its 0 default
        # (see the dict literal above) -- the native pre-steps report their
        # own counts through the ledger, same as issue_pr_links (CHAOS-5249)
        # and pr_commit_links/pr_commit_edges (CHAOS-5264). The heuristic
        # step reads its own "already explicitly linked" exclusion set fresh
        # from work_graph_issue_pr (by the time it runs, the native
        # issue_pr_links pre-step AND the two issuepredges pre-steps ahead of
        # it have already committed their rows there) -- see
        # issuepredges.ExplicitLink's doc comment.

        # 3b. Build issue->commit edges from commit message parsing
        stats["issue_commit_edges"] = self._build_issue_commit_edges_from_text_parsing()

        # 4b/5. PR->commit link derivation and fast-path edges: CHAOS-5264
        # ported both to Go (internal/jobs/workgraph/prcommit), wired as
        # native pre-steps ahead of this bridge call, not here. stats stays at
        # its 0 default (line 441) -- the native pre-steps report their own
        # counts through the ledger, same as issue_pr_links (CHAOS-5249).

        # 6. Commit->file edges are handled by view over git_commit_stats
        stats["commit_file_edges"] = self._count_commit_file_edges()

        # 7/8. Feature-flag GUARDS edges and operational-incident edges:
        # CHAOS-4924 ported both to Go (internal/jobs/workgraph/operationaledges),
        # wired as native pre-steps ahead of this bridge call, not here. stats
        # stay at their 0 default (see the dict literal above) -- the native
        # pre-steps report their own counts through the ledger, same as
        # issue_pr_links (CHAOS-5249) and pr_commit_links/pr_commit_edges
        # (CHAOS-5264).

        logger.info(
            "Work graph build complete: %s",
            ", ".join(f"{k}={v}" for k, v in stats.items()),
        )

        return stats

    def _delete_stale_pr_dependency_issue_edges(self) -> None:
        if not self.config.org_id:
            return
        command = getattr(getattr(self.sink, "client", None), "command", None)
        if not callable(command):
            return

        where_parts = [
            "source_type = 'issue'",
            "target_type = 'issue'",
            "evidence = 'linear_attachment'",
            "startsWith(target_id, 'linear:')",
            "(startsWith(source_id, 'ghpr:') OR startsWith(source_id, 'gitlab:'))",
        ]
        params: dict[str, str] = {}
        if self.config.org_id:
            where_parts.append("org_id = {org_id:String}")
            params["org_id"] = self.config.org_id

        command(
            "ALTER TABLE work_graph_edges DELETE WHERE "
            + " AND ".join(where_parts)
            + " SETTINGS mutations_sync=2",
            parameters=params or None,
        )

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
        FROM work_items FINAL
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


# CHAOS-5303 r1 P2: this module used to end with its own standalone
# `main()`/`argparse` CLI (`python -m work_graph.builder ...`), a SECOND,
# undocumented legacy entry point distinct from the tracked, documented one
# (`work_graph/runner.py`'s `run_work_graph_build`, wired to `dev-hops
# work-graph build` -- see docs/operate/runbooks/operator-commands.md and
# docs/go-migration-matrix.md, both of which already list it as legacy and
# scheduled for retirement, CHAOS-4441). This one was reachable ONLY via a
# direct `python -m` invocation, appeared in no `dev-hops` dispatch table and
# no operator runbook, and (found by codex review, not by design) received no
# Go pre-step coverage at all -- an org that ran the work graph exclusively
# through this path got zero issue<->issue edges post-CHAOS-4924 with no
# error, since `builder.build()`'s own stats dict silently reports 0 for a
# retired stage. Deleted outright rather than ported: it duplicated
# `runner.py`'s CLI with a strictly smaller flag set (no `--org` blank-scope
# guard) and no live caller could be found for it beyond its own README
# examples (README.md updated in the same commit).
