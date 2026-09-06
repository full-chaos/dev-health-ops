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
from dev_health_ops.work_graph.ids import (
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

        # 3b. Build issue->commit edges from commit message parsing:
        # CHAOS-5304 ported to Go (internal/jobs/workgraph/issuecommitedges),
        # wired as a native pre-step ahead of this bridge call, not here. stats
        # stays at its 0 default (see the dict literal above) -- the native
        # pre-step reports its own count through the ledger, same as
        # issue_pr_links (CHAOS-5249) and pr_commit_links/pr_commit_edges
        # (CHAOS-5264).

        # 4b/5. PR->commit link derivation and fast-path edges: CHAOS-5264
        # ported both to Go (internal/jobs/workgraph/prcommit), wired as
        # native pre-steps ahead of this bridge call, not here. stats stays at
        # its 0 default (line 441) -- the native pre-steps report their own
        # counts through the ledger, same as issue_pr_links (CHAOS-5249).

        # 6. Commit->file edges are handled by view over git_commit_stats:
        # CHAOS-5306 ported to Go (issuecommitedges.CountCommitFileEdges),
        # wired as a native pre-step ahead of this bridge call, not here. stats
        # stays at its 0 default (see the dict literal above) -- the native
        # pre-step reports its own count through the ledger.

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
