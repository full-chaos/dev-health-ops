"""ClickHouse write methods for AI workflow Work Graph evidence."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dev_health_ops.metrics.sinks.clickhouse._insert import (
    DEFAULT_BATCH_SIZE,
    _chunked,
    _dt_to_clickhouse_datetime,
)
from dev_health_ops.models.ai_workflow import (
    AIWorkflowArtifactEdge,
    AIWorkflowIssueEdge,
    AIWorkflowRun,
    WorkGraphDeploymentIncidentEdge,
    WorkGraphPRDeploymentEdge,
    WorkGraphPRReviewOutcomeEdge,
)

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)

AI_WORKFLOW_RUN_COLUMNS = [
    "run_id",
    "org_id",
    "provider",
    "run_kind",
    "status",
    "tool",
    "model",
    "actor",
    "repo_id",
    "prompts_redacted",
    "prompt_hash",
    "prompt_length",
    "started_at",
    "completed_at",
    "observed_at",
    "metadata",
    "computed_at",
]

AI_WORKFLOW_ARTIFACT_EDGE_COLUMNS = [
    "edge_id",
    "org_id",
    "run_id",
    "artifact_type",
    "artifact_id",
    "provider",
    "repo_id",
    "confidence",
    "source",
    "evidence",
    "observed_at",
    "computed_at",
]

AI_WORKFLOW_ISSUE_EDGE_COLUMNS = [
    "edge_id",
    "org_id",
    "issue_id",
    "run_id",
    "provider",
    "repo_id",
    "confidence",
    "source",
    "evidence",
    "observed_at",
    "computed_at",
]

PR_REVIEW_OUTCOME_EDGE_COLUMNS = [
    "edge_id",
    "org_id",
    "pr_id",
    "review_outcome_id",
    "outcome",
    "provider",
    "repo_id",
    "confidence",
    "source",
    "evidence",
    "observed_at",
    "computed_at",
]

PR_DEPLOYMENT_EDGE_COLUMNS = [
    "edge_id",
    "org_id",
    "pr_id",
    "deployment_id",
    "provider",
    "repo_id",
    "confidence",
    "source",
    "evidence",
    "observed_at",
    "computed_at",
]

DEPLOYMENT_INCIDENT_EDGE_COLUMNS = [
    "edge_id",
    "org_id",
    "deployment_id",
    "incident_id",
    "provider",
    "repo_id",
    "confidence",
    "source",
    "evidence",
    "observed_at",
    "computed_at",
]


def _computed_at() -> datetime:
    return datetime.now(timezone.utc)


def _uuid(value: object | None) -> str | None:
    return str(value) if value is not None else None


def _run_to_row(run: AIWorkflowRun) -> list[object]:
    return [
        run.run_id,
        str(run.org_id),
        run.provider,
        str(run.run_kind),
        str(run.status) if run.status is not None else None,
        run.tool,
        run.model,
        run.actor,
        _uuid(run.repo_id),
        bool(run.prompts_redacted),
        run.prompt_hash,
        run.prompt_length,
        _dt_to_clickhouse_datetime(run.started_at),
        _dt_to_clickhouse_datetime(run.completed_at),
        _dt_to_clickhouse_datetime(run.observed_at),
        run.metadata_json(),
        _dt_to_clickhouse_datetime(_computed_at()),
    ]


def _artifact_edge_to_row(edge: AIWorkflowArtifactEdge) -> list[object]:
    return [
        edge.edge_id,
        str(edge.org_id),
        edge.run_id,
        str(edge.artifact_type),
        edge.artifact_id,
        edge.provider,
        _uuid(edge.repo_id),
        float(edge.confidence),
        edge.source,
        edge.evidence,
        _dt_to_clickhouse_datetime(edge.observed_at),
        _dt_to_clickhouse_datetime(_computed_at()),
    ]


def _issue_edge_to_row(edge: AIWorkflowIssueEdge) -> list[object]:
    return [
        edge.edge_id,
        str(edge.org_id),
        edge.issue_id,
        edge.run_id,
        edge.provider,
        _uuid(edge.repo_id),
        float(edge.confidence),
        edge.source,
        edge.evidence,
        _dt_to_clickhouse_datetime(edge.observed_at),
        _dt_to_clickhouse_datetime(_computed_at()),
    ]


def _review_edge_to_row(edge: WorkGraphPRReviewOutcomeEdge) -> list[object]:
    return [
        edge.edge_id,
        str(edge.org_id),
        edge.pr_id,
        edge.review_outcome_id,
        edge.outcome,
        edge.provider,
        _uuid(edge.repo_id),
        float(edge.confidence),
        edge.source,
        edge.evidence,
        _dt_to_clickhouse_datetime(edge.observed_at),
        _dt_to_clickhouse_datetime(_computed_at()),
    ]


def _pr_deployment_edge_to_row(edge: WorkGraphPRDeploymentEdge) -> list[object]:
    return [
        edge.edge_id,
        str(edge.org_id),
        edge.pr_id,
        edge.deployment_id,
        edge.provider,
        _uuid(edge.repo_id),
        float(edge.confidence),
        edge.source,
        edge.evidence,
        _dt_to_clickhouse_datetime(edge.observed_at),
        _dt_to_clickhouse_datetime(_computed_at()),
    ]


def _deployment_incident_edge_to_row(
    edge: WorkGraphDeploymentIncidentEdge,
) -> list[object]:
    return [
        edge.edge_id,
        str(edge.org_id),
        edge.deployment_id,
        edge.incident_id,
        edge.provider,
        _uuid(edge.repo_id),
        float(edge.confidence),
        edge.source,
        edge.evidence,
        _dt_to_clickhouse_datetime(edge.observed_at),
        _dt_to_clickhouse_datetime(_computed_at()),
    ]


class AIWorkflowMixin(_ClickHouseSinkBase):
    """Mixin for AI workflow Work Graph persistence."""

    def write_ai_workflow_runs(
        self,
        runs: Sequence[AIWorkflowRun],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if not runs:
            return
        for chunk in _chunked(list(runs), batch_size):
            self.client.insert(
                "ai_workflow_runs",
                [_run_to_row(run) for run in chunk],
                column_names=AI_WORKFLOW_RUN_COLUMNS,
            )
        logger.debug("write_ai_workflow_runs: persisted %d run(s)", len(runs))

    def write_ai_workflow_artifact_edges(
        self,
        edges: Sequence[AIWorkflowArtifactEdge],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._write_edges(
            "ai_workflow_artifact_edges",
            edges,
            AI_WORKFLOW_ARTIFACT_EDGE_COLUMNS,
            _artifact_edge_to_row,
            batch_size,
        )

    def write_ai_workflow_issue_edges(
        self,
        edges: Sequence[AIWorkflowIssueEdge],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._write_edges(
            "ai_workflow_issue_edges",
            edges,
            AI_WORKFLOW_ISSUE_EDGE_COLUMNS,
            _issue_edge_to_row,
            batch_size,
        )

    def write_work_graph_pr_review_outcome_edges(
        self,
        edges: Sequence[WorkGraphPRReviewOutcomeEdge],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._write_edges(
            "work_graph_pr_review_outcome_edges",
            edges,
            PR_REVIEW_OUTCOME_EDGE_COLUMNS,
            _review_edge_to_row,
            batch_size,
        )

    def write_work_graph_pr_deployment_edges(
        self,
        edges: Sequence[WorkGraphPRDeploymentEdge],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._write_edges(
            "work_graph_pr_deployment_edges",
            edges,
            PR_DEPLOYMENT_EDGE_COLUMNS,
            _pr_deployment_edge_to_row,
            batch_size,
        )

    def write_work_graph_deployment_incident_edges(
        self,
        edges: Sequence[WorkGraphDeploymentIncidentEdge],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._write_edges(
            "work_graph_deployment_incident_edges",
            edges,
            DEPLOYMENT_INCIDENT_EDGE_COLUMNS,
            _deployment_incident_edge_to_row,
            batch_size,
        )

    def _write_edges(
        self,
        table: str,
        edges: Sequence[Any],
        columns: Sequence[str],
        row_factory: Callable[[Any], list[object]],
        batch_size: int,
    ) -> None:
        if not edges:
            return
        for chunk in _chunked(list(edges), batch_size):
            self.client.insert(
                table,
                [row_factory(edge) for edge in chunk],
                column_names=list(columns),
            )
        logger.debug("%s: persisted %d edge(s)", table, len(edges))
