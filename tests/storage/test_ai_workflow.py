from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from dev_health_ops.metrics.sinks.clickhouse.ai_workflow import (
    AI_WORKFLOW_ARTIFACT_EDGE_COLUMNS,
    AI_WORKFLOW_ISSUE_EDGE_COLUMNS,
    AI_WORKFLOW_RUN_COLUMNS,
    AIWorkflowMixin,
    _artifact_edge_to_row,
    _issue_edge_to_row,
    _run_to_row,
)
from dev_health_ops.models.ai_workflow import (
    AIWorkflowArtifactEdge,
    AIWorkflowArtifactType,
    AIWorkflowIssueEdge,
    AIWorkflowRun,
    AIWorkflowRunKind,
)

ORG = uuid4()
REPO = uuid4()
NOW = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)


class _FakeSink(AIWorkflowMixin):
    def __init__(self) -> None:
        self.client = MagicMock()


def _row(columns: list[str], values: list[object]) -> dict[str, object]:
    return dict(zip(columns, values))


def test_ai_workflow_run_row_redacts_prompt_content() -> None:
    run = AIWorkflowRun(
        run_id="run-1",
        org_id=ORG,
        provider="github",
        run_kind=AIWorkflowRunKind.AGENT_AUTONOMOUS,
        repo_id=REPO,
        prompts_redacted=True,
        prompt_hash=AIWorkflowRun.hash_prompt("secret prompt"),
        prompt_length=13,
        observed_at=NOW,
        metadata={"subject_id": "pr-1"},
    )

    row = _row(AI_WORKFLOW_RUN_COLUMNS, _run_to_row(run))

    assert row["prompts_redacted"] is True
    assert row["prompt_hash"] != "secret prompt"
    assert "secret prompt" not in str(row)
    forbidden_columns = {"prompt", "session", "transcript", "ide", "keystroke"}
    assert forbidden_columns.isdisjoint(AI_WORKFLOW_RUN_COLUMNS)


def test_ai_workflow_sink_writes_runs_and_edges() -> None:
    sink = _FakeSink()
    issue_edge = AIWorkflowIssueEdge(
        edge_id="edge-issue",
        org_id=ORG,
        issue_id="CHAOS-1583",
        run_id="run-1",
        provider="github",
        confidence=0.95,
        source="pr_label",
        evidence='{"label":"ai"}',
        observed_at=NOW,
        repo_id=REPO,
    )
    artifact_edge = AIWorkflowArtifactEdge(
        edge_id="edge-artifact",
        org_id=ORG,
        run_id="run-1",
        artifact_type=AIWorkflowArtifactType.PULL_REQUEST,
        artifact_id=f"{REPO}:42",
        provider="github",
        confidence=0.95,
        source="pr_label",
        evidence='{"label":"ai"}',
        observed_at=NOW,
        repo_id=REPO,
    )

    sink.write_ai_workflow_issue_edges([issue_edge])
    sink.write_ai_workflow_artifact_edges([artifact_edge])

    first_call = sink.client.insert.call_args_list[0]
    assert first_call.args[0] == "ai_workflow_issue_edges"
    assert first_call.kwargs["column_names"] == AI_WORKFLOW_ISSUE_EDGE_COLUMNS
    assert first_call.args[1][0][:-1] == _issue_edge_to_row(issue_edge)[:-1]

    second_call = sink.client.insert.call_args_list[1]
    assert second_call.args[0] == "ai_workflow_artifact_edges"
    assert second_call.kwargs["column_names"] == AI_WORKFLOW_ARTIFACT_EDGE_COLUMNS
    assert second_call.args[1][0][:-1] == _artifact_edge_to_row(artifact_edge)[:-1]
