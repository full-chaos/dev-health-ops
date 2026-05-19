"""Typed models for AI workflow evidence in the Work Graph.

The model deliberately stores metadata only. It has no fields for raw prompts,
sessions, transcripts, IDE telemetry, or keystrokes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from uuid import UUID


class AIWorkflowRunKind(StrEnum):
    CHAT_ASSISTED = "chat_assisted"
    AGENT_AUTONOMOUS = "agent_autonomous"
    UNKNOWN = "unknown"


class AIWorkflowRunStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    RUNNING = "running"
    UNKNOWN = "unknown"


class AIWorkflowArtifactType(StrEnum):
    PULL_REQUEST = "pull_request"
    DIFF = "diff"


@dataclass(frozen=True)
class AIWorkflowRun:
    run_id: str
    org_id: UUID
    provider: str
    run_kind: AIWorkflowRunKind = AIWorkflowRunKind.UNKNOWN
    status: AIWorkflowRunStatus | str | None = None
    tool: str | None = None
    model: str | None = None
    actor: str | None = None
    repo_id: UUID | None = None
    prompts_redacted: bool = True
    prompt_hash: str | None = None
    prompt_length: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, object] = field(default_factory=dict)

    @staticmethod
    def hash_prompt(prompt: str) -> str:
        return hashlib.sha256(prompt.encode("utf-8")).hexdigest()

    def metadata_json(self) -> str:
        return json.dumps(self.metadata, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class AIWorkflowIssueEdge:
    edge_id: str
    org_id: UUID
    issue_id: str
    run_id: str
    provider: str
    confidence: float
    source: str
    evidence: str
    observed_at: datetime
    repo_id: UUID | None = None


@dataclass(frozen=True)
class AIWorkflowArtifactEdge:
    edge_id: str
    org_id: UUID
    run_id: str
    artifact_type: AIWorkflowArtifactType
    artifact_id: str
    provider: str
    confidence: float
    source: str
    evidence: str
    observed_at: datetime
    repo_id: UUID | None = None


@dataclass(frozen=True)
class WorkGraphPRReviewOutcomeEdge:
    edge_id: str
    org_id: UUID
    pr_id: str
    review_outcome_id: str
    provider: str
    confidence: float
    source: str
    evidence: str
    observed_at: datetime
    outcome: str | None = None
    repo_id: UUID | None = None


@dataclass(frozen=True)
class WorkGraphPRDeploymentEdge:
    edge_id: str
    org_id: UUID
    pr_id: str
    deployment_id: str
    provider: str
    confidence: float
    source: str
    evidence: str
    observed_at: datetime
    repo_id: UUID | None = None


@dataclass(frozen=True)
class WorkGraphDeploymentIncidentEdge:
    edge_id: str
    org_id: UUID
    deployment_id: str
    incident_id: str
    provider: str
    confidence: float
    source: str
    evidence: str
    observed_at: datetime
    repo_id: UUID | None = None


AIWorkflowEdge = (
    AIWorkflowIssueEdge
    | AIWorkflowArtifactEdge
    | WorkGraphPRReviewOutcomeEdge
    | WorkGraphPRDeploymentEdge
    | WorkGraphDeploymentIncidentEdge
)
