"""Persisted models for AI governance coverage and policy events.

These models intentionally describe artifacts and policy coverage only. They do
not carry prompt content, IDE telemetry, keystrokes, sessions, transcripts, or
person-level scores.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
from uuid import UUID, uuid4


class AIPolicyRule(StrEnum):
    """Canonical, hard-coded AI governance rules."""

    MISSING_AI_DECLARATION = "MISSING_AI_DECLARATION"
    MISSING_HUMAN_REVIEW = "MISSING_HUMAN_REVIEW"
    SENSITIVE_REPO_DISALLOWED = "SENSITIVE_REPO_DISALLOWED"
    DISALLOWED_TOOL = "DISALLOWED_TOOL"
    MISSING_SECURITY_SCAN = "MISSING_SECURITY_SCAN"
    NEW_LICENSE_FINDING_FROM_AI_PR = "NEW_LICENSE_FINDING_FROM_AI_PR"


class AIPolicySeverity(StrEnum):
    """Stable severity labels for policy events."""

    INFO = "info"
    WARNING = "warning"
    HIGH = "high"
    CRITICAL = "critical"


class ToolAllowlistStatus(StrEnum):
    """Canonical tool/model allowlist statuses."""

    ALLOWED = "allowed"
    DISALLOWED = "disallowed"
    DEPRECATED = "deprecated"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class AIToolAllowlistEntry:
    """A persisted org-level AI tool/model allowlist policy entry.

    ``model_name=None`` means the policy applies to every model of the tool.
    Rows are versioned by ``computed_at`` (ReplacingMergeTree), so updating an
    entry is a plain re-insert with the same (org_id, tool_name, model_name).

    Empty-string models are normalised to ``None`` at construction: migration
    038's ORDER BY uses ``ifNull(model_name, '')``, so a NULL wildcard row
    and a ``''`` "exact" row share the SAME ReplacingMergeTree dedup key and
    would silently replace each other on background merge. Readers must
    treat ``''`` as wildcard for the same reason (see the governance
    loader's ``nullIf`` handling); making the key distinguish them requires
    a schema migration — flagged as follow-up, out of scope this wave.
    """

    org_id: str
    tool_name: str
    status: ToolAllowlistStatus
    model_name: str | None = None
    reason: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        tool = (self.tool_name or "").strip()
        if not tool:
            raise ValueError("AIToolAllowlistEntry.tool_name must be non-empty")
        object.__setattr__(self, "tool_name", tool)
        model = (self.model_name or "").strip()
        object.__setattr__(self, "model_name", model or None)


@dataclass(frozen=True)
class AIGovernanceArtifact:
    """A policy-evaluation input for an AI-relevant engineering artifact."""

    org_id: str
    subject_type: str
    subject_id: str
    observed_at: datetime
    team_id: str | None = None
    repo_id: UUID | None = None
    ai_detected: bool = False
    declared_ai: bool = False
    human_reviewed: bool | None = None
    sensitive_repo: bool = False
    repo_allows_ai: bool = True
    security_scanned: bool | None = None
    license_or_dependency_finding: bool = False
    tool_name: str | None = None
    model_name: str | None = None
    tool_allowlist_status: ToolAllowlistStatus = ToolAllowlistStatus.UNKNOWN
    evidence: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AIGovernanceViolation:
    """A persisted AI policy event linked to an affected artifact."""

    org_id: str
    rule_id: AIPolicyRule
    severity: AIPolicySeverity
    subject_type: str
    subject_id: str
    observed_at: datetime
    event_id: UUID = field(default_factory=uuid4)
    team_id: str | None = None
    repo_id: UUID | None = None
    evidence: dict[str, object] = field(default_factory=dict)
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def evidence_json(self) -> str:
        """Serialize artifact references and policy facts for ClickHouse."""
        return json.dumps(self.evidence, default=str, sort_keys=True)


@dataclass(frozen=True)
class AIGovernanceCoverageDaily:
    """Daily team/repo AI governance coverage rollup."""

    org_id: str
    team_id: str | None
    repo_id: UUID | None
    day: date
    ai_artifacts: int
    declared_artifacts: int
    human_reviewed_prs: int
    security_scanned_prs: int
    in_policy_artifacts: int
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def declaration_coverage(self) -> float:
        return _ratio(self.declared_artifacts, self.ai_artifacts)

    @property
    def human_review_coverage(self) -> float:
        return _ratio(self.human_reviewed_prs, self.ai_artifacts)

    @property
    def security_scan_coverage(self) -> float:
        return _ratio(self.security_scanned_prs, self.ai_artifacts)

    @property
    def in_policy_coverage(self) -> float:
        return _ratio(self.in_policy_artifacts, self.ai_artifacts)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 1.0
    return numerator / denominator
