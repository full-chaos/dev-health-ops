"""
Canonical models for AI attribution storage.

These types represent both the lightweight *signal* detected at normalization time
and the full *record* persisted to ClickHouse.

Source precedence (highest → lowest):
    MANUAL > PR_LABEL > BOT_AUTHOR > COMMIT_TRAILER > CI_ANNOTATION > BRANCH_NAME > PR_BODY

Write-time: persist every detected signal, deduped on (subject, source).
Read-time:  the ``ai_attribution_resolved`` view resolves the effective attribution.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class AIAttributionSource(StrEnum):
    """Where the AI attribution signal came from (ordered by precedence, high→low)."""

    PR_LABEL = "pr_label"  # explicit label on the PR — highest signal
    BOT_AUTHOR = "bot_author"  # GitHub App / known bot user
    COMMIT_TRAILER = "commit_trailer"  # AI-Assisted-By / Co-authored-by AI bot
    BRANCH_NAME = "branch_name"  # weak heuristic signal
    PR_BODY = "pr_body"  # weak heuristic signal
    CI_ANNOTATION = "ci_annotation"  # workflow step annotation
    MANUAL = "manual"  # user override — always wins regardless of position


class AIAttributionKind(StrEnum):
    """The type of AI involvement."""

    AI_ASSISTED = "ai_assisted"  # human authored with AI assistance
    AGENT_CREATED = "agent_created"  # autonomous agent produced this artifact
    AI_REVIEW = "ai_review"  # AI performed the review
    UNKNOWN = "unknown"  # signal detected but kind unclear — do NOT guess
    HUMAN = "human"  # explicit human attribution (e.g., manual override / fixture)


# ---------------------------------------------------------------------------
# Source precedence mapping
# ---------------------------------------------------------------------------

#: Lower integer = higher precedence.  MANUAL wins all others.
SOURCE_PRECEDENCE: dict[AIAttributionSource, int] = {
    AIAttributionSource.MANUAL: 1,
    AIAttributionSource.PR_LABEL: 2,
    AIAttributionSource.BOT_AUTHOR: 3,
    AIAttributionSource.COMMIT_TRAILER: 4,
    AIAttributionSource.CI_ANNOTATION: 5,
    AIAttributionSource.BRANCH_NAME: 6,
    AIAttributionSource.PR_BODY: 7,
}

SubjectType = Literal["pull_request", "commit", "issue", "workflow_run", "review"]


# ---------------------------------------------------------------------------
# AIAttributionSignal — lightweight detection output (pre-persistence)
# ---------------------------------------------------------------------------


@dataclass
class AIAttributionSignal:
    """
    Detection output from a single heuristic.

    Produced by ``providers/_ai_detection.py`` during normalization.
    Does NOT carry org/subject context — the caller attaches that before
    constructing a full ``AIAttributionRecord``.
    """

    kind: AIAttributionKind
    source: AIAttributionSource
    confidence: float  # [0.0, 1.0]
    actor: str | None  # bot name / agent name / "human" / None
    evidence: dict[
        str, object
    ]  # source-specific raw signal (label text, trailer key, etc.)


# ---------------------------------------------------------------------------
# AIAttributionRecord — full persisted record
# ---------------------------------------------------------------------------


@dataclass
class AIAttributionRecord:
    """
    Full AI attribution record, ready for ClickHouse persistence.

    One record per detected signal per subject.  Multiple records for the same
    subject (from different sources) are all persisted; resolution happens at
    read time via the ``ai_attribution_resolved`` view.

    Deduplication key (ClickHouse ORDER BY):
        (org_id, provider, subject_type, subject_id, source)

    Supersession:
        When a MANUAL record is created, ``superseded_by`` on earlier records
        may be set to ``record_id`` of the MANUAL record.  The resolved view
        filters ``superseded_by IS NULL`` records first, then applies precedence.
    """

    org_id: UUID
    provider: str  # github | gitlab | jira | local
    subject_type: SubjectType
    subject_id: str  # provider-native id
    repo_id: UUID | None
    kind: AIAttributionKind
    source: AIAttributionSource
    confidence: float  # [0.0, 1.0]
    actor: str | None  # bot name / agent name / "human"
    evidence: dict[str, object]  # source-specific raw signal
    observed_at: datetime  # when signal was emitted by the provider
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    superseded_by: UUID | None = None
    record_id: UUID = field(default_factory=uuid4)

    def evidence_json(self) -> str:
        """Serialize evidence to JSON string for ClickHouse storage."""
        return json.dumps(self.evidence, default=str)

    @classmethod
    def from_signal(
        cls,
        signal: AIAttributionSignal,
        *,
        org_id: UUID,
        provider: str,
        subject_type: SubjectType,
        subject_id: str,
        repo_id: UUID | None,
        observed_at: datetime,
    ) -> AIAttributionRecord:
        """Promote a detection signal to a full persisted record."""
        return cls(
            org_id=org_id,
            provider=provider,
            subject_type=subject_type,
            subject_id=subject_id,
            repo_id=repo_id,
            kind=signal.kind,
            source=signal.source,
            confidence=signal.confidence,
            actor=signal.actor,
            evidence=signal.evidence,
            observed_at=observed_at,
        )
