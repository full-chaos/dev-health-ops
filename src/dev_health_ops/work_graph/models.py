"""
Data models for work graph edges and nodes.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional


class NodeType(str, Enum):
    """Types of nodes in the work graph."""

    ISSUE = "issue"
    PR = "pr"
    COMMIT = "commit"
    FILE = "file"


class EdgeType(str, Enum):
    """Types of edges in the work graph."""

    # Issue-to-issue relationships (from work_item_dependencies)
    BLOCKS = "blocks"
    RELATES = "relates"
    DUPLICATES = "duplicates"
    IS_BLOCKED_BY = "is_blocked_by"
    IS_RELATED_TO = "is_related_to"
    IS_DUPLICATE_OF = "is_duplicate_of"
    PARENT_OF = "parent_of"
    CHILD_OF = "child_of"

    # Issue-to-PR relationships
    REFERENCES = "references"  # PR references issue
    IMPLEMENTS = "implements"  # PR implements/closes issue
    FIXES = "fixes"  # PR fixes issue

    # PR-to-commit relationships
    CONTAINS = "contains"  # PR contains commit

    # Commit-to-file relationships
    TOUCHES = "touches"  # Commit touches file


class Provenance(str, Enum):
    """How an edge was discovered."""

    NATIVE = "native"  # From provider API (e.g., work_item_dependencies)
    EXPLICIT_TEXT = "explicit_text"  # Parsed from text (e.g., "ABC-123" in PR title)
    HEURISTIC = "heuristic"  # Inferred by rules (e.g., same repo + time window)


@dataclass(frozen=True)
class WorkGraphEdge:
    """
    Generic edge in the work graph with provenance tracking.

    All edges are stored with:
    - Source and target node identifiers
    - Edge type (relationship kind)
    - Provenance (how it was discovered)
    - Confidence score (0.0 - 1.0)
    - Evidence (token or rule that created this edge)
    """

    edge_id: str  # Deterministic hash
    source_type: NodeType
    source_id: str
    target_type: NodeType
    target_id: str
    edge_type: EdgeType
    provenance: Provenance
    confidence: float
    evidence: str
    repo_id: Optional[uuid.UUID] = None
    provider: Optional[str] = None
    discovered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    day: Optional[date] = None

    def __post_init__(self):
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"confidence must be between 0 and 1, got {self.confidence}"
            )
        # If day is not provided, derive from event_ts
        if self.day is None and self.event_ts:
            object.__setattr__(self, "day", self.event_ts.date())


@dataclass(frozen=True)
class WorkGraphIssuePR:
    """
    Fast path model for issue <-> PR relationships.

    Optimized for common queries linking work items to pull requests.
    """

    repo_id: uuid.UUID
    work_item_id: str
    pr_number: int
    confidence: float
    provenance: Provenance
    evidence: str
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class WorkGraphPRCommit:
    """
    Fast path model for PR <-> commit relationships.

    Stores commits belonging to a pull request.
    """

    repo_id: uuid.UUID
    pr_number: int
    commit_hash: str
    confidence: float
    provenance: Provenance
    evidence: str
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
