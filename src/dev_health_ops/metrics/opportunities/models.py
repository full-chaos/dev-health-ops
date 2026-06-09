"""Domain models for the Improve/Flow opportunity engine (CHAOS-2218).

These models are **independent of the GraphQL layer** (no ``strawberry``
decorators) so they can be used in pure Python tests and by any downstream
code that does not import the full API stack.

The GraphQL contracts (``schemas.py``, ``OpportunityCard``, etc.) are NOT
changed in Phase 1 — this module is a foundation only.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum


class ImproveOpportunityKind(Enum):
    """Canonical opportunity kinds for the Flow / Improve engine.

    Each value maps to a threshold rule implemented in
    :class:`~dev_health_ops.metrics.opportunities.flow_detector.FlowOpportunityDetector`.
    """

    HIGH_REVIEW_LATENCY = "high_review_latency"
    SLOW_CYCLE_TIME = "slow_cycle_time"
    HIGH_REWORK = "high_rework"
    HIGH_WIP = "high_wip"
    LOW_THROUGHPUT = "low_throughput"
    HIGH_CHURN = "high_churn"
    HIGH_CHANGE_FAILURE = "high_change_failure"


@dataclass(frozen=True)
class ImproveOpportunity:
    """A single scored flow / improve opportunity surfaced by the detector.

    Fields
    ------
    opportunity_id
        Stable 24-hex-char identifier derived deterministically from
        (kind, entity_type, entity_id).  See :func:`scoring.stable_opportunity_id`.
    kind
        The rule that fired (see :class:`ImproveOpportunityKind`).
    entity_type
        ``"repo"`` or ``"team"`` — the primary addressable unit.
    entity_id
        Opaque string identifier for the entity (repo UUID or team ID string).
    entity_display_name
        Human-readable label; ``None`` in Phase 1 (no extra join needed).
    title
        Short, actionable title suitable for card headers.
    rationale
        One-sentence plain-English explanation referencing the triggering values.
    score
        Normalised float in [0, 1].  Higher == more urgent.
    severity
        ``"high"``, ``"medium"``, or ``"low"`` derived from score bands.
    evidence_refs
        List of opaque ``"table:column:entity_id"`` references that trace back
        to the ClickHouse row(s) that triggered the rule.
    recommended_action
        Prescribed next step drawn from the static action map in
        :data:`flow_detector._RECOMMENDED_ACTIONS`.
    """

    opportunity_id: str
    kind: ImproveOpportunityKind
    entity_type: str
    entity_id: str
    entity_display_name: str | None
    title: str
    rationale: str
    score: float
    severity: str
    evidence_refs: list[str]
    recommended_action: str


@dataclass(frozen=True)
class FlowScopeInput:
    """Optional scope filter for :class:`FlowOpportunityDetector`.

    Mirrors the ``_Scope`` private dataclass in ``ai_detector.py`` but is
    exposed as a public, named type so callers do not depend on internals.

    Both fields are optional; omitting them means "whole org".
    """

    repo_id: uuid.UUID | None = field(default=None)
    team_id: str | None = field(default=None)
