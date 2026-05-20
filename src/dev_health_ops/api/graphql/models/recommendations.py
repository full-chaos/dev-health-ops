"""Strawberry GraphQL types for the rule-based recommendations engine."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

import strawberry


@strawberry.enum
class Severity(Enum):
    """Operational recommendation severity level."""

    WARNING = "warning"
    CRITICAL = "critical"


@strawberry.enum
class WindowUnit(Enum):
    """Unit for a recommendations lookback window."""

    DAY = "day"
    WEEK = "week"
    CYCLE = "cycle"


@strawberry.input
class WindowInput:
    """Lookback window for querying persisted recommendations.

    Examples:
        WindowInput(value=7, unit=WindowUnit.DAY)   → last 7 days
        WindowInput(value=4, unit=WindowUnit.WEEK)  → last 4 weeks (default)
        WindowInput(value=2, unit=WindowUnit.CYCLE) → last 2 cycles (~4 weeks)
    """

    value: int = 4
    unit: WindowUnit = WindowUnit.WEEK


@strawberry.type
class EvidenceRef:
    """Reference to the specific metric row that triggered a recommendation.

    Every field traces directly to a stored ClickHouse row so consumers can
    drill through from recommendation → raw metric without opaque IDs.
    """

    team_id: str
    metric_table: str
    window_start: date
    window_end: date
    field: str
    value: float


@strawberry.type
class Recommendation:
    """A single rule-based, deterministic recommendation for a team.

    Persisted in ``recommendations_daily``; read via argMax(..., computed_at)
    to retrieve the latest computation for each (team_id, rule_id, day) tuple.
    """

    rule_id: str
    team_id: str
    org_id: str
    computed_at: datetime
    window_start: date
    window_end: date
    severity: Severity
    title: str
    rationale: str
    success_criterion: str
    evidence: list[EvidenceRef]
