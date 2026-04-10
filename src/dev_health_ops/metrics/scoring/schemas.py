"""Scoring model data contracts.

Defines the value objects used by dimension scorers and the composite scorer.
All scores are normalized to the 0.0-1.0 range where higher suggests better health.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(frozen=True)
class SignalValue:
    """A single normalised signal contributing to a dimension score.

    Attributes:
        name: Canonical signal identifier (e.g. ``pipeline_success_rate``).
        raw_value: The original metric value before normalisation.
        normalized_value: Value mapped to 0.0-1.0 (higher appears healthier).
        weight: Relative importance within the parent dimension (0.0-1.0).
        source_table: ClickHouse table the raw value was drawn from.
    """

    name: str
    raw_value: float | None
    normalized_value: float | None
    weight: float
    source_table: str


@dataclass(frozen=True)
class DimensionScore:
    """Weighted score for a single health dimension.

    Attributes:
        dimension: Human-readable dimension label (e.g. ``delivery``).
        score: Weighted blend of available signals (0.0-1.0), or ``None``
            when no signals could be computed.
        signals: Breakdown of individual signal contributions.
        day: The calendar day this score covers.
        org_id: Organisation scope.
        team_id: Optional team scope (``None`` for org-wide).
        computed_at: Timestamp of computation.
    """

    dimension: str
    score: float | None
    signals: list[SignalValue] = field(default_factory=list)
    day: date | None = None
    org_id: str = ""
    team_id: str | None = None
    computed_at: datetime | None = None


@dataclass(frozen=True)
class CompositeScore:
    """Blended platform-health score across all dimensions.

    Attributes:
        score: Weighted composite of dimension scores (0.0-1.0), or ``None``
            when no dimensions could be scored.
        dimensions: Per-dimension breakdowns.
        day: Calendar day.
        org_id: Organisation scope.
        team_id: Optional team scope.
        computed_at: Timestamp of computation.
    """

    score: float | None
    dimensions: list[DimensionScore] = field(default_factory=list)
    day: date | None = None
    org_id: str = ""
    team_id: str | None = None
    computed_at: datetime | None = None
