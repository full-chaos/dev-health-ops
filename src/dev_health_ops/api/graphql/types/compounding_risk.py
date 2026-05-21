"""GraphQL types for the Compounding Risk surface (CHAOS-1642)."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

import strawberry


@strawberry.enum
class CompoundingRiskScope(Enum):
    """Breakout scope for Compounding Risk queries.

    Per the no-surveillance contract, ``DEVELOPER`` is intentionally absent.
    """

    REPO = "repo"
    TEAM = "team"


@strawberry.enum
class CompoundingRiskSeverity(Enum):
    """Severity bucket persisted alongside the composite score."""

    UNKNOWN = "unknown"
    LOW = "low"
    ELEVATED = "elevated"
    HIGH = "high"


@strawberry.type
class CompoundingRiskComponents:
    """Per-component normalized values and the raw inputs they came from.

    All fields are nullable — a missing input propagates as ``None`` rather
    than zero (data unavailable is not "no risk").
    """

    churn_norm: float | None = strawberry.field(name="churnNorm")
    complexity_norm: float | None = strawberry.field(name="complexityNorm")
    ownership_norm: float | None = strawberry.field(name="ownershipNorm")
    review_norm: float | None = strawberry.field(name="reviewNorm")

    rework_churn: float | None = strawberry.field(name="reworkChurn")
    complexity_delta: float | None = strawberry.field(name="complexityDelta")
    bus_factor: float | None = strawberry.field(name="busFactor")
    ownership_gini: float | None = strawberry.field(name="ownershipGini")
    single_owner_ratio: float | None = strawberry.field(name="singleOwnerRatio")
    review_latency_p90h: float | None = strawberry.field(name="reviewLatencyP90h")


@strawberry.type
class CompoundingRiskWeights:
    """Weights used at compute time (audit trail; sums to 1.0)."""

    churn: float
    complexity: float
    ownership: float
    review: float


@strawberry.type
class CompoundingRiskThresholds:
    """Severity bucket thresholds used at compute time (audit trail)."""

    elevated: float
    high: float


@strawberry.type
class CompoundingRiskPoint:
    """One Compounding Risk row (latest ``computed_at`` per scope/day)."""

    day: date
    scope: CompoundingRiskScope
    scope_id: str = strawberry.field(name="scopeId")
    scope_label: str = strawberry.field(name="scopeLabel")

    score: float | None  # nullable when any required input is missing
    severity: CompoundingRiskSeverity

    components: CompoundingRiskComponents
    weights: CompoundingRiskWeights
    thresholds: CompoundingRiskThresholds

    computed_at: datetime = strawberry.field(name="computedAt")


@strawberry.type
class CompoundingRiskTrendPoint:
    """One day in a per-scope trend series."""

    day: date
    score: float | None
    severity: CompoundingRiskSeverity


@strawberry.type
class CompoundingRiskResult:
    """Top-level Compounding Risk response.

    ``rows`` contains the latest per-scope record sorted by score descending
    (nulls last). ``trend`` is the per-day series for the selected scope (or
    the aggregate trend when no scope is selected).
    """

    org_id: str = strawberry.field(name="orgId")
    breakout: CompoundingRiskScope
    rows: list[CompoundingRiskPoint]
    trend: list[CompoundingRiskTrendPoint]
    generated_at: datetime = strawberry.field(name="generatedAt")


@strawberry.input
class CompoundingRiskFilterInput:
    """Filter for the Compounding Risk query.

    Per the no-surveillance contract, this input intentionally does not
    accept a person/developer scope. ``breakout`` chooses repo vs team
    aggregation; specific ``repoIds`` or ``teamIds`` narrow the result set
    further.
    """

    day: date | None = None  # default: latest available day
    breakout: CompoundingRiskScope = CompoundingRiskScope.REPO
    repo_ids: list[str] | None = strawberry.field(default=None, name="repoIds")
    team_ids: list[str] | None = strawberry.field(default=None, name="teamIds")
    trend_days: int = strawberry.field(default=30, name="trendDays")
