"""Benchmarking and insights data schemas.

Canonical dataclasses for period-over-period comparison, internal baselines,
maturity classification, anomaly detection, and correlation insights.

All computations use pure Python — no numpy/scipy dependencies.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import date

# ---------------------------------------------------------------------------
# Period-over-Period Comparison (CHAOS-1179)
# ---------------------------------------------------------------------------


class Trend(enum.Enum):
    """Direction of metric change between periods."""

    UP = "up"
    DOWN = "down"
    FLAT = "flat"


@dataclass(frozen=True)
class PeriodComparison:
    """Result of comparing a metric across two time periods."""

    metric: str
    scope: str
    current_value: float
    previous_value: float
    absolute_delta: float
    pct_change: float
    trend: Trend


# ---------------------------------------------------------------------------
# Internal Baselines (CHAOS-1180)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BaselineRecord:
    """Baseline statistics for a metric across a population (teams/repos)."""

    metric: str
    scope: str
    period_days: int
    mean: float
    p25: float
    p50: float
    p75: float
    p90: float
    percentile_rank: float


# ---------------------------------------------------------------------------
# Maturity Bands (CHAOS-1181)
# ---------------------------------------------------------------------------


class MaturityBand(enum.Enum):
    """Maturity classification bands based on percentile rank."""

    EMERGING = "emerging"
    DEVELOPING = "developing"
    ESTABLISHED = "established"
    LEADING = "leading"


@dataclass(frozen=True)
class MaturityClassification:
    """Maturity assessment for a metric within a scope."""

    metric: str
    scope: str
    band: MaturityBand
    confidence: float
    score: float


# ---------------------------------------------------------------------------
# Anomaly Detection (CHAOS-1182)
# ---------------------------------------------------------------------------


class AnomalySeverity(enum.Enum):
    """Severity of an anomalous data point."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AnomalyDirection(enum.Enum):
    """Direction category of an anomaly."""

    REGRESSION = "regression"
    IMPROVEMENT = "improvement"
    VOLATILITY = "volatility"


@dataclass(frozen=True)
class AnomalyRecord:
    """A single anomalous metric observation."""

    metric: str
    scope: str
    day: date
    value: float
    baseline_mean: float
    baseline_std: float
    z_score: float
    severity: AnomalySeverity
    direction: AnomalyDirection


# ---------------------------------------------------------------------------
# Correlation Insights (CHAOS-1183)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CorrelationRecord:
    """Pearson correlation result between two metrics."""

    metric_a: str
    metric_b: str
    scope: str
    coefficient: float
    p_value: float
    significant: bool
    interpretation: str
