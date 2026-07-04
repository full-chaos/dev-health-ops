"""Compounding Risk composite metric (CHAOS-1641).

Deterministic, inspectable composite that combines four signals already
computed elsewhere in the metrics pipeline:

    churn       — rework churn ratio over the trailing window
    complexity  — repo complexity trend (cyclomatic_per_kloc delta)
    ownership   — concentration (max of single-owner ratio and gini)
    review      — review-latency p90 in hours (pr_first_review_p90_hours)

Each input is normalized into [0, 1] against a reference value, then
combined via a weighted sum. The output, weights, raw inputs, normalized
components, and severity bucket are all persisted so historical rows
remain auditable.

Public surface:
    DEFAULT_WEIGHTS, DEFAULT_THRESHOLDS, REFERENCE_VALUES
    CompoundingInputs, CompoundingWeights, CompoundingThresholds
    compute_compounding_risk(...)
    severity_for(score, thresholds)
    load_repo_complexity_delta_30d(sink, repo_id, day)   # I/O helper
    build_compounding_risk_rows_for_day(...)             # orchestrator

The core compute function is pure: no I/O, no logging side effects, no globals
mutated. The orchestrator at the bottom is the only I/O entry point and is kept
thin and explicit.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Final

from dev_health_ops.metrics.schemas import CompoundingRiskDailyRecord

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Reference values used to normalize raw inputs into [0, 1].
#: Each input ``x`` is mapped as ``clamp01(x / REF)``. These are tuning
#: defaults — they are persisted on every row so historical rows remain
#: inspectable even if defaults change.
REFERENCE_VALUES: Final[dict[str, float]] = {
    "churn_ref": 0.30,  # rework_churn_ratio of 0.30 == saturation
    "complexity_ref": 0.20,  # 20% rise in cyclomatic_per_kloc == saturation
    "review_ref": 48.0,  # 48h pr_first_review_p90_hours == saturation
}


@dataclass(frozen=True)
class CompoundingWeights:
    """Weights for the four normalized components. Must sum to 1.0."""

    churn: float = 0.30
    complexity: float = 0.30
    ownership: float = 0.20
    review: float = 0.20

    def __post_init__(self) -> None:
        total = self.churn + self.complexity + self.ownership + self.review
        # Allow tiny float drift but reject anything materially off.
        if abs(total - 1.0) > 1e-9:
            raise ValueError(f"CompoundingWeights must sum to 1.0, got {total!r}")


@dataclass(frozen=True)
class CompoundingThresholds:
    """Severity bucket boundaries (inclusive at the lower edge).

    score < elevated  -> low
    elevated <= score < high -> elevated
    score >= high     -> high
    score is None     -> unknown
    """

    elevated: float = 0.40
    high: float = 0.65

    def __post_init__(self) -> None:
        if not (0.0 <= self.elevated <= self.high <= 1.0):
            raise ValueError(
                "CompoundingThresholds must satisfy 0 <= elevated <= high <= 1, "
                f"got elevated={self.elevated} high={self.high}"
            )


DEFAULT_WEIGHTS: Final[CompoundingWeights] = CompoundingWeights()
DEFAULT_THRESHOLDS: Final[CompoundingThresholds] = CompoundingThresholds()


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompoundingInputs:
    """Raw inputs consumed by the composite.

    ``None`` on any *required* field (churn / complexity_delta /
    review_latency_p90h, plus at least one ownership signal) causes the
    score to be ``None``. Data unavailable is **not** zero risk.
    """

    rework_churn: float | None
    complexity_delta: float | None
    review_latency_p90h: float | None
    # Ownership inputs — use max(single_owner_ratio, ownership_gini) as the
    # concentration norm. Either alone is acceptable; both None blocks the
    # ownership component (and therefore the composite).
    single_owner_ratio: float | None = None
    ownership_gini: float | None = None
    # Pure metadata, not part of the formula, surfaced for inspectability.
    bus_factor: float | None = None

    def has_required_inputs(self) -> bool:
        if self.rework_churn is None:
            return False
        if self.complexity_delta is None:
            return False
        if self.review_latency_p90h is None:
            return False
        if self.single_owner_ratio is None and self.ownership_gini is None:
            return False
        return True


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _normalize_churn(rework_churn: float | None, *, ref: float) -> float | None:
    if rework_churn is None:
        return None
    return _clamp01(max(0.0, rework_churn) / ref)


def _normalize_complexity(delta: float | None, *, ref: float) -> float | None:
    """Falling complexity is *not* risk. Clamp negatives to 0 first."""
    if delta is None:
        return None
    return _clamp01(max(0.0, delta) / ref)


def _normalize_ownership(
    single_owner_ratio: float | None,
    ownership_gini: float | None,
) -> float | None:
    """Concentration norm = max(single_owner_ratio, gini). Already in [0,1]."""
    candidates = [v for v in (single_owner_ratio, ownership_gini) if v is not None]
    if not candidates:
        return None
    return _clamp01(max(candidates))


def _normalize_review(latency_hours: float | None, *, ref: float) -> float | None:
    if latency_hours is None:
        return None
    return _clamp01(max(0.0, latency_hours) / ref)


def severity_for(
    score: float | None,
    thresholds: CompoundingThresholds = DEFAULT_THRESHOLDS,
) -> str:
    if score is None:
        return "unknown"
    if score >= thresholds.high:
        return "high"
    if score >= thresholds.elevated:
        return "elevated"
    return "low"


# ---------------------------------------------------------------------------
# Public compute
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _NormalizedComponents:
    churn_norm: float | None
    complexity_norm: float | None
    ownership_norm: float | None
    review_norm: float | None


def _normalize_all(
    inputs: CompoundingInputs,
    refs: dict[str, float],
) -> _NormalizedComponents:
    return _NormalizedComponents(
        churn_norm=_normalize_churn(inputs.rework_churn, ref=refs["churn_ref"]),
        complexity_norm=_normalize_complexity(
            inputs.complexity_delta, ref=refs["complexity_ref"]
        ),
        ownership_norm=_normalize_ownership(
            inputs.single_owner_ratio, inputs.ownership_gini
        ),
        review_norm=_normalize_review(
            inputs.review_latency_p90h, ref=refs["review_ref"]
        ),
    )


def compute_compounding_risk(
    *,
    day: date,
    scope: str,
    scope_id: str,
    org_id: str,
    inputs: CompoundingInputs,
    computed_at: datetime,
    weights: CompoundingWeights = DEFAULT_WEIGHTS,
    thresholds: CompoundingThresholds = DEFAULT_THRESHOLDS,
    refs: dict[str, float] | None = None,
) -> CompoundingRiskDailyRecord:
    """Compute one Compounding Risk row.

    The function always returns a row. The row's ``compounding_risk`` is
    ``None`` (severity ``unknown``) when any required input is missing —
    the row is still persisted so absence-of-signal is itself inspectable.

    Args:
        day: The day this score is computed *for* (not the compute moment).
        scope: ``"repo"`` or ``"team"``.
        scope_id: Repo id (uuid str) or team id.
        org_id: Org id for partitioning.
        inputs: Raw signal values.
        computed_at: Compute moment in UTC. Passed explicitly for determinism.
        weights: Default weights or an override (must sum to 1).
        thresholds: Severity bucket boundaries.
        refs: Reference normalization values; defaults to ``REFERENCE_VALUES``.

    Returns:
        A ``CompoundingRiskDailyRecord`` ready for the sink.
    """
    if scope not in ("repo", "team"):
        raise ValueError(f"scope must be 'repo' or 'team', got {scope!r}")

    resolved_refs = refs if refs is not None else REFERENCE_VALUES

    components = _normalize_all(inputs, resolved_refs)

    score: float | None
    if inputs.has_required_inputs():
        # All four normalized components are non-None here (required-input
        # gate covers each of them).
        assert components.churn_norm is not None
        assert components.complexity_norm is not None
        assert components.ownership_norm is not None
        assert components.review_norm is not None

        score = (
            weights.churn * components.churn_norm
            + weights.complexity * components.complexity_norm
            + weights.ownership * components.ownership_norm
            + weights.review * components.review_norm
        )
        # Floating-point housekeeping: snap to [0, 1] in case of drift.
        score = _clamp01(score)
    else:
        score = None

    return CompoundingRiskDailyRecord(
        day=day,
        scope=scope,
        scope_id=scope_id,
        compounding_risk=score,
        severity=severity_for(score, thresholds),
        churn_norm=components.churn_norm,
        complexity_norm=components.complexity_norm,
        ownership_norm=components.ownership_norm,
        review_norm=components.review_norm,
        rework_churn=inputs.rework_churn,
        complexity_delta=inputs.complexity_delta,
        bus_factor=inputs.bus_factor,
        ownership_gini=inputs.ownership_gini,
        single_owner_ratio=inputs.single_owner_ratio,
        review_latency_p90h=inputs.review_latency_p90h,
        w_churn=weights.churn,
        w_complexity=weights.complexity,
        w_ownership=weights.ownership,
        w_review=weights.review,
        threshold_elevated=thresholds.elevated,
        threshold_high=thresholds.high,
        computed_at=computed_at,
        org_id=org_id,
    )


# ---------------------------------------------------------------------------
# I/O helpers and orchestrator (CHAOS-1641 wiring into job_daily)
# ---------------------------------------------------------------------------

#: Window for the complexity delta computation in days.
COMPLEXITY_WINDOW_DAYS: Final[int] = 30


def load_repo_complexity_delta_30d(
    sink: Any,
    *,
    repo_id: str,
    day: date,
    org_id: str,
    window_days: int = COMPLEXITY_WINDOW_DAYS,
) -> float | None:
    """Return relative change in ``cyclomatic_per_kloc`` for a repo over the window.

    Reads from ``repo_complexity_daily`` via the sink's ``query_dicts``.
    Returns ``None`` if there is no data on either side of the window midpoint.

    Formula: ``(second_half_avg - first_half_avg) / max(first_half_avg, 1.0)``.
    The ``max(..., 1.0)`` keeps the denominator stable for low-LOC repos.
    """
    if window_days < 2:
        raise ValueError("window_days must be >= 2")

    window_start = day - timedelta(days=window_days - 1)
    midpoint = window_start + timedelta(days=window_days // 2)

    query = """
        SELECT
            avg(if(day < {mid:Date}, cpk, NULL)) AS first_half,
            avg(if(day >= {mid:Date}, cpk, NULL)) AS second_half
        FROM (
            SELECT day, argMax(cyclomatic_per_kloc, computed_at) AS cpk
            FROM repo_complexity_daily
            WHERE repo_id = {repo_id:UUID}
              AND day >= {start:Date} AND day <= {end:Date}
              AND org_id = {org_id:String}
            GROUP BY day
        )
    """
    params = {
        "repo_id": str(repo_id),
        "start": window_start,
        "mid": midpoint,
        "end": day,
        "org_id": org_id,
    }

    rows = sink.query_dicts(query, params)
    if not rows:
        return None
    first = rows[0].get("first_half")
    second = rows[0].get("second_half")
    if first is None or second is None:
        return None
    first_f = float(first)
    second_f = float(second)
    return (second_f - first_f) / max(first_f, 1.0)


def build_compounding_risk_rows_for_day(
    *,
    sink: Any,
    day: date,
    org_id: str,
    repo_metrics_rows: Iterable[Any],
    computed_at: datetime,
    weights: CompoundingWeights = DEFAULT_WEIGHTS,
    thresholds: CompoundingThresholds = DEFAULT_THRESHOLDS,
    repo_to_team: dict[str, str] | None = None,
) -> list[CompoundingRiskDailyRecord]:
    """Compose Compounding Risk rows for every repo (and optionally team).

    Args:
        sink: ClickHouse sink (read-only for the complexity delta).
        day: Compute target day.
        org_id: Org id (also injected into the complexity query).
        repo_metrics_rows: In-memory repo_metrics rows that were just persisted.
        computed_at: UTC compute moment, passed explicitly for determinism.
        weights: Composite weights.
        thresholds: Severity bucket boundaries.
        repo_to_team: Optional mapping ``{repo_id_str: team_id}``. When
            provided, the function emits both ``scope='repo'`` rows and
            ``scope='team'`` rows (one per team) by aggregating the per-repo
            inputs. Without this map only repo rows are emitted.
    """
    repo_rows: list[CompoundingRiskDailyRecord] = []
    # Capture the raw inputs alongside the persisted row so we can compose
    # team aggregations without re-querying ClickHouse.
    repo_inputs_for_team: dict[str, CompoundingInputs] = {}
    for row in repo_metrics_rows:
        repo_id = getattr(row, "repo_id", None)
        if repo_id is None:
            continue
        repo_id_str = str(repo_id)
        complexity_delta = load_repo_complexity_delta_30d(
            sink, repo_id=repo_id_str, day=day, org_id=org_id
        )
        inputs = CompoundingInputs(
            rework_churn=_nullable_float(getattr(row, "rework_churn_ratio_30d", None)),
            complexity_delta=complexity_delta,
            review_latency_p90h=_nullable_float(
                getattr(row, "pr_first_review_p90_hours", None)
            ),
            single_owner_ratio=_nullable_float(
                getattr(row, "single_owner_file_ratio_30d", None)
            ),
            ownership_gini=_nullable_float(getattr(row, "code_ownership_gini", None)),
            bus_factor=_nullable_float(getattr(row, "bus_factor", None)),
        )
        repo_inputs_for_team[repo_id_str] = inputs
        repo_rows.append(
            compute_compounding_risk(
                day=day,
                scope="repo",
                scope_id=str(repo_id),
                org_id=org_id,
                inputs=inputs,
                computed_at=computed_at,
                weights=weights,
                thresholds=thresholds,
            )
        )

    if not repo_to_team:
        return repo_rows

    team_rows = _build_team_rows(
        day=day,
        org_id=org_id,
        repo_inputs=repo_inputs_for_team,
        repo_to_team=repo_to_team,
        computed_at=computed_at,
        weights=weights,
        thresholds=thresholds,
    )
    return repo_rows + team_rows


def _mean_or_none(values: list[float | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _build_team_rows(
    *,
    day: date,
    org_id: str,
    repo_inputs: dict[str, CompoundingInputs],
    repo_to_team: dict[str, str],
    computed_at: datetime,
    weights: CompoundingWeights,
    thresholds: CompoundingThresholds,
) -> list[CompoundingRiskDailyRecord]:
    """Aggregate per-repo inputs into one row per team.

    Strategy: unweighted mean of each *raw input* across the team's repos,
    then feed the means into the same ``compute_compounding_risk`` so the
    score is computed under the same formula as the repo rows. This keeps
    the team score auditable in the same way as repo rows.
    """
    by_team: dict[str, list[CompoundingInputs]] = {}
    for repo_id, inputs in repo_inputs.items():
        team_id = repo_to_team.get(repo_id)
        if not team_id:
            continue
        by_team.setdefault(team_id, []).append(inputs)

    out: list[CompoundingRiskDailyRecord] = []
    for team_id, all_inputs in by_team.items():
        team_inputs = CompoundingInputs(
            rework_churn=_mean_or_none([i.rework_churn for i in all_inputs]),
            complexity_delta=_mean_or_none([i.complexity_delta for i in all_inputs]),
            review_latency_p90h=_mean_or_none(
                [i.review_latency_p90h for i in all_inputs]
            ),
            single_owner_ratio=_mean_or_none(
                [i.single_owner_ratio for i in all_inputs]
            ),
            ownership_gini=_mean_or_none([i.ownership_gini for i in all_inputs]),
            bus_factor=_mean_or_none([i.bus_factor for i in all_inputs]),
        )
        out.append(
            compute_compounding_risk(
                day=day,
                scope="team",
                scope_id=team_id,
                org_id=org_id,
                inputs=team_inputs,
                computed_at=computed_at,
                weights=weights,
                thresholds=thresholds,
            )
        )
    return out


def _nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "COMPLEXITY_WINDOW_DAYS",
    "CompoundingInputs",
    "CompoundingThresholds",
    "CompoundingWeights",
    "DEFAULT_THRESHOLDS",
    "DEFAULT_WEIGHTS",
    "REFERENCE_VALUES",
    "build_compounding_risk_rows_for_day",
    "compute_compounding_risk",
    "load_repo_complexity_delta_30d",
    "severity_for",
]
