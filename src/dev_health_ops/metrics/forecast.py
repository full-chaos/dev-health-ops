"""Throughput-based capacity forecasting.

This module intentionally sits beside the existing Monte-Carlo capacity model.
It uses an empirical rolling-window approach instead of simulation:

* daily completed-item samples are converted into rolling 4/8/12 week mean
  weekly throughput distributions;
* the forecast confidence bands are derived from conservative quantiles of the
  selected rolling distribution: P50 weeks uses the median throughput, P75 uses
  the 25th percentile throughput, and P90 uses the 10th percentile throughput;
* weeks-to-complete is ``ceil(backlog_size / weekly_throughput)``.

The P75/P90 outputs are therefore "finish within this many weeks if future
throughput is no worse than the slowest 25%/10% of comparable historical
windows". These are planning forecasts, not commitments.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from statistics import fmean

from dev_health_ops.metrics.compute_capacity import ThroughputHistory

MIN_DAYS_PER_WEEK = 7
ROLLING_WINDOWS_WEEKS = (4, 8, 12)
WIP_CONGESTION_THRESHOLD = 1.25
REVIEW_BOTTLENECK_THRESHOLD_HOURS = 48.0
INCIDENT_LOAD_THRESHOLD = 10.0


class RiskKind(str, Enum):
    """Forecast risk categories exposed to UI surfaces."""

    WIP = "wip"
    REVIEW = "review"
    INCIDENT = "incident_load"
    NONE = "none"


@dataclass(frozen=True)
class RollingWindowThroughput:
    """Empirical weekly-throughput distribution for one rolling window size."""

    window_weeks: int
    mean_weekly_throughput: float
    samples: tuple[float, ...]
    insufficient_history: bool


@dataclass(frozen=True)
class RiskOverlay:
    """Risk signal used as a forecast overlay."""

    kind: RiskKind
    score: float
    label: str
    value: float
    threshold: float
    active: bool


@dataclass(frozen=True)
class ThroughputForecastResult:
    """Result of a rolling throughput capacity forecast."""

    forecast_id: str
    computed_at: datetime
    team_id: str | None
    work_scope_id: str | None
    backlog_size: int
    history_weeks: int
    p50_weeks: int | None
    p75_weeks: int | None
    p90_weeks: int | None
    rolling_windows: tuple[RollingWindowThroughput, ...]
    primary_risk: RiskOverlay
    wip_congestion: RiskOverlay
    review_bottleneck: RiskOverlay
    incident_load: RiskOverlay
    insufficient_history: bool


def _percentile(values: list[float], percentile: float) -> float:
    """Return an interpolated percentile for sorted or unsorted values."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def rolling_weekly_throughput(
    history: ThroughputHistory,
    window_weeks: int,
) -> RollingWindowThroughput:
    """Compute rolling mean weekly throughput for a window size."""
    if window_weeks <= 0:
        raise ValueError("window_weeks must be positive")

    throughputs = [max(0, int(sample.items_completed)) for sample in history.samples]
    window_days = window_weeks * MIN_DAYS_PER_WEEK
    if not throughputs:
        return RollingWindowThroughput(window_weeks, 0.0, (), True)

    if len(throughputs) < window_days:
        # Insufficient history for this window size. Emit a no-estimate
        # contract instead of fabricating a point estimate from a partial
        # window. The previous `total / max(weeks, 1.0)` floor ignored
        # ``window_weeks`` and produced the SAME value for every window size,
        # collapsing 4w/8w/12w into one number that masqueraded as a confident
        # forecast (CHAOS-2574). Honest behaviour: no samples, zero mean,
        # flagged insufficient. Percentile selection falls back to a shorter
        # window that DOES have enough history; when none do, the forecast
        # returns no estimate.
        return RollingWindowThroughput(
            window_weeks=window_weeks,
            mean_weekly_throughput=0.0,
            samples=(),
            insufficient_history=True,
        )

    rolling = [
        sum(throughputs[index : index + window_days]) / window_weeks
        for index in range(0, len(throughputs) - window_days + 1)
    ]
    insufficient = len(rolling) < MIN_SAMPLES_FOR_ESTIMATE
    return RollingWindowThroughput(
        window_weeks=window_weeks,
        mean_weekly_throughput=fmean(rolling),
        samples=tuple(rolling),
        insufficient_history=insufficient,
    )


def compute_rolling_windows(
    history: ThroughputHistory,
    windows_weeks: tuple[int, ...] = ROLLING_WINDOWS_WEEKS,
) -> tuple[RollingWindowThroughput, ...]:
    """Compute all requested rolling throughput distributions."""
    return tuple(rolling_weekly_throughput(history, weeks) for weeks in windows_weeks)


MIN_SAMPLES_FOR_ESTIMATE = 2


def _select_percentile_distribution(
    windows: tuple[RollingWindowThroughput, ...], history_weeks: int
) -> tuple[float, ...]:
    """Return the sample distribution to use for percentile estimates.

    Selects the window whose ``window_weeks`` matches ``history_weeks``
    (falling back to the longest available window). When the selected
    window has fewer than ``MIN_SAMPLES_FOR_ESTIMATE`` samples the function
    tries to fall back to the longest shorter window that meets the
    minimum. If no window meets the minimum, an empty tuple is returned so
    that callers produce no-estimate (None) outputs rather than emitting
    percentiles derived from a single data point.
    """
    if not windows:
        return ()

    selected = next(
        (window for window in windows if window.window_weeks == history_weeks),
        windows[-1],
    )
    if len(selected.samples) >= MIN_SAMPLES_FOR_ESTIMATE:
        return selected.samples

    # Selected window has 0 or 1 sample — try the longest shorter window
    # that meets the minimum sample threshold.
    shorter_windows = sorted(
        (
            window
            for window in windows
            if window.window_weeks < selected.window_weeks
            and len(window.samples) >= MIN_SAMPLES_FOR_ESTIMATE
        ),
        key=lambda window: window.window_weeks,
        reverse=True,
    )
    if shorter_windows:
        return shorter_windows[0].samples

    # No window meets the minimum — emit no estimate.
    return ()


def _weeks_to_complete(backlog_size: int, weekly_throughput: float) -> int | None:
    if backlog_size <= 0:
        return 0
    if weekly_throughput <= 0:
        return None
    return max(1, math.ceil(backlog_size / weekly_throughput))


def _assert_monotonic_weeks(
    p50_weeks: int | None, p75_weeks: int | None, p90_weeks: int | None
) -> None:
    if p50_weeks is None or p75_weeks is None or p90_weeks is None:
        return
    if not p50_weeks <= p75_weeks <= p90_weeks:
        raise ValueError(
            "forecast weeks must be monotonic: "
            f"P50={p50_weeks}, P75={p75_weeks}, P90={p90_weeks}"
        )


def _risk_overlay(
    kind: RiskKind,
    value: float,
    threshold: float,
    label: str,
) -> RiskOverlay:
    score = value / threshold if threshold > 0 else 0.0
    return RiskOverlay(
        kind=kind,
        score=score,
        label=label,
        value=value,
        threshold=threshold,
        active=value >= threshold,
    )


def compute_risk_overlays(
    *,
    current_wip: float = 0.0,
    average_wip: float = 0.0,
    review_latency_hours: float = 0.0,
    incident_count: float = 0.0,
) -> tuple[RiskOverlay, RiskOverlay, RiskOverlay, RiskOverlay]:
    """Compute WIP, review, incident, and primary risk overlays.

    WIP congestion compares current WIP to recent average WIP. Review
    bottleneck uses PR review latency in hours. Incident load uses the recent
    count/rate supplied by the resolver. The highest active normalized score is
    the primary callout; if none are active, a neutral callout is returned.
    """
    wip_ratio = current_wip / average_wip if average_wip > 0 else 0.0
    wip = _risk_overlay(
        RiskKind.WIP,
        wip_ratio,
        WIP_CONGESTION_THRESHOLD,
        "WIP congestion",
    )
    review = _risk_overlay(
        RiskKind.REVIEW,
        review_latency_hours,
        REVIEW_BOTTLENECK_THRESHOLD_HOURS,
        "Review bottleneck",
    )
    incident = _risk_overlay(
        RiskKind.INCIDENT,
        incident_count,
        INCIDENT_LOAD_THRESHOLD,
        "Incident load",
    )

    active = [overlay for overlay in (wip, review, incident) if overlay.active]
    if active:
        primary = max(active, key=lambda overlay: overlay.score)
    else:
        primary = RiskOverlay(
            kind=RiskKind.NONE,
            score=0.0,
            label="No elevated risk",
            value=0.0,
            threshold=0.0,
            active=False,
        )
    return primary, wip, review, incident


def forecast_throughput_capacity(
    *,
    history: ThroughputHistory,
    backlog_size: int,
    team_id: str | None = None,
    work_scope_id: str | None = None,
    history_weeks: int = 12,
    current_wip: float = 0.0,
    average_wip: float = 0.0,
    review_latency_hours: float = 0.0,
    incident_count: float = 0.0,
) -> ThroughputForecastResult:
    """Forecast weeks to complete a backlog from rolling throughput history."""
    if backlog_size < 0:
        raise ValueError("backlog_size must be non-negative")
    if history_weeks <= 0:
        raise ValueError("history_weeks must be positive")

    windows = compute_rolling_windows(history)
    distribution = list(_select_percentile_distribution(windows, history_weeks))
    # Detect whether the requested window itself has too few rolling samples
    # for a reliable percentile estimate (< MIN_SAMPLES_FOR_ESTIMATE). When
    # true, _select_percentile_distribution fell back to a shorter window (or
    # returned no estimate). Either way the provenance signal must be surfaced:
    # the caller asked for history_weeks but the estimate came from less data.
    #
    # Additionally, when history_weeks does not correspond to any standard
    # window (ROLLING_WINDOWS_WEEKS), the distribution selection silently
    # falls back to the longest standard window. That is also a provenance
    # mismatch: the estimate did NOT use the requested history_weeks window.
    # Mark insufficient_history=True so callers can detect the fallback.
    window_matched = any(w.window_weeks == history_weeks for w in windows)
    requested_window = next(
        (w for w in windows if w.window_weeks == history_weeks), windows[-1]
    )
    selected_window_insufficient = (
        not window_matched or len(requested_window.samples) < MIN_SAMPLES_FOR_ESTIMATE
    )
    p50_throughput = _percentile(distribution, 0.50)
    p75_throughput = _percentile(distribution, 0.25)
    p90_throughput = _percentile(distribution, 0.10)
    p50_weeks = _weeks_to_complete(backlog_size, p50_throughput)
    p75_weeks = _weeks_to_complete(backlog_size, p75_throughput)
    p90_weeks = _weeks_to_complete(backlog_size, p90_throughput)
    _assert_monotonic_weeks(p50_weeks, p75_weeks, p90_weeks)
    primary, wip, review, incident = compute_risk_overlays(
        current_wip=current_wip,
        average_wip=average_wip,
        review_latency_hours=review_latency_hours,
        incident_count=incident_count,
    )
    return ThroughputForecastResult(
        forecast_id=str(uuid.uuid4()),
        computed_at=datetime.now(timezone.utc),
        team_id=team_id,
        work_scope_id=work_scope_id,
        backlog_size=backlog_size,
        history_weeks=history_weeks,
        p50_weeks=p50_weeks,
        p75_weeks=p75_weeks,
        p90_weeks=p90_weeks,
        rolling_windows=windows,
        primary_risk=primary,
        wip_congestion=wip,
        review_bottleneck=review,
        incident_load=incident,
        insufficient_history=selected_window_insufficient,
    )
