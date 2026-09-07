"""Golden vectors for the Go throughput forecast kernel (CHAOS-5349).

Captured from the REAL producer -- dev_health_ops.metrics.forecast -- so the Go
port in cmd/query-api/internal/throughputforecast is measured against the
function that actually serves `throughputForecast` today, not against a reading
of it. Captured BEFORE any Python deletion, which is the whole point: once
metrics/forecast.py is gone there is no oracle left to disagree with.

Unlike the capacity kernel next door, this model is DETERMINISTIC -- no RNG, no
numpy, no scipy; only math.floor/ceil, statistics.fmean and stdlib sorted. A
plain input->output table is therefore a complete oracle, and no MT19937 port is
involved.

The cases exist to pin behaviours a tidier port would "fix":

* A PARTIAL WINDOW EMITS NO ESTIMATE, NOT A SCALED ONE. rolling_weekly_throughput
  returns (mean 0.0, samples (), insufficient True) when it has fewer than
  window_weeks*7 daily samples. The previous `total / max(weeks, 1.0)` floor
  produced the SAME number for 4w/8w/12w and masqueraded as a confident forecast
  (CHAOS-2574). A port that averages whatever it has reproduces the old defect.
* THE PERCENTILE ARGUMENTS ARE FRACTIONS, AND THE CONFIDENCE LABEL IS INVERTED.
  p50 uses 0.50, p75 uses 0.25 and p90 uses 0.10 -- a HIGHER confidence band
  reads a LOWER throughput quantile. Reading 0.75/0.90 there is the natural
  mistake and inverts every p75_weeks/p90_weeks.
* THIS `_percentile` IS NOT compute_capacity's. Here it is
  `ordered[lo] + (ordered[hi] - ordered[lo]) * fraction`, float-valued; there it
  is `int(v[lo]*(1-frac) + v[hi]*frac)`, truncating. The two round differently
  and BOTH are FMA-fusable on arm64, where Go fuses and CPython does not.
* SELECTION FALLS BACK TO THE LONGEST SHORTER WINDOW. When the requested window
  has fewer than 2 rolling samples, selection retries the longest SHORTER window
  that has 2+, and only then gives up. Cases 3 and 4 sit on both sides of that
  boundary.
* insufficient_history IS PROVENANCE, NOT DATA VOLUME. A history_weeks outside
  ROLLING_WINDOWS_WEEKS (4, 8, 12) can never match a window, so it is flagged
  insufficient even with years of history behind it (case 5).
* _weeks_to_complete's FLOOR OF 1. A backlog that would finish in a fraction of
  a week still reports 1, while a backlog of 0 reports 0 and a non-positive
  throughput reports None. Three different zero-ish answers, one function.
* THE PRIMARY RISK IS max() OVER ACTIVE OVERLAYS BY SCORE, and score is
  value/threshold -- so overlays on different units are comparable only after
  normalisation. Case 8 has two active overlays whose raw values order the
  opposite way from their scores.

Run: python3 tests/fixtures/generate_throughput_forecast_golden.py > \
    tests/fixtures/throughput_forecast_golden.json
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from typing import Any

from dev_health_ops.metrics.compute_capacity import ThroughputHistory, ThroughputSample
from dev_health_ops.metrics.forecast import (
    compute_risk_overlays,
    compute_rolling_windows,
    forecast_throughput_capacity,
)

# Anchor day. Nothing in this kernel reads the clock -- forecast_id and
# computed_at are the only wall-clock outputs and both are excluded below -- so
# the anchor exists purely to give the samples stable, readable days.
ANCHOR = date(2026, 8, 23)


def history(values: list[int]) -> ThroughputHistory:
    """Build a history whose samples run forward from ANCHOR, one per day."""
    return ThroughputHistory(
        [
            ThroughputSample(
                day=ANCHOR + timedelta(days=index),
                items_completed=value,
                team_id=None,
                work_scope_id=None,
            )
            for index, value in enumerate(values)
        ]
    )


def ramp(count: int, start: int = 1, step: int = 1) -> list[int]:
    """A strictly increasing series, so every rolling window differs."""
    return [start + step * index for index in range(count)]


def sawtooth(count: int, period: int = 5) -> list[int]:
    """A repeating series with a non-power-of-two period.

    Rolling sums over a sawtooth land on non-integer means, which is what makes
    the percentile interpolation (and therefore the FMA-sensitive expression)
    actually interpolate rather than pick an existing sample.
    """
    return [(index % period) * 3 + 1 for index in range(count)]


def overlay_to_json(overlay: Any) -> dict[str, Any]:
    return {
        "kind": overlay.kind.value,
        "score": overlay.score,
        "label": overlay.label,
        "value": overlay.value,
        "threshold": overlay.threshold,
        "active": overlay.active,
    }


def result_to_json(result: Any) -> dict[str, Any]:
    """Serialise everything EXCEPT forecast_id and computed_at.

    Both are volatile per call on both sides -- uuid4 and the wall clock -- so
    including them would produce a fixture that can never pass. They belong to
    the resolver, not the kernel.
    """
    return {
        "team_id": result.team_id,
        "work_scope_id": result.work_scope_id,
        "backlog_size": result.backlog_size,
        "history_weeks": result.history_weeks,
        "p50_weeks": result.p50_weeks,
        "p75_weeks": result.p75_weeks,
        "p90_weeks": result.p90_weeks,
        "rolling_windows": [
            {
                "window_weeks": window.window_weeks,
                "mean_weekly_throughput": window.mean_weekly_throughput,
                "samples": list(window.samples),
                "sample_count": len(window.samples),
                "insufficient_history": window.insufficient_history,
            }
            for window in result.rolling_windows
        ],
        "primary_risk": overlay_to_json(result.primary_risk),
        "wip_congestion": overlay_to_json(result.wip_congestion),
        "review_bottleneck": overlay_to_json(result.review_bottleneck),
        "incident_load": overlay_to_json(result.incident_load),
        "insufficient_history": result.insufficient_history,
    }


CASES: list[dict[str, Any]] = [
    {
        "name": "full_12w_history_matched_window",
        "why": (
            "120 days of strictly increasing throughput: the 12w window has 37 "
            "rolling samples, history_weeks=12 matches it, so it is selected AND "
            "insufficient_history is False. This is the only fully-satisfied "
            "case in the table -- note that 84 days is NOT enough for it (case "
            "16), because 12 weeks of dailies yields exactly one rolling sample."
        ),
        "values": ramp(120),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "full_12w_history_matched_4w_window",
        "why": (
            "Same history, history_weeks=4. The 4w window has 57 rolling samples "
            "and is selected instead of the 12w one -- a port that always reads "
            "windows[-1] gets different percentiles here and only here."
        ),
        "values": ramp(84),
        "backlog_size": 500,
        "history_weeks": 4,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "28d_history_single_4w_sample_no_estimate",
        "why": (
            "Exactly 28 days: the 4w window has ONE rolling sample (below the "
            "minimum of 2) and 8w/12w have none. Selection finds no window with "
            "2+ samples, returns an empty distribution, every percentile is 0.0 "
            "and every weeks value is None despite a positive backlog."
        ),
        "values": ramp(28),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "29d_history_falls_back_to_shorter_window",
        "why": (
            "One day more than case 3: the 4w window now has TWO rolling samples, "
            "so selection falls back from the empty 12w window to it. This is the "
            "longest-shorter-window fallback, and the pair 28/29 sits either side "
            "of the boundary."
        ),
        "values": ramp(29),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "nonstandard_history_weeks_6_is_provenance_insufficient",
        "why": (
            "history_weeks=6 matches no window in (4, 8, 12), so the distribution "
            "silently comes from the 12w window and insufficient_history is True "
            "even though the history is ample. Provenance, not volume."
        ),
        "values": ramp(84),
        "backlog_size": 500,
        "history_weeks": 6,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "nonstandard_history_weeks_16_is_provenance_insufficient",
        "why": (
            "The same rule above the window range, not just below it: 16 matches "
            "nothing, so windows[-1] is used and the flag is set."
        ),
        "values": ramp(120),
        "backlog_size": 500,
        "history_weeks": 16,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "sawtooth_interpolating_percentiles",
        "why": (
            "A period-5 sawtooth puts the percentile ranks BETWEEN samples, so "
            "_percentile actually interpolates. This is the FMA-sensitive case: "
            "`lo + (hi - lo) * fraction` fuses on arm64 in Go and does not in "
            "CPython, and the ceil() downstream can turn a last-ulp difference "
            "into a whole week."
        ),
        "values": sawtooth(97),
        "backlog_size": 313,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "two_active_overlays_primary_by_normalised_score",
        "why": (
            "review_latency_hours=60 (score 1.25) and incident_count=15 (score "
            "1.5): the incident overlay has the SMALLER raw value but the LARGER "
            "normalised score, so it is primary. A port that maxes on value picks "
            "review here."
        ),
        "values": ramp(84),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 60.0,
        "incident_count": 15.0,
    },
    {
        "name": "wip_ratio_active_no_average_wip_guard",
        "why": (
            "current_wip=40, average_wip=25 gives a ratio of 1.6 over the 1.25 "
            "threshold. Pairs with case 10, where average_wip=0 must yield a "
            "ratio of 0.0 rather than a division by zero or an infinity."
        ),
        "values": ramp(84),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 40.0,
        "average_wip": 25.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "zero_average_wip_yields_zero_ratio",
        "why": (
            "average_wip=0 with a positive current_wip: the guard returns 0.0, "
            "so the WIP overlay is inactive. Without the guard this is +Inf and "
            "always primary."
        ),
        "values": ramp(84),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 40.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "threshold_exactly_met_is_active",
        "why": (
            "active is `value >= threshold`, not `>`. review_latency_hours=48.0 "
            "sits exactly on the threshold and must be ACTIVE with a score of "
            "exactly 1.0."
        ),
        "values": ramp(84),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 48.0,
        "incident_count": 0.0,
    },
    {
        "name": "zero_backlog_reports_zero_weeks_not_none",
        "why": (
            "backlog_size=0 short-circuits _weeks_to_complete to 0 for all three "
            "bands, BEFORE the throughput is consulted. Distinct from the None "
            "that a zero throughput produces (case 13)."
        ),
        "values": ramp(84),
        "backlog_size": 0,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "all_zero_throughput_reports_none_weeks",
        "why": (
            "84 days of zero completions: the windows exist and are sufficient, "
            "but every percentile is 0.0, so _weeks_to_complete returns None for "
            "a positive backlog. Zero throughput and zero backlog must not be "
            "collapsed into the same answer."
        ),
        "values": [0] * 84,
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "tiny_backlog_hits_the_floor_of_one_week",
        "why": (
            "A backlog of 1 against a large throughput would compute a fraction "
            "of a week; max(1, ceil(...)) reports 1. A port that drops the floor "
            "reports 0 and claims the work is already done."
        ),
        "values": [100] * 84,
        "backlog_size": 1,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "negative_daily_values_are_clamped_to_zero",
        "why": (
            "rolling_weekly_throughput applies max(0, int(...)) per sample. A "
            "negative row -- which ClickHouse can produce from a corrected "
            "aggregate -- must not subtract from the window's total."
        ),
        "values": [-5 if index % 7 == 0 else 10 for index in range(84)],
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
    {
        "name": "exactly_one_rolling_sample_at_84_days_for_12w",
        "why": (
            "84 days is exactly 12 weeks, so the 12w window yields exactly ONE "
            "rolling sample and is flagged insufficient; selection falls back to "
            "the 8w window, which has 29. The matched-window flag is still set "
            "because the REQUESTED window is the insufficient one -- provenance "
            "again, and the case most likely to be got wrong by a port that "
            "checks the SELECTED window instead."
        ),
        "values": ramp(84),
        "backlog_size": 500,
        "history_weeks": 12,
        "current_wip": 0.0,
        "average_wip": 0.0,
        "review_latency_hours": 0.0,
        "incident_count": 0.0,
    },
]


def main() -> None:
    cases: list[dict[str, Any]] = []
    for case in CASES:
        result = forecast_throughput_capacity(
            history=history(case["values"]),
            backlog_size=case["backlog_size"],
            team_id="team-alpha",
            work_scope_id="scope-beta",
            history_weeks=case["history_weeks"],
            current_wip=case["current_wip"],
            average_wip=case["average_wip"],
            review_latency_hours=case["review_latency_hours"],
            incident_count=case["incident_count"],
        )
        cases.append(
            {
                "name": case["name"],
                "why": case["why"],
                "input": {
                    "daily_throughputs": case["values"],
                    "backlog_size": case["backlog_size"],
                    "team_id": "team-alpha",
                    "work_scope_id": "scope-beta",
                    "history_weeks": case["history_weeks"],
                    "current_wip": case["current_wip"],
                    "average_wip": case["average_wip"],
                    "review_latency_hours": case["review_latency_hours"],
                    "incident_count": case["incident_count"],
                },
                "expected": result_to_json(result),
            }
        )

    # The resolver's no-history path does NOT call forecast_throughput_capacity
    # at all -- it hand-builds a result from compute_rolling_windows(history) and
    # compute_risk_overlays() with no arguments. Capturing that separately keeps
    # the Go resolver's empty-scope payload honest, since no case above can
    # reach it.
    empty = history([])
    primary, wip, review, incident = compute_risk_overlays()
    no_history = {
        "name": "resolver_no_history_payload",
        "why": (
            "resolve_throughput_forecast returns a STRUCTURED payload, not null, "
            "when the scope has no samples: forecast_id is the literal "
            '"no-history", all three weeks are None, every rolling window is '
            "empty and insufficient, and the overlays are the neutral defaults. "
            "This path never calls forecast_throughput_capacity, so it needs its "
            "own vector."
        ),
        "expected": {
            "forecast_id": "no-history",
            "p50_weeks": None,
            "p75_weeks": None,
            "p90_weeks": None,
            "insufficient_history": True,
            "rolling_windows": [
                {
                    "window_weeks": window.window_weeks,
                    "mean_weekly_throughput": window.mean_weekly_throughput,
                    "samples": list(window.samples),
                    "sample_count": len(window.samples),
                    "insufficient_history": window.insufficient_history,
                }
                for window in compute_rolling_windows(empty)
            ],
            "primary_risk": overlay_to_json(primary),
            "wip_congestion": overlay_to_json(wip),
            "review_bottleneck": overlay_to_json(review),
            "incident_load": overlay_to_json(incident),
        },
    }

    json.dump(
        {
            "source": "dev_health_ops.metrics.forecast.forecast_throughput_capacity",
            "generator": "tests/fixtures/generate_throughput_forecast_golden.py",
            "ticket": "CHAOS-5349",
            "python": sys.version.split()[0],
            "note": (
                "forecast_id and computed_at are excluded from every case: both "
                "are volatile per call (uuid4 / wall clock) on both sides and "
                "belong to the resolver rather than the kernel."
            ),
            "cases": cases,
            "resolver_paths": [no_history],
        },
        sys.stdout,
        indent=2,
        sort_keys=False,
    )
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
