"""Golden generator for the benchmarking family (CHAOS-4288).

Frozen output of the REAL Python compute primitives -- `compute_internal_baselines`,
`classify_maturity_bands`, `detect_metric_anomalies`, `compute_period_comparison`,
`compute_metric_correlation` and `generate_benchmark_insights` -- driven from FIXED
`MetricPoint` series rather than through the sink, so the corpus is the compute's
input rather than a database's state.

CORPUS AXES, and why each is here
---------------------------------
Chosen so a wrong port fails rather than passes by luck. The two axes that hide
parity bugs in this family are the NUMBER of summands (CPython's compensated
``sum()``) and the MAGNITUDE of operands (``math.isclose``'s default ``rel_tol``),
so both are swept deliberately.

* ``scope-a`` -- a long, irregular series with many summands and fractional values.
  Exercises Neumaier compensation in ``mean``/``population_stdev``: a Go ``+=``
  loop disagrees with CPython on roughly 16% of such inputs, and only lists of
  three or more summands can show it.
* ``scope-b`` -- a LARGE-magnitude series (values around 5e6). This is the axis
  for ``isclose``: a naive ``abs(a-b) <= abs_tol`` diverges from CPython here,
  and ``anomalies.py:72`` compares two such values to decide z_score 0.0 vs 3.0,
  i.e. "info" vs "critical".
* ``scope-c`` -- a ZERO-VARIANCE history (every value identical). Drives the
  ``isclose(stdev, 0.0)`` branch, and pairs with scope-b so both sides of that
  branch are covered.
* ``scope-d`` -- history exactly at the ``min_history_points`` boundary (5), plus
  one scope with 4 points that must produce NO anomaly.
* ``scope-e`` -- a baseline of exactly 0.0, so the volatility denominator is
  zero and ``volatility_score`` must stay 0.0 rather than dividing.
* Percentile ranks landing exactly on 25.0 / 50.0 / 75.0, so the maturity band's
  strict ``<`` boundaries are pinned (a rank ON a boundary belongs to the HIGHER
  band).
* A period comparison whose ``comparison_value`` is 0.0 -> ``percentage_change``
  is None, and one whose delta is 0.0 -> ``trend_direction`` "stable".
* A percentage change of exactly 5.0, the insight-suppression threshold
  (``abs(pct) < 5.0`` is strict, so exactly 5.0 IS emitted).
* A negative-direction metric (``flake_rate``) alongside a positive-direction one,
  since the improving/regressing and up/down mapping inverts between them.
* Correlations: one strongly positive (``is_significant`` True), one genuinely
  weak at r = 0.2143 (``is_significant`` False, so its insight is SUPPRESSED --
  without this the suppression branch is never taken), and one series pair with
  4 common days (below ``min_points``, so no row at all).
* ``scope-h`` -- values whose compensated mean differs from a naive ``+=`` mean
  EVEN AFTER ``round(x, 4)``. Only ~0.15% of random value sets do (587 of
  400,000 measured), so this had to be searched for; without it the fixture is
  blind to a naive-summation port, because the 4-decimal rounding on every
  output erases an ordinary last-bit difference.
* ``scope-i`` -- the ``isclose`` discriminator: zero-variance history at large
  magnitude with the current point differing by more than ``abs_tol`` but far
  less than ``rel_tol*|a|``. CPython emits NO anomaly; a naive absolute compare
  emits a spurious "critical" one.
* An anomaly whose |z| lands in [2, 3) so severity is "warning" rather than
  "critical", and a volatility score below 1.0 so that row is "warning" too --
  otherwise every anomaly in the corpus is "critical" and the ladder is
  untested.

WHAT IS DELIBERATELY ABSENT
---------------------------
No correlation is placed near ``p_value == 0.05`` and none near a 6-decimal
rounding boundary. ``fisher_two_tailed_p_value`` calls ``math.log``/``math.erfc``,
which CPython takes from the system libm and Go implements itself, so the RAW
values can differ in the last bit across runtimes. Nothing persists the raw value
-- ``correlations.py:70`` writes ``round(p_value, 6)``, which is ~15 orders of
magnitude coarser -- so the RECORD is bit-exact everywhere, PROVIDED no case sits
on a boundary where one ulp flips the rounded digit or the ``< 0.05`` decision.
Keeping cases away from those boundaries is what makes the frozen file portable.

Regenerate with `PYTHONPATH=src python tests/fixtures/generate_daily_benchmarking_python_golden.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.benchmarking import generate_benchmark_insights
from dev_health_ops.metrics.benchmarking._common import MetricPoint
from dev_health_ops.metrics.benchmarking.anomalies import detect_metric_anomalies
from dev_health_ops.metrics.benchmarking.baselines import compute_internal_baselines
from dev_health_ops.metrics.benchmarking.correlations import compute_metric_correlation
from dev_health_ops.metrics.benchmarking.maturity import classify_maturity_bands
from dev_health_ops.metrics.benchmarking.period_comparison import (
    compute_period_comparison,
)

OUTPUT = Path(__file__).with_name("daily_benchmarking_python_golden.json")

ORG_ID = "org-benchmarking-golden"
AS_OF = date(2026, 8, 24)
COMPUTED_AT = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)


def _series(values: list[float], *, end_day: date = AS_OF) -> list[MetricPoint]:
    """One point per day, ending on end_day, in ascending day order."""
    start = end_day - timedelta(days=len(values) - 1)
    return [
        MetricPoint(day=start + timedelta(days=index), value=value)
        for index, value in enumerate(values)
    ]


# scope-a: many fractional summands -- the compensated-sum axis.
SCOPE_A = _series(
    [0.1, 0.2, 0.3, 0.7, 1.1, 2.9, 5.5, 0.15, 0.25, 3.35, 1.05, 0.95, 4.45, 2.15]
)
# scope-b: large magnitudes -- the isclose axis.
SCOPE_B = _series(
    [
        5_000_000.0,
        5_000_000.5,
        5_000_001.0,
        5_000_000.25,
        5_000_000.75,
        5_000_002.0,
        5_000_003.5,
        5_000_000.125,
    ]
)
# scope-c: zero variance -- drives isclose(stdev, 0.0) and the 0.0/3.0 z_score split.
SCOPE_C = _series([42.0] * 8)
# scope-d: exactly min_history_points (5) of history plus the current point.
SCOPE_D = _series([1.0, 2.0, 3.0, 4.0, 5.0, 12.0])
# scope-e: baseline exactly 0.0 -- volatility denominator is zero.
SCOPE_E = _series([0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
# scope-f: only 4 history points -- below min_history_points, no anomaly.
SCOPE_F = _series([1.0, 2.0, 3.0, 9.0])
# scope-g: current point placed so |z| lands in [2, 3) -> severity "warning"
# rather than "critical", and volatility 0.4714 < 1.0 -> that row is "warning"
# too. Without this every anomaly in the corpus is "critical" and the
# severity ladder is untested. (Measured: z = 2.8284.)
SCOPE_G = _series([1.0, 2.0, 3.0, 4.0, 5.0, 7.0])
# scope-h: values whose COMPENSATED mean differs from a naive `+=` mean AFTER
# round(x, 4). Found by search: only ~0.15% of random value sets survive the
# rounding (587 of 400,000), which is exactly why a hand-built corpus never
# hits one by accident and why every earlier version of this fixture was BLIND
# to a naive-summation port. compensated -> 3.347, naive -> 3.3469.
SCOPE_H = _series([5.8481, 3.6, 3.7054, 5.584, 1.275, 2.742, 2.3621, 1.659])
# scope-i: the isclose discriminator. A ZERO-VARIANCE history at large
# magnitude, with the current point differing by more than abs_tol (1e-9) but
# far less than rel_tol*|a| (5e-3). CPython's isclose says CLOSE -> z_score
# 0.0 -> NO anomaly row. A naive `abs(a-b) <= 1e-9` says NOT close -> z_score
# 3.0 -> a spurious "critical" anomaly. One row exists or does not.
SCOPE_I = _series([5_000_000.0] * 6 + [5_000_000.000000005])

BASELINE_SERIES: dict[str, list[MetricPoint]] = {
    "scope-a": SCOPE_A,
    "scope-b": SCOPE_B,
    "scope-c": SCOPE_C,
    "scope-d": SCOPE_D,
    "scope-e": SCOPE_E,
    "scope-f": SCOPE_F,
    "scope-g": SCOPE_G,
    "scope-h": SCOPE_H,
    "scope-i": SCOPE_I,
}

# Period comparison inputs, chosen for the three special outcomes.
PERIOD_CASES: list[tuple[str, str, list[float], list[float]]] = [
    # Ordinary improvement on a positive-direction metric, well past the 5% gate.
    ("success_rate", "scope-a", [0.90, 0.92, 0.94], [0.70, 0.72, 0.74]),
    # comparison_value is 0.0 -> percentage_change is None (and the insight is
    # still emitted, because the None does NOT trip the < 5.0 suppression).
    ("success_rate", "scope-zero", [0.5, 0.5, 0.5], [0.0, 0.0, 0.0]),
    # Identical periods -> absolute_delta 0.0 -> "stable" -> insight suppressed.
    ("success_rate", "scope-flat", [0.5, 0.5], [0.5, 0.5]),
    # Negative-direction metric getting worse -> "regressing" -> severity warning.
    ("flake_rate", "scope-a", [0.30, 0.32], [0.10, 0.11]),
    # Exactly 5.0% change: the suppression test is strict `< 5.0`, so this IS
    # emitted. current 1.05 vs comparison 1.0.
    ("success_rate", "scope-five", [1.05], [1.0]),
]

# Correlation inputs: strong, weak, and below-min-points.
CORRELATION_LEFT: dict[str, list[MetricPoint]] = {
    "scope-a": _series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
    "scope-weak": _series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
    "scope-short": _series([1.0, 2.0, 3.0, 4.0]),
}
CORRELATION_RIGHT: dict[str, list[MetricPoint]] = {
    "scope-a": _series([2.3, 4.1, 6.4, 8.2, 10.5, 12.1, 14.4]),
    "scope-weak": _series([5.0, 4.0, 6.0, 3.0, 7.0, 2.0, 8.0]),
    "scope-short": _series([9.0, 8.0, 7.0, 6.0]),
}


def _serialize(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _records(rows: list[Any]) -> list[dict[str, Any]]:
    return [
        {field: _serialize(item) for field, item in row.__dict__.items()}
        for row in rows
    ]


def main() -> str:
    baselines = compute_internal_baselines(
        metric_name="success_rate",
        scope_type="repo",
        series_by_scope=BASELINE_SERIES,
        as_of_day=AS_OF,
        computed_at=COMPUTED_AT,
        org_id=ORG_ID,
    )
    maturity_bands = classify_maturity_bands(baselines, computed_at=COMPUTED_AT)

    anomalies = detect_metric_anomalies(
        metric_name="success_rate",
        scope_type="repo",
        series_by_scope=BASELINE_SERIES,
        as_of_day=AS_OF,
        computed_at=COMPUTED_AT,
        org_id=ORG_ID,
    )
    # A negative-direction metric over the same series, so the improving/
    # regressing and up/down inversion is covered too.
    anomalies += detect_metric_anomalies(
        metric_name="flake_rate",
        scope_type="team",
        series_by_scope=BASELINE_SERIES,
        as_of_day=AS_OF,
        computed_at=COMPUTED_AT,
        org_id=ORG_ID,
    )

    current_end = AS_OF
    current_start = AS_OF - timedelta(days=6)
    prior_end = current_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=6)

    period_comparisons = []
    for metric_name, scope_key, current_values, comparison_values in PERIOD_CASES:
        record = compute_period_comparison(
            metric_name=metric_name,
            scope_type="repo",
            scope_key=scope_key,
            current_period_start=current_start,
            current_period_end=current_end,
            comparison_period_start=prior_start,
            comparison_period_end=prior_end,
            current_period_points=_series(current_values, end_day=current_end),
            comparison_period_points=_series(comparison_values, end_day=prior_end),
            computed_at=COMPUTED_AT,
            org_id=ORG_ID,
        )
        if record is not None:
            period_comparisons.append(record)

    correlation_start = AS_OF - timedelta(days=29)
    correlations = compute_metric_correlation(
        metric_name="flake_rate",
        paired_metric_name="cycle_time_hours",
        scope_type="team",
        left_series_by_scope=CORRELATION_LEFT,
        right_series_by_scope=CORRELATION_RIGHT,
        period_start=correlation_start,
        period_end=AS_OF,
        computed_at=COMPUTED_AT,
        org_id=ORG_ID,
    )

    insights = generate_benchmark_insights(
        period_comparisons=period_comparisons,
        anomalies=anomalies,
        correlations=correlations,
        computed_at=COMPUTED_AT,
    )

    document = {
        "baselines": _records(baselines),
        "maturity_bands": _records(maturity_bands),
        "anomalies": _records(anomalies),
        "period_comparisons": _records(period_comparisons),
        "correlations": _records(correlations),
        "insights": _records(insights),
    }
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    import sys

    rendered = main()
    if "--stdout" in sys.argv:
        sys.stdout.write(rendered)
    else:
        OUTPUT.write_text(rendered)
        print(f"wrote {OUTPUT}")
