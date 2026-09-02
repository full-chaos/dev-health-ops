#!/usr/bin/env python3
"""Generate the recommendation-rule parity corpus from the LIVE reference.

This imports the shipped evaluators in
``src/dev_health_ops/recommendations/rules/`` and records what they actually
return. It never restates their logic, so a corpus entry cannot agree with a
mistaken reading of the reference -- which is the failure this file exists to
prevent: the Go port's first draft paraphrased two success-criterion strings
from the firing logic instead of copying them, and both paraphrases were wrong.

Every float crosses the boundary as its exact IEEE-754 bit pattern, so a value
cannot be perturbed on the way into the fixture. ``float.hex()`` was used first
and is exact for every float EXCEPT NaN, which it spells "nan" -- dropping the
payload, so the two implementations were handed different inputs and the
comparison reported a divergence neither of them had.

Usage:
    python generate_recommendations_rules_golden.py            # write beside this file
    python generate_recommendations_rules_golden.py --stdout   # print instead
    python generate_recommendations_rules_golden.py --out PATH
"""

from __future__ import annotations

import argparse
import json
import platform
import random
import struct
import sys
import unicodedata
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO_ROOT / "src"))

from dev_health_ops.recommendations.rules import RULE_EVALUATORS  # noqa: E402
from dev_health_ops.recommendations.snapshot import MetricsSnapshot  # noqa: E402

OUTPUT_PATH = Path(__file__).parent / "recommendations_rules_golden.json"

TEAM_ID = "team-70d529e0"
ORG_ID = "org-70d529e0"
WINDOW_START = date(2026, 8, 1)
WINDOW_END = date(2026, 9, 1)
NOW = datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc)

NAN = float("nan")
INF = float("inf")

# Slope lists chosen so the OLS slope lands ON a threshold, just under it, and
# just over it. The first is the list measured to give EXACTLY 0.1 under
# CPython's compensated sum() and 0.09999999999999999 under naive accumulation
# -- the case that decides whether saturation and sustainability-risk fire at
# all, so it must be in the corpus for both rules.
SLOPE_EXACTLY_AT_THRESHOLD = [0.1, 0.0, 1.0, 0.8, 0.2]
SLOPE_JUST_UNDER = [0.0, 0.05, 0.1, 0.15, 0.19]
SLOPE_JUST_OVER = [0.0, 0.11, 0.22, 0.33, 0.44]
SLOPE_FLAT = [3.0, 3.0, 3.0]
SLOPE_FALLING = [9.0, 6.0, 3.0, 0.0]

# Scalars per rule: None, just below the threshold, exactly on it, just above,
# plus the non-finite values, which are load-bearing rather than decorative --
# `nan < 24.0` is False, so a NaN latency does NOT take the early return and
# the rule fires with "nan" rendered into the persisted rationale.
LATENCY_VALUES = [None, 0.0, 23.999999, 24.0, 24.000001, 1e9, NAN, INF, -INF]
GINI_VALUES = [None, 0.0, 0.5999999, 0.6, 0.6000001, 1.0, NAN, INF]
CHURN_VALUES = [None, 0.0, 0.2999999, 0.3, 0.3000001, 1.0, NAN, INF]
AFTER_HOURS_VALUES = [None, 0.0, 0.1999999, 0.2, 0.2000001, 1.0, NAN, INF]
COMPLEXITY_VALUES = [None, 0.0, 0.1999999, 0.2, 0.2000001, 5.0, NAN, INF]
OVERLAP_VALUES = [None, 0.0, 0.3999999, 0.4, 0.4000001, 1.0, NAN, INF]

# Severity is a plain str in the reference with no validation, so the corpus
# covers the two firing values, the near misses, the case variants that do NOT
# match, and None.
SEVERITY_VALUES = [
    None,
    "",
    "low",
    "moderate",
    "elevated",
    "high",
    "High",
    "HIGH",
    "elevated ",
]
SCORE_VALUES = [None, 0.0, -0.0, 0.5, 0.7071067811865476, 1.0, NAN, INF]

# Values whose .3f / .2f / .1f rendering or round(x, N) is a known CPython
# trap: exact-binary ties that round half-to-even rather than half-away.
ROUNDING_TRAPS = [2.675, 0.125, 0.135, 2.5, 0.5, 1.005, 8.835, 1e16 + 2.0, 5e-324]


def snapshot(**overrides) -> MetricsSnapshot:
    base = dict(
        team_id=TEAM_ID,
        org_id=ORG_ID,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        wip_by_day=list(SLOPE_JUST_OVER),
        throughput_by_cycle=[5.0, 3.0],
        review_latency_p75_hours=30.0,
        reviewer_gini=0.75,
        rework_churn_ratio=0.5,
        after_hours_ratio=0.35,
        cycle_time_by_day=list(SLOPE_JUST_OVER),
        hotspot_complexity_delta=0.5,
        hotspot_churn_overlap=0.6,
        compounding_risk_score=None,
        compounding_risk_severity=None,
    )
    base.update(overrides)
    return MetricsSnapshot(**base)


def build_cases() -> list[tuple[str, MetricsSnapshot]]:
    cases: list[tuple[str, MetricsSnapshot]] = []

    cases.append(("baseline-all-fire", snapshot()))

    # --- list-shaped axes, shared by saturation / thrash / sustainability ---
    list_axes = {
        "empty": [],
        "single": [4.0],
        "at-threshold": SLOPE_EXACTLY_AT_THRESHOLD,
        "just-under": SLOPE_JUST_UNDER,
        "just-over": SLOPE_JUST_OVER,
        "flat": SLOPE_FLAT,
        "falling": SLOPE_FALLING,
        "two-equal": [2.0, 2.0],
        "with-nan": [1.0, NAN, 3.0],
        "with-inf": [1.0, INF, 3.0],
        "negative": [-1.0, -2.0, -3.0],
        "huge": [1e308, 1.5e308],
        "tiny": [5e-324, 1e-323],
        "rounding-traps": list(ROUNDING_TRAPS),
    }
    for name, values in list_axes.items():
        cases.append((f"wip-{name}", snapshot(wip_by_day=values)))
        cases.append((f"throughput-{name}", snapshot(throughput_by_cycle=values)))
        cases.append((f"cycletime-{name}", snapshot(cycle_time_by_day=values)))

    # --- scalar axes ---
    for value in LATENCY_VALUES:
        cases.append((f"latency-{value!r}", snapshot(review_latency_p75_hours=value)))
    for value in GINI_VALUES:
        cases.append((f"gini-{value!r}", snapshot(reviewer_gini=value)))
    for value in CHURN_VALUES:
        cases.append((f"churn-{value!r}", snapshot(rework_churn_ratio=value)))
    for value in AFTER_HOURS_VALUES:
        cases.append((f"afterhours-{value!r}", snapshot(after_hours_ratio=value)))
    for value in COMPLEXITY_VALUES:
        cases.append(
            (f"complexity-{value!r}", snapshot(hotspot_complexity_delta=value))
        )
    for value in OVERLAP_VALUES:
        cases.append((f"overlap-{value!r}", snapshot(hotspot_churn_overlap=value)))

    # Rounding traps reaching the evidence values and the rationale text.
    for value in ROUNDING_TRAPS:
        cases.append(
            (f"latency-trap-{value!r}", snapshot(review_latency_p75_hours=value + 24.0))
        )
        cases.append((f"gini-trap-{value!r}", snapshot(reviewer_gini=value)))
        cases.append((f"churn-trap-{value!r}", snapshot(rework_churn_ratio=value)))

    # --- compounding-risk: the composite path shadows the proxy path ---
    for severity in SEVERITY_VALUES:
        for score in SCORE_VALUES:
            cases.append(
                (
                    f"composite-{severity!r}-{score!r}",
                    snapshot(
                        compounding_risk_severity=severity, compounding_risk_score=score
                    ),
                )
            )
    # Composite fires even when the proxy fields are absent, and suppresses the
    # proxy even when the proxy fields would themselves have fired.
    for severity in ("elevated", "high"):
        cases.append(
            (
                f"composite-{severity}-proxy-absent",
                snapshot(
                    compounding_risk_severity=severity,
                    compounding_risk_score=0.9,
                    hotspot_complexity_delta=None,
                    hotspot_churn_overlap=None,
                ),
            )
        )
        cases.append(
            (
                f"composite-{severity}-proxy-would-fire",
                snapshot(
                    compounding_risk_severity=severity,
                    compounding_risk_score=None,
                    hotspot_complexity_delta=0.99,
                    hotspot_churn_overlap=0.99,
                ),
            )
        )

    # --- None combinations across every optional scalar at once ---
    cases.append(
        (
            "all-scalars-none",
            snapshot(
                review_latency_p75_hours=None,
                reviewer_gini=None,
                rework_churn_ratio=None,
                after_hours_ratio=None,
                hotspot_complexity_delta=None,
                hotspot_churn_overlap=None,
            ),
        )
    )
    cases.append(
        (
            "all-lists-empty",
            snapshot(wip_by_day=[], throughput_by_cycle=[], cycle_time_by_day=[]),
        )
    )

    # --- seeded random sweep: combinations no hand-written axis reaches ---
    rng = random.Random(20260902)
    pool_scalar = [
        None,
        0.0,
        0.05,
        0.19,
        0.2,
        0.3,
        0.45,
        0.6,
        0.75,
        0.99,
        1.5,
        24.0,
        30.0,
        NAN,
        INF,
    ]
    for index in range(240):
        length = rng.choice([0, 1, 2, 3, 5, 8])

        def draw() -> list[float]:
            return [rng.uniform(-5.0, 20.0) for _ in range(length)]

        cases.append(
            (
                f"random-{index:03d}",
                snapshot(
                    wip_by_day=draw(),
                    throughput_by_cycle=draw(),
                    cycle_time_by_day=draw(),
                    review_latency_p75_hours=rng.choice(pool_scalar),
                    reviewer_gini=rng.choice(pool_scalar),
                    rework_churn_ratio=rng.choice(pool_scalar),
                    after_hours_ratio=rng.choice(pool_scalar),
                    hotspot_complexity_delta=rng.choice(pool_scalar),
                    hotspot_churn_overlap=rng.choice(pool_scalar),
                    compounding_risk_score=rng.choice(pool_scalar),
                    compounding_risk_severity=rng.choice(SEVERITY_VALUES),
                ),
            )
        )

    return cases


def encode_bits(value: float) -> str:
    """Exact IEEE-754 bit pattern, big-endian, as 16 hex digits.

    ``float.hex()`` is exact for every float EXCEPT NaN: it renders any NaN as
    the bare string "nan", discarding the payload. That is normally harmless --
    the payload is not part of the contract, because what actually reaches
    storage is json.dumps, which writes the token NaN with no payload either.

    It is recorded anyway so the corpus states what CPython produced rather than
    eliding it. The comparison in the Go test is then a deliberate, documented
    choice to ignore the payload, backed by an oracle that knows it, instead of
    a choice forced by an oracle that does not.
    """
    return struct.pack(">d", float(value)).hex()


def encode_float(value: float | None) -> str | None:
    """Exact bit pattern. None stays None so absence is never mistaken for a value.

    Snapshot INPUTS are encoded as bits rather than as float.hex() literals,
    which are more readable but lossy for NaN. That is not a cosmetic
    preference: the corpus's whole job is to feed the reference and the port the
    identical input. A NaN input that round-trips through "nan" arrives in Go as
    math.NaN() (0x7ff8000000000001) while CPython held the hardware default
    (0x7ff8000000000000), so the two implementations are handed DIFFERENT values
    and any comparison of what they return is meaningless -- it reports a
    divergence neither of them has, and would equally hide a real one.

    Evidence OUTPUTS carry both encodings: bits to compare against, hex as a
    cross-check that the two agree.
    """
    return None if value is None else encode_bits(value)


def encode_list(values: list[float]) -> list[str]:
    return [encode_bits(v) for v in values]


def encode_snapshot(snap: MetricsSnapshot) -> dict:
    return {
        "team_id": snap.team_id,
        "org_id": snap.org_id,
        "window_start": snap.window_start.isoformat(),
        "window_end": snap.window_end.isoformat(),
        "wip_by_day": encode_list(snap.wip_by_day),
        "throughput_by_cycle": encode_list(snap.throughput_by_cycle),
        "review_latency_p75_hours": encode_float(snap.review_latency_p75_hours),
        "reviewer_gini": encode_float(snap.reviewer_gini),
        "rework_churn_ratio": encode_float(snap.rework_churn_ratio),
        "after_hours_ratio": encode_float(snap.after_hours_ratio),
        "cycle_time_by_day": encode_list(snap.cycle_time_by_day),
        "hotspot_complexity_delta": encode_float(snap.hotspot_complexity_delta),
        "hotspot_churn_overlap": encode_float(snap.hotspot_churn_overlap),
        "compounding_risk_score": encode_float(snap.compounding_risk_score),
        "compounding_risk_severity": snap.compounding_risk_severity,
    }


def encode_result(result) -> dict | None:
    if result is None:
        return None
    payload = asdict(result)
    return {
        "rule_id": payload["rule_id"],
        "team_id": payload["team_id"],
        "org_id": payload["org_id"],
        "severity": payload["severity"],
        "title": payload["title"],
        "rationale": payload["rationale"],
        "success_criterion": payload["success_criterion"],
        "evidence": [
            {
                "team_id": ref["team_id"],
                "metric_table": ref["metric_table"],
                "window_start": ref["window_start"].isoformat(),
                "window_end": ref["window_end"].isoformat(),
                "field": ref["field"],
                "value_hex": float(ref["value"]).hex(),
                "value_bits": encode_bits(ref["value"]),
            }
            for ref in payload["evidence"]
        ],
    }


def build_document() -> dict:
    rule_order = list(RULE_EVALUATORS.keys())
    cases = build_cases()

    entries = []
    for name, snap in cases:
        results = {}
        for rule_id, evaluator in RULE_EVALUATORS.items():
            results[rule_id] = encode_result(evaluator(snap, NOW))
        entries.append(
            {"name": name, "snapshot": encode_snapshot(snap), "results": results}
        )

    # Success criteria are module-level constants built at import time from the
    # thresholds. Pinned separately as well as inside every fired result, so a
    # corpus in which no rule happens to fire still pins them.
    from dev_health_ops.recommendations.rules import (
        compounding_risk,
        review_concentration,
        saturation,
        sustainability_risk,
        thrash,
    )

    success_criteria = {
        "saturation": saturation.SUCCESS_CRITERION,
        "review-concentration": review_concentration.SUCCESS_CRITERION,
        "thrash": thrash.SUCCESS_CRITERION,
        "sustainability-risk": sustainability_risk.SUCCESS_CRITERION,
        "compounding-risk": compounding_risk.SUCCESS_CRITERION,
    }

    return {
        "environment": {
            "python_version": sys.version,
            "version_info": list(sys.version_info[:3]),
            "implementation": platform.python_implementation(),
            "machine": platform.machine(),
            "unicode_version": unicodedata.unidata_version,
            "float_repr_style": sys.float_repr_style,
        },
        "rule_order": rule_order,
        "success_criteria": success_criteria,
        "cases": entries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()

    document = build_document()
    text = json.dumps(document, indent=2, ensure_ascii=False, sort_keys=False) + "\n"

    if args.stdout:
        sys.stdout.write(text)
        return 0
    args.out.write_text(text, encoding="utf-8")
    fired = sum(
        1
        for case in document["cases"]
        for result in case["results"].values()
        if result is not None
    )
    total = len(document["cases"]) * len(document["rule_order"])
    print(
        f"wrote {args.out}: {len(document['cases'])} cases, {fired}/{total} rule firings"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
