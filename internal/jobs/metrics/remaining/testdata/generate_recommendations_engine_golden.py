#!/usr/bin/env python3
"""Generate the engine/tombstone parity corpus from the LIVE reference.

Drives the real ``RuleEngine.evaluate_state`` through an injected fake loader
and records every ``RecommendationRecord`` it returns -- fired rows AND
``fired=False`` tombstones. No database: the loader is the engine's own
injection point.

WHY THE TOMBSTONES ARE THE POINT. evaluate_state's own docstring records that
the readers do ``argMax(fired, computed_at)`` per
``(org_id, team_id, rule_id, window_end)`` and keep ``HAVING latest_fired =
true``, so without a ``fired=False`` row at the new window_end a rule that fired
yesterday and has since recovered keeps surfacing stale guidance (CHAOS-2373).
Tombstones are correctness, not bookkeeping.

AND THEY ARE NOT SYMMETRIC WITH THE FIRED PATH. A fired row takes title,
severity and success_criterion from the EVALUATOR; a tombstone takes them from
the registry's static RuleDef. They disagree for all five rules -- a
sustainability-risk tombstone carries severity="critical" while its fired row
carries "warning". A port that reuses the evaluator's values for tombstones
diverges on every non-fired row and passes every fired-path test.

Usage:
    python generate_recommendations_engine_golden.py            # write beside this file
    python generate_recommendations_engine_golden.py --stdout
"""

from __future__ import annotations

import argparse
import json
import platform
import struct
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO_ROOT / "src"))

from dev_health_ops.recommendations.engine import RuleEngine  # noqa: E402
from dev_health_ops.recommendations.snapshot import MetricsSnapshot  # noqa: E402

OUTPUT_PATH = Path(__file__).parent / "recommendations_engine_golden.json"

NOW_BASE = datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc)
WINDOW_DAYS = 30

# Inputs chosen so each rule can be driven to fire or not INDEPENDENTLY of the
# others, which is what lets a case isolate one rule's tombstone.
RISING = [0.0, 0.5, 1.0, 1.5, 2.0]
FALLING = [9.0, 6.0, 3.0, 0.0]


class FakeLoader:
    """The engine's own injection point; returns one canned snapshot."""

    def __init__(self, snapshot: MetricsSnapshot) -> None:
        self._snapshot = snapshot

    def load_team_metrics_window(self, team_id, org_id, window_start, window_end):  # noqa: ARG002
        # The engine derives the window itself; echo its values back so the
        # record's window fields come from the engine, not from this fake.
        from dataclasses import replace

        return replace(
            self._snapshot,
            team_id=team_id,
            org_id=org_id,
            window_start=window_start,
            window_end=window_end,
        )


def snapshot(**overrides) -> MetricsSnapshot:
    base = dict(
        team_id="t",
        org_id="o",
        window_start=date(2026, 8, 1),
        window_end=date(2026, 9, 1),
        wip_by_day=[],
        throughput_by_cycle=[],
        review_latency_p75_hours=None,
        reviewer_gini=None,
        rework_churn_ratio=None,
        after_hours_ratio=None,
        cycle_time_by_day=[],
        hotspot_complexity_delta=None,
        hotspot_churn_overlap=None,
        compounding_risk_score=None,
        compounding_risk_severity=None,
    )
    base.update(overrides)
    return MetricsSnapshot(**base)


FIRE = {
    "saturation": dict(wip_by_day=RISING, throughput_by_cycle=[5.0, 3.0]),
    "review-concentration": dict(review_latency_p75_hours=30.0, reviewer_gini=0.75),
    "thrash": dict(rework_churn_ratio=0.5, throughput_by_cycle=[5.0, 3.0]),
    "sustainability-risk": dict(after_hours_ratio=0.35, cycle_time_by_day=RISING),
    "compounding-risk": dict(
        compounding_risk_severity="elevated", compounding_risk_score=0.5
    ),
}


def build_cases():
    cases = [
        ("none-fire-all-tombstones", snapshot()),
    ]
    everything = {}
    for spec in FIRE.values():
        for key, value in spec.items():
            if key == "throughput_by_cycle" and key in everything:
                continue
            everything[key] = value
    cases.append(("all-five-fire", snapshot(**everything)))

    # Each rule alone: isolates that rule's FIRED row against four tombstones.
    for rule, spec in FIRE.items():
        cases.append((f"only-{rule}-fires", snapshot(**spec)))

    # Each rule alone SUPPRESSED: isolates that rule's TOMBSTONE against four
    # fired rows -- the inverse, and the one that catches a tombstone built
    # from the evaluator's fields instead of the registry's.
    for rule in FIRE:
        subset = {}
        for other, spec in FIRE.items():
            if other == rule:
                continue
            for key, value in spec.items():
                if key == "throughput_by_cycle" and key in subset:
                    continue
                subset[key] = value
        cases.append((f"only-{rule}-tombstones", snapshot(**subset)))

    # compounding-risk is the only rule that can emit critical; both severities
    # and the score-absent branch.
    for severity in ("elevated", "high"):
        for score in (0.62, None):
            cases.append(
                (
                    f"compounding-{severity}-score-{'set' if score is not None else 'none'}",
                    snapshot(
                        compounding_risk_severity=severity, compounding_risk_score=score
                    ),
                )
            )
    return cases


def bits(value):
    """Exact IEEE-754 bit pattern; None stays None so absence is not a value."""
    return None if value is None else struct.pack(">d", float(value)).hex()


def encode_snapshot(snap) -> dict:
    """The engine's INPUT, so the Go side replays rather than assumes."""
    return {
        "wip_by_day": [bits(v) for v in snap.wip_by_day],
        "throughput_by_cycle": [bits(v) for v in snap.throughput_by_cycle],
        "review_latency_p75_hours": bits(snap.review_latency_p75_hours),
        "reviewer_gini": bits(snap.reviewer_gini),
        "rework_churn_ratio": bits(snap.rework_churn_ratio),
        "after_hours_ratio": bits(snap.after_hours_ratio),
        "cycle_time_by_day": [bits(v) for v in snap.cycle_time_by_day],
        "hotspot_complexity_delta": bits(snap.hotspot_complexity_delta),
        "hotspot_churn_overlap": bits(snap.hotspot_churn_overlap),
        "compounding_risk_score": bits(snap.compounding_risk_score),
        "compounding_risk_severity": snap.compounding_risk_severity,
    }


def encode_record(record) -> dict:
    payload = asdict(record)
    return {
        "team_id": payload["team_id"],
        "org_id": payload["org_id"],
        "rule_id": payload["rule_id"],
        "rule_version": payload["rule_version"],
        "window_start": payload["window_start"].isoformat(),
        "window_end": payload["window_end"].isoformat(),
        "fired": payload["fired"],
        "severity": payload["severity"],
        "title": payload["title"],
        "rationale": payload["rationale"],
        "success_criterion": payload["success_criterion"],
        # Exact bytes. This is what reaches the column, bare NaN/Infinity
        # tokens included.
        "evidence_json": payload["evidence_json"],
        "computed_at": payload["computed_at"].isoformat(),
    }


def build_document() -> dict:
    entries = []
    for index, (name, snap) in enumerate(build_cases()):
        # Per-case instant, for the reason the rules corpus varies it: one
        # frozen `now` pins the field's value and not its propagation.
        case_now = NOW_BASE + timedelta(
            days=index * 3, hours=index * 5, minutes=index * 7
        )
        engine = RuleEngine(loader=FakeLoader(snap), now=case_now)
        records = engine.evaluate_state(
            team_id=f"team-{index:04d}",
            window=WINDOW_DAYS,
            org_id=f"org-{index:04d}",
            rule_version="1.0.0",
        )
        entries.append(
            {
                "name": name,
                "now": case_now.isoformat(),
                "window_days": WINDOW_DAYS,
                # The INPUTS as well as the outcomes. A corpus of outcomes
                # alone cannot be replayed: the Go side needs the snapshot
                # that produced them, or it compares against results it
                # cannot regenerate. Floats cross as exact bit patterns --
                # float.hex() is lossy for NaN.
                "snapshot": encode_snapshot(snap),
                "records": [encode_record(r) for r in records],
            }
        )

    fired = sum(1 for e in entries for r in e["records"] if r["fired"])
    total = sum(len(e["records"]) for e in entries)
    return {
        "purpose": (
            "Engine parity: RuleEngine.evaluate_state driven through an injected "
            "fake loader. Pins the fired rows AND the fired=False tombstones, "
            "whose title/severity/success_criterion come from the registry "
            "RuleDef rather than the evaluator and disagree with it."
        ),
        "environment": {
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "float_repr_style": sys.float_repr_style,
            "machine_not_compared": platform.machine(),
        },
        "record_counts": {"total": total, "fired": fired, "tombstones": total - fired},
        "cases": entries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()

    document = build_document()
    text = json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.stdout:
        sys.stdout.write(text)
        return 0
    args.out.write_text(text, encoding="utf-8")
    counts = document["record_counts"]
    print(
        f"wrote {args.out}: {len(document['cases'])} cases, "
        f"{counts['total']} records ({counts['fired']} fired, {counts['tombstones']} tombstones)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
