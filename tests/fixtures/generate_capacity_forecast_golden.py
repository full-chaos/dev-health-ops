"""Golden vectors for the Go capacity forecast kernel (CUT-20 R2, milestone 1).

Captured from the REAL producer -- dev_health_ops.metrics.compute_capacity --
so the Go port is measured against the function that actually runs, not against
a reading of it.

The cases exist to pin behaviours a tidier port would "fix":

* BOTH MODES IN ONE CALL. Python's two forecast modes are independent `if`
  statements, so a request carrying target_items AND target_date runs both
  simulations and fills both column sets. An `else` drops half the output, and
  a single-mode fixture would never notice.
* EACH SIMULATION RESEEDS. monte_carlo_forecast_days and _items each call
  random.seed themselves, so the second restarts the identical stream. A port
  sharing one generator across both produces different numbers for whichever
  mode runs second -- visible ONLY in the both-modes cases.
* THE ITEMS PERCENTILES ARE FLIPPED. [50, 15, 5] is assigned to p50/p85/p95
  because a LOW percentile is the conservative answer for items. Asking for
  [50, 85, 95] is the natural mistake and silently inverts p85/p95.
* EARLY RETURNS SKIP SEEDING. target_items <= 0 and days_available <= 0 return
  before random.seed, so the generator is neither seeded nor consumed.
* HISTORY LENGTHS THAT REJECT. random.choice over a non-power-of-two length
  exercises _randbelow's rejection loop; over 2/4/8 it never rejects. A fixture
  built only on power-of-two lengths would pass against a broken rejection loop.
* DATE ARITHMETIC ACROSS BOUNDARIES. today + timedelta(days=n) over month
  ends, year ends and a leap day -- cases the parity comparison cannot reach,
  because p50_date/p85_date/p95_date are excluded there as wall-clock derived.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from typing import Any

from dev_health_ops.metrics.compute_capacity import (
    ThroughputHistory,
    ThroughputSample,
    forecast_capacity,
)

# Lengths 7 and 10 reject inside _randbelow; 8 never does. Length 1 is the
# degenerate case where choice is constant and the RNG stops mattering -- kept
# deliberately so the Go side is pinned there too, NOT as parity evidence.
HISTORIES: dict[str, list[int]] = {
    "len7_rejects": [3, 8, 1, 5, 13, 2, 9],
    "len8_no_reject": [4, 7, 2, 9, 1, 6, 3, 8],
    "len10_rejects": [5, 2, 9, 14, 1, 7, 3, 11, 6, 4],
    "len1_degenerate": [6],
    "len14_sufficient": [3, 8, 1, 5, 13, 2, 9, 4, 7, 2, 9, 1, 6, 3],
    "high_variance": [1, 1, 1, 90, 1, 1, 120],
}

SEEDS = [0, 1, 42, -7, 20260823]
ANCHOR = date(2026, 8, 23)


def history_of(values: list[int]) -> ThroughputHistory:
    # ThroughputHistory derives days_of_history from the SAMPLE list, so the
    # samples must be built rather than the throughputs passed directly.
    return ThroughputHistory(
        [
            ThroughputSample(day=ANCHOR - timedelta(days=offset), items_completed=value)
            for offset, value in enumerate(values)
        ]
    )


def encode(result: Any) -> dict[str, Any]:
    def day(value: date | None) -> str | None:
        return value.isoformat() if value is not None else None

    return {
        "p50_days": result.p50_days,
        "p85_days": result.p85_days,
        "p95_days": result.p95_days,
        "p50_date": day(result.p50_date),
        "p85_date": day(result.p85_date),
        "p95_date": day(result.p95_date),
        "p50_items": result.p50_items,
        "p85_items": result.p85_items,
        "p95_items": result.p95_items,
        "throughput_mean": repr(result.throughput_mean),
        "throughput_stddev": repr(result.throughput_stddev),
        "history_days": result.history_days,
        "simulation_count": result.simulation_count,
        "insufficient_history": result.insufficient_history,
        "high_variance": result.high_variance,
    }


def case(
    name: str,
    history_key: str,
    seed: int,
    *,
    target_items: int | None = None,
    target_date: date | None = None,
    today: date = ANCHOR,
    simulations: int = 200,
) -> dict[str, Any]:
    result = forecast_capacity(
        history=history_of(HISTORIES[history_key]),
        target_items=target_items,
        target_date=target_date,
        simulations=simulations,
        seed=seed,
        today=today,
    )
    return {
        "name": name,
        "history_key": history_key,
        "throughputs": HISTORIES[history_key],
        "seed": seed,
        "target_items": target_items,
        "target_date": target_date.isoformat() if target_date else None,
        "today": today.isoformat(),
        "simulations": simulations,
        "expected": encode(result),
    }


def build() -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    for seed in SEEDS:
        for key in HISTORIES:
            cases.append(case(f"days/{key}/seed{seed}", key, seed, target_items=40))
            cases.append(
                case(
                    f"items/{key}/seed{seed}",
                    key,
                    seed,
                    target_date=ANCHOR + timedelta(days=10),
                )
            )
            # BOTH modes in one call -- the independent-`if` trap, and the only
            # place a shared generator across the two simulations shows up.
            cases.append(
                case(
                    f"both/{key}/seed{seed}",
                    key,
                    seed,
                    target_items=40,
                    target_date=ANCHOR + timedelta(days=10),
                )
            )

    # Early returns: neither seeds nor consumes the generator.
    cases.append(case("edge/zero_target_items", "len7_rejects", 42, target_items=0))
    cases.append(
        case("edge/negative_target_items", "len7_rejects", 42, target_items=-5)
    )
    cases.append(case("edge/target_date_today", "len7_rejects", 42, target_date=ANCHOR))
    cases.append(
        case(
            "edge/target_date_past",
            "len7_rejects",
            42,
            target_date=ANCHOR - timedelta(days=3),
        )
    )
    # Scale: the production simulation count, once, so the port is pinned at the
    # size it actually runs rather than only at the size that is cheap to record.
    cases.append(
        case(
            "scale/full_simulations",
            "len14_sufficient",
            42,
            target_items=40,
            simulations=10000,
        )
    )

    # Date arithmetic across boundaries the parity comparison cannot reach,
    # because the date columns are excluded there as wall-clock derived.
    date_cases: list[dict[str, Any]] = []
    for label, anchor, offset in [
        ("month_end", date(2026, 1, 31), 1),
        ("month_end_long", date(2026, 8, 31), 1),
        ("year_end", date(2026, 12, 31), 1),
        ("year_end_far", date(2026, 12, 15), 30),
        ("leap_day_into", date(2028, 2, 28), 1),
        ("leap_day_over", date(2028, 2, 28), 2),
        ("non_leap_feb", date(2027, 2, 28), 1),
        ("leap_year_span", date(2028, 1, 1), 366),
        ("zero_offset", date(2026, 8, 23), 0),
        ("long_offset", date(2026, 8, 23), 365),
    ]:
        date_cases.append(
            {
                "name": f"date/{label}",
                "today": anchor.isoformat(),
                "days": offset,
                "expected": (anchor + timedelta(days=offset)).isoformat(),
            }
        )

    return {
        "generator": "tests/fixtures/generate_capacity_forecast_golden.py",
        "python_version": sys.version.split()[0],
        "cases": cases,
        "date_cases": date_cases,
    }


def main(argv: list[str]) -> int:
    document = build()
    if len(argv) > 1 and argv[1] == "--check":
        with open(argv[2], encoding="utf-8") as handle:
            existing = json.load(handle)
        for key in ("cases", "date_cases"):
            if existing.get(key) != document[key]:
                sys.stderr.write(
                    f"capacity forecast golden is STALE in {key}: the live "
                    "producer no longer returns the recorded values\n"
                )
                return 1
        sys.stdout.write("CAPACITY_FORECAST_GOLDEN_CURRENT\n")
        return 0
    sys.stdout.write(json.dumps(document, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
