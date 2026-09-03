#!/usr/bin/env python3
"""Dense evidence_json probe across the 1e8..7e15 repr band.

Written by lane-3092 as a throwaway proof and promoted into the shipped
corpus by orchestrator ruling: a peer's one-off verification is worth more
frozen than re-derived, and it covers a band the hand-built corpus only
crossed rather than sampled (3 values in band, versus 476 here).

Built from the REAL producer -- recommendation_to_record -> json.dumps -- not a
restatement of it, so the bytes are what the live path writes.

The band matters because CPython's repr keeps fixed notation up to 1e16 and
switches to exponential at it, so 1e8..7e15 is the region where fixed notation
carries the most digits and a mirror is most likely to drift. Values are passed
through the same round(x, 4) and round(x, 2) the evidence sites apply, because
the rounded number is what actually reaches the column.
"""

from __future__ import annotations

import json
import math
import platform
import random
import struct
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dev_health_ops.recommendations.loader import recommendation_to_record  # noqa: E402
from dev_health_ops.recommendations.schema import (  # noqa: E402
    EvidenceRef,
    Recommendation,
)

OUTPUT_PATH = Path(__file__).parent / "evidence_json_repr_band_python_golden.json"

WS, WE = date(2026, 8, 1), date(2026, 9, 1)
NOW = datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc)


def build_values() -> list[float]:
    values: list[float] = []
    # Decade walk with several shapes at each decade: the decade itself, a value
    # just below it (where notation flips), a long-mantissa value, and the
    # decade plus/minus one ulp.
    exponent = 8
    while exponent <= 16:
        base = float(10**exponent)
        values += [
            base,
            math.nextafter(base, 0.0),
            math.nextafter(base, math.inf),
            base - 1.0,
            base + 1.0,
            base * 1.5,
            base * 7.0,
            base + 0.5,
            base + 0.25,
        ]
        exponent += 1
    # Powers of two through the band, including 2**53 where consecutive integers
    # stop being representable.
    for power in range(26, 54):
        two = float(2**power)
        values += [two, two - 1.0, two + 1.0, two + 0.5]
    # Long-mantissa randoms, seeded so the fixture is reproducible.
    rng = random.Random(20260902)
    for _ in range(240):
        values.append(rng.uniform(1e8, 7e15))
    for _ in range(60):
        values.append(rng.uniform(1e8, 7e15) + rng.random())
    # Keep only the band, plus the two decade endpoints just outside it so the
    # boundary itself is covered rather than approached.
    keep = [v for v in values if 1e8 <= abs(v) <= 7e15]
    keep += [1e16, 9.999999999999998e15, 1e8, 99999999.99999999]
    # Distinct bit patterns, ordered, so the fixture has no silent duplicates.
    seen: dict[int, float] = {}
    for v in keep:
        seen.setdefault(struct.unpack("<Q", struct.pack("<d", v))[0], v)
    return sorted(seen.values())


def evidence_bytes(value: float, digits: int) -> dict:
    rounded = round(value, digits)
    rec = Recommendation(
        rule_id="saturation",
        team_id="t",
        org_id="o",
        computed_at=NOW,
        window_start=WS,
        window_end=WE,
        severity="warning",
        title="T",
        rationale="R",
        success_criterion="S",
        evidence=(
            EvidenceRef(
                "t", "work_item_metrics_daily", WS, WE, "wip_count_end_of_day", rounded
            ),
        ),
    )
    return {
        "input_bits": struct.pack(">d", value).hex(),
        "input_repr": repr(value),
        "round_digits": digits,
        "rounded_bits": struct.pack(">d", rounded).hex(),
        "rounded_repr": repr(rounded),
        "evidence_json": recommendation_to_record(rec).evidence_json,
    }


def main() -> int:
    values = build_values()
    cases = [evidence_bytes(v, d) for v in values for d in (4, 2)]
    document = {
        "purpose": "dense 1e8..7e15 repr-band coverage for MarshalPythonJSONInsertionOrder (#2140)",
        # Interpreter provenance. RECORDED, NEVER COMPARED.
        #
        # The rot guard compares the PAYLOAD (the `cases` array, and
        # `distinct_input_values` where present) rather than the whole document,
        # so this block is documentation that a byte-comparison cannot reach.
        #
        # It has to be that way round. Freezing it inside the comparison already
        # fired once: `sys.version` embeds the build string, so a macOS-generated
        # fixture could never match a Linux runner. Recording the bare version
        # narrowed the window without closing it -- the live interpreter is
        # UNPINNED (parityLivePython takes $PYTHON, else whatever `python3`
        # resolves to), so CPython 3.14.8 would reproduce the same incident on
        # CPython's release schedule.
        #
        # The distinction worth keeping, from lane-ci-flakes: a frozen
        # environment value is a DEFECT when the environment can drift without a
        # decision, and a FEATURE when it cannot. This one drifts without a
        # decision, so it is recorded outside the comparison rather than deleted.
        "environment": {
            "python_version": platform.python_version(),
            "float_repr_style": sys.float_repr_style,
        },
        "distinct_input_values": len(values),
        "cases": cases,
    }
    text = json.dumps(document, indent=2, sort_keys=True) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(text)
        return 0
    out = OUTPUT_PATH
    out.write_text(text, encoding="utf-8")
    exponential = sum(1 for c in cases if "e+" in c["rounded_repr"])
    print(f"wrote {out}")
    print(f"  distinct input values in band: {len(values)}")
    print(f"  cases (x2 rounding depths):    {len(cases)}")
    print(f"  rounded values in exponential notation: {exponential}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
