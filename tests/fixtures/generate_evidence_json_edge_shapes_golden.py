"""Edge-shape evidence_json cases the real corpus does not reach.

Contributed by lane-3092 during its peer read of the insertion-order encoder,
and promoted into the shipped corpus by orchestrator ruling.

WHY THESE ARE SYNTHETIC, AND WHY THAT IS THE POINT
--------------------------------------------------
lane-3092 compared 302 real evidence shapes from its 437-case rule corpus
against the Go encoder. Those rows carry NaN and +Infinity, but **zero
-Infinity**, and every string in them is ASCII. So the real corpus, however
large, could not exercise:

  * the -Infinity token (the third of the three allow_nan spellings)
  * ensure_ascii on non-ASCII, CJK, emoji, or astral-plane text
  * the short escapes: quote, backslash, newline, tab
  * C0 control characters, including NUL
  * signed zero, where -0.0 must keep its sign
  * the empty evidence list

Each of those is a real spelling of the same column. A corpus that only samples
what production happened to emit last week will miss them, and missing them is
invisible: the encoder returns plausible bytes either way.

Each shape is built through the LIVE producer -- a real Recommendation passed to
recommendation_to_record -- so `evidence_json` is what loader.py:448 emits, not
a restatement of what it ought to emit.

Values are carried as `value_bits`, the raw IEEE-754 big-endian hex, because
NaN, the infinities and -0.0 have no faithful decimal spelling to round-trip
through. The bits ARE the value.

Usage:
    uv run python tests/fixtures/generate_evidence_json_edge_shapes_golden.py [--stdout]
"""

from __future__ import annotations

import json
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

OUTPUT_PATH = Path(__file__).parent / "evidence_json_edge_shapes_python_golden.json"

WS, WE = date(2026, 8, 1), date(2026, 9, 1)
NOW = datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc)

NAN = "7ff8000000000000"
POS_INF = "7ff0000000000000"
NEG_INF = "fff0000000000000"
POS_ZERO = "0000000000000000"
NEG_ZERO = "8000000000000000"


def bits_to_float(bits: str) -> float:
    return struct.unpack(">d", bytes.fromhex(bits))[0]


def row(team: str, table: str, field: str, bits: str) -> dict:
    return {
        "team_id": team,
        "metric_table": table,
        "window_start": WS.isoformat(),
        "window_end": WE.isoformat(),
        "field": field,
        "value_bits": bits,
    }


# Each entry: (name, why it is here, rows). The `why` ships in the fixture so a
# future reader deleting a case has to argue with the reason rather than guess.
SHAPES: list[tuple[str, str, list[dict]]] = [
    (
        "neg-inf-single",
        "-Infinity is the one allow_nan token the real corpus never emitted",
        [row("t", "work_item_metrics_daily", "wip_count_end_of_day", NEG_INF)],
    ),
    (
        "neg-inf-pair",
        "two -Infinity rows: proves the ', ' separator is not swallowed next to a bare token",
        [
            row("t", "work_item_metrics_daily", "wip_count_end_of_day", NEG_INF),
            row("t", "work_item_metrics_daily", "items_completed_delta", NEG_INF),
        ],
    ),
    (
        "all-three-tokens",
        "NaN, +Infinity and -Infinity in one list, in that order",
        [
            row("t", "a", "f1", NAN),
            row("t", "b", "f2", POS_INF),
            row("t", "c", "f3", NEG_INF),
        ],
    ),
    (
        "neg-inf-with-nan",
        "a bare token adjacent to another bare token, no finite value between them",
        [row("t", "a", "f1", NEG_INF), row("t", "b", "f2", NAN)],
    ),
    (
        "signed-zero",
        "-0.0 must keep its sign; Go's constant -0.0 is exactly +0.0, so this is easy to lose",
        [row("t", "a", "f1", POS_ZERO), row("t", "b", "f2", NEG_ZERO)],
    ),
    (
        "empty-evidence",
        "the empty list; engine.py:223 writes a literal '[]' for non-fired rows",
        [],
    ),
    (
        "non-ascii-team",
        "ensure_ascii=True escapes accented Latin; Go's encoder emits it literally",
        [
            row(
                "équipe-café",
                "work_item_metrics_daily",
                "wip_count_end_of_day",
                "3ff8000000000000",
            )
        ],
    ),
    (
        "emoji-and-cjk",
        "CJK plus an astral emoji: the emoji becomes a surrogate PAIR under ensure_ascii",
        [row("チーム\U0001f680", "a", "f1", "4004000000000000")],
    ),
    (
        "quote-backslash-newline",
        "the short escapes, including a tab inside a FIELD name rather than a value",
        [row('he said "hi"\\ and\n', "a", "f\tb", "400c000000000000")],
    ),
    (
        "control-chars",
        "C0 controls including NUL, which has no short escape and must go to \\u0000",
        [row("\x00\x01\x1f", "a", "f", "4012000000000000")],
    ),
    (
        "surrogate-pair-astral",
        "two astral code points back to back: four \\u escapes, no literal bytes",
        [row("\U0001f600\U0001d11e", "a", "f", "4016000000000000")],
    ),
]


def build(name: str, why: str, rows: list[dict]) -> dict:
    recommendation = Recommendation(
        rule_id=f"synthetic:{name}",
        team_id="t",
        org_id="o",
        computed_at=NOW,
        window_start=WS,
        window_end=WE,
        severity="warning",
        title="T",
        rationale="R",
        success_criterion="S",
        evidence=tuple(
            EvidenceRef(
                r["team_id"],
                r["metric_table"],
                WS,
                WE,
                r["field"],
                bits_to_float(r["value_bits"]),
            )
            for r in rows
        ),
    )
    return {
        "name": name,
        "why": why,
        "rows": rows,
        "evidence_json": recommendation_to_record(recommendation).evidence_json,
    }


def main() -> int:
    cases = [build(name, why, rows) for name, why, rows in SHAPES]
    document = {
        "purpose": (
            "edge-shape evidence_json cases the real rule corpus cannot reach: "
            "-Infinity, non-ASCII/CJK/astral text, short escapes, C0 controls, "
            "signed zero and the empty list"
        ),
        "provenance": (
            "shapes identified by lane-3092 while peer-reviewing #2140; its "
            "302-shape real-row comparison carried zero -Infinity and no "
            "non-ASCII, so these were added synthetically to cover the gap"
        ),
        "environment": {
            "python_version": sys.version,
            "float_repr_style": sys.float_repr_style,
        },
        "cases": cases,
    }
    text = json.dumps(document, indent=2, sort_keys=True) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(text)
        return 0
    OUTPUT_PATH.write_text(text, encoding="utf-8")
    print(f"wrote {OUTPUT_PATH}")
    print(f"  cases: {len(cases)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
