"""Generate the evidence-quality golden for CHAOS-4441.

Covers the arithmetic plane of work_graph/investment/evidence.py:
compute_evidence_quality and the helpers it composes -- clamp, _graph_density,
_edge_confidence, _float_value -- plus evidence_quality_band.

WHY THIS IS NOT JUST ARITHMETIC
-------------------------------
`clamp` is `max(low, min(high, value))`, and that nesting is load-bearing under
NaN. Python's `min`/`max` return the FIRST argument when the comparison is
False, and every comparison with NaN is False, so:

    min(1.0, nan) -> 1.0        max(0.0, nan) -> 0.0

which means `clamp(nan)` is **1.0**, the HIGH bound. Written the other way
round -- `min(high, max(low, value))` -- it would be 0.0. Go's `math.Min`
and `math.Max` propagate NaN instead, returning NaN from both.

So a NaN evidence quality reports as MAXIMUM quality in Python, a naive Go port
reports NaN, and the two disagree in the worst available direction: garbage
confidence data scores as perfect evidence rather than as none.

NaN is reachable. `_float_value` returns float(value) for any int/float, and
`confidence` arrives from a Float32 ClickHouse column with no finite-value
guard on the writer side -- the same reachability argument that made the PR1
non-finite confidence work necessary.

`evidence_quality_band` is safe by contrast: every `>=` against NaN is False in
both languages, so it falls through to "very_low" identically. It is included
anyway because the composition matters -- in the pipeline clamp runs FIRST, so
a NaN quality is banded "high", not "very_low".

Usage:
    uv run python tests/fixtures/generate_investment_quality_golden.py [--stdout]
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

from dev_health_ops.utils.normalization import clamp, evidence_quality_band
from dev_health_ops.work_graph.investment.components import (
    _edge_confidence as _components_edge_confidence,
)
from dev_health_ops.work_graph.investment.evidence import (
    _edge_confidence,
    _float_value,
    _graph_density,
    compute_evidence_quality,
)
from dev_health_ops.work_graph.investment.types import TextBundle

OUTPUT_PATH = Path(__file__).parent / "investment_quality_python_golden.json"

NAN = float("nan")
INF = float("inf")


def _num(value: float) -> Any:
    """Render a float in a form JSON can carry and Go can read back exactly."""
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return value


def _tag(value: Any) -> dict[str, Any]:
    """Encode a confidence value unambiguously.

    A float NaN and the STRING "nan" are different inputs with different
    results, and both would render as the JSON string "nan" under a naive
    encoding -- so the type is carried explicitly rather than inferred by the
    reader. bool is checked before int because bool IS an int in Python, which
    is the same ordering _float_value itself depends on.
    """
    if value == "__MISSING__":
        return {"kind": "missing"}
    if value is None:
        return {"kind": "none"}
    if isinstance(value, bool):
        return {"kind": "bool", "value": value}
    if isinstance(value, float):
        return {"kind": "float", "value": _num(value)}
    if isinstance(value, int):
        return {"kind": "int", "value": value}
    if isinstance(value, str):
        return {"kind": "str", "value": value}
    return {"kind": "other", "repr": repr(value)}


def _clamp_cases() -> list[dict[str, Any]]:
    values = [
        NAN,
        INF,
        -INF,
        -1e308,
        -5.0,
        -1.0,
        -0.0,
        0.0,
        1e-300,
        0.25,
        0.5,
        0.75,
        1.0,
        1.0000001,
        5.0,
        1e308,
    ]
    return [{"value": _num(v), "clamped": _num(clamp(v))} for v in values]


def _band_cases() -> list[dict[str, Any]]:
    values = [
        NAN,
        INF,
        -INF,
        -1.0,
        0.0,
        0.39,
        0.4,
        0.5999999,
        0.6,
        0.7,
        0.7999999,
        0.8,
        1.0,
        2.0,
    ]
    return [{"value": _num(v), "band": evidence_quality_band(v)} for v in values]


def _float_value_cases() -> list[dict[str, Any]]:
    """_float_value's type axis -- bool is checked BEFORE int/float."""
    raw: list[Any] = [
        None,
        True,
        False,
        0,
        1,
        -3,
        2.5,
        NAN,
        INF,
        -INF,
        "1.5",
        "  2.5  ",
        "nan",
        "inf",
        "-inf",
        "",
        "abc",
        "0x10",
        "1_000",
        [],
        {},
        ("a",),
    ]
    cases = []
    for value in raw:
        cases.append(
            {
                "repr": repr(value),
                "result": _num(_float_value(value)),
            }
        )
    return cases


def _coercion_agreement() -> dict[str, Any]:
    """Check that evidence._float_value still matches components._edge_confidence.

    These are two SEPARATE copies of the same coercion in two modules. The Go
    port reuses ONE function (units.ConfidenceFromValue) for both, which is only
    correct while the two Python copies agree -- and nothing in Python stops one
    from being changed without the other.

    So the agreement is recorded here rather than assumed. If it ever breaks,
    the rot guard fails and the Go side has to be split into two functions
    instead of silently absorbing the divergence.

    The huge-int case is excluded because BOTH raise OverflowError there: the
    try/except catches only ValueError, and only in the str branch. That is a
    shared crash, not a disagreement, and it is unreachable through the real
    path (confidence is Float32 in ClickHouse, so the driver yields a float).
    """
    values: list[Any] = [
        None,
        True,
        False,
        0,
        1,
        -3,
        2.5,
        NAN,
        INF,
        -INF,
        "1.5",
        "  2.5  ",
        "nan",
        "inf",
        "-inf",
        "",
        "abc",
        "0x10",
        "1_000",
        "\x1c1.5",
        [],
        {},
        ("a",),
        -0.0,
        1e308,
        2**63,
        2**200,
    ]
    disagreements = []
    for value in values:
        left = _components_edge_confidence({"confidence": value})
        right = _float_value(value)
        same = left == right or (math.isnan(left) and math.isnan(right))
        if not same:
            disagreements.append(
                {"repr": repr(value), "components": _num(left), "evidence": _num(right)}
            )
    return {
        "compared": len(values),
        "disagreements": disagreements,
        "huge_int_raises_in_both": True,
    }


def _density_cases() -> list[dict[str, Any]]:
    pairs = [
        (0, 0),
        (1, 0),
        (1, 5),
        (2, 0),
        (2, 1),
        (2, 5),
        (3, 3),
        (5, 10),
        (5, 100),
        (10, 0),
        (10, 45),
        (100, 4950),
        (1000, 10),
    ]
    return [
        {"node_count": n, "edge_count": e, "density": _num(_graph_density(n, e))}
        for n, e in pairs
    ]


def _edge_confidence_cases() -> list[dict[str, Any]]:
    edge_sets: list[tuple[str, list[dict[str, Any]]]] = [
        ("empty", []),
        ("single", [{"confidence": 0.5}]),
        ("missing_key", [{}]),
        ("none_value", [{"confidence": None}]),
        (
            "mixed_types",
            [{"confidence": 0.5}, {"confidence": "0.25"}, {"confidence": None}],
        ),
        ("bool_is_zero", [{"confidence": True}, {"confidence": False}]),
        ("with_nan", [{"confidence": 1.0}, {"confidence": NAN}]),
        ("with_inf", [{"confidence": 1.0}, {"confidence": INF}]),
        ("inf_and_neg_inf", [{"confidence": INF}, {"confidence": -INF}]),
        ("unparseable_string", [{"confidence": "abc"}, {"confidence": 1.0}]),
        ("all_zero", [{"confidence": 0.0}, {"confidence": 0.0}]),
        # The SUMMATION axis. Every case above has 0-2 edges, and below three
        # summands Neumaier compensation is always zero -- so this corpus was
        # structurally blind to a naive `total +=` port until these were added.
        ("twenty_tenths", [{"confidence": 0.1} for _ in range(20)]),
        ("hundred_tenths", [{"confidence": 0.1} for _ in range(100)]),
        (
            "wide_spread",
            [{"confidence": 1e16}]
            + [{"confidence": 1.0}] * 10
            + [{"confidence": -1e16}],
        ),
        (
            "alternating_magnitudes",
            [{"confidence": 1e12 if n % 2 == 0 else 1e-12} for n in range(12)],
        ),
    ]
    return [
        {
            "label": label,
            # Edges are carried as the confidence values alone; that is the only
            # key _edge_confidence reads.
            "confidences": [_tag(e.get("confidence", "__MISSING__")) for e in edges],
            "confidence": _num(_edge_confidence(edges)),
        }
        for label, edges in edge_sets
    ]


def _bundle(
    source_texts: dict[str, dict[str, str]], count: int, chars: int
) -> TextBundle:
    return TextBundle(
        source_block="",
        source_texts=source_texts,
        handle_map={},
        input_hash="",
        text_source_count=count,
        text_char_count=chars,
    )


def _quality_cases() -> list[dict[str, Any]]:
    """Cross the three score components against each other.

    text_score depends on text_source_count and text_char_count;
    agreement_score on how many source TYPES have any entries (note: on
    `if texts`, i.e. a non-empty dict, NOT on whether the texts are non-empty);
    structural on nodes_count and the edge confidences.
    """
    source_shapes: list[tuple[str, dict[str, dict[str, str]]]] = [
        ("no_types", {"issue": {}, "pr": {}, "commit": {}}),
        ("one_type", {"issue": {"a": "x"}, "pr": {}, "commit": {}}),
        ("two_types", {"issue": {"a": "x"}, "pr": {"b": "y"}, "commit": {}}),
        ("three_types", {"issue": {"a": "x"}, "pr": {"b": "y"}, "commit": {"c": "z"}}),
        # A type whose dict is non-empty but whose TEXT is empty still counts
        # toward agreement -- the predicate is on the dict, not the strings.
        ("type_with_empty_text", {"issue": {"a": ""}, "pr": {}, "commit": {}}),
    ]
    counts = [0, 1, 2, 3, 6]
    chars = [0, 600, 1200, 5000]
    edge_shapes: list[tuple[str, list[dict[str, Any]]]] = [
        ("no_edges", []),
        ("mid", [{"confidence": 0.5}]),
        ("full", [{"confidence": 1.0}]),
        ("nan", [{"confidence": NAN}]),
        ("inf", [{"confidence": INF}]),
    ]
    node_counts = [0, 1, 2, 10]

    cases = []
    for shape_label, source_texts in source_shapes:
        for count in counts:
            for char in chars:
                for edge_label, edges in edge_shapes:
                    for nodes in node_counts:
                        bundle = _bundle(source_texts, count, char)
                        value = compute_evidence_quality(
                            text_bundle=bundle,
                            nodes_count=nodes,
                            edges=edges,
                        )
                        cases.append(
                            {
                                "label": f"{shape_label}__c{count}__ch{char}__{edge_label}__n{nodes}",
                                "source_texts": source_texts,
                                "text_source_count": count,
                                "text_char_count": char,
                                "nodes_count": nodes,
                                "confidences": [_tag(e["confidence"]) for e in edges],
                                "quality": _num(value),
                                "band": evidence_quality_band(value),
                            }
                        )
    return cases


def main() -> None:
    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_investment_quality_golden.py. "
            "Do not hand-edit."
        ),
        "clamp_cases": _clamp_cases(),
        "coercion_agreement": _coercion_agreement(),
        "band_cases": _band_cases(),
        "float_value_cases": _float_value_cases(),
        "density_cases": _density_cases(),
        "edge_confidence_cases": _edge_confidence_cases(),
        "quality_cases": _quality_cases(),
    }

    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return

    OUTPUT_PATH.write_text(rendered)
    print(f"wrote {OUTPUT_PATH}")
    for key, value in payload.items():
        if isinstance(value, list):
            print(f"  {key}: {len(value)}")


if __name__ == "__main__":
    main()
