"""Generate the work_unit_membership projection golden for CHAOS-4441.

IMPORTS `lexical_argmax` and `membership_categories` from the reference and
drives them; it never reimplements them.

THE AXIS THAT DOMINATES THIS UNIT: DICT ORDER
---------------------------------------------
`membership_categories` iterates `distribution.items()`. Python dicts preserve
INSERTION order; Go maps randomise iteration deliberately. So the emitted row
order is insertion order in one plane and arbitrary in the other, and a Go port
backed by a plain map[string]float64 produces a different row sequence on every
run. Every case below therefore records the distribution as an ORDERED list of
pairs, not as a JSON object, so the Go side can replay the exact order Python
saw. A fixture that stored these as objects would be re-ordered by any JSON
library that sorts keys, and the axis would vanish silently.

THE DOCSTRING IS WRONG, AND THE CORPUS PINS THE TRUE BEHAVIOUR
--------------------------------------------------------------
`lexical_argmax` says its tie-break "makes the dominant choice deterministic
across runs regardless of dict ordering". That holds only while every weight is
a real number. The key is `(-_float_value(v), k)`; with a NaN weight, `-nan`
compares False against everything, so tuple comparison never reaches the second
element and `min` returns whatever it saw FIRST. Measured:

    {"z": nan, "a": nan}  -> "z"      (insertion order, NOT lexical)
    {"a": nan, "b": nan}  -> "a"
    {"b": nan, "a": nan}  -> "b"

`is_dominant` is persisted, so this is nondeterminism in stored data whenever a
NaN weight reaches the projection. The corpus pins the ACTUAL behaviour rather
than the documented one -- a port matching the docstring would fail these.

THE UNREACHABLE BRANCH
----------------------
`membership_categories` ends with a "Defensive: ensure the dominant row is
present even if it was filtered" branch. It cannot fire. `lexical_argmax` returns
a key OF the distribution (the empty case returns early), the loop visits every
key, and the dominant key always takes the `or is_dominant` arm -- so it is
always in `seen`. Probed across NaN, negative, and -inf weights, all
`defensive_needed=False`. The Go port reproduces the branch anyway, as dead code
with a comment, because deleting it would make a future reader believe the
guarantee was never intended.

OTHER AXES
----------
  weight kind     below / at / above the 0.2 threshold; exactly 0.2; NaN; inf;
                  -inf; -0.0; huge; subnormal
  value type      float, int, bool, numeric string, unparseable string, None
  cardinality     empty; one; many; all-below-threshold
  ordering        dominant first, last, middle
  ties            equal weights resolved lexically; equal weights with NaN

Usage:
    uv run python tests/fixtures/generate_membership_golden.py [--stdout]
"""

from __future__ import annotations

import json
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dev_health_ops.work_graph.investment.constants import (
    MEMBERSHIP_WEIGHT_THRESHOLD,
)
from dev_health_ops.work_graph.investment.membership import (
    build_membership_records,
    lexical_argmax,
    membership_categories,
)

NodeKeyT = tuple[str, str]

OUTPUT_PATH = Path(__file__).parent / "membership_python_golden.json"

NAN = float("nan")
INF = float("inf")


def _bits(value: float) -> str:
    """IEEE-754 bits, so -0.0 and NaN payloads cannot slip through.

    A value comparison accepts -0.0 == 0.0 and rejects nan == nan, which is
    exactly backwards for a parity corpus.
    """
    return format(struct.unpack("<Q", struct.pack("<d", float(value)))[0], "016x")


def _encode_raw(category: str, weight: object) -> dict[str, Any]:
    """Encode a raw weight in a form the Go side can reconstruct WITHOUT parsing repr.

    The obvious encoding is repr() plus type().__name__, and it is a trap: the
    reader then has to re-derive Python's repr grammar to get a value back --
    stripping quotes from `'0.75'`, mapping `True`/`None`, recognising `nan` and
    `inf`. That is inferring data from FORMATTING, which is how an earlier probe
    in this lane produced a false result by diffing `0x0030` against `0x30` and
    concluding the tables differed.

    So the kind is explicit and the payload is exact: floats travel as IEEE-754
    bits, strings as code points (so an exotic one survives any transport), and
    the rest as themselves. repr is kept alongside for a human reading a failure,
    never as the source of truth.
    """
    encoded: dict[str, Any] = {"category": category, "repr": repr(weight)}
    # bool BEFORE int: isinstance(True, int) is True, and the whole point of
    # _float_value's ladder is that bool is handled first. An encoder that
    # checked int first would label True as an int and quietly test the wrong
    # branch -- the same ordering bug, one layer out.
    if isinstance(weight, bool):
        encoded["kind"] = "bool"
        encoded["bool"] = weight
    elif isinstance(weight, int):
        encoded["kind"] = "int"
        encoded["int"] = weight
    elif isinstance(weight, float):
        encoded["kind"] = "float"
        encoded["bits"] = _bits(weight)
    elif isinstance(weight, str):
        encoded["kind"] = "str"
        encoded["codepoints"] = [ord(character) for character in weight]
    elif weight is None:
        encoded["kind"] = "none"
    else:  # pragma: no cover - the corpus has no other types today
        raise SystemExit(f"unencodable weight type {type(weight).__name__}")
    return encoded


def _scenarios() -> list[tuple[str, list[tuple[str, Any]]]]:
    """(label, ordered (category, raw_weight) pairs).

    Ordered pairs, not dicts, because insertion order is a tested axis.
    """
    t = MEMBERSHIP_WEIGHT_THRESHOLD
    return [
        ("empty", []),
        ("single_above_threshold", [("quality.testing", 0.9)]),
        ("single_below_threshold", [("quality.testing", 0.01)]),
        # The comparison is `>=`, so exactly-threshold is INCLUDED. A port using
        # `>` drops it.
        #
        # These MUST be non-dominant to test anything. A lone category is always
        # the argmax and is emitted via the `or is_dominant` arm whatever its
        # weight, so a singleton at exactly-0.2 yields one row under both `>=`
        # and `>` and proves nothing. The first version of this corpus made
        # exactly that mistake -- three threshold cases, all singletons, all
        # emitting one row for a reason unrelated to the threshold. Each case
        # below therefore carries a clear dominant at 0.9 and puts the
        # boundary weight on a SECOND category, where inclusion is decided by
        # the comparison and nothing else.
        (
            "nondominant_exactly_at_threshold",
            [("a.dominant", 0.9), ("b.boundary", t)],
        ),
        (
            "nondominant_just_below_threshold",
            [("a.dominant", 0.9), ("b.boundary", t - 1e-12)],
        ),
        (
            "nondominant_just_above_threshold",
            [("a.dominant", 0.9), ("b.boundary", t + 1e-12)],
        ),
        # 0.2 has no exact binary representation (bits 3fc999999999999a), so
        # "the literal 0.2" and "a value computed to 0.2" need not be the same
        # double. Both spellings are pinned.
        (
            "nondominant_computed_threshold",
            [("a.dominant", 0.9), ("b.boundary", 0.1 + 0.1)],
        ),
        (
            "nondominant_threshold_from_string",
            [("a.dominant", 0.9), ("b.boundary", "0.2")],
        ),
        # Retained as singletons for the dominant-always-wins path itself.
        ("singleton_at_threshold", [("quality.testing", t)]),
        ("singleton_below_threshold", [("quality.testing", t - 1e-12)]),
        (
            "mixed_above_and_below",
            [
                ("quality.testing", 0.5),
                ("risk.security", 0.05),
                ("maintenance.debt", 0.45),
            ],
        ),
        (
            "all_below_threshold_only_dominant_survives",
            [
                ("quality.testing", 0.1),
                ("risk.security", 0.05),
                ("maintenance.debt", 0.02),
            ],
        ),
        # Lexical tie-break with real numbers: smallest key wins.
        (
            "exact_tie_resolved_lexically",
            [("risk.security", 0.5), ("maintenance.debt", 0.5)],
        ),
        (
            "exact_tie_reversed_insertion_order",
            [("maintenance.debt", 0.5), ("risk.security", 0.5)],
        ),
        # --- the NaN axis: argmax becomes insertion-order dependent ---
        ("nan_single", [("quality.testing", NAN)]),
        ("nan_tie_ab", [("a.one", NAN), ("b.two", NAN)]),
        ("nan_tie_ba", [("b.two", NAN), ("a.one", NAN)]),
        ("nan_tie_za", [("z.last", NAN), ("a.first", NAN)]),
        # NaN alongside a real weight. nan >= threshold is False, so a
        # non-dominant NaN row is dropped; but -nan sorts before everything.
        ("nan_with_real_weight", [("a.nan", NAN), ("b.real", 0.9)]),
        ("real_weight_then_nan", [("b.real", 0.9), ("a.nan", NAN)]),
        # --- infinities and signed zero ---
        ("positive_infinity", [("a.inf", INF), ("b.real", 0.5)]),
        ("negative_infinity", [("a.neginf", -INF), ("b.real", 0.5)]),
        ("negative_zero", [("a.negzero", -0.0), ("b.zero", 0.0)]),
        ("all_zero", [("a.one", 0.0), ("b.two", 0.0)]),
        ("negative_weights", [("a.one", -1.0), ("b.two", -2.0)]),
        # --- value TYPES, exercising _float_value's isinstance ladder ---
        # bool is checked BEFORE int because isinstance(True, int) is True.
        ("bool_true_coerces_to_zero", [("a.flag", True), ("b.real", 0.1)]),
        ("bool_false_coerces_to_zero", [("a.flag", False), ("b.real", 0.1)]),
        ("int_weight", [("a.int", 1), ("b.real", 0.5)]),
        ("numeric_string_weight", [("a.str", "0.75"), ("b.real", 0.1)]),
        ("unparseable_string_weight", [("a.str", "high"), ("b.real", 0.1)]),
        ("none_weight", [("a.none", None), ("b.real", 0.1)]),
        # Strings float() accepts but strconv.ParseFloat does not, and vice
        # versa -- the reason floatcoerce.go exists rather than a ParseFloat call.
        ("string_with_underscores", [("a.str", "0_5"), ("b.real", 0.1)]),
        ("string_with_spaces", [("a.str", " 0.5 "), ("b.real", 0.1)]),
        ("string_fullwidth_digits", [("a.str", "０.５"), ("b.real", 0.1)]),
        ("string_hex_float_rejected", [("a.str", "0x1p-2"), ("b.real", 0.1)]),
        ("string_overflow_to_inf", [("a.str", "1e309"), ("b.real", 0.1)]),
        # --- ordering: the dominant entry first, last, middle ---
        (
            "dominant_first",
            [("a.top", 0.9), ("b.mid", 0.3), ("c.low", 0.25)],
        ),
        (
            "dominant_last",
            [("a.low", 0.25), ("b.mid", 0.3), ("c.top", 0.9)],
        ),
        (
            "dominant_middle",
            [("a.low", 0.25), ("b.top", 0.9), ("c.mid", 0.3)],
        ),
        # Realistic full theme and subcategory distributions.
        (
            "realistic_theme_distribution",
            [
                ("feature_delivery", 0.55),
                ("maintenance", 0.25),
                ("quality", 0.15),
                ("risk", 0.03),
                ("operational", 0.02),
            ],
        ),
        (
            "realistic_subcategory_distribution",
            [
                ("feature_delivery.roadmap", 0.40),
                ("feature_delivery.customer", 0.20),
                ("maintenance.refactor", 0.20),
                ("quality.testing", 0.12),
                ("risk.security", 0.08),
            ],
        ),
    ]


def main() -> None:
    cases: list[dict[str, Any]] = []
    for label, pairs in _scenarios():
        distribution = dict(pairs)
        rows = membership_categories(distribution)
        cases.append(
            {
                "label": label,
                # Ordered pairs. The raw weight is carried both as a repr (so an
                # exotic type is legible) and, where it is numeric, as bits.
                "distribution": [
                    _encode_raw(category, weight) for category, weight in pairs
                ],
                "dominant": lexical_argmax(distribution),
                "rows": [
                    {
                        "category": category,
                        "weight_bits": _bits(weight),
                        "weight_repr": repr(weight),
                        "is_dominant": is_dominant,
                    }
                    for category, weight, is_dominant in rows
                ],
            }
        )

    # --- build_membership_records: the NESTING ORDER is the thing tested ---
    #
    # Python emits, per node, every theme row and then every subcategory row --
    # not all themes for all nodes followed by all subcategories. Swapping the
    # loop nesting yields the same multiset of rows in a different sequence,
    # which a set comparison cannot see. Hence multiple nodes AND both
    # distributions non-trivial in the same case: with one node, or with one
    # distribution empty, the two nestings coincide and the case proves nothing.
    theme_dist = {"feature_delivery": 0.7, "maintenance": 0.3}
    subcat_dist = {"feature_delivery.roadmap": 0.6, "maintenance.debt": 0.25}
    record_scenarios: list[tuple[str, list[NodeKeyT], dict[str, Any], dict[str, Any]]] = [
        ("no_nodes", [], theme_dist, subcat_dist),
        ("single_node", [("issue", "i1")], theme_dist, subcat_dist),
        (
            "multiple_nodes_interleave_theme_then_subcategory",
            [("issue", "i1"), ("pr", "p1"), ("commit", "c1")],
            theme_dist,
            subcat_dist,
        ),
        # A repeated node is NOT deduplicated by the reference.
        (
            "duplicate_node_is_not_deduplicated",
            [("issue", "i1"), ("issue", "i1")],
            theme_dist,
            subcat_dist,
        ),
        ("empty_theme_distribution", [("issue", "i1")], {}, subcat_dist),
        ("empty_subcategory_distribution", [("issue", "i1")], theme_dist, {}),
        ("both_distributions_empty", [("issue", "i1")], {}, {}),
    ]

    record_cases: list[dict[str, Any]] = []
    for label, nodes, themes, subcats in record_scenarios:
        records = build_membership_records(
            unit_nodes=nodes,
            work_unit_id="wu-1",
            theme_distribution=themes,
            subcategory_distribution=subcats,
            categorization_status="llm",
            computed_at=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
            org_id="org-1",
            run_id="run-1",
        )
        record_cases.append(
            {
                "label": label,
                "nodes": [list(node) for node in nodes],
                "theme_distribution": [
                    _encode_raw(c, w) for c, w in themes.items()
                ],
                "subcategory_distribution": [
                    _encode_raw(c, w) for c, w in subcats.items()
                ],
                "records": [
                    {
                        "node_type": r.node_type,
                        "node_id": r.node_id,
                        "work_unit_id": r.work_unit_id,
                        "category_kind": r.category_kind,
                        "category": r.category,
                        "weight_bits": _bits(r.weight),
                        "is_dominant": r.is_dominant,
                        "categorization_status": r.categorization_status,
                        "computed_at": r.computed_at.isoformat(),
                        "org_id": r.org_id,
                        "run_id": r.run_id,
                    }
                    for r in records
                ],
            }
        )

    payload = {
        "record_cases": record_cases,
        "_comment": (
            "Generated by tests/fixtures/generate_membership_golden.py. "
            "Do not hand-edit."
        ),
        "_policy": (
            "Row ORDER is Python dict insertion order; the Go port must not use a "
            "plain map. The threshold comparison is >=, so exactly-0.2 is "
            "included. lexical_argmax is insertion-order dependent whenever a "
            "weight is NaN, contrary to its own docstring; the corpus pins the "
            "measured behaviour."
        ),
        "membership_weight_threshold": MEMBERSHIP_WEIGHT_THRESHOLD,
        "membership_weight_threshold_bits": _bits(MEMBERSHIP_WEIGHT_THRESHOLD),
        "cases": cases,
    }

    rendered = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return

    OUTPUT_PATH.write_text(rendered)
    total_rows = sum(len(case["rows"]) for case in cases)
    print(f"wrote {OUTPUT_PATH}")
    print(f"  cases: {len(cases)}  rows emitted: {total_rows}")
    print(
        f"  threshold: {MEMBERSHIP_WEIGHT_THRESHOLD} ({_bits(MEMBERSHIP_WEIGHT_THRESHOLD)})"
    )


if __name__ == "__main__":
    main()
