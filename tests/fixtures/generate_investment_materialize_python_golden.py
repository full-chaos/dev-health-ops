#!/usr/bin/env python3
"""Regenerate the investment.materialize orchestration golden (CHAOS-4441).

WHAT THIS IS THE ORACLE FOR
---------------------------
The Go cutover (`internal/jobs/investment/materialize.go`) composes pieces
that ALREADY have their own Python goldens -- components, evidence, effort,
labels, ids, repo-effort, taxonomy. What had no oracle, and what this file
supplies, is the ORCHESTRATION between them: which categorization outcome
lands on which record, and the four decisions materialize.py makes that a
transcription gets wrong quietly rather than loudly.

  1. THE FALLBACK GATES (materialize.py:1363-1381). A bundle under
     MIN_EVIDENCE_CHARS, or with zero text sources, never reaches the LLM and
     gets a fallback outcome -- but still gets a ROW. A port that skipped the
     unit instead would make it vanish from the allocation view rather than
     appear as uncategorised, and the row count would still look plausible.
  2. THE invalid_llm_output CLAMP (materialize.py:1686-1687). Evidence
     quality is capped at 0.3 AND the BAND is recomputed from the capped
     value. Recomputing the band is the half a port forgets, because the
     assembly already derived a band from the uncapped number.
  3. THE THEME ROLL-UP. `rollup_subcategories_to_themes` accumulates with a
     plain `+=` loop and then normalises with a builtin `sum()`. Since
     CPython 3.12 `sum()` is Neumaier-compensated and `+=` is not
     (CHAOS-4824), so the two summations differ IN THE REFERENCE and the port
     has to differ the same way. Using compensated summation for both would
     be better numerics and a wrong answer.
  4. THE AUDIT ARRAY. `json.dumps([*errors, *warnings])` -- errors first, and
     `[]` (never `null`) when both are empty.

HERMETIC BY CONSTRUCTION
------------------------
No ClickHouse, no network, no LLM. Every function below is IMPORTED from the
deployed reference and driven over frozen inputs -- none is re-implemented
here, which is the point: a generator that re-stated materialize.py's logic
would only prove this file agrees with itself.

Usage:
    python tests/fixtures/generate_investment_materialize_python_golden.py
    python tests/fixtures/generate_investment_materialize_python_golden.py --stdout
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from dev_health_ops.utils.normalization import evidence_quality_band
from dev_health_ops.work_graph.investment.categorize import (
    CategorizationOutcome,
    fallback_outcome,
)
from dev_health_ops.work_graph.investment.constants import MIN_EVIDENCE_CHARS
from dev_health_ops.work_graph.investment.utils import (
    ensure_full_subcategory_vector,
    rollup_subcategories_to_themes,
)

OUTPUT_PATH = Path(__file__).parent / "investment_materialize_python_golden.json"


def _subcategory_vectors() -> list[tuple[str, dict[str, float]]]:
    """Inputs for the theme roll-up, chosen to exercise the summation seam.

    Every vector goes through ensure_full_subcategory_vector first, exactly as
    every real caller's does (validate_llm_payload's success branch and
    fallback_outcome's _fallback_distribution both return one), so the roll-up
    sees the same sorted-key insertion order the reference gives it.
    """
    return [
        (
            "single_subcategory",
            {"quality.bugfix": 1.0},
        ),
        (
            "one_per_theme",
            {
                "feature_delivery.roadmap": 0.2,
                "maintenance.debt": 0.2,
                "operational.on_call": 0.2,
                "quality.bugfix": 0.2,
                "risk.security": 0.2,
            },
        ),
        (
            # Several subcategories inside ONE theme: this is the case where
            # the `+=` accumulation order is observable at all.
            "many_within_one_theme",
            {
                "feature_delivery.roadmap": 0.1,
                "feature_delivery.customer": 0.2,
                "quality.bugfix": 0.7,
            },
        ),
        (
            # Weights that do not associate exactly in binary floating point,
            # so a different accumulation order produces a different last bit.
            "non_associative_weights",
            {
                "feature_delivery.roadmap": 0.1,
                "feature_delivery.customer": 0.2,
                "maintenance.debt": 0.3,
                "quality.bugfix": 0.7,
                "risk.security": 1e-17,
            },
        ),
        (
            # DISCRIMINATES normalize_scores' `sum()` from a naive `+=`.
            #
            # Found by search, not by intuition: ~13% of random normalized
            # subcategory vectors make the two summations disagree in the last
            # bit, but the hand-picked cases above all happen NOT to -- a
            # mutation swapping pythonparity.Sum for a plain loop passed the
            # corpus until this case was added. Without it, the Go port's claim
            # to mirror CPython 3.12's Neumaier-compensated sum() would rest on
            # reading the reference rather than on a test that reddens.
            "neumaier_vs_naive_summation_differ",
            {
                "feature_delivery.enablement": 0.3488219635946776,
                "maintenance.debt": 0.620291119869306,
                "operational.incident_response": 0.49607455365966535,
                "risk.compliance": 0.1323636559695694,
                "risk.security": 0.41247018789151907,
            },
        ),
        (
            "all_zero_falls_through_to_uniform",
            {},
        ),
        (
            "unknown_key_is_ignored_not_an_error",
            {"quality.bugfix": 0.5, "not_a_real_subcategory": 9.0},
        ),
    ]


def _rollup_cases() -> list[dict[str, object]]:
    cases: list[dict[str, object]] = []
    for label, raw in _subcategory_vectors():
        subcategories = ensure_full_subcategory_vector(raw)
        cases.append(
            {
                "label": label,
                "subcategories": subcategories,
                "themes": rollup_subcategories_to_themes(subcategories),
            }
        )
    return cases


def _outcome_cases() -> list[dict[str, object]]:
    """The outcome -> record decisions, one case per branch.

    structural_evidence_quality is the value the deterministic assembly
    produced BEFORE any outcome existed; final_* is what materialize.py's
    post-process loop writes after the clamp rule is applied.
    """
    ok_outcome = CategorizationOutcome(
        subcategories=ensure_full_subcategory_vector({"quality.bugfix": 1.0}),
        evidence_quotes=[],
        uncertainty="",
        status="ok",
        errors=[],
        warnings=["weights_normalized:0.9000"],
    )
    repaired_outcome = CategorizationOutcome(
        subcategories=ensure_full_subcategory_vector({"maintenance.debt": 1.0}),
        evidence_quotes=[],
        uncertainty="",
        status="repaired",
        errors=[],
        warnings=[],
    )
    invalid_outcome = CategorizationOutcome(
        subcategories=ensure_full_subcategory_vector({}),
        evidence_quotes=[],
        uncertainty="",
        status="invalid_llm_output",
        errors=["missing_top_level_keys:['uncertainty']", "subcategories_not_object"],
        warnings=["weights_normalized:1.2000"],
    )

    cases: list[dict[str, object]] = []
    for label, outcome, structural_quality in [
        ("ok_high_quality", ok_outcome, 0.91),
        ("repaired_moderate_quality", repaired_outcome, 0.62),
        # The clamp only BINDS when the structural score exceeds 0.3 ...
        ("invalid_output_clamps_down", invalid_outcome, 0.87),
        # ... and must NOT raise a score that is already below it.
        ("invalid_output_does_not_raise_a_low_score", invalid_outcome, 0.12),
        (
            "insufficient_evidence_fallback",
            fallback_outcome("insufficient_evidence"),
            0.20,
        ),
        ("no_text_sources_fallback", fallback_outcome("no_text_sources"), 0.05),
        ("llm_task_failed_fallback", fallback_outcome("llm_task_failed"), 0.44),
    ]:
        quality = float(structural_quality)
        if outcome.status == "invalid_llm_output":
            quality = min(quality, 0.3)
        cases.append(
            {
                "label": label,
                "status": outcome.status,
                "structural_evidence_quality": structural_quality,
                "final_evidence_quality": quality,
                "final_evidence_quality_band": evidence_quality_band(quality),
                "subcategory_distribution": outcome.subcategories,
                "theme_distribution": rollup_subcategories_to_themes(
                    outcome.subcategories
                ),
                "categorization_errors_json": json.dumps(
                    [*outcome.errors, *outcome.warnings]
                ),
            }
        )
    return cases


def _gate_cases() -> list[dict[str, object]]:
    """Which (text_char_count, text_source_count) pairs reach the LLM.

    MIN_EVIDENCE_CHARS is imported rather than written as 300, so a change to
    the constant moves the golden instead of silently invalidating it.
    """
    cases: list[dict[str, object]] = []
    for chars, sources in [
        (0, 0),
        (MIN_EVIDENCE_CHARS - 1, 3),
        (MIN_EVIDENCE_CHARS, 0),
        (MIN_EVIDENCE_CHARS, 1),
        (MIN_EVIDENCE_CHARS + 1, 2),
    ]:
        # materialize.py:1363-1381's order: the char gate is tested FIRST, so
        # a bundle failing both reports insufficient_evidence, not
        # no_text_sources.
        if chars < MIN_EVIDENCE_CHARS:
            disposition = "fallback:insufficient_evidence"
        elif sources == 0:
            disposition = "fallback:no_text_sources"
        else:
            disposition = "llm"
        cases.append(
            {
                "text_char_count": chars,
                "text_source_count": sources,
                "disposition": disposition,
            }
        )
    return cases


def build_golden() -> dict[str, object]:
    return {
        "min_evidence_chars": MIN_EVIDENCE_CHARS,
        "rollup_cases": _rollup_cases(),
        "outcome_cases": _outcome_cases(),
        "gate_cases": _gate_cases(),
    }


def main() -> int:
    text = json.dumps(build_golden(), indent=2, sort_keys=True) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(text)
        return 0
    OUTPUT_PATH.write_text(text)
    print(f"wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
