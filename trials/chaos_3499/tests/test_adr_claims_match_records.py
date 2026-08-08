"""The ADR may not state a number its own records contradict.

Two rounds of adversarial review found stale or unsupported figures in
`docs/adr-draft.md` that a manual sweep had already "fixed" once. Manual
sweeps keep failing because the document is long and the numbers move every
run, so this file replaces the sweep with a mechanical check: the ADR's
load-bearing claims are re-derived from the committed records and compared.

This is the ADR's own regression suite. It needs no model calls.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_DOCS = Path(__file__).resolve().parents[1] / "docs"
_AUTHORED_CLASS_C = (
    "O3_supersession",
    "O5_conflicts",
    "O5_conflicts_injected",
    "O6_recurring_pattern",
)
_BASELINE_ARMS = ("native", "episode_readback")


@pytest.fixture(scope="module")
def records() -> dict:
    return json.loads((_DOCS / "measured-trial-results.records.json").read_text())


@pytest.fixture(scope="module")
def adr() -> str:
    """The ADR with newlines collapsed to single spaces.

    Markdown re-wrapping is not a change of claim, and a guard that broke
    every time a paragraph rewrapped would be weakened or deleted within a
    round. Assertions here are about WORDING, so they run against the
    normalised text; anything that needs exact layout says so.
    """
    raw = (_DOCS / "adr-draft.md").read_text()
    return re.sub(r"\s+", " ", raw)


def _verdict(records: dict, tier_key: str, oracle_id: str, arm: str) -> str | None:
    tier = next(t for t in records["tiers"] if t["tier_key"] == tier_key)
    for row in tier["rows"]:
        if row["oracle_id"] == oracle_id and row["arm"] == arm:
            return row["verdict"]
    return None


def test_class_c_per_tier_totals_in_the_adr_match_the_records(
    records: dict, adr: str
) -> None:
    """The class-(c) row is the most-quoted line in the document."""
    totals = {
        t["tier_key"]: sum(
            1
            for o in _AUTHORED_CLASS_C
            if _verdict(records, t["tier_key"], o, "extraction_llm") == "pass"
        )
        for t in records["tiers"]
    }
    expected_row = (
        f"| **total** | **{totals['gpt-5-nano']}/4** | **{totals['gpt-5-mini']}/4** "
        f"| **{totals['gpt-5.6-luna']}/4** | **{totals['gemma-4-e4b-local']}/4** "
        f"| **{totals['gemma-4-31b-local']}/4** |"
    )
    expected_row = re.sub(r"\s+", " ", expected_row)
    assert expected_row in adr, (
        "the ADR's class-(c) total row disagrees with the records; expected "
        f"{expected_row!r}"
    )


def test_the_baseline_class_c_claim_names_its_population(
    records: dict, adr: str
) -> None:
    """[codex final H1] The baseline's only class-(c) pass is NOT one of the
    four authored oracles, so on the shared population it is 0/4 while on
    the full population it is 1/15. Printing one where the other belongs is
    how an unsupported number reached a decision document.
    """
    tier = records["tiers"][0]
    class_c_passes = {
        row["oracle_id"]
        for row in tier["rows"]
        if row["question_class"] == "c"
        and row["arm"] in _BASELINE_ARMS
        and row["verdict"] == "pass"
    }
    shared = len(class_c_passes & set(_AUTHORED_CLASS_C))
    assert shared == 0, (
        "baseline now passes an authored class-(c) oracle; update the ADR"
    )
    assert f"**{shared}/4** on the shared population" in adr
    assert f"({len(class_c_passes)}/15 full)" in adr


def test_control_status_claims_match_the_records(records: dict, adr: str) -> None:
    statuses = {
        t["control_status"] for t in records["tiers"] if t["status"] == "measured"
    }
    assert statuses == {"held"}, f"control status changed: {statuses}"
    assert "returned **`held` on all five tiers.**" in adr


def test_no_retired_claims_survive_in_the_adr(adr: str) -> None:
    """[codex final H2] A stale-claim guard, because two manual sweeps both
    missed something. Each entry is a phrase that was TRUE of an earlier
    run and is false now; they must never reappear.
    """
    retired = [
        "no tier exceeds 1/4",
        "scores the same 1/4 on",
        "all four tiers",
        "four model tiers",
        "run 6, five model tiers",
        "**What changed in run 3**",
        "1/4 measured |",
    ]
    found = [phrase for phrase in retired if phrase in adr]
    assert not found, f"retired claim(s) still present in the ADR: {found}"


def test_the_injection_result_matches_the_records(records: dict, adr: str) -> None:
    passing = [
        t["tier_key"]
        for t in records["tiers"]
        if _verdict(records, t["tier_key"], "O5_conflicts_injected", "extraction_llm")
        == "pass"
    ]
    assert passing == ["gpt-5.6-luna"], f"injection result changed: {passing}"
    assert "`gpt-5.6-luna` only, in both runs" in adr
    assert "the honest result is 1/5" in adr


def test_the_observed_vs_inferred_lead_is_backed_at_every_tier(
    records: dict, adr: str
) -> None:
    """The ADR calls this its cheapest lead; it must be in the records at
    every tier, not inferred from one probe."""
    for tier in records["tiers"]:
        details = [
            f["detail"]
            for row in tier["rows"]
            if row["oracle_id"] == "O5_conflicts" and row["arm"] == "extraction_llm"
            for f in row["failed_assertions"]
        ]
        assert any("expected inferred" in d for d in details), (
            f"{tier['tier_key']} does not record the observed-vs-inferred failure"
        )
    assert "claim_kind=observed, expected inferred" in adr


def test_the_31b_exclusion_is_attributed_not_asserted(adr: str) -> None:
    """[codex final M1] Adjudicated three times. The latency is a measured
    fact; the deployment exclusion is the decision owner's constraint, and
    the ADR must not voice it as its own recommendation.
    """
    assert "excluded by the decision owner (chris, 2026-08-08)" in adr
    assert "not proposed for deployment in any environment" not in adr


def test_the_adr_still_makes_no_recommendation(adr: str) -> None:
    assert "NO RECOMMENDATION" in adr
    assert "No recommendation is made" in adr
    for banned in ("we recommend", "i recommend", "should adopt", "we should"):
        assert banned not in adr.lower(), f"recommendation language: {banned}"
