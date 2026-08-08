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
    """[codex final H1 + M] The baseline's only class-(c) pass is not among
    the authored oracles, so on the shared population it is 0/N while on the
    full population it is 1/M. Printing one where the other belongs is how an
    unsupported number reached a decision document.

    Everything here is DERIVED: the authored set, both denominators, and the
    identity of the baseline's passing oracle. An earlier version of this
    guard read tiers[0] only and hardcoded all three, so it would have
    agreed with a document that had drifted on any of them.
    """
    baseline_arms = ("native", "episode_readback")
    authored, base_passes, full_population = set(), set(), set()
    for tier in records["tiers"]:
        for row in tier["rows"]:
            if row["question_class"] != "c":
                continue
            full_population.add(row["oracle_id"])
            if row["arm"] == "extraction_llm" and row["verdict"] != "not_measured":
                authored.add(row["oracle_id"])
            if row["arm"] in baseline_arms and row["verdict"] == "pass":
                base_passes.add(row["oracle_id"])

    assert authored, "no authored class-(c) oracles found in the records"
    # The claim is only interesting because these two sets are disjoint --
    # assert that explicitly rather than inferring it from a count.
    assert base_passes.isdisjoint(authored), (
        "the baseline now passes an authored class-(c) oracle "
        f"({sorted(base_passes & authored)}); the ADR's population argument "
        "needs rewriting, not just renumbering"
    )
    assert base_passes == {"O4_prior_attempts"}, (
        f"the baseline's class-(c) pass set changed: {sorted(base_passes)}"
    )
    shared = len(base_passes & authored)
    assert f"**{shared}/{len(authored)}** on the shared population" in adr
    assert f"({len(base_passes)}/{len(full_population)} full)" in adr
    assert "`O4_prior_attempts`" in adr, (
        "the ADR must NAME the baseline's passing oracle; a bare count lets "
        "a reader assume it is one of the compared four"
    )


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


# ==========================================================================
# CLOSURE ARGUMENT for stale numeric prose.
#
# This defect class recurred three times: stale numbers survived two manual
# sweeps and one exact-match guard. The failure modes, diagnosed from the
# third occurrence, were NOT case sensitivity:
#
#   1. DIGIT vs WORD  -- patterns matched "four tiers" but the document said
#      "all 4 tiers" and "4-tier range".
#   2. LINE WRAPPING  -- `grep` is line-based, so "No tier\n exceeds 1/4"
#      could not match a single-line pattern however it was spelled.
#   3. EXACT VARIANTS -- "four explicitly named tiers" matched neither
#      "four tiers" nor "four model tiers".
#
# The closure is therefore not another list of phrases. It is:
#   (a) all text is NORMALISED (lowercased, whitespace collapsed) before
#       matching, which kills modes 1-part and 2 outright; and
#   (b) numbers are matched STRUCTURALLY -- every tier-count and every
#       fraction in the document is extracted by pattern and checked
#       against a value DERIVED FROM RECORDS, rather than compared to a
#       hand-written variant that has to be guessed in advance.
#
# What is now impossible: a numeric tier-count or class-score fraction can
# no longer sit in hand-written prose without either matching the records
# or being declared in _HISTORICAL_FRACTIONS with a justification. Any new
# spelling, wrapping, or digit/word form is caught, because nothing here
# depends on how the claim is phrased.
# ==========================================================================

_WORD_NUMBERS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}

#: Fractions that are TRUE STATEMENTS ABOUT EARLIER RUNS or about the PRD,
#: and so cannot be derived from the current records. Every entry needs a
#: reason: an undeclared historical number is indistinguishable from a
#: stale one, which is the whole defect being closed.
_HISTORICAL_FRACTIONS = {
    "1/15": "runs 1-2 class (c) full-population score (§3.6 history table)",
    "2/15": "run 2 class (c) full-population score (§3.6 history table)",
    "0/15": "run 1/6 class (c) full-population score (§3.6, §3.4 history)",
}


def _normalised(text: str) -> str:
    """Lowercased, whitespace-collapsed, with fenced code blocks removed.

    Fenced blocks are stripped because the ADR's own closure argument has to
    QUOTE the stale phrasings it eliminated ("all 4 tiers", "no tier exceeds
    1/4"). An illustration of a defect is not an instance of it, and a guard
    that could not tell the difference would force the document to stop
    explaining itself -- or be switched off. Everything outside a fence is
    prose, and prose is where claims live.
    """
    without_fences = re.sub(r"```.*?```", " ", text, flags=re.S)
    return re.sub(r"\s+", " ", without_fences).lower()


def _declared_tier_count(records: dict) -> int:
    return len(records["declared_tiers"])


def test_no_tier_count_in_the_adr_contradicts_the_records(
    records: dict, adr: str
) -> None:
    """Structural, not phrase-based: finds every "<n> tier(s)" and
    "<n>-tier" in ANY spelling and requires it to equal the declared count.
    """
    expected = _declared_tier_count(records)
    text = _normalised(adr)
    pattern = re.compile(
        r"\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten)"
        r"[\s-]*(?:model[\s-]*)?(?:explicitly[\s-]*named[\s-]*)?tiers?\b"
    )
    wrong = []
    for match in pattern.finditer(text):
        raw = match.group(1)
        value = int(raw) if raw.isdigit() else _WORD_NUMBERS[raw]
        before = text[max(0, match.start() - 6) : match.start()]
        after = text[match.end() : match.end() + 8]
        # "one tier above parity" is a RELATIVE POSITION, not a count, and
        # "run-3 tiers" is a run number. Neither is a claim about how many
        # tiers exist, and flagging them would push someone to delete the
        # guard rather than fix a number.
        if before.endswith("run-") or before.endswith("run "):
            continue
        if before.endswith("within ") or before.endswith("ithin "):
            continue
        if re.match(r"\s*(above|below|up|down)\b", after):
            continue
        if value != expected:
            start = max(0, match.start() - 60)
            wrong.append(f"...{text[start : match.end() + 20]}...")
    assert not wrong, (
        f"tier-count claim(s) contradicting the records ({expected} declared "
        f"tiers):\n  " + "\n  ".join(wrong)
    )


def _derivable_fractions(records: dict) -> set[str]:
    """Every fraction the current records legitimately support."""
    out: set[str] = set()
    tiers = records["tiers"]
    n_tiers = len(tiers)
    authored_c = sorted(
        {
            row["oracle_id"]
            for t in tiers
            for row in t["rows"]
            if row["question_class"] == "c"
            and row["arm"] == "extraction_llm"
            and row["verdict"] != "not_measured"
        }
    )
    for tier in tiers:
        rows = [r for r in tier["rows"] if r["arm"] == "extraction_llm"]
        by_class: dict[str, list[str]] = {}
        for row in rows:
            by_class.setdefault(row["question_class"], []).append(row["verdict"])
        for verdicts in by_class.values():
            measured = sum(1 for v in verdicts if v != "not_measured")
            passed = sum(1 for v in verdicts if v == "pass")
            out.add(f"{passed}/{measured}")
            out.add(f"{passed}/{len(verdicts)}")
        base_by_oracle: dict[str, set[str]] = {}
        for row in tier["rows"]:
            if row["arm"] in ("native", "episode_readback"):
                base_by_oracle.setdefault(row["oracle_id"], set()).add(row["verdict"])
        base_class: dict[str, list[str]] = {}
        for row in rows:
            component_verdicts = base_by_oracle.get(row["oracle_id"], set())
            folded = (
                "pass"
                if "pass" in component_verdicts
                else (
                    "not_measured" if "not_measured" in component_verdicts else "fail"
                )
            )
            base_class.setdefault(row["question_class"], []).append(folded)
        for verdicts_list in base_class.values():
            measured = sum(1 for v in verdicts_list if v != "not_measured")
            passed = sum(1 for v in verdicts_list if v == "pass")
            out.add(f"{passed}/{measured}")
            out.add(f"{passed}/{len(verdicts_list)}")
        c_pass = sum(
            1 for r in rows if r["oracle_id"] in authored_c and r["verdict"] == "pass"
        )
        out.add(f"{c_pass}/{len(authored_c)}")
        out.add(f"{tier['oracles_measured_by_candidate']}/{tier['oracles_total']}")
    # counts expressed out of the number of tiers (e.g. the injection result)
    for k in range(n_tiers + 1):
        out.add(f"{k}/{n_tiers}")
    # baseline populations
    for tier in tiers:
        c_rows = [r for r in tier["rows"] if r["question_class"] == "c"]
        base_pass = {
            r["oracle_id"]
            for r in c_rows
            if r["arm"] in ("native", "episode_readback") and r["verdict"] == "pass"
        }
        total_c = len({r["oracle_id"] for r in c_rows})
        out.add(f"{len(base_pass)}/{total_c}")
        out.add(f"{len(base_pass & set(authored_c))}/{len(authored_c)}")
    return out


def test_every_fraction_in_the_adr_is_derivable_or_declared_historical(
    records: dict, adr: str
) -> None:
    """The core closure. Any "a/b" in the document must either be derivable
    from the records or be an explicitly declared historical value.
    """
    derivable = _derivable_fractions(records)
    # Two-digit-max parts, never adjacent to word chars: an issue reference
    # such as "CHAOS-3563/3564" is not a score, and a guard that flagged it
    # would be switched off within a round.
    found = {
        m.group(0)
        for m in re.finditer(r"(?<![\w/-])\d{1,2}/\d{1,2}(?![\w/-])", _normalised(adr))
    }
    unexplained = sorted(found - derivable - set(_HISTORICAL_FRACTIONS))
    assert not unexplained, (
        "fraction(s) in the ADR that the records do not support and that are "
        f"not declared historical: {unexplained}\n"
        f"derivable from records: {sorted(derivable)}"
    )


def test_no_tier_is_paired_with_a_score_that_is_not_its_own(
    records: dict, adr: str
) -> None:
    """The fraction guard alone is not enough, and this test exists because
    of a live example: section 8.3 said "`gpt-5.6-luna` 1/4" after luna had
    moved to 2/4. `1/4` is a perfectly derivable fraction -- it is
    `gpt-5-mini`'s -- so a guard that only checks the SET of fractions
    blesses it. What was stale was the PAIRING of a tier with a score.

    So: wherever a tier name and an "n/4" appear adjacent, the score must be
    that tier's own class-(c) authored total.
    """
    authored_c = sorted(
        {
            row["oracle_id"]
            for t in records["tiers"]
            for row in t["rows"]
            if row["question_class"] == "c"
            and row["arm"] == "extraction_llm"
            and row["verdict"] != "not_measured"
        }
    )
    denominator = len(authored_c)
    expected = {}
    for tier in records["tiers"]:
        passed = sum(
            1
            for row in tier["rows"]
            if row["arm"] == "extraction_llm"
            and row["oracle_id"] in authored_c
            and row["verdict"] == "pass"
        )
        expected[tier["requested_model"].lower()] = f"{passed}/{denominator}"

    text = _normalised(adr)
    wrong = []
    for model, correct in expected.items():
        short = model.split("/")[-1]
        for name in {model, short}:
            # tier name, then up to ~40 chars of punctuation/words, then n/4
            for match in re.finditer(
                rf"{re.escape(name)}[`\s,:*]*(\d{{1,2}}/{denominator})\b", text
            ):
                if match.group(1) != correct:
                    start = max(0, match.start() - 40)
                    wrong.append(
                        f"{name} paired with {match.group(1)} "
                        f"(records say {correct}): ...{text[start : match.end()]}..."
                    )
    assert not wrong, (
        "tier/score pairing(s) contradicting the records:\n  " + "\n  ".join(wrong)
    )
