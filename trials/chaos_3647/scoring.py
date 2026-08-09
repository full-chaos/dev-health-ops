"""Scoring one leg's resolution against the frozen corpus oracle.

Every expectation used here already exists in
``investigation_corpus.oracles``. Nothing is authored in this module, and
that is deliberate: a trial that measures a new retrieval path and also
invents its own notion of "correct" can always be made to look good, and no
reader can tell the two decisions apart afterwards.

**Precision is reported at two horizons and neither is the headline.**
``precision_at_3`` asks whether the contract's own top-3 subject horizon is
free of stray candidates; ``precision_over_all`` asks the same of everything
the leg returned inside its limit. A retrieval leg will usually pass the
first and fail the second, because similarity search returns a ranked tail
where a lookup returns nothing. Reporting only the first would flatter it;
reporting only the second would condemn it for having a tail at all. Both
are recorded, and the tail is priced explicitly by ``stray_candidates``.

**A leg that commits where the case demands a clarification fails, and is
not scored as a partial success.** The corpus says so — H05 and H07 set
``committed_subject_id=None`` and ``expected_answer=CLARIFIED`` — and the
distinction is the whole point of those cases: silently choosing between two
live candidates is the failure they exist to catch.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from dev_health_ops.api.dev.investigation_corpus.cases import AnswerDisposition
from dev_health_ops.api.dev.investigation_corpus.oracles import oracle_for

from .legs import LegResolution

__all__ = [
    "TOP_N_HORIZON",
    "LegScore",
    "Verdict",
    "score_leg",
]

#: The frozen contract's own subject horizon. Not a knob: the packet may
#: commit to at most this many subjects, so precision beyond it is a
#: different question from precision inside it.
TOP_N_HORIZON = 3


class Verdict(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class LegScore:
    """One leg's verdicts on one case, each with the detail behind it."""

    case_id: str
    leg: str
    resolved: bool
    #: Rank-1 is the case's committed subject. NOT_APPLICABLE where the case
    #: expects a clarification rather than a commitment.
    subject_top_1: Verdict
    subject_top_1_detail: str
    #: The committed subject appears within the contract's top-3 horizon.
    subject_top_3: Verdict
    subject_top_3_detail: str
    #: No stray candidate inside the top-3 horizon.
    precision_at_3: Verdict
    precision_at_3_detail: str
    #: No stray candidate anywhere in what the leg returned.
    precision_over_all: Verdict
    precision_over_all_detail: str
    #: No forbidden subject ranked anywhere. For H08 every real project is
    #: forbidden, which makes this the no-widening measurement.
    no_forbidden_subject: Verdict
    no_forbidden_subject_detail: str
    #: The leg committed where the case demands a clarification.
    must_not_commit: Verdict
    must_not_commit_detail: str
    stray_candidates: tuple[str, ...]
    #: The one boolean the delta is computed from. **Subject resolution
    #: only** — this package builds no packets, so it says nothing about
    #: drivers, evidence or synthesis, and a reader must not read it as
    #: "the arm answered the case correctly".
    subject_resolution_correct: bool
    subject_resolution_detail: str


def score_leg(case_id: str, resolution: LegResolution) -> LegScore:
    """Score ``resolution`` against ``case_id``'s frozen oracle."""

    oracle = oracle_for(case_id)
    committed = oracle.committed_subject_id
    permitted = frozenset(oracle.permitted_candidate_ids)
    forbidden = frozenset(oracle.forbidden_subject_ids)
    ranked = tuple(subject.canonical_id for subject in resolution.subjects)
    top_3 = ranked[:TOP_N_HORIZON]

    if committed is None:
        top_1 = Verdict.NOT_APPLICABLE
        top_1_detail = (
            "the case expects a clarification, so there is no committed "
            "subject to be rank-1"
        )
        top_3_verdict = Verdict.NOT_APPLICABLE
        top_3_detail = top_1_detail
        must_not_commit = Verdict.PASS if len(ranked) != 1 else Verdict.FAIL
        must_not_commit_detail = (
            f"ranked {len(ranked)} candidates; a single ranked candidate is a "
            "silent commitment where the case demands a clarification"
        )
    else:
        got = ranked[0] if ranked else ""
        top_1 = Verdict.PASS if got == committed else Verdict.FAIL
        top_1_detail = f"rank-1 was {got or '(nothing)'}; expected {committed}"
        top_3_verdict = Verdict.PASS if committed in top_3 else Verdict.FAIL
        top_3_detail = f"top-3 {list(top_3)}; expected {committed}"
        must_not_commit = Verdict.NOT_APPLICABLE
        must_not_commit_detail = "the case expects a commitment"

    stray_at_3 = tuple(sorted(set(top_3) - permitted))
    stray_all = tuple(sorted(set(ranked) - permitted))
    precision_at_3 = Verdict.PASS if not stray_at_3 else Verdict.FAIL
    precision_all = Verdict.PASS if not stray_all else Verdict.FAIL

    ranked_forbidden = tuple(sorted(set(ranked) & forbidden))
    if not forbidden:
        no_forbidden = Verdict.NOT_APPLICABLE
        no_forbidden_detail = "the case forbids no subject"
    else:
        no_forbidden = Verdict.PASS if not ranked_forbidden else Verdict.FAIL
        no_forbidden_detail = (
            f"ranked forbidden subjects {list(ranked_forbidden)}"
            if ranked_forbidden
            else "no forbidden subject ranked"
        )

    correct, correct_detail = _subject_resolution_correct(
        expected_answer=oracle.expected_answer,
        committed=committed,
        permitted=permitted,
        ranked=ranked,
        top_3=top_3,
        ranked_forbidden=ranked_forbidden,
    )

    return LegScore(
        case_id=case_id,
        leg=resolution.leg.value,
        subject_resolution_correct=correct,
        subject_resolution_detail=correct_detail,
        resolved=resolution.resolved,
        subject_top_1=top_1,
        subject_top_1_detail=top_1_detail,
        subject_top_3=top_3_verdict,
        subject_top_3_detail=top_3_detail,
        precision_at_3=precision_at_3,
        precision_at_3_detail=(
            f"stray candidates inside the top-3 horizon {list(stray_at_3)}"
            if stray_at_3
            else "no stray candidate inside the top-3 horizon"
        ),
        precision_over_all=precision_all,
        precision_over_all_detail=(
            f"{len(stray_all)} stray candidates across {len(ranked)} ranked"
            if stray_all
            else f"no stray candidate across {len(ranked)} ranked"
        ),
        no_forbidden_subject=no_forbidden,
        no_forbidden_subject_detail=no_forbidden_detail,
        must_not_commit=must_not_commit,
        must_not_commit_detail=must_not_commit_detail,
        stray_candidates=stray_all,
    )


def _subject_resolution_correct(
    *,
    expected_answer: AnswerDisposition,
    committed: str | None,
    permitted: frozenset[str],
    ranked: tuple[str, ...],
    top_3: tuple[str, ...],
    ranked_forbidden: tuple[str, ...],
) -> tuple[bool, str]:
    """Whether a leg resolved this case's subject correctly.

    Branches on the corpus's own ``expected_answer`` rather than on whether a
    committed subject happens to be set, because the three dispositions want
    three different things and collapsing them is how a no-match case gets
    scored as a success for returning a ranked list.

    ``UNAVAILABLE`` is the strict one: H08 names a subject that does not
    exist, every real project is forbidden, and the only correct resolution
    is an **empty** one. A leg that returns its nearest neighbours has
    substituted a guess for an answer, which is the failure the case exists
    to catch — and it is exactly what a similarity search does by
    construction, so scoring it any other way would hide the finding.
    """

    if expected_answer is AnswerDisposition.UNAVAILABLE:
        if ranked:
            return False, (
                f"the case names a subject that does not exist; the leg "
                f"ranked {len(ranked)} candidates anyway, led by {ranked[0]}"
            )
        return True, "nothing was ranked, which is the only correct resolution"

    if expected_answer is AnswerDisposition.CLARIFIED:
        if len(ranked) == 1:
            return False, (
                f"committed silently to {ranked[0]} where the case demands a "
                "clarification between two live candidates"
            )
        if not ranked:
            return False, "resolved nothing; the case has two real candidates"
        if ranked_forbidden:
            return False, f"ranked forbidden subjects {list(ranked_forbidden)}"
        stray = sorted(set(top_3) - permitted)
        if stray:
            return False, (
                f"the top-3 clarification set contains {stray}, which the "
                "case does not permit as candidates"
            )
        return True, (
            f"offered {list(top_3)} inside the top-3 horizon, all permitted, "
            "without committing"
        )

    if committed is None:  # pragma: no cover - corpus invariant
        raise ValueError(
            f"expected_answer {expected_answer!r} implies a committed subject "
            "but the oracle names none"
        )
    if ranked_forbidden:
        return False, f"ranked forbidden subjects {list(ranked_forbidden)}"
    if not ranked:
        return False, f"resolved nothing; expected {committed}"
    if ranked[0] != committed:
        return False, f"rank-1 was {ranked[0]}; expected {committed}"
    return True, f"rank-1 was {committed}, as expected"
