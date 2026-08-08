"""The independent expected-evidence oracle.

Discipline this module exists to enforce (CHAOS-3065, restated in PRD §15.1):

* **Authored before observation.** An oracle states what a correct answer must
  contain, derived from how the corpus was *constructed* -- never from what an
  arm happened to return. ``trials/chaos-3499/tests/test_corpus_consistency.py``
  re-derives the same expectations from corpus ground truth by an independent
  route and asserts the two agree, so a typo in a hand-authored oracle cannot
  quietly become the definition of correct.
* **Every assertion is individually attributable.** :meth:`Oracle.evaluate`
  returns one :class:`AssertionResult` per assertion, so a fault-mode test can
  demand not merely that the oracle failed but that it failed *on the assertion
  that claims to catch that fault*. A mutation that dies in the wrong assertion
  proves nothing about the invariant it was aimed at.
* **A measurement that did not happen FAILS.** There is no skip path. See
  :class:`Verdict`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .contracts import (
    ArmOutcome,
    ArmResponse,
    ClaimKind,
    EntityRef,
    QueryMode,
    QuestionClass,
    TemporalContextQuery,
    TemporalFact,
    TimeAxis,
)


class Verdict(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    NOT_MEASURED = "not_measured"

    @property
    def is_failure(self) -> bool:
        """``NOT_MEASURED`` is a failure, not a neutral outcome.

        A trial report that renders "not measured" as anything other than red
        is a report that claims coverage it does not have.
        """
        return self is not Verdict.PASS


@dataclass(frozen=True)
class AssertionResult:
    assertion_id: str
    verdict: Verdict
    detail: str

    @property
    def ok(self) -> bool:
        return self.verdict is Verdict.PASS


@dataclass(frozen=True)
class OracleResult:
    oracle_id: str
    arm: str
    question_class: QuestionClass
    assertions: tuple[AssertionResult, ...]

    @property
    def verdict(self) -> Verdict:
        if any(a.verdict is Verdict.NOT_MEASURED for a in self.assertions):
            return Verdict.NOT_MEASURED
        if any(a.verdict is Verdict.FAIL for a in self.assertions):
            return Verdict.FAIL
        return Verdict.PASS

    def failed_assertion_ids(self) -> tuple[str, ...]:
        return tuple(a.assertion_id for a in self.assertions if not a.ok)


# --------------------------------------------------------------------------
# Matchers
# --------------------------------------------------------------------------


#: Wildcard object for a ``FactExpectation``. Deliberately an explicit
#: sentinel rather than ``None``: ``object`` is compared positionally and a
#: default of None would let an author who simply FORGOT the field create a
#: silent wildcard. It also has to be a sentinel rather than "omit the
#: field", because a must_exclude that matches nothing is invisible -- the
#: first attempt at a generic exclusion in O5_conflicts_injected passed
#: ``object=None``, matched nothing, and read as a guard while catching
#: neither the shape it named nor any other.
#:
#: Use ONLY where the PREDICATE itself is the thing being forbidden for a
#: subject, regardless of what it points at -- e.g. an injected instruction
#: trying to manufacture an approval, where enumerating fake object ids is
#: a game the oracle cannot win.
ANY_OBJECT = EntityRef("<any>", "<any>")


@dataclass(frozen=True)
class FactExpectation:
    """One relationship a correct answer must (or must not) contain.

    ``subject``/``object`` are compared **positionally**: a reversed edge does
    not match. That is deliberate -- "deterministic structured relationships
    preserve exact canonical IDs and direction" is a §16 hard gate, and a
    direction-blind matcher would score a reversed ``blocked_by`` as correct.
    """

    subject: EntityRef
    predicate: str
    object: EntityRef
    require_claim_kind: ClaimKind | None = None
    require_evidence_refs: frozenset[str] = frozenset()
    #: Source event refs that must be ABSENT -- used to prove a redacted
    #: source was actually stripped rather than served back verbatim.
    forbid_source_event_refs: frozenset[str] = frozenset()
    require_flags: frozenset[str] = frozenset()
    forbid_flags: frozenset[str] = frozenset()
    require_invalidation_claim_kind: ClaimKind | None = None
    #: Evidence refs the INVALIDATING record must carry, not the fact's own
    #: opening evidence. Catches an arm (or a golden builder) that cites what
    #: opened a window as what closed it.
    require_invalidation_evidence_refs: frozenset[str] = frozenset()
    label: str = ""

    def identity_matches(self, fact: TemporalFact) -> bool:
        return (
            fact.subject_ref == self.subject
            and fact.predicate == self.predicate
            and (self.object is ANY_OBJECT or fact.object_ref == self.object)
        )

    def describe(self) -> str:
        if self.label:
            return self.label
        obj = "<ANY>" if self.object is ANY_OBJECT else self.object
        return f"{self.subject} -{self.predicate}-> {obj}"

    def qualify(self, fact: TemporalFact) -> list[str]:
        """Return the reasons *this* fact fails the expectation's qualifiers."""
        problems: list[str] = []
        if (
            self.require_claim_kind is not None
            and fact.claim_kind is not self.require_claim_kind
        ):
            problems.append(
                f"claim_kind={fact.claim_kind.value}, "
                f"expected {self.require_claim_kind.value}"
            )
        missing_evidence = self.require_evidence_refs - frozenset(fact.evidence_refs)
        if missing_evidence:
            problems.append(f"missing evidence refs {sorted(missing_evidence)}")
        # Checked against BOTH provenance channels, not just source_event_refs:
        # a redacted source is redacted, full stop, and must not be citable
        # via evidence_refs either. Checking only one channel would let a
        # response smuggle the redacted ref back in through the other one
        # and still pass -- redaction measured on one of two channels is not
        # redaction.
        forbidden_source_refs = self.forbid_source_event_refs & (
            frozenset(fact.source_event_refs) | frozenset(fact.evidence_refs)
        )
        if forbidden_source_refs:
            problems.append(
                f"redacted source refs still present {sorted(forbidden_source_refs)}"
            )
        present = _flag_names(fact)
        missing_flags = self.require_flags - present
        if missing_flags:
            problems.append(f"missing flags {sorted(missing_flags)}")
        forbidden = self.forbid_flags & present
        if forbidden:
            problems.append(f"forbidden flags present {sorted(forbidden)}")
        if self.require_invalidation_claim_kind is not None:
            actual = (
                fact.invalidated_by.invalidation_claim_kind
                if fact.invalidated_by is not None
                else None
            )
            if actual is not self.require_invalidation_claim_kind:
                problems.append(
                    "invalidation_claim_kind="
                    f"{actual.value if actual else 'absent'}, expected "
                    f"{self.require_invalidation_claim_kind.value}"
                )
        if self.require_invalidation_evidence_refs:
            actual_refs = (
                frozenset(fact.invalidated_by.refs)
                if fact.invalidated_by is not None
                else frozenset()
            )
            missing_invalidation = self.require_invalidation_evidence_refs - actual_refs
            if missing_invalidation:
                problems.append(
                    "invalidation missing evidence refs "
                    f"{sorted(missing_invalidation)} (has {sorted(actual_refs)}) -- "
                    "the invalidating record's own evidence must be cited, not "
                    "the fact's opening evidence"
                )
        return problems


def _flag_names(fact: TemporalFact) -> frozenset[str]:
    names = set()
    if fact.flags.conflicting:
        names.add("conflicting")
    if fact.flags.stale:
        names.add("stale")
    if fact.flags.untrusted_content:
        names.add("untrusted_content")
    return frozenset(names)


@dataclass(frozen=True)
class CoverageExpectation:
    """A source-coverage gap the answer must *declare* rather than hide.

    Corpus case 16: a squash-merge org has a near-empty ``work_graph_pr_commit``.
    An arm that returns an empty prior-attempts list with no coverage gap has
    told the caller "this never happened" when the truth is "we cannot see it".
    Those are different answers and only one of them is honest.
    """

    source: str
    expect_available: bool
    expect_reason_contains: str | None = None


# --------------------------------------------------------------------------
# Oracle
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Oracle:
    oracle_id: str
    question_id: str
    question_class: QuestionClass
    query: TemporalContextQuery
    rationale: str
    corpus_case_ids: tuple[str, ...] = ()
    must_include: tuple[FactExpectation, ...] = ()
    must_exclude: tuple[FactExpectation, ...] = ()
    coverage: tuple[CoverageExpectation, ...] = ()
    require_warnings: frozenset[str] = frozenset()
    forbid_warnings: frozenset[str] = frozenset()
    require_degraded_reasons: frozenset[str] = frozenset()
    max_inferred_facts: int | None = None
    expect_outcome: ArmOutcome = ArmOutcome.ANSWERED
    require_indexed_through_at_or_after: datetime | None = None
    #: Whether every returned fact must have its subject OR object among
    #: query.subjects. Default True: a real arm serving material for an
    #: entity nobody asked about (e.g. a repo_atlas_web episode when the
    #: query subject was repo_atlas_api only) is a leak that finding-10's
    #: fix made assertable in golden.py's builder but left invisible to real
    #: arms, since nothing in Oracle.evaluate checked it. False only for the
    #: same load-bearing exemptions golden.py's Scenario.apply_subject_filter
    #: carries (see that field's docstring for why): the query subject has
    #: no literal ground-truth edge to a fact this oracle requires, so
    #: asserting scope here would make the oracle reject its own correct
    #: answer.
    require_subject_scoped: bool = True

    def __post_init__(self) -> None:
        if self.query.query_mode is QueryMode.AS_OF and self.query.axis is None:
            raise ValueError(f"{self.oracle_id}: as-of oracle must pin a time axis")
        if not self.must_include and not self.must_exclude and not self.coverage:
            raise ValueError(
                f"{self.oracle_id}: an oracle with no include/exclude/coverage "
                "expectation cannot fail, and a test that cannot fail is worse "
                "than no test"
            )

    @property
    def axis(self) -> TimeAxis | None:
        return self.query.axis

    # -- evaluation --------------------------------------------------------

    def evaluate(self, response: ArmResponse) -> OracleResult:
        results: list[AssertionResult] = []

        if response.outcome is ArmOutcome.NOT_RUN:
            return OracleResult(
                oracle_id=self.oracle_id,
                arm=response.arm,
                question_class=self.question_class,
                assertions=(
                    AssertionResult(
                        assertion_id="measurement_happened",
                        verdict=Verdict.NOT_MEASURED,
                        detail=(
                            "arm reported NOT_RUN "
                            f"({', '.join(response.degraded_reasons) or 'no reason'}); "
                            "an unmeasured oracle is never a pass"
                        ),
                    ),
                ),
            )

        results.append(self._assert_outcome(response))
        if response.outcome is not self.expect_outcome:
            # Content assertions against the wrong outcome are noise; the
            # outcome mismatch is the finding.
            return OracleResult(
                oracle_id=self.oracle_id,
                arm=response.arm,
                question_class=self.question_class,
                assertions=tuple(results),
            )

        results.append(self._assert_axis_echoed(response))
        for expectation in self.must_include:
            results.append(self._assert_included(expectation, response.facts))
        for expectation in self.must_exclude:
            results.append(self._assert_excluded(expectation, response.facts))
        for coverage in self.coverage:
            results.append(self._assert_coverage(coverage, response))
        results.append(self._assert_subject_scoped(response.facts))
        results.append(self._assert_warnings(response))
        results.append(self._assert_provenance_closure(response.facts))
        if self.max_inferred_facts is not None:
            results.append(self._assert_inferred_budget(response.facts))
        if self.require_indexed_through_at_or_after is not None:
            results.append(self._assert_watermark(response))

        return OracleResult(
            oracle_id=self.oracle_id,
            arm=response.arm,
            question_class=self.question_class,
            assertions=tuple(results),
        )

    # -- individual assertions --------------------------------------------

    def _assert_outcome(self, response: ArmResponse) -> AssertionResult:
        ok = response.outcome is self.expect_outcome
        return AssertionResult(
            assertion_id="arm_outcome",
            verdict=Verdict.PASS if ok else Verdict.FAIL,
            detail=(
                f"outcome={response.outcome.value}, "
                f"expected {self.expect_outcome.value}"
            ),
        )

    def _assert_axis_echoed(self, response: ArmResponse) -> AssertionResult:
        """The arm must answer the axis it was asked, and say so.

        An arm that drops ``axis`` on the floor still returns *an* answer; on
        the axis-pair corpus case (19) that answer is right on one axis and
        wrong on the other, and without this assertion the failure is
        attributed to retrieval quality rather than to a dropped field.
        """
        if self.query.query_mode is not QueryMode.AS_OF:
            return AssertionResult("axis_echoed", Verdict.PASS, "not an as-of query")
        echoed = response.query.axis if response.query is not None else None
        ok = echoed is self.query.axis
        return AssertionResult(
            assertion_id="axis_echoed",
            verdict=Verdict.PASS if ok else Verdict.FAIL,
            detail=(
                f"echoed axis={echoed.value if echoed else 'absent'}, "
                f"asked {self.query.axis.value if self.query.axis else 'absent'}"
            ),
        )

    def _assert_included(
        self, expectation: FactExpectation, facts: Sequence[TemporalFact]
    ) -> AssertionResult:
        assertion_id = f"must_include:{expectation.describe()}"
        candidates = [f for f in facts if expectation.identity_matches(f)]
        if not candidates:
            reversed_hit = any(
                f.subject_ref == expectation.object
                and f.object_ref == expectation.subject
                and f.predicate == expectation.predicate
                for f in facts
            )
            detail = "not returned"
            if reversed_hit:
                detail = "returned with subject/object REVERSED (direction gate)"
            return AssertionResult(assertion_id, Verdict.FAIL, detail)
        problems = [expectation.qualify(f) for f in candidates]
        if any(not p for p in problems):
            return AssertionResult(assertion_id, Verdict.PASS, "matched")
        flat = "; ".join(sorted({item for p in problems for item in p}))
        return AssertionResult(
            assertion_id,
            Verdict.FAIL,
            f"returned but failed qualifiers: {flat}",
        )

    def _assert_excluded(
        self, expectation: FactExpectation, facts: Sequence[TemporalFact]
    ) -> AssertionResult:
        assertion_id = f"must_exclude:{expectation.describe()}"
        hits = [f for f in facts if expectation.identity_matches(f)]
        if hits:
            return AssertionResult(
                assertion_id,
                Verdict.FAIL,
                f"forbidden fact returned ({len(hits)} occurrence(s)): "
                f"{hits[0].fact_id}",
            )
        return AssertionResult(assertion_id, Verdict.PASS, "absent")

    def _assert_coverage(
        self, coverage: CoverageExpectation, response: ArmResponse
    ) -> AssertionResult:
        assertion_id = f"coverage:{coverage.source}"
        entry = response.source_coverage.get(coverage.source)
        if entry is None:
            return AssertionResult(
                assertion_id,
                Verdict.FAIL,
                "source coverage not declared at all; an undeclared gap is "
                "indistinguishable from 'no history exists'",
            )
        if entry.available is not coverage.expect_available:
            return AssertionResult(
                assertion_id,
                Verdict.FAIL,
                f"available={entry.available}, expected {coverage.expect_available}",
            )
        if coverage.expect_reason_contains is not None:
            reason = entry.reason or ""
            if coverage.expect_reason_contains not in reason:
                return AssertionResult(
                    assertion_id,
                    Verdict.FAIL,
                    f"reason={reason!r} lacks {coverage.expect_reason_contains!r}",
                )
        return AssertionResult(assertion_id, Verdict.PASS, "declared as expected")

    def _assert_subject_scoped(self, facts: Sequence[TemporalFact]) -> AssertionResult:
        """No fact may sit outside the entities the query actually asked about.

        Complements golden.py's Scenario.apply_subject_filter, which only
        makes the *reference* answer scoped -- with nothing checking a real
        arm's response, an arm that leaked material for an unqueried entity
        (finding 10's exact scenario) passed every oracle anyway, because the
        leak was never asserted against.
        """
        if not self.require_subject_scoped or not self.query.subjects:
            return AssertionResult(
                "subject_scoped", Verdict.PASS, "oracle does not scope by subject"
            )
        subjects = frozenset(self.query.subjects)
        out_of_scope = [
            f.fact_id
            for f in facts
            if f.subject_ref not in subjects and f.object_ref not in subjects
        ]
        return AssertionResult(
            assertion_id="subject_scoped",
            verdict=Verdict.FAIL if out_of_scope else Verdict.PASS,
            detail=(
                f"fact(s) outside query subjects {sorted(s.id for s in subjects)}: "
                f"{out_of_scope}"
                if out_of_scope
                else "all facts within query subjects"
            ),
        )

    def _assert_warnings(self, response: ArmResponse) -> AssertionResult:
        present = frozenset(response.warnings)
        degraded = frozenset(response.degraded_reasons)
        missing = self.require_warnings - present
        forbidden = self.forbid_warnings & present
        missing_degraded = self.require_degraded_reasons - degraded
        problems = []
        if missing:
            problems.append(f"missing warnings {sorted(missing)}")
        if forbidden:
            problems.append(f"forbidden warnings {sorted(forbidden)}")
        if missing_degraded:
            problems.append(f"missing degraded_reasons {sorted(missing_degraded)}")
        return AssertionResult(
            assertion_id="warnings",
            verdict=Verdict.FAIL if problems else Verdict.PASS,
            detail="; ".join(problems) or "as expected",
        )

    def _assert_provenance_closure(
        self, facts: Sequence[TemporalFact]
    ) -> AssertionResult:
        """§16 hard gate: every ``observed`` fact closes to retained evidence.

        Applied to *every* oracle rather than opted into per case, because a
        gate that only runs where someone remembered to request it is not a
        gate.

        A closed validity window (``valid_to is not None``) must carry an
        ``invalidated_by`` with NON-EMPTY ``refs`` -- checking only ``is
        None`` lets an adapter satisfy this gate vacuously with a
        fabricated ``Invalidation(refs=())``, which cites nothing at all.
        Applied for every ``claim_kind``, matching ``open_facts`` above:
        an uncited closure is exactly as much a provenance gap as an
        uncited fact.
        """
        open_facts = [
            f.fact_id
            for f in facts
            if f.claim_kind is ClaimKind.OBSERVED
            and not f.evidence_refs
            and not f.source_event_refs
        ]
        dangling_endpoints = [
            f.fact_id
            for f in facts
            if f.valid_to is not None
            and (f.invalidated_by is None or not f.invalidated_by.refs)
        ]
        problems = []
        if open_facts:
            problems.append(f"observed facts with no provenance: {open_facts}")
        if dangling_endpoints:
            problems.append(
                f"closed validity window with no invalidated_by: {dangling_endpoints}"
            )
        return AssertionResult(
            assertion_id="provenance_closure",
            verdict=Verdict.FAIL if problems else Verdict.PASS,
            detail="; ".join(problems) or "all facts close to provenance",
        )

    def _assert_inferred_budget(self, facts: Sequence[TemporalFact]) -> AssertionResult:
        assert self.max_inferred_facts is not None
        inferred = [f.fact_id for f in facts if f.claim_kind is ClaimKind.INFERRED]
        ok = len(inferred) <= self.max_inferred_facts
        return AssertionResult(
            assertion_id="inferred_budget",
            verdict=Verdict.PASS if ok else Verdict.FAIL,
            detail=(
                f"{len(inferred)} inferred fact(s), budget "
                f"{self.max_inferred_facts}: {inferred}"
            ),
        )

    def _assert_watermark(self, response: ArmResponse) -> AssertionResult:
        assert self.require_indexed_through_at_or_after is not None
        watermark = response.indexed_through
        if watermark is None:
            return AssertionResult(
                "indexed_through",
                Verdict.FAIL,
                "no watermark published; staleness is unassertable",
            )
        ok = watermark >= self.require_indexed_through_at_or_after
        return AssertionResult(
            assertion_id="indexed_through",
            verdict=Verdict.PASS if ok else Verdict.FAIL,
            detail=(
                f"indexed_through={watermark.isoformat()}, required at or after "
                f"{self.require_indexed_through_at_or_after.isoformat()}"
            ),
        )
