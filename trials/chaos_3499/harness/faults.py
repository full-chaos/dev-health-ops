"""Fault-mode self-tests: proof that each assertion rejects what it claims to.

An oracle that passes on a correct answer has demonstrated nothing. The thing
worth demonstrating is that it *fails* on the specific defect it exists to
catch, and fails **in the assertion that claims to catch it** -- a mutation
that dies somewhere else has proved nothing about the invariant it was aimed
at.

Two design decisions are load-bearing:

* **Mutators see the oracle, not just the response.** A mutator that blindly
  corrupts ``facts[0]`` corrupts whichever fact happened to sort first, which
  may be one no expectation covers -- and then "the oracle did not fail" is a
  statement about the mutation, not about the oracle. Targeting a fact the
  oracle actually requires is what makes a caught fault meaningful.
* **Applicability is decided against what the oracle asserts.** A fault that
  cannot bite a given oracle reports ``INAPPLICABLE`` rather than producing a
  mutation nothing is watching for. The suite then fails on any fault that is
  inapplicable *everywhere*, so a fault that silently no-ops cannot masquerade
  as a fault that was caught.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

from .contracts import (
    PROJECTION_VERSION,
    ArmOutcome,
    ArmResponse,
    ClaimKind,
    EntityRef,
    FactFlags,
    QueryMode,
    SourceCoverage,
    TemporalFact,
    TimeAxis,
)
from .oracle import FactExpectation, Oracle


class FaultApplication(str, Enum):
    APPLIED = "applied"
    INAPPLICABLE = "inapplicable"


@dataclass(frozen=True)
class MutationOutcome:
    application: FaultApplication
    response: ArmResponse
    note: str = ""


Mutator = Callable[[Oracle, ArmResponse], MutationOutcome]


@dataclass(frozen=True)
class FaultMode:
    fault_id: str
    description: str
    expected_assertion_id: str
    mutate: Mutator

    def apply(self, oracle: Oracle, response: ArmResponse) -> MutationOutcome:
        return self.mutate(oracle, response)


def _skip(response: ArmResponse, note: str) -> MutationOutcome:
    return MutationOutcome(FaultApplication.INAPPLICABLE, response, note)


def _applied(response: ArmResponse) -> MutationOutcome:
    return MutationOutcome(FaultApplication.APPLIED, response)


def _required_fact(
    oracle: Oracle, response: ArmResponse
) -> tuple[int, TemporalFact, FactExpectation] | None:
    """Find a returned fact that one of the oracle's must_include covers.

    Mutating anything else would be mutating a fact the oracle never promised
    to check, which tells us nothing about the oracle.
    """
    for expectation in oracle.must_include:
        for index, fact in enumerate(response.facts):
            if expectation.identity_matches(fact):
                return index, fact, expectation
    return None


def _swap(response: ArmResponse, index: int, fact: TemporalFact) -> ArmResponse:
    facts = list(response.facts)
    facts[index] = fact
    return dataclasses.replace(response, facts=tuple(facts))


# --------------------------------------------------------------------------
# Mutators
# --------------------------------------------------------------------------


def omit_required_fact(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    target = _required_fact(oracle, response)
    if target is None:
        return _skip(response, "oracle requires no fact that was returned")
    index, _, _ = target
    facts = list(response.facts)
    del facts[index]
    return _applied(dataclasses.replace(response, facts=tuple(facts)))


def reverse_required_edge(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """Swap subject and object on a required edge -- the §16 direction gate."""
    target = _required_fact(oracle, response)
    if target is None:
        return _skip(response, "oracle requires no fact that was returned")
    index, fact, _ = target
    if fact.subject_ref == fact.object_ref:
        return _skip(response, "self-edge: reversal is a no-op")
    return _applied(
        _swap(
            response,
            index,
            dataclasses.replace(
                fact, subject_ref=fact.object_ref, object_ref=fact.subject_ref
            ),
        )
    )


def substitute_canonical_id(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """Keep the shape, corrupt the canonical id.

    Catches an arm that minted its own identity for a canonical entity, which
    reads as a correct-looking edge between the wrong things.
    """
    target = _required_fact(oracle, response)
    if target is None:
        return _skip(response, "oracle requires no fact that was returned")
    index, fact, _ = target
    imposter = EntityRef(fact.object_ref.kind, fact.object_ref.id + "-imposter")
    return _applied(
        _swap(response, index, dataclasses.replace(fact, object_ref=imposter))
    )


def downgrade_required_to_inferred(
    oracle: Oracle, response: ArmResponse
) -> MutationOutcome:
    """An observed relationship quietly becomes model-composed."""
    for expectation in oracle.must_include:
        if expectation.require_claim_kind is not ClaimKind.OBSERVED:
            continue
        for index, fact in enumerate(response.facts):
            if expectation.identity_matches(fact):
                return _applied(
                    _swap(
                        response,
                        index,
                        dataclasses.replace(fact, claim_kind=ClaimKind.INFERRED),
                    )
                )
    return _skip(response, "oracle demands no observed claim kind")


def strip_evidence_provenance(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    observed = [
        (i, f)
        for i, f in enumerate(response.facts)
        if f.claim_kind is ClaimKind.OBSERVED
    ]
    if not observed:
        return _skip(response, "no observed facts to strip")
    index, fact = observed[0]
    return _applied(
        _swap(
            response,
            index,
            dataclasses.replace(fact, evidence_refs=(), source_event_refs=()),
        )
    )


def strip_invalidation_provenance(
    oracle: Oracle, response: ArmResponse
) -> MutationOutcome:
    """A closed validity window with no record of what closed it.

    The endpoint-laundering path of PRD §6.3: the fact stays ``observed``
    while its ``valid_to`` came from LLM judgement.
    """
    closed = [
        (i, f)
        for i, f in enumerate(response.facts)
        if f.valid_to is not None and f.invalidated_by is not None
    ]
    if not closed:
        return _skip(response, "no closed validity windows in this answer")
    index, fact = closed[0]
    return _applied(
        _swap(response, index, dataclasses.replace(fact, invalidated_by=None))
    )


def clear_required_flag(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """Drop a flag the oracle requires -- e.g. conflicting, or stale."""
    for expectation in oracle.must_include:
        if not expectation.require_flags:
            continue
        for index, fact in enumerate(response.facts):
            if expectation.identity_matches(fact):
                return _applied(
                    _swap(response, index, dataclasses.replace(fact, flags=FactFlags()))
                )
    return _skip(response, "oracle requires no fact flags")


def restore_redacted_source_ref(
    oracle: Oracle, response: ArmResponse
) -> MutationOutcome:
    """Serve a redacted source ref back verbatim -- the redaction that never happened.

    Built from ``forbid_source_event_refs`` the same way the other must_include
    mutators are built from their own qualifier: it targets exactly the ref
    the oracle asserts must be gone, so a mutation that dies elsewhere proves
    nothing about the redaction invariant.
    """
    for expectation in oracle.must_include:
        if not expectation.forbid_source_event_refs:
            continue
        for index, fact in enumerate(response.facts):
            if expectation.identity_matches(fact):
                restored = tuple(
                    set(fact.source_event_refs) | expectation.forbid_source_event_refs
                )
                return _applied(
                    _swap(
                        response,
                        index,
                        dataclasses.replace(fact, source_event_refs=restored),
                    )
                )
    return _skip(response, "oracle forbids no source event ref")


def smuggle_redacted_ref_via_evidence(
    oracle: Oracle, response: ArmResponse
) -> MutationOutcome:
    """Serve a redacted ref back through evidence_refs instead of source_event_refs.

    The other half of the redaction-laundering path: an arm that cannot
    re-emit a redacted ref as a source event might still cite it as
    evidence. Checking only source_event_refs for the forbidden ref would
    let this smuggling attempt pass; the qualifier checks both channels.
    """
    for expectation in oracle.must_include:
        if not expectation.forbid_source_event_refs:
            continue
        for index, fact in enumerate(response.facts):
            if expectation.identity_matches(fact):
                smuggled = tuple(
                    set(fact.evidence_refs) | expectation.forbid_source_event_refs
                )
                return _applied(
                    _swap(
                        response,
                        index,
                        dataclasses.replace(fact, evidence_refs=smuggled),
                    )
                )
    return _skip(response, "oracle forbids no source event ref")


def cite_opening_evidence_as_invalidation(
    oracle: Oracle, response: ArmResponse
) -> MutationOutcome:
    """Regress invalidation provenance to the fact's own opening evidence.

    The exact CHAOS-3499 defect: citing what opened a window as what closed
    it, when the oracle names the record that actually closed it.
    """
    for expectation in oracle.must_include:
        if not expectation.require_invalidation_evidence_refs:
            continue
        for index, fact in enumerate(response.facts):
            if not expectation.identity_matches(fact) or fact.invalidated_by is None:
                continue
            return _applied(
                _swap(
                    response,
                    index,
                    dataclasses.replace(
                        fact,
                        invalidated_by=dataclasses.replace(
                            fact.invalidated_by,
                            refs=fact.evidence_refs or fact.source_event_refs,
                        ),
                    ),
                )
            )
    return _skip(response, "oracle requires no specific invalidation evidence")


def emit_forbidden_fact(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """Return something the oracle forbids: a leak, an injection, a poisoning.

    Built from the oracle's own ``must_exclude`` because that is precisely the
    material the corpus planted to be excluded -- cross-tenant rows, injected
    instructions, entity-linking poison, superseded intervals.
    """
    if not oracle.must_exclude:
        return _skip(response, "oracle forbids nothing")
    forbidden = oracle.must_exclude[0]
    leaked = TemporalFact(
        fact_id=f"tf_forbidden_{forbidden.predicate}",
        subject_ref=forbidden.subject,
        predicate=forbidden.predicate,
        object_ref=forbidden.object,
        observed_at=(
            response.facts[0].observed_at
            if response.facts
            else response.indexed_through or _FALLBACK_TIME
        ),
        claim_kind=ClaimKind.OBSERVED,
        projection_version=PROJECTION_VERSION,
        evidence_refs=("ev1_should_not_be_here",),
    )
    return _applied(dataclasses.replace(response, facts=(*response.facts, leaked)))


def leak_out_of_subject_fact(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """Return a fact for an entity nobody asked about -- finding 10's leak.

    Unlike ``emit_forbidden_fact``, this is not built from the oracle's
    ``must_exclude`` (a leak outside query.subjects is wrong regardless of
    whether anyone thought to name it as forbidden); it is a synthetic
    subject/object guaranteed not to collide with the oracle's own query
    subjects.
    """
    if not oracle.require_subject_scoped or not oracle.query.subjects:
        return _skip(response, "oracle does not scope by subject")
    subjects = frozenset(oracle.query.subjects)
    leaked = TemporalFact(
        fact_id="tf_forbidden_out_of_subject_leak",
        subject_ref=EntityRef("agent_episode", "ep_out_of_subject_leak"),
        predicate="touched",
        object_ref=EntityRef("repository", "repo_out_of_subject_leak"),
        observed_at=(
            response.facts[0].observed_at
            if response.facts
            else response.indexed_through or _FALLBACK_TIME
        ),
        claim_kind=ClaimKind.OBSERVED,
        projection_version=PROJECTION_VERSION,
        evidence_refs=("ev1_out_of_subject_leak",),
    )
    assert leaked.subject_ref not in subjects and leaked.object_ref not in subjects, (
        "synthetic leak entity collided with a real query subject -- fault "
        "mode needs a different synthetic id"
    )
    return _applied(dataclasses.replace(response, facts=(*response.facts, leaked)))


def hide_coverage_gap(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """Report an unavailable source as available: silent emptiness.

    Targets a source named in the oracle's OWN ``coverage`` expectations, not
    just the first unavailable entry in the response. Picking an arbitrary
    response-side gap can hit a source no assertion is watching -- on a
    two-gap scenario that mutates the wrong key, leaves the oracle's actual
    coverage expectation intact, and the fault is scored as caught when it
    never touched anything the oracle checks.
    """
    watched = [cov.source for cov in oracle.coverage]
    gaps = [
        source
        for source in watched
        if (entry := response.source_coverage.get(source)) is not None
        and not entry.available
    ]
    if not gaps:
        return _skip(response, "no declared coverage gap the oracle asserts against")
    coverage = dict(response.source_coverage)
    key = gaps[0]
    coverage[key] = SourceCoverage(source=key, available=True, reason=None)
    return _applied(dataclasses.replace(response, source_coverage=coverage))


def answer_the_other_axis(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """The arm honoured the wrong time axis.

    Flipping the echoed axis rather than deleting it keeps the mutated
    response a *valid* query -- an arm that silently answers observed-time
    when asked valid-time is a realistic defect; one that emits a
    schema-invalid query is not.
    """
    if response.query is None or response.query.query_mode is not QueryMode.AS_OF:
        return _skip(response, "not an as-of query")
    current = response.query.axis
    if current is None:
        return _skip(response, "no axis echoed to flip")
    other = (
        TimeAxis.OBSERVED_TIME
        if current is TimeAxis.VALID_TIME
        else TimeAxis.VALID_TIME
    )
    return _applied(
        dataclasses.replace(
            response, query=dataclasses.replace(response.query, axis=other)
        )
    )


def rewind_watermark(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    if oracle.require_indexed_through_at_or_after is None:
        return _skip(response, "oracle asserts no freshness requirement")
    if response.indexed_through is None:
        return _skip(response, "no watermark published")
    return _applied(
        dataclasses.replace(
            response, indexed_through=response.indexed_through - timedelta(days=30)
        )
    )


def suppress_warnings(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    if not oracle.require_warnings and not oracle.require_degraded_reasons:
        return _skip(response, "oracle requires no warnings")
    return _applied(dataclasses.replace(response, warnings=(), degraded_reasons=()))


def answer_through_outage(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """An arm that was supposed to be down returns results instead."""
    if oracle.expect_outcome is not ArmOutcome.UNAVAILABLE:
        return _skip(response, "oracle does not expect an unavailable outcome")
    return _applied(dataclasses.replace(response, outcome=ArmOutcome.ANSWERED))


def measurement_never_ran(oracle: Oracle, response: ArmResponse) -> MutationOutcome:
    """The one fault that applies to every oracle without exception.

    It is the failure this whole harness exists to make impossible to miss:
    an oracle that was never measured must never read as a pass.
    """
    return _applied(
        ArmResponse.not_run(response.arm, "fault-mode: harness never ran the arm")
    )


_FALLBACK_TIME = __import__("datetime").datetime(
    2026, 7, 31, tzinfo=__import__("datetime").UTC
)


# --------------------------------------------------------------------------
# Registry
# --------------------------------------------------------------------------

FAULT_MODES: tuple[FaultMode, ...] = (
    FaultMode(
        "omit_expected_evidence",
        "drop a fact the oracle requires",
        "must_include",
        omit_required_fact,
    ),
    FaultMode(
        "reverse_edge_direction",
        "return a required relationship with subject and object swapped",
        "must_include",
        reverse_required_edge,
    ),
    FaultMode(
        "substitute_canonical_id",
        "attach a required relationship to a look-alike, non-canonical id",
        "must_include",
        substitute_canonical_id,
    ),
    FaultMode(
        "downgrade_observed_to_inferred",
        "present a required observed relationship as model-composed",
        "must_include",
        downgrade_required_to_inferred,
    ),
    FaultMode(
        "clear_required_flag",
        "return a required fact without its conflict/staleness flag",
        "must_include",
        clear_required_flag,
    ),
    FaultMode(
        "restore_redacted_source_ref",
        "serve a redacted source ref back verbatim, as if never redacted",
        "must_include",
        restore_redacted_source_ref,
    ),
    FaultMode(
        "smuggle_redacted_ref_via_evidence",
        "cite a redacted ref through evidence_refs instead of source_event_refs",
        "must_include",
        smuggle_redacted_ref_via_evidence,
    ),
    FaultMode(
        "cite_opening_evidence_as_invalidation",
        "cite the fact's own opening evidence as what closed its window",
        "must_include",
        cite_opening_evidence_as_invalidation,
    ),
    FaultMode(
        "emit_forbidden_fact",
        "return material the oracle forbids (leak, injection, poisoning)",
        "must_exclude",
        emit_forbidden_fact,
    ),
    FaultMode(
        "leak_out_of_subject_fact",
        "return a fact for an entity the query never asked about",
        "subject_scoped",
        leak_out_of_subject_fact,
    ),
    FaultMode(
        "strip_evidence_provenance",
        "return an observed fact that closes to nothing",
        "provenance_closure",
        strip_evidence_provenance,
    ),
    FaultMode(
        "strip_invalidation_provenance",
        "close a validity window without recording what closed it",
        "provenance_closure",
        strip_invalidation_provenance,
    ),
    FaultMode(
        "hide_source_coverage_gap",
        "report an unavailable source as available (silent emptiness)",
        "coverage",
        hide_coverage_gap,
    ),
    FaultMode(
        "answer_the_other_axis",
        "honour the opposite time axis to the one requested",
        "axis_echoed",
        answer_the_other_axis,
    ),
    FaultMode(
        "rewind_indexing_watermark",
        "publish a watermark older than the corpus requires",
        "indexed_through",
        rewind_watermark,
    ),
    FaultMode(
        "suppress_warnings",
        "drop the degradation warnings the answer depends on",
        "warnings",
        suppress_warnings,
    ),
    FaultMode(
        "answer_through_outage",
        "return results when the correct answer is explicit unavailability",
        "arm_outcome",
        answer_through_outage,
    ),
    FaultMode(
        "measurement_never_ran",
        "the harness never invoked the arm at all",
        "measurement_happened",
        measurement_never_ran,
    ),
)

FAULT_MODES_BY_ID = {mode.fault_id: mode for mode in FAULT_MODES}
