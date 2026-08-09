"""CHAOS-3629 / CHAOS-3630: the lineage emitter stops asserting constants.

Two defects in one function, so one suite.

**CHAOS-3629 — relevance was a literal.** ``RelevanceState.CURRENT`` appeared
at eight construction sites and nothing anywhere computed relevance. The
corpus plants ``dep_pulse_ratelimitd`` (``proj_pulse -depends_on->
dep_ratelimitd``, ``valid_to`` 2026-06-12, two months before ``TRIAL_NOW``)
precisely so a historical relationship can be seen being reported as
historical; ``PathStep.is_current_at`` already returned ``False`` for it and
the emitter threw that away. CHAOS-3619's ``current_relevance`` dimension was
scoring a constant.

**CHAOS-3630 — path evidence was a literal.** ``LineagePath.evidence_ref_ids``
was ``()``. The data was present the whole time: corpus edges carry
``observation_ids`` derived from evidence slugs, and ``PathStep`` carries them
through. Drivers close to evidence (``_assert_support_is_closed``);
relationships could not, because the field was a constant.

**What "honest" means at each site, and why it differs.** The rule is not
"compute something everywhere" — that would replace one invention with
another. It is: derive where the readout carries validity, and say
``UNKNOWN`` where it does not.

* hops carry a validity interval, so they are derived;
* a path is only as current as its weakest hop;
* an entity is as current as the *best* path that reaches it — one live route
  is enough;
* an observation carries no interval at all, so evidence entries are
  ``UNKNOWN``. That is the contract's own word for "no basis to say", and it
  is the honest answer until observations carry validity. Saying ``CURRENT``
  there is the defect this ticket exists to remove.

Input symmetry is seeded at both ends throughout: a historical edge must read
historical AND a current edge must still read current, or a mutation that
hardcoded ``HISTORICAL_ONLY`` would pass everything here.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
    RelevanceState,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    TrialContext,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

_PRODUCED_AT = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
_RUN_ID = "3c629a33-4444-4555-8666-777788889999"

#: The planted historical dependency, and the evidence the corpus attaches to
#: it. Named here so a corpus change that removes either makes these tests
#: fail loudly rather than quietly stop testing anything.
_HISTORICAL_SOURCE = "proj_pulse"
_HISTORICAL_TARGET = "dep_ratelimitd"
_HISTORICAL_EVIDENCE = "wg_ratelimitd_removed"


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


def _read(projection, seed: str, *, max_hops: int = 2):
    grant = sorted(adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST))
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[seed],
            authorized_entity_ids=grant,
            max_hops=max_hops,
        )
    )


def _packet(readout, signer):
    return build_packet(
        readout=readout,
        job=JobContext(
            job_id="job_chaos_3629",
            question_family=QuestionFamilyID.PROJECT_STATUS_DRIVERS,
            job_statement="What is the current state of this subject?",
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
            window_start=world.WINDOW_START,
            window_end=world.WINDOW_END,
        ),
        watermark=IndexWatermark(
            indexed_through=world.WINDOW_END,
            projected_at=world.WINDOW_END,
            records_indexed=len(readout.entities),
        ),
        signer=signer,
        trial=TrialContext(run_id=_RUN_ID),
        produced_at=_PRODUCED_AT,
    )


def _hops(packet):
    for path in packet.related_context.paths:
        for hop in path.hops:
            yield path, hop


def _historical_hop(packet):
    for path, hop in _hops(packet):
        if (
            hop.source_entity_id == _HISTORICAL_SOURCE
            and hop.target_entity_id == _HISTORICAL_TARGET
        ):
            return path, hop
    raise AssertionError(
        "the planted historical dependency is not in this packet; every "
        "assertion about it would pass vacuously"
    )


class TestRelevanceIsDerivedNotDeclared:
    def test_the_planted_historical_dependency_reads_historical(
        self, helio, signer
    ) -> None:
        """The corpus planted it to be reported. Now it is."""

        edge = world.RELATIONSHIPS_BY_KEY["dep_pulse_ratelimitd"]
        assert edge.valid_to is not None and edge.valid_to < world.WINDOW_START, (
            "the corpus no longer plants this edge as closed before the "
            "window; this test would pass vacuously"
        )

        _, hop = _historical_hop(_packet(_read(helio, _HISTORICAL_SOURCE), signer))

        assert hop.relevance is RelevanceState.HISTORICAL_ONLY

    def test_a_live_dependency_still_reads_current(self, helio, signer) -> None:
        """The other end of the pair.

        Without this, hardcoding ``HISTORICAL_ONLY`` would satisfy every other
        assertion in this class — one constant swapped for another.
        """

        packet = _packet(_read(helio, _HISTORICAL_SOURCE), signer)
        current = [
            hop for _, hop in _hops(packet) if hop.relevance is RelevanceState.CURRENT
        ]

        assert current, "no hop reads current; relevance is a constant again"

    def test_a_path_is_only_as_current_as_its_weakest_hop(self, helio, signer) -> None:
        packet = _packet(_read(helio, _HISTORICAL_SOURCE), signer)
        path, _ = _historical_hop(packet)

        assert path.relevance is RelevanceState.HISTORICAL_ONLY

    def test_evidence_entries_say_unknown_rather_than_claiming_current(
        self, helio, signer
    ) -> None:
        """An observation carries no validity interval, so the arm has none.

        ``UNKNOWN`` is the contract's word for exactly that. Claiming
        ``CURRENT`` would be an assertion about a record's continued relevance
        that nothing in the readout supports.
        """

        packet = _packet(_read(helio, _HISTORICAL_SOURCE), signer)

        assert packet.evidence_coverage.evidence_index
        for entry in packet.evidence_coverage.evidence_index:
            assert entry.relevance is RelevanceState.UNKNOWN

    def test_the_packet_no_longer_reports_one_relevance_everywhere(
        self, helio, signer
    ) -> None:
        """The whole-packet form of the defect, in one assertion.

        Every site emitting the same member is what "relevance is a constant"
        looks like from outside, and it is what CHAOS-3619's dimension was
        measuring.
        """

        packet = _packet(_read(helio, _HISTORICAL_SOURCE), signer)
        seen = (
            {hop.relevance for _, hop in _hops(packet)}
            | {path.relevance for path in packet.related_context.paths}
            | {entry.relevance for entry in packet.evidence_coverage.evidence_index}
            | {entity.relevance for entity in packet.related_context.entities}
        )

        assert len(seen) > 1, f"every site still reports {seen}"


class TestLineagePathsCloseToEvidence:
    def test_the_historical_edges_evidence_reaches_the_path(
        self, helio, signer
    ) -> None:
        """The data existed the whole time; the emitter dropped it."""

        edge = world.RELATIONSHIPS_BY_KEY["dep_pulse_ratelimitd"]
        assert _HISTORICAL_EVIDENCE in edge.evidence_slugs

        path, _ = _historical_hop(_packet(_read(helio, _HISTORICAL_SOURCE), signer))

        assert world.EVIDENCE_BY_SLUG[_HISTORICAL_EVIDENCE].handle in (
            path.evidence_ref_ids
        )

    def test_every_path_reference_is_in_the_evidence_index(self, helio, signer) -> None:
        """Closure, the same property drivers already had.

        A handle cited by a path but absent from the index is a reference the
        packet does not actually carry — which is the fault
        ``_assert_support_is_closed`` exists to prevent for drivers.
        """

        packet = _packet(_read(helio, _HISTORICAL_SOURCE), signer)
        indexed = {
            entry.evidence.evidence_ref_id
            for entry in packet.evidence_coverage.evidence_index
        }
        cited = {
            handle
            for path in packet.related_context.paths
            for handle in path.evidence_ref_ids
        }

        assert cited, "no path cites evidence; this would pass vacuously"
        assert not cited - indexed, (
            f"unindexed path evidence: {sorted(cited - indexed)}"
        )

    def test_a_path_whose_edges_carry_no_evidence_says_so(self, helio, signer) -> None:
        """Honest emptiness, not fabrication.

        Half the corpus's edges carry no evidence slugs at all. Those paths
        must emit no references rather than borrowing one from a neighbour,
        and this is what stops "thread the ids through" from becoming "put
        something in the field".
        """

        packet = _packet(_read(helio, "proj_identity_rewrite"), signer)
        readout = _read(helio, "proj_identity_rewrite")
        evidence_free = {
            path.path_id
            for path in readout.paths
            if not any(step.observation_ids for step in path.steps)
        }

        assert evidence_free, "no evidence-free path exists; vacuous"
        for path in packet.related_context.paths:
            if path.path_id in evidence_free:
                assert path.evidence_ref_ids == ()
