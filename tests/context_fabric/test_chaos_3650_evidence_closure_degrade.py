"""CHAOS-3650: canonical evidence refusal narrows the answer, it does not
crash it.

Before this fix, ``packet_builder.py::_driver_candidate`` could not tell two
very different reasons a driver's cited evidence carries no handle apart:
nothing observed it (an internal inconsistency -- discovery and emission
disagree), or the canonical evidence service legitimately declined to admit
a record the arm's traversal genuinely reached. Both reached the same
``ValueError("... never indexed ... Discovery and emission disagree")``,
which is a real defect ONLY in the first case and an honest refusal
mislabelled as an arm bug in the second -- and either way, one driver's
missing support aborted the WHOLE packet, including unrelated drivers that
never cited the refused record.

The reproduction below uses ``admitted_evidence={}`` -- an admission map
that is present (so the admission code path runs) but empty (so every
locator is refused) -- which is the simplest possible shape of "canonical
admission declined everything the traversal reached" and needs no synthetic
``EvidenceCandidate``/``EvidenceService`` plumbing to construct honestly.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    DriverExclusionReason,
    DriverStanding,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.drivers import discover_drivers
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    TrialContext,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

_PROBE_NOW = world.TRIAL_NOW


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


def _readout(projection, subject: str):
    grant = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[subject],
            authorized_entity_ids=sorted(grant),
            max_hops=2,
        )
    )


def _packet(readout, signer, *, drivers, admitted_evidence=None):
    return build_packet(
        readout=readout,
        job=JobContext(
            job_id="job_drivers",
            question_family=QuestionFamilyID("project_status_drivers"),
            job_statement="Why is this subject not finished?",
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
            window_start=world.AS_OF_JUL_15,
            window_end=world.TRIAL_NOW,
        ),
        watermark=IndexWatermark(
            indexed_through=world.TRIAL_NOW,
            projected_at=world.TRIAL_NOW,
            records_indexed=1,
        ),
        signer=signer,
        trial=TrialContext(
            run_id="4f9a2c1e-2222-4333-8444-555566667777",
            corpus_version=adapter.CORPUS_VERSION,
        ),
        produced_at=_PROBE_NOW,
        drivers=drivers,
        admitted_evidence=admitted_evidence,
    )


def _honest_and_unrelated(helio):
    """A finding whose evidence a full admission run WOULD refuse, and a
    genuinely evidence-free sibling that must survive that refusal
    untouched -- proving unrelated drivers are not collateral damage.

    The sibling is synthesised from ``honest`` (a distinct driver_id,
    CANDIDATE_ONLY standing, no evidence and no path) rather than searched
    for in the corpus: what matters for this test is that it cites nothing
    admission could refuse, and CANDIDATE_ONLY sidesteps the unrelated
    principal/contributing-standing closure requirements
    (``validate_principal_standing_is_earned``) that a hand-built finding
    would otherwise have to satisfy for reasons this test is not about.
    """

    readout = _readout(helio, "proj_identity_rewrite")
    findings = discover_drivers(
        readout, "proj_identity_rewrite", as_of=world.TRIAL_NOW
    )[0]
    honest = next(
        item for item in findings if item.driver_id == "drv_block_wu_authcore_release"
    )
    assert honest.evidence_ids, "the finding carries no evidence; vacuous"
    unrelated = replace(
        honest,
        driver_id="drv_test_unrelated_evidence_free",
        standing=DriverStanding.CANDIDATE_ONLY,
        evidence_ids=(),
        conflicting_evidence_ids=(),
        path_ids=(),
        exclusion_reason=None,
    )
    return readout, honest, unrelated


class TestTheDefectIsReproduced:
    def test_admission_off_the_finding_indexes_cleanly(self, helio, signer) -> None:
        """The control: without admission, the finding's own evidence mints
        a handle exactly as ``discover_drivers`` built it."""

        readout, honest, _unrelated = _honest_and_unrelated(helio)
        packet = _packet(readout, signer, drivers=(honest,))
        cited = {
            candidate.driver_id: candidate.supporting_evidence_ids
            for candidate in packet.driver_analysis.candidates
        }
        assert cited[honest.driver_id]

    def test_a_canonically_refused_citation_no_longer_aborts_construction(
        self, helio, signer
    ) -> None:
        """The CHAOS-3650 repro: admission present but refuses everything.

        Before the fix this raised ``ValueError`` naming the refusal
        "discovery and emission disagree" -- an honest canonical refusal
        misclassified as an arm bug. After the fix the packet constructs.
        """

        readout, honest, _unrelated = _honest_and_unrelated(helio)
        packet = _packet(readout, signer, drivers=(honest,), admitted_evidence={})
        assert packet is not None


class TestTheDroppedDriverIsDisclosedNotFabricated:
    def test_the_unsupported_driver_does_not_reach_the_packet(
        self, helio, signer
    ) -> None:
        """Drop, not admit-and-pretend. The frozen contract's own rule --
        no asserted driver may cite evidence absent from the packet -- must
        hold even for the STANDING this finding actually carries."""

        readout, honest, _unrelated = _honest_and_unrelated(helio)
        packet = _packet(readout, signer, drivers=(honest,), admitted_evidence={})
        assert honest.driver_id not in {
            candidate.driver_id for candidate in packet.driver_analysis.candidates
        }

    def test_no_refused_record_is_ever_cited(self, helio, signer) -> None:
        """The invariant the ticket names explicitly: refused evidence must
        never be admitted through the back door."""

        readout, honest, _unrelated = _honest_and_unrelated(helio)
        packet = _packet(readout, signer, drivers=(honest,), admitted_evidence={})
        assert packet.evidence_coverage.evidence_index == ()

    def test_the_refusal_is_disclosed(self, helio, signer) -> None:
        readout, honest, _unrelated = _honest_and_unrelated(helio)
        packet = _packet(readout, signer, drivers=(honest,), admitted_evidence={})
        details = " ".join(
            limitation.detail for limitation in packet.evidence_coverage.limitations
        )
        assert "not admitted by the canonical evidence service" in details

    def test_an_unrelated_driver_survives_a_sibling_refusal(
        self, helio, signer
    ) -> None:
        """The other half of "does not abort unrelated evidence and
        answers": a driver that cites no refused evidence must reach the
        packet even when a sibling driver's evidence was refused."""

        readout, honest, unrelated = _honest_and_unrelated(helio)
        packet = _packet(
            readout, signer, drivers=(honest, unrelated), admitted_evidence={}
        )
        candidate_ids = {
            candidate.driver_id for candidate in packet.driver_analysis.candidates
        }
        assert unrelated.driver_id in candidate_ids
        assert honest.driver_id not in candidate_ids

    def test_a_conflicting_citation_of_refused_evidence_also_drops_cleanly(
        self, helio, signer
    ) -> None:
        """The same rule applies to conflicting_evidence_ids, not only
        supporting evidence -- a driver cannot cite refused evidence through
        either channel."""

        readout, honest, _unrelated = _honest_and_unrelated(helio)
        as_conflict = replace(
            honest,
            standing=DriverStanding.EXCLUDED,
            exclusion_reason=DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED,
            evidence_ids=(),
            conflicting_evidence_ids=honest.evidence_ids,
        )
        packet = _packet(readout, signer, drivers=(as_conflict,), admitted_evidence={})
        assert as_conflict.driver_id not in {
            candidate.driver_id for candidate in packet.driver_analysis.candidates
        }


class TestAuthorizationWithholdingIsUnchanged:
    """The pre-existing behaviour this fix must not touch: a driver resting
    on evidence outside the CALLER's grant is a different refusal
    (authorization, not canonical authority) and still raises."""

    def test_an_authorization_withheld_citation_still_raises(
        self, helio, signer
    ) -> None:
        from dataclasses import replace as dc_replace

        readout, honest, _unrelated = _honest_and_unrelated(helio)
        # An id genuinely unobserved (never in the readout at all) must
        # still be treated as the internal inconsistency it is -- this fix
        # is scoped to canonical refusal, not to relaxing this guard.
        tampered = dc_replace(
            honest, evidence_ids=(*honest.evidence_ids, "obs_never_indexed_at_all")
        )
        with pytest.raises(ValueError, match="never indexed"):
            _packet(readout, signer, drivers=(tampered,))
