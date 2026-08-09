"""CHAOS-3628: withdrawn evidence never reaches an emitted packet.

The arm had **no evidence-state concept at all**. ``corpus_batch`` filtered on
tenant and known-entity only (``corpus_adapter.py:199-203``) and demoted the
state to a display string (``:218``); ``grep -rn "EvidenceState|revoked|
redacted|withdraw" src/dev_health_ops/context_fabric/`` returned nothing. So
REVOKED and DELETED records reached emitted evidence indexes on five corpus
seeds, and REDACTED stayed out only because the *authorization* filter removed
``proj_quarry`` — redaction itself did no work at all.

**Where the exclusion happens, and why there.** Three places were possible:

* *at ingestion* — rejected. The adapter's own docstring already argues this
  for false-claim edges and adversarial evidence: material dropped at the door
  makes every exclusion expectation pass without the arm doing anything. An
  arm that never held a revoked record cannot be observed declining to cite
  one, and the corpus plants these records precisely to be declined.
* *at emission* — rejected. Withdrawn evidence would still reach driver
  discovery and cohort construction, so a driver could rest on a revoked
  record and merely not show it. Support that exists and is invisible is
  worse than support that does not exist.
* *at read time* — chosen. It is where the authorization filter already draws
  the same line, it is the first point at which the **arm** rather than the
  adapter makes the decision, and everything downstream — drivers, cohort,
  evidence index — is then working from material the arm may actually
  present.

The counts are kept apart. ``authorization_filtered_count`` answers "what did
the caller's grant remove"; ``withdrawn_evidence_filtered_count`` answers
"what did the source withdraw". One reason per flag is a rule this module
learned the expensive way (``InvestigationReadout.evidence_truncated``).

**REDACTED needs the compliance principal.** The corpus's only redacted
record, ``wi_quarry_redacted``, is about ``proj_quarry`` — the entity the
analyst cannot see. Asserting its absence under the analyst would pass with
the state filter deleted, because authorization removes it first. The
compliance principal *can* see ``proj_quarry``, so the state filter is the
only thing that can be doing the work. The corpus is frozen and no record was
added to make this test possible.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.authorization import (
    audit_authorization,
)
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
_RUN_ID = "3c628a22-3333-4444-8555-666677778888"

#: One seed per withdrawn state, with the principal that can actually reach
#: it. Every one of these records is present in the world and reachable by
#: the traversal, which is what makes "absent from the packet" a statement
#: about the arm rather than about the corpus.
_WITHDRAWN_CASES = (
    pytest.param(
        "proj_vertex",
        world.PRINCIPAL_ANALYST,
        "rv_vertex_revoked",
        world.EvidenceState.REVOKED,
        id="revoked",
    ),
    pytest.param(
        "proj_beacon",
        world.PRINCIPAL_ANALYST,
        "wi_beacon_deleted",
        world.EvidenceState.DELETED,
        id="deleted",
    ),
    pytest.param(
        world.PROJ_QUARRY,
        world.PRINCIPAL_COMPLIANCE,
        "wi_quarry_redacted",
        world.EvidenceState.REDACTED,
        id="redacted-under-a-principal-who-can-see-the-subject",
    ),
)


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


def _read(projection, seed: str, principal: str, *, max_hops: int = 2):
    grant = sorted(adapter.authorized_entity_ids_for(principal))
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
            job_id="job_chaos_3628",
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


def _cited_handles(packet) -> set[str]:
    return {
        entry.evidence.evidence_ref_id
        for entry in packet.evidence_coverage.evidence_index
    }


class TestWithdrawnEvidenceIsPresentInTheWorldAndAbsentFromThePacket:
    """Both halves asserted, per state. Absence alone proves nothing.

    A test that only checked "the handle is not in the packet" would pass
    against a world that never held the record, against a traversal that never
    reached it, and against an arm that dropped it at ingestion. Each case
    below first proves the record IS in the world and DID reach the traversal.
    """

    @pytest.mark.parametrize(("seed", "principal", "slug", "state"), _WITHDRAWN_CASES)
    def test_the_record_is_reachable_but_never_cited(
        self, helio, signer, seed: str, principal: str, slug: str, state
    ) -> None:
        record = world.EVIDENCE_BY_SLUG[slug]
        assert record.state is state, "the corpus no longer plants this state"

        readout = _read(helio, seed, principal)
        assert (
            slug in {observation.canonical_id for observation in readout.observations}
            or readout.withdrawn_evidence_filtered_count
        ), (
            f"{slug} neither reached the traversal nor was counted as "
            "withdrawn; this case would pass vacuously"
        )

        packet = _packet(readout, signer)
        assert record.handle not in _cited_handles(packet)

    @pytest.mark.parametrize(("seed", "principal", "slug", "state"), _WITHDRAWN_CASES)
    def test_the_oracle_agrees_nothing_withdrawn_was_cited(
        self, helio, signer, seed: str, principal: str, slug: str, state
    ) -> None:
        """The independent check, not the arm's own bookkeeping."""

        packet = _packet(_read(helio, seed, principal), signer)
        audit = audit_authorization(packet, principal, case_id="chaos_3628")

        # ``summary()`` as the message on purpose: it renders the oracle's own
        # ``handles-withdrawn=[...]`` clause, so a failure here evidences the
        # withdrawal rather than printing an empty-vs-non-empty tuple that
        # says nothing about which check fired.
        assert audit.withdrawn_evidence_handles == (), audit.summary()

    def test_the_withdrawn_count_moves_independently_of_the_grant(
        self, helio, signer
    ) -> None:
        """Not folded into ``authorization_filtered_count``.

        A shared counter would say "the caller's grant removed this" about
        material the caller's grant had nothing to do with, and a reader
        checking why their answer is thin would be told the wrong thing.

        Asserting the two numbers merely *differ* would be satisfied by an
        accident of arithmetic — on ``proj_beacon`` under the analyst they are
        both 1. Each is therefore moved on its own: a principal who can see
        everything still has withdrawn evidence removed (grant 0, withdrawn
        non-zero), and a restricted principal still has the grant remove
        something (grant non-zero) — both ends of the pair, seeded.
        """

        unrestricted = _read(helio, "proj_beacon", world.PRINCIPAL_COMPLIANCE)
        assert unrestricted.authorization_filtered_count == 0
        assert unrestricted.withdrawn_evidence_filtered_count > 0

        restricted = _read(helio, world.TEAM_CINDER, world.PRINCIPAL_ANALYST)
        assert restricted.authorization_filtered_count > 0
        assert restricted.withdrawn_evidence_filtered_count == 0


def _probe_batch(**attributes):
    """CHAOS-3627's fix round added a third member to the source-evidence
    triple (the entity the RECORD is about), so a handle-bearing probe must
    supply it or the pairing guard raises before the state guard is reached.
    Defaulted here rather than at each call site: these probes exist to
    exercise ONE guard each, and a probe that trips a sibling proves nothing
    about the guard it names.
    """

    if "source_evidence_handle" in attributes:
        attributes.setdefault("source_evidence_entity_id", "proj_state_probe")
    """One entity and one observation about it, with the given attributes.

    Built here rather than borrowed from the corpus so a state probe cannot
    accidentally be answered by some other property of a real record.
    """

    from dev_health_ops.api.dev.contracts_v2.base import SourceClass
    from dev_health_ops.context_fabric.graph_arm.records import (
        CanonicalRef,
        EntityRecord,
        IngestionBatch,
        ObservationRecord,
    )
    from dev_health_ops.context_fabric.graph_arm.vocabulary import (
        GraphEntityKind,
        GraphObservationKind,
    )

    return IngestionBatch(
        org_id=world.ORG_HELIO,
        entities=(
            EntityRecord(
                org_id=world.ORG_HELIO,
                kind=GraphEntityKind.PROJECT,
                canonical_id="proj_state_probe",
                display_label="State probe",
                source_class=SourceClass.WORK_GRAPH,
                observed_at=world.WINDOW_END,
            ),
        ),
        observations=(
            ObservationRecord(
                org_id=world.ORG_HELIO,
                kind=GraphObservationKind.REVIEW,
                canonical_id="rev_state_probe",
                title="probe review",
                source_class=SourceClass.REVIEW,
                observed_at=world.WINDOW_END,
                subjects=(
                    CanonicalRef(
                        kind=GraphEntityKind.PROJECT,
                        canonical_id="proj_state_probe",
                    ),
                ),
                attributes=attributes,
            ),
        ),
    )


class TestAnUnreadableStateIsRefused:
    """A state the arm cannot read must not become a citable record.

    The only available default is "citable", so an unrecognised token would
    make a record withdrawn under a newer vocabulary indistinguishable from a
    live one. Refused at ingestion, where the record is still in scope.
    """

    def test_a_state_outside_the_arms_vocabulary_is_refused(self) -> None:
        from dev_health_ops.context_fabric.graph_arm.projection import ProjectionError

        batch = _probe_batch(
            source_evidence_handle=world.evidence_handle("rev_state_probe"),
            source_evidence_id="rev_state_probe",
            source_evidence_state="quarantined",
        )
        with pytest.raises(ProjectionError, match="source evidence state"):
            build_projection(batch)

    def test_the_states_the_corpus_uses_are_all_accepted(self) -> None:
        """The other half: the refusal is narrow, not a blanket rejection.

        Without this, deleting the whole vocabulary check would still look
        correct — everything would be refused and the test above would pass.
        """

        for state in world.EvidenceState:
            batch = _probe_batch(
                source_evidence_handle=world.evidence_handle("rev_state_probe"),
                source_evidence_id="rev_state_probe",
                source_evidence_state=str(state),
            )
            assert build_projection(batch).observation_nodes()


class TestAnIssuedHandleMustDeclareItsState:
    """What makes readback's absent-state default safe.

    ``_is_citable`` treats an absent state as citable, which is correct for a
    source with no withdrawal concept and catastrophic for one whose state was
    dropped in transit. The two are only distinguishable because a
    handle-issuing source is required to declare state here.
    """

    def test_a_handle_without_a_state_is_refused(self) -> None:
        from dev_health_ops.context_fabric.graph_arm.projection import ProjectionError

        batch = _probe_batch(
            source_evidence_handle=world.evidence_handle("rev_state_probe"),
            source_evidence_id="rev_state_probe",
        )
        with pytest.raises(ProjectionError, match="no source_evidence_state"):
            build_projection(batch)

    def test_a_source_that_issues_no_handle_needs_no_state(self) -> None:
        """The legitimate absent-state case, kept working.

        A source with no withdrawal concept — the arm's own fixture world
        before CHAOS-3627, and any provider that issues no handles — must
        still be ingestible, or the requirement above would be a ban on
        sources rather than a guard on provenance.
        """

        assert build_projection(_probe_batch()).observation_nodes()


class TestTheOracleBackstopIsAlive:
    """Prove the *check* works, without shipping behaviour that trips it.

    The exclusion above is the arm's guarantee. This is the independent
    backstop behind it: if a future emitter loses the filter, the CHAOS-3616
    oracle must say so. Proven with a deliberately defective emitter —
    withdrawn observations put back into the readout by hand — rather than by
    letting the shipped path cite a revoked record, which is the behaviour the
    whole ticket exists to remove.
    """

    def test_an_emitter_that_keeps_withdrawn_evidence_is_caught(
        self, helio, signer
    ) -> None:
        clean = _read(helio, "proj_vertex", world.PRINCIPAL_ANALYST)
        assert (
            audit_authorization(
                _packet(clean, signer), world.PRINCIPAL_ANALYST
            ).withdrawn_evidence_handles
            == ()
        ), "the shipped path already cites withdrawn evidence; nothing to prove"

        defective = dataclasses.replace(
            clean, observations=_reinstate_withdrawn(helio, clean)
        )
        audit = audit_authorization(_packet(defective, signer), world.PRINCIPAL_ANALYST)

        assert world.EVIDENCE_BY_SLUG["rv_vertex_revoked"].handle in (
            audit.withdrawn_evidence_handles
        )
        assert not audit.is_clean


def _reinstate_withdrawn(projection, readout):
    """The withdrawn observations the filter removed, put back verbatim.

    Reads them off the projection rather than reconstructing them, so the
    defective emitter is emitting exactly what an unfiltered reader would
    have handed it.
    """

    from dev_health_ops.context_fabric.graph_arm.readback import (
        _adjacency_from_projection,
    )

    reached = {entity.canonical_id for entity in readout.entities}
    kept = list(readout.observations)
    seen = {observation.canonical_id for observation in kept}
    for observation in _adjacency_from_projection(projection).observations:
        if observation.canonical_id in seen:
            continue
        subjects = tuple(
            subject
            for subject in observation.subject_canonical_ids
            if subject in reached
        )
        if not subjects:
            continue
        kept.append(dataclasses.replace(observation, subject_canonical_ids=subjects))
    return tuple(sorted(kept, key=lambda item: item.canonical_id))
