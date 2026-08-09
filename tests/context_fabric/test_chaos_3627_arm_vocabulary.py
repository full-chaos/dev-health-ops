"""CHAOS-3627: the graph arm speaks the frozen corpus's vocabulary.

The CHAOS-3616 corpus is the spec and it is frozen. Its authorization oracle
(``investigation_corpus.authorization.audit_authorization``) reads three
fields of an emitted packet as corpus vocabulary:

* ``related_context.authorized_entity_ids`` -- **entity** ids, compared
  against the principal's true grant (``authorization.py:254-255``);
* ``evidence_coverage.evidence_index[].evidence.entity_id`` -- the **entity
  the evidence is about**, read as an entity sighting
  (``authorization.py:190``) and checked against the world's own
  ``WorldEvidence.entity_id`` semantics (``authorization.py:276-277``);
* every cited ``evidence_ref_id`` -- a handle the **world minted**
  (``world.evidence_handle``), looked up in ``EVIDENCE_BY_HANDLE``.

The arm used to disagree on all three: it declared entity ids *plus*
observation ids plus measurement keys, it put the observation's own slug in
``evidence.entity_id``, and it re-minted every handle through its own signer.
The arm's *filtering* was already correct -- ``proj_quarry`` never reached a
packet -- but the audit reported 31 false authorization claims, 31 fabricated
entities and 31 fabricated handles on the analyst/``team_cinder`` path, which
buries any real leak, and the withdrawn-evidence check
(``authorization.py:278-279``) was unreachable dead code on arm packets
because the handle lookup at ``:272`` missed first and ``continue``d.

These tests use the oracle as the instrument. Two of them are the controls
that keep the first one from being vacuous: a planted ``proj_quarry`` leak
must flip the audit red, and a cited *revoked* world record must show up in
``withdrawn_evidence_handles`` -- a field that could not be non-empty on an
arm packet before this change.
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
_RUN_ID = "3c627a11-2222-4333-8444-555566667777"

#: The measurement whose backing world record is about a DIFFERENT entity
#: than the measurement itself: ``pulse_deployments`` is about ``proj_pulse``
#: and is evidenced by ``dp_pulse_prod``, which is about ``svc_pulse_api``.
#: That split is what makes "the entity the evidence is about" a real choice
#: rather than a restatement of the observation's subject.
_MEASUREMENT = "pulse_deployments"
_BACKING_RECORD = "dp_pulse_prod"
_BACKING_SUBJECT = "svc_pulse_api"


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


def _read(projection, seed: str, *, authorized=None, max_hops: int = 2):
    grant = authorized
    if grant is None:
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
            job_id="job_chaos_3627",
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


def _entry_by_handle(packet, handle: str):
    return next(
        entry
        for entry in packet.evidence_coverage.evidence_index
        if entry.evidence.evidence_ref_id == handle
    )


class TestTheAuthorizationOracleCanScoreAnArmPacket:
    """The acceptance instrument, and the two controls that arm it."""

    def test_the_analyst_packet_is_authorization_clean(self, helio, signer) -> None:
        """The CHAOS-3620 reproduction, at 1ab76d955: 31 / 31 / 31.

        The counts are asserted individually rather than only through
        ``is_clean`` so a regression names which vocabulary drifted back.
        """

        packet = _packet(_read(helio, world.TEAM_CINDER), signer)
        audit = audit_authorization(
            packet, world.PRINCIPAL_ANALYST, case_id="chaos_3627_repro"
        )

        assert audit.false_authorization_claims == ()
        assert audit.fabricated_entities == ()
        assert audit.fabricated_evidence_handles == ()
        assert audit.unauthorized_disclosures == ()
        assert audit.unauthorized_evidence_handles == ()
        assert audit.withdrawn_evidence_handles == ()
        assert audit.is_clean, audit.summary()

    def test_a_tenant_derived_grant_leaks_the_restricted_project(
        self, helio, signer
    ) -> None:
        """The negative control: the oracle is load-bearing, not vacuous.

        ``proj_quarry`` sits inside the analyst's OWN tenant, so this is the
        leak no tenant comparison catches. Authorizing by tenant -- the
        mistake the corpus plants ``proj_quarry`` to detect -- is reproduced
        by handing the traversal the tenant's entities instead of the
        principal's grant.
        """

        leaked = _read(
            helio,
            world.TEAM_CINDER,
            authorized=list(adapter.seed_ids_for_tenant(world.ORG_HELIO)),
        )
        assert world.PROJ_QUARRY in {
            entity.canonical_id for entity in leaked.entities
        }, "the leak was not planted; this control would pass vacuously"

        audit = audit_authorization(
            _packet(leaked, signer), world.PRINCIPAL_ANALYST, case_id="chaos_3627_leak"
        )

        assert not audit.is_clean
        assert world.PROJ_QUARRY in audit.false_authorization_claims
        assert world.PROJ_QUARRY in {
            sighting.entity_id for sighting in audit.unauthorized_disclosures
        }

    def test_a_cited_revoked_world_record_is_reported_as_withdrawn(
        self, helio, signer
    ) -> None:
        """The check at ``authorization.py:278-279`` comes alive.

        It could not fire on an arm packet before: every cited handle missed
        the ``EVIDENCE_BY_HANDLE`` lookup at ``:272`` and ``continue``d, so
        the state test below it was unreachable. ``rv_vertex_revoked`` is a
        REVOKED record the corpus plants on ``pr_vertex_401``; the arm
        ingests it (the adapter deliberately does not filter withdrawn
        material at the door) and cites it, so the oracle must now say so.
        """

        packet = _packet(_read(helio, "proj_vertex"), signer)
        revoked = world.EVIDENCE_BY_SLUG["rv_vertex_revoked"]
        assert revoked.state is world.EvidenceState.REVOKED

        audit = audit_authorization(
            packet, world.PRINCIPAL_ANALYST, case_id="chaos_3627_withdrawn"
        )

        assert revoked.handle in audit.withdrawn_evidence_handles
        assert not audit.is_clean


class TestEvidenceCitesTheHandleItWasIssued:
    """Provenance: cite the handle you were issued, never a re-mint.

    Re-minting breaks evidence identity. The corpus says so in
    ``world.evidence_handle``'s docstring -- the corpus cannot key the
    platform's HMAC, so a handle the arm re-signs is a handle no oracle,
    and no dereference against the issuing source, can match.
    """

    def test_every_corpus_originated_entry_carries_the_worlds_handle(
        self, helio, signer
    ) -> None:
        packet = _packet(_read(helio, world.TEAM_CINDER), signer)
        minted = {evidence.handle for evidence in world.EVIDENCE_BY_SLUG.values()}

        cited = {
            entry.evidence.evidence_ref_id
            for entry in packet.evidence_coverage.evidence_index
        }
        assert cited, "no evidence was indexed; this would pass vacuously"
        assert cited <= minted

    def test_no_entry_names_an_observation_as_the_entity_it_is_about(
        self, helio, signer
    ) -> None:
        """``evidence.entity_id`` is entity vocabulary.

        The oracle reads it as an entity sighting, and the frozen contract's
        ``validate_every_entity_is_authorized`` checks it against the
        declared entity set. An observation slug in that field is what
        forced the arm to widen the declared set in the first place.
        """

        readout = _read(helio, world.TEAM_CINDER)
        observation_ids = {item.canonical_id for item in readout.observations}
        entity_ids = set(world.ENTITIES_BY_ID)
        packet = _packet(readout, signer)

        assert packet.evidence_coverage.evidence_index
        for entry in packet.evidence_coverage.evidence_index:
            assert entry.evidence.entity_id not in observation_ids
            assert entry.evidence.entity_id in entity_ids

    def test_a_reached_source_record_names_the_entity_the_record_is_about(
        self, helio, signer
    ) -> None:
        """The world's own semantics decide, not the citing observation.

        ``dp_pulse_prod`` is a deployment record about ``svc_pulse_api``. A
        measurement about ``proj_pulse`` cites it. When the traversal reaches
        both, the entry is about the service -- which is what
        ``authorization.py:276-277`` compares against the entity grant --
        while ``supports_entity_ids`` keeps the full set.
        """

        packet = _packet(_read(helio, world.TEAM_CINDER), signer)
        handle = world.EVIDENCE_BY_SLUG[_BACKING_RECORD].handle
        entry = _entry_by_handle(packet, handle)

        assert entry.evidence.entity_id == _BACKING_SUBJECT
        assert set(entry.supports_entity_ids) >= {_BACKING_SUBJECT, "proj_pulse"}

    def test_the_record_and_the_measurement_that_cites_it_are_one_entry(
        self, helio, signer
    ) -> None:
        """One source record is one piece of evidence, however many
        observations project it. The frozen contract refuses a repeated
        handle in the index (``packet.py:1241-1243``), so carrying the
        source's identity makes merging the only coherent option -- and the
        merge is the honest statement, not a workaround.
        """

        readout = _read(helio, world.TEAM_CINDER)
        assert {_MEASUREMENT, _BACKING_RECORD} <= {
            item.canonical_id for item in readout.observations
        }, "both ends were not reached; this would pass vacuously"

        packet = _packet(readout, signer)
        handle = world.EVIDENCE_BY_SLUG[_BACKING_RECORD].handle
        matching = [
            entry
            for entry in packet.evidence_coverage.evidence_index
            if entry.evidence.evidence_ref_id == handle
        ]
        assert len(matching) == 1

    def test_a_citing_observation_alone_still_carries_the_source_handle(
        self, helio, signer
    ) -> None:
        """Input symmetry: seed the OTHER end of the same directed link.

        At one hop from ``proj_pulse`` the measurement is reached and the
        record it cites is not, because that record is about a service the
        traversal never got to. The handle is still the world's -- the source
        issued it, and the arm carries what it was issued -- while the entity
        the entry is about falls back to the subject the citing observation
        does name.
        """

        readout = _read(helio, "proj_pulse", max_hops=1)
        reached = {item.canonical_id for item in readout.observations}
        assert _MEASUREMENT in reached
        assert _BACKING_RECORD not in reached, (
            "the backing record was reached after all; this asymmetric case "
            "is no longer exercised and the test would pass vacuously"
        )

        packet = _packet(readout, signer)
        handle = world.EVIDENCE_BY_SLUG[_BACKING_RECORD].handle
        entry = _entry_by_handle(packet, handle)

        assert entry.evidence.entity_id == "proj_pulse"
        assert audit_authorization(
            packet, world.PRINCIPAL_ANALYST, case_id="chaos_3627_symmetry"
        ).is_clean


class TestTheDeclaredSetIsEntityVocabulary:
    def test_declared_authorized_ids_are_exactly_the_traversals_entity_grant(
        self, helio, signer
    ) -> None:
        readout = _read(helio, world.TEAM_CINDER)
        packet = _packet(readout, signer)

        assert packet.related_context.authorized_entity_ids == tuple(
            sorted(readout.authorized_entity_ids)
        )

    def test_no_observation_id_is_declared_authorized(self, helio, signer) -> None:
        """The widening the oracle counted as 31 false authorization claims.

        Declaring an observation id 'authorized' is not a harmless superset:
        the packet-level validator uses the same field, so every id added to
        it is one more thing the contract's own leak check stops catching.
        """

        readout = _read(helio, world.TEAM_CINDER)
        packet = _packet(readout, signer)
        declared = set(packet.related_context.authorized_entity_ids)

        assert declared
        assert not declared & {item.canonical_id for item in readout.observations}


class TestTheInternalInvariantsSurviveTheNarrowing:
    """The narrowed field must not cost the arm a guard.

    Both of these were enforced *through* the widened declared set before.
    They are checked in the builder now, so they survive the field getting
    narrower -- and each is observed refusing, not merely present.
    """

    def test_a_hop_endpoint_outside_the_grant_is_still_refused(
        self, helio, signer
    ) -> None:
        readout = _read(helio, world.TEAM_CINDER)
        tampered = dataclasses.replace(
            readout, authorized_entity_ids=(world.TEAM_CINDER,)
        )
        with pytest.raises(PermissionError, match="not in the authorized entity set"):
            _packet(tampered, signer)

    def test_an_observation_about_an_unauthorized_entity_is_refused(
        self, helio, signer
    ) -> None:
        """Refuse, do not silently narrow.

        The builder used to intersect an observation's subjects with the
        entities it had already decided to return, so a subject outside the
        grant simply vanished from the evidence entry. A reader bug -- or a
        second reader -- could therefore hand the builder unauthorized
        material and get a packet that looked clean. This is the evidence
        twin of the hop-endpoint check above.
        """

        readout = _read(helio, world.TEAM_CINDER)
        leaked = dataclasses.replace(
            readout.observations[0],
            subject_canonical_ids=(
                *readout.observations[0].subject_canonical_ids,
                world.PROJ_QUARRY,
            ),
        )
        tampered = dataclasses.replace(
            readout,
            observations=(leaked, *readout.observations[1:]),
        )
        with pytest.raises(PermissionError, match=world.PROJ_QUARRY):
            _packet(tampered, signer)
