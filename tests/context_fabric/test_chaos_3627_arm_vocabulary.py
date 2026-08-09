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
from unittest import mock

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
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
from dev_health_ops.context_fabric.graph_arm.projection import ProjectionError
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
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
        assert not cited - minted, (
            f"handles the world never minted: {sorted(cited - minted)}"
        )
        # Codex #5: set membership alone would accept a PERMUTATION of valid
        # handles among same-tenant observations. Exact issued-for pairing is
        # the property that matters, so it is what is asserted: each cited
        # handle must be the one the world issued for the record the entry
        # says it is about.
        for entry in packet.evidence_coverage.evidence_index:
            record = world.EVIDENCE_BY_HANDLE[entry.evidence.evidence_ref_id]
            assert record.handle == world.evidence_handle(record.slug)
            assert entry.evidence.entity_id == record.entity_id

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
            assert entry.evidence.entity_id not in observation_ids, (
                f"evidence entity is an observation: {entry.evidence.entity_id}"
            )
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

    def test_a_citing_observation_alone_still_names_the_records_own_entity(
        self, helio, signer
    ) -> None:
        """Input symmetry: seed the OTHER end of the same directed link.

        At one hop from ``proj_pulse`` the measurement is reached and the
        record it cites is not, because that record is about a service the
        traversal never got to. The handle is still the world's, and so is the
        entity the entry is about.

        **This test previously asserted the opposite**, pinning
        ``entity_id == "proj_pulse"`` -- the citing observation's subject --
        as correct. Both reviewers of PR #1617 measured that as a defect and
        the orchestrator overruled the pin: the arm's own sweep found 33 of 96
        packets carrying at least one entry whose ``entity_id`` contradicted
        the world record its handle named, and the corpus oracle is blind to
        it because it compares the handle's record against the grant and the
        packet's entity against the world, never the two against each other.
        What a citation is about does not change what the record is about.
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

        assert entry.evidence.entity_id == (
            world.EVIDENCE_BY_SLUG[_BACKING_RECORD].entity_id
        ), (
            "the packet contradicts the world record its handle names: says "
            f"{entry.evidence.entity_id}, record is about "
            f"{world.EVIDENCE_BY_SLUG[_BACKING_RECORD].entity_id}"
        )
        assert entry.evidence.entity_id == _BACKING_SUBJECT
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
        leaked = sorted(declared & {item.canonical_id for item in readout.observations})
        assert not leaked, f"observation ids declared authorized: {leaked}"


class TestProvenanceIsRefusedRatherThanRepaired:
    """Carrying a handle is only safe if a broken one cannot be carried.

    Each of these is a way the pair could arrive wrong, and each is refused
    at the door -- at ingestion, where the record that carried it is still in
    scope and the error can name it.
    """

    def _observation(self, **attributes) -> IngestionBatch:
        return IngestionBatch(
            org_id=world.ORG_HELIO,
            entities=(
                EntityRecord(
                    org_id=world.ORG_HELIO,
                    kind=GraphEntityKind.PROJECT,
                    canonical_id="proj_probe",
                    display_label="Probe",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=world.WINDOW_END,
                ),
            ),
            observations=(
                ObservationRecord(
                    org_id=world.ORG_HELIO,
                    kind=GraphObservationKind.REVIEW,
                    canonical_id="rev_probe",
                    title="probe review",
                    source_class=SourceClass.REVIEW,
                    observed_at=world.WINDOW_END,
                    subjects=(
                        CanonicalRef(
                            kind=GraphEntityKind.PROJECT, canonical_id="proj_probe"
                        ),
                    ),
                    attributes=attributes,
                ),
            ),
        )

    def test_a_handle_with_no_record_id_is_refused(self) -> None:
        batch = self._observation(
            source_evidence_handle=world.evidence_handle("rev_probe"),
            source_evidence_entity_id="proj_probe",
        )
        with pytest.raises(ProjectionError, match="one half of the source evidence"):
            build_projection(batch)

    def test_a_record_id_with_no_handle_is_refused(self) -> None:
        """The half that silently restores re-minting.

        An id with no handle leaves the builder minting its own while the
        record looks, to a reader, like it carried provenance.
        """

        batch = self._observation(source_evidence_id="rev_probe")
        with pytest.raises(ProjectionError, match="one half of the source evidence"):
            build_projection(batch)

    def test_a_handle_outside_the_contracts_grammar_is_refused(self) -> None:
        """Refused, not repaired: a handle is an identity.

        Trimming or re-deriving a malformed handle would attribute the record
        to whatever the repaired string happened to name.
        """

        batch = self._observation(
            source_evidence_handle="EV1_NOT-THE-GRAMMAR",
            source_evidence_id="rev_probe",
            source_evidence_entity_id="proj_probe",
        )
        with pytest.raises(ProjectionError, match="EvidenceHandle grammar"):
            build_projection(batch)

    def test_two_records_under_one_handle_are_refused(self, helio, signer) -> None:
        """One handle names one record.

        This is reachable through the arm's OWN mint, not only through a
        malformed source: ``EvidenceReferenceSigner._payload`` identifies a
        record by ``(org, source_system, source_version, entity_type,
        entity_id, repositories)``, and ``entity_id`` is now -- correctly --
        the entity the evidence is about, so two records of the same kind
        about one entity mint the same handle. Presenting them as one piece
        of evidence would lose one of them silently; the arm refuses instead.
        """

        readout = _read(helio, world.TEAM_CINDER)
        first, second = readout.observations[0], readout.observations[1]
        collided = dataclasses.replace(
            second,
            attributes={
                **second.attributes,
                "source_evidence_handle": first.attributes["source_evidence_handle"],
                "source_evidence_id": second.canonical_id,
            },
            subject_canonical_ids=first.subject_canonical_ids,
        )
        tampered = dataclasses.replace(
            readout, observations=(first, collided, *readout.observations[2:])
        )
        with pytest.raises(ValueError, match="already issued for"):
            _packet(tampered, signer)

    def test_a_measurement_naming_evidence_the_world_never_minted_is_refused(
        self,
    ) -> None:
        """The adapter refuses at ingestion rather than at citation.

        A canonical number whose named record does not exist has no citable
        source, and minting a handle for it here would be the arm asserting a
        provenance the corpus never issued.
        """

        measurement = dataclasses.replace(
            world.WORLD_MEASUREMENTS[0], evidence_slug="ev_the_world_never_minted"
        )
        with (
            mock.patch.object(world, "WORLD_MEASUREMENTS", (measurement,)),
            pytest.raises(ValueError, match="which the world never minted"),
        ):
            adapter.corpus_batch(world.ORG_HELIO)


class TestTheFallbackMintDiscriminatesRecords:
    """A handle-less source must not brick packet production.

    CHAOS-3627 fix round, codex #3 / verifier F4. The platform mint identifies
    a record by ``(org, source_system, source_version, entity_type, entity_id,
    repositories)`` and ``entity_id`` is the ENTITY, so two same-kind records
    about one entity minted the same handle — the contract refuses a repeated
    index handle, and the arm's refusal then killed the whole packet. On a
    legitimate world. The arm's own fixtures hold exactly such a pair on
    purpose: ``dec_auth_1`` superseded by ``dec_auth_2``, both decisions about
    ``proj_nightfall_migration``.

    The previous collision test never reached this branch — it copied a
    carried handle onto a second observation, which exercises duplicate SOURCE
    provenance, not the fallback mint. Both are tested now, and they must stay
    distinct: one is a refusal that is correct, the other was a defect.
    """

    def _handle_less(self, batch):
        return dataclasses.replace(
            batch,
            observations=tuple(
                dataclasses.replace(
                    observation,
                    attributes={
                        key: value
                        for key, value in observation.attributes.items()
                        if not key.startswith("source_evidence_")
                    },
                )
                for observation in batch.observations
            ),
        )

    def test_a_handle_less_world_with_two_same_kind_records_still_builds(
        self, signer
    ) -> None:
        from dev_health_ops.context_fabric.graph_arm import fixtures

        batch = self._handle_less(fixtures.alpha_batch())
        colliding = [
            observation
            for observation in batch.observations
            if observation.kind is GraphObservationKind.DECISION
        ]
        assert len(colliding) >= 2, "no same-kind pair to collide; vacuous"
        assert (
            len({observation.subjects[0].canonical_id for observation in colliding})
            == 1
        ), "the pair is no longer about one entity; vacuous"

        projection = build_projection(batch)
        readout = asyncio.run(
            ProjectionGraphReader(projection).neighbourhood(
                org_id=fixtures.ALPHA_ORG,
                seed_canonical_ids=["proj_nightfall_migration"],
                authorized_entity_ids=fixtures.alpha_authorized_ids(),
                max_hops=3,
            )
        )

        packet = build_packet(
            readout=readout,
            job=JobContext(
                job_id="job_chaos_3627_fallback",
                question_family=QuestionFamilyID.PROJECT_STATUS_DRIVERS,
                job_statement="What is the current state of this subject?",
                comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
                window_start=fixtures.WINDOW_START,
                window_end=fixtures.WINDOW_END,
            ),
            watermark=IndexWatermark(
                indexed_through=fixtures.WINDOW_END,
                projected_at=fixtures.WINDOW_END,
                records_indexed=len(readout.entities),
            ),
            signer=signer,
            trial=TrialContext(run_id=_RUN_ID),
            produced_at=_PRODUCED_AT,
        )

        indexed = {
            entry.evidence.evidence_ref_id
            for entry in packet.evidence_coverage.evidence_index
        }
        assert len(indexed) == len(packet.evidence_coverage.evidence_index), (
            "the fallback mint collided again"
        )
        assert len(indexed) >= len(colliding), (
            "the colliding records did not both reach the index"
        )


class TestTheMergeBoundIsPinned:
    """The measured bound on the merge, in place of the denial.

    PR #1617 review: I claimed the shared-handle merge was "not a weakening".
    That was asserted without measurement and is withdrawn. The verifier
    measured what it actually admits — 9 of 19 handles merge, 3 gain a subject
    beyond the record's own — and every one of those arrives through a link
    the WORLD asserts: a measurement naming the record that evidences it.

    So the bound is: an entry's ``supports_entity_ids`` may exceed the
    record's own subjects only by the subjects of observations that name THIS
    record as their source. Nothing else may join, and this pins it
    structurally so any future widening goes red rather than being argued
    about again.
    """

    def test_every_extra_subject_arrives_through_a_declared_source_link(
        self, helio, signer
    ) -> None:
        readout = _read(helio, world.TEAM_CINDER)
        packet = _packet(readout, signer)

        by_source: dict[str, set[str]] = {}
        own: dict[str, set[str]] = {}
        for observation in readout.observations:
            source_id = observation.attributes.get("source_evidence_id")
            if source_id is None:
                continue
            target = own if source_id == observation.canonical_id else by_source
            target.setdefault(source_id, set()).update(
                observation.subject_canonical_ids
            )

        merged = 0
        for entry in packet.evidence_coverage.evidence_index:
            record = world.EVIDENCE_BY_HANDLE.get(entry.evidence.evidence_ref_id)
            if record is None:
                continue
            permitted = own.get(record.slug, set()) | by_source.get(record.slug, set())
            extra = set(entry.supports_entity_ids) - {record.entity_id}
            if extra:
                merged += 1
            assert not extra - permitted, (
                f"{record.slug} gained {sorted(extra - permitted)}, which no "
                "observation declared this record as its source for"
            )

        assert merged, "no entry merged at all; this bound would be vacuous"

    def test_the_entry_still_describes_the_record_not_the_citation(
        self, helio, signer
    ) -> None:
        """What makes the merge visible rather than silent.

        A merged entry names the record's own entity, so a reader can see that
        the extra subjects are additions to it rather than a redefinition of
        what the evidence is.
        """

        packet = _packet(_read(helio, world.TEAM_CINDER), signer)

        for entry in packet.evidence_coverage.evidence_index:
            record = world.EVIDENCE_BY_HANDLE.get(entry.evidence.evidence_ref_id)
            if record is not None:
                assert entry.evidence.entity_id == record.entity_id


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
        # Matched on the PATH-shaped message, not the shared closing clause.
        # Verifier F7: the evidence twin raises the same exception type ending
        # in the same words, so the loose match was satisfied by either guard
        # -- the identical hazard that made this test's sibling in
        # test_chaos_3617_authorization.py report a SURVIVED mutation.
        with pytest.raises(PermissionError, match=r"path p\d+ traverses"):
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
