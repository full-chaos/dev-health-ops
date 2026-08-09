"""CHAOS-3617: contract and golden parity against the frozen CHAOS-3615 packet.

Everything here goes through the **canonical Pydantic validator**
(``INVESTIGATION_CONTRACT_MODELS``), never the JSON Schema alone: the
contract manifest declares ``schema_only_validation_is_sufficient: false``,
and a schema-valid packet has had none of its cross-field rules checked.

The parity claim being made is narrow and worth stating exactly. It is *not*
"the arm produces the golden fixture" — the golden is a different
investigation over a different world, and reproducing it would prove
nothing. It is "the arm's output is accepted by the same validator, uses the
same section shapes and the same closed vocabularies as the frozen goldens,
and is refused when it violates the same rules".
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
    TRIAL_SOURCE_ALLOWLIST,
    ComparisonShape,
    InvestigationOutcome,
    QuestionFamilyID,
)
from dev_health_ops.context_fabric.graph_arm import fixtures
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    ARM_ID,
    PRODUCER_ID,
    RANKING_VERSION,
    JobContext,
    TrialContext,
    UnsupportedComparisonShapeError,
    build_packet,
    signer_from_environment,
)
from dev_health_ops.context_fabric.graph_arm.projection import PROJECTION_VERSION
from dev_health_ops.context_fabric.graph_arm.readback import (
    QUERY_VERSION,
    ProjectionGraphReader,
)
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

_PRODUCED_AT = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
_RUN_ID = "4f9a2c1e-1111-4222-8333-444455556666"

#: Every graph-backend word that must not appear anywhere on the wire outside
#: ``versions.trial``. Includes the backends the arm does *not* use, because
#: the neutrality claim is about the contract, not about today's choice.
_BACKEND_VOCABULARY = (
    "graphiti",
    "falkor",
    "neo4j",
    "kuzu",
    "neptune",
    "cypher",
    "group_id",
    "episodic",
    "name_embedding",
    "fact_embedding",
)


def _readout(projection, seeds=("proj_nightfall_migration",)):
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=projection.org_id,
            seed_canonical_ids=list(seeds),
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
    )


def _packet(readout, signer, **overrides):
    job = JobContext(
        job_id=overrides.pop("job_id", "job_status"),
        question_family=overrides.pop(
            "question_family", QuestionFamilyID("project_status_drivers")
        ),
        job_statement="Status of the Nightfall Migration project.",
        comparison_shape=overrides.pop(
            "comparison_shape", ComparisonShape.SINGULAR_SUBJECT
        ),
        window_start=fixtures.WINDOW_START,
        window_end=fixtures.WINDOW_END,
    )
    watermark = overrides.pop(
        "watermark",
        IndexWatermark(
            indexed_through=fixtures.WINDOW_END,
            projected_at=fixtures.WINDOW_END,
            records_indexed=42,
        ),
    )
    embedder = overrides.pop("embedder", None)
    return build_packet(
        readout=readout,
        job=job,
        watermark=watermark,
        signer=signer,
        trial=TrialContext(run_id=_RUN_ID, **overrides),
        produced_at=_PRODUCED_AT,
        embedder=embedder,
    )


@pytest.fixture
def packet(alpha_projection, signer):
    return _packet(_readout(alpha_projection), signer)


class TestCanonicalValidatorParity:
    def test_the_packet_revalidates_through_the_canonical_validator(
        self, packet
    ) -> None:
        """Wire shape, not Python shape.

        Dumping to JSON mode and revalidating is the only way to prove the
        *serialized* packet passes; constructing the model proves the
        constructor's view of it.
        """

        payload = json.loads(packet.model_dump_json())
        model = INVESTIGATION_CONTRACT_MODELS["ask_dev_investigation_packet.v1"]
        model.model_validate(payload)

    def test_every_embedded_section_revalidates_as_its_own_contract(
        self, packet
    ) -> None:
        payload = json.loads(packet.model_dump_json())
        for field, schema_version in (
            ("analytical_job", "ask_dev_analytical_job.v1"),
            ("subject_discovery", "ask_dev_subject_discovery.v1"),
            ("comparison_cohort", "ask_dev_comparison_cohort.v1"),
            ("related_context", "ask_dev_related_context.v1"),
            ("driver_analysis", "ask_dev_driver_analysis.v1"),
            ("evidence_coverage", "ask_dev_evidence_coverage.v1"),
            ("versions", "ask_dev_investigation_versions.v1"),
        ):
            INVESTIGATION_CONTRACT_MODELS[schema_version].model_validate(payload[field])

    def test_the_packet_is_json_serializable_within_the_byte_budget(
        self, packet
    ) -> None:
        assert len(packet.model_dump_json()) < 2_000_000


class TestBackendNeutrality:
    def test_no_backend_vocabulary_appears_outside_trial_metadata(self, packet) -> None:
        payload = json.loads(packet.model_dump_json())
        payload["versions"].pop("trial", None)
        serialized = json.dumps(payload).lower()
        offenders = [word for word in _BACKEND_VOCABULARY if word in serialized]
        assert not offenders, (
            f"backend vocabulary leaked onto the wire: {offenders}; the packet "
            "must not couple a consumer to any graph backend"
        )

    def test_arm_identity_lives_only_in_trial_metadata(self, packet) -> None:
        assert packet.versions.trial is not None
        assert packet.versions.trial.arm_id == ARM_ID
        assert packet.versions.trial.producer_id == PRODUCER_ID
        payload = json.loads(packet.model_dump_json())
        payload["versions"].pop("trial")
        assert ARM_ID not in json.dumps(payload)

    def test_trial_metadata_is_optional_on_the_contract(self) -> None:
        """A native arm must be complete without it.

        If the field were required, "arm identity is evaluation metadata"
        would be false and the native arm would have to carry a graph-shaped
        field.
        """

        from dev_health_ops.api.dev.investigation_contract import InvestigationVersions

        assert InvestigationVersions.model_fields["trial"].is_required() is False


class TestReproducibilityMetadata:
    def test_every_version_the_run_is_reproducible_against_is_recorded(
        self, packet
    ) -> None:
        assert packet.versions.query_version == QUERY_VERSION
        assert packet.versions.ranking_version == RANKING_VERSION
        assert packet.versions.projection_version.startswith(
            PROJECTION_VERSION.removesuffix(".v1")
        )

    def test_the_projection_version_names_the_embedder_that_produced_it(
        self, alpha_projection, signer
    ) -> None:
        """Two embedders, two projections — and the version has to say so.

        A store embedded with a hash and a store embedded with a real model
        are not the same projection. The frozen contract has no field for the
        embedder and forbids extras, so the identity is folded into
        ``projection_version`` — which is where it belongs anyway, because a
        version that called those two runs the same would make incomparable
        results look comparable.
        """

        from dev_health_ops.context_fabric.graph_arm.backend import (
            CloudEmbedder,
            DeterministicEmbedder,
        )

        readout = _readout(alpha_projection)
        hashed = _packet(readout, signer, embedder=DeterministicEmbedder())
        semantic = _packet(
            readout, signer, embedder=CloudEmbedder(model="text-embedding-3-small")
        )

        assert "deterministic" in hashed.versions.projection_version
        assert "openai" in semantic.versions.projection_version
        assert (
            hashed.versions.projection_version != semantic.versions.projection_version
        )

    def test_the_projection_version_stays_a_platform_version_token(
        self, alpha_projection, signer
    ) -> None:
        """The fold must not break the frozen token grammar.

        ``PlatformVersionToken`` exists so a provenance block cannot carry
        producer-authored copy; a model name spliced in carelessly (dots,
        dashes) would either fail validation or smuggle punctuation through.
        """

        import re

        from dev_health_ops.context_fabric.graph_arm.backend import CloudEmbedder

        pattern = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v\d+(?:\.\d+)*$")
        for model in ("text-embedding-3-small", "text-embedding-3-large"):
            packet = _packet(
                _readout(alpha_projection), signer, embedder=CloudEmbedder(model=model)
            )
            assert pattern.fullmatch(packet.versions.projection_version), (
                packet.versions.projection_version
            )

    def test_the_packet_id_is_derived_from_the_run_and_job(
        self, alpha_projection, signer
    ) -> None:
        """Two runs of the same job produce the same packet id.

        A random id would make a recorded run impossible to match against a
        re-run, which is the whole point of the reproduction procedure.
        """

        first = _packet(_readout(alpha_projection), signer)
        second = _packet(_readout(alpha_projection), signer)
        assert first.packet_id == second.packet_id

    def test_corpus_and_fixture_versions_are_carried_when_supplied(
        self, alpha_projection, signer
    ) -> None:
        packet = _packet(
            _readout(alpha_projection),
            signer,
            corpus_version="corpus.v1",
            fixture_version="graph_arm_fixture.v1",
        )
        assert packet.versions.corpus_version == "corpus.v1"
        assert packet.versions.trial is not None
        assert packet.versions.trial.fixture_version == "graph_arm_fixture.v1"


class TestEvidenceIdentity:
    def test_every_evidence_handle_verifies_against_the_platform_signer(
        self, packet, signer
    ) -> None:
        """The handles are the evidence service's, not a parallel scheme.

        Re-signing through the same ``EvidenceReferenceSigner`` the service
        uses is the only check that means anything here: a handle that merely
        matches the ``ev1_`` grammar would pass a shape assertion and fail
        the moment anyone dereferenced it.
        """

        assert packet.evidence_coverage.evidence_index
        for entry in packet.evidence_coverage.evidence_index:
            assert signer.verify(packet.organization_id, entry.evidence)

    def test_a_handle_minted_for_another_organization_does_not_verify(
        self, packet, signer
    ) -> None:
        """The negative control for the assertion above."""

        entry = packet.evidence_coverage.evidence_index[0]
        assert not signer.verify("org_someone_else", entry.evidence)

    def test_every_indexed_item_supports_something_in_the_packet(self, packet) -> None:
        known = {entity.entity_id for entity in packet.related_context.entities}
        for entry in packet.evidence_coverage.evidence_index:
            assert entry.supports_entity_ids
            assert set(entry.supports_entity_ids) <= known

    def test_evidence_spans_more_than_one_source_class(self, packet) -> None:
        """Cross-source association, at the level the packet can show it."""

        classes = {
            entry.source_class for entry in packet.evidence_coverage.evidence_index
        }
        assert len(classes) >= 4, (
            "an arm that indexed one source class has not associated across "
            f"sources; saw {sorted(str(item) for item in classes)}"
        )

    def test_no_source_class_outside_the_trial_allowlist_is_claimed(
        self, packet
    ) -> None:
        allowed = set(TRIAL_SOURCE_ALLOWLIST)
        for entry in packet.evidence_coverage.evidence_index:
            assert entry.source_class in allowed
        for observation in packet.evidence_coverage.source_health:
            assert observation.source_class in allowed


class TestHonestOutcome:
    def test_an_arm_with_no_driver_synthesis_never_claims_a_supported_outcome(
        self, packet
    ) -> None:
        """The dashboard-redirect fault mode, refused at the source.

        This revision discovers context and asserts no cause. A packet that
        said ``supported`` while asserting no driver is exactly what the
        frozen contract calls a redirect rather than an answer.
        """

        assert packet.outcome is InvestigationOutcome.UNSUPPORTED
        assert packet.driver_analysis.candidates == ()
        assert packet.evidence_coverage.limitations

    def test_the_missing_capability_is_disclosed_not_merely_absent(
        self, packet
    ) -> None:
        details = " ".join(item.detail for item in packet.evidence_coverage.limitations)
        assert "synthesizes no" in details

    def test_a_cohort_shape_this_revision_cannot_build_is_refused_loudly(
        self, alpha_projection, signer
    ) -> None:
        """Better a raised error than a fabricated one-member cohort.

        A cohort-bearing shape carrying a single member would be scored as a
        comparison the arm never made.
        """

        with pytest.raises(UnsupportedComparisonShapeError, match="fabricated"):
            _packet(
                _readout(alpha_projection),
                signer,
                comparison_shape=ComparisonShape.DISCOVERED_COHORT,
                question_family=QuestionFamilyID("struggling_teams"),
            )

    def test_a_family_that_forbids_the_requested_shape_is_refused(
        self, alpha_projection, signer
    ) -> None:
        with pytest.raises(ValueError, match="does not permit"):
            _packet(
                _readout(alpha_projection),
                signer,
                question_family=QuestionFamilyID("struggling_teams"),
            )


class TestSubjectCommitment:
    def test_the_committed_subject_is_the_rank_one_candidate(self, packet) -> None:
        assert packet.subject_discovery.committed_subject_ids == (
            "proj_nightfall_migration",
        )
        first = packet.subject_discovery.candidates[0]
        assert first.rank == 1
        assert first.canonical_id == "proj_nightfall_migration"

    def test_the_commitment_rests_on_an_exact_identifier_not_a_fuzzy_label(
        self, packet
    ) -> None:
        """A fuzzy match alone is what returns the decoy project."""

        signals = {
            signal.signal.value
            for candidate in packet.subject_discovery.candidates
            for signal in candidate.match_signals
        }
        assert signals == {"exact_canonical_id"}
        assert "fuzzy_label" not in signals

    def test_the_decoy_project_is_not_committed(self, packet) -> None:
        """``proj_nightfall`` is real, adjacent and wrong."""

        assert "proj_nightfall" not in packet.subject_discovery.committed_subject_ids


class TestSigner:
    def test_the_signer_refuses_to_operate_without_a_secret(self, monkeypatch) -> None:
        monkeypatch.delenv("JWT_SECRET_KEY", raising=False)
        with pytest.raises(RuntimeError, match="JWT_SECRET_KEY is unset"):
            signer_from_environment()

    def test_the_signer_is_available_when_the_secret_is_set(self, signing_env) -> None:
        assert signer_from_environment() is not None


class TestSourceHealthAndFreshness:
    def test_a_stale_projection_is_disclosed_on_the_packet(
        self, alpha_projection, signer
    ) -> None:
        stale = IndexWatermark(
            indexed_through=fixtures.WINDOW_END - timedelta(days=3),
            projected_at=fixtures.WINDOW_END,
            records_indexed=42,
        )
        packet = _packet(_readout(alpha_projection), signer, watermark=stale)
        kinds = {item.kind.value for item in packet.evidence_coverage.limitations}
        assert "stale_source" in kinds
        for observation in packet.evidence_coverage.source_health:
            assert observation.state.value == "available_stale"

    def test_a_never_projected_store_reports_unavailable_not_empty(
        self, alpha_projection, signer
    ) -> None:
        """An empty store must not look like a complete, uneventful answer."""

        packet = _packet(
            _readout(alpha_projection),
            signer,
            watermark=IndexWatermark(indexed_through=None),
        )
        for observation in packet.evidence_coverage.source_health:
            assert observation.state.value == "unavailable"
