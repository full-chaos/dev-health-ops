"""CHAOS-3618: the native projection must produce a packet that is true.

Every test here runs the produced packet through the **canonical**
validator — the same ``INVESTIGATION_CONTRACT_MODELS`` entry the frozen
manifest names — rather than the generated JSON Schema, which catches 3 of
41 semantic faults. A projection that only satisfied the schema would pass
a green suite while emitting packets the trial's own contract rejects.

The negative tests are the point of the file. They plant the two ways a
baseline arm flatters itself — inventing a relationship, and widening to
the organization after a name failed to resolve — and require the
projection to refuse.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import (
    DevEvidenceFlags,
    DevEvidenceRef,
    FreshnessState,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    DeficiencyCategory,
    DeficiencyFinding,
    DeficiencyRemediation,
    DeficiencySeverity,
)
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.contracts_v2.health_rules import RuleApplicability
from dev_health_ops.api.dev.contracts_v2.intent import DevQuestionIntent
from dev_health_ops.api.dev.contracts_v2.result import (
    DevInvestigationResult,
    DevSourceContent,
    DevSourceObservation,
)
from dev_health_ops.api.dev.contracts_v2.subject import (
    DevEntityRefV2,
    DevResolutionCandidate,
    DevResolutionEntry,
    DevResolutionLedger,
    DevSubjectSet,
    ResolutionOutcome,
)
from dev_health_ops.api.dev.investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
    QUESTION_FAMILY_REGISTRY,
    ComparisonShape,
    DriverStanding,
    InvestigationEvidenceEntry,
    InvestigationOutcome,
    PacketLimitationKind,
    PacketSection,
    QuestionFamilyID,
    RelevanceState,
    SubjectCommitmentState,
    SubjectDiscovery,
    UnresolvedMention,
    UnresolvedMentionReason,
)
from dev_health_ops.api.dev.question_interpreter import InterpretedQuestion
from dev_health_ops.context_fabric.native_arm import capabilities as caps
from dev_health_ops.context_fabric.native_arm import projection as proj

_NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
_ORG = "org-1"
_NAMESPACE = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _handle(seed: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, seed))


def _evidence_handle(seed: str) -> str:
    """A structurally valid ``EvidenceHandle`` (``ev1_`` + 40 hex)."""

    return "ev1_" + hashlib.sha1(seed.encode()).hexdigest()


def _ref(entity_id: str, kind: EntityKind, label: str) -> DevEntityRefV2:
    return DevEntityRefV2(entity_kind=kind, entity_id=entity_id, display_label=label)


def _intent(
    *,
    intent_id: QuestionIntentID = QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY,
    cardinality: Cardinality = Cardinality.SINGULAR,
    confidence: float = 0.9,
) -> DevQuestionIntent:
    return DevQuestionIntent(
        schema_version="dev_question_intent.v1",
        intent_id=intent_id,
        interpreter_version="intent_interpreter.v1",
        cardinality=cardinality,
        subject_kinds=(EntityKind.PROJECT,),
        mention_ordinals=(
            ()
            if cardinality is Cardinality.ORGANIZATION_WIDE
            else (0,)
            if cardinality is Cardinality.SINGULAR
            else (0, 1)
        ),
        confidence=confidence,
        interpretation_reasons=("deterministic recognizer matched",),
        requires_clarification=False,
        generated_at=_NOW,
    )


def _interpretation(**kwargs: object) -> InterpretedQuestion:
    return InterpretedQuestion(
        intent=_intent(**kwargs),  # type: ignore[arg-type]
        mentions=(),
        untyped_mention_ids=frozenset(),
        total_named_mention_count=1,
    )


def _ledger(
    *,
    outcome: ResolutionOutcome = ResolutionOutcome.EXACT_MATCH,
    committed: DevEntityRefV2 | None = None,
    candidates: tuple[DevResolutionCandidate, ...] = (),
) -> DevResolutionLedger:
    """A ledger in the shape preflight actually appends.

    ``EXACT_MATCH`` cannot carry candidates (``validate_outcome_payload``),
    so a run that saw alternatives and then committed is two entries for
    one mention — which is also what really happens.
    """

    mention_id = _handle("mention-0")
    entries: list[DevResolutionEntry] = []
    if candidates:
        entries.append(
            DevResolutionEntry(
                entry_ordinal=len(entries),
                mention_id=mention_id,
                outcome=ResolutionOutcome.AMBIGUOUS_CANDIDATES,
                candidates=candidates,
                resolver_version="subject_preflight.v1",
                query_version="scope_catalog.v1",
                resolved_at=_NOW,
            )
        )
    entries.append(
        DevResolutionEntry(
            entry_ordinal=len(entries),
            mention_id=mention_id,
            outcome=outcome,
            committed_entity_ref=committed,
            resolver_version="subject_preflight.v1",
            query_version="scope_catalog.v1",
            resolved_at=_NOW,
        )
    )
    return DevResolutionLedger(
        schema_version="dev_resolution_ledger.v1",
        ledger_id=_handle("ledger"),
        mention_ids=(mention_id,),
        entries=tuple(entries),
        updated_at=_NOW,
    )


def _deficiency_finding(*, evidence_ref_ids: tuple[str, ...]) -> DeficiencyFinding:
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=_handle("finding-1"),
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.test_rule.v1",
        rule_version="deficiency_rule.test_rule.v1",
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        severity=DeficiencySeverity.AT_RISK,
        fact_kind="observed",
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        sample_count=3,
        coverage=1.0,
        current_window_days=30,
        comparison_window_days=None,
        evidence_ref_ids=evidence_ref_ids,
        evidence_classification=None,
        blast_radius="Deployment evidence is absent for a declared-complete project.",
        remediation=DeficiencyRemediation(
            schema_version="deficiency_remediation.v1",
            remediation_template="Attach a release record.",
            verification_condition="Resolves once a deployment is observed.",
        ),
        evaluated_at=_NOW,
    )


def _evidence(entity_id: str, handle: str) -> DevEvidenceRefV2:
    return DevEvidenceRefV2.model_validate(
        DevEvidenceRef(
            schema_version="dev_evidence_ref.v1",
            evidence_ref_id=handle,
            source_system="work_items",
            source_version="status.entity.v2",
            entity_type="project",
            entity_id=entity_id,
            display_label="Atlas migration",
            observed_at=_NOW,
            freshness=FreshnessState.FRESH,
            provenance="persisted",
            confidence=1.0,
            flags=DevEvidenceFlags(),
        ).model_dump()
    )


def _investigation_result(
    *, deficiency_findings: tuple[DeficiencyFinding, ...] = ()
) -> DevInvestigationResult:
    return DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id=_handle("result"),
        plan_id="deficiency.operational.v1",
        plan_version="deficiency.operational.v1.0",
        run_id=_handle("run"),
        subject_entity_id="proj-1",
        observations=(
            DevSourceObservation(
                schema_version="dev_source_observation.v1",
                observation_id=_handle("obs-1"),
                source_class=SourceClass.DEFICIENCY_INVENTORY,
                adapter_id="operational_deficiency_service.evaluate.v1",
                requirement_level="mandatory",
                observed_state=SourceRequirementState.AVAILABLE_CURRENT,
                data_semantics="measured_zero",
                subject_coverage=1.0,
                usable_fact_count=len(deficiency_findings),
                observed_at=_NOW,
                query_version="deficiency.operational.v1",
                content=DevSourceContent(
                    schema_version="dev_source_content.v1",
                    deficiency_findings=deficiency_findings,
                ),
            ),
            DevSourceObservation(
                schema_version="dev_source_observation.v1",
                observation_id=_handle("obs-2"),
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="status_snapshot.v1",
                requirement_level="mandatory",
                observed_state=SourceRequirementState.AVAILABLE_CURRENT,
                data_semantics="measured_zero",
                subject_coverage=1.0,
                usable_fact_count=1,
                observed_at=_NOW,
                query_version="status.entity.v2",
            ),
        ),
        completed_steps=("deficiency_evaluation",),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=True,
        completed_at=_NOW,
    )


def _payload(**overrides: object) -> proj.NativeProjectionInput:
    subject = _ref("proj-1", EntityKind.PROJECT, "Atlas migration")
    handle = _evidence_handle("evidence-1")
    defaults: dict[str, object] = {
        "org_id": _ORG,
        "run_id": _handle("run"),
        "produced_at": _NOW,
        "interpretation": _interpretation(),
        "ledger": _ledger(committed=subject),
        "subject_set": None,
        "committed_subject": subject,
        "investigation_result": _investigation_result(
            deficiency_findings=(_deficiency_finding(evidence_ref_ids=(handle,)),)
        ),
        "evidence": (_evidence("proj-1", handle),),
        "window_start": datetime(2026, 7, 9, tzinfo=UTC),
        "window_end": _NOW,
    }
    defaults.update(overrides)
    return proj.NativeProjectionInput(**defaults)  # type: ignore[arg-type]


def _validate(packet: object) -> None:
    """Round-trip through the canonical validator the manifest names."""

    model = INVESTIGATION_CONTRACT_MODELS["ask_dev_investigation_packet.v1"]
    assert packet is not None
    model.model_validate(packet.model_dump(mode="json"))  # type: ignore[attr-defined]


# --------------------------------------------------------------------------
# The happy path is still an honest path
# --------------------------------------------------------------------------


def test_a_projected_packet_passes_the_canonical_validator() -> None:
    outcome = proj.project_native_investigation(_payload())
    assert outcome.gaps == ()
    _validate(outcome.packet)


def test_the_packet_names_the_native_arm_and_its_projection_version() -> None:
    packet = proj.project_native_investigation(_payload()).packet
    assert packet is not None
    trial = packet.versions.trial
    assert trial is not None
    assert trial.arm_id == proj.NATIVE_ARM_ID
    assert trial.producer_id == proj.NATIVE_PROJECTION_VERSION
    assert packet.versions.packet_schema_version == "ask_dev_investigation_packet.v1"


def test_no_native_run_can_assert_a_driver_today() -> None:
    """The headline result of the baseline, asserted rather than narrated.

    ``DriverCandidate.validate_principal_standing_is_earned`` needs a
    supporting relationship path for principal standing, and
    ``validate_supported_outcome_asserts_a_judgment`` needs an asserted
    driver for a supported outcome. Every question family the native
    interpreter can be classified into — ``project_status_drivers``,
    ``pressure_signals``, ``struggling_teams``, ``declared_versus_actual``
    — also requires a populated ``related_context`` section, and no native
    relationship survives into a contract lineage path.

    So the current product cannot, for any substantive question it can
    classify, assert a driver at all. That is not a defect in this
    projection; it is the measurement CHAOS-3614 asked for, and if a future
    change makes it false this test should fail loudly and be rewritten
    rather than relaxed.
    """

    substantive = {
        family
        for (_intent, shape), family in caps.NATIVE_QUESTION_FAMILY.items()
        if shape is not ComparisonShape.ORGANIZATION_WIDE
    }
    assert substantive, "the mapping is empty; this test would be vacuous"
    for family in substantive:
        required = QUESTION_FAMILY_REGISTRY[family].required_packet_sections
        assert PacketSection.RELATED_CONTEXT in required, family.value


def test_a_deficiency_finding_stops_at_candidate_only() -> None:
    """A real, measured finding still cannot become an asserted driver.

    The finding is genuine and its evidence is indexed — what is missing is
    the lineage the contract requires before a packet may assert a
    judgment. Recording it as ``candidate_only`` is the honest halfway
    house: the observation survives into the trial, the claim does not.
    """

    packet = proj.project_native_investigation(_payload()).packet
    assert packet is not None
    assert packet.driver_analysis.principal_driver_ids == ()
    standings = {
        candidate.standing.value for candidate in packet.driver_analysis.candidates
    }
    assert standings == {"candidate_only"}


def test_the_outcome_is_unsupported_and_says_why() -> None:
    """Unsupported with a stated reason beats a supported outcome it cannot earn."""

    packet = proj.project_native_investigation(_payload()).packet
    assert packet is not None
    assert packet.outcome is InvestigationOutcome.UNSUPPORTED
    details = " ".join(item.detail for item in packet.evidence_coverage.limitations)
    assert "related context" in details
    assert "lineage path" in details


# --------------------------------------------------------------------------
# Honest gaps
# --------------------------------------------------------------------------


def test_a_metric_shaped_intent_produces_no_packet_and_says_why() -> None:
    """Refusing to project is a measurement, not a failure."""

    outcome = proj.project_native_investigation(
        _payload(
            interpretation=_interpretation(intent_id=QuestionIntentID.METRIC_COMPARISON)
        )
    )
    assert outcome.packet is None
    assert [gap.reason for gap in outcome.gaps] == [
        proj.NativeProjectionGapReason.NO_REPRESENTABLE_QUESTION_FAMILY
    ]


def test_an_unobserved_required_source_is_declared_missing_not_omitted() -> None:
    """A family's required sources must be accounted for, honestly.

    ``declared_versus_actual`` requires ``ci_run`` and ``test_report``,
    neither of which any registered plan step mints. Omitting them would let
    an unobserved source read as an observed-empty one.
    """

    packet = proj.project_native_investigation(_payload()).packet
    assert packet is not None
    missing = {
        item.source_class.value for item in packet.evidence_coverage.missing_sources
    }
    assert {"ci_run", "test_report"} <= missing
    kinds = {item.kind for item in packet.evidence_coverage.limitations}
    assert PacketLimitationKind.MISSING_SOURCE in kinds


def test_an_unmeasurable_source_class_says_the_measurement_exists_elsewhere() -> None:
    """cognitive_load and investment_allocation are computed but unobservable.

    ``native_team_workload`` genuinely computes both; no plan step declares
    a source requirement under either class. Reporting them as flatly
    absent would understate the product, and reporting them as observed
    would overstate it.
    """

    impact = proj._missing_impact(SourceClass.COGNITIVE_LOAD)
    assert "team workload service" in impact
    assert "not observable as a source" in impact
    assert (
        proj._missing_impact(SourceClass.REVIEW)
        == "no registered plan step mints content under this source class"
    )


# --------------------------------------------------------------------------
# The negative tests — a flattering packet must be unreachable
# --------------------------------------------------------------------------


def test_no_relationship_or_related_entity_is_ever_fabricated() -> None:
    """The native arm has no projectable lineage, and must claim none.

    Work-graph edges lose both endpoint kinds before they reach the
    investigation result, so a ``LineageHop`` cannot be constructed from
    one. A projection that emitted paths anyway would be inventing the
    single thing the corrected trial is trying to measure.
    """

    packet = proj.project_native_investigation(_payload()).packet
    assert packet is not None
    assert packet.related_context.paths == ()
    assert packet.related_context.entities == ()
    for candidate in packet.driver_analysis.candidates:
        assert candidate.supporting_path_ids == ()


def test_no_cohort_member_is_ever_a_peer_the_question_did_not_name() -> None:
    """Native cohorts are named subjects, never constructed ones."""

    subject = _ref("proj-1", EntityKind.PROJECT, "Atlas migration")
    other = _ref("proj-2", EntityKind.PROJECT, "Beacon rollout")
    subject_set = DevSubjectSet(
        schema_version="dev_subject_set.v1",
        set_id=_handle("set"),
        entity_kind=EntityKind.PROJECT,
        committed_entity_refs=(subject, other),
        original_mention_count=2,
        unresolved_mention_ids=(),
        ambiguous_mention_ids=(),
        cohort_complete=True,
        fingerprint=_handle("fingerprint"),
    )
    packet = proj.project_native_investigation(
        _payload(
            interpretation=_interpretation(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.PLURAL_COHORT,
            ),
            subject_set=subject_set,
        )
    ).packet
    assert packet is not None
    assert packet.comparison_cohort.comparison_shape is ComparisonShape.EXPLICIT_COHORT
    for member in packet.comparison_cohort.members:
        assert [basis.value for basis in member.inclusion_basis] == ["explicitly_named"]
        classification = member.inclusion_evidence_classification
        assert classification is not None
        assert classification.value == "explicitly_named_by_question"


def test_an_unresolved_reference_never_widens_into_a_substantive_answer() -> None:
    """Widening after a failed resolution answers a question nobody asked.

    The run really did go organization-wide while a named reference stayed
    unresolved. The packet must say so — ``organization_wide`` shape, the
    clarification family, and a clarification need — rather than present the
    org-wide sweep as though it were the answer to the question asked.
    ``validate_no_unsafe_organization_widening`` is the contract-side
    backstop; this asserts the producer reaches the same conclusion first.
    """

    projects = (
        _ref("proj-1", EntityKind.PROJECT, "Atlas migration"),
        _ref("proj-2", EntityKind.PROJECT, "Beacon rollout"),
    )
    subject_set = DevSubjectSet(
        schema_version="dev_subject_set.v1",
        set_id=_handle("set-org"),
        entity_kind=EntityKind.PROJECT,
        committed_entity_refs=projects,
        original_mention_count=2,
        unresolved_mention_ids=(),
        ambiguous_mention_ids=(),
        cohort_complete=True,
        fingerprint=_handle("fingerprint-org"),
    )
    outcome = proj.project_native_investigation(
        _payload(
            interpretation=_interpretation(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.ORGANIZATION_WIDE,
            ),
            ledger=_ledger(
                outcome=ResolutionOutcome.NO_AUTHORIZED_MATCH, committed=None
            ),
            committed_subject=None,
            subject_set=subject_set,
        )
    )
    packet = outcome.packet
    assert packet is not None
    _validate(packet)
    assert packet.analytical_job.comparison_shape is ComparisonShape.ORGANIZATION_WIDE
    assert packet.analytical_job.question_family.value == "clarification_and_no_match"
    assert packet.outcome is InvestigationOutcome.NEEDS_CLARIFICATION
    assert packet.evidence_coverage.clarification_needs
    assert all(
        candidate.standing.value not in {"principal_driver", "contributing_driver"}
        for candidate in packet.driver_analysis.candidates
    )


def test_evidence_outside_the_authorized_set_is_dropped_not_admitted() -> None:
    """Widening the authorized set to fit the evidence would be a rubber stamp.

    Calls the filter directly. Two earlier versions of this test went
    through the whole projection and neither could prove the filter: first
    the contract's ``validate_every_entity_is_authorized`` rejected the
    packet, then (once the projection became total) the wrapper turned that
    rejection into a gap and ``assert packet is not None`` fired. Both times
    the plant "killed" the test without the filter ever running.
    """

    rogue = _evidence("proj-999", _evidence_handle("evidence-rogue"))
    entry = InvestigationEvidenceEntry(
        evidence=rogue,
        source_class=SourceClass.WORK_ITEM,
        supports_subject_ids=("proj-1",),
        relevance=RelevanceState.CURRENT,
    )
    kept, filtered = proj._restrict_evidence_to_authorized(
        (entry,), authorized=frozenset({"proj-1"})
    )
    assert kept == (), "GUARD unauthorized_evidence_is_dropped"
    assert filtered == 1, "GUARD unauthorized_evidence_is_counted"


def test_a_packet_with_dropped_evidence_still_validates() -> None:
    """Defence in depth: the contract agrees with the producer."""

    packet = proj.project_native_investigation(
        _payload(evidence=(_evidence("proj-999", _evidence_handle("evidence-rogue")),))
    ).packet
    assert packet is not None
    _validate(packet)


def test_a_driver_citing_an_unindexed_handle_is_not_asserted() -> None:
    """An attribution resting on evidence nobody can dereference is unsupported."""

    packet = proj.project_native_investigation(
        _payload(
            investigation_result=_investigation_result(
                deficiency_findings=(
                    _deficiency_finding(
                        evidence_ref_ids=(_evidence_handle("never-indexed"),)
                    ),
                )
            )
        )
    ).packet
    assert packet is not None
    _validate(packet)
    assert packet.driver_analysis.principal_driver_ids == ()
    assert all(
        candidate.standing.value != "contributing_driver"
        for candidate in packet.driver_analysis.candidates
    )


def test_a_committed_subject_always_ranks_first() -> None:
    """``validate_commitment_is_evidenced`` requires it; so does honesty."""

    subject = _ref("proj-1", EntityKind.PROJECT, "Atlas migration")
    decoy = DevResolutionCandidate(
        entity_ref=_ref("proj-2", EntityKind.PROJECT, "Atlas migration (legacy)"),
        reason="label prefix match",
    )
    packet = proj.project_native_investigation(
        _payload(ledger=_ledger(committed=subject, candidates=(decoy,)))
    ).packet
    assert packet is not None
    _validate(packet)
    first = packet.subject_discovery.candidates[0]
    assert first.canonical_id == "proj-1"
    assert first.commitment_state is SubjectCommitmentState.COMMITTED


def test_an_outcome_carries_exactly_one_of_a_packet_or_a_gap() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        proj.NativeProjectionOutcome(packet=None, gaps=())


# --------------------------------------------------------------------------
# Totality — a packet or a gap, never a raise (codex H2)
# --------------------------------------------------------------------------


def _ambiguous_ledger() -> DevResolutionLedger:
    """Two candidates, nothing committed — the shape that used to crash."""

    a = _ref("proj-1", EntityKind.PROJECT, "Atlas migration")
    b = _ref("proj-2", EntityKind.PROJECT, "Atlas migration (legacy)")
    mention = _handle("mention-0")
    return DevResolutionLedger(
        schema_version="dev_resolution_ledger.v1",
        ledger_id=_handle("ledger"),
        mention_ids=(mention,),
        entries=(
            DevResolutionEntry(
                entry_ordinal=0,
                mention_id=mention,
                outcome=ResolutionOutcome.AMBIGUOUS_CANDIDATES,
                candidates=(
                    DevResolutionCandidate(entity_ref=a, reason="label match"),
                    DevResolutionCandidate(entity_ref=b, reason="label match"),
                ),
                resolver_version="subject_preflight.v1",
                query_version="scope_catalog.v1",
                resolved_at=_NOW,
            ),
        ),
        updated_at=_NOW,
    )


def test_an_empty_organization_wide_cohort_is_a_named_gap_not_a_crash() -> None:
    """The projection's contract is a packet or a gap. Never an exception.

    This exact input used to raise ``ValidationError`` out of
    ``project_native_investigation``. That matters more than it looks: the
    consumer is a shadow seam whose job is to contain faults, so a raise
    would have been recorded as a *seam* fault — attributing the arm's
    inability to express its own run to the harness measuring it.
    """

    outcome = proj.project_native_investigation(
        _payload(
            interpretation=_interpretation(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.ORGANIZATION_WIDE,
            ),
            ledger=_ambiguous_ledger(),
            committed_subject=None,
            subject_set=None,
        )
    )
    assert outcome.packet is None
    assert [gap.reason for gap in outcome.gaps] == [
        proj.NativeProjectionGapReason.COHORT_TOO_SMALL_TO_COMPARE
    ]


def test_an_unresolved_mention_blocks_driver_assertion_even_without_lineage_need() -> (
    None
):
    """The clarification family requires no related context, so the
    family-based half of ``_may_assert`` does not fire. The unresolved-mention
    half must, or an org-wide run promotes a driver for a subject it could
    not name.
    """

    projects = (
        _ref("proj-1", EntityKind.PROJECT, "Atlas migration"),
        _ref("proj-3", EntityKind.PROJECT, "Beacon rollout"),
    )
    subject_set = DevSubjectSet(
        schema_version="dev_subject_set.v1",
        set_id=_handle("set-amb"),
        entity_kind=EntityKind.PROJECT,
        committed_entity_refs=projects,
        original_mention_count=2,
        unresolved_mention_ids=(),
        ambiguous_mention_ids=(),
        cohort_complete=True,
        fingerprint=_handle("fp-amb"),
    )
    packet = proj.project_native_investigation(
        _payload(
            interpretation=_interpretation(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.ORGANIZATION_WIDE,
            ),
            ledger=_ambiguous_ledger(),
            committed_subject=None,
            subject_set=subject_set,
        )
    ).packet
    assert packet is not None
    _validate(packet)
    assert packet.outcome is InvestigationOutcome.NEEDS_CLARIFICATION
    assert {c.standing.value for c in packet.driver_analysis.candidates} == {
        "candidate_only"
    }


def _discovery(*, unresolved: bool) -> SubjectDiscovery:
    base = SubjectDiscovery(
        schema_version="ask_dev_subject_discovery.v1",
        candidates=(),
        unresolved_mentions=(),
        committed_subject_ids=(),
        authorization_filtered_count=0,
        candidates_truncated=False,
    )
    if not unresolved:
        return base
    return base.model_copy(
        update={
            "unresolved_mentions": (
                UnresolvedMention(
                    mention_id=_handle("m"),
                    mention_text="the atlas thing",
                    reason=UnresolvedMentionReason.NO_CANDIDATE,
                ),
            )
        }
    )


def test_a_lineage_requiring_family_forbids_assertion() -> None:
    """Assert the DECISION, so a plant against it fails on this assertion.

    The original suite observed ``_may_assert`` only through the final
    packet, which meant removing it failed inside the contract validator —
    proving the contract's invariant, not the projection's. One invariant
    per test, so one plant cannot be satisfied by a different guard firing
    first.
    """

    assert (
        proj._may_assert(
            family=QuestionFamilyID.DECLARED_VERSUS_ACTUAL,
            discovery=_discovery(unresolved=False),
        )
        is False
    ), "GUARD lineage_family_forbids_assertion"


def test_an_unresolved_mention_forbids_assertion_even_without_lineage_need() -> None:
    """The clarification family needs no related context, so the family half
    of the decision does not fire. The unresolved-mention half must."""

    # Control: with everything resolved, this family DOES permit assertion,
    # so the test below is not passing for an unrelated reason.
    assert (
        proj._may_assert(
            family=QuestionFamilyID.CLARIFICATION_AND_NO_MATCH,
            discovery=_discovery(unresolved=False),
        )
        is True
    )
    assert (
        proj._may_assert(
            family=QuestionFamilyID.CLARIFICATION_AND_NO_MATCH,
            discovery=_discovery(unresolved=True),
        )
        is False
    ), "GUARD unresolved_mention_forbids_assertion"


def test_driver_analysis_drops_evidence_handles_the_packet_never_indexed() -> None:
    """Assert the DEMOTION directly, not the packet the contract would reject.

    A driver resting on a handle nobody can dereference is an unsupported
    attribution. The end-to-end test observes this through the final packet,
    where the contract's own evidence-closure rule also fires — so a plant
    removing this filter used to fail inside the validator and never reach
    the projection's own behaviour. This calls ``_driver_analysis`` directly
    with an empty indexed set, so the only thing that can fail is the filter.
    """

    unindexed = _evidence_handle("never-indexed")
    limitations = proj._Limitations()
    drivers = proj._driver_analysis(
        _payload(
            investigation_result=_investigation_result(
                deficiency_findings=(
                    _deficiency_finding(evidence_ref_ids=(unindexed,)),
                )
            )
        ),
        subject_ids=["proj-1"],
        evidence_handles=frozenset(),
        may_assert=True,
        limitations=limitations,
    )
    cited = {
        handle
        for candidate in drivers.candidates
        for handle in candidate.supporting_evidence_ids
    }
    assert unindexed not in cited, "GUARD unindexed_handle_never_supports_a_driver"
    assert all(
        candidate.standing is not DriverStanding.CONTRIBUTING_DRIVER
        for candidate in drivers.candidates
    ), "GUARD unindexed_handle_never_earns_contributing"


def test_a_dimension_needs_content_that_actually_measured_it() -> None:
    """A source class label is not a measurement.

    The original map was keyed on ``SourceClass`` and credited any
    ``PULL_REQUEST`` observation with ``review_load`` — even an empty one,
    and even though ``PULL_REQUEST`` has no approved content slot at all
    (``pull_requests`` is minted under ``STATUS_CHANGE``). A cohort claiming
    a dimension no query produced supports a comparison it cannot make.
    """

    empty = DevSourceContent(schema_version="dev_source_content.v1")
    observation = DevSourceObservation(
        schema_version="dev_source_observation.v1",
        observation_id=_handle("obs-empty"),
        source_class=SourceClass.PULL_REQUEST,
        adapter_id="status_snapshot.v1",
        requirement_level="optional",
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        subject_coverage=1.0,
        usable_fact_count=0,
        observed_at=_NOW,
        query_version="status.entity.v2",
        content=empty,
    )
    result = DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id=_handle("result-empty"),
        plan_id="status.entity.v2",
        plan_version="status.entity.v2.0",
        run_id=_handle("run"),
        subject_entity_id="proj-1",
        observations=(observation,),
        completed_steps=("status_snapshot",),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=True,
        completed_at=_NOW,
    )
    dimensions = proj._supported_dimensions(
        _payload(investigation_result=result, evidence=())
    )
    assert dimensions == (), "GUARD dimension_needs_measured_content"


def test_a_run_that_never_reached_the_plan_executor_is_a_gap_not_a_thin_packet() -> (
    None
):
    """The gap reason existed from the start and nothing ever returned it.

    A run with no ``dev_investigation_result.v1`` used to emit an
    ``unsupported`` packet anyway — a comparable artefact, carrying
    independently supplied evidence and a fabricated fallback
    source-contract version. That turns a run that never executed into
    something the trial would score, instead of a measured projection gap.
    """

    outcome = proj.project_native_investigation(_payload(investigation_result=None))
    assert outcome.packet is None, "GUARD missing_result_is_a_gap"
    assert [gap.reason for gap in outcome.gaps] == [
        proj.NativeProjectionGapReason.NO_PLAN_GOVERNED_RESULT
    ]


def test_a_result_from_another_run_is_refused() -> None:
    """A projection input is one finished run, not an assembly of parts.

    The packet's identity comes from ``run_id`` and its observations from
    ``investigation_result``. When those disagreed nothing noticed, so a
    stale retry could publish one run's findings under another run's
    identity.
    """

    foreign = _investigation_result().model_copy(
        update={"run_id": _handle("some-other-run")}
    )
    with pytest.raises(ValueError, match="belongs to run"):
        _payload(investigation_result=foreign)
