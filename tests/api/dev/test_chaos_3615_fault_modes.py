"""CHAOS-3615: every named fault mode, proved rejected by the contract.

The corrective plan names eleven fault shapes and requires, for each, a test
proving the contract catches *that* defect. A golden-response test is
explicitly not sufficient — so every test here feeds the validator an
**arm-shaped bad packet**: a payload an implementation could plausibly emit,
differing from the golden only in the behaviour under test.

Two properties make these tests non-vacuous, and both are asserted rather
than assumed:

1. **The rejection comes from the named guard.** Each test asserts on a
   phrase unique to the validator the fault-mode registry names, not merely
   that "something raised". A payload rejected by an unrelated field error
   would otherwise read as coverage while the real guard was missing.
2. **The legitimate case still validates.** Every rule that could be
   "enforced" by making the good case impossible has a positive control
   here — a missing staffing denominator that is qualified rather than
   killed, discovery with nothing committed, a not-comparable historical
   slice that is still a supported investigation. A contract that rejected
   both shapes would pass every negative test and be useless.

The guards' *load-bearing-ness* — that removing a validator makes its bad
packet validate — is proved separately and permanently by
``scripts/verify_chaos_3615_fault_mode_guards.py``, which neutralizes each
named validator in a subprocess and asserts the arm-shaped payload is then
accepted. That script is what turns "this test passes" into "this test
would fail if the guard were gone".
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from dev_health_ops.api.dev.investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
    AnalyticalJob,
    AskDevInvestigationPacket,
    ComparisonCohort,
    DriverAnalysis,
    EvidenceCoverage,
    InvestigationOutcome,
    RelatedContext,
    SubjectDiscovery,
)
from dev_health_ops.api.dev.investigation_contract.fixtures import (
    negative_fixtures,
    positive_variant_fixtures,
)

PACKET = "ask_dev_investigation_packet.v1"


def _negative(contract: str, label: str) -> dict[str, Any]:
    for case_label, payload in negative_fixtures()[contract]:
        if case_label == label:
            return payload
    raise AssertionError(f"no negative fixture {contract}/{label}")


def _variant(label: str) -> dict[str, Any]:
    for case_label, payload in positive_variant_fixtures()[PACKET]:
        if case_label == label:
            return payload
    raise AssertionError(f"no positive variant {label}")


def _rejects(
    model: type[BaseModel], payload: dict[str, Any], expected_phrase: str
) -> None:
    """Assert the payload is rejected, and that the named guard rejected it."""

    with pytest.raises(ValidationError) as caught:
        model.model_validate(payload)
    message = str(caught.value)
    assert expected_phrase in message, (
        f"{model.__name__} rejected the payload, but not for the reason under "
        f"test. Expected a message containing {expected_phrase!r}; got:\n{message}"
    )


# --------------------------------------------------------------------------
# Fault 1: a wrong but similarly named subject ranks (and commits) first
# --------------------------------------------------------------------------


def test_commitment_on_fuzzy_label_alone_is_rejected() -> None:
    _rejects(
        SubjectDiscovery,
        _negative("ask_dev_subject_discovery.v1", "commitment_on_fuzzy_label_alone"),
        "committed on weak signals alone",
    )


def test_commitment_below_rank_one_is_rejected() -> None:
    """The same fault wearing a different hat: commit to the runner-up.

    Distinct from the fuzzy-label case because the signal here is strong —
    an exact display-name match on the *wrong* project. What makes it a
    fault is that the arm itself ranked something else higher and committed
    anyway, with no clarification.
    """

    _rejects(
        SubjectDiscovery,
        _negative("ask_dev_subject_discovery.v1", "commitment_below_rank_one"),
        "is committed at rank",
    )


def test_candidate_ranks_out_of_order_are_rejected() -> None:
    _rejects(
        SubjectDiscovery,
        _negative("ask_dev_subject_discovery.v1", "candidate_ranks_out_of_order"),
        "candidate ranks must be 1..n in declaration order",
    )


# --------------------------------------------------------------------------
# Fault 2: organization widening after an unresolved named reference
# --------------------------------------------------------------------------


def test_organization_widening_after_unresolved_mention_is_rejected() -> None:
    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "organization_widening_after_unresolved_reference"),
        "widened to organization scope",
    )


def test_organization_scope_is_legal_when_the_packet_is_asking() -> None:
    """Positive control for the widening guard.

    Organization scope with an unresolved mention outstanding is legitimate
    in exactly one case — the packet is asking for clarification rather than
    answering. If this validated nothing, the widening guard could have been
    implemented as "never allow organization scope", which would ban a
    correct behaviour.
    """

    packet = AskDevInvestigationPacket.model_validate(
        _variant("needs_clarification_with_widening")
    )
    assert packet.outcome is InvestigationOutcome.NEEDS_CLARIFICATION
    assert packet.subject_discovery.unresolved_mentions
    assert packet.evidence_coverage.clarification_needs


# --------------------------------------------------------------------------
# Fault 3: irrelevant high-volume evidence displaces the expected lineage
# --------------------------------------------------------------------------


def test_evidence_that_supports_nothing_is_rejected() -> None:
    _rejects(
        EvidenceCoverage,
        _negative("ask_dev_evidence_coverage.v1", "evidence_supporting_nothing"),
        "supports nothing in this packet",
    )


def test_evidence_cited_but_not_indexed_is_rejected() -> None:
    """The other half of evidence closure: a citation with nothing behind it."""

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "evidence_citation_absent_from_index"),
        "cited but absent from the evidence",
    )


# --------------------------------------------------------------------------
# Fault 4: a symptom labelled the principal driver without supporting paths
# --------------------------------------------------------------------------


def test_symptom_promoted_to_principal_driver_is_rejected() -> None:
    _rejects(
        DriverAnalysis,
        _negative("ask_dev_driver_analysis.v1", "symptom_promoted_to_principal_driver"),
        "a symptom is not a driver",
    )


def test_principal_driver_without_supporting_path_is_rejected() -> None:
    _rejects(
        DriverAnalysis,
        _negative(
            "ask_dev_driver_analysis.v1", "principal_driver_without_supporting_path"
        ),
        "no supporting relationship path",
    )


def test_historical_driver_promoted_to_principal_is_rejected() -> None:
    _rejects(
        DriverAnalysis,
        _negative(
            "ask_dev_driver_analysis.v1", "historical_driver_promoted_to_principal"
        ),
        "not a principal current driver",
    )


def test_symptom_remains_a_legitimate_candidate() -> None:
    """Positive control: symptoms are reportable, just not promotable.

    The golden carries a rising-cycle-time candidate classified ``symptom``
    and excluded from principal standing with a stated reason. If the guard
    had been implemented as "reject symptoms", the packet would lose the
    observation that makes the driver legible.
    """

    from dev_health_ops.api.dev.investigation_contract.fixtures import (
        positive_fixtures,
    )

    analysis = DriverAnalysis.model_validate(
        positive_fixtures()["ask_dev_driver_analysis.v1"]
    )
    symptoms = [
        candidate for candidate in analysis.candidates if candidate.role == "symptom"
    ]
    assert symptoms, "the golden must keep a symptom candidate to be meaningful"
    assert all(candidate.exclusion_reason is not None for candidate in symptoms)


# --------------------------------------------------------------------------
# Fault 5: a staffing claim presented as certain without allocation evidence
# --------------------------------------------------------------------------


def test_certain_staffing_claim_without_denominator_is_rejected() -> None:
    _rejects(
        DriverAnalysis,
        _negative(
            "ask_dev_driver_analysis.v1", "staffing_certainty_without_denominator"
        ),
        "presents a staffing claim as certain",
    )


def test_capacity_driver_without_any_staffing_qualification_is_rejected() -> None:
    _rejects(
        DriverAnalysis,
        _negative(
            "ask_dev_driver_analysis.v1",
            "capacity_driver_without_staffing_qualification",
        ),
        "says nothing about its denominator",
    )


def test_missing_denominator_still_supports_a_qualified_capacity_claim() -> None:
    """Positive control, and the anti-drift rule that matters most here.

    The correction addendum requires that missing staffing data *reduce
    confidence* rather than make capacity questions unsupported. This packet
    asserts a capacity driver with ``denominator_absent`` at ``qualified``
    confidence and must validate — otherwise the staffing guard would have
    silently converted "qualify" into "refuse".
    """

    packet = AskDevInvestigationPacket.model_validate(
        _variant("qualified_capacity_without_denominator")
    )
    capacity = [
        candidate
        for candidate in packet.driver_analysis.candidates
        if candidate.staffing_qualification is not None
    ]
    assert capacity, "the variant must actually carry a capacity claim"
    assert all(
        candidate.staffing_qualification is not None
        and candidate.staffing_qualification.denominator_state == "denominator_absent"
        for candidate in capacity
    )
    assert packet.outcome in {
        InvestigationOutcome.SUPPORTED,
        InvestigationOutcome.SUPPORTED_WITH_GAPS,
    }


# --------------------------------------------------------------------------
# Fault 6: the cohort includes an unrelated project
# --------------------------------------------------------------------------


def test_cohort_member_without_inclusion_evidence_is_rejected() -> None:
    _rejects(
        ComparisonCohort,
        _negative(
            "ask_dev_comparison_cohort.v1",
            "unrelated_member_without_inclusion_evidence",
        ),
        "neither evidence nor an explicit no-evidence classification",
    )


# --------------------------------------------------------------------------
# Fault 7: a relationship is reversed
# --------------------------------------------------------------------------


def test_reversed_relationship_direction_is_rejected() -> None:
    _rejects(
        RelatedContext,
        _negative("ask_dev_related_context.v1", "reversed_relationship_direction"),
        "contradicts the canonical orientation",
    )


def test_disconnected_path_presented_as_a_chain_is_rejected() -> None:
    """A path whose hops do not join is a false lineage built of true edges."""

    _rejects(
        RelatedContext,
        _negative("ask_dev_related_context.v1", "disconnected_path_presented_as_chain"),
        "is not connected",
    )


# --------------------------------------------------------------------------
# Fault 8: a path crosses an unauthorized entity
# --------------------------------------------------------------------------


def test_path_through_unauthorized_entity_is_rejected() -> None:
    """The unauthorized entity is an *intermediate* node, not a result.

    Deliberately so: the fixture drops only the terminal dependency from the
    authorized set while every returned entity record stays visible. A guard
    that checked returned entities alone would pass this payload and still
    leak the existence of a restricted node through the path that crosses
    it.
    """

    _rejects(
        RelatedContext,
        _negative("ask_dev_related_context.v1", "path_crosses_unauthorized_entity"),
        "outside the authorized set",
    )


# --------------------------------------------------------------------------
# Fault 9: dashboard redirection without a direct judgment
# --------------------------------------------------------------------------


def test_supported_outcome_without_any_asserted_driver_is_rejected() -> None:
    """The structural form of "here are some links, you work it out".

    The payload is well formed, carries a full evidence index, a populated
    lineage section and surface references — everything except a judgment.
    """

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "dashboard_redirect_without_direct_judgment"),
        "has redirected, not answered",
    )


def test_no_match_without_limitation_or_clarification_is_rejected() -> None:
    """ "No match" may not become the silent, privileged default."""

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "no_match_without_limitation_or_clarification"),
        "silent default wearing an outcome label",
    )


# --------------------------------------------------------------------------
# Fault 10: an absent required field silently defaults to a privileged value
# --------------------------------------------------------------------------


def test_absent_truncation_flag_is_rejected_rather_than_defaulted() -> None:
    _rejects(
        SubjectDiscovery,
        _negative("ask_dev_subject_discovery.v1", "absent_truncation_disclosure_field"),
        "candidates_truncated",
    )


def test_disclosure_fields_have_no_reassuring_default() -> None:
    """No boolean or count field anywhere in the contract may be optional.

    Derived from the models themselves rather than from a hand-maintained
    list, so a disclosure field added later cannot dodge the rule by simply
    not being written down here. Every ``bool`` and every ``*_count`` on
    every contract model is a disclosure — "was anything hidden from you",
    "was this truncated", "how many results were filtered" — and each has
    exactly one comfortable default that a forgetful producer would inherit.
    """

    from dev_health_ops.api.dev.investigation_contract import packet as packet_module

    checked = 0
    offenders: list[str] = []
    for name in dir(packet_module):
        model = getattr(packet_module, name)
        if not (isinstance(model, type) and issubclass(model, BaseModel)):
            continue
        if model.__module__ != packet_module.__name__:
            continue
        for field_name, field in model.model_fields.items():
            is_flag = field.annotation is bool
            is_count = field_name.endswith("_count")
            if not (is_flag or is_count):
                continue
            checked += 1
            if not field.is_required():
                offenders.append(f"{name}.{field_name}")
    assert checked >= 8, (
        "expected the contract to carry several boolean/count disclosure "
        f"fields; only found {checked}, which suggests this test is scanning "
        "the wrong models"
    )
    assert not offenders, (
        "these disclosure fields would silently default to the flattering "
        f"value if a producer omitted them: {sorted(offenders)}"
    )


def test_non_boolean_disclosure_fields_are_also_required() -> None:
    """The disclosures whose privileged default is not ``False`` or ``0``."""

    from dev_health_ops.api.dev.investigation_contract.packet import (
        BoundedTimeContext,
        LineagePath,
    )

    pinned: tuple[tuple[type[BaseModel], str], ...] = (
        (ComparisonCohort, "completeness"),
        (LineagePath, "source_health"),
        (BoundedTimeContext, "historical_comparability"),
        (BoundedTimeContext, "analytical_slice"),
        (AskDevInvestigationPacket, "outcome"),
    )
    for model, field_name in pinned:
        assert model.model_fields[field_name].is_required(), (
            f"{model.__name__}.{field_name} must be required; its most "
            "convenient default is also its most flattering one"
        )


# --------------------------------------------------------------------------
# Fault 11: a wildcard or optional field makes the check vacuous
# --------------------------------------------------------------------------


def test_cohort_claiming_comparison_without_dimensions_is_rejected() -> None:
    """A cohort that supports no comparison cannot claim to be one."""

    _rejects(
        ComparisonCohort,
        _negative(
            "ask_dev_comparison_cohort.v1", "comparison_claimed_without_dimensions"
        ),
        "declares no supported comparison dimension",
    )


def test_identifier_grammar_admits_no_wildcard_token() -> None:
    """``*`` cannot appear in any identifier this contract accepts.

    The cheapest way to make an authorization or cohort check vacuous is an
    identifier that means "all of them". ``OpaqueID``'s grammar excludes the
    character outright, so the class of attack is closed rather than
    policed; this test pins that the grammar has not been loosened.
    """

    packet = _variant("trial_metadata_present")
    wildcarded = {
        **packet,
        "organization_id": "*",
    }
    _rejects(AskDevInvestigationPacket, wildcarded, "organization_id")


def test_truncation_without_a_reason_is_rejected() -> None:
    _rejects(
        ComparisonCohort,
        _negative("ask_dev_comparison_cohort.v1", "truncated_without_reason"),
        "declares no truncation_reason",
    )


# --------------------------------------------------------------------------
# Anti-drift positive controls for the remaining named rules
# --------------------------------------------------------------------------


def test_discovery_without_any_commitment_is_legal() -> None:
    """Exact subject commitment is not a prerequisite for discovery.

    A packet with every candidate merely ``proposed`` — no committed
    subject at all — still carries a full related-context and driver
    section, and validates. This is the correction addendum's rule stated as
    a fixture: an arm that refused to investigate before committing could
    not answer the struggling-teams family at all.
    """

    packet = AskDevInvestigationPacket.model_validate(
        _variant("discovery_without_commitment")
    )
    assert packet.subject_discovery.committed_subject_ids == ()
    assert packet.subject_discovery.candidates
    assert packet.related_context.paths
    assert packet.driver_analysis.principal_driver_ids


def test_not_comparable_historical_slice_is_valid_and_supported() -> None:
    """CHAOS-3569-gapped history is NOT COMPARABLE, not a blocker.

    The packet declares ``not_comparable_missing_edge_validity``, discloses
    the limitation, and stays a supported investigation. A contract that
    downgraded the outcome here would turn an open dependency into a
    permanent trial failure.
    """

    packet = AskDevInvestigationPacket.model_validate(
        _variant("not_comparable_historical_slice")
    )
    assert (
        packet.analytical_job.time_context.historical_comparability
        == "not_comparable_missing_edge_validity"
    )
    assert packet.outcome is InvestigationOutcome.SUPPORTED
    assert any(
        limitation.kind == "historical_slice_not_comparable"
        for limitation in packet.evidence_coverage.limitations
    )


def test_not_comparable_slice_must_still_be_disclosed() -> None:
    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "not_comparable_slice_undisclosed"),
        "HISTORICAL_SLICE_NOT_COMPARABLE limitation is disclosed",
    )


def test_authorization_filtering_must_be_disclosed() -> None:
    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "authorization_filtering_undisclosed"),
        "AUTHORIZATION_FILTERED limitation is disclosed",
    )


def test_trial_metadata_is_optional_and_changes_nothing_else() -> None:
    """Arm identity is evaluation metadata, not product truth.

    The golden validates without ``versions.trial`` at all; the variant adds
    it and nothing else. If any other field were required to change when an
    arm identified itself, the contract would be coupling consumers to the
    arm.
    """

    from dev_health_ops.api.dev.investigation_contract.fixtures import (
        positive_fixtures,
    )

    plain = positive_fixtures()[PACKET]
    assert plain["versions"]["trial"] is None
    AskDevInvestigationPacket.model_validate(plain)

    with_trial = _variant("trial_metadata_present")
    packet = AskDevInvestigationPacket.model_validate(with_trial)
    assert packet.versions.trial is not None

    stripped = {**with_trial, "versions": {**with_trial["versions"]}}
    stripped["versions"]["trial"] = None
    stripped["versions"]["corpus_version"] = None
    assert stripped == plain, (
        "the trial-metadata variant differs from the golden in fields other "
        "than versions.trial/corpus_version, so arm identity is not isolated"
    )


# --------------------------------------------------------------------------
# Round-1 adversarial review: holes found by attacking the first draft
#
# Each of these was a packet the first version of the contract ACCEPTED. They
# are kept as tests rather than folded into the sections above because the
# named eleven fault shapes came from the corrective plan, while these came
# from an adversary reading the implementation — a different provenance, and
# a different thing to keep honest.
# --------------------------------------------------------------------------


def test_cohort_member_outside_the_authorized_set_is_rejected() -> None:
    """H2. Authorization covered lineage only; a cohort member walked past it."""

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "cohort_member_outside_authorized_set"),
        "not in related_context.authorized_entity_ids",
    )


def test_evidence_naming_an_unauthorized_entity_is_rejected() -> None:
    """H2. An indexed evidence item is a label reaching a consumer too."""

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "evidence_entity_outside_authorized_set"),
        "not in related_context.authorized_entity_ids",
    )


def test_source_class_off_the_trial_allowlist_is_rejected() -> None:
    """H3. CHAOS-3567's inert temporal stub was claimable as coverage."""

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "source_class_off_the_trial_allowlist"),
        "not on the trial allowlist",
    )


def test_question_family_shape_mismatch_is_rejected() -> None:
    """H4. Family was metadata: an arm could claim the easiest one and skip its work."""

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "question_family_shape_mismatch"),
        "does not permit the singular_subject comparison shape",
    )


def test_family_required_source_neither_observed_nor_declared_missing() -> None:
    """H4. A required source may be absent — it may not be unmentioned."""

    _rejects(
        AskDevInvestigationPacket,
        _negative(PACKET, "family_required_source_unaccounted"),
        "neither observed nor declared missing",
    )


def test_declaring_a_required_source_missing_is_still_legal() -> None:
    """Positive control for the family-obligation guard.

    The golden declares ``investment_allocation`` unconfigured and is
    accepted. If the guard had required every source to be *present*, an arm
    would be pushed to fabricate coverage rather than disclose the gap —
    the opposite of what the rule is for.
    """

    from dev_health_ops.api.dev.investigation_contract.fixtures import (
        positive_fixtures,
    )

    packet = AskDevInvestigationPacket.model_validate(positive_fixtures()[PACKET])
    assert any(
        missing.source_class == "investment_allocation"
        for missing in packet.evidence_coverage.missing_sources
    )


def test_path_cited_by_an_entity_it_never_reaches_is_rejected() -> None:
    """M5. Existence is not attachment: the path terminated somewhere else."""

    _rejects(
        RelatedContext,
        _negative("ask_dev_related_context.v1", "path_cited_but_never_reaching_entity"),
        "cites paths that never reach it",
    )


def test_source_reported_both_observed_and_missing_is_rejected() -> None:
    """M6. Two contradictory coverage claims, whichever flatters the score."""

    _rejects(
        EvidenceCoverage,
        _negative("ask_dev_evidence_coverage.v1", "source_both_observed_and_missing"),
        "both as observed and as missing",
    )


def test_comparable_history_without_edge_validity_is_rejected() -> None:
    """M7. A historical delta computed off the live projection is fabricated."""

    _rejects(
        AnalyticalJob,
        _negative(
            "ask_dev_analytical_job.v1", "comparable_history_without_edge_validity"
        ),
        "is not a legal edge_validity_basis",
    )


def test_unavailable_edge_validity_must_use_the_chaos_3569_state() -> None:
    """M7. The gap may not be relabelled as a vaguer non-comparability."""

    _rejects(
        AnalyticalJob,
        _negative("ask_dev_analytical_job.v1", "unavailable_edge_validity_mislabelled"),
        "not_comparable_missing_edge_validity",
    )


def test_observed_edge_intervals_still_permit_a_comparable_history() -> None:
    """Positive control for the edge-validity guard.

    An arm that genuinely reconstructed the as-of state must still be able
    to say so, or the rule would permanently ban the capability CHAOS-3569
    exists to deliver.
    """

    from dev_health_ops.api.dev.investigation_contract.fixtures import (
        positive_fixtures,
    )

    job = deepcopy(positive_fixtures()["ask_dev_analytical_job.v1"])
    job["time_context"] = {
        **job["time_context"],
        "analytical_slice": "historical",
        "as_of": "2026-05-08T00:00:00Z",
        "historical_comparability": "comparable",
        "edge_validity_basis": "observed_intervals",
    }
    parsed = AnalyticalJob.model_validate(job)
    assert parsed.time_context.historical_comparability == "comparable"


@pytest.mark.parametrize("contract", sorted(INVESTIGATION_CONTRACT_MODELS))
def test_every_contract_has_a_negative_fixture_that_actually_fails(
    contract: str,
) -> None:
    """A negative fixture that quietly starts passing is a dead guard."""

    cases = negative_fixtures()[contract]
    assert cases, f"{contract} has no negative fixture"
    model = INVESTIGATION_CONTRACT_MODELS[contract]
    for label, payload in cases:
        with pytest.raises(ValidationError):
            model.model_validate(payload)
        assert label, f"{contract} has an unlabelled negative fixture"
