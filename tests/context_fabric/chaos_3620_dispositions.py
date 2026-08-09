"""CHAOS-3620: what this lane proved, what it did not, and why — machine-checked.

A proof suite's most dangerous output is a green run, because green is read
as "all of it holds". This lane's green run does not mean that: four of the
issue's requirements are violated by merged code, one is blocked on
CHAOS-3612, one is blocked on CHAOS-3627 and several are honestly unmeasured.
A reader who has to reconstruct that from test names will not, so it is
written down here in a form that cannot rot.

Three properties make this a ledger rather than a README:

* **Every entry names tests, and the tests must resolve.**
  ``test_chaos_3620_dispositions.py`` imports each named module and walks each
  selector. A requirement claiming ``PROVEN`` on a test that was renamed away
  is red, not quietly unproven.
* **Only ``PROVEN`` needs no excuse.** Every other status must carry a reason
  and, where something else owns the fix, a Linear blocker. An entry cannot
  be downgraded without saying who it is waiting for.
* **Totality is enforced against the issue's own bullets.**
  :data:`ISSUE_BULLETS` is the requirement list transcribed from CHAOS-3620.
  A bullet with no entry, or an entry for a bullet the issue does not
  contain, fails — so the ledger cannot drift into describing a different
  issue than the one being closed.

The rendered form (:func:`render`) is the "authorization/adversarial report"
and "provenance/deletion/revocation report" the issue asks for as required
evidence, generated from the same data the tests check rather than written
alongside it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

__all__ = [
    "INHERITED_INVARIANTS",
    "ISSUE_BULLETS",
    "REQUIREMENTS",
    "InheritedInvariant",
    "Requirement",
    "Status",
    "Transfer",
    "render",
]


class Status(StrEnum):
    #: Holds, and the named tests are what establish it.
    PROVEN = "proven"
    #: Violated by merged code. The named tests pin the current behaviour so
    #: the violation cannot close silently, and the reason carries file:line.
    DEFECT = "defect"
    #: Cannot be accepted yet because something else must land first. The
    #: blocker is a Linear id, and the requirement is never scored as a pass.
    NOT_ACCEPTED = "not_accepted"
    #: Not measured by this lane, with a stated reason. Distinct from
    #: NOT_ACCEPTED: nothing is blocking a decision, the measurement simply
    #: does not exist and saying so is more honest than a proxy.
    UNMEASURED = "unmeasured"


@dataclass(frozen=True)
class Requirement:
    requirement_id: str
    #: The issue's own words. Matched against :data:`ISSUE_BULLETS` so the
    #: ledger cannot quietly restate a requirement into something easier.
    issue_text: str
    status: Status
    proving_tests: tuple[str, ...] = ()
    reason: str = ""
    blocker: str = ""
    notes: tuple[str, ...] = field(default_factory=tuple)


_AUTHZ = "tests/context_fabric/test_chaos_3620_authorization.py"
_ADV = "tests/context_fabric/test_chaos_3620_adversarial.py"
_PROV = "tests/context_fabric/test_chaos_3620_provenance.py"
_SEM = "tests/context_fabric/test_chaos_3620_semantic_safety.py"
_A3617 = "tests/context_fabric/test_chaos_3617_authorization.py"
_C3617 = "tests/context_fabric/test_chaos_3617_containment.py"
_P3617 = "tests/context_fabric/test_chaos_3617_no_caller_supplied_partition.py"


#: Transcribed from the CHAOS-3620 issue body. The ledger is total over this
#: and nothing else.
ISSUE_BULLETS: tuple[tuple[str, str], ...] = (
    ("A1", "cross-tenant and cross-repository near-duplicate entities"),
    ("A2", "current authorization after access revocation"),
    ("A3", "graph paths containing both authorized and unauthorized entities"),
    ("A4", "candidate-subject and cohort filtering"),
    ("A5", "evidence-id substitution and scope confusion"),
    ("A6", "server-owned organization/partition derivation"),
    ("A7", "no authorization inferred from graph membership"),
    ("A8", "no organization widening after unresolved references"),
    ("A9", "hard requirement: zero unauthorized result leakage"),
    (
        "P1",
        "every material graph-assisted driver or relationship closes to "
        "authorized canonical evidence or an approved retained ACR episode",
    ),
    ("P2", "structured relationships preserve canonical IDs and direction"),
    (
        "P3",
        "extracted source assertions remain distinguishable from canonical "
        "measurements and system inference",
    ),
    (
        "P4",
        "conflicts retain both source assertions rather than silently choosing one",
    ),
    ("P5", "driver paths identify why each item was included"),
    (
        "P6",
        "deleted, redacted and revoked sources disappear from packets and "
        "downstream frames correctly",
    ),
    ("P7", "multi-source facts retain only authorized surviving provenance"),
    ("X1", "prompt injection inside source documents and episodes"),
    ("X2", "keyword-stuffed irrelevant evidence"),
    ("X3", "poisoned entity linkage"),
    ("X4", "fake aliases attached to canonical entities"),
    ("X5", "repeated low-quality paths displacing relevant evidence"),
    ("X6", "manipulated truncation and pagination"),
    ("X7", "stale indexing watermark"),
    ("X8", "graph backend outage"),
    ("X9", "extraction/provider outage or policy-forbidden mode"),
    ("X10", "malformed graph/extraction responses"),
    ("X11", "person-level productivity and staffing bait"),
    (
        "S1",
        "graph output cannot create canonical metric values, completion, "
        "readiness, health, workload, attribution or staffing truth",
    ),
    (
        "S2",
        "capacity-constrained/lightly-loaded findings disclose missing "
        "allocation or headcount data without refusing the whole question",
    ),
    ("S3", "no person-level ranking or health judgment"),
    ("S4", "symptoms are not promoted to drivers without supporting lineage"),
    ("S5", "current versus historical evidence remains explicit"),
    (
        "S6",
        "no graph-native query, mutation, partition or maintenance surface "
        "reaches Ask Dev or MCP clients",
    ),
    (
        "D1",
        "cross-runtime differential proof across the internal contract, the "
        "graph adapter, ACR, acr-mcp and the Ask Dev shadow frame",
    ),
    ("O1", "projection/query latency and failures"),
    ("O2", "indexed-through lag"),
    ("O3", "candidate, cohort, path and result counts"),
    ("O4", "authorization-filtered results"),
    ("O5", "provenance-closure failures"),
    ("O6", "stale/truncated/outage fallbacks"),
    ("O7", "extraction/provider/model/version and cost"),
    ("O8", "Graphiti/backend/version"),
    ("O9", "native versus graph packet/frame outcome differences"),
    (
        "O10",
        "no question text, answer prose, source excerpts, prompts, "
        "transcripts, credentials, person names or unbounded entity labels "
        "in ordinary telemetry",
    ),
)

_BULLET_TEXT = dict(ISSUE_BULLETS)


def _req(
    requirement_id: str,
    status: Status,
    *proving_tests: str,
    reason: str = "",
    blocker: str = "",
    notes: tuple[str, ...] = (),
) -> Requirement:
    return Requirement(
        requirement_id=requirement_id,
        issue_text=_BULLET_TEXT[requirement_id],
        status=status,
        proving_tests=proving_tests,
        reason=reason,
        blocker=blocker,
        notes=notes,
    )


REQUIREMENTS: tuple[Requirement, ...] = (
    # ---- authorization -------------------------------------------------
    _req(
        "A1",
        Status.PROVEN,
        f"{_A3617}::TestTenantIsolation::"
        "test_an_identical_canonical_id_in_two_tenants_is_two_distinct_nodes",
        f"{_A3617}::TestTenantIsolation::"
        "test_betas_near_duplicate_project_is_unreachable_from_alpha",
        f"{_A3617}::TestRepositoryScoping::"
        "test_evidence_carries_repository_scope_through_to_the_packet",
        f"{_ADV}::TestAnAliasCannotRedirectASubject::"
        "test_a_shared_acronym_across_tenants_never_crosses_the_partition",
    ),
    _req(
        "A2",
        Status.PROVEN,
        f"{_AUTHZ}::TestAuthorizationIsCurrentAfterRevocation::"
        "test_narrowing_the_grant_removes_the_entity_from_the_next_packet",
        f"{_AUTHZ}::TestAuthorizationIsCurrentAfterRevocation::"
        "test_a_packet_built_before_revocation_is_caught_by_the_audit_after_it",
        f"{_AUTHZ}::TestAuthorizationIsCurrentAfterRevocation::"
        "test_the_readout_records_the_grant_it_actually_used",
    ),
    _req(
        "A3",
        Status.PROVEN,
        f"{_AUTHZ}::TestPathsNeverMixAuthorizedAndUnauthorizedEntities::"
        "test_every_hop_endpoint_in_every_emitted_path_is_inside_the_grant",
        f"{_AUTHZ}::TestPathsNeverMixAuthorizedAndUnauthorizedEntities::"
        "test_the_builder_refuses_it_before_the_contract_ever_sees_it",
        f"{_AUTHZ}::TestPathsNeverMixAuthorizedAndUnauthorizedEntities::"
        "test_and_the_frozen_contract_refuses_the_same_shape_independently",
        notes=(
            "Two independent enforcers, both exercised. Guard injection "
            "established which one fires first.",
        ),
    ),
    _req(
        "A4",
        Status.PROVEN,
        f"{_AUTHZ}::TestCandidateSearchWithholdsBeforeRanking::"
        "test_the_analyst_cannot_find_it_by_label_id_or_partial_name",
        f"{_AUTHZ}::TestCohortConstructionWithholdsAndCounts::"
        "test_an_unauthorized_peer_is_withheld_and_counted",
        f"{_AUTHZ}::TestCohortConstructionWithholdsAndCounts::"
        "test_an_unauthorized_peer_reaches_the_EXCLUSION_list_too",
        f"{_AUTHZ}::TestCohortConstructionWithholdsAndCounts::"
        "test_an_unauthorized_ANCHOR_is_withheld_and_counted_separately",
        notes=(
            "Cohort construction authorizes at two sites; both are covered "
            "and both have their own guard-injection mutation.",
        ),
    ),
    _req(
        "A5",
        Status.PROVEN,
        f"{_AUTHZ}::TestEvidenceScopeConfusion::"
        "test_no_evidence_about_the_restricted_project_is_ever_indexed",
        f"{_AUTHZ}::TestEvidenceScopeConfusion::"
        "test_a_handle_minted_for_another_organization_does_not_verify",
    ),
    _req(
        "A6",
        Status.PROVEN,
        f"{_P3617}::TestNoPartitionParameterExists::"
        "test_no_public_callable_accepts_a_partition",
        f"{_P3617}::TestPublicEntryPointsRejectTheKeyword::"
        "test_the_partition_a_traversal_uses_comes_from_the_org_id",
        f"{_SEM}::TestNoGraphNativeSurfaceLeaves::"
        "test_the_storage_partition_name_never_appears_in_the_packet",
    ),
    _req(
        "A7",
        Status.PROVEN,
        f"{_AUTHZ}::TestNoAuthorizationIsInferredFromGraphMembership::"
        "test_the_restricted_project_is_ingested_into_the_analysts_own_partition",
        f"{_AUTHZ}::TestNoAuthorizationIsInferredFromGraphMembership::"
        "test_it_is_reachable_in_one_hop_from_an_entity_the_analyst_owns",
        f"{_AUTHZ}::TestNoAuthorizationIsInferredFromGraphMembership::"
        "test_the_grant_and_the_partition_disagree_by_exactly_the_restricted_project",
        f"{_AUTHZ}::TestAGrantDerivedFromTenancyLeaksIt::"
        "test_a_tenant_derived_grant_puts_it_in_front_of_the_analyst",
    ),
    _req(
        "A8",
        Status.PROVEN,
        f"{_AUTHZ}::TestNoOrganizationWideningAfterAnUnresolvedReference::"
        "test_an_unresolvable_reference_does_not_return_the_tenant",
        f"{_AUTHZ}::TestNoOrganizationWideningAfterAnUnresolvedReference::"
        "test_the_packet_commits_no_subject_and_names_no_entity",
    ),
    _req(
        "A9",
        Status.NOT_ACCEPTED,
        f"{_AUTHZ}::TestTheRestrictedProjectNeverReachesAConsumer::"
        "test_no_packet_the_analyst_can_produce_discloses_any_unauthorized_entity",
        f"{_AUTHZ}::TestTheIndependentOracleCannotYetScoreThisArm::"
        "test_the_audit_can_therefore_never_be_clean_for_this_arm",
        blocker="CHAOS-3627",
        reason=(
            "Zero leakage is MEASURED and HOLDS: across every project and "
            "team the analyst may see, no packet the arm can produce "
            "discloses any entity outside the true grant, and the same code "
            "path under a tenant-derived grant does leak, so the result is "
            "earned. The hard gate still cannot be signed off, because the "
            "oracle that owns this dimension -- audit_authorization -- "
            "cannot return clean for ANY graph-arm packet: the arm mints "
            "evidence handles with the platform signer (packet_builder.py:836) "
            "where the world mints its own (world.py:158), and the declared "
            "authorized set is widened with observation ids "
            "(packet_builder.py:890-892) that the oracle reads as entity "
            "claims. A gate whose oracle cannot pass is not a green gate."
        ),
    ),
    # ---- provenance ----------------------------------------------------
    _req(
        "P1",
        Status.DEFECT,
        f"{_PROV}::TestEveryAssertedDriverClosesToEvidenceInThisPacket::"
        "test_the_closure_check_REJECTS_a_driver_citing_unindexed_evidence",
        f"{_PROV}::TestRelationshipsDoNotCloseToEvidence::"
        "test_the_readout_carries_evidence_for_its_relationships",
        f"{_PROV}::TestRelationshipsDoNotCloseToEvidence::"
        "test_and_the_emitted_paths_carry_none",
        reason=(
            "The DRIVER half holds and is enforced by _assert_support_is_closed "
            "(packet_builder.py:379-447), shown rejecting an arm-shaped bad "
            "response. The RELATIONSHIP half does not: _lineage_path emits "
            "evidence_ref_ids=() as a literal (packet_builder.py:632) while "
            "the corpus edge's observation_ids reach the emitter via "
            "PathStep (corpus_adapter.py:195). No emitted relationship closes "
            "to evidence, and none can."
        ),
    ),
    _req(
        "P2",
        Status.PROVEN,
        f"{_PROV}::TestCanonicalIdsAndDirectionSurviveEmission::"
        "test_every_hop_endpoint_is_a_canonical_world_id",
        f"{_PROV}::TestCanonicalIdsAndDirectionSurviveEmission::"
        "test_every_hop_orientation_is_permitted_by_the_frozen_allowlist",
        f"{_PROV}::TestCanonicalIdsAndDirectionSurviveEmission::"
        "test_the_emitted_direction_agrees_with_the_world_that_produced_it",
        f"{_PROV}::TestCanonicalIdsAndDirectionSurviveEmission::"
        "test_the_contract_REFUSES_a_reversed_hop",
        notes=(
            "Direction is checked twice: against the allowlist, which "
            "constrains only the KINDS, and against the corpus edge that "
            "produced the hop, which is the only thing that knows the true "
            "orientation.",
        ),
    ),
    _req(
        "P3",
        Status.PROVEN,
        f"{_PROV}::TestTheThreeKindsOfClaimStayDistinguishable::"
        "test_no_structural_finding_claims_to_be_measured",
        f"{_PROV}::TestTheThreeKindsOfClaimStayDistinguishable::"
        "test_a_measurement_finding_can_never_be_asserted_as_a_driver",
        f"{_PROV}::TestTheThreeKindsOfClaimStayDistinguishable::"
        "test_an_uncomparable_measurement_is_excluded_for_insufficient_measurement",
    ),
    _req(
        "P4",
        Status.NOT_ACCEPTED,
        f"{_PROV}::TestConflictingAssertionsAreNotRetained::"
        "test_no_packet_retains_a_conflict",
        f"{_PROV}::TestConflictingAssertionsAreNotRetained::"
        "test_the_contract_can_express_a_conflict_even_though_the_arm_emits_none",
        blocker="CHAOS-3612",
        reason=(
            "BLOCKED BY CHAOS-3612 and never scored as a pass anywhere in "
            "this lane. The packet's conflicts tuple is an empty literal "
            "(packet_builder.py:1220), so no conflict is retained and none is "
            "chosen either. CHAOS-3612 records that the conflict case's "
            "ground truth and its authored sources cite disjoint evidence-id "
            "vocabularies, so no conflict-provenance expectation is "
            "satisfiable by any arm until they are reconciled. The frozen "
            "contract does carry the field, so CHAOS-3612 is the only "
            "blocker -- asserted rather than assumed."
        ),
    ),
    _req(
        "P5",
        Status.PROVEN,
        f"{_PROV}::TestInclusionIsAlwaysExplained::"
        "test_every_related_entity_states_why_it_is_there",
        f"{_PROV}::TestInclusionIsAlwaysExplained::"
        "test_every_path_states_why_it_is_there",
        f"{_PROV}::TestEveryAssertedDriverClosesToEvidenceInThisPacket::"
        "test_every_indexed_item_says_what_it_supports",
    ),
    _req(
        "P6",
        Status.DEFECT,
        f"{_ADV}::TestWithdrawnSourcesDoNotDisappear::"
        "test_withdrawn_evidence_reaches_the_emitted_packet",
        f"{_ADV}::TestWithdrawnSourcesDoNotDisappear::"
        "test_the_arm_has_no_concept_of_evidence_state_at_all",
        reason=(
            "REVOKED and DELETED evidence reaches the emitted packet: "
            "rv_vertex_revoked at proj_vertex and proj_meridian, "
            "wi_beacon_deleted at proj_acr, proj_beacon, proj_meridian and "
            "proj_pulse. Nothing in context_fabric reads evidence state -- "
            "the adapter carries it through as a display attribute "
            "(corpus_adapter.py:218) and no branch reads it back. Only "
            "wi_quarry_redacted stays out, and for the wrong reason: its "
            "subject is unauthorized, so the authorization filter removes it "
            "and redaction does no work anywhere. The check that would have "
            "caught this is authorization.py:278-279, dead for the reason "
            "recorded against A9 -- one defect was masking the other."
        ),
    ),
    _req(
        "P7",
        Status.PROVEN,
        f"{_PROV}::TestMultiSourceFactsRetainOnlyAuthorizedProvenance::"
        "test_evidence_indexed_for_the_analyst_is_only_about_entities_they_see",
        f"{_PROV}::TestMultiSourceFactsRetainOnlyAuthorizedProvenance::"
        "test_a_subject_whose_evidence_is_partly_restricted_still_gets_an_answer",
    ),
    # ---- adversarial ---------------------------------------------------
    _req(
        "X1",
        Status.PROVEN,
        f"{_ADV}::TestPromptInjectionNeverReachesAConsumer::"
        "test_the_injected_instruction_text_appears_nowhere_in_a_packet",
        f"{_ADV}::TestPromptInjectionNeverReachesAConsumer::"
        "test_an_injected_document_is_refused_even_when_its_trust_says_otherwise",
        f"{_ADV}::TestTheLoadBearingInjectionCase::"
        "test_its_payload_still_never_reaches_a_packet",
        f"{_ADV}::TestTheLoadBearingInjectionCase::"
        "test_because_nothing_reads_the_approved_set_at_all",
        notes=(
            "PROVEN, but read the reason before relying on it. The "
            "corpus-only result is weak: every corpus document is unapproved, "
            "so it measures the corpus rather than the arm. The load-bearing "
            "case -- a poisoned document that IS approved -- is built by the "
            "suite, and its payload still never reaches a packet. NOT because "
            "approval works: projection.approved_documents has ZERO consumers "
            "in src/, so containment today is 'no extraction pass exists'. "
            "Approval is a gate on a pass nobody built, and it becomes the "
            "only gate the moment one is. A structural test goes red when "
            "anything reads the approved set.",
            "Episodes are covered only insofar as the corpus models the "
            "adversarial one as an evidence record; the arm does not ingest "
            "WORLD_EPISODES as episodes today.",
        ),
    ),
    _req(
        "X2",
        Status.PROVEN,
        f"{_ADV}::TestKeywordStuffedBaitCannotBeRetrieved::"
        "test_no_query_the_bait_targets_returns_it_as_a_subject",
        f"{_ADV}::TestKeywordStuffedBaitCannotBeRetrieved::"
        "test_an_observation_is_not_searchable_as_a_subject_at_all",
        f"{_ADV}::TestKeywordStuffedBaitCannotBeRetrieved::"
        "test_the_bait_does_not_displace_the_relevant_evidence",
    ),
    _req(
        "X3",
        Status.PROVEN,
        f"{_ADV}::TestAPoisonedLinkageIsPresentAndRefused::"
        "test_the_corpus_plants_exactly_one_and_it_is_ingested",
        f"{_ADV}::TestAPoisonedLinkageIsPresentAndRefused::"
        "test_it_never_earns_driver_standing",
        f"{_ADV}::TestAPoisonedLinkageIsPresentAndRefused::"
        "test_the_same_edge_with_a_trusted_voucher_IS_asserted",
        notes=(
            "The positive control is the argument: a probe world identical "
            "except for the trust of the vouching record IS asserted, so the "
            "exclusion is the trust gate acting.",
        ),
    ),
    _req(
        "X4",
        Status.PROVEN,
        f"{_ADV}::TestAnAliasCannotRedirectASubject::"
        "test_a_planted_alias_matching_a_restricted_project_resolves_to_the_decoy_only",
        f"{_ADV}::TestAnAliasCannotRedirectASubject::"
        "test_the_committed_subject_rests_on_an_identifier_not_a_label",
    ),
    _req(
        "X5",
        Status.PROVEN,
        f"{_ADV}::TestTruncationIsDisclosedNotSilent::"
        "test_shorter_lineage_is_cited_before_longer_lineage",
        f"{_ADV}::TestKeywordStuffedBaitCannotBeRetrieved::"
        "test_the_bait_never_supports_an_asserted_driver",
    ),
    _req(
        "X6",
        Status.PROVEN,
        f"{_ADV}::TestTruncationIsDisclosedNotSilent::"
        "test_a_path_budget_that_bites_is_reported_with_its_own_reason",
        f"{_ADV}::TestTruncationIsDisclosedNotSilent::"
        "test_the_packet_carries_the_truncation_forward",
        f"{_ADV}::TestTruncationIsDisclosedNotSilent::"
        "test_a_truncation_flag_without_a_reason_is_refused",
    ),
    _req(
        "X7",
        Status.PROVEN,
        f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
        "test_every_lineage_path_reports_the_stale_source_state",
        f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
        "test_the_packet_names_how_far_behind_the_index_is",
        f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
        "test_a_current_index_makes_no_staleness_claim",
        f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
        "test_a_never_projected_index_is_unavailable_rather_than_empty",
        notes=(
            "A residual is recorded alongside: the watermark is a "
            "build_packet argument never reconciled against the readout "
            "(packet_builder.py:642), and the write path derives "
            "indexed_through from source-controlled observed_at "
            "(store.py:211-214).",
        ),
    ),
    _req(
        "X8",
        Status.PROVEN,
        f"{_ADV}::TestADegradedBackendIsLoudNotEmpty::"
        "test_an_unreachable_store_raises_rather_than_returning_nothing",
        f"{_ADV}::TestADegradedBackendIsLoudNotEmpty::"
        "test_an_empty_store_is_distinguishable_from_a_broken_one",
    ),
    _req(
        "X9",
        Status.PROVEN,
        f"{_ADV}::TestADegradedBackendIsLoudNotEmpty::"
        "test_the_extraction_dependency_is_unavailable_by_name",
        f"{_ADV}::TestPromptInjectionNeverReachesAConsumer::"
        "test_an_unknown_trust_level_raises_rather_than_defaulting",
        notes=(
            "Policy-forbidden mode is the unapproved-document path: approval "
            "is what points a model at text, and no corpus document is "
            "approved.",
        ),
    ),
    _req(
        "X10",
        Status.PROVEN,
        f"{_ADV}::TestADegradedBackendIsLoudNotEmpty::"
        "test_a_response_of_the_wrong_shape_raises",
        f"{_ADV}::TestADegradedBackendIsLoudNotEmpty::"
        "test_a_row_missing_a_declared_column_raises",
        f"{_ADV}::TestADegradedBackendIsLoudNotEmpty::"
        "test_a_stored_prose_fact_is_refused_by_name",
    ),
    _req(
        "X11",
        Status.PROVEN,
        f"{_ADV}::TestPersonLevelBaitIsRefusedNotRanked::"
        "test_the_contract_has_no_person_subject_kind",
        f"{_ADV}::TestPersonLevelBaitIsRefusedNotRanked::"
        "test_no_person_query_resolves_to_a_subject",
        f"{_ADV}::TestPersonLevelBaitIsRefusedNotRanked::"
        "test_person_counting_metrics_are_refused_as_driver_material",
    ),
    # ---- semantic safety -----------------------------------------------
    _req(
        "S1",
        Status.PROVEN,
        f"{_SEM}::TestNoCanonicalTruthIsCreated::"
        "test_no_staffing_or_capacity_claim_is_ever_asserted",
        f"{_SEM}::TestNoCanonicalTruthIsCreated::"
        "test_no_measurement_only_category_reaches_asserted_standing",
        f"{_SEM}::TestNoCanonicalTruthIsCreated::"
        "test_a_measured_value_is_cited_verbatim_and_never_recomputed",
        f"{_SEM}::TestNoCanonicalTruthIsCreated::"
        "test_a_declared_status_is_reported_as_a_symptom_not_a_completion_verdict",
    ),
    _req(
        "S2",
        Status.PROVEN,
        f"{_SEM}::TestAMissingDenominatorDisclosesRatherThanRefuses::"
        "test_that_project_still_receives_an_investigation",
        f"{_SEM}::TestAMissingDenominatorDisclosesRatherThanRefuses::"
        "test_the_absence_is_surfaced_rather_than_swallowed",
        f"{_SEM}::TestAMissingDenominatorDisclosesRatherThanRefuses::"
        "test_an_uncomparable_measurement_says_why_rather_than_disappearing",
    ),
    _req(
        "S3",
        Status.PROVEN,
        f"{_SEM}::TestSymptomsAreNotPromotedWithoutLineage::"
        "test_no_symptom_anywhere_reaches_asserted_standing",
        f"{_ADV}::TestPersonLevelBaitIsRefusedNotRanked::"
        "test_no_packet_names_a_person_shaped_subject",
    ),
    _req(
        "S4",
        Status.PROVEN,
        f"{_SEM}::TestSymptomsAreNotPromotedWithoutLineage::"
        "test_no_symptom_anywhere_reaches_asserted_standing",
        f"{_SEM}::TestSymptomsAreNotPromotedWithoutLineage::"
        "test_symptoms_are_actually_produced",
        f"{_SEM}::TestSymptomsAreNotPromotedWithoutLineage::"
        "test_at_most_one_principal_driver_per_investigation",
        f"{_SEM}::TestSymptomsAreNotPromotedWithoutLineage::"
        "test_a_principal_driver_is_produced_somewhere",
    ),
    _req(
        "S5",
        Status.DEFECT,
        f"{_PROV}::TestHistoricalRelationshipsAreEmittedAsCurrent::"
        "test_the_traversal_knows_the_relationship_has_ended",
        f"{_PROV}::TestHistoricalRelationshipsAreEmittedAsCurrent::"
        "test_the_driver_layer_correctly_refuses_to_assert_on_it",
        f"{_PROV}::TestHistoricalRelationshipsAreEmittedAsCurrent::"
        "test_but_the_emitted_lineage_calls_it_current",
        reason=(
            "The corpus's closed dependency -- proj_pulse depends_on "
            "dep_ratelimitd, valid_to 2026-06-12, two months before "
            "TRIAL_NOW -- is emitted with relevance=current on both the hop "
            "and the path. The arm KNOWS: PathStep.is_current_at returns "
            "False (readback.py:157-169) and discover_drivers correctly "
            "excludes the driver built on it with not_currently_relevant. "
            "relevance is a literal RelevanceState.CURRENT at eight emitter "
            "sites (packet_builder.py 542, 618, 630, 751, 799, 868, 935, "
            "982) and nothing computes it, so the frozen scoring dimension "
            "current_relevance is measuring a constant."
        ),
    ),
    _req(
        "S6",
        Status.PROVEN,
        f"{_SEM}::TestNoGraphNativeSurfaceLeaves::"
        "test_no_backend_vocabulary_appears_in_the_packet",
        f"{_SEM}::TestNoGraphNativeSurfaceLeaves::"
        "test_the_storage_partition_name_never_appears_in_the_packet",
        f"{_SEM}::TestNoGraphNativeSurfaceLeaves::"
        "test_the_shadow_record_carries_no_backend_vocabulary_either",
        f"{_SEM}::TestNoGraphNativeSurfaceLeaves::"
        "test_the_seam_imports_no_arm_and_reads_arm_identity_off_the_packet",
        f"{_C3617}::TestNoProductionCoupling::"
        "test_the_arm_exposes_no_router_task_or_tool_registration",
        f"{_C3617}::TestNoProductionCoupling::test_no_declared_query_writes_or_maintains",
    ),
    # ---- cross-runtime differential ------------------------------------
    _req(
        "D1",
        Status.UNMEASURED,
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_a_real_graph_arm_packet_is_recorded_at_all",
        blocker="CHAOS-3619",
        reason=(
            "DEFERRED BY INSTRUCTION and not started. The differential leg "
            "needs CHAOS-3619's trial runner to exist; building a parallel "
            "runner in this lane was explicitly forbidden and would produce "
            "a second definition of what a run IS, which is the drift "
            "CHAOS-3396 exists to prevent. What this lane establishes "
            "meanwhile is the boundary the differential will run through: a "
            "real graph-arm packet is accepted by the real shadow seam and "
            "produces a content-safe record. The remaining runtimes -- ACR "
            "and acr-mcp -- carry no graph surface at all today, which is "
            "asserted under S6 rather than assumed."
        ),
    ),
    # ---- observability -------------------------------------------------
    _req(
        "O1",
        Status.PROVEN,
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_the_record_carries_latency_versions_outcome_and_counts",
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_every_fallback_has_a_named_status_rather_than_a_silent_drop",
    ),
    _req(
        "O2",
        Status.PROVEN,
        f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
        "test_the_packet_names_how_far_behind_the_index_is",
        notes=(
            "Observable on the PACKET. It does not reach the shadow record, "
            "which carries no freshness field.",
        ),
    ),
    _req(
        "O3",
        Status.PROVEN,
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_the_record_carries_latency_versions_outcome_and_counts",
    ),
    _req(
        "O4",
        Status.UNMEASURED,
        f"{_SEM}::TestTheObservabilityGapsAreNamed::"
        "test_the_shadow_record_carries_no_authorization_filtered_count",
        f"{_AUTHZ}::TestTheAuthorizationFilteredCountIsPartlyReal::"
        "test_the_two_hardcoded_zeros_are_recorded_as_a_gap_not_a_result",
        reason=(
            "Partly real and partly absent, and the exact shape matters more "
            "than the summary. The traversal count (readback.py:654) and the "
            "cohort count (cohort.py:315) are real and reach the packet's "
            "subject_discovery and comparison_cohort sections. "
            "related_context.authorization_filtered_count "
            "(packet_builder.py:1145) and "
            "evidence_coverage.authorization_filtered_count "
            "(packet_builder.py:1224) are literal zeros on a run that "
            "demonstrably filtered one entity -- and the first of those is "
            "the field the frozen scoring registry names as evidence for "
            "zero_unauthorized_results (scoring.py:660-676). Nothing "
            "authorization-shaped reaches the shadow record at all, so an "
            "operator watching the trial cannot see that an answer was "
            "narrowed."
        ),
    ),
    _req(
        "O5",
        Status.UNMEASURED,
        f"{_PROV}::TestEveryAssertedDriverClosesToEvidenceInThisPacket::"
        "test_the_closure_check_REJECTS_a_driver_citing_unindexed_evidence",
        reason=(
            "There is no provenance-closure FAILURE COUNTER because a "
            "closure failure is not a counted outcome: _assert_support_is_closed "
            "and the handle lookup raise (packet_builder.py:379-447, :510-516), "
            "which aborts emission. A run that hits one produces no packet "
            "and therefore no shadow record to count. Recorded as unmeasured "
            "rather than proven: an operator would see the absence of a "
            "record, not a closure-failure signal."
        ),
    ),
    _req(
        "O6",
        Status.PROVEN,
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_every_fallback_has_a_named_status_rather_than_a_silent_drop",
        f"{_ADV}::TestTruncationIsDisclosedNotSilent::"
        "test_the_packet_carries_the_truncation_forward",
        f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
        "test_a_never_projected_index_is_unavailable_rather_than_empty",
    ),
    _req(
        "O7",
        Status.UNMEASURED,
        f"{_ADV}::TestPromptInjectionNeverReachesAConsumer::"
        "test_no_document_in_the_world_is_approved_for_extraction",
        reason=(
            "Nothing to observe yet, and that is the honest answer rather "
            "than a proxy. The structured projection path makes no model "
            "call at all -- no corpus document is approved for extraction -- "
            "so there is no provider, model, token count or cost to record. "
            "The embedder IS recorded, in projection_version. A cost signal "
            "invented now would be a counter that only ever reads zero and "
            "would train a reader to ignore it."
        ),
    ),
    _req(
        "O8",
        Status.UNMEASURED,
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_the_record_carries_latency_versions_outcome_and_counts",
        reason=(
            "The shadow record carries the packet schema version and the "
            "projection version (which names the embedder), but no backend "
            "or Graphiti version. The graph-trial store is pinned in "
            "pyproject and compose rather than reported at run time, so a "
            "trial artifact cannot currently attribute a result to the "
            "backend build that produced it."
        ),
    ),
    _req(
        "O9",
        Status.UNMEASURED,
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_a_real_graph_arm_packet_is_recorded_at_all",
        blocker="CHAOS-3619",
        reason=(
            "Native-versus-graph outcome differences need both arms run over "
            "the same cases, which is CHAOS-3619's runner. The record shape "
            "that would carry the comparison exists and is content-safe; "
            "what does not exist is the paired run. Same deferral as D1."
        ),
    ),
    _req(
        "O10",
        Status.PROVEN,
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_no_entity_display_label_reaches_the_record",
        f"{_SEM}::TestTheTelemetryIsContentSafe::"
        "test_no_question_text_or_source_prose_reaches_the_record",
        f"{_SEM}::TestNoGraphNativeSurfaceLeaves::"
        "test_the_shadow_record_carries_no_backend_vocabulary_either",
    ),
)


class Transfer(StrEnum):
    """Whether a CHAOS-3617 result carries to the corpus world under true grants.

    The orchestrator's caveat, made checkable: where a 3617 proof ran only on
    the synthetic ``alpha``/``beta`` fixtures — whose authorized set is a
    hand-written tuple — "proven" does not automatically transfer to the
    corpus world under real per-principal grants. This lane relies on several
    3617 invariants instead of re-proving them, so each reliance carries a
    disposition rather than an assumption.
    """

    #: Re-proved on the corpus world by this lane. The strongest disposition.
    RE_PROVEN = "re_proven"
    #: Structural — a property of the arm's API surface, its imports or its
    #: declared query set, with no dependence on which world is loaded. A
    #: transfer question does not arise.
    WORLD_INDEPENDENT = "world_independent"
    #: Proved on the synthetic fixtures and NOT exercised on the corpus,
    #: with a stated reason why the corpus cannot reach it. Relied on with
    #: that limitation visible.
    SYNTHETIC_ONLY = "synthetic_only"


@dataclass(frozen=True)
class InheritedInvariant:
    invariant: str
    transfer: Transfer
    #: A CHAOS-3617 test that established it, or a CHAOS-3620 test that
    #: re-established it on the corpus. Resolved like every other node id.
    evidence: tuple[str, ...]
    reason: str = ""


#: Every CHAOS-3617 result this lane leans on instead of re-proving from
#: scratch. Short by design: if it is not here, this lane proved it itself.
INHERITED_INVARIANTS: tuple[InheritedInvariant, ...] = (
    InheritedInvariant(
        "authorized traversal never routes through an unauthorized entity",
        Transfer.RE_PROVEN,
        (
            f"{_AUTHZ}::TestPathsNeverMixAuthorizedAndUnauthorizedEntities::"
            "test_every_hop_endpoint_in_every_emitted_path_is_inside_the_grant",
        ),
        reason=(
            "Re-proved on the corpus world under the analyst's true grant, "
            "which is the case the synthetic fixtures cannot express: their "
            "restricted entity is excluded by a hand-written tuple, the "
            "corpus's is excluded by a per-principal grant while sharing the "
            "caller's tenant."
        ),
    ),
    InheritedInvariant(
        "cross-tenant near-duplicates stay distinct and unreachable",
        Transfer.RE_PROVEN,
        (
            f"{_ADV}::TestAnAliasCannotRedirectASubject::"
            "test_a_shared_acronym_across_tenants_never_crosses_the_partition",
        ),
        reason=(
            "Re-proved with the corpus's own cross-tenant collision: the "
            "acronym ACR resolves in both Helio and Lumen, and a Helio search "
            "returns only the Helio project."
        ),
    ),
    InheritedInvariant(
        "budget truncation is bounded and disclosed with a per-flag reason",
        Transfer.RE_PROVEN,
        (
            f"{_ADV}::TestTruncationIsDisclosedNotSilent::"
            "test_a_path_budget_that_bites_is_reported_with_its_own_reason",
            f"{_ADV}::TestTruncationIsDisclosedNotSilent::"
            "test_the_packet_carries_the_truncation_forward",
        ),
        reason=(
            "Re-proved on the corpus world with a lowered path budget, so the "
            "disclosure is observed on a graph dense enough to truncate "
            "rather than on a fixture sized to fit."
        ),
    ),
    InheritedInvariant(
        "the watermark's freshness state reaches every consumer surface",
        Transfer.RE_PROVEN,
        (
            f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
            "test_every_lineage_path_reports_the_stale_source_state",
            f"{_ADV}::TestAStaleIndexIsDisclosedEverywhere::"
            "test_a_current_index_makes_no_staleness_claim",
        ),
        reason=(
            "Re-proved on the corpus world in both directions, including the "
            "negative control: an unconditional staleness disclosure would "
            "carry no information."
        ),
    ),
    InheritedInvariant(
        "no caller-supplied partition, and the partition is server-derived",
        Transfer.WORLD_INDEPENDENT,
        (
            f"{_P3617}::TestNoPartitionParameterExists::"
            "test_no_public_callable_accepts_a_partition",
            f"{_SEM}::TestNoGraphNativeSurfaceLeaves::"
            "test_the_storage_partition_name_never_appears_in_the_packet",
        ),
        reason=(
            "The 3617 half inspects the arm's public callables' signatures, "
            "which no world can change. This lane adds the emitted-output "
            "half on the corpus: the derived partition string reaches no "
            "packet."
        ),
    ),
    InheritedInvariant(
        "the arm registers no router, task, tool or telemetry surface",
        Transfer.WORLD_INDEPENDENT,
        (
            f"{_C3617}::TestNoProductionCoupling::"
            "test_the_arm_exposes_no_router_task_or_tool_registration",
            f"{_C3617}::TestNoProductionCoupling::"
            "test_no_declared_query_writes_or_maintains",
        ),
        reason=(
            "Import-graph and declared-query-set properties. Which world is "
            "loaded cannot add a route or a write."
        ),
    ),
    InheritedInvariant(
        "a semantic match claim requires an attested semantic embedder",
        Transfer.SYNTHETIC_ONLY,
        (
            "tests/context_fabric/test_chaos_3617_semantic_claims.py::"
            "TestSemanticClaimsAreRefusedUnderANonSemanticEmbedder::"
            "test_an_embedding_derived_match_is_refused",
        ),
        reason=(
            "NOT exercised on the corpus world, and the reason is structural: "
            "the corpus path runs the DeterministicEmbedder, "
            "ProjectionGraphReader attests no embedder "
            "(readout.embedder_model_id is None), and every corpus subject "
            "resolves by EXACT_CANONICAL_ID. No semantic claim is ever made, "
            "so the guard that refuses one is inert on this path. Relied on "
            "as a 3617 result with that limitation stated: if a later "
            "revision resolves corpus subjects by similarity, the transfer "
            "question becomes live and this entry must be re-derived."
        ),
    ),
)


#: Statuses that must carry a stated reason. ``PROVEN`` is the only status
#: that speaks for itself; everything else is a claim about why something was
#: not established, and an unexplained one is indistinguishable from an
#: oversight.
NEEDS_REASON = frozenset({Status.DEFECT, Status.NOT_ACCEPTED, Status.UNMEASURED})

#: Statuses that must name who owns the unblocking. ``DEFECT`` is excluded:
#: the defect is in this repository's own merged code and the reason carries
#: file:line, which is a stronger pointer than a ticket id.
NEEDS_BLOCKER = frozenset({Status.NOT_ACCEPTED})

_BLOCKER_PATTERN = re.compile(r"^CHAOS-\d+$")


def render() -> str:
    """The disposition table, as Markdown, from the same data the tests check."""

    rows = [
        "| Requirement | Status | Blocker | Requirement text (CHAOS-3620) |",
        "| --- | --- | --- | --- |",
    ]
    for requirement in REQUIREMENTS:
        rows.append(
            f"| `{requirement.requirement_id}` "
            f"| {requirement.status.value} "
            f"| {requirement.blocker or '—'} "
            f"| {requirement.issue_text} |"
        )
    detail = ["", "## Why each non-proven requirement is not proven", ""]
    for requirement in REQUIREMENTS:
        if requirement.status is Status.PROVEN:
            continue
        detail.append(
            f"### `{requirement.requirement_id}` — {requirement.status.value}"
            + (f" (blocked by {requirement.blocker})" if requirement.blocker else "")
        )
        detail.append("")
        detail.append(requirement.reason)
        detail.append("")
    return "\n".join(rows + detail)
