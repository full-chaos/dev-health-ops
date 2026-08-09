#!/usr/bin/env python3
"""CHAOS-3617: prove each guard's test actually catches the defect it names.

RED-first evidence, the CHAOS-3615 house way: for every guard the arm relies
on, disable *that guard alone* by an exact source substitution, run the tests
that claim to cover it, and require them to FAIL. Then restore and require
them to PASS again.

Three rules this harness follows, learned the hard way and worth stating
because a mutation harness that breaks any of them reports coverage it does
not have:

1. **A mutation that does not apply is INVALID, not KILLED.** If the anchor
   text is not found verbatim, the run aborts naming the mutation — an
   anchor that silently stopped matching after a refactor would otherwise
   turn into a permanently green "the guard is proven" line.
2. **The restore is verified by re-running the tests, never by a git check.**
   A disabled guard still compiles and ``git diff`` calls a restored file
   clean whatever it contains. Only green tests prove the guard is back.
3. **Where the mutation died is recorded**, so a reader can check that the
   failure is the one the guard exists to prevent rather than an unrelated
   collapse somewhere in setup.

One residual shape in the region check, found by adversarial verification and
**unexploited**: the ``FAILED <nodeid>`` line is inside the region, so a token
that happened to be a substring of a mutation's own test node id would be
credited without any assertion carrying it — audited across all mutations,
zero do, and ``test_no_expected_reason_hides_inside_its_own_node_id`` keeps it
that way.

Usage::

    uv run python scripts/chaos_3617_guard_injection.py            # all
    uv run python scripts/chaos_3617_guard_injection.py --only ID  # one
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src" / "dev_health_ops" / "context_fabric" / "graph_arm"
TESTS = "tests/context_fabric"


@dataclasses.dataclass(frozen=True)
class Mutation:
    """One guard, disabled by an exact substitution."""

    mutation_id: str
    #: What the guard prevents, in the words of the issue or the fault-mode
    #: registry. This is what the recorded RED line has to be checked against.
    defect: str
    path: Path
    anchor: str
    replacement: str
    tests: tuple[str, ...]
    #: The failure the disabled guard must produce, as a substring of pytest's
    #: FAILED/error line. Required, and it is the whole point: a mutation that
    #: merely makes the suite red has proved nothing about the guard it names.
    #:
    #: Adversarial review caught exactly that here. Disabling the prose-fact
    #: guard with ``if False and match is None`` let ``None`` reach
    #: ``match.group(...)``, so the test failed with ``AttributeError`` from a
    #: downstream dereference rather than because prose was accepted -- and
    #: the harness reported KILLED. Recording *where* a mutation died was
    #: never enough; the category has to be checked against the claim.
    expect_failure: str


MUTATIONS: tuple[Mutation, ...] = (
    Mutation(
        mutation_id="reversed-relationship-accepted",
        defect=(
            "a reversed relationship record is stored, so lineage reads "
            "plausibly and points the wrong way"
        ),
        path=SRC / "projection.py",
        anchor="    if not orientation.permits(source_kind, target_kind):",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestDirectionSurvivesTheRoundTrip::"
            "test_a_reversed_relationship_record_is_refused_at_ingestion",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="cross-tenant-records-accepted",
        defect=(
            "a record belonging to another organization is written into this "
            "organization's partition"
        ),
        path=SRC / "records.py",
        anchor="    if foreign:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestIngestionRefusals::test_a_foreign_record_never_reaches_the_store",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="org-dropped-from-node-identity",
        defect=(
            "two organizations holding the same canonical id collide onto one "
            "node, sharing data no downstream filter could unshare"
        ),
        path=SRC / "identity.py",
        anchor='    name = "\\0".join((org_id, discriminator, kind, canonical_id))',
        replacement='    name = "\\0".join((discriminator, kind, canonical_id))',
        tests=(f"{TESTS}/test_chaos_3617_identity.py::TestTenantScopedAddressing",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="unauthorized-entity-traversed",
        defect=(
            "a path routes through a restricted entity, disclosing that it "
            "exists and links two things the caller can see"
        ),
        path=SRC / "readback.py",
        anchor="            if other not in authorized:",
        replacement="            if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_authorization.py::TestAuthorizationFiltering",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="partition-trusted-not-rederived",
        defect=(
            "a caller-supplied graph partition is accepted as an authorization claim"
        ),
        path=SRC / "identity.py",
        anchor="    if partition != expected:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_identity.py::TestServerDerivedPartition::"
            "test_a_partition_from_another_org_is_rejected",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="prose-allowed-in-structured-attributes",
        defect=(
            "a structured record is converted into hand-authored prose via an "
            "attribute value"
        ),
        path=SRC / "projection.py",
        anchor="        if isinstance(value, str) and len(value) > MAX_ATTRIBUTE_CHARS:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestStructuredRecordsAreNotProse::"
            "test_an_attribute_cannot_hold_a_sentence",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="prose-fact-accepted-on-read",
        defect="a stored prose fact is presented to a consumer as evidence",
        path=SRC / "backend.py",
        # The disabled guard must PARSE the prose, not crash on it. The
        # earlier version of this mutation only skipped the raise, so `None`
        # reached `match.group(...)` and the test failed with AttributeError
        # -- red for a reason that had nothing to do with prose being
        # accepted. Adversarial review caught it; `expect_failure` below now
        # makes that class of miss impossible.
        anchor=(
            "    match = TRIPLE_FACT_PATTERN.fullmatch(fact)\n    if match is None:"
        ),
        replacement=(
            "    match = TRIPLE_FACT_PATTERN.fullmatch(fact)\n"
            "    if match is None:\n"
            "        parts = fact.split()\n"
            "        return (parts[0], RelationshipType.REFERENCES, parts[-1])\n"
            "    if False:"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestStructuredRecordsAreNotProse::test_a_prose_fact_is_rejected_on_read",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="unapproved-document-reaches-extraction",
        defect=("model extraction is pointed at unstructured material nobody approved"),
        path=SRC / "projection.py",
        anchor="    approved = tuple(document for document in batch.documents if document.approved)",
        replacement="    approved = tuple(batch.documents)",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestStructuredRecordsAreNotProse::"
            "test_unapproved_documents_never_reach_extraction",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="projection-flag-ignored",
        defect="the shadow arm writes to the trial store with its flag off",
        path=SRC / "store.py",
        anchor="        if not graph_projection_enabled():",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestIndexingFailureAndFallback::"
            "test_a_write_with_the_projection_flag_off_is_refused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="never-projected-store-reports-fresh",
        defect=(
            "an empty store answers as though it were current, so a packet "
            "built on nothing looks complete"
        ),
        path=SRC / "watermark.py",
        anchor="        if self.indexed_through is None:\n            return SourceRequirementState.UNAVAILABLE",
        replacement="        if self.indexed_through is None:\n            return SourceRequirementState.AVAILABLE_CURRENT",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestWatermark::"
            "test_a_never_projected_store_is_unavailable_not_fresh",
            f"{TESTS}/test_chaos_3617_packet_contract.py::"
            "TestSourceHealthAndFreshness::"
            "test_a_never_projected_store_reports_unavailable_not_empty",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="traversal-order-dependent",
        defect=(
            "which explanatory paths survive the per-entity cap depends on "
            "edge arrival order, so a recorded trial run is irreproducible "
            "(the defect the live differential oracle actually found)"
        ),
        path=SRC / "readback.py",
        anchor=(
            "    return tuple(\n"
            "        sorted(\n"
            "            adjacency.edges.get(canonical_id, ()),\n"
            "            key=lambda edge: (\n"
            "                edge.relationship.value,\n"
            "                edge.other_canonical_id,\n"
            "                edge.direction.value,\n"
            "            ),\n"
            "        )\n"
            "    )"
        ),
        replacement="    return tuple(adjacency.edges.get(canonical_id, ()))",
        tests=(f"{TESTS}/test_chaos_3617_determinism.py::TestOrderIndependence",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="builder-trusts-the-readouts-authorization-claim",
        defect=(
            "the arm stops refusing an unauthorized hop endpoint itself and "
            "leaves it to the frozen contract. NOTE the honest scope, found "
            "by the failure-category check: with this guard disabled the "
            "packet is still refused -- by RelatedContext's own "
            "validate_paths_stay_inside_authorized_set. So this mutation "
            "proves the arm refuses EARLIER and with a typed PermissionError "
            "naming the endpoint, not that the endpoint would otherwise "
            "reach a consumer. Two independent refusals is the actual state, "
            "and claiming more would be the over-claim this harness exists "
            "to catch"
        ),
        path=SRC / "packet_builder.py",
        anchor="                if endpoint not in authorized_entity_ids:",
        replacement="                if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_authorization.py::TestAuthorizationFiltering::"
            "test_the_builder_refuses_a_readout_whose_paths_escape_the_set",
        ),
        expect_failure="ValidationError",
    ),
    Mutation(
        mutation_id="graphiti-telemetry-left-on",
        defect=(
            "the trial phones one organization's structure home to a "
            "third-party analytics host"
        ),
        path=SRC / "backend.py",
        anchor='    os.environ[TELEMETRY_ENV_VAR] = "false"',
        replacement="    pass",
        tests=(
            f"{TESTS}/test_chaos_3617_containment.py::TestTelemetryContainment::"
            "test_telemetry_is_forced_off",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="traversal-work-is-unbounded",
        defect=(
            "path enumeration expands without limit in a dense neighbourhood, "
            "reaching no new entity while doing unbounded work"
        ),
        path=SRC / "readback.py",
        anchor="        if not work_outcome.within_budget:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_node_visit_budget_bounds_work_not_only_results",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="traversal-has-no-wall-clock-backstop",
        defect=(
            "a traversal shape a work count cannot predict runs past its time "
            "budget with nothing to stop it"
        ),
        path=SRC / "readback.py",
        anchor="        if not elapsed_outcome.within_budget:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_wall_clock_budget_bounds_the_traversal",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="packet-byte-budget-not-applied",
        defect="a packet larger than any declared bound reaches a consumer",
        path=SRC / "packet_builder.py",
        anchor="    if not outcome.within_budget:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::"
            "TestPacketByteBudget::"
            "test_a_packet_over_the_byte_budget_is_refused_not_trimmed",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="preview-creates-the-keyspace-it-previewed",
        defect=(
            "a dry-run org deletion constructs a store and thereby creates an "
            "empty keyspace for every organization it previewed"
        ),
        path=SRC / "store.py",
        anchor="    if not exists:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestDeterministicCleanup::"
            "test_previewing_an_absent_organization_constructs_no_store",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="separator-bytes-accepted-into-joined-attributes",
        defect=(
            "a source value containing the storage join byte is stored and "
            "comes back split into several, inventing an alias no source "
            "supplied -- which a later alias search would then match"
        ),
        path=SRC / "projection.py",
        anchor="        if separator in value:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestSeparatorBytesAreRefused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="default-read-path-truncates-silently",
        defect=(
            "the walk stops at the hop ceiling with edges still unexplored "
            "and reports complete -- reachable authorized entities missing, "
            "every flag False, on the path nobody configures"
        ),
        path=SRC / "readback.py",
        anchor="    if declined_with_edges_remaining:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_default_read_path_discloses_when_it_stops_early",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="one-shared-truncation-reason-misattributes",
        defect=(
            "path truncation and evidence truncation collapse onto one "
            "reason, so a consumer asking why lineage is partial is told "
            "evidence_budget"
        ),
        path=SRC / "readback.py",
        anchor="        evidence_reason = evidence_outcome.truncation_reason",
        replacement="        paths_reason = evidence_outcome.truncation_reason\n        evidence_reason = evidence_outcome.truncation_reason",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_evidence_budget_sets_the_evidence_flag_not_the_path_flag",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="packet-sections-share-one-truncation-reason",
        defect=(
            "the packet's evidence section reports the path bound's reason "
            "when both fire -- the defect the readout-only fix MOVED rather "
            "than killed, because both sections kept reading the first-wins "
            "convenience property"
        ),
        path=SRC / "packet_builder.py",
        anchor="        truncation_reason=readout.evidence_truncation_reason,",
        replacement="        truncation_reason=readout.truncation_reason,",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_each_flag_carries_its_own_reason_when_two_bounds_fire",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="control-characters-accepted",
        defect=(
            "a NUL is silently dropped by the live store (so the readers "
            "disagree about what the source supplied) and defeats the "
            "NUL-separated hash inputs identity.py relies on to keep two "
            "relationships from sharing one address"
        ),
        path=SRC / "projection.py",
        anchor="    found = sorted(_CONTROL_CHARACTERS & set(value))",
        replacement="    found = []",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestSeparatorBytesAreRefused::test_a_control_character_is_refused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="entity-flag-over-reports-on-a-diamond",
        defect=(
            "the entity-truncation flag fires whenever an edge was left "
            "unfollowed, even when every entity was returned by another "
            "branch -- over-reporting reads as noise as fast as silence"
        ),
        path=SRC / "readback.py",
        anchor="                if any(other not in reached for other in unfollowed):",
        replacement="                if True:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_a_diamond_does_not_claim_missing_entities",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="semantic-claim-allowed-under-a-hash-embedder",
        defect=(
            "a match produced by similarity over non-semantic hash vectors is "
            "emitted as a real subject match, and scores as a retrieval "
            "capability the arm does not have"
        ),
        path=SRC / "packet_builder.py",
        anchor=(
            "    if embedder.semantic and attested_embedder is not None:\n"
            "        return"
        ),
        replacement="    if True:\n        return",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::"
            "TestSemanticClaimsAreRefusedUnderANonSemanticEmbedder",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="embedding-budget-not-checked-before-spending",
        defect=(
            "a projection spends unbounded embedding calls, and an "
            "over-budget run pays for most of them before stopping"
        ),
        path=SRC / "store.py",
        anchor="            if not outcome.within_budget:",
        replacement="            if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::TestEmbeddingBudget::"
            "test_a_projection_over_the_embedding_budget_is_refused_before_any_call",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="projection-version-omits-the-embedder",
        defect=(
            "two runs embedded with different models report the same "
            "projection version, making incomparable results look comparable"
        ),
        path=SRC / "packet_builder.py",
        anchor=(
            "        projection_version=(\n"
            "            f\"{PROJECTION_VERSION.removesuffix('.v1')}.\"\n"
            '            f"{embedder_projection_suffix(active_embedder)}.v1"\n'
            "        ),"
        ),
        replacement="        projection_version=PROJECTION_VERSION,",
        tests=(
            f"{TESTS}/test_chaos_3617_packet_contract.py::"
            "TestReproducibilityMetadata::"
            "test_the_projection_version_names_the_embedder_that_produced_it",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="cloud-embedder-degrades-silently-without-a-key",
        defect=(
            "a run with no API key silently uses hash vectors while every "
            "artifact says it was semantic"
        ),
        path=SRC / "backend.py",
        anchor="        if not key:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::TestEmbedderContracts::"
            "test_the_cloud_embedder_refuses_to_degrade_silently",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="candidate-search-ranks-a-withheld-entity",
        defect=(
            "a restricted entity that matches the query is filtered AFTER "
            "ranking, so its position leaks even though its record is never "
            "returned -- and the corpus's restricted project is same-tenant, "
            "so no tenant check catches it"
        ),
        path=SRC / "discovery.py",
        anchor="        if node.canonical_id not in authorized:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_candidate_search.py::"
            "TestAuthorizationBoundsTheSearch",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="candidate-ranking-depends-on-node-order",
        defect=(
            "the ranking falls back on iteration order, so the same world and "
            "the same query rank differently between runs and a recorded "
            "trial result cannot be reproduced"
        ),
        path=SRC / "discovery.py",
        anchor="    ranked = sorted(matches.values(), key=lambda item: item.rank_key)",
        replacement="    ranked = list(matches.values())",
        tests=(
            f"{TESTS}/test_chaos_3617_candidate_search.py::"
            "TestRankingIsTotalAndContentDerived::"
            "test_shuffling_the_node_order_does_not_change_the_ranking",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="fuzzy-match-accepts-an-infix",
        defect=(
            "fuzzy matching becomes substring containment, so 'acr' matches "
            "'sacred' and a wrong-but-confident subject reaches rank"
        ),
        path=SRC / "discovery.py",
        anchor="    return any(query_tokens <= set(text.split()) for text in haystacks)",
        replacement="    return any(normalized_query in text for text in haystacks)",
        tests=(
            f"{TESTS}/test_chaos_3617_candidate_search.py::"
            "TestFuzzyMatchingIsConservative::"
            "test_a_query_appearing_only_as_an_infix_matches_nothing",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="corpus-authorization-falls-back-to-tenancy",
        defect=(
            "the arm authorizes by tenant instead of by grant, so the "
            "same-tenant restricted project is returned to a principal who "
            "cannot see it -- invisible to every tenant-level check"
        ),
        path=SRC / "corpus_adapter.py",
        anchor="    return world.authorized_entity_ids(principal_id)",
        replacement=(
            "    return frozenset(\n"
            "        seed_ids_for_tenant(world.PRINCIPALS[principal_id].tenant_id)\n"
            "    )"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_corpus_adapter.py::"
            "TestAuthorizationIsByGrantNotTenancy",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="exhaustive-comparison-shape-silently-accepted",
        defect=(
            "a portfolio-wide or organization-wide shape is built from a "
            "peer cohort, presenting a partial sweep as an exhaustive one"
        ),
        path=SRC / "packet_builder.py",
        anchor="    elif job.comparison_shape not in _COHORT_CAPABLE_SHAPES:",
        replacement="    elif False:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestTheRefusalThatRemains::"
            "test_an_exhaustive_shape_is_still_refused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="cohort-shape-emitted-with-no-cohort-behind-it",
        defect=(
            "a cohort-bearing shape is emitted with only the subject in it, "
            "scoring as a comparison the arm never made. Note what the RED "
            "line shows: the emission is still blocked -- by the frozen "
            "contract, late and as a validation error. What this refusal "
            "adds is the typed, attributable 'this arm cannot do that', "
            "which is a different statement from 'this packet is malformed'"
        ),
        path=SRC / "packet_builder.py",
        anchor="    elif cohort is None:",
        replacement="    elif False:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestTheRefusalThatRemains::"
            "test_a_cohort_shape_with_no_proposal_is_refused",
        ),
        expect_failure="needs at least two members",
    ),
    Mutation(
        mutation_id="incomparable-cohort-emitted-as-a-comparison",
        defect=(
            "a cohort with nothing to compare against, or nothing to compare "
            "on, is emitted as though a comparison were performed. Contract-"
            "backed like the mutation above: the packet still cannot be "
            "built, and what is lost is the distinction between a capability "
            "gap and an empty result"
        ),
        path=SRC / "packet_builder.py",
        anchor="        if len(cohort_members) < 2 or not cohort_dimensions:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestTheRefusalThatRemains::"
            "test_a_cohort_that_cannot_compare_is_refused_distinguishably",
        ),
        expect_failure="needs at least two members",
    ),
    Mutation(
        mutation_id="cohort-built-against-a-wider-grant-accepted",
        defect=(
            "a cohort built with a wider authorization set than the traversal "
            "used names entities the caller may not see. The frozen "
            "contract's own cross-section check is the backstop and fires "
            "here; this refusal is the earlier, attributable one"
        ),
        path=SRC / "packet_builder.py",
        anchor="        if outside:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestTheRefusalThatRemains::"
            "test_a_cohort_naming_an_unauthorized_entity_is_refused",
        ),
        expect_failure="not in related_context.authorized_entity_ids",
    ),
    Mutation(
        mutation_id="outcome-asserted-instead-of-derived",
        defect=(
            "the packet claims a supported outcome without a driver that "
            "earned standing -- the dashboard-redirect fault, stated as an "
            "answer"
        ),
        path=SRC / "packet_builder.py",
        anchor="    if not asserted or not evidence:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::"
            "TestOutcomeIsDerivedFromDriversNotFromShape",
        ),
        expect_failure="InvestigationOutcome.UNSUPPORTED",
    ),
    Mutation(
        mutation_id="cohort-anchor-authorization-skipped",
        defect=(
            "a peer reached only through a team the caller cannot see joins "
            "the cohort, disclosing the shared owner by its own membership"
        ),
        path=SRC / "cohort.py",
        anchor="            if far not in authorized:",
        replacement="            if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestAuthorizationBoundsTheCohort::"
            "test_a_peer_reachable_only_through_an_unseen_anchor_is_excluded",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="cohort-peer-authorization-skipped",
        defect=(
            "a restricted same-tenant peer joins the cohort, which is the "
            "leak no tenant-level check catches"
        ),
        path=SRC / "cohort.py",
        anchor="            if peer not in authorized:",
        replacement="            if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestAuthorizationBoundsTheCohort::"
            "test_a_restricted_peer_is_withheld_and_counted",
        ),
        expect_failure="proj_quarry",
    ),
    Mutation(
        mutation_id="cohort-size-bound-not-applied",
        defect=(
            "the cohort size bound is not applied, so a caller that asked "
            "for a bounded comparison silently gets an unbounded one"
        ),
        path=SRC / "cohort.py",
        anchor="    included = considered[:max_members]",
        replacement="    included = considered",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestBoundsAreDisclosed::"
            "test_the_size_bound_truncates_and_says_so",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="cohort-dimensions-outlive-the-members-that-earned-them",
        defect=(
            "the cohort claims a comparison dimension whose only supporting "
            "member was dropped by the size bound, so the packet asserts a "
            "comparison nothing in it can make"
        ),
        path=SRC / "cohort.py",
        anchor="    for member in included:",
        replacement="    for member in considered:",
        tests=(
            f"{TESTS}/test_chaos_3617_cohort.py::TestBoundsAreDisclosed::"
            "test_a_dimension_only_a_dropped_member_supported_is_dropped_too",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="edge-validity-dropped-on-read",
        defect=(
            "a dependency that closed before the window comes back with no "
            "interval, so a resolved cause reads as a live one"
        ),
        path=SRC / "readback.py",
        anchor="                valid_to=neighbour.valid_to,",
        replacement="                valid_to=None,",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestReaderDifferential::"
            "test_edge_validity_survives_the_live_round_trip",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="declared-attributes-not-read-back",
        defect=(
            "an attribute the arm stores is silently unreadable, so every "
            "consumer sees an absent field while the store holds the value"
        ),
        path=SRC / "readback.py",
        anchor="        if attributes.get(key) is not None",
        replacement="        if False",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestReaderDifferential::"
            "test_the_live_reader_agrees_with_the_reference",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="observation-attributes-dropped-by-the-traversal",
        defect=(
            "an observation's trust level is lost when the traversal narrows "
            "its subject list, so an untrusted record reads as canonical -- "
            "invisible to the differential oracle because both readers share "
            "the function that drops it"
        ),
        path=SRC / "readback.py",
        anchor="            replace(observation, subject_canonical_ids=subjects)",
        replacement=(
            "            DiscoveredObservation(\n"
            "                canonical_id=observation.canonical_id,\n"
            "                kind=observation.kind,\n"
            "                title=observation.title,\n"
            "                source_class=observation.source_class,\n"
            "                observed_at=observation.observed_at,\n"
            "                subject_canonical_ids=subjects,\n"
            "                repository_ids=observation.repository_ids,\n"
            "                outcome=observation.outcome,\n"
            "            )"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_corpus_adapter.py::"
            "TestObservationTrustSurvivesTheTraversal",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="driver-support-scoped-to-the-entity-not-the-edge",
        defect=(
            "a canonical record attached to a DIFFERENT edge vouches for a "
            "fabricated one, promoting the corpus's planted false dependency "
            "to principal driver"
        ),
        path=SRC / "drivers.py",
        anchor=(
            "            if step.relationship is not relationship:\n"
            "                continue\n"
            "            if {step.from_canonical_id, step.to_canonical_id} "
            "!= {near, far}:\n"
            "                continue"
        ),
        replacement=(
            "            if far not in {step.from_canonical_id, "
            "step.to_canonical_id}:\n"
            "                continue"
        ),
        tests=(f"{TESTS}/test_chaos_3617_drivers.py::TestPoisonedLinkageIsRefused",),
        # The specific canonical record that must not vouch for the fabricated
        # edge. A bare ``assert`` here would be satisfied by either of this
        # class's other two tests going red for any reason at all.
        expect_failure="wg_authcore_shared",
    ),
    Mutation(
        mutation_id="untrusted-record-defaults-to-canonical",
        defect=(
            "a record with no trust level reads as canonical, so a stripped "
            "attribute silently turns every untrusted note into a "
            "trustworthy one -- in the direction that manufactures claims"
        ),
        path=SRC / "drivers.py",
        anchor="    return trust is not None and trust in TRUSTED_ATTRIBUTION_LEVELS",
        replacement=("    return (trust or 'canonical') in TRUSTED_ATTRIBUTION_LEVELS"),
        tests=(f"{TESTS}/test_chaos_3617_drivers.py::TestTrustHasNoDefault",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="symptom-promoted-to-driver",
        defect=(
            "an effect observed on the subject is classified as a cause, "
            "which is unsupported attribution in its most recognisable form"
        ),
        path=SRC / "drivers.py",
        anchor="                role=DriverRole.SYMPTOM,",
        replacement="                role=DriverRole.DRIVER,",
        tests=(f"{TESTS}/test_chaos_3617_drivers.py::TestSymptomsAreNeverDrivers",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="absent-status-read-as-unfinished",
        defect=(
            "an entity with no completion concept reads as unfinished, so "
            "every dependency becomes a blocker of whatever depends on it"
        ),
        path=SRC / "drivers.py",
        anchor="    return declared is not None and declared not in COMPLETE_DECLARED_STATUSES",
        replacement="    return declared not in COMPLETE_DECLARED_STATUSES",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestAbsentStatusIsNotEvidenceOfIncompleteness",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="parent-of-read-as-contributes-to",
        defect=(
            "the two child relationships are read with ONE ordering, so "
            "``parent parent_of child`` is taken for ``child contributes_to "
            "parent`` -- a parent reported as the open child of its own "
            "child, and the child rule's real finding lost. Survived the "
            "whole suite before this: the corpus reaches the child rule only "
            "through contributes_to, and the corpus-wide orientation sweep "
            "filtered to drv_block_*"
        ),
        path=SRC / "drivers.py",
        anchor=(
            "            if step.relationship is RelationshipType.PARENT_OF:\n"
            "                parent_id, child_id = source_id, target_id\n"
            "            else:\n"
            "                child_id, parent_id = source_id, target_id"
        ),
        replacement="            child_id, parent_id = source_id, target_id",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestOrientationIsReadFromDirectionNotTraversalOrder",
        ),
        expect_failure="the child rule is reading the edge backwards",
    ),
    Mutation(
        mutation_id="child-candidate-taken-from-a-non-adjacent-step",
        defect=(
            "any parent_of step anywhere on a path yields an open-child "
            "candidate, so a portfolio becomes a child of a project it "
            "merely co-occurred with"
        ),
        path=SRC / "drivers.py",
        anchor="            if parent_id != context.subject_id:",
        replacement="            if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::TestChildCandidatesMustBeAdjacent",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="historical-edge-silently-dropped-instead-of-excluded",
        defect=(
            "a dependency that closed before the window disappears instead "
            "of being reported as considered-and-rejected, so the currency "
            "guard is never exercised and the reader cannot see it was asked"
        ),
        path=SRC / "drivers.py",
        anchor="            if not step.is_current_at(context.as_of):",
        replacement="            if not step.is_current_at(context.as_of):\n                continue\n            if False:",
        tests=(f"{TESTS}/test_chaos_3617_drivers.py::TestCurrency",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="withheld-evidence-reported-as-unsupported",
        defect=(
            "evidence the caller may not see is reported as absent, which "
            "tells a reader 'nothing supports this' when the truth is 'you "
            "may not see what does'"
        ),
        path=SRC / "drivers.py",
        anchor="    if not trusted and withheld:",
        replacement="    if False:",
        tests=(f"{TESTS}/test_chaos_3617_drivers.py::TestAuthorization",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="principal-standing-awarded-on-a-tie",
        defect=(
            "two equally-supported blockers make 'the principal driver' a "
            "coin toss, and a coin toss presented as a judgment is worse "
            "than reporting both as contributing"
        ),
        path=SRC / "drivers.py",
        anchor="    if len(ordered) > 1 and rank(ordered[0])[0] == rank(ordered[1])[0]:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::TestPrincipalStandingIsEarned::"
            "test_principal_standing_is_withheld_when_two_candidates_tie",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="a-different-driver-is-promoted",
        defect=(
            "the driver credited with the supported outcome is not the one "
            "that earned it -- an outcome assertion on the enum alone stays "
            "green under exactly this substitution"
        ),
        path=SRC / "drivers.py",
        anchor="    winner = ordered[0].driver_id",
        replacement="    winner = ordered[-1].driver_id",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::TestPrincipalStandingIsEarned::"
            "test_principal_standing_is_withheld_when_two_candidates_tie",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="excluded-drivers-listed-as-principal",
        defect=(
            "the packet's principal list names candidates that never held "
            "principal standing, so an excluded candidate is presented as "
            "the judgment"
        ),
        path=SRC / "packet_builder.py",
        anchor="            if candidate.standing is DriverStanding.PRINCIPAL_DRIVER",
        replacement="            if candidate.standing is not None",
        tests=(f"{TESTS}/test_chaos_3617_drivers.py::TestTheFirstSupportedPacket",),
        expect_failure="principal_driver_ids must be exactly",
    ),
    Mutation(
        mutation_id="driver-cites-evidence-the-packet-never-indexed",
        defect=(
            "a driver cites an observation the packet does not carry, so "
            "discovery and emission disagree about what the run observed and "
            "the driver is emitted with less support than it was built from"
        ),
        path=SRC / "packet_builder.py",
        # Restores the ORIGINAL silent-drop behaviour rather than merely
        # blanking the check: with ``missing = []`` the code goes on to a raw
        # dict lookup and dies with a bare ``KeyError``, which blocks the
        # packet but proves nothing about the guard -- the guard's value is
        # an attributable refusal, and the fault it prevents is the driver
        # being emitted with less support than it was built from.
        anchor=(
            "        missing = sorted(set(ids) - set(handle_by_observation))\n"
            "        if missing:"
        ),
        replacement=(
            "        ids = [item for item in ids if item in handle_by_observation]\n"
            "        missing = []\n"
            "        if missing:"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::TestEvidenceTranslationFailsLoudly",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="arm-derives-a-number-from-two-measurements",
        defect=(
            "the arm computes a ratio from a measurement and its cohort "
            "median, putting a number no canonical service produced into a "
            "packet under that service's authority"
        ),
        path=SRC / "drivers.py",
        anchor="    return left > right if metric in HIGHER_IS_WORSE else left < right",
        replacement=(
            "    ratio = left / right if right else left\n"
            "    return ratio > 1.0 if metric in HIGHER_IS_WORSE else ratio < 1.0"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_measurements.py::TestTheArmPerformsNoArithmetic",
        ),
        expect_failure="derives a number",
    ),
    Mutation(
        mutation_id="cohort-direction-inferred-instead-of-declared",
        defect=(
            "one comparison rule governs every metric, so completed_items "
            "and work_in_progress are reported in opposite directions and "
            "one of them is exactly backwards"
        ),
        path=SRC / "drivers.py",
        anchor="    return left > right if metric in HIGHER_IS_WORSE else left < right",
        replacement="    return left > right",
        tests=(
            f"{TESTS}/test_chaos_3617_measurements.py::"
            "TestTheArmPerformsNoArithmetic::"
            "test_comparison_is_not_arithmetic_and_is_still_allowed",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="person-counting-metric-becomes-a-driver",
        defect=(
            "a count of people becomes the subject of a driver, which is one "
            "inference away from naming them and is the person-level "
            "attribution the contract bans outright"
        ),
        path=SRC / "drivers.py",
        anchor="        if metric is None or metric in PERSON_COUNTING_METRICS:",
        replacement="        if metric is None:",
        tests=(
            f"{TESTS}/test_chaos_3617_measurements.py::"
            "TestNoPersonLevelClaimIsEverBuilt",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="uncomparable-measurement-silently-dropped",
        defect=(
            "a number with no cohort comparison disappears instead of being "
            "disclosed, so a reader cannot tell the arm had the measurement "
            "and still could not answer"
        ),
        path=SRC / "drivers.py",
        anchor="        if median is None:",
        replacement="        if median is None:\n            continue\n        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_measurements.py::TestTheQualifiedCapacityCase",
        ),
        expect_failure="KeyError",
    ),
    Mutation(
        mutation_id="cited-measurement-promoted-to-a-judgment",
        defect=(
            "a number being high is presented as a cause, so the judgment "
            "stops coming from the graph and starts coming from a metric "
            "threshold -- the measuring-something-adjacent fault"
        ),
        path=SRC / "drivers.py",
        # Compound on purpose. Flipping the role ALONE is not enough, and
        # that is a property worth recording rather than a mutation worth
        # fixing: a cited measurement carries no lineage, so the pathless
        # rule refuses it even when it is mislabelled a driver. Both have to
        # go before the fault appears, which is what defence in depth means
        # when it is real.
        anchor=(
            "                paths=(),\n"
            "                support=support,\n"
            "                mechanism=StandingMechanism.CITED_MEASUREMENT,\n"
            "                assertion_basis=AssertionBasis.MEASURED,"
        ),
        replacement=(
            "                paths=readout.paths,\n"
            "                support=support,\n"
            "                mechanism=StandingMechanism.STRUCTURAL,\n"
            "                assertion_basis=AssertionBasis.MEASURED,"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_measurements.py::TestTheStrugglingTeamCase::"
            "test_a_cited_measurement_never_becomes_the_judgment",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="relationship-direction-ignored-blocking",
        defect=(
            "candidate roles are read from traversal order instead of the "
            "edge's canonical orientation, so seeding a blocker reports the "
            "thing it blocks as the PRINCIPAL DRIVER of its own blocker -- "
            "causality inverted, at the highest standing the contract offers"
        ),
        path=SRC / "drivers.py",
        anchor=(
            "    if step.direction is RelationshipDirection.FORWARD:\n"
            "        return step.from_canonical_id, step.to_canonical_id\n"
            "    return step.to_canonical_id, step.from_canonical_id"
        ),
        replacement="    return step.from_canonical_id, step.to_canonical_id",
        # Both seeding directions. The defect survived 60 killed mutations
        # because every test seeded one END of the edge; a mutation checked
        # from that same end would have survived too.
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestOrientationIsReadFromDirectionNotTraversalOrder",
        ),
        expect_failure="causality is inverted",
    ),
    Mutation(
        mutation_id="asserted-driver-support-not-closed-at-the-packet",
        defect=(
            "an asserted driver's support is not required to be its own: "
            "evidence about a different subject, or no lineage at all, still "
            "yields a supported outcome -- a judgment with nothing behind it"
        ),
        path=SRC / "packet_builder.py",
        anchor="        if problems:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestAssertedSupportMustBeTheDriversOwn",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="semantic-claim-rests-on-the-callers-word",
        defect=(
            "a semantic claim is authorized by the PASSED embedder's "
            "self-report rather than by anything that can be shown about the "
            "vectors, so an embedder with no connection to whatever wrote the "
            "store unlocks a retrieval capability claim"
        ),
        path=SRC / "packet_builder.py",
        anchor="    if embedder.semantic and attested_embedder is not None:",
        replacement="    if embedder.semantic:",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::"
            "TestASemanticClaimNeedsProvenanceNotAPromise::"
            "test_a_usable_semantic_embedder_alone_does_not_unlock_a_claim",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="packet-stamps-an-embedder-that-did-not-write-the-store",
        defect=(
            "the projection version names an embedder the partition does not "
            "attest, so a packet is stamped for an OpenAI model while the "
            "stored vectors are BLAKE2b hashes -- and that stamp is what a "
            "consumer uses to decide two runs are comparable"
        ),
        path=SRC / "packet_builder.py",
        anchor="    if attested is not None and attested != embedder.model_id:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::"
            "TestASemanticClaimNeedsProvenanceNotAPromise::"
            "test_the_stamped_projection_version_cannot_name_another_embedder",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="cloud-embedder-reports-semantics-without-a-key",
        defect=(
            "a bare CloudEmbedder with api_key=None still reports semantic, "
            "so an instance that cannot embed anything unlocks semantic "
            "claims -- the guard never asks it to embed, it reads the flag"
        ),
        path=SRC / "backend.py",
        anchor="        return bool(self.api_key)",
        replacement="        return True",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::"
            "TestASemanticClaimNeedsProvenanceNotAPromise::"
            "test_a_bare_cloud_embedder_carries_no_semantics",
        ),
        expect_failure="a bare CloudEmbedder reports semantics it cannot produce",
    ),
    Mutation(
        mutation_id="projection-records-no-embedder-provenance",
        defect=(
            "the store writes vectors without recording what produced them, "
            "so the only available answer to 'were these vectors semantic' is "
            "the caller's own claim"
        ),
        path=SRC / "backend.py",
        anchor="            PROJECTION_EMBEDDER_ATTRIBUTE: embedder.model_id,",
        replacement="            PROJECTION_EMBEDDER_ATTRIBUTE: None,",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestReaderDifferential::"
            "test_the_live_readout_attests_the_embedder_that_wrote_it",
        ),
        expect_failure="the partition attests no embedder",
    ),
    Mutation(
        mutation_id="mixed-partition-provenance-collapses-to-one",
        defect=(
            "a partition whose vectors came from two embedders reads back as "
            "one of them, so an incomparable mixture is stamped with whichever "
            "model won the read"
        ),
        path=SRC / "readback.py",
        anchor="    if len(attested) > 1:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::"
            "TestOnePartitionMustAttestOneEmbedder",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="trusted-record-vouches-for-a-linkage-it-is-not-about",
        defect=(
            "trust is read off the record and never checked against what the "
            "record is ABOUT, so an edge that merely CITES a canonical record "
            "inherits its trust -- the residue of the scoping defect, which "
            "put the corpus's planted false dependency back at principal "
            "standing"
        ),
        path=SRC / "drivers.py",
        anchor=(
            "        vouching=tuple(\n"
            "            item\n"
            "            for item in trusted\n"
            "            if endpoints & set("
            "context.observations[item].subject_canonical_ids)\n"
            "        ),"
        ),
        replacement="        vouching=tuple(trusted),",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestOnlyARecordAboutTheLinkageMayVouchForIt",
        ),
        expect_failure="vouched for a linkage it is not about",
    ),
    Mutation(
        mutation_id="attribution-proceeds-without-readable-attachment",
        defect=(
            "a reader that cannot say what a record is about still attributes "
            "on it, or reports the support as withheld -- an authorization "
            "claim about the caller's grant that nothing supports"
        ),
        path=SRC / "drivers.py",
        anchor="    if not trusted and not context.observation_attachment_available:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestAttributionNeedsAReaderThatKnowsWhatARecordIsAbout",
        ),
        expect_failure="DriverExclusionReason.UNAUTHORIZED_EVIDENCE",
    ),
    Mutation(
        mutation_id="live-reader-overclaims-attachment-capability",
        defect=(
            "the live reader declares it can recover observation attachment "
            "when it cannot, which turns the 'is this record about the "
            "linkage' rule into a silent no-op on the live path alone"
        ),
        path=SRC / "readback.py",
        anchor="            observation_attachment_available=False,",
        replacement="            observation_attachment_available=True,",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestReaderDifferential::"
            "test_each_reader_declares_the_attachment_capability_it_actually_has",
        ),
        expect_failure="claims it can recover observation attachment",
    ),
    Mutation(
        mutation_id="packet-assembly-derives-an-unnamed-number",
        defect=(
            "the no-arithmetic proof stops at the two discovery modules while "
            "packet ASSEMBLY derives a number straight into the packet -- the "
            "blind spot adversarial review found, with the derivation landing "
            "in a limitation string a consumer reads"
        ),
        path=SRC / "packet_builder.py",
        anchor="    if filtered_total:",
        replacement=(
            "    filtered_share = filtered_total / max(len(related_entities), 1)\n"
            "    if filtered_total or filtered_share:"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_measurements.py::TestTheArmPerformsNoArithmetic",
        ),
        expect_failure="the operational allowlist does not name",
    ),
    Mutation(
        mutation_id="caller-can-supply-entity-status",
        defect=(
            "a caller-supplied attribute channel reappears on discovery, "
            "through which a caller can declare the corpus's blocker complete "
            "and delete the arm's own principal driver"
        ),
        path=SRC / "drivers.py",
        anchor="    as_of: datetime,\n    max_candidates: int = 50,",
        replacement=(
            "    as_of: datetime,\n"
            "    entity_attributes: Mapping[str, Mapping[str, str]] | None = None,\n"
            "    max_candidates: int = 50,"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestDeclaredStatusComesOnlyFromTheReadout",
        ),
        expect_failure="entity_attributes",
    ),
    Mutation(
        mutation_id="driver-truncation-not-disclosed",
        defect=(
            "the driver candidate bound fires and the packet discloses no "
            "TRUNCATED_TRAVERSAL limitation, so the frozen contract refuses "
            "the packet outright -- and the only way past that refusal is for "
            "a caller to drop the flag, which presents a capped candidate set "
            "as the complete one"
        ),
        path=SRC / "packet_builder.py",
        anchor="        cohort_truncated,\n        drivers_truncated,\n    )",
        replacement="        cohort_truncated,\n    )",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestATruncatedCandidateSetIsDisclosed",
        ),
        expect_failure="no TRUNCATED_TRAVERSAL limitation is disclosed",
    ),
    Mutation(
        mutation_id="driver-truncation-discloses-but-does-not-weaken",
        defect=(
            "a bound that discloses a limitation without weakening the "
            "outcome lets a partial investigation reach a fully SUPPORTED "
            "verdict with the limitation nobody reads sitting beside it"
        ),
        path=SRC / "packet_builder.py",
        anchor="                or any(truncation_bounds)",
        replacement="                or False",
        tests=(
            f"{TESTS}/test_chaos_3617_drivers.py::"
            "TestATruncatedCandidateSetIsDisclosed::"
            "test_a_truncated_candidate_set_cannot_reach_an_ungapped_outcome",
        ),
        expect_failure="InvestigationOutcome.SUPPORTED_WITH_GAPS",
    ),
    Mutation(
        mutation_id="live-gate-skips-when-a-run-was-required",
        defect=(
            "a live-store measurement that did not happen reads as coverage "
            "in a recorded reproduction"
        ),
        path=REPO_ROOT / TESTS / "live_gate.py",
        # Anchored on the guard PLUS the message that identifies it. The bare
        # `if live_store_required():` stopped being unique the moment a second
        # gate was added for the graphiti extra, and the harness refused the
        # run as INVALID rather than disabling both -- which is the anchor
        # rule working, not a nuisance: a mutation that turns off two guards
        # proves nothing about either.
        anchor=(
            "    if live_store_required():\n"
            "        pytest.fail(\n"
            '            f"{REQUIRE_LIVE_FLAG}=1 was set, so a live-store '
            'measurement was "'
        ),
        replacement=(
            "    if False:\n"
            "        pytest.fail(\n"
            '            f"{REQUIRE_LIVE_FLAG}=1 was set, so a live-store '
            'measurement was "'
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_live_gate.py::"
            "test_the_gate_fails_rather_than_skips_when_a_live_run_is_required",
        ),
        expect_failure="Failed",
    ),
    Mutation(
        mutation_id="graphiti-extra-gate-skips-when-a-run-was-required",
        defect=(
            "a measurement that needs the optional extra is skipped in a run "
            "that required it, so an unmeasured half of the suite reads as "
            "coverage in a recorded reproduction"
        ),
        path=REPO_ROOT / TESTS / "live_gate.py",
        anchor=(
            "        if live_store_required():\n"
            "            pytest.fail(\n"
            '                f"{REQUIRE_LIVE_FLAG}=1 was set, so a measurement '
            'needing "'
        ),
        replacement=(
            "        if False:\n"
            "            pytest.fail(\n"
            '                f"{REQUIRE_LIVE_FLAG}=1 was set, so a measurement '
            'needing "'
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_live_gate.py::"
            "test_the_extra_gate_fails_rather_than_skips_when_a_run_is_required",
        ),
        expect_failure="Failed",
    ),
)

_ENV = {
    **os.environ,
    "OTEL_ENABLED": "false",
    "PYTHONPATH": str(REPO_ROOT / "src"),
}


def _run_tests(node_ids: tuple[str, ...]) -> tuple[int, str]:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", *node_ids, "-q", "-p", "no:randomly"],
        cwd=REPO_ROOT,
        env=_ENV,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout + result.stderr


#: pytest's summary line, e.g. ``2 failed, 13 passed in 1.24s``.
#:
#: Written with the separator INSIDE the repeated group rather than as an
#: optional suffix. The earlier form -- ``(?:\d+ \w+(?:, )?)+`` -- made the
#: comma optional, so ``\d+ \w+`` could match with or without a trailing
#: separator and the engine had exponentially many ways to split an input
#: like ``"0 " + "000 " * n`` before failing. That is a real ReDoS on a
#: pattern fed with subprocess output, and CodeQL was right to block on it.
#: With the separator mandatory between repetitions there is exactly one way
#: to parse any candidate, so backtracking is linear.
_SUMMARY = re.compile(r"^\d+ \w+(?:, \d+ \w+)* in [\d.]+s$", re.MULTILINE)


def _summary_line(output: str) -> str:
    matches = _SUMMARY.findall(output)
    for line in reversed(output.splitlines()):
        if " in " in line and ("passed" in line or "failed" in line):
            return line.strip()
    return matches[-1] if matches else "<no pytest summary>"


def _first_failure(output: str) -> str:
    """Where the mutation died.

    Recorded so a reader can check the failure is the one the guard exists
    to prevent rather than an unrelated collapse in setup -- a mutation that
    dies in a fixture proves nothing about the invariant it claims to test.
    """

    for line in output.splitlines():
        if line.startswith("FAILED "):
            return line.strip()
    for line in output.splitlines():
        if line.startswith("E   ") and line.strip() != "E":
            return line.strip()
    return "<no failure line captured>"


def failure_region(output: str) -> str:
    """Only the lines pytest attributes to an actual failure.

    Two shapes and nothing else: ``E ``-prefixed assertion/exception lines,
    and the ``FAILED <nodeid> - <message>`` summary. The category can appear
    in either — an assertion message for a guard that raises a typed error,
    or the exception name for one that refuses by construction.

    **Why the region matters more than the token.** pytest echoes the failing
    test's SOURCE up to the failing line, and echoed source is indented, not
    ``E ``-prefixed. So a phrase sitting in a docstring, a comment, or an
    assertion that PASSED earlier in the same test cannot enter this region —
    while a looser filter would credit it and report an unearned kill.

    This function previously also admitted any line containing ``" - "``,
    which let skipped/xfailed reason prose in. Those lines carry long
    free-text explanations and are not failures at all; a token appearing in
    one would have been credited exactly like a real assertion message.

    A sibling lane demonstrated an unearned kill from this class of hole. The
    fix is the region, not a cleverer phrase: the region is what makes any
    phrase trustworthy.
    """

    return "\n".join(
        line
        for line in output.splitlines()
        if line.startswith(("E ", "E\t")) or _FAILED_SUMMARY.match(line)
    )


#: ``FAILED <nodeid> - <message>``. Anchored, so a stray line that merely
#: contains the word cannot pass for the summary.
_FAILED_SUMMARY = re.compile(r"^FAILED \S+")


def _reason_is_proven(region: str, expected: str) -> tuple[bool, str]:
    """Whether the failure region actually evidences the expected reason.

    Both emptiness checks are load-bearing. An empty ``expected`` would make
    ``"" in region`` true and credit every mutation ever run; an empty region
    means pytest attributed nothing to a failure, so there is no evidence to
    read whatever the token says.
    """

    if not expected.strip():
        return False, "the mutation declares an empty expected failure"
    if not region.strip():
        return False, "pytest attributed no lines to a failure"
    if expected not in region:
        return False, "the expected reason is absent from the failure region"
    return True, ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="run a single mutation by id")
    args = parser.parse_args()

    selected = [
        mutation
        for mutation in MUTATIONS
        if args.only is None or mutation.mutation_id == args.only
    ]
    if not selected:
        print(f"no mutation matches --only {args.only!r}", file=sys.stderr)
        return 2

    print(f"CHAOS-3617 guard injection: {len(selected)} mutation(s)\n")
    verdicts: list[tuple[str, str, str]] = []
    for mutation in selected:
        original = mutation.path.read_text()
        if mutation.anchor not in original:
            # INVALID, not KILLED. A silently non-matching anchor would turn
            # this whole harness into a green light with no bulb behind it.
            print(
                f"INVALID  {mutation.mutation_id}: anchor not found verbatim in "
                f"{mutation.path.relative_to(REPO_ROOT)}. The guard was probably "
                "refactored; update the anchor before trusting this run.",
                file=sys.stderr,
            )
            return 1
        if original.count(mutation.anchor) != 1:
            print(
                f"INVALID  {mutation.mutation_id}: anchor appears "
                f"{original.count(mutation.anchor)} times; a mutation that "
                "disables more than the guard under test proves nothing about "
                "that guard.",
                file=sys.stderr,
            )
            return 1

        mutation.path.write_text(
            original.replace(mutation.anchor, mutation.replacement)
        )
        try:
            code, output = _run_tests(mutation.tests)
        finally:
            mutation.path.write_text(original)

        if code == 0:
            print(
                f"SURVIVED {mutation.mutation_id}: the guard was disabled and "
                "every test still passed. That test does not cover the defect "
                f"it names: {mutation.defect}",
                file=sys.stderr,
            )
            print(output, file=sys.stderr)
            return 1

        died_at = _first_failure(output)
        evidence = failure_region(output)
        proven, why = _reason_is_proven(evidence, mutation.expect_failure)
        if not proven:
            print(
                f"WRONG-REASON {mutation.mutation_id}: {why}.\n"
                f"      expected the failure to mention: "
                f"{mutation.expect_failure!r}\n"
                f"      actual: {died_at}\n"
                "      A mutation that merely turns the suite red is not "
                "evidence that the guard catches the defect it names.",
                file=sys.stderr,
            )
            return 1
        verdicts.append((mutation.mutation_id, _summary_line(output), died_at))
        print(f"RED   {mutation.mutation_id}")
        print(f"      defect: {mutation.defect}")
        print(f"      {_summary_line(output)}")
        print(f"      died at: {died_at}\n")

        # Rule 2: the restore is verified by re-running, never by a git check.
        code, output = _run_tests(mutation.tests)
        if code != 0:
            print(
                f"RESTORE FAILED {mutation.mutation_id}: the guard did not come "
                f"back. {_summary_line(output)}",
                file=sys.stderr,
            )
            return 1
        print(f"GREEN {mutation.mutation_id} restored: {_summary_line(output)}\n")

    print(f"\nAll {len(verdicts)} mutation(s) KILLED and restored green.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
