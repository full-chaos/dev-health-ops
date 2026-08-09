"""CHAOS-3620: zero unauthorized results, proved on the composed arm.

CHAOS-3617 proved the traversal filters. This module proves the *product of
the whole arm* — grant, traversal, emission, contract — discloses nothing the
caller may not see, and it does so on the one world where the interesting
mistake is possible.

**Why the corpus world and not the synthetic fixtures.** ``alpha_batch``'s
restricted project is restricted by a hand-written tuple
(``graph_arm/fixtures.py:509``). The corpus's ``proj_quarry`` is restricted by
a *grant* (``world.py:2861-2871``) while living in the caller's **own
tenant** — so every tenant comparison, every org-id check and every partition
assertion in the arm says it is fine to return. Only a check that knows the
true per-principal grant catches it. A suite that never ran that composition
has not tested the fault the corpus was built to expose.

**Every claim here is paired with its fault shape.** "The analyst never sees
Quarry" is worth nothing until the same traversal under a tenant-derived
grant is observed *including* it, and until someone is shown to see it at
all. Both controls are in this file, adjacent to the claim they earn.

**One measurement in this file is a recorded gap, not a pass.**
``TestTheIndependentOracleCannotYetScoreThisArm`` pins three vocabulary
mismatches that make ``audit_authorization(...).is_clean`` structurally
unreachable for any graph-arm packet. They are asserted as *current
behaviour* so that fixing them turns this module red and forces the record to
be updated — the alternative, leaving them out, would let a reader of a green
suite conclude the independent oracle had cleared the arm when it cannot yet
run on it.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    PacketLimitationKind,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.authorization import (
    audit_authorization,
    entity_sightings,
)
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.cohort import build_cohort
from dev_health_ops.context_fabric.graph_arm.discovery import search_candidates
from tests.context_fabric import chaos_3620_spine as spine

#: Every canonical entity in the world, whatever tenant or grant it belongs
#: to. The set the *independent* oracle measures fabrication against.
KNOWN_ENTITY_IDS = frozenset(world.ENTITIES_BY_ID)


def _analyst_grant() -> frozenset[str]:
    return adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)


def _subject_seeds() -> tuple[str, ...]:
    """Every project and team the analyst may see, as an investigation seed.

    Sorted and derived from the grant rather than listed, so a corpus that
    grows a subject is swept without anyone remembering to add it here. A
    hand-written seed list is how a leak-sweep silently stops covering the
    entity that later leaks.
    """

    return tuple(
        sorted(item for item in _analyst_grant() if item.startswith(("proj_", "team_")))
    )


def _entities_disclosed(packet, principal_id: str) -> list[str]:
    """Canonical world entities in the packet that ``principal_id`` cannot see.

    Deliberately narrowed to ids the world actually knows. The full audit
    also reports the arm's observation ids as "fabricated entities" — a
    vocabulary artifact pinned in ``TestTheIndependentOracleCannotYetScoreThisArm``
    — and folding that noise in here would make a real disclosure
    indistinguishable from a naming mismatch, which is precisely the failure
    mode this lane exists to prevent.
    """

    visible = world.PRINCIPALS[principal_id].visible_entity_ids
    return sorted(
        entity_id
        for entity_id in entity_sightings(packet)
        if entity_id in KNOWN_ENTITY_IDS and entity_id not in visible
    )


# --------------------------------------------------------------------------
# The same-tenant restricted entity
# --------------------------------------------------------------------------


class TestNoAuthorizationIsInferredFromGraphMembership:
    """Presence in the partition must confer nothing.

    The three tests below are one argument. The restricted project *is* in
    the graph the analyst queries, it *is* one hop from a team the analyst
    owns, and it *is* returned to someone. Without all three, the absence
    proved further down could be an absence of data — and a filter that is
    never asked to filter anything is not a filter.
    """

    def test_the_restricted_project_is_ingested_into_the_analysts_own_partition(
        self,
    ) -> None:
        projection = spine.helio_projection()
        node_ids = {node.canonical_id for node in projection.entity_nodes()}
        assert world.PROJ_QUARRY in node_ids, (
            "the restricted project is absent from the projection, so every "
            "downstream absence proves nothing about authorization"
        )

    def test_it_is_reachable_in_one_hop_from_an_entity_the_analyst_owns(self) -> None:
        neighbours = {
            other
            for edge in world.RELATIONSHIPS_BY_KEY.values()
            for near, other in (
                (edge.source_entity_id, edge.target_entity_id),
                (edge.target_entity_id, edge.source_entity_id),
            )
            if near == world.PROJ_QUARRY
        }
        assert neighbours & _analyst_grant(), (
            "the restricted project has no authorized neighbour, so no "
            "traversal could reach it and the filter is never exercised"
        )

    def test_the_grant_and_the_partition_disagree_by_exactly_the_restricted_project(
        self,
    ) -> None:
        by_tenant = frozenset(adapter.seed_ids_for_tenant(world.ORG_HELIO))
        assert by_tenant - _analyst_grant() == {world.PROJ_QUARRY}, (
            "tenancy and the grant no longer disagree, so this world can no "
            "longer distinguish a tenant-derived authorization set from a "
            "correct one"
        )


class TestTheRestrictedProjectNeverReachesAConsumer:
    @pytest.mark.parametrize("seed", _subject_seeds())
    def test_no_packet_the_analyst_can_produce_mentions_it(self, seed: str) -> None:
        """The sweep, not a sample.

        Every authorized subject is investigated and every packet is walked
        exhaustively by the corpus's own ``entity_sightings`` — candidates,
        committed subjects, unresolved mentions, cohort members *and*
        exclusions, related entities, path origins, terminals and hops,
        driver-affected subjects, the evidence index and what it supports,
        clarification needs, and surface refs. A leak into any one of those
        is an identifier reaching a consumer.
        """

        investigation = spine.investigate(seed)
        sightings = entity_sightings(investigation.packet)
        assert world.PROJ_QUARRY not in sightings, (
            f"the restricted project leaked into a packet seeded at {seed} at "
            f"{sorted(sightings.get(world.PROJ_QUARRY, ()))}"
        )

    @pytest.mark.parametrize("seed", _subject_seeds())
    def test_no_packet_the_analyst_can_produce_discloses_any_unauthorized_entity(
        self, seed: str
    ) -> None:
        """The general claim the restricted project is only one instance of."""

        investigation = spine.investigate(seed)
        disclosed = _entities_disclosed(investigation.packet, world.PRINCIPAL_ANALYST)
        assert not disclosed, (
            f"a packet seeded at {seed} disclosed entities outside the "
            f"caller's grant: {disclosed}"
        )

    def test_seeding_the_investigation_AT_the_restricted_project_returns_nothing(
        self,
    ) -> None:
        """An unauthorized seed is not an investigation with no results.

        It must not be investigated at all: a traversal that started there
        and returned an empty neighbourhood would still have confirmed the
        entity exists to anyone who could time it.
        """

        readout = spine.readout_for((world.PROJ_QUARRY,))
        assert not readout.entities, (
            "an unauthorized seed produced a neighbourhood, so the seed "
            "filter did not refuse it"
        )
        assert readout.authorization_filtered_count == 1, (
            "an unauthorized seed was dropped without being counted, so the "
            "packet cannot disclose that the answer was narrowed"
        )

    def test_the_compliance_principal_DOES_receive_it_from_the_same_seed(self) -> None:
        """The control that makes every absence above an earned one.

        Same world, same projection, same seed, same code path — only the
        grant differs. If this fails, nothing in this class is measuring the
        grant.
        """

        investigation = spine.investigate(
            "team_cinder", principal=world.PRINCIPAL_COMPLIANCE
        )
        sightings = entity_sightings(investigation.packet)
        assert world.PROJ_QUARRY in sightings, (
            "the restricted project is unreachable even for the principal "
            "granted it, so the analyst's clean result is structural and not "
            "an authorization decision"
        )


class TestAGrantDerivedFromTenancyLeaksIt:
    """The fault shape the corpus was built to expose.

    ``seed_ids_for_tenant`` is the mistake spelled out: every entity in the
    caller's organization, which is what an arm that authorized by tenancy
    would compute, and what every org-id and partition check in this package
    would accept without complaint.
    """

    def test_a_tenant_derived_grant_puts_it_in_front_of_the_analyst(self) -> None:
        widened = frozenset(adapter.seed_ids_for_tenant(world.ORG_HELIO))
        investigation = spine.investigate("team_cinder", authorized_entity_ids=widened)
        locations = sorted(
            entity_sightings(investigation.packet).get(world.PROJ_QUARRY, ())
        )
        assert locations, (
            "a tenant-derived authorization set did NOT disclose the "
            "restricted project, so the correct-grant result proves nothing "
            "and this whole module is vacuous"
        )

    def test_the_independent_oracle_names_it(self) -> None:
        """Not merely red — red for the right reason, naming the right id.

        The frozen contract cannot make this call: every check it performs
        compares the packet against ``related_context.authorized_entity_ids``,
        which the producer filled in. A widened claim is self-consistent, so
        the contract passes it. Only the world's true grant refuses it.
        """

        widened = frozenset(adapter.seed_ids_for_tenant(world.ORG_HELIO))
        investigation = spine.investigate("team_cinder", authorized_entity_ids=widened)
        audit = audit_authorization(
            investigation.packet, world.PRINCIPAL_ANALYST, case_id="widened-grant"
        )
        disclosed = {item.entity_id for item in audit.unauthorized_disclosures}
        assert world.PROJ_QUARRY in disclosed, (
            "the authorization audit did not report the restricted project "
            f"as an unauthorized disclosure; it reported {sorted(disclosed)}"
        )

    def test_the_frozen_contract_accepts_the_widened_packet_without_complaint(
        self,
    ) -> None:
        """Why the independent oracle has to exist at all.

        Recorded as a test rather than a docstring claim because "the
        contract cannot catch this" is exactly the kind of statement that
        stops being true silently, and if it ever does the oracle's reason
        for existing changes.
        """

        widened = frozenset(adapter.seed_ids_for_tenant(world.ORG_HELIO))
        investigation = spine.investigate("team_cinder", authorized_entity_ids=widened)
        revalidated = investigation.packet.model_validate(
            investigation.packet.model_dump(mode="json")
        )
        assert revalidated.organization_id == world.ORG_HELIO, (
            "the frozen contract rejected a widened-grant packet, which "
            "would mean the independent authorization oracle is no longer "
            "the only thing standing between a false claim and a consumer"
        )


# --------------------------------------------------------------------------
# Revocation
# --------------------------------------------------------------------------


class TestAuthorizationIsCurrentAfterRevocation:
    """A packet is only as current as the grant the traversal used.

    The arm holds no grant cache — ``neighbourhood`` takes the authorized set
    per call (``graph_arm/readback.py:318-326``) — so "current after
    revocation" reduces to two checkable claims: a narrowed grant narrows the
    next read, and a packet produced under the *old* grant is detectable
    afterwards rather than laundered by the contract.
    """

    def test_narrowing_the_grant_removes_the_entity_from_the_next_packet(self) -> None:
        before = spine.investigate("team_cinder", principal=world.PRINCIPAL_COMPLIANCE)
        after = spine.investigate("team_cinder", principal=world.PRINCIPAL_ANALYST)

        assert world.PROJ_QUARRY in entity_sightings(before.packet), (
            "the pre-revocation packet never contained the entity, so its "
            "later absence is not a revocation result"
        )
        assert world.PROJ_QUARRY not in entity_sightings(after.packet), (
            "revoking the grant did not remove the entity from the next investigation"
        )

    def test_a_packet_built_before_revocation_is_caught_by_the_audit_after_it(
        self,
    ) -> None:
        """The one that matters operationally.

        Nothing rewrites an already-emitted packet, so the only defence
        against a stale-grant packet reaching a consumer is that it can be
        judged against the *current* grant and refused. This is that
        judgment, on a real packet the arm really produced.
        """

        stale = spine.investigate("team_cinder", principal=world.PRINCIPAL_COMPLIANCE)
        audit = audit_authorization(
            stale.packet, world.PRINCIPAL_ANALYST, case_id="post-revocation"
        )
        disclosed = {item.entity_id for item in audit.unauthorized_disclosures}
        assert world.PROJ_QUARRY in disclosed, (
            "a packet produced under a grant that has since been revoked "
            "audited clean against the narrowed grant, so revocation has no "
            "effect on material already emitted"
        )

    def test_the_readout_records_the_grant_it_actually_used(self) -> None:
        """Provenance of the decision, not just its result.

        Without this the two claims above could both hold while the packet
        declared some third set, and a reviewer would have no way to tell
        which grant produced which packet.
        """

        investigation = spine.investigate("team_cinder")
        declared = set(investigation.readout.authorized_entity_ids)
        assert declared == set(investigation.grant), (
            "the readout declares an authorized set that is not the one the "
            f"traversal was given; symmetric difference "
            f"{sorted(declared ^ set(investigation.grant))}"
        )


# --------------------------------------------------------------------------
# Paths that mix authorized and unauthorized entities
# --------------------------------------------------------------------------


class TestPathsNeverMixAuthorizedAndUnauthorizedEntities:
    @pytest.mark.parametrize("seed", _subject_seeds())
    def test_every_hop_endpoint_in_every_emitted_path_is_inside_the_grant(
        self, seed: str
    ) -> None:
        investigation = spine.investigate(seed)
        grant = investigation.grant
        escapes = sorted(
            {
                endpoint
                for path in investigation.packet.related_context.paths
                for hop in path.hops
                for endpoint in (hop.source_entity_id, hop.target_entity_id)
                if endpoint not in grant
            }
        )
        assert not escapes, (
            f"a lineage path emitted for seed {seed} routes through entities "
            f"outside the caller's grant: {escapes}"
        )

    def test_the_builder_refuses_a_readout_whose_path_escapes_the_declared_grant(
        self,
    ) -> None:
        """The emitter is not allowed to trust the traversal.

        Constructed by relabelling a *real* compliance-grant readout with the
        *analyst's* narrower grant: a genuine path through the restricted
        project, now outside the set the packet will declare. This is the
        shape a stale grant or a re-used readout produces, and it must not be
        emittable.
        """

        privileged = spine.readout_for(
            ("team_cinder",), principal=world.PRINCIPAL_COMPLIANCE
        )
        crossing = [
            path for path in privileged.paths if world.PROJ_QUARRY in path.touched_ids()
        ]
        assert crossing, (
            "the privileged readout contains no path through the restricted "
            "project, so relabelling it cannot produce the escape this test "
            "is about"
        )

        relabelled = spine.with_grant(privileged, _analyst_grant())
        with pytest.raises(PermissionError) as raised:
            spine.packet_from(relabelled)
        assert world.PROJ_QUARRY in str(raised.value), (
            "the builder refused the readout but did not name the entity the "
            f"path escaped through; it said {str(raised.value)!r}"
        )


# --------------------------------------------------------------------------
# Candidate and cohort filtering
# --------------------------------------------------------------------------


class TestCandidateSearchWithholdsBeforeRanking:
    @pytest.mark.parametrize(
        "query", ("Quarry Compliance", world.PROJ_QUARRY, "Quarry")
    )
    def test_the_analyst_cannot_find_it_by_label_id_or_partial_name(
        self, query: str
    ) -> None:
        candidates, filtered = search_candidates(
            query, spine.helio_projection().entity_nodes(), _analyst_grant()
        )
        assert world.PROJ_QUARRY not in {
            candidate.canonical_id for candidate in candidates
        }, f"a search for {query!r} returned the restricted project"
        assert filtered == 1, (
            f"a search for {query!r} matched and withheld the restricted "
            f"project without counting it; reported {filtered}"
        )

    @pytest.mark.parametrize(
        "query", ("Quarry Compliance", world.PROJ_QUARRY, "Quarry")
    )
    def test_the_compliance_principal_finds_it_with_the_same_query(
        self, query: str
    ) -> None:
        """Anti-vacuity: the queries above really do match the entity."""

        candidates, filtered = search_candidates(
            query,
            spine.helio_projection().entity_nodes(),
            adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE),
        )
        assert world.PROJ_QUARRY in {
            candidate.canonical_id for candidate in candidates
        }, (
            f"a search for {query!r} does not match the restricted project "
            "for anyone, so withholding it from the analyst measured nothing"
        )
        assert filtered == 0, (
            "the fully-granted principal was told results were withheld"
        )


class TestCohortConstructionWithholdsAndCounts:
    def _labels(self):
        return {
            node.canonical_id: (node.entity_kind, node.display_label)
            for node in spine.helio_projection().entity_nodes()
        }

    def test_an_unauthorized_peer_is_withheld_and_counted(self) -> None:
        projection = spine.helio_projection()
        proposal = build_cohort(
            "team_cinder", projection.edges, self._labels(), _analyst_grant()
        )
        assert world.PROJ_QUARRY not in {
            member.canonical_id for member in proposal.members
        }, "the restricted project was proposed as a cohort member"
        assert world.PROJ_QUARRY not in {
            exclusion.canonical_id for exclusion in proposal.exclusions
        }, (
            "the restricted project was named in a cohort EXCLUSION, which "
            "discloses it just as surely as membership would"
        )
        assert proposal.authorization_filtered_count == 1, (
            "an unauthorized cohort candidate was withheld without being "
            f"counted; reported {proposal.authorization_filtered_count}"
        )

    def test_the_same_cohort_under_the_granted_principal_withholds_nothing(
        self,
    ) -> None:
        projection = spine.helio_projection()
        proposal = build_cohort(
            "team_cinder",
            projection.edges,
            self._labels(),
            adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE),
        )
        assert proposal.authorization_filtered_count == 0, (
            "the fully-granted principal was told a cohort candidate was "
            "withheld, so the analyst's count is not measuring the grant"
        )

    def test_an_unauthorized_cohort_anchor_yields_no_cohort_at_all(self) -> None:
        """Not a partial cohort, and not an error naming the subject.

        A cohort built around a subject the caller cannot see would disclose
        the subject through its own membership rationale.
        """

        projection = spine.helio_projection()
        proposal = build_cohort(
            world.PROJ_QUARRY, projection.edges, self._labels(), _analyst_grant()
        )
        assert not proposal.members and not proposal.exclusions, (
            "a cohort was constructed around an unauthorized subject"
        )


# --------------------------------------------------------------------------
# Evidence identity: substitution and scope confusion
# --------------------------------------------------------------------------


class TestEvidenceScopeConfusion:
    def test_no_evidence_about_the_restricted_project_is_ever_indexed(self) -> None:
        """The corpus plants two: one active, one redacted.

        Evidence is filtered by its *subject's* authorization, not by its own
        id — so an arm that authorized evidence separately from entities
        would leak the restricted project's activity while never naming the
        project.
        """

        restricted_slugs = {
            slug
            for slug, evidence in world.EVIDENCE_BY_SLUG.items()
            if evidence.entity_id == world.PROJ_QUARRY
        }
        assert restricted_slugs, (
            "the corpus no longer plants evidence about the restricted "
            "project, so this test cannot detect evidence-scope confusion"
        )

        for seed in _subject_seeds():
            packet = spine.investigate(seed).packet
            indexed = {
                entry.evidence.entity_id
                for entry in packet.evidence_coverage.evidence_index
            }
            leaked = sorted(indexed & restricted_slugs)
            assert not leaked, (
                f"a packet seeded at {seed} indexed evidence about the "
                f"restricted project: {leaked}"
            )

    def test_a_handle_minted_for_another_organization_does_not_verify(self) -> None:
        """Substitution across the org boundary, at the signer.

        The handle is scoped to the organization it was issued for, so a
        handle lifted from a Lumen packet into a Helio one is refused by the
        thing that minted it rather than by a downstream string comparison.
        """

        investigation = spine.investigate(
            "lumen_proj_acr",
            principal=world.PRINCIPAL_LUMEN,
            projection=spine.lumen_projection(),
        )
        entries = investigation.packet.evidence_coverage.evidence_index
        assert entries, (
            "the Lumen investigation produced no evidence, so no handle "
            "exists to substitute and this test measures nothing"
        )
        handle = entries[0].evidence.evidence_ref_id
        signer = spine.signer()
        assert signer.verify(world.ORG_LUMEN, entries[0].evidence), (
            "a handle does not verify for the organization it was minted "
            "for, so verification cannot distinguish scopes at all"
        )
        assert not signer.verify(world.ORG_HELIO, entries[0].evidence), (
            f"handle {handle} minted for {world.ORG_LUMEN} also verifies for "
            f"{world.ORG_HELIO}: evidence identity is not org-scoped"
        )


# --------------------------------------------------------------------------
# Organization widening after an unresolved reference
# --------------------------------------------------------------------------


class TestNoOrganizationWideningAfterAnUnresolvedReference:
    def test_an_unresolvable_reference_does_not_return_the_tenant(self) -> None:
        """The named fault mode from the frozen registry.

        An arm that answered "I could not resolve that" by returning every
        entity it *could* see would be maximally helpful and maximally
        wrong — and it would pass every authorization check in the package,
        because everything it returned is authorized.
        """

        readout = spine.readout_for(("proj_does_not_exist_anywhere",))
        assert not readout.entities, (
            "an unresolvable reference produced a neighbourhood of "
            f"{len(readout.entities)} entities: the answer widened to the "
            "organization"
        )
        assert not readout.paths, "an unresolvable reference produced lineage paths"

    def test_the_packet_commits_no_subject_and_names_no_entity(self) -> None:
        packet = spine.packet_from(spine.readout_for(("proj_does_not_exist_anywhere",)))
        assert not packet.subject_discovery.committed_subject_ids, (
            "a subject was committed for a reference that resolved to nothing"
        )
        assert not entity_sightings(packet), (
            "a packet for an unresolvable reference names entities: "
            f"{sorted(entity_sightings(packet))}"
        )


# --------------------------------------------------------------------------
# The filtered count: what it really measures
# --------------------------------------------------------------------------


class TestTheAuthorizationFilteredCountIsPartlyReal:
    """The handoff recorded this as "always 0". It is not, and the exact
    shape matters more than the summary.

    Two of the four packet-level counters carry a real number; two are
    literal zeros in the emitter. One of the literal zeros is the field the
    frozen scoring registry names as evidence for
    ``zero_unauthorized_results`` itself
    (``investigation_contract/scoring.py:660-676``), which makes the gap
    load-bearing rather than cosmetic.
    """

    def test_the_traversal_counts_the_entity_it_refused(self) -> None:
        investigation = spine.investigate("team_cinder")
        assert investigation.readout.authorization_filtered_count == 1, (
            "the traversal refused the restricted project without counting "
            f"it; reported {investigation.readout.authorization_filtered_count}"
        )

    def test_the_subject_discovery_section_carries_that_count(self) -> None:
        investigation = spine.investigate("team_cinder")
        assert (
            investigation.packet.subject_discovery.authorization_filtered_count == 1
        ), "the traversal's filtered count did not reach the packet"

    def test_the_packet_discloses_that_the_answer_was_narrowed(self) -> None:
        investigation = spine.investigate("team_cinder")
        kinds = {
            limitation.kind
            for limitation in investigation.packet.evidence_coverage.limitations
        }
        assert PacketLimitationKind.AUTHORIZATION_FILTERED in kinds, (
            "a packet whose traversal withheld an entity carries no "
            "authorization-filtered limitation, so a consumer cannot know "
            "the answer was narrowed"
        )

    def test_a_run_that_filters_nothing_claims_no_filtering(self) -> None:
        """The negative control. Without it the disclosure above could be
        unconditional, which would make it noise rather than a signal."""

        investigation = spine.investigate(
            "team_cinder", principal=world.PRINCIPAL_COMPLIANCE
        )
        kinds = {
            limitation.kind
            for limitation in investigation.packet.evidence_coverage.limitations
        }
        assert PacketLimitationKind.AUTHORIZATION_FILTERED not in kinds, (
            "a run that withheld nothing still claimed authorization "
            "filtering, so the disclosure carries no information"
        )

    def test_the_two_hardcoded_zeros_are_recorded_as_a_gap_not_a_result(self) -> None:
        """A NAMED, currently-unmeasured field. Asserted, not narrated.

        ``related_context.authorization_filtered_count`` and
        ``evidence_coverage.authorization_filtered_count`` are literal ``0``
        in the emitter (``graph_arm/packet_builder.py:1145`` and ``:1224``)
        on a run that demonstrably filtered one entity out of the *related
        context traversal*. The first of those is the field the scoring
        registry declares for ``zero_unauthorized_results``.

        Pinned as current behaviour so that supplying a real count turns this
        test red and forces the gap record to be updated, rather than the gap
        quietly closing and this file continuing to describe it.
        """

        investigation = spine.investigate("team_cinder")
        assert investigation.readout.authorization_filtered_count == 1, (
            "this pin is only meaningful on a run that filtered something"
        )
        assert investigation.packet.related_context.authorization_filtered_count == 0, (
            "related_context.authorization_filtered_count is no longer "
            "hardcoded to zero -- the CHAOS-3620 gap record must be updated"
        )
        assert (
            investigation.packet.evidence_coverage.authorization_filtered_count == 0
        ), (
            "evidence_coverage.authorization_filtered_count is no longer "
            "hardcoded to zero -- the CHAOS-3620 gap record must be updated"
        )


# --------------------------------------------------------------------------
# What the independent oracle can and cannot say about this arm today
# --------------------------------------------------------------------------


class TestTheIndependentOracleCannotYetScoreThisArm:
    """Three disjoint id vocabularies, each fatal on its own.

    The CHAOS-3612 defect shape — two id vocabularies that never overlap, so
    an expectation is unsatisfiable by every possible arm — recurring in the
    authorization dimension. Recorded here in full because a green suite that
    omitted it would let a reader conclude the independent oracle had cleared
    the arm, when it cannot currently run on it.
    """

    def test_the_arm_and_the_world_mint_different_handles_for_one_evidence_record(
        self,
    ) -> None:
        investigation = spine.investigate("team_cinder")
        entries = [
            entry
            for entry in investigation.packet.evidence_coverage.evidence_index
            if entry.evidence.entity_id in world.EVIDENCE_BY_SLUG
        ]
        assert entries, (
            "no indexed evidence entry names a corpus slug, so the two mints "
            "cannot be compared and this record is stale"
        )
        entry = entries[0]
        assert entry.evidence.evidence_ref_id != world.evidence_handle(
            entry.evidence.entity_id
        ), (
            "the arm and the world now mint the same handle -- the CHAOS-3620 "
            "vocabulary-mismatch record must be updated"
        )

    def test_the_withdrawn_evidence_check_is_therefore_dead_on_an_arm_packet(
        self,
    ) -> None:
        """The consequence that is not merely noise.

        ``audit_authorization`` looks a cited handle up in
        ``EVIDENCE_BY_HANDLE``; on a miss it records "fabricated" and
        ``continue``s (``investigation_corpus/authorization.py:272-274``), so
        the revoked/redacted/deleted check two lines below it
        (``:278-279``) never executes for any graph-arm packet. CHAOS-3620's
        requirement that withdrawn sources disappear from packets is
        currently unmeasurable by the oracle that owns it.
        """

        investigation = spine.investigate("team_cinder")
        audit = audit_authorization(investigation.packet, world.PRINCIPAL_ANALYST)
        cited = {
            entry.evidence.evidence_ref_id
            for entry in investigation.packet.evidence_coverage.evidence_index
        }
        assert cited, "the packet cites no evidence, so nothing is being checked"
        assert set(audit.fabricated_evidence_handles) >= cited, (
            "some cited handle now resolves in the world's mint -- the "
            "CHAOS-3620 dead-withdrawn-check record must be updated"
        )
        assert not audit.withdrawn_evidence_handles, (
            "the withdrawn-evidence check produced a result on an arm packet, "
            "which contradicts the recorded gap"
        )

    def test_the_declared_authorized_set_carries_ids_the_oracle_reads_as_entities(
        self,
    ) -> None:
        investigation = spine.investigate("team_cinder")
        declared = set(investigation.packet.related_context.authorized_entity_ids)
        non_entities = declared - KNOWN_ENTITY_IDS
        assert non_entities, (
            "the declared authorized set now contains entity ids only -- the "
            "CHAOS-3620 authorized-set-widening record must be updated"
        )
        assert non_entities & set(world.EVIDENCE_BY_SLUG), (
            "the declared set contains non-entity ids that are not corpus "
            f"evidence slugs either: {sorted(non_entities)[:5]}"
        )

    def test_the_audit_can_therefore_never_be_clean_for_this_arm(self) -> None:
        """The bottom line, stated where it cannot be missed.

        Zero real leakage (proved throughout this module) and an audit that
        still reports unclean. Until the vocabularies are reconciled, the
        arm's ``zero_unauthorized_results`` column in any trial is a failure
        it did not earn — and a genuine leak would be one line among dozens.
        """

        investigation = spine.investigate("team_cinder")
        audit = audit_authorization(investigation.packet, world.PRINCIPAL_ANALYST)
        assert not audit.unauthorized_disclosures, (
            "a real unauthorized disclosure was found; this is no longer a "
            f"vocabulary artifact: "
            f"{sorted(item.entity_id for item in audit.unauthorized_disclosures)}"
        )
        assert not audit.is_clean, (
            "the audit is now clean for a graph-arm packet -- the CHAOS-3620 "
            "oracle-mismatch record must be updated and the gap closed"
        )
