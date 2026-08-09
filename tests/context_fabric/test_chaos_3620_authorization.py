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

from pathlib import Path

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


#: Seeds whose driver-bearing packet cannot be constructed at all.
#: ``team_atlas`` produces a capacity driver with no staffing qualification,
#: which the frozen contract refuses.
#:
#: **CHAOS-3634 is DESCOPED — the abort is the design, not a pending fix.**
#: The ticket owns the disposition of record. This exemption is therefore
#: permanent rather than temporary, which makes pinning it *more* important,
#: not less: a permanent exemption is exactly the kind that quietly widens.
#: ``test_the_only_unconstructible_seed_is_the_one_we_named`` keeps it
#: honest, and a second unconstructible seed appearing is a finding rather
#: than a quiet gap in the sweep.
UNCONSTRUCTIBLE_WITH_DRIVERS = {"team_atlas"}


def _subject_seeds() -> tuple[str, ...]:
    """**Every** entity the analyst may see, as an investigation seed.

    Adversarial review found the first version of this function narrowed to
    ``proj_*`` and ``team_*``: 19 of the grant's 47 members. Repositories,
    work units, issues, services, dependencies, portfolios, initiatives and
    pull requests — every one of them a legal subject kind on the frozen
    contract — went unswept, while the module claimed a measured
    zero-leakage result. The claim was right and its scope was not, which is
    the more dangerous of the two ways to be wrong.

    Derived from the grant and never listed, so a corpus that grows a
    subject is swept without anyone remembering to come back here.
    """

    return tuple(sorted(_analyst_grant()))


def _native_packet(**overrides):
    """One packet from the REAL native producer.

    Built through CHAOS-3618's own payload fixture rather than a
    hand-assembled input, so the per-site claims below are about the arm the
    trial will run and not about a shape this module invented. The coupling
    to a sibling test module's helper is deliberate: the alternative is a
    second, divergent payload builder, and a divergent one is how a
    filtered-count claim ends up true of nothing that ships.
    """

    from dev_health_ops.context_fabric.native_arm import projection as native
    from tests.context_fabric import test_chaos_3618_projection as native_fixture

    return native.project_native_investigation(
        native_fixture._payload(**overrides)
    ).packet


def _native_packet_with_unauthorized_evidence():
    """A native run whose evidence names an entity the run cannot see.

    ``_restrict_evidence_to_authorized`` drops by ``evidence.entity_id``, so
    the plant is an evidence ref about an entity that is not the committed
    subject and not in the authorized set the run derives.
    """

    from tests.context_fabric import test_chaos_3618_projection as native_fixture

    handle = native_fixture._evidence_handle("evidence-unauthorized")
    return _native_packet(
        evidence=(
            *native_fixture._payload().evidence,
            native_fixture._evidence("proj-not-authorized", handle),
        )
    )


def _entities_disclosed_SUPERSEDED(packet, principal_id: str) -> list[str]:
    """The disclosure check this module used to have. **Kept only as RED evidence.**

    It filtered ``entity_sightings`` to ids the world knows as *entities*,
    which silently discarded two whole disclosure channels: observation and
    evidence identifiers (an indexed item's ``entity_id`` carries an evidence
    slug, not an entity id) and every prose field. Adversarial review proved
    both reachable.

    Retained, unused by any claim, so
    ``TestTheDisclosureWalkerCatchesWhatTheOldCheckMissed`` can demonstrate
    the miss against the same inputs rather than assert it. Deleting it would
    leave the fix's justification as prose.
    """

    visible = world.PRINCIPALS[principal_id].visible_entity_ids
    return sorted(
        entity_id
        for entity_id in entity_sightings(packet)
        if entity_id in KNOWN_ENTITY_IDS and entity_id not in visible
    )


def _forge(packet, **evidence_updates):
    """One indexed evidence entry, altered. The arm-shaped disclosure plant.

    Nothing about the packet's shape changes — it still validates — so the
    only thing under test is whether the disclosure check reads the field.
    """

    index = packet.evidence_coverage.evidence_index
    first = index[0]
    forged_entry = first.model_copy(
        update={"evidence": first.evidence.model_copy(update=evidence_updates)}
    )
    return packet.model_copy(
        update={
            "evidence_coverage": packet.evidence_coverage.model_copy(
                update={"evidence_index": (forged_entry, *index[1:])}
            )
        }
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
    @pytest.mark.parametrize("with_drivers", (False, True))
    def test_no_packet_the_analyst_can_produce_discloses_anything_restricted(
        self, seed: str, with_drivers: bool
    ) -> None:
        """The general claim, over the WHOLE grant and both packet shapes.

        Every one of the 47 entities the analyst may see, investigated with
        and without driver discovery, checked by the full disclosure walker —
        restricted entity ids, restricted evidence slugs, and restricted
        display labels, each matched as a whole token.

        The earlier version of this test swept 19 seeds and read one channel.
        Both narrowings were found by review, and neither was visible from
        the result: it was green then and it is green now.
        """

        if with_drivers and seed in UNCONSTRUCTIBLE_WITH_DRIVERS:
            pytest.skip(
                f"{seed} cannot produce a driver-bearing packet at all "
                "(CHAOS-3634: a capacity driver with no staffing "
                "qualification is refused by the frozen contract). Pinned by "
                "test_the_only_unconstructible_seed_is_the_one_we_named so "
                "this exemption cannot widen silently."
            )

        investigation = spine.investigate(seed, with_drivers=with_drivers)
        found = spine.disclosures(investigation.packet, world.PRINCIPAL_ANALYST)
        assert not found, (
            f"a packet seeded at {seed} (drivers={with_drivers}) disclosed "
            f"restricted material: {found}"
        )

    def test_the_only_unconstructible_seed_is_the_one_we_named(self) -> None:
        """A skip is a hole in the sweep unless it is pinned.

        The exemption above is the one place this sweep does not measure. If
        a second seed becomes unconstructible, the sweep would quietly cover
        less while still reporting green — so the exemption set is asserted
        to be exactly what it claims, by running every seed and collecting
        the failures.
        """

        from pydantic import ValidationError

        unconstructible: dict[str, str] = {}
        unexpected: list[tuple[str, str]] = []
        for seed in _subject_seeds():
            try:
                spine.investigate(seed, with_drivers=True)
            except ValidationError as refusal:
                # Only a CONTRACT refusal counts as the documented case, and
                # only for the documented reason. Review found this catching
                # bare Exception, so an unrelated emitter or walker crash for
                # team_atlas would have been accepted as the same exemption
                # and the sweep would have stayed green while covering less.
                if "staffing_qualification" not in str(refusal):
                    unexpected.append((seed, f"ValidationError: {refusal}"[:120]))
                else:
                    unconstructible[seed] = "staffing_qualification"
            except Exception as failure:  # noqa: BLE001
                unexpected.append((seed, f"{type(failure).__name__}: {failure}"[:120]))

        assert not unexpected, (
            "seeds failed for reasons other than the documented "
            f"staffing-qualification refusal: {unexpected}"
        )
        assert set(unconstructible) == UNCONSTRUCTIBLE_WITH_DRIVERS, (
            "the set of seeds that cannot produce a driver-bearing packet "
            f"changed to {sorted(unconstructible)}; the sweep's exemption "
            "list must be updated and the new entry ticketed"
        )

    @pytest.mark.parametrize(
        "principal", (world.PRINCIPAL_COMPLIANCE, world.PRINCIPAL_LUMEN)
    )
    def test_no_packet_ANY_principal_can_produce_discloses_anything_restricted(
        self, principal: str
    ) -> None:
        """The other two principals, swept the same way.

        The compliance principal has a wider grant and the Lumen principal
        lives in the other tenant — so "restricted" means something different
        for each, and the walker derives it per principal rather than
        assuming the analyst's view.

        The Lumen sweep is what caught the walker's own false positive: the
        label ``Ember`` matched inside the JSON key ``"members"`` on four
        clean packets before whole-token matching landed.

        **The compliance leg measures something weaker, and says so.** Its
        restricted set is ``{lumen_proj_acr, lumen_team_core}`` — entirely
        cross-tenant, with nothing in the Helio partition it traverses. No
        Helio traversal can reach those at any grant, so this leg
        re-measures PARTITION ISOLATION (CHAOS-3617's ground) rather than the
        grant. Kept because a partition-isolation regression would show up
        here, scoped because calling it a grant test would overstate it.
        ``test_each_grant_discriminating_leg_has_reachable_restricted_material``
        is what stops another leg quietly becoming this one.
        """

        projection = (
            spine.lumen_projection()
            if principal == world.PRINCIPAL_LUMEN
            else spine.helio_projection()
        )
        leaks: dict[str, list[str]] = {}
        for seed in sorted(world.PRINCIPALS[principal].visible_entity_ids):
            for with_drivers in (False, True):
                if with_drivers and seed in UNCONSTRUCTIBLE_WITH_DRIVERS:
                    continue
                investigation = spine.investigate(
                    seed,
                    principal=principal,
                    projection=projection,
                    with_drivers=with_drivers,
                )
                found = spine.disclosures(investigation.packet, principal)
                if found:
                    leaks[f"{seed}/drivers={with_drivers}"] = found
        assert not leaks, f"{principal} received restricted material: {leaks}"

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


class TestTheDisclosureWalkerCatchesWhatTheOldCheckMissed:
    """RED-first evidence for the fix, against the same inputs.

    Adversarial review found the previous disclosure check blind to two
    channels. Both counterexamples are executed here, and each is checked
    twice: the superseded helper reports nothing, the walker names it. A fix
    whose justification is only prose is a fix nobody can audit.

    Both plants are arm-shaped — one field of one real indexed evidence
    entry, on a packet the arm really produced, still contract-valid.
    """

    def test_a_restricted_evidence_slug_riding_an_entity_id_field(self) -> None:
        """Channel one: identifiers that are not entity ids.

        ``wi_quarry_redacted`` is real corpus evidence whose subject is the
        restricted project. It is not in ``ENTITIES_BY_ID``, so the old
        check — which filtered to known entity ids — discarded it while
        ``entity_sightings`` reported it plainly.
        """

        clean = spine.investigate("team_cinder").packet
        forged = _forge(clean, entity_id="wi_quarry_redacted")

        assert "wi_quarry_redacted" in entity_sightings(forged), (
            "the plant did not reach the packet, so neither check is being exercised"
        )
        assert not _entities_disclosed_SUPERSEDED(forged, world.PRINCIPAL_ANALYST), (
            "the superseded check now catches this, which would mean the "
            "recorded RED evidence no longer demonstrates the miss"
        )
        assert spine.disclosures(forged, world.PRINCIPAL_ANALYST) == [
            "evidence_slug:wi_quarry_redacted"
        ], "the walker did not name the restricted evidence slug"

    def test_a_restricted_display_LABEL_carried_in_prose(self) -> None:
        """Channel two: names, not identifiers.

        A packet that never mentions ``proj_quarry`` but does say "Quarry
        Compliance" has disclosed it to any human reading the answer. No
        identifier-based check can see this.
        """

        clean = spine.investigate("team_cinder").packet
        forged = _forge(clean, display_label="Quarry Compliance rollout notes")

        assert not _entities_disclosed_SUPERSEDED(forged, world.PRINCIPAL_ANALYST), (
            "the superseded check now catches the prose channel"
        )
        assert spine.disclosures(forged, world.PRINCIPAL_ANALYST) == [
            "label:Quarry Compliance"
        ], "the walker did not name the restricted label"

    def test_the_walker_is_silent_on_the_unforged_packet(self) -> None:
        """The control. A walker that reported something on every packet
        would pass both tests above and mean nothing."""

        clean = spine.investigate("team_cinder").packet
        assert spine.disclosures(clean, world.PRINCIPAL_ANALYST) == []

    def test_the_ambiguous_set_is_EXACTLY_these_two_names(self) -> None:
        """The blind spot the exclusion rule creates, pinned by name.

        Adversarial review demonstrated a label-only leak: a packet whose
        ``display_label`` is ``Core`` — the restricted Lumen team's name —
        produces zero disclosures, because ``Core`` is excluded as ambiguous.

        The finding is real. Its stated cause was not, and the correction
        matters more than the fix: review attributed it to classification
        using substring containment while matching used whole tokens. Making
        them consistent (done) recovers **nothing** — measured, both rules
        exclude exactly ``{Agent Context Runtime, Core}`` for the analyst and
        ``{Agent Context Runtime}`` for Lumen. ``Core`` is *genuinely*
        ambiguous: the analyst may legitimately see ``platform core``,
        team_atlas's previous name, which contains ``core`` as a whole token.

        So the residual is not a bug to fix but a limit to state: **a
        label-only prose leak of an ambiguous name is undetectable by this
        walker**, and this test says exactly which names that is. Pinned as a
        set so a corpus that makes a third name ambiguous — silently widening
        the blind spot — turns this red.
        """

        analyst = spine.restricted_material(world.PRINCIPAL_ANALYST)
        assert analyst.ambiguous_labels == {"Agent Context Runtime", "Core"}, (
            "the analyst's ambiguous-label set changed; the walker's blind "
            f"spot moved and must be re-stated: {sorted(analyst.ambiguous_labels)}"
        )
        lumen = spine.restricted_material(world.PRINCIPAL_LUMEN)
        assert lumen.ambiguous_labels == {"Agent Context Runtime"}, (
            f"Lumen's ambiguous-label set changed: {sorted(lumen.ambiguous_labels)}"
        )

    def test_every_ambiguous_name_is_still_covered_by_its_ID_channel(self) -> None:
        """What keeps the blind spot narrow, asserted rather than assumed.

        A label this walker cannot match belongs to an entity whose
        **identifier** it still matches. So the residual is precisely: a
        packet that carries the restricted entity's NAME and never its ID.
        Anything that names the entity the way the arm actually emits
        entities is caught.

        If an ambiguous label ever belonged to an entity outside the id
        channel too, the blind spot would be total for that entity — this is
        the test that would say so.
        """

        material = spine.restricted_material(world.PRINCIPAL_ANALYST)
        for label in material.ambiguous_labels:
            owners = [
                entity_id
                for entity_id, entity in world.ENTITIES_BY_ID.items()
                if entity.display_label == label
            ]
            restricted_owners = [
                entity_id for entity_id in owners if entity_id in material.entity_ids
            ]
            assert restricted_owners, (
                f"ambiguous label {label!r} belongs to no restricted entity, "
                "so nothing explains why it was a candidate at all"
            )
            for entity_id in restricted_owners:
                assert entity_id in material.entity_ids, (
                    f"{entity_id} carries an unmatchable label AND is outside "
                    "the id channel: the walker is fully blind to it"
                )

    def test_the_label_only_leak_of_an_ambiguous_name_is_a_STATED_residual(
        self,
    ) -> None:
        """The leak review demonstrated, executed and recorded as the limit.

        Kept as an executable statement of what this walker does **not**
        catch, so the limit is discoverable from the suite rather than only
        from a review transcript. If a future change makes it detectable this
        goes red and the residual is retired deliberately.
        """

        packet = spine.investigate("team_cinder").packet
        forged = _forge(packet, display_label="Core")
        assert spine.disclosures(forged, world.PRINCIPAL_ANALYST) == [], (
            "a label-only leak of an ambiguous name is now detected -- the "
            "CHAOS-3620 ambiguity residual must be updated and this test "
            "replaced by the proof that it is caught"
        )
        # And the reason it is tolerable: the same entity's id IS caught.
        by_id = _forge(packet, entity_id="lumen_team_core")
        assert spine.disclosures(by_id, world.PRINCIPAL_ANALYST) == [
            "entity_id:lumen_team_core"
        ], "the id channel does not cover the entity whose label is unmatchable"

    def test_an_ambiguous_label_is_excluded_and_recorded_not_matched(self) -> None:
        """Why the walker cannot simply match every restricted name.

        The corpus's cross-tenant near-duplicate gives ``lumen_proj_acr`` the
        label "Agent Context Runtime" — identical to the Helio project the
        analyst is entitled to read about. Matching it would report a
        disclosure on every legitimate ACR packet. The exclusion is carried
        on the material rather than applied silently, so it is auditable.
        """

        material = spine.restricted_material(world.PRINCIPAL_ANALYST)
        assert "Agent Context Runtime" in material.ambiguous_labels, (
            "the cross-tenant label collision is no longer recorded as "
            "ambiguous; the walker will false-positive on every ACR packet"
        )
        assert "Agent Context Runtime" not in material.labels
        assert "Quarry Compliance" in material.labels, (
            "the restricted project's own label was excluded as ambiguous, "
            "which would make the prose channel unmeasured"
        )

    def test_the_material_covers_all_three_channels(self) -> None:
        """Anti-vacuity: an empty channel silently measures nothing."""

        material = spine.restricted_material(world.PRINCIPAL_ANALYST)
        assert material.entity_ids, "no restricted entity ids"
        assert material.evidence_slugs, "no restricted evidence slugs"
        assert material.labels, "no matchable restricted labels"


class TestTheSweepLegsAreNotSecretlyVacuous:
    """Which legs can actually fail, measured.

    Found by asking "what would this test fail on?" rather than by reading
    it: the compliance leg cannot fail at any grant, because everything it
    is forbidden lives in another partition. A leg like that is not wrong —
    it is just measuring something other than what its name suggests, and
    the danger is another leg silently becoming one after a world change.
    """

    #: Measured, not assumed — and the measurement corrected me. I reported
    #: Lumen as "the strong, genuinely discriminating leg" because its
    #: restricted set is large (48 entities). Every one of them is
    #: CROSS-TENANT: the Lumen analyst sees everything in its own tenant, so
    #: like compliance it re-measures partition isolation. **Exactly one leg
    #: is grant-discriminating** — the analyst, whose restricted set contains
    #: the same-tenant proj_quarry the corpus exists to plant.
    #:
    #: Both other legs are kept: a partition-isolation regression shows up
    #: there, and the Lumen leg is what caught the walker's Ember/"members"
    #: false positive, which is a property of the WALKER rather than of any
    #: grant. Only the naming was wrong.
    GRANT_DISCRIMINATING = (world.PRINCIPAL_ANALYST,)
    PARTITION_ISOLATION_ONLY = (world.PRINCIPAL_COMPLIANCE, world.PRINCIPAL_LUMEN)

    @pytest.mark.parametrize("principal", GRANT_DISCRIMINATING)
    def test_each_grant_discriminating_leg_has_reachable_restricted_material(
        self, principal: str
    ) -> None:
        """A leg proves something about the grant only if what it forbids is
        reachable in the partition it walks."""

        material = spine.restricted_material(principal)
        tenant = world.PRINCIPALS[principal].tenant_id
        same_tenant_restricted = {
            entity_id
            for entity_id in material.entity_ids
            if world.ENTITIES_BY_ID[entity_id].tenant_id == tenant
        }
        assert same_tenant_restricted, (
            f"{principal} has no restricted entity inside its own tenant, so "
            "nothing it is forbidden is reachable by its traversal and this "
            "leg has become a partition-isolation test rather than a grant "
            "test. Re-scope it or restore the same-tenant restriction"
        )

    @pytest.mark.parametrize("principal", PARTITION_ISOLATION_ONLY)
    def test_the_other_legs_are_the_known_partition_isolation_ones(
        self, principal: str
    ) -> None:
        """Pinned so the split above stays honest in both directions.

        If either of these gains same-tenant restricted material it becomes
        grant-discriminating and belongs in the other list — a strengthening
        that should be noticed and moved deliberately, not absorbed.
        """

        material = spine.restricted_material(principal)
        tenant = world.PRINCIPALS[principal].tenant_id
        assert not {
            entity_id
            for entity_id in material.entity_ids
            if world.ENTITIES_BY_ID[entity_id].tenant_id == tenant
        }, (
            f"{principal} now has same-tenant restricted material, so its leg "
            "has become grant-discriminating and should move to "
            "GRANT_DISCRIMINATING"
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

    def test_revocation_through_the_PRODUCTION_grant_source_is_not_constructible(
        self,
    ) -> None:
        """The boundary, measured — because the honest answer is "you can't".

        Round-2 review was right that the test below passes both grants
        explicitly and so proves per-call filtering rather than revocation
        through the real authorization source. The instruction was to measure
        what the boundary IS rather than build a fake production path to
        satisfy the finding, and the measurement is unambiguous:

        ``derive_authorized_entity_ids`` raises unconditionally
        (``readback.py:294-312``) and has **zero call sites in src/**. Grant
        supply is caller-side by design — the H7 boundary CHAOS-3617 recorded
        deliberately, with CHAOS-3616's authorization oracle scoring the
        grant externally instead. There is no production grant source on this
        arm to revoke *through*.

        So "current authorization after revocation, via the production
        derivation" is **not constructible on this arm**, for a stated design
        reason rather than a missing test. What IS constructible — that a
        narrowed grant narrows the next read, and that an already-emitted
        packet is caught by the audit afterwards — is proved below and in
        this class's other tests.
        """

        from dev_health_ops.context_fabric.graph_arm.readback import (
            AuthorizationDerivationNotImplementedError,
            derive_authorized_entity_ids,
        )

        with pytest.raises(AuthorizationDerivationNotImplementedError):
            derive_authorized_entity_ids(world.ORG_HELIO, world.PRINCIPAL_ANALYST)

        arm_root = (
            Path(spine.__file__).resolve().parents[2]
            / "src"
            / "dev_health_ops"
            / "context_fabric"
        )
        callers = sorted(
            path.name
            for path in arm_root.rglob("*.py")
            if "derive_authorized_entity_ids(" in path.read_text(encoding="utf-8")
            and path.name != "readback.py"
        )
        assert not callers, (
            "production code now derives the grant "
            f"({callers}); revocation through the real source has become "
            "constructible and this disposition must be replaced by the test"
        )

    def test_a_grant_narrowed_between_calls_narrows_the_next_read(self) -> None:
        """What IS constructible: per-call filtering, stated as exactly that.

        Two investigations by ONE principal with a narrowed set between them.
        This is *not* revocation through a production grant source — that is
        not constructible (see above) — it is the property the arm actually
        offers: ``neighbourhood`` takes the authorized set per call and holds
        no cache, so a caller that narrows gets a narrowed answer.

        Named for what it proves rather than for what would be more
        impressive. The earlier name claimed a session-level revocation this
        cannot demonstrate.
        """

        principal = world.PRINCIPAL_COMPLIANCE
        before = adapter.authorized_entity_ids_for(principal)
        assert world.PROJ_QUARRY in before, (
            "the principal used for the transition cannot see the restricted "
            "project to begin with, so there is nothing to revoke"
        )

        wide = spine.investigate(
            "team_cinder", principal=principal, authorized_entity_ids=before
        )
        assert world.PROJ_QUARRY in entity_sightings(wide.packet), (
            "the pre-revocation investigation did not contain the entity, so "
            "its later absence is not a revocation result"
        )

        after = before - {world.PROJ_QUARRY}
        narrowed = spine.investigate(
            "team_cinder", principal=principal, authorized_entity_ids=after
        )
        assert world.PROJ_QUARRY not in entity_sightings(narrowed.packet), (
            "the SAME principal still receives the entity after its grant was "
            "narrowed; a grant transition is not taking effect"
        )
        assert not spine.disclosures(narrowed.packet, world.PRINCIPAL_ANALYST), (
            "the post-revocation packet still discloses restricted material "
            "through some channel"
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

    def _escaping_readout(self):
        """A real privileged readout, relabelled with the narrower grant.

        A genuine path through the restricted project, now outside the set
        the packet will declare. This is the shape a stale grant or a re-used
        readout produces, and it must not be emittable.
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
        return spine.with_grant(privileged, _analyst_grant())

    def test_the_builder_refuses_it_before_the_contract_ever_sees_it(self) -> None:
        """The emitter is not allowed to trust the traversal.

        The emitter's own check is the *first* of two independent refusals.
        Which one fires matters: guard injection showed that disabling this
        one does not let the packet out — the frozen contract's
        ``validate_paths_stay_inside_authorized_set`` catches it instead — so
        the claim being made here is specifically that the emitter refuses
        early and names the entity, not that a packet cannot escape.
        """

        with pytest.raises(PermissionError) as raised:
            spine.packet_from(self._escaping_readout())
        assert world.PROJ_QUARRY in str(raised.value), (
            "the builder refused the readout but did not name the entity the "
            f"path escaped through; it said {str(raised.value)!r}"
        )

    def test_and_the_frozen_contract_refuses_the_same_shape_independently(
        self,
    ) -> None:
        """The second layer, exercised without disabling the first.

        Recorded because two enforcers are worth having only if both are
        known to work; an unexercised second layer is an assumption, and this
        one is what actually holds if the emitter check is ever refactored
        away.
        """

        from pydantic import ValidationError

        from dev_health_ops.api.dev.investigation_contract.packet import RelatedContext

        readout = self._escaping_readout()
        with pytest.raises(ValidationError) as raised:
            RelatedContext(
                schema_version="ask_dev_related_context.v1",
                entities=(),
                paths=tuple(spine.lineage_path_for(path) for path in readout.paths),
                authorized_entity_ids=tuple(sorted(_analyst_grant())),
                authorization_filtered_count=0,
                entities_truncated=False,
                paths_truncated=False,
                truncation_reason=None,
            )
        assert "authorized" in str(raised.value), (
            "the contract refused the escaping paths for some reason other "
            f"than authorization: {str(raised.value)[:200]!r}"
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
    #: A subject that shares its owning team with the restricted project, so
    #: the restricted project is a genuine *peer* rather than merely a nearby
    #: node. Chosen after guard injection showed that a subject where the
    #: restricted project could never have been a member kills nothing: the
    #: membership assertion passed with the authorization filter disabled,
    #: and only the count moved.
    PEER_SUBJECT = "proj_pulse"

    def _labels(self):
        return {
            node.canonical_id: (node.entity_kind, node.display_label)
            for node in spine.helio_projection().entity_nodes()
        }

    def test_the_restricted_project_really_is_a_peer_of_this_subject(self) -> None:
        """Anti-vacuity, and the thing guard injection caught.

        Under the granted principal the restricted project appears as a
        cohort MEMBER. Without that, "it is not a member for the analyst" is
        satisfied by a subject it could never have been a member of.
        """

        projection = spine.helio_projection()
        proposal = build_cohort(
            self.PEER_SUBJECT,
            projection.edges,
            self._labels(),
            adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE),
        )
        assert world.PROJ_QUARRY in {
            member.canonical_id for member in proposal.members
        }, (
            f"{world.PROJ_QUARRY} is not a cohort peer of {self.PEER_SUBJECT} "
            "for anyone, so withholding it from the analyst measures nothing"
        )

    def test_an_unauthorized_peer_is_withheld_and_counted(self) -> None:
        projection = spine.helio_projection()
        proposal = build_cohort(
            self.PEER_SUBJECT, projection.edges, self._labels(), _analyst_grant()
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

    def test_an_unauthorized_peer_reaches_the_EXCLUSION_list_too(self) -> None:
        """The channel a membership-only check would miss.

        For a subject whose peers are filtered out on kind, the restricted
        project surfaces as an *exclusion* under the granted principal — a
        named entity with a stated reason, which is a disclosure. The analyst
        must see neither.
        """

        projection = spine.helio_projection()
        privileged = build_cohort(
            "repo_pulse",
            projection.edges,
            self._labels(),
            adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE),
        )
        assert world.PROJ_QUARRY in {
            exclusion.canonical_id for exclusion in privileged.exclusions
        }, (
            "the restricted project no longer reaches the exclusion list for "
            "anyone, so this disclosure channel is untested"
        )
        narrowed = build_cohort(
            "repo_pulse", projection.edges, self._labels(), _analyst_grant()
        )
        assert world.PROJ_QUARRY not in {
            exclusion.canonical_id for exclusion in narrowed.exclusions
        }, "the restricted project was disclosed through a cohort exclusion"

    def test_the_same_cohort_under_the_granted_principal_withholds_nothing(
        self,
    ) -> None:
        projection = spine.helio_projection()
        proposal = build_cohort(
            self.PEER_SUBJECT,
            projection.edges,
            self._labels(),
            adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE),
        )
        assert proposal.authorization_filtered_count == 0, (
            "the fully-granted principal was told a cohort candidate was "
            "withheld, so the analyst's count is not measuring the grant"
        )

    def test_an_unauthorized_ANCHOR_is_withheld_and_counted_separately(self) -> None:
        """The other of the two authorization checks in cohort construction.

        Guard injection made the distinction necessary: ``build_cohort``
        filters the *anchor* a peer is reached through
        (``cohort.py:232``) and the *peer* itself (``cohort.py:252``) at two
        different sites, and disabling either alone leaves the other
        standing. The anchor check's observable effect on this subject is the
        withheld count, and a count that silently went to zero would tell a
        consumer the answer was complete when it was narrowed.
        """

        projection = spine.helio_projection()
        proposal = build_cohort(
            "team_cinder", projection.edges, self._labels(), _analyst_grant()
        )
        assert proposal.authorization_filtered_count == 1, (
            "an unauthorized cohort ANCHOR was skipped without being counted; "
            f"reported {proposal.authorization_filtered_count}"
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

    def test_a_substituted_handle_is_rejected_by_the_consumer_side_check(
        self,
    ) -> None:
        """The substitution attack the requirement actually names.

        Review was right that org-scoping alone is not "evidence-id
        substitution": the interesting attack is *within* an organization —
        swap one cited handle for another legitimately-minted one and see
        whether anything downstream notices.

        Something does. The handle is an HMAC over the record's own payload,
        and ``EvidenceExpansionService._authorize_expansion``
        (``evidence_service.py:517-528``) refuses to expand anything whose
        signature does not verify — collapsing it to ``UNAUTHORIZED`` /
        ``not_found``. Both directions of the swap are exercised, because an
        attacker can move the handle to the record or the record to the
        handle.
        """

        packet = spine.investigate("team_cinder").packet
        first, second = (
            packet.evidence_coverage.evidence_index[0].evidence,
            packet.evidence_coverage.evidence_index[1].evidence,
        )
        signer = spine.signer()

        assert signer.verify(world.ORG_HELIO, first), (
            "a handle this run minted does not verify, so every rejection "
            "below would be indistinguishable from a broken signer"
        )
        assert signer.verify(world.ORG_HELIO, second)

        wearing_another_handle = first.model_copy(
            update={"evidence_ref_id": second.evidence_ref_id}
        )
        assert not signer.verify(world.ORG_HELIO, wearing_another_handle), (
            "an evidence record wearing a DIFFERENT record's legitimately "
            "minted handle passed verification: handles are interchangeable "
            "within an organization"
        )

        relabelled_under_its_own_handle = first.model_copy(
            update={"entity_id": second.entity_id}
        )
        assert not signer.verify(world.ORG_HELIO, relabelled_under_its_own_handle), (
            "an evidence record relabelled to point at a different entity "
            "still verified under its original handle: the signature does not "
            "cover the subject"
        )

    def test_the_production_expansion_path_is_gated_on_that_verification(
        self,
    ) -> None:
        """A SOURCE check, and its limit is stated rather than implied.

        Review is right that this cannot catch a gate that computes
        ``verify(...)`` and ignores the result — a static read sees the call,
        not its use. It is kept because it catches the cheaper and likelier
        regression (the call deleted outright), and because the alternative
        is claiming coverage this suite does not own.

        **Behavioural coverage of the expansion path belongs to
        ``tests/api/dev/test_evidence_service.py``**, which exercises the
        service rather than reading it. A5's ledger claim is scoped to what
        this suite establishes — substitution is refused by the signer, and
        the gate still calls it — with the behavioural proof credited to its
        real owner, whose existence is asserted below so the credit cannot
        dangle.
        """

        from dev_health_ops.api.dev import evidence_service

        source = Path(evidence_service.__file__).read_text(encoding="utf-8")
        gate = source[source.index("async def _authorize_expansion") :]
        gate = gate.split("\n    async def ", 1)[0].split("\n    def ", 1)[0]
        assert "self._signer.verify(" in gate, (
            "the evidence-expansion authorization gate no longer verifies the "
            "handle signature, so a substituted handle would be expanded"
        )
        assert "UNAUTHORIZED" in gate, (
            "the gate no longer collapses a failed verification to UNAUTHORIZED"
        )
        owner = (
            Path(spine.__file__).resolve().parents[2]
            / "tests"
            / "api"
            / "dev"
            / "test_evidence_service.py"
        )
        assert owner.is_file(), (
            "the production owner of this path's BEHAVIOURAL coverage "
            f"({owner.name}) is gone; A5's ledger entry credits it and that "
            "credit is now dangling"
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

    def test_the_native_arm_reports_a_TRUTHFUL_zero_for_related_context(self) -> None:
        """Not every zero is a gap, and calling them all gaps is its own error.

        The native arm's ``related_context`` is built with ``entities=()``
        and ``paths=()``: it performs no traversal at all. A zero
        filtered-count there is the truth — nothing could have been filtered
        out of a traversal that never happened. This is the opposite of the
        graph arm's zero, which sits on a run that demonstrably filtered one
        entity.

        Observed by running the real native producer rather than read off
        the source, because "this zero is truthful" is precisely the kind of
        claim a source read gets wrong.
        """

        packet = _native_packet()
        assert not packet.related_context.entities, (
            "the native arm now emits related entities, so a zero "
            "filtered-count there is no longer self-evidently truthful"
        )
        assert not packet.related_context.paths, (
            "the native arm now emits lineage paths, so its filtered-count "
            "zero needs the same scrutiny as the graph arm's"
        )
        assert packet.related_context.authorization_filtered_count == 0

    def test_the_native_subject_count_is_caller_supplied_and_honest_when_absent(
        self,
    ) -> None:
        """A declared default, not a silent one — and it discloses.

        ``NativeProjectionInput.authorization_filtered_count`` defaults to
        zero and the field's own comment says an unknown count is reported as
        zero rather than guessed. The half that matters is that a supplied
        count is carried AND disclosed as a limitation, so the default is a
        real "we did not count" rather than a claim that nothing was
        filtered.
        """

        absent = _native_packet()
        assert absent.subject_discovery.authorization_filtered_count == 0
        assert PacketLimitationKind.AUTHORIZATION_FILTERED not in {
            limitation.kind for limitation in absent.evidence_coverage.limitations
        }, "the native arm claimed authorization filtering it was never told about"

        supplied = _native_packet(authorization_filtered_count=3)
        assert supplied.subject_discovery.authorization_filtered_count == 3, (
            "a caller-supplied filtered count did not reach the native packet"
        )
        assert PacketLimitationKind.AUTHORIZATION_FILTERED in {
            limitation.kind for limitation in supplied.evidence_coverage.limitations
        }, (
            "the native arm carried a filtered count without disclosing that "
            "the answer was narrowed"
        )

    def test_the_native_cohort_zero_is_an_UNMEASURED_site_not_a_truthful_one(
        self,
    ) -> None:
        """The one native site that is a real gap.

        ``_comparison_cohort`` emits ``authorization_filtered_count=0`` as a
        literal (``native_arm/projection.py:783``) while returning members,
        and the builder applies no authorization filter of its own. So the
        zero means "no filter ran", not "the filter removed nothing" — and
        those are indistinguishable to a consumer.

        Pinned with a member present, because a zero beside an empty cohort
        would be truthful for the same reason the related-context zero is.
        """

        packet = _native_packet()
        assert packet.comparison_cohort.members, (
            "the native cohort is empty, so its zero is truthful and this "
            "pin measures nothing"
        )
        assert packet.comparison_cohort.authorization_filtered_count == 0, (
            "the native cohort now reports a filtered count -- the "
            "CHAOS-3620 per-site filtered-count record must be updated"
        )

    def test_the_native_evidence_count_is_real_and_can_be_non_zero(self) -> None:
        """The native site that genuinely measures.

        ``_restrict_evidence_to_authorized`` drops evidence whose entity is
        outside the authorized set and returns the count it dropped. Driven
        with an entity the run is not authorized for, so the non-zero is
        observed rather than inferred from the function's shape.
        """

        packet = _native_packet()
        assert packet.evidence_coverage.authorization_filtered_count == 0, (
            "the baseline native run already filters evidence, so a non-zero "
            "below would not be attributable to this test's plant"
        )

        restricted = _native_packet_with_unauthorized_evidence()
        assert restricted.evidence_coverage.authorization_filtered_count >= 1, (
            "evidence outside the authorized set was not dropped and counted "
            "by the native arm; the count is "
            f"{restricted.evidence_coverage.authorization_filtered_count}"
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
        assert non_entities <= set(world.EVIDENCE_BY_SLUG) | {
            item.measurement_key for item in world.WORLD_MEASUREMENTS
        }, (
            "the declared set contains non-entity ids that are neither corpus "
            "evidence slugs nor measurement keys. Set containment, not "
            "intersection-nonempty: an intersection test passes while an "
            "UNKNOWN id rides alongside a known one, which is exactly the "
            f"shape a leak takes: {sorted(non_entities - set(world.EVIDENCE_BY_SLUG))[:5]}"
        )

    def test_every_evidence_attribution_the_oracle_reads_is_unsound(self) -> None:
        """SCOPE PIN for this lane's zero-leakage claim. Measured, not inherited.

        ``entity_sightings`` treats ``evidence_index[].evidence.entity_id`` as
        a **sighting of an entity** (``authorization.py:190``). The arm puts
        the observation's own canonical id there (``packet_builder.py:828``),
        which for corpus-originated evidence is an evidence *slug* — never
        the entity the world says that evidence is *about*.

        **WHICH CODE STATE THIS MEASURES, because two figures are in
        circulation and they must not be averaged.** This branch is cut from
        ``1ab76d955`` and contains **no** ``src/`` changes — in particular not
        the #1617 vocabulary fix. Pre-fix, ``entity_id`` is an observation
        slug or a measurement key on *every* slug-bearing entry, so **785 of
        785 is the defect's definition, not a rate**: CHAOS-3627's third
        vocabulary mismatch, seen from the sighting side.

        The #1617 verifier's 115/291 (~40%) was measured **on the fixed
        branch**, where ``entity_id`` is supposed to be an entity and the 40%
        is the *residual* mis-attribution of un-reached records. One defect
        family at two stages of repair, not two numbers for one defect.
        Averaging them would describe a state that has never existed.

        The 532 entries carrying measurement keys are a third vocabulary
        again, consistent with CHAOS-3627's second mismatch and covered by
        its declared-set fix.

        **Re-derive after the rebase onto the fix**, which is what the
        assertions below force rather than request.

        **Why this scopes the headline.** The dangerous direction is masking:
        evidence genuinely about a restricted entity is attributed to its own
        slug, so a sighting-based check files it under "not a known entity"
        rather than "unauthorized disclosure". This lane's full-packet walker
        widens the channels read — and cannot correct an attribution. Only the
        arm fix can.

        So the claim this lane makes is precisely: **zero canonical-id
        leakage, measured over sightings whose evidence attributions are
        known-unsound pre-CHAOS-3627; to be re-derived post-fix.** This test
        flips red at the same rebase that flips the pins above, which is what
        forces the re-derivation rather than letting the caveat decay into a
        footnote nobody re-checks.
        """

        sound = unsound = measurement_keys = 0
        for seed in _subject_seeds():
            packet = spine.investigate(seed).packet
            for entry in packet.evidence_coverage.evidence_index:
                record = world.EVIDENCE_BY_SLUG.get(entry.evidence.entity_id)
                if record is None:
                    # Measurement keys: a THIRD vocabulary. Counted rather
                    # than skipped -- review found that skipping let a
                    # partial vocabulary fix pass silently, because a fixed
                    # entry stops being a known slug and simply vanishes from
                    # the denominator.
                    measurement_keys += 1
                    continue
                if entry.evidence.entity_id == record.entity_id:
                    sound += 1
                else:
                    unsound += 1

        # CARDINALITY, not a direction. `unsound > 0 and sound == 0` was
        # tautological under a partial fix: entries that became sound would
        # leave the slug vocabulary entirely and be skipped, so both halves
        # stayed true while the defect shrank. Pinning the exact counts makes
        # any movement -- in either direction -- loud.
        assert (unsound, sound, measurement_keys) == (785, 0, 532), (
            "the mis-attribution census moved from the documented "
            f"785 unsound / 0 sound / 532 measurement-key entries to "
            f"{unsound} / {sound} / {measurement_keys}. The zero-leakage "
            "claim's scope has changed and must be RE-DERIVED; update the "
            "CHAOS-3620 mis-attribution record with the new census"
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
