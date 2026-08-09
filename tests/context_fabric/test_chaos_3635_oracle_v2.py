"""CHAOS-3635: the authorization oracle grows prose channels, provably.

chris unfroze this ticket on 2026-08-09 under one rule — **strengthening
only**: a change may turn passes into failures and never failures into
passes. This module is what makes that rule checkable rather than asserted.

The design the ticket names is honoured exactly: the CHAOS-3620 suite's own
disclosure walker is the reference implementation, and its planted
counterexamples are the acceptance set. Two independent things are therefore
proved here, and neither substitutes for the other:

* **the new channel finds what the old one missed** — the forged-label
  counterexample is executed, the v1 channels report the packet clean, and
  v2 names the leak. Prose is not enough for that claim: a strengthening
  whose evidence is only a docstring is a strengthening nobody can audit;
* **nothing that used to fail now passes** — ``is_clean`` implies
  ``is_clean_v1_channels_only`` over the whole sweep. That is the direction
  the rule constrains, and the implication is one-way on purpose.

``is_clean_v1_channels_only`` exists solely for this file's second claim. It
is not an escape hatch for a caller who finds the new channel inconvenient,
and a consumer reaching for it is asking to be blind to exactly the leak this
ticket was raised about.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.authorization import (
    audit_authorization,
    entity_sightings,
    prose_sightings,
    prose_sightings_in_text,
    restricted_vocabulary,
)

from . import chaos_3620_spine as spine

#: Seeds whose packets are the sweep. Chosen to span an authorized team, a
#: project, and a cross-tenant-adjacent subject, so the implication below is
#: measured over packets with genuinely different contents rather than over
#: one packet three times.
_SWEEP_SEEDS: tuple[str, ...] = (
    "team_cinder",
    "team_atlas",
    world.PROJ_IDENTITY_REWRITE,
    world.PROJ_ACR,
    world.PROJ_VERTEX,
)


def _forge(packet, **evidence_updates):
    """One indexed evidence entry, altered. The arm-shaped disclosure plant.

    Same helper the CHAOS-3620 suite uses, reproduced rather than imported
    because importing a private name from a sibling test module couples two
    suites that must be able to fail independently. The packet's shape does
    not change and it still validates, so the only variable is whether a
    check reads the field.
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


class TestTheAcceptanceSetIsRediscovered:
    """The CHAOS-3620 counterexamples, run against the oracle itself."""

    def test_a_restricted_label_in_prose_is_caught_by_v2_and_missed_by_v1(
        self,
    ) -> None:
        """The whole reason CHAOS-3635 exists, executed rather than described.

        A packet that never names ``proj_quarry`` but does say "Quarry
        Compliance" has disclosed the restricted project to any human reading
        the answer. No identifier-based check can see that, and the assertion
        below on ``is_clean_v1_channels_only`` is the executed proof that the
        pre-3635 oracle did not.
        """

        clean = spine.investigate("team_cinder").packet
        forged = _forge(clean, display_label="Quarry Compliance rollout notes")

        # The plant must be invisible to every id channel, or this test is
        # measuring the id walker and not the prose one.
        assert world.PROJ_QUARRY not in entity_sightings(forged), (
            "the plant leaked a canonical id, so it no longer isolates prose"
        )

        audit = audit_authorization(forged, world.PRINCIPAL_ANALYST)
        assert audit.is_clean_v1_channels_only, (
            "the pre-3635 channels now catch this, so the recorded RED "
            "evidence no longer demonstrates the miss this ticket fixes"
        )
        assert not audit.is_clean, "oracle v2 did not catch the prose leak"
        assert [f"{item.channel}:{item.token}" for item in audit.prose_disclosures] == [
            "label:Quarry Compliance"
        ]

    def test_a_restricted_evidence_slug_is_named_by_channel(self) -> None:
        """The second planted channel: identifiers that are not entity ids.

        ``wi_quarry_redacted`` is real corpus evidence whose subject the
        analyst cannot see. v1 catches it — as a *fabricated entity*, because
        it is not in ``ENTITIES_BY_ID`` — so this is not a miss. It is
        included because a strengthening that quietly stopped catching a case
        the acceptance set already covers would be a regression the
        implication test alone cannot see.
        """

        clean = spine.investigate("team_cinder").packet
        forged = _forge(clean, entity_id="wi_quarry_redacted")

        audit = audit_authorization(forged, world.PRINCIPAL_ANALYST)
        assert not audit.is_clean_v1_channels_only
        assert not audit.is_clean
        assert "evidence_slug:wi_quarry_redacted" in {
            f"{item.channel}:{item.token}" for item in audit.prose_disclosures
        }

    def test_the_unforged_packet_is_clean_on_both_channel_sets(self) -> None:
        """The control.

        An oracle that reported a disclosure on every packet would pass both
        tests above and mean nothing. This is also the test that fails if the
        new channel false-positives on the corpus's own serialization —
        which is exactly what the ambiguous-label exclusion exists to
        prevent, and what the sweep below re-checks at scale.
        """

        clean = spine.investigate("team_cinder").packet
        audit = audit_authorization(clean, world.PRINCIPAL_ANALYST)
        assert audit.prose_disclosures == ()
        assert audit.is_clean
        assert audit.is_clean_v1_channels_only


class TestStrengtheningOnly:
    @pytest.mark.parametrize("seed", _SWEEP_SEEDS)
    def test_v2_clean_implies_v1_clean(self, seed: str) -> None:
        """The unfreeze rule, as a one-way implication.

        Passes may become failures; failures may never become passes. So
        ``is_clean`` must imply ``is_clean_v1_channels_only`` and the
        converse must be free to fail — which the forged-label test above
        demonstrates it does.
        """

        audit = audit_authorization(
            spine.investigate(seed).packet, world.PRINCIPAL_ANALYST
        )
        if audit.is_clean:
            assert audit.is_clean_v1_channels_only, (
                f"{seed}: oracle v2 declared a packet clean that the v1 "
                "channels flagged; that is a weakening, which the CHAOS-3635 "
                "unfreeze forbids"
            )

    @pytest.mark.parametrize("seed", _SWEEP_SEEDS)
    def test_every_v1_field_is_unchanged_by_the_new_channel(self, seed: str) -> None:
        """The additive claim, field by field.

        ``prose_disclosures`` is computed from the packet's rendering and
        touches nothing else. Asserted rather than trusted because "additive"
        is the kind of claim that stays true until someone factors a shared
        helper out of two branches.
        """

        packet = spine.investigate(seed).packet
        audit = audit_authorization(packet, world.PRINCIPAL_ANALYST)
        sightings = entity_sightings(packet)
        visible = world.PRINCIPALS[world.PRINCIPAL_ANALYST].visible_entity_ids
        known = set(world.ENTITIES_BY_ID)

        expected_unauthorized = sorted(
            entity_id
            for entity_id in sightings
            if entity_id in known and entity_id not in visible
        )
        assert (
            sorted(item.entity_id for item in audit.unauthorized_disclosures)
            == expected_unauthorized
        )
        expected_fabricated = sorted(
            entity_id for entity_id in sightings if entity_id not in known
        )
        assert (
            sorted(item.entity_id for item in audit.fabricated_entities)
            == expected_fabricated
        )


class TestAgreementWithTheReferenceImplementation:
    @pytest.mark.parametrize("seed", _SWEEP_SEEDS)
    @pytest.mark.parametrize(
        "principal", [world.PRINCIPAL_ANALYST, world.PRINCIPAL_COMPLIANCE]
    )
    def test_the_oracle_and_the_3620_walker_agree(
        self, seed: str, principal: str
    ) -> None:
        """Two implementations, same inputs, same answer.

        The CHAOS-3620 suite keeps its walker deliberately — a reference
        implementation that has been deleted in favour of the thing it
        certifies is no longer a reference. This is the differential that
        makes keeping it worth something.
        """

        packet = spine.investigate(seed, principal=principal).packet
        assert sorted(
            f"{item.channel}:{item.token}"
            for item in prose_sightings(packet, principal)
        ) == sorted(spine.disclosures(packet, principal))


class TestTheResidualBlindSpotIsStatedNotHidden:
    def test_the_analysts_ambiguous_label_set_is_pinned(self) -> None:
        """The exclusion is auditable, and its contents are asserted.

        A restricted name that also occurs in material the caller may
        legitimately read cannot be matched without reporting a disclosure
        that is not one — ``Agent Context Runtime`` is the cross-tenant
        near-duplicate's label, verbatim identical to a project the analyst
        may see, and ``Core`` occurs inside ``platform core``, team_atlas's
        previous name.

        Both are genuine ambiguity rather than a defect in the rule. What
        would be a defect is leaving that in a field nobody reads, so the
        audit carries the set and this test pins it: a change to the corpus
        that alters what the walker is blind to has to be noticed here.
        """

        vocabulary = restricted_vocabulary(world.PRINCIPAL_ANALYST)
        assert sorted(vocabulary.ambiguous_labels) == [
            "Agent Context Runtime",
            "Core",
        ]
        audit = audit_authorization(
            spine.investigate("team_cinder").packet, world.PRINCIPAL_ANALYST
        )
        assert audit.ambiguous_labels_not_matched == (
            "Agent Context Runtime",
            "Core",
        )

    def test_a_label_only_leak_inside_the_ambiguous_set_is_NOT_caught(self) -> None:
        """The honest statement of what this oracle still cannot see.

        Kept executable rather than written as a caveat. If a future change
        makes this leak detectable, this test fails and the residual it
        documents has to be re-stated — which is the point: an inaccurate
        coverage claim is worse than an admitted gap, because a reader who
        sees "covered" stops checking.
        """

        clean = spine.investigate("team_cinder").packet
        forged = _forge(clean, display_label="Core")
        audit = audit_authorization(forged, world.PRINCIPAL_ANALYST)
        assert audit.prose_disclosures == (), (
            "the ambiguous-label residual has changed; re-state it rather "
            "than deleting this test"
        )


class TestNoPrincipalDefaultsToSeeingEverything:
    def test_an_unknown_principal_raises(self) -> None:
        """The tempting default empties the restricted set entirely."""

        with pytest.raises(KeyError):
            restricted_vocabulary("principal_does_not_exist")

    def test_the_text_scan_refuses_an_unknown_principal_too(self) -> None:
        with pytest.raises(KeyError):
            prose_sightings_in_text("Quarry Compliance", "principal_does_not_exist")

    def test_the_text_scan_finds_a_restricted_label_in_bare_text(self) -> None:
        """The surface CHAOS-3647's retrieval leg is measured on.

        It ranks subjects and never assembles a packet, so
        ``prose_sightings`` has nothing to walk. "We could not check because
        there was no packet" is how a channel stays unmeasured until it
        leaks.
        """

        found = prose_sightings_in_text(
            "proj_alpha\nQuarry Compliance",
            world.PRINCIPAL_ANALYST,
            include_evidence_slugs=False,
        )
        assert [f"{item.channel}:{item.token}" for item in found] == [
            "label:Quarry Compliance"
        ]
