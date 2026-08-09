"""CHAOS-3617 PR2: structural driver discovery, and what it refuses to assert.

The capability the correction hinges on. The native arm cannot assert a
driver at all; whether this one earns principal standing under the frozen
rules is the trial's live question, so the tests here are mostly about the
ways it must *fail* to.

Four defects were found building this module and every one was invisible to
the type checker, the linter and the existing suite — they showed up only
when real corpus output was printed. Each has a test below named after what
it did, because the next person to refactor this needs to know which
innocuous-looking simplification reintroduces which false claim:

* support scoped to the cause *entity* rather than the asserting *edge*,
  which promoted the corpus's planted false dependency to PRINCIPAL DRIVER;
* child candidates taken from any ``parent_of`` step on any path, so a
  portfolio became an "open child" of a project it merely co-occurred with;
* ``not _is_complete(...)`` reading a service's absent completion concept as
  "unfinished", making every dependency a blocker;
* a trust lookup defaulting to ``canonical``, which is what kept the first
  defect invisible.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    DriverCategory,
    DriverExclusionReason,
    DriverRole,
    DriverStanding,
    InvestigationOutcome,
    PacketLimitationKind,
    RelationshipType,
)
from dev_health_ops.api.dev.investigation_contract.vocabulary import (
    SUPPORTED_OUTCOMES,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.drivers import (
    MEASUREMENT_ONLY_CATEGORIES,
    StandingMechanism,
    discover_drivers,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.records import (
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
    RelationshipRecord,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    GraphEntityKind,
    GraphObservationKind,
)

_PROBE_ORG = "org_driver_probe"
_PROBE_NOW = datetime(2026, 8, 8, tzinfo=UTC)


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


def _readout(projection, subject: str, principal: str, *, max_hops: int = 2):
    grant = adapter.authorized_entity_ids_for(principal)
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=projection.org_id,
            seed_canonical_ids=[subject],
            authorized_entity_ids=sorted(grant),
            max_hops=max_hops,
        )
    )


def _findings(projection, subject: str, principal: str, *, max_hops: int = 2):
    readout = _readout(projection, subject, principal, max_hops=max_hops)
    return discover_drivers(readout, subject, as_of=world.TRIAL_NOW)[0]


def _by_id(findings):
    return {item.driver_id: item for item in findings}


# --------------------------------------------------------------------------
# The capability: standing that is earned
# --------------------------------------------------------------------------


class TestPrincipalStandingIsEarned:
    def test_a_real_blocking_chain_reaches_principal_driver(self, helio) -> None:
        """The trial's live question, answered in the affirmative.

        ``proj_identity_rewrite`` is blocked by an open work unit, the
        blockage is recorded by canonical CI and work-item records, and the
        edge is current. That is everything the frozen contract requires of a
        principal driver — lineage explaining the mechanism, evidence
        supporting it, current relevance — and none of it is a number.
        """

        found = _by_id(
            _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        )
        blocker = found["drv_block_wu_authcore_release"]
        assert blocker.standing is DriverStanding.PRINCIPAL_DRIVER
        assert blocker.role is DriverRole.DRIVER
        assert blocker.category is DriverCategory.EXTERNAL_BLOCKER
        assert blocker.path_ids
        assert blocker.evidence_ids
        assert blocker.mechanism == StandingMechanism.STRUCTURAL

    def test_an_open_child_under_a_complete_declaration_is_the_driver(
        self, helio
    ) -> None:
        """The declared-versus-actual divergence, as a cause rather than a note.

        ``proj_ledger_migration`` is declared complete with one of its three
        children still open. The honest answer to "why is this not finished"
        is that child.
        """

        found = _by_id(
            _findings(helio, "proj_ledger_migration", world.PRINCIPAL_ANALYST)
        )
        child = found["drv_open_wu_ledger_backfill"]
        assert child.standing is DriverStanding.PRINCIPAL_DRIVER
        assert child.category is DriverCategory.SCOPE_CHANGE
        assert "wi_ledger_backfill_open" in child.evidence_ids

    def test_release_incompleteness_is_distinguished_from_implementation(
        self, helio
    ) -> None:
        """``proj_pulse`` shipped; its operational controls did not.

        The corpus is explicit that this is release-incomplete rather than
        implementation-incomplete, and the driver has to be the open runbook
        rather than the merged pull request.
        """

        found = _by_id(_findings(helio, "proj_pulse", world.PRINCIPAL_ANALYST))
        runbook = found["drv_open_wu_pulse_runbook"]
        assert runbook.standing is DriverStanding.PRINCIPAL_DRIVER
        assert "wi_pulse_runbook_open" in runbook.evidence_ids

    def test_principal_standing_is_withheld_when_two_candidates_tie(self) -> None:
        """A coin toss presented as a judgment is worse than reporting both.

        Two equally-supported declared blockers make "the principal driver"
        arbitrary. The frozen contract would accept either; this refuses to
        pick, and both stay contributing.
        """

        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_one",)),
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_two", ("obs_two",)),
            ),
            observations=(("obs_one", "wu_one"), ("obs_two", "wu_two")),
        )
        findings = _probe_findings(projection, grant, "proj_subject")
        standings = {item.driver_id: item.standing for item in findings}
        assert standings == {
            "drv_block_wu_one": DriverStanding.CONTRIBUTING_DRIVER,
            "drv_block_wu_two": DriverStanding.CONTRIBUTING_DRIVER,
        }

        # The control: break the tie with a weaker second candidate and the
        # stronger one is promoted. Without this, "never promotes" would pass
        # for an implementation that promotes nothing at all.
        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_one",)),
                ("proj_subject", RelationshipType.DEPENDS_ON, "dep_two", ("obs_two",)),
            ),
            observations=(("obs_one", "wu_one"), ("obs_two", "dep_two")),
        )
        promoted = _by_id(_probe_findings(projection, grant, "proj_subject"))
        assert promoted["drv_block_wu_one"].standing is DriverStanding.PRINCIPAL_DRIVER


# --------------------------------------------------------------------------
# Symptom versus driver
# --------------------------------------------------------------------------


class TestOrientationIsReadFromDirectionNotTraversalOrder:
    """The seeding-direction sweep. The suite's largest blind spot, closed.

    Every structural driver test seeded the *blocked* end of a directed
    edge, so no test and none of eight mutations ever traversed one in
    reverse — and the arm asserted, at PRINCIPAL standing with lineage and
    canonical evidence behind it, that a blocked project was the driver of
    its own blocker.

    ``PathStep`` separates traversal order from canonical orientation
    precisely so this is detectable. The rules now read ``direction``; these
    tests seed BOTH ends of every directed family so a regression cannot
    hide on the side nobody looks at.
    """

    @pytest.mark.parametrize(
        ("blocked", "blocker"),
        [("proj_identity_rewrite", "wu_authcore_release")],
    )
    def test_a_blocker_is_never_a_driver_of_the_thing_it_blocks(
        self, helio, blocked, blocker
    ) -> None:
        """Seeded from the BLOCKER end: the blocked thing is not a cause.

        This is the exact reproduction. Before the fix it returned
        ``drv_block_proj_identity_rewrite`` at ``PRINCIPAL_DRIVER``.
        """

        from_blocker = _by_id(_findings(helio, blocker, world.PRINCIPAL_ANALYST))
        inverted = [name for name in from_blocker if name == f"drv_block_{blocked}"]
        assert not inverted, (
            f"{blocked} was reported as a driver of its own blocker "
            f"{blocker}; causality is inverted"
        )

    def test_the_same_edge_still_yields_the_driver_from_the_blocked_end(
        self, helio
    ) -> None:
        """The control, and the half that must not regress while fixing the other.

        A rule that refused both directions would pass the test above while
        destroying the capability.
        """

        from_blocked = _by_id(
            _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        )
        assert (
            from_blocked["drv_block_wu_authcore_release"].standing
            is DriverStanding.PRINCIPAL_DRIVER
        )

    def test_a_parent_is_never_the_open_child_of_its_own_child(self) -> None:
        """The same flaw in the child rule, on both orientations.

        ``parent_of`` and ``contributes_to`` each declare which end is the
        larger unit. Taking "the end that is not the subject" made a parent
        the child of its own child whenever the walk arrived from below.
        """

        projection, grant = _probe_world(
            relationships=(
                ("wu_one", RelationshipType.CONTRIBUTES_TO, "proj_subject", ("obs_c",)),
            ),
            observations=(("obs_c", "wu_one"),),
            subject_status="complete",
        )

        # Seeded from the PARENT: the open child is found, as it should be.
        from_parent = _by_id(_probe_findings(projection, grant, "proj_subject"))
        assert "drv_open_wu_one" in from_parent

        # Seeded from the CHILD: the parent must not become an open child of
        # its own child. Before the fix, "the end that is not the subject"
        # made exactly that finding.
        from_child = _by_id(_probe_findings(projection, grant, "wu_one"))
        leaked = sorted(name for name in from_child if name.startswith("drv_open_"))
        assert not leaked, leaked

    def test_every_asserted_driver_agrees_with_canonical_orientation(
        self, helio
    ) -> None:
        """The sweep. Seed every authorized subject, check every assertion.

        For each asserted structural driver, the subject must be the
        canonical SOURCE of a blocking edge whose target is the named cause
        — checked against the projection's own edges, which are stored in
        canonical orientation and never in traversal order.
        """

        projection = helio
        canonical = {
            (edge.source_canonical_id, edge.relationship, edge.target_canonical_id)
            for edge in projection.edges
        }
        checked = 0
        for principal in (world.PRINCIPAL_ANALYST, world.PRINCIPAL_COMPLIANCE):
            grant = adapter.authorized_entity_ids_for(principal)
            for subject in sorted(grant):
                for item in _findings(helio, subject, principal):
                    if not item.is_asserted or not item.driver_id.startswith(
                        "drv_block_"
                    ):
                        continue
                    checked += 1
                    assert any(
                        (subject, relationship, item.cause_id) in canonical
                        for relationship in (
                            RelationshipType.BLOCKED_BY,
                            RelationshipType.DEPENDS_ON,
                        )
                    ), (subject, item.driver_id, item.cause_id)
        assert checked, "no asserted blocking driver was produced; vacuous"


class TestSymptomsAreNeverDrivers:
    def test_a_status_change_is_classified_as_a_symptom(self, helio) -> None:
        found = _by_id(
            _findings(helio, "proj_ledger_migration", world.PRINCIPAL_ANALYST)
        )
        symptom = found["drv_symptom_sc_ledger_declared_complete"]
        assert symptom.role is DriverRole.SYMPTOM

    def test_a_symptom_is_excluded_once_its_cause_is_on_the_table(self, helio) -> None:
        found = _by_id(
            _findings(helio, "proj_ledger_migration", world.PRINCIPAL_ANALYST)
        )
        symptom = found["drv_symptom_sc_ledger_declared_complete"]
        assert symptom.standing is DriverStanding.EXCLUDED
        assert (
            symptom.exclusion_reason
            is DriverExclusionReason.SYMPTOM_OF_ANOTHER_CANDIDATE
        )

    def test_a_symptom_with_no_explanation_survives_as_a_candidate(self, helio) -> None:
        """Deleting the one observation a reader had would not be honesty.

        ``proj_acr`` has a status observation and no structural cause the
        graph can see. The symptom stays reportable — as a candidate, never
        as an assertion.
        """

        found = _findings(helio, "proj_acr", world.PRINCIPAL_ANALYST)
        symptoms = [item for item in found if item.role is DriverRole.SYMPTOM]
        assert symptoms
        assert all(item.standing is DriverStanding.CANDIDATE_ONLY for item in symptoms)

    def test_no_symptom_anywhere_in_the_corpus_holds_asserted_standing(
        self, helio
    ) -> None:
        """The whole-world sweep. One mislabelled symptom is one false cause.

        A per-subject test can pass while some other subject promotes a
        symptom, so this walks every authorized subject in the tenant.
        """

        seen = 0
        for principal in (world.PRINCIPAL_ANALYST, world.PRINCIPAL_COMPLIANCE):
            grant = adapter.authorized_entity_ids_for(principal)
            for subject in sorted(grant):
                for item in _findings(helio, subject, principal):
                    if item.role is not DriverRole.SYMPTOM:
                        continue
                    seen += 1
                    assert not item.is_asserted, (subject, item.driver_id)
        assert seen, "no symptom was produced at all; this sweep was vacuous"


# --------------------------------------------------------------------------
# The four defects, each named after what it did
# --------------------------------------------------------------------------


class TestPoisonedLinkageIsRefused:
    def test_the_planted_false_dependency_never_becomes_a_driver(self, helio) -> None:
        """The corpus's adversarial case, and the defect it caught.

        ``proj_meridian blocked_by dep_authcore`` is asserted only in an
        untrusted planning note. Both endpoints are real canonical entities,
        which is exactly what makes the fabrication plausible — nothing about
        the edge's shape marks it false.
        """

        found = _by_id(_findings(helio, "proj_meridian", world.PRINCIPAL_ANALYST))
        claim = found["drv_block_dep_authcore"]
        assert claim.standing is DriverStanding.EXCLUDED
        assert (
            claim.exclusion_reason is DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED
        )
        assert "doc_false_dependency_claim" in claim.conflicting_evidence_ids
        assert claim.evidence_ids == ()

    def test_it_is_refused_for_the_TRUST_reason_not_by_disappearing(
        self, helio
    ) -> None:
        """The wrong-reason catch, pinned so it cannot come back.

        An earlier version made this claim vanish because ``dep_authcore``
        carries no declared status — not because the arm judged the record
        untrustworthy. It passed, for a reason that would have evaporated the
        moment the corpus gave that dependency a status. The claim must be
        *present and excluded*, not absent.
        """

        found = _by_id(_findings(helio, "proj_meridian", world.PRINCIPAL_ANALYST))
        assert "drv_block_dep_authcore" in found, (
            "the false claim is not being considered at all, so the trust "
            "guard is not what is rejecting it"
        )

    def test_support_is_scoped_to_the_asserting_edge_not_the_entity(
        self, helio
    ) -> None:
        """The defect that promoted the false claim to principal driver.

        ``dep_authcore`` is a genuine dependency of four real projects, so a
        canonical record attached to one of those TRUE edges was read as
        support for the FABRICATED one. The two live in the same graph
        neighbourhood and only the edge distinguishes them.
        """

        readout = _readout(helio, "proj_meridian", world.PRINCIPAL_ANALYST)
        meridian = _by_id(
            discover_drivers(readout, "proj_meridian", as_of=world.TRIAL_NOW)[0]
        )
        # The canonical record is REACHED and TRUSTED in this very readout --
        # it asserts ``proj_identity_rewrite depends_on dep_authcore``, a real
        # edge in the same neighbourhood. Asserted rather than waved past: an
        # earlier version wrote ``any(...) or True``, which could not tell
        # "the scoping refused it" from "the record was never there", and
        # would have stayed green if the corpus stopped carrying it at all.
        visible = {item.canonical_id: item for item in readout.observations}
        assert "wg_authcore_shared" in visible, (
            "the canonical record is not even in the readout, so its absence "
            "below is not the scoping refusing it"
        )
        assert visible["wg_authcore_shared"].attributes["corpus_trust"] == "canonical"
        # ...and it must NOT appear as support for the fabricated one.
        assert (
            "wg_authcore_shared" not in meridian["drv_block_dep_authcore"].evidence_ids
        )


class TestOnlyARecordAboutTheLinkageMayVouchForIt:
    """The residue of the scoping defect, and the half scoping did not close.

    Scoping support to the asserting *edge* stopped a canonical record on a
    neighbouring edge from vouching. Adversarial review then made the
    fabricated edge **cite** a canonical record that is about neither of its
    endpoints, and the claim reached PRINCIPAL DRIVER again — because trust
    was read off the record and never checked against what the record is
    about. A canonical record's trust says its own content can be relied on.
    It says nothing about an edge that merely names it.

    The rule has to be honest in both directions, which is why the tests come
    in pairs: a record about a third entity may not establish a linkage, and
    a linkage something else already vouched for still keeps such a record as
    support — the corpus has real ones, and deleting genuine evidence would
    trade a false claim for a false absence.
    """

    @staticmethod
    def _world(*, attached_to: str):
        """The fabricated citation, with the cited record's subject varied.

        ``proj_other`` sits two hops out so the record is genuinely REACHED
        and genuinely trusted -- otherwise its rejection would be the
        traversal never returning it rather than the rule refusing it.
        """

        return _probe_world(
            relationships=(
                (
                    "proj_subject",
                    RelationshipType.BLOCKED_BY,
                    "wu_one",
                    ("obs_elsewhere",),
                ),
                ("proj_subject", RelationshipType.DEPENDS_ON, "dep_two", ()),
                ("proj_other", RelationshipType.DEPENDS_ON, "dep_two", ()),
            ),
            observations=(("obs_elsewhere", attached_to),),
        )

    def test_a_canonical_record_about_a_third_entity_cannot_establish_a_linkage(
        self,
    ) -> None:
        """The reproduction. This was PRINCIPAL_DRIVER before the endpoint rule."""

        projection, grant = self._world(attached_to="proj_other")
        readout = _probe_readout(projection, grant, "proj_subject", max_hops=3)
        # Anti-vacuity: the record really is in this readout, really is
        # canonical, and really is about neither endpoint.
        visible = {item.canonical_id: item for item in readout.observations}
        assert "obs_elsewhere" in visible, "the record was never reached; vacuous"
        assert visible["obs_elsewhere"].attributes["corpus_trust"] == "canonical"
        assert visible["obs_elsewhere"].subject_canonical_ids == ("proj_other",)

        claim = _by_id(discover_drivers(readout, "proj_subject", as_of=_PROBE_NOW)[0])[
            "drv_block_wu_one"
        ]
        assert claim.standing is DriverStanding.EXCLUDED, (
            "a canonical record vouched for a linkage it is not about, which "
            "is how the corpus's planted false dependency reached principal "
            "standing"
        )
        assert claim.evidence_ids == ()
        # Present and excluded: the reader is told what was cited and why it
        # could not be used, rather than being told nothing was cited.
        assert claim.conflicting_evidence_ids == ("obs_elsewhere",)
        assert claim.conflict_detail is not None
        assert "neither end of the linkage" in claim.conflict_detail

    def test_the_same_record_about_an_endpoint_does_establish_it(self) -> None:
        """The control, and the one that catches a rule tightened too far.

        Identical world, identical record, identical trust — attached to the
        blocker instead. A rule that refused both would pass the test above
        while destroying the capability the arm exists to demonstrate.
        """

        projection, grant = self._world(attached_to="wu_one")
        found = _by_id(_probe_findings(projection, grant, "proj_subject", max_hops=3))
        claim = found["drv_block_wu_one"]
        assert claim.standing is DriverStanding.PRINCIPAL_DRIVER
        assert claim.evidence_ids == ("obs_elsewhere",)

    def test_a_third_entity_record_still_rides_along_with_a_real_voucher(
        self, helio
    ) -> None:
        """Corroboration is kept; it just cannot be the thing that establishes.

        The corpus models exactly this: ``proj_identity_rewrite blocked_by
        wu_authcore_release`` cites the work-item record about the blocker
        AND a CI run recorded against ``repo_identity``, which is neither
        endpoint. The CI run is genuine evidence and must survive — a rule
        that only kept endpoint records would trade a false claim for a false
        absence and would quietly lower the arm's evidence recall.
        """

        blocker = _by_id(
            _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        )["drv_block_wu_authcore_release"]
        assert blocker.standing is DriverStanding.PRINCIPAL_DRIVER
        assert "wi_authcore_release_open" in blocker.evidence_ids  # the voucher
        assert "ci_identity_blocked" in blocker.evidence_ids  # the corroborator


class TestAttributionNeedsAReaderThatKnowsWhatARecordIsAbout:
    """The live reader cannot recover attachment, and must say so.

    Making the endpoint rule conditional on attachments being present would
    turn it into a silent no-op on precisely the reader that cannot perform
    it — the original defect, live-only. Declaring the capability instead
    makes the gap an attributable state: nothing is attributed, and the
    exclusion says why in its own words rather than claiming the caller may
    not see the support.

    CHAOS-3617's H3 residual is what makes this reachable, and it is stated
    in the PR body as owed rather than closed.
    """

    def _readout_without_attachment(self):
        import dataclasses

        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_one",)),
            ),
            observations=(("obs_one", "wu_one"),),
        )
        honest = _probe_readout(projection, grant, "proj_subject")
        # The live reader's shape, reproduced exactly: the observations come
        # back with no subjects, and the reader declares it cannot recover
        # them. Both halves, because either alone is a different world.
        return honest, dataclasses.replace(
            honest,
            observations=(),
            observation_attachment_available=False,
        )

    def test_nothing_is_attributed_when_attachment_cannot_be_read(self) -> None:
        honest, blind = self._readout_without_attachment()
        claim = _by_id(discover_drivers(blind, "proj_subject", as_of=_PROBE_NOW)[0])[
            "drv_block_wu_one"
        ]
        assert claim.standing is DriverStanding.EXCLUDED
        assert claim.evidence_ids == ()

    def test_the_refusal_names_the_capability_and_not_authorization(self) -> None:
        """ "You may not see it" is a claim about the caller's grant.

        Reporting UNAUTHORIZED_EVIDENCE here would assert something the arm
        has no basis for: the record was not withheld from this caller, it
        was unreadable to this reader.
        """

        _, blind = self._readout_without_attachment()
        claim = _by_id(discover_drivers(blind, "proj_subject", as_of=_PROBE_NOW)[0])[
            "drv_block_wu_one"
        ]
        assert (
            claim.exclusion_reason is DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED
        )
        assert claim.exclusion_reason is not (
            DriverExclusionReason.UNAUTHORIZED_EVIDENCE
        )
        assert claim.conflict_detail is not None
        assert "no observation attachment" in claim.conflict_detail

    def test_the_same_world_resolves_on_a_reader_that_can_read_attachment(
        self,
    ) -> None:
        """The control. Otherwise the refusal could be the world being empty."""

        honest, _ = self._readout_without_attachment()
        assert honest.observation_attachment_available is True
        claim = _by_id(discover_drivers(honest, "proj_subject", as_of=_PROBE_NOW)[0])[
            "drv_block_wu_one"
        ]
        assert claim.standing is DriverStanding.PRINCIPAL_DRIVER


class TestChildCandidatesMustBeAdjacent:
    def test_a_portfolio_reached_further_along_a_path_is_not_an_open_child(
        self, helio
    ) -> None:
        """The co-occurrence defect.

        Any ``parent_of`` step anywhere on a discovered path used to produce
        a candidate, so ``pf_platform`` became an open child of
        ``proj_ledger_migration`` because both appeared on the same walk.
        Adjacency to the subject is what "child of" means.
        """

        found = _by_id(
            _findings(helio, "proj_ledger_migration", world.PRINCIPAL_ANALYST)
        )
        assert "drv_open_pf_platform" not in found
        # Anti-vacuity: the rule still finds the child that IS adjacent.
        assert "drv_open_wu_ledger_backfill" in found

    def test_an_open_unit_two_hops_out_is_not_a_child_of_the_subject(self) -> None:
        """The adjacency check on its own, isolated from the status rule.

        In the corpus the two guards overlap: the non-adjacent entity that
        leaked in was a portfolio, which carries no declared status, so the
        status rule would have caught it anyway — and the mutation that
        disables adjacency alone SURVIVED because of that overlap. A guard
        whose only evidence comes from a case another guard also catches is
        not proven.

        This constructs the case where adjacency is the only thing standing
        between the subject and a fabricated child: an open work unit, two
        hops out, contributing to the subject's blocker rather than to the
        subject.
        """

        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.DEPENDS_ON, "dep_two", ("obs_one",)),
                ("proj_other", RelationshipType.DEPENDS_ON, "dep_two", ("obs_two",)),
                (
                    "wu_two",
                    RelationshipType.CONTRIBUTES_TO,
                    "proj_other",
                    ("obs_three",),
                ),
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_four",)),
            ),
            observations=(
                ("obs_one", "dep_two"),
                ("obs_two", "proj_other"),
                ("obs_three", "wu_two"),
                ("obs_four", "wu_one"),
            ),
            subject_status="complete",
        )
        found = _by_id(_probe_findings(projection, grant, "proj_subject", max_hops=3))
        # The subject has NO adjacent child edge, so it may have no
        # open-child candidate at all. Asserted over the whole family rather
        # than by naming one id: which non-adjacent entity leaks depends on
        # which end of the step the rule happens to pick, and a version of
        # this test that named the wrong one passed while the guard was
        # disabled — the mutation reported SURVIVED and was right to.
        leaked = sorted(name for name in found if name.startswith("drv_open_"))
        assert not leaked, leaked
        # Anti-vacuity: the walk really did reach the non-adjacent
        # ``contributes_to`` edge, and the subject really is declared
        # complete, so the open-child rule ran and found nothing to attach.
        assert "drv_block_wu_one" in found
        assert "drv_block_dep_two" in found


class TestAbsentStatusIsNotEvidenceOfIncompleteness:
    def test_a_service_dependency_is_not_a_blocker_merely_for_existing(
        self, helio
    ) -> None:
        """``not _is_complete(...)`` made every dependency a driver.

        ``svc_auth_gateway`` has no completion concept at all. Reading that
        silence as "unfinished" turned the identity rewrite's own service
        into a cause of its delay.
        """

        found = _by_id(
            _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        )
        assert "drv_block_svc_auth_gateway" not in found

    def test_a_declared_blocker_still_counts_without_a_status_of_its_own(
        self, helio
    ) -> None:
        """The asymmetry, and why it is not a loophole.

        ``blocked_by`` IS the provider asserting that something blocks, so
        the far end needs no status. ``depends_on`` needs the far end
        declared open, because what makes a dependency a *pressure* is that
        it is unfinished. Collapsing the two either loses real blockers or
        makes every dependency a driver — the corpus has a case for each.
        """

        found = _by_id(_findings(helio, "proj_meridian", world.PRINCIPAL_ANALYST))
        claim = found["drv_block_dep_authcore"]
        assert claim.category is DriverCategory.EXTERNAL_BLOCKER


class TestTrustHasNoDefault:
    def test_a_trusted_record_in_the_same_shape_resolves(self) -> None:
        """The control, so "always excluded" cannot pass for the guard."""

        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_ok",)),
            ),
            observations=(("obs_ok", "wu_one"),),
            trust="canonical",
        )
        found = _by_id(_probe_findings(projection, grant, "proj_subject"))
        assert found["drv_block_wu_one"].standing is DriverStanding.PRINCIPAL_DRIVER

    def test_an_untrusted_record_never_reads_as_canonical(self, helio) -> None:
        """The default that hid a marshalling failure.

        ``corpus_trust`` defaulted to ``"canonical"`` when absent, so a
        readback bug that stripped the attribute turned every untrusted note
        in the world into a canonical one — silently, and in the direction
        that manufactures false claims.
        """

        # ``None`` is the case the default hid: the record carries NO trust
        # attribute at all. It has to be asserted separately, because the
        # present-but-untrusted case passes whether or not a default exists.
        for trust in ("untrusted_content", None):
            projection, grant = _probe_world(
                relationships=(
                    (
                        "proj_subject",
                        RelationshipType.BLOCKED_BY,
                        "wu_one",
                        ("obs_untrusted",),
                    ),
                ),
                observations=(("obs_untrusted", "wu_one"),),
                trust=trust,
            )
            found = _by_id(_probe_findings(projection, grant, "proj_subject"))
            claim = found["drv_block_wu_one"]
            assert claim.standing is DriverStanding.EXCLUDED, trust
            assert (
                claim.exclusion_reason
                is DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED
            ), trust


# --------------------------------------------------------------------------
# Currency, authorization, and what cannot be built at all
# --------------------------------------------------------------------------


class TestDeclaredStatusComesOnlyFromTheReadout:
    """The status the rules read is the traversal's, and there is no other.

    ``discover_drivers`` used to take ``entity_attributes``, documented as
    something a caller could not use to supply a different status than the
    traversal read — while doing ``setdefault(id, {}).update(extra)``, which
    overrode it. Adversarial review reproduced the consequence on the corpus:
    declaring the blocker ``complete`` deleted the principal driver, so a
    caller could delete the arm's own judgment by asserting a status the graph
    never held.

    Checked in two ways on purpose. The behavioural half proves declared
    status decides anything at all — without it, "no override channel" would
    hold trivially for an arm that ignored status. The structural half proves
    the channel is *absent* rather than validated, because an absent parameter
    cannot be widened by a later refactor.
    """

    #: Parameter names through which a caller could inject entity state.
    #: Every spelling, not just the one that existed: the rule is about the
    #: capability, and renaming it must not be a way around it.
    _ATTRIBUTE_PARAMETERS = frozenset(
        {
            "attributes",
            "entity_attributes",
            "declared_status",
            "declared_statuses",
            "entity_status",
            "statuses",
            "overrides",
            "extra_attributes",
        }
    )

    def _blocker_findings(self, blocker_status: str):
        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_one",)),
            ),
            observations=(("obs_one", "wu_one"),),
            statuses={"wu_one": blocker_status},
        )
        return _by_id(_probe_findings(projection, grant, "proj_subject"))

    def test_the_status_the_graph_holds_decides_the_finding(self) -> None:
        """The anti-vacuity half, both directions."""

        assert (
            self._blocker_findings("in_progress")["drv_block_wu_one"].standing
            is DriverStanding.PRINCIPAL_DRIVER
        )
        assert "drv_block_wu_one" not in self._blocker_findings("complete")

    def test_discovery_exposes_no_channel_for_supplying_status(self) -> None:
        """The reproduction, as a property rather than as a case.

        A test that merely passed ``entity_attributes`` and expected a raise
        would keep the channel alive and prove only that today's guard fires.
        """

        import inspect

        parameters = set(inspect.signature(discover_drivers).parameters)
        assert not parameters & self._ATTRIBUTE_PARAMETERS, sorted(
            parameters & self._ATTRIBUTE_PARAMETERS
        )
        # Anti-vacuity: the signature really was read, and the parameters the
        # function IS supposed to take are still there.
        assert {"readout", "subject_id", "as_of", "max_candidates"} <= parameters

    def test_the_removed_keyword_is_refused_at_the_call(self) -> None:
        """The runtime half. A signature check alone would miss ``**kwargs``."""

        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_one",)),
            ),
            observations=(("obs_one", "wu_one"),),
        )
        readout = _probe_readout(projection, grant, "proj_subject")
        with pytest.raises(TypeError, match="entity_attributes"):
            discover_drivers(
                readout,
                "proj_subject",
                as_of=_PROBE_NOW,
                # The keyword the defect travelled through. mypy rejects it
                # too, which is the point -- the ignore is what lets the
                # RUNTIME refusal be observed rather than assumed.
                entity_attributes={  # type: ignore[call-arg]
                    "wu_one": {"declared_status": "complete"}
                },
            )

    def test_the_docstring_no_longer_claims_a_guarantee_it_lacks(self) -> None:
        """The half that was worse than the behaviour.

        The old text told a reader that callers could not supply a different
        status, which is the sentence a reviewer would have trusted instead of
        reading the two lines below it.
        """

        assert discover_drivers.__doc__ is not None
        # Whitespace-normalised: the sentences under test wrap, and a test
        # that broke on a re-wrap would be edited away rather than fixed.
        prose = " ".join(discover_drivers.__doc__.split())
        assert "cannot quietly supply a different status" not in prose
        assert "comes from the readout and from nowhere else" in prose


class TestCurrency:
    def test_a_dependency_closed_before_the_window_is_excluded_not_hidden(
        self, helio
    ) -> None:
        """``proj_pulse depends_on dep_ratelimitd`` closed in valid time.

        Reported as considered-and-rejected rather than dropped: "why isn't
        the old dependency the answer" is a question the packet exists to
        answer, and an absence answers nothing.
        """

        found = _by_id(_findings(helio, "proj_pulse", world.PRINCIPAL_ANALYST))
        historical = found["drv_block_dep_ratelimitd"]
        assert historical.standing is DriverStanding.EXCLUDED
        assert (
            historical.exclusion_reason is DriverExclusionReason.NOT_CURRENTLY_RELEVANT
        )

    def test_it_is_excluded_for_CURRENCY_not_for_want_of_a_status(self, helio) -> None:
        """The second wrong-reason catch.

        ``dep_ratelimitd`` carries no declared status either, so the
        dependency rule would have dropped it silently. The currency guard
        has to be the thing that rejects it, or the guard is untested and the
        historical edge is refused by accident.
        """

        found = _by_id(_findings(helio, "proj_pulse", world.PRINCIPAL_ANALYST))
        assert "drv_block_dep_ratelimitd" in found


class TestAuthorization:
    def test_evidence_the_caller_cannot_see_is_reported_as_withheld(self) -> None:
        """``UNAUTHORIZED_EVIDENCE``, on a constructed world.

        The corpus has no case for this — an edge referencing an observation
        the traversal withheld — so it is built here rather than added to the
        corpus, which belongs to CHAOS-3616.

        The distinction matters because "nothing supports this" and "you may
        not see what supports this" are identical in a packet and opposite in
        meaning.
        """

        projection, _ = _probe_world(
            relationships=(
                (
                    "proj_subject",
                    RelationshipType.BLOCKED_BY,
                    "wu_one",
                    ("obs_secret",),
                ),
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_restricted", ()),
            ),
            observations=(("obs_secret", "wu_restricted"),),
        )
        # The grant deliberately omits ``wu_restricted``, so the traversal
        # never reaches it and withholds the observation hanging off it —
        # while the edge backing ``wu_one`` still references that observation
        # by id.
        grant = ("proj_subject", "wu_one")
        found = _by_id(_probe_findings(projection, grant, "proj_subject"))
        claim = found["drv_block_wu_one"]
        assert claim.standing is DriverStanding.EXCLUDED
        assert claim.exclusion_reason is DriverExclusionReason.UNAUTHORIZED_EVIDENCE

    def test_the_same_shape_resolves_when_the_support_is_visible_and_its_own(
        self,
    ) -> None:
        """The control. Otherwise 'withheld' could mean 'never existed'.

        The record has to be *about* the linkage as well as visible, which is
        the H1 rule and is why this control cites a record attached to
        ``wu_one`` rather than the one attached to ``wu_restricted``. An
        earlier version of this control simply widened the grant over the
        withheld world, and after the endpoint rule landed it went red — for
        the right reason: making a record about a third entity visible does
        not make it a statement about this edge. That case is now asserted in
        its own right below rather than lost.
        """

        projection, _ = _probe_world(
            relationships=(
                (
                    "proj_subject",
                    RelationshipType.BLOCKED_BY,
                    "wu_one",
                    ("obs_own",),
                ),
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_restricted", ()),
            ),
            observations=(("obs_own", "wu_one"),),
        )
        grant = ("proj_subject", "wu_one", "wu_restricted")
        found = _by_id(_probe_findings(projection, grant, "proj_subject"))
        assert found["drv_block_wu_one"].standing is not DriverStanding.EXCLUDED

    def test_a_visible_record_about_a_third_entity_still_does_not_vouch(self) -> None:
        """Authorization and relevance are different questions.

        The same world as the withheld case with the grant widened: the
        caller can now see ``obs_secret``, and it still says nothing about
        the edge between ``proj_subject`` and ``wu_one``. The reason must
        move from UNAUTHORIZED_EVIDENCE to the unvouched one rather than the
        claim resolving — being allowed to read a record is not the same as
        the record being about your question.
        """

        projection, _ = _probe_world(
            relationships=(
                (
                    "proj_subject",
                    RelationshipType.BLOCKED_BY,
                    "wu_one",
                    ("obs_secret",),
                ),
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_restricted", ()),
            ),
            observations=(("obs_secret", "wu_restricted"),),
        )
        grant = ("proj_subject", "wu_one", "wu_restricted")
        claim = _by_id(_probe_findings(projection, grant, "proj_subject"))[
            "drv_block_wu_one"
        ]
        assert claim.standing is DriverStanding.EXCLUDED
        assert (
            claim.exclusion_reason is DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED
        )
        # Present and excluded, never merely absent: the record it could not
        # use is named, and the note says why it could not use it.
        assert "obs_secret" in claim.conflicting_evidence_ids
        assert claim.conflict_detail is not None
        assert "neither end of the linkage" in claim.conflict_detail


class TestWhatTheStructuralRulesCannotProduce:
    """Two reasons are absent by construction. That is a property, not a gap."""

    def test_every_candidate_is_path_born(self, helio) -> None:
        """Why ``NO_SUPPORTING_PATH`` is unconstructable here.

        Every finding that can be ASSERTED derives from a step on a
        discovered path, so a driver without lineage cannot be built — a
        positive property of the graph arm and precisely what an arm without
        a graph cannot say about its own output.

        Scoped to asserted standing rather than to every candidate, because
        two kinds of finding legitimately carry no lineage and neither makes
        a causal claim: a symptom is an effect observed on the subject, and
        a cited measurement is a canonical number, not a mechanism. Both are
        capped at ``CANDIDATE_ONLY``, so neither can become the judgment —
        which is what makes narrowing the sweep safe rather than convenient.
        """

        checked = 0
        for principal in (world.PRINCIPAL_ANALYST, world.PRINCIPAL_COMPLIANCE):
            grant = adapter.authorized_entity_ids_for(principal)
            for subject in sorted(grant):
                for item in _findings(helio, subject, principal):
                    if not item.is_asserted:
                        continue
                    checked += 1
                    assert item.path_ids, (subject, item.driver_id)
                    assert item.mechanism == StandingMechanism.STRUCTURAL
                    assert (
                        item.exclusion_reason
                        is not DriverExclusionReason.NO_SUPPORTING_PATH
                    )
        assert checked, "no asserted finding was produced; this was vacuous"

    def test_nothing_without_lineage_can_ever_be_asserted(self, helio) -> None:
        """The other half, and the reason the narrowing above is not a hole.

        If a pathless finding could reach asserted standing, scoping the
        sweep to asserted findings would have hidden exactly the case it
        exists to catch.
        """

        seen = 0
        for principal in (world.PRINCIPAL_ANALYST, world.PRINCIPAL_COMPLIANCE):
            grant = adapter.authorized_entity_ids_for(principal)
            for subject in sorted(grant):
                for item in _findings(helio, subject, principal):
                    if item.path_ids:
                        continue
                    seen += 1
                    assert not item.is_asserted, (subject, item.driver_id)
        assert seen, "no pathless finding exists at all; this was vacuous"

    def test_no_structural_rule_emits_a_measurement_only_category(self, helio) -> None:
        """The scope declaration, as a claim that can fail today.

        Cycle time, review load, capacity and investment mix are statements
        about numbers this revision does not hold. A structural rule that
        started approximating one from graph shape — "this team owns a lot of
        projects" becoming a delivery-pressure claim — is the measuring-
        something-adjacent fault the whole correction exists to prevent.
        """

        assert MEASUREMENT_ONLY_CATEGORIES
        seen = 0
        for principal in (world.PRINCIPAL_ANALYST, world.PRINCIPAL_COMPLIANCE):
            grant = adapter.authorized_entity_ids_for(principal)
            for subject in sorted(grant):
                for item in _findings(helio, subject, principal):
                    if item.mechanism != StandingMechanism.STRUCTURAL:
                        # A CITED measurement may hold one of these
                        # categories — that is the whole point of citing it.
                        # What must never happen is a STRUCTURAL rule
                        # approximating a number from graph shape.
                        continue
                    seen += 1
                    assert item.category not in MEASUREMENT_ONLY_CATEGORIES, (
                        subject,
                        item.driver_id,
                        item.category,
                    )
        assert seen, "no structural finding was produced; this sweep was vacuous"

    def test_every_finding_declares_the_structural_mechanism(self, helio) -> None:
        """So CHAOS-3619 can tell structure from cited measurement per family.

        Until the measurement commit lands, every finding must say
        ``structural``. A finding that already claimed the other mechanism
        would make the graph hypothesis look confirmed by evidence that came
        from somewhere else.
        """

        findings = _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        assert findings
        assert {item.mechanism for item in findings} == {StandingMechanism.STRUCTURAL}


class TestDeterminism:
    def test_the_same_world_produces_the_same_findings(self, helio) -> None:
        """A recorded trial run has to be comparable with a re-run."""

        first = _findings(helio, "proj_pulse", world.PRINCIPAL_ANALYST)
        for _ in range(3):
            assert _findings(helio, "proj_pulse", world.PRINCIPAL_ANALYST) == first


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _probe_world(
    *,
    relationships,
    observations,
    trust: str | None = "canonical",
    subject_status: str = "in_progress",
    status_observations=(),
    statuses=None,
):
    """A minimal hand-shaped world, built through the REAL projection.

    Hand-authored graph objects would skip the relationship allowlist and the
    canonical-orientation check, so a fixture could assert driver behaviour
    over an edge the arm can never actually hold.
    """

    declared = {"proj_subject": subject_status, **(statuses or {})}
    entities = [
        (name, kind, declared.get(name, "in_progress"))
        for name, kind in (
            ("proj_subject", GraphEntityKind.PROJECT),
            ("wu_one", GraphEntityKind.WORK_UNIT),
            ("wu_two", GraphEntityKind.WORK_UNIT),
            ("wu_restricted", GraphEntityKind.WORK_UNIT),
            ("dep_two", GraphEntityKind.DEPENDENCY),
            ("proj_other", GraphEntityKind.PROJECT),
        )
    ]
    kinds = {name: kind for name, kind, _ in entities}
    batch = IngestionBatch(
        org_id=_PROBE_ORG,
        entities=tuple(
            EntityRecord(
                org_id=_PROBE_ORG,
                kind=kind,
                canonical_id=name,
                display_label=name.replace("_", " ").title(),
                source_class=SourceClass.WORK_GRAPH,
                observed_at=_PROBE_NOW,
                attributes={"declared_status": status},
            )
            for name, kind, status in entities
        ),
        relationships=tuple(
            RelationshipRecord(
                org_id=_PROBE_ORG,
                source=CanonicalRef(kind=kinds[source], canonical_id=source),
                relationship=relationship,
                target=CanonicalRef(kind=kinds[target], canonical_id=target),
                source_class=SourceClass.WORK_GRAPH,
                observed_at=_PROBE_NOW,
                observation_ids=observation_ids,
            )
            for source, relationship, target, observation_ids in relationships
        ),
        observations=tuple(
            ObservationRecord(
                org_id=_PROBE_ORG,
                kind=GraphObservationKind.AGENT_TASK,
                canonical_id=name,
                title=name.replace("_", " ").title(),
                source_class=SourceClass.WORK_ITEM,
                observed_at=_PROBE_NOW,
                subjects=(CanonicalRef(kind=kinds[about], canonical_id=about),),
                attributes=({"corpus_trust": trust} if trust is not None else {}),
            )
            for name, about in observations
        )
        # A second source class, so a probe world can satisfy a question
        # family's required set and be judged on its own bounds rather than
        # on a MISSING_SOURCE gap it was always going to have.
        + tuple(
            ObservationRecord(
                org_id=_PROBE_ORG,
                kind=GraphObservationKind.STATUS_CHANGE,
                canonical_id=name,
                title=name.replace("_", " ").title(),
                source_class=SourceClass.STATUS_CHANGE,
                observed_at=_PROBE_NOW,
                subjects=(CanonicalRef(kind=kinds[about], canonical_id=about),),
                attributes=({"corpus_trust": trust} if trust is not None else {}),
            )
            for name, about in status_observations
        ),
    )
    projection = build_projection(batch)
    return projection, tuple(sorted(kinds))


def _probe_readout(projection, grant, subject: str, *, max_hops: int = 2):
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=_PROBE_ORG,
            seed_canonical_ids=[subject],
            authorized_entity_ids=sorted(grant),
            max_hops=max_hops,
        )
    )


def _probe_findings(projection, grant, subject: str, *, max_hops: int = 2):
    readout = _probe_readout(projection, grant, subject, max_hops=max_hops)
    return discover_drivers(readout, subject, as_of=_PROBE_NOW)[0]


def _probe_packet(readout, signer, *, drivers, drivers_truncated: bool):
    """A packet over the probe world, so a bound can be reached for real.

    The corpus is dense enough that every readout it produces is already
    truncated on entities and paths, which would disclose
    ``TRUNCATED_TRAVERSAL`` for a reason that has nothing to do with the
    driver bound. The probe world reads clean on every flag, so a limitation
    appearing here can only have come from the driver bound.
    """

    from dev_health_ops.api.dev.investigation_contract import (
        ComparisonShape,
        QuestionFamilyID,
    )
    from dev_health_ops.context_fabric.graph_arm.packet_builder import (
        JobContext,
        TrialContext,
        build_packet,
    )
    from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

    return build_packet(
        readout=readout,
        job=JobContext(
            job_id="job_probe",
            # ``ambiguous_identity`` requires work_graph, work_item and
            # status_change -- exactly what the probe world observes -- so no
            # MISSING_SOURCE gap masks the bound under test. Under
            # ``project_status_drivers`` (five required classes, two present)
            # every probe packet is gapped whatever its bounds do, and the
            # control below could not tell a driver bound from a thin world.
            question_family=QuestionFamilyID("ambiguous_identity"),
            job_statement="Why is this subject not finished?",
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
            window_start=datetime(2026, 7, 15, tzinfo=UTC),
            window_end=_PROBE_NOW,
        ),
        watermark=IndexWatermark(
            indexed_through=_PROBE_NOW,
            projected_at=_PROBE_NOW,
            records_indexed=1,
        ),
        signer=signer,
        trial=TrialContext(run_id="4f9a2c1e-1111-4222-8333-444455556666"),
        produced_at=_PROBE_NOW,
        drivers=drivers,
        drivers_truncated=drivers_truncated,
    )


class TestATruncatedCandidateSetIsDisclosed:
    """The driver bound is a bound like any other, and must disclose like one.

    ``drivers_truncated`` reached ``DriverAnalysis.candidates_truncated`` and
    nowhere else: not the ``TRUNCATED_TRAVERSAL`` limitation, not the outcome's
    ``gaps``. The frozen contract refused the packet outright — "truncated
    results but no TRUNCATED_TRAVERSAL limitation is disclosed" — so the only
    way past it was for a caller to drop the flag, which turns a capped
    candidate set into one presented as complete.

    Every test here reaches the bound through ``discover_drivers(...,
    max_candidates=1)`` rather than by passing the flag. Passing the flag
    tests the plumbing; capping for real tests the bound.
    """

    @staticmethod
    def _capped(signer, *, max_candidates: int):
        projection, grant = _probe_world(
            relationships=(
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_one", ("obs_one",)),
                ("proj_subject", RelationshipType.BLOCKED_BY, "wu_two", ("obs_two",)),
            ),
            observations=(("obs_one", "wu_one"), ("obs_two", "wu_two")),
            status_observations=(("sc_subject", "proj_subject"),),
        )
        readout = _probe_readout(projection, grant, "proj_subject")
        # Anti-vacuity: nothing else in this world is truncated, so any
        # TRUNCATED_TRAVERSAL limitation below came from the driver bound.
        assert not (
            readout.entities_truncated
            or readout.paths_truncated
            or readout.evidence_truncated
        )
        findings, truncated = discover_drivers(
            readout,
            "proj_subject",
            as_of=_PROBE_NOW,
            max_candidates=max_candidates,
        )
        return _probe_packet(
            readout, signer, drivers=findings, drivers_truncated=truncated
        ), truncated

    def test_a_real_candidate_cap_produces_a_valid_packet(self, signer) -> None:
        """The reproduction. This raised before the flag was wired in."""

        packet, truncated = self._capped(signer, max_candidates=1)
        assert truncated, "the cap did not bite, so this proves nothing"
        assert packet.driver_analysis.candidates_truncated is True
        assert len(packet.driver_analysis.candidates) == 1

    def test_the_truncated_candidate_set_discloses_a_limitation(self, signer) -> None:
        """The disclosure the contract demands, from the driver bound alone."""

        packet, _ = self._capped(signer, max_candidates=1)
        assert PacketLimitationKind.TRUNCATED_TRAVERSAL in {
            item.kind for item in packet.evidence_coverage.limitations
        }

    def test_a_truncated_candidate_set_cannot_reach_an_ungapped_outcome(
        self, signer
    ) -> None:
        """A bound that discloses but does not weaken is half a disclosure.

        ``SUPPORTED`` says the investigation was complete. A run that stopped
        enumerating causes at a configured bound has not earned it, whatever
        the limitation list says beside it.
        """

        packet, _ = self._capped(signer, max_candidates=1)
        assert packet.outcome is InvestigationOutcome.SUPPORTED_WITH_GAPS

    def test_the_uncapped_run_is_the_control(self, signer) -> None:
        """Otherwise "always truncated" and "always gapped" would pass above.

        Same world, same builder, a cap that does not bite: no driver
        truncation, no limitation, and the outcome is fully supported.
        """

        packet, truncated = self._capped(signer, max_candidates=50)
        assert truncated is False
        assert packet.driver_analysis.candidates_truncated is False
        assert PacketLimitationKind.TRUNCATED_TRAVERSAL not in {
            item.kind for item in packet.evidence_coverage.limitations
        }
        assert packet.outcome is InvestigationOutcome.SUPPORTED


class TestTheFirstSupportedPacket:
    """The arm's first non-``unsupported`` outcome, and what must back it.

    A supported outcome is a claim about a *specific driver's* standing, not
    a value in an enum field. An assertion on the outcome alone would pass
    under driver substitution — swap the driver that earned it for any other
    and the enum is unchanged — so every test here names the driver, its
    standing, its mechanism and the evidence it rests on.
    """

    def test_the_packet_reaches_a_supported_outcome(self, helio, signer) -> None:
        packet = _packet(helio, "proj_identity_rewrite", signer)
        assert packet.outcome in SUPPORTED_OUTCOMES

    def test_the_supported_outcome_names_the_driver_that_earned_it(
        self, helio, signer
    ) -> None:
        """Driver substitution must not leave this test green."""

        packet = _packet(helio, "proj_identity_rewrite", signer)
        assert packet.driver_analysis.principal_driver_ids == (
            "drv_block_wu_authcore_release",
        )
        principal = next(
            item
            for item in packet.driver_analysis.candidates
            if item.driver_id == "drv_block_wu_authcore_release"
        )
        assert principal.standing is DriverStanding.PRINCIPAL_DRIVER
        assert principal.role is DriverRole.DRIVER
        assert principal.category is DriverCategory.EXTERNAL_BLOCKER
        assert principal.affected_subject_ids == ("proj_identity_rewrite",)

    def test_the_earning_driver_rests_on_lineage_and_real_evidence(
        self, helio, signer
    ) -> None:
        """Standing is what the contract says it is, checked on the wire.

        Handles rather than raw observation ids, and every one of them
        present in the packet's own evidence index — a cited handle the
        index does not carry is an unresolvable reference presented as
        evidence.
        """

        packet = _packet(helio, "proj_identity_rewrite", signer)
        principal = next(
            item
            for item in packet.driver_analysis.candidates
            if item.driver_id == "drv_block_wu_authcore_release"
        )
        assert principal.supporting_path_ids
        assert principal.supporting_evidence_ids
        indexed = {
            entry.evidence.evidence_ref_id
            for entry in packet.evidence_coverage.evidence_index
        }
        assert set(principal.supporting_evidence_ids) <= indexed

    def test_the_standing_was_earned_structurally_not_by_measurement(
        self, helio
    ) -> None:
        """Which mechanism earned it, so CHAOS-3619 can split per family.

        A supported outcome reached by citing a canonical measurement is a
        different answer to the trial's question than one reached from the
        graph alone. Until the measurement commit lands, every mechanism must
        read ``structural`` — and this is the assertion that would fail if a
        finding started claiming otherwise.
        """

        findings = _by_id(
            _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        )
        earner = findings["drv_block_wu_authcore_release"]
        assert earner.standing is DriverStanding.PRINCIPAL_DRIVER
        assert earner.mechanism == StandingMechanism.STRUCTURAL

    def test_a_packet_with_no_drivers_is_still_unsupported(self, helio, signer) -> None:
        """The control. Otherwise "supported" could be the constant.

        Same builder, same world, same subject — drivers withheld. If this
        also came back supported, the outcome would not be derived from
        anything.
        """

        packet = _packet(helio, "proj_identity_rewrite", signer, drivers=())
        assert packet.driver_analysis.candidates == ()
        assert packet.outcome is InvestigationOutcome.UNSUPPORTED

    def test_a_subject_whose_only_candidate_is_excluded_is_unsupported(
        self, helio, signer
    ) -> None:
        """``proj_meridian``'s only candidate is the planted false claim.

        Excluded for untrusted support, so the packet asserts no judgment —
        which is the honest answer and the one the whole poisoned-linkage
        case exists to produce.
        """

        packet = _packet(helio, "proj_meridian", signer)
        assert packet.driver_analysis.candidates
        assert packet.driver_analysis.principal_driver_ids == ()
        assert packet.outcome is InvestigationOutcome.UNSUPPORTED


class TestEvidenceTranslationFailsLoudly:
    def test_a_driver_citing_unindexed_evidence_is_refused(self, helio, signer) -> None:
        """Discovery and emission must agree about what the run observed.

        An earlier version dropped an unknown id silently. The harness
        disabled that filter and every test still passed — SURVIVED — which
        showed the branch was unreachable in every world under test and so
        was dead code wearing the appearance of a safety net.

        Raising is reachable, and this constructs the reach: a finding built
        by discovery, then given one evidence id the packet never indexed.
        Silently dropping it would emit a driver with less support than it
        was built from, which is a weaker claim presented as the same one.
        """

        from dataclasses import replace

        findings = _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        honest = next(
            item
            for item in findings
            if item.driver_id == "drv_block_wu_authcore_release"
        )
        assert honest.evidence_ids, "the finding carries no evidence; vacuous"

        # The control: unmodified, it emits.
        assert _packet(helio, "proj_identity_rewrite", signer, drivers=(honest,))

        tampered = replace(
            honest, evidence_ids=(*honest.evidence_ids, "obs_never_indexed")
        )
        with pytest.raises(ValueError, match="never indexed"):
            _packet(helio, "proj_identity_rewrite", signer, drivers=(tampered,))


class TestAssertedSupportMustBeTheDriversOwn:
    """The packet boundary, which `discover_drivers` does not defend.

    The contract's supported-outcome rule reads stronger than it is: "some
    asserted driver exists AND the evidence index is non-empty". Adversarial
    review reproduced both gaps — an honest driver whose evidence was swapped
    for an indexed handle belonging to a DIFFERENT subject still produced a
    supported outcome, and so did one with its paths emptied. Neither driver
    had support; the packet had support somewhere, and the outcome could not
    tell the difference.

    A caller-assembled driver arrives exactly here, so the bar has to be at
    the boundary rather than inside discovery.
    """

    def test_a_driver_citing_another_subjects_evidence_is_refused(
        self, helio, signer
    ) -> None:
        from dataclasses import replace

        findings = _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        honest = findings[0]
        base = _packet(helio, "proj_identity_rewrite", signer)
        elsewhere = sorted(
            entry.evidence.entity_id
            for entry in base.evidence_coverage.evidence_index
            if entry.evidence.entity_id not in honest.evidence_ids
        )
        assert elsewhere, "no unrelated indexed evidence exists; vacuous"
        swapped = replace(
            honest,
            standing=DriverStanding.CONTRIBUTING_DRIVER,
            evidence_ids=(elsewhere[0],),
        )
        with pytest.raises(ValueError, match="evidence about other subjects"):
            _packet(helio, "proj_identity_rewrite", signer, drivers=(swapped,))

    def test_an_asserted_driver_with_no_lineage_is_refused(self, helio, signer) -> None:
        """Stricter than the contract, deliberately.

        The contract permits a CONTRIBUTING driver with evidence and no path.
        This arm does not: "every asserted finding is path-born" is the
        property the whole graph claim rests on, and enforcing it only inside
        discovery left the boundary open.
        """

        from dataclasses import replace

        findings = _findings(helio, "proj_identity_rewrite", world.PRINCIPAL_ANALYST)
        pathless = replace(
            findings[0],
            standing=DriverStanding.CONTRIBUTING_DRIVER,
            path_ids=(),
        )
        with pytest.raises(ValueError, match="cites no lineage path"):
            _packet(helio, "proj_identity_rewrite", signer, drivers=(pathless,))

    def test_the_honest_driver_still_passes_the_boundary(self, helio, signer) -> None:
        """The control, and the one that catches a rule tightened too far.

        A blocker's evidence is about the BLOCKER, not about the thing it
        blocks. A first version of this check demanded the affected subject
        and refused every honest blocking driver in the corpus.
        """

        packet = _packet(helio, "proj_identity_rewrite", signer)
        assert packet.driver_analysis.principal_driver_ids == (
            "drv_block_wu_authcore_release",
        )
        assert packet.outcome in SUPPORTED_OUTCOMES

    def test_each_cited_entry_names_the_driver_that_cites_it(
        self, helio, signer
    ) -> None:
        """Closure in both directions, not just from the driver outwards."""

        packet = _packet(helio, "proj_identity_rewrite", signer)
        naming = {
            entry.evidence.evidence_ref_id: entry.supports_driver_ids
            for entry in packet.evidence_coverage.evidence_index
            if entry.supports_driver_ids
        }
        assert naming, "no evidence entry names a driver; closure is one-way"
        principal = next(
            item
            for item in packet.driver_analysis.candidates
            if item.driver_id == "drv_block_wu_authcore_release"
        )
        for handle in principal.supporting_evidence_ids:
            assert principal.driver_id in naming[handle]


def _packet(projection, subject: str, signer, *, drivers=None):
    from dev_health_ops.api.dev.investigation_contract import (
        ComparisonShape,
        QuestionFamilyID,
    )
    from dev_health_ops.context_fabric.graph_arm.packet_builder import (
        JobContext,
        TrialContext,
        build_packet,
    )
    from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

    grant = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
    readout = asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[subject],
            authorized_entity_ids=sorted(grant),
            max_hops=2,
        )
    )
    findings = (
        discover_drivers(readout, subject, as_of=world.TRIAL_NOW)[0]
        if drivers is None
        else drivers
    )
    return build_packet(
        readout=readout,
        job=JobContext(
            job_id="job_drivers",
            question_family=QuestionFamilyID("project_status_drivers"),
            job_statement="Why is this subject not finished?",
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
            window_start=world.AS_OF_JUL_15,
            window_end=world.TRIAL_NOW,
        ),
        watermark=IndexWatermark(
            indexed_through=world.TRIAL_NOW,
            projected_at=world.TRIAL_NOW,
            records_indexed=1,
        ),
        signer=signer,
        trial=TrialContext(
            run_id="4f9a2c1e-1111-4222-8333-444455556666",
            corpus_version=adapter.CORPUS_VERSION,
        ),
        produced_at=_PROBE_NOW,
        drivers=findings,
    )
