"""CHAOS-3645: guards for the subjectless cohort entry mode.

Each guard below is paired with the defect it exists to catch, and the
defect is CONSTRUCTED here rather than described, so "this passes" means the
specific bad thing was built and rejected.

Two of these are differential rather than self-referential, and those are the
load-bearing ones. The arm decides a member's inclusion basis and the cohort's
supported dimensions from the projection; the corpus oracle decides whether
those claims are true using ``world.shares_basis`` and ``world.comparable_on``,
over the world's own records, by an implementation this module never calls
into. A test that asserted only "a basis was stated" would pass against an arm
that states ``same_portfolio`` for entities holding no portfolio edge -- which
is precisely the "well-explained but factually irrelevant member" fault. So
the assertions here run the ORACLE's predicate over the ARM's output.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import replace

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    CohortInclusionBasis,
    ComparisonShape,
    QuestionFamilyID,
    SubjectCommitmentState,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.cohort import (
    CohortEntryMode,
    build_cohort,
)
from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
    FAMILY_CANDIDATE_KINDS,
    CohortDiscovery,
    UnsupportedCohortFamilyError,
    discover_cohort,
)
from dev_health_ops.context_fabric.graph_arm.projection import build_projection
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

_TEAM_FAMILY = QuestionFamilyID.STRUGGLING_TEAMS
_PROJECT_FAMILY = QuestionFamilyID.PROJECT_CAPACITY
#: The corpus's planted restricted project: inside the caller's own tenant and
#: outside their grant, which is what makes a tenant-derived scope look right
#: and be wrong.
_RESTRICTED = "proj_quarry"


@pytest.fixture(scope="module")
def projection():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


@pytest.fixture(scope="module")
def grant() -> frozenset[str]:
    return frozenset(adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST))


def _discover(projection, grant, family=_TEAM_FAMILY, **kwargs) -> CohortDiscovery:
    return discover_cohort(
        question_family=family,
        nodes=kwargs.pop("nodes", projection.nodes),
        edges=kwargs.pop("edges", projection.edges),
        authorized_entity_ids=grant,
        as_of=kwargs.pop("as_of", world.WINDOW_END),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# The closed family map
# ---------------------------------------------------------------------------


class TestOnlyDeclaredFamiliesGetASubjectlessEntry:
    def test_a_family_outside_the_map_is_refused_by_name(self, projection, grant):
        """The org-widening fault, refused rather than answered.

        ``clarification_and_no_match`` is "what is going sideways?" -- a
        question whose right answer is a clarification. An arm that answered
        it by enumerating every team in the organization would score
        ``no_unsafe_organization_widening`` as a failure, and would do it
        while looking helpful.
        """

        assert QuestionFamilyID.CLARIFICATION_AND_NO_MATCH not in FAMILY_CANDIDATE_KINDS
        with pytest.raises(UnsupportedCohortFamilyError, match="no subjectless"):
            _discover(
                projection, grant, family=QuestionFamilyID.CLARIFICATION_AND_NO_MATCH
            )

    def test_a_family_inside_the_map_is_not_refused(self, projection, grant):
        """The control. Without it the refusal above also passes against a
        module that refuses every family, which would be a capability of
        exactly zero dressed as a safety property."""

        discovery = _discover(projection, grant, family=_TEAM_FAMILY)
        assert discovery.proposal.members

    def test_the_universe_kinds_are_the_families_declared_kinds(
        self, projection, grant
    ):
        """A team question must not return projects.

        The fault this catches is a universe built from "every authorized
        entity" with the kind filter forgotten: it would produce a large,
        confident cohort comparing teams with the repositories they own.
        """

        teams = _discover(projection, grant, family=_TEAM_FAMILY)
        assert {member.kind for member in teams.proposal.members} == {
            GraphEntityKind.TEAM
        }
        projects = _discover(projection, grant, family=_PROJECT_FAMILY)
        assert {member.kind for member in projects.proposal.members} == {
            GraphEntityKind.PROJECT
        }


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------


class TestEnumerationNeverWidensTheAuthorizedScope:
    def test_a_restricted_entity_is_neither_member_nor_exclusion(
        self, projection, grant
    ):
        """Naming a subject in order to say it was left OUT still discloses it.

        The restricted project is of exactly the kind the project families
        enumerate, so an implementation that filtered on the way out rather
        than on the way in would put it in the exclusion list with a tidy
        ``out_of_authorized_scope`` reason -- and tell the caller it exists.
        """

        assert _RESTRICTED not in grant
        discovery = _discover(projection, grant, family=_PROJECT_FAMILY)
        named = {member.canonical_id for member in discovery.proposal.members} | {
            exclusion.canonical_id for exclusion in discovery.proposal.exclusions
        }
        assert _RESTRICTED not in named
        assert discovery.authorization_filtered_count == 1
        assert discovery.proposal.authorization_filtered_count == 1

    def test_the_same_entity_is_enumerated_when_the_grant_allows_it(
        self, projection, grant
    ):
        """The control, and it is the one that makes the guard above mean
        something. Without it, an implementation that dropped
        ``proj_quarry`` for any reason at all -- a typo in a kind name, an
        off-by-one -- would pass the withholding test while withholding
        nothing on purpose."""

        widened = grant | {_RESTRICTED}
        discovery = _discover(projection, widened, family=_PROJECT_FAMILY)
        members = {member.canonical_id for member in discovery.proposal.members}
        exclusions = {
            exclusion.canonical_id for exclusion in discovery.proposal.exclusions
        }
        assert _RESTRICTED in members | exclusions
        assert discovery.authorization_filtered_count == 0


# ---------------------------------------------------------------------------
# Every stated basis is true of the world (differential against the oracle)
# ---------------------------------------------------------------------------


class TestEveryStatedBasisIsTrueOfTheWorld:
    @pytest.mark.parametrize("family", sorted(FAMILY_CANDIDATE_KINDS, key=str))
    def test_the_oracles_own_predicate_accepts_every_member_basis(
        self, projection, grant, family
    ):
        """The claim the arm makes, checked by the code that will judge it.

        ``world.shares_basis`` is the corpus's half of
        ``cohort_inclusion_explainability``: a basis has to be true of the
        world, not merely stated. It reads the relationship with the MEMBER as
        the edge source, so an arm that derived ``same_portfolio`` for a team
        -- teams hold no ``belongs_to_portfolio`` edge; their projects do --
        would state a basis this predicate denies. That exact reading was the
        first design here and this assertion is what rejected it.
        """

        discovery = _discover(projection, grant, family=family)
        peers = [member.canonical_id for member in discovery.proposal.members]
        assert len(peers) >= 2, "a cohort with fewer than two members proves nothing"
        for member in discovery.proposal.members:
            for basis in member.bases:
                assert world.shares_basis(basis, member.canonical_id, peers), (
                    f"the arm stated {basis.value} for {member.canonical_id} and "
                    "the world does not support it; a well-explained member "
                    "that is factually unrelated is the named cohort fault"
                )

    def test_a_basis_the_world_denies_is_detected_by_this_assertion(
        self, projection, grant
    ):
        """The planted defect, so the guard above is known to be able to fail.

        ``same_portfolio`` on a TEAM is the specific wrong answer the first
        implementation produced -- teams do sit inside portfolios in the way a
        human means it, via the projects they own, and the world's predicate
        still says no. Constructed here rather than described, because an
        assertion nobody has watched fail is an assertion nobody has tested.
        """

        discovery = _discover(projection, grant, family=_TEAM_FAMILY)
        peers = [member.canonical_id for member in discovery.proposal.members]
        forged = replace(
            discovery.proposal.members[0],
            basis_anchors=((CohortInclusionBasis.SAME_PORTFOLIO, ("pf_platform",)),),
        )
        assert not world.shares_basis(
            CohortInclusionBasis.SAME_PORTFOLIO, forged.canonical_id, peers
        )


class TestEveryDeclaredDimensionIsSupportedByTheMembers:
    @pytest.mark.parametrize("family", sorted(FAMILY_CANDIDATE_KINDS, key=str))
    def test_the_oracles_own_predicate_accepts_every_dimension(
        self, projection, grant, family
    ):
        """A declared axis nobody can be compared on is a comparison in name.

        ``world.comparable_on`` needs two entities measured on the same metric
        (or, for dependency exposure, two depending on the same thing). The
        arm derives its dimensions from an INDEPENDENT metric table of its own
        -- deliberately not imported from the corpus -- so this is a real
        comparison of two implementations rather than a table agreeing with
        itself.
        """

        discovery = _discover(projection, grant, family=family)
        member_ids = [member.canonical_id for member in discovery.proposal.members]
        assert discovery.proposal.dimensions, "no dimension means no comparison"
        for dimension in discovery.proposal.dimensions:
            assert world.comparable_on(dimension, member_ids), (
                f"the arm declared {dimension.value} supported and the world "
                "cannot compare these members on it"
            )

    def test_a_dimension_only_one_member_supports_is_not_declared(
        self, projection, grant
    ):
        """One number is not a comparison.

        ``feed_lag_days`` is measured on exactly one corpus team, and the
        arm's metric table maps it to ``data_coverage``. An implementation
        that derived dimensions from "any metric any member carries" would
        declare that axis, and the world would deny it -- which is the shape
        of the fault, made concrete on a metric the corpus really does have
        only once.
        """

        discovery = _discover(projection, grant, family=_TEAM_FAMILY)
        member_ids = [member.canonical_id for member in discovery.proposal.members]
        from dev_health_ops.api.dev.investigation_contract import ComparisonDimension

        assert not world.comparable_on(ComparisonDimension.DATA_COVERAGE, member_ids)
        assert ComparisonDimension.DATA_COVERAGE not in discovery.proposal.dimensions


# ---------------------------------------------------------------------------
# Exclusions are results
# ---------------------------------------------------------------------------


class TestACandidateSharingNothingIsExcludedRatherThanDropped:
    def test_every_authorized_candidate_is_accounted_for(self, projection, grant):
        """A silently dropped candidate is a comparison quietly narrowed.

        The corpus makes this concrete: ``team_cinder`` is measured only on
        metrics no other team carries, so it genuinely cannot be compared.
        The right answer is to say so; the wrong one, and the easy one, is for
        it to vanish.
        """

        discovery = _discover(projection, grant, family=_TEAM_FAMILY)
        accounted = len(discovery.proposal.members) + len(discovery.proposal.exclusions)
        assert accounted == discovery.universe_size
        excluded = {
            exclusion.canonical_id for exclusion in discovery.proposal.exclusions
        }
        assert "team_cinder" in excluded

    def test_an_exclusion_states_a_reason_a_reader_can_act_on(self, projection, grant):
        discovery = _discover(projection, grant, family=_TEAM_FAMILY)
        for exclusion in discovery.proposal.exclusions:
            assert exclusion.rationale.strip()
            assert exclusion.reason is not None


# ---------------------------------------------------------------------------
# Validity
# ---------------------------------------------------------------------------


class TestALapsedRelationshipCannotCarryAMembership:
    def test_an_edge_that_ended_before_as_of_creates_no_basis(self, projection, grant):
        """A cohort assembled from last quarter's org, presented as this one's.

        The projection carries validity intervals precisely so a lapsed fact
        can be told from a live one. An enumeration that ignored them would
        keep comparing two projects through a portfolio one of them left.
        """

        live = _discover(projection, grant, family=_PROJECT_FAMILY)
        lapsed_edges = [
            replace(edge, valid_to=world.WINDOW_START)
            if edge.relationship.value == "belongs_to_portfolio"
            else edge
            for edge in projection.edges
        ]
        after = _discover(projection, grant, family=_PROJECT_FAMILY, edges=lapsed_edges)

        def portfolio_anchors(discovery):
            return {
                member.canonical_id
                for member in discovery.proposal.members
                if CohortInclusionBasis.SAME_PORTFOLIO in member.bases
            }

        assert portfolio_anchors(live), "the control found no live portfolio basis"
        assert not portfolio_anchors(after)


# ---------------------------------------------------------------------------
# The entry mode does not depend on extraction
# ---------------------------------------------------------------------------


class TestTheEntryModeCannotBeHandedAQuestion:
    def test_no_parameter_could_carry_a_question_or_a_seed(self):
        """3(a) fairness, enforced by signature rather than by discipline.

        The seeded path guarantees fairness by taking a question and nothing
        that reveals the answer. This path guarantees the other half: it takes
        no question at all, so it cannot be quietly turned into a
        mention-dependent capability by a later caller passing one in.
        """

        parameters = set(inspect.signature(discover_cohort).parameters)
        assert parameters == {
            "question_family",
            "nodes",
            "edges",
            "authorized_entity_ids",
            "as_of",
            "max_members",
            "max_exclusions",
        }

    def test_the_module_does_not_import_the_question_interpreter(self):
        """The extractor is absent, not merely unused.

        ``no_mention_extracted`` is the category every one of the fourteen
        cohort refusals fell into. A second entry mode that reached for the
        same extractor -- even as a hint, even as a tiebreak -- would inherit
        the limit it exists to route around, and the cheapest guarantee that
        it does not is that the import is not there.
        """

        import ast

        from dev_health_ops.context_fabric.graph_arm import cohort_discovery

        tree = ast.parse(inspect.getsource(cohort_discovery))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
                imported.update(alias.name for alias in node.names)
        # Asserted over the IMPORT GRAPH rather than over the source text: the
        # module docstring names ``extract_mentions`` in order to explain why
        # it is absent, and a substring search would either fail on the prose
        # or force the prose to stop saying the load-bearing thing.
        assert not any("question_interpreter" in name for name in imported)
        assert "extract_mentions" not in imported


# ---------------------------------------------------------------------------
# The packet builder's half
# ---------------------------------------------------------------------------


def _readout(projection, grant, seeds):
    from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader

    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=list(seeds),
            authorized_entity_ids=sorted(grant),
            max_hops=3,
        )
    )


def _build(projection, grant, cohort, seeds, *, family=_TEAM_FAMILY):
    from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
    from dev_health_ops.context_fabric.graph_arm.packet_builder import (
        IndexWatermark,
        JobContext,
        TrialContext,
        build_packet,
    )

    return build_packet(
        readout=_readout(projection, grant, seeds),
        job=JobContext(
            job_id="job_cohort_3645",
            question_family=family,
            job_statement="Which teams are currently struggling, and why?",
            comparison_shape=ComparisonShape.DISCOVERED_COHORT,
            window_start=world.WINDOW_START,
            window_end=world.WINDOW_END,
        ),
        watermark=IndexWatermark(
            indexed_through=world.WINDOW_END,
            projected_at=world.WINDOW_END,
            records_indexed=len(projection.nodes),
        ),
        signer=EvidenceReferenceSigner("chaos-3645-test-signing-secret-not-a-real-key"),
        trial=TrialContext(
            run_id="3645a11e-0000-4000-8000-00000000c04e",
            corpus_version=adapter.CORPUS_VERSION,
        ),
        produced_at=world.TRIAL_NOW,
        cohort=cohort,
    )


class TestAScopeEnumeratedPacketClaimsNoSubjectItDidNotResolve:
    def test_it_emits_no_subject_candidate_and_no_commitment(self, projection, grant):
        """The false-claim this mode exists to avoid.

        ``SubjectCandidate.match_signals`` is ``min_length=1`` and every
        member of the frozen ``SubjectMatchSignal`` vocabulary describes
        matching a reference the question supplied. A cohort question supplies
        none, so a packet that named a candidate here would necessarily be
        asserting an ``exact_canonical_id`` match against a question that
        contains no identifier -- a fabricated match signal inside a scored
        packet.
        """

        discovery = _discover(projection, grant, family=_TEAM_FAMILY)
        seeds = [member.canonical_id for member in discovery.proposal.members]
        packet = _build(projection, grant, discovery.proposal, seeds)

        assert packet.subject_discovery.candidates == ()
        assert packet.subject_discovery.committed_subject_ids == ()
        assert len(packet.comparison_cohort.members) >= 2
        assert {
            member.canonical_id for member in packet.comparison_cohort.members
        } == set(seeds)

    def test_the_subject_anchored_mode_still_commits_a_subject(self, projection, grant):
        """The control, and it is not decorative: the change that suppresses
        subject candidates runs inside the shared builder, so an
        over-broad version of it would silently strip the committed subject
        out of every seeded packet the trial scores."""

        subject = "proj_identity_rewrite"
        labels = {
            node.canonical_id: (node.entity_kind, node.display_label)
            for node in projection.nodes
            if node.is_entity
            and node.entity_kind is not None
            and node.entity_kind is not GraphEntityKind.ORGANIZATION
        }
        cohort = build_cohort(subject, projection.edges, labels, grant)
        assert cohort.entry_mode is CohortEntryMode.SUBJECT_ANCHORED
        packet = _build(projection, grant, cohort, [subject])
        assert packet.subject_discovery.committed_subject_ids == (subject,)
        assert any(
            candidate.commitment_state is SubjectCommitmentState.COMMITTED
            for candidate in packet.subject_discovery.candidates
        )

    def test_a_scope_enumerated_cohort_that_names_a_subject_is_refused(
        self, projection, grant
    ):
        """A proposal must not hold both beliefs about its own question.

        Planted directly: take the enumerated proposal and write a subject id
        onto it. Every member's rationale was composed for a cohort with no
        subject -- they read "shares same_portfolio through pf_platform with
        the other members" -- so a subject arriving beside them is a caller
        contradiction, and emitting it would produce a packet whose stated
        reasons are about one thing and whose subject field is about another.
        """

        discovery = _discover(projection, grant, family=_PROJECT_FAMILY)
        seeds = [member.canonical_id for member in discovery.proposal.members]

        # The control: unchanged, the same proposal emits cleanly.
        _build(projection, grant, discovery.proposal, seeds, family=_PROJECT_FAMILY)

        with pytest.raises(ValueError, match="scope-enumerated cohort names subject"):
            _build(
                projection,
                grant,
                replace(discovery.proposal, subject_id=seeds[0]),
                seeds,
                family=_PROJECT_FAMILY,
            )
