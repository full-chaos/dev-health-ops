"""CHAOS-3617 PR2: alias/acronym candidate search, and the claim it must not make.

Every reference the corpus plants — an alias, an acronym, a previous name, a
provider identifier — is **stored text**. Resolving it is a lookup. That is
worth stating loudly because the two ways of resolving it produce packets
that score identically and prove completely different things: "we found 'the
auth work' by looking it up" is a capability the arm has, and "we found it by
retrieval" is a capability it does not have while the hash embedder is
active.

So the load-bearing test here is not that the aliases resolve. It is that
**no candidate ever carries a semantic mechanism**, and that the emission
guard would refuse one if it did.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
    SubjectMatchSignal,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.backend import (
    SEMANTIC_MECHANISMS,
    DeterministicEmbedder,
    MatchMechanism,
)
from dev_health_ops.context_fabric.graph_arm.discovery import (
    SIGNAL_RANK,
    search_candidates,
)
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    SubjectMatchFinding,
    TrialContext,
    UnsupportedMatchMechanismError,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


@pytest.fixture(scope="module")
def analyst_grant():
    return adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)


class TestEveryAliasFamilyResolves:
    @pytest.mark.parametrize(
        ("query", "expected_id", "expected_signal"),
        [
            ("the auth work", "proj_identity_rewrite", SubjectMatchSignal.ALIAS),
            ("IPR", "proj_identity_rewrite", SubjectMatchSignal.ACRONYM),
            (
                "Northstar",
                "proj_identity_rewrite",
                SubjectMatchSignal.PREVIOUS_NAME,
            ),
            (
                "HEL-IPR",
                "proj_identity_rewrite",
                SubjectMatchSignal.PROVIDER_IDENTIFIER,
            ),
            ("platform core", "team_atlas", SubjectMatchSignal.PREVIOUS_NAME),
            (
                "proj_identity_rewrite",
                "proj_identity_rewrite",
                SubjectMatchSignal.EXACT_CANONICAL_ID,
            ),
        ],
    )
    def test_the_reference_resolves_with_the_right_signal(
        self, helio, analyst_grant, query, expected_id, expected_signal
    ) -> None:
        candidates, _ = search_candidates(query, helio.nodes, analyst_grant)
        assert candidates, f"{query!r} resolved to nothing"
        assert candidates[0].canonical_id == expected_id
        assert candidates[0].signal is expected_signal

    def test_case_and_punctuation_do_not_defeat_a_lookup(
        self, helio, analyst_grant
    ) -> None:
        """Normalisation is for comparison only; the finding reports stored text."""

        candidates, _ = search_candidates(
            "  THE   AUTH-WORK  ", helio.nodes, analyst_grant
        )
        assert candidates
        assert candidates[0].canonical_id == "proj_identity_rewrite"
        assert candidates[0].matched_text == "the auth work", (
            "the finding must report what the arm recognised, not what was typed"
        )

    def test_an_ambiguous_acronym_returns_several_ranked_candidates(
        self, helio, analyst_grant
    ) -> None:
        """Ambiguity is a real answer, not a failure.

        The contract has a whole outcome for it, and the ranking is what makes
        a clarification prompt useful rather than a list.
        """

        candidates, _ = search_candidates("ACR", helio.nodes, analyst_grant)
        assert len(candidates) > 1
        assert candidates[0].signal is SubjectMatchSignal.ACRONYM
        ranks = [SIGNAL_RANK[item.signal] for item in candidates]
        assert ranks == sorted(ranks)


class TestNoRetrievalClaimIsEverMade:
    """The reason the mechanism field exists."""

    def test_no_candidate_carries_a_semantic_mechanism(
        self, helio, analyst_grant
    ) -> None:
        queries = [
            "the auth work",
            "IPR",
            "Northstar",
            "HEL-IPR",
            "ACR",
            "platform core",
            "identity modernisation",
            "auth",
        ]
        seen: set[MatchMechanism] = set()
        for query in queries:
            candidates, _ = search_candidates(query, helio.nodes, analyst_grant)
            seen.update(item.mechanism for item in candidates)
        assert seen, "no candidate was produced at all; this would be vacuous"
        assert not (seen & SEMANTIC_MECHANISMS), sorted(
            str(item) for item in seen & SEMANTIC_MECHANISMS
        )

    def test_the_search_never_touches_the_embedder(self, helio, analyst_grant) -> None:
        """Structural: an embedder passed in would be a retrieval path.

        ``search_candidates`` takes nodes and an authorized set. There is no
        parameter for an embedder, so there is no way for this capability to
        become similarity search without a signature change a reviewer sees.
        """

        import inspect

        from dev_health_ops.context_fabric.graph_arm import discovery

        parameters = set(inspect.signature(discovery.search_candidates).parameters)
        assert not (parameters & {"embedder", "embedding", "vector", "index"})

    def test_a_semantic_finding_is_refused_at_emission_under_the_hash_embedder(
        self, helio, analyst_grant, signer
    ) -> None:
        """The guard, exercised with real corpus candidates.

        Takes a genuine candidate this search produced and relabels only its
        mechanism. Everything else about the packet is identical, so the
        refusal is attributable to the claim rather than to a malformed
        input.
        """

        candidates, _ = search_candidates("the auth work", helio.nodes, analyst_grant)
        assert candidates
        honest = candidates[0]
        readout = _readout(helio, analyst_grant, honest.canonical_id)

        def finding(mechanism: MatchMechanism) -> SubjectMatchFinding:
            return SubjectMatchFinding(
                canonical_id=honest.canonical_id,
                signal=honest.signal,
                matched_text=honest.matched_text,
                source_class=honest.source_class,
                mechanism=mechanism,
            )

        # Honest mechanism: emits.
        assert _packet(readout, signer, [finding(honest.mechanism)]) is not None

        # Same candidate, relabelled as retrieval: refused.
        with pytest.raises(UnsupportedMatchMechanismError):
            _packet(readout, signer, [finding(MatchMechanism.EMBEDDING_SIMILARITY)])

    def test_the_active_embedder_really_is_non_semantic(self) -> None:
        """Anti-vacuity: the refusal above depends on this being False."""

        assert DeterministicEmbedder().semantic is False


class TestAuthorizationBoundsTheSearch:
    def test_the_restricted_project_is_withheld_and_counted(self, helio) -> None:
        """It matches by exact display name, and the analyst still cannot see it."""

        analyst = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        candidates, withheld = search_candidates(
            "Quarry Compliance", helio.nodes, analyst
        )
        assert not any(item.canonical_id == world.PROJ_QUARRY for item in candidates)
        assert withheld >= 1, (
            "the match was dropped without being counted, so the packet would "
            "report a complete candidate list"
        )

    def test_the_compliance_principal_does_see_it(self, helio) -> None:
        """The control. Otherwise 'withheld' could mean 'never matched'."""

        compliance = adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE)
        candidates, withheld = search_candidates(
            "Quarry Compliance", helio.nodes, compliance
        )
        assert any(item.canonical_id == world.PROJ_QUARRY for item in candidates)
        assert withheld == 0

    def test_a_withheld_entity_never_occupies_a_rank(self, helio) -> None:
        """Filtering after ranking would leak position, and position is data."""

        analyst = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        compliance = adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE)
        as_analyst, _ = search_candidates("Quarry", helio.nodes, analyst)
        as_compliance, _ = search_candidates("Quarry", helio.nodes, compliance)
        assert len(as_compliance) > len(as_analyst)
        assert [item.canonical_id for item in as_analyst] == [
            item.canonical_id
            for item in as_compliance
            if item.canonical_id != world.PROJ_QUARRY
        ]


class TestRankingIsTotalAndContentDerived:
    def test_shuffling_the_node_order_does_not_change_the_ranking(
        self, helio, analyst_grant
    ) -> None:
        """Same world, same query, same answer — on every machine.

        A ranking that depended on iteration order would make a recorded
        trial run incomparable with a re-run, which is the defect the
        differential oracle already caught once in the traversal.
        """

        baseline = [
            item.canonical_id
            for item in search_candidates("ACR", helio.nodes, analyst_grant)[0]
        ]
        assert baseline
        for seed in range(6):
            shuffled = list(helio.nodes)
            random.Random(seed).shuffle(shuffled)
            assert [
                item.canonical_id
                for item in search_candidates("ACR", shuffled, analyst_grant)[0]
            ] == baseline

    def test_every_signal_has_a_rank(self) -> None:
        """An unranked signal would sort by whatever ``min`` saw first."""

        assert set(SIGNAL_RANK) == set(SubjectMatchSignal)
        assert len(set(SIGNAL_RANK.values())) == len(SIGNAL_RANK)

    def test_fuzzy_label_ranks_last(self) -> None:
        """It is the one signal the contract refuses to let commit alone."""

        assert SIGNAL_RANK[SubjectMatchSignal.FUZZY_LABEL] == max(SIGNAL_RANK.values())


class TestFuzzyMatchingIsConservative:
    def test_every_fuzzy_match_is_whole_token_containment(
        self, helio, analyst_grant
    ) -> None:
        """Checked with the module's own normaliser, not by hand.

        The first version of this test did its own lowercasing and splitting
        and got a slash wrong (``helio/acr``), failing on a match that was
        entirely correct. Asserting a property with a reimplementation of the
        thing under test is how you end up debugging the test.
        """

        from dev_health_ops.context_fabric.graph_arm.discovery import _normalize

        candidates, _ = search_candidates("acr", helio.nodes, analyst_grant)
        fuzzy = [
            item for item in candidates if item.signal is SubjectMatchSignal.FUZZY_LABEL
        ]
        for item in fuzzy:
            node = next(
                candidate
                for candidate in helio.nodes
                if candidate.canonical_id == item.canonical_id
            )
            haystacks = [_normalize(node.display_label)] + [
                _normalize(alias.value) for alias in node.aliases
            ]
            assert any("acr" in text.split() for text in haystacks), (
                f"{item.canonical_id} matched 'acr' without containing it as a "
                "whole token"
            )

    def test_a_query_appearing_only_as_an_infix_matches_nothing(
        self, helio, analyst_grant
    ) -> None:
        """The negative control, and the reason containment is token-level.

        Substring matching is how 'acr' matches 'sacred'. This plants exactly
        that shape: a fragment that appears inside real corpus labels but
        never as a token.
        """

        for fragment in ("den", "ewri", "ompl"):
            candidates, _ = search_candidates(fragment, helio.nodes, analyst_grant)
            assert candidates == (), (
                f"{fragment!r} matched {[item.canonical_id for item in candidates]} "
                "as an infix; fuzzy matching must be whole-token"
            )

    def test_an_empty_or_punctuation_only_query_matches_nothing(
        self, helio, analyst_grant
    ) -> None:
        """Otherwise every entity is a candidate and the ranking is noise."""

        for query in ("", "   ", "!!!", "---"):
            candidates, withheld = search_candidates(query, helio.nodes, analyst_grant)
            assert candidates == ()
            assert withheld == 0


def _readout(projection, authorized, seed):
    import asyncio

    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[seed],
            authorized_entity_ids=sorted(authorized),
            max_hops=2,
        )
    )


def _packet(readout, signer, matches):
    return build_packet(
        readout=readout,
        job=JobContext(
            job_id="job_candidates",
            question_family=QuestionFamilyID("ambiguous_identity"),
            job_statement="Which project is 'the auth work'?",
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
        produced_at=datetime(2026, 8, 8, 12, tzinfo=UTC),
        subject_matches=matches,
    )
