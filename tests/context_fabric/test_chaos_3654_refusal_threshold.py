"""CHAOS-3654: the semantic leg's own refusal/clarification threshold.

Every case here is synthetic and constructed for its *shape* — the RRF
margin, the tie count, whether BM25 found anything at all — never lifted
from the CHAOS-3619/3647 frozen corpus's actual questions or entity names.
That is deliberate and is the whole point of
``semantic_retrieval.assess_disposition``: the two measured failures
("How is Halcyon doing?" returning 20 candidates, "What's holding it up?"
with no referent returning 11) are instances of one shape — a diffuse,
lexically-ungrounded fusion result — and a rule proven only on those two
specific queries would be exactly the corpus-tuned threshold the ticket
forbids. These cases are held out from that corpus by construction: they
exercise the shape with entirely different synthetic node names and RRF
scores, so a rule that only worked on "Halcyon" would fail here too.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal
from dev_health_ops.context_fabric.graph_arm.backend import MatchMechanism
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    RetrievalMethod,
    SemanticCandidate,
    SemanticDisposition,
    SemanticRetrieval,
    assess_disposition,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind


def _candidate(
    canonical_id: str,
    rrf_score: float,
    rank: int,
    *,
    methods: frozenset[RetrievalMethod],
) -> SemanticCandidate:
    mechanism = (
        MatchMechanism.EMBEDDING_SIMILARITY
        if RetrievalMethod.COSINE in methods
        else MatchMechanism.LEXICAL_FUZZY
    )
    return SemanticCandidate(
        canonical_id=canonical_id,
        kind=GraphEntityKind.PROJECT,
        display_label=canonical_id,
        signal=SubjectMatchSignal.FUZZY_LABEL,
        mechanism=mechanism,
        matched_text=canonical_id,
        source_class=SourceClass.WORK_GRAPH,
        methods=methods,
        rrf_score=rrf_score,
        rank=rank,
    )


def _retrieval(
    candidates: tuple[SemanticCandidate, ...],
    *,
    bm25_order: tuple[str, ...] = (),
    cosine_order: tuple[str, ...] = (),
    query: str = "query",
) -> SemanticRetrieval:
    return SemanticRetrieval(
        query=query,
        candidates=candidates,
        authorization_filtered_count=0,
        withheld_canonical_ids=(),
        retrieved_before_authorization=tuple(c.canonical_id for c in candidates),
        bm25_order=bm25_order,
        cosine_order=cosine_order,
        observation_hits=(),
    )


_COSINE = frozenset({RetrievalMethod.COSINE})
_BM25 = frozenset({RetrievalMethod.BM25})
_BOTH = frozenset({RetrievalMethod.BM25, RetrievalMethod.COSINE})


class TestNoMatchRefusesSafely:
    """The two measured shapes: no lexical grounding anywhere."""

    def test_a_nonexistent_entity_shaped_result_refuses(self) -> None:
        """Held-out analogue of "How is Halcyon doing?" — 20 candidates.

        Different node count, different scores, different names from the
        measured case: only the shape is shared — BM25 found nothing at all,
        and a wide field of cosine-only candidates decays smoothly with no
        leader. If this rule only worked on the literal Halcyon numbers, it
        would not refuse here.
        """

        candidates = tuple(
            _candidate(f"proj_synthetic_{i}", 0.031 - i * 0.0007, i, methods=_COSINE)
            for i in range(20)
        )
        retrieval = _retrieval(
            candidates,
            bm25_order=(),
            cosine_order=tuple(c.canonical_id for c in candidates),
        )
        result = assess_disposition(retrieval)
        assert result.disposition is SemanticDisposition.REFUSE
        assert result.presented == ()

    def test_a_referentless_pronoun_shaped_result_refuses(self) -> None:
        """Held-out analogue of "What's holding it up?" — 11 candidates."""

        candidates = tuple(
            _candidate(f"wu_synthetic_{i}", 0.028 - i * 0.001, i, methods=_COSINE)
            for i in range(11)
        )
        retrieval = _retrieval(candidates, bm25_order=())
        result = assess_disposition(retrieval)
        assert result.disposition is SemanticDisposition.REFUSE

    def test_no_candidates_at_all_refuses(self) -> None:
        result = assess_disposition(_retrieval(()))
        assert result.disposition is SemanticDisposition.REFUSE
        assert result.presented == ()

    def test_a_lone_cosine_only_candidate_with_no_competition_refuses(self) -> None:
        """One result is not automatically safe. Nothing corroborates it."""

        candidate = _candidate("proj_lonely", 0.02, 0, methods=_COSINE)
        retrieval = _retrieval((candidate,), bm25_order=("wu_unrelated",))
        result = assess_disposition(retrieval)
        assert result.disposition is SemanticDisposition.REFUSE


class TestAGenuineLeaderProposes:
    """The controls: a rule that never proposes is not a rule."""

    def test_a_lexically_grounded_lone_candidate_proposes(self) -> None:
        candidate = _candidate("proj_solo", 0.05, 0, methods=_BOTH)
        retrieval = _retrieval((candidate,), bm25_order=("proj_solo",))
        result = assess_disposition(retrieval)
        assert result.disposition is SemanticDisposition.PROPOSE
        assert result.presented == (candidate,)

    def test_a_clear_margin_over_a_corroborated_leader_proposes(self) -> None:
        leader = _candidate("proj_leader", 0.05, 0, methods=_BOTH)
        runner_up = _candidate("proj_other", 0.02, 1, methods=_COSINE)
        retrieval = _retrieval(
            (leader, runner_up), bm25_order=("proj_leader", "some_other_hit")
        )
        result = assess_disposition(retrieval)
        assert result.disposition is SemanticDisposition.PROPOSE
        assert result.presented == (leader,)

    def test_an_uncorroborated_leader_with_a_clear_margin_still_clarifies_or_refuses(
        self,
    ) -> None:
        """FUZZY_LABEL alone never earns PROPOSE, margin or not.

        A cosine-only leader with a wide margin over the next candidate is
        still a single unverified vector guess; the contract's own
        ``WEAK_SUBJECT_MATCH_SIGNALS`` rule is exactly why this leg does not
        propose on cosine alone without lexical corroboration.
        """

        leader = _candidate("proj_leader", 0.05, 0, methods=_COSINE)
        runner_up = _candidate("proj_other", 0.01, 1, methods=_COSINE)
        retrieval = _retrieval((leader, runner_up), bm25_order=("something_else",))
        result = assess_disposition(retrieval)
        assert result.disposition is not SemanticDisposition.PROPOSE


class TestATightFieldClarifies:
    def test_two_near_tied_corroborated_candidates_clarify(self) -> None:
        first = _candidate("proj_a", 0.030, 0, methods=_BOTH)
        second = _candidate("proj_b", 0.029, 1, methods=_BOTH)
        retrieval = _retrieval((first, second), bm25_order=("proj_a", "proj_b"))
        result = assess_disposition(retrieval)
        assert result.disposition is SemanticDisposition.CLARIFY
        assert set(c.canonical_id for c in result.presented) == {"proj_a", "proj_b"}

    def test_clarification_is_bounded_even_with_many_tied_candidates(self) -> None:
        """Past the bound, a tied field is a diffuse ranking, not finalists."""

        candidates = tuple(
            _candidate(f"proj_tied_{i}", 0.030 - i * 0.0001, i, methods=_BOTH)
            for i in range(9)
        )
        retrieval = _retrieval(
            candidates, bm25_order=tuple(c.canonical_id for c in candidates)
        )
        result = assess_disposition(retrieval, clarification_limit=5)
        assert result.disposition is SemanticDisposition.REFUSE, (
            "9 tied candidates past a limit of 5 must not silently become an "
            "enumeration of the authorized world under a clarify heading"
        )

    def test_clarification_candidates_are_within_the_configured_limit(self) -> None:
        candidates = tuple(
            _candidate(f"proj_tied_{i}", 0.030 - i * 0.0001, i, methods=_BOTH)
            for i in range(4)
        )
        retrieval = _retrieval(
            candidates, bm25_order=tuple(c.canonical_id for c in candidates)
        )
        result = assess_disposition(retrieval, clarification_limit=5)
        assert result.disposition is SemanticDisposition.CLARIFY
        assert len(result.presented) <= 5


class TestTheRuleReadsShapeNotCorpusContent:
    """Anti-corpus-tuning control: swap every identity, keep the shape."""

    @pytest.mark.parametrize(
        "prefix",
        ["totally_unrelated_prefix", "another_org_shape", "zzz_synthetic"],
    )
    def test_the_no_match_shape_refuses_under_any_naming(self, prefix: str) -> None:
        candidates = tuple(
            _candidate(f"{prefix}_{i}", 0.03 - i * 0.0006, i, methods=_COSINE)
            for i in range(15)
        )
        retrieval = _retrieval(candidates, bm25_order=())
        result = assess_disposition(retrieval)
        assert result.disposition is SemanticDisposition.REFUSE
