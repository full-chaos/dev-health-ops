"""CHAOS-3617: no semantic capability claim while the embedder has no semantics.

:class:`~.backend.DeterministicEmbedder` produces vectors from BLAKE2b. They
are reproducible and free, and they carry **no semantic similarity
whatsoever** — nearest-neighbour search over them returns a confident,
arbitrary ordering. The danger is not that it fails; it is that it *succeeds
convincingly*: a packet presenting that ordering as an alias or fuzzy-label
match would be scored by CHAOS-3616 as a retrieval capability the arm does
not have, and nothing in the packet would look wrong.

A doc note cannot prevent that. So the rule is a guard: ``build_packet``
refuses to emit a match whose mechanism needs semantics, or whose *signal* is
inherently semantic, while the active embedder reports ``semantic=False``.

The mechanism is arm-internal and never reaches the wire — the frozen
contract has no field for it and forbids extras. That is the right place for
it: it is an integrity concern about how the arm produced a claim, not
something a consumer should branch on.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
    SubjectMatchSignal,
)
from dev_health_ops.context_fabric.graph_arm import fixtures
from dev_health_ops.context_fabric.graph_arm.backend import (
    SEMANTIC_MECHANISMS,
    CloudEmbedder,
    DeterministicEmbedder,
    MatchMechanism,
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

_RUN_ID = "4f9a2c1e-1111-4222-8333-444455556666"
_SUBJECT = "proj_nightfall_migration"


@dataclass(frozen=True, slots=True)
class _StubSemanticEmbedder:
    """A semantic embedder that makes no network call.

    Needed for the *negative control*: without it, "the guard raised" could
    equally mean "the builder is broken for every embedder", and the guard
    would look effective while proving nothing.
    """

    @property
    def model_id(self) -> str:
        return "stub_semantic"

    @property
    def semantic(self) -> bool:
        return True

    async def create(self, input_data: Any) -> list[float]:  # pragma: no cover
        return [0.0]


def _readout(projection):
    import asyncio

    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=projection.org_id,
            seed_canonical_ids=[_SUBJECT],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
    )


def _build(readout, signer, *, embedder, matches):
    return build_packet(
        readout=readout,
        job=JobContext(
            job_id="job_semantic",
            question_family=QuestionFamilyID("project_status_drivers"),
            job_statement="Status of the Nightfall Migration project.",
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
            window_start=fixtures.WINDOW_START,
            window_end=fixtures.WINDOW_END,
        ),
        watermark=IndexWatermark(
            indexed_through=fixtures.WINDOW_END,
            projected_at=fixtures.WINDOW_END,
            records_indexed=42,
        ),
        signer=signer,
        trial=TrialContext(run_id=_RUN_ID),
        produced_at=datetime(2026, 8, 8, 12, tzinfo=UTC),
        embedder=embedder,
        subject_matches=matches,
    )


def _finding(mechanism: MatchMechanism, signal: SubjectMatchSignal):
    return SubjectMatchFinding(
        canonical_id=_SUBJECT,
        signal=signal,
        matched_text="the auth work",
        source_class=SourceClass.WORK_GRAPH,
        mechanism=mechanism,
    )


class TestSemanticClaimsAreRefusedUnderANonSemanticEmbedder:
    @pytest.mark.parametrize("mechanism", sorted(SEMANTIC_MECHANISMS))
    def test_an_embedding_derived_match_is_refused(
        self, alpha_projection, signer, mechanism
    ) -> None:
        with pytest.raises(UnsupportedMatchMechanismError, match="does not have"):
            _build(
                _readout(alpha_projection),
                signer,
                embedder=DeterministicEmbedder(),
                matches=[_finding(mechanism, SubjectMatchSignal.FUZZY_LABEL)],
            )

    def test_an_inherently_semantic_signal_is_refused_whatever_it_claims(
        self, alpha_projection, signer
    ) -> None:
        """A producer can get this wrong from either end.

        Claiming ``CONVERSATIONAL_REFERENCE`` — "the other project we
        discussed" — while asserting it came from an exact lookup is not a
        mechanism the arm can perform, so the signal is checked as well as
        the mechanism.
        """

        with pytest.raises(UnsupportedMatchMechanismError):
            _build(
                _readout(alpha_projection),
                signer,
                embedder=DeterministicEmbedder(),
                matches=[
                    _finding(
                        MatchMechanism.EXACT_LOOKUP,
                        SubjectMatchSignal.CONVERSATIONAL_REFERENCE,
                    )
                ],
            )

    @pytest.mark.parametrize(
        "mechanism",
        [
            MatchMechanism.EXACT_LOOKUP,
            MatchMechanism.ALIAS_LOOKUP,
            MatchMechanism.LEXICAL_FUZZY,
        ],
    )
    def test_non_semantic_mechanisms_are_permitted(
        self, alpha_projection, signer, mechanism
    ) -> None:
        """The guard must not ban honest lexical work.

        ``FUZZY_LABEL`` from Levenshtein over stored labels needs no model
        and is a legitimate match. A guard that banned it under a hash
        embedder would push a future implementation toward mislabelling its
        own mechanism to get past the check.
        """

        packet = _build(
            _readout(alpha_projection),
            signer,
            embedder=DeterministicEmbedder(),
            matches=[_finding(mechanism, SubjectMatchSignal.FUZZY_LABEL)],
        )
        assert packet is not None

    @pytest.mark.parametrize("mechanism", sorted(SEMANTIC_MECHANISMS))
    def test_the_same_match_is_permitted_under_a_semantic_embedder(
        self, alpha_projection, signer, mechanism
    ) -> None:
        """The negative control.

        Without this, "the guard raised" could mean the builder is broken for
        every embedder, and the guard would look effective while proving
        nothing about the property it claims to enforce.
        """

        packet = _build(
            _readout(alpha_projection),
            signer,
            embedder=_StubSemanticEmbedder(),
            matches=[_finding(mechanism, SubjectMatchSignal.FUZZY_LABEL)],
        )
        assert packet is not None

    def test_the_default_embedder_is_the_non_semantic_one(
        self, alpha_projection, signer
    ) -> None:
        """Omitting the embedder must not quietly relax the guard.

        Defaulting to "semantic" would make the check opt-in, and the one
        caller that forgot would be the one that emitted the unsupported
        claim.
        """

        with pytest.raises(UnsupportedMatchMechanismError):
            _build(
                _readout(alpha_projection),
                signer,
                embedder=None,
                matches=[
                    _finding(
                        MatchMechanism.EMBEDDING_SIMILARITY,
                        SubjectMatchSignal.FUZZY_LABEL,
                    )
                ],
            )


class TestEmbedderContracts:
    def test_the_deterministic_embedder_declares_itself_non_semantic(self) -> None:
        assert DeterministicEmbedder().semantic is False

    def test_the_cloud_embedder_declares_itself_semantic(self) -> None:
        assert CloudEmbedder().semantic is True

    def test_the_cloud_embedder_refuses_to_degrade_silently(self, monkeypatch) -> None:
        """No key means no semantic run — not a hash run wearing its label.

        A fallback here would produce a run that looks semantic in every
        artifact and scores like noise, which is strictly worse than failing.
        """

        monkeypatch.delenv("LLM_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        with pytest.raises(RuntimeError, match="Refusing to fall back"):
            CloudEmbedder.from_environment()

    def test_the_cloud_embedder_reads_the_repos_credential_convention(
        self, monkeypatch
    ) -> None:
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.setenv("LLM_API_KEY", "sk-not-a-real-key")
        monkeypatch.setenv(
            "CONTEXT_FABRIC_GRAPH_EMBEDDING_MODEL", "text-embedding-3-large"
        )
        embedder = CloudEmbedder.from_environment()
        assert embedder.model == "text-embedding-3-large"
        assert embedder.api_key == "sk-not-a-real-key"

    def test_the_semantic_mechanism_set_is_not_empty(self) -> None:
        """Anti-vacuity: an empty set makes every refusal test above pass."""

        assert SEMANTIC_MECHANISMS
        assert MatchMechanism.EMBEDDING_SIMILARITY in SEMANTIC_MECHANISMS
        assert MatchMechanism.EXACT_LOOKUP not in SEMANTIC_MECHANISMS


class TestEmbeddingBudget:
    def test_a_projection_over_the_embedding_budget_is_refused_before_any_call(
        self, alpha_projection, monkeypatch
    ) -> None:
        """Checked pre-flight, so an over-budget run costs nothing.

        The count is known before the first call — one vector per node and
        per edge — so there is no reason to spend most of the budget and then
        stop half-written.
        """

        from dev_health_ops.context_fabric.graph_arm.budgets import TrialBudgets
        from dev_health_ops.context_fabric.graph_arm.store import (
            EmbeddingBudgetExceededError,
            GraphArmStore,
        )

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        calls: list[object] = []

        class _CountingEmbedder(_StubSemanticEmbedder):
            async def create(self, input_data: Any) -> list[float]:
                calls.append(input_data)
                return [0.0]

        # The driver has to be complete enough that the write would REALLY
        # proceed when the guard is off -- otherwise the mutation "kills" this
        # test by crashing on a missing attribute and proves nothing about the
        # budget. (That is exactly what happened: the first version had only
        # `close`, so disabling the guard failed with
        # `'_RefusingDriver' object has no attribute 'session'` and the
        # harness's failure-category check caught it.)
        class _FakeSession:
            async def execute_write(self, func, *args: object, **kwargs: object):
                return await func(self, *args, **kwargs)

            async def run(self, query: object, **kwargs: object) -> None:
                return None

            async def close(self) -> None:
                return None

        class _RecordingDriver:
            provider = "fake"
            graph_operations_interface = None

            def session(self) -> _FakeSession:
                return _FakeSession()

            async def close(self) -> None:
                return None

        store = GraphArmStore(
            org_id=fixtures.ALPHA_ORG,
            driver=_RecordingDriver(),
            embedder=_CountingEmbedder(),
        )
        with pytest.raises(EmbeddingBudgetExceededError, match="embedding calls"):
            import asyncio

            asyncio.run(
                store.write_projection(
                    alpha_projection, budgets=TrialBudgets(max_embedding_calls=1)
                )
            )
        assert calls == [], "the budget must bite before the first call is made"

    def test_the_embedder_is_really_exercised_under_a_generous_budget(
        self, alpha_projection, monkeypatch
    ) -> None:
        """Anti-vacuity for ``calls == []`` in the test above.

        That assertion only means "the bound bit before spending" if the
        embedder would otherwise have been called at all. If the stub driver
        or the write path silently skipped embedding, ``calls == []`` would
        hold for a reason that has nothing to do with the budget — and the
        pre-flight claim would be unproven while looking proven.
        """

        from dev_health_ops.context_fabric.graph_arm.budgets import TrialBudgets
        from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        calls: list[object] = []

        class _CountingEmbedder(_StubSemanticEmbedder):
            async def create(self, input_data: Any) -> list[float]:
                calls.append(input_data)
                return [0.0]

        class _FakeSession:
            async def execute_write(self, func, *args: object, **kwargs: object):
                return await func(self, *args, **kwargs)

            async def run(self, query: object, **kwargs: object) -> None:
                return None

            async def close(self) -> None:
                return None

        class _RecordingDriver:
            provider = "fake"
            graph_operations_interface = None

            def session(self) -> _FakeSession:
                return _FakeSession()

            async def close(self) -> None:
                return None

        store = GraphArmStore(
            org_id=fixtures.ALPHA_ORG,
            driver=_RecordingDriver(),
            embedder=_CountingEmbedder(),
        )
        import asyncio

        asyncio.run(
            store.write_projection(
                alpha_projection, budgets=TrialBudgets(max_embedding_calls=5_000)
            )
        )
        assert calls, (
            "the embedder was never called even with a generous budget, so "
            "`calls == []` in the budget test proves nothing about pre-flight"
        )

    def test_a_non_semantic_embedder_is_not_charged(self, alpha_projection) -> None:
        """DeterministicEmbedder makes no calls, so it spends no budget."""

        from dev_health_ops.context_fabric.graph_arm.budgets import TrialBudgets

        budgets = TrialBudgets(max_embedding_calls=1)
        assert not budgets.check_embedding_calls(
            len(alpha_projection.nodes) + len(alpha_projection.edges)
        ).within_budget
        assert DeterministicEmbedder().semantic is False
