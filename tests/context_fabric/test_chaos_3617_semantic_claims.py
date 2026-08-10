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
    EmbedderProvenanceMismatchError,
    JobContext,
    SubjectMatchFinding,
    TrialContext,
    UnsupportedMatchMechanismError,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark
from tests.context_fabric import live_gate

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


def _readout(projection, *, attested: str | None = None):
    """A readout, optionally attesting which embedder wrote its vectors.

    The in-memory reader attests nothing and is right not to — an unwritten
    projection holds no vectors. ``attested`` stands in for a store that
    recorded one, which is what the live partition really does; the live
    suite measures the real thing against a real FalkorDB write.
    """

    import asyncio
    import dataclasses

    readout = asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=projection.org_id,
            seed_canonical_ids=[_SUBJECT],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
    )
    assert readout.embedder_model_id is None, (
        "the in-memory reader attested an embedder; it has no store to ask"
    )
    if attested is None:
        return readout
    return dataclasses.replace(readout, embedder_model_id=attested)


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
    def test_the_same_match_is_permitted_under_an_ATTESTED_semantic_embedder(
        self, alpha_projection, signer, mechanism
    ) -> None:
        """The negative control.

        Without this, "the guard raised" could mean the builder is broken for
        every embedder, and the guard would look effective while proving
        nothing about the property it claims to enforce.

        The readout has to *attest* the embedder now, which is the finding
        this guard was widened for: an embedder object saying it carries
        semantics is a claim about the object, and the question is what
        produced the vectors that were searched.
        """

        packet = _build(
            _readout(alpha_projection, attested="stub_semantic"),
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


class TestASemanticClaimNeedsProvenanceNotAPromise:
    """The guard asked the caller whether the caller was right.

    ``embedder`` is an argument. ``GraphArmStore`` embeds at write time with
    whatever embedder it was constructed with, and ``build_packet`` is called
    later with an unrelated one; nothing compared them. So the check "does
    this embedder carry semantics" answered a different question from the one
    that matters — "were the vectors this readout was searched over produced
    by something semantic" — and three consequences followed, each reproduced
    by adversarial review and each pinned below.
    """

    def test_a_usable_semantic_embedder_alone_does_not_unlock_a_claim(
        self, alpha_projection, signer
    ) -> None:
        """Trap 1: the vectors and the embedder were never associated.

        Nothing about a perfectly good embedder says it produced what is in
        the store. On a readout that attests nothing, a semantic claim rests
        on the caller's word and is refused.
        """

        with pytest.raises(UnsupportedMatchMechanismError, match="nothing attests"):
            _build(
                _readout(alpha_projection),
                signer,
                embedder=_StubSemanticEmbedder(),
                matches=[
                    _finding(
                        MatchMechanism.EMBEDDING_SIMILARITY,
                        SubjectMatchSignal.FUZZY_LABEL,
                    )
                ],
            )

    def test_a_bare_cloud_embedder_carries_no_semantics(self) -> None:
        """Trap 2: ``CloudEmbedder()`` with no key still reported semantic.

        It refuses to *embed* without a key — but the guard never asks it to
        embed, it reads the flag. So a bare, unusable instance unlocked
        semantic claims outright.
        """

        assert CloudEmbedder().semantic is False, (
            "a bare CloudEmbedder reports semantics it cannot produce, so an "
            "instance that could not embed anything unlocks a semantic claim"
        )
        assert CloudEmbedder(api_key="sk-not-a-real-key").semantic is True

    def test_the_stamped_projection_version_cannot_name_another_embedder(
        self, alpha_projection, signer
    ) -> None:
        """Trap 3: the suffix is a label, and a consumer trusts it.

        ``embedder_projection_suffix`` goes into
        ``versions.projection_version``, so a packet could be stamped for an
        OpenAI model while the stored vectors are BLAKE2b hashes. Where the
        partition attests an embedder, a disagreeing caller is refused rather
        than silently restamped.
        """

        with pytest.raises(EmbedderProvenanceMismatchError, match="did not embed"):
            _build(
                _readout(alpha_projection, attested="deterministic_blake2b.v1.d1024"),
                signer,
                embedder=CloudEmbedder(api_key="sk-not-a-real-key"),
                matches=[],
            )

    def test_the_attested_embedder_still_builds_and_stamps_itself(
        self, alpha_projection, signer
    ) -> None:
        """The control. A refusal that fires on everything proves nothing."""

        from dev_health_ops.context_fabric.graph_arm.backend import (
            embedder_projection_suffix,
        )

        embedder = DeterministicEmbedder()
        packet = _build(
            _readout(alpha_projection, attested=embedder.model_id),
            signer,
            embedder=embedder,
            matches=[],
        )
        assert (
            embedder_projection_suffix(embedder) in packet.versions.projection_version
        )

    def test_an_unattested_readout_still_builds_without_a_semantic_claim(
        self, alpha_projection, signer
    ) -> None:
        """The scope of the refusal, stated as a test.

        An in-memory readout has no vectors, so there is nothing to disagree
        with and nothing to search by similarity. It may still produce a
        packet — refusing those would break every non-live path — it just may
        not carry a semantic match.
        """

        packet = _build(
            _readout(alpha_projection),
            signer,
            embedder=DeterministicEmbedder(),
            matches=[
                _finding(
                    MatchMechanism.EXACT_LOOKUP, SubjectMatchSignal.EXACT_CANONICAL_ID
                )
            ],
        )
        assert packet is not None


class TestOnePartitionMustAttestOneEmbedder:
    """Two embedders in one keyspace is a mixture, not a projection.

    Tested against the function rather than the store: producing a genuinely
    mixed partition needs two disjoint writes with different embedders, and a
    second write over the same nodes upserts them — so a live reproduction
    would quietly test the single-value case while looking like the mixed
    one. The read is what has to refuse, and the read is what is exercised.
    """

    @staticmethod
    def _driver(*rows):
        class _Driver:
            async def execute_query(self, query: str, **params: object):
                return list(rows), None, None

        return _Driver()

    def _attested(self, *rows):
        import asyncio

        from dev_health_ops.context_fabric.graph_arm import readback

        return asyncio.run(
            readback._attested_embedder(self._driver(*rows), "cfgraph_orgalpha")
        )

    def test_two_attested_embedders_are_refused(self) -> None:
        from dev_health_ops.context_fabric.graph_arm.readback import (
            MixedProjectionProvenanceError,
        )

        with pytest.raises(MixedProjectionProvenanceError, match="two embedders"):
            self._attested(
                {"embedder_model_id": "deterministic_blake2b.v1.d1024"},
                {"embedder_model_id": "openai_text_embedding_3_small"},
            )

    def test_one_attested_embedder_is_returned(self) -> None:
        """The control: a normal partition reads back its own embedder."""

        assert (
            self._attested({"embedder_model_id": "deterministic_blake2b.v1.d1024"})
            == "deterministic_blake2b.v1.d1024"
        )

    def test_a_partition_that_attests_nothing_says_so(self) -> None:
        """A store written before the attestation existed. Not a permission.

        ``None`` refuses every semantic claim downstream, which is the safe
        direction; returning a plausible default would be the permissive one.
        """

        assert self._attested() is None
        assert self._attested({"embedder_model_id": None}) is None


class TestEmbedderContracts:
    def test_the_deterministic_embedder_declares_itself_non_semantic(self) -> None:
        assert DeterministicEmbedder().semantic is False

    def test_a_configured_cloud_embedder_declares_itself_semantic(self) -> None:
        """Keyed on being usable. See the trap-2 test above for the why."""

        assert CloudEmbedder(api_key="sk-not-a-real-key").semantic is True

    def test_the_cloud_embedder_refuses_to_degrade_silently(self, monkeypatch) -> None:
        """No key means no semantic run — not a hash run wearing its label.

        A fallback here would produce a run that looks semantic in every
        artifact and scores like noise, which is strictly worse than failing.
        """

        monkeypatch.delenv("LLM_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        with pytest.raises(RuntimeError, match="Refusing to fall back"):
            CloudEmbedder.from_environment()

    @pytest.mark.asyncio
    async def test_a_bare_cloud_embedder_cannot_embed_via_an_ambient_credential(
        self, monkeypatch
    ) -> None:
        """Issue 3632: ``semantic=False`` must be load-bearing, not advisory.

        The gap this closes: ``CloudEmbedder(api_key=None)`` correctly
        reports ``semantic=False`` -- but the underlying OpenAI SDK falls
        back to an AMBIENT ``OPENAI_API_KEY`` environment variable whenever
        an explicit ``api_key=None`` is passed through. Confirmed directly
        (not assumed): before this guard existed, setting ``OPENAI_API_KEY``
        in the process environment and calling ``create()`` on a bare,
        unconfigured ``CloudEmbedder()`` attempted a real outbound
        connection using that ambient credential -- one this instance never
        explicitly received through the arm's own credential convention
        (:meth:`CloudEmbedder.from_environment`).

        This matters specifically for the issue 3632 document path:
        ``to_graphiti_document_nodes`` calls ``embedder.create()``
        unconditionally for every embedder (unlike ``to_graphiti_nodes``'s
        deterministic-only special case), so a bare ``CloudEmbedder()``
        reaching that path would send a document's body text to a provider
        under a credential this instance's own state says it does not have
        -- exactly the "fail closed ... when providers/text indexing are
        disallowed" acceptance criterion this ticket names.

        Socket-blocked rather than network-mocked, so this proves NO
        connection is attempted, not merely that one eventually fails.
        """

        import socket

        monkeypatch.setenv("OPENAI_API_KEY", "sk-ambient-not-explicitly-passed")

        def _blocked_connect(self, *_args, **_kwargs):
            raise AssertionError(
                "CloudEmbedder.create() attempted a real socket connection "
                "with no api_key configured -- it must refuse before "
                "constructing a client at all, never after"
            )

        monkeypatch.setattr(socket.socket, "connect", _blocked_connect)

        embedder = CloudEmbedder()
        assert embedder.semantic is False

        with pytest.raises(RuntimeError, match="refusing to fall back"):
            await embedder.create(input_data=["a document's body text"])

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

        Unlike its sibling, this one lets the write **proceed**, and the write
        path builds Graphiti node and edge objects — so it needs the optional
        extra even though it needs no server. It was failing in CI for exactly
        that reason (``ModuleNotFoundError: graphiti_core``, reported as an
        arm error) rather than skipping like the rest of the suite's
        Graphiti-dependent half. Routed through the gate so the outcome is the
        declared two: a failure when a run was required, a named skip
        otherwise. Never a silent error that reads like a defect in the arm.
        """

        live_gate.require_graphiti_extra()

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

        class _FakeConnection:
            """CHAOS-3679: ``persist_watermark`` reaches this after a real
            write, via ``driver.client.connection``. Not a FalkorDB fake --
            this test never talks to one -- just enough to let the
            durable-watermark write this store now always performs on a
            successful projection complete without erroring."""

            async def set(self, _key: str, _value: str) -> None:
                return None

        class _FakeClient:
            connection = _FakeConnection()

        class _RecordingDriver:
            provider = "fake"
            graph_operations_interface = None
            client = _FakeClient()

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
