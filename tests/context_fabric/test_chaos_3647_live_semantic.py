"""CHAOS-3647: the semantic leg against a live store and a real model.

This module is the only place the leg is exercised end to end, and it is
gated twice — a reachable FalkorDB and a real embedding model — because
neither can be faked without destroying what is being measured. A stubbed
embedder is the deterministic baseline wearing a costume, and a stubbed store
is an assertion about a fixture.

Both gates route through :mod:`live_gate`, so an unavailable store **fails**
under ``CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1`` rather than skipping. The
embedder gate is spelled out here rather than borrowed, because "no API key"
is not the same missing piece and a message naming the store would send a
reader to the wrong place.

What is asserted here is deliberately not "the semantic leg answers well".
That is a measurement, it lives in ``trials/chaos_3647/results/``, and it
would be an unstable and self-fulfilling assertion in a test. What is
asserted is that the leg is *sound*: it reproduces the pinned baseline on the
deterministic path, its authorization filter is exercised by something real,
and its cross-partition exclusion is a differential rather than an empty
result.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import pytest_asyncio

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.backend import CloudEmbedder
from dev_health_ops.context_fabric.graph_arm.projection import build_projection
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    wait_for_fulltext_index,
)
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore
from trials.chaos_3647.legs import LegId, resolve_deterministic, resolve_semantic
from trials.chaos_3647.probes import PROBES, run_probe

from . import live_gate

#: ``loop_scope="module"`` is load-bearing, not tidiness. The FalkorDB driver
#: the store holds binds its connection pool to the loop it was created on,
#: so a module-scoped fixture handing that store to function-scoped loops
#: fails with "attached to a different loop" — on the *second* test, which is
#: how it reads as a flake rather than as a configuration error. One loop for
#: the module keeps the single expensive ingestion and the driver together.
pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio(loop_scope="module")]

_PINNED = (
    Path(__file__).resolve().parents[2]
    / "trials"
    / "chaos_3619"
    / "results"
    / "trial-results.records.json"
)


def _require_semantic_embedder() -> CloudEmbedder:
    """Two outcomes, like the store gate. Never a silent downgrade."""

    if os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY"):
        return CloudEmbedder.from_environment()
    message = (
        "no LLM_API_KEY/OPENAI_API_KEY is set, so no real embedding model is "
        "available and the semantic leg cannot be measured. Falling back to "
        "DeterministicEmbedder is refused: the run would look semantic and "
        "score like noise"
    )
    from dev_health_ops.context_fabric.graph_arm.flags import live_store_required

    if live_store_required():
        pytest.fail(f"a live measurement was required and did not happen: {message}")
    pytest.skip(message)
    raise AssertionError("pytest.skip always raises")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def semantic_world():
    """Both tenants, written once under a real embedding model.

    Module-scoped because every write costs real embedding calls — 226 for
    Helio — and per-test ingestion would multiply that by the number of tests
    without changing a single answer.

    The Lumen partition is written for one reason: the cross-tenant probe.
    An exclusion asserted against an empty neighbouring keyspace is an
    assertion that cannot fail.
    """

    config = live_gate.require_live_store()
    embedder = _require_semantic_embedder()
    os.environ["CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED"] = "1"

    helio = build_projection(adapter.corpus_batch(world.ORG_HELIO))
    lumen = build_projection(adapter.corpus_batch(world.ORG_LUMEN))
    helio_store = GraphArmStore.for_org(
        world.ORG_HELIO, config=config, embedder=embedder
    )
    lumen_store = GraphArmStore.for_org(
        world.ORG_LUMEN, config=config, embedder=embedder
    )
    try:
        for store, projection in ((helio_store, helio), (lumen_store, lumen)):
            await store.purge_org()
            await store.build_indices()
            await store.write_projection(projection)
        # BM25 must be live before any test reads a hybrid result. Without
        # this the suite's "the leg runs against the real thing" assertion
        # passes on a cosine-only result — which is exactly what one recorded
        # trial run did, silently.
        await wait_for_fulltext_index(
            helio_store,
            probe_query=world.ENTITIES_BY_ID[world.PROJ_IDENTITY_REWRITE].display_label,
            expected_canonical_id=world.PROJ_IDENTITY_REWRITE,
        )
        await wait_for_fulltext_index(
            lumen_store,
            probe_query=world.ENTITIES_BY_ID[world.LUMEN_PROJ_ACR].display_label,
            expected_canonical_id=world.LUMEN_PROJ_ACR,
        )
        yield helio_store, lumen_store, helio, embedder
    finally:
        for store in (helio_store, lumen_store):
            try:
                await store.purge_org()
            finally:
                await store.close()
        os.environ.pop("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", None)


class TestThePinnedBaselineIsReproduced:
    """The re-run deterministic leg must agree with the pinned artifact.

    Re-running the baseline in-process rather than reading its numbers is
    what makes the comparison same-world and same-commit. It is also how a
    baseline silently drifts, so the agreement is asserted rather than
    assumed — on the two ambiguity cases the pinned run actually scored.
    """

    @pytest.mark.parametrize(
        ("case_id", "question", "expected"),
        [
            ("H01_acronym_resolution", "status on IPR?", "proj_identity_rewrite"),
            (
                "H02_old_and_current_name",
                "how's the thing we used to call Northstar going?",
                "proj_identity_rewrite",
            ),
        ],
    )
    async def test_the_deterministic_leg_still_resolves_what_it_resolved(
        self, semantic_world, case_id: str, question: str, expected: str
    ) -> None:
        _helio_store, _lumen_store, projection, _embedder = semantic_world
        resolution = resolve_deterministic(
            question=question,
            projection=projection,
            authorized_entity_ids=adapter.authorized_entity_ids_for(
                world.PRINCIPAL_ANALYST
            ),
            over_mentions=True,
        )
        assert resolution.leg is LegId.DETERMINISTIC_MENTIONS
        assert resolution.top_1 == expected

        pinned = json.loads(_PINNED.read_text(encoding="utf-8"))
        dispositions = {
            arm.get("disposition")
            for case in pinned["cases"]
            if case.get("case_id") == case_id
            for arm in case.get("arms", ())
            if arm.get("arm_id") == "graph_assisted_shadow_arm"
        }
        assert "scored" in dispositions, (
            f"{case_id} is no longer scored in the pinned artifact, so this "
            "reproduction is comparing against something that changed"
        )

    async def test_the_colloquial_cases_still_resolve_nothing_deterministically(
        self, semantic_world
    ) -> None:
        """The gap CHAOS-3647 was opened about, re-measured.

        The pinned run recorded ``no authorized subject resolved from the
        question`` for these. If that ever stops being true the semantic
        leg's whole delta is measured against a different baseline, and the
        artifact would say otherwise without anything failing.
        """

        _helio_store, _lumen_store, projection, _embedder = semantic_world
        grant = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        for question in (
            "What about the auth work?",
            "what's holding it up?",
            "What about the other project we discussed?",
            "What happened with the project that kept cycling in review?",
            "how's the payments work going",
        ):
            resolution = resolve_deterministic(
                question=question,
                projection=projection,
                authorized_entity_ids=grant,
                over_mentions=True,
            )
            assert resolution.subjects == (), (
                f"{question!r} now resolves deterministically; the pinned "
                "baseline this leg is measured against has moved"
            )


class TestTheSemanticLegRunsAgainstTheRealThing:
    async def test_it_retrieves_over_live_vectors_a_real_model_wrote(
        self, semantic_world
    ) -> None:
        """Soundness, not quality: the leg reaches the store and ranks.

        The embedder identity is asserted because it is the one fact that
        makes every other number in the artifact mean what it says. A run
        under ``deterministic_blake2b`` would produce a complete, plausible
        result set that is noise in every row.
        """

        helio_store, _lumen_store, _projection, embedder = semantic_world
        assert embedder.semantic
        assert embedder.model_id.startswith("openai_")

        resolution = await resolve_semantic(
            question="how's the payments work going",
            store=helio_store,
            authorized_entity_ids=adapter.authorized_entity_ids_for(
                world.PRINCIPAL_ANALYST
            ),
        )
        assert resolution.leg is LegId.SEMANTIC_HYBRID
        assert resolution.subjects, (
            "hybrid retrieval returned nothing for a query naming a real "
            "project; the store, the index or the vectors are not what this "
            "leg thinks they are"
        )
        assert resolution.cosine_order, "the cosine primitive returned nothing"
        assert resolution.bm25_order, (
            "the BM25 primitive returned nothing, so this 'hybrid' result is "
            "cosine-only. A recorded run of this trial looked exactly like "
            "this in all eight cases and nothing failed"
        )
        assert all(
            subject.mechanism in {"embedding_similarity", "lexical_fuzzy"}
            for subject in resolution.subjects
        )


class TestTheAuthorizationProbesAreEffective:
    """Not "authorization held" — "authorization was asked, and held".

    The eight corpus ambiguity questions withhold nothing, because none is
    near the restricted project. A verdict drawn from those alone would be a
    property of the questions.
    """

    @pytest.mark.parametrize("probe", PROBES, ids=lambda p: p.probe_id)
    async def test_each_probe_measures_what_it_claims(
        self, semantic_world, probe
    ) -> None:
        helio_store, lumen_store, _projection, embedder = semantic_world
        outcome = await run_probe(
            probe,
            store=helio_store,
            authorized_entity_ids=adapter.authorized_entity_ids_for(probe.principal_id),
            presence_store=lumen_store if probe.presence_partition else None,
            presence_authorized_entity_ids=(
                adapter.authorized_entity_ids_for(probe.presence_principal_id)
                if probe.presence_partition
                else None
            ),
        )
        assert outcome.effective, (
            f"{probe.probe_id} did not reach the boundary it exists to test: "
            f"{outcome.detail}. A probe that measures nothing must not be "
            "read as a clean result"
        )
        assert outcome.verdict == "pass", outcome.detail

    async def test_a_restricted_entity_is_retrieved_and_then_withheld(
        self, semantic_world
    ) -> None:
        """The load-bearing one, spelled out.

        Retrieval must surface ``proj_quarry`` — it is in the analyst's own
        partition, one hop from teams the analyst owns, and its display label
        is exactly the query — and the grant must be what removes it. If the
        first half stops being true, the second half proves nothing.
        """

        helio_store, _lumen_store, _projection, embedder = semantic_world
        resolution = await resolve_semantic(
            question="Quarry Compliance",
            store=helio_store,
            authorized_entity_ids=adapter.authorized_entity_ids_for(
                world.PRINCIPAL_ANALYST
            ),
        )
        assert world.PROJ_QUARRY in resolution.withheld_canonical_ids, (
            "retrieval never surfaced the restricted project, so the filter "
            "was not exercised and the clean result below is vacuous"
        )
        assert resolution.authorization_filtered_count >= 1
        assert world.PROJ_QUARRY not in resolution.top_n(len(resolution.subjects))

    async def test_the_same_query_ranks_it_for_the_principal_who_may_see_it(
        self, semantic_world
    ) -> None:
        """The other half of the differential.

        Without this, "the analyst does not get proj_quarry" could be an
        absence of data rather than an authorization decision.
        """

        helio_store, _lumen_store, _projection, embedder = semantic_world
        resolution = await resolve_semantic(
            question="Quarry Compliance",
            store=helio_store,
            authorized_entity_ids=adapter.authorized_entity_ids_for(
                world.PRINCIPAL_COMPLIANCE
            ),
        )
        assert world.PROJ_QUARRY in {
            subject.canonical_id for subject in resolution.subjects
        }, (
            "the compliance principal, who IS granted the restricted "
            "project, cannot retrieve it either — so the analyst's empty "
            "result is structural and not an authorization decision"
        )
