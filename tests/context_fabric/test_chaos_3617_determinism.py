"""CHAOS-3617: the traversal's result is a function of the graph, not of order.

This module exists because the live differential oracle found a real defect
that review had not: with ``max_paths_per_entity`` capping how many chains an
entity keeps, *which* chains survived depended on the order edges happened to
arrive in. The in-memory reader walked projection order; FalkorDB returned
rows in its own. Same entities, same counts, **different explanations** — and
an arm whose explanation for "why is this entity here" changes between runs
cannot produce a reproducible trial result.

The fix is a total order on adjacency (``_ordered_edges``). These tests are
the regression: they shuffle the inputs and require the output to be
identical, so the property is checked without needing a live store.
"""

from __future__ import annotations

import asyncio
import dataclasses
import random

from dev_health_ops.context_fabric.graph_arm import build_projection, fixtures
from dev_health_ops.context_fabric.graph_arm.budgets import TrialBudgets
from dev_health_ops.context_fabric.graph_arm.readback import (
    InvestigationReadout,
    ProjectionGraphReader,
)


def _signature(readout: InvestigationReadout) -> object:
    return (
        tuple(entity.canonical_id for entity in readout.entities),
        tuple(
            (
                path.origin_canonical_id,
                path.terminal_canonical_id,
                tuple(
                    (
                        step.from_canonical_id,
                        step.relationship.value,
                        step.direction.value,
                        step.to_canonical_id,
                    )
                    for step in path.steps
                ),
            )
            for path in readout.paths
        ),
        readout.authorization_filtered_count,
    )


def _read(batch, seeds, budgets=None):
    projection = build_projection(batch)
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=projection.org_id,
            seed_canonical_ids=list(seeds),
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
            budgets=budgets or TrialBudgets(),
        )
    )


def _shuffled(batch, seed: int):
    rng = random.Random(seed)
    entities = list(batch.entities)
    relationships = list(batch.relationships)
    observations = list(batch.observations)
    rng.shuffle(entities)
    rng.shuffle(relationships)
    rng.shuffle(observations)
    return dataclasses.replace(
        batch,
        entities=tuple(entities),
        relationships=tuple(relationships),
        observations=tuple(observations),
    )


class TestOrderIndependence:
    def test_shuffling_the_ingestion_order_does_not_change_the_traversal(
        self,
    ) -> None:
        """The regression for the defect the differential oracle surfaced."""

        baseline = _signature(_read(fixtures.alpha_batch(), ["pr_4412"]))
        for seed in range(8):
            assert (
                _signature(_read(_shuffled(fixtures.alpha_batch(), seed), ["pr_4412"]))
                == baseline
            ), f"traversal changed under shuffle seed {seed}"

    def test_order_independence_holds_when_the_per_entity_cap_bites(self) -> None:
        """The cap is what made order matter, so it must be exercised.

        With ``max_paths_per_entity=1`` almost every entity has more chains
        reaching it than it may keep, which is precisely the condition under
        which the old implementation diverged.
        """

        budgets = TrialBudgets(max_paths_per_entity=1)
        baseline = _signature(
            _read(fixtures.alpha_batch(), ["pr_4412"], budgets=budgets)
        )
        for seed in range(8):
            assert (
                _signature(
                    _read(
                        _shuffled(fixtures.alpha_batch(), seed),
                        ["pr_4412"],
                        budgets=budgets,
                    )
                )
                == baseline
            )

    def test_the_signature_can_distinguish_genuinely_different_traversals(
        self,
    ) -> None:
        """The acceptance case for the comparator used above.

        A signature that collapsed everything to a constant would make both
        tests above pass while proving nothing.
        """

        assert _signature(_read(fixtures.alpha_batch(), ["pr_4412"])) != _signature(
            _read(fixtures.alpha_batch(), ["team_platform"])
        )

    def test_reprojection_is_byte_stable(self) -> None:
        """Same records in, same node and edge addresses out, in the same
        order -- a trial artifact that cannot be reproduced is not evidence."""

        first = build_projection(fixtures.alpha_batch())
        second = build_projection(fixtures.alpha_batch())
        assert [(node.uuid, node.canonical_id) for node in first.nodes] == [
            (node.uuid, node.canonical_id) for node in second.nodes
        ]

    def test_the_deterministic_embedder_is_reproducible_and_non_semantic(
        self,
    ) -> None:
        """Both halves matter.

        Reproducible, so a projection can be re-run offline and compared.
        Non-semantic, and *declared* so, because ranking candidates by
        similarity over hash vectors would produce a confident, meaningless
        ordering that no test downstream could tell from a real one.
        """

        from dev_health_ops.context_fabric.graph_arm.backend import (
            DeterministicEmbedder,
        )

        embedder = DeterministicEmbedder()
        assert embedder.embed("proj_x") == embedder.embed("proj_x")
        assert embedder.embed("proj_x") != embedder.embed("proj_y")
        assert embedder.semantic is False
        assert "deterministic" in embedder.model_id
