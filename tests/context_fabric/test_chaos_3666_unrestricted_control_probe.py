"""CHAOS-3666: ``unrestricted_control``'s pass condition predates the
CHAOS-3653 observation->entity hop and is now stale.

**The gap this closes.** Regenerating the CHAOS-3647 trial artifact live
(2026-08-10) flipped ``unrestricted_control`` from pass to fail for the
first time since the probe was written -- not from a leak
(``any_unauthorized_entity_ranked`` stayed ``false`` throughout, and the
restricted entity never once reached ``ranked_canonical_ids``, only
``withheld_canonical_ids``), but because CHAOS-3653's hop legitimately
widens what gets retrieved: an authorized observation's own subjects now
enter the candidate pool, and for this control's query that pool
incidentally included the restricted ``proj_quarry``, which the filter then
correctly excluded.

The probe's original pass condition, ``authorization_filtered_count == 0``,
assumed a query "aimed at nothing restricted" would never retrieve anything
restricted at all. That assumption predates the hop. Every id in
``withheld_canonical_ids`` is, by construction, genuinely outside
``authorized_entity_ids`` (see ``semantic_retrieval.retrieve_candidates``'s
own withholding logic) -- so a nonzero count here can only mean "something
else, legitimately restricted, was correctly excluded", never "the control
target was over-filtered". Conflating the two turns a correct, expected
consequence of a widened retrieval net into a false failure.

The real invariant the control exists to protect -- "the control target
itself is not being swallowed by an over-eager filter" -- is preserved by
checking ``target_ranked`` alone; a withholding of something else is a
different, unrelated, correct exclusion this control was never measuring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
from trials.chaos_3647.probes import AuthorizationProbe, run_probe

from . import live_gate

pytestmark = [pytest.mark.graphiti, pytest.mark.asyncio]

_PARTITION = "cf_trial_org_test"


@dataclass
class _FakeNode:
    uuid: str
    name: str
    group_id: str = _PARTITION
    attributes: dict[str, Any] = field(default_factory=dict)


def _entity_node(uuid: str, canonical_id: str, label: str) -> _FakeNode:
    return _FakeNode(
        uuid=uuid,
        name=label,
        attributes={
            "cf_canonical_id": canonical_id,
            "cf_entity_kind": GraphEntityKind.PROJECT.value,
            "cf_source_class": SourceClass.WORK_GRAPH.value,
        },
    )


@dataclass(frozen=True)
class _SemanticEmbedder:
    model_id: str = "test_semantic_double.v1"
    semantic: bool = True

    async def create(self, input_data: Any) -> list[float]:
        return [0.0] * 8


@dataclass(frozen=True)
class _FakeStore:
    driver: Any
    partition: str
    embedder: Any


def _install(monkeypatch: pytest.MonkeyPatch, *, bm25: list, cosine: list) -> None:
    live_gate.require_graphiti_extra()
    from graphiti_core.search import search_utils

    async def fake_fulltext(driver, query, search_filter, group_ids, limit):
        return list(bm25)

    async def fake_similarity(
        driver, search_vector, search_filter, group_ids, limit, *args, **kwargs
    ):
        return list(cosine)

    monkeypatch.setattr(search_utils, "node_fulltext_search", fake_fulltext)
    monkeypatch.setattr(search_utils, "node_similarity_search", fake_similarity)


_CONTROL = AuthorizationProbe(
    probe_id="test_unrestricted_control",
    query="Identity Platform Rewrite",
    principal_id="principal_test",
    target_entity_id="proj_target",
    target_is_restricted=False,
    rationale="test",
)


class TestUnrestrictedControlUnderTheHopsWiderNet:
    async def test_target_ranked_cleanly_alongside_an_incidental_restricted_withholding_passes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exact live shape: the control target ranks, and the hop's
        wider net also (incidentally) surfaces something restricted, which
        gets correctly withheld. This must PASS -- the boundary held.
        """

        target = _entity_node("u_target", "proj_target", "Target Project")
        incidental_restricted = _entity_node(
            "u_other", "proj_other_restricted", "Some Other Restricted Project"
        )
        _install(monkeypatch, bm25=[target, incidental_restricted], cosine=[target])

        outcome = await run_probe(
            _CONTROL,
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_target"}),
        )

        assert outcome.target_ranked is True
        assert outcome.authorization_filtered_count == 1
        assert outcome.verdict == "pass", outcome.detail

    async def test_target_not_ranked_at_all_still_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression guard: the one real failure mode this control exists
        for -- the target itself unreachable -- must still fail.
        """

        _install(monkeypatch, bm25=[], cosine=[])

        outcome = await run_probe(
            _CONTROL,
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_target"}),
        )

        assert outcome.target_ranked is False
        assert outcome.verdict == "fail"

    async def test_clean_run_with_nothing_withheld_still_passes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Baseline, unchanged: the original ideal case must still pass."""

        target = _entity_node("u_target", "proj_target", "Target Project")
        _install(monkeypatch, bm25=[target], cosine=[target])

        outcome = await run_probe(
            _CONTROL,
            store=_FakeStore(object(), _PARTITION, _SemanticEmbedder()),
            authorized_entity_ids=frozenset({"proj_target"}),
        )

        assert outcome.target_ranked is True
        assert outcome.authorization_filtered_count == 0
        assert outcome.verdict == "pass"
