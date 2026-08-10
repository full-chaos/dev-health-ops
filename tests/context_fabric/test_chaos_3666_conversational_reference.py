"""CHAOS-3666: conversation-context resolution for pronouns and prior-turn
references.

``resolve_conversational_reference`` is a deliberately narrow capability:
given a query that is confidently NOTHING but a bare pronoun/deictic
reference, and an explicit, caller-supplied list of prior-turn subject
ids, resolve to that subject if -- and only if -- exactly one is
authorized and currently real. It reads no vector and no conversation
text; "which subject the prior turn resolved to" is an input this arm is
handed, not something it infers.

Every test here plants the specific shape its guard exists to catch: a
query that WOULD be wrongly resolved, or wrongly refused, unless the
function under test is what decides it correctly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal
from dev_health_ops.context_fabric.graph_arm.backend import MatchMechanism
from dev_health_ops.context_fabric.graph_arm.readback import EntityLookupRow
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    resolve_conversational_reference,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

pytestmark = pytest.mark.asyncio

_PARTITION = "cf_trial_org_test"


@dataclass(frozen=True)
class _FakeStore:
    driver: Any
    partition: str
    embedder: Any = None


def _install_lookup(monkeypatch: pytest.MonkeyPatch, rows: tuple[EntityLookupRow, ...]):
    calls: dict[str, Any] = {}

    async def fake_lookup(driver, partition, canonical_ids):
        calls["partition"] = partition
        calls["canonical_ids"] = tuple(canonical_ids)
        return tuple(row for row in rows if row.canonical_id in canonical_ids)

    import dev_health_ops.context_fabric.graph_arm.semantic_retrieval as module

    monkeypatch.setattr(module, "_entities_by_canonical_id", fake_lookup)
    return calls


_ROW = EntityLookupRow(
    canonical_id="proj_vertex",
    kind=GraphEntityKind.PROJECT,
    display_label="Vertex Platform",
    source_class=SourceClass.WORK_GRAPH,
)


class TestPureReferencesResolve:
    @pytest.mark.parametrize(
        "query",
        [
            "what about it",
            "how is that doing",
            "what's the status of the other project",
            "what happened with that one",
            "is it still going",
        ],
    )
    async def test_a_bare_reference_resolves_to_the_single_prior_subject(
        self, monkeypatch: pytest.MonkeyPatch, query: str
    ) -> None:
        _install_lookup(monkeypatch, (_ROW,))
        result = await resolve_conversational_reference(
            query,
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=("proj_vertex",),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )
        assert result.candidate is not None
        assert result.candidate.canonical_id == "proj_vertex"
        assert result.candidate.signal is SubjectMatchSignal.CONVERSATIONAL_REFERENCE
        assert result.candidate.mechanism is MatchMechanism.EXACT_LOOKUP


class TestContentBearingQueriesAreRefused:
    """The control: a query with real content must never resolve here,
    even with a perfectly good prior subject sitting available."""

    @pytest.mark.parametrize(
        "query",
        [
            "what's holding it up",  # CHAOS-3654's own measured case
            "why is the auth work still failing",
            "how is Halcyon doing",
            "what is the payments rewrite status",
        ],
    )
    async def test_a_content_bearing_query_is_refused(
        self, monkeypatch: pytest.MonkeyPatch, query: str
    ) -> None:
        calls = _install_lookup(monkeypatch, (_ROW,))
        result = await resolve_conversational_reference(
            query,
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=("proj_vertex",),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )
        assert result.candidate is None
        assert "canonical_ids" not in calls, (
            "a content-bearing query must not even reach the entity lookup"
        )


class TestNoPriorSubjectRefusesSafely:
    async def test_an_empty_prior_subject_list_refuses(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_lookup(monkeypatch, ())
        result = await resolve_conversational_reference(
            "what about it",
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=(),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )
        assert result.candidate is None
        assert "no prior-turn subject" in result.reason


class TestAuthorizationIsRecheckedNotTrusted:
    async def test_an_unauthorized_prior_subject_does_not_resolve(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A grant that changed between turns must not be papered over."""

        calls = _install_lookup(monkeypatch, (_ROW,))
        result = await resolve_conversational_reference(
            "what about it",
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=("proj_vertex",),
            authorized_entity_ids=frozenset(),
        )
        assert result.candidate is None
        assert "canonical_ids" not in calls, (
            "an unauthorized prior subject must never reach the lookup"
        )


class TestMultiplePriorSubjectsRefuseRatherThanGuess:
    async def test_two_authorized_prior_subjects_refuse(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_lookup(
            monkeypatch,
            (
                _ROW,
                EntityLookupRow(
                    canonical_id="proj_beacon",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Beacon",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )
        result = await resolve_conversational_reference(
            "what about it",
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=("proj_vertex", "proj_beacon"),
            authorized_entity_ids=frozenset({"proj_vertex", "proj_beacon"}),
        )
        assert result.candidate is None
        assert "equally plausible" in result.reason

    async def test_a_duplicated_prior_subject_is_not_treated_as_two(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The same subject named twice (e.g. two turns in a row) must
        still resolve -- de-duplication, not accidental ambiguity."""

        _install_lookup(monkeypatch, (_ROW,))
        result = await resolve_conversational_reference(
            "what about it",
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=("proj_vertex", "proj_vertex"),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )
        assert result.candidate is not None
        assert result.candidate.canonical_id == "proj_vertex"


class TestExistenceIsRereadNotTrusted:
    async def test_a_deleted_prior_subject_refuses(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_lookup(monkeypatch, ())
        result = await resolve_conversational_reference(
            "what about it",
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=("proj_vertex",),
            authorized_entity_ids=frozenset({"proj_vertex"}),
        )
        assert result.candidate is None
        assert "no longer exists" in result.reason

    async def test_the_organization_is_never_a_referent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_lookup(
            monkeypatch,
            (
                EntityLookupRow(
                    canonical_id="org_helio",
                    kind=GraphEntityKind.ORGANIZATION,
                    display_label="Helio",
                    source_class=SourceClass.WORK_GRAPH,
                ),
            ),
        )
        result = await resolve_conversational_reference(
            "what about it",
            store=_FakeStore(object(), _PARTITION),
            prior_subject_ids=("org_helio",),
            authorized_entity_ids=frozenset({"org_helio"}),
        )
        assert result.candidate is None
        assert "organization" in result.reason
