"""CHAOS-3678: EXACT-only live subject resolution for production.

Per team-lead's ruling (§4 binds hard): production's first COMPLETED-path
slice resolves a mention to a subject on ``EXACT_CANONICAL_ID`` or
``EXACT_DISPLAY_NAME`` only -- never alias, acronym, previous-name or fuzzy
matching, which ``discovery.search_candidates`` supports for the trial but
which this module deliberately does not reach. A mention that only fuzzy- or
alias-matches must resolve to *nothing* here, not a guess.

Tested against a fake driver exposing ``execute_query`` (the same contract
``readback._rows`` calls), not a live FalkorDB -- this module reuses
``readback._ENTITY_QUERY`` verbatim so the query itself is already covered
by the live-store tests that exercise ``LiveGraphReader``; what is new here
is the exact-only matching and authorization filtering on top of it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    RelationshipType,
    SubjectMatchSignal,
)
from dev_health_ops.context_fabric.graph_arm.backend import MatchMechanism
from dev_health_ops.context_fabric.graph_arm.subject_resolution import (
    _live_cohort_edges,
    _live_entities,
    _live_entity_labels,
    _LiveEntity,
    _resolve_exact_subjects,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

pytestmark = pytest.mark.asyncio

_PARTITION = "cf_trial_org_test"


def _entity_record(
    canonical_id: str,
    display_label: str,
    *,
    kind: str = GraphEntityKind.PROJECT.value,
    source_class: str = SourceClass.WORK_GRAPH.value,
) -> dict[str, Any]:
    return {
        "canonical_id": canonical_id,
        "entity_kind": kind,
        "display_label": display_label,
        "source_class": source_class,
    }


@dataclass
class _FakeDriver:
    """Mirrors ``readback._rows``'s contract: ``execute_query`` returns a
    ``(records, header, stats)`` triple; only ``records`` is read.
    """

    rows: list[dict[str, Any]] = field(default_factory=list)
    call_count: int = 0

    async def execute_query(self, query: str, **params: object) -> tuple:
        self.call_count += 1
        return (self.rows, None, None)


async def test_exact_canonical_id_match_resolves() -> None:
    driver = _FakeDriver(
        rows=[_entity_record("proj_nightfall_migration", "Nightfall Migration")]
    )
    matches = await _resolve_exact_subjects(
        driver,
        _PARTITION,
        queries=["proj_nightfall_migration"],
        authorized_entity_ids=("proj_nightfall_migration",),
    )
    assert len(matches) == 1
    assert matches[0].canonical_id == "proj_nightfall_migration"
    assert matches[0].signal is SubjectMatchSignal.EXACT_CANONICAL_ID
    assert matches[0].mechanism is MatchMechanism.EXACT_LOOKUP


async def test_exact_display_name_match_resolves() -> None:
    driver = _FakeDriver(
        rows=[_entity_record("proj_nightfall_migration", "Nightfall Migration")]
    )
    matches = await _resolve_exact_subjects(
        driver,
        _PARTITION,
        queries=["Nightfall Migration"],
        authorized_entity_ids=("proj_nightfall_migration",),
    )
    assert len(matches) == 1
    assert matches[0].canonical_id == "proj_nightfall_migration"
    assert matches[0].signal is SubjectMatchSignal.EXACT_DISPLAY_NAME


async def test_case_and_punctuation_insensitive_but_still_exact() -> None:
    """Normalization tolerance, not fuzziness: casing/punctuation/whitespace
    variance is not the widening §4 forbids -- a genuinely different string
    (below) must still not match.
    """

    driver = _FakeDriver(
        rows=[_entity_record("proj_nightfall_migration", "Nightfall Migration")]
    )
    matches = await _resolve_exact_subjects(
        driver,
        _PARTITION,
        queries=["  NIGHTFALL-migration  "],
        authorized_entity_ids=("proj_nightfall_migration",),
    )
    assert len(matches) == 1


async def test_no_match_returns_empty_not_a_guess() -> None:
    """The negative control for the module's whole reason to exist: a
    fuzzy/partial textual overlap must resolve to nothing here, even though
    ``discovery.search_candidates`` (the trial's fuller capability) would
    treat "Nightfall" as a ``FUZZY_LABEL`` hit against "Nightfall Migration".
    """

    driver = _FakeDriver(
        rows=[_entity_record("proj_nightfall_migration", "Nightfall Migration")]
    )
    matches = await _resolve_exact_subjects(
        driver,
        _PARTITION,
        queries=["Nightfall"],
        authorized_entity_ids=("proj_nightfall_migration",),
    )
    assert matches == ()


async def test_unauthorized_match_is_withheld() -> None:
    """Authorization applied before the match is ever returned -- mirrors
    ``discovery.search_candidates``'s own "withheld before ranking" rule.
    """

    driver = _FakeDriver(
        rows=[_entity_record("proj_nightfall_migration", "Nightfall Migration")]
    )
    matches = await _resolve_exact_subjects(
        driver,
        _PARTITION,
        queries=["proj_nightfall_migration"],
        authorized_entity_ids=(),
    )
    assert matches == ()


async def test_organization_kind_is_never_a_subject_match() -> None:
    driver = _FakeDriver(
        rows=[
            _entity_record(
                "org_acme", "Acme Corp", kind=GraphEntityKind.ORGANIZATION.value
            )
        ]
    )
    matches = await _resolve_exact_subjects(
        driver,
        _PARTITION,
        queries=["Acme Corp"],
        authorized_entity_ids=("org_acme",),
    )
    assert matches == ()


async def test_empty_queries_short_circuits_without_a_round_trip() -> None:
    driver = _FakeDriver(rows=[_entity_record("proj_x", "Project X")])
    matches = await _resolve_exact_subjects(
        driver, _PARTITION, queries=(), authorized_entity_ids=("proj_x",)
    )
    assert matches == ()
    assert driver.call_count == 0, (
        "an empty query list has nothing to look up; a query sent anyway "
        "is not a query worth sending"
    )


# ---- CHAOS-3688: cohort-seeding live helpers -------------------------------


async def test_live_entities_skips_rows_with_no_legible_kind() -> None:
    """Mirrors readback's own rule for a node this arm cannot classify --
    dropped rather than guessed at.
    """

    driver = _FakeDriver(
        rows=[
            _entity_record("proj_a", "Project A"),
            {
                "canonical_id": "proj_b",
                "entity_kind": None,
                "display_label": "Project B",
                "source_class": SourceClass.WORK_GRAPH.value,
            },
        ]
    )
    entities = await _live_entities(driver, _PARTITION)
    assert [entity.canonical_id for entity in entities] == ["proj_a"]


async def test_live_entity_labels_excludes_organization() -> None:
    """A pure function (no ``await`` needed) -- ``async def`` only to match
    the module-level ``pytestmark = pytest.mark.asyncio``, which
    pytest-asyncio warns about applying to a sync test.

    The partition root must never become a cohort label -- a cohort that
    could contain it would be a cohort containing the tenant.
    """

    entities = (
        _LiveEntity(
            canonical_id="proj_nightfall_migration",
            kind=GraphEntityKind.PROJECT,
            display_label="Nightfall Migration",
            source_class=SourceClass.WORK_GRAPH,
        ),
        _LiveEntity(
            canonical_id="org_acme",
            kind=GraphEntityKind.ORGANIZATION,
            display_label="Acme Corp",
            source_class=SourceClass.WORK_GRAPH,
        ),
    )
    labels = _live_entity_labels(entities)
    assert labels == {
        "proj_nightfall_migration": (GraphEntityKind.PROJECT, "Nightfall Migration")
    }


def _edge_record(source: str, relationship: RelationshipType, target: str) -> dict:
    return {
        "fact": f"{source} {relationship.value} {target}",
        "source_class": SourceClass.WORK_GRAPH.value,
        "observed_at": "2026-05-01T00:00:00+00:00",
        "observation_ids": "",
        "valid_from": None,
        "valid_to": None,
    }


async def test_live_cohort_edges_parses_the_stored_triple() -> None:
    driver = _FakeDriver(
        rows=[
            _edge_record(
                "proj_nightfall_migration",
                RelationshipType.OWNED_BY_TEAM,
                "team_platform",
            )
        ]
    )
    edges = await _live_cohort_edges(driver, _PARTITION)
    assert len(edges) == 1
    assert edges[0].source_canonical_id == "proj_nightfall_migration"
    assert edges[0].relationship is RelationshipType.OWNED_BY_TEAM
    assert edges[0].target_canonical_id == "team_platform"


async def test_live_cohort_edges_rejects_a_prose_fact() -> None:
    """The same detection ``parse_triple_fact`` already gives every other
    live reader: a stored fact containing prose is caught on read, not
    quietly presented as evidence.
    """

    driver = _FakeDriver(
        rows=[
            {
                "fact": "this is not a canonical triple",
                "source_class": SourceClass.WORK_GRAPH.value,
                "observed_at": "2026-05-01T00:00:00+00:00",
                "observation_ids": "",
                "valid_from": None,
                "valid_to": None,
            }
        ]
    )
    with pytest.raises(ValueError, match="not a canonical triple rendering"):
        await _live_cohort_edges(driver, _PARTITION)
