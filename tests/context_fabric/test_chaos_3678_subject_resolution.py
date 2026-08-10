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
from dev_health_ops.api.dev.investigation_contract import SubjectMatchSignal
from dev_health_ops.context_fabric.graph_arm.backend import MatchMechanism
from dev_health_ops.context_fabric.graph_arm.subject_resolution import (
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
