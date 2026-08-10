"""Live Python oracle for Linear work-item team attribution."""

from __future__ import annotations

from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_item_derived_helpers import (
    SCHEMA_SOURCE,
    DerivedCase,
    columns,
)

_RECORD = "WorkItemTeamAttributionRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    return columns(list(DerivedCase(case).team_attributions()), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/team-attributions",
        build_row=_build_row,
        reflected_fields=_fields,
    )
)
