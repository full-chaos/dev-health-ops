"""Live Python oracle for Linear issue-type metrics."""

from __future__ import annotations

from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_item_derived_helpers import (
    SCHEMA_SOURCE,
    DerivedCase,
    columns,
)

_RECORD = "IssueTypeMetricsRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    issue_types, _classifications, _metrics = DerivedCase(
        case
    ).engine_destinations_all_days()
    return columns(list(issue_types), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/issue-type-metrics",
        build_row=_build_row,
        reflected_fields=_fields,
    )
)
