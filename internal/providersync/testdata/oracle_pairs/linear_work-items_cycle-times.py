"""Live Python oracle for Linear work-item cycle times."""

from __future__ import annotations

from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_item_metrics_helpers import (
    ORG_ID_EXCLUSION,
    SCHEMA_SOURCE,
    columns,
    compute_triplet,
)

_RECORD = "WorkItemCycleTimeRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    _records, _users, cycles = compute_triplet(case)
    return columns(list(cycles), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/cycle-times",
        build_row=_build_row,
        reflected_fields=_fields,
        excluded_fields={"org_id": ORG_ID_EXCLUSION},
    )
)
