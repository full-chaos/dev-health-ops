"""Live Python oracle for the daily time-in-state rollup.

Compared surface: every field ``WorkItemStateDurationDailyRecord`` declares,
including ``avg_wip``, which Python computes as the day total divided by a
fixed 24 regardless of how much of the day the window actually covers.
"""

from __future__ import annotations

from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_item_derived_helpers import (
    ORG_ID_EXCLUSION,
    SCHEMA_SOURCE,
    DerivedCase,
    columns,
)

_RECORD = "WorkItemStateDurationDailyRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    return columns(list(DerivedCase(case).state_durations()), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/state-durations",
        build_row=_build_row,
        reflected_fields=_fields,
        excluded_fields={"org_id": ORG_ID_EXCLUSION},
    )
)
