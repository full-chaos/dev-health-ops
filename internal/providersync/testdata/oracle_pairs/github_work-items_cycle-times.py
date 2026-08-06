"""Live Python oracle for the per-item work-item cycle-time records.

Compared surface: every field ``WorkItemCycleTimeRecord`` declares -- the COMPUTE
contract, which includes active_time_hours, wait_time_hours and flow_efficiency.
Those three are computed by Python and then dropped by its ClickHouse sink,
which names only sixteen columns. That narrowing is a PERSISTENCE fact, proved
separately against the live sink source by
TestGitHubWorkItemCycleTimePersistenceProjectionMatchesPythonSink; excluding
them here instead would stop comparing the flow breakdown altogether, which is
the part of this record most likely to diverge.
"""

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
    _group_records, _user_records, cycle_records = compute_triplet(case)
    return columns(list(cycle_records), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/cycle-times",
        build_row=_build_row,
        reflected_fields=_fields,
        excluded_fields={"org_id": ORG_ID_EXCLUSION},
    )
)
