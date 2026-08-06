"""Live Python oracle for the per-day, per-user work-item metrics rollup.

Compared surface: every field ``WorkItemUserMetricsDailyRecord`` declares, for
the complete ordered record list ``compute_work_item_metrics_daily`` returns for
one UTC day. This is the projection whose user identity comes from the FIRST
assignee only, with Python's falsy-empty fallback to "unassigned"; the Go port
has to reproduce both halves, so the field set is reflected from the production
dataclass rather than chosen here.
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

_RECORD = "WorkItemUserMetricsDailyRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    _group_records, user_records, _cycle_records = compute_triplet(case)
    return columns(list(user_records), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/user-metrics-daily",
        build_row=_build_row,
        reflected_fields=_fields,
        excluded_fields={"org_id": ORG_ID_EXCLUSION},
    )
)
