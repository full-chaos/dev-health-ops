"""Live Python producer oracle for Jira work-item group metrics.

The Jira route feeds the same provider-neutral daily computation invoked by
``job_work_items.py``.  This pair deliberately executes that checked-in
production function over rows emitted by the Go Jira route; it does not infer
required effects from the route manifest or from a hand-maintained count.
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

_RECORD = "WorkItemMetricsDailyRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    group_records, _user_records, _cycle_records = compute_triplet(case)
    return columns(list(group_records), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="jira/work-items/metrics-daily",
        build_row=_build_row,
        reflected_fields=_fields,
        excluded_fields={"org_id": ORG_ID_EXCLUSION},
    )
)
