"""Live Python oracle for the daily estimate-coverage rollup.

Compared surface: every field ``EstimateCoverageMetricsDailyRecord`` declares,
reflected from the production dataclass rather than listed here, so a field
added or removed upstream breaks this comparison instead of silently narrowing
it.
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

_RECORD = "EstimateCoverageMetricsDailyRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    return columns(list(DerivedCase(case).estimate_coverage()), _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/estimate-coverage",
        build_row=_build_row,
        reflected_fields=_fields,
        excluded_fields={"org_id": ORG_ID_EXCLUSION},
    )
)
