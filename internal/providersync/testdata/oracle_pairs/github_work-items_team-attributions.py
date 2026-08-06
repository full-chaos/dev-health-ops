"""Live Python oracle for the per-item team attribution candidate rows.

Compared surface: every field ``WorkItemTeamAttributionRecord`` declares.
Unlike the daily rollups this record carries the UNNORMALISED candidate team
id and name straight from ``resolve_team_attribution`` -- a null team_id stays
null here while the same resolution becomes "unassigned" in the coverage
rollup. That asymmetry is Python's, and the ``unassigned_candidate`` case
exists to pin it.
"""

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
        id="github/work-items/team-attributions",
        build_row=_build_row,
        reflected_fields=_fields,
        # NO org_id exclusion here, unlike the two daily rollups. This record is
        # the one member of the family Python populates itself
        # (compute_work_item_team_attributions copies item.org_id rather than
        # leaving the dataclass default for the sink to fill), so org_id is a
        # genuinely comparable value on both sides and excluding it would hide
        # a real tenant-stamping divergence behind a reason that is simply
        # untrue for this pair. Proved by running the pair: it emits
        # org_id="org-acme", not "".
    )
)
