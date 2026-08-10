"""Live Python oracle for Linear work-item status transitions."""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"
_NORMALIZE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/linear/normalize.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_NORMALIZE_SOURCE)
    from dev_health_ops.models.work_items import WorkItemStatusTransition
    from dev_health_ops.providers.identity import IdentityResolver
    from dev_health_ops.providers.status_mapping import StatusMapping

    _, transitions = module.linear_issue_to_work_item(
        issue=case["raw_issue"],
        status_mapping=StatusMapping({}, {}, {}, {}),
        identity=IdentityResolver({}),
        history=case.get("history", []),
    )
    row = transitions[int(case.get("row_index", 0))]
    if not isinstance(row, WorkItemStatusTransition):
        raise TypeError(f"Linear producer returned {type(row)!r}, not transition")
    return dataclasses.asdict(dataclasses.replace(row, org_id=case["org_id"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/status-transition",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemStatusTransition"
        ),
    )
)
