"""Live Python oracle for Linear project catalog normalization."""

from __future__ import annotations

import dataclasses
import pathlib
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_linear.py"
_SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/schemas.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    now = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    rows = module._linear_project_records(
        [case["node"]], org_id=case["org_id"], now=now
    )
    if len(rows) != 1:
        raise ValueError(f"expected one project row, got {len(rows)}")
    return dataclasses.asdict(rows[0])


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/reference-project",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _SCHEMA_SOURCE.read_text(), "ProjectRecord"
        ),
    )
)
