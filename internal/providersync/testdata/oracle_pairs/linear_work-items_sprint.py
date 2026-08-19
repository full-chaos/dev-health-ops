"""Live Python oracle for Linear cycle -> Sprint rows."""

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
    from dataclasses import replace

    row = module.linear_cycle_to_sprint(case["raw_cycle"])
    row = replace(
        row, native_team_key=case.get("native_team_key"), org_id=case["org_id"]
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/sprint",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "Sprint"
        ),
        excluded_fields={
            "last_synced": "Python's sprint constructor stamps wall-clock time; Go uses the unit normalizedAt for retry-stable effects.",
        },
    )
)
