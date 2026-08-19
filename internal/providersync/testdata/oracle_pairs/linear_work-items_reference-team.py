"""Live Python oracle for Linear team catalog row construction."""

from __future__ import annotations

import pathlib
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_linear.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    return module._linear_team_row(
        org_id=case["org_id"],
        team_id=case["team_id"],
        name=case["name"],
        description=case["description"],
        project_keys=case["project_keys"],
        now=datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00")),
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/reference-team",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _PRODUCER_SOURCE.read_text(), "_linear_team_row", (RETURN_LITERAL,)
        ),
    )
)
