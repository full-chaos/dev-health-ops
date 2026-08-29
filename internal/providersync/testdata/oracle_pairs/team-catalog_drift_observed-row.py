"""Live Python oracle for the team-catalog drift-review engine's
per-team observation row (CHAOS-4444) -- clickhouse_team_drift_projector.py's
``_observed_row``, the function every native collector's staged/write-through
decision (and every team_provider_observations row) is built on top of.
"""

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
_PRODUCER_SOURCE = (
    REPO_ROOT
    / "src/dev_health_ops/api/services/configuration/clickhouse_team_drift_projector.py"
)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    team_row: dict[str, Any] = {
        "id": case["team_row_id"],
        "provider": case.get("provider"),
        "native_team_key": case.get("native_team_key"),
        "name": case.get("name"),
        "description": case.get("description"),
        "members": case.get("members"),
        "project_keys": case.get("project_keys"),
        "repo_patterns": case.get("repo_patterns"),
        "parent_team_id": case.get("parent_team_id"),
    }
    if "is_active" in case:
        team_row["is_active"] = case["is_active"]
    now = datetime.fromisoformat(case["now"].replace("Z", "+00:00"))
    return module._observed_row(
        case["org_id"], team_row, discovered_at=now, updated_at=now
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="team-catalog/drift/observed-row",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _PRODUCER_SOURCE.read_text(), "_observed_row", (RETURN_LITERAL,)
        ),
    )
)
