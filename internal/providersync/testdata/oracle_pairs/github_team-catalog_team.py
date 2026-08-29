"""Live Python oracle for GitHub team catalog row construction (CHAOS-4434)."""

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
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_github.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    return module._github_team_row(
        org_id=case["org_id"],
        team_id=module._team_id(case["team_slug"]),
        name=case["name"],
        description=case["description"],
        repo_patterns=case["repo_patterns"],
        native_team_key=case["team_slug"],
        parent_team_id=None,
        now=datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00")),
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/team-catalog/team",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _PRODUCER_SOURCE.read_text(), "_github_team_row", (RETURN_LITERAL,)
        ),
    )
)
