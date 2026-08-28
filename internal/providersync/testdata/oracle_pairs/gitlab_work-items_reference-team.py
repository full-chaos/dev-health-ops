"""Live Python oracle for GitLab team catalog row construction (CHAOS-4432)."""

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
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_gitlab.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    full_path = case["full_path"]
    # Derive team_id/native_team_key/parent_team_id the SAME way _team_rows
    # does for every real GitLab group -- via the module's own path-derived
    # helpers, never a value the case hands in pre-computed. GitLab
    # associations never carry an explicit parent_team_id/parent_provider_
    # team_id key (only repo_patterns/provider_org), so that branch of
    # _parent_team_id never fires for this provider; an empty dict here is
    # the real input shape, not a simplification.
    team_id = module._team_id(full_path)
    parent_team_id = module._parent_team_id(full_path, {})
    return module._gitlab_team_row(
        org_id=case["org_id"],
        team_id=team_id,
        name=case["name"],
        description=case["description"],
        project_keys=case["project_keys"],
        native_team_key=full_path,
        parent_team_id=parent_team_id,
        now=datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00")),
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/reference-team",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _PRODUCER_SOURCE.read_text(), "_gitlab_team_row", (RETURN_LITERAL,)
        ),
    )
)
