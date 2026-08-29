"""Live Python oracle for GitLab team_project_ownership row construction
(CHAOS-4432). Exercises the depth-derived specificity ladder
(_project_ownership_rows / _parent_by_team / _depth), the piece most likely
to silently diverge from a from-scratch Go port.
"""

from __future__ import annotations

import dataclasses
import pathlib
import types
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_gitlab.py"
_SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/schemas.py"


def _discovered_team(entry: dict[str, Any]) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        provider_team_id=entry["full_path"],
        name=entry.get("name", entry["full_path"]),
        description=entry.get("description"),
        associations={"repo_patterns": entry.get("repo_patterns", [])},
    )


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    teams = [_discovered_team(entry) for entry in case["teams"]]
    now = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    rows = module._project_ownership_rows(org_id=case["org_id"], teams=teams, now=now)
    matches = [
        row
        for row in rows
        if row.team_id == module._team_id(case["target_full_path"])
        and row.project_id == case["target_project_path"]
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one ownership row for team={case['target_full_path']!r} "
            f"project={case['target_project_path']!r}, got {len(matches)} of {len(rows)}"
        )
    return dataclasses.asdict(matches[0])


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/reference-ownership",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _SCHEMA_SOURCE.read_text(), "TeamProjectOwnershipRecord"
        ),
    )
)
