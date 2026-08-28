"""Live Python oracle for GitHub team-repo-ownership row construction
(CHAOS-4434 scope correction). This is the ONLY producer of GitHub's
team_repo_ownership rows -- team_repo_ownership_derivation (CHAOS-4365)
derives solely from team_project_ownership, which GitHub never writes.
"""

from __future__ import annotations

import dataclasses
import pathlib
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PRODUCER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/team_autoimport_github.py"
_SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/schemas.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    now = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    row = module._github_repo_ownership_row(
        org_id=case["org_id"],
        team_id=module._team_id(case["team_slug"]),
        repo_full_name=case["repo_full_name"],
        specificity=module.BASE_SPECIFICITY,
        now=now,
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/team-catalog/repo-ownership",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _SCHEMA_SOURCE.read_text(), "TeamRepoOwnershipRecord"
        ),
    )
)
