"""Live Python oracle for GitLab native project catalog normalization
(CHAOS-3380/CHAOS-4432): archived -> inactive, id prefixing, path_with_
namespace as project_key.
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


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_PRODUCER_SOURCE)
    project = types.SimpleNamespace(
        id=case["native_id"],
        path_with_namespace=case["path_with_namespace"],
        name=case.get("name") or "",
        archived=case["archived"],
        web_url=case.get("web_url") or "",
    )
    now = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    rows = module._gitlab_project_catalog_rows(
        org_id=case["org_id"], projects=[project], source_external_ids=None, now=now
    )
    if len(rows) != 1:
        raise ValueError(f"expected one project row, got {len(rows)}")
    return dataclasses.asdict(rows[0])


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/reference-project",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _SCHEMA_SOURCE.read_text(), "ProjectRecord"
        ),
    )
)
