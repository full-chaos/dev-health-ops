"""Live Python oracle for a GitLab group-epic work item row (CHAOS-9857).

The producer is GitLabProvider._ingest_with_client: see _gitlab_epic_common.
"""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._gitlab_epic_common import ingest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    _batch, epic = ingest(case)
    # The ingest boundary stamps the tenant before the write, as the Go route
    # does from its claim; an epic has no repository.
    return dataclasses.asdict(dataclasses.replace(epic, org_id=case["org_id"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/epic",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItem"
        ),
    )
)
