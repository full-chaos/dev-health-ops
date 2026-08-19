"""Live Python oracle for GitHub Projects v2 status transitions."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.normalize import (
        github_project_v2_item_to_work_item,
    )
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.status_mapping import load_status_mapping

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    _, transitions = github_project_v2_item_to_work_item(
        item_node=case["item_node"],
        project_scope_id=case["project_scope_id"],
        status_mapping=load_status_mapping(),
        identity=load_identity_resolver(),
    )
    row = transitions[int(case["transition_index"])]
    return dataclasses.asdict(dataclasses.replace(row, org_id=case["org_id"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items-project-v2/transition",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemStatusTransition"
        ),
    )
)
