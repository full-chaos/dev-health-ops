"""Live Python oracle for GitHub work-item status transition rows."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
from typing import Any
from uuid import UUID

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
    object_from_case,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.normalize import (
        github_issue_to_work_item,
        github_pr_to_work_item,
    )
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.status_mapping import load_status_mapping

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    common = {
        "repo_full_name": case["repo_full_name"],
        "repo_id": UUID(case["repo_id"]),
        "status_mapping": load_status_mapping(),
        "identity": load_identity_resolver(),
        "events": object_from_case(case["raw_events"]),
    }
    if "raw_pr" in case:
        _, transitions = github_pr_to_work_item(
            pr=object_from_case(case["raw_pr"]), **common
        )
    else:
        _, transitions = github_issue_to_work_item(
            issue=object_from_case(case["raw_issue"]), **common
        )
    row = dataclasses.replace(
        transitions[int(case["transition_index"])], org_id=case["org_id"]
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/status-transition",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemStatusTransition"
        ),
    )
)
