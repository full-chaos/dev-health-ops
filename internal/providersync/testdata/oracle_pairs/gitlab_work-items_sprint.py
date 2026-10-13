"""Live Python oracle for GitLab milestone -> Sprint normalization.

GitLab sends a milestone's start_date and due_date as date-only strings
("2026-08-01"), and python-gitlab hands them on unparsed, so the case keeps
them as strings: the real gitlab_milestone_to_sprint parses them itself.
"""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
from types import SimpleNamespace
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
    from dev_health_ops.providers.gitlab.normalize import gitlab_milestone_to_sprint

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    milestone = SimpleNamespace(**dict(case["raw_milestone"]))
    row = gitlab_milestone_to_sprint(
        milestone=milestone,
        project_full_path=case["repo_full_name"],
    )
    # The tenant stamp (org_id) is applied by the outer producer before the
    # sink write, not by gitlab_milestone_to_sprint itself; stamp it here so
    # the row matches the shape a caller sees (as github_work-items_sprint.py).
    row = dataclasses.replace(row, org_id=case["org_id"])
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/sprint",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "Sprint"
        ),
        excluded_fields={
            "last_synced": (
                "the production constructor stamps wall-clock now and has no "
                "clock parameter; Go stamps the complete unit normalizedAt"
            ),
        },
    )
)
