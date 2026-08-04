"""Live Python oracle for GitHub milestone -> Sprint normalization."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.normalize import github_milestone_to_sprint

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    values = dict(case["raw_milestone"])
    for key in ("created_at", "due_on"):
        if isinstance(values.get(key), str):
            values[key] = datetime.fromisoformat(values[key].replace("Z", "+00:00"))
    milestone = SimpleNamespace(**values)
    row = github_milestone_to_sprint(
        milestone=milestone,
        repo_full_name=case["repo_full_name"],
    )
    # run_work_items_sync_job owns the tenant stamp immediately before the
    # sink write; include that real outer producer boundary in this row oracle.
    row = dataclasses.replace(row, org_id=case["org_id"])
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/sprint",
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
