"""Live Python oracle for GitHub reopen event rows."""

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
    object_from_case,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.normalize import detect_github_reopen_events
    from dev_health_ops.providers.identity import load_identity_resolver

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    rows = detect_github_reopen_events(
        work_item_id=case["work_item_id"],
        events=object_from_case(case["raw_events"]),
        identity=load_identity_resolver(),
    )
    row = dataclasses.replace(
        rows[int(case.get("row_index", 0))], org_id=case["org_id"]
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/reopen",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemReopenEvent"
        ),
        excluded_fields={
            "last_synced": (
                "the production constructor stamps wall-clock now and has no clock "
                "parameter; Go stamps the complete unit normalizedAt and its retry "
                "stability is asserted separately"
            ),
        },
    )
)
