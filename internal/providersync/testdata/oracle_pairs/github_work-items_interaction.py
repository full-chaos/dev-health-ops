"""Live Python oracle for GitHub comment interaction rows."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    object_from_case,
)

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.normalize import (
        github_comment_to_interaction_event,
    )
    from dev_health_ops.providers.identity import load_identity_resolver

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    row = github_comment_to_interaction_event(
        comment=object_from_case(case["raw_comment"]),
        work_item_id=case["work_item_id"],
        identity=load_identity_resolver(),
    )
    if row is None:
        raise ValueError("production comment normalizer rejected oracle case")
    row = dataclasses.replace(row, org_id=case["org_id"])
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/interaction",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemInteractionEvent"
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
