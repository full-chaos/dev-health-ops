"""Live Projects v2 PullRequest inclusion-decision oracle."""

from __future__ import annotations

import contextlib
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.normalize import (
        github_project_v2_item_to_work_item,
    )
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.status_mapping import load_status_mapping

_THIS_FILE = pathlib.Path(__file__)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    row, transitions = github_project_v2_item_to_work_item(
        item_node=case["item_node"],
        project_scope_id=case["project_scope_id"],
        status_mapping=load_status_mapping(),
        identity=load_identity_resolver(),
    )
    return {"emitted": row is not None, "transition_count": len(transitions)}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items-project-v2/pr-skip",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _THIS_FILE.read_text(), "_build_row", (RETURN_LITERAL,)
        ),
    )
)
