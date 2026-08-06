"""Live Python Projects v2 composition oracle."""

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
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.metrics.job_work_items import _merge_github_project_v2_rows
    from dev_health_ops.models.work_items import WorkItem

_THIS_FILE = pathlib.Path(__file__)


def _work_item(payload: dict[str, Any]) -> WorkItem:
    return WorkItem(
        work_item_id=str(payload["work_item_id"]),
        provider="github",
        title=str(payload["title"]),
        type="issue",
        status="todo",
        status_raw="todo",
    )


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    repository = [_work_item(item) for item in case["repository_items"]]
    projects = [_work_item(item) for item in case["project_items"]]
    items, transitions = _merge_github_project_v2_rows(
        repository,
        list(case["repository_transitions"]),
        projects,
        list(case["project_transitions"]),
    )
    return {
        "item_ids": [item.work_item_id for item in items],
        "titles": [item.title for item in items],
        "transitions": transitions,
    }


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items-project-v2/composition",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _THIS_FILE.read_text(), "_build_row", (RETURN_LITERAL,)
        ),
    )
)
