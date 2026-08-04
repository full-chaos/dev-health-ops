"""Live Python oracle for GitHub body and trusted-comment dependency rows."""

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
    from dev_health_ops.providers.github.normalize import (
        extract_github_comment_dependencies,
        extract_github_dependencies,
    )

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    if case["producer"] == "body":
        raw_item = case["raw_pr"] if "raw_pr" in case else case["raw_issue"]
        rows = extract_github_dependencies(
            work_item_id=case["work_item_id"],
            issue_or_pr=object_from_case(raw_item),
            repo_full_name=case["repo_full_name"],
        )
    elif case["producer"] == "comment":
        rows = extract_github_comment_dependencies(
            work_item_id=case["work_item_id"],
            comments=[
                (
                    comment.get("body"),
                    (comment.get("user") or {}).get("login"),
                )
                for comment in case["raw_comments"]
            ],
        )
    else:
        raise ValueError(f"unknown dependency producer {case['producer']!r}")
    row = dataclasses.replace(
        rows[int(case.get("row_index", 0))], org_id=case["org_id"]
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/dependency",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemDependency"
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
