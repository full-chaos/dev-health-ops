"""Live Python oracle for a GitLab issue row of the canonical provider batch.

The producer is GitLabProvider._ingest_with_client itself: the issue loop
normalizes the issue (gitlab_issue_to_work_item), enriches it with the label
priority, and links it to its epic (build_epic_id_for_issue, only for a project
inside a group). The fake client serves one issue and its label events; every
other fetch is switched off by the provider's own environment flags, and the
group lookup returns nothing, so no epic rows are added.
"""

from __future__ import annotations

import contextlib
import dataclasses
import io
import os
import pathlib
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID

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
    from dev_health_ops.providers.base import IngestionContext, IngestionWindow
    from dev_health_ops.providers.gitlab.provider import GitLabProvider
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.status_mapping import load_status_mapping

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"

_PROVIDER_FLAGS = {
    "GITLAB_INCLUDE_MRS": "false",
    "GITLAB_FETCH_NOTES": "false",
    "GITLAB_FETCH_LINKS": "false",
    "GITLAB_FETCH_MILESTONES": "false",
    "GITLAB_FETCH_EPICS": "true",
}


def _object(value: Any) -> Any:
    if isinstance(value, dict):
        return SimpleNamespace(
            **{
                key: (
                    datetime.fromisoformat(item.replace("Z", "+00:00"))
                    if key.endswith("_at") and isinstance(item, str)
                    else _object(item)
                )
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return [_object(item) for item in value]
    return value


class _Client:
    def __init__(self, issue: object, label_events: list[object]) -> None:
        self.issue = issue
        self.label_events = label_events

    def get_group(self, _group_full_path: str) -> None:
        return None

    def iter_project_issues(self, **_kwargs: object):
        yield self.issue

    def get_issue_resource_label_events(self, _issue: object) -> list[object]:
        return list(self.label_events)

    def get_issue_resource_state_events(self, _issue: object) -> list[object]:
        return []


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    issue = _object(case["raw_issue"])
    label_events = [_object(event) for event in case.get("label_events", [])]
    saved = {key: os.environ.get(key) for key in _PROVIDER_FLAGS}
    os.environ.update(_PROVIDER_FLAGS)
    try:
        provider = GitLabProvider(
            status_mapping=load_status_mapping(), identity=load_identity_resolver()
        )
        batch = provider._ingest_with_client(
            client=_Client(issue, label_events),
            ctx=IngestionContext(
                window=IngestionWindow(),
                repo=case["repo_full_name"],
            ),
        )
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    if len(batch.work_items) != 1:
        raise AssertionError(f"expected one work item, got {len(batch.work_items)}")
    # The provider batch carries no repository or tenant; the ingest boundary
    # stamps the unit's repository and org before the write, as the Go route
    # does from its claim.
    row = dataclasses.replace(
        batch.work_items[0], repo_id=UUID(case["repo_id"]), org_id=case["org_id"]
    )
    return dataclasses.asdict(row)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/issue-provider",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItem"
        ),
    )
)
