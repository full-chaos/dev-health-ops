"""Shared fake client and ingest for the GitLab group-epic oracle pairs (CHAOS-9857).

The producer is GitLabProvider._ingest_with_client itself: for a project inside
a group, with GITLAB_FETCH_EPICS on, it lists the group's epics, reads each
epic's resource state events, and normalizes the epic (gitlab_epic_to_work_item,
priority enrichment) into a work item of type "epic" with its status
transitions. The fake client serves one epic and its state events; the project
has no issues, and every other fetch is switched off by the provider's own
environment flags.
"""

from __future__ import annotations

import contextlib
import io
import os
from datetime import datetime
from types import SimpleNamespace
from typing import Any

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
    def __init__(self, epic: object, state_events: list[object]) -> None:
        self.epic = epic
        self.state_events = state_events

    def get_group(self, group_full_path: str) -> object:
        return SimpleNamespace(full_path=group_full_path)

    def iter_group_epics(self, **_kwargs: object):
        yield self.epic

    def get_epic_resource_state_events(self, _epic: object) -> list[object]:
        return list(self.state_events)

    def iter_project_issues(self, **_kwargs: object):
        return iter(())


def ingest(case: dict[str, Any]):
    epic = _object(case["raw_epic"])
    state_events = [_object(event) for event in case.get("state_events", [])]
    saved = {key: os.environ.get(key) for key in _PROVIDER_FLAGS}
    os.environ.update(_PROVIDER_FLAGS)
    try:
        provider = GitLabProvider(
            status_mapping=load_status_mapping(), identity=load_identity_resolver()
        )
        batch = provider._ingest_with_client(
            client=_Client(epic, state_events),
            ctx=IngestionContext(window=IngestionWindow(), repo=case["repo_full_name"]),
        )
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    epics = [item for item in batch.work_items if item.type == "epic"]
    if len(epics) != 1:
        raise AssertionError(f"expected one epic work item, got {len(epics)}")
    return batch, epics[0]
