"""Live Python oracle for ``fetch_gitlab_work_items`` issue rows.

The fake client only supplies transport-shaped issue and label-event objects;
the compared row is produced by the real metrics entrypoint and the real
GitLab normalizer.  Merge-request attribution is disabled with the same blank
tenant argument a CLI-only fetch uses, so no fake attribution record can hide
an issue-row mismatch.
"""

from __future__ import annotations

import contextlib
import dataclasses
import io
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
    import dev_health_ops.metrics.work_items as work_items_module
    from dev_health_ops.models.work_items import WorkItem, WorkItemStatusTransition
    from dev_health_ops.providers.gitlab.normalize import gitlab_issue_to_work_item
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.status_mapping import load_status_mapping

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"


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


class _LabelEvents:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def list(self, **_kwargs: object) -> list[object]:
        return list(self._events)


class _Client:
    def __init__(self, issue: object, label_events: list[object]) -> None:
        self.issue = issue
        self.label_events = label_events
        self.issue_calls: list[dict[str, object]] = []
        self.merge_request_calls = 0
        setattr(self.issue, "resource_label_events", _LabelEvents(label_events))

    def iter_project_issues(self, **kwargs: object):
        self.issue_calls.append(kwargs)
        yield self.issue

    def iter_project_merge_requests(self, **_kwargs: object):
        self.merge_request_calls += 1
        return iter(())


class _Dependencies:
    def __init__(self, client: _Client) -> None:
        self.client = client
        self.gitlab_issue_to_work_item = gitlab_issue_to_work_item

    def gitlab_client_factory(self, *, token: str, gitlab_url: str | None) -> _Client:
        if token != "oracle-token" or gitlab_url != "https://gitlab.example":
            raise AssertionError("fetch_gitlab_work_items did not thread credentials")
        return self.client


def _fetch(
    case: dict[str, Any],
) -> tuple[list[WorkItem], list[WorkItemStatusTransition]]:
    issue = _object(case["raw_issue"])
    label_events = [_object(event) for event in case.get("label_events", [])]
    client = _Client(issue, label_events)
    work_items_module.get_metrics_dependencies = lambda: _Dependencies(client)
    repo = work_items_module.DiscoveredRepo(
        repo_id=UUID(case["repo_id"]),
        full_name=case["repo_full_name"],
        source="gitlab",
        settings={},
    )
    since = datetime.fromisoformat(case["since"].replace("Z", "+00:00"))
    items, transitions, attributions = work_items_module.fetch_gitlab_work_items(
        repos=[repo],
        since=since,
        status_mapping=load_status_mapping(),
        identity=load_identity_resolver(),
        token="oracle-token",
        gitlab_url="https://gitlab.example",
        include_label_events=True,
        org_id="",
    )
    if attributions:
        raise AssertionError("blank-org fetch unexpectedly emitted attribution rows")
    if client.merge_request_calls:
        raise AssertionError("blank-org fetch unexpectedly scanned merge requests")
    if len(items) != 1:
        raise AssertionError(f"expected one fetched work item, got {len(items)}")
    return items, transitions


def _build_issue_row(case: dict[str, Any]) -> dict[str, Any]:
    items, _ = _fetch(case)
    # fetch_gitlab_work_items emits semantic rows with the dataclass's blank
    # tenant default.  The production job stamps the owning unit before the
    # ClickHouse write; compare that real boundary to the Go claim row.
    return dataclasses.asdict(dataclasses.replace(items[0], org_id=case["org_id"]))


def _build_transition_row(case: dict[str, Any]) -> dict[str, Any]:
    _, transitions = _fetch(case)
    index = int(case.get("transition_index", 0))
    if index < 0 or index >= len(transitions):
        raise AssertionError(
            f"transition index {index} outside {len(transitions)} rows"
        )
    return dataclasses.asdict(
        dataclasses.replace(transitions[index], org_id=case["org_id"])
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/issue",
        build_row=_build_issue_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItem"
        ),
    )
)
