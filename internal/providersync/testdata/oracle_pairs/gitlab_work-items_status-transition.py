"""Live Python oracle for GitLab label-driven work-item transitions."""

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
    from dev_health_ops.models.work_items import WorkItemStatusTransition
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
        self.merge_request_calls = 0
        setattr(self.issue, "resource_label_events", _LabelEvents(label_events))

    def iter_project_issues(self, **_kwargs: object):
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


def _fetch(case: dict[str, Any]) -> list[WorkItemStatusTransition]:
    issue = _object(case["raw_issue"])
    label_events = [_object(event) for event in case.get("label_events", [])]
    client = _Client(issue, label_events)
    setattr(
        work_items_module,
        "get_metrics_dependencies",
        lambda: _Dependencies(client),
    )
    repo = work_items_module.DiscoveredRepo(
        repo_id=UUID(case["repo_id"]),
        full_name=case["repo_full_name"],
        source="gitlab",
        settings={},
    )
    items, transitions, attributions = work_items_module.fetch_gitlab_work_items(
        repos=[repo],
        since=datetime.fromisoformat(case["since"].replace("Z", "+00:00")),
        status_mapping=load_status_mapping(),
        identity=load_identity_resolver(),
        token="oracle-token",
        gitlab_url="https://gitlab.example",
        include_label_events=True,
        org_id="",
    )
    if len(items) != 1 or attributions or client.merge_request_calls:
        raise AssertionError(
            "unexpected GitLab fetch side effects in transition oracle"
        )
    return transitions


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    transitions = _fetch(case)
    index = int(case["transition_index"])
    if index < 0 or index >= len(transitions):
        raise AssertionError(
            f"transition index {index} outside {len(transitions)} rows"
        )
    # The worker stamps the fetched semantic transition with the owning unit's
    # tenant immediately before its ClickHouse write.
    return dataclasses.asdict(
        dataclasses.replace(transitions[index], org_id=case["org_id"])
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/work-items/status-transition",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItemStatusTransition"
        ),
    )
)
