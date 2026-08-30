"""Real Atlassian-client worklog and board-sprint producer oracle.

The transport is deterministic, but the provider method, canonical models,
identity resolver, worklog/sprint normalizers, reference-sprint guard, and
ProviderBatch boundary are all loaded from this checkout's production source.
"""
# SIZE-OK: one producer oracle keeps transport patches and provenance checks together.

from __future__ import annotations

import contextlib
import dataclasses
import io
import os
import pathlib
import sys
from datetime import datetime, timezone
from importlib.machinery import ModuleSpec
from types import ModuleType
from typing import Any, cast

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
SRC_ROOT = REPO_ROOT / "src"
PROVIDER_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/jira/provider.py"
sys.path.insert(0, str(SRC_ROOT))
install_minimal_oracle_imports()

# Import the real connector utility without executing unrelated connector clients.
_connectors = ModuleType("dev_health_ops.connectors")
setattr(_connectors, "__path__", [str(SRC_ROOT / "dev_health_ops/connectors")])
_connectors.__spec__ = ModuleSpec(_connectors.__name__, loader=None, is_package=True)
sys.modules[_connectors.__name__] = _connectors

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    import atlassian
    from atlassian import canonical_models
    from atlassian.graph.api import jira_worklogs as jira_worklogs_api
    from atlassian.rest.api import jira_boards

    from dev_health_ops.models.work_items import Sprint
    from dev_health_ops.providers.base import (
        IngestionContext,
        IngestionWindow,
        ProviderBatch,
    )
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.jira import atlassian_compat
    from dev_health_ops.providers.jira.provider import JiraProvider
    from dev_health_ops.providers.status_mapping import load_status_mapping

if (
    pathlib.Path(
        str(JiraProvider._ingest_via_atlassian_client.__code__.co_filename)
    ).resolve()
    != PROVIDER_SOURCE.resolve()
):
    raise RuntimeError("Atlassian Jira producer did not come from this checkout")

_STATUS_MAPPING_PATH = REPO_ROOT / "src/dev_health_ops/config/status_mapping.yaml"
_IDENTITY_MAPPING_PATH = REPO_ROOT / "src/dev_health_ops/config/identity_mapping.yaml"
_NORMALIZED_AT = datetime(2026, 8, 10, 12, 0, 0, 123456, tzinfo=timezone.utc)


class _FakeRestClient:
    def __init__(self, case: dict[str, Any]):
        self.case = case
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _issue(case: dict[str, Any]) -> canonical_models.JiraIssue:
    raw = case["issue"]
    fields = raw["fields"]
    sprint_ids = [str(item["id"]) for item in fields.get("customfield_10020", [])]
    return canonical_models.JiraIssue(
        cloud_id="cloud-301",
        key=str(raw["key"]),
        project_key=str(fields["project"]["key"]),
        issue_type=str(fields["issuetype"]["name"]),
        status=str(fields["status"]["name"]),
        created_at=str(fields["created"]),
        updated_at=str(fields["updated"]),
        resolved_at=None,
        assignee=None,
        reporter=None,
        labels=[str(value) for value in fields.get("labels", [])],
        components=[],
        story_points=None,
        sprint_ids=sprint_ids,
    )


def _fake_issue_iter(client: _FakeRestClient, cloud_id: str, jql: str, **_: Any):
    yield _issue(client.case)


def _fake_changelog_iter(client: _FakeRestClient, *, issue_key: str, **_: Any):
    return
    yield  # pragma: no cover - keeps this a generator under all Python versions


def _fake_worklog_iter(client: _FakeRestClient, *, issue_key: str, **_: Any):
    for raw in client.case.get("worklogs", []):
        author = raw.get("author")
        user = None
        if author:
            user = canonical_models.JiraUser(
                account_id=str(author["accountId"]),
                display_name=str(author.get("displayName") or author["accountId"]),
                email=author.get("emailAddress"),
            )
        yield canonical_models.JiraWorklog(
            issue_key=issue_key,
            worklog_id=str(raw["id"]),
            started_at=str(raw["started"]),
            time_spent_seconds=int(raw["timeSpentSeconds"]),
            created_at=str(raw["created"]),
            updated_at=str(raw["updated"]),
            author=user,
        )


def _fake_board_iter(client: _FakeRestClient, page_size: int = 50):
    if client.case.get("reference_sprints"):
        return
    yield canonical_models.JiraBoard(id="77", name="Operations", type="scrum")


def _fake_sprint_iter(client: _FakeRestClient, *, board_id: int, **_: Any):
    for raw in client.case.get("board_sprints", []):
        yield canonical_models.JiraSprint(
            id=str(raw["id"]),
            name=str(raw["name"]),
            state=str(raw["state"]),
            start_at=raw.get("startDate"),
            end_at=raw.get("endDate"),
            complete_at=raw.get("completeDate"),
        )


def _reference_sprints(case: dict[str, Any], org_id: str) -> list[Sprint]:
    rows: list[Sprint] = []
    for raw in case.get("reference_sprints", []):
        rows.append(
            Sprint(
                provider="jira",
                sprint_id=str(raw["id"]),
                name=raw.get("name"),
                state=raw.get("state"),
                started_at=_parse_time(raw.get("startDate")),
                ended_at=_parse_time(raw.get("endDate")),
                completed_at=_parse_time(raw.get("completeDate")),
                org_id=org_id,
                last_synced=_NORMALIZED_AT,
            )
        )
    return rows


def _parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def _deterministic(value: Any, org_id: str) -> dict[str, Any]:
    if not dataclasses.is_dataclass(value):
        raise TypeError(f"producer returned non-dataclass {type(value)!r}")
    names = {field.name for field in dataclasses.fields(value)}
    updates: dict[str, Any] = {}
    if "org_id" in names:
        updates["org_id"] = org_id
    if "last_synced" in names:
        updates["last_synced"] = _NORMALIZED_AT
    replaced = cast(Any, dataclasses.replace(cast(Any, value), **updates))
    return dataclasses.asdict(replaced)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    org_id = str(case["org_id"])
    status_mapping = load_status_mapping(path=_STATUS_MAPPING_PATH)
    identity = load_identity_resolver(path=_IDENTITY_MAPPING_PATH)
    client = _FakeRestClient(case)
    provider = JiraProvider(status_mapping=status_mapping, identity=identity)
    context = IngestionContext(
        window=IngestionWindow(
            updated_since=_parse_time(case["since"]),
            active_until=_parse_time(case["until"]),
        ),
        project_key=str(case["project_key"]),
        org_id=None,
        reference_sprints=_reference_sprints(case, org_id),
    )
    old = {
        "JIRA_FETCH_WORKLOGS": os.environ.get("JIRA_FETCH_WORKLOGS"),
        "JIRA_FETCH_BOARD_SPRINTS": os.environ.get("JIRA_FETCH_BOARD_SPRINTS"),
        "ATLASSIAN_GQL_ENABLED": os.environ.get("ATLASSIAN_GQL_ENABLED"),
    }
    old_functions = (
        atlassian.iter_issues_via_rest,
        atlassian.iter_issue_changelog_via_rest,
        atlassian.iter_issue_worklogs_via_rest,
        jira_worklogs_api.iter_issue_worklogs_via_graphql,
        atlassian.iter_board_sprints_via_rest,
        jira_boards.iter_boards_via_rest,
        atlassian_compat.build_atlassian_rest_client,
        atlassian_compat.build_atlassian_graphql_client,
        atlassian_compat.get_atlassian_cloud_id,
    )
    graphql_attempted = False

    def _failing_graphql_iter(*_args: Any, **_kwargs: Any):
        nonlocal graphql_attempted
        graphql_attempted = True
        raise RuntimeError("graphql worklog test failure")

    try:
        os.environ["JIRA_FETCH_WORKLOGS"] = "1" if case.get("fetch_worklogs") else "0"
        os.environ["JIRA_FETCH_BOARD_SPRINTS"] = (
            "1" if case.get("fetch_board_sprints") else "0"
        )
        os.environ["ATLASSIAN_GQL_ENABLED"] = (
            "1" if case.get("graphql_fallback") else "0"
        )
        atlassian.iter_issues_via_rest = lambda _client, cloud_id, jql, **kwargs: (
            _fake_issue_iter(client, cloud_id, jql, **kwargs)
        )
        atlassian.iter_issue_changelog_via_rest = (
            lambda _client, *, issue_key, **kwargs: _fake_changelog_iter(
                client, issue_key=issue_key, **kwargs
            )
        )
        atlassian.iter_issue_worklogs_via_rest = (
            lambda _client, *, issue_key, **kwargs: _fake_worklog_iter(
                client, issue_key=issue_key, **kwargs
            )
        )
        jira_worklogs_api.iter_issue_worklogs_via_graphql = _failing_graphql_iter
        atlassian.iter_board_sprints_via_rest = lambda _client, *, board_id, **kwargs: (
            _fake_sprint_iter(client, board_id=board_id, **kwargs)
        )
        jira_boards.iter_boards_via_rest = lambda _client, page_size=50: (
            _fake_board_iter(client, page_size)
        )
        setattr(
            atlassian_compat,
            "build_atlassian_rest_client",
            lambda *args: client,
        )
        setattr(
            atlassian_compat,
            "build_atlassian_graphql_client",
            lambda *args, **kwargs: client,
        )
        setattr(
            atlassian_compat,
            "get_atlassian_cloud_id",
            lambda: "cloud-301",
        )
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            batch: ProviderBatch = provider._ingest_via_atlassian_client(context)
    finally:
        for name, value in old.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        (
            atlassian.iter_issues_via_rest,
            atlassian.iter_issue_changelog_via_rest,
            atlassian.iter_issue_worklogs_via_rest,
        ) = old_functions[:3]
        (
            jira_worklogs_api.iter_issue_worklogs_via_graphql,
            atlassian.iter_board_sprints_via_rest,
            jira_boards.iter_boards_via_rest,
        ) = old_functions[3:6]
        setattr(atlassian_compat, "build_atlassian_rest_client", old_functions[6])
        setattr(atlassian_compat, "build_atlassian_graphql_client", old_functions[7])
        setattr(atlassian_compat, "get_atlassian_cloud_id", old_functions[8])
    if not client.closed:
        raise RuntimeError("Atlassian Jira producer did not close REST client")
    if case.get("graphql_fallback") and not graphql_attempted:
        raise RuntimeError("Atlassian Jira producer did not attempt GraphQL worklogs")
    return {
        "work_items": [_deterministic(row, org_id) for row in batch.work_items],
        "status_transitions": [
            _deterministic(row, org_id) for row in batch.status_transitions
        ],
        "dependencies": [_deterministic(row, org_id) for row in batch.dependencies],
        "interactions": [_deterministic(row, org_id) for row in batch.interactions],
        "reopen_events": [_deterministic(row, org_id) for row in batch.reopen_events],
        "worklogs": [_deterministic(row, org_id) for row in batch.worklogs],
        "sprints": [_deterministic(row, org_id) for row in batch.sprints],
        "ai_attributions": [
            _deterministic(row, org_id) for row in batch.ai_attributions
        ],
        "observations": batch.observations,
    }


def _reflected_fields() -> frozenset[str]:
    return frozenset(field.name for field in dataclasses.fields(ProviderBatch))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="jira/work-items/atlassian",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "work_items": "This pair owns Atlassian enrichment only.",
            "status_transitions": "This pair owns Atlassian enrichment only.",
            "dependencies": "The Atlassian provider returns explicit empty dependencies; legacy parity is covered elsewhere.",
            "interactions": "The Atlassian provider returns explicit empty interactions; legacy parity is covered elsewhere.",
            "reopen_events": "This pair owns Atlassian enrichment only.",
            "observations": "Usage observations are transport metadata, not the worklog/sprint persistence contract.",
        },
    )
)
