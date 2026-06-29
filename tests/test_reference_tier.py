from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dev_health_ops.metrics.work_items import fetch_jira_work_items_with_extras
from dev_health_ops.models.work_items import Sprint
from dev_health_ops.providers.base import IngestionContext, IngestionWindow
from dev_health_ops.providers.linear.provider import LinearProvider


class _StatusMapping:
    def normalize_status(self, **_kwargs: Any) -> str:
        return "todo"

    def normalize_type(self, **_kwargs: Any) -> str:
        return "task"


class _Identity:
    def resolve(self, **_kwargs: Any) -> str:
        return "user@example.com"


class _LinearClient:
    def __init__(self) -> None:
        self.iter_teams_calls = 0
        self.get_team_by_key_calls = 0
        self.iter_cycles_calls = 0

    def iter_teams(self) -> list[dict[str, Any]]:
        self.iter_teams_calls += 1
        return [{"id": "api-eng", "key": "ENG", "name": "Engineering"}]

    def get_team_by_key(self, team_key: str) -> dict[str, Any] | None:
        self.get_team_by_key_calls += 1
        if team_key == "ENG":
            return {"id": "api-eng", "key": "ENG", "name": "Engineering"}
        return None

    def iter_cycles(self, *, team_id: str | None = None) -> list[dict[str, Any]]:
        self.iter_cycles_calls += 1
        assert team_id == "api-eng"
        return [
            {
                "id": "cycle-1",
                "name": "Cycle 1",
                "startsAt": "2024-01-01T00:00:00Z",
                "endsAt": "2024-01-14T00:00:00Z",
                "completedAt": None,
                "progress": 0,
            }
        ]

    def iter_issues_pages(self, **_kwargs: Any) -> list[list[dict[str, Any]]]:
        return []

    def iter_issues(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return []


class _ReferenceSink:
    def __init__(self) -> None:
        self.teams: list[dict[str, Any]] = []
        self.sprints: list[Sprint] = []

    async def insert_teams(self, teams: list[dict[str, Any]]) -> None:
        self.teams.extend(teams)

    def write_sprints(self, sprints: list[Sprint]) -> None:
        self.sprints.extend(sprints)


def _linear_provider(client: _LinearClient) -> LinearProvider:
    status_mapping: Any = _StatusMapping()
    identity: Any = _Identity()
    fake_client: Any = client
    return LinearProvider(
        status_mapping=status_mapping,
        identity=identity,
        client=fake_client,
    )


def test_linear_store_hit_avoids_reference_api() -> None:
    client = _LinearClient()
    sprint = Sprint(
        provider="linear",
        sprint_id="linear:cycle:cycle-1",
        name="Cycle 1",
        state="future",
        started_at=None,
        ended_at=None,
        completed_at=None,
    )
    ctx = IngestionContext(
        window=IngestionWindow(),
        repo="ENG",
        reference_teams=[
            {
                "id": "ENG",
                "name": "Engineering",
                "provider": "linear",
                "native_team_key": "ENG",
                "project_keys": ["ENG"],
            }
        ],
        reference_sprints=[sprint],
    )

    batches = list(_linear_provider(client).iter_ingest(ctx))

    assert client.iter_teams_calls == 0
    assert client.get_team_by_key_calls == 0
    assert client.iter_cycles_calls == 0
    assert [item.sprint_id for batch in batches for item in batch.sprints] == [
        "linear:cycle:cycle-1"
    ]


def test_linear_store_miss_fetches_scoped_references_once() -> None:
    client = _LinearClient()
    sink = _ReferenceSink()
    ctx = IngestionContext(
        window=IngestionWindow(),
        repo="ENG",
        reference_teams=[],
        reference_sprints=[],
        reference_sink=sink,
    )

    batches = list(_linear_provider(client).iter_ingest(ctx))

    assert client.iter_teams_calls == 0
    assert client.get_team_by_key_calls <= 2
    assert client.iter_cycles_calls == 1
    assert sink.teams[0]["native_team_key"] == "ENG"
    assert [sprint.sprint_id for sprint in sink.sprints] == ["linear:cycle:cycle-1"]
    assert [item.sprint_id for batch in batches for item in batch.sprints] == [
        "linear:cycle:cycle-1"
    ]


class _JiraClient:
    def __init__(self) -> None:
        self.get_sprint_calls = 0
        self.closed = False

    def iter_issues(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "key": "ENG-1",
                "fields": {
                    "summary": "Do work",
                    "project": {"key": "ENG", "id": "100", "name": "Engineering"},
                    "status": {"name": "To Do", "statusCategory": {"key": "new"}},
                    "issuetype": {"name": "Task"},
                    "labels": [],
                    "created": "2024-01-01T00:00:00.000+0000",
                    "updated": "2024-01-02T00:00:00.000+0000",
                    "customfield_10020": [{"id": 7, "name": "Sprint 7"}],
                },
            }
        ]

    def iter_issue_comments(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return []

    def get_sprint(self, *, sprint_id: str) -> dict[str, Any]:
        self.get_sprint_calls += 1
        return {"id": sprint_id, "name": f"Sprint {sprint_id}", "state": "active"}

    def close(self) -> None:
        self.closed = True


def _jira_args(client: _JiraClient, **kwargs: Any) -> dict[str, Any]:
    base = {
        "since": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "until": datetime(2024, 1, 3, tzinfo=timezone.utc),
        "status_mapping": _StatusMapping(),
        "identity": _Identity(),
        "project_keys": ["ENG"],
        "client": client,
        "use_env_query_options": False,
    }
    base.update(kwargs)
    return base


def test_jira_store_hit_avoids_per_sprint_api(monkeypatch: Any) -> None:
    monkeypatch.setenv("JIRA_FETCH_COMMENTS", "0")
    monkeypatch.setenv("JIRA_SPRINT_FIELD", "customfield_10020")
    client = _JiraClient()
    sprint = Sprint(
        provider="jira",
        sprint_id="7",
        name="Sprint 7",
        state="active",
        started_at=None,
        ended_at=None,
        completed_at=None,
    )

    *_, sprints = fetch_jira_work_items_with_extras(
        **_jira_args(client, reference_sprints=[sprint])
    )

    assert client.get_sprint_calls == 0
    assert [item.sprint_id for item in sprints] == ["7"]


def test_jira_store_miss_fetches_and_persists_sprint_once(monkeypatch: Any) -> None:
    monkeypatch.setenv("JIRA_FETCH_COMMENTS", "0")
    monkeypatch.setenv("JIRA_SPRINT_FIELD", "customfield_10020")
    client = _JiraClient()
    sink = _ReferenceSink()

    *_, sprints = fetch_jira_work_items_with_extras(
        **_jira_args(client, reference_sprints=[], reference_sink=sink)
    )

    assert client.get_sprint_calls == 1
    assert [item.sprint_id for item in sprints] == ["7"]
    assert [item.sprint_id for item in sink.sprints] == ["7"]
