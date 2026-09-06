from __future__ import annotations

from typing import Any

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
        native_team_key="ENG",
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


def test_linear_store_populated_by_team_fetches_cycles_at_most_once_per_run() -> None:
    client = _LinearClient()
    eng_sprint = Sprint(
        provider="linear",
        sprint_id="linear:cycle:eng-cycle",
        name="Engineering Cycle",
        state="future",
        started_at=None,
        ended_at=None,
        completed_at=None,
        native_team_key="ENG",
    )
    ops_sprint = Sprint(
        provider="linear",
        sprint_id="linear:cycle:ops-cycle",
        name="Ops Cycle",
        state="future",
        started_at=None,
        ended_at=None,
        completed_at=None,
        native_team_key="OPS",
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
        reference_sprints=[eng_sprint, ops_sprint],
    )

    batches = list(_linear_provider(client).iter_ingest(ctx))

    assert client.iter_cycles_calls <= 1
    assert client.iter_cycles_calls == 0
    assert [item.sprint_id for batch in batches for item in batch.sprints] == [
        "linear:cycle:eng-cycle"
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
    assert client.get_team_by_key_calls == 1
    assert client.iter_cycles_calls == 1
    assert sink.teams == []
    assert [sprint.sprint_id for sprint in sink.sprints] == ["linear:cycle:cycle-1"]
    assert [sprint.native_team_key for sprint in sink.sprints] == ["ENG"]
    assert [item.sprint_id for batch in batches for item in batch.sprints] == [
        "linear:cycle:cycle-1"
    ]
    assert [item.native_team_key for batch in batches for item in batch.sprints] == [
        "ENG"
    ]


def test_linear_unscoped_sprint_cache_does_not_skip_current_team_fetch() -> None:
    client = _LinearClient()
    other_team_sprint = Sprint(
        provider="linear",
        sprint_id="linear:cycle:other-team-cycle",
        name="Other Cycle",
        state="future",
        started_at=None,
        ended_at=None,
        completed_at=None,
        native_team_key="OPS",
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
        reference_sprints=[other_team_sprint],
    )

    batches = list(_linear_provider(client).iter_ingest(ctx))

    assert client.get_team_by_key_calls == 1
    assert client.iter_cycles_calls == 1
    assert client.iter_cycles_calls <= 1
    assert [item.sprint_id for batch in batches for item in batch.sprints] == [
        "linear:cycle:cycle-1"
    ]
