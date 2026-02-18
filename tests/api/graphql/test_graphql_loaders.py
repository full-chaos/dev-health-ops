from __future__ import annotations

import pytest

from dev_health_ops.api.graphql.loaders.base import CachedDataLoader, make_cache_key
from dev_health_ops.api.graphql.loaders.dimension_loader import load_dimension_values
from dev_health_ops.api.graphql.loaders.repo_loader import RepoByNameLoader, RepoLoader
from dev_health_ops.api.graphql.loaders.team_loader import TeamByNameLoader, TeamLoader


class _Cache:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value


class _IntLoader(CachedDataLoader[int, int | None]):
    def __init__(self, cache=None):
        super().__init__(cache=cache, cache_prefix="ints")
        self.calls = 0

    async def batch_load(self, keys):
        self.calls += 1
        return [k * 10 for k in keys]


@pytest.mark.asyncio
async def test_cached_data_loader_uses_external_cache_for_hits():
    cache = _Cache()
    cached_key = make_cache_key("ints", 1)
    cache.set(cached_key, 999)

    loader = _IntLoader(cache=cache)
    values = await loader._load_with_cache([1, 2])

    assert values == [999, 20]
    assert loader.calls == 1
    assert cache.get(make_cache_key("ints", 2)) == 20


@pytest.mark.asyncio
async def test_load_dimension_values_maps_rows(monkeypatch):
    async def fake_query_dicts(_client, _sql, _params):
        return [{"value": "team-a", "count": 3}]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    rows = await load_dimension_values(
        client=object(),
        dimension="team",
        org_id="org-1",
        limit=10,
        timeout=5,
        filters=None,
    )

    assert rows == [{"value": "team-a", "count": 3}]


@pytest.mark.asyncio
async def test_repo_and_team_loaders_return_data_in_key_order(monkeypatch):
    async def fake_query_dicts(_client, _sql, params):
        if "repo_ids" in params:
            return [{"repo_id": "r2", "repo_name": "repo-two", "org_id": "org-1"}]
        if "repo_names" in params:
            return [{"repo_id": "r1", "repo_name": "Repo-One", "org_id": "org-1"}]
        if "team_ids" in params:
            return [{"team_id": "t1", "team_name": "Team One", "org_id": "org-1", "member_count": 2}]
        if "team_names" in params:
            return [{"team_id": "t2", "team_name": "Core", "org_id": "org-1", "member_count": 4}]
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    repo_loader = RepoLoader(client=object(), org_id="org-1")
    repo_by_name_loader = RepoByNameLoader(client=object(), org_id="org-1")
    team_loader = TeamLoader(client=object(), org_id="org-1")
    team_by_name_loader = TeamByNameLoader(client=object(), org_id="org-1")

    repos = await repo_loader.batch_load(["r1", "r2"])
    repos_by_name = await repo_by_name_loader.batch_load(["repo-one", "missing"])
    teams = await team_loader.batch_load(["t1", "missing"])
    teams_by_name = await team_by_name_loader.batch_load(["core", "missing"])

    assert repos[0] is None
    assert repos[1] is not None and repos[1].repo_name == "repo-two"

    assert repos_by_name[0] is not None and repos_by_name[0].repo_id == "r1"
    assert repos_by_name[1] is None

    assert teams[0] is not None and teams[0].member_count == 2
    assert teams[1] is None

    assert teams_by_name[0] is not None and teams_by_name[0].team_id == "t2"
    assert teams_by_name[1] is None
