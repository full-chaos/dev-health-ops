import argparse
import pytest
from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.fixtures.runner import (
    _build_repo_team_assignments,
    run_fixtures_generation,
)
from dev_health_ops.storage import SQLAlchemyStore


@pytest.mark.asyncio
async def test_fixtures_generation_smoke_sqlite(tmp_path):
    """
    Smoke test to ensure fixtures generation runs without crashing in SQLite.
    This would have caught the 'now' scope error and missing imports.
    """
    db_file = tmp_path / "test_fixtures.db"
    db_uri = f"sqlite:///{db_file}"

    # Mock argparse.Namespace
    ns = argparse.Namespace(
        db=db_uri,
        db_type="sqlite",
        repo_name="test/repo",
        repo_count=1,
        days=2,
        commits_per_day=2,
        pr_count=2,
        seed=42,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=True,
        team_count=2,
    )

    # Run the generation
    # We expect this to complete without raising NameError, SyntaxError, etc.
    result = await run_fixtures_generation(ns)

    assert result == 0
    assert db_file.exists()


@pytest.mark.asyncio
async def test_fixtures_generation_minimal_no_metrics(tmp_path):
    """
    Ensure minimal generation works without the metrics flag.
    """
    db_file = tmp_path / "test_minimal.db"
    db_uri = f"sqlite:///{db_file}"

    ns = argparse.Namespace(
        db=db_uri,
        db_type="sqlite",
        repo_name="test/minimal",
        repo_count=1,
        days=1,
        commits_per_day=1,
        pr_count=1,
        seed=1,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=1,
    )

    result = await run_fixtures_generation(ns)
    assert result == 0
    assert db_file.exists()


@pytest.mark.asyncio
async def test_fixtures_generation_ensures_tables(tmp_path, monkeypatch):
    db_file = tmp_path / "test_ensure_tables.db"
    db_uri = f"sqlite:///{db_file}"

    called = {"value": False}
    original = SQLAlchemyStore.ensure_tables

    async def _wrapped(self):
        called["value"] = True
        return await original(self)

    monkeypatch.setattr(SQLAlchemyStore, "ensure_tables", _wrapped)

    ns = argparse.Namespace(
        db=db_uri,
        db_type="sqlite",
        repo_name="test/ensure",
        repo_count=1,
        days=1,
        commits_per_day=1,
        pr_count=1,
        seed=2,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=1,
    )

    result = await run_fixtures_generation(ns)

    assert result == 0
    assert db_file.exists()
    assert called["value"] is True


def test_repo_team_assignments_distribution():
    teams = SyntheticDataGenerator(seed=123).get_team_assignment(count=6)["teams"]
    assignments = _build_repo_team_assignments(teams, repo_count=20, seed=123)

    assert len(assignments) == 20

    unowned_count = sum(1 for repo_teams in assignments if not repo_teams)
    assert unowned_count <= int(20 * 0.1)

    owned_by_team = {team.id: 0 for team in teams}
    for repo_teams in assignments:
        for team in repo_teams:
            owned_by_team[team.id] += 1
    assert all(count >= 1 for count in owned_by_team.values())

    multi_owned = sum(1 for count in owned_by_team.values() if count >= 2)
    assert multi_owned >= min(3, len(owned_by_team))
