import argparse

import pytest

from dev_health_ops.fixtures import runner
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
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
        repo_name="test/repo",
        repo_count=1,
        days=2,
        commits_per_day=2,
        pr_count=2,
        seed=42,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=2,
    )

    # Run the generation (metrics require ClickHouse since CHAOS-641)
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
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
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
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
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


@pytest.mark.asyncio
async def test_fixtures_generation_initializes_license_manager(tmp_path):
    from dev_health_ops.licensing import LicenseManager, LicenseTier
    from dev_health_ops.licensing.gating import LicenseAuditLogger

    LicenseManager.reset()
    LicenseAuditLogger.reset()

    db_file = tmp_path / "test_license.db"
    db_uri = f"sqlite:///{db_file}"

    ns = argparse.Namespace(
        sink=db_uri,
        db_type="sqlite",
        org_id="test-org",
        repo_name="test/license-check",
        repo_count=1,
        days=1,
        commits_per_day=1,
        pr_count=1,
        seed=99,
        provider="synthetic",
        with_work_graph=False,
        with_metrics=False,
        team_count=1,
    )

    result = await run_fixtures_generation(ns)
    assert result == 0

    manager = LicenseManager.get_instance()
    assert manager.is_licensed is True
    assert manager.tier == LicenseTier.ENTERPRISE

    LicenseManager.reset()
    LicenseAuditLogger.reset()


class _QueryResult:
    def __init__(self, value: int):
        self.result_rows = [(value,)]


class _ValidationClient:
    def __init__(
        self,
        *,
        records: int = 12,
        expected_repos: int = 3,
        expected_teams: int = 4,
        covered_repos: int = 3,
        covered_teams: int = 4,
    ):
        self.records = records
        self.expected_repos = expected_repos
        self.expected_teams = expected_teams
        self.covered_repos = covered_repos
        self.covered_teams = covered_teams

    def query(self, sql: str):
        normalized = " ".join(sql.split())
        if "FROM work_unit_investments AS wui" in normalized:
            return _QueryResult(self.covered_teams)
        if "FROM work_unit_investments" in normalized:
            if "countDistinct(repo_id)" in normalized:
                return _QueryResult(self.covered_repos)
            return _QueryResult(self.records)
        if "FROM repo_metrics_daily" in normalized:
            return _QueryResult(self.expected_repos)
        if "FROM team_metrics_daily" in normalized:
            return _QueryResult(self.expected_teams)
        raise AssertionError(f"Unexpected query: {normalized}")


def _all_tables_exist(name: str) -> bool:
    return name in {
        "work_unit_investments",
        "repo_metrics_daily",
        "team_metrics_daily",
    }


def test_work_unit_investment_validation_accepts_density_and_coverage():
    client = _ValidationClient()
    validate = getattr(runner, "validate_work_unit_investment_density_and_coverage")

    assert (
        validate(
            client,
            table_exists=_all_tables_exist,
        )
        is True
    )


def test_work_unit_investment_validation_rejects_low_density():
    client = _ValidationClient(records=4, expected_repos=3, expected_teams=4)
    validate = getattr(runner, "validate_work_unit_investment_density_and_coverage")

    assert (
        validate(
            client,
            table_exists=_all_tables_exist,
        )
        is False
    )


def test_work_unit_investment_validation_rejects_low_repo_or_team_coverage():
    low_repo_client = _ValidationClient(covered_repos=2, expected_repos=3)
    low_team_client = _ValidationClient(covered_teams=2, expected_teams=4)
    validate = getattr(runner, "validate_work_unit_investment_density_and_coverage")

    assert (
        validate(
            low_repo_client,
            table_exists=_all_tables_exist,
        )
        is False
    )
    assert (
        validate(
            low_team_client,
            table_exists=_all_tables_exist,
        )
        is False
    )
