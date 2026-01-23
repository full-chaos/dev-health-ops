import argparse
import pytest
from fixtures.runner import run_fixtures_generation


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
