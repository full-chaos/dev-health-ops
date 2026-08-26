import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.loaders.sqlalchemy import SqlAlchemyDataLoader


@pytest.mark.asyncio
async def test_clickhouse_loader_async():
    mock_client = MagicMock()
    # Mocking successful queries
    mock_result = MagicMock()
    mock_result.column_names = [
        "repo_id",
        "commit_hash",
        "author_email",
        "author_name",
        "committer_when",
        "file_path",
        "additions",
        "deletions",
    ]
    mock_result.result_rows = []
    mock_client.query.return_value = mock_result

    loader = ClickHouseDataLoader(mock_client)
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    # Verify it's awaitable and doesn't crash
    res = await loader.load_git_rows(start, end, uuid.uuid4())
    assert len(res) == 3
    assert isinstance(res[0], list)


@pytest.mark.asyncio
async def test_sqlalchemy_loader_async():
    engine = create_engine("sqlite:///:memory:")
    # Simple setup: tables usually exist, but we just verify the async call wrap
    loader = SqlAlchemyDataLoader(engine)
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=1)

    # This will likely fail on missing tables if executed, but we want to verify the async signature
    with pytest.raises(Exception):
        await loader.load_git_rows(start, end, uuid.uuid4())


@pytest.mark.asyncio
async def test_sqlalchemy_loader_pr_query_scopes_by_repo_id():
    """CHAOS-4324: the SqlAlchemy mirror had the identical unparenthesized-OR
    bug, scoped to ``repo_id`` instead of ``org_id`` (this loader has no org
    concept -- it backs the semantic/test DB only). A PR row from a
    DIFFERENT repo, created inside the same requested window, must not leak
    into a repo-scoped ``load_git_rows`` call.
    """
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE git_pull_requests ("
                "repo_id TEXT, number INTEGER, author_email TEXT, "
                "author_name TEXT, created_at TEXT, merged_at TEXT, "
                "first_review_at TEXT, first_comment_at TEXT, "
                "changes_requested_count INTEGER, reviews_count INTEGER, "
                "comments_count INTEGER, additions INTEGER, "
                "deletions INTEGER, changed_files INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE git_commits (repo_id TEXT, hash TEXT, "
                "author_email TEXT, author_name TEXT, committer_when TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE git_commit_stats (repo_id TEXT, commit_hash TEXT, "
                "file_path TEXT, additions INTEGER, deletions INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE git_pull_request_reviews (repo_id TEXT, number INTEGER, "
                "reviewer TEXT, submitted_at TEXT, state TEXT)"
            )
        )

        start = datetime(2026, 3, 1, tzinfo=timezone.utc)
        inside_window = start + timedelta(hours=6)
        repo_a = uuid.uuid4()
        repo_b = uuid.uuid4()
        for repo, email in (
            (repo_a, "alice@repo-a.example"),
            (repo_b, "mallory@repo-b.example"),
        ):
            conn.execute(
                text(
                    "INSERT INTO git_pull_requests "
                    "(repo_id, number, author_email, author_name, created_at, "
                    "changes_requested_count, reviews_count, comments_count, "
                    "additions, deletions, changed_files) "
                    "VALUES (:repo_id, 1, :email, 'name', :created_at, 0, 0, 0, 0, 0, 0)"
                ),
                {
                    "repo_id": str(repo),
                    "email": email,
                    "created_at": inside_window.isoformat(),
                },
            )

    loader = SqlAlchemyDataLoader(engine)
    end = start + timedelta(days=1)
    _commits, prs, _reviews = await loader.load_git_rows(start, end, repo_id=repo_a)

    leaked = [p for p in prs if p["author_email"] == "mallory@repo-b.example"]
    assert not leaked, (
        f"repo-scoped load_git_rows leaked another repo's PR row: {leaked!r}"
    )
    own = [p for p in prs if p["author_email"] == "alice@repo-a.example"]
    assert len(own) == 1, f"expected repo A's own PR row, got: {prs!r}"
