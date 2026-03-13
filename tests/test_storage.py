import uuid
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import select, text

from dev_health_ops.models import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from dev_health_ops.models.git import Base
from dev_health_ops.storage import (
    ClickHouseStore,
    SQLAlchemyStore,
    create_store,
    detect_db_type,
    model_to_dict,
)


class TestDetectDbType:
    """Tests for the detect_db_type function."""

    def test_detect_clickhouse(self):
        """Test detection of ClickHouse connection strings."""
        assert detect_db_type("clickhouse://localhost:8123/default") == "clickhouse"
        assert (
            detect_db_type("clickhouse+http://localhost:8123/default") == "clickhouse"
        )
        assert (
            detect_db_type("clickhouse+https://localhost:8443/default") == "clickhouse"
        )

    def test_detect_postgresql(self):
        """Test detection of PostgreSQL connection strings."""
        assert detect_db_type("postgresql://localhost/mydb") == "postgres"
        assert detect_db_type("postgres://localhost/mydb") == "postgres"
        assert detect_db_type("postgresql+asyncpg://localhost/mydb") == "postgres"

    def test_detect_sqlite(self):
        """Test detection of SQLite connection strings."""
        assert detect_db_type("sqlite:///test.db") == "sqlite"
        assert detect_db_type("sqlite+aiosqlite:///:memory:") == "sqlite"

    def test_detect_case_insensitive(self):
        """Test that detection is case-insensitive."""
        assert detect_db_type("PostgreSQL://localhost/mydb") == "postgres"
        assert detect_db_type("POSTGRESQL://localhost/mydb") == "postgres"
        assert detect_db_type("SQLite:///test.db") == "sqlite"
        assert detect_db_type("SQLITE:///test.db") == "sqlite"

    def test_detect_empty_string_raises(self):
        """Test that empty connection string raises ValueError."""
        with pytest.raises(ValueError, match="Connection string is required"):
            detect_db_type("")

    def test_detect_unknown_raises(self):
        """Test that unknown connection string raises ValueError."""
        with pytest.raises(ValueError, match="Could not detect database type"):
            detect_db_type("unknown://localhost/mydb")


class TestCreateStore:
    """Tests for the create_store factory function."""

    def test_create_sqlalchemy_store_postgres(self):
        """Test creation of SQLAlchemyStore for PostgreSQL."""
        store = create_store("postgresql+asyncpg://localhost/mydb")
        assert isinstance(store, SQLAlchemyStore)

    def test_create_sqlalchemy_store_sqlite(self):
        """Test creation of SQLAlchemyStore for SQLite."""
        store = create_store("sqlite+aiosqlite:///:memory:")
        assert isinstance(store, SQLAlchemyStore)

    def test_create_clickhouse_store(self):
        """Test creation of ClickHouseStore from connection string."""
        store = create_store("clickhouse://localhost:8123/default")
        assert isinstance(store, ClickHouseStore)

    def test_create_store_with_explicit_db_type(self):
        """Test creation with explicit db_type overriding auto-detection."""
        # Force PostgreSQL even though URL looks like SQLite
        store = create_store("postgresql+asyncpg://localhost/mydb", db_type="postgres")
        assert isinstance(store, SQLAlchemyStore)

    def test_create_store_unsupported_type_raises(self):
        """Test that unsupported db_type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported database type"):
            create_store("postgresql://localhost/mydb", db_type="oracle")


def test_model_to_dict_serializes_uuid_and_fields(repo_uuid):
    blame = GitBlame(
        repo_id=repo_uuid,
        path="file.txt",
        line_no=1,
        author_email="author@example.com",
        author_name="Author",
        author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
        commit_hash="abc123",
        line="content",
    )

    doc = model_to_dict(blame)

    assert doc["repo_id"] == str(repo_uuid)
    assert doc["path"] == "file.txt"
    assert doc["line_no"] == 1
    assert doc["commit_hash"] == "abc123"


@pytest.fixture
def test_db_url():
    """Return a SQLite in-memory database URL for testing."""
    return "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def sqlalchemy_store(test_db_url):
    """Create a SQLAlchemyStore instance with an in-memory database."""
    store = SQLAlchemyStore(test_db_url)

    # Create tables
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield store

    # Cleanup
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await store.engine.dispose()


@pytest.mark.asyncio
async def test_sqlalchemy_store_context_manager(test_db_url):
    """Test that SQLAlchemyStore can be used as an async context manager."""
    store = SQLAlchemyStore(test_db_url)

    # Create tables first
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with store as s:
        assert s.session is not None
        assert s == store

    # After exiting context, session should be closed
    # Note: session is closed but not None
    await store.engine.dispose()


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_repo(sqlalchemy_store):
    """Test inserting a repository into the database."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)

        # Verify the repo was inserted
        result = await store.session.execute(
            select(Repo).where(Repo.id == test_repo.id)
        )
        saved_repo = result.scalar_one_or_none()

        assert saved_repo is not None
        assert saved_repo.id == test_repo.id
        assert saved_repo.repo == "https://github.com/test/repo.git"
        assert saved_repo.ref == "main"


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_repo_duplicate(sqlalchemy_store):
    """Test that inserting a duplicate repo does not cause an error."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    async with sqlalchemy_store as store:
        # Insert the repo twice
        await store.insert_repo(test_repo)
        await store.insert_repo(test_repo)

        # Verify only one repo exists
        result = await store.session.execute(
            select(Repo).where(Repo.id == test_repo.id)
        )
        repos = result.scalars().all()

        assert len(repos) == 1


@pytest.mark.asyncio
async def test_sqlalchemy_store_get_complexity_snapshots_latest_for_repo(
    sqlalchemy_store,
):
    repo_id = uuid.uuid4()
    other_repo_id = uuid.uuid4()
    computed_at = datetime(2025, 1, 10, tzinfo=timezone.utc).isoformat()

    async with sqlalchemy_store as store:
        await store.insert_repo(Repo(id=repo_id, repo="owner/repo"))
        await store.insert_repo(Repo(id=other_repo_id, repo="owner/other"))

        await store.session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS file_complexity_snapshots (
                  repo_id TEXT NOT NULL,
                  as_of_day TEXT NOT NULL,
                  ref TEXT NOT NULL,
                  file_path TEXT NOT NULL,
                  language TEXT,
                  loc INTEGER NOT NULL,
                  functions_count INTEGER NOT NULL,
                  cyclomatic_total INTEGER NOT NULL,
                  cyclomatic_avg REAL NOT NULL,
                  high_complexity_functions INTEGER NOT NULL,
                  very_high_complexity_functions INTEGER NOT NULL,
                  computed_at TEXT NOT NULL,
                  PRIMARY KEY (repo_id, as_of_day, file_path)
                )
                """
            )
        )
        await store.session.commit()

        # repo_id: snapshots on two different days
        await store.session.execute(
            text(
                """
                INSERT INTO file_complexity_snapshots
                (repo_id, as_of_day, ref, file_path, language, loc, functions_count,
                 cyclomatic_total, cyclomatic_avg, high_complexity_functions,
                 very_high_complexity_functions, computed_at)
                VALUES
                (:repo_id, '2025-01-01', 'main', 'a.py', 'python', 10, 1, 5, 5.0, 0, 0, :computed_at),
                (:repo_id, '2025-01-03', 'main', 'a.py', 'python', 12, 1, 8, 8.0, 0, 0, :computed_at),
                (:repo_id, '2025-01-03', 'main', 'b.py', 'python', 20, 2, 3, 1.5, 0, 0, :computed_at)
                """
            ),
            {"repo_id": str(repo_id), "computed_at": computed_at},
        )
        # other_repo_id: only one day
        await store.session.execute(
            text(
                """
                INSERT INTO file_complexity_snapshots
                (repo_id, as_of_day, ref, file_path, language, loc, functions_count,
                 cyclomatic_total, cyclomatic_avg, high_complexity_functions,
                 very_high_complexity_functions, computed_at)
                VALUES
                (:repo_id, '2025-01-02', 'main', 'x.py', 'python', 1, 1, 1, 1.0, 0, 0, :computed_at)
                """
            ),
            {"repo_id": str(other_repo_id), "computed_at": computed_at},
        )
        await store.session.commit()

        snaps = await store.get_complexity_snapshots(
            as_of_day=date(2025, 1, 5),
            repo_id=repo_id,
        )
        assert {s.file_path for s in snaps} == {"a.py", "b.py"}
        assert {s.as_of_day for s in snaps} == {date(2025, 1, 3)}


@pytest.mark.asyncio
async def test_sqlalchemy_store_get_complexity_snapshots_latest_for_all_repos(
    sqlalchemy_store,
):
    repo_id = uuid.uuid4()
    other_repo_id = uuid.uuid4()
    computed_at = datetime(2025, 1, 10, tzinfo=timezone.utc).isoformat()

    async with sqlalchemy_store as store:
        await store.insert_repo(Repo(id=repo_id, repo="owner/repo"))
        await store.insert_repo(Repo(id=other_repo_id, repo="owner/other"))

        await store.session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS file_complexity_snapshots (
                  repo_id TEXT NOT NULL,
                  as_of_day TEXT NOT NULL,
                  ref TEXT NOT NULL,
                  file_path TEXT NOT NULL,
                  language TEXT,
                  loc INTEGER NOT NULL,
                  functions_count INTEGER NOT NULL,
                  cyclomatic_total INTEGER NOT NULL,
                  cyclomatic_avg REAL NOT NULL,
                  high_complexity_functions INTEGER NOT NULL,
                  very_high_complexity_functions INTEGER NOT NULL,
                  computed_at TEXT NOT NULL,
                  PRIMARY KEY (repo_id, as_of_day, file_path)
                )
                """
            )
        )
        await store.session.commit()

        await store.session.execute(
            text(
                """
                INSERT INTO file_complexity_snapshots
                (repo_id, as_of_day, ref, file_path, language, loc, functions_count,
                 cyclomatic_total, cyclomatic_avg, high_complexity_functions,
                 very_high_complexity_functions, computed_at)
                VALUES
                (:repo1, '2025-01-01', 'main', 'a.py', 'python', 10, 1, 5, 5.0, 0, 0, :computed_at),
                (:repo1, '2025-01-03', 'main', 'a.py', 'python', 12, 1, 8, 8.0, 0, 0, :computed_at),
                (:repo2, '2025-01-02', 'main', 'x.py', 'python', 1, 1, 1, 1.0, 0, 0, :computed_at),
                (:repo2, '2025-01-04', 'main', 'x.py', 'python', 2, 1, 2, 2.0, 0, 0, :computed_at)
                """
            ),
            {
                "repo1": str(repo_id),
                "repo2": str(other_repo_id),
                "computed_at": computed_at,
            },
        )
        await store.session.commit()

        snaps = await store.get_complexity_snapshots(as_of_day=date(2025, 1, 5))
        by_repo = {}
        for s in snaps:
            by_repo.setdefault(s.repo_id, []).append(s)

        assert {s.as_of_day for s in by_repo[repo_id]} == {date(2025, 1, 3)}
        assert {s.as_of_day for s in by_repo[other_repo_id]} == {date(2025, 1, 4)}


@pytest.mark.asyncio
async def test_sqlalchemy_store_get_work_item_user_metrics_daily(sqlalchemy_store):
    computed_at = datetime(2025, 1, 10, tzinfo=timezone.utc).isoformat()

    async with sqlalchemy_store as store:
        await store.session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS work_item_user_metrics_daily (
                  day TEXT NOT NULL,
                  provider TEXT NOT NULL,
                  work_scope_id TEXT NOT NULL,
                  user_identity TEXT NOT NULL,
                  team_id TEXT NOT NULL,
                  team_name TEXT NOT NULL,
                  items_started INTEGER NOT NULL,
                  items_completed INTEGER NOT NULL,
                  wip_count_end_of_day INTEGER NOT NULL,
                  cycle_time_p50_hours REAL,
                  cycle_time_p90_hours REAL,
                  computed_at TEXT NOT NULL,
                  PRIMARY KEY (provider, work_scope_id, user_identity, day)
                )
                """
            )
        )
        await store.session.execute(
            text(
                """
                INSERT INTO work_item_user_metrics_daily
                (day, provider, work_scope_id, user_identity, team_id, team_name,
                 items_started, items_completed, wip_count_end_of_day,
                 cycle_time_p50_hours, cycle_time_p90_hours, computed_at)
                VALUES
                ('2025-01-05', 'github', 'gh:owner/repo', 'dev@example.com', 'team-a', 'Team A',
                 2, 1, 3, 12.0, 48.0, :computed_at)
                """
            ),
            {"computed_at": computed_at},
        )
        await store.session.commit()

        rows = await store.get_work_item_user_metrics_daily(day=date(2025, 1, 5))
        assert len(rows) == 1
        assert rows[0].user_identity == "dev@example.com"
        assert rows[0].items_completed == 1


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_file_data(sqlalchemy_store):
    """Test inserting git file data into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="file1.txt",
            executable=False,
            contents="content1",
        ),
        GitFile(
            repo_id=test_repo_id,
            path="file2.txt",
            executable=True,
            contents="content2",
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_file_data(file_data)

        # Verify files were inserted
        result = await store.session.execute(
            select(GitFile).where(GitFile.repo_id == test_repo_id)
        )
        saved_files = result.scalars().all()

        assert len(saved_files) == 2
        assert saved_files[0].path in ["file1.txt", "file2.txt"]
        assert saved_files[1].path in ["file1.txt", "file2.txt"]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_file_data_upsert(sqlalchemy_store):
    """Test that inserting duplicate git file data performs an upsert."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    initial = [
        GitFile(
            repo_id=test_repo_id,
            path="file.txt",
            executable=False,
            contents="content1",
        )
    ]
    updated = [
        GitFile(
            repo_id=test_repo_id,
            path="file.txt",
            executable=True,
            contents="content2",
        )
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_file_data(initial)
        await store.insert_git_file_data(updated)

        result = await store.session.execute(
            select(GitFile).where(GitFile.repo_id == test_repo_id)
        )
        saved_files = result.scalars().all()

        assert len(saved_files) == 1
        assert saved_files[0].path == "file.txt"
        assert saved_files[0].executable
        assert saved_files[0].contents == "content2"


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_file_data_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_git_file_data([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_data(sqlalchemy_store):
    """Test inserting git commit data into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    commit_data = [
        GitCommit(
            repo_id=test_repo_id,
            hash="abc123",
            message="Initial commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            parents=0,
        ),
        GitCommit(
            repo_id=test_repo_id,
            hash="def456",
            message="Second commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 2, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 2, tzinfo=timezone.utc),
            parents=1,
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_commit_data(commit_data)

        # Verify commits were inserted
        result = await store.session.execute(
            select(GitCommit).where(GitCommit.repo_id == test_repo_id)
        )
        saved_commits = result.scalars().all()

        assert len(saved_commits) == 2
        assert saved_commits[0].hash in ["abc123", "def456"]
        assert saved_commits[1].hash in ["abc123", "def456"]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_data_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_git_commit_data([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_stats(sqlalchemy_store):
    """Test inserting git commit stats into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    commit_stats = [
        GitCommitStat(
            repo_id=test_repo_id,
            commit_hash="abc123",
            file_path="file1.txt",
            additions=10,
            deletions=5,
            old_file_mode="100644",
            new_file_mode="100644",
        ),
        GitCommitStat(
            repo_id=test_repo_id,
            commit_hash="abc123",
            file_path="file2.txt",
            additions=20,
            deletions=3,
            old_file_mode="100644",
            new_file_mode="100755",
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_commit_stats(commit_stats)

        # Verify commit stats were inserted
        result = await store.session.execute(
            select(GitCommitStat).where(GitCommitStat.repo_id == test_repo_id)
        )
        saved_stats = result.scalars().all()

        assert len(saved_stats) == 2
        assert saved_stats[0].file_path in ["file1.txt", "file2.txt"]
        assert saved_stats[1].file_path in ["file1.txt", "file2.txt"]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_stats_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_git_commit_stats([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_blame_data(sqlalchemy_store):
    """Test inserting git blame data into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    blame_data = [
        GitBlame(
            repo_id=test_repo_id,
            path="file.txt",
            line_no=1,
            author_email="author@example.com",
            author_name="Test Author",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            commit_hash="abc123",
            line="line 1 content",
        ),
        GitBlame(
            repo_id=test_repo_id,
            path="file.txt",
            line_no=2,
            author_email="author@example.com",
            author_name="Test Author",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            commit_hash="abc123",
            line="line 2 content",
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_blame_data(blame_data)

        # Verify blame data was inserted
        result = await store.session.execute(
            select(GitBlame).where(GitBlame.repo_id == test_repo_id)
        )
        saved_blames = result.scalars().all()

        assert len(saved_blames) == 2
        assert saved_blames[0].line_no in [1, 2]
        assert saved_blames[1].line_no in [1, 2]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_blame_data_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_blame_data([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_session_management(test_db_url):
    """Test session lifecycle management in SQLAlchemyStore."""
    store = SQLAlchemyStore(test_db_url)

    # Create tables
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Before entering context, session should be None
    assert store.session is None

    async with store as s:
        # Inside context, session should be available
        assert s.session is not None

    # After exiting context, the session reference still exists but is closed
    # We can't easily test if it's closed, but we can verify engine is disposed

    await store.engine.dispose()


@pytest.mark.asyncio
async def test_sqlalchemy_store_transaction_commit(sqlalchemy_store):
    """Test that transactions are committed properly."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)

        # Create a new session to verify the commit happened
        async with store.session_factory() as new_session:
            result = await new_session.execute(
                select(Repo).where(Repo.id == test_repo.id)
            )
            saved_repo = result.scalar_one_or_none()

            assert saved_repo is not None
            assert saved_repo.id == test_repo.id


@pytest.mark.asyncio
async def test_sqlalchemy_store_multiple_operations(sqlalchemy_store):
    """Test performing multiple operations in a single session."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="file.txt",
            executable=False,
            contents="content",
        )
    ]

    commit_data = [
        GitCommit(
            repo_id=test_repo_id,
            hash="abc123",
            message="Initial commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            parents=0,
        )
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_file_data(file_data)
        await store.insert_git_commit_data(commit_data)

        # Verify all data was inserted
        repo_result = await store.session.execute(
            select(Repo).where(Repo.id == test_repo_id)
        )
        assert repo_result.scalar_one_or_none() is not None

        file_result = await store.session.execute(
            select(GitFile).where(GitFile.repo_id == test_repo_id)
        )
        assert len(file_result.scalars().all()) == 1

        commit_result = await store.session.execute(
            select(GitCommit).where(GitCommit.repo_id == test_repo_id)
        )
        assert len(commit_result.scalars().all()) == 1


# ClickHouse Tests


@pytest.mark.asyncio
async def test_clickhouse_store_context_manager_initializes_and_creates_tables():
    mock_client = MagicMock()
    mock_client.command = MagicMock()
    mock_client.insert = MagicMock()
    mock_client.query = MagicMock(return_value=MagicMock(result_rows=[]))
    mock_client.close = MagicMock()

    # Mock filesystem operations
    mock_sql_file = MagicMock()
    mock_sql_file.read_text.return_value = "CREATE TABLE test1; CREATE TABLE test2;"

    import sys
    from types import SimpleNamespace

    get_client = MagicMock(return_value=mock_client)
    fake_clickhouse_connect = SimpleNamespace(get_client=get_client)

    with (
        patch.dict(sys.modules, {"clickhouse_connect": fake_clickhouse_connect}),
        patch("dev_health_ops.storage.clickhouse.Path") as MockPath,
    ):
        # Setup the chain: Path(__file__).resolve().parent / "migrations" / "clickhouse"
        mock_file_path = MagicMock()
        mock_resolved_path = MagicMock()
        mock_package_root_path = MagicMock()
        mock_migrations_path = MagicMock()
        mock_clickhouse_path = MagicMock()

        MockPath.return_value = mock_file_path
        mock_file_path.resolve.return_value = mock_resolved_path
        mock_resolved_path.parents = {1: mock_package_root_path}
        mock_package_root_path.__truediv__.return_value = mock_migrations_path
        mock_migrations_path.__truediv__.return_value = mock_clickhouse_path

        mock_clickhouse_path.exists.return_value = True

        # Configure mock_sql_file with necessary attributes
        mock_sql_file.name = "000_test.sql"
        mock_sql_file.suffix = ".sql"
        mock_sql_file.stem = "000_test"

        # Configure glob to return the mock file only for .sql pattern
        def glob_side_effect(pattern):
            if pattern == "*.sql":
                return [mock_sql_file]
            return []

        mock_clickhouse_path.glob.side_effect = glob_side_effect

        store = ClickHouseStore("clickhouse://localhost:8123/default")
        async with store as s:
            assert s.client is mock_client

    get_client.assert_called_once_with(dsn="clickhouse://localhost:8123/default")
    # Expect 4 calls:
    # 1. CREATE TABLE schema_migrations
    # 2. CREATE TABLE test1 (from sql file)
    # 3. CREATE TABLE test2 (from sql file)
    # 4. INSERT INTO schema_migrations (record migration)
    assert mock_client.command.call_count == 4
    mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_clickhouse_store_insert_git_file_data_calls_insert():
    test_repo_id = uuid.uuid4()
    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="file.txt",
            executable=False,
            contents="content",
        )
    ]

    mock_client = MagicMock()
    mock_client.command = MagicMock()
    mock_client.insert = MagicMock()
    mock_client.query = MagicMock(return_value=MagicMock(result_rows=[]))
    mock_client.close = MagicMock()

    import sys
    from types import SimpleNamespace

    get_client = MagicMock(return_value=mock_client)
    fake_clickhouse_connect = SimpleNamespace(get_client=get_client)

    with patch.dict(sys.modules, {"clickhouse_connect": fake_clickhouse_connect}):
        store = ClickHouseStore("clickhouse://localhost:8123/default")
        async with store:
            await store.insert_git_file_data(file_data)

    args, kwargs = mock_client.insert.call_args
    assert args[0] == "git_files"
    assert kwargs["column_names"] == [
        "repo_id",
        "path",
        "executable",
        "contents",
        "last_synced",
    ]


# SQLite-specific Tests


class TestSQLiteAsyncStorage:
    """Test SQLite async storage specific functionality."""

    @pytest.fixture
    def sqlite_memory_url(self):
        """Return a SQLite in-memory database URL for testing."""
        return "sqlite+aiosqlite:///:memory:"

    @pytest.fixture
    def sqlite_file_url(self, tmp_path):
        """Return a SQLite file-based database URL for testing."""
        db_path = tmp_path / "test_mergestat.db"
        return f"sqlite+aiosqlite:///{db_path}"

    @pytest_asyncio.fixture
    async def sqlite_memory_store(self, sqlite_memory_url):
        """Create a SQLAlchemyStore instance with an in-memory SQLite database."""
        store = SQLAlchemyStore(sqlite_memory_url)

        # Create tables
        async with store.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        yield store

        # Cleanup
        async with store.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await store.engine.dispose()

    @pytest_asyncio.fixture
    async def sqlite_file_store(self, sqlite_file_url):
        """Create a SQLAlchemyStore instance with a file-based SQLite database."""
        store = SQLAlchemyStore(sqlite_file_url)

        # Create tables
        async with store.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        yield store

        # Cleanup
        async with store.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await store.engine.dispose()

    @pytest.mark.asyncio
    async def test_sqlite_memory_store_connection_string_detection(
        self, sqlite_memory_url
    ):
        """Test that SQLite connection strings are correctly identified."""
        store = SQLAlchemyStore(sqlite_memory_url)

        # SQLite should not have pool_size configured (uses StaticPool by default)
        engine = store.engine
        assert "sqlite" in str(engine.url).lower()
        await store.engine.dispose()

    @pytest.mark.asyncio
    async def test_sqlite_file_store_connection_string_detection(self, sqlite_file_url):
        """Test that SQLite file-based connection strings are correctly identified."""
        store = SQLAlchemyStore(sqlite_file_url)

        engine = store.engine
        assert "sqlite" in str(engine.url).lower()
        await store.engine.dispose()

    @pytest.mark.asyncio
    async def test_sqlite_memory_store_insert_and_query(self, sqlite_memory_store):
        """Test inserting and querying data with SQLite in-memory database."""
        test_repo = Repo(
            id=uuid.uuid4(),
            repo="https://github.com/test/sqlite-test.git",
            ref="main",
            settings={},
            tags=[],
        )

        async with sqlite_memory_store as store:
            await store.insert_repo(test_repo)

            # Verify the repo was inserted
            result = await store.session.execute(
                select(Repo).where(Repo.id == test_repo.id)
            )
            saved_repo = result.scalar_one_or_none()

            assert saved_repo is not None
            assert saved_repo.id == test_repo.id
            assert saved_repo.repo == "https://github.com/test/sqlite-test.git"

    @pytest.mark.asyncio
    async def test_sqlite_file_store_insert_and_query(self, sqlite_file_store):
        """Test inserting and querying data with SQLite file database."""
        test_repo = Repo(
            id=uuid.uuid4(),
            repo="https://github.com/test/sqlite-file-test.git",
            ref="develop",
            settings={"source": "test"},
            tags=["test"],
        )

        async with sqlite_file_store as store:
            await store.insert_repo(test_repo)

            # Verify the repo was inserted
            result = await store.session.execute(
                select(Repo).where(Repo.id == test_repo.id)
            )
            saved_repo = result.scalar_one_or_none()

            assert saved_repo is not None
            assert saved_repo.id == test_repo.id
            assert saved_repo.repo == "https://github.com/test/sqlite-file-test.git"

    @pytest.mark.asyncio
    async def test_sqlite_store_insert_all_data_types(self, sqlite_memory_store):
        """Test inserting all data types (commits, files, stats, blame) into SQLite."""
        test_repo_id = uuid.uuid4()
        test_repo = Repo(
            id=test_repo_id,
            repo="https://github.com/test/all-types.git",
            ref="main",
            settings={},
            tags=[],
        )

        file_data = [
            GitFile(
                repo_id=test_repo_id,
                path="src/main.py",
                executable=False,
                contents="print('hello')",
            ),
        ]

        commit_data = [
            GitCommit(
                repo_id=test_repo_id,
                hash="abc123def456",
                message="Initial commit",
                author_name="Test Author",
                author_email="test@example.com",
                author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
                committer_name="Test Committer",
                committer_email="committer@example.com",
                committer_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
                parents=0,
            ),
        ]

        commit_stats = [
            GitCommitStat(
                repo_id=test_repo_id,
                commit_hash="abc123def456",
                file_path="src/main.py",
                additions=10,
                deletions=0,
                old_file_mode="100644",
                new_file_mode="100644",
            ),
        ]

        blame_data = [
            GitBlame(
                repo_id=test_repo_id,
                path="src/main.py",
                line_no=1,
                author_email="test@example.com",
                author_name="Test Author",
                author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
                commit_hash="abc123def456",
                line="print('hello')",
            ),
        ]

        async with sqlite_memory_store as store:
            # Insert all data types
            await store.insert_repo(test_repo)
            await store.insert_git_file_data(file_data)
            await store.insert_git_commit_data(commit_data)
            await store.insert_git_commit_stats(commit_stats)
            await store.insert_blame_data(blame_data)

            # Verify all data was inserted
            repo_result = await store.session.execute(
                select(Repo).where(Repo.id == test_repo_id)
            )
            assert repo_result.scalar_one_or_none() is not None

            file_result = await store.session.execute(
                select(GitFile).where(GitFile.repo_id == test_repo_id)
            )
            assert len(file_result.scalars().all()) == 1

            commit_result = await store.session.execute(
                select(GitCommit).where(GitCommit.repo_id == test_repo_id)
            )
            assert len(commit_result.scalars().all()) == 1

            stats_result = await store.session.execute(
                select(GitCommitStat).where(GitCommitStat.repo_id == test_repo_id)
            )
            assert len(stats_result.scalars().all()) == 1

            blame_result = await store.session.execute(
                select(GitBlame).where(GitBlame.repo_id == test_repo_id)
            )
            assert len(blame_result.scalars().all()) == 1

    @pytest.mark.asyncio
    async def test_sqlite_store_no_connection_pooling_params(self, sqlite_memory_url):
        """Test that SQLite stores don't use PostgreSQL-specific pooling params."""
        store = SQLAlchemyStore(sqlite_memory_url)

        # The engine should be created without pool_size, max_overflow params
        # which are PostgreSQL-specific
        engine = store.engine

        # Verify the engine was created successfully
        assert engine is not None

        # SQLite connections should work
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        await store.engine.dispose()

    @pytest.mark.asyncio
    async def test_sqlite_store_multiple_sessions(self, sqlite_file_url):
        """Test that multiple sessions can be created with SQLite file store."""
        store = SQLAlchemyStore(sqlite_file_url)

        # Create tables
        async with store.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        test_repo = Repo(
            id=uuid.uuid4(),
            repo="https://github.com/test/multi-session.git",
            ref="main",
            settings={},
            tags=[],
        )

        # First session - insert data
        async with store as s1:
            await s1.insert_repo(test_repo)

        # Second session - verify data persists
        async with store as s2:
            assert s2.session is not None
            result = await s2.session.execute(
                select(Repo).where(Repo.id == test_repo.id)
            )
            saved_repo = result.scalar_one_or_none()
            assert saved_repo is not None
            assert str(getattr(saved_repo, "id")) == str(test_repo.id)

        # Cleanup
        async with store.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await store.engine.dispose()

    @pytest.mark.asyncio
    async def test_sqlite_store_batch_inserts(self, sqlite_memory_store):
        """Test that batch inserts work correctly with SQLite."""
        test_repo_id = uuid.uuid4()
        test_repo = Repo(
            id=test_repo_id,
            repo="https://github.com/test/batch.git",
            ref="main",
            settings={},
            tags=[],
        )

        async with sqlite_memory_store as store:
            await store.insert_repo(test_repo)

            # Create multiple files to insert in batches
            files = [
                GitFile(
                    repo_id=test_repo_id,
                    path=f"file{i}.txt",
                    executable=False,
                    contents=f"content{i}",
                )
                for i in range(10)
            ]

            # Insert in batches
            await store.insert_git_file_data(files[:5])
            await store.insert_git_file_data(files[5:])

            # Verify all files were inserted
            result = await store.session.execute(
                select(GitFile).where(GitFile.repo_id == test_repo_id)
            )
            saved_files = result.scalars().all()
            assert len(saved_files) == 10
