from dev_health_ops.metrics.db_utils import (
    normalize_db_url,
    normalize_postgres_url,
    normalize_sqlite_url,
)


class TestNormalizeSqliteUrl:
    def test_converts_aiosqlite_to_sqlite(self):
        url = "sqlite+aiosqlite:///data.db"
        assert normalize_sqlite_url(url) == "sqlite:///data.db"

    def test_preserves_sync_sqlite_url(self):
        url = "sqlite:///data.db"
        assert normalize_sqlite_url(url) == "sqlite:///data.db"

    def test_preserves_in_memory_url(self):
        url = "sqlite+aiosqlite:///:memory:"
        assert normalize_sqlite_url(url) == "sqlite:///:memory:"

    def test_preserves_absolute_path(self):
        url = "sqlite+aiosqlite:////absolute/path/data.db"
        assert normalize_sqlite_url(url) == "sqlite:////absolute/path/data.db"

    def test_preserves_non_sqlite_url(self):
        url = "postgresql://user:pass@localhost/db"
        assert normalize_sqlite_url(url) == url


class TestNormalizePostgresUrl:
    def test_converts_asyncpg_to_sync(self):
        url = "postgresql+asyncpg://user:pass@localhost:5432/db"
        assert normalize_postgres_url(url) == "postgresql://user:pass@localhost:5432/db"

    def test_preserves_sync_postgres_url(self):
        url = "postgresql://user:pass@localhost/db"
        assert normalize_postgres_url(url) == "postgresql://user:pass@localhost/db"

    def test_preserves_psycopg2_url(self):
        url = "postgresql+psycopg2://user:pass@localhost/db"
        assert normalize_postgres_url(url) == url

    def test_preserves_non_postgres_url(self):
        url = "sqlite:///data.db"
        assert normalize_postgres_url(url) == url


class TestNormalizeDbUrl:
    def test_normalizes_sqlite_url(self):
        url = "sqlite+aiosqlite:///data.db"
        assert normalize_db_url(url) == "sqlite:///data.db"

    def test_normalizes_postgres_url(self):
        url = "postgresql+asyncpg://user:pass@host/db"
        assert normalize_db_url(url) == "postgresql://user:pass@host/db"

    def test_preserves_clickhouse_url(self):
        url = "clickhouse://localhost:8123/default"
        assert normalize_db_url(url) == url

    def test_preserves_mongodb_url(self):
        url = "mongodb://localhost:27017/stats"
        assert normalize_db_url(url) == url

    def test_only_replaces_first_occurrence(self):
        url = "sqlite+aiosqlite:///path/sqlite+aiosqlite/data.db"
        result = normalize_db_url(url)
        assert result == "sqlite:///path/sqlite+aiosqlite/data.db"
        assert result.count("sqlite+aiosqlite") == 1
