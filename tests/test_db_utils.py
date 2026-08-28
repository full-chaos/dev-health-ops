import pytest
from sqlalchemy.engine import make_url

from dev_health_ops import db
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


class TestPostgresRuntimeUrlNormalization:
    def test_async_normalizer_converts_plain_postgres_sslmode_to_asyncpg_ssl(self):
        result = db.normalize_async_postgres_uri(
            "postgresql://u:p@h/db?sslmode=require"
        )

        url = make_url(result)
        assert url.drivername == "postgresql+asyncpg"
        assert url.query["ssl"] == "require"
        assert "sslmode" not in url.query

    def test_async_normalizer_removes_channel_binding(self):
        result = db.normalize_async_postgres_uri(
            "postgresql+asyncpg://u:p@h/db?sslmode=require&channel_binding=require"
        )

        url = make_url(result)
        assert url.drivername == "postgresql+asyncpg"
        assert url.query["ssl"] == "require"
        assert "channel_binding" not in url.query

    def test_async_normalizer_preserves_unrelated_query_params(self):
        result = db.normalize_async_postgres_uri(
            "postgresql://u:p@h/db?sslmode=require&application_name=api"
        )

        url = make_url(result)
        assert url.query["ssl"] == "require"
        assert url.query["application_name"] == "api"

    def test_sync_normalizer_converts_asyncpg_ssl_to_libpq_sslmode(self):
        result = db.normalize_sync_postgres_uri(
            "postgresql+asyncpg://u:p@h/db?ssl=require"
        )

        url = make_url(result)
        assert url.drivername == "postgresql"
        assert url.query["sslmode"] == "require"
        assert "ssl" not in url.query

    def test_sync_normalizer_keeps_existing_sslmode_compatible(self):
        result = db.normalize_sync_postgres_uri(
            "postgresql+asyncpg://u:p@h/db?sslmode=require"
        )

        url = make_url(result)
        assert url.drivername == "postgresql"
        assert url.query["sslmode"] == "require"
        assert "ssl" not in url.query

    def test_sync_normalizer_removes_channel_binding(self):
        result = db.normalize_sync_postgres_uri(
            "postgresql+asyncpg://u:p@h/db?sslmode=require&channel_binding=require"
        )

        url = make_url(result)
        assert url.query["sslmode"] == "require"
        assert "channel_binding" not in url.query

    def test_sync_normalizer_preserves_unrelated_query_params(self):
        result = db.normalize_sync_postgres_uri(
            "postgresql+asyncpg://u:p@h/db?ssl=require&application_name=worker"
        )

        url = make_url(result)
        assert url.query["sslmode"] == "require"
        assert url.query["application_name"] == "worker"

    def test_get_sync_postgres_uri_normalizes_env_uri(self, monkeypatch):
        monkeypatch.setenv(
            "POSTGRES_URI",
            "postgresql+asyncpg://u:p@h/db?ssl=require&channel_binding=require",
        )
        monkeypatch.delenv("DATABASE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        result = db._get_sync_postgres_uri()

        assert result is not None
        url = make_url(result)
        assert url.drivername == "postgresql"
        assert url.query["sslmode"] == "require"
        assert "ssl" not in url.query
        assert "channel_binding" not in url.query

    def test_sync_engine_explicit_uri_uses_normalized_dsn(self, monkeypatch):
        captured: dict[str, str] = {}

        def create_engine(dsn: str, **kwargs):
            captured["dsn"] = dsn
            return object()

        monkeypatch.setattr(db, "create_engine", create_engine)

        db.get_postgres_sync_engine(
            "postgresql+asyncpg://u:p@h/db?ssl=require&application_name=worker"
        )

        url = make_url(captured["dsn"])
        assert url.drivername == "postgresql"
        assert url.query["sslmode"] == "require"
        assert url.query["application_name"] == "worker"


class TestRejectsUnresolvedEnvTemplate:
    """CHAOS-4402: a docker-compose-style ``${VAR}`` DSN exported into the
    ambient shell (meant for compose's OWN substitution, never expanded for
    this host process) must fail with a clear message naming the real
    problem, not reach asyncpg verbatim and raise a confusing
    ``InvalidPasswordError`` for a truncated username.
    """

    def test_get_postgres_uri_rejects_unresolved_template_in_postgres_uri(
        self, monkeypatch
    ):
        monkeypatch.setenv(
            "POSTGRES_URI",
            "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/devhealth",
        )
        monkeypatch.delenv("DATABASE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(ValueError, match=r"unresolved template placeholder"):
            db.get_postgres_uri()

    def test_get_postgres_uri_rejects_unresolved_template_in_database_uri_fallback(
        self, monkeypatch
    ):
        monkeypatch.delenv("POSTGRES_URI", raising=False)
        monkeypatch.setenv(
            "DATABASE_URI",
            "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/devhealth",
        )

        with pytest.raises(ValueError, match=r"unresolved template placeholder"):
            db.get_postgres_uri()

    def test_get_postgres_uri_passes_through_a_fully_resolved_uri(self, monkeypatch):
        monkeypatch.setenv(
            "POSTGRES_URI", "postgresql://devhealth:devhealth@localhost:5432/devhealth"
        )
        monkeypatch.delenv("DATABASE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        result = db.get_postgres_uri()

        assert result is not None
        assert "${" not in result

    def test_normalize_async_postgres_uri_rejects_unresolved_template(self):
        with pytest.raises(ValueError, match=r"unresolved template placeholder"):
            db.normalize_async_postgres_uri(
                "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/db"
            )

    @pytest.mark.parametrize(
        "uri",
        [
            "postgresql://${POSTGRES_USER:-postgres}@postgres:5432/db",
            "postgresql://${POSTGRES_USER-postgres}@postgres:5432/db",
            "postgresql://${POSTGRES_USER:=postgres}@postgres:5432/db",
            "postgresql://${POSTGRES_USER:?required}@postgres:5432/db",
            "postgresql://${POSTGRES_USER:+alt}@postgres:5432/db",
        ],
    )
    def test_rejects_compose_parameter_expansion_forms(self, uri):
        """Codex review round 1 (P2): the bare ${VAR} pattern missed
        Compose/POSIX parameter-expansion forms with a default, error, or
        alternate-value operator -- a DSN using any of those still reached
        asyncpg verbatim."""
        with pytest.raises(ValueError, match=r"unresolved template placeholder"):
            db.normalize_async_postgres_uri(uri)
