import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

# Mock redis before importing cache backends
import sys

sys.modules["redis"] = MagicMock()

from dev_health_ops.api.services.cache import MemoryBackend, RedisBackend, TTLCache  # noqa: E402
from dev_health_ops.api.main import _check_database_service, health  # noqa: E402
from dev_health_ops.metrics.sinks.factory import SinkBackend  # noqa: E402


class TestRedisHealthCheck(unittest.IsolatedAsyncioTestCase):
    def test_memory_backend_status(self):
        backend = MemoryBackend()
        self.assertEqual(backend.status(), "ok")

    def test_redis_backend_status_ok(self):
        with patch("redis.from_url") as mock_redis:
            mock_client = mock_redis.return_value
            mock_client.ping.return_value = True

            backend = RedisBackend("redis://localhost:6379")
            self.assertEqual(backend.status(), "ok")
            mock_client.ping.assert_called()

    def test_redis_backend_status_down(self):
        with patch("redis.from_url") as mock_redis:
            mock_client = mock_redis.return_value
            # Initial connect succeeds
            mock_client.ping.return_value = True
            backend = RedisBackend("redis://localhost:6379")

            # Later ping fails
            mock_client.ping.side_effect = Exception("Connection lost")
            self.assertEqual(backend.status(), "down")

    def test_ttl_cache_status(self):
        mock_backend = MagicMock()
        mock_backend.status.return_value = "ok"
        cache = TTLCache(ttl_seconds=60, backend=mock_backend)
        self.assertEqual(cache.status(), "ok")

    @patch("dev_health_ops.api.main._db_url", return_value="sqlite:///:memory:")
    @patch("dev_health_ops.api.main._check_database_service", new_callable=AsyncMock)
    @patch("dev_health_ops.api.main.HOME_CACHE")
    async def test_health_endpoint_integration(
        self, mock_cache, mock_db_check, mock_db_url
    ):
        # Setup mocks
        mock_db_check.return_value = ("sqlite", "ok")
        mock_cache.status.return_value = "ok"

        # Call health endpoint

        response = await health()

        # Verify redis is in the services list
        self.assertEqual(response.services["sqlite"], "ok")
        self.assertEqual(response.services["redis"], "ok")
        self.assertEqual(response.status, "ok")


class TestDatabaseHealthCheck(unittest.IsolatedAsyncioTestCase):
    async def test_check_database_service_clickhouse_ok(self):
        @asynccontextmanager
        async def _fake_clickhouse_client(_dsn):
            yield MagicMock()

        with patch(
            "dev_health_ops.api.main.detect_backend",
            return_value=SinkBackend.CLICKHOUSE,
        ), patch(
            "dev_health_ops.api.main.clickhouse_client",
            _fake_clickhouse_client,
        ), patch(
            "dev_health_ops.api.main.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = [{"ok": 1}]
            backend, status = await _check_database_service(
                "clickhouse://localhost:8123/default"
            )

        self.assertEqual(backend, "clickhouse")
        self.assertEqual(status, "ok")

    async def test_check_database_service_sqlite_ok(self):
        with patch(
            "dev_health_ops.api.main.detect_backend",
            return_value=SinkBackend.SQLITE,
        ), patch(
            "dev_health_ops.api.main.asyncio.to_thread",
            new_callable=AsyncMock,
        ) as mock_to_thread:
            mock_to_thread.return_value = True
            backend, status = await _check_database_service("sqlite:///:memory:")

        self.assertEqual(backend, "sqlite")
        self.assertEqual(status, "ok")

    async def test_check_database_service_postgres_ok(self):
        with patch(
            "dev_health_ops.api.main.detect_backend",
            return_value=SinkBackend.POSTGRES,
        ), patch(
            "dev_health_ops.api.main.asyncio.to_thread",
            new_callable=AsyncMock,
        ) as mock_to_thread:
            mock_to_thread.return_value = True
            backend, status = await _check_database_service(
                "postgresql://localhost:5432/dev_health"
            )

        self.assertEqual(backend, "postgres")
        self.assertEqual(status, "ok")

    async def test_check_database_service_postgres_asyncpg_ok(self):
        with patch(
            "dev_health_ops.api.main.detect_backend",
            return_value=SinkBackend.POSTGRES,
        ), patch(
            "dev_health_ops.api.main._check_sqlalchemy_health_async",
            new_callable=AsyncMock,
        ) as mock_async_check, patch(
            "dev_health_ops.api.main.asyncio.to_thread",
            new_callable=AsyncMock,
        ) as mock_to_thread:
            mock_async_check.return_value = True
            backend, status = await _check_database_service(
                "postgresql+asyncpg://postgres:postgres@postgres:5432/postgres"
            )

        mock_async_check.assert_called_once()
        mock_to_thread.assert_not_called()
        self.assertEqual(backend, "postgres")
        self.assertEqual(status, "ok")

    async def test_check_database_service_mongo_ok(self):
        with patch(
            "dev_health_ops.api.main.detect_backend",
            return_value=SinkBackend.MONGO,
        ), patch(
            "dev_health_ops.api.main.asyncio.to_thread",
            new_callable=AsyncMock,
        ) as mock_to_thread:
            mock_to_thread.return_value = True
            backend, status = await _check_database_service(
                "mongo://localhost:27017/dev_health"
            )

        self.assertEqual(backend, "mongo")
        self.assertEqual(status, "ok")

    async def test_check_database_service_unknown_backend(self):
        with patch(
            "dev_health_ops.api.main.detect_backend",
            side_effect=ValueError("bad scheme"),
        ):
            backend, status = await _check_database_service("bad://dsn")

        self.assertEqual(backend, "database")
        self.assertEqual(status, "down")


if __name__ == "__main__":
    unittest.main()
