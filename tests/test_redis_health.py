# Mock redis before importing cache backends
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.modules["valkey"] = MagicMock()

from dev_health_ops.api.main import health  # noqa: E402
from dev_health_ops.api.services.cache import (  # noqa: E402
    MemoryBackend,
    RedisBackend,
    TTLCache,
)
from dev_health_ops.api.models.schemas import HealthResponse  # noqa: E402


class TestRedisHealthCheck(unittest.IsolatedAsyncioTestCase):
    def test_memory_backend_status(self):
        backend = MemoryBackend()
        self.assertEqual(backend.status(), "ok")

    def test_redis_backend_status_ok(self):
        with patch("valkey.from_url") as mock_redis:
            mock_client = mock_redis.return_value
            mock_client.ping.return_value = True

            backend = RedisBackend("redis://localhost:6379")
            self.assertEqual(backend.status(), "ok")
            mock_client.ping.assert_called()

    def test_redis_backend_status_down(self):
        with patch("valkey.from_url") as mock_redis:
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

    @patch("dev_health_ops.api.main._check_redis_health", new_callable=AsyncMock)
    @patch("dev_health_ops.api.main._check_postgres_health", new_callable=AsyncMock)
    @patch("dev_health_ops.api.main._check_clickhouse_health", new_callable=AsyncMock)
    async def test_health_endpoint_integration(
        self, mock_ch_check, mock_pg_check, mock_redis_check
    ):
        # Setup mocks
        mock_pg_check.return_value = ("postgres", "ok")
        mock_ch_check.return_value = ("clickhouse", "ok")
        mock_redis_check.return_value = ("redis", "ok")

        # Call health endpoint
        response = await health()
        self.assertIsInstance(response, HealthResponse)
        assert isinstance(response, HealthResponse)

        # Verify services are in the response
        self.assertEqual(response.services["postgres"], "ok")
        self.assertEqual(response.services["clickhouse"], "ok")
        self.assertEqual(response.services["redis"], "ok")
        # Celery is no longer part of /health — checked via /health/workers
        self.assertNotIn("celery", response.services)
        self.assertEqual(response.status, "ok")


if __name__ == "__main__":
    unittest.main()
