import unittest
from unittest.mock import MagicMock, patch

# Mock redis before importing cache backends
import sys

sys.modules["redis"] = MagicMock()

from api.services.cache import MemoryBackend, RedisBackend, TTLCache  # noqa: E402
from api.main import health  # noqa: E402


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

    @patch("api.main.clickhouse_client")
    @patch("api.main.HOME_CACHE")
    @patch("api.main.query_dicts")
    async def test_health_endpoint_integration(self, mock_query, mock_cache, mock_ch):
        # Setup mocks
        mock_ch.return_value.__aenter__.return_value = MagicMock()
        mock_query.return_value = [{"ok": 1}]
        mock_cache.status.return_value = "ok"

        # Call health endpoint

        response = await health()

        # Verify redis is in the services list
        self.assertEqual(response.services["redis"], "ok")
        self.assertEqual(response.status, "ok")


if __name__ == "__main__":
    unittest.main()
