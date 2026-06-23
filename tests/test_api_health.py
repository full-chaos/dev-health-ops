import pytest
from sqlalchemy.engine import make_url

from dev_health_ops.api import _health


@pytest.mark.asyncio
async def test_postgres_health_uses_normalized_async_uri(monkeypatch):
    captured: dict[str, str] = {}

    async def check(dsn: str) -> bool:
        captured["dsn"] = dsn
        return True

    monkeypatch.setenv(
        "POSTGRES_URI",
        "postgresql+asyncpg://u:p@ep-cool-boat-af967qaf-pooler.c-2.us-west-2.aws.neon.tech/db?sslmode=require&channel_binding=require",
    )
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(_health, "_check_sqlalchemy_health_async", check)

    service, status = await _health._check_postgres_health()

    assert service == "postgres"
    assert status == "ok"
    url = make_url(captured["dsn"])
    assert url.drivername == "postgresql+asyncpg"
    assert url.query["ssl"] == "require"
    assert "sslmode" not in url.query
    assert "channel_binding" not in url.query
