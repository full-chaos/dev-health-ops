from __future__ import annotations

from enum import Enum
from urllib.parse import urlparse


class SinkBackend(str, Enum):
    CLICKHOUSE = "clickhouse"
    MONGO = "mongo"
    SQLITE = "sqlite"
    POSTGRES = "postgres"


def detect_backend(dsn: str) -> SinkBackend:
    parsed = urlparse(dsn)
    scheme = parsed.scheme.lower()

    if scheme in (
        "clickhouse",
        "clickhouse+native",
        "clickhouse+http",
        "clickhouse+https",
    ):
        return SinkBackend.CLICKHOUSE
    elif scheme in ("mongodb", "mongodb+srv", "mongo"):
        return SinkBackend.MONGO
    elif scheme in ("sqlite", "sqlite+aiosqlite"):
        return SinkBackend.SQLITE
    elif scheme in (
        "postgresql",
        "postgresql+asyncpg",
        "postgresql+psycopg2",
        "postgres",
    ):
        return SinkBackend.POSTGRES
    else:
        raise ValueError(
            f"Unknown sink scheme '{scheme}'. Supported: "
            "clickhouse, mongo/mongodb, sqlite, postgres/postgresql"
        )
