from __future__ import annotations

import pytest

from dev_health_ops.metrics.sinks.clickhouse.connection import (
    ClickHouseConnectionUriError,
    clickhouse_client_kwargs,
)

AUTHENTICATED_HTTPS_DSN = "clickhouse+https://reader%40service:secret%2Fword@clickhouse.example:8443/analytics"
EXPECTED_CLIENT_KWARGS = {
    "host": "clickhouse.example",
    "port": 8443,
    "username": "reader@service",
    "password": "secret/word",
    "database": "analytics",
    "interface": "https",
    "secure": True,
    "settings": {"max_query_size": 1024 * 1024},
}


def test_clickhouse_client_kwargs_decodes_uri_and_explicitly_sets_tls() -> None:
    # Given: an authenticated ClickHouse HTTPS URI with encoded userinfo.
    # When: the connection kwargs are built.
    # Then: clickhouse-connect receives decoded, explicit connection fields.
    assert (
        clickhouse_client_kwargs(
            AUTHENTICATED_HTTPS_DSN,
            settings={"max_query_size": 1024 * 1024},
        )
        == EXPECTED_CLIENT_KWARGS
    )


@pytest.mark.parametrize(
    ("dsn", "port"),
    [
        ("clickhouse://clickhouse.example:443/analytics", 443),
        ("clickhouse://clickhouse.example:8443/analytics", 8443),
        ("clickhouse://clickhouse.example:8123/analytics?secure=true", 8123),
    ],
)
def test_clickhouse_client_kwargs_preserves_tls_uri_variants(
    dsn: str, port: int
) -> None:
    # Given: a generic URI that selects TLS by port or secure query option.
    # When: the connection kwargs are built.
    # Then: the driver receives explicit HTTPS settings.
    kwargs = clickhouse_client_kwargs(dsn, settings={})

    assert kwargs["port"] == port
    assert kwargs["interface"] == "https"
    assert kwargs["secure"] is True


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://reader:secret@clickhouse.example/analytics",
        "clickhouse://reader:secret@/analytics",
        "clickhouse://reader:secret@clickhouse.example/analytics/extra",
    ],
)
def test_clickhouse_client_kwargs_rejects_invalid_connection_uris(dsn: str) -> None:
    # Given: a malformed or unsupported connection URI.
    # When: the connection kwargs are built.
    # Then: a typed error excludes raw credentials from its message.
    with pytest.raises(ClickHouseConnectionUriError) as exc_info:
        clickhouse_client_kwargs(dsn, settings={})

    assert "secret" not in str(exc_info.value)
    assert dsn not in str(exc_info.value)
