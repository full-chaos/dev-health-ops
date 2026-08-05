from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import anyio
import pytest

from dev_health_ops.api.queries import client as query_client
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

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


def test_clickhouse_core_passes_explicit_authenticated_client_kwargs() -> None:
    # Given: an authenticated HTTPS ClickHouse URI.
    # When: the core sink constructs its client.
    # Then: the driver receives explicit, decoded connection kwargs.
    raw_client = MagicMock()
    with patch(
        "clickhouse_connect.get_client",
        side_effect=lambda **kwargs: _client_without_dsn(raw_client, kwargs),
    ) as get_client:
        sink = ClickHouseMetricsSink(AUTHENTICATED_HTTPS_DSN)

    assert sink.client is raw_client
    get_client.assert_called_once_with(**EXPECTED_CLIENT_KWARGS)


def test_thread_query_passes_explicit_authenticated_client_kwargs() -> None:
    # Given: a sink retaining an authenticated HTTPS ClickHouse URI.
    # When: an API query creates its thread-local client.
    # Then: the driver receives explicit, decoded connection kwargs.
    result = MagicMock(column_names=["answer"], result_rows=[(42,)])
    raw_client = MagicMock()
    raw_client.query.return_value = result
    sink = MagicMock(dsn=AUTHENTICATED_HTTPS_DSN)

    with patch(
        "clickhouse_connect.get_client",
        side_effect=lambda **kwargs: _client_without_dsn(raw_client, kwargs),
    ) as get_client:
        rows = anyio.run(query_client.query_dicts, sink, "SELECT 42 AS answer", {})

    assert rows == [{"answer": 42}]
    get_client.assert_called_once_with(**EXPECTED_CLIENT_KWARGS)
    raw_client.close.assert_called_once_with()


def test_global_sink_log_redacts_clickhouse_userinfo(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Given: a newly initialized global sink using authenticated userinfo.
    # When: the sink is created.
    # Then: an operational log is present without the URI or password.
    sink = MagicMock()
    with (
        patch.object(query_client, "_SHARED_SINK", None),
        patch.object(query_client, "_SHARED_DSN", None),
        patch.object(query_client, "create_sink", return_value=sink),
        caplog.at_level(logging.INFO, logger="dev_health_ops.api.queries.client"),
    ):
        assert anyio.run(query_client.get_global_sink, AUTHENTICATED_HTTPS_DSN) is sink

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "Initializing global metrics sink" in messages
    assert AUTHENTICATED_HTTPS_DSN not in messages
    assert "reader@service" not in messages
    assert "secret/word" not in messages


def _client_without_dsn(client: MagicMock, kwargs: dict[str, object]) -> MagicMock:
    assert "dsn" not in kwargs, "raw DSN passed to driver"
    return client
