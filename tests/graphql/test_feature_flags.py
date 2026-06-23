from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.feature_flags import (
    FEATURE_FLAG_EVENT_NOT_MATERIALIZED,
    FEATURE_FLAG_NOT_MATERIALIZED,
    resolve_feature_flag_events,
    resolve_feature_flags,
)
from dev_health_ops.work_graph.ids import generate_feature_flag_id

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")


class MockClient:
    pass


class MissingTableError(Exception):
    code = 60


@pytest.fixture
def mock_context() -> GraphQLContext:
    return GraphQLContext(
        org_id="test-org",
        db_url="clickhouse://localhost:8123/default",
        client=MockClient(),
    )


def _missing_table(table_name: str) -> MissingTableError:
    return MissingTableError(
        f"UNKNOWN_TABLE Unknown table expression identifier '{table_name}'"
    )


@pytest.mark.asyncio
async def test_feature_flags_query_scopes_org_filters_and_orders(
    mock_context: GraphQLContext,
) -> None:
    created_at = datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.return_value = [
            {
                "provider": "launchdarkly",
                "flag_key": "checkout-v2",
                "project_key": "web",
                "environment": "prod",
                "flag_type": "boolean",
                "created_at": created_at,
                "archived_at": None,
            }
        ]

        result = await resolve_feature_flags(
            mock_context,
            provider="launchdarkly",
            project="web",
            include_archived=False,
            limit=25,
        )

    sql = mock_query.call_args[0][1]
    params = mock_query.call_args[0][2]
    assert "FROM feature_flag FINAL" in sql
    assert "WHERE org_id = %(org_id)s" in sql
    assert "provider = %(provider)s" in sql
    assert "project_key = %(project)s" in sql
    assert "archived_at IS NULL" in sql
    assert "ORDER BY provider, project_key, flag_key" in sql
    assert "LIMIT %(limit)s" in sql
    assert params == {
        "org_id": "test-org",
        "provider": "launchdarkly",
        "project": "web",
        "limit": 25,
    }
    assert result.total_count == 1
    assert result.flags[0].flag_id == generate_feature_flag_id(
        "test-org", "launchdarkly", "web", "checkout-v2"
    )
    assert result.flags[0].created_at == created_at.isoformat()
    assert result.flags[0].archived_at is None


@pytest.mark.asyncio
async def test_feature_flags_include_archived_omits_archived_filter(
    mock_context: GraphQLContext,
) -> None:
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.return_value = []

        await resolve_feature_flags(mock_context, include_archived=True)

    sql = mock_query.call_args[0][1]
    params = mock_query.call_args[0][2]
    assert "archived_at IS NULL" not in sql
    assert params == {"org_id": "test-org", "limit": 1000}


@pytest.mark.asyncio
async def test_feature_flags_clamps_out_of_range_limits(
    mock_context: GraphQLContext,
) -> None:
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.return_value = []
        await resolve_feature_flags(mock_context, limit=10_000)
        await resolve_feature_flags(mock_context, limit=-5)

    huge_params = mock_query.call_args_list[0][0][2]
    negative_params = mock_query.call_args_list[1][0][2]
    assert huge_params["limit"] == 1000
    assert negative_params["limit"] == 1


@pytest.mark.asyncio
async def test_feature_flag_events_clamps_out_of_range_limits(
    mock_context: GraphQLContext,
) -> None:
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.return_value = []
        await resolve_feature_flag_events(mock_context, limit=10_000)
        await resolve_feature_flag_events(mock_context, limit=0)

    huge_params = mock_query.call_args_list[0][0][2]
    zero_params = mock_query.call_args_list[1][0][2]
    assert huge_params["limit"] == 1000
    assert zero_params["limit"] == 1


@pytest.mark.asyncio
async def test_feature_flag_events_query_scopes_org_filters_and_orders(
    mock_context: GraphQLContext,
) -> None:
    event_ts = datetime(2026, 6, 1, 10, 1, tzinfo=timezone.utc)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.return_value = [
            {
                "flag_key": "checkout-v2",
                "event_type": "enabled",
                "prev_state": "off",
                "next_state": "on",
                "actor_type": "user",
                "environment": "prod",
                "event_ts": event_ts,
            }
        ]

        result = await resolve_feature_flag_events(
            mock_context,
            flag_key="checkout-v2",
            environment="prod",
            limit=10,
        )

    sql = mock_query.call_args[0][1]
    params = mock_query.call_args[0][2]
    assert "FROM feature_flag_event" in sql
    assert "FROM feature_flag_event FINAL" not in sql
    assert "WHERE org_id = %(org_id)s" in sql
    assert "flag_key = %(flag_key)s" in sql
    assert "environment = %(environment)s" in sql
    assert "provider" not in sql
    assert "project_key" not in sql
    assert "ORDER BY event_ts ASC" in sql
    assert params == {
        "org_id": "test-org",
        "flag_key": "checkout-v2",
        "environment": "prod",
        "limit": 10,
    }
    assert result.total_count == 1
    assert result.events[0].event_ts == event_ts.isoformat()


@pytest.mark.asyncio
async def test_feature_flags_missing_table_degrades_but_other_errors_reraise(
    mock_context: GraphQLContext,
) -> None:
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.side_effect = _missing_table("feature_flag")

        result = await resolve_feature_flags(mock_context)

    assert result.flags == []
    assert result.total_count == 0
    assert result.degraded_reason == FEATURE_FLAG_NOT_MATERIALIZED

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.side_effect = _missing_table("other_table")

        with pytest.raises(MissingTableError):
            await resolve_feature_flags(mock_context)


@pytest.mark.asyncio
async def test_feature_flag_events_missing_table_degrades_but_other_errors_reraise(
    mock_context: GraphQLContext,
) -> None:
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.side_effect = _missing_table("feature_flag_event")

        result = await resolve_feature_flag_events(mock_context)

    assert result.events == []
    assert result.total_count == 0
    assert result.degraded_reason == FEATURE_FLAG_EVENT_NOT_MATERIALIZED

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
    ) as mock_query:
        mock_query.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await resolve_feature_flag_events(mock_context)


@pytest.mark.clickhouse
@pytest.mark.skipif(
    not CLICKHOUSE_URI,
    reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
)
@pytest.mark.asyncio
async def test_feature_flag_registry_and_events_live_clickhouse() -> None:
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)
    org_id = f"test-chaos-2629-{uuid.uuid4()}"
    repo_id = str(uuid.uuid4())
    base_ts = datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc)

    flag_rows: list[list[Any]] = [
        [
            org_id,
            "launchdarkly",
            "checkout-v2",
            "web",
            repo_id,
            "prod",
            "boolean",
            base_ts,
            None,
            base_ts,
        ],
        [
            org_id,
            "gitlab",
            "search-rollout",
            "platform",
            repo_id,
            "prod",
            "percentage",
            base_ts,
            None,
            base_ts,
        ],
    ]
    event_rows: list[list[Any]] = [
        [
            org_id,
            "created",
            "checkout-v2",
            "prod",
            repo_id,
            "system",
            "missing",
            "off",
            base_ts,
            base_ts,
            "evt-1",
            f"{org_id}:evt-1",
        ],
        [
            org_id,
            "enabled",
            "search-rollout",
            "prod",
            repo_id,
            "user",
            "off",
            "on",
            base_ts + timedelta(minutes=1),
            base_ts + timedelta(minutes=1),
            "evt-2",
            f"{org_id}:evt-2",
        ],
        [
            org_id,
            "disabled",
            "checkout-v2",
            "prod",
            repo_id,
            "user",
            "on",
            "off",
            base_ts + timedelta(minutes=2),
            base_ts + timedelta(minutes=2),
            "evt-3",
            f"{org_id}:evt-3",
        ],
    ]

    try:
        sink.client.insert(
            "feature_flag",
            flag_rows,
            column_names=[
                "org_id",
                "provider",
                "flag_key",
                "project_key",
                "repo_id",
                "environment",
                "flag_type",
                "created_at",
                "archived_at",
                "last_synced",
            ],
        )
        sink.client.insert(
            "feature_flag_event",
            event_rows,
            column_names=[
                "org_id",
                "event_type",
                "flag_key",
                "environment",
                "repo_id",
                "actor_type",
                "prev_state",
                "next_state",
                "event_ts",
                "ingested_at",
                "source_event_id",
                "dedupe_key",
            ],
        )

        context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)
        registry = await resolve_feature_flags(context, limit=10)
        events = await resolve_feature_flag_events(context, limit=10)

        assert registry.total_count == 2
        assert {(f.provider, f.flag_key) for f in registry.flags} == {
            ("gitlab", "search-rollout"),
            ("launchdarkly", "checkout-v2"),
        }
        assert [event.flag_key for event in events.events] == [
            "checkout-v2",
            "search-rollout",
            "checkout-v2",
        ]
        assert [event.event_type for event in events.events] == [
            "created",
            "enabled",
            "disabled",
        ]
        assert events.total_count == 3
    finally:
        sink.client.command(
            "ALTER TABLE feature_flag DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
        sink.client.command(
            "ALTER TABLE feature_flag_event DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
        sink.close()
