"""Real ClickHouse proof for the environment-aware feature-flag registry."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.feature_flags import resolve_feature_flags
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.work_graph.ids import generate_feature_flag_id

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


@pytest.mark.asyncio
async def test_registry_aggregates_two_physical_environments_to_one_logical_flag() -> (
    None
):
    assert CLICKHOUSE_URI is not None
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-3703-registry-{uuid.uuid4()}"
    project_key = f"project-{uuid.uuid4()}"
    flag_key = "checkout-v2"
    second_flag_key = "dark-mode"
    older = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    # Equal last_synced values intentionally exercise the deterministic
    # environment tie-break: staging sorts after production and must win.
    rows = [
        [
            org_id,
            "launchdarkly",
            flag_key,
            project_key,
            "repo-1",
            "production",
            "json",
            older,
            older + timedelta(hours=1),
            older,
        ],
        [
            org_id,
            "launchdarkly",
            flag_key,
            project_key,
            "repo-1",
            "staging",
            "boolean",
            older + timedelta(minutes=1),
            None,
            older,
        ],
        [
            org_id,
            "launchdarkly",
            second_flag_key,
            project_key,
            "repo-1",
            "production",
            "boolean",
            older,
            None,
            older,
        ],
    ]
    columns = [
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
    ]
    try:
        sink.client.insert("feature_flag", rows, column_names=columns)
        sink.client.command("OPTIMIZE TABLE feature_flag FINAL")

        result = await resolve_feature_flags(
            GraphQLContext(
                org_id=org_id,
                db_url=CLICKHOUSE_URI,
                client=sink.client,
            ),
            provider="launchdarkly",
            project=project_key,
            include_archived=True,
        )

        assert result.total_count == 2
        assert len(result.flags) == 2
        assert {flag.flag_id for flag in result.flags} == {
            generate_feature_flag_id(org_id, "launchdarkly", project_key, flag_key),
            generate_feature_flag_id(
                org_id, "launchdarkly", project_key, second_flag_key
            ),
        }
        flag = next(item for item in result.flags if item.flag_key == flag_key)
        assert flag.flag_id == generate_feature_flag_id(
            org_id, "launchdarkly", project_key, flag_key
        )
        assert flag.flag_type == "boolean"
        assert (
            flag.created_at
            == (older + timedelta(minutes=1)).replace(tzinfo=None).isoformat()
        )
        assert flag.archived_at is None
        assert not hasattr(flag, "environment")
    finally:
        sink.client.command(
            "ALTER TABLE feature_flag DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()
