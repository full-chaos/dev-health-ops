from __future__ import annotations

import importlib.util
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

import clickhouse_connect
import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "src/dev_health_ops/migrations/clickhouse/074_feature_flag_environment_identity.py"
)
LEGACY_KEY = "org_id, provider, project_key, flag_key"
TARGET_KEY = "org_id, provider, project_key, flag_key, environment"

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("migration_074_live", MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _database_uri(base_uri: str, database: str) -> str:
    parsed = urlparse(base_uri)
    return urlunparse(parsed._replace(path=f"/{database}"))


def _legacy_rows(org_id: str) -> list[list[object]]:
    timestamp = datetime(2026, 8, 10, 12, tzinfo=timezone.utc)
    return [
        [
            org_id,
            "gitlab",
            "checkout-v2",
            "group/project",
            "repo-1",
            environment,
            "boolean",
            timestamp,
            None,
            timestamp + timedelta(seconds=index),
        ]
        for index, environment in enumerate(("production", "staging"))
    ]


def test_legacy_clickhouse_migration_crash_rerun_and_stale_shadow_cleanup() -> None:
    assert CLICKHOUSE_URI is not None
    migration = _load_migration()
    database = f"chaos_3703_074_legacy_{uuid4().hex}"
    isolated_uri = _database_uri(CLICKHOUSE_URI, database)
    admin_uri = os.environ.get(
        "CLICKHOUSE_ADMIN_URI", _database_uri(CLICKHOUSE_URI, "default")
    )
    admin = clickhouse_connect.get_client(dsn=admin_uri)
    admin.command(f"CREATE DATABASE `{database}`")
    client = clickhouse_connect.get_client(dsn=isolated_uri)
    org_id = f"chaos-3703-legacy-{uuid4()}"
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
        client.command(
            "CREATE TABLE feature_flag ("
            "org_id String, provider String, flag_key String, project_key String, "
            "repo_id String, environment String, flag_type String, "
            "created_at DateTime64(3, 'UTC'), "
            "archived_at Nullable(DateTime64(3, 'UTC')), "
            "last_synced DateTime64(3, 'UTC')"
            ") ENGINE = ReplacingMergeTree(last_synced) "
            "ORDER BY (org_id, provider, project_key, flag_key) "
            "SETTINGS optimize_on_insert = 0"
        )
        client.command("SYSTEM STOP MERGES feature_flag")
        for row in _legacy_rows(org_id):
            client.insert("feature_flag", [row], column_names=columns)
        assert client.query(
            "SELECT count() FROM `feature_flag` WHERE org_id = {org_id:String}",
            parameters={"org_id": org_id},
        ).result_rows == [(2,)]
        assert (
            migration._normalize_sorting_key(
                migration._sorting_key(client, "feature_flag")
            )
            == LEGACY_KEY
        )

        shadow = "feature_flag_074_new_live_crash"
        original_command = client.command
        failed = False

        def fail_after_exchange(command: str, *args: Any, **kwargs: Any) -> Any:
            nonlocal failed
            if (
                not failed
                and command == f"INSERT INTO `feature_flag` SELECT * FROM `{shadow}`"
            ):
                failed = True
                raise RuntimeError("injected live post-exchange crash")
            return original_command(command, *args, **kwargs)

        with (
            patch.object(client, "command", side_effect=fail_after_exchange),
            pytest.raises(RuntimeError, match="injected live post-exchange crash"),
        ):
            migration._rebuild_table(
                client, "feature_flag", migration.TABLES["feature_flag"], shadow=shadow
            )

        assert (
            migration._normalize_sorting_key(
                migration._sorting_key(client, "feature_flag")
            )
            == TARGET_KEY
        )
        assert migration._table_exists(client, shadow)
        assert client.query(
            f"SELECT count() FROM `{shadow}` WHERE org_id = {{org_id:String}}",
            parameters={"org_id": org_id},
        ).result_rows == [(2,)]

        migration._rebuild_table(
            client,
            "feature_flag",
            migration.TABLES["feature_flag"],
            shadow="feature_flag_074_new_live_retry",
        )
        assert not migration._shadow_tables(client, "feature_flag")
        assert client.query(
            "SELECT environment FROM `feature_flag` FINAL "
            "WHERE org_id = {org_id:String} ORDER BY environment",
            parameters={"org_id": org_id},
        ).result_rows == [("production",), ("staging",)]

        stale_target = "feature_flag_074_new_live_target"
        ddl = str(client.query("SHOW CREATE TABLE `feature_flag`").result_rows[0][0])
        client.command(migration._replace_table_name(ddl, "feature_flag", stale_target))
        client.command(f"INSERT INTO `{stale_target}` SELECT * FROM `feature_flag`")
        before = client.query(
            "SELECT count() FROM `feature_flag` FINAL WHERE org_id = {org_id:String}",
            parameters={"org_id": org_id},
        ).result_rows[0][0]
        migration._rebuild_table(
            client,
            "feature_flag",
            migration.TABLES["feature_flag"],
            shadow="feature_flag_074_new_live_final",
        )
        after = client.query(
            "SELECT count() FROM `feature_flag` FINAL WHERE org_id = {org_id:String}",
            parameters={"org_id": org_id},
        ).result_rows[0][0]

        assert before == after == 2
        assert not migration._table_exists(client, stale_target)
        assert client.query(
            "SELECT environment FROM `feature_flag` FINAL "
            "WHERE org_id = {org_id:String} ORDER BY environment",
            parameters={"org_id": org_id},
        ).result_rows == [("production",), ("staging",)]
    finally:
        client.close()
        admin.command(f"DROP DATABASE IF EXISTS `{database}`")
        admin.close()
