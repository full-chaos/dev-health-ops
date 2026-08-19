"""Contract and failure-recovery tests for ClickHouse migration 075."""

from __future__ import annotations

import importlib.util
import re
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import TypedDict

import pytest

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)
MIGRATION = "075_feature_flag_environment_identity.py"
LEGACY_KEY = "org_id, provider, project_key, flag_key"
TARGET_KEY = "org_id, provider, project_key, flag_key, environment"


def _load_migration(filename: str = MIGRATION) -> ModuleType:
    path = MIGRATIONS_DIR / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None, f"cannot load {filename}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_075_declares_the_deployed_feature_flag_identity() -> None:
    migration = _load_migration()

    assert callable(getattr(migration, "upgrade", None))
    assert migration.TABLES == {
        "feature_flag": "(org_id, provider, project_key, flag_key, environment)"
    }


def test_default_shadow_names_are_unique_per_invocation() -> None:
    migration = _load_migration()

    names = {migration._shadow_name("feature_flag") for _ in range(8)}

    assert len(names) == 8
    assert all(name.startswith("feature_flag_075_new_") for name in names)


def test_migration_075_runs_after_the_feature_flag_table_exists() -> None:
    assert (MIGRATIONS_DIR / "034_feature_flag_user_impact_tables.sql").exists()
    assert "034_feature_flag_user_impact_tables.sql" < MIGRATION
    assert (MIGRATIONS_DIR / "073_linear_project_lifecycle.sql").exists()
    assert "073_linear_project_lifecycle.sql" < MIGRATION


def test_migration_034_declares_the_legacy_key_that_075_rebuilds() -> None:
    ddl = (MIGRATIONS_DIR / "034_feature_flag_user_impact_tables.sql").read_text(
        encoding="utf-8"
    )
    feature_flag = ddl.split("CREATE TABLE IF NOT EXISTS feature_flag", 1)[1].split(
        "CREATE TABLE IF NOT EXISTS feature_flag_event", 1
    )[0]
    assert re.search(
        r"ORDER BY\s*\(org_id,\s*provider,\s*project_key,\s*flag_key\)",
        feature_flag,
        re.IGNORECASE,
    )


class _Result:
    def __init__(self, rows: list[list[object]]) -> None:
        self.result_rows = rows


class _TableState(TypedDict):
    ddl: str
    sorting_key: str
    rows: list[dict[str, str]]


class _FeatureFlagClient:
    """Small ClickHouse client model for the migration state machine.

    Data is retained per table so the tests can assert that rows for both
    environments survive the snapshot/exchange/catch-up sequence.  It is not
    a substitute for ClickHouse FINAL; the opt-in live test and provider
    integration suite exercise that physical behavior.
    """

    def __init__(
        self,
        *,
        rows: list[dict[str, str]],
        created_sorting_key: str = TARGET_KEY,
        fail_on: str | None = None,
        before_exchange: Callable[[_FeatureFlagClient], None] | None = None,
        after_shadow_copy: Callable[[_FeatureFlagClient], None] | None = None,
        distinct_counts: dict[str, int] | None = None,
        database_engine: str = "Atomic",
    ) -> None:
        old_ddl = (
            "CREATE TABLE feature_flag ("
            "org_id String, provider String, project_key String, flag_key String, "
            "repo_id String, environment String, flag_type String, "
            "created_at DateTime64(3), archived_at Nullable(DateTime64(3)), "
            "last_synced DateTime64(3)) ENGINE = ReplacingMergeTree(last_synced) "
            "ORDER BY (org_id, provider, project_key, flag_key)"
        )
        self.tables: dict[str, _TableState] = {
            "feature_flag": {
                "ddl": old_ddl,
                "sorting_key": LEGACY_KEY,
                "rows": [dict(row) for row in rows],
            }
        }
        self.created_sorting_key = created_sorting_key
        self.fail_on = fail_on
        self.before_exchange = before_exchange
        self.after_shadow_copy = after_shadow_copy
        self.distinct_counts = distinct_counts or {}
        self.database_engine = database_engine
        self.commands: list[str] = []

    def query(self, query: str, parameters: dict[str, str] | None = None) -> _Result:
        if "FROM system.databases" in query:
            return _Result([[self.database_engine]])
        if "name LIKE" in query:
            assert parameters is not None
            prefix = parameters["prefix"].rstrip("%")
            return _Result([[name] for name in self.tables if name.startswith(prefix)])
        if "count() FROM system.tables" in query:
            assert parameters is not None
            return _Result([[1 if parameters["name"] in self.tables else 0]])
        if "sorting_key FROM system.tables" in query:
            assert parameters is not None
            table_state = self.tables.get(parameters["name"])
            return _Result([[table_state["sorting_key"]]] if table_state else [])
        if query.startswith("SHOW CREATE TABLE"):
            name = query.split("`")[1]
            return _Result([[self.tables[name]["ddl"]]])
        if "uniqExact" in query:
            table_name = query.split("FROM `", 1)[1].split("`", 1)[0]
            if table_name in self.distinct_counts:
                return _Result([[self.distinct_counts[table_name]]])
            columns = [
                "org_id",
                "provider",
                "project_key",
                "flag_key",
                "environment",
            ]
            keys = {
                tuple(row[column] for column in columns)
                for row in self.tables[table_name]["rows"]
            }
            return _Result([[len(keys)]])
        raise AssertionError(f"unexpected query: {query}")

    def command(self, command: str) -> None:
        self.commands.append(command)
        if self.fail_on and self.fail_on in command:
            self.fail_on = None
            raise RuntimeError(f"injected failure on: {command}")

        if command.startswith("DROP TABLE"):
            table = command.split("`")[1]
            self.tables.pop(table, None)
            return

        if command.startswith("CREATE TABLE"):
            match = re.search(r"CREATE TABLE\s+`?(\w+)`?", command)
            assert match is not None
            name = match.group(1)
            self.tables[name] = {
                "ddl": command,
                "sorting_key": self.created_sorting_key,
                "rows": [],
            }
            return

        if command.startswith("INSERT INTO"):
            destination = command.split("`")[1]
            source = command.split("FROM `", 1)[1].split("`", 1)[0]
            copied: list[dict[str, str]] = [
                dict(row) for row in self.tables[source]["rows"]
            ]
            self.tables[destination]["rows"].extend(copied)
            if self.after_shadow_copy is not None and destination != "feature_flag":
                callback = self.after_shadow_copy
                self.after_shadow_copy = None
                callback(self)
            return

        if command.startswith("EXCHANGE TABLES"):
            if self.before_exchange is not None:
                self.before_exchange(self)
            names = re.findall(r"`(\w+)`", command)
            assert len(names) == 2
            first, second = names
            self.tables[first], self.tables[second] = (
                self.tables[second],
                self.tables[first],
            )
            return

        raise AssertionError(f"unexpected command: {command}")


def _row(environment: str, *, org_id: str = "org-a") -> dict[str, str]:
    return {
        "org_id": org_id,
        "provider": "gitlab",
        "project_key": "group/project",
        "flag_key": "checkout-v2",
        "environment": environment,
    }


def test_rebuild_preserves_same_flag_in_multiple_environments() -> None:
    migration = _load_migration()
    client = _FeatureFlagClient(rows=[_row("production"), _row("staging")])

    migration._rebuild_table(
        client,
        "feature_flag",
        migration.TABLES["feature_flag"],
        shadow="feature_flag_new",
    )

    assert client.tables["feature_flag"]["sorting_key"] == TARGET_KEY
    assert "feature_flag_new" not in client.tables
    assert {row["environment"] for row in client.tables["feature_flag"]["rows"]} == {
        "production",
        "staging",
    }


def test_rebuild_catches_up_a_row_written_between_snapshot_and_exchange() -> None:
    migration = _load_migration()

    def write_late_environment(client: _FeatureFlagClient) -> None:
        client.tables["feature_flag"]["rows"].append(_row("canary"))

    client = _FeatureFlagClient(
        rows=[_row("production"), _row("staging")],
        before_exchange=write_late_environment,
    )
    migration._rebuild_table(
        client,
        "feature_flag",
        migration.TABLES["feature_flag"],
        shadow="feature_flag_new",
    )

    assert {row["environment"] for row in client.tables["feature_flag"]["rows"]} == {
        "production",
        "staging",
        "canary",
    }
    snapshot = client.commands.index(
        "INSERT INTO `feature_flag_new` SELECT * FROM `feature_flag`"
    )
    exchange = client.commands.index(
        "EXCHANGE TABLES `feature_flag` AND `feature_flag_new`"
    )
    catch_up = client.commands.index(
        "INSERT INTO `feature_flag` SELECT * FROM `feature_flag_new`"
    )
    assert snapshot < exchange < catch_up


def test_rerun_converges_leftover_old_table_after_exchange() -> None:
    migration = _load_migration()
    client = _FeatureFlagClient(rows=[_row("production")])
    migration._rebuild_table(
        client,
        "feature_flag",
        migration.TABLES["feature_flag"],
        shadow="feature_flag_new",
    )

    # Model a crash after EXCHANGE: main has the target key and the old table
    # remains as feature_flag_new.  The second run must catch it up and drop it.
    old_rows = [_row("staging")]
    leftover = "feature_flag_075_new_crashed"
    client.tables[leftover] = {
        "ddl": f"CREATE TABLE {leftover} ...",
        "sorting_key": LEGACY_KEY,
        "rows": old_rows,
    }
    client.tables["feature_flag"]["rows"].append(_row("canary"))
    before = len(client.commands)

    migration._rebuild_table(
        client,
        "feature_flag",
        migration.TABLES["feature_flag"],
        shadow="feature_flag_new",
    )

    assert leftover not in client.tables
    assert {row["environment"] for row in client.tables["feature_flag"]["rows"]} == {
        "production",
        "canary",
        "staging",
    }
    assert client.commands[before:] == [
        f"INSERT INTO `feature_flag` SELECT * FROM `{leftover}`",
        f"DROP TABLE `{leftover}`",
    ]


def test_post_exchange_failure_leaves_shadow_for_rerun() -> None:
    migration = _load_migration()
    client = _FeatureFlagClient(
        rows=[_row("production")],
        fail_on="INSERT INTO `feature_flag` SELECT",
    )

    with pytest.raises(RuntimeError, match="injected failure"):
        migration._rebuild_table(
            client,
            "feature_flag",
            migration.TABLES["feature_flag"],
            shadow="feature_flag_new",
        )

    assert "feature_flag_new" in client.tables
    assert client.tables["feature_flag"]["sorting_key"] == TARGET_KEY


def test_concurrent_interleaving_of_two_unique_shadows_never_restores_legacy_key() -> (
    None
):
    migration = _load_migration()
    client = _FeatureFlagClient(rows=[_row("production"), _row("staging")])

    def run_second_runner(client: _FeatureFlagClient) -> None:
        # The fake invokes this immediately before runner A's exchange. Disable
        # the callback for runner B so this is one deterministic A/B interleave,
        # not recursive simulation.
        client.before_exchange = None
        migration._rebuild_table(
            client,
            "feature_flag",
            migration.TABLES["feature_flag"],
            shadow="feature_flag_075_new_runner_b",
        )

    client.before_exchange = run_second_runner
    migration._rebuild_table(
        client,
        "feature_flag",
        migration.TABLES["feature_flag"],
        shadow="feature_flag_075_new_runner_a",
    )

    assert client.tables["feature_flag"]["sorting_key"] == TARGET_KEY
    assert "feature_flag_075_new_runner_a" not in client.tables
    assert "feature_flag_075_new_runner_b" not in client.tables
    assert sum("EXCHANGE TABLES" in command for command in client.commands) == 2


def test_target_runner_drains_failed_peer_legacy_shadow_before_success() -> None:
    """A target-key runner cannot strand a peer's failed catch-up shadow.

    B snapshots the legacy table first.  A exchanges and then fails before it
    can catch up a canary in A's old-side shadow.  B observes A's target key
    after its snapshot and must drain both its own snapshot and A's recoverable
    legacy shadow before it can return successfully (and let the ledger record
    075).
    """
    migration = _load_migration()
    client = _FeatureFlagClient(rows=[_row("production"), _row("staging")])
    peer_shadow = "feature_flag_075_new_runner_a"
    runner_shadow = "feature_flag_075_new_runner_b"
    client.tables[peer_shadow] = {
        "ddl": f"CREATE TABLE {peer_shadow} ...",
        "sorting_key": TARGET_KEY,
        "rows": [dict(row) for row in client.tables["feature_flag"]["rows"]],
    }

    def exchange_failed_peer(client: _FeatureFlagClient) -> None:
        # A has already snapshotted and exchanges while B is between its copy
        # and source-key preflight.  A's failed catch-up leaves a new canary in
        # the legacy-key old side.
        client.command(f"EXCHANGE TABLES `feature_flag` AND `{peer_shadow}`")
        client.tables[peer_shadow]["rows"].append(_row("canary"))

    client.after_shadow_copy = exchange_failed_peer
    migration._rebuild_table(
        client,
        "feature_flag",
        migration.TABLES["feature_flag"],
        shadow=runner_shadow,
    )

    assert client.tables["feature_flag"]["sorting_key"] == TARGET_KEY
    assert peer_shadow not in client.tables
    assert runner_shadow not in client.tables
    assert {row["environment"] for row in client.tables["feature_flag"]["rows"]} == {
        "production",
        "staging",
        "canary",
    }
    assert not migration._legacy_shadows(client, "feature_flag")


def test_distinct_key_mismatch_aborts_before_exchange() -> None:
    migration = _load_migration()
    client = _FeatureFlagClient(
        rows=[_row("production"), _row("staging")],
        distinct_counts={"feature_flag": 2, "feature_flag_new": 1},
    )

    with pytest.raises(RuntimeError, match="distinct-key mismatch"):
        migration._rebuild_table(
            client,
            "feature_flag",
            migration.TABLES["feature_flag"],
            shadow="feature_flag_new",
        )

    assert "feature_flag_new" not in client.tables
    assert not any("EXCHANGE" in command for command in client.commands)


def test_upgrade_fails_closed_when_database_engine_cannot_exchange_tables() -> None:
    migration = _load_migration()
    client = _FeatureFlagClient(rows=[_row("production")], database_engine="Ordinary")

    with pytest.raises(RuntimeError, match="Atomic or Shared"):
        migration.upgrade(client)

    assert client.commands == []


def test_migration_documents_forward_only_application_rollback_boundary() -> None:
    text = (MIGRATIONS_DIR / MIGRATION).read_text(encoding="utf-8")

    assert "forward-only for application compatibility" in text
    assert "not a safe rollback target" in text


def test_sorting_key_rewrite_fails_closed_before_copy() -> None:
    migration = _load_migration()
    client = _FeatureFlagClient(
        rows=[_row("production")],
        created_sorting_key="org_id, provider, project_key, flag_key",
    )

    with pytest.raises(RuntimeError, match="sorting key mismatch"):
        migration._rebuild_table(
            client,
            "feature_flag",
            migration.TABLES["feature_flag"],
            shadow="feature_flag_new",
        )

    assert "feature_flag_new" not in client.tables
    assert client.tables["feature_flag"]["sorting_key"] == LEGACY_KEY
    assert not any("INSERT INTO" in command for command in client.commands)
    assert not any("EXCHANGE" in command for command in client.commands)


def test_unexpected_source_key_does_not_get_silently_rewritten() -> None:
    migration = _load_migration()
    client = _FeatureFlagClient(rows=[_row("production")])
    client.tables["feature_flag"]["sorting_key"] = "org_id, provider, flag_key"

    with pytest.raises(RuntimeError, match="unexpected source sorting key"):
        migration._rebuild_table(
            client,
            "feature_flag",
            migration.TABLES["feature_flag"],
            shadow="feature_flag_new",
        )

    assert not any("CREATE TABLE" in command for command in client.commands)
