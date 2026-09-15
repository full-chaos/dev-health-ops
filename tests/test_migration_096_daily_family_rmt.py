"""Migration 096: daily-family output tables -> ReplacingMergeTree(computed_at).

Driven against a fake client, so no database is needed. The fake derives each
table's engine, version column and sorting key from the DDL the migration
itself generated, so a wrong rewrite shows up as a wrong key, not as a string
the test happened to expect.

Properties pinned:

  READER KEY   each table is sorted by the key its readers deduplicate on,
               never narrower (a narrower key merges rows readers keep apart);
  FAIL CLOSED  an unexpected live key, a wrong shadow, a lost key or a 0 == 0
               match with rows present all abort before the swap;
  REDRIVE      a crash after the swap converges on rerun; a held lock waits;
  PROJECTION   a projection-carrying table gets the rebuild dedup mode, and a
               projection's own ORDER BY is never rewritten.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest

MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
    / "096_daily_family_tables_replacing_merge_tree.py"
)


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(MIGRATION.stem, MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def migration() -> ModuleType:
    return _load()


def _ddl(
    table: str,
    key: str,
    *,
    engine: str = "MergeTree",
    projection: str = "",
    settings: str = "SETTINGS index_granularity = 8192",
) -> str:
    return (
        f"CREATE TABLE default.{table}\n(\n    `org_id` String,\n    `day` Date,\n"
        f"    `computed_at` DateTime('UTC'){projection}\n)\n"
        f"ENGINE = {engine}\nPARTITION BY toYYYYMM(day)\nORDER BY ({key})\n{settings}"
    )


class _Result:
    def __init__(self, rows: list[list]) -> None:
        self.result_rows = rows


class FakeClient:
    def __init__(
        self,
        tables: dict[str, str],
        *,
        projections: dict[str, list[str]] | None = None,
        distinct: dict[str, int] | None = None,
        rows: dict[str, int] | None = None,
        shadow_key_override: str | None = None,
        fail_on: str | None = None,
    ) -> None:
        self.tables = dict(tables)
        self.projections = projections or {}
        self.distinct = distinct or {}
        self.rows = rows or {}
        self.shadow_key_override = shadow_key_override
        self.fail_on = fail_on
        self.commands: list[str] = []
        self.distinct_queries: list[str] = []

    def _key(self, name: str) -> str:
        if name.endswith("_new") and self.shadow_key_override is not None:
            return self.shadow_key_override
        ddl = self.tables[name]
        tail = ddl[ddl.index("ENGINE") :]
        match = re.search(r"ORDER BY \(([^)]*)\)", tail)
        assert match is not None
        return match.group(1)

    def query(self, query: str, parameters: dict | None = None) -> _Result:
        name = (parameters or {}).get("name") or (parameters or {}).get("t")
        if "count() FROM system.tables" in query:
            return _Result([[1 if name in self.tables else 0]])
        if "engine FROM system.tables" in query:
            ddl = self.tables.get(name, "")
            engine = re.search(r"ENGINE = (\w+)", ddl)
            return _Result([[engine.group(1)]] if engine else [])
        if "engine_full FROM system.tables" in query:
            match = re.search(r"ENGINE = (\S+)", self.tables.get(name, ""))
            return _Result([[match.group(1)]] if match else [])
        if "sorting_key FROM system.tables" in query:
            return _Result([[self._key(name)]] if name in self.tables else [])
        if "FROM system.columns" in query:
            return _Result([[1]])
        if "FROM system.projections" in query:
            table = (parameters or {})["table"]
            return _Result([[p] for p in self.projections.get(table, [])])
        if query.startswith("SHOW CREATE TABLE"):
            return _Result([[self.tables[query.split("`")[1]]]])
        if "uniqExact" in query:
            self.distinct_queries.append(query)
            table = query.split("FROM `")[1].rstrip("`")
            return _Result([[self.distinct.get(table, 5)]])
        if query.startswith("SELECT count() FROM `"):
            return _Result([[self.rows.get(query.split("`")[1], 5)]])
        raise AssertionError(f"unexpected query: {query}")

    def command(self, command: str) -> None:
        if self.fail_on and command.startswith(self.fail_on):
            raise RuntimeError(f"injected failure: {command}")
        self.commands.append(command)
        if command.startswith("DROP TABLE"):
            self.tables.pop(command.split("`")[1], None)
        elif command.startswith("CREATE TABLE"):
            match = re.search(r"CREATE TABLE (?:\w+\.)?`?(\w+)`?", command)
            assert match is not None
            if match.group(1) in self.tables:
                raise RuntimeError("Code: 57. TABLE_ALREADY_EXISTS")
            self.tables[match.group(1)] = command
        elif command.startswith("EXCHANGE TABLES"):
            a, b = re.findall(r"`(\w+)`", command)
            self.tables[a], self.tables[b] = self.tables[b], self.tables[a]


# ---------------------------------------------------------------------------
# Reader key
# ---------------------------------------------------------------------------


def test_the_nineteen_daily_family_tables_are_converted(migration) -> None:
    assert set(migration.TABLES) == {
        "repo_metrics_daily",
        "user_metrics_daily",
        "commit_metrics",
        "team_metrics_daily",
        "file_metrics_daily",
        "file_hotspot_daily",
        "review_edges_daily",
        "cicd_metrics_daily",
        "deploy_metrics_daily",
        "incident_metrics_daily",
        "work_item_state_durations_daily",
        "team_cognitive_load_daily",
        "compounding_risk_daily",
        "testops_pipeline_metrics_daily",
        "testops_test_metrics_daily",
        "testops_coverage_metrics_daily",
        "testops_release_confidence",
        "testops_quality_drag",
        "testops_pipeline_stability",
    }


def test_target_keys_are_the_registered_reader_keys(migration) -> None:
    from dev_health_ops.api.queries.metrics import _DEDUP_BY_COMPUTED_AT
    from dev_health_ops.clickhouse_dedup import _APPEND_ONLY_DAILY_KEYS

    registered = set(migration.TARGET_SORT_KEYS) & set(_APPEND_ONLY_DAILY_KEYS)
    assert len(registered) == 16
    for table in registered:
        assert migration.TARGET_SORT_KEYS[table] == _APPEND_ONLY_DAILY_KEYS[table]

    durations = "work_item_state_durations_daily"
    assert set(migration.TARGET_SORT_KEYS[durations]) == {
        "org_id",
        *_DEDUP_BY_COMPUTED_AT[durations],
    }
    # Readers of these two group by exactly this key before their argMax.
    assert migration.TARGET_SORT_KEYS["team_cognitive_load_daily"] == (
        "org_id",
        "team_id",
        "day",
    )
    assert migration.TARGET_SORT_KEYS["compounding_risk_daily"] == (
        "org_id",
        "scope",
        "scope_id",
        "day",
    )


def test_only_the_three_mismatched_key_shapes_are_rewritten(migration) -> None:
    target, legacy = migration.TARGET_SORT_KEYS, migration.LEGACY_SORT_KEYS
    testops = {t for t in target if t.startswith("testops_")}
    assert set(legacy) == {"team_metrics_daily", "compounding_risk_daily", *testops}
    assert legacy["team_metrics_daily"] == tuple(
        c for c in target["team_metrics_daily"] if c != "repo_id"
    )
    assert legacy["compounding_risk_daily"] == (
        *target["compounding_risk_daily"],
        "computed_at",
    )
    for table in testops:
        assert legacy[table] == tuple(c for c in target[table] if c != "org_id")


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------


def test_convert_rewrites_engine_and_key_then_swaps_and_catches_up(migration) -> None:
    table, shadow = "team_metrics_daily", "team_metrics_daily_new"
    client = FakeClient({table: _ddl(table, "org_id, team_id, day")})

    migration._rebuild_table(client, table)

    assert "ReplacingMergeTree(computed_at)" in client.tables[table]
    assert client._key(table) == "org_id, team_id, repo_id, day"
    assert shadow not in client.tables
    assert f"{table}_096_lock" not in client.tables
    order = [
        client.commands.index(f"INSERT INTO `{shadow}` SELECT * FROM `{table}`"),
        client.commands.index(f"EXCHANGE TABLES `{table}` AND `{shadow}`"),
        client.commands.index(f"INSERT INTO `{table}` SELECT * FROM `{shadow}`"),
        client.commands.index(f"DROP TABLE `{shadow}`"),
    ]
    assert order == sorted(order)
    # Keys are verified on the reader key, not the narrower legacy key.
    assert all("`repo_id`" in q for q in client.distinct_queries)


def test_convert_keeps_a_key_that_is_already_the_reader_key(migration) -> None:
    table = "repo_metrics_daily"
    client = FakeClient({table: _ddl(table, "org_id, repo_id, day")})
    migration._rebuild_table(client, table)
    assert client._key(table) == "org_id, repo_id, day"
    assert "ReplacingMergeTree(computed_at)" in client.tables[table]


def test_compounding_risk_loses_computed_at_from_its_key(migration) -> None:
    table = "compounding_risk_daily"
    client = FakeClient(
        {table: _ddl(table, "org_id, scope, scope_id, day, computed_at")}
    )
    migration._rebuild_table(client, table)
    assert client._key(table) == "org_id, scope, scope_id, day"


def test_an_unexpected_live_key_is_refused_before_any_shadow(migration) -> None:
    table = "repo_metrics_daily"
    client = FakeClient({table: _ddl(table, "repo_id, day")})
    with pytest.raises(RuntimeError, match="neither the expected legacy key"):
        migration._rebuild_table(client, table)
    assert not any(
        c.startswith(f"CREATE TABLE default.{table}_new") for c in client.commands
    )
    assert f"{table}_096_lock" not in client.tables


def test_a_shadow_with_the_wrong_key_is_dropped_and_never_swapped(migration) -> None:
    table = "team_metrics_daily"
    client = FakeClient(
        {table: _ddl(table, "org_id, team_id, day")},
        shadow_key_override="org_id, team_id, day",
    )
    with pytest.raises(RuntimeError, match="shadow sorted by"):
        migration._rebuild_table(client, table)
    assert not any(c.startswith("EXCHANGE") for c in client.commands)
    assert f"{table}_new" not in client.tables
    assert "ReplacingMergeTree" not in client.tables[table]


def test_a_lost_reader_key_refuses_the_swap(migration) -> None:
    table = "cicd_metrics_daily"
    client = FakeClient(
        {table: _ddl(table, "org_id, repo_id, day")},
        distinct={table: 7, f"{table}_new": 6},
    )
    with pytest.raises(RuntimeError, match="distinct reader keys differ"):
        migration._rebuild_table(client, table)
    assert not any(c.startswith("EXCHANGE") for c in client.commands)
    assert f"{table}_new" not in client.tables


def test_zero_keys_with_rows_present_refuses_the_swap(migration) -> None:
    table = "deploy_metrics_daily"
    client = FakeClient(
        {table: _ddl(table, "org_id, repo_id, day")},
        distinct={table: 0, f"{table}_new": 0},
        rows={table: 240},
    )
    with pytest.raises(RuntimeError, match="returned 0 for a table that holds rows"):
        migration._rebuild_table(client, table)
    assert not any(c.startswith("EXCHANGE") for c in client.commands)


def test_a_genuinely_empty_table_still_converts(migration) -> None:
    table = "incident_metrics_daily"
    client = FakeClient(
        {table: _ddl(table, "org_id, repo_id, day")},
        distinct={table: 0, f"{table}_new": 0},
        rows={table: 0},
    )
    migration._rebuild_table(client, table)
    assert "ReplacingMergeTree(computed_at)" in client.tables[table]


# ---------------------------------------------------------------------------
# Projections and DDL shapes
# ---------------------------------------------------------------------------


def test_a_projection_table_gets_the_rebuild_dedup_mode(migration) -> None:
    table = "file_hotspot_daily"
    projection = (
        ",\n    PROJECTION prj_acr_file_hotspot_runs\n    (\n        SELECT org_id, "
        "repo_id, day, computed_at\n        GROUP BY org_id, repo_id, day, computed_at\n    )"
    )
    client = FakeClient(
        {table: _ddl(table, "org_id, repo_id, day, file_path", projection=projection)},
        projections={table: ["prj_acr_file_hotspot_runs"]},
    )
    migration._rebuild_table(client, table)
    assert (
        "SETTINGS deduplicate_merge_projection_mode = 'rebuild', index_granularity"
        in client.tables[table]
    )
    assert "PROJECTION prj_acr_file_hotspot_runs" in client.tables[table]


def test_a_table_without_projections_gets_no_projection_setting(migration) -> None:
    table = "file_metrics_daily"
    client = FakeClient({table: _ddl(table, "org_id, repo_id, day, path")})
    migration._rebuild_table(client, table)
    assert "deduplicate_merge_projection_mode" not in client.tables[table]


def test_a_projection_order_by_is_never_rewritten(migration) -> None:
    ddl = _ddl(
        "file_hotspot_daily",
        "org_id, repo_id, day, file_path",
        projection=",\n    PROJECTION p\n    (\n        SELECT * ORDER BY (file_path)\n    )",
    )
    rewritten = migration._rewrite_engine_and_key(
        ddl, ("org_id", "repo_id", "day", "file_path")
    )
    assert "SELECT * ORDER BY (file_path)" in rewritten
    assert rewritten.endswith(
        "ENGINE = ReplacingMergeTree(computed_at)\nPARTITION BY toYYYYMM(day)\n"
        "ORDER BY (org_id, repo_id, day, file_path)\nSETTINGS index_granularity = 8192"
    )


def test_an_explicit_primary_key_is_refused(migration) -> None:
    ddl = _ddl("repo_metrics_daily", "org_id, repo_id, day").replace(
        "ORDER BY", "PRIMARY KEY (org_id)\nORDER BY"
    )
    with pytest.raises(ValueError, match="PRIMARY KEY"):
        migration._rewrite_engine_and_key(ddl, ("org_id", "repo_id", "day"))


# ---------------------------------------------------------------------------
# Rerun, crash convergence and concurrency
# ---------------------------------------------------------------------------


def _converted(table: str, key: str) -> str:
    return _ddl(table, key, engine="ReplacingMergeTree(computed_at)")


def test_an_already_converted_table_is_a_no_op(migration) -> None:
    table = "review_edges_daily"
    client = FakeClient(
        {table: _converted(table, "org_id, repo_id, reviewer, author, day")}
    )
    migration._rebuild_table(client, table)
    assert client.commands == []


def test_a_converted_table_with_a_narrower_key_is_refused(migration) -> None:
    table = "team_metrics_daily"
    client = FakeClient({table: _converted(table, "org_id, team_id, day")})
    with pytest.raises(RuntimeError, match="not the reader key"):
        migration._rebuild_table(client, table)
    assert client.commands == []


def test_a_converted_table_with_another_version_column_is_refused(migration) -> None:
    table = "repo_metrics_daily"
    client = FakeClient(
        {table: _ddl(table, "org_id, repo_id, day", engine="ReplacingMergeTree(day)")}
    )
    with pytest.raises(RuntimeError, match="version column"):
        migration._rebuild_table(client, table)


def test_a_leftover_shadow_after_the_swap_is_converged(migration) -> None:
    table = "user_metrics_daily"
    shadow = f"{table}_new"
    client = FakeClient(
        {
            table: _converted(table, "org_id, repo_id, author_email, day"),
            shadow: _ddl(shadow, "org_id, repo_id, author_email, day"),
        }
    )
    migration._rebuild_table(client, table)
    assert client.commands.index(
        f"INSERT INTO `{table}` SELECT * FROM `{shadow}`"
    ) < client.commands.index(f"DROP TABLE `{shadow}`")
    assert shadow not in client.tables
    assert f"{table}_096_lock" not in client.tables


def test_a_failed_catch_up_keeps_the_shadow_for_the_rerun(migration) -> None:
    table = "commit_metrics"
    key = "org_id, repo_id, day, author_email, commit_hash"
    client = FakeClient(
        {table: _ddl(table, key)},
        fail_on=f"INSERT INTO `{table}` SELECT * FROM `{table}_new`",
    )
    with pytest.raises(RuntimeError, match="injected failure"):
        migration._rebuild_table(client, table)
    assert f"{table}_new" in client.tables
    assert "ReplacingMergeTree(computed_at)" in client.tables[table]

    client.fail_on = None
    migration._rebuild_table(client, table)
    assert f"{table}_new" not in client.tables


def test_a_held_lock_waits_for_the_other_runner(migration, monkeypatch) -> None:
    table = "testops_quality_drag"
    lock = f"{table}_096_lock"
    client = FakeClient(
        {table: _ddl(table, "repo_id, day"), lock: "CREATE TABLE x ENGINE = Memory"}
    )

    def other_runner_finishes(_seconds: float) -> None:
        client.tables[table] = _converted(table, "org_id, repo_id, day")
        client.tables.pop(lock)

    monkeypatch.setattr(migration.time, "sleep", other_runner_finishes)
    migration._rebuild_table(client, table)
    assert not any(c.startswith("EXCHANGE") for c in client.commands)


def test_a_lock_released_without_a_conversion_raises(migration) -> None:
    table = "testops_pipeline_stability"
    lock = f"{table}_096_lock"
    client = FakeClient(
        {table: _ddl(table, "repo_id, day"), lock: "CREATE TABLE x ENGINE = Memory"}
    )
    original_query = client.query
    calls = {"n": 0}

    def query(q: str, parameters: dict | None = None) -> _Result:
        if "count() FROM system.tables" in q and (parameters or {}).get("name") == lock:
            calls["n"] += 1
            return _Result([[0]])
        return original_query(q, parameters)

    client.query = query  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="released its lock without finishing"):
        migration._wait_for_concurrent_conversion(client, table)
    assert calls["n"] == 1


def test_a_failed_probe_propagates_instead_of_skipping(migration) -> None:
    class Failing(FakeClient):
        def query(self, query: str, parameters: dict | None = None) -> _Result:
            raise RuntimeError("connection reset by peer")

    client = Failing({"repo_metrics_daily": _ddl("repo_metrics_daily", "org_id")})
    with pytest.raises(RuntimeError, match="connection reset"):
        migration._rebuild_table(client, "repo_metrics_daily")
    assert client.commands == []


def test_upgrade_converts_every_table(migration) -> None:
    tables = {
        table: _ddl(
            table,
            ", ".join(
                migration.LEGACY_SORT_KEYS.get(table, migration.TARGET_SORT_KEYS[table])
            ),
        )
        for table in migration.TABLES
    }
    client = FakeClient(tables)
    migration.upgrade(client)
    for table in migration.TABLES:
        assert "ReplacingMergeTree(computed_at)" in client.tables[table]
        assert client._key(table) == ", ".join(migration.TARGET_SORT_KEYS[table])
