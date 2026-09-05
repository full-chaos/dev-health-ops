"""Migration 087: complexity tables -> ReplacingMergeTree(computed_at).

The complexity family writes append-only with no ALTER/DELETE anywhere, while
``internal/jobs/metrics/remaining/families.json`` declares
``"replay": "generation_replace"`` -- a contract nothing implements, because no
table carries a generation column. Measured before the migration: 6.85x
duplication on ``repo_complexity_daily``, 6.01x on ``file_complexity_snapshots``
and 20.0x on ``team_complexity_daily``.

These tests drive the rebuild against a fake client, so they need no database
and run on every leg. They cover the three properties that make an engine swap
safe to run against live data:

  PARTIAL   a run that dies mid-flight must not leave data behind or half-swapped;
  REDRIVE   re-running must converge, including from a leftover shadow;
  ONE ROW   the copy must preserve every distinct sorting-key tuple, and must
            refuse to swap if it does not.

The sorting key is the thing that must NOT move. All three tables are
org_id-first, so a key that changed during the engine swap could collapse two
tenants' rows into one -- which is why the migration re-reads the key from
system.tables and aborts on any difference rather than trusting its own regex.
"""

from __future__ import annotations

import importlib.util
import re
import time
from pathlib import Path
from types import ModuleType

import pytest

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)

MIGRATION_087 = "087_complexity_tables_replacing_merge_tree.py"

# codex r1's reviewed tip (chaos-4291-2250-r1-20260905T052512), frozen per the
# push-recipe rule: this sha is NEVER amended, only built on top of. Named
# here only for the frozen fixture's own header/provenance -- nothing below
# reads it or reaches git/network for it (team-lead, 09-05: "a test that
# depends on git depth or network is a flake by construction"). See
# tests/fixtures/migration_087_prefix_frozen.py for the actual pre-fix code.
FROZEN_R1_TIP = "5f3533b8531aa37df21f18fdf9959cb089a9ceb6"

_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_frozen_prefix_module() -> ModuleType:
    """Load the static, in-repo frozen copy of migration 087's pre-fix
    functions (tests/fixtures/migration_087_prefix_frozen.py).

    No git, no network, no dependency on checkout depth: the file is
    committed source, read like any other test fixture.
    """
    path = _FIXTURES_DIR / "migration_087_prefix_frozen.py"
    spec = importlib.util.spec_from_file_location("migration_087_prefix_frozen", path)
    assert spec is not None and spec.loader is not None, f"cannot load {path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The live sorting keys, read from system.tables (NOT from migration 007, whose
# ORDER BY has since been amended to be org_id-first).
LIVE_KEYS = {
    "file_complexity_snapshots": "org_id, repo_id, as_of_day, file_path",
    "repo_complexity_daily": "org_id, repo_id, day",
    "team_complexity_daily": "org_id, team_id, day",
}


def _load_migration(filename: str) -> ModuleType:
    """Load a migration module standalone, exactly like the runner does."""
    path = MIGRATIONS_DIR / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None, f"cannot load {filename}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def migration() -> ModuleType:
    return _load_migration(MIGRATION_087)


def _ddl(table: str, key: str, engine: str = "MergeTree") -> str:
    return (
        f"CREATE TABLE {table} (`org_id` String, `repo_id` UUID, `day` Date, "
        f"`computed_at` DateTime) ENGINE = {engine} "
        f"PARTITION BY toYYYYMM(day) ORDER BY ({key})"
    )


class _FakeResult:
    def __init__(self, rows: list[list]) -> None:
        self.result_rows = rows


class FakeClient:
    """ClickHouse client double that tracks commands and mutates a catalog.

    ``key_counts`` maps table name -> distinct sorting-key tuple count, so a
    test can make the shadow copy disagree with its source and prove the
    migration refuses to swap.
    """

    def __init__(
        self,
        tables: dict[str, dict[str, str]],
        *,
        created_sorting_key: str | None = None,
        created_engine: str = "ReplacingMergeTree",
        key_counts: dict[str, int] | None = None,
        columns: set[str] | None = None,
        fail_on: str | None = None,
        fail_query_on: str | None = None,
        projections: dict[str, list[str]] | None = None,
    ) -> None:
        self.tables = {name: dict(spec) for name, spec in tables.items()}
        self.created_sorting_key = created_sorting_key
        self.created_engine = created_engine
        self.key_counts = key_counts or {}
        self.columns = columns if columns is not None else {"computed_at"}
        self.fail_on = fail_on
        # r1 P1 (fail-open existence probe): a query() failure must propagate,
        # never be swallowed. Substring-matched against the query text, like
        # fail_on is for command().
        self.fail_query_on = fail_query_on
        self.projections = {
            name: list(names) for name, names in (projections or {}).items()
        }
        self.commands: list[str] = []

    def query(self, query: str, parameters: dict | None = None) -> _FakeResult:
        if self.fail_query_on and self.fail_query_on in query:
            raise RuntimeError(f"injected query failure on: {query}")
        if "count() FROM system.tables" in query:
            assert parameters is not None
            return _FakeResult([[1 if parameters["name"] in self.tables else 0]])
        if "engine_full FROM system.tables" in query:
            # r3 P1: engine_full carries the version-column ARGUMENT that
            # bare `engine` cannot -- defaults to the correct
            # ReplacingMergeTree(computed_at) for any catalog entry that
            # says engine=="ReplacingMergeTree" and doesn't explicitly
            # override "engine_full", so every EXISTING test (which never
            # sets engine_full) keeps exercising the "correctly converted"
            # case unchanged. A test proving the wrong-version-column bug
            # sets "engine_full" explicitly to something else.
            assert parameters is not None
            spec = self.tables.get(parameters["name"])
            if not spec:
                return _FakeResult([])
            engine_full = spec.get("engine_full")
            if engine_full is None:
                engine_full = (
                    "ReplacingMergeTree(computed_at)"
                    if spec["engine"] == "ReplacingMergeTree"
                    else spec["engine"]
                )
            return _FakeResult([[engine_full]])
        if "engine FROM system.tables" in query:
            assert parameters is not None
            spec = self.tables.get(parameters["name"])
            return _FakeResult([[spec["engine"]]] if spec else [])
        if "sorting_key FROM system.tables" in query:
            assert parameters is not None
            spec = self.tables.get(parameters["name"])
            return _FakeResult([[spec["sorting_key"]]] if spec else [])
        if "count() FROM system.columns" in query:
            assert parameters is not None
            return _FakeResult([[1 if parameters["c"] in self.columns else 0]])
        if "name FROM system.projections" in query:
            assert parameters is not None
            return _FakeResult(
                [[name] for name in self.projections.get(parameters["table"], [])]
            )
        if query.startswith("SHOW CREATE TABLE"):
            name = query.split("`")[1]
            return _FakeResult([[self.tables[name]["ddl"]]])
        if "uniqExact" in query:
            name = re.search(r"FROM `(\w+)`", query).group(1)  # type: ignore[union-attr]
            return _FakeResult([[self.key_counts.get(name, 5)]])
        raise AssertionError(f"unexpected query: {query}")

    def command(self, cmd: str) -> None:
        self.commands.append(cmd)
        if self.fail_on and self.fail_on in cmd:
            self.fail_on = None  # fail once
            raise RuntimeError(f"injected failure on: {cmd}")
        if cmd.startswith("DROP TABLE"):
            self.tables.pop(cmd.split("`")[1], None)
        elif cmd.startswith("CREATE TABLE"):
            name = re.search(r"CREATE TABLE\s+`?(\w+)`?", cmd).group(1)  # type: ignore[union-attr]
            if name in self.tables:
                # Real ClickHouse CREATE TABLE (without IF NOT EXISTS) fails
                # on a name collision -- this atomicity is exactly what
                # _acquire_lock relies on to act as a mutex.
                raise RuntimeError(f"table already exists: {name}")
            source = re.search(r"ORDER BY \(([^)]*)\)", cmd)
            self.tables[name] = {
                "ddl": cmd,
                "engine": self.created_engine,
                "sorting_key": (
                    self.created_sorting_key
                    if self.created_sorting_key is not None
                    else (source.group(1) if source else "")
                ),
            }
        elif cmd.startswith("EXCHANGE TABLES"):
            a, b = re.findall(r"`(\w+)`", cmd)
            self.tables[a], self.tables[b] = self.tables[b], self.tables[a]


# ---------------------------------------------------------------------------
# The migration's declared surface
# ---------------------------------------------------------------------------


def test_migration_exists_and_has_upgrade(migration) -> None:
    assert callable(getattr(migration, "upgrade", None))


def test_migration_covers_all_three_complexity_tables(migration) -> None:
    assert set(migration.TABLES) == set(LIVE_KEYS), (
        f"087 must convert exactly the three complexity tables; got {migration.TABLES}"
    )


def test_version_column_is_computed_at(migration) -> None:
    # All three tables carry computed_at, and it is the only monotonic column
    # available to act as the RMT version.
    assert migration.RMT_VERSION_COLUMN == "computed_at"


def test_engine_rewrite_targets_replacing_merge_tree(migration) -> None:
    rewritten = migration._replace_engine_with_rmt(
        _ddl("repo_complexity_daily", LIVE_KEYS["repo_complexity_daily"])
    )
    assert "ENGINE = ReplacingMergeTree(computed_at)" in rewritten


def test_engine_rewrite_refuses_a_non_mergetree_engine(migration) -> None:
    # Fail closed: if the DDL is not a plain MergeTree, a silent no-op rewrite
    # would produce a shadow identical to the source and swap it in, leaving
    # the duplication in place while reporting success.
    already = _ddl(
        "repo_complexity_daily",
        "org_id, repo_id, day",
        engine="ReplacingMergeTree(computed_at)",
    )
    with pytest.raises(ValueError):
        migration._replace_engine_with_rmt(already)


def test_table_name_rewrite_refuses_when_name_absent(migration) -> None:
    with pytest.raises(ValueError):
        migration._replace_table_name(
            _ddl("repo_complexity_daily", "org_id, repo_id, day"),
            "some_other_table",
            "some_other_table_new",
        )


# ---------------------------------------------------------------------------
# PARTIAL / REDRIVE / ONE-ROW-PER-KEY
# ---------------------------------------------------------------------------


def _catalog(table: str) -> dict[str, dict[str, str]]:
    return {
        table: {
            "ddl": _ddl(table, LIVE_KEYS[table]),
            "engine": "MergeTree",
            "sorting_key": LIVE_KEYS[table],
        }
    }


def test_rebuild_runs_catch_up_after_exchange_then_drops_shadow(migration) -> None:
    """The happy path still has to catch up: rows written between the snapshot
    copy and the EXCHANGE live only in the shadow afterwards."""
    table = "repo_complexity_daily"
    client = FakeClient(_catalog(table))

    migration._rebuild_table(client, table)

    joined = "\n".join(client.commands)
    exchange_at = next(
        i for i, c in enumerate(client.commands) if c.startswith("EXCHANGE TABLES")
    )
    catch_up_at = next(
        i
        for i, c in enumerate(client.commands)
        if c.startswith(f"INSERT INTO `{table}` SELECT * FROM `{table}_new`")
    )
    drop_at = next(
        i for i, c in enumerate(client.commands) if c == f"DROP TABLE `{table}_new`"
    )
    assert exchange_at < catch_up_at < drop_at, (
        "the catch-up must run AFTER the swap and BEFORE the drop:\n" + joined
    )
    assert f"{table}_new" not in client.tables


def test_rebuild_does_not_drop_shadow_when_catch_up_fails(migration) -> None:
    """A post-EXCHANGE failure must LEAVE the shadow: it now holds the old
    table, and dropping it would lose the rows the catch-up had not copied."""
    table = "repo_complexity_daily"
    client = FakeClient(
        _catalog(table),
        fail_on=f"INSERT INTO `{table}` SELECT * FROM `{table}_new`",
    )

    with pytest.raises(RuntimeError):
        migration._rebuild_table(client, table)

    assert f"{table}_new" in client.tables, (
        "shadow was dropped after a failed catch-up; the un-copied rows are gone"
    )


def test_skip_path_converges_leftover_shadow(migration) -> None:
    """REDRIVE: a rerun after a crash between EXCHANGE and DROP must finish the
    catch-up, not skip past it because the engine already looks converted."""
    table = "repo_complexity_daily"
    client = FakeClient(
        {
            table: {
                "ddl": _ddl(
                    table, LIVE_KEYS[table], engine="ReplacingMergeTree(computed_at)"
                ),
                "engine": "ReplacingMergeTree",
                "sorting_key": LIVE_KEYS[table],
            },
            f"{table}_new": {
                "ddl": _ddl(f"{table}_new", LIVE_KEYS[table]),
                "engine": "MergeTree",
                "sorting_key": LIVE_KEYS[table],
            },
        }
    )

    migration._rebuild_table(client, table)

    assert any(
        c.startswith(f"INSERT INTO `{table}` SELECT * FROM `{table}_new`")
        for c in client.commands
    ), "leftover shadow was not caught up on the skip path"
    assert f"{table}_new" not in client.tables


def test_already_converted_without_shadow_is_a_no_op(migration) -> None:
    """Idempotence: a second clean run must not rebuild anything."""
    table = "repo_complexity_daily"
    client = FakeClient(
        {
            table: {
                "ddl": _ddl(
                    table, LIVE_KEYS[table], engine="ReplacingMergeTree(computed_at)"
                ),
                "engine": "ReplacingMergeTree",
                "sorting_key": LIVE_KEYS[table],
            }
        }
    )

    migration._rebuild_table(client, table)

    assert client.commands == [], f"expected no commands, got {client.commands}"


def test_refuses_to_swap_when_distinct_key_count_differs(migration) -> None:
    """ONE ROW PER KEY: raw counts legitimately fall (that IS the dedup), so the
    guard compares DISTINCT sorting-key tuples. A shadow holding fewer distinct
    keys than its source means real rows were lost, and must abort BEFORE the
    swap."""
    table = "repo_complexity_daily"
    client = FakeClient(
        _catalog(table),
        key_counts={table: 500, f"{table}_new": 499},
    )

    with pytest.raises(RuntimeError, match="distinct-key mismatch"):
        migration._rebuild_table(client, table)

    assert not any(c.startswith("EXCHANGE TABLES") for c in client.commands), (
        "aborted run must not have swapped"
    )
    assert f"{table}_new" not in client.tables, "disposable shadow must be dropped"


def test_refuses_when_the_sorting_key_moves(migration) -> None:
    """The dedup key must be byte-for-byte unchanged. These tables are
    org_id-first; a key that lost org_id would silently merge two tenants."""
    table = "repo_complexity_daily"
    client = FakeClient(_catalog(table), created_sorting_key="repo_id, day")

    with pytest.raises(RuntimeError, match="sorting key changed"):
        migration._rebuild_table(client, table)

    assert not any(c.startswith("EXCHANGE TABLES") for c in client.commands)
    assert f"{table}_new" not in client.tables


def test_refuses_when_shadow_is_not_replacing_merge_tree(migration) -> None:
    """If the regex rewrite silently failed to change the engine, the swap would
    install a plain MergeTree and the duplication would continue -- while the
    migration reported success."""
    table = "repo_complexity_daily"
    client = FakeClient(_catalog(table), created_engine="MergeTree")

    with pytest.raises(RuntimeError, match="not ReplacingMergeTree"):
        migration._rebuild_table(client, table)

    assert not any(c.startswith("EXCHANGE TABLES") for c in client.commands)
    assert f"{table}_new" not in client.tables


def test_refuses_when_version_column_is_absent(migration) -> None:
    """ReplacingMergeTree(computed_at) on a table without computed_at would be
    a DDL error at best and an arbitrary-winner dedup at worst."""
    table = "repo_complexity_daily"
    client = FakeClient(_catalog(table), columns=set())

    with pytest.raises(ValueError, match="computed_at"):
        migration._rebuild_table(client, table)


def test_missing_table_is_skipped_not_an_error(migration) -> None:
    """A fresh database that has not created these tables yet must not fail the
    whole migration run."""
    client = FakeClient({})
    migration._rebuild_table(client, "repo_complexity_daily")
    assert client.commands == []


def test_refuses_to_convert_a_table_with_the_wrong_sorting_key(migration) -> None:
    """r1 P1: the pre-existing key must itself be the org-scoped key this
    migration requires, not just internally consistent with its own
    shadow. A table someone created with a non-org-prefixed key must never
    be silently "converted" with that wrong key preserved."""
    table = "repo_complexity_daily"
    client = FakeClient(
        {
            table: {
                "ddl": _ddl(table, "repo_id, day"),
                "engine": "MergeTree",
                "sorting_key": "repo_id, day",
            }
        }
    )

    with pytest.raises(
        RuntimeError, match="does not match the required org-scoped key"
    ):
        migration._rebuild_table(client, table)

    assert not any(c.startswith("EXCHANGE TABLES") for c in client.commands)
    assert not any(
        c.startswith("CREATE TABLE") and "_new" in c for c in client.commands
    )


def test_refuses_to_treat_a_wrong_key_replacing_merge_tree_table_as_done(
    migration,
) -> None:
    """r1 P1: the 'already ReplacingMergeTree, skip' path checked engine
    name only. A table that reached RMT some other way, with the wrong
    key, must not be silently accepted as safe -- two tenants' rows could
    already be colliding on that key."""
    table = "repo_complexity_daily"
    client = FakeClient(
        {
            table: {
                "ddl": _ddl(
                    table, "repo_id, day", engine="ReplacingMergeTree(computed_at)"
                ),
                "engine": "ReplacingMergeTree",
                "sorting_key": "repo_id, day",
            }
        }
    )

    with pytest.raises(
        RuntimeError, match="does not match the required org-scoped key"
    ):
        migration._rebuild_table(client, table)


def test_table_projections_reads_from_system_projections(migration) -> None:
    """team-lead 09-05: projection presence is read from the live catalog
    (system.projections), not pattern-matched out of DDL text."""
    table = "file_complexity_snapshots"
    client = FakeClient(
        _catalog(table), projections={table: ["prj_acr_file_complexity_runs"]}
    )
    assert migration._table_projections(client, table) == [
        "prj_acr_file_complexity_runs"
    ]
    assert migration._table_projections(client, "repo_complexity_daily") == []


def test_ensure_projection_dedup_setting_is_a_no_op_without_a_projection(
    migration,
) -> None:
    ddl = _ddl("repo_complexity_daily", LIVE_KEYS["repo_complexity_daily"])
    assert migration._ensure_projection_dedup_setting(ddl, []) == ddl


def test_ensure_projection_dedup_setting_adds_a_settings_clause(migration) -> None:
    # ClickHouse code 344 SUPPORT_IS_DISABLED: a ReplacingMergeTree table
    # with a PROJECTION and no explicit deduplicate_merge_projection_mode
    # fails at CREATE time. Measured live against file_complexity_snapshots.
    ddl = (
        "CREATE TABLE file_complexity_snapshots (`org_id` String, "
        "`computed_at` DateTime, PROJECTION p (SELECT count())) "
        "ENGINE = ReplacingMergeTree(computed_at) ORDER BY (org_id)"
    )
    rewritten = migration._ensure_projection_dedup_setting(ddl, ["p"])
    assert "deduplicate_merge_projection_mode = 'rebuild'" in rewritten


def test_ensure_projection_dedup_setting_extends_an_existing_settings_clause(
    migration,
) -> None:
    ddl = (
        "CREATE TABLE file_complexity_snapshots (`org_id` String, "
        "`computed_at` DateTime, PROJECTION p (SELECT count())) "
        "ENGINE = ReplacingMergeTree(computed_at) ORDER BY (org_id) "
        "SETTINGS index_granularity = 8192"
    )
    rewritten = migration._ensure_projection_dedup_setting(ddl, ["p"])
    assert "deduplicate_merge_projection_mode = 'rebuild'" in rewritten
    assert "index_granularity = 8192" in rewritten
    # Both settings must survive as one clause, not two.
    assert rewritten.count("SETTINGS") == 1


def test_rebuild_applies_the_projection_setting_to_the_shadow_ddl(migration) -> None:
    """End-to-end: a table whose live system.projections carries a row gets
    the dedup-mode setting on the CREATE actually issued for its shadow --
    driven by the catalog read, not by the DDL text containing the word
    PROJECTION (it does here too, but that is not what gates the setting)."""
    table = "file_complexity_snapshots"
    key = LIVE_KEYS[table]
    ddl_with_projection = (
        f"CREATE TABLE {table} (`org_id` String, `repo_id` UUID, "
        f"`as_of_day` Date, `file_path` String, `computed_at` DateTime, "
        f"PROJECTION p (SELECT count())) ENGINE = MergeTree "
        f"ORDER BY ({key})"
    )
    client = FakeClient(
        {
            table: {
                "ddl": ddl_with_projection,
                "engine": "MergeTree",
                "sorting_key": key,
            }
        },
        projections={table: ["p"]},
    )

    migration._rebuild_table(client, table)

    create_shadow = next(
        c
        for c in client.commands
        if c.startswith("CREATE TABLE") and f"{table}_new" in c
    )
    assert "deduplicate_merge_projection_mode = 'rebuild'" in create_shadow


def test_rebuild_does_not_apply_the_setting_when_system_projections_is_empty(
    migration,
) -> None:
    """The inverse of the test above: even DDL text that happens to mention
    the word PROJECTION must NOT trigger the setting if system.projections
    reports none for this table -- the catalog is authoritative, not the
    DDL string."""
    table = "repo_complexity_daily"
    key = LIVE_KEYS[table]
    ddl_mentioning_the_word = (
        f"CREATE TABLE {table} (`org_id` String, `repo_id` UUID, `day` Date, "
        f"`computed_at` DateTime, `notes` String COMMENT 'no PROJECTION here') "
        f"ENGINE = MergeTree ORDER BY ({key})"
    )
    client = FakeClient(
        {
            table: {
                "ddl": ddl_mentioning_the_word,
                "engine": "MergeTree",
                "sorting_key": key,
            }
        },
        projections={},
    )

    migration._rebuild_table(client, table)

    create_shadow = next(
        c
        for c in client.commands
        if c.startswith("CREATE TABLE") and f"{table}_new" in c
    )
    assert "deduplicate_merge_projection_mode" not in create_shadow


def test_table_exists_fails_closed_not_open_on_a_query_exception(migration) -> None:
    """r1 P1, proven against BOTH sides: on the frozen r1 tip, an exception
    from the `system.tables` existence probe was swallowed into `False`
    (fail-OPEN) -- `_rebuild_table` then logged "table does not exist,
    skipping" and issued ZERO commands, even though the table is real and a
    real error occurred. This lane's fix makes the same scenario raise.

    BEFORE (the frozen fixture, tests/fixtures/migration_087_prefix_frozen.py):
    the assertion below on `old_client.commands == []` with NO exception
    PASSES -- reproducing the bug against the actual pre-fix source, not
    just quoting the finding.
    AFTER (this fix): the equivalent call on `new_client` RAISES the
    injected error instead of swallowing it -- proven with pytest.raises.
    """
    table = "repo_complexity_daily"

    old_migration = _load_frozen_prefix_module()
    old_client = FakeClient(_catalog(table), fail_query_on="count() FROM system.tables")
    old_migration._rebuild_table(old_client, table)  # must NOT raise -- that's the bug
    assert old_client.commands == [], (
        "expected the frozen pre-fix fixture to reproduce the fail-open bug "
        "(silently do nothing on a query exception) -- if this now fails, "
        "the fixture may have been edited (it must never be)"
    )

    new_client = FakeClient(_catalog(table), fail_query_on="count() FROM system.tables")
    with pytest.raises(RuntimeError, match="injected query failure"):
        migration._rebuild_table(new_client, table)


def test_concurrent_runner_lock_did_not_exist_before_this_fix(migration) -> None:
    """r1 P1, proven against BOTH sides: the frozen r1 tip had NO mutual-
    exclusion mechanism at all -- `_acquire_lock` did not exist as a name in
    that module, so nothing could have stopped two runners from both passing
    every check, both building a shadow, and interleaving their EXCHANGEs
    (the exact scenario the finding describes: the live table ends up back
    on MergeTree despite the migration being recorded as applied).

    BEFORE (the frozen fixture): `hasattr(old_migration, "_acquire_lock")`
    is False -- there is no lock to even attempt acquiring twice.
    AFTER (this fix): a second concurrent `_acquire_lock` call, while the
    first is still held, is refused.
    """
    old_migration = _load_frozen_prefix_module()
    assert not hasattr(old_migration, "_acquire_lock"), (
        "expected the frozen pre-fix fixture to have NO lock mechanism -- "
        "if this now fails, the fixture may have been edited (it must never be)"
    )

    table = "repo_complexity_daily"
    client = FakeClient(_catalog(table))
    assert migration._acquire_lock(client, table) is True
    assert migration._acquire_lock(client, table) is False, (
        "a second concurrent acquire must be refused while the first "
        "runner's lock is still held -- this is the actual mechanism that "
        "did not exist before this fix"
    )


def test_acquire_lock_fails_when_a_lock_is_already_held(migration) -> None:
    """r1 P1: two concurrent runners must not both proceed to build a
    shadow -- the second `CREATE TABLE ..._087_lock` must fail because the
    first runner's lock table already exists."""
    table = "repo_complexity_daily"
    client = FakeClient(
        {
            f"{table}{migration._LOCK_SUFFIX}": {
                "ddl": f"CREATE TABLE {table}{migration._LOCK_SUFFIX} (x UInt8) ENGINE = Memory",
                "engine": "Memory",
                "sorting_key": "",
            }
        }
    )

    assert migration._acquire_lock(client, table) is False


def _acquire_lock_pre_r2_fix(client, table: str, migration: ModuleType) -> bool:
    """Frozen inline copy of `_acquire_lock`'s pre-r2-fix behavior (codex r2
    P3 #7): EVERY exception from the mutex `CREATE TABLE` -- contention or
    not -- was converted into the same `False`. Kept inline here, not in a
    fixture file, because this bug was introduced by this lane's own r1 fix
    (the frozen r1-prefix fixture predates `_acquire_lock` entirely --
    see test_concurrent_runner_lock_did_not_exist_before_this_fix -- so
    there is no earlier real tip to load it from). This is a literal
    snapshot of the code being fixed, used only to prove the OLD behavior
    on the SAME injected failure the fixed test below exercises.
    """
    try:
        client.command(
            f"CREATE TABLE `{table}{migration._LOCK_SUFFIX}` (x UInt8) ENGINE = Memory"
        )
        return True
    except Exception:
        return False


def test_acquire_lock_masked_non_contention_failures_before_r2_fix(migration) -> None:
    """PROVE-BY-FAILURE for r2 P3 #7: an injected non-contention failure
    (a permission/transport/resource error, not a name collision) during
    the lock's CREATE TABLE read as ordinary contention under the pre-fix
    code -- reported as False exactly like a real second-runner collision,
    with the real cause discarded.
    """
    table = "repo_complexity_daily"
    lock_name = f"{table}{migration._LOCK_SUFFIX}"
    client = FakeClient(_catalog(table), fail_on=f"CREATE TABLE `{lock_name}`")

    assert _acquire_lock_pre_r2_fix(client, table, migration) is False, (
        "expected the pre-fix behavior to swallow ANY exception into False "
        "-- if this now fails, the inline copy above may have drifted from "
        "the actual pre-fix code (it must stay a literal snapshot)"
    )


def test_acquire_lock_reraises_a_non_contention_failure(migration) -> None:
    """r2 P3 #7, fixed: the identical injected non-contention failure must
    now propagate instead of being reported as lock contention -- a caller
    treating this as "another runner holds the lock" would either wait out
    the full timeout or hit `_wait_for_concurrent_conversion`'s own
    "released without converting" error, both of which bury the real
    exception.
    """
    table = "repo_complexity_daily"
    lock_name = f"{table}{migration._LOCK_SUFFIX}"
    client = FakeClient(_catalog(table), fail_on=f"CREATE TABLE `{lock_name}`")

    with pytest.raises(RuntimeError, match="injected failure"):
        migration._acquire_lock(client, table)


def test_wait_for_concurrent_conversion_returns_once_the_other_runner_finishes(
    migration, monkeypatch
) -> None:
    """The wait loop must notice the OTHER runner's success rather than
    only ever timing out or only ever checking once."""
    table = "repo_complexity_daily"
    lock_name = f"{table}{migration._LOCK_SUFFIX}"
    client = FakeClient(
        {
            table: {
                "ddl": _ddl(table, LIVE_KEYS[table]),
                "engine": "MergeTree",
                "sorting_key": LIVE_KEYS[table],
            },
            lock_name: {"ddl": "", "engine": "Memory", "sorting_key": ""},
        }
    )
    monkeypatch.setattr(migration, "_LOCK_POLL_INTERVAL_SECS", 0)

    calls = {"n": 0}
    real_engine_name = migration._engine_name

    def flipping_engine_name(c, t):
        calls["n"] += 1
        if calls["n"] >= 2:
            client.tables[table]["engine"] = "ReplacingMergeTree"
        return real_engine_name(c, t)

    monkeypatch.setattr(migration, "_engine_name", flipping_engine_name)

    migration._wait_for_concurrent_conversion(client, table)  # must not raise/time out
    assert calls["n"] >= 2


def test_wait_for_concurrent_conversion_raises_if_lock_vanishes_without_success(
    migration, monkeypatch
) -> None:
    """If the other runner's lock disappears without the table ever
    reaching ReplacingMergeTree, its attempt failed -- this must be raised,
    not treated as an implicit skip."""
    table = "repo_complexity_daily"
    client = FakeClient(
        {
            table: {
                "ddl": _ddl(table, LIVE_KEYS[table]),
                "engine": "MergeTree",
                "sorting_key": LIVE_KEYS[table],
            }
        }
    )
    monkeypatch.setattr(migration, "_LOCK_POLL_INTERVAL_SECS", 0)

    with pytest.raises(RuntimeError, match="lock was released"):
        migration._wait_for_concurrent_conversion(client, table)


def _wait_for_concurrent_conversion_pre_r2_fix(
    client, table: str, migration: ModuleType
) -> None:
    """Frozen inline copy of `_wait_for_concurrent_conversion`'s pre-r2-fix
    success condition (codex r2 P1 #1): success was declared the instant
    the engine read back as ReplacingMergeTree, with no check that the
    `<table>_new` shadow -- and therefore the post-EXCHANGE catch-up -- was
    actually done. Kept inline, not in a fixture file, for the same reason
    as `_acquire_lock_pre_r2_fix` above: this function did not exist before
    this lane's own r1 fix, so there is no earlier real tip to load it
    from. This is a literal snapshot of the removed condition.
    """
    deadline = time.monotonic() + migration._LOCK_WAIT_TIMEOUT_SECS
    while time.monotonic() < deadline:
        if migration._engine_name(client, table) == "ReplacingMergeTree":
            return  # THE BUG: no check that the shadow is gone too.
        if not migration._table_exists(client, f"{table}{migration._LOCK_SUFFIX}"):
            raise RuntimeError(
                f"{table}: another runner's migration 087 lock was released "
                f"without converting the table to ReplacingMergeTree"
            )
        time.sleep(migration._LOCK_POLL_INTERVAL_SECS)
    raise RuntimeError(f"{table}: timed out")


def _crash_window_catalog(
    table: str, migration: ModuleType
) -> dict[str, dict[str, str]]:
    """The exact state codex r2 P1 #1 describes: a first runner completed
    EXCHANGE (live table already ReplacingMergeTree) but was hard-killed
    before `_catch_up_and_drop` and `_release_lock` could run -- so the
    shadow (holding rows written between the snapshot copy and the swap)
    and the lock table are both still present, and nothing will ever come
    back to finish either.
    """
    catalog = _catalog(table)
    catalog[table]["engine"] = "ReplacingMergeTree"
    catalog[table]["sorting_key"] = LIVE_KEYS[table]
    catalog[f"{table}_new"] = {
        "ddl": _ddl(f"{table}_new", LIVE_KEYS[table]),
        "engine": "MergeTree",
        "sorting_key": LIVE_KEYS[table],
    }
    catalog[f"{table}{migration._LOCK_SUFFIX}"] = {
        "ddl": "",
        "engine": "Memory",
        "sorting_key": "",
    }
    return catalog


def test_wait_for_concurrent_conversion_accepted_the_crash_window_before_r2_fix(
    migration,
) -> None:
    """PROVE-BY-FAILURE for r2 P1 #1: under the pre-fix condition, the
    crash-window state (engine already RMT, shadow and lock both still
    present, owning runner gone forever) reads as "the other runner
    finished" -- returning cleanly with the shadow, and its never-caught-up
    rows, left stranded.
    """
    table = "repo_complexity_daily"
    client = FakeClient(_crash_window_catalog(table, migration))

    _wait_for_concurrent_conversion_pre_r2_fix(
        client, table, migration
    )  # must NOT raise
    assert f"{table}_new" in client.tables, (
        "expected the pre-fix condition to reproduce the bug (declare "
        "success with the shadow still present) -- if this now fails, the "
        "inline copy above may have drifted from the actual pre-fix code"
    )


def test_wait_for_concurrent_conversion_fails_closed_on_the_crash_window(
    migration, monkeypatch
) -> None:
    """r2 P1 #1, fixed: the identical crash-window state must NOT be
    reported as done. The owning runner is gone and its lock will never be
    released, so this must keep waiting and then fail closed -- never
    silently declare the conversion finished while the shadow's rows are
    still stranded there.
    """
    monkeypatch.setattr(migration, "_LOCK_WAIT_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr(migration, "_LOCK_POLL_INTERVAL_SECS", 0.01)

    table = "repo_complexity_daily"
    client = FakeClient(_crash_window_catalog(table, migration))

    with pytest.raises(RuntimeError, match="timed out"):
        migration._wait_for_concurrent_conversion(client, table)
    assert f"{table}_new" in client.tables, (
        "the shadow -- and its never-caught-up rows -- must still be there: "
        "this runner correctly refused to silently drop them"
    )


def test_lock_absent_shadow_present_still_runs_catch_up(migration) -> None:
    """team-lead 09-05: engine already RMT, lock ABSENT (no other runner is
    active), a leftover shadow from an earlier interrupted run --
    `_rebuild_table`'s own convergence branch (reached because
    `_acquire_lock` succeeds immediately; `_wait_for_concurrent_conversion`
    is never even called) must still perform catch-up, not treat "already
    ReplacingMergeTree" as "nothing to do" and leave the shadow's rows
    stranded forever.
    """
    table = "repo_complexity_daily"
    catalog = _catalog(table)
    catalog[table]["engine"] = "ReplacingMergeTree"
    catalog[table]["sorting_key"] = LIVE_KEYS[table]
    catalog[f"{table}_new"] = {
        "ddl": _ddl(f"{table}_new", LIVE_KEYS[table]),
        "engine": "MergeTree",
        "sorting_key": LIVE_KEYS[table],
    }
    client = FakeClient(catalog)

    migration._rebuild_table(client, table)

    assert f"{table}_new" not in client.tables, (
        "catch-up must drop the shadow once it has run"
    )
    assert any(
        cmd.startswith("INSERT INTO") and f"`{table}_new`" in cmd
        for cmd in client.commands
    ), "catch-up must copy the shadow's rows forward before dropping it"


# ---------------------------------------------------------------------------
# r3: version-column check on the "already RMT" paths, lock-release telemetry
# ---------------------------------------------------------------------------


def test_already_rmt_table_with_wrong_version_column_is_refused(migration) -> None:
    """PROVE-BY-FAILURE for r3 P1: a table that reads back as
    ReplacingMergeTree, with the RIGHT sorting key, but the WRONG (or no)
    version-column argument must be REFUSED, not silently accepted as
    already converted -- codex's own exact repro construction
    (ReplacingMergeTree(other_version)).
    """
    table = "repo_complexity_daily"
    catalog = _catalog(table)
    catalog[table]["engine"] = "ReplacingMergeTree"
    catalog[table]["sorting_key"] = LIVE_KEYS[table]
    catalog[table]["engine_full"] = "ReplacingMergeTree(other_version)"
    client = FakeClient(catalog)

    with pytest.raises(RuntimeError, match="version column"):
        migration._rebuild_table(client, table)
    assert client.commands == [], (
        "must refuse before issuing any mutating command, not attempt a rebuild"
    )


def test_already_rmt_table_with_bare_engine_no_version_column_is_refused(
    migration,
) -> None:
    """Same finding, the OTHER shape codex named: a bare
    `ReplacingMergeTree()` with no explicit version-column argument at all.
    """
    table = "repo_complexity_daily"
    catalog = _catalog(table)
    catalog[table]["engine"] = "ReplacingMergeTree"
    catalog[table]["sorting_key"] = LIVE_KEYS[table]
    catalog[table]["engine_full"] = "ReplacingMergeTree()"
    client = FakeClient(catalog)

    with pytest.raises(RuntimeError, match="version column"):
        migration._rebuild_table(client, table)


def test_already_rmt_table_with_correct_version_column_is_still_accepted(
    migration,
) -> None:
    """Positive control for r3 P1's fix: the ORDINARY already-converted
    case (ReplacingMergeTree(computed_at), FakeClient's own default) must
    still be accepted as done -- the new check must not reject the
    common, correct case.
    """
    table = "repo_complexity_daily"
    catalog = _catalog(table)
    catalog[table]["engine"] = "ReplacingMergeTree"
    catalog[table]["sorting_key"] = LIVE_KEYS[table]
    client = FakeClient(catalog)

    migration._rebuild_table(client, table)  # must not raise

    assert client.commands == []


def test_wait_for_concurrent_conversion_also_checks_the_version_column(
    migration, monkeypatch
) -> None:
    """r3 P1's fix applies to the WAITING runner's success path too, not
    just `_rebuild_table`'s own skip branches -- a table another runner
    left with the wrong version column must not be accepted here either.
    """
    monkeypatch.setattr(migration, "_LOCK_WAIT_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr(migration, "_LOCK_POLL_INTERVAL_SECS", 0.01)

    table = "repo_complexity_daily"
    catalog = _catalog(table)
    catalog[table]["engine"] = "ReplacingMergeTree"
    catalog[table]["sorting_key"] = LIVE_KEYS[table]
    catalog[table]["engine_full"] = "ReplacingMergeTree(other_version)"
    # No shadow, no lock: the loop's very first iteration must hit the
    # version-column check, not spin until timeout.
    client = FakeClient(catalog)

    with pytest.raises(RuntimeError, match="version column"):
        migration._wait_for_concurrent_conversion(client, table)


def test_release_lock_failure_after_a_clean_conversion_now_raises(migration) -> None:
    """PROVE-BY-FAILURE for r3 P2: before this fix, a lock-release failure
    on an otherwise-SUCCESSFUL conversion was swallowed into a bare
    warning log, and `_rebuild_table` returned normally -- silently
    leaving the lock stuck forever with no signal in the migration's own
    control flow. It must now raise.
    """
    table = "repo_complexity_daily"
    lock_name = f"{table}{migration._LOCK_SUFFIX}"
    client = FakeClient(
        _catalog(table),
        fail_on=f"DROP TABLE IF EXISTS `{lock_name}`",
    )

    with pytest.raises(RuntimeError, match="lock"):
        migration._rebuild_table(client, table)


def test_release_lock_failure_on_the_in_lock_convergence_skip_path_also_raises(
    migration,
) -> None:
    """The SAME finding, but on the in-lock re-check's early `return`
    (the "already ReplacingMergeTree but leftover shadow -- converging"
    branch) specifically -- this is the path a naive `try/except/else`
    fix would have missed entirely (an early `return` inside a `try`
    skips `else`), so it gets its own direct test rather than trusting
    the fall-through happy-path test above to cover it. (The OTHER
    already-RMT skip branch, with no leftover shadow, returns before the
    lock is ever acquired at all, so it has nothing to test here.)
    """
    table = "repo_complexity_daily"
    lock_name = f"{table}{migration._LOCK_SUFFIX}"
    client = FakeClient(
        {
            table: {
                "ddl": _ddl(
                    table, LIVE_KEYS[table], engine="ReplacingMergeTree(computed_at)"
                ),
                "engine": "ReplacingMergeTree",
                "sorting_key": LIVE_KEYS[table],
            },
            f"{table}_new": {
                "ddl": _ddl(f"{table}_new", LIVE_KEYS[table]),
                "engine": "MergeTree",
                "sorting_key": LIVE_KEYS[table],
            },
        },
        fail_on=f"DROP TABLE IF EXISTS `{lock_name}`",
    )

    with pytest.raises(RuntimeError, match="lock"):
        migration._rebuild_table(client, table)


def test_release_lock_failure_during_a_real_error_does_not_mask_it(migration) -> None:
    """The lock-release failure must NEVER replace a real, already-
    propagating exception (e.g. a failed catch-up) -- Python's own
    finally-raises-eats-the-original-exception semantics is exactly the
    failure mode `_release_lock`'s internal swallow (still in force on
    this path) exists to avoid. The ORIGINAL error must be what the
    caller sees.
    """
    table = "repo_complexity_daily"
    lock_name = f"{table}{migration._LOCK_SUFFIX}"
    client = FakeClient(
        _catalog(table),
        fail_on=f"INSERT INTO `{table}` SELECT * FROM `{table}_new`",
    )
    # Also make the lock-release fail, on top of the catch-up failure.
    real_command = client.command

    def command_with_lock_release_also_failing(cmd: str) -> None:
        if cmd == f"DROP TABLE IF EXISTS `{lock_name}`":
            raise RuntimeError("injected lock-release failure")
        return real_command(cmd)

    client.command = command_with_lock_release_also_failing  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="injected failure on"):
        migration._rebuild_table(client, table)


def test_upgrade_processes_every_table(migration) -> None:
    catalog: dict[str, dict[str, str]] = {}
    for table, key in LIVE_KEYS.items():
        catalog[table] = {
            "ddl": _ddl(table, key),
            "engine": "MergeTree",
            "sorting_key": key,
        }
    client = FakeClient(catalog)

    migration.upgrade(client)

    for table in LIVE_KEYS:
        assert any(
            c.startswith("EXCHANGE TABLES") and f"`{table}`" in c
            for c in client.commands
        ), f"{table} was never swapped"
        assert client.tables[table]["engine"] == "ReplacingMergeTree"
