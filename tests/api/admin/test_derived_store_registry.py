"""CHAOS-3566: explicit derived-store registry, backstopping the migration-
directory regex scan `org_deletion._clickhouse_tables_from_migrations()` uses
to discover which ClickHouse tables org deletion purges.

The regex scan stays the actual deletion-time source of truth (no behavior
change to what is deleted for existing stores). The registry is the explicit,
committed list a human reviewed; these tests are the guard that FAILS loudly
the moment a new org_id-bearing ClickHouse table exists that the registry
does not cover -- an unmeasured deletion path must fail, not silently pass.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.derived_store_registry import (
    CLICKHOUSE_DERIVED_STORES,
    EXTERNAL_DERIVED_STORES,
    DerivedStore,
    DerivedStoreKind,
    registered_clickhouse_tables,
    unregistered_clickhouse_tables,
)
from dev_health_ops.api.services.org_deletion import (
    OrganizationDeletionService,
    _clickhouse_tables_from_migrations,
)

#: `_purge_clickhouse`/`_purge_external_stores` never touch `self.session` --
#: a real AsyncSession is unnecessary ceremony for these tests, but the
#: constructor is typed to require one.
_NO_SESSION = cast(AsyncSession, None)


def test_unregistered_clickhouse_tables_flags_a_store_the_registry_misses():
    """Plant the exact defect the guard exists to catch: a discovered table
    that was never added to the registry. The pure comparison must surface
    it -- this is the mechanism the CI guard test below relies on.
    """
    missing = unregistered_clickhouse_tables(
        ["repos", "totally_new_derived_store"], registered=["repos"]
    )
    assert missing == frozenset({"totally_new_derived_store"})


def test_unregistered_clickhouse_tables_is_empty_when_fully_covered():
    missing = unregistered_clickhouse_tables(
        ["repos", "work_items"], registered=["repos", "work_items", "extra_table"]
    )
    assert missing == frozenset()


def test_unregistered_clickhouse_tables_defaults_to_the_real_registry():
    assert unregistered_clickhouse_tables(CLICKHOUSE_DERIVED_STORES) == frozenset()
    assert unregistered_clickhouse_tables(["not_a_real_table"]) == frozenset(
        {"not_a_real_table"}
    )


def test_registry_covers_every_table_the_migration_scan_discovers():
    """The actual CI guard: a future migration that adds an org_id-bearing
    ClickHouse table without registering it here must fail this test, not
    pass silently.

    This is a deletion-completeness invariant, not a file-list drift check:
    the failure demands a real per-table decision (does org deletion purge
    this store, or is it deliberately excluded?), not merely "the list is
    stale, copy the new name over."
    """
    discovered = frozenset(_clickhouse_tables_from_migrations())
    registered = registered_clickhouse_tables()

    assert discovered, "ClickHouse migration table catalog must not be empty"
    missing = discovered - registered
    assert not missing, (
        "Org deletion has no reviewed deletion-completeness decision for "
        f"the following ClickHouse table(s), each newly discovered with an "
        f"org_id column: {sorted(missing)}. For EACH one, decide and record "
        "in api/services/derived_store_registry.py: add its name to "
        "CLICKHOUSE_DERIVED_STORES to confirm org deletion purges it (the "
        "normal case -- _purge_clickhouse already will, once registered), "
        "or document why it must be excluded from org-scoped deletion. This "
        "is not a list to copy the new name into without that decision."
    )
    stale = registered - discovered
    assert not stale, (
        "api/services/derived_store_registry.py's CLICKHOUSE_DERIVED_STORES "
        f"lists table(s) no migration discovers with an org_id column "
        f"anymore: {sorted(stale)}. Remove them, or restore the migration if "
        "this is unintended so the registry keeps reflecting a real, current "
        "deletion-completeness decision -- not a stale entry nobody re-checked."
    )


def test_migration_scan_discovers_every_create_table_in_one_statement_chunk(
    tmp_path, monkeypatch
):
    """Self-discovered while wiring migration 074's second table
    (`project_declared_state_floor`, PR #1602 round-2 review NEW-1) into
    this registry: `_clickhouse_tables_from_migrations()` splits each
    migration file's raw TEXT on `;`, then used `re.search` (first match
    only, not `re.finditer`) for `CREATE TABLE` within each chunk. Two
    `CREATE TABLE ... org_id ...` statements defined back-to-back as
    Python triple-quoted string constants -- as migration 074 now has --
    contain no literal `;` between them, so they land in the SAME chunk,
    and the SECOND table was silently never discovered as an org-deletion
    target at all (not even a "missing org_id column" warning -- it never
    reached that code path). Over-discovery is safe: a discovered name
    without a real `org_id` column is skipped downstream, WITH a warning,
    by `_purge_clickhouse`'s own `system.columns` probe. Under-discovery is
    not: a genuinely org_id-bearing table nobody added to
    `CLICKHOUSE_DERIVED_STORES` AND that this scan also misses would never
    be purged, and would never even warn -- an org-deletion completeness
    gap silent at every layer.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    migration_dir = tmp_path / "clickhouse"
    migration_dir.mkdir()
    (migration_dir / "999_two_tables_one_chunk.py").write_text(
        '_CREATE_FIRST_SQL = """\n'
        "CREATE TABLE IF NOT EXISTS first_table (\n"
        "    org_id String,\n"
        "    id String\n"
        ") ENGINE = MergeTree\n"
        "ORDER BY (org_id, id)\n"
        '"""\n'
        "\n"
        '_CREATE_SECOND_SQL = """\n'
        "CREATE TABLE IF NOT EXISTS second_table (\n"
        "    org_id String,\n"
        "    id String\n"
        ") ENGINE = MergeTree\n"
        "ORDER BY (org_id, id)\n"
        '"""\n'
    )

    monkeypatch.setattr(
        org_deletion_module, "_CLICKHOUSE_MIGRATIONS_DIR", migration_dir
    )
    # @lru_cache(maxsize=1): must clear before (a prior test may have
    # cached the REAL migrations directory's result) and after (so the
    # next real caller recomputes against the real directory, not this
    # synthetic one).
    org_deletion_module._clickhouse_tables_from_migrations.cache_clear()
    try:
        discovered = org_deletion_module._clickhouse_tables_from_migrations()
    finally:
        org_deletion_module._clickhouse_tables_from_migrations.cache_clear()

    assert "first_table" in discovered
    assert "second_table" in discovered, (
        "the SECOND CREATE TABLE sharing a semicolon-chunk with the first "
        "must still be discovered"
    )


def test_migration_scan_does_not_discover_a_table_from_prose(tmp_path, monkeypatch):
    """PR #1602 round-3 review D (LOW, pre-existing): `_CREATE_TABLE_RE`
    matched mid-sentence PROSE inside a comment in migration 027's real
    source -- "Regex: table name in CREATE TABLE statement (handles..." --
    because the regex had no anchor requiring it to actually START a
    statement. A phantom `statement` table entered the discovery scan,
    producing a spurious "missing or has no org_id column; skipped."
    warning on EVERY production org deletion (it is not a real table,
    `system.columns` finds nothing for it, so it's always "skipped", never
    silently wrong -- but the noise is real and pre-existing).

    Reproduces the EXACT prose shape (a comment mentioning "CREATE TABLE
    statement" followed by a parenthesis later on the same logical
    sentence) in a synthetic migration file, alongside one REAL CREATE
    TABLE, and asserts only the real one is discovered.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    migration_dir = tmp_path / "clickhouse"
    migration_dir.mkdir()
    (migration_dir / "999_prose_table_name.py").write_text(
        "# Regex: table name in CREATE TABLE statement (handles optional "
        "db prefix + backticks), also matches org_id\n"
        '_CREATE_REAL_SQL = """\n'
        "CREATE TABLE IF NOT EXISTS real_table (\n"
        "    org_id String,\n"
        "    id String\n"
        ") ENGINE = MergeTree\n"
        "ORDER BY (org_id, id)\n"
        '"""\n'
    )

    monkeypatch.setattr(
        org_deletion_module, "_CLICKHOUSE_MIGRATIONS_DIR", migration_dir
    )
    org_deletion_module._clickhouse_tables_from_migrations.cache_clear()
    try:
        discovered = org_deletion_module._clickhouse_tables_from_migrations()
    finally:
        org_deletion_module._clickhouse_tables_from_migrations.cache_clear()

    assert "real_table" in discovered
    assert "statement" not in discovered, (
        "prose mentioning 'CREATE TABLE statement (...' must not be "
        "mistaken for an actual CREATE TABLE statement"
    )


def test_registry_has_no_phantom_statement_entry():
    """Regression guard for the ACTUAL migration 027 prose match (not the
    synthetic fixture above): the real, current migration set must not
    discover a `statement` table at all.
    """
    from dev_health_ops.api.services.org_deletion import (
        _clickhouse_tables_from_migrations,
    )

    assert "statement" not in _clickhouse_tables_from_migrations()


def test_clickhouse_derived_stores_has_no_duplicates():
    assert len(CLICKHOUSE_DERIVED_STORES) == len(set(CLICKHOUSE_DERIVED_STORES))


def test_external_derived_stores_are_typed_as_external():
    for store in EXTERNAL_DERIVED_STORES:
        assert store.kind is DerivedStoreKind.EXTERNAL


@pytest.mark.asyncio
async def test_purge_clickhouse_warns_when_registry_is_incomplete(monkeypatch):
    """Runtime defense-in-depth: if the registry ever drifts behind the real
    migration scan (the CI guard above should prevent this, but a warning
    fires anyway), org deletion still purges the table -- no behavior change
    -- but the drift is surfaced in the API response, not swallowed.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def __init__(self):
            self.commands = []

        def query(self, query, parameters=None):
            class _Result:
                result_rows: list[tuple[Any, ...]] = (
                    [("String",)] if "system.columns" in query else [(1,)]
                )

            return _Result()

        def command(self, query, parameters=None):
            self.commands.append((query, parameters))

    monkeypatch.setattr(
        org_deletion_module,
        "_clickhouse_tables_from_migrations",
        lambda: ("repos", "a_table_missing_from_the_registry"),
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    assert result.clickhouse.tables["a_table_missing_from_the_registry"] == 1
    drift_warning = next(
        warning
        for warning in result.warnings
        if "a_table_missing_from_the_registry" in warning
    )
    # No behavior change: the table is still purged even though unregistered.
    assert (
        "ALTER TABLE `a_table_missing_from_the_registry` DELETE"
        in clickhouse.commands[-1][0]
    )
    # PR #1602 review F7 (CONFIRMED), sharpened by round-2 review C3: this
    # run genuinely purged THIS SPECIFIC TABLE (dry_run is False, a real
    # client was resolved, the count was 1, and the DELETE succeeded), so
    # the per-table outcome may correctly say so -- see the dry-run,
    # unresolved-client, no-org-id, count-failure, and delete-failure
    # variants below, where that claim would be FALSE for THAT table.
    assert "a_table_missing_from_the_registry: purged (1 row(s))" in drift_warning


@pytest.mark.asyncio
async def test_purge_clickhouse_drift_warning_says_would_purge_on_dry_run(
    monkeypatch,
):
    """PR #1602 review F7 (CONFIRMED): on a dry run, `_purge_clickhouse`
    never issues a single `ALTER TABLE ... DELETE` (see the per-table `if
    dry_run or count == 0: continue` guard) -- the drift warning must not
    claim "this run still purged them" when nothing was actually deleted.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def query(self, query, parameters=None):
            class _Result:
                result_rows: list[tuple[Any, ...]] = (
                    [("String",)] if "system.columns" in query else [(1,)]
                )

            return _Result()

        def command(self, query, parameters=None):
            raise AssertionError("a dry run must never issue ALTER TABLE DELETE")

    monkeypatch.setattr(
        org_deletion_module,
        "_clickhouse_tables_from_migrations",
        lambda: ("repos", "a_table_missing_from_the_registry"),
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=True
    )
    await service._purge_clickhouse(result.organization_id, dry_run=True, result=result)

    drift_warning = next(
        warning
        for warning in result.warnings
        if "a_table_missing_from_the_registry" in warning
    )
    assert "a_table_missing_from_the_registry: would purge (1 row(s))" in drift_warning
    assert "a_table_missing_from_the_registry: purged" not in drift_warning


@pytest.mark.asyncio
async def test_purge_clickhouse_drift_warning_says_not_verified_when_client_unresolved(
    monkeypatch,
):
    """PR #1602 review F7 (CONFIRMED): when the ClickHouse client cannot be
    resolved at all (no URI configured), `_purge_clickhouse` returns before
    touching a single table -- the drift warning must say the unregistered
    tables were never verified or purged this run, not claim they "still"
    were.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    monkeypatch.setattr(
        org_deletion_module,
        "_clickhouse_tables_from_migrations",
        lambda: ("repos", "a_table_missing_from_the_registry"),
    )
    monkeypatch.setattr(org_deletion_module, "get_clickhouse_uri", lambda: None)

    service = OrganizationDeletionService(_NO_SESSION)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    assert result.clickhouse.tables == {}
    drift_warning = next(
        warning
        for warning in result.warnings
        if "a_table_missing_from_the_registry" in warning
    )
    assert "not verified" in drift_warning
    assert "this run purged them" not in drift_warning
    assert "this run would purge them" not in drift_warning


@pytest.mark.asyncio
async def test_purge_clickhouse_drift_warning_reports_no_org_id_per_table(
    monkeypatch,
):
    """PR #1602 round-2 review C3 (CONFIRMED): an unregistered table with
    no `org_id` column is skipped (already warned separately by the
    `system.columns` probe) -- but before this fix, the DRIFT warning's
    blanket dry_run-derived wording still claimed it was "purged" anyway.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def query(self, query, parameters=None):
            class _Result:
                # No `system.columns` row at all -- the table has no
                # org_id column (or does not exist).
                result_rows: list[tuple[Any, ...]] = []

            return _Result()

        def command(self, query, parameters=None):
            raise AssertionError("no org_id column -- must never reach DELETE")

    monkeypatch.setattr(
        org_deletion_module,
        "_clickhouse_tables_from_migrations",
        lambda: ("repos", "a_table_missing_from_the_registry"),
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    drift_warning = next(
        warning
        for warning in result.warnings
        if "a_table_missing_from_the_registry" in warning
        and "Per-table outcome" in warning
    )
    assert (
        "a_table_missing_from_the_registry: skipped (no org_id column)" in drift_warning
    )
    assert "a_table_missing_from_the_registry: purged" not in drift_warning


@pytest.mark.asyncio
async def test_purge_clickhouse_drift_warning_reports_count_failure_per_table(
    monkeypatch,
):
    """PR #1602 round-2 review C3 (CONFIRMED): a count query that fails
    used to return 0 -- indistinguishable from a genuinely empty table,
    which the old blanket wording would have called "purged" (0 rows,
    technically true but misleading: this table was never actually
    VERIFIED empty, the count itself errored).
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def query(self, query, parameters=None):
            if "system.columns" in query:

                class _ColumnsResult:
                    result_rows: list[tuple[Any, ...]] = [("String",)]

                return _ColumnsResult()
            raise RuntimeError("count query exploded")

        def command(self, query, parameters=None):
            raise AssertionError("count failed -- must never reach DELETE")

    monkeypatch.setattr(
        org_deletion_module,
        "_clickhouse_tables_from_migrations",
        lambda: ("repos", "a_table_missing_from_the_registry"),
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    drift_warning = next(
        warning
        for warning in result.warnings
        if "a_table_missing_from_the_registry" in warning
        and "Per-table outcome" in warning
    )
    assert (
        "a_table_missing_from_the_registry: not verified (count query failed)"
        in drift_warning
    )
    assert "a_table_missing_from_the_registry: purged" not in drift_warning


@pytest.mark.asyncio
async def test_purge_clickhouse_drift_warning_reports_delete_failure_per_table(
    monkeypatch,
):
    """PR #1602 round-2 review C3 (CONFIRMED): a DELETE that raises is
    caught and logged, but the old code never propagated that failure to
    the drift warning -- it still claimed "purged them".
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def query(self, query, parameters=None):
            class _Result:
                result_rows: list[tuple[Any, ...]] = (
                    [("String",)] if "system.columns" in query else [(3,)]
                )

            return _Result()

        def command(self, query, parameters=None):
            raise RuntimeError("delete exploded")

    monkeypatch.setattr(
        org_deletion_module,
        "_clickhouse_tables_from_migrations",
        lambda: ("repos", "a_table_missing_from_the_registry"),
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    assert result.clickhouse.tables["a_table_missing_from_the_registry"] == 3
    drift_warning = next(
        warning
        for warning in result.warnings
        if "a_table_missing_from_the_registry" in warning
        and "Per-table outcome" in warning
    )
    assert "a_table_missing_from_the_registry: failed (delete raised)" in drift_warning
    assert "a_table_missing_from_the_registry: purged" not in drift_warning


@pytest.mark.asyncio
async def test_purge_clickhouse_warns_on_a_registered_tables_failed_delete(
    monkeypatch,
):
    """PR #1602 round-3 review C (CONFIRMED, proven live on `repos`): the
    C3 per-table outcome tracking only ever surfaced through the
    unregistered-drift warning, which by construction only iterates the
    UNREGISTERED subset. A REGISTERED table's failed DELETE produced NO
    warning at all -- its `result.clickhouse.tables[t]` entry (the row
    count from BEFORE the failed delete) read exactly like a genuine
    success, indistinguishable from one. `repos` is registered
    (`derived_store_registry.CLICKHOUSE_DERIVED_STORES`); this must warn
    regardless.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def query(self, query, parameters=None):
            class _Result:
                result_rows: list[tuple[Any, ...]] = (
                    [("String",)] if "system.columns" in query else [(5,)]
                )

            return _Result()

        def command(self, query, parameters=None):
            raise RuntimeError("delete exploded")

    monkeypatch.setattr(
        org_deletion_module, "_clickhouse_tables_from_migrations", lambda: ("repos",)
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    assert result.clickhouse.tables["repos"] == 5
    assert any(
        "repos" in warning and "failed" in warning for warning in result.warnings
    ), (
        "a registered table's failed delete must still warn -- silence "
        "here is indistinguishable from success"
    )


@pytest.mark.asyncio
async def test_purge_clickhouse_warns_on_a_registered_tables_failed_count(
    monkeypatch,
):
    """PR #1602 round-3 review C, the companion case: a registered table's
    COUNT query failing (never even reaching the delete decision) must
    also warn -- `not verified` is not the same fact as `0 rows, safely
    skipped`.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def query(self, query, parameters=None):
            if "system.columns" in query:

                class _ColumnsResult:
                    result_rows: list[tuple[Any, ...]] = [("String",)]

                return _ColumnsResult()
            raise RuntimeError("count query exploded")

        def command(self, query, parameters=None):
            raise AssertionError("count failed -- must never reach DELETE")

    monkeypatch.setattr(
        org_deletion_module, "_clickhouse_tables_from_migrations", lambda: ("repos",)
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    assert any(
        "repos" in warning and "not verified" in warning for warning in result.warnings
    )


@pytest.mark.asyncio
async def test_purge_clickhouse_no_warning_for_a_registered_tables_clean_purge(
    monkeypatch,
):
    """Negative control: a registered table that purges CLEANLY must not
    spuriously warn -- only failed/not-verified outcomes should.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    class _FakeClickHouseClient:
        def __init__(self):
            self.commands = []

        def query(self, query, parameters=None):
            class _Result:
                result_rows: list[tuple[Any, ...]] = (
                    [("String",)] if "system.columns" in query else [(2,)]
                )

            return _Result()

        def command(self, query, parameters=None):
            self.commands.append((query, parameters))

    monkeypatch.setattr(
        org_deletion_module, "_clickhouse_tables_from_migrations", lambda: ("repos",)
    )

    clickhouse = _FakeClickHouseClient()
    service = OrganizationDeletionService(_NO_SESSION, clickhouse_client=clickhouse)
    result = org_deletion_module.DeletionResult(
        organization_id=str(uuid.uuid4()), dry_run=False
    )
    await service._purge_clickhouse(
        result.organization_id, dry_run=False, result=result
    )

    assert result.clickhouse.tables["repos"] == 2
    assert not any("repos" in warning for warning in result.warnings)


@pytest.mark.asyncio
async def test_purge_external_stores_visits_a_registered_store(monkeypatch):
    """PR #1602 review F8 (CONFIRMED): a SUCCESSFUL external-store visit is
    not a problem -- it must be recorded through a typed result bucket
    (``result.external``, parallel to ``result.postgres``/
    ``result.clickhouse``), not through ``result.warnings``, which is
    reserved for genuine problems (not-wired, visit failure -- see
    ``test_purge_external_stores_warns_when_not_wired`` and
    ``test_purge_external_stores_warns_on_visit_failure`` below).
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    calls: list[tuple[str, bool]] = []

    async def _visit(org_id: str, dry_run: bool) -> int:
        calls.append((org_id, dry_run))
        return 7

    fake_store = DerivedStore(
        name="fake_shadow_store",
        kind=DerivedStoreKind.EXTERNAL,
        visit=_visit,
    )
    monkeypatch.setattr(org_deletion_module, "EXTERNAL_DERIVED_STORES", (fake_store,))

    service = OrganizationDeletionService(_NO_SESSION)
    result = org_deletion_module.DeletionResult(organization_id="org-1", dry_run=True)
    await service._purge_external_stores("org-1", dry_run=True, result=result)

    assert calls == [("org-1", True)]
    assert result.external.tables["fake_shadow_store"] == 7
    assert result.external.total == 7
    assert not any("fake_shadow_store" in warning for warning in result.warnings), (
        "a successful visit is not a warning-worthy problem"
    )


@pytest.mark.asyncio
async def test_purge_external_stores_warns_on_visit_failure(monkeypatch):
    """PR #1602 review F8, the companion case: a FAILED visit is a genuine
    problem and must still go through ``result.warnings`` (unchanged from
    before this fix) -- only the success path moved to ``result.external``.
    """
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    async def _visit(org_id: str, dry_run: bool) -> int:
        raise RuntimeError("boom")

    failing_store = DerivedStore(
        name="failing_store", kind=DerivedStoreKind.EXTERNAL, visit=_visit
    )
    monkeypatch.setattr(
        org_deletion_module, "EXTERNAL_DERIVED_STORES", (failing_store,)
    )

    service = OrganizationDeletionService(_NO_SESSION)
    result = org_deletion_module.DeletionResult(organization_id="org-1", dry_run=False)
    await service._purge_external_stores("org-1", dry_run=False, result=result)

    assert "failing_store" not in result.external.tables
    assert any(
        "failing_store" in warning and "failed" in warning
        for warning in result.warnings
    )


@pytest.mark.asyncio
async def test_purge_external_stores_warns_when_not_wired(monkeypatch):
    from dev_health_ops.api.services import org_deletion as org_deletion_module

    unwired_store = DerivedStore(
        name="unwired_store", kind=DerivedStoreKind.EXTERNAL, visit=None
    )
    monkeypatch.setattr(
        org_deletion_module, "EXTERNAL_DERIVED_STORES", (unwired_store,)
    )

    service = OrganizationDeletionService(_NO_SESSION)
    result = org_deletion_module.DeletionResult(organization_id="org-1", dry_run=False)
    await service._purge_external_stores("org-1", dry_run=False, result=result)

    assert any(
        "unwired_store" in warning and "not wired" in warning
        for warning in result.warnings
    )
