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
    # PR #1602 review F7 (CONFIRMED): this run genuinely purged (dry_run is
    # False and a real client was resolved), so the warning may correctly
    # claim so -- see the dry-run and unresolved-client variants below,
    # where that claim would be FALSE.
    assert "purged them" in drift_warning
    assert "would purge" not in drift_warning


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
    assert "would purge" in drift_warning
    assert "this run still purged them" not in drift_warning


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
