"""CHAOS-4106: coverage scope must respect the operator's dataset intent.

``resolve_effective_scope`` is the Python half of a lockstep pair with
``resolveScope`` in ``internal/synccoverage/repository.go``. Both narrow a
target-scoped config's dataset list to the rows the operator left enabled, so
coverage stops advertising backfill windows the planner
(``sync/planner.py::_load_enabled_datasets``) would refuse to plan.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.sync_coverage import (
    _CoverageQueryBudget,
    resolve_effective_scope,
)
from dev_health_ops.models import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncConfiguration,
)
from dev_health_ops.models.git import Base
from tests._helpers import tables_of

ORG_ID = "org-chaos-4106"
WORK_ITEM_FAMILY = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)
GIT_FAMILY = ("repo-metadata", "commits", "commit-stats", "files")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'scope.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(
                    Integration,
                    IntegrationSource,
                    IntegrationDataset,
                    SyncConfiguration,
                ),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db(session_maker):
    async with session_maker() as session:
        yield session


async def _seed(
    db: AsyncSession,
    *,
    enabled_keys: tuple[str, ...],
    disabled_keys: tuple[str, ...] = (),
    sync_targets: list[str],
    planner_managed: bool,
    source_scoped: bool = False,
) -> SyncConfiguration:
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    db.add(
        Integration(
            id=integration_id,
            org_id=ORG_ID,
            provider="github",
            name="acme",
            is_active=True,
        )
    )
    db.add(
        IntegrationSource(
            id=source_id,
            org_id=ORG_ID,
            integration_id=integration_id,
            provider="github",
            source_type="repository",
            external_id="acme/api",
            name="api",
            full_name="acme/api",
            is_enabled=True,
        )
    )
    for key in enabled_keys:
        db.add(
            IntegrationDataset(
                id=uuid.uuid4(),
                org_id=ORG_ID,
                integration_id=integration_id,
                dataset_key=key,
                is_enabled=True,
                options={},
            )
        )
    for key in disabled_keys:
        db.add(
            IntegrationDataset(
                id=uuid.uuid4(),
                org_id=ORG_ID,
                integration_id=integration_id,
                dataset_key=key,
                is_enabled=False,
                options={},
            )
        )
    config = SyncConfiguration(
        org_id=ORG_ID,
        name="github",
        provider="github",
        sync_targets=sync_targets,
        sync_options={},
        is_active=True,
        planner_managed=planner_managed,
        integration_id=integration_id,
        source_id=source_id if source_scoped else None,
    )
    db.add(config)
    await db.flush()
    return config


@pytest.mark.asyncio
async def test_target_scoped_scope_excludes_user_disabled_datasets(db: AsyncSession):
    """The CHAOS-4106 case: selected in sync_targets, switched off in the intent plane."""

    config = await _seed(
        db,
        enabled_keys=GIT_FAMILY,
        disabled_keys=WORK_ITEM_FAMILY,
        sync_targets=["git", "work-items"],
        planner_managed=False,
    )

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())

    for key in WORK_ITEM_FAMILY:
        assert key not in scope.dataset_keys, (
            f"{key} is disabled in integration_datasets but still in coverage scope"
        )
    # The enabled half of the same selection must survive untouched.
    for key in GIT_FAMILY:
        assert key in scope.dataset_keys


@pytest.mark.asyncio
async def test_source_scoped_child_scope_excludes_user_disabled_datasets(
    db: AsyncSession,
):
    config = await _seed(
        db,
        enabled_keys=GIT_FAMILY,
        disabled_keys=WORK_ITEM_FAMILY,
        sync_targets=["git", "work-items"],
        planner_managed=True,
        source_scoped=True,
    )

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())

    assert not set(scope.dataset_keys).intersection(WORK_ITEM_FAMILY)
    assert set(GIT_FAMILY).issubset(set(scope.dataset_keys))


@pytest.mark.asyncio
async def test_planner_managed_parent_keeps_platform_managed_datasets(db: AsyncSession):
    """The narrowing must NOT run for a planner-managed parent.

    ``blame`` and ``security`` are enabled but derive from no operator-selectable
    target, so intersecting the parent's scope with sync_targets would blind
    coverage to two datasets that really are syncing.
    """

    config = await _seed(
        db,
        enabled_keys=GIT_FAMILY + ("blame", "security"),
        sync_targets=["git"],
        planner_managed=True,
    )

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())

    assert "blame" in scope.dataset_keys
    assert "security" in scope.dataset_keys


@pytest.mark.asyncio
async def test_unseeded_intent_plane_does_not_blank_coverage(db: AsyncSession):
    """Zero dataset rows means "never seeded", not "everything disabled"."""

    config = await _seed(
        db,
        enabled_keys=(),
        sync_targets=["git"],
        planner_managed=False,
    )

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())

    assert set(GIT_FAMILY).issubset(set(scope.dataset_keys))


@pytest.mark.asyncio
async def test_all_selected_datasets_disabled_yields_empty_scope(db: AsyncSession):
    """Disabling every selection must empty the scope, not invert into the fallback.

    The pre-existing "no dataset keys -> use every enabled row" fallback would
    otherwise turn a fully opted-out selection into "advertise everything".
    """

    config = await _seed(
        db,
        enabled_keys=("security",),
        disabled_keys=GIT_FAMILY,
        sync_targets=["git"],
        planner_managed=False,
    )

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())

    assert scope.dataset_keys == ()


@pytest.mark.asyncio
async def test_rows_present_but_all_disabled_yields_empty_scope(db: AsyncSession):
    """The TRUE all-disabled state: rows exist for this integration, none enabled.

    Distinct from `test_all_selected_datasets_disabled_yields_empty_scope`, which
    leaves an unrelated `security` row enabled and therefore never exercises the
    empty-enabled-set branch at all (Codex adversarial review, round 2). Because
    the intent query filters `is_enabled IS TRUE`, an empty result is ambiguous
    between "never seeded" and "all switched off" -- and reading the second as
    the first advertises exactly the datasets the operator opted out of.
    """

    config = await _seed(
        db,
        enabled_keys=(),
        disabled_keys=GIT_FAMILY,
        sync_targets=["git"],
        planner_managed=False,
    )

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())

    assert scope.dataset_keys == ()
