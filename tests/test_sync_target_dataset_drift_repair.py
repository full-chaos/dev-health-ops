"""CHAOS-4106: repair and prevent sync_targets / integration_datasets drift.

Two halves, tested together because they share one definition of "the operator
selected this": the alembic data repair for rows that already drifted, and the
self-healing reconciliation that stops new drift surviving an edit.
"""

from __future__ import annotations

import importlib
import json
import os
import uuid

import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.routers.sync import (
    _reconcile_dataset_rows_for_sync_targets,
)
from dev_health_ops.models import (
    Integration,
    IntegrationDataset,
    SyncConfiguration,
)
from dev_health_ops.models.git import Base
from dev_health_ops.sync.datasets import (
    OPERATOR_SELECTABLE_SYNC_TARGETS,
    operator_controlled_dataset_keys,
    planner_dataset_keys,
)
from tests._helpers import tables_of

WORK_ITEM_FAMILY = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)
# The production shape that produced the bug: work-items absent from the
# checkbox selection, its dataset rows enabled anyway.
PROD_GITHUB_TARGETS = ["prs", "deployments", "git", "cicd", "incidents", "tests"]


def _schema(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE sync_configurations (
                id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                sync_targets TEXT NOT NULL,
                integration_id TEXT,
                source_id TEXT
            )
            """
        )
    )
    connection.execute(
        sa.text(
            """
            CREATE TABLE integration_datasets (
                id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL,
                integration_id TEXT NOT NULL,
                dataset_key TEXT NOT NULL,
                is_enabled BOOLEAN NOT NULL
            )
            """
        )
    )


def _add_config(
    connection: sa.Connection,
    *,
    org_id: str,
    integration_id: str,
    provider: str,
    sync_targets: list[str],
    source_id: str | None = None,
) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO sync_configurations
                (id, org_id, provider, sync_targets, integration_id, source_id)
            VALUES (:id, :org_id, :provider, :targets, :integration_id, :source_id)
            """
        ),
        {
            "id": str(uuid.uuid4()),
            "org_id": org_id,
            "provider": provider,
            "targets": json.dumps(sync_targets),
            "integration_id": integration_id,
            "source_id": source_id,
        },
    )


def _add_datasets(
    connection: sa.Connection,
    *,
    org_id: str,
    integration_id: str,
    keys: tuple[str, ...],
    enabled: bool = True,
) -> None:
    for key in keys:
        connection.execute(
            sa.text(
                """
                INSERT INTO integration_datasets
                    (id, org_id, integration_id, dataset_key, is_enabled)
                VALUES (:id, :org_id, :integration_id, :key, :enabled)
                """
            ),
            {
                "id": str(uuid.uuid4()),
                "org_id": org_id,
                "integration_id": integration_id,
                "key": key,
                "enabled": enabled,
            },
        )


def _enabled_keys(connection: sa.Connection, integration_id: str) -> set[str]:
    rows = connection.execute(
        sa.text(
            """
            SELECT dataset_key FROM integration_datasets
            WHERE integration_id = :integration_id AND is_enabled
            """
        ),
        {"integration_id": integration_id},
    ).fetchall()
    return {row[0] for row in rows}


def _run_repair(connection: sa.Connection) -> None:
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0108_repair_sync_target_dataset_drift"
    )
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        migration.upgrade()


# ---------------------------------------------------------------------------
# The controlled universe -- the definition both halves share
# ---------------------------------------------------------------------------


def test_security_is_outside_operator_control_on_every_provider():
    """security is platform-managed, so no checkbox may ever disable it."""

    for provider in ("github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty"):
        assert "security" not in operator_controlled_dataset_keys(provider)
    assert "security" not in OPERATOR_SELECTABLE_SYNC_TARGETS


def test_blame_follows_the_git_checkbox_symmetrically():
    """blame IS operator-controlled -- via git-implies-blame, in both directions."""

    for provider in ("github", "gitlab"):
        universe = operator_controlled_dataset_keys(provider)
        assert "blame" in universe, "blame must be reachable from the git checkbox"
        with_git = set(planner_dataset_keys(provider, ["git"]))
        assert "blame" in with_git
        # git unchecked -> blame lands in the disable set, not stranded enabled.
        without_git = set(planner_dataset_keys(provider, ["prs"]))
        assert "blame" in universe - without_git


# ---------------------------------------------------------------------------
# The alembic data repair
# ---------------------------------------------------------------------------


def test_repair_disables_the_drifted_rows_and_spares_the_rest():
    """The production case: five work-item rows enabled, work-items unchecked."""

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = "org-prod", str(uuid.uuid4())
            _add_config(
                connection,
                org_id=org,
                integration_id=integration,
                provider="github",
                sync_targets=PROD_GITHUB_TARGETS,
            )
            selected = tuple(planner_dataset_keys("github", PROD_GITHUB_TARGETS))
            _add_datasets(
                connection, org_id=org, integration_id=integration, keys=selected
            )
            _add_datasets(
                connection,
                org_id=org,
                integration_id=integration,
                keys=WORK_ITEM_FAMILY + ("security",),
            )

            _run_repair(connection)

            enabled = _enabled_keys(connection, integration)
            for key in WORK_ITEM_FAMILY:
                assert key not in enabled, f"{key} drifted enabled and was not repaired"
            assert "security" in enabled, "platform-managed security must survive"
            assert "blame" in enabled, "git is checked, so blame must survive"
            for key in selected:
                assert key in enabled
    finally:
        engine.dispose()


def test_repair_is_idempotent():
    """Running twice must leave exactly the state one run produced."""

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = "org-idem", str(uuid.uuid4())
            _add_config(
                connection,
                org_id=org,
                integration_id=integration,
                provider="github",
                sync_targets=PROD_GITHUB_TARGETS,
            )
            _add_datasets(
                connection,
                org_id=org,
                integration_id=integration,
                keys=tuple(planner_dataset_keys("github", PROD_GITHUB_TARGETS))
                + WORK_ITEM_FAMILY,
            )

            _run_repair(connection)
            after_first = _enabled_keys(connection, integration)
            _run_repair(connection)
            after_second = _enabled_keys(connection, integration)

            assert after_first == after_second
    finally:
        engine.dispose()


@pytest.mark.parametrize("provider", ["linear", "jira"])
def test_repair_keeps_work_item_rows_for_a_provider_that_selects_them(provider: str):
    """Input symmetry: the same family, SELECTED, must survive untouched.

    The repair is only correct if it keys off the selection rather than off the
    dataset family. A linear or jira integration exists to sync work items;
    disabling those rows there would be the same bug with the sign flipped.
    """

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = f"org-{provider}", str(uuid.uuid4())
            _add_config(
                connection,
                org_id=org,
                integration_id=integration,
                provider=provider,
                sync_targets=["work-items"],
            )
            _add_datasets(
                connection,
                org_id=org,
                integration_id=integration,
                keys=WORK_ITEM_FAMILY,
            )

            _run_repair(connection)

            enabled = _enabled_keys(connection, integration)
            for key in WORK_ITEM_FAMILY:
                assert key in enabled, (
                    f"{provider} selects work-items; {key} must stay enabled"
                )
    finally:
        engine.dispose()


def test_repair_ignores_source_scoped_child_configs():
    """A child's narrower selection must not disable rows for its siblings."""

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = "org-child", str(uuid.uuid4())
            # Whole-integration parent selects git AND work-items.
            _add_config(
                connection,
                org_id=org,
                integration_id=integration,
                provider="github",
                sync_targets=["git", "work-items"],
            )
            # A source-scoped child covers only git.
            _add_config(
                connection,
                org_id=org,
                integration_id=integration,
                provider="github",
                sync_targets=["git"],
                source_id=str(uuid.uuid4()),
            )
            _add_datasets(
                connection,
                org_id=org,
                integration_id=integration,
                keys=WORK_ITEM_FAMILY + ("commits",),
            )

            _run_repair(connection)

            enabled = _enabled_keys(connection, integration)
            for key in WORK_ITEM_FAMILY:
                assert key in enabled, (
                    "the parent still selects work-items; a child must not disable it"
                )
    finally:
        engine.dispose()


def test_repair_skips_an_integration_whose_targets_cannot_be_mapped():
    """A PagerDuty config with non-operational targets is unreadable, not empty."""

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = "org-pd", str(uuid.uuid4())
            _add_config(
                connection,
                org_id=org,
                integration_id=integration,
                provider="pagerduty",
                sync_targets=["incidents"],
            )
            _add_datasets(
                connection,
                org_id=org,
                integration_id=integration,
                keys=("services", "incidents"),
            )

            _run_repair(connection)

            assert _enabled_keys(connection, integration) == {"services", "incidents"}
    finally:
        engine.dispose()


def test_repair_downgrade_is_a_no_op():
    """Documented irreversibility: downgrade must not resurrect disabled syncs."""

    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0108_repair_sync_target_dataset_drift"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = "org-down", str(uuid.uuid4())
            _add_datasets(
                connection,
                org_id=org,
                integration_id=integration,
                keys=WORK_ITEM_FAMILY,
                enabled=False,
            )
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.downgrade()
            assert _enabled_keys(connection, integration) == set()
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# The self-healing reconciliation (stops new drift surviving an edit)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def async_session(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'reconcile.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(Integration, IntegrationDataset, SyncConfiguration),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with maker() as session:
            yield session
    finally:
        await engine.dispose()


async def _seed_rows(
    session: AsyncSession,
    integration_id: uuid.UUID,
    keys: tuple[str, ...],
    *,
    enabled: bool = True,
) -> None:
    for key in keys:
        session.add(
            IntegrationDataset(
                id=uuid.uuid4(),
                org_id="org-reconcile",
                integration_id=integration_id,
                dataset_key=key,
                is_enabled=enabled,
                options={},
            )
        )
    await session.flush()


async def _enabled_after(session: AsyncSession, integration_id: uuid.UUID) -> set[str]:
    rows = (
        await session.execute(
            sa.select(IntegrationDataset.dataset_key).where(
                IntegrationDataset.integration_id == integration_id,
                IntegrationDataset.is_enabled.is_(True),
            )
        )
    ).fetchall()
    return {row[0] for row in rows}


@pytest.mark.asyncio
async def test_reconcile_repairs_drift_a_diff_would_never_see(async_session):
    """An edit that changes nothing still repairs a pre-existing drifted row.

    previous_sync_targets == sync_targets, so the old diff-based disable set
    (previous - new) is EMPTY. Only a self-healing reconcile reaches these rows.
    """

    integration_id = uuid.uuid4()
    await _seed_rows(
        async_session,
        integration_id,
        tuple(planner_dataset_keys("github", PROD_GITHUB_TARGETS)) + WORK_ITEM_FAMILY,
    )

    await _reconcile_dataset_rows_for_sync_targets(
        async_session,
        "org-reconcile",
        integration_id,
        "github",
        list(PROD_GITHUB_TARGETS),
        list(PROD_GITHUB_TARGETS),
        uuid.uuid4(),
    )

    enabled = await _enabled_after(async_session, integration_id)
    for key in WORK_ITEM_FAMILY:
        assert key not in enabled, f"{key} drifted and the reconcile did not repair it"
    assert "blame" in enabled, "git is still checked, so blame stays enabled"


@pytest.mark.asyncio
async def test_reconcile_still_enables_newly_selected_targets(async_session):
    """The CHAOS-3398 enable path must survive the self-heal rewrite."""

    integration_id = uuid.uuid4()
    await _seed_rows(async_session, integration_id, ("commits",))

    await _reconcile_dataset_rows_for_sync_targets(
        async_session,
        "org-reconcile",
        integration_id,
        "github",
        ["git", "work-items"],
        ["git"],
        uuid.uuid4(),
    )

    enabled = await _enabled_after(async_session, integration_id)
    for key in WORK_ITEM_FAMILY:
        assert key in enabled, f"{key} was newly selected and must be enabled"


@pytest.mark.asyncio
async def test_reconcile_leaves_platform_managed_security_alone(async_session):
    integration_id = uuid.uuid4()
    await _seed_rows(async_session, integration_id, ("security", "commits"))

    await _reconcile_dataset_rows_for_sync_targets(
        async_session,
        "org-reconcile",
        integration_id,
        "github",
        ["git"],
        ["git"],
        uuid.uuid4(),
    )

    assert "security" in await _enabled_after(async_session, integration_id)


@pytest.mark.asyncio
async def test_reconcile_does_not_disable_a_sibling_whole_integration_selection(
    async_session,
):
    """Two whole-integration configs share one row set; neither may clobber the other.

    IntegrationDataset rows are shared per integration (CHAOS-2762). The
    single-parent invariant (`_assert_single_planner_parent_for_integration`)
    only forbids a second PLANNER-MANAGED parent, and no unique constraint
    covers `source_id IS NULL` generally -- so two whole-integration configs on
    one integration are reachable.

    Widening the disable set to `universe - desired` (CHAOS-4106) makes that
    reachable state dangerous in a way the old `previous - desired` diff was
    not: an edit to a config that never selected work-items would disable the
    work-item rows a SIBLING config still selects, silently stopping the
    sibling's planner. The migration already unions desired across siblings;
    the live path must agree.
    """

    integration_id = uuid.uuid4()
    await _seed_rows(
        async_session,
        integration_id,
        tuple(planner_dataset_keys("github", ["git"])) + WORK_ITEM_FAMILY,
    )
    # The sibling: a second whole-integration config that DOES select work-items.
    async_session.add(
        SyncConfiguration(
            org_id="org-reconcile",
            name="sibling",
            provider="github",
            sync_targets=["work-items"],
            integration_id=integration_id,
            source_id=None,
        )
    )
    await async_session.flush()

    # Edit the git-only config. It never selected work-items.
    await _reconcile_dataset_rows_for_sync_targets(
        async_session,
        "org-reconcile",
        integration_id,
        "github",
        ["git"],
        ["git"],
        uuid.uuid4(),
    )

    enabled = await _enabled_after(async_session, integration_id)
    for key in WORK_ITEM_FAMILY:
        assert key in enabled, (
            f"{key} is still selected by a sibling whole-integration config; "
            "editing an unrelated config must not disable it"
        )


@pytest.mark.parametrize(
    "raw_targets",
    ["{}", "not json at all", '{"git": true}', "[1, 2]", '"git"'],
    ids=[
        "json-object",
        "invalid-json",
        "object-with-keys",
        "non-string-items",
        "bare-string",
    ],
)
def test_repair_fails_closed_on_unreadable_sync_targets(raw_targets: str):
    """Unreadable intent must never be read as "the operator selected nothing".

    An empty selection is a VALID statement that makes every operator-controlled
    dataset undesired -- i.e. it disables all of them. Mapping corrupt JSON onto
    that same value turns one bad row into a mass-disable for the whole
    integration (Codex adversarial review, round 2). The repair must skip.
    """

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = "org-corrupt", str(uuid.uuid4())
            connection.execute(
                sa.text(
                    """
                    INSERT INTO sync_configurations
                        (id, org_id, provider, sync_targets, integration_id, source_id)
                    VALUES (:id, :org, 'github', :targets, :integration_id, NULL)
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "org": org,
                    "targets": raw_targets,
                    "integration_id": integration,
                },
            )
            seeded = tuple(planner_dataset_keys("github", ["git"])) + WORK_ITEM_FAMILY
            _add_datasets(
                connection, org_id=org, integration_id=integration, keys=seeded
            )

            _run_repair(connection)

            assert _enabled_keys(connection, integration) == set(seeded), (
                "unreadable sync_targets must leave every shared row untouched"
            )
    finally:
        engine.dispose()


def test_repair_skips_the_whole_integration_when_one_sibling_is_unreadable():
    """Shared rows mean one unreadable config poisons the whole integration."""

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _schema(connection)
            org, integration = "org-mixed", str(uuid.uuid4())
            _add_config(
                connection,
                org_id=org,
                integration_id=integration,
                provider="github",
                sync_targets=["git"],
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO sync_configurations
                        (id, org_id, provider, sync_targets, integration_id, source_id)
                    VALUES (:id, :org, 'github', '{}', :integration_id, NULL)
                    """
                ),
                {"id": str(uuid.uuid4()), "org": org, "integration_id": integration},
            )
            seeded = tuple(planner_dataset_keys("github", ["git"])) + WORK_ITEM_FAMILY
            _add_datasets(
                connection, org_id=org, integration_id=integration, keys=seeded
            )

            _run_repair(connection)

            assert _enabled_keys(connection, integration) == set(seeded), (
                "a readable sibling must not authorise disabling rows the "
                "unreadable config may select"
            )
    finally:
        engine.dispose()


_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"


@pytest.mark.asyncio
async def test_reconcile_holds_a_per_integration_advisory_lock():
    """Observe the LOCK the reconciliation itself takes, not a lock I take here.

    The sibling read and the enable/disable writes are separate statements, so
    correctness under concurrency rests on a transaction-scoped advisory lock
    keyed on the integration. A test that only checked the final rows would
    pass just as happily with no lock at all -- it would be watching the
    outcome rather than the mechanism.

    So this calls the real reconciliation, keeps its transaction OPEN, and asks
    a second connection to take the same key with ``pg_try_advisory_xact_lock``
    -- which returns false only while someone else holds it. Removing the lock
    from the reconciliation turns this red.
    """

    uri = os.environ.get(_POSTGRES_URI_ENV)
    if not uri:
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    async_uri = uri.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(async_uri)
    probe_engine = create_async_engine(async_uri)
    org_id = "org-lock"
    integration_id = uuid.uuid4()
    lock_key = f"sync-target-dataset-reconcile:{org_id}:{integration_id}"
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        # The reconciliation's enable pass creates dataset rows, which carry an
        # FK to integrations; seed the parent so the lock, not a constraint, is
        # what this test exercises.
        async with engine.begin() as setup:
            await setup.execute(
                sa.text(
                    """
                    INSERT INTO integrations
                        (id, org_id, provider, name, config, is_active,
                         created_at, updated_at)
                    VALUES (:id, :org, 'github', 'lock-test',
                            CAST('{}' AS json), TRUE, now(), now())
                    """
                ),
                {"id": integration_id, "org": org_id},
            )
        async with maker() as session:
            # Real reconciliation, real transaction, still open below.
            await _reconcile_dataset_rows_for_sync_targets(
                session,
                org_id,
                integration_id,
                "github",
                ["git"],
                ["git"],
                uuid.uuid4(),
            )
            async with probe_engine.begin() as probe:
                acquired = (
                    await probe.execute(
                        sa.select(
                            sa.func.pg_try_advisory_xact_lock(
                                sa.func.hashtextextended(lock_key, 0)
                            )
                        )
                    )
                ).scalar_one()
            assert acquired is False, (
                "a second transaction took this integration's reconciliation "
                "lock while a reconciliation was in flight -- the "
                "read-then-write pair is not serialised"
            )
            await session.rollback()

        # After the holding transaction ends the key must be free, or this is a
        # deadlock generator rather than a serialiser.
        async with probe_engine.begin() as probe:
            freed = (
                await probe.execute(
                    sa.select(
                        sa.func.pg_try_advisory_xact_lock(
                            sa.func.hashtextextended(lock_key, 0)
                        )
                    )
                )
            ).scalar_one()
        assert freed is True, "the advisory lock outlived its transaction"
    finally:
        await engine.dispose()
        await probe_engine.dispose()
