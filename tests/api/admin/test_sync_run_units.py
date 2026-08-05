"""Sync run unit endpoint regressions."""

from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationSource,
    SyncRun,
    SyncRunUnit,
)
from dev_health_ops.models.settings import SyncWatermark
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    Integration,
    IntegrationSource,
    SyncRun,
    SyncRunUnit,
    SyncWatermark,
)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "sync-run-units.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded_state(session_maker):
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="test-org", name="Test Org", tier="pro"),
                User(id=user_id, email="admin@example.com", is_active=True),
            ]
        )
        await session.commit()
    return {"org_id": str(org_id), "user_id": str(user_id)}


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)
    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, seeded_state

    app.dependency_overrides.clear()


async def _seed_run_with_units(session_maker, org_id: str, unit_count: int) -> str:
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    run_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Integration(
                    id=integration_id,
                    org_id=org_id,
                    provider="github",
                    name="sync-test",
                    config={},
                    is_active=True,
                ),
                IntegrationSource(
                    id=source_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    provider="github",
                    source_type="repository",
                    external_id="owner/repo",
                    name="repo",
                    full_name="owner/repo",
                    metadata_={"owner": "owner"},
                    is_enabled=True,
                ),
                SyncRun(
                    id=run_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    triggered_by="test",
                    mode="incremental",
                    status="success",
                    total_units=unit_count,
                    completed_units=unit_count,
                    failed_units=0,
                ),
            ]
        )
        session.add_all(
            [
                SyncRunUnit(
                    org_id=org_id,
                    sync_run_id=run_id,
                    integration_id=integration_id,
                    source_id=source_id,
                    provider="github",
                    dataset_key="git",
                    cost_class="standard",
                    mode="incremental",
                    status="success",
                    attempts=1,
                )
                for _ in range(unit_count)
            ]
        )
        await session.commit()
    return str(run_id)


@pytest.mark.asyncio
async def test_get_sync_run_units_default_includes_every_unit(client, session_maker):
    ac, seeded_state = client
    run_id = await _seed_run_with_units(session_maker, seeded_state["org_id"], 201)

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()
    assert data["unit_count"] == 201
    assert len(data["units"]) == 201


@pytest.mark.asyncio
async def test_get_sync_run_units_explicit_limit_still_slices_units(
    client, session_maker
):
    ac, seeded_state = client
    run_id = await _seed_run_with_units(session_maker, seeded_state["org_id"], 5)

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units?limit=3")
    assert resp.status_code == 200
    data = resp.json()
    assert data["unit_count"] == 5
    assert len(data["units"]) == 3
    assert data["by_status"]["success"] == 5
    assert data["by_dataset"]["git"]["success"] == 5
    assert data["by_cost_class"]["standard"] == 5


# ---------------------------------------------------------------------------
# CHAOS-3430: watermark lag for ratcheting HEAVY datasets
# ---------------------------------------------------------------------------

EXTERNAL_ID = "owner/repo"


async def _seed_ratchet_run(
    session_maker,
    org_id: str,
    *,
    datasets: list[tuple[str, str]],
    watermarks: dict[str, datetime] | None = None,
) -> str:
    """Seed one successful incremental run over ``(dataset_key, cost_class)``.

    ``watermarks`` maps dataset_key -> stored watermark; a dataset omitted
    from it has no watermark row at all (never-stamped cold start).
    """
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    run_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Integration(
                    id=integration_id,
                    org_id=org_id,
                    provider="github",
                    name="ratchet-test",
                    config={},
                    is_active=True,
                ),
                IntegrationSource(
                    id=source_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    provider="github",
                    source_type="repository",
                    external_id=EXTERNAL_ID,
                    name="repo",
                    full_name=EXTERNAL_ID,
                    metadata_={"owner": "owner"},
                    is_enabled=True,
                ),
                SyncRun(
                    id=run_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    triggered_by="scheduler",
                    mode="incremental",
                    status="success",
                    total_units=len(datasets),
                    completed_units=len(datasets),
                    failed_units=0,
                ),
            ]
        )
        session.add_all(
            [
                SyncRunUnit(
                    org_id=org_id,
                    sync_run_id=run_id,
                    integration_id=integration_id,
                    source_id=source_id,
                    provider="github",
                    dataset_key=dataset_key,
                    cost_class=cost_class,
                    mode="incremental",
                    status="success",
                    attempts=1,
                )
                for dataset_key, cost_class in datasets
            ]
        )
        for dataset_key, stamped_at in (watermarks or {}).items():
            session.add(
                SyncWatermark(
                    org_id=org_id,
                    repo_id=EXTERNAL_ID,
                    source_id=EXTERNAL_ID,
                    target=dataset_key,
                    dataset_key=dataset_key,
                    last_synced_at=stamped_at,
                )
            )
        await session.commit()
    return str(run_id)


def _entry(data: dict, dataset_key: str) -> dict:
    matches = [e for e in data["dataset_freshness"] if e["dataset_key"] == dataset_key]
    assert matches, f"no freshness entry for {dataset_key!r}"
    return matches[0]


@pytest.mark.asyncio
async def test_units_response_flags_heavy_dataset_still_ratcheting(
    client, session_maker
):
    """A capped tick finalizes as SUCCESS; the response must still say behind."""
    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("commit-stats", "heavy")],
        watermarks={"commit-stats": now - timedelta(days=83)},
    )

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    assert resp.status_code == 200
    data = resp.json()

    # The run itself still reads fully successful — that is the whole problem.
    assert data["by_status"]["success"] == 1

    assert data["catching_up_dataset_count"] == 1
    entry = _entry(data, "commit-stats")
    assert entry["catching_up"] is True
    assert entry["cost_class"] == "heavy"
    assert entry["window_cap_days"] == 7
    assert entry["ticks_behind"] == 12
    assert entry["watermark_at"] is not None
    assert entry["lag_seconds"] >= int(timedelta(days=82).total_seconds())
    # Never surface a raw source id where a resolved name exists.
    assert entry["source_name"] == EXTERNAL_ID


@pytest.mark.asyncio
async def test_units_response_clears_the_flag_once_caught_up(client, session_maker):
    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("commit-stats", "heavy")],
        watermarks={"commit-stats": now - timedelta(hours=1)},
    )

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    data = resp.json()

    assert data["catching_up_dataset_count"] == 0
    entry = _entry(data, "commit-stats")
    assert entry["catching_up"] is False
    assert entry["ticks_behind"] is None
    # Lag is still reported honestly, just without the verdict.
    assert entry["lag_seconds"] is not None


@pytest.mark.asyncio
async def test_units_response_never_flags_a_non_heavy_dataset(client, session_maker):
    """Only HEAVY families ratchet — a trailing LIGHT watermark is not catch-up."""
    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("prs", "light"), ("commits", "medium")],
        watermarks={
            "prs": now - timedelta(days=200),
            "commits": now - timedelta(days=200),
        },
    )

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    data = resp.json()

    assert data["catching_up_dataset_count"] == 0
    for dataset_key in ("prs", "commits"):
        entry = _entry(data, dataset_key)
        assert entry["catching_up"] is False
        assert entry["ticks_behind"] is None
        assert entry["lag_seconds"] >= int(timedelta(days=199).total_seconds())


@pytest.mark.asyncio
async def test_units_response_reports_a_never_stamped_dataset_as_unknown(
    client, session_maker
):
    """No watermark row: nothing was read, so nothing is claimed as behind."""
    ac, seeded_state = client
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("commit-stats", "heavy")],
        watermarks=None,
    )

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    data = resp.json()

    assert data["catching_up_dataset_count"] == 0
    entry = _entry(data, "commit-stats")
    assert entry["watermark_at"] is None
    assert entry["lag_seconds"] is None
    assert entry["catching_up"] is False


@pytest.mark.asyncio
async def test_units_response_omits_datasets_that_never_stamp_a_watermark(
    client, session_maker
):
    """``repo-metadata`` has WatermarkBehavior.NONE — no lag exists to report."""
    ac, seeded_state = client
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("repo-metadata", "light"), ("commit-stats", "heavy")],
        watermarks=None,
    )

    resp = await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")
    data = resp.json()

    keys = {entry["dataset_key"] for entry in data["dataset_freshness"]}
    assert "repo-metadata" not in keys
    assert "commit-stats" in keys


@pytest.mark.asyncio
async def test_units_response_respects_the_configured_window_cap(
    client, session_maker, monkeypatch
):
    """The verdict must move with the cap the ratchet is actually configured to."""
    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("commit-stats", "heavy")],
        watermarks={"commit-stats": now - timedelta(days=20)},
    )

    monkeypatch.setenv("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", "30")
    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()
    assert _entry(data, "commit-stats")["catching_up"] is False
    assert _entry(data, "commit-stats")["window_cap_days"] == 30

    monkeypatch.setenv("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", "7")
    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()
    assert _entry(data, "commit-stats")["catching_up"] is True
    assert _entry(data, "commit-stats")["window_cap_days"] == 7


@pytest.mark.asyncio
async def test_units_response_dedupes_pairs_and_orders_deterministically(
    client, session_maker
):
    """Repeated (source, dataset) units collapse to one freshness entry."""
    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[
            ("commit-stats", "heavy"),
            ("commit-stats", "heavy"),
            ("blame", "heavy"),
        ],
        watermarks={"commit-stats": now - timedelta(days=83)},
    )

    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()
    entries = data["dataset_freshness"]

    assert [e["dataset_key"] for e in entries] == ["blame", "commit-stats"]
    assert data["catching_up_dataset_count"] == 1


@pytest.mark.asyncio
async def test_composite_work_item_unit_reports_each_child_watermark(
    client, session_maker
):
    """A collapsed work-item-family unit must not hide its child watermarks.

    CHAOS-2721: the planner collapses the work-item family into ONE composite
    unit with ``dataset_key="work-items"`` and boolean ``family_dataset_<key>``
    processor flags, while the worker
    (``workers/sync_units.py::_watermark_dataset_keys``) advances a SEPARATE
    watermark per enabled child. Reading the composite's raw dataset_key would
    report one freshness row for a dataset nothing stamps, and would render a
    badly-stale child -- here work-item-comments -- completely invisible.
    """
    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    org_id = seeded_state["org_id"]
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    run_id = uuid.uuid4()

    async with session_maker() as session:
        session.add_all(
            [
                Integration(
                    id=integration_id,
                    org_id=org_id,
                    provider="jira",
                    name="family-test",
                    config={},
                    is_active=True,
                ),
                IntegrationSource(
                    id=source_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    provider="jira",
                    source_type="project",
                    external_id=EXTERNAL_ID,
                    name="proj",
                    full_name=EXTERNAL_ID,
                    metadata_={},
                    is_enabled=True,
                ),
                SyncRun(
                    id=run_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    triggered_by="scheduler",
                    mode="incremental",
                    status="success",
                    total_units=1,
                    completed_units=1,
                    failed_units=0,
                ),
                SyncRunUnit(
                    org_id=org_id,
                    sync_run_id=run_id,
                    integration_id=integration_id,
                    source_id=source_id,
                    provider="jira",
                    dataset_key="work-items",
                    cost_class="medium",
                    mode="incremental",
                    status="success",
                    attempts=1,
                    processor_flags={
                        "family_dataset_work_items": True,
                        "family_dataset_work_item_comments": True,
                    },
                ),
            ]
        )
        # The worker stamps children independently: work-items is current,
        # work-item-comments is badly stale.
        session.add_all(
            [
                SyncWatermark(
                    org_id=org_id,
                    repo_id=EXTERNAL_ID,
                    source_id=EXTERNAL_ID,
                    target="work-items",
                    dataset_key="work-items",
                    last_synced_at=now - timedelta(hours=1),
                ),
                SyncWatermark(
                    org_id=org_id,
                    repo_id=EXTERNAL_ID,
                    source_id=EXTERNAL_ID,
                    target="work-item-comments",
                    dataset_key="work-item-comments",
                    last_synced_at=now - timedelta(days=45),
                ),
            ]
        )
        await session.commit()

    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()
    keys = {entry["dataset_key"] for entry in data["dataset_freshness"]}

    # Both children are reported, each against its OWN watermark.
    assert "work-items" in keys
    assert "work-item-comments" in keys

    current = _entry(data, "work-items")
    stale = _entry(data, "work-item-comments")
    assert current["lag_seconds"] < int(timedelta(days=1).total_seconds())
    assert stale["lag_seconds"] >= int(timedelta(days=44).total_seconds())
    # The stale child's lag is the whole point: it was invisible before.
    assert stale["watermark_at"] is not None


async def _seed_composite_run(
    session_maker,
    org_id: str,
    *,
    composite_cost_class: str = "medium",
    watermarks: dict[str, datetime] | None = None,
) -> str:
    """Seed one collapsed work-item-family composite unit."""
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    run_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Integration(
                    id=integration_id,
                    org_id=org_id,
                    provider="jira",
                    name="family-cost-test",
                    config={},
                    is_active=True,
                ),
                IntegrationSource(
                    id=source_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    provider="jira",
                    source_type="project",
                    external_id=EXTERNAL_ID,
                    name="proj",
                    full_name=EXTERNAL_ID,
                    metadata_={},
                    is_enabled=True,
                ),
                SyncRun(
                    id=run_id,
                    org_id=org_id,
                    integration_id=integration_id,
                    triggered_by="scheduler",
                    mode="incremental",
                    status="success",
                    total_units=1,
                    completed_units=1,
                    failed_units=0,
                ),
                SyncRunUnit(
                    org_id=org_id,
                    sync_run_id=run_id,
                    integration_id=integration_id,
                    source_id=source_id,
                    provider="jira",
                    dataset_key="work-items",
                    cost_class=composite_cost_class,
                    mode="incremental",
                    status="success",
                    attempts=1,
                    processor_flags={
                        "family_dataset_work_items": True,
                        "family_dataset_work_item_labels": True,
                        "family_dataset_work_item_comments": True,
                    },
                ),
            ]
        )
        for dataset_key, stamped_at in (watermarks or {}).items():
            session.add(
                SyncWatermark(
                    org_id=org_id,
                    repo_id=EXTERNAL_ID,
                    source_id=EXTERNAL_ID,
                    target=dataset_key,
                    dataset_key=dataset_key,
                    last_synced_at=stamped_at,
                )
            )
        await session.commit()
    return str(run_id)


@pytest.mark.asyncio
async def test_composite_children_report_their_own_cost_class(client, session_maker):
    """Each expanded child carries ITS registered cost class, not the composite's.

    The composite unit is one row with one cost_class ("work-items" is medium),
    but its children are separately registered — work-item-labels is LIGHT.
    Stamping every child with the composite's class misreports the child, and
    (see the next test) can silently suppress a child's catch-up verdict, since
    flagging keys on cost_class == "heavy".
    """
    ac, seeded_state = client
    run_id = await _seed_composite_run(session_maker, seeded_state["org_id"])

    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()

    assert _entry(data, "work-items")["cost_class"] == "medium"
    assert _entry(data, "work-item-labels")["cost_class"] == "light"
    assert _entry(data, "work-item-comments")["cost_class"] == "medium"


@pytest.mark.asyncio
async def test_heavy_child_of_a_non_heavy_composite_is_still_flagged(
    client, session_maker, monkeypatch
):
    """A HEAVY child must be flagged even when its composite is not heavy.

    No work-item-family child is registered heavy today, so this is latent
    rather than live — but provider-specific cost classes exist, so a child
    could become heavy, and inheriting the composite's class would suppress its
    catch-up verdict silently. Patch the real registry entry the production code
    reads, so this exercises the actual lookup rather than a stand-in.
    """
    from dataclasses import replace

    from dev_health_ops.sync import datasets as datasets_mod
    from dev_health_ops.sync.datasets import CostClass

    spec = datasets_mod.get_dataset_spec("jira", "work-item-comments")
    assert spec is not None
    monkeypatch.setitem(
        datasets_mod._REGISTRY["jira"],
        "work-item-comments",
        replace(spec, default_cost_class=CostClass.HEAVY),
    )

    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    run_id = await _seed_composite_run(
        session_maker,
        seeded_state["org_id"],
        composite_cost_class="medium",
        watermarks={"work-item-comments": now - timedelta(days=83)},
    )

    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()
    child = _entry(data, "work-item-comments")

    assert child["cost_class"] == "heavy"
    assert child["catching_up"] is True
    assert child["ticks_behind"] is not None
    assert data["catching_up_dataset_count"] == 1


@pytest.mark.asyncio
async def test_freshness_is_declared_run_scoped(client, session_maker):
    """The block must declare its scope so a filtered run's zero is not read
    as a workspace-wide clean bill of health (CHAOS-3430 F4)."""
    ac, seeded_state = client
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("commit-stats", "heavy")],
        watermarks=None,
    )

    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()

    assert data["dataset_freshness_scope"] == "run"


@pytest.mark.asyncio
async def test_freshness_never_leaks_another_orgs_watermark(client, session_maker):
    """A same-external-id watermark in another org must not satisfy this one."""
    ac, seeded_state = client
    now = datetime.now(timezone.utc)
    run_id = await _seed_ratchet_run(
        session_maker,
        seeded_state["org_id"],
        datasets=[("commit-stats", "heavy")],
        watermarks=None,
    )
    async with session_maker() as session:
        session.add(
            SyncWatermark(
                org_id=str(uuid.uuid4()),
                repo_id=EXTERNAL_ID,
                source_id=EXTERNAL_ID,
                target="commit-stats",
                dataset_key="commit-stats",
                last_synced_at=now,
            )
        )
        await session.commit()

    data = (await ac.get(f"/api/v1/admin/sync-runs/{run_id}/units")).json()
    entry = _entry(data, "commit-stats")
    assert entry["watermark_at"] is None
    assert entry["lag_seconds"] is None
