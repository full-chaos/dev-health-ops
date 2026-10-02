"""Tests for the billing modules the Python CLI and API startup still use (stripe_client, bundle_validation) and the billing models."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from dev_health_ops.api.billing.stripe_client import reset_price_tier_map
from tests._helpers import tables_of


@pytest.fixture(autouse=True)
def _reset_price_map():
    reset_price_tier_map()
    yield
    reset_price_tier_map()


@pytest.fixture(autouse=True)
def _billing_env():
    with patch.dict("os.environ", {"APP_BASE_URL": "https://example.com"}):
        yield


# ---------------------------------------------------------------------------
# Checkout tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Portal tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Entitlements tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# stripe_client unit tests
# ---------------------------------------------------------------------------


def test_map_price_id_to_tier():
    from dev_health_ops.api.billing.stripe_client import map_price_id_to_tier

    with patch.dict(
        "os.environ",
        {"STRIPE_PRICE_ID_TEAM": "price_t", "STRIPE_PRICE_ID_ENTERPRISE": "price_e"},
    ):
        reset_price_tier_map()
        from dev_health_ops.licensing.types import LicenseTier

        assert map_price_id_to_tier("price_t") == LicenseTier.TEAM
        assert map_price_id_to_tier("price_e") == LicenseTier.ENTERPRISE
        assert map_price_id_to_tier("price_unknown") is None


def test_get_tier_from_line_items():
    from dev_health_ops.api.billing.stripe_client import get_tier_from_line_items
    from dev_health_ops.licensing.types import LicenseTier

    with patch.dict("os.environ", {"STRIPE_PRICE_ID_ENTERPRISE": "price_e"}):
        reset_price_tier_map()
        items = [{"price": {"id": "price_e"}}]
        assert get_tier_from_line_items(items) == LicenseTier.ENTERPRISE

    reset_price_tier_map()
    assert get_tier_from_line_items([]) == LicenseTier.TEAM


def test_get_tier_price_id():
    from dev_health_ops.api.billing.stripe_client import get_tier_price_id
    from dev_health_ops.licensing.types import LicenseTier

    with patch.dict("os.environ", {"STRIPE_PRICE_ID_TEAM": "price_t"}):
        reset_price_tier_map()
        assert get_tier_price_id(LicenseTier.TEAM) == "price_t"
        assert get_tier_price_id(LicenseTier.ENTERPRISE) is None


# ---------------------------------------------------------------------------
# FeatureBundle key validation — Layer 1 (write-time)
# ---------------------------------------------------------------------------


def test_validate_bundle_feature_keys_valid():
    """Creating a bundle with known keys succeeds."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    # "git_sync" and "api_access" are both in STANDARD_FEATURES
    validate_bundle_feature_keys(["git_sync", "api_access"])


def test_validate_bundle_feature_keys_accepts_acr_purchased_feature():
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    validate_bundle_feature_keys(["agent_context_runtime"])


def test_validate_bundle_feature_keys_unknown_raises():
    """Creating a bundle with an unknown key raises ValueError naming the key."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    with pytest.raises(ValueError) as exc_info:
        validate_bundle_feature_keys(["git_sync", "totally_fake_feature"])

    assert "totally_fake_feature" in str(exc_info.value)


def test_validate_bundle_feature_keys_empty_succeeds():
    """Empty feature list is valid (no keys to check)."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )

    validate_bundle_feature_keys([])


def test_validate_bundle_feature_keys_all_standard():
    """All STANDARD_FEATURES keys pass validation."""
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )
    from dev_health_ops.models.licensing import STANDARD_FEATURES

    all_keys = [key for key, *_rest in STANDARD_FEATURES]
    validate_bundle_feature_keys(all_keys)  # must not raise


# ---------------------------------------------------------------------------
# FeatureBundle key validation — Layer 2 (startup-time)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_bundle_keys_clean_db_passes():
    """Startup check passes when all bundles reference known keys."""

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("core-bundle", ["git_sync", "basic_analytics"]),
        ("team-bundle", ["investment_view", "api_access"]),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    # Should not raise
    await validate_bundle_keys(mock_session)


@pytest.mark.asyncio
async def test_validate_bundle_keys_stale_raises():
    """Startup check raises RuntimeError when a stale key is found."""

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("good-bundle", ["git_sync"]),
        ("bad-bundle", ["git_sync", "old_removed_feature"]),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    with pytest.raises(RuntimeError) as exc_info:
        await validate_bundle_keys(mock_session)

    assert (
        "old_removed_feature" in str(exc_info.value)
        or "integrity check failed" in str(exc_info.value).lower()
    )


@pytest.mark.asyncio
async def test_validate_bundle_keys_allow_stale_env_var():
    """ALLOW_STALE_FEATURE_BUNDLES=1 causes stale keys to be logged as warnings
    instead of raising RuntimeError."""

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("bad-bundle", ["unknown_key_xyz"]),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    with patch.dict("os.environ", {"ALLOW_STALE_FEATURE_BUNDLES": "1"}):
        # Should NOT raise — only warn
        await validate_bundle_keys(mock_session)


@pytest.mark.asyncio
async def test_validate_bundle_keys_empty_bundles_passes():
    """Startup check passes when no bundles exist."""

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    await validate_bundle_keys(mock_session)


@pytest.mark.asyncio
async def test_validate_bundle_keys_null_features_passes():
    """Bundles with null/empty features list are skipped without error."""

    from dev_health_ops.api.billing.bundle_validation import validate_bundle_keys

    mock_result = MagicMock()
    mock_result.all.return_value = [
        ("empty-bundle", []),
        ("null-bundle", None),
    ]
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    await validate_bundle_keys(mock_session)


# ---------------------------------------------------------------------------
# G4 (CHAOS-1207) — Bridge: plan subscription → org feature enablement
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# G7 (CHAOS-1210) — billing_prices ON DELETE CASCADE
#
# SQLite requires PRAGMA foreign_keys=ON to enforce FK constraints.
# We set it via a connection event listener so cascade fires in unit tests.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def billing_cascade_db(tmp_path):
    """SQLite DB with FK enforcement, containing billing + subscription tables."""

    from sqlalchemy import event as sa_event
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from dev_health_ops.models.billing import (
        BillingPlan,
        BillingPrice,
        FeatureBundle,
        PlanFeatureBundle,
    )
    from dev_health_ops.models.git import Base
    from dev_health_ops.models.subscriptions import Subscription, SubscriptionEvent
    from dev_health_ops.models.users import Organization

    db_path = tmp_path / "billing-cascade.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}", connect_args={"check_same_thread": False}
    )

    @sa_event.listens_for(engine.sync_engine, "connect")
    def _set_fk_pragma(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    _tables = tables_of(
        Organization,
        BillingPlan,
        BillingPrice,
        FeatureBundle,
        PlanFeatureBundle,
        Subscription,
        SubscriptionEvent,
    )

    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=_tables))

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_delete_billing_plan_cascades_to_prices(billing_cascade_db):
    """Deleting a BillingPlan removes its BillingPrice rows (G7, CHAOS-1210)."""
    from datetime import datetime, timezone

    from sqlalchemy import select

    from dev_health_ops.models.billing import BillingPlan, BillingPrice

    plan_id = uuid.uuid4()
    price_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    async with billing_cascade_db() as session:
        plan = BillingPlan(
            id=plan_id,
            key="cascade-plan",
            name="Cascade Plan",
            tier="team",
            created_at=now,
            updated_at=now,
        )
        price = BillingPrice(
            id=price_id,
            plan_id=plan_id,
            interval="monthly",
            amount=2900,
            created_at=now,
            updated_at=now,
        )
        session.add_all([plan, price])
        await session.commit()

    async with billing_cascade_db() as session:
        assert (
            await session.execute(
                select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one_or_none() is not None

    async with billing_cascade_db() as session:
        plan_obj = (
            await session.execute(select(BillingPlan).where(BillingPlan.id == plan_id))
        ).scalar_one()
        await session.delete(plan_obj)
        await session.commit()

    async with billing_cascade_db() as session:
        gone = (
            await session.execute(
                select(BillingPrice).where(BillingPrice.id == price_id)
            )
        ).scalar_one_or_none()
        assert gone is None, (
            "billing_prices row must cascade away when its plan is deleted"
        )


def test_subscription_billing_plan_fk_has_no_cascade():
    """Assert at model-metadata level that Subscription.billing_plan_id has no ondelete.

    G7 (CHAOS-1210): billing_prices.plan_id gets CASCADE; subscriptions.billing_plan_id
    intentionally does NOT, so subscription history survives plan deletion.

    NOTE: On PostgreSQL, deleting a plan with active subscriptions that reference
    its prices (via billing_price_id) will raise an IntegrityError unless those
    subscriptions are cleaned up first or billing_prices gets SET NULL — this is
    expected behaviour; subscription rows are historical records and should be
    archived before plan deletion in production.
    """
    from sqlalchemy import inspect

    from dev_health_ops.models.subscriptions import Subscription

    mapper = inspect(Subscription)
    for col in mapper.columns:
        if col.name == "billing_plan_id":
            fk = list(col.foreign_keys)[0]
            assert fk.ondelete is None or fk.ondelete.upper() != "CASCADE", (
                "subscriptions.billing_plan_id must NOT cascade — it is a historical reference"
            )
            return
    raise AssertionError("billing_plan_id column not found on Subscription model")
