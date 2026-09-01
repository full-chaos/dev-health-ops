"""Tests for the async envelope inputs (CHAOS-4697 prerequisite).

`resolve_org_tier_async` and `resolve_licensed_features_async` are the two
values `graphql/app.py::get_context` needs to eventually mint an
effective-principal envelope (`principal_envelope.issue_effective_principal_envelope`),
but the seam is async while `resolve_org_tier` is sync and
`licensed_features` had no production producer at all before this change.

Tier tests exercise the precedence one branch at a time (`OrgLicense.tier`
wins, else `Organization.tier`, else COMMUNITY) so a fallback that silently
always returns community cannot hide behind a test that only exercises the
common path. `resolve_org_tier_async` delegates to `resolve_org_tier` via
`AsyncSession.run_sync` rather than reimplementing the precedence -- these
tests are exercising that delegation holds, not a second copy of the rule.

`licensed_features` tests prove both directions (an org WITH features
returns them, an org WITHOUT returns empty) AND that a genuine storage
failure raises `LicensedFeaturesLookupError` instead of silently returning
the same empty list an org-without-features would produce.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dev_health_ops.api.services.licensing import (
    LicensedFeaturesLookupError,
    resolve_licensed_features_async,
    resolve_org_tier_async,
)
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

_TABLES = tables_of(Organization, OrgLicense, FeatureFlag, OrgFeatureOverride)


@pytest_asyncio.fixture
async def db(tmp_path):
    """aiosqlite in-memory DB with the licensing tables, no feature_flags rows."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'licensing.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _make_org(
    maker: async_sessionmaker[AsyncSession],
    *,
    org_tier: str | None = "community",
) -> uuid.UUID:
    org_id = uuid.uuid4()
    async with maker() as session:
        session.add(
            Organization(
                id=org_id,
                slug=f"org-{org_id.hex[:8]}",
                name="Test Org",
                tier=org_tier,
            )
        )
        await session.commit()
    return org_id


# ---------------------------------------------------------------------------
# resolve_org_tier_async — precedence, one branch at a time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tier_async_org_license_wins_over_organization_tier(db) -> None:
    """OrgLicense.tier must win even when Organization.tier disagrees."""
    org_id = await _make_org(db, org_tier="community")
    async with db() as session:
        session.add(OrgLicense(org_id=org_id, tier="enterprise"))
        await session.commit()

    async with db() as session:
        tier = await resolve_org_tier_async(session, org_id)

    assert tier is LicenseTier.ENTERPRISE


@pytest.mark.asyncio
async def test_tier_async_falls_back_to_organization_tier_when_no_license(
    db,
) -> None:
    """No OrgLicense row -- must fall back to Organization.tier, not community."""
    org_id = await _make_org(db, org_tier="team")

    async with db() as session:
        tier = await resolve_org_tier_async(session, org_id)

    assert tier is LicenseTier.TEAM


@pytest.mark.asyncio
async def test_tier_async_defaults_to_community_when_nothing_set(db) -> None:
    """No OrgLicense row AND no Organization.tier -- must default to community,
    not raise or silently pick another tier."""
    org_id = await _make_org(db, org_tier=None)

    async with db() as session:
        tier = await resolve_org_tier_async(session, org_id)

    assert tier is LicenseTier.COMMUNITY


@pytest.mark.asyncio
async def test_tier_async_defaults_to_community_for_unknown_org(db) -> None:
    """An org_id with no Organization row at all -- must default to community,
    not raise (the seam may resolve org_id before Organization is guaranteed
    to exist, e.g. a stale/impersonated org_id)."""
    async with db() as session:
        tier = await resolve_org_tier_async(session, uuid.uuid4())

    assert tier is LicenseTier.COMMUNITY


# ---------------------------------------------------------------------------
# resolve_licensed_features_async — both directions + failure distinguishability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_licensed_features_org_with_access_returns_keys(db) -> None:
    org_id = await _make_org(db, org_tier="enterprise")
    async with db() as session:
        session.add(
            FeatureFlag(
                key="ai_review",
                name="AI Review",
                category="analytics",
                min_tier="community",
            )
        )
        await session.commit()

    async with db() as session:
        features = await resolve_licensed_features_async(session, org_id)

    assert features == ["ai_review"]


@pytest.mark.asyncio
async def test_licensed_features_org_without_access_returns_empty(db) -> None:
    """A feature exists but requires a tier this org does not have -- the
    result must be an empty list, not an error and not a stale value."""
    org_id = await _make_org(db, org_tier="community")
    async with db() as session:
        session.add(
            FeatureFlag(
                key="sso_saml",
                name="SSO/SAML",
                category="security",
                min_tier="enterprise",
            )
        )
        await session.commit()

    async with db() as session:
        features = await resolve_licensed_features_async(session, org_id)

    assert features == []


@pytest.mark.asyncio
async def test_licensed_features_no_registered_features_returns_empty(db) -> None:
    """No FeatureFlag rows registered at all -- genuine "no features",
    matching feature_flag_state's existing 'unregistered' treatment."""
    org_id = await _make_org(db, org_tier="enterprise")

    async with db() as session:
        features = await resolve_licensed_features_async(session, org_id)

    assert features == []


@pytest.mark.asyncio
async def test_licensed_features_disabled_flag_excluded(db) -> None:
    """A globally-disabled feature must be excluded even at a qualifying tier
    -- proves the result reflects the real decision, not just tier math."""
    org_id = await _make_org(db, org_tier="enterprise")
    async with db() as session:
        session.add(
            FeatureFlag(
                key="killed_feature",
                name="Killed Feature",
                category="core",
                min_tier="community",
                is_enabled=False,
            )
        )
        await session.commit()

    async with db() as session:
        features = await resolve_licensed_features_async(session, org_id)

    assert features == []


@pytest.mark.asyncio
async def test_licensed_features_storage_error_raises_not_empty_list(
    db, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A genuine storage failure (simulated the way it really happens: the
    row-load call underneath evaluate_org_features_async raises, which that
    function's own try/except turns into STORAGE_ERROR decisions -- not a
    raw exception) must RAISE, never silently return [] -- otherwise "no
    features" and "lookup failed" are indistinguishable to a caller building
    a security-relevant claim on this result.

    A naive implementation that only kept `decision.allowed` (STORAGE_ERROR
    decisions have allowed=False) would return `[]` here -- identical to
    the "no access" case above -- which is exactly the defect this test
    guards against.
    """
    org_id = await _make_org(db, org_tier="enterprise")
    async with db() as session:
        session.add(
            FeatureFlag(
                key="ai_review",
                name="AI Review",
                category="analytics",
                min_tier="community",
            )
        )
        await session.commit()

    from sqlalchemy.exc import SQLAlchemyError

    from dev_health_ops.licensing import feature_decisions

    async def _boom(*args: object, **kwargs: object) -> None:
        raise SQLAlchemyError("simulated storage failure")

    monkeypatch.setattr(
        feature_decisions, "load_feature_rows_async", _boom, raising=True
    )

    async with db() as session:
        with pytest.raises(LicensedFeaturesLookupError):
            await resolve_licensed_features_async(session, org_id)
