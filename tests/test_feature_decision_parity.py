from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from dev_health_ops.licensing.feature_decisions import (
    evaluate_org_feature_async,
    evaluate_org_feature_sync,
)
from dev_health_ops.licensing.feature_policy import (
    FeatureDecisionContext,
    FeatureDecisionReason,
    FeatureOverrideSnapshot,
    decide_feature,
)
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

_FEATURE_KEY = "canonical_incident_ingestion"
_TABLES = tables_of(Organization, FeatureFlag, OrgFeatureOverride, OrgLicense)


@dataclass(frozen=True, slots=True)
class DecisionScenario:
    name: str
    expected_allowed: bool
    expected_reason: FeatureDecisionReason
    has_flag: bool = True
    globally_enabled: bool = True
    org_override: bool | None = None
    override_expires_at: datetime | None = None
    license_override: bool | None = None


_SCENARIOS = (
    DecisionScenario(
        name="missing-row",
        has_flag=False,
        expected_allowed=False,
        expected_reason=FeatureDecisionReason.FEATURE_NOT_REGISTERED,
    ),
    DecisionScenario(
        name="missing-override",
        expected_allowed=False,
        expected_reason=FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED,
    ),
    DecisionScenario(
        name="active-override",
        org_override=True,
        expected_allowed=True,
        expected_reason=FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE,
    ),
    DecisionScenario(
        name="false-override",
        org_override=False,
        expected_allowed=False,
        expected_reason=FeatureDecisionReason.ORG_OVERRIDE_DISABLED,
    ),
    DecisionScenario(
        name="expired-override",
        org_override=True,
        override_expires_at=datetime(2000, 1, 1, tzinfo=UTC),
        expected_allowed=False,
        expected_reason=FeatureDecisionReason.ORG_OVERRIDE_EXPIRED,
    ),
    DecisionScenario(
        name="global-kill-switch",
        globally_enabled=False,
        org_override=True,
        expected_allowed=False,
        expected_reason=FeatureDecisionReason.GLOBAL_DISABLED,
    ),
    DecisionScenario(
        name="license-override-denied",
        license_override=True,
        expected_allowed=False,
        expected_reason=FeatureDecisionReason.ORG_OVERRIDE_REQUIRED,
    ),
)


def test_override_expiring_at_evaluation_time_is_expired() -> None:
    boundary = datetime(2030, 1, 1, tzinfo=UTC)
    context = FeatureDecisionContext(
        feature_key=_FEATURE_KEY,
        is_registered=True,
        is_storage_valid=True,
        globally_enabled=True,
        min_tier=LicenseTier.COMMUNITY,
        org_tier=LicenseTier.ENTERPRISE,
        org_override=FeatureOverrideSnapshot(
            is_enabled=True,
            expires_at=boundary,
        ),
        license_override=None,
        evaluated_at=boundary,
    )

    decision = decide_feature(context)

    assert decision.allowed is False
    assert decision.reason is FeatureDecisionReason.ORG_OVERRIDE_EXPIRED


def test_expired_override_preserves_existing_explicit_purchase_fallback() -> None:
    boundary = datetime(2030, 1, 1, tzinfo=UTC)
    context = FeatureDecisionContext(
        feature_key="agent_context_runtime",
        is_registered=True,
        is_storage_valid=True,
        globally_enabled=True,
        min_tier=LicenseTier.COMMUNITY,
        org_tier=LicenseTier.ENTERPRISE,
        org_override=FeatureOverrideSnapshot(
            is_enabled=True,
            expires_at=boundary,
        ),
        license_override=True,
        evaluated_at=datetime(2030, 1, 2, tzinfo=UTC),
    )

    decision = decide_feature(context)

    assert decision.allowed is True
    assert decision.reason is FeatureDecisionReason.ENABLED_BY_LICENSE_OVERRIDE


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", _SCENARIOS, ids=lambda scenario: scenario.name)
async def test_sync_and_async_feature_decisions_have_parity(
    tmp_path,
    scenario: DecisionScenario,
) -> None:
    database_path = tmp_path / f"{scenario.name}.db"
    sync_engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(sync_engine, tables=_TABLES)
    org_id = uuid.uuid4()

    with Session(sync_engine) as session:
        session.add(
            Organization(
                id=org_id,
                slug=f"parity-{scenario.name}",
                name=f"Parity {scenario.name}",
                tier="enterprise",
            )
        )
        feature = None
        if scenario.has_flag:
            feature = FeatureFlag(
                key=_FEATURE_KEY,
                name="Canonical Incident Ingestion",
                category="integrations",
                min_tier="community",
                is_enabled=scenario.globally_enabled,
            )
            session.add(feature)
            session.flush()
        if scenario.org_override is not None and feature is not None:
            session.add(
                OrgFeatureOverride(
                    org_id=org_id,
                    feature_id=feature.id,
                    is_enabled=scenario.org_override,
                    expires_at=scenario.override_expires_at,
                )
            )
        if scenario.license_override is not None:
            session.add(
                OrgLicense(
                    org_id=org_id,
                    tier="enterprise",
                    features_override={_FEATURE_KEY: scenario.license_override},
                )
            )
        session.commit()
        sync_decision = evaluate_org_feature_sync(session, org_id, _FEATURE_KEY)
    sync_engine.dispose()

    async_engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    maker = async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        async with maker() as session:
            async_decision = await evaluate_org_feature_async(
                session,
                org_id,
                _FEATURE_KEY,
            )
    finally:
        await async_engine.dispose()

    assert sync_decision == async_decision
    assert sync_decision.allowed is scenario.expected_allowed
    assert sync_decision.reason is scenario.expected_reason


@pytest.mark.asyncio
async def test_async_feature_decision_storage_failure_fails_closed(tmp_path) -> None:
    database_path = tmp_path / "storage-failure.db"
    sync_engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(sync_engine, tables=_TABLES)
    org_id = uuid.uuid4()
    with Session(sync_engine) as session:
        session.add(
            Organization(
                id=org_id,
                slug="async-storage-failure",
                name="Async Storage Failure",
                tier="enterprise",
            )
        )
        session.commit()
    sync_engine.dispose()

    async_engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    maker = async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        async with maker() as session:
            await session.execute(text("DROP TABLE feature_flags"))
            await session.commit()
            decision = await evaluate_org_feature_async(
                session,
                org_id,
                _FEATURE_KEY,
            )
    finally:
        await async_engine.dispose()

    assert decision.allowed is False
    assert decision.reason is FeatureDecisionReason.STORAGE_ERROR
