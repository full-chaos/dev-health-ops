from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from dev_health_ops.api.services.licensing import FeatureService
from dev_health_ops.licensing.registry import (
    get_features_for_tier,
    is_explicit_purchase_feature,
)
from dev_health_ops.licensing.types import TIER_ORDER
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

_FEATURE_KEY = "canonical_incident_ingestion"


@pytest.fixture
def feature_session() -> Iterator[tuple[Session, uuid.UUID, uuid.UUID]]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=tables_of(Organization, FeatureFlag, OrgFeatureOverride, OrgLicense),
    )
    first_org_id = uuid.uuid4()
    second_org_id = uuid.uuid4()
    session = Session(engine)
    session.add_all(
        [
            Organization(
                id=first_org_id,
                slug="canonical-incidents-first",
                name="Canonical Incidents First",
                tier="enterprise",
            ),
            Organization(
                id=second_org_id,
                slug="canonical-incidents-second",
                name="Canonical Incidents Second",
                tier="enterprise",
            ),
            FeatureFlag(
                key=_FEATURE_KEY,
                name="Canonical Incident Ingestion",
                category="integrations",
                min_tier="community",
            ),
        ]
    )
    session.commit()

    try:
        yield session, first_org_id, second_org_id
    finally:
        session.close()
        engine.dispose()


def test_existing_explicit_purchase_license_override_enables_acr() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=tables_of(Organization, FeatureFlag, OrgFeatureOverride, OrgLicense),
    )
    org_id = uuid.uuid4()

    try:
        with Session(engine) as session:
            session.add_all(
                [
                    Organization(
                        id=org_id,
                        slug="explicit-purchase-characterization",
                        name="Explicit Purchase Characterization",
                        tier="team",
                    ),
                    FeatureFlag(
                        key="agent_context_runtime",
                        name="Agent Context Runtime",
                        category="integrations",
                        min_tier="community",
                    ),
                    OrgLicense(
                        org_id=org_id,
                        tier="team",
                        features_override={"agent_context_runtime": True},
                    ),
                ]
            )
            session.commit()

            decision = FeatureService(session).check_feature_access(
                org_id,
                "agent_context_runtime",
            )

        assert decision.allowed is True
    finally:
        engine.dispose()


def test_canonical_incident_ingestion_is_false_for_every_tier() -> None:
    for tier in TIER_ORDER:
        features = get_features_for_tier(tier)

        assert features[_FEATURE_KEY] is False

    assert is_explicit_purchase_feature(_FEATURE_KEY) is True


def test_canonical_incident_ingestion_requires_org_override(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, org_id, _ = feature_session

    decision = FeatureService(session).check_feature_access(org_id, _FEATURE_KEY)

    assert decision.allowed is False


def test_canonical_incident_ingestion_org_override_is_org_scoped(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, enabled_org_id, disabled_org_id = feature_session
    feature = session.query(FeatureFlag).filter_by(key=_FEATURE_KEY).one()
    session.add(
        OrgFeatureOverride(
            org_id=enabled_org_id,
            feature_id=feature.id,
            is_enabled=True,
        )
    )
    session.commit()

    enabled = FeatureService(session).check_feature_access(
        enabled_org_id,
        _FEATURE_KEY,
    )
    disabled = FeatureService(session).check_feature_access(
        disabled_org_id,
        _FEATURE_KEY,
    )

    assert enabled.allowed is True
    assert disabled.allowed is False


def test_canonical_incident_ingestion_false_override_fails_closed(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, org_id, _ = feature_session
    feature = session.query(FeatureFlag).filter_by(key=_FEATURE_KEY).one()
    session.add(
        OrgFeatureOverride(
            org_id=org_id,
            feature_id=feature.id,
            is_enabled=False,
        )
    )
    session.commit()

    decision = FeatureService(session).check_feature_access(org_id, _FEATURE_KEY)

    assert decision.allowed is False


def test_canonical_incident_ingestion_expired_override_fails_closed(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, org_id, _ = feature_session
    feature = session.query(FeatureFlag).filter_by(key=_FEATURE_KEY).one()
    session.add(
        OrgFeatureOverride(
            org_id=org_id,
            feature_id=feature.id,
            is_enabled=True,
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
    )
    session.commit()

    decision = FeatureService(session).check_feature_access(org_id, _FEATURE_KEY)

    assert decision.allowed is False


def test_canonical_incident_ingestion_global_kill_switch_wins(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, org_id, _ = feature_session
    feature = session.query(FeatureFlag).filter_by(key=_FEATURE_KEY).one()
    feature.is_enabled = False
    session.add(
        OrgFeatureOverride(
            org_id=org_id,
            feature_id=feature.id,
            is_enabled=True,
        )
    )
    session.commit()

    decision = FeatureService(session).check_feature_access(org_id, _FEATURE_KEY)

    assert decision.allowed is False


def test_canonical_incident_ingestion_license_override_cannot_enable(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, org_id, _ = feature_session
    session.add(
        OrgLicense(
            org_id=org_id,
            tier="enterprise",
            features_override={_FEATURE_KEY: True},
        )
    )
    session.commit()

    decision = FeatureService(session).check_feature_access(org_id, _FEATURE_KEY)

    assert decision.allowed is False


def test_canonical_incident_ingestion_override_removal_rolls_back(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, org_id, _ = feature_session
    feature = session.query(FeatureFlag).filter_by(key=_FEATURE_KEY).one()
    override = OrgFeatureOverride(
        org_id=org_id,
        feature_id=feature.id,
        is_enabled=True,
    )
    session.add(override)
    session.commit()
    session.delete(override)
    session.commit()

    decision = FeatureService(session).check_feature_access(org_id, _FEATURE_KEY)

    assert decision.allowed is False


def test_canonical_incident_ingestion_storage_failure_fails_closed(
    feature_session: tuple[Session, uuid.UUID, uuid.UUID],
) -> None:
    session, org_id, _ = feature_session
    session.execute(text("DROP TABLE feature_flags"))
    session.commit()

    decision = FeatureService(session).check_feature_access(org_id, _FEATURE_KEY)

    assert decision.allowed is False
